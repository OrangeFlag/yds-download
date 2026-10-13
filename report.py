"""Update existing backup course cards from verified downloader events."""
import atexit
import fcntl
from datetime import datetime
import json
from local_paths import local_path
import os
import subprocess
from pathlib import Path
import re
from threading import Event, Lock, Thread
import time
import unicodedata
import uuid

import network
import storage

BEGIN = '<!-- yds-download:begin -->'
END = '<!-- yds-download:end -->'
BLOCK = re.compile(re.escape(BEGIN) + r'.*?' + re.escape(END) + r'\n?', re.S)
PUBLISHERS = {}
PUBLISH_LOCK = Lock()


def stop_reports():
    with PUBLISH_LOCK:
        workers = list(PUBLISHERS.values())
    for worker, stopping in workers:
        stopping.set()
    for worker, _ in workers:
        worker.join()


atexit.register(stop_reports)


def queue_report(destination, repo, mark, git_push=False, message=None):
    from download import write_json
    destination = Path(destination)
    storage.check(destination)
    directory = destination / '.download/report-pending'
    directory.mkdir(exist_ok=True)
    descriptor = os.open(directory.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    path = directory / f'{time.time_ns()}-{uuid.uuid4().hex}.json'
    write_json(path, dict(repo=str(repo), mark=mark, git_push=git_push, message=message,
                         attempts=0, next_attempt=0))
    retry_reports(destination)


def _publish_reports(destination, stopping):
    from download import write_json
    directory = destination / '.download/report-pending'
    try:
        with (directory / 'publish.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            for path in sorted(directory.glob('*.json')):
                job = json.loads(path.read_text())
                if stopping.is_set() or job['next_attempt'] > time.time():
                    return
                while not stopping.is_set():
                    try:
                        update_report(destination, Path(job['repo']), job['mark'], job['git_push'], job['message'], stopping=stopping)
                    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
                        if storage.disk_error(error):
                            raise
                        print(f'REPORT PENDING: {error}', flush=True)
                        if job['git_push'] and network.wait_if_offline(stopping):
                            continue
                        job['attempts'] += 1
                        job['next_attempt'] = time.time() + min(300, 30 * 2 ** min(job['attempts'] - 1, 4))
                        write_json(path, job)
                        return
                    path.unlink()
                    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                    break
    except OSError as error:
        print(f'REPORT PENDING: {error}', flush=True)


def retry_reports(destination, stopping=None):
    destination = Path(destination)
    storage.check(destination)
    if not (destination / '.download/report-pending').exists():
        return
    with PUBLISH_LOCK:
        saved = PUBLISHERS.get(destination)
        if saved is None or not saved[0].is_alive():
            stopping = stopping if stopping is not None else Event()
            worker = Thread(target=_publish_reports, args=(destination, stopping), daemon=True)
            PUBLISHERS[destination] = worker, stopping
            worker.start()


def wait_reports(destinations):
    while True:
        pending = False
        for destination in destinations:
            retry_reports(destination)
            pending |= any((Path(destination) / '.download/report-pending').glob('*.json'))
        if not pending:
            return
        time.sleep(1)


def normalized(value):
    return unicodedata.normalize('NFC', value)


def update_report(destination, repo, mark, git_push=False, message=None, stopping=None):
    repo = repo.resolve()
    if (repo / ".git").exists():
        common = Path(subprocess.check_output(["git", "-C", str(repo), "rev-parse", "--path-format=absolute", "--git-common-dir"], text=True, timeout=15 if stopping is not None else 120).strip())
        with (common / "yds-report.lock").open("a") as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if stopping is not None and stopping.wait(.25):
                        raise InterruptedError('Report publishing interrupted')
                    if stopping is None:
                        time.sleep(.25)
            if stopping is not None and stopping.is_set():
                raise InterruptedError('Report publishing interrupted')
            return _update_report(destination, repo, mark, git_push, message, stopping)
    return _update_report(destination, repo, mark, git_push, message, stopping)


def _update_report(destination, repo, mark, git_push=False, message=None, stopping=None):
    from download import read_events
    if not mark or any(c in mark for c in '|\n\r'):
        raise ValueError('Mark must be nonempty and contain no pipes or line breaks')
    repo = repo.resolve()
    dirty = set()
    git_timeout = 15 if stopping is not None else 120
    if git_push:
        staged = subprocess.run(['git', '-C', str(repo), 'diff', '--cached', '--quiet'], timeout=git_timeout)
        if staged.returncode != 0:
            raise RuntimeError('Report publish requires an empty Git index')
        subprocess.run(['git', '-C', str(repo), 'symbolic-ref', '--quiet', 'HEAD'], check=True, stdout=subprocess.DEVNULL, timeout=git_timeout)
        dirty = set(subprocess.check_output(['git', '-C', str(repo), 'diff', '--name-only', '-z'], timeout=git_timeout).decode().split('\0'))
    state = destination / '.download'
    source = json.loads((state / 'source.json').read_text(encoding='utf-8'))['source'].rstrip('/')
    events = {}
    observations = {}
    for event in read_events(state / 'events.jsonl'):
        key = normalized(event['path'])
        events[key] = event
        if event.get('source_current'):
            observations[key] = event
    expected = dict(events)
    manifest_times = {}
    for manifest in sorted(state.glob('*-manifest.json'), key=lambda path: path.stat().st_mtime_ns):
        rows = {normalized(row['path']): row for row in json.loads(manifest.read_text())}
        expected.update(rows)
        manifest_times.update(dict.fromkeys(rows, manifest.stat().st_mtime))
    # Deep observes current source identities while retaining the worker's older manifests.
    for key, event in observations.items():
        if datetime.fromisoformat(event['time']).timestamp() <= manifest_times.get(key, 0):
            continue
        expected[key] = event
        if (event.get('status') != 'valid' or 'mtime_ns' not in event
                or (events[key]['size'], events[key]['sha256']) == (event['size'], event['sha256'])):
            continue
        local = Path(event['local']) if event.get('local') else local_path(destination, event['path'])
        if (local.exists() and local.stat().st_size == event['size']
                and local.stat().st_mtime_ns == event['mtime_ns'] and not Path(str(local) + '.aria2').exists()):
            events[key] = event
    def same_identity(row):
        identity = expected.get(normalized(row['path']))
        return identity is None or (identity['size'], identity['sha256']) == (row['size'], row['sha256'])
    external = {}
    known_external = {}
    registry = state / 'local-copies.json'
    if registry.exists():
        for copy in json.loads(registry.read_text(encoding='utf-8')):
            if copy['source'].rstrip('/') != source:
                continue
            known_external[normalized(copy['path'])] = dict(copy, status='missing')
            if copy['status'] != 'valid' or not same_identity(copy):
                continue
            local = Path(copy['local'])
            if local.exists():
                stat = local.stat()
                if stat.st_size == copy['size'] and stat.st_mtime_ns == copy['mtime_ns'] and not Path(str(local) + '.aria2').exists():
                    external[normalized(copy['path'])] = copy
    partial = {}
    known_partial = {}
    registry = state / 'partial-copies.json'
    if registry.exists():
        for copy in json.loads(registry.read_text()):
            if copy['source'].rstrip('/') != source:
                continue
            key = normalized(copy['path'])
            known_partial[key] = copy
            local = (destination / copy['recovered_path']).resolve()
            if not local.is_relative_to(destination.resolve()):
                raise ValueError('Unsafe partial copy path')
            if local.exists():
                stat = local.stat()
                if (stat.st_size == copy['local_size'] and stat.st_mtime_ns == copy['mtime_ns']
                        and (copy.get('downloaded_bytes') or 0) * 2 > copy['size']
                        and not copy.get('sha256_mismatch') and same_identity(copy)):
                    partial[key] = copy
    terminal = {}
    for path in (destination / 'corrupted').glob('**/failed.json'):
        for event in json.loads(path.read_text(encoding='utf-8')):
            key = normalized(event['path'])
            if key not in terminal or event['time'] > terminal[key]['time']:
                terminal[key] = event
    changes = []
    reports = []
    totals = {}
    for card in sorted((repo / 'courses').glob('*.md')):
        original = card.read_text(encoding='utf-8')
        year_match = re.search(r'^- Год: (\d+)$', original, re.M)
        old_status = re.search(r'^\*\*Статус:\*\* (.+)$', original, re.M)
        if year_match and old_status:
            counts = totals.setdefault(year_match[1], [0, 0])
            counts[0 if old_status[1] == 'копия есть' else 1] += 1
        source_match = re.search(r'^- Источник: .*\((https?://[^)]+)\)', original, re.M)
        path_match = re.search(r'^- Путь: `(.+)`$', original, re.M)
        if not source_match or not path_match or source_match[1].rstrip('/') != source:
            continue
        prefix = '/' + path_match[1].strip('/') + '/'
        if not any(key.startswith(normalized(prefix)) for key in events.keys() | known_external.keys() | known_partial.keys()):
            continue
        rows = original.splitlines(keepends=True)
        results = []
        for index, line in enumerate(rows):
            if not line.startswith('| `'):
                continue
            cells = re.split(r'(?<!\\)\|', line.strip())[1:-1]
            if len(cells) != 5:
                raise ValueError('Unexpected file table: ' + str(card))
            name = cells[0].strip()[1:-1].replace('\\|', '|')
            key = normalized(prefix + name)
            local_copy = external.get(key)
            partial_copy = partial.get(key)
            event = local_copy or events.get(key) or partial_copy or known_external.get(key) or known_partial.get(key)
            status = 'unknown'
            if event:
                status = event['status']
                local = Path(local_copy['local']) if local_copy else local_path(destination, event['path'])
                if status == 'valid' and (not same_identity(event) or not local.exists() or local.stat().st_size != event['size']
                                         or ('mtime_ns' in event and local.stat().st_mtime_ns != event['mtime_ns'])
                                         or Path(str(local) + '.aria2').exists()):
                    status = 'missing'
                final = terminal.get(normalized(event['path']))
                if status != 'valid' and (not final or final['time'] < event['time']):
                    status = 'pending'
            copies = [cell.strip() for cell in cells[2:]]
            partial_prefix = mark + ' (partially available;'
            if status == 'valid':
                for position, value in enumerate(copies):
                    if value.startswith(partial_prefix):
                        copies[position] = '' if mark in copies else mark
            if status == 'valid' and mark not in copies:
                if '' not in copies:
                    print('REPORT ROW SKIPPED: All copy columns occupied: ' + str(card) + ': ' + name, flush=True)
                else:
                    copies[copies.index('')] = mark
            elif event and status != 'valid':
                copies = ['' if value == mark else value for value in copies]
            if key in known_partial:
                label = (f'{partial_prefix} байтов {partial_copy["downloaded_percent"]:.2f}%)'
                         if partial_copy and partial_copy.get('downloaded_percent') is not None and status != 'valid' else '')
                previous = next((i for i, value in enumerate(copies) if value.startswith(partial_prefix)), None)
                if previous is not None:
                    copies[previous] = label
                elif label:
                    if '' not in copies:
                        print('REPORT ROW SKIPPED: All copy columns occupied: ' + str(card) + ': ' + name, flush=True)
                    else:
                        copies[copies.index('')] = label
            if copies != [cell.strip() for cell in cells[2:]]:

                rows[index] = f'| {cells[0].strip()} | {cells[1].strip()} | ' + ' | '.join(copies) + ' |\n'
            results.append((name, status, event))
        observed = [result for result in results if result[2]]
        if not observed:
            continue
        valid = sum(status == 'valid' for _, status, _ in results)
        failed = [result for result in results if result[1] in ('missing', 'partial', 'invalid')]
        pending = sum(status in ('pending', 'unknown') for _, status, _ in results)
        report = ['## Результат скачивания', '',
                  f'- Отметка владельца копии: **{mark}**',
                  f'- Подтверждено SHA-256: **{valid}/{len(results)}** файлов.',
                  f'- Не скачано после всех попыток: **{len(failed)}**.',
                  f'- Ещё не проверено / ожидает повтора: **{pending}**.',
                  '- Последняя запись проверки: ' + max(event['time'] for _, _, event in observed), '']
        if failed:
            report.extend(['### Неудачи', ''])
            for name, status, event in failed:
                label = dict(missing='нет файла', partial='частичная загрузка', invalid='повреждён')[status]
                saved = partial.get(normalized(event['path'])) or terminal.get(normalized(event['path']), event)
                percent = saved.get('downloaded_percent')
                fraction = f'; сохранено {percent:.2f}% ({saved["downloaded_bytes"]}/{saved["size"]} байт)' if percent is not None else ''
                report.append(f'- `{name}` — {label}; {event["detail"]}; попытка {event["attempt"]}{fraction}.')
            report.append('')
        reports.append((state / 'catalog-reports' / card.name, '\n'.join(report) + '\n'))
        updated = ''.join(rows)
        own_complete = bool(results) and valid == len(results)
        other_complete = bool(results) and all(
            any(cell.strip() and cell.strip() != mark and 'полнота не проверена' not in cell and 'partially available' not in cell.lower() for cell in
                re.split(r'(?<!\\)\|', line.strip())[3:-1])
            for line in updated.splitlines() if line.startswith('| `'))
        status = 'копия есть' if own_complete or other_complete else 'не все видео есть, какие-то не удается скачать'
        updated = re.sub(r'^\*\*Статус:\*\*.*$', '**Статус:** ' + status, updated, flags=re.M)
        updated = BLOCK.sub('', updated).rstrip() + '\n'
        if updated != original:
            changes.append((card, updated))
            if year_match and old_status and old_status[1] != status:
                counts = totals[year_match[1]]
                counts[0 if old_status[1] == 'копия есть' else 1] -= 1
                counts[0 if status == 'копия есть' else 1] += 1
    readme = repo / 'README.md'
    if changes and readme.exists():
        original = readme.read_text(encoding='utf-8')
        updated = original
        for card, body in changes:
            status = re.search(r'^\*\*Статус:\*\* (.+)$', body, re.M)[1]
            pattern = r'(^- .*\(courses/' + re.escape(card.name) + r'\).*?\. )(копия есть|не все видео есть, какие-то не удается скачать)(\.)$'
            updated = re.sub(pattern, lambda match: match[1] + status + match[3], updated, flags=re.M)
        ok = sum(counts[0] for counts in totals.values())
        gap = sum(counts[1] for counts in totals.values())
        updated = re.sub(r'Копия есть: \*\*\d+\*\*\. Не все видео есть: \*\*\d+\*\*\.',
                         f'Копия есть: **{ok}**. Не все видео есть: **{gap}**.', updated)
        for year, counts in totals.items():
            pattern = r'^\| ' + re.escape(year) + r' \|.*$'
            def year_row(match):
                cells = match[0].split('|')
                cells[-3], cells[-2] = f' {counts[0]} ', f' {counts[1]} '
                return '|'.join(cells)
            updated = re.sub(pattern, year_row, updated, flags=re.M)
        def total_row(match):
            cells = match[0].split('|')
            cells[-3], cells[-2] = f' **{ok}** ', f' **{gap}** '
            return '|'.join(cells)
        updated = re.sub(r'^\| \*\*Всего\*\* \|.*$', total_row, updated, flags=re.M)
        if updated != original:
            changes.append((readme, updated))
    paths = [str(path.relative_to(repo)) for path, _ in changes]
    if git_push and dirty.intersection(paths):
        raise RuntimeError('Report would include existing edits: ' + ', '.join(sorted(dirty.intersection(paths))))
    for path, body in reports:
        path.parent.mkdir(exist_ok=True)
        temporary = path.with_name(path.name + '.tmp')
        temporary.write_text(body, encoding='utf-8')
        temporary.replace(path)
    for path, updated in changes:
        temporary = path.with_name(path.name + '.tmp')
        temporary.write_text(updated, encoding='utf-8')
        temporary.replace(path)
    print(f'REPORT: {len(changes)} files updated; mark={mark}', flush=True)
    if git_push:
        git_env = os.environ.copy()
        git_env.setdefault('GIT_SSH_COMMAND', 'ssh -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=5 -o ServerAliveCountMax=2')
        def git(*args):
            subprocess.run(['git', '-C', str(repo), *args], check=True,
                           timeout=git_timeout if args[0] == 'push' else 120, env=git_env)
        if paths:
            git('add', '--', *paths)
            git('commit', '-m', message or f'Update {mark} verified download report')
        # Also retry pushing an earlier local commit when this report is unchanged.
        if stopping is not None and stopping.is_set():
            raise InterruptedError('Report publishing interrupted')
        git('push', 'origin', 'HEAD')
        print('REPORT PUSHED', flush=True)
    return len(changes)
