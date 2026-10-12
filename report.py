"""Update existing backup course cards from verified downloader events."""
import fcntl
import json
from local_paths import local_path
import os
import subprocess
from pathlib import Path
import re
import unicodedata

BEGIN = '<!-- yds-download:begin -->'
END = '<!-- yds-download:end -->'
BLOCK = re.compile(re.escape(BEGIN) + r'.*?' + re.escape(END) + r'\n?', re.S)


def normalized(value):
    return unicodedata.normalize('NFC', value)


def update_report(destination, repo, mark, git_push=False, message=None):
    repo = repo.resolve()
    if (repo / ".git").exists():
        common = Path(subprocess.check_output(["git", "-C", str(repo), "rev-parse", "--path-format=absolute", "--git-common-dir"], text=True).strip())
        with (common / "yds-report.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            return _update_report(destination, repo, mark, git_push, message)
    return _update_report(destination, repo, mark, git_push, message)


def _update_report(destination, repo, mark, git_push=False, message=None):
    from download import read_events
    if not mark or any(c in mark for c in '|\n\r'):
        raise ValueError('Mark must be nonempty and contain no pipes or line breaks')
    repo = repo.resolve()
    dirty = set()
    if git_push:
        staged = subprocess.run(['git', '-C', str(repo), 'diff', '--cached', '--quiet'])
        if staged.returncode != 0:
            raise RuntimeError('Report publish requires an empty Git index')
        subprocess.run(['git', '-C', str(repo), 'symbolic-ref', '--quiet', 'HEAD'], check=True, stdout=subprocess.DEVNULL)
        dirty = set(subprocess.check_output(['git', '-C', str(repo), 'diff', '--name-only', '-z']).decode().split('\0'))
    state = destination / '.download'
    source = json.loads((state / 'source.json').read_text(encoding='utf-8'))['source'].rstrip('/')
    events = {}
    for event in read_events(state / 'events.jsonl'):
        events[normalized(event['path'])] = event
    expected = dict(events)
    for manifest in sorted(state.glob('*-manifest.json'), key=lambda path: path.stat().st_mtime_ns):
        expected.update({normalized(row['path']): row for row in json.loads(manifest.read_text())})
    def same_identity(row):
        identity = expected.get(normalized(row['path']))
        return identity is None or (identity['size'], identity['sha256']) == (row['size'], row['sha256'])
    external = {}
    registry = state / 'local-copies.json'
    if registry.exists():
        for copy in json.loads(registry.read_text(encoding='utf-8')):
            if copy['source'].rstrip('/') != source or copy['status'] != 'valid' or not same_identity(copy):
                continue
            local = Path(copy['local'])
            if local.exists():
                stat = local.stat()
                if stat.st_size == copy['size'] and stat.st_mtime_ns == copy['mtime_ns'] and not Path(str(local) + '.aria2').exists():
                    external[normalized(copy['path'])] = copy
    partial = {}
    known_partial = set()
    registry = state / 'partial-copies.json'
    if registry.exists():
        for copy in json.loads(registry.read_text()):
            if copy['source'].rstrip('/') != source:
                continue
            key = normalized(copy['path'])
            known_partial.add(key)
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
        if not any(key.startswith(normalized(prefix)) for key in events.keys() | external.keys() | known_partial):
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
            event = local_copy or events.get(key) or partial_copy
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
        git_env.setdefault('GIT_SSH_COMMAND', 'ssh -o BatchMode=yes -o ConnectTimeout=15')
        def git(*args):
            subprocess.run(['git', '-C', str(repo), *args], check=True, timeout=120, env=git_env)
        if paths:
            git('add', '--', *paths)
            git('commit', '-m', message or f'Update {mark} verified download report')
        # Also retry pushing an earlier local commit when this report is unchanged.
        git('push', 'origin', 'HEAD')
        print('REPORT PUSHED', flush=True)
    return len(changes)
