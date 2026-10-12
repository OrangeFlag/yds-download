#!/usr/bin/env python3
"""Download public Yandex Disk courses, one course at a time."""
import argparse
from datetime import datetime
import fcntl
import hashlib
import json
from local_paths import local_path
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import subprocess
import time
from threading import RLock, Lock
import urllib.error
import urllib.parse
import urllib.request
from settings import OPTIONS, add_arguments, resolve

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
ENV = {k: v for k, v in os.environ.items()
       if k.lower() not in ('http_proxy', 'https_proxy', 'all_proxy')}
STATE_LOCK = RLock()
HASH_LOCK = Lock()


def sha256_file(path, stopping=None):
    while not HASH_LOCK.acquire(timeout=0.25):
        if stopping is not None and stopping.is_set():
            raise InterruptedError('SHA interrupted')
    try:
        before = path.stat()
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            while chunk := stream.read(1024 * 1024):
                if stopping is not None and stopping.is_set():
                    raise InterruptedError('SHA interrupted')
                digest.update(chunk)
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise OSError('File changed during SHA verification')
        return digest.hexdigest()
    finally:
        HASH_LOCK.release()


def api(source, path='/', offset=0):
    url = 'https://cloud-api.yandex.net/v1/disk/public/resources?' + urllib.parse.urlencode(
        dict(public_key=source, path=path, limit=1000, offset=offset))
    for attempt in range(5):
        try:
            with OPENER.open(url, timeout=40) as response:
                return json.load(response)
        except Exception as error:
            if (isinstance(error, urllib.error.HTTPError)
                    and 400 <= error.code < 500 and error.code not in (408, 429)) or attempt == 4:
                raise
            time.sleep(min(2 ** (attempt + 1), 30))


def children(source, path='/'):
    offset = 0
    while True:
        page = api(source, path, offset)['_embedded']
        items = page['items']
        yield from items
        offset += len(items)
        if offset >= page['total']:
            return
        if not items:
            raise RuntimeError('Incomplete directory listing: ' + path)


def catalog(source, root_mode=False):
    root = api(source)
    if root_mode:
        return dict(source=source, title=root['name'], root=True,
                    courses=[dict(name='.', enabled=True, url=source)])
    return dict(source=source, title=root['name'], courses=[
        dict(name=item['name'], enabled=True,
             url=source.rstrip('/') + '/' + urllib.parse.quote(item['name'], safe=''))
        for item in children(source) if item['type'] == 'dir'])


def select_courses(data, include, exclude):
    names = [item['name'] for item in data['courses']]
    if any(not isinstance(name, str) or not name or name.startswith('/')
           or '..' in PurePosixPath(name).parts for name in names):
        raise ValueError('Course names must be nonempty relative paths without ..')
    if len(names) != len(set(names)):
        raise ValueError('Duplicate course names in catalog')
    unknown = (set(include or []) | set(exclude)) - set(names)
    if unknown:
        raise ValueError('Unknown courses: ' + ', '.join(sorted(unknown)))
    for item in data['courses']:
        if not isinstance(item.get('enabled', True), bool):
            raise ValueError('enabled must be true or false: ' + item['name'])
    chosen = set(include) if include is not None else {
        item['name'] for item in data['courses'] if item.get('enabled', True)}
    return [name for name in names if name in chosen and name not in exclude]


def files(source, path):
    if path == '/.':
        root = api(source)
        if root['type'] == 'file':
            return [dict(root, path='/' + root['name'])]
        path = '/'
    result = []
    for item in children(source, path):
        if item['type'] == 'dir':
            result.extend(files(source, item['path']))
        else:
            result.append(item)
    return result


def target(root, item, course):
    path = PurePosixPath(item['path'])
    course_path = PurePosixPath('/' + course)
    if (not path.is_absolute() or '..' in path.parts or '..' in course_path.parts
            or len(path.parts) <= len(course_path.parts)
            or path.parts[:len(course_path.parts)] != course_path.parts):
        raise ValueError('Unexpected source path')
    destination = local_path(root, item['path'])
    if not destination.resolve().is_relative_to(root) or any(ord(c) < 32 for c in str(destination)):
        raise ValueError('Unsafe local path')
    if destination.relative_to(root).parts[0] in ('.download', 'corrupted'):
        raise ValueError('Course name conflicts with a reserved directory')
    if not re.fullmatch('[0-9a-f]{64}', item['sha256']):
        raise ValueError('Missing or invalid SHA-256')
    if urllib.parse.urlsplit(item['file']).scheme != 'https' or any(ord(c) < 32 for c in item['file']):
        raise ValueError('Invalid HTTPS download URL')
    return destination


def write_json(path, data):
    with STATE_LOCK:
        temporary = path.with_name(path.name + '.tmp')
        temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
        temporary.replace(path)


def read_events(path, repair=False):
    if not path.exists():
        return []
    data = path.read_bytes()
    lines = data.splitlines(keepends=True)
    events = []
    offset = 0
    for index, line in enumerate(lines):
        if line.strip():
            try:
                events.append(json.loads(line))
            except (ValueError, UnicodeDecodeError):
                if index != len(lines) - 1 or line.endswith(b'\n'):
                    raise
                if repair:
                    with path.open('r+b') as stream:
                        stream.truncate(offset)
                break
        offset += len(line)
    # A complete last JSON object may still lack the separator for the next append.
    if repair and data and not data.endswith(b'\n') and offset == len(data):
        with path.open('ab') as stream:
            stream.write(b'\n')
    return events


def inspect(root, item, course, path=None, stopping=None):
    path = path if path is not None else target(root, item, course)
    if not path.exists():
        return 'missing', 'No local file'
    if Path(str(path) + '.aria2').exists():
        return 'partial', 'Resume metadata retained'
    if path.stat().st_size != item['size']:
        return 'invalid', 'Size mismatch'
    actual = sha256_file(path, stopping)
    if actual != item['sha256']:
        return 'invalid', 'SHA-256 mismatch: ' + actual
    return 'valid', 'SHA-256 verified'


def record(state, item, course, attempt, status, detail, local=None):
    event = dict(time=datetime.now().astimezone().isoformat(), course=course,
                 path=item['path'], attempt=attempt, status=status, detail=detail,
                 size=item['size'], sha256=item['sha256'])
    if local is not None:
        event['local'] = str(local)
    local = local if local is not None else local_path(state.parent, item['path'])
    from progress import progress
    event.update(progress(local, item['size'], valid=status == 'valid', sha256=item['sha256']))
    if status == 'valid':
        event['mtime_ns'] = local.stat().st_mtime_ns
    with (state / 'events.jsonl').open('a', encoding='utf-8') as log:
        log.write(json.dumps(event, ensure_ascii=False) + '\n')
    print(f"{status.upper()}: {item['path']} (attempt {attempt}) {detail}", flush=True)
    return event


def inspect_copies(root, item, course, cached, source):
    state = root / '.download'
    registry = state / 'local-copies.json'
    copies = json.loads(registry.read_text()) if registry.exists() else []
    matching = [copy for copy in copies if copy['path'] == item['path']
                and copy['source'].rstrip('/') == source.rstrip('/')]
    locations = [local_path(root, item['path'])]
    if cached.get('local'):
        locations.append(Path(cached['local']))
    locations.extend(Path(copy['local']) for copy in matching)
    result = None
    for local in dict.fromkeys(locations):
        status, detail = inspect(root, item, course, local)
        event = record(state, item, course, 0, status, detail, local=local)
        for copy in matching:
            if Path(copy['local']) == local:
                copy.update(event)
        if result is None or (result['status'] != 'valid' and status != 'missing'):
            result = event
    if registry.exists():
        write_json(registry, copies)
    # The final proof must win over a missing alternative in the event log.
    if result is not event:
        with (state / 'events.jsonl').open('a') as log:
            log.write(json.dumps(result, ensure_ascii=False) + '\n')
    return result


def bind_resume(root, entries, previous, events, deep_only):
    state = root / '.download'
    path = state / 'resume-identities.json'
    identities = json.loads(path.read_text()) if path.exists() else {}
    destinations = [local_path(root, item['path']) for item in entries]
    if len(destinations) != len(set(destinations)):
        raise ValueError('Source names collide after local shortening')
    for item in entries:
        local = local_path(root, item['path'])
        control = Path(str(local) + '.aria2')
        old = identities.get(item['path'], previous.get(item['path'], events.get(item['path'], {})))
        changed = old and (old.get('size'), old.get('sha256')) != (item['size'], item['sha256'])
        unbound = control.exists() and not old
        if (changed or unbound) and (local.exists() or control.exists()):
            if deep_only:
                identities[item['path']] = old
                continue
            archive = root / 'corrupted' / (datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '-source-change')
            archive.mkdir(parents=True)
            write_json(archive / 'source-change.json', dict(path=item['path'], previous=old,
                       current={key: item[key] for key in ('size', 'sha256')}))
            retained = local_path(archive, item['path'])
            retained.parent.mkdir(parents=True, exist_ok=True)
            for suffix in ('', '.aria2', '.ranges.json'):
                existing = Path(str(local) + suffix)
                if existing.exists():
                    existing.rename(Path(str(retained) + suffix))
            print('SOURCE CHANGED / UNBOUND RESUME PRESERVED: ' + item['path'], flush=True)
        identities[item['path']] = {key: item[key] for key in ('size', 'sha256')}
    write_json(path, identities)


def list_course(source, course, retries=2, retry_delay=60):
    for attempt in range(retries + 1):
        try:
            return sorted(files(source, '/' + course), key=lambda item: item['size'])
        except (OSError, RuntimeError) as error:
            if (isinstance(error, urllib.error.HTTPError)
                    and 400 <= error.code < 500 and error.code not in (408, 429)) or attempt == retries:
                raise
            print(f'Listing error: {error}; retry in {retry_delay} seconds', flush=True)
            time.sleep(retry_delay)


def download(data, courses, root, retries=2, report_repo=None, mark=None, git_push=False, deep=False, retry_check=False, tuning=None, defer_range=False, range_budget=None, range_files=16):
    if deep:
        return download_deep(data, courses, root, retries, report_repo, mark, git_push, deep, retry_check, tuning)
    from pipeline import run
    return run(data, courses, root, retries, report_repo, mark, git_push, retry_check, tuning, defer_range, range_budget, range_files)


def download_deep(data, courses, root, retries=2, report_repo=None, mark=None, git_push=False, deep=False, retry_check=False, tuning=None):
    tuning = resolve(tuning)
    if not all(shutil.which(name) for name in ('aria2c', 'cp')):
        raise RuntimeError('Install aria2c and coreutils first')
    root.mkdir(parents=True, exist_ok=True)
    state = root / '.download'
    state.mkdir(mode=0o700, exist_ok=True)
    with (state / 'download.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another downloader is using this destination') from None
        source = data['source']
        identity = state / 'source.json'
        if identity.exists() and json.loads(identity.read_text(encoding='utf-8'))['source'] != source:
            raise ValueError('Use a separate destination for a different public folder/year')
        write_json(identity, dict(source=source, title=data['title']))
        write_json(state / 'settings.json', dict(retries=retries, deep=deep, retry_check=retry_check, tuning=tuning))
        summary_path = state / 'summary.json'
        handoff = json.loads(summary_path.read_text()) if deep and not retry_check and summary_path.exists() else {}
        handoff = {key: handoff[key] for key in ('downloads_complete', 'pending_courses') if key in handoff}
        read_events(state / 'events.jsonl', repair=True)
        recovered = {}
        if retry_check:
            from recovery import recover_corrupted
            recovered = {event['path']: event for event in recover_corrupted(
                root, source, retries, True, report_repo, mark, git_push, courses=courses, force=True, tuning=tuning, deep=deep)}
        history_path = state / 'course-history.json'
        history = json.loads(history_path.read_text()) if history_path.exists() else {}
        for path in state.glob('*-status.json'):
            previous = json.loads(path.read_text())
            if previous.get('source') == source and previous.get('state') in ('finished', 'running'):
                name = previous['course']
                if previous['state'] == 'running' and not previous.get('time'):
                    previous['time'] = datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat()
                if previous.get('time', '') >= history.get(name, {}).get('time', ''):
                    history[name] = previous
        write_json(history_path, history)
        recovery_courses = [] if retry_check or deep else [
            course for course in courses if history.get(course, {}).get('state') == 'running']
        if recovery_courses:
            from recovery import recover_corrupted
            recovered = {event['path']: event for event in recover_corrupted(
                root, source, retries, True, courses=recovery_courses, tuning=tuning)}
        events = {event['path']: event for event in read_events(state / 'events.jsonl')}
        registry = state / 'local-copies.json'
        local_copies = json.loads(registry.read_text()) if registry.exists() else []
        for event in local_copies:
            if event['source'].rstrip('/') == source.rstrip('/') and (deep or event['status'] == 'valid'):
                local = Path(event['local'])
                if deep or (local.exists() and local.stat().st_size == event['size']
                        and local.stat().st_mtime_ns == event['mtime_ns']
                        and not Path(str(local) + '.aria2').exists()):
                    events[event['path']] = event
        write_json(state / 'catalog.json', data)
        write_json(state / 'courses.json', courses)
        lines = [data['title'] + ' — порядок скачивания', 'Источник: ' + source,
                 f'Курсы последовательно; первая попытка + {retries} повтора; неудачи: corrupted.', '']
        for index, course in enumerate(courses, 1):
            lines.extend([f'{index:02d}. {course}', source.rstrip('/') + '/' +
                          urllib.parse.quote(course, safe=''), ''])
        (root / 'Порядок скачивания.txt').write_text('\n'.join(lines), encoding='utf-8')
        broken_root = root / 'corrupted' / datetime.now().strftime('%Y%m%d-%H%M%S-%f')
        prior_failures = {}
        for path in (root / 'corrupted').glob('*/failed.json'):
            for event in json.loads(path.read_text()):
                if event['course'] in courses and event['time'] > prior_failures.get(event['path'], {}).get('time', ''):
                    prior_failures[event['path']] = event
        failed = [event for path, event in prior_failures.items()
                  if events.get(path, {}).get('status') != 'valid']
        verified = 0
        for index, course in enumerate(courses, 1):
            print(f'COURSE {index}/{len(courses)}: {course}', flush=True)
            status_path = state / f'{index:02d}-status.json'
            previous = history.get(course, {})
            if (not deep and previous.get('state') == 'finished' and previous.get('source') == source
                    and not (retry_check and previous.get('failed'))):
                write_json(status_path, previous)
                verified += previous['verified']
                print(f'COURSE SKIPPED: {course}; already processed; verified={previous["verified"]}; failed={previous["failed"]}', flush=True)
                continue
            failed = [event for event in failed if event['course'] != course]
            running = dict(previous, course=course, source=source, complete=False, state='running',
                           time=datetime.now().astimezone().isoformat())
            if not (deep and not retry_check):
                history[course] = running
                write_json(history_path, history)
                write_json(status_path, running)
            entries = list_course(source, course, retries, tuning['listing_retry_delay'])
            for item in entries:
                target(root, item, course)
            previous_entries = {}
            for manifest in sorted(state.glob('*-manifest.json'), key=lambda path: path.stat().st_mtime_ns):
                previous_entries.update({row['path']: row for row in json.loads(manifest.read_text())})
            bind_resume(root, entries, previous_entries, events, deep and not retry_check)
            write_json(state / f'{index:02d}-manifest.json', [
                {k: item[k] for k in ('path', 'size', 'sha256')} for item in entries])
            print(f"FILES {len(entries)}; SIZE {sum(x['size'] for x in entries)/1024**3:.2f} GiB", flush=True)
            pending = []
            course_failed = []
            for item in entries:
                archived = recovered.get(item['path'])
                if (archived and not archived.get('range_recovered_complete')
                        and (archived['size'], archived['sha256']) == (item['size'], item['sha256'])):
                    course_failed.append(archived)
                    failed.append(archived)
                    continue
                local = target(root, item, course)
                cached = events.get(item['path'], {})
                if cached.get('local') and (deep or cached.get('status') == 'valid'):
                    local = Path(cached['local'])
                if deep and not retry_check:
                    event = inspect_copies(root, item, course, cached, source)
                    if event['status'] == 'valid':
                        verified += 1
                    else:
                        course_failed.append(event)
                        failed.append(event)
                    continue
                if ((not deep or archived) and cached.get('status') == 'valid'
                        and cached.get('sha256') == item['sha256'] and cached.get('size') == item['size']
                        and local.exists() and local.stat().st_size == item['size']
                        and ('mtime_ns' not in cached or local.stat().st_mtime_ns == cached['mtime_ns'])
                        and not Path(str(local) + '.aria2').exists()):
                    verified += 1
                    print('FILE SKIPPED: ' + item['path'] + '; previously SHA-256 verified', flush=True)
                else:
                    if deep:
                        event = inspect_copies(root, item, course, cached, source)
                        if event['status'] == 'valid':
                            verified += 1
                            continue
                    pending.append(item)
            needed = sum(max(0, item['size'] - (target(root, item, course).stat().st_size
                         if target(root, item, course).exists() else 0)) for item in pending)
            if pending and shutil.disk_usage(root).free < needed + tuning['free_space_mib'] * 1024 ** 2:
                raise RuntimeError('Insufficient free space for the course')
            for attempt in range(1, retries + 2):
                if not pending:
                    break
                if attempt > 1:
                    time.sleep(tuning['retry_delay'])
                    fresh = {item['path']: item for item in list_course(source, course, retries, tuning['listing_retry_delay'])}
                    pending = [dict(item, file=fresh.get(item['path'], item)['file']) for item in pending]
                queue = []
                for item in pending:
                    destination = target(root, item, course)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    queue.append(f"{item['file']}\n dir={destination.parent}\n out={destination.name}\n checksum=sha-256={item['sha256']}\n")
                (state / 'queue.txt').write_text(''.join(queue), encoding='utf-8')
                code = subprocess.call([
                    'aria2c', '--input-file=' + str(state / 'queue.txt'),
                    '--continue=true', '--always-resume=true', '--check-integrity=true',
                    '--auto-file-renaming=false', '--allow-overwrite=true',
                    f"--max-concurrent-downloads={tuning['download_concurrency']}",
                    '--optimize-concurrent-downloads=' + (
                        f"{tuning['auto_concurrency_a']:g}:{tuning['auto_concurrency_b']:g}"
                        if tuning['auto_concurrency'] else 'false'),
                    f"--max-connection-per-server={tuning['connections']}", f"--split={tuning['split']}",
                    f"--min-split-size={tuning['min_split_size_mib']}M", '--file-allocation=none',
                    f"--disk-cache={tuning['disk_cache_mib']}M",
                    f"--max-overall-download-limit={tuning['bandwidth_limit']}",
                    f"--auto-save-interval={tuning['auto_save_interval']}", '--max-tries=1',
                    f"--connect-timeout={tuning['connect_timeout']}", f"--timeout={tuning['timeout']}",
                    f"--summary-interval={tuning['summary_interval']}", '--enable-color=false',
                    '--console-log-level=warn', '--log=' + str(state / 'aria2.log'), '--log-level=notice',
                ], env=ENV)
                if code == 9:
                    raise RuntimeError('aria2 reports insufficient disk space')
                remaining = []
                for item in pending:
                    status, detail = inspect(root, item, course)
                    event = record(state, item, course, attempt, status, detail)
                    if status == 'valid':
                        verified += 1
                    elif attempt < retries + 1:
                        remaining.append(item)
                    else:
                        destination = target(root, item, course)
                        broken = broken_root / destination.relative_to(root)
                        broken.parent.mkdir(parents=True, exist_ok=True)
                        event['corrupted_path'] = str(broken.relative_to(root))
                        course_failed.append(event)
                        failed.append(event)
                        write_json(root / 'corrupted' / 'failed.json', failed)
                        write_json(broken_root / 'failed.json', failed)
                        for src, out in [(destination, broken),
                                         (Path(str(destination) + '.aria2'), Path(str(broken) + '.aria2'))]:
                            if src.exists():
                                if out.exists():
                                    raise FileExistsError(out)
                                src.rename(out)
                        try:
                            from recovery import finish_recovery
                            finish_recovery(root, event, item, retries, tuning=tuning)
                        except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
                            event['recovery_error'] = str(error)
                            print('RECOVERY ERROR: ' + item['path'] + '; ' + type(error).__name__, flush=True)
                        if event.get('range_recovered_complete'):
                            verified += 1
                            course_failed.remove(event)
                            failed.remove(event)
                            write_json(root / 'corrupted' / 'failed.json', failed)
                            write_json(broken_root / 'failed.json', failed)
                            continue
                        with (state / 'events.jsonl').open('a') as log:
                            log.write(json.dumps(event, ensure_ascii=False) + '\n')
                        write_json(root / 'corrupted' / 'failed.json', failed)
                        write_json(broken_root / 'failed.json', failed)
                pending = remaining
            complete = not course_failed
            course_status = dict(course=course, source=source, complete=complete,
                                 state=previous.get('state', 'pending') if deep and not retry_check else 'finished',
                                 total=len(entries), verified=len(entries) - len(course_failed),
                                 failed=len(course_failed), time=datetime.now().astimezone().isoformat())
            write_json(status_path, course_status)
            history[course] = course_status
            write_json(history_path, history)
            if report_repo:
                try:
                    from report import update_report
                    update_report(root, report_repo, mark, git_push,
                                  f"Update {mark} copies: {data['title']} / {course}")
                except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
                    print(f'REPORT ERROR: {error}', flush=True)
            print(('COURSE VERIFIED COMPLETE: ' if complete else 'COURSE FINISHED WITH ERRORS: ') + course, flush=True)
            write_json(state / 'summary.json', dict(handoff, verified=verified, failed=len(failed),
                       last_finished_course=course, complete=False))
        (root / 'corrupted').mkdir(exist_ok=True)
        write_json(root / 'corrupted' / 'failed.json', failed)
        failed_count = sum(history[course]['failed'] for course in courses)
        write_json(state / 'summary.json', dict(handoff, verified=verified, failed=failed_count,
                   complete=not failed_count and not handoff.get('pending_courses')))
        print(f'ALL SELECTED COURSES PROCESSED: verified={verified}, failed={failed_count}', flush=True)
        return 1 if failed_count else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    listing = commands.add_parser('catalog', help='Generate an editable course catalog')
    listing.add_argument('--source', required=True)
    listing.add_argument('--output', required=True, type=Path)
    listing.add_argument('--root', action='store_true', help='Select the whole published folder or file')
    fetching = commands.add_parser('download', help='Download selected courses sequentially')
    source = fetching.add_mutually_exclusive_group(required=True)
    source.add_argument('--source')
    source.add_argument('--catalog', type=Path)
    fetching.add_argument('--destination', required=True, type=Path)
    fetching.add_argument('--root', action='store_true', help='Download the whole published folder or file')
    fetching.add_argument('--include', action='append', help='Exact course name; repeatable')
    fetching.add_argument('--exclude', action='append', default=[], help='Exact course name; repeatable')
    fetching.add_argument('--retry-check', action='store_true', help='Retry independent missing ranges of previous failures before ordinary courses')
    fetching.add_argument('--defer-range', action='store_true', help='Leave durable Range tasks for range_queue.py')
    fetching.add_argument('--deep', action='store_true', help='Verify local SHA-256 without downloading; combine with --retry-check to repair')
    fetching.add_argument('--retries', type=int, default=2, help='Retries after the first attempt (default: 2)')
    fetching.add_argument('--report-repo', type=Path, help='Update course cards after each course')
    fetching.add_argument('--mark', help='Custom copy owner mark for reports')
    fetching.add_argument('--range-budget', type=Path, help='Shared budget directory; run Range during downloads and hand unfinished work off at archive end')
    fetching.add_argument('--range-files', type=int, default=16, help='Range files during overlapping downloads (default: 16)')
    fetching.add_argument('--git-push', action='store_true', help='Commit and push reports after each course')
    add_arguments(fetching)
    salvage = commands.add_parser('recover', help='Retain videos with over 50 percent confirmed bytes next to originals')
    salvage.add_argument('--destination', required=True, type=Path)
    damaged = commands.add_parser('corrupted', help='Save corrupted download progress and list retained files over 30 percent')
    damaged.add_argument('--destination', required=True, type=Path)
    reporting = commands.add_parser('report', help='Update backup course cards from download results')
    reporting.add_argument('--destination', required=True, type=Path)
    reporting.add_argument('--repo', required=True, type=Path)
    reporting.add_argument('--mark', required=True)
    reporting.add_argument('--git-push', action='store_true', help='Commit and push the report')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.command == 'catalog':
            data = catalog(args.source, args.root)
            write_json(args.output, data)
            print(f"{data['title']}: {len(data['courses'])} courses → {args.output}")
            return 0
        if args.command == 'recover':
            from recovery import recover_corrupted
            root = args.destination.resolve()
            lock = (root / '.download/download.lock').open('a')
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            source = json.loads((root / '.download/source.json').read_text())['source']
            recover_corrupted(root, source)
            return 0
        if args.command == 'corrupted':
            from progress import corrupted_report
            for event in corrupted_report(args.destination):
                print(f"{event['downloaded_percent']:.2f}% — {event['path']} — {event['corrupted_path']}")
            return 0
        if args.command == 'report':
            from report import update_report
            update_report(args.destination.resolve(), args.repo.resolve(), args.mark, args.git_push)
            return 0
        if args.git_push and not args.report_repo:
            raise ValueError('--git-push requires --report-repo')
        if args.report_repo and not args.mark:
            raise ValueError('--report-repo requires --mark')
        if args.retries < 0:
            raise ValueError('--retries must be non-negative')
        tuning = resolve({name: getattr(args, name) for name in OPTIONS})
        data = json.loads(args.catalog.read_text(encoding='utf-8')) if args.catalog else catalog(args.source, args.root)
        if args.root and args.catalog and not data.get('root'):
            raise ValueError('--root requires a root catalog')
        if data.get('root') and (args.report_repo or [row['name'] for row in data['courses']] != ['.']):
            raise ValueError('Root catalogs must contain only . and cannot publish course reports')
        chosen = select_courses(data, args.include, args.exclude)
        if not chosen:
            raise ValueError('No courses selected')
        if args.catalog and not data.get('root'):
            available = {item['name'] for item in catalog(data['source'])['courses']}
            if {PurePosixPath(name).parts[0] for name in chosen} - available:
                raise ValueError('Selected courses no longer exist in the source folder')
        return download(data, chosen, args.destination.resolve(), args.retries, args.report_repo, args.mark, args.git_push, args.deep, args.retry_check, tuning, args.defer_range, args.range_budget, args.range_files)
    except (ValueError, KeyError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(2, f'Error: {error}\n')


def cli(entrypoint):
    def interrupt(signum, frame):
        # systemd and the queue parent may both signal the same child.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, interrupt)
    try:
        return entrypoint()
    except KeyboardInterrupt:
        print('STOPPED: interrupted; resume state retained', flush=True)
        return 130
    finally:
        signal.signal(signal.SIGINT, previous)


if __name__ == '__main__':
    raise SystemExit(cli(main))
