#!/usr/bin/env python3
"""Download public Yandex Disk courses, one course at a time."""
import argparse
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import time
import urllib.parse
import urllib.request

OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
ENV = {k: v for k, v in os.environ.items()
       if k.lower() not in ('http_proxy', 'https_proxy', 'all_proxy')}


def api(source, path='/', offset=0):
    url = 'https://cloud-api.yandex.net/v1/disk/public/resources?' + urllib.parse.urlencode(
        dict(public_key=source, path=path, limit=1000, offset=offset))
    for attempt in range(5):
        try:
            with OPENER.open(url, timeout=40) as response:
                return json.load(response)
        except Exception:
            if attempt == 4:
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


def catalog(source):
    root = api(source)
    return dict(source=source, title=root['name'], courses=[
        dict(name=item['name'], enabled=True,
             url=source.rstrip('/') + '/' + urllib.parse.quote(item['name'], safe=''))
        for item in children(source) if item['type'] == 'dir'])


def select_courses(data, include, exclude):
    names = [item['name'] for item in data['courses']]
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
    result = []
    for item in children(source, path):
        if item['type'] == 'dir':
            result.extend(files(source, item['path']))
        else:
            result.append(item)
    return result


def target(root, item, course):
    path = PurePosixPath(item['path'])
    if not path.is_absolute() or len(path.parts) < 3 or '..' in path.parts or path.parts[1] != course:
        raise ValueError('Unexpected source path')
    destination = root.joinpath(*path.parts[1:])
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
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(path)


def inspect(root, item, course):
    path = target(root, item, course)
    if not path.exists():
        return 'missing', 'No local file'
    if Path(str(path) + '.aria2').exists():
        return 'partial', 'Resume metadata retained'
    if path.stat().st_size != item['size']:
        return 'invalid', 'Size mismatch'
    with path.open('rb') as stream:
        actual = hashlib.file_digest(stream, 'sha256').hexdigest()
    if actual != item['sha256']:
        return 'invalid', 'SHA-256 mismatch: ' + actual
    return 'valid', 'SHA-256 verified'


def record(state, item, course, attempt, status, detail):
    event = dict(time=datetime.now().astimezone().isoformat(), course=course,
                 path=item['path'], attempt=attempt, status=status, detail=detail,
                 size=item['size'], sha256=item['sha256'])
    with (state / 'events.jsonl').open('a', encoding='utf-8') as log:
        log.write(json.dumps(event, ensure_ascii=False) + '\n')
    print(f"{status.upper()}: {item['path']} (attempt {attempt}) {detail}", flush=True)
    return event


def list_course(source, course):
    while True:
        try:
            return sorted(files(source, '/' + course), key=lambda item: item['size'])
        except (OSError, RuntimeError) as error:
            print(f'Listing error: {error}; retry in 60 seconds', flush=True)
            time.sleep(60)


def download(data, courses, root, retries=2):
    if not shutil.which('aria2c'):
        raise RuntimeError('Install aria2c first')
    root.mkdir(parents=True, exist_ok=True)
    state = root / '.download'
    state.mkdir(mode=0o700, exist_ok=True)
    lock = (state / 'download.lock').open('a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError('Another downloader is using this destination') from None
    source = data['source']
    identity = state / 'source.json'
    if identity.exists() and json.loads(identity.read_text(encoding='utf-8'))['source'] != source:
        raise ValueError('Use a separate destination for a different public folder/year')
    write_json(identity, dict(source=source, title=data['title']))
    write_json(state / 'catalog.json', data)
    write_json(state / 'courses.json', courses)
    lines = [data['title'] + ' — порядок скачивания', 'Источник: ' + source,
             f'Курсы последовательно; первая попытка + {retries} повтора; неудачи: corrupted.', '']
    for index, course in enumerate(courses, 1):
        lines.extend([f'{index:02d}. {course}', source.rstrip('/') + '/' +
                      urllib.parse.quote(course, safe=''), ''])
    (root / 'Порядок скачивания.txt').write_text('\n'.join(lines), encoding='utf-8')
    broken_root = root / 'corrupted' / datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    failed = []
    verified = 0
    for index, course in enumerate(courses, 1):
        print(f'COURSE {index}/{len(courses)}: {course}', flush=True)
        status_path = state / f'{index:02d}-status.json'
        write_json(status_path, dict(course=course, source=source, complete=False, state='running'))
        entries = list_course(source, course)
        for item in entries:
            target(root, item, course)
        write_json(state / f'{index:02d}-manifest.json', [
            {k: item[k] for k in ('path', 'size', 'sha256')} for item in entries])
        needed = sum(max(0, item['size'] - (target(root, item, course).stat().st_size
                     if target(root, item, course).exists() else 0)) for item in entries)
        if shutil.disk_usage(root).free < needed + 1024 ** 3:
            raise RuntimeError('Insufficient free space for the course')
        print(f"FILES {len(entries)}; SIZE {sum(x['size'] for x in entries)/1024**3:.2f} GiB", flush=True)
        pending = entries
        course_failed = []
        for attempt in range(1, retries + 2):
            if not pending:
                break
            if attempt > 1:
                time.sleep(10)
                fresh = {item['path']: item for item in list_course(source, course)}
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
                '--max-concurrent-downloads=8', '--max-connection-per-server=4', '--split=4',
                '--min-split-size=4M', '--file-allocation=none', '--disk-cache=64M',
                '--auto-save-interval=5', '--max-tries=1', '--connect-timeout=20',
                '--timeout=60', '--summary-interval=30', '--enable-color=false',
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
                    for src, out in [(destination, broken),
                                     (Path(str(destination) + '.aria2'), Path(str(broken) + '.aria2'))]:
                        if src.exists():
                            if out.exists():
                                raise FileExistsError(out)
                            src.rename(out)
                    event['corrupted_path'] = str(broken.relative_to(root))
                    course_failed.append(event)
                    failed.append(event)
                    write_json(root / 'corrupted' / 'failed.json', failed)
                    write_json(broken_root / 'failed.json', failed)
            pending = remaining
        complete = not course_failed
        course_status = dict(course=course, source=source, complete=complete, state='finished',
                             total=len(entries), verified=len(entries) - len(course_failed),
                             failed=len(course_failed), time=datetime.now().astimezone().isoformat())
        write_json(status_path, course_status)
        print(('COURSE VERIFIED COMPLETE: ' if complete else 'COURSE FINISHED WITH ERRORS: ') + course, flush=True)
        write_json(state / 'summary.json', dict(verified=verified, failed=len(failed),
                   last_finished_course=course, complete=False))
    (root / 'corrupted').mkdir(exist_ok=True)
    write_json(root / 'corrupted' / 'failed.json', failed)
    write_json(state / 'summary.json', dict(verified=verified, failed=len(failed), complete=not failed))
    print(f'ALL SELECTED COURSES PROCESSED: verified={verified}, failed={len(failed)}', flush=True)
    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    listing = commands.add_parser('catalog', help='Generate an editable course catalog')
    listing.add_argument('--source', required=True)
    listing.add_argument('--output', required=True, type=Path)
    fetching = commands.add_parser('download', help='Download selected courses sequentially')
    source = fetching.add_mutually_exclusive_group(required=True)
    source.add_argument('--source')
    source.add_argument('--catalog', type=Path)
    fetching.add_argument('--destination', required=True, type=Path)
    fetching.add_argument('--include', action='append', help='Exact course name; repeatable')
    fetching.add_argument('--exclude', action='append', default=[], help='Exact course name; repeatable')
    fetching.add_argument('--retries', type=int, default=2, help='Retries after the first attempt (default: 2)')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.command == 'catalog':
            data = catalog(args.source)
            write_json(args.output, data)
            print(f"{data['title']}: {len(data['courses'])} courses → {args.output}")
            return 0
        if args.retries < 0:
            raise ValueError('--retries must be non-negative')
        data = json.loads(args.catalog.read_text(encoding='utf-8')) if args.catalog else catalog(args.source)
        chosen = select_courses(data, args.include, args.exclude)
        if not chosen:
            raise ValueError('No courses selected')
        if args.catalog:
            available = {item['name'] for item in catalog(data['source'])['courses']}
            if set(chosen) - available:
                raise ValueError('Selected courses no longer exist in the source folder')
        return download(data, chosen, args.destination.resolve(), args.retries)
    except (ValueError, KeyError, OSError, RuntimeError) as error:
        parser.exit(2, f'Error: {error}\n')


if __name__ == '__main__':
    raise SystemExit(main())
