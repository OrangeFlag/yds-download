"""Retry independent ranges and retain partial videos by confirmed byte coverage."""
from datetime import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from threading import Event, Lock
import urllib.request
from progress import merge_ranges, saved_ranges, progress

SUFFIX = ' (partially available)'


def body_path(root, event):
    path = root / event['corrupted_path']
    if not path.resolve().is_relative_to(root / 'corrupted'):
        raise ValueError('Unsafe corrupted path')
    return path


def copy_saved(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Preserve sparse holes and the archive used for future retries.
    with tempfile.NamedTemporaryFile(dir=destination.parent, prefix='.partial-', delete=False) as temporary:
        name = Path(temporary.name)
    try:
        subprocess.run(['cp', '--reflink=auto', '--sparse=always', '--', str(source), str(name)], check=True)
        name.replace(destination)
    finally:
        name.unlink(missing_ok=True)


def promote_partial(root, event):
    from download import write_json
    if event.get('sha256_mismatch'):
        return False
    path = body_path(root, event)
    if not path.exists() or path.stat().st_size == 0:
        return False
    event.update(progress(path, event['size'], sha256=event['sha256']))
    for key in ('duration_seconds', 'available_seconds', 'available_time_percent', 'available_intervals', 'time_method', 'timeline_error'):
        event.pop(key, None)
    if (event.get('downloaded_bytes') or 0) * 2 <= event['size']:
        return False
    original_path = Path(event['path'])
    if (not original_path.is_absolute() or len(original_path.parts) < 3
            or '..' in original_path.parts or original_path.parts[1] in ('.download', 'corrupted')):
        raise ValueError('Unsafe recovered destination')
    original = root.joinpath(*original_path.parts[1:])
    if not original.resolve().is_relative_to(root) or not original.parent.resolve().is_relative_to(root):
        raise ValueError('Unsafe recovered destination')
    destination = original.with_name(original.stem + SUFFIX + original.suffix)
    registry = root / '.download' / 'partial-copies.json'
    copies = json.loads(registry.read_text()) if registry.exists() else []
    source = json.loads((root / '.download/source.json').read_text())['source']
    old = next((row for row in copies if row['source'] == source and row['path'] == event['path']), None)
    if old and destination.exists():
        stat = destination.stat()
        if (stat.st_size, stat.st_mtime_ns) != (old['local_size'], old['mtime_ns']):
            raise FileExistsError('Partial copy changed locally: ' + str(destination))
        if not old.get('sha256_mismatch') and (old.get('downloaded_bytes') or 0) >= (event.get('downloaded_bytes') or 0):
            return True
    if destination.exists() and not old:
        raise FileExistsError('Unregistered partial copy: ' + str(destination))
    copy_saved(path, destination)
    stat = destination.stat()
    copy = dict(event, source=source, recovered_path=str(destination.relative_to(root)),
                local_size=stat.st_size, mtime_ns=stat.st_mtime_ns,
                recovery_time=datetime.now().astimezone().isoformat())
    copies = [row for row in copies if not (row['source'] == source and row['path'] == event['path'])]
    copies.append(copy)
    write_json(registry, copies)
    print(f'PARTIALLY AVAILABLE: {event["path"]}; bytes={event["downloaded_percent"]:.2f}%', flush=True)
    return True


def new_recovery_body(root, event, reason):
    from download import write_json
    archive = root / 'corrupted' / (datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '-recovery')
    archive.mkdir(parents=True)
    event.setdefault('preserved_archive_paths', []).append(event['corrupted_path'])
    event['corrupted_path'] = str(archive.joinpath(*Path(event['path']).parts[1:]).relative_to(root))
    for key in ('sha256_mismatch', 'recovery_error', 'available_seconds', 'available_time_percent', 'available_intervals', 'time_method', 'range_attempts'):
        event.pop(key, None)
    event['restart_reason'] = reason
    event.update(progress(body_path(root, event), event['size'], sha256=event['sha256']))
    write_json(archive / 'failed.json', [event])


def retry_ranges(root, event, item, retries):
    from download import OPENER, write_json, files
    if item['size'] != event['size'] or item['sha256'] != event['sha256']:
        raise ValueError('Source file changed; refuse to combine ranges')
    path = body_path(root, event)
    path.parent.mkdir(parents=True, exist_ok=True)
    ranges = saved_ranges(path, event['size'], event['sha256'])
    if path.exists() and path.stat().st_size and not ranges:
        new_recovery_body(root, event, 'No trusted ranges in existing body; retain it separately')
        path = body_path(root, event)
        path.parent.mkdir(parents=True, exist_ok=True)
    journal = Path(str(path) + '.ranges.json')
    cursor = json.loads(journal.read_text()).get('salvage_cursor', 0) if journal.exists() else 0
    additional = [list(bounds) for bounds in ranges]
    # Drop proofs past the retained body before a new write can extend it again.
    write_json(journal, dict(size=event['size'], sha256=event['sha256'], ranges=additional, salvage_cursor=cursor))
    total = event['size']
    missing_bytes = total - sum(end - start for start, end in ranges)
    if shutil.disk_usage(root).free < missing_bytes + 1024 ** 3:
        raise RuntimeError('Insufficient disk space for range recovery')
    rate_lock = Lock()
    stopping = Event()
    next_request = 0.0
    def fetch(bounds):
        nonlocal next_request
        start, end = bounds
        with rate_lock:
            time.sleep(max(0, next_request - time.monotonic()))
            next_request = time.monotonic() + 1 / 6
        if stopping.is_set():
            return bounds, None, 'Interrupted'
        request = urllib.request.Request(item['file'], headers={'Range': f'bytes={start}-{end - 1}', 'Accept-Encoding': 'identity'})
        try:
            with OPENER.open(request, timeout=30) as response:
                served = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', response.headers.get('Content-Range', ''))
                if (response.status != 206 or not served or int(served[1]) != start
                        or not start <= int(served[2]) < end or int(served[3]) != total
                        or response.headers.get('Content-Encoding', 'identity') != 'identity'):
                    return bounds, None, 'Range not honoured'
                length = int(served[2]) - start + 1
                data = bytearray()
                error = None
                try:
                    while len(data) < length and not stopping.is_set():
                        chunk = response.read1(min(64 * 1024, length - len(data)))
                        if not chunk:
                            break
                        data.extend(chunk)
                except http.client.IncompleteRead as failure:
                    data.extend(failure.partial)
                    error = 'Incomplete response'
                except (OSError, http.client.HTTPException) as failure:
                    error = type(failure).__name__
                if len(data) > length:
                    return bounds, None, 'Oversized response'
                return bounds, data or None, error or (None if len(data) == length else 'Incomplete response')
        except (OSError, http.client.HTTPException) as error:
            return bounds, None, 'HTTP ' + str(error.code) if hasattr(error, 'code') else type(error).__name__
    attempts = []
    for attempt in range(1, retries + 3):
        # Backend block geometry is unpublished; refine failed intervals by observed responses.
        block = max(64 * 1024, 16 * 1024 ** 2 // (4 ** min(attempt - 1, 4)))
        fine = attempt == retries + 2
        if fine:
            block = 64 * 1024
        missing = []
        position = 0
        for start, end in ranges + [[total, total]]:
            while position < start:
                stop = min(start, (position // block + 1) * block)
                missing.append((position, stop))
                position = stop
            position = end
        if not missing:
            break
        unprobed = 0
        if fine:
            # Bound the cost of an unavailable multi-GiB file; later runs continue this scan.
            missing = [bounds for bounds in missing if bounds[0] >= cursor] + [bounds for bounds in missing if bounds[0] < cursor]
            unprobed = max(0, len(missing) - 256)
            missing = missing[:256]
        if attempt > 1:
            time.sleep(10)
        source = json.loads((root / '.download/source.json').read_text())['source']
        fresh = next((row for row in files(source, '/' + event['course']) if row['path'] == event['path']), None)
        if not fresh or (fresh['size'], fresh['sha256']) != (event['size'], event['sha256']):
            raise ValueError('Source file changed or disappeared; refuse to combine ranges')
        item = fresh
        outcomes = []
        def save_result(future):
            nonlocal additional, ranges
            bounds, data, error = future.result()
            if data is not None:
                saved = [bounds[0], bounds[0] + len(data)]
                with path.open('r+b' if path.exists() else 'w+b') as stream:
                    stream.seek(saved[0]); stream.write(data); stream.flush(); os.fsync(stream.fileno())
                additional = merge_ranges(additional + [saved])
                write_json(journal, dict(size=total, sha256=event['sha256'], ranges=additional, salvage_cursor=cursor))
                ranges = merge_ranges(ranges + [saved])
            outcomes.append(dict(start=bounds[0], end=bounds[1], saved_bytes=len(data) if data else 0, error=error))
        # Consume completed requests immediately so Ctrl+C cannot lose later successful chunks.
        pool = ThreadPoolExecutor(max_workers=8)
        futures = set()
        try:
            pending = iter(missing)
            for bounds in missing[:8]:
                futures.add(pool.submit(fetch, bounds))
            for _ in missing[:8]:
                next(pending)
            while futures:
                completed, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in completed:
                    save_result(future)
                    futures.remove(future)
                    bounds = next(pending, None)
                    if bounds is not None:
                        futures.add(pool.submit(fetch, bounds))
        except KeyboardInterrupt:
            stopping.set()
            pool.shutdown(wait=True, cancel_futures=True)
            for future in futures:
                if not future.cancelled():
                    save_result(future)
            raise
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
        if fine:
            cursor = missing[-1][1] % total
            write_json(journal, dict(size=total, sha256=event['sha256'], ranges=additional, salvage_cursor=cursor))
        attempts.append(dict(attempt=attempt, ranges=outcomes, unprobed=unprobed, fine=fine))
        event['range_attempts'] = attempts
        event.update(progress(path, total, sha256=event['sha256']))
        print(f'RANGE PASS {attempt}/{retries + 2}: {event["path"]}; chunk={block}; saved={sum(row["saved_bytes"] > 0 for row in outcomes)}/{len(outcomes)}; bytes={event["downloaded_percent"]}; unprobed={unprobed}', flush=True)
    return ranges


def reconcile_course(root, course):
    from download import read_events, write_json
    state = root / '.download'
    history = state / 'course-history.json'
    statuses = json.loads(history.read_text()) if history.exists() else {}
    status = statuses.get(course)
    if not status or status.get('state') != 'finished':
        return
    source = json.loads((state / 'source.json').read_text())['source'].rstrip('/')
    proofs = {row['path']: row for row in read_events(state / 'events.jsonl')}
    registry = state / 'local-copies.json'
    if registry.exists():
        for row in json.loads(registry.read_text()):
            if row['source'].rstrip('/') == source and row['status'] == 'valid':
                local = Path(row['local'])
                if local.exists() and (local.stat().st_size, local.stat().st_mtime_ns) == (row['size'], row['mtime_ns']):
                    proofs[row['path']] = row
    prefix = '/' + course + '/'
    expected = [row for path, row in proofs.items() if path.startswith(prefix)]
    for manifest in sorted(state.glob('*-manifest.json'), key=lambda path: path.stat().st_mtime_ns, reverse=True):
        rows = json.loads(manifest.read_text())
        if len(rows) == status['total'] and all(row['path'].startswith(prefix) for row in rows):
            expected = rows
            break
    if len(expected) != status['total']:
        return
    verified = 0
    for row in expected:
        proof = proofs.get(row['path'], {})
        local = Path(proof['local']) if proof.get('local') else root.joinpath(*Path(row['path']).parts[1:])
        if (proof.get('status') == 'valid' and proof.get('sha256') == row['sha256']
                and proof.get('size') == row['size'] and local.exists()
                and local.stat().st_size == row['size']
                and ('mtime_ns' not in proof or local.stat().st_mtime_ns == proof['mtime_ns'])
                and not Path(str(local) + '.aria2').exists()):
            verified += 1
    status.update(verified=verified, failed=status['total'] - verified,
                  complete=verified == status['total'], time=datetime.now().astimezone().isoformat())
    write_json(history, statuses)
    for path in state.glob('*-status.json'):
        previous = json.loads(path.read_text())
        if previous.get('course') == course and previous.get('state') == 'finished':
            write_json(path, status)


def finish_recovery(root, event, item=None, retries=2, _redownload=True):
    from download import record, target, write_json
    path = body_path(root, event)
    if item:
        try:
            retry_ranges(root, event, item, retries)
        except OSError as error:
            event['recovery_error'] = str(error)
            print('RECOVERY ERROR: ' + event['path'] + '; ' + type(error).__name__, flush=True)
        path = body_path(root, event)
    ranges = saved_ranges(path, event['size'], event['sha256'])
    if ranges == [[0, event['size']]] and path.exists():
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if digest != event['sha256']:
            rejected = dict(event, sha256_mismatch=True, recovery_error='SHA-256 mismatch: ' + digest)
            for manifest in (root / 'corrupted').glob('**/failed.json'):
                rows = json.loads(manifest.read_text())
                changed = False
                for index, row in enumerate(rows):
                    relative = row.get('corrupted_path') or str(manifest.parent.joinpath(*Path(row['path']).parts[1:]).relative_to(root))
                    if relative == event['corrupted_path']:
                        rows[index] = rejected
                        changed = True
                if changed:
                    write_json(manifest, rows)
            registry = root / '.download/partial-copies.json'
            if registry.exists():
                copies = json.loads(registry.read_text())
                for copy in copies:
                    if copy['path'] == event['path'] and copy['sha256'] == event['sha256']:
                        copy['sha256_mismatch'] = True
                write_json(registry, copies)
            event.update(rejected)
            if not item or not _redownload:
                raise ValueError('SHA-256 mismatch; archived body retained and not promoted')
            # Retry in a separate body: no block hashes exist to locate the damaged bytes.
            event.setdefault('sha256_rejected_paths', []).append(event['corrupted_path'])
            new_recovery_body(root, event, 'SHA-256 mismatch; retry in a separate body')
            return finish_recovery(root, event, item, retries, _redownload=False)
        if digest == event['sha256'] and item:
            destination = target(root, item, event['course'])
            copy_saved(path, destination)
            Path(str(destination) + '.aria2').unlink(missing_ok=True)
            record(root / '.download', item, event['course'], 0, 'valid', 'Recovered all ranges and SHA-256 verified')
            reconcile_course(root, event['course'])
            event['range_recovered_complete'] = True
            return True
    return promote_partial(root, event)


def recover_corrupted(root, source, retries=2, retry=False, report_repo=None, mark=None, git_push=False, courses=None):
    from download import files, write_json, read_events
    root = root.resolve()
    candidates = {}
    for manifest in (root / 'corrupted').glob('**/failed.json'):
        for row in json.loads(manifest.read_text()):
            if courses is not None and row['course'] not in courses:
                continue
            if 'corrupted_path' not in row:
                row = dict(row, corrupted_path=str(manifest.parent.joinpath(*Path(row['path']).parts[1:]).relative_to(root)))
            path = body_path(root, row)
            amount = progress(path, row['size'], sha256=row['sha256']).get('downloaded_bytes') or 0
            previous = candidates.get(row['path'])
            rank = (not row.get('sha256_mismatch', False), amount, row['time'])
            old_rank = (not previous[1].get('sha256_mismatch', False), previous[0], previous[1]['time']) if previous else None
            if previous is None or rank > old_rank:
                candidates[row['path']] = (amount, row)
    valid = {row['path']: row for row in read_events(root / '.download/events.jsonl', repair=True)}
    registry = root / '.download/local-copies.json'
    if registry.exists():
        for row in json.loads(registry.read_text()):
            if row['source'].rstrip('/') == source.rstrip('/') and row['status'] == 'valid':
                local = Path(row['local'])
                if local.exists() and (local.stat().st_size, local.stat().st_mtime_ns) == (row['size'], row['mtime_ns']):
                    valid[row['path']] = row
    metadata = {}
    results = []
    for _, event in candidates.values():
        known = valid.get(event['path'], {})
        local = Path(known['local']) if known.get('local') else root.joinpath(*Path(event['path']).parts[1:])
        complete = (known.get('status') == 'valid' and known.get('sha256') == event['sha256']
                    and known.get('size') == event['size'] and local.exists()
                    and local.stat().st_size == known['size']
                    and ('mtime_ns' not in known or local.stat().st_mtime_ns == known['mtime_ns'])
                    and not Path(str(local) + '.aria2').exists())
        if retry and complete:
            reconcile_course(root, event['course'])
            continue
        archived = body_path(root, event)
        original = root.joinpath(*Path(event['path']).parts[1:])
        # Finish an archive move interrupted between manifest, body and control writes.
        control = Path(str(original) + '.aria2')
        if retry and (control.exists() or (original.exists() and not archived.exists())):
            archived.parent.mkdir(parents=True, exist_ok=True)
            for src, out in ((original, archived), (control, Path(str(archived) + '.aria2'))):
                if src.exists() and not out.exists():
                    src.rename(out)
        try:
            item = None
            if retry:
                course = event['course']
                if course not in metadata:
                    metadata[course] = {row['path']: row for row in files(source, '/' + course)}
                item = metadata[course].get(event['path'])
                if item is None:
                    raise ValueError('File no longer listed in source')
            finish_recovery(root, event, item, retries)
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
            event['recovery_error'] = str(error)
            print('RECOVERY ERROR: ' + event['path'] + '; ' + type(error).__name__, flush=True)
            if isinstance(error, OSError):
                try:
                    finish_recovery(root, event)
                except (OSError, ValueError, KeyError, subprocess.SubprocessError) as local_error:
                    event['local_recovery_error'] = str(local_error)
        if retry and not event.get('range_recovered_complete'):
            event.update(time=datetime.now().astimezone().isoformat(),
                         status='partial' if (event.get('downloaded_bytes') or 0) > 0 else 'missing',
                         detail='Independent HTTP Range recovery; incomplete original')
            with (root / '.download/events.jsonl').open('a') as log:
                log.write(json.dumps(event, ensure_ascii=False) + '\n')
        results.append(event)
        write_json(root / '.download/range-retry.json', results)
    by_path = {row['corrupted_path']: row for row in results}
    for manifest in (root / 'corrupted').glob('**/failed.json'):
        rows = json.loads(manifest.read_text())
        changed = False
        for index, row in enumerate(rows):
            relative = row.get('corrupted_path') or str(manifest.parent.joinpath(*Path(row['path']).parts[1:]).relative_to(root))
            replacement = by_path.get(relative)
            if replacement:
                rows[index] = replacement
                changed = True
        if changed:
            write_json(manifest, rows)
    if report_repo:
        from report import update_report
        update_report(root, report_repo, mark, git_push, f'Update {mark} partially available copies and range recovery')
    return results
