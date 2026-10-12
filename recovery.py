"""Retry independent ranges and retain partial videos by confirmed byte coverage."""
from datetime import datetime
import hashlib
import http.client
import json
from local_paths import local_path
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
import urllib.error
from progress import merge_ranges, saved_ranges, progress
from settings import resolve

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
    from download import STATE_LOCK
    with STATE_LOCK:
        return _promote_partial(root, event)


def _promote_partial(root, event):
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
    if (not original_path.is_absolute() or len(original_path.parts) < 2
            or '..' in original_path.parts or original_path.parts[1] in ('.download', 'corrupted')):
        raise ValueError('Unsafe recovered destination')
    original = local_path(root, event['path'])
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
        if (not old.get('sha256_mismatch') and (old['size'], old['sha256']) == (event['size'], event['sha256'])
                and (old.get('downloaded_bytes') or 0) >= (event.get('downloaded_bytes') or 0)):
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
    event['corrupted_path'] = str(local_path(archive, event['path']).relative_to(root))
    for key in ('sha256_mismatch', 'recovery_error', 'available_seconds', 'available_time_percent', 'available_intervals', 'time_method', 'range_attempts'):
        event.pop(key, None)
    event['restart_reason'] = reason
    event.update(progress(body_path(root, event), event['size'], sha256=event['sha256']))
    recovery_state(root, event, 'running')
    write_json(archive / 'failed.json', [event])


def missing_chunks(ranges, total, block):
    """Split only unconfirmed holes, keeping the remote byte alignment."""
    missing = []
    position = 0
    for start, end in ranges + [[total, total]]:
        while position < start:
            stop = min(start, (position // block + 1) * block)
            missing.append((position, stop))
            position = stop
        position = end
    return missing


def spread_probes(missing, limit):
    count = min(len(missing), limit)
    if count < 2:
        return missing[:count]
    return [missing[i * (len(missing) - 1) // (count - 1)] for i in range(count)]


def expand_probes(ranges, total, outcomes, block, limit, minimum, discoveries=None):
    """Try larger neighbours of discoveries without overlapping saved/pending bytes."""
    planned = []
    reserved = ranges
    failures = {(row['start'], row['end']) for row in outcomes if not row['saved_bytes']}
    for row in outcomes if discoveries is None else discoveries:
        if not row['saved_bytes']:
            continue
        for anchor, backwards in ((row['start'], True), (row['start'] + row['saved_bytes'], False)):
            for start, end in missing_chunks(reserved, total, total):
                if backwards and start < anchor <= end:
                    bounds = (max(start, anchor - block), anchor)
                elif not backwards and start <= anchor < end:
                    bounds = (anchor, min(end, anchor + block))
                else:
                    continue
                while bounds in failures and bounds[1] - bounds[0] > minimum:
                    length = max(minimum, (bounds[1] - bounds[0]) // 2)
                    bounds = (bounds[1] - length, bounds[1]) if backwards else (bounds[0], bounds[0] + length)
                if bounds in failures:
                    break
                if len(planned) >= limit:
                    return planned
                planned.append(bounds)
                reserved = merge_ranges(reserved + [list(bounds)])
                break
    return planned


def retry_ranges(root, event, item, retries, tuning=None, stopping=None, download_active=None, request_budget=None):
    tuning = resolve(tuning)
    from download import OPENER, write_json, api
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
    write_json(journal, dict(size=event['size'], sha256=event['sha256'], ranges=additional, salvage_cursor=cursor, control_imported=True))
    total = event['size']
    missing_bytes = total - sum(end - start for start, end in ranges)
    if not missing_bytes:
        return ranges
    if shutil.disk_usage(root).free < missing_bytes + tuning['free_space_mib'] * 1024 ** 2:
        raise RuntimeError('Insufficient disk space for range recovery')
    rate_lock = Lock()
    stopping = stopping if stopping is not None else Event()
    foreground_limit = Lock()
    next_request = 0.0
    def fetch(bounds):
        if request_budget is not None:
            try:
                with request_budget.request(stopping):
                    return fetch_range(bounds)
            except InterruptedError:
                return bounds, None, 'Interrupted'
        if download_active is not None and download_active.is_set():
            while not foreground_limit.acquire(timeout=0.25):
                if stopping.is_set():
                    return bounds, None, 'Interrupted'
            try:
                return fetch_range(bounds)
            finally:
                foreground_limit.release()
        return fetch_range(bounds)
    def fetch_range(bounds):
        nonlocal next_request
        start, end = bounds
        with rate_lock:
            delay = max(0, next_request - time.monotonic())
            while delay > 0:
                if stopping.is_set():
                    return bounds, None, 'Interrupted'
                pause = min(delay, 0.25)
                time.sleep(pause)
                delay -= pause
            next_request = time.monotonic() + 1 / tuning['range_rps']
        if stopping.is_set():
            return bounds, None, 'Interrupted'
        request = urllib.request.Request(item['file'], headers={'Range': f'bytes={start}-{end - 1}', 'Accept-Encoding': 'identity'})
        try:
            with OPENER.open(request, timeout=tuning['range_timeout']) as response:
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
    max_passes = tuning['range_max_passes'] or min(4, 2 * retries) + 2
    source = json.loads((root / '.download/source.json').read_text())['source']
    metadata_path = event['path']
    def refresh():
        nonlocal metadata_path
        try:
            fresh = api(source, metadata_path)
        except urllib.error.HTTPError as error:
            if error.code != 404 or event['course'] != '.':
                raise
            fresh = api(source)
            if fresh.get('type') != 'file' or '/' + fresh.get('name', '') != event['path']:
                raise ValueError('Source file disappeared; refuse to combine ranges')
            metadata_path = '/'
        if (fresh.get('size'), fresh.get('sha256')) != (event['size'], event['sha256']):
            raise ValueError('Source file changed or disappeared; refuse to combine ranges')
        from download import target
        target(root, dict(fresh, path=event['path']), event['course'])
        return fresh
    item = refresh()
    failed_count = 0
    remaining_span = total
    for attempt in range(1, max_passes + 1):
        if stopping.is_set():
            raise InterruptedError('Range interrupted')
        # Backend block geometry is unpublished; refine failed intervals by observed responses.
        minimum = tuning['range_min_block_size_kib'] * 1024
        block = max(minimum, tuning['range_block_size_kib'] * 1024 // (2 ** (attempt - 1)))
        if attempt > 1:
            block = min(block, max(minimum, (remaining_span + 1) // 2))
        fine = attempt > 1 and (block <= minimum or failed_count > tuning['range_failure_limit'] or attempt == max_passes)
        if fine:
            block = tuning['range_linear_block_size_kib'] * 1024
        missing = missing_chunks(ranges, total, block)
        if not missing:
            break
        unprobed = 0
        if attempt > 1:
            if download_active is None:
                time.sleep(tuning['range_retry_delay'])
            else:
                delay = tuning['range_retry_delay']
                while delay > 0:
                    if stopping.is_set():
                        raise InterruptedError('Range interrupted')
                    pause = min(delay, 0.25)
                    time.sleep(pause)
                    delay -= pause
        if attempts and any(row['error'] in ('HTTP 401', 'HTTP 403') for row in attempts[-1]['ranges']):
            item = refresh()
        outcomes = []
        def save_result(future):
            nonlocal additional, ranges
            bounds, data, error = future.result()
            if data is not None:
                saved = [bounds[0], bounds[0] + len(data)]
                with path.open('r+b' if path.exists() else 'w+b') as stream:
                    stream.seek(saved[0]); stream.write(data); stream.flush(); os.fsync(stream.fileno())
                additional = merge_ranges(additional + [saved])
                write_json(journal, dict(size=total, sha256=event['sha256'], ranges=additional, salvage_cursor=cursor, control_imported=True))
                ranges = merge_ranges(ranges + [saved])
            outcomes.append(dict(start=bounds[0], end=bounds[1], saved_bytes=len(data) if data else 0, error=error))
        def execute(batch, mode):
            # Persist each completed request before waiting for the rest of the batch.
            workers = tuning['range_workers']
            pool = ThreadPoolExecutor(max_workers=workers)
            futures = set()
            first_outcome = len(outcomes)
            submitted = 0
            try:
                pending = iter(batch)
                for _ in range(workers):
                    bounds = next(pending, None)
                    if bounds is not None:
                        futures.add(pool.submit(fetch, bounds))
                        submitted += 1
                while futures:
                    completed, _ = wait(futures, return_when=FIRST_COMPLETED)
                    for future in completed:
                        save_result(future)
                        futures.remove(future)
                        rows = outcomes[first_outcome:]
                        cutoff = (mode == 'halving' and attempt < max_passes
                                  and len(rows) > tuning['range_failure_limit']
                                  and not any(row['saved_bytes'] for row in rows))
                        bounds = None if cutoff else next(pending, None)
                        if bounds is not None and not stopping.is_set():
                            futures.add(pool.submit(fetch, bounds))
                            submitted += 1
            except KeyboardInterrupt:
                stopping.set()
                pool.shutdown(wait=True, cancel_futures=True)
                for future in futures:
                    if not future.cancelled():
                        save_result(future)
                raise
            finally:
                pool.shutdown(wait=True, cancel_futures=True)
                for row in outcomes[first_outcome:]:
                    row['mode'] = mode
            if stopping.is_set():
                raise InterruptedError('Range interrupted')
            return len(batch) - submitted

        if fine:
            budget = tuning['range_linear_probes']
            # Reserve at least half the budget for expansion and resumable linear coverage.
            ordered = [bounds for bounds in missing if bounds[0] >= cursor]
            ordered += [bounds for bounds in missing if bounds[0] < cursor]
            scatter = spread_probes(ordered, budget // 2)
            execute(scatter, 'scatter')
            previous = [row for earlier in attempts for row in earlier['ranges']]
            expansions = expand_probes(ranges, total, previous + outcomes,
                                       tuning['range_block_size_kib'] * 1024, budget // 4, block, outcomes)
            execute(expansions, 'expand')
            remaining = missing_chunks(ranges, total, block)
            tested = set(scatter)
            linear = [bounds for bounds in remaining if bounds[0] >= cursor and bounds not in tested]
            linear += [bounds for bounds in remaining if bounds[0] < cursor and bounds not in tested]
            linear = linear[:budget - len(outcomes)]
            execute(linear, 'linear')
            if linear:
                cursor = linear[-1][1] % total
            small_tests = tested | set(linear)
            unprobed = sum(bounds not in small_tests for bounds in missing_chunks(ranges, total, block))
            write_json(journal, dict(size=total, sha256=event['sha256'], ranges=additional, salvage_cursor=cursor, control_imported=True))
        else:
            unprobed = execute(missing, 'halving')
        attempts.append(dict(attempt=attempt, ranges=outcomes, unprobed=unprobed, fine=fine))
        failed_count = sum(row['saved_bytes'] < row['end'] - row['start'] for row in outcomes)
        remaining_span = max((row['end'] - row['start'] - row['saved_bytes'] for row in outcomes), default=0)
        event['range_attempts'] = attempts
        event.update(progress(path, total, sha256=event['sha256']))
        print(f'RANGE PASS {attempt}/{max_passes}: {event["path"]}; chunk={block}; mode={"adaptive" if fine else "halving"}; saved={sum(row["saved_bytes"] > 0 for row in outcomes)}/{len(outcomes)}; bytes={event["downloaded_percent"]}; unprobed={unprobed}', flush=True)
        if fine:
            break
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
    prefix = '/' if course == '.' else '/' + course + '/'
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
        local = Path(proof['local']) if proof.get('local') else local_path(root, row['path'])
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


def recovery_state(root, event, state):
    from download import write_json, STATE_LOCK
    path = root / '.download/recovery-state.json'
    with STATE_LOCK:
        ledger = json.loads(path.read_text()) if path.exists() else {}
        ledger[event['path']] = dict(state=state, event=dict(event))
        write_json(path, ledger)


def finish_recovery(root, event, item=None, retries=2, _redownload=True, tuning=None, stopping=None, download_active=None, background=False, request_budget=None):
    if event.get('sha256_rejected_paths'):
        _redownload = False
    if item is None:
        return _finish_recovery(root, event, item, retries, _redownload, tuning, stopping, download_active, background, request_budget)
    recovery_state(root, event, 'running')
    try:
        result = _finish_recovery(root, event, item, retries, _redownload, tuning, stopping, download_active, background, request_budget)
    except ValueError:
        recovery_state(root, event, 'finished')
        raise
    recovery_state(root, event, 'finished')
    return result


def _finish_recovery(root, event, item=None, retries=2, _redownload=True, tuning=None, stopping=None, download_active=None, background=False, request_budget=None):
    from download import record, target, write_json, sha256_file, inspect
    path = body_path(root, event)
    if item:
        try:
            if background:
                retry_ranges(root, event, item, retries, tuning, stopping, download_active, request_budget)
            else:
                retry_ranges(root, event, item, retries, tuning)
        except InterruptedError:
            raise
        except OSError as error:
            if background and not isinstance(error, urllib.error.URLError):
                raise
            event['recovery_error'] = str(error)
            print('RECOVERY ERROR: ' + event['path'] + '; ' + type(error).__name__, flush=True)
        path = body_path(root, event)
    ranges = saved_ranges(path, event['size'], event['sha256'])
    if ranges == [[0, event['size']]] and path.exists():
        digest = sha256_file(path, stopping)
        if digest != event['sha256']:
            rejected = dict(event, sha256_mismatch=True, recovery_error='SHA-256 mismatch: ' + digest)
            from download import STATE_LOCK
            with STATE_LOCK:
                for manifest in (root / 'corrupted').glob('**/failed.json'):
                    rows = json.loads(manifest.read_text())
                    changed = False
                    for index, row in enumerate(rows):
                        relative = row.get('corrupted_path') or str(local_path(manifest.parent, row['path']).relative_to(root))
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
            return finish_recovery(root, event, item, retries, _redownload=False, tuning=tuning,
                                   stopping=stopping, download_active=download_active, background=background, request_budget=request_budget)
        if digest == event['sha256'] and item:
            destination = target(root, item, event['course'])
            control = Path(str(destination) + '.aria2')
            if destination.exists() or control.exists():
                status, _ = inspect(root, item, event['course'], path=destination, stopping=stopping)
                if status != 'valid':
                    raise FileExistsError('Refusing to replace an unverified original: ' + str(destination))
            else:
                copy_saved(path, destination)
            record(root / '.download', item, event['course'], 0, 'valid', 'Recovered all ranges and SHA-256 verified')
            if not background:
                reconcile_course(root, event['course'])
            event['range_recovered_complete'] = True
            return True
    return promote_partial(root, event)


def recover_corrupted(root, source, retries=2, retry=False, report_repo=None, mark=None, git_push=False, courses=None, force=False, tuning=None, deep=False):
    from download import files, write_json, read_events
    root = root.resolve()
    candidates = {}
    for manifest in (root / 'corrupted').glob('**/failed.json'):
        for row in json.loads(manifest.read_text()):
            if courses is not None and row['course'] not in courses:
                continue
            if 'corrupted_path' not in row:
                row = dict(row, corrupted_path=str(local_path(manifest.parent, row['path']).relative_to(root)))
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
    ledger_path = root / '.download/recovery-state.json'
    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
    metadata = {}
    results = []
    for _, event in candidates.values():
        known = valid.get(event['path'], {})
        if retry and deep:
            from download import inspect_copies
            checked = inspect_copies(root, event, event['course'], known, source)
            if checked['status'] == 'valid':
                checked['range_recovered_complete'] = True
                results.append(checked)
                reconcile_course(root, event['course'])
                continue
        local = Path(known['local']) if known.get('local') else local_path(root, event['path'])
        complete = (known.get('status') == 'valid' and known.get('sha256') == event['sha256']
                    and known.get('size') == event['size'] and local.exists()
                    and local.stat().st_size == known['size']
                    and ('mtime_ns' not in known or local.stat().st_mtime_ns == known['mtime_ns'])
                    and not Path(str(local) + '.aria2').exists())
        if retry and complete:
            reconcile_course(root, event['course'])
            continue
        ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
        saved = ledger.get(event['path'])
        if saved and (saved['event']['size'], saved['event']['sha256']) == (event['size'], event['sha256']):
            event = dict(saved['event'])
            if retry and not force and saved['state'] == 'finished':
                results.append(event)
                print('RECOVERY SKIPPED: ' + event['path'] + '; attempt already finished', flush=True)
                continue
        elif not saved and retry and not force and event.get('range_attempts'):
            # Previous completed passes predate the per-file ledger.
            ledger[event['path']] = dict(state='finished', event=dict(event))
            write_json(ledger_path, ledger)
            results.append(event)
            continue
        archived = body_path(root, event)
        original = local_path(root, event['path'])
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
                    event['recovery_error'] = 'File no longer listed in source'
                    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
                    ledger[event['path']] = dict(state='finished', event=dict(event))
                    write_json(ledger_path, ledger)
                    raise ValueError('File no longer listed in source')
            finish_recovery(root, event, item, retries, tuning=tuning)
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
            event['recovery_error'] = str(error)
            print('RECOVERY ERROR: ' + event['path'] + '; ' + type(error).__name__, flush=True)
            if isinstance(error, OSError):
                try:
                    finish_recovery(root, event)
                    if retry:
                        recovery_state(root, event, 'finished')
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
    by_path = {row['corrupted_path']: row for row in results if row.get('corrupted_path')}
    for manifest in (root / 'corrupted').glob('**/failed.json'):
        rows = json.loads(manifest.read_text())
        changed = False
        for index, row in enumerate(rows):
            relative = row.get('corrupted_path') or str(local_path(manifest.parent, row['path']).relative_to(root))
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
