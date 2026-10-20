"""Retry independent ranges and retain partial videos by confirmed byte coverage."""
from datetime import datetime
from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from itertools import islice
import hashlib
import errno
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
import network
import file_hashes
import storage
from file_state import FileState
from progress import merge_ranges, saved_ranges, progress
from settings import resolve

SUFFIX = ' (partially available)'


def body_path(root, event):
    storage.check(root)
    path = root / event['corrupted_path']
    if not path.resolve().is_relative_to(root / 'corrupted'):
        raise ValueError('Unsafe corrupted path')
    return path


def copy_saved(source, destination, overwrite=True, before_publish=None, expected_sha256=None, stopping=None):
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Preserve sparse holes and the archive used for future retries.
    with tempfile.NamedTemporaryFile(dir=destination.parent, prefix='.partial-', delete=False) as temporary:
        name = Path(temporary.name)
    try:
        try:
            subprocess.run(['cp', '--reflink=auto', '--sparse=always', '--', str(source), str(name)],
                           check=True, stderr=subprocess.PIPE, env=dict(os.environ, LC_ALL='C'))
        except subprocess.CalledProcessError as error:
            message = (error.stderr or b'').decode('utf-8', errors='replace')
            for text, code in [('Input/output error', errno.EIO), ('Read-only file system', errno.EROFS),
                               ('No space left on device', errno.ENOSPC), ('Stale file handle', errno.ESTALE),
                               ('No such device', errno.ENODEV)]:
                if text in message:
                    raise OSError(code, message.strip(), str(source)) from error
            raise
        with name.open('rb') as stream:
            os.fsync(stream.fileno())
        stat = name.stat()
        if before_publish is not None:
            before_publish([stat.st_ino, stat.st_size, stat.st_mtime_ns])
        if overwrite:
            name.replace(destination)
        else:
            os.link(name, destination)
        name.unlink(missing_ok=True)
        published = file_hashes.proof_stat(destination.stat())
        if published[:3] != file_hashes.proof_stat(stat)[:3]:
            raise OSError('Copy changed during publication')
        storage.sync_directory(destination.parent)
        if file_hashes.proof_stat(destination.stat()) != published:
            raise OSError('Copy changed during publication')
        if expected_sha256 is not None:
            file_hashes.calculate(destination, stopping, expected_sha256=expected_sha256)
            if file_hashes.proof_stat(destination.stat()) != published:
                raise OSError('Copy changed during publication')
        return published
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
    if old and old.get('pending_stat'):
        stat = destination.stat() if destination.exists() else None
        stamp = [stat.st_ino, stat.st_size, stat.st_mtime_ns] if stat else None
        if stamp == old['pending_stat']:
            storage.sync_directory(destination.parent)
            old = dict(old)
            old.pop('pending_stat'); old.pop('previous', None)
            copies = [row for row in copies if not (row['source'] == source and row['path'] == event['path'])] + [old]
            write_json(registry, copies)
        else:
            old = old.get('previous')
    if old and destination.exists():
        stat = destination.stat()
        if (stat.st_size, stat.st_mtime_ns) != (old['local_size'], old['mtime_ns']):
            raise FileExistsError('Partial copy changed locally: ' + str(destination))
        if old.get('ctime_ns') is not None and old['ctime_ns']!=stat.st_ctime_ns:
            raise FileExistsError('Partial copy changed locally: ' + str(destination))
        if (not old.get('sha256_mismatch') and (old['size'], old['sha256']) == (event['size'], event['sha256'])
                and (old.get('downloaded_bytes') or 0) >= (event.get('downloaded_bytes') or 0)):
            return True
    if destination.exists() and not old:
        raise FileExistsError('Unregistered partial copy: ' + str(destination))
    others = [row for row in copies if not (row['source'] == source and row['path'] == event['path'])]
    copy = None
    def prepare(stamp):
        nonlocal copy
        copy = dict(event, source=source, recovered_path=str(destination.relative_to(root)),
                    local_size=stamp[1], mtime_ns=stamp[2], pending_stat=stamp, previous=old,
                    recovery_time=datetime.now().astimezone().isoformat())
        write_json(registry, others + [copy])
    published=copy_saved(path, destination, overwrite=bool(old and destination.exists()), before_publish=prepare)
    copy['ctime_ns']=published[3]
    copy.pop('pending_stat'); copy.pop('previous')
    write_json(registry, others + [copy])
    print(f'PARTIALLY AVAILABLE: {event["path"]}; bytes={event["downloaded_percent"]:.2f}%', flush=True)
    return True


def new_recovery_body(root, event, reason):
    from download import write_json, write_failures
    archive = root / 'corrupted' / (datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '-recovery')
    archive.mkdir(parents=True)
    event.setdefault('preserved_archive_paths', []).append(event['corrupted_path'])
    event['corrupted_path'] = str(local_path(archive, event['path']).relative_to(root))
    for key in ('sha256_mismatch', 'recovery_error', 'available_seconds', 'available_time_percent', 'available_intervals', 'time_method', 'range_attempts', 'range_attempted', 'recheck_continue', 'recheck_full_pass'):
        event.pop(key, None)
    event['restart_reason'] = reason
    event.update(progress(body_path(root, event), event['size'], sha256=event['sha256']))
    recovery_state(root, event, 'running')
    write_failures(archive / 'failed.json', [event])


class MissingChunks(Sequence):
    """Index missing blocks using only gap boundaries, including cursor rotation."""
    def __init__(self, ranges, total, block, cursor=0):
        self.holes, self.ends = [], []
        self.block, self.offset = block, 0
        position, count = 0, 0
        for start, end in ranges + [[total, total]]:
            if position < start:
                self.holes.append((position, start))
                count += (start - 1) // block - position // block + 1
                self.ends.append(count)
            position = end
        if count and cursor:
            self.offset = bisect_left(self, cursor, key=lambda bounds: bounds[0]) % count

    def __len__(self):
        return self.ends[-1] if self.ends else 0

    def __contains__(self, bounds):
        start, end = bounds
        gap = bisect_right(self.holes, start, key=lambda hole: hole[0]) - 1
        if gap < 0:
            return False
        a, b = self.holes[gap]
        aligned = start // self.block * self.block
        return a <= start < b and (start, end) == (max(a, aligned), min(b, aligned + self.block))

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        if index < 0:
            index += len(self)
        if not 0 <= index < len(self):
            raise IndexError(index)
        index = (index + self.offset) % len(self)
        gap = bisect_right(self.ends, index)
        start, end = self.holes[gap]
        number = index - (self.ends[gap - 1] if gap else 0)
        aligned = (start // self.block + number) * self.block
        return max(start, aligned), min(end, aligned + self.block)


def missing_chunks(ranges, total, block, cursor=0):
    return MissingChunks(ranges, total, block, cursor)


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


def recheck_probe_ranges(ranges, total, small_block, large_block, limit, cursor):
    """Sample missing byte positions without enumerating small chunks of large files."""
    holes = missing_chunks(ranges, total, total)
    missing = sum(end - start for start, end in holes)
    if not missing or not limit:
        return []
    offset = sum(max(0, min(cursor, end) - start) for start, end in holes) % missing
    def bounds(position, block):
        for start, end in holes:
            length = end - start
            if position < length:
                start += position
                return start, min(end, start + block)
            position -= length
    probes = [bounds(offset, large_block)]
    count = min(limit - 1, (missing + small_block - 1) // small_block)
    for index in range(count):
        probe = bounds((offset + index * missing // count) % missing, small_block)
        if probe not in probes:
            probes.append(probe)
    return probes


def retry_ranges(root, event, item, retries, tuning=None, stopping=None, download_active=None, request_budget=None, metrics=None, probe_requests=0, on_request=None):
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
    continuation = event.get('recheck_continue', False)
    if probe_requests or continuation:
        event['recheck_full_pass'] = bool(continuation)
    if not missing_bytes:
        event.pop('recheck_continue', None)
        return ranges
    if shutil.disk_usage(root).free < missing_bytes + tuning['free_space_mib'] * 1024 ** 2:
        raise OSError(errno.ENOSPC, 'Insufficient disk space for range recovery')
    rate_lock = Lock()
    stopping = stopping if stopping is not None else Event()
    foreground_limit = Lock()
    next_request = 0.0
    refresh_lock = Lock()
    refresh_errors = []
    pass_refreshed = False
    def fetch(bounds):
        nonlocal item, pass_refreshed
        started = time.monotonic()
        timing = dict(request_seconds=0.0, first_byte_seconds=None, request_attempts=0,
                      retry_reasons={}, request_errors={}, last_failure=None)
        data = bytearray()
        retry = network.RequestRetry(item['sha256'] + item['path'], stopping)
        auth_replayed = False
        after_eof = False
        previous_eof_bytes = 0
        while True:
            if refresh_errors:
                result = bounds, data or None, 'Interrupted'
                break
            timing['last_failure'] = None
            before_attempt = timing['request_attempts']
            before_seconds = timing['request_seconds']
            request_start = bounds[0] + len(data)
            _, chunk, error = fetch_limited((request_start, bounds[1]), timing)
            if after_eof and timing['request_attempts'] > before_attempt and metrics is not None:
                try:
                    metrics.eof_retry(dict(path=event['path'], sha256=event['sha256'],
                        attempt=timing['request_attempts'], start=request_start, end=bounds[1],
                        accepted_bytes=len(chunk) if chunk else 0,
                        request_seconds=timing['request_seconds']-before_seconds,
                        previous_eof_bytes=previous_eof_bytes, error=error))
                except (OSError, ValueError, TypeError, AttributeError) as metric_error:
                    metrics.warning(metric_error)
            received = len(chunk) if chunk else 0
            if not data:
                data = chunk if chunk is not None else bytearray()
            elif chunk:
                data.extend(chunk)
            del chunk
            failed_url = timing.pop('_request_url', None)
            details = timing['last_failure']
            eof = (error == 'Incomplete response' or details is not None
                   and details['error_type'] in ('SSLEOFError', 'IncompleteRead', 'RemoteDisconnected'))
            if details:
                key = f'{details["phase"]}:{details["category"]}:{details["errno"]}:{details["http_status"]}'
                timing['request_errors'][key] = timing['request_errors'].get(key, 0) + 1
            try:
                if details and details['http_status'] in (401, 403) and not auth_replayed:
                    while not refresh_lock.acquire(timeout=.25):
                        if stopping.is_set() or refresh_errors:
                            raise InterruptedError('Link refresh interrupted')
                    try:
                        if stopping.is_set() or refresh_errors:
                            raise InterruptedError('Link refresh interrupted')
                        if not pass_refreshed and item['file'] == failed_url:
                            item = refresh()
                            pass_refreshed = True
                    except InterruptedError:
                        raise
                    except Exception as error:
                        refresh_errors.append(error)
                        raise InterruptedError('Link metadata unavailable') from error
                    finally:
                        refresh_lock.release()
                    auth_replayed = True
                    after_eof = False
                    timing['retry_reasons']['link_expired'] = 1
                    continue
                again = (not refresh_errors and len(data) < bounds[1] - bounds[0] and error is not None
                         and details is not None and not (eof and details['phase'] == 'body')
                         and retry.retry(details))
            except InterruptedError:
                result = bounds, data or None, 'Interrupted'
                break
            if not again:
                result = bounds, data or None, error
                break
            after_eof = eof
            previous_eof_bytes = received
            reason = details['category']
            timing['retry_reasons'][reason] = timing['retry_reasons'].get(reason, 0) + 1
        timing['seconds'] = time.monotonic() - started
        return (*result, timing)
    def fetch_limited(bounds, timing):
        if request_budget is not None:
            try:
                with request_budget.request(stopping):
                    return fetch_range(bounds, timing)
            except InterruptedError:
                return bounds, None, 'Interrupted'
        if download_active is not None and download_active.is_set():
            while not foreground_limit.acquire(timeout=0.25):
                if stopping.is_set():
                    return bounds, None, 'Interrupted'
            try:
                return fetch_range(bounds, timing)
            finally:
                foreground_limit.release()
        return fetch_range(bounds, timing)
    def fetch_range(bounds, timing):
        nonlocal next_request
        start, end = bounds
        with rate_lock:
            delay = max(0, next_request - time.monotonic())
            while delay > 0:
                if stopping.is_set() or refresh_errors:
                    return bounds, None, 'Interrupted'
                pause = min(delay, 0.25)
                time.sleep(pause)
                delay -= pause
            next_request = time.monotonic() + 1 / tuning['range_rps']
        if stopping.is_set() or refresh_errors:
            return bounds, None, 'Interrupted'
        timing['_request_url'] = item['file']
        request = urllib.request.Request(timing['_request_url'], headers={'Range': f'bytes={start}-{end - 1}', 'Accept-Encoding': 'identity'})
        request_started = time.monotonic()
        timing['request_attempts'] += 1
        timing['last_failure'] = None
        phase = 'connect'
        if on_request is not None:
            on_request()
        try:
            with OPENER.open(request, timeout=tuning['range_timeout']) as response:
                phase = 'headers'
                network.GUARD.observe(item['sha256'] + item['path'])
                served = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', response.headers.get('Content-Range', ''))
                if (response.status != 206 or not served or int(served[1]) != start
                        or not start <= int(served[2]) < end or int(served[3]) != total
                        or response.headers.get('Content-Encoding', 'identity') != 'identity'):
                    return bounds, None, 'Range not honoured'
                length = int(served[2]) - start + 1
                content_length = response.headers.get('Content-Length')
                transfer_encoding = response.headers.get('Transfer-Encoding')
                if transfer_encoding is not None and (transfer_encoding.strip().lower() != 'chunked'
                                                       or not getattr(response, 'chunked', True)):
                    return bounds, None, 'Unsupported Range Transfer-Encoding'
                if content_length is not None and transfer_encoding is not None:
                    return bounds, None, 'Ambiguous Range framing'
                if content_length is not None:
                    content_length = content_length.strip()
                if content_length is not None and (not re.fullmatch(r'[0-9]+', content_length)
                                                   or int(content_length) != length):
                    return bounds, None, 'Invalid Range Content-Length'
                data = bytearray()
                error = None
                phase = 'body'
                try:
                    while len(data) < length and not stopping.is_set() and not refresh_errors:
                        chunk = response.read1(min(64 * 1024, length - len(data)))
                        if not chunk:
                            break
                        if timing['first_byte_seconds'] is None:
                            timing['first_byte_seconds'] = time.monotonic() - request_started
                        data.extend(chunk)
                    if len(data) == length and content_length is None and not stopping.is_set():
                        if response.read1(1):
                            return bounds, None, 'Oversized response'
                except http.client.IncompleteRead as failure:
                    if failure.partial and timing['first_byte_seconds'] is None:
                        timing['first_byte_seconds'] = time.monotonic() - request_started
                    data.extend(failure.partial)
                    error = 'Incomplete response'
                    timing['last_failure'] = network.failure(failure, phase)
                except (OSError, http.client.HTTPException) as failure:
                    error = type(failure).__name__
                    timing['last_failure'] = network.failure(failure, phase)
                if len(data) > length:
                    return bounds, None, 'Oversized response'
                if len(data) < length and timing['last_failure'] is None:
                    timing['last_failure'] = network.failure('Incomplete response', phase)
                if data:
                    network.GUARD.observe(item['sha256'] + item['path'])
                return bounds, data or None, error or (None if len(data) == length else 'Incomplete response')
        except (OSError, http.client.HTTPException) as error:
            timing['last_failure'] = network.failure(error, phase)
            if isinstance(error, http.client.IncompleteRead):
                # The opener may read a redirect body before returning the Range response.
                timing['last_failure']['phase'] = phase
            return bounds, None, 'HTTP ' + str(error.code) if hasattr(error, 'code') else type(error).__name__
        finally:
            timing['request_seconds'] += time.monotonic() - request_started
    attempts = []
    max_passes = tuning['range_max_passes'] or min(4, 2 * retries) + 2
    source = json.loads((root / '.download/source.json').read_text())['source']
    metadata_path = event['path']
    def refresh():
        nonlocal metadata_path
        try:
            fresh = api(source, metadata_path, stopping=stopping)
        except urllib.error.HTTPError as error:
            if error.code != 404 or event['course'] != '.':
                raise
            fresh = api(source, stopping=stopping)
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
    for attempt in range(0 if probe_requests and not continuation else 1, max_passes + 1):
        pass_refreshed = False
        pass_started = time.monotonic()
        if stopping.is_set():
            raise InterruptedError('Range interrupted')
        # Backend block geometry is unpublished; refine failed intervals by observed responses.
        minimum = tuning['range_min_block_size_kib'] * 1024
        block = max(minimum, tuning['range_block_size_kib'] * 1024 // (2 ** max(0, attempt - 1)))
        if attempt > 1:
            block = min(block, max(minimum, (remaining_span + 1) // 2))
        fine = attempt > 1 and (block <= minimum or failed_count > tuning['range_failure_limit'] or attempt == max_passes)
        if fine:
            block = tuning['range_linear_block_size_kib'] * 1024
        missing = (recheck_probe_ranges(ranges, total, tuning['range_linear_block_size_kib'] * 1024,
                                        block, probe_requests, cursor) if attempt == 0
                   else missing_chunks(ranges, total, block))
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
        outcomes = []
        def save_result(future):
            nonlocal additional, ranges
            bounds, data, error, timing = future.result()
            save_started = time.monotonic()
            if data:
                if attempt == 0 and not event.get('recheck_continue'):
                    # Commit escalation before writing bytes so interruption cannot lose the decision.
                    event.update(recheck_continue=True, recheck_full_pass=True)
                    recovery_state(root, event, 'running')
                saved = [bounds[0], bounds[0] + len(data)]
                with path.open('r+b' if path.exists() else 'w+b') as stream:
                    stream.seek(saved[0]); stream.write(data); stream.flush(); os.fsync(stream.fileno())
                additional = merge_ranges(additional + [saved])
                write_json(journal, dict(size=total, sha256=event['sha256'], ranges=additional, salvage_cursor=cursor, control_imported=True))
                ranges = merge_ranges(ranges + [saved])
            outcomes.append(dict(start=bounds[0], end=bounds[1], saved_bytes=len(data) if data else 0, error=error,
                                 save_seconds=time.monotonic() - save_started, **timing))
            if metrics is not None:
                metrics.range_request(outcomes[-1])
        def execute(batch, mode):
            # Persist each completed request before waiting for the rest of the batch.
            workers = 1 if mode == 'probe' else tuning['range_workers']
            pool = ThreadPoolExecutor(max_workers=workers)
            futures = set()
            first_outcome = len(outcomes)
            submitted = 0
            try:
                pending = iter(batch)
                for _ in range(workers):
                    if refresh_errors:
                        break
                    bounds = next(pending, None)
                    if bounds is not None:
                        futures.add(pool.submit(fetch, bounds))
                        submitted += 1
                while futures:
                    completed = wait(futures, return_when=FIRST_COMPLETED).done
                    while completed:
                        future = completed.pop()
                        save_result(future)
                        futures.remove(future)
                        del future
                        rows = outcomes[first_outcome:]
                        cutoff = (mode == 'probe' and any(row['saved_bytes'] for row in rows)
                                  or mode == 'halving' and attempt < max_passes
                                  and len(rows) > tuning['range_failure_limit']
                                  and not any(row['saved_bytes'] for row in rows))
                        bounds = None if cutoff else next(pending, None)
                        if bounds is not None and not stopping.is_set() and not refresh_errors:
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
            if refresh_errors:
                raise refresh_errors[0]
            if stopping.is_set():
                raise InterruptedError('Range interrupted')
            return len(batch) - submitted

        if attempt == 0:
            unprobed = execute(missing, 'probe')
            cursor = missing[min(1, len(missing) - 1)][1] % total
            write_json(journal, dict(size=total, sha256=event['sha256'], ranges=additional, salvage_cursor=cursor, control_imported=True))
        elif fine:
            budget = tuning['range_linear_probes']
            # Reserve at least half the budget for expansion and resumable linear coverage.
            ordered = missing_chunks(ranges, total, block, cursor)
            scatter = spread_probes(ordered, budget // 2)
            execute(scatter, 'scatter')
            previous = [row for earlier in attempts for row in earlier['ranges']]
            expansions = expand_probes(ranges, total, previous + outcomes,
                                       tuning['range_block_size_kib'] * 1024, budget // 4, block, outcomes)
            execute(expansions, 'expand')
            remaining = missing_chunks(ranges, total, block, cursor)
            tested = set(scatter)
            linear = list(islice((bounds for bounds in remaining if bounds not in tested),
                                 max(0, budget - len(outcomes))))
            execute(linear, 'linear')
            if linear:
                cursor = linear[-1][1] % total
            small_tests = tested | set(linear)
            remaining = missing_chunks(ranges, total, block)
            unprobed = len(remaining) - sum(bounds in remaining for bounds in small_tests)
            write_json(journal, dict(size=total, sha256=event['sha256'], ranges=additional, salvage_cursor=cursor, control_imported=True))
        else:
            unprobed = execute(missing, 'halving')
        attempts.append(dict(attempt=attempt, ranges=outcomes, unprobed=unprobed, fine=fine,
                             seconds=time.monotonic() - pass_started))
        failed_count = sum(row['saved_bytes'] < row['end'] - row['start'] for row in outcomes)
        remaining_span = max((row['end'] - row['start'] - row['saved_bytes'] for row in outcomes), default=0)
        event['range_attempts'] = attempts
        event.update(progress(path, total, sha256=event['sha256']))
        first_bytes = sorted(row['first_byte_seconds'] for row in outcomes if row['first_byte_seconds'] is not None)
        first_byte_p95 = first_bytes[min(len(first_bytes) - 1, int(len(first_bytes) * .95))] if first_bytes else None
        if metrics is not None:
            metrics.range_pass(outcomes,attempts[-1]['seconds'],count_requests=False)
        print(f'RANGE PASS {attempt}/{max_passes}: {event["path"]}; chunk={block}; mode={"probe" if attempt == 0 else "adaptive" if fine else "halving"}; saved={sum(row["saved_bytes"] > 0 for row in outcomes)}/{len(outcomes)}; bytes={event["downloaded_percent"]}; unprobed={unprobed}; seconds={attempts[-1]["seconds"]:.3f}; request_seconds={sum(row["request_seconds"] for row in outcomes):.3f}; io_seconds={sum(row["save_seconds"] for row in outcomes):.3f}; new_bytes={sum(row["saved_bytes"] for row in outcomes)}; first_byte_p95={first_byte_p95}', flush=True)
        if attempt == 0:
            gained = sum(row['saved_bytes'] for row in outcomes)
            if metrics is not None:
                metrics.add(recheck_probe_runs=1, recheck_probe_requests=len(outcomes),
                            recheck_probe_new_bytes=gained, recheck_probe_seconds=attempts[-1]['seconds'],
                            recheck_probe_positive=int(bool(gained)), recheck_probe_empty=int(not gained))
            print(f'RANGE PROBE: {event["path"]}; requests={len(outcomes)}; new_bytes={gained}; escalate={bool(gained)}', flush=True)
            if not gained:
                break
        if fine:
            break
    event.pop('recheck_continue', None)
    return ranges


def reconcile_course(root, course):
    from download import iter_events, write_json
    state = root / '.download'
    history = state / 'course-history.json'
    statuses = json.loads(history.read_text()) if history.exists() else {}
    status = statuses.get(course)
    if not status or status.get('state') != 'finished':
        return
    source = json.loads((state / 'source.json').read_text())['source'].rstrip('/')
    proofs = {row['path']: row for row in iter_events(state / 'events.jsonl')}
    registry = state / 'local-copies.json'
    if registry.exists():
        for row in json.loads(registry.read_text()):
            if row['source'].rstrip('/') == source and row['status'] == 'valid':
                local = storage.saved_local(root, row['local'])
                if file_hashes.proof_current(local,row,root=root,legacy=True):
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
        local = storage.saved_local(root, proof['local']) if proof.get('local') else local_path(root, row['path'])
        if (proof.get('status') == 'valid' and proof.get('sha256') == row['sha256']
                and proof.get('size') == row['size'] and local.exists()
                and file_hashes.proof_current(local,proof,root=root,legacy=True)
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
    FileState(root/'.download', 'recovery-state').set(event['path'], dict(state=state, event=dict(event)))


def prefer_saved_event(root, event, proof):
    if (proof.get('size'), proof.get('sha256')) != (event.get('size'), event.get('sha256')):
        return False
    if proof.get('corrupted_path') == event.get('corrupted_path'):
        return True
    if proof.get('sha256_mismatch'):
        return False
    if event.get('sha256_mismatch'):
        return True
    retained = lambda row: progress(body_path(root, row), row['size'], sha256=row['sha256']).get('downloaded_bytes') or 0
    return retained(proof) >= retained(event)


def range_result_current(root, item, event):
    if not event.get('range_recovered_complete'):
        return True
    from download import target
    local = target(root, item, event['course'])
    if not local.exists() or Path(str(local) + '.aria2').exists():
        return False
    stat = local.stat()
    return stat.st_size == item['size'] and event.get('recovered_stat') == file_hashes.proof_stat(stat)


def finish_recovery(root, event, item=None, retries=2, _redownload=True, tuning=None, stopping=None, download_active=None, background=False, request_budget=None, metrics=None, probe_requests=0, on_request=None):
    event.pop('range_recovered_complete', None)
    if event.get('sha256_rejected_paths'):
        _redownload = False
    if item is None:
        return _finish_recovery(root, event, item, retries, _redownload, tuning, stopping, download_active, background, request_budget, metrics, probe_requests, on_request)
    recovery_state(root, event, 'running')
    try:
        result = _finish_recovery(root, event, item, retries, _redownload, tuning, stopping, download_active, background, request_budget, metrics, probe_requests, on_request)
    except ValueError:
        recovery_state(root, event, 'finished')
        raise
    recovery_state(root, event, 'finished')
    return result


def _finish_recovery(root, event, item=None, retries=2, _redownload=True, tuning=None, stopping=None, download_active=None, background=False, request_budget=None, metrics=None, probe_requests=0, on_request=None):
    from download import record, target, write_json, write_failures, sha256_file, inspect
    path = body_path(root, event)
    if item:
        try:
            if background:
                retry_ranges(root, event, item, retries, tuning, stopping, download_active, request_budget, metrics=metrics,
                             **(dict(on_request=on_request) if on_request else {}),
                             **(dict(probe_requests=probe_requests) if probe_requests else {}))
            else:
                retry_ranges(root, event, item, retries, tuning)
        except InterruptedError:
            raise
        except OSError as error:
            if storage.disk_error(error) or background and not isinstance(error, urllib.error.URLError):
                raise
            event['recovery_error'] = str(error)
            print('RECOVERY ERROR: ' + event['path'] + '; ' + type(error).__name__, flush=True)
        path = body_path(root, event)
    ranges = saved_ranges(path, event['size'], event['sha256'])
    if ranges == [[0, event['size']]] and path.exists():
        before = path.stat()
        archive_stat = file_hashes.proof_stat(before)

        def check_archive(copied=None):
            stat = path.stat() if path.exists() else None
            if stat is not None and archive_stat == file_hashes.proof_stat(stat):
                return
            event.update(sha256_mismatch=True, recovery_error='Archive changed after SHA verification')
            # Discard byte proofs for the changed version; keep its body and control.
            write_json(Path(str(path) + '.ranges.json'), dict(size=event['size'], sha256=event['sha256'],
                       ranges=[], control_imported=True))
            if copied is not None and copied.exists():
                rejected = path.with_name(path.name + '.unverified-copy-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
                copied.rename(rejected)
                event.setdefault('preserved_archive_paths', []).append(str(rejected.relative_to(root)))
            recovery_state(root, event, 'running')
            raise OSError(event['recovery_error'])

        try:
            digest = sha256_file(path, stopping)
        except OSError:
            check_archive()
            raise
        check_archive()
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
                        write_failures(manifest, rows)
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
                                   stopping=stopping, download_active=download_active, background=background,
                                   request_budget=request_budget, metrics=metrics, on_request=on_request)
        if digest == event['sha256'] and item:
            destination = target(root, item, event['course'])
            control = Path(str(destination) + '.aria2')
            if destination.exists() or control.exists():
                stat = destination.stat() if destination.exists() else None
                verified_stat = file_hashes.proof_stat(stat) if stat is not None else None
                status, _ = inspect(root, item, event['course'], path=destination, stopping=stopping)
                if status != 'valid':
                    raise FileExistsError('Refusing to replace an unverified original: ' + str(destination))
                check_archive()
            else:
                try:
                    verified_stat = copy_saved(path, destination, overwrite=False,
                                               expected_sha256=event['sha256'], stopping=stopping)
                except OSError:
                    check_archive(destination)
                    raise
                check_archive(destination)
            proof = record(root / '.download', item, event['course'], 0, 'valid',
                           'Recovered all ranges and SHA-256 verified', verified_stat=verified_stat)
            if proof['status'] != 'valid':
                raise FileExistsError('Recovered original changed after SHA verification: ' + str(destination))
            event['recovered_stat'] = verified_stat
            if not background:
                reconcile_course(root, event['course'])
            event['range_recovered_complete'] = True
            return True
    return promote_partial(root, event)


def recover_corrupted(root, source, retries=2, retry=False, report_repo=None, mark=None, git_push=False, courses=None, force=False, tuning=None, deep=False):
    from download import files, write_json, write_failures, iter_events
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
    valid = {row['path']: row for row in iter_events(root / '.download/events.jsonl', repair=True)}
    registry = root / '.download/local-copies.json'
    if registry.exists():
        for row in json.loads(registry.read_text()):
            if row['source'].rstrip('/') == source.rstrip('/') and row['status'] == 'valid':
                local = storage.saved_local(root, row['local'])
                if file_hashes.proof_current(local,row,root=root):
                    valid[row['path']] = row
    ledger = FileState(root/'.download', 'recovery-state')
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
        local = storage.saved_local(root, known['local']) if known.get('local') else local_path(root, event['path'])
        complete = (known.get('status') == 'valid' and known.get('sha256') == event['sha256']
                    and known.get('size') == event['size'] and local.exists()
                    and file_hashes.proof_current(local,known,root=root)
                    and not Path(str(local) + '.aria2').exists())
        if retry and complete:
            reconcile_course(root, event['course'])
            continue
        local_only = False
        saved = ledger.get(event['path'])
        if saved and not prefer_saved_event(root, event, saved['event']):
            saved = None
        if saved and (saved['event']['size'], saved['event']['sha256']) == (event['size'], event['sha256']):
            event = dict(saved['event'])
            local_only = (event.get('range_recovered_complete', False)
                          and not range_result_current(root, dict(event, file=source), event))
            if retry and not force and saved['state'] == 'finished' and not local_only:
                results.append(event)
                print('RECOVERY SKIPPED: ' + event['path'] + '; attempt already finished', flush=True)
                continue
        elif not saved and retry and not force and (event.get('range_attempts') or event.get('range_attempted')):
            # Previous completed passes predate the per-file ledger.
            ledger.set(event['path'], dict(state='finished', event=dict(event)))
            results.append(event)
            continue
        archived = body_path(root, event)
        original = local_path(root, event['path'])
        # Finish an archive move interrupted between manifest, body and control writes.
        control = Path(str(original) + '.aria2')
        if retry and not local_only and (control.exists() or (original.exists() and not archived.exists())):
            archived.parent.mkdir(parents=True, exist_ok=True)
            for src, out in ((original, archived), (control, Path(str(archived) + '.aria2'))):
                if src.exists() and not out.exists():
                    src.rename(out)
        try:
            item = None
            if local_only:
                if saved_ranges(archived, event['size'], event['sha256']) == [[0, event['size']]]:
                    item = dict(event, file=source)
            elif retry:
                course = event['course']
                if course not in metadata:
                    metadata[course] = {row['path']: row for row in files(source, '/' + course)}
                item = metadata[course].get(event['path'])
                if item is None:
                    event['recovery_error'] = 'File no longer listed in source'
                    ledger.set(event['path'], dict(state='finished', event=dict(event)))
                    raise ValueError('File no longer listed in source')
            finish_recovery(root, event, item, retries, _redownload=not local_only, tuning=tuning)
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
            if storage.disk_error(error):
                raise
            event['recovery_error'] = str(error)
            print('RECOVERY ERROR: ' + event['path'] + '; ' + type(error).__name__, flush=True)
            if isinstance(error, OSError):
                try:
                    finish_recovery(root, event)
                    if retry:
                        recovery_state(root, event, 'finished')
                except (OSError, ValueError, KeyError, subprocess.SubprocessError) as local_error:
                    if storage.disk_error(local_error):
                        raise
                    event['local_recovery_error'] = str(local_error)
        if retry and not event.get('range_recovered_complete'):
            event.update(time=datetime.now().astimezone().isoformat(),
                         status='partial' if (event.get('downloaded_bytes') or 0) > 0 else 'missing',
                         detail='Independent HTTP Range recovery; incomplete original')
            from download import append_events
            append_events(root / '.download/events.jsonl', [event])
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
            write_failures(manifest, rows)
    if report_repo:
        from report import queue_report
        queue_report(root, report_repo, mark, git_push, f'Update {mark} partially available copies and range recovery')
    return results
