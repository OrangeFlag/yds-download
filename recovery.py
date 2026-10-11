"""Retry independent ranges and retain partial videos with usable timeline coverage."""
from bisect import bisect_right
from datetime import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from threading import Lock
import urllib.request
from progress import merge_ranges, saved_ranges, progress

SUFFIX = ' (partially available)'


def timeline(path, event):
    ranges = saved_ranges(path, event['size'], event['sha256'])
    if not ranges:
        raise ValueError('No confirmed saved blocks')
    starts = [start for start, end in ranges]
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(['ffprobe', '-v', 'quiet', '-show_packets', '-show_streams',
                                 '-show_format', '-show_entries',
                                 'format=duration:stream=index,codec_type:packet=stream_index,pts_time,duration_time,pos,size,flags',
                                 '-of', 'json', str(path)], stdout=output, stderr=subprocess.DEVNULL, timeout=60)
        if result.returncode:
            raise ValueError('ffprobe cannot read the video container')
        output.seek(0)
        data = json.load(output)
    duration = float(data.get('format', {}).get('duration', 0))
    if not 0 < duration < float('inf'):
        raise ValueError('Original duration unavailable')
    streams = data.get('streams', [])
    video = next((stream['index'] for stream in streams if stream['codec_type'] == 'video'), None)
    audio = next((stream['index'] for stream in streams if stream['codec_type'] == 'audio'), None)
    if video is None:
        raise ValueError('No video stream')
    intervals = {video: []}
    if audio is not None:
        intervals[audio] = []
    decoding = False
    logical_size = path.stat().st_size
    for packet in data.get('packets', []):
        stream = packet['stream_index']
        if stream not in intervals or not all(key in packet for key in ('pos', 'size', 'pts_time', 'duration_time')):
            continue
        position, size = int(packet['pos']), int(packet['size'])
        index = bisect_right(starts, position) - 1
        flags = packet.get('flags', '')
        intact = index >= 0 and position + size <= min(ranges[index][1], logical_size) and 'C' not in flags
        if stream == video:
            if not intact:
                decoding = False
            elif 'K' in flags:
                decoding = True
            intact = intact and decoding
        if intact:
            start = max(0.0, float(packet['pts_time']))
            end = min(duration, float(packet['pts_time']) + float(packet['duration_time']))
            if end > start:
                intervals[stream].append([start, end])
    available = merge_ranges(intervals[video])
    if audio is not None:
        sound = merge_ranges(intervals[audio])
        shared = []
        i = j = 0
        while i < len(available) and j < len(sound):
            start = max(available[i][0], sound[j][0])
            end = min(available[i][1], sound[j][1])
            if end > start:
                shared.append([start, end])
            if available[i][1] < sound[j][1]:
                i += 1
            else:
                j += 1
        available = merge_ranges(shared)
    seconds = sum(end - start for start, end in available)
    return dict(duration_seconds=duration, available_seconds=seconds,
                available_time_percent=100 * seconds / duration, available_intervals=available,
                time_method='saved video packets from keyframes intersected with saved audio packets')


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
    path = body_path(root, event)
    if not path.exists() or path.stat().st_size == 0:
        return False
    event.update(progress(path, event['size'], sha256=event['sha256']))
    event.update(timeline(path, event))
    if event['available_time_percent'] <= 50:
        return False
    original_path = Path(event['path'])
    if not original_path.is_absolute() or '..' in original_path.parts or original_path.parts[1] in ('.download', 'corrupted'):
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
        if old['available_seconds'] >= event['available_seconds']:
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
    print(f'PARTIALLY AVAILABLE: {event["path"]}; bytes={event["downloaded_percent"]:.2f}%; time={event["available_time_percent"]:.2f}%', flush=True)
    return True


def retry_ranges(root, event, item, retries):
    from download import OPENER, write_json
    if item['size'] != event['size'] or item['sha256'] != event['sha256']:
        raise ValueError('Source file changed; refuse to combine ranges')
    path = body_path(root, event)
    path.parent.mkdir(parents=True, exist_ok=True)
    ranges = saved_ranges(path, event['size'], event['sha256'])
    journal = Path(str(path) + '.ranges.json')
    additional = json.loads(journal.read_text())['ranges'] if journal.exists() else []
    total = event['size']
    missing_bytes = total - sum(end - start for start, end in ranges)
    if shutil.disk_usage(root).free < missing_bytes + 1024 ** 3:
        raise RuntimeError('Insufficient disk space for range recovery')
    rate_lock = Lock()
    next_request = 0.0
    def fetch(bounds):
        nonlocal next_request
        start, end = bounds
        with rate_lock:
            time.sleep(max(0, next_request - time.monotonic()))
            next_request = time.monotonic() + 1 / 6
        request = urllib.request.Request(item['file'], headers={'Range': f'bytes={start}-{end - 1}', 'Accept-Encoding': 'identity'})
        try:
            with OPENER.open(request, timeout=30) as response:
                expected = f'bytes {start}-{end - 1}/{total}'
                if response.status != 206 or response.headers.get('Content-Range') != expected:
                    return bounds, None, 'Range not honoured'
                try:
                    data = response.read(end - start)
                except http.client.IncompleteRead as error:
                    data = error.partial
                if len(data) > end - start:
                    return bounds, None, 'Oversized response'
                return bounds, data or None, None if len(data) == end - start else 'Incomplete response'
        except (OSError, http.client.HTTPException) as error:
            return bounds, None, 'HTTP ' + str(error.code) if hasattr(error, 'code') else type(error).__name__
    attempts = []
    for attempt in range(1, retries + 2):
        # Backend block geometry is unpublished; refine failed intervals by observed responses.
        block = max(64 * 1024, 16 * 1024 ** 2 // (4 ** min(attempt - 1, 4)))
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
        if attempt > 1:
            time.sleep(10)
        outcomes = []
        # Consume completed requests immediately so Ctrl+C cannot lose later successful chunks.
        pool = ThreadPoolExecutor(max_workers=8)
        try:
            pending = iter(missing)
            futures = {pool.submit(fetch, bounds) for bounds in missing[:8]}
            for _ in missing[:8]:
                next(pending)
            while futures:
                completed, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in completed:
                    futures.remove(future)
                    bounds, data, error = future.result()
                    if data is not None:
                        saved = [bounds[0], bounds[0] + len(data)]
                        with path.open('r+b' if path.exists() else 'w+b') as stream:
                            stream.seek(saved[0]); stream.write(data); stream.flush(); os.fsync(stream.fileno())
                        additional = merge_ranges(additional + [saved])
                        write_json(journal, dict(size=total, sha256=event['sha256'], ranges=additional))
                        ranges = merge_ranges(ranges + [saved])
                    outcomes.append(dict(start=bounds[0], end=bounds[1], saved_bytes=len(data) if data else 0, error=error))
                    bounds = next(pending, None)
                    if bounds is not None:
                        futures.add(pool.submit(fetch, bounds))
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
        attempts.append(dict(attempt=attempt, ranges=outcomes))
        event['range_attempts'] = attempts
        event.update(progress(path, total, sha256=event['sha256']))
        print(f'RANGE PASS {attempt}/{retries + 1}: {event["path"]}; chunk={block}; saved={sum(row["saved_bytes"] > 0 for row in outcomes)}/{len(outcomes)}; bytes={event["downloaded_percent"]}', flush=True)
    return ranges


def finish_recovery(root, event, item=None, retries=2):
    from download import record, target, write_json
    path = body_path(root, event)
    if item:
        retry_ranges(root, event, item, retries)
    ranges = saved_ranges(path, event['size'], event['sha256'])
    if ranges == [[0, event['size']]] and path.exists():
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if digest == event['sha256'] and item:
            destination = target(root, item, event['course'])
            copy_saved(path, destination)
            Path(str(destination) + '.aria2').unlink(missing_ok=True)
            record(root / '.download', item, event['course'], 0, 'valid', 'Recovered all ranges and SHA-256 verified')
            history = root / '.download/course-history.json'
            if history.exists():
                statuses = json.loads(history.read_text())
                status = statuses.get(event['course'])
                if status:
                    status['verified'] = min(status['total'], status['verified'] + 1)
                    status['failed'] = max(0, status['failed'] - 1)
                    status['complete'] = status['failed'] == 0 and status['verified'] == status['total']
                    status['time'] = datetime.now().astimezone().isoformat()
                    write_json(history, statuses)
                    for old in (root / '.download').glob('*-status.json'):
                        if json.loads(old.read_text()).get('course') == event['course']:
                            write_json(old, status)
            event['range_recovered_complete'] = True
            return True
    return promote_partial(root, event)


def recover_corrupted(root, source, retries=2, retry=False, report_repo=None, mark=None, git_push=False):
    from download import files, write_json
    root = root.resolve()
    candidates = {}
    for manifest in (root / 'corrupted').glob('**/failed.json'):
        for row in json.loads(manifest.read_text()):
            if 'corrupted_path' not in row:
                row = dict(row, corrupted_path=str(manifest.parent.joinpath(*Path(row['path']).parts[1:]).relative_to(root)))
            path = body_path(root, row)
            amount = progress(path, row['size'], sha256=row['sha256']).get('downloaded_bytes') or 0
            previous = candidates.get(row['path'])
            if previous is None or amount > previous[0] or (amount == previous[0] and row['time'] > previous[1]['time']):
                candidates[row['path']] = (amount, row)
    valid = {}
    for line in (root / '.download/events.jsonl').read_text().splitlines():
        if line.strip():
            row = json.loads(line); valid[row['path']] = row
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
        complete = known.get('status') == 'valid' and local.exists() and local.stat().st_size == known['size'] and not Path(str(local) + '.aria2').exists()
        if retry and complete:
            continue
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
