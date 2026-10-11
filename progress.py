"""Read saved HTTP aria2 progress and inventory retained corrupted files."""
import json
from pathlib import Path
import struct


def received_bytes(control, expected):
    # aria2 HTTP control format v1; in-flight blocks are 16 KiB (Piece.h).
    data = control.read_bytes()
    offset = 0
    def read(fmt):
        nonlocal offset
        values = struct.unpack_from(fmt, data, offset)
        offset += struct.calcsize(fmt)
        return values
    def bitmap(length, block):
        nonlocal offset
        count = (length + block - 1) // block
        size, = read('>I')
        if size != (count + 7) // 8 or offset + size > len(data):
            raise ValueError('Invalid aria2 bitmap')
        bits = data[offset:offset + size]
        offset += size
        indices = [i for i in range(count) if bits[i // 8] & (128 >> (i % 8))]
        return sum(min(block, length - i * block) for i in indices), set(indices)
    version, extension, hash_length = read('>HII')
    if (version, extension, hash_length) != (1, 0, 0):
        raise ValueError('Unsupported aria2 HTTP control format')
    piece, total, uploaded = read('>IQQ')
    if not piece or total != expected:
        raise ValueError('aria2 size mismatch')
    received, completed = bitmap(total, piece)
    count, = read('>I')
    seen = set()
    for _ in range(count):
        index, length = read('>II')
        if index in seen or index in completed or index * piece >= total or length != min(piece, total - index * piece):
            raise ValueError('Invalid aria2 in-flight piece')
        seen.add(index)
        partial, _ = bitmap(length, 16 * 1024)
        received += partial
    if offset != len(data) or received > total:
        raise ValueError('Invalid aria2 control length')
    return received


def progress(path, expected, valid=False):
    control = Path(str(path) + '.aria2')
    result = dict(local_size=path.stat().st_size if path.exists() else 0,
                  downloaded_bytes=None, downloaded_percent=None, progress_source='unknown')
    if not path.exists():
        result.update(downloaded_bytes=0, downloaded_percent=0.0, progress_source='missing')
    elif valid:
        result.update(downloaded_bytes=expected, downloaded_percent=100.0, progress_source='sha256')
    elif control.exists():
        try:
            received = received_bytes(control, expected)
            result.update(downloaded_bytes=received,
                          downloaded_percent=round(100 * received / expected, 4) if expected else 100.0,
                          progress_source='aria2-control')
        except (OSError, ValueError, struct.error) as error:
            result['progress_error'] = str(error)
    return result


def corrupted_report(root):
    from download import write_json
    root = root.resolve()
    folder = root / 'corrupted'
    rows = {}
    for manifest in sorted(folder.glob('**/failed.json')):
        events = json.loads(manifest.read_text())
        changed = False
        for event in events:
            relative = event.get('corrupted_path')
            path = root / relative if relative else manifest.parent.joinpath(*Path(event['path']).parts[1:])
            if not path.resolve().is_relative_to(folder):
                raise ValueError('Unsafe corrupted path')
            if 'downloaded_bytes' not in event:
                event.update(progress(path, event['size']))
                changed = True
            # Retain original progress if a body was subsequently moved/removed.
            row = dict(event, retained=path.exists(), corrupted_path=str(path.relative_to(root)))
            rows[row['corrupted_path']] = row
        if changed:
            write_json(manifest, events)
    items = list(rows.values())
    folder.mkdir(exist_ok=True)
    write_json(folder / 'progress.json', items)
    lines = ['Сохранённые неудачные загрузки; процент из aria2, не из размера MP4.', '']
    for event in sorted(items, key=lambda row: -(row.get('downloaded_percent') or 0)):
        percent = event.get('downloaded_percent')
        value = f'{percent:.2f}%' if percent is not None else 'неизвестно'
        lines.extend([event['course'] + ' — ' + Path(event['path']).name,
                      value + '; байты: ' + str(event.get('downloaded_bytes')) + '/' + str(event['size']),
                      event['corrupted_path'] + '; файл сохранён: ' + str(event['retained']), ''])
    (folder / 'Прогресс загрузки.txt').write_text('\n'.join(lines))
    return [event for event in items if event['retained'] and event.get('downloaded_percent') is not None and event['downloaded_percent'] > 30]
