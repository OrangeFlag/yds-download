"""Archive-local hashes of stable files, shared by verification and uploads."""
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from threading import Lock
import time

import storage

ALGORITHMS = ('sha256', 'sha1', 'md5')
VIDEO = {'.mp4', '.mkv', '.mov', '.webm', '.avi', '.m4v', '.ts', '.3gp'}
HASH_LOCK = Lock()


def _stamp(stat):
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def fingerprint(path):
    return _stamp(Path(path).stat())


def proof_stat(stat):
    return _stamp(stat)[1:]


def proof_current(path, proof, root=None, *, legacy=False):
    """Legacy display marks survive; actual reuse needs ctime or a current SHA cache."""
    storage.check(path)
    try:
        stat = Path(path).stat()
    except FileNotFoundError:
        return False
    if stat.st_size != proof.get('local_size', proof.get('size')):
        return False
    if proof.get('mtime_ns') is None or stat.st_mtime_ns == proof['mtime_ns']:
        if proof.get('ctime_ns') == stat.st_ctime_ns:
            return True
        if legacy and proof.get('ctime_ns') is None:
            return True
    cached = load(path, root=root)
    return bool(cached and cached['sha256'] == proof.get('actual_sha256', proof.get('sha256')))


def _location(path, root):
    path = Path(os.path.abspath(path))
    storage.check(path)
    if root is None:
        for parent in path.parents:
            if parent.name == '.download':
                root = parent.parent
                break
            try:
                (parent / '.download').stat()
            except FileNotFoundError:
                continue
            root = parent
            break
    if root is None:
        return None
    root = Path(os.path.abspath(root))
    storage.check(root)
    root.stat()  # Never recreate a missing archive on the system disk.
    local = storage.portable_local(root, str(path))
    key = hashlib.sha256(local.encode('utf-8')).hexdigest()
    return root / '.download' / 'file-hashes' / key[:2] / (key + '.json'), local


def _valid(hashes):
    return isinstance(hashes, dict) and all(
        isinstance(hashes.get(name), str)
        and re.fullmatch('[0-9a-f]{%d}' % length, hashes[name])
        for name, length in zip(ALGORITHMS, (64, 40, 32)))


def load(path, root=None):
    location = _location(path, root)
    if location is None:
        return None
    cache, local = location
    storage.check(cache)
    try:
        proof = json.loads(cache.read_text(encoding='utf-8'))
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if (not isinstance(proof, dict) or type(proof.get('schema')) is not int or proof['schema'] != 1
            or proof.get('local') != local or not _valid(proof.get('hashes'))):
        return None
    try:
        current = fingerprint(path)
    except FileNotFoundError:
        return None
    return {name: proof['hashes'][name] for name in ALGORITHMS} if proof.get('stat') == current else None


def save(path, hashes, expected_stat, root=None):
    if not _valid(hashes):
        raise ValueError('Missing or invalid file hashes')
    expected = _stamp(expected_stat) if hasattr(expected_stat, 'st_dev') else list(expected_stat)
    location = _location(path, root)
    if location is None:
        return False
    cache, local = location
    storage.check(cache)
    if fingerprint(path) != expected:
        return False
    missing = []
    directory = cache.parent
    while not directory.exists():
        missing.append(directory)
        directory = directory.parent
    for directory in reversed(missing):
        storage.check(directory)
        directory.mkdir(exist_ok=True)
        storage.sync_directory(directory.parent)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=cache.parent,
                                         prefix=cache.name + '.', suffix='.tmp', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(dict(schema=1, local=local, stat=expected,
                           hashes={name: hashes[name] for name in ALGORITHMS}), stream)
            stream.flush()
            os.fsync(stream.fileno())
        storage.check(path)
        storage.check(cache)
        if fingerprint(path) != expected:
            return False
        temporary.replace(cache)
        storage.sync_directory(cache.parent)
        return fingerprint(path) == expected
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def calculate(path, stopping=None, root=None, progress=None, expected_sha256=None):
    """Always read once; only explicit load() calls may reuse saved hashes."""
    path = Path(path)
    storage.check(path)
    while not HASH_LOCK.acquire(timeout=0.25):
        if stopping is not None and stopping.is_set():
            raise InterruptedError('SHA interrupted')
    try:
        if stopping is not None and stopping.is_set():
            raise InterruptedError('SHA interrupted')
        before = fingerprint(path)
        digests = {name: hashlib.new(name) for name in ALGORITHMS}
        amount = 0
        last_progress = time.monotonic()
        with path.open('rb') as stream:
            if _stamp(os.fstat(stream.fileno())) != before:
                raise OSError('File changed during SHA verification')
            while chunk := stream.read(1024 * 1024):
                if stopping is not None and stopping.is_set():
                    raise InterruptedError('SHA interrupted')
                storage.check(path)
                for digest in digests.values():
                    digest.update(chunk)
                amount += len(chunk)
                now = time.monotonic()
                if progress is not None and now - last_progress >= 30:
                    progress(amount, before[2])
                    last_progress = now
            os.fsync(stream.fileno())
            if _stamp(os.fstat(stream.fileno())) != before:
                raise OSError('File changed during SHA verification')
        storage.check(path)
        if stopping is not None and stopping.is_set():
            raise InterruptedError('SHA interrupted')
        if amount != before[2] or fingerprint(path) != before:
            raise OSError('File changed during SHA verification')
        hashes = {name: digest.hexdigest() for name, digest in digests.items()}
        if expected_sha256 is not None and hashes['sha256'] != expected_sha256:
            raise OSError('File changed during publication: SHA-256 mismatch')
        if not save(path, hashes, before, root=root) and fingerprint(path) != before:
            raise OSError('File changed during SHA verification')
        return hashes
    finally:
        HASH_LOCK.release()
