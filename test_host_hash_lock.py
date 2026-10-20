"""Concurrent downloader processes share one interruptible payload-read lock."""
import fcntl
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class HostHashLockChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.lock = self.root / 'sha.lock'
        self.video = self.root / 'video.mp4'
        self.video.write_bytes(b'lecture')
        self.env = dict(os.environ, YDS_HASH_LOCK=str(self.lock))

    def child(self, script, *args):
        child = subprocess.Popen([sys.executable, '-c', script, str(self.video), *map(str, args)],
                                 cwd=Path(__file__).parent, env=self.env,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(child.stdout.close)
        self.addCleanup(child.stderr.close)
        self.addCleanup(lambda: child.poll() is None and (child.kill(), child.wait(timeout=3)))
        return child

    def test_both_hash_paths_wait_for_the_other_process_before_reading(self):
        script = '''import hashlib, sys
from pathlib import Path
from unittest.mock import patch
import download, file_hashes
video, entered, reading = map(Path, sys.argv[1:])
original = Path.open
def opening(path, *args, **kwargs):
    if path == video and args == ('rb',):
        reading.touch()
    return original(path, *args, **kwargs)
entered.touch()
with patch.object(Path, 'open', opening):
    actual = download.sha256_file(video)
assert actual == hashlib.sha256(b'lecture').hexdigest()
print('verified', flush=True)
'''
        for suffix in ('.mp4', '.txt'):
            with self.subTest(suffix=suffix):
                self.video = self.video.with_suffix(suffix)
                self.video.write_bytes(b'lecture')
                entered, reading = [self.root / (name + suffix) for name in ('entered', 'reading')]
                # The child announces entry with a marker; stdout is reserved for completion.
                with self.lock.open('a+b') as owner:
                    fcntl.flock(owner, fcntl.LOCK_EX)
                    child = self.child(script, entered, reading)
                    import time
                    deadline = time.monotonic() + 3
                    while not entered.exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue(entered.exists(), 'Hasher did not start')
                    time.sleep(.35)
                    self.assertFalse(reading.exists(), 'Payload was read while another process held SHA lock')
                stdout, stderr = child.communicate(timeout=3)
                self.assertEqual(child.returncode, 0, stderr)
                self.assertEqual(stdout.strip(), 'verified')
                self.assertTrue(reading.exists())
        self.assertEqual(self.lock.stat().st_size, 0)

    def test_interrupted_wait_releases_thread_lock_and_can_retry(self):
        script = '''import sys
from pathlib import Path
from threading import Event, Timer
import download, file_hashes
stopping = Event()
timer = Timer(.2, stopping.set)
timer.start()
try:
    download.sha256_file(Path(sys.argv[1]), stopping)
except InterruptedError:
    assert not file_hashes.HASH_LOCK.locked()
    print('interrupted', flush=True)
else:
    raise AssertionError('Hash ignored the locked host and cancellation')
finally:
    timer.cancel()
'''
        with self.lock.open('a+b') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX)
            child = self.child(script)
            stdout, stderr = child.communicate(timeout=3)
            self.assertEqual(child.returncode, 0, stderr)
            self.assertEqual(stdout.strip(), 'interrupted')
        retry = self.child("import sys; from pathlib import Path; import download; print(download.sha256_file(Path(sys.argv[1])))")
        stdout, stderr = retry.communicate(timeout=3)
        self.assertEqual(retry.returncode, 0, stderr)
        self.assertEqual(stdout.strip(), hashlib.sha256(b'lecture').hexdigest())

    def test_interrupted_payload_releases_host_and_thread_locks(self):
        from threading import Event
        from unittest.mock import patch
        import download
        import file_hashes
        original = Path.open
        stopping = Event()
        def opening(path, *args, **kwargs):
            stream = original(path, *args, **kwargs)
            if path == self.video and args == ('rb',):
                read = stream.read
                def interrupted_read(*args):
                    chunk = read(*args)
                    stopping.set()
                    return chunk
                stream.read = interrupted_read
            return stream
        with patch.dict(os.environ, self.env), patch.object(Path, 'open', opening):
            with self.assertRaises(InterruptedError):
                download.sha256_file(self.video, stopping)
        self.assertFalse(file_hashes.HASH_LOCK.locked())
        with self.lock.open('a+b') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
