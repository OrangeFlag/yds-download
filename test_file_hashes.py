"""Shared proofs save disk reads without weakening explicit SHA verification."""
import errno
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from threading import Event
import unittest
from unittest.mock import Mock, patch

import download
import file_hashes as hashes


class FileHashChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / 'archive'
        (self.root / '.download').mkdir(parents=True)
        self.video = self.root / 'Course' / 'video.mp4'
        self.video.parent.mkdir()
        self.body = b'lecture content\x00' * 100000
        self.video.write_bytes(self.body)
        self.expected = {name: hashlib.new(name, self.body).hexdigest() for name in hashes.ALGORITHMS}

    def proof(self):
        return next((self.root / '.download/file-hashes').glob('*/*.json'))

    def test_original_video_verification_calculates_three_hashes_in_one_read_pass(self):
        original = Path.open
        reads = []
        def opening(path, *args, **kwargs):
            stream = original(path, *args, **kwargs)
            if path == self.video and args == ('rb',):
                stream.read = Mock(wraps=stream.read)
                reads.append(stream.read)
            return stream
        with patch.object(Path, 'open', opening):
            self.assertEqual(download.sha256_file(self.video), self.expected['sha256'])
        self.assertEqual(len(reads), 1)
        self.assertEqual(sum(len(call.args) and call.args[0] for call in reads[0].call_args_list),
                         3 * 1024 * 1024)
        self.assertEqual(reads[0].call_count, 3)  # Two payload reads and one EOF.
        self.assertEqual(hashes.load(self.video), self.expected)
        self.assertIs(download.HASH_LOCK, hashes.HASH_LOCK)
        proof = json.loads(self.proof().read_text())
        self.assertEqual(proof['local'], 'Course/video.mp4')
        self.assertEqual(proof['stat'], hashes.fingerprint(self.video))

    def test_sha_large_file_does_not_probe_mount_for_every_megabyte(self):
        self.video.write_bytes(b'v' * (12 * 1024 * 1024))
        original = hashes.storage.check
        with patch.object(hashes.storage, 'check', wraps=original) as check:
            result = hashes.calculate(self.video, root=self.root)
        self.assertEqual(result['sha256'], hashlib.sha256(self.video.read_bytes()).hexdigest())
        checks = sum(call.args[0] == self.video for call in check.call_args_list)
        self.assertLessEqual(checks, 5, 'Repeated statvfs and mount checks cause needless metadata I/O')
        self.assertEqual(hashes.load(self.video, root=self.root), result)

    def test_fresh_process_load_uses_persistent_metadata_without_reading_video(self):
        hashes.calculate(self.video)
        script = '''import json, sys
from pathlib import Path
from unittest.mock import patch
import file_hashes
video = Path(sys.argv[1])
original = Path.open
def opening(path, *args, **kwargs):
    if path == video:
        raise AssertionError('Cache reuse read video contents')
    return original(path, *args, **kwargs)
with patch.object(Path, 'open', opening):
    print(json.dumps(file_hashes.load(video)))
'''
        output = subprocess.run([sys.executable, '-c', script, str(self.video)],
                                cwd=Path(__file__).parent, check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(output.stdout), self.expected)

    def test_explicit_calculation_always_reads_despite_valid_cache(self):
        hashes.calculate(self.video)
        original = Path.open
        def opening(path, *args, **kwargs):
            if path == self.video and args == ('rb',):
                raise OSError('Actual verification attempted a video read')
            return original(path, *args, **kwargs)
        with patch.object(Path, 'open', opening), self.assertRaisesRegex(OSError, 'video read'):
            download.sha256_file(self.video)

    def test_same_size_and_restored_mtime_edit_invalidates_ctime(self):
        hashes.calculate(self.video)
        before = self.video.stat()
        with self.video.open('r+b') as stream:
            stream.write(b'CHANGED')
        os.utime(self.video, ns=(before.st_atime_ns, before.st_mtime_ns))
        after = self.video.stat()
        self.assertEqual((after.st_size, after.st_mtime_ns), (before.st_size, before.st_mtime_ns))
        self.assertNotEqual(after.st_ctime_ns, before.st_ctime_ns)
        self.assertIsNone(hashes.load(self.video))
        self.assertFalse(hashes.save(self.video, self.expected, before))

    def test_malformed_proofs_are_cache_misses(self):
        hashes.calculate(self.video)
        cache = self.proof()
        valid = json.loads(cache.read_text())
        for body in ('{', '[]', json.dumps({**valid, 'schema': 2}), json.dumps({**valid, 'schema': True}),
                     json.dumps({**valid, 'local': 'elsewhere.mp4'}),
                     json.dumps({**valid, 'stat': valid['stat'][:-1]}),
                     json.dumps({**valid, 'hashes': {'sha256': self.expected['sha256']}}),
                     json.dumps({**valid, 'hashes': {**self.expected, 'md5': 'g' * 32}})):
            with self.subTest(body=body[:60]):
                cache.write_text(body)
                self.assertIsNone(hashes.load(self.video))
        cache.write_bytes(b'\xff')
        self.assertIsNone(hashes.load(self.video))

    def test_interruption_during_real_read_does_not_publish_proof(self):
        stopping = Event()
        original = Path.open
        def opening(path, *args, **kwargs):
            stream = original(path, *args, **kwargs)
            if path == self.video and args == ('rb',):
                read = stream.read
                def interrupt(amount):
                    data = read(amount)
                    stopping.set()
                    return data
                stream.read = interrupt
            return stream
        with patch.object(Path, 'open', opening), self.assertRaises(InterruptedError):
            hashes.calculate(self.video, stopping=stopping)
        self.assertIsNone(hashes.load(self.video))
        self.assertFalse(hashes.HASH_LOCK.locked())

    def test_mutation_during_read_does_not_publish_proof(self):
        original = os.fsync
        def mutate(descriptor):
            with self.video.open('r+b') as stream:
                stream.write(b'MUTATED')
            original(descriptor)
        with patch.object(hashes.os, 'fsync', side_effect=mutate), self.assertRaisesRegex(OSError, 'changed'):
            hashes.calculate(self.video)
        self.assertIsNone(hashes.load(self.video))

    def test_atomic_cache_publication_does_not_hide_disk_full(self):
        hashes.calculate(self.video)
        previous = self.proof().read_bytes()
        original = os.fsync
        def fail_write(descriptor):
            if os.readlink('/proc/self/fd/' + str(descriptor)).endswith('.tmp'):
                raise OSError(errno.ENOSPC, 'Disk full')
            original(descriptor)
        with patch.object(hashes.os, 'fsync', side_effect=fail_write), self.assertRaises(OSError) as raised:
            hashes.calculate(self.video)
        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        self.assertEqual(self.proof().read_bytes(), previous)
        self.assertEqual(list(self.proof().parent.glob('*.tmp')), [])
        self.assertEqual(hashes.load(self.video), self.expected)

    def test_cache_read_io_error_and_missing_mount_propagate(self):
        hashes.calculate(self.video)
        with patch.object(Path, 'read_text', side_effect=OSError(errno.EIO, 'Disk offline')):
            with self.assertRaises(OSError):
                hashes.load(self.video)
        with patch.object(hashes.storage, 'check', side_effect=OSError(errno.EIO, 'Disk offline')):
            with self.assertRaises(OSError):
                hashes.calculate(self.video)

    def test_external_file_uses_explicit_archive_and_internal_download_uses_parent(self):
        external = self.root.parent / 'CamRip.mp4'
        external.write_bytes(self.body)
        self.assertEqual(hashes.calculate(external, root=self.root), self.expected)
        self.assertEqual(hashes.load(external, root=self.root), self.expected)
        self.assertIsNone(hashes.load(external))
        self.assertEqual(json.loads(self.proof().read_text())['local'], str(external))
        internal = self.root / '.download' / 'working.mp4'
        internal.write_bytes(self.body)
        self.assertEqual(hashes.calculate(internal), self.expected)
        self.assertEqual(hashes.load(internal, root=self.root), self.expected)
        self.assertFalse((self.root / '.download/.download').exists())

    def test_nonvideo_sha_stays_sha_only_without_cache(self):
        photo = self.video.with_suffix('.jpg')
        photo.write_bytes(self.body)
        with patch.object(hashes, 'calculate', side_effect=AssertionError('Photo requested three hashes')):
            self.assertEqual(download.sha256_file(photo), self.expected['sha256'])
        self.assertFalse((self.root / '.download/file-hashes').exists())

    def test_unowned_file_hashes_without_creating_global_cache(self):
        external = self.root.parent / 'external.mp4'
        external.write_bytes(self.body)
        self.assertEqual(hashes.calculate(external), self.expected)
        self.assertFalse((self.root.parent / '.download').exists())
        self.assertIsNone(hashes.load(external))


if __name__ == '__main__':
    unittest.main()
