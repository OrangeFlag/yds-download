"""SHA proofs must describe the file version that was actually hashed."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import download
import recovery


class VerifiedStatTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root / '.download'
        self.state.mkdir()
        self.local = self.root / 'Course/video.mp4'
        self.local.parent.mkdir()
        self.local.write_bytes(b'good')
        self.item = dict(path='/Course/video.mp4', size=4,
                         sha256=hashlib.sha256(b'good').hexdigest(), file='https://example.test/file')

    def test_deep_rejects_change_after_hash(self):
        original = download.sha256_file
        def mutate(path, stopping=None):
            digest = original(path, stopping)
            path.write_bytes(b'evil')
            return digest
        with patch.object(download, 'sha256_file', side_effect=mutate):
            proof = download.inspect_copies(self.root, self.item, 'Course', {}, 'https://example.test/source')
        self.assertNotEqual(proof['status'], 'valid')
        self.assertNotIn('mtime_ns', proof)

    def test_record_rejects_change_between_inspect_and_record(self):
        stat = self.local.stat()
        stamp = [stat.st_ino, stat.st_size, stat.st_mtime_ns]
        self.local.write_bytes(b'evil')
        proof = download.record(self.state, self.item, 'Course', 0, 'valid', 'checked', verified_stat=stamp)
        self.assertNotEqual(proof['status'], 'valid')
        self.assertNotIn('mtime_ns', proof)

    def test_recovery_rejects_archive_change_after_hash(self):
        self.local.unlink()
        archive = self.root / 'corrupted/test/Course/video.mp4'
        archive.parent.mkdir(parents=True)
        archive.write_bytes(b'good')
        Path(str(archive) + '.ranges.json').write_text(json.dumps(dict(size=4, sha256=self.item['sha256'], ranges=[[0, 4]])))
        event = dict(self.item, course='Course', attempt=1, status='partial', detail='test',
                     time='2026-10-13', corrupted_path=str(archive.relative_to(self.root)))
        original = download.sha256_file
        def mutate(path, stopping=None):
            digest = original(path, stopping)
            path.write_bytes(b'evil')
            return digest
        with patch.object(recovery, 'retry_ranges'), patch.object(download, 'sha256_file', side_effect=mutate):
            with self.assertRaisesRegex(OSError, 'changed'):
                recovery.finish_recovery(self.root, event, self.item, retries=0, background=True)
        self.assertFalse(event.get('range_recovered_complete'))
        self.assertFalse(self.local.exists())
        self.assertFalse(any(row['status'] == 'valid' for row in download.read_events(self.state / 'events.jsonl')))

    def test_recovery_rejects_archive_change_during_copy(self):
        self.local.unlink()
        archive = self.root / 'corrupted/test/Course/video.mp4'
        archive.parent.mkdir(parents=True)
        archive.write_bytes(b'good')
        Path(str(archive) + '.ranges.json').write_text(json.dumps(dict(size=4, sha256=self.item['sha256'], ranges=[[0, 4]])))
        event = dict(self.item, course='Course', attempt=1, status='partial', detail='test',
                     time='2026-10-13', corrupted_path=str(archive.relative_to(self.root)))
        original = recovery.copy_saved
        def mutate(source, destination, **kwargs):
            source.write_bytes(b'evil')
            return original(source, destination, **kwargs)
        with patch.object(recovery, 'retry_ranges'), patch.object(recovery, 'copy_saved', side_effect=mutate):
            with self.assertRaisesRegex(OSError, 'changed'):
                recovery.finish_recovery(self.root, event, self.item, retries=0, background=True)
        self.assertFalse(event.get('range_recovered_complete'))
        self.assertFalse(self.local.exists())
        self.assertFalse(any(row['status'] == 'valid' for row in download.read_events(self.state / 'events.jsonl')))

    def test_full_copy_preserves_destination_created_during_copy(self):
        source = self.root / 'source.mp4'
        source.write_bytes(b'good')
        self.local.unlink()
        original = recovery.subprocess.run
        def create_destination(*args, **kwargs):
            result = original(*args, **kwargs)
            self.local.write_bytes(b'keep')
            return result
        with patch.object(recovery.subprocess, 'run', side_effect=create_destination):
            with self.assertRaises(FileExistsError):
                recovery.copy_saved(source, self.local, overwrite=False)
        self.assertEqual(self.local.read_bytes(), b'keep')

    def test_partial_promotion_preserves_destination_created_during_copy(self):
        download.write_json(self.state / 'source.json', dict(source='https://example.test/source'))
        archive = self.root / 'corrupted/test/Course/video.mp4'
        archive.parent.mkdir(parents=True)
        archive.write_bytes(b'goo')
        download.write_json(Path(str(archive) + '.ranges.json'),
                            dict(size=4, sha256=self.item['sha256'], ranges=[[0, 3]]))
        event = dict(self.item, course='Course', status='partial',
                     corrupted_path=str(archive.relative_to(self.root)))
        destination = self.local.with_name('video' + recovery.SUFFIX + '.mp4')
        original = recovery.subprocess.run
        def create_destination(*args, **kwargs):
            result = original(*args, **kwargs)
            destination.write_bytes(b'keep')
            return result
        with patch.object(recovery.subprocess, 'run', side_effect=create_destination):
            with self.assertRaises(FileExistsError):
                recovery.promote_partial(self.root, event)
        self.assertEqual(destination.read_bytes(), b'keep')
