"""Temporary proof-read errors must keep partial recovery resumable."""
import errno
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import download
import progress
import recovery


class OutageProgressChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / '.download').mkdir()
        self.content = b'A' * 1024
        self.item = dict(path='/Course/video.mp4', size=1024,
                         sha256=hashlib.sha256(self.content).hexdigest(),
                         file='https://example.invalid/video')
        self.event = dict(self.item, course='Course',
                          corrupted_path='corrupted/old/Course/video.mp4')
        self.body = self.root / self.event['corrupted_path']
        self.body.parent.mkdir(parents=True)
        self.body.write_bytes(self.content[:600])
        self.journal = Path(str(self.body) + '.ranges.json')
        download.write_json(self.journal, dict(size=1024, sha256=self.item['sha256'],
                                              ranges=[[0, 600]]))
        download.write_json(self.root / '.download/source.json', dict(source='source'))

    def test_journal_disk_error_is_propagated(self):
        with patch.object(Path, 'read_text', side_effect=OSError(errno.EIO, 'Disk interrupted')):
            with self.assertRaises(OSError) as error:
                progress.progress(self.body, 1024, sha256=self.item['sha256'])
        self.assertEqual(error.exception.errno, errno.EIO)

    def test_proof_read_outage_keeps_recovery_running_until_partial_promotion(self):
        with patch.object(recovery, 'retry_ranges', return_value=[[0, 600]]):
            with patch.object(progress, 'saved_ranges', side_effect=OSError(errno.EIO, 'Disk interrupted')):
                with self.assertRaises(OSError):
                    recovery.finish_recovery(self.root, self.event, self.item, background=True)
            ledger = self.root / '.download/recovery-state.json'
            self.assertEqual(json.loads(ledger.read_text())[self.item['path']]['state'], 'running')
            self.assertEqual(self.body.read_bytes(), self.content[:600])
            self.assertTrue(recovery.finish_recovery(self.root, self.event, self.item, background=True))
        self.assertEqual(json.loads(ledger.read_text())[self.item['path']]['state'], 'finished')
        partial = self.root / 'Course/video (partially available).mp4'
        self.assertEqual(partial.read_bytes(), self.content[:600])
        self.assertFalse((self.root / 'Course/video.mp4').exists())

    def test_local_fallback_disk_error_is_propagated(self):
        event = dict(self.event, status='partial', time='2026-10-13T00:00:00+00:00', attempt=1)
        download.write_json(self.root / 'corrupted/failed.json', [event])
        with patch.object(download, 'files', side_effect=OSError('Remote failure')), \
                patch.object(recovery, 'finish_recovery', side_effect=OSError(errno.EIO, 'Disk interrupted')):
            with self.assertRaises(OSError) as error:
                recovery.recover_corrupted(self.root, 'source', retry=True)
        self.assertEqual(error.exception.errno, errno.EIO)
