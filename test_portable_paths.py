"""Portable saved paths must survive a new disk mount while old records still work."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import download as d
import storage


class PortablePathChecks(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.disk=Path(self.tmp.name)/'old_mount'
        self.archive=self.disk/'YandexDisk/Course'
        (self.archive/'.download').mkdir(parents=True)
        self.file=self.archive/'video.mp4'
        self.file.write_bytes(b'video')
        self.source='https://example.invalid/source'
        self.item=dict(path='/Course/video.mp4',size=5,sha256=hashlib.sha256(b'video').hexdigest())

    def test_valid_proof_is_relative_and_resolves_after_remount(self):
        event=d.record(self.archive/'.download',self.item,'Course',0,'valid','verified',local=self.file)
        written=json.loads((self.archive/'.download/events.jsonl').read_text().splitlines()[-1])
        self.assertEqual(written['local'],'video.mp4')
        self.assertEqual(d.saved_local(self.archive,written['local']),self.file)
        self.assertEqual(d.saved_local(self.archive,str(self.file)),self.file)
        newer=self.disk.with_name('new_mount')
        self.disk.rename(newer)
        actual=newer/'YandexDisk/Course/video.mp4'
        self.assertEqual(d.saved_local(newer/'YandexDisk/Course',written['local']),actual)

    def test_nested_recovery_state_serializes_only_internal_local_paths(self):
        outside=Path(self.tmp.name)/'external.mp4'
        data=dict(local=str(self.file),retained_copies={'one':dict(local=str(self.file)),
                   'two':dict(local=str(outside))},source=self.source)
        destination=self.archive/'.download/nextcloud-downloads/job.json'
        destination.parent.mkdir()
        d.write_json(destination,data)
        stored=json.loads(destination.read_text())
        self.assertEqual(stored['local'],'video.mp4')
        self.assertEqual(stored['retained_copies']['one']['local'],'video.mp4')
        self.assertEqual(stored['retained_copies']['two']['local'],str(outside))

    def test_queue_paths_are_relative_to_queue_and_support_absolute_legacy(self):
        queue=self.disk/'YandexDisk/.download/semester-queue/queue.json'
        queue.parent.mkdir(parents=True)
        catalog=self.archive/'.download/catalog.json'
        catalog.write_text('{}')
        d.write_json(queue,{'archives':[dict(destination=str(self.archive),catalog=str(catalog))],
                            'wait_for':dict(destination=str(self.archive))})
        stored=json.loads(queue.read_text())
        self.assertFalse(Path(stored['archives'][0]['destination']).is_absolute())
        self.assertFalse(Path(stored['archives'][0]['catalog']).is_absolute())
        self.assertEqual(storage.queue_path(queue,stored['archives'][0]['destination']),self.archive)
        self.assertEqual(storage.queue_path(queue,stored['archives'][0]['catalog']),catalog)
        self.assertEqual(storage.queue_path(queue,str(self.archive)),self.archive)
        newer=self.disk.with_name('another_mount')
        self.disk.rename(newer)
        target=newer/'YandexDisk/Course'
        queue_new=newer/'YandexDisk/.download/semester-queue/queue.json'
        self.assertEqual(storage.queue_path(queue_new,stored['archives'][0]['destination']),target)
        self.assertEqual(storage.read_queue(queue_new)['archives'][0]['destination'],str(target))


if __name__=='__main__':
    unittest.main()
