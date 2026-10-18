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


    def test_external_catalog_stays_absolute_across_different_mount_depths(self):
        external=Path(self.tmp.name)/'config/catalog.json'
        external.parent.mkdir()
        external.write_text('{}')
        queue=self.disk/'YandexDisk/.download/semester-queue/queue.json'
        queue.parent.mkdir(parents=True)
        d.write_json(queue,{'archives':[dict(destination=str(self.archive), catalog=str(external))]})
        stored=json.loads(queue.read_text())
        self.assertEqual(stored['archives'][0]['catalog'],str(external))
        new_mount=Path(self.tmp.name)/'nested/deeper/mount'
        new_mount.parent.mkdir(parents=True)
        self.disk.rename(new_mount)
        moved=new_mount/'YandexDisk/.download/semester-queue/queue.json'
        self.assertEqual(storage.read_queue(moved)['archives'][0]['catalog'],str(external))

    def test_queue_outside_metadata_preserves_sibling_archives_and_external_catalog(self):
        queue=self.disk/'queues/queue.json'
        queue.parent.mkdir()
        external=Path(self.tmp.name)/'config/catalog.json'
        external.parent.mkdir()
        external.write_text('{}')
        d.write_json(queue,{'archives':[dict(destination=str(self.archive),catalog=str(external))],
                            'wait_for':dict(destination=str(self.archive))})
        stored=json.loads(queue.read_text())
        self.assertFalse(Path(stored['archives'][0]['destination']).is_absolute())
        self.assertFalse(Path(stored['wait_for']['destination']).is_absolute())
        self.assertEqual(stored['archives'][0]['catalog'],str(external))
        new_mount=Path(self.tmp.name)/'nested/deeper/mount'
        new_mount.parent.mkdir(parents=True)
        self.disk.rename(new_mount)
        moved=storage.read_queue(new_mount/'queues/queue.json')
        expected=str(new_mount/'YandexDisk/Course')
        self.assertEqual(moved['archives'][0]['destination'],expected)
        self.assertEqual(moved['wait_for']['destination'],expected)
        self.assertEqual(moved['archives'][0]['catalog'],str(external))

    def test_outside_queue_relocates_internal_catalog_for_absolute_and_relative_destinations(self):
        queue=self.disk/'queues/queue.json'
        queue.parent.mkdir()
        catalog=self.archive/'.download/catalog.json'
        catalog.write_text('{}')
        d.write_json(queue,{'archives':[
            dict(destination=destination,catalog=str(catalog))
            for destination in (str(self.archive),'../YandexDisk/Course')]})
        stored=json.loads(queue.read_text())
        self.assertTrue(all(not Path(row['catalog']).is_absolute() for row in stored['archives']))
        new_mount=Path(self.tmp.name)/'nested/deeper/mount'
        new_mount.parent.mkdir(parents=True)
        self.disk.rename(new_mount)
        moved=storage.read_queue(new_mount/'queues/queue.json')
        for row in moved['archives']:
            self.assertEqual(row['destination'],str(new_mount/'YandexDisk/Course'))
            self.assertEqual(row['catalog'],str(new_mount/'YandexDisk/Course/.download/catalog.json'))

if __name__=='__main__':
    unittest.main()
