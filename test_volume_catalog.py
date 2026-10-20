"""Sequential-disk catalog: offline volumes remain known without mounting them."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import download as d
import storage
import volume_catalog as catalog


OLD = '088d300e-87f5-4293-999d-80726a0b2250'
NEW = '4b45c6a0-122e-4dc1-9175-04fc6c765330'


class VolumeCatalogChecks(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.host = self.root / 'host'
        self.old = self.root / 'old'
        self.new = self.root / 'new'
        self.old.mkdir()
        self.new.mkdir()
        self.online = {OLD: self.old}
        def select(uuid, hint):
            path = self.online.get(uuid)
            return path if path is not None else None
        self.enterContext(patch.object(storage, 'select_mount', side_effect=select))
        self.enterContext(patch.object(storage, 'probe', return_value=True))
        self.enterContext(patch.object(storage, 'sync_filesystem'))

    def archive(self, mount, title, name='Lecture.mp4', body=b'lecture'):
        root = mount / title
        state = root / '.download'
        state.mkdir(parents=True)
        source = 'https://disk.yandex.ru/d/' + title
        item = dict(path='/Course/' + name, size=len(body),
                    sha256=hashlib.sha256(body).hexdigest())
        path = root / 'Course' / name
        path.parent.mkdir()
        path.write_bytes(body)
        d.write_json(state / 'source.json', dict(source=source, title=title))
        d.write_json(state / '01-manifest.json', [item])
        stat = path.stat()
        d.append_events(state / 'events.jsonl',
                        [dict(item, status='valid', mtime_ns=stat.st_mtime_ns,
                              ctime_ns=stat.st_ctime_ns, local=str(path))])
        return root, item, path

    def test_switch_only_one_connected_disk_keeps_offline_files_in_host_and_new_mirror(self):
        archive, item, local = self.archive(self.old, 'Archive 1')
        with patch.object(d, 'sha256_file', side_effect=AssertionError('Rehashed entire HDD')):
            catalog.snapshot(self.old, OLD, self.host, archives=[archive], label='old')
        saved = catalog.load(self.host)
        first = saved['volumes'][OLD]['archives']['Archive 1']['files'][item['path']]
        self.assertEqual(first['sha256'], item['sha256'])
        self.assertEqual(first['local'], 'Archive 1/Course/Lecture.mp4')
        self.assertEqual(first['status'], 'verified')
        self.assertEqual(catalog.load(self.host), catalog.load(self.old / '.yds-download'))
        # Never mount old and new disks at the same time.
        self.online = {NEW: self.new}
        catalog.synchronize(self.new, NEW, self.host, label='new')
        self.assertIn(OLD, catalog.load(self.new / '.yds-download')['volumes'])
        second_archive, second_item, _ = self.archive(self.new, 'Archive 2')
        catalog.snapshot(self.new, NEW, self.host, archives=[second_archive])
        self.assertEqual(set(catalog.load(self.host)['volumes']), {OLD, NEW})
        self.assertEqual(catalog.load(self.host), catalog.load(self.new / '.yds-download'))
        self.online = {OLD: self.old}
        catalog.synchronize(self.old, OLD, self.host)
        self.assertEqual(catalog.load(self.host), catalog.load(self.old / '.yds-download'))
        self.assertIn(NEW, catalog.load(self.old / '.yds-download')['volumes'])

    def test_edited_file_is_listed_as_expected_but_never_stays_verified(self):
        archive, item, path = self.archive(self.old, 'Archive 1')
        catalog.snapshot(self.old, OLD, self.host, archives=[archive])
        path.write_bytes(b'changed')
        catalog.snapshot(self.old, OLD, self.host, archives=[archive])
        proof = catalog.load(self.host)['volumes'][OLD]['archives']['Archive 1']['files'][item['path']]
        self.assertEqual(proof['sha256'], item['sha256'])
        self.assertEqual(proof['status'], 'expected')
        self.assertNotIn('local', proof)

    def test_missing_host_catalog_restores_all_volumes_from_inserted_disk(self):
        a, _, _ = self.archive(self.old, 'Archive 1')
        catalog.snapshot(self.old, OLD, self.host, archives=[a])
        self.online = {NEW: self.new}
        catalog.synchronize(self.new, NEW, self.host)
        (self.host / 'volume-catalog.json').unlink()
        catalog.synchronize(self.new, NEW, self.host)
        self.assertIn(OLD, catalog.load(self.host)['volumes'])

    def test_offline_mount_and_wrong_uuid_fail_without_mutating_catalog(self):
        archive, _, _ = self.archive(self.old, 'Archive 1')
        with self.assertRaises(ValueError):
            catalog.snapshot(self.old, NEW, self.host, archives=[archive])
        self.assertFalse((self.host / 'volume-catalog.json').exists())
        self.online = {}
        with self.assertRaises(ValueError):
            catalog.synchronize(self.old, OLD, self.host)
        self.assertFalse((self.old / '.yds-download').exists())

    def test_queue_snapshot_reads_portable_archive_destinations(self):
        archive, item, _ = self.archive(self.old, 'Archive 1')
        queue = self.old / 'queue.json'
        d.write_json(queue, dict(archives=[dict(destination='Archive 1',
                                                catalog='Archive 1/.download/catalog.json')]))
        catalog.snapshot(self.old, OLD, self.host, queues=[queue])
        self.assertEqual(catalog.load(self.host)['volumes'][OLD]['archives']['Archive 1']
                         ['files'][item['path']]['status'], 'verified')
        d.write_json(queue, dict(archives=[dict(destination=str(self.new / 'Archive 2'))]))
        with self.assertRaisesRegex(ValueError,'Archive is not on this volume'):
            catalog.snapshot(self.old, OLD, self.host, queues=[queue])

    def test_partial_rescan_keeps_other_archives_but_replaces_scanned_proofs(self):
        first, _, _ = self.archive(self.old, 'Archive 1')
        second, _, _ = self.archive(self.old, 'Archive 2')
        catalog.snapshot(self.old, OLD, self.host, archives=[first, second])
        (first / 'Course/Lecture.mp4').unlink()
        catalog.snapshot(self.old, OLD, self.host, archives=[first])
        archives = catalog.load(self.host)['volumes'][OLD]['archives']
        self.assertEqual(archives['Archive 1']['files']['/Course/Lecture.mp4']['status'], 'expected')
        self.assertEqual(archives['Archive 2']['files']['/Course/Lecture.mp4']['status'], 'verified')

    def test_corrupt_host_recovers_from_disk_but_corrupt_both_aborts(self):
        archive, _, _ = self.archive(self.old, 'Archive 1')
        catalog.snapshot(self.old, OLD, self.host, archives=[archive])
        (self.host / 'volume-catalog.json').write_text('{"broken": true}')
        catalog.synchronize(self.old, OLD, self.host)
        self.assertIn(OLD, catalog.load(self.host)['volumes'])
        (self.host / 'volume-catalog.json').write_text('corrupt')
        (self.old / '.yds-download/volume-catalog.json').write_text('corrupt')
        with self.assertRaises(ValueError):
            catalog.synchronize(self.old, OLD, self.host)

    def test_full_volume_uses_existing_event_checkpoint_without_writing_new_one(self):
        archive, item, path = self.archive(self.old, 'Archive 1')
        journal = archive / '.download/events.jsonl'
        d.EventIndex(journal, repair=False, checkpoint=True).refresh()
        stat = path.stat()
        d.append_events(journal,[dict(item, status='valid',
             mtime_ns=stat.st_mtime_ns, ctime_ns=stat.st_ctime_ns)])
        with patch.object(d.EventIndex, 'CHECKPOINT_INTERVAL', 1), \
                patch.object(d.EventIndex, '_save_checkpoint',
                             side_effect=AssertionError('Snapshot must not write to full HDD')):
            catalog.snapshot(self.old, OLD, self.host, archives=[archive])
        proof = catalog.load(self.host)['volumes'][OLD]['archives']['Archive 1']['files'][item['path']]
        self.assertEqual(proof['status'],'verified')

    def test_unfinished_jsonl_tail_is_not_repaired_by_catalog(self):
        archive, _, _ = self.archive(self.old, 'Archive 1')
        path = archive / '.download/events.jsonl'
        with path.open('ab') as output:
            output.write(b'{"incomplete":')
        before = path.read_bytes()
        catalog.snapshot(self.old, OLD, self.host, archives=[archive])
        self.assertEqual(path.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
