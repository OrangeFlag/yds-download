"""Metadata handoff works after source removal and never carries local progress."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import prepare_disk as prep
import storage
import volume_catalog

OLD = '088d300e-87f5-4293-999d-80726a0b2250'
NEW = '4b45c6a0-122e-4dc1-9175-04fc6c765330'


class PrepareDiskChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.old, self.new = self.home / 'old', self.home / 'new'
        self.old.mkdir()
        self.new.mkdir()
        self.online = {OLD: self.old}
        self.enterContext(patch.object(storage, 'select_mount', side_effect=lambda identity, hint: self.online.get(identity)))
        self.enterContext(patch.object(storage, 'probe', return_value=True))
        self.state = self.home / 'registry'
        self.bundle = self.home / 'handoff.json'
        self.output = self.home / 'services-new.json'
        self.config = self.home / 'services.json'
        self.course = self.old / 'Archive/.download/queue.json'
        self.personal = self.old / 'Personal/.download/queue.json'
        for queue, prefix in ((self.course, 'Course'), (self.personal, 'Personal')):
            jobs = []
            for index in range(2):
                catalog = queue.parent / f'{index}.json'
                self.write(catalog, dict(source='https://disk.yandex.ru/d/' + prefix,
                                         title=prefix, courses=[dict(name='B', enabled=True), dict(name='A', enabled=False)]))
                jobs.append(dict(title=f'{prefix}{index}', catalog=str(catalog),
                                 destination=str(self.old / f'{prefix}{index}'), state='finished',
                                 downloads_complete=True, exit_code=1, time='yesterday', enabled=True))
            self.write(queue, dict(retries=2, archives=jobs, retry_check=True,
                                   deep=True, wait_for=dict(unit='old', destination=str(self.old))))
        extra = self.old / 'Spring2026'
        self.write(extra / '.download/catalog.json', dict(source='https://disk.yandex.ru/d/spring',
                   title='spring', courses=[dict(name='A', enabled=True), dict(name='B', enabled=False)]))
        self.write(extra / '.download/courses.json', ['B', 'A'])
        self.write(extra / '.download/events.jsonl', dict(status='valid', local='old-data'))
        config = dict(repo=str(self.home), state_dir=str(self.home / 'state'), mount_root=str(self.old),
                      disk_uuid=OLD, previous_mounts=['/previous'], course_queue='$mount_root/Archive/.download/queue.json',
                      services=dict(queue=dict(command=['python3', '$repo/semester_queue.py', '$mount_root/Archive/.download/queue.json']),
                                    range=dict(command=['python3', '$repo/range_queue.py', '--queue', '$mount_root/Personal/.download/queue.json']),
                                    opencloud=dict(command=['python3', '$repo/opencloud.py', '--destination', '$mount_root/Spring2026'])))
        self.write(self.config, config)

    def write(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data))

    def export(self):
        return prep.export(self.config, self.bundle, self.state)

    def provision(self, identity=NEW):
        return prep.provision(self.bundle, self.new, identity, self.output)

    def test_sequential_handoff_preserves_selection_order_but_drops_progress(self):
        saved = self.export()
        self.assertFalse(any('events' in name or 'manifest' in name for name in saved['files']))
        self.assertEqual(saved['files']['Spring2026/.download/catalog.json']['courses'],
                         [dict(name='B', enabled=True), dict(name='A', enabled=True)])
        self.online = {NEW: self.new}
        self.old.rename(self.home / 'unplugged')
        result = self.provision()
        self.assertEqual(result['previous_mounts'], [])
        self.assertEqual(result['environment']['YDS_VOLUME_UUID'], NEW)
        self.assertEqual(result['environment']['YDS_VOLUME_CATALOG'], str(self.state))
        command = result['services']['queue']['command']
        self.assertEqual(command[-2:], ['--before', str(self.new / 'Personal/.download/queue.json')])
        self.assertEqual(command.count('--before'), 1)
        course = storage.read_queue(self.new / 'Archive/.download/queue.json')
        self.assertEqual([row['title'] for row in course['archives']], ['spring', 'Course0', 'Course1'])
        self.assertNotIn('wait_for', course)
        self.assertNotIn('deep', course)
        for queue in ('Archive', 'Personal'):
            data = storage.read_queue(self.new / queue / '.download/queue.json')
            for row in data['archives']:
                self.assertEqual(row['state'], 'pending')
                self.assertNotIn('downloads_complete', row)
                self.assertNotIn('exit_code', row)
                self.assertTrue(Path(row['destination']).is_relative_to(self.new))
                self.assertTrue(Path(row['catalog']).is_file())
        self.assertEqual(prep._read(self.new / 'Archive/.download/0.json')['courses'],
                         [dict(name='B', enabled=True), dict(name='A', enabled=False)])
        self.assertIn(NEW, volume_catalog.load(self.new / volume_catalog.MIRROR_DIR)['volumes'])

    def test_offline_registry_is_carried_by_bundle(self):
        volume_catalog.synchronize(self.old, OLD, self.state)
        self.export()
        (self.state / volume_catalog.CATALOG).unlink()
        self.online = {NEW: self.new}
        self.provision()
        self.assertEqual(set(volume_catalog.load(self.state)['volumes']), {OLD, NEW})

    def test_source_uuid_and_unmounted_target_are_rejected(self):
        self.export()
        with self.assertRaisesRegex(ValueError, 'must differ'):
            self.provision(OLD)
        with self.assertRaisesRegex(ValueError, 'not mounted writable'):
            self.provision()
        self.assertEqual(list(self.new.iterdir()), [])

    def test_unrelated_data_and_existing_config_are_rejected(self):
        self.export()
        self.online = {NEW: self.new}
        unrelated = self.new / 'keep.txt'
        unrelated.write_text('keep')
        with self.assertRaisesRegex(ValueError, 'must be empty'):
            self.provision()
        self.assertEqual(unrelated.read_text(), 'keep')
        unrelated.unlink()
        self.output.write_text('keep config')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.provision()
        self.assertEqual(list(self.new.iterdir()), [])

    def test_metadata_escape_rejected_before_target_writes(self):
        saved = self.export()
        saved['files']['../escape.json'] = {}
        self.write(self.bundle, saved)
        self.online = {NEW: self.new}
        with self.assertRaisesRegex(ValueError, 'Unsafe handoff'):
            self.provision()
        self.assertEqual(list(self.new.iterdir()), [])

    def test_export_refuses_disk_bundle_and_conflicts(self):
        with self.assertRaisesRegex(ValueError, 'on the host'):
            prep.export(self.config, self.old / 'bundle.json', self.state)
        self.export()
        with self.assertRaises(FileExistsError):
            self.export()


if __name__ == '__main__':
    unittest.main()
