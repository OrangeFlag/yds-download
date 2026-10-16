import json
import fcntl
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import services
import storage


class ServiceControlTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'storage.py').touch()
        self.config = dict(repo=str(self.root), state_dir=str(self.root / 'state'),
                           mount_root=str(self.root / 'disk'),
                           disk_uuid='088d300e-87f5-4293-999d-80726a0b2250',
                           previous_mounts=['/old'], environment={'HTTPS_PROXY': ''},
                           services={'queue': {'command': ['python3', '$repo/semester_queue.py',
                                                          '$mount_root/a b/queue.json']},
                                     'range': {'command': ['python3', '$repo/range_queue.py']}})
        self.states = {'queue': 'inactive', 'range': 'inactive'}

    def test_command_preserves_spaces_and_wraps_storage(self):
        command = services.start_command(self.config, 'queue', self.root / 'disk')
        self.assertIn('--setenv=HTTPS_PROXY=', command)
        self.assertIn('--property=KillMode=control-group', command)
        self.assertIn('--expand-environment=no', command)
        self.assertEqual(command[-3:], ['python3', str(self.root / 'semester_queue.py'),
                                        str(self.root / 'disk/a b/queue.json')])
        self.assertIn(str(self.root / 'storage.py'), command)

    def test_speed_targets_apply_to_the_correct_direction(self):
        self.config['speed_targets_mbit_s'] = {'download': 90, 'upload': 80}
        self.config['services']['nextcloud'] = {'command': ['python3', '$repo/nextcloud.py']}
        for name, speed in [('queue', 90), ('range', 90), ('nextcloud', 80)]:
            command = services.start_command(self.config, name, self.root / 'disk')
            self.assertIn('--setenv=YDS_TRANSFER_TARGET_MBIT_S=' + str(speed), command)

    def test_invalid_speed_target_is_rejected_before_any_systemd_calls(self):
        path = self.root / 'services.json'
        for targets in ({'download': -1}, {'upload': True}, {'other': 90}, {'download': None}):
            self.config['speed_targets_mbit_s'] = targets
            path.write_text(json.dumps(self.config))
            with self.subTest(targets=targets), self.assertRaises(ValueError):
                services.load(path)

    def test_down_does_not_touch_or_require_disk(self):
        with patch('services.states', return_value={'queue': 'active', 'range': 'inactive'}), \
                patch('services.subprocess.run') as run, \
                patch('storage.select_mount', side_effect=AssertionError('Disk accessed')):
            services.control(self.config, 'down', ['queue', 'range'])
        self.assertEqual(run.call_args.args[0], ['systemctl', '--user', 'stop', services.unit('queue')])

    def test_no_mount_never_starts_or_creates_payload_directories(self):
        with patch('services.states', return_value=self.states), \
                patch('storage.select_mount', return_value=None), \
                patch('services.subprocess.run') as run:
            with self.assertRaisesRegex(ValueError, 'disk'):
                services.control(self.config, 'up', ['queue'])
        run.assert_not_called()
        self.assertFalse((self.root / 'disk').exists())

    def test_migration_cannot_run_beside_active_worker(self):
        (self.root / 'disk').mkdir()
        with patch('services.states', return_value={'queue': 'inactive', 'range': 'active'}), \
                patch('storage.select_mount', return_value=self.root / 'disk'), \
                patch('storage.probe', return_value=True), \
                patch('storage.migrate') as migrate:
            with self.assertRaisesRegex(ValueError, 'restart'):
                services.control(self.config, 'up', ['queue'])
        migrate.assert_not_called()

    def test_up_is_idempotent_for_running_services(self):
        with patch('services.states', return_value={'queue': 'active', 'range': 'active'}), \
                patch('services.subprocess.run') as run, patch('storage.select_mount') as mount:
            services.control(self.config, 'up', ['queue', 'range'])
        run.assert_not_called()
        mount.assert_not_called()

    def test_failed_group_start_stops_only_units_started_in_this_call(self):
        def run(command, **kwargs):
            if command[0] == 'systemd-run' and '--unit=' + services.unit('range') in command:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0)
        with patch('services.states', return_value=self.states), \
                patch('services.prepare', return_value=self.root / 'disk'), \
                patch('services.subprocess.run', side_effect=run) as invoke:
            with self.assertRaises(subprocess.CalledProcessError):
                services.control(self.config, 'up', ['queue', 'range'])
        self.assertEqual(invoke.call_args.args[0], ['systemctl', '--user', 'stop',
                                                  services.unit('queue'), services.unit('range')])

    def test_config_rejects_unknown_service_and_bad_template_before_stopping(self):
        path = self.root / 'services.json'
        self.config['services']['queue']['command'] = ['python3', '$typo/file.py']
        path.write_text(json.dumps(self.config))
        with self.assertRaisesRegex(ValueError, 'typo'):
            services.load(path)
        self.config['services']['oops'] = self.config['services'].pop('queue')
        path.write_text(json.dumps(self.config))
        with self.assertRaisesRegex(ValueError, 'service'):
            services.load(path)

    def test_old_paths_are_migrated_together_even_if_new_root_is_already_recorded(self):
        disk = self.root / 'disk'; state = self.root / 'state'
        path = disk / 'course/.download/task.json'; path.parent.mkdir(parents=True)
        path.write_text(json.dumps(['/old0/file.mp4', '/old1/file.mp4']))
        storage.migrate('/old1', disk, 'uuid', state)
        storage.migrate(disk, disk, 'uuid', state, previous=['/old0', '/old1'])
        self.assertEqual(json.loads(path.read_text()), [str(disk / 'file.mp4')] * 2)
        with patch('storage.os.walk', side_effect=AssertionError('Unnecessary rescan')):
            storage.migrate(disk, disk, 'uuid', state, previous=['/old0', '/old1'])

    def test_omitted_active_unit_is_still_seen_and_stopped(self):
        self.config['services'].pop('range')
        with patch('services.states', return_value={'queue': 'inactive', 'range': 'active',
                                                    'nextcloud': 'inactive', 'opencloud': 'inactive'}) as query, \
                patch('services.subprocess.run') as run:
            services.control(self.config, 'down', list(services.NAMES))
        self.assertEqual(set(query.call_args.args[0]), set(services.NAMES))
        self.assertEqual(run.call_args.args[0], ['systemctl', '--user', 'stop', services.unit('range')])

    def test_queue_escape_is_rejected_before_migration_or_write(self):
        disk = self.root / 'disk'; disk.mkdir()
        outside = self.root / 'queue.json'; outside.write_text('{}')
        (disk / 'link').symlink_to(self.root, target_is_directory=True)
        self.config['queue_tuning'] = {'timeout': 30}
        for queue in ('$mount_root/../queue.json', '$mount_root/link/queue.json'):
            with self.subTest(queue=queue):
                self.config['course_queue'] = queue
                with patch('storage.select_mount', return_value=disk), \
                        patch('storage.probe', return_value=True), patch('storage.migrate') as migrate:
                    with self.assertRaisesRegex(ValueError, 'course_queue'):
                        services.prepare(self.config, self.states)
                migrate.assert_not_called()
                self.assertEqual(outside.read_text(), '{}')

    def test_controller_lock_is_shared_even_with_another_state_directory(self):
        lock_path = self.root / 'controller.lock'
        self.config['state_dir'] = str(self.root / 'another-state')
        with lock_path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch('services.CONTROL_LOCK', lock_path, create=True), \
                    patch('services.load', return_value=self.config), \
                    patch('services.control') as control, \
                    patch('sys.argv', ['services.py', 'down']):
                self.assertEqual(services.main(), 1)
            control.assert_not_called()

    def test_first_launch_records_mount_and_later_remount_preserves_paths(self):
        first = self.root / 'disk'; second = self.root / 'reattached'
        metadata = first / 'course/.download/task.json'; metadata.parent.mkdir(parents=True)
        metadata.write_text(json.dumps({'destination': str(first / 'course')}))
        self.config['previous_mounts'] = []
        with patch('storage.select_mount', return_value=first):
            services.prepare(self.config, self.states)
        record = self.root / 'state' / (self.config['disk_uuid'] + '.json')
        self.assertEqual(json.loads(record.read_text())['mount'], str(first))
        first.rename(second)
        with patch('storage.select_mount', return_value=second):
            services.prepare(self.config, self.states)
        self.assertEqual(json.loads((second / 'course/.download/task.json').read_text()),
                         {'destination': str(second / 'course')})

    def test_first_launch_after_remount_uses_configured_old_path(self):
        disk = self.root / 'reattached'
        metadata = disk / 'course/.download/task.json'; metadata.parent.mkdir(parents=True)
        metadata.write_text(json.dumps({'destination': str(self.root / 'disk/course')}))
        self.config['previous_mounts'] = []
        with patch('storage.select_mount', return_value=disk):
            services.prepare(self.config, self.states)
        self.assertEqual(json.loads(metadata.read_text()), {'destination': str(disk / 'course')})

    def test_disk_disappearing_during_probe_never_migrates_or_changes_queue(self):
        disk = self.root / 'disk'; disk.mkdir()
        queue = disk / 'queue.json'; queue.write_text('{"tuning": {"timeout": 60}}')
        self.config.update(course_queue='$mount_root/queue.json', queue_tuning={'timeout': 30})
        with patch('storage.select_mount', side_effect=[disk, None]), \
                patch('storage.probe', return_value=True), patch('storage.migrate') as migrate:
            with self.assertRaisesRegex(ValueError, 'disk'):
                services.prepare(self.config, self.states)
        migrate.assert_not_called()
        self.assertEqual(json.loads(queue.read_text())['tuning']['timeout'], 60)


if __name__ == '__main__':
    unittest.main()
