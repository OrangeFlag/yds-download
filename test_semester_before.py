"""Personal queue chaining preserves budgets and allows course progress during Range."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import semester_queue as queue
import storage


class BeforeQueueChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.course = self.root / 'course.json'
        self.personal = self.root / 'personal.json'
        for path in (self.course, self.personal):
            path.write_text('{"archives": []}')
        self.enterContext(patch.object(queue, 'wait_reports'))
        self.enterContext(patch.object(queue, 'retry_reports'))

    def run_queue(self, *args):
        with patch('sys.argv', ['semester_queue.py', str(self.course), *map(str, args)]):
            return queue.main()

    def test_before_preserves_budgets_and_excludes_cloud_flags(self):
        budget = self.root / 'range-budget'
        downloads = self.root / 'download-budget'
        with patch.object(queue, 'transfer_process', return_value=1, autospec=True) as transfer:
            self.run_queue('--before', self.personal, '--defer-range', '--range-budget', budget,
                           '--range-files', '3', '--download-budget', downloads,
                           '--download-min-files', '1', '--download-max-files', '5',
                           '--nextcloud-fallback', 'https://example.com/share',
                           '--opencloud-fallback', 'https://example.com',
                           '--opencloud-username', 'user', '--opencloud-password-file', self.root / 'token')
        command = transfer.call_args.args[0]
        self.assertEqual(command[2], str(self.personal))
        for flag in ('--defer-range', '--no-wait-range'):
            self.assertIn(flag, command)
        for flag, expected in (('--range-budget', str(budget)), ('--range-files', '3'),
                               ('--download-budget', str(downloads)), ('--download-min-files', '1'),
                               ('--download-max-files', '5')):
            self.assertEqual(command[command.index(flag) + 1], expected)
        for flag in ('--before', '--nextcloud-fallback', '--opencloud-fallback', '--s3-fallback'):
            self.assertNotIn(flag, command)

    def test_children_run_in_order_and_parent_runs_after_them(self):
        second = self.root / 'second.json'
        second.write_text('{"archives": []}')
        destination = self.root / 'archive'
        self.course.write_text(json.dumps(dict(archives=[dict(title='course', catalog='catalog.json',
                                                             destination=str(destination))])))
        calls = []
        def transfer(command, **kwargs):
            calls.append(command)
            return 0
        with patch.object(queue, 'transfer_process', side_effect=transfer):
            self.run_queue('--before', self.personal, '--before', second)
        self.assertEqual([calls[0][2], calls[1][2]], [str(self.personal), str(second)])
        self.assertEqual(calls[2][2], 'download')

    def test_child_failure_and_interrupt_stop_chain(self):
        for code, exception in ((storage.TEMPFAIL, storage.StorageUnavailable), (2, RuntimeError),
                                (130, KeyboardInterrupt), (-2, KeyboardInterrupt),
                                (143, KeyboardInterrupt), (-15, KeyboardInterrupt)):
            with self.subTest(code=code), patch.object(queue, 'transfer_process', return_value=code) as transfer:
                with self.assertRaises(exception):
                    self.run_queue('--before', self.personal)
                self.assertEqual(transfer.call_count, 1)

    def test_self_and_duplicates_rejected_before_transfer(self):
        for paths in ((self.course,), (self.personal, self.personal)):
            with self.subTest(paths=paths), patch.object(queue, 'transfer_process') as transfer:
                flags = [part for path in paths for part in ('--before', path)]
                with self.assertRaises(SystemExit) as error:
                    self.run_queue(*flags)
                self.assertEqual(error.exception.code, 2)
                transfer.assert_not_called()

    def test_no_wait_range_leaves_recovery_for_worker(self):
        destination = self.root / 'archive'
        state = destination / '.download'
        state.mkdir(parents=True)
        (state / 'summary.json').write_text(json.dumps(dict(downloads_complete=True,
                                                          pending_courses=['Course'])))
        self.course.write_text(json.dumps(dict(archives=[dict(title='course', catalog='catalog.json',
                                                             destination=str(destination))])))
        with patch.object(queue, 'transfer_process') as transfer, patch.object(queue.time, 'sleep',
                                                                               side_effect=AssertionError('Waited for Range')):
            self.run_queue('--defer-range', '--no-wait-range')
        transfer.assert_not_called()
        saved = json.loads(self.course.read_text())
        self.assertEqual(saved['archives'][0]['state'], 'recovering')

    def test_finished_offline_archive_reenters_downloader(self):
        destination = self.root / 'archive'
        state = destination / '.download'
        state.mkdir(parents=True)
        (state / 'summary.json').write_text(json.dumps(dict(downloads_complete=True, offline_available=1)))
        self.course.write_text(json.dumps(dict(archives=[dict(title='course', state='finished',
                          downloads_complete=True, catalog='catalog.json', destination=str(destination))])))
        with patch.object(queue, 'transfer_process', return_value=0) as transfer:
            self.run_queue('--defer-range', '--no-wait-range')
        self.assertEqual(transfer.call_count, 1)


if __name__ == '__main__':
    unittest.main()
