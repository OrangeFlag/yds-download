"""Finished archives must retain and retry publication independently of file work."""
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
from threading import Event
import unittest
from unittest.mock import patch

import download as d
import report
import semester_queue


class PendingReportChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / '.download').mkdir()

    def enqueue(self, message='Course'):
        with patch.object(report, 'retry_reports'):
            report.queue_report(self.root, self.root / 'repo', 'Walterit', True, message)
        return list((self.root / '.download/report-pending').glob('*.json'))

    def test_deep_repair_retains_final_publication_for_finished_course(self):
        data = dict(source='https://disk.yandex.ru/d/test', title='Year')
        item = dict(path='/Course/v', size=7, sha256=hashlib.sha256(b'lecture').hexdigest(),
                    file='https://example.invalid/v')
        local = self.root / 'Course/v'; local.parent.mkdir(); local.write_bytes(b'lecture')
        with patch.object(report, 'retry_reports'), patch.object(d, 'list_course', return_value=[item]):
            self.assertEqual(d.download(data, ['Course'], self.root, retries=0, deep=True,
                                       retry_check=True, report_repo=self.root / 'repo', mark='Walterit', git_push=True), 0)
        jobs = list((self.root / '.download/report-pending').glob('*.json'))
        self.assertTrue(any(json.loads(path.read_text())['message'].endswith('/ Course') for path in jobs))
        with patch.object(report, 'update_report', side_effect=RuntimeError('push failed')), \
                patch('network.wait_if_offline', return_value=False):
            report._publish_reports(self.root, Event())
        with patch.object(report, 'retry_reports'), \
                patch.object(d, 'list_course', side_effect=AssertionError('finished course reprocessed')):
            self.assertEqual(d.download(data, ['Course'], self.root, retries=0,
                                       report_repo=self.root / 'repo', mark='Walterit', git_push=True), 0)
        self.assertTrue(all(path.exists() for path in jobs))
        next_attempt = max(json.loads(path.read_text())['next_attempt'] for path in jobs)
        with patch.object(report, 'update_report') as publish, \
                patch.object(report.time, 'time', return_value=next_attempt + 1):
            report._publish_reports(self.root, Event())
        self.assertEqual(publish.call_count, len(jobs))
        self.assertFalse(any(path.exists() for path in jobs))

    def test_failed_publish_keeps_intent_and_backoff_until_success(self):
        paths = self.enqueue()
        with patch.object(report, 'update_report', side_effect=subprocess.CalledProcessError(1, 'git push')), \
                patch('network.wait_if_offline', return_value=False):
            report._publish_reports(self.root, Event())
        saved = json.loads(paths[0].read_text())
        self.assertEqual(saved['mark'], 'Walterit')
        self.assertEqual(saved['attempts'], 1)
        self.assertGreater(saved['next_attempt'], 0)
        stopping = Event()
        with patch.object(report, 'update_report') as publish, \
                patch.object(report.time, 'time', return_value=saved['next_attempt'] + 1):
            report._publish_reports(self.root, stopping)
        publish.assert_called_once_with(self.root, self.root / 'repo', 'Walterit', True, 'Course', stopping=stopping)
        self.assertFalse(paths[0].exists())

    def test_new_course_intent_survives_successful_previous_publish(self):
        self.enqueue('First')
        def publish(*args, **kwargs):
            if args[-1] == 'First':
                self.enqueue('Second')
        with patch.object(report, 'update_report', side_effect=publish):
            report._publish_reports(self.root, Event())
        jobs = list((self.root / '.download/report-pending').glob('*.json'))
        self.assertEqual([json.loads(path.read_text())['message'] for path in jobs], ['Second'])

    def test_local_network_outage_retries_without_spending_backoff_attempt(self):
        paths = self.enqueue()
        with patch.object(report, 'update_report', side_effect=[subprocess.CalledProcessError(1, 'git push'), None]) as publish, \
                patch('network.wait_if_offline', return_value=True) as wait:
            report._publish_reports(self.root, Event())
        self.assertEqual(publish.call_count, 2)
        wait.assert_called_once()
        self.assertFalse(paths[0].exists())

    def test_stop_interrupts_shared_git_lock(self):
        repo = self.root / 'repo'; common = repo / '.git'; common.mkdir(parents=True)
        stopping = Event(); stopping.set()
        with (common / 'yds-report.lock').open('a') as lock, \
                patch.object(report.subprocess, 'check_output', return_value=str(common)):
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(InterruptedError):
                report.update_report(self.root, repo, 'Walterit', stopping=stopping)

    def test_hung_push_uses_short_timeout_and_keeps_publication_pending(self):
        paths = self.enqueue()
        source = 'https://disk.yandex.ru/d/test'
        d.write_json(self.root / '.download/source.json', {'source': source})
        local = self.root / 'Course/v'; local.parent.mkdir(); local.write_bytes(b'lecture')
        d.record(self.root / '.download', dict(path='/Course/v', size=7, sha256='1' * 64), 'Course', 1, 'valid', 'Verified')
        cards = self.root / 'repo/courses'; cards.mkdir(parents=True)
        (cards / 'course.md').write_text(f'- Источник: [disk]({source})\n- Путь: `Course`\n'
                                      '**Статус:** не все видео есть, какие-то не удается скачать\n'
                                      '| `v` | 7 | | | |\n')
        calls = []
        def git(command, **kwargs):
            calls.append((command[3], kwargs['timeout']))
            if 'push' in command:
                raise subprocess.TimeoutExpired(command, kwargs['timeout'])
            return subprocess.CompletedProcess(command, 0)
        with patch.object(report.subprocess, 'run', side_effect=git), \
                patch.object(report.subprocess, 'check_output', return_value=b''), \
                patch('network.wait_if_offline', return_value=False):
            report._publish_reports(self.root, Event())
        self.assertEqual(calls[-3:], [('add', 120), ('commit', 120), ('push', 15)])
        self.assertTrue(paths[0].exists())

    def test_finished_archive_retries_failed_final_course_without_redownload(self):
        paths = self.enqueue()
        with patch.object(report, 'update_report', side_effect=RuntimeError('push failed')), \
                patch('network.wait_if_offline', return_value=False):
            report._publish_reports(self.root, Event())
        queue = self.root / 'queue.json'
        d.write_json(queue, {'archives': [{'title': 'Done', 'destination': str(self.root),
                                         'catalog': 'unused', 'state': 'finished'}]})
        next_attempt = json.loads(paths[0].read_text())['next_attempt']
        with patch('sys.argv', ['semester_queue.py', str(queue)]), \
                patch.object(semester_queue, 'transfer_process', side_effect=AssertionError('redownload')), \
                patch.object(report, 'update_report') as publish, \
                patch.object(report.time, 'time', return_value=next_attempt + 1):
            semester_queue.main()
        publish.assert_called_once()
        self.assertFalse(paths[0].exists())
