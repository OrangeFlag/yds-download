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

    def publication_repo(self):
        repo = self.root / 'repo'; repo.mkdir()
        remote = self.root / 'remote'
        subprocess.run(['git', 'init', '--bare', '-q', str(remote)], check=True)
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        for key, value in [('user.name', 'Test'), ('user.email', 'test@example.invalid')]:
            subprocess.run(['git', '-C', str(repo), 'config', key, value], check=True)
        subprocess.run(['git', '-C', str(repo), 'remote', 'add', 'origin', str(remote)], check=True)
        source = 'https://disk.yandex.ru/d/test'
        cards = repo / 'courses'; cards.mkdir()
        card = cards / 'course.md'
        card.write_text(f'- Источник: [disk]({source})\n- Путь: `Course`\n'
                        '**Статус:** не все видео есть, какие-то не удается скачать\n'
                        '| `v` | 7 | | | |\n')
        subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(repo), 'commit', '-qm', 'Initial'], check=True)
        d.write_json(self.root / '.download/source.json', {'source': source})
        local = self.root / 'Course/v'; local.parent.mkdir(); local.write_bytes(b'lecture')
        d.record(self.root / '.download', dict(path='/Course/v', size=7, sha256=hashlib.sha256(b'lecture').hexdigest()), 'Course', 1, 'valid', 'Verified')
        return repo, card

    def interrupt_commit(self, repo):
        real_run = subprocess.run
        def run(command, **kwargs):
            if command[:3] == ['git', '-C', str(repo)] and command[3] == 'commit':
                raise OSError(28, 'No space left on device')
            return real_run(command, **kwargs)
        with patch.object(report.subprocess, 'run', side_effect=run):
            with self.assertRaises(OSError):
                report.update_report(self.root, repo, 'Walterit', True)

    def test_failed_commit_replays_own_edits_without_staged_deadlock(self):
        repo, _ = self.publication_repo()
        self.interrupt_commit(repo)
        report.update_report(self.root, repo, 'Walterit', True)
        committed = subprocess.check_output(['git', '-C', str(repo), 'show', 'HEAD:courses/course.md'], text=True)
        self.assertIn('Walterit', committed)
        self.assertEqual(subprocess.run(['git', '-C', str(repo), 'diff', '--cached', '--quiet']).returncode, 0)
        self.assertFalse((repo / '.git/yds-report-transaction.json').exists())

    def test_failed_commit_does_not_revert_later_user_edits(self):
        repo, card = self.publication_repo()
        self.interrupt_commit(repo)
        card.write_text(card.read_text() + 'User edit\n')
        body = card.read_bytes()
        with self.assertRaises(RuntimeError):
            report.update_report(self.root, repo, 'Walterit', True)
        self.assertEqual(card.read_bytes(), body)

    def test_failed_commit_preserves_later_unrelated_staged_edits(self):
        repo, _ = self.publication_repo()
        self.interrupt_commit(repo)
        other = repo / 'other.txt'; other.write_text('User edit')
        subprocess.run(['git', '-C', str(repo), 'add', '--', 'other.txt'], check=True)
        index = subprocess.check_output(['git', '-C', str(repo), 'diff', '--cached'])
        with self.assertRaises(RuntimeError):
            report.update_report(self.root, repo, 'Walterit', True)
        self.assertEqual(subprocess.check_output(['git', '-C', str(repo), 'diff', '--cached']), index)

    def test_committed_report_replays_after_transaction_cleanup_outage(self):
        repo, _ = self.publication_repo()
        unlink = Path.unlink
        def interrupted(path, *args, **kwargs):
            if path.name == 'yds-report-transaction.json':
                raise OSError(5, 'Storage failure after commit')
            return unlink(path, *args, **kwargs)
        with patch.object(Path, 'unlink', interrupted):
            with self.assertRaises(OSError):
                report.update_report(self.root, repo, 'Walterit', True)
        head = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'])
        report.update_report(self.root, repo, 'Walterit', True)
        self.assertEqual(subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD']), head)
        self.assertFalse((repo / '.git/yds-report-transaction.json').exists())

    def test_publication_commit_excludes_concurrently_staged_other_file(self):
        repo, _ = self.publication_repo()
        write_json = d.write_json
        def staged(path, data):
            write_json(path, data)
            if path.name == 'yds-report-transaction.json':
                (repo / 'other.txt').write_text('User edit')
                subprocess.run(['git', '-C', str(repo), 'add', '--', 'other.txt'], check=True)
        with patch.object(d, 'write_json', side_effect=staged):
            report.update_report(self.root, repo, 'Walterit', True)
        files = subprocess.check_output(['git', '-C', str(repo), 'diff-tree', '--no-commit-id', '--name-only', '-r', 'HEAD'], text=True)
        self.assertEqual(files.strip(), 'courses/course.md')
        index = subprocess.check_output(['git', '-C', str(repo), 'diff', '--cached', '--name-only'], text=True)
        self.assertEqual(index.strip(), 'other.txt')

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
        self.assertEqual(calls[-2:], [('commit', 120), ('push', 15)])
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
