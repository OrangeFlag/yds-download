"""Supervise concurrent queues without changing their individual execution."""
import signal
import subprocess
import unittest
from unittest.mock import patch

import semester_queue as queue
import storage
import test_semester_before


class ParallelQueueChecks(unittest.TestCase):
    setUp = test_semester_before.BeforeQueueChecks.setUp
    def run_queue(self, *args):
        if '--download-budget' not in args:
            args = (*args, '--download-budget', self.root / 'budget')
        return test_semester_before.BeforeQueueChecks.run_queue(self, *args)

    def test_parallel_requires_shared_download_budget(self):
        with patch.object(queue, 'parallel_queues') as spawn:
            with self.assertRaises(SystemExit) as error:
                test_semester_before.BeforeQueueChecks.run_queue(
                    self, '--parallel-before', '--before', self.personal)
        self.assertEqual(error.exception.code, 2)
        spawn.assert_not_called()

    def test_parallel_starts_personal_first_preserves_budgets_and_waits(self):
        events = []
        processes = []
        class Process:
            def __init__(self, command, **kwargs):
                self.command = command
                self.options = kwargs
                self.pid = 100 + len(processes)
                self.polls = 0
                processes.append(self)
                events.append(command[2])
            def poll(self):
                self.polls += 1
                return 0 if self.polls > (3 if self is processes[0] else 1) else None
            def wait(self, timeout=None):
                return 0
        with patch.object(queue.subprocess, 'Popen', side_effect=Process), patch.object(queue.time, 'sleep'), \
                patch.object(queue, 'transfer_process') as transfer:
            self.run_queue('--parallel-before', '--before', self.personal, '--download-budget', self.root / 'budget',
                           '--nextcloud-fallback', 'https://example.com/share')
        self.assertEqual(events, [str(self.personal), str(self.course)])
        self.assertGreaterEqual(processes[0].polls, 4)
        self.assertTrue(all(p.options['start_new_session'] for p in processes))
        self.assertIn('--download-budget', processes[0].command)
        self.assertNotIn('--nextcloud-fallback', processes[0].command)
        self.assertIn('--nextcloud-fallback', processes[1].command)
        self.assertNotIn('--before', processes[1].command)
        self.assertNotIn('--parallel-before', processes[1].command)
        self.assertEqual(processes[0].options['env']['YDS_DOWNLOAD_PRIORITY'], 'personal')
        self.assertEqual(processes[1].options['env']['YDS_DOWNLOAD_PRIORITY'], 'normal')
        transfer.assert_not_called()

    def test_successful_personal_does_not_interrupt_running_course(self):
        personal = unittest.mock.Mock(pid=123)
        personal.poll.return_value = 0
        course = unittest.mock.Mock(pid=124)
        course.poll.side_effect = [None, None, 1]
        with patch.object(queue.subprocess, 'Popen', side_effect=[personal, course]), \
                patch.object(queue.time, 'sleep'):
            self.run_queue('--parallel-before', '--before', self.personal)
        personal.send_signal.assert_not_called()
        course.send_signal.assert_not_called()
        self.assertEqual(course.poll.call_count, 3)

    def test_failed_queue_stops_and_reaps_peer_once(self):
        for code, exception in ((storage.TEMPFAIL, storage.StorageUnavailable), (2, RuntimeError),
                                (130, KeyboardInterrupt), (-15, KeyboardInterrupt)):
            with self.subTest(code=code):
                child = unittest.mock.Mock(pid=123)
                child.poll.return_value = code
                peer = unittest.mock.Mock(pid=124)
                peer.poll.return_value = None
                with patch.object(queue.subprocess, 'Popen', side_effect=[child, peer]), \
                        patch.object(queue.os, 'killpg'), patch.object(queue.time, 'sleep'):
                    with self.assertRaises(exception):
                        self.run_queue('--parallel-before', '--before', self.personal)
                peer.send_signal.assert_called_once_with(signal.SIGINT)
                peer.wait.assert_called_once()
                child.send_signal.assert_not_called()

    def test_parent_spawn_failure_cleans_started_personal(self):
        child = unittest.mock.Mock(pid=123)
        child.poll.return_value = None
        with patch.object(queue.subprocess, 'Popen', side_effect=[child, OSError('spawn failed')]), \
                patch.object(queue.os, 'killpg'):
            with self.assertRaisesRegex(OSError, 'spawn failed'):
                self.run_queue('--parallel-before', '--before', self.personal)
        child.send_signal.assert_called_once_with(signal.SIGINT)
        child.wait.assert_called_once()

    def test_interrupt_and_timeout_reap_whole_child_groups(self):
        children = [unittest.mock.Mock(pid=123), unittest.mock.Mock(pid=124)]
        for child in children:
            child.poll.return_value = None
            child.wait.side_effect = [subprocess.TimeoutExpired('queue', 90), -9]
        with patch.object(queue.subprocess, 'Popen', side_effect=children), \
                patch.object(queue.time, 'sleep', side_effect=KeyboardInterrupt), \
                patch.object(queue.os, 'killpg') as killpg:
            with self.assertRaises(KeyboardInterrupt):
                self.run_queue('--parallel-before', '--before', self.personal)
        for child in children:
            child.send_signal.assert_called_once_with(signal.SIGINT)
            self.assertEqual(child.wait.call_count, 2)
            self.assertIn(unittest.mock.call(child.pid, signal.SIGKILL), killpg.call_args_list)


if __name__ == '__main__':
    unittest.main()
