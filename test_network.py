"""Classify failures using actual requests; never open connectivity probes."""
import errno
import http.client
import json
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path
from threading import Event
from unittest.mock import Mock, patch
import urllib.error
import network


class NetworkChecks(unittest.TestCase):
    def setUp(self):
        self.guard = network.NetworkGuard()
        replacement = patch.object(network, 'GUARD', self.guard)
        replacement.start(); self.addCleanup(replacement.stop)
        self.stop = Mock(); self.stop.is_set.return_value = False; self.stop.wait.return_value = False
        probe = patch.object(socket, 'create_connection', side_effect=AssertionError('No TCP probes'))
        probe.start(); self.addCleanup(probe.stop)

    def test_wrapped_route_failure_retries_real_request_without_probe(self):
        error = urllib.error.URLError(OSError(errno.EHOSTUNREACH, 'No route to host'))
        operation = Mock(side_effect=[error, error, 'metadata'])
        self.assertEqual(network.retry_request(operation, self.stop, target='metadata'), 'metadata')
        self.assertEqual(operation.call_count, 3)
        self.assertEqual([c.args[0] for c in self.stop.wait.call_args_list], [0, .1])

    def test_persistent_route_failure_backs_off_and_is_interruptible(self):
        self.stop.wait.side_effect = [False] * 9 + [True]
        operation = Mock(side_effect=OSError(errno.ENETUNREACH, 'Network unreachable'))
        with self.assertRaises(InterruptedError):
            network.retry_request(operation, self.stop)
        self.assertEqual([c.args[0] for c in self.stop.wait.call_args_list],
                         [0, .1, .2, .4, .8, 1.6, 3.2, 5, 5, 5])

    def test_http_error_does_not_become_local_network_failure(self):
        error = urllib.error.HTTPError('hidden', 500, 'Unavailable', {}, None)
        operation = Mock(side_effect=error)
        with self.assertRaises(urllib.error.HTTPError):
            network.retry_request(operation, self.stop)
        self.assertEqual(operation.call_count, 1)
        self.stop.wait.assert_not_called()

    def test_ambiguous_error_gets_only_two_confirmation_retries(self):
        operation = Mock(side_effect=http.client.IncompleteRead(b'{'))
        with self.assertRaises(http.client.IncompleteRead):
            network.retry_request(operation, self.stop, target='single-file')
        self.assertEqual(operation.call_count, 3)

    def test_json_eof_is_ambiguous_but_disk_error_is_not(self):
        self.assertEqual(network.failure(json.JSONDecodeError('EOF', '{', 1))['category'], 'ambiguous')
        self.assertEqual(network.failure(OSError(errno.ENOSPC, 'Disk full'))['category'], 'other')
        self.assertEqual(network.failure(urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, 'Temporary DNS')))['category'], 'routing')
        self.assertEqual(network.failure(urllib.error.URLError(socket.gaierror(socket.EAI_NONAME, 'Bad hostname')))['category'], 'other')

    def test_correlated_failures_keep_attempt_without_spending_budget(self):
        with patch.object(network.time, 'monotonic', return_value=100):
            for target in ('file-a', 'file-b'):
                self.guard.observe(target, network.failure(TimeoutError()))
            retry = network.RequestRetry('file-c', self.stop)
            for _ in range(5):
                self.assertTrue(retry.retry(network.failure(TimeoutError())))

    def test_successful_transfer_prevents_false_global_outage(self):
        with patch.object(network.time, 'monotonic', return_value=100):
            for target in ('file-a', 'file-b', 'file-c'):
                self.guard.observe(target, network.failure(TimeoutError()))
        with patch.object(network.time, 'monotonic', return_value=101):
            self.guard.observe('healthy-file')
            retry = network.RequestRetry('file-c', self.stop)
            self.assertTrue(retry.retry(network.failure(TimeoutError())))
            self.assertTrue(retry.retry(network.failure(TimeoutError())))
            self.assertFalse(retry.retry(network.failure(TimeoutError())))

    def test_old_failures_do_not_create_new_outage(self):
        with patch.object(network.time, 'monotonic', return_value=100):
            for target in ('file-a', 'file-b'):
                self.guard.observe(target, network.failure(TimeoutError()))
        with patch.object(network.time, 'monotonic', return_value=131):
            retry = network.RequestRetry('file-c', self.stop)
            self.assertTrue(retry.retry(network.failure(TimeoutError())))
            self.assertTrue(retry.retry(network.failure(TimeoutError())))
            self.assertFalse(retry.retry(network.failure(TimeoutError())))

    def test_known_route_error_is_not_hidden_by_other_successes(self):
        self.guard.observe('healthy-file')
        retry = network.RequestRetry('blocked-file', self.stop)
        for _ in range(5):
            self.assertTrue(retry.retry(network.failure(OSError(errno.EHOSTUNREACH, 'Filtered'))))

    def test_non_connection_error_never_retries(self):
        operation = Mock(side_effect=OSError(errno.EIO, 'Disk unavailable'))
        with self.assertRaises(OSError):
            network.retry_request(operation, self.stop)
        operation.assert_called_once()

    def test_aria_retries_route_failure_from_current_log_only(self):
        import pipeline
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'aria.log'
            log.write_text('Exception: errorCode=1 cause: No route to host\n')
            calls = []
            def run(*args, **kwargs):
                calls.append(1)
                with log.open('a') as stream:
                    stream.write('Exception: errorCode=1 cause: No route to host\n' if len(calls)==1 else 'Download complete: file\n')
                return 1 if len(calls)==1 else 0
            with patch.object(pipeline, '_transfer_process_once', side_effect=run):
                self.assertEqual(pipeline.transfer_process(['aria2c', '--log='+str(log)], env={}, on_tick=lambda: None, stopping=self.stop), 0)
            self.assertEqual(len(calls), 2)
            with patch.object(pipeline, '_transfer_process_once', return_value=1) as run:
                self.assertEqual(pipeline.transfer_process(['aria2c', '--log='+str(log)], env={}, on_tick=lambda: None, stopping=self.stop), 1)
            run.assert_called_once()

    def test_http_retry_after_is_honoured_and_bounded(self):
        retry = network.RequestRetry('busy-file', self.stop)
        error = urllib.error.HTTPError('hidden', 503, 'Busy', {'Retry-After': '7'}, None)
        self.assertTrue(retry.retry(network.failure(error)))
        self.assertTrue(retry.retry(network.failure(error)))
        self.assertFalse(retry.retry(network.failure(error)))
        self.assertEqual([c.args[0] for c in self.stop.wait.call_args_list], [7, 7])

    def test_aria_http_response_is_evidence_of_connectivity(self):
        error = network.failure('Exception: errorCode=22 The response status is not successful. status=503', 'transfer')
        self.assertEqual(error['category'], 'http')
        self.assertEqual(error['http_status'], 503)

    def test_two_aria_timeouts_do_not_count_as_three_resources(self):
        self.stop.wait.side_effect = [False, False, True]
        import pipeline
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'aria.log'
            def run(*args, **kwargs):
                with log.open('a') as stream:
                    for resource in ('a', 'b'):
                        stream.write('Download aborted. URI=https://example.invalid/'+resource+'\nException: errorCode=2 Timeout.\n')
                return 1
            with patch.object(pipeline, '_transfer_process_once', side_effect=run) as transfer:
                self.assertEqual(pipeline.transfer_process(['aria2c', '--log='+str(log)], env={}, on_tick=lambda: None, stopping=self.stop), 1)
            self.assertEqual(transfer.call_count, 3)

    def test_subprocess_failure_uses_cause_not_exit_status(self):
        error = subprocess.CalledProcessError(128, ['git', 'push'], stderr='connect: No route to host')
        self.assertEqual(network.failure(error)['category'], 'routing')
        self.assertEqual(network.failure(subprocess.CalledProcessError(128, ['git', 'push'], stderr='non-fast-forward'))['category'], 'other')
