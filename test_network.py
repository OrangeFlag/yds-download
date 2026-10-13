import unittest
import errno
import io
from contextlib import redirect_stdout
from threading import Event
from unittest.mock import MagicMock, Mock, patch
import urllib.error
import network


class NetworkChecks(unittest.TestCase):
    def test_recent_online_probe_does_not_hide_new_outage(self):
        guard = network.NetworkGuard()
        stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
        with patch.object(guard, 'online', side_effect=[True, False, True]) as probe:
            self.assertFalse(guard.wait_if_offline(stop))
            self.assertTrue(guard.wait_if_offline(stop, force=False))
        self.assertEqual(probe.call_count, 3)
        stop.wait.assert_called_once_with(0)

    def test_waits_until_connection_returns(self):
        guard = network.NetworkGuard()
        stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
        with patch.object(guard, 'online', side_effect=[False, False, True]):
            self.assertTrue(guard.wait_if_offline(stop))
        self.assertEqual(stop.wait.call_count, 2)

    def test_persistent_outage_backs_off_and_caps_wait(self):
        guard = network.NetworkGuard()
        stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
        with patch.object(guard, 'online', side_effect=[False] * 10 + [True]):
            self.assertTrue(guard.wait_if_offline(stop))
        self.assertEqual([call.args[0] for call in stop.wait.call_args_list],
                         [0, .1, .2, .4, .8, 1.6, 3.2, 5, 5, 5])

    def test_next_outage_starts_with_immediate_retry_again(self):
        guard = network.NetworkGuard()
        stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
        with patch.object(guard, 'online', side_effect=[False, False, True, False, True]):
            self.assertTrue(guard.wait_if_offline(stop))
            self.assertTrue(guard.wait_if_offline(stop))
        self.assertEqual([call.args[0] for call in stop.wait.call_args_list], [0, .1, 0])

    def test_failed_probes_log_all_errors_then_successful_node(self):
        guard = network.NetworkGuard()
        stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
        errors = ExceptionGroup('create_connection failed', [
            OSError(errno.EHOSTUNREACH, 'No route to host'),
            OSError(errno.ENETUNREACH, 'Network is unreachable')])
        output = io.StringIO()
        with patch.object(network.socket, 'create_connection', side_effect=[errors, TimeoutError('timed out'), MagicMock()]) as connect, \
                redirect_stdout(output):
            self.assertTrue(guard.wait_if_offline(stop))
        log = output.getvalue()
        self.assertIn('NETWORK PROBE FAILED:', log)
        self.assertIn('cloud-api.yandex.net:443', log)
        self.assertIn('EHOSTUNREACH(113)', log)
        self.assertIn('ENETUNREACH(101)', log)
        self.assertIn('api.github.com:443', log)
        self.assertIn('TimeoutError', log)
        self.assertIn('NETWORK PROBE OK:', log)
        self.assertRegex(log, r'waited=\d+\.\d{3}s; rechecks=1')
        self.assertNotIn('NETWORK OFFLINE', log)
        stop.wait.assert_called_once_with(0)
        self.assertTrue(all(call.kwargs.get('all_errors') for call in connect.call_args_list))

    def test_one_reachable_probe_does_not_pause_requests(self):
        guard = network.NetworkGuard()
        stop = Mock(); stop.is_set.return_value = False
        with patch.object(network.socket, 'create_connection', side_effect=[OSError(errno.EHOSTUNREACH, 'No route to host'), MagicMock()]):
            self.assertFalse(guard.wait_if_offline(stop))
        stop.wait.assert_not_called()
        self.assertIn('api.github.com:443', guard.probe_success)
        self.assertIn('EHOSTUNREACH(113)', guard.probe_errors[0])

    def test_online_error_does_not_retry_forever(self):
        with patch('network.wait_if_offline', return_value=False):
            operation = Mock(side_effect=urllib.error.URLError('Remote server unavailable'))
            with self.assertRaises(urllib.error.URLError):
                network.retry_while_offline(operation)
        self.assertEqual(operation.call_count, 1)

    def test_metadata_outage_does_not_spend_retry_budget(self):
        operation = Mock(side_effect=[urllib.error.URLError('No route'), 'metadata'])
        with patch('network.wait_if_offline', return_value=True):
            self.assertEqual(network.retry_while_offline(operation), 'metadata')
        self.assertEqual(operation.call_count, 2)

    def test_remote_http_error_is_not_local_outage(self):
        operation = Mock(side_effect=urllib.error.HTTPError('hidden', 500, 'Unavailable', {}, None))
        with patch('network.wait_if_offline', side_effect=AssertionError('Unexpected local-network check')):
            with self.assertRaises(urllib.error.HTTPError):
                network.retry_while_offline(operation)

    def test_offline_wait_is_interruptible(self):
        guard = network.NetworkGuard()
        stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = True
        with patch.object(guard, 'online', return_value=False):
            with self.assertRaises(InterruptedError):
                guard.wait_if_offline(stop)

    def test_aria_resumes_same_transfer_after_local_outage(self):
        import pipeline
        with patch.object(pipeline, '_transfer_process_once', side_effect=[1, 0]) as run, \
                patch('network.wait_if_offline', return_value=True):
            self.assertEqual(pipeline.transfer_process(['aria2c'], env={}, on_tick=lambda: None), 0)
        self.assertEqual(run.call_count, 2)
