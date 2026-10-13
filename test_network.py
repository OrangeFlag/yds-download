import unittest
from threading import Event
from unittest.mock import Mock, patch
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
        stop.wait.assert_called_once_with(5)

    def test_waits_until_connection_returns(self):
        guard = network.NetworkGuard()
        stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
        with patch.object(guard, 'online', side_effect=[False, False, True]):
            self.assertTrue(guard.wait_if_offline(stop))
        self.assertEqual(stop.wait.call_count, 2)

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
