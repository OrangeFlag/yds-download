"""Targeted stop signals must let transfer children save their checkpoint."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


class SignalChecks(unittest.TestCase):
    def stop_transfer(self, signum, deep=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = '''
import download, pipeline, subprocess, sys, pathlib, hashlib
from unittest.mock import patch
root = pathlib.Path(sys.argv[1])
real = subprocess.Popen
child = """import pathlib, signal, sys, time, os
root = pathlib.Path(sys.argv[1])
def stop(*args):
    (root / 'checkpoint').write_text('saved')
    raise SystemExit(130)
signal.signal(signal.SIGINT, stop)
(root / 'ready').write_text(str(os.getpid()))
time.sleep(60)
"""
def launch(command, **kwargs):
    return real([sys.executable, '-c', child, str(root)], stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, **kwargs)
item = dict(path='/Course/a', size=1,
            sha256=hashlib.sha256(b'a').hexdigest(), file='https://example.invalid/a')
with patch.object(subprocess, 'Popen', side_effect=launch), \
        patch.object(download, 'list_course', return_value=[item]), \
        patch('recovery.recover_corrupted', return_value=[]):
    if sys.argv[2] == 'deep':
        entrypoint = lambda: download.download(dict(source='https://disk.yandex.ru/d/test', title='test'),
                            ['Course'], root, deep=True, retry_check=True, retries=0)
    else:
        entrypoint = lambda: pipeline.transfer_process(['aria2c'], env=None, on_tick=lambda: None)
    raise SystemExit(download.cli(entrypoint))
'''
            process = subprocess.Popen([sys.executable, '-c', script, directory,
                                        'deep' if deep else 'normal'],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            child_pid = None
            try:
                deadline = time.monotonic() + 5
                while not (root / 'ready').exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(.02)
                self.assertTrue((root / 'ready').exists(), 'Transfer child never started')
                child_pid = int((root / 'ready').read_text())
                process.send_signal(signum)
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 128 + signum, stderr + stdout)
                self.assertTrue((root / 'checkpoint').exists(), 'Child was killed without saving its checkpoint')
                with self.assertRaises(ProcessLookupError):
                    os.kill(child_pid, 0)
            finally:
                if process.poll() is None:
                    process.kill()
                if child_pid is not None:
                    try:
                        os.kill(child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                process.communicate()

    def test_sigterm_stops_and_reaps_transfer(self):
        self.stop_transfer(signal.SIGTERM)

    def test_deep_retry_sigint_saves_transfer_checkpoint(self):
        self.stop_transfer(signal.SIGINT, deep=True)

    def test_repeated_mixed_signals_do_not_interrupt_cleanup(self):
        script = '''
import download, os, signal
previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
def entrypoint():
    try:
        os.kill(os.getpid(), signal.SIGTERM)
    finally:
        os.kill(os.getpid(), signal.SIGINT)
        os.kill(os.getpid(), signal.SIGTERM)
        print('cleaned', flush=True)
assert download.cli(entrypoint) == 143
assert all(signal.getsignal(number) == handler for number, handler in previous.items())
'''
        result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('cleaned', result.stdout)
