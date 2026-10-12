"""Foreground Range overlap retains exclusive archive ownership and bounded requests."""
import json
from pathlib import Path
import subprocess
import sys
import time
import tempfile
from threading import Event
import unittest
from unittest.mock import patch

import download as d
import pipeline
import test_pipeline


class OverlapChecks(unittest.TestCase):
    setUp = test_pipeline.PipelineChecks.setUp
    transfer = test_pipeline.PipelineChecks.transfer
    def test_overlap_runs_ranges_during_next_course_and_hands_off_without_waiting(self):
        self.items['First']['size'] = 20
        started = Event()
        def ranges(root, event, item, retries, tuning=None, stopping=None, download_active=None, request_budget=None, metrics=None):
            self.assertEqual(tuning['range_workers'], 2)
            self.assertIsNotNone(request_budget)
            started.set()
            self.assertTrue(self.second_started.wait(2), 'Range blocked next course')
            self.assertTrue(stopping.wait(2), 'Archive waited for Range instead of handing off')
            raise InterruptedError('handoff')
        original = self.transfer
        def transfer(command, **kwargs):
            if '/Second' in (self.root / '.download/queue.txt').read_text():
                deadline = time.monotonic() + 2
                while not started.wait(.01) and time.monotonic() < deadline:
                    kwargs['on_tick']()
                self.assertTrue(started.is_set(), 'Range did not start during ordinary downloads')
            return original(command, **kwargs)
        with patch.object(d, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
             patch('pipeline.transfer_process', side_effect=transfer), \
             patch('recovery.retry_ranges', side_effect=ranges):
            self.assertEqual(d.download(self.data, ['First', 'Second'], self.root, retries=0,
                                       defer_range=True, range_budget=self.root/'budget'), 1)
        state = self.root/'.download'
        jobs = [json.loads(p.read_text()) for p in (state/'tasks').glob('*.json')]
        self.assertEqual([j['state'] for j in jobs if j['kind']=='range'], ['pending'])
        self.assertTrue(json.loads((state/'summary.json').read_text())['downloads_complete'])
        self.assertEqual(json.loads((state/'02-status.json').read_text())['verified'], 1)


class BudgetChecks(unittest.TestCase):
    def test_budget_limits_another_process_and_releases_on_exit(self):
        from request_budget import ProcessRangeBudget
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            budget = ProcessRangeBudget(root, 1, 100)
            marker = root/'acquired'
            code = "from pathlib import Path; from threading import Event; from request_budget import ProcessRangeBudget; import sys; b=ProcessRangeBudget(Path(sys.argv[1]),1,100); print('ready',flush=True)\nwith b.request(Event()): Path(sys.argv[2]).touch()\n"
            with budget.request(Event()):
                child = subprocess.Popen([sys.executable,'-c',code,str(root),str(marker)], stdout=subprocess.PIPE, text=True)
                try:
                    self.assertEqual(child.stdout.readline().strip(), 'ready')
                    self.assertFalse(marker.exists())
                finally:
                    child.stdout.close()
            self.assertEqual(child.wait(timeout=3), 0)
            self.assertTrue(marker.exists())
            with self.assertRaises(ValueError):
                ProcessRangeBudget(root, 2, 100)
