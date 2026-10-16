"""Cloud uploaders share slots and count accepted payload, including resumed TUS chunks."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from threading import Event, Thread
import unittest
from unittest.mock import patch

import download
import nextcloud


class SharedUploadChecks(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.path=Path(tmp.name)

    def budget(self, name, minimum=2, maximum=8):
        cls=getattr(nextcloud,'SharedUploadBudget',None)
        self.assertIsNotNone(cls,'Both uploaders need a shared automatic upload budget')
        result=cls(self.path,name,minimum,maximum)
        self.addCleanup(result.close)
        return result

    def state(self):return json.loads((self.path/'state.json').read_text())

    def test_two_clouds_share_the_initial_limit_and_unblock_after_release(self):
        a,b=self.budget('nextcloud'),self.budget('opencloud')
        stop=Event();entered=Event()
        with a.slot(stop):
            with b.slot(stop):
                def extra():
                    try:
                        with a.slot(stop):entered.set()
                    except InterruptedError:pass
                worker=Thread(target=extra);worker.start()
                self.assertFalse(entered.wait(.15))
                self.assertEqual(sum(x['active'] for x in self.state()['members'].values()),2)
            try:self.assertTrue(entered.wait(3))
            finally:stop.set();worker.join(3)
            self.assertFalse(worker.is_alive())

    def test_aggregate_confirmed_speed_drives_one_controller(self):
        a,b=self.budget('nextcloud'),self.budget('opencloud')
        stop=Event()
        with patch('time.monotonic',return_value=100) as clock,a.slot(stop),b.slot(stop):
            clock.return_value=101;a.update()
            a.confirmed(900);b.confirmed(1800)
            clock.return_value=102;a.update()
            clock.return_value=191;b.update()
            state=self.state()
            self.assertEqual(state['controller']['total'],2700)
            self.assertEqual(state['controller']['limit'],3)
            self.assertAlmostEqual(state['controller']['last_rate'],30)
            self.assertEqual(sum(x['confirmed_bytes'] for x in state['members'].values()),2700)

    def test_stopping_a_waiter_does_not_consume_a_slot(self):
        a=self.budget('nextcloud');stop=Event()
        with a.slot(stop),a.slot(stop):
            stop.set()
            with self.assertRaises(InterruptedError):
                with a.slot(stop):self.fail('Stopped upload admitted')
            member=next(iter(self.state()['members'].values()))
            self.assertEqual((member['active'],member['waiting']),(2,0))
        self.assertEqual(next(iter(self.state()['members'].values()))['active'],0)

    def test_crashed_process_releases_its_slot(self):
        a=self.budget('nextcloud')
        child=subprocess.Popen([sys.executable,'-u','-c',
            'from upload_concurrency import SharedUploadBudget\nfrom threading import Event\nimport sys,time\n'
            'b=SharedUploadBudget(sys.argv[1],"opencloud",2,8)\n'
            'with b.slot(Event()):\n print("READY",flush=True)\n time.sleep(30)\n',str(self.path)],stdout=subprocess.PIPE,text=True)
        self.addCleanup(lambda:child.poll() is None and child.kill())
        try:
            self.assertEqual(child.stdout.readline().strip(),'READY')
            child.kill();child.wait(3)
            with a.slot(Event()),a.slot(Event()):
                self.assertEqual(len(self.state()['members']),1)
        finally:
            if child.poll() is None:child.kill();child.wait()
            child.stdout.close()

    def test_mismatched_configuration_is_rejected(self):
        self.budget('nextcloud')
        with self.assertRaises(ValueError):self.budget('opencloud',maximum=4)

    def test_failed_admission_write_keeps_existing_slots_before_or_after_commit(self):
        a=self.budget('nextcloud');real_write=download.write_json
        with a.slot(Event()):
            for after_commit in (False,True):
                with self.subTest(after_commit=after_commit):
                    failed=False
                    def fail(path,state):
                        nonlocal failed
                        if not failed and sum(x['active'] for x in state['members'].values())==2:
                            failed=True
                            if after_commit:real_write(path,state)
                            raise OSError(28,'Injected failed admission write')
                        real_write(path,state)
                    with patch.object(download,'write_json',side_effect=fail):
                        with self.assertRaises(OSError):
                            with a.slot(Event()):self.fail('Uncommitted slot admitted')
                    member=next(iter(self.state()['members'].values()))
                    self.assertEqual((member['active'],member['waiting']),(1,0))

    def test_confirmed_bytes_are_not_lost_or_doubled_after_write_failure(self):
        a=self.budget('nextcloud');real_write=download.write_json
        for after_commit in (False,True):
            with self.subTest(after_commit=after_commit):
                a.confirmed(10)
                def fail(path,state):
                    if after_commit:real_write(path,state)
                    raise OSError(28,'Injected failed statistics write')
                with patch.object(download,'write_json',side_effect=fail):
                    with self.assertRaises(OSError):
                        with a.state():pass
                with a.state():pass
        self.assertEqual(self.state()['controller']['total'],20)
