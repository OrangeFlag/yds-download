"""Exercise adaptive admission against actual local HTTP and aria2 RPC."""
import importlib
import importlib.util
from concurrent.futures import wait as wait_all
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
from threading import Event, Thread
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import nextcloud


class AdaptiveDownloadChecks(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec('adaptive_downloads'),'Adaptive aria2 runner is missing')
        self.ad=importlib.import_module('adaptive_downloads')
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);self.root=Path(tmp.name)
        self.content=b'lecture video data'*65536;self.requests=[]
        owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_GET(self):
                owner.requests.append((self.path,self.headers.get('Range')))
                if self.path=='/bad':self.send_error(500);return
                offset=int(self.headers['Range'].split('=')[1].split('-')[0]) if self.headers.get('Range') else 0
                self.send_response(206 if offset else 200)
                self.send_header('Content-Length',str(len(owner.content)-offset))
                self.send_header('Accept-Ranges','bytes')
                if offset:self.send_header('Content-Range',f'bytes {offset}-{len(owner.content)-1}/{len(owner.content)}')
                self.end_headers()
                try:self.wfile.write(owner.content[offset:])
                except (BrokenPipeError,ConnectionResetError):pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        Thread(target=self.server.serve_forever,daemon=True).start()
        self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)
        self.config=dict(directory=str(self.root/'budget'),minimum=2,maximum=4)
        self.env={k:v for k,v in os.environ.items() if k.lower() not in ('all_proxy','http_proxy','https_proxy','ftp_proxy')}
        self.budget=nextcloud.SharedUploadBudget(self.root/'budget','yandex',2,4,probe_zero=True)
        self.addCleanup(self.budget.close)

    def command(self, endpoint='/file'):
        self.target=self.root/'lecture with spaces.mp4'
        queue=self.root/'queue.txt'
        queue.write_text(f'http://127.0.0.1:{self.server.server_port}{endpoint}\n dir={self.root}\n out={self.target.name}\n')
        return ['aria2c','--input-file='+str(queue),'--continue=true','--always-resume=true',
            '--auto-file-renaming=false','--allow-overwrite=true','--file-allocation=none',
            '--max-connection-per-server=1','--split=1','--max-tries=1','--timeout=3',
            '--log='+str(self.root/'aria2.log'),'--log-level=info','--console-log-level=error']

    def test_real_transfer_resumes_and_counts_only_new_bytes(self):
        command=self.command();prefix=262144;self.target.write_bytes(self.content[:prefix])
        runner=self.ad.AriaBatch(self.budget,Event(),{str(self.target):prefix})
        self.assertEqual(runner.run(command,env=self.env,on_tick=lambda:None),0)
        self.assertEqual(self.target.read_bytes(),self.content)
        state=json.loads((self.root/'budget/state.json').read_text())
        self.assertEqual(state['controller']['total'],len(self.content)-prefix)
        before=len(self.requests)
        self.assertEqual(runner.run(command,env=self.env,on_tick=lambda:None),0)
        self.assertEqual(len(self.requests),before)

    def test_http_failure_is_not_reported_as_completed(self):
        result=self.ad.AriaBatch(self.budget,Event()).run(self.command('/bad'),env=self.env,on_tick=lambda:None)
        self.assertNotEqual(result,0)
        state=json.loads((self.root/'budget/state.json').read_text())
        self.assertTrue(all(r['active']==r['waiting']==0 for r in state['members'].values()))

    def test_lost_submission_response_does_not_submit_the_file_twice(self):
        original=self.ad.RPC.call;submitted=[]
        def lose(rpc,method,*args):
            result=original(rpc,method,*args)
            if method=='addUri':
                submitted.append(args)
                raise TimeoutError('Lost local RPC reply after acceptance')
            return result
        with patch.object(self.ad.RPC,'call',lose):
            with self.assertRaises(RuntimeError):
                self.ad.AriaBatch(self.budget,Event()).run(self.command(),env=self.env,on_tick=lambda:None)
        self.assertEqual(len(submitted),1)
        self.assertTrue(all(r['active']==0 for r in json.loads((self.root/'budget/state.json').read_text())['members'].values()))

    def test_status_timeout_retries_same_gid_without_releasing_slot(self):
        self.command();self.target.write_bytes(self.content);calls=[]
        runner=self.ad.AriaBatch(self.budget,Event())
        def call(method,*args):
            calls.append((method,args))
            if method=='addUri':return 'same-gid'
            state=json.loads((self.root/'budget/state.json').read_text())
            self.assertEqual(state['members'][self.budget.key]['active'],1)
            if sum(m=='tellStatus' for m,_ in calls)<3:raise TimeoutError('slow status')
            return dict(status='complete',completedLength=str(len(self.content)))
        process=SimpleNamespace(poll=lambda:None,send_signal=lambda *_:self.fail('healthy engine stopped'))
        entry=('http://example.invalid/file',dict(dir=str(self.root),out=self.target.name),self.target)
        self.assertEqual(runner.file(SimpleNamespace(call=call),process,entry),0)
        self.assertEqual([args[0] for method,args in calls if method=='tellStatus'],['same-gid']*3)
        self.assertEqual(sum(method=='addUri' for method,_ in calls),1)
        self.assertFalse(runner.stopping.is_set())

    def test_status_timeout_during_stop_does_not_replace_user_cancellation(self):
        self.command();stopping=Event();runner=self.ad.AriaBatch(self.budget,stopping)
        process=SimpleNamespace(poll=lambda:0)
        def call(method,*args):
            if method=='addUri':return 'gid'
            stopping.set();raise TimeoutError('socket timed out during shutdown')
        entry=('http://example.invalid/file',dict(dir=str(self.root),out=self.target.name),self.target)
        with self.assertRaises(InterruptedError):runner.file(SimpleNamespace(call=call),process,entry)
        self.assertIsNone(runner.first_error)

    def test_dead_engine_status_timeout_is_not_retried(self):
        self.command();calls=[];runner=self.ad.AriaBatch(self.budget,Event())
        def call(method,*args):
            calls.append(method)
            if method=='addUri':return 'gid'
            raise TimeoutError('engine unresponsive')
        process=SimpleNamespace(poll=lambda:0)
        entry=('http://example.invalid/file',dict(dir=str(self.root),out=self.target.name),self.target)
        with self.assertRaisesRegex(RuntimeError,'control channel lost'):
            runner.file(SimpleNamespace(call=call),process,entry)
        self.assertEqual(calls,['addUri','tellStatus'])

    def test_live_unresponsive_engine_status_retries_are_bounded(self):
        self.command();calls=[];runner=self.ad.AriaBatch(self.budget,Event())
        process=SimpleNamespace(running=True)
        process.poll=lambda:None if process.running else 0
        process.send_signal=lambda *_:setattr(process,'running',False)
        process.wait=lambda **_:0
        def call(method,*args):
            calls.append(method)
            if method=='addUri':return 'gid'
            raise TimeoutError('persistent slow RPC')
        entry=('http://example.invalid/file',dict(dir=str(self.root),out=self.target.name),self.target)
        with self.assertRaisesRegex(RuntimeError,'control channel lost'):
            runner.file(SimpleNamespace(call=call),process,entry)
        self.assertEqual(calls,['addUri']+['tellStatus']*3)
        self.assertFalse(process.running)

    def test_permanent_rpc_rejection_is_not_retried(self):
        self.command();calls=[];runner=self.ad.AriaBatch(self.budget,Event())
        process=SimpleNamespace(poll=lambda:0)
        def call(method,*args):
            calls.append(method)
            if method=='addUri':return 'gid'
            raise RuntimeError('RPC rejected tellStatus')
        entry=('http://example.invalid/file',dict(dir=str(self.root),out=self.target.name),self.target)
        with self.assertRaises(RuntimeError):runner.file(SimpleNamespace(call=call),process,entry)
        self.assertEqual(calls,['addUri','tellStatus'])

    def test_rpc_timeout_and_metrics_describe_existing_calls(self):
        import io
        from collections import Counter
        counters=Counter();rpc=self.ad.RPC(1,'never logged',timeout=17,metrics=SimpleNamespace(add=counters.update))
        with patch.object(rpc.opener,'open',return_value=io.BytesIO(b'{"result": {}}')) as opened:
            self.assertEqual(rpc.call('getVersion'),{})
            self.assertEqual(opened.call_args.kwargs['timeout'],17)
        with patch.object(rpc.opener,'open',side_effect=TimeoutError('slow local server')):
            with self.assertRaises(TimeoutError):rpc.call('tellStatus','gid')
        self.assertEqual(counters['aria_rpc_calls'],2)
        self.assertEqual(counters['aria_rpc_failures'],1)
        self.assertEqual(counters['aria_rpc_timeouts'],1)
        self.assertGreaterEqual(counters['aria_rpc_seconds'],0)

    def test_startup_rpc_timeout_is_capped_by_remaining_deadline(self):
        command=self.command();clock=[0.0];observed=[]
        process=SimpleNamespace(running=True)
        process.poll=lambda:None if process.running else 0
        process.send_signal=lambda *_:setattr(process,'running',False)
        process.wait=lambda **_:0
        rpc=SimpleNamespace()
        def call(method,*args):
            observed.append(rpc.timeout);clock[0]+=rpc.timeout;raise TimeoutError('startup unresponsive')
        rpc.call=call
        with patch.object(self.ad.subprocess,'Popen',return_value=process),patch.object(self.ad,'RPC',return_value=rpc),patch.object(self.ad.time,'monotonic',side_effect=lambda:clock[0]):
            with self.assertRaisesRegex(RuntimeError,'failed to start'):
                self.ad.AriaBatch(self.budget,Event(),rpc_timeout=30).run(command,env=self.env,on_tick=lambda:None)
        self.assertEqual(observed,[15])
        self.assertFalse(process.running)

    def test_zero_speed_can_probe_but_no_gain_rolls_back(self):
        with patch('time.monotonic',return_value=100) as clock,self.budget.slot(Event()),self.budget.slot(Event()):
            clock.return_value=101;self.budget.update()
            clock.return_value=191;self.budget.update()
            self.assertEqual(json.loads((self.root/'budget/state.json').read_text())['controller']['limit'],3)
            with self.budget.slot(Event()):
                clock.return_value=192;self.budget.update()
                clock.return_value=282;self.budget.update()
                self.assertEqual(json.loads((self.root/'budget/state.json').read_text())['controller']['limit'],2)

    def test_network_retry_does_not_count_received_prefix_twice(self):
        self.command();self.target.write_bytes(b'x'*200)
        runner=self.ad.AriaBatch(self.budget,Event(),{str(self.target):100})
        rows=iter([dict(status='error',completedLength='150',errorCode='1'),
                   dict(status='complete',completedLength='200',errorCode='0')])
        rpc=SimpleNamespace(call=lambda method,*_: 'abc' if method=='addUri' else next(rows))
        process=SimpleNamespace(poll=lambda:None)
        entry=('http://example.invalid/video',dict(dir=str(self.root),out=self.target.name),self.target)
        self.assertEqual(runner.file(rpc,process,entry),1)
        self.assertEqual(runner.file(rpc,process,entry),0)
        self.assertEqual(json.loads((self.root/'budget/state.json').read_text())['controller']['total'],100)

    def test_ambiguous_submission_waits_for_engine_exit_before_releasing_slot(self):
        self.command();observed=[]
        class Process:
            running=True
            def poll(self):return None if self.running else 0
            def send_signal(self,_):pass
            def wait(process,timeout=None):
                state=json.loads((self.root/'budget/state.json').read_text())
                observed.append(sum(r['active'] for r in state['members'].values()))
                process.running=False;return 0
        process=Process()
        def call(*_):raise TimeoutError('accepted then lost reply')
        rpc=SimpleNamespace(call=call)
        entry=('http://example.invalid/video',dict(dir=str(self.root),out=self.target.name),self.target)
        with self.assertRaises(RuntimeError):self.ad.AriaBatch(self.budget,Event()).file(rpc,process,entry)
        self.assertFalse(process.running)
        self.assertEqual(observed,[1])

    def test_unknown_resume_prefix_waits_for_first_nonzero_status(self):
        self.command()
        runner=self.ad.AriaBatch(self.budget,Event(),{str(self.target):None})
        rows=iter([dict(status='active',completedLength='0'),dict(status='active',completedLength='150'),
                   dict(status='error',completedLength='200',errorCode='1')])
        rpc=SimpleNamespace(call=lambda method,*_: 'abc' if method=='addUri' else next(rows))
        entry=('http://example.invalid/video',dict(dir=str(self.root),out=self.target.name),self.target)
        self.assertEqual(runner.file(rpc,SimpleNamespace(poll=lambda:None),entry),1)
        self.assertEqual(json.loads((self.root/'budget/state.json').read_text())['controller']['total'],50)

    def test_disk_full_stops_engine_without_turning_into_user_interrupt(self):
        self.command();stopping=Event();runner=self.ad.AriaBatch(self.budget,stopping)
        class Process:
            running=True
            def poll(self):return None if self.running else 0
            def send_signal(self,_):self.running=False
            def wait(self,timeout=None):return 0
        process=Process()
        rpc=SimpleNamespace(call=lambda method,*_: 'abc' if method=='addUri' else dict(status='error',completedLength='0',errorCode='9'))
        entry=('http://example.invalid/video',dict(dir=str(self.root),out=self.target.name),self.target)
        self.assertEqual(runner.file(rpc,process,entry),9)
        self.assertTrue(stopping.is_set())
        self.assertFalse(process.running)
        self.assertEqual(runner.fatal_code,9)

    def test_rpc_failure_wins_over_sibling_budget_interruption(self):
        import time
        command=self.command();queue=self.root/'queue.txt'
        queue.write_text(queue.read_text()+f'http://127.0.0.1:{self.server.server_port}/file\n dir={self.root}\n out=second.mp4\n')
        release=Event()
        class Process:
            running=True
            def poll(this):return None if this.running else 0
            def send_signal(this, signal):pass
            def wait(this, timeout=None):time.sleep(.6);this.running=False;return 0
        def rpc(method,*args):
            if method in ('getVersion','addUri'):return 'gid'
            if not release.wait(3):raise AssertionError('Batch did not start')
            raise TimeoutError('RPC response timed out')
        with nextcloud.SharedUploadBudget(self.root/'single-budget','yandex',1,2,probe_zero=True) as budget:
            runner=self.ad.AriaBatch(budget,Event())
            with patch.object(self.ad.subprocess,'Popen',return_value=Process()),patch.object(self.ad,'RPC',return_value=SimpleNamespace(call=rpc)):
                with self.assertRaisesRegex(RuntimeError,'control channel lost') as error:
                    runner.run(command,env=self.env,on_tick=release.set)
            self.assertIsInstance(error.exception.__cause__,TimeoutError)
        self.assertTrue(runner.stopping.is_set())

    def test_disk_full_wins_over_sibling_interruption_during_result_collection(self):
        command=self.command();queue=self.root/'queue.txt'
        queue.write_text(queue.read_text()+f'http://127.0.0.1:{self.server.server_port}/file\n dir={self.root}\n out=second.mp4\n')
        stopping=Event();release=Event();runner=self.ad.AriaBatch(self.budget,stopping)
        def file(rpc,process,entry):
            if not release.wait(3):raise AssertionError('No result collection')
            if entry[2].name=='second.mp4':raise InterruptedError('Sibling stopped after disk full')
            return 9
        def finish(futures,**kwargs):
            runner.fatal_code=9;stopping.set();release.set()
            done=wait_all(futures,timeout=3).done
            self.assertEqual(len(done),2)
            return SimpleNamespace(done=sorted(done,key=lambda f:f.exception() is None))
        with patch.object(runner,'file',file),patch.object(self.ad,'wait',finish):
            self.assertEqual(runner.run(command,env=self.env,on_tick=lambda:None),9)
