"""Admit each aria2 file through the shared measured download budget."""
import atexit
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
from threading import Event, Lock
import time
import urllib.error
import urllib.request

from upload_concurrency import SharedUploadBudget

_groups={}
_groups_lock=Lock()


def get_budgets(config):
    if not config:return None
    key=(str(Path(config['directory']).expanduser().resolve()),config['minimum'],config['maximum'])
    with _groups_lock:
        if key not in _groups:
            group={}
            try:
                for source in ('yandex','nextcloud','opencloud'):
                    group[source]=SharedUploadBudget(key[0],source,key[1],key[2],probe_zero=True)
            except BaseException:
                for budget in group.values():budget.close()
                raise
            _groups[key]=group
        return _groups[key]


@atexit.register
def close_budgets():
    for group in _groups.values():
        for budget in group.values():budget.close()
    _groups.clear()


class RPC:
    def __init__(self, port, secret):
        self.url=f'http://127.0.0.1:{port}/jsonrpc'
        self.secret=secret
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def call(self, method, *args):
        request=urllib.request.Request(self.url,json.dumps(dict(jsonrpc='2.0',id=1,
            method='aria2.'+method,params=['token:'+self.secret,*args])).encode(),
            headers={'Content-Type':'application/json'})
        with self.opener.open(request,timeout=3) as response:row=json.load(response)
        if 'error' in row:raise RuntimeError('aria2 RPC rejected '+method)
        return row['result']


class AriaBatch:
    def __init__(self, budget, stopping=None, progress=None):
        self.budget=budget
        self.stopping=stopping if stopping is not None else Event()
        self.progress=progress or {}
        self.completed={}
        self.engine_lock=Lock()
        self.stop_timeout=30
        self.fatal_code=None

    @staticmethod
    def stamp(path):
        s=path.stat();return s.st_ino,s.st_size,s.st_mtime_ns

    def entries(self, queue):
        entries=[]
        for line in queue.read_text().splitlines():
            if not line.strip():continue
            if not line[:1].isspace():
                if not line.startswith(('http://','https://')):raise ValueError('Expected HTTP download URL')
                entries.append((line,{}))
            else:
                key,separator,value=line.strip().partition('=')
                if not separator:raise ValueError('Invalid aria2 input option')
                entries[-1][1][key]=value
        result=[]
        for uri,options in entries:
            path=Path(options['dir'])/options['out']
            if path.exists() and self.completed.get(str(path))==self.stamp(path):continue
            result.append((uri,options,path))
        return result

    def stop_engine(self, process):
        with self.engine_lock:
            if process.poll() is None:
                try:process.send_signal(signal.SIGINT)
                except ProcessLookupError:pass
                try:process.wait(timeout=self.stop_timeout)
                except subprocess.TimeoutExpired:process.kill();process.wait()

    def file(self, rpc, process, entry):
        import storage
        uri,options,path=entry
        with self.budget.slot(self.stopping):
            inactive=False
            try:
                storage.check(path)
                if self.stopping.is_set():raise InterruptedError('Download interrupted')
                # An ambiguous addUri is never retried while its engine can still write.
                try:gid=rpc.call('addUri',[uri],dict(options,gid=secrets.token_hex(8)))
                except (OSError,ValueError,RuntimeError) as error:
                    raise RuntimeError('aria2 submission unconfirmed; progress retained') from error
                seen=self.progress.get(str(path),0)
                while not self.stopping.is_set():
                    storage.check(path)
                    try:row=rpc.call('tellStatus',gid,['status','completedLength','errorCode'])
                    except (OSError,ValueError,RuntimeError) as error:
                        raise RuntimeError('aria2 control channel lost; progress retained') from error
                    complete=int(row['completedLength'])
                    if seen is None:
                        # Initial zero can precede loading the existing resume controls.
                        if complete>0:seen=complete
                    else:
                        self.budget.confirmed(max(0,complete-seen));seen=max(seen,complete)
                    self.progress[str(path)]=seen
                    if row['status']=='complete':
                        inactive=True
                        self.completed[str(path)]=self.stamp(path)
                        return 0
                    if row['status'] in ('error','removed'):
                        inactive=True
                        code=int(row.get('errorCode') or 1)
                        if code==9:
                            self.fatal_code=9
                            self.stopping.set()
                            self.stop_engine(process)
                        return code
                    if process.poll() is not None:raise RuntimeError('aria2 stopped before file completion')
                    self.stopping.wait(.5)
                raise InterruptedError('Download interrupted')
            finally:
                if not inactive:
                    self.stopping.set()
                    self.stop_engine(process)

    def run(self, command, *, env, on_tick, stop_timeout=30):
        self.stop_timeout=stop_timeout
        queue=Path(next(x.split('=',1)[1] for x in command if x.startswith('--input-file=')))
        entries=self.entries(queue)
        if not entries:return 0
        with socket.socket() as candidate:
            candidate.bind(('127.0.0.1',0));port=candidate.getsockname()[1]
        secret=secrets.token_urlsafe(32);rpc=RPC(port,secret)
        args=[x for x in command if not x.startswith(('--input-file=','--optimize-concurrent-downloads=','--max-concurrent-downloads='))]
        args += ['--enable-rpc=true','--rpc-listen-all=false','--rpc-allow-origin-all=false',
                 '--rpc-listen-port='+str(port),'--rpc-secret='+secret,'--optimize-concurrent-downloads=false',
                 '--max-concurrent-downloads='+str(self.budget.maximum),'--stop-with-process='+str(os.getpid())]
        process=subprocess.Popen(args,env=env)
        pool=None;finished=False
        try:
            deadline=time.monotonic()+15
            while True:
                if self.stopping.is_set():return self.fatal_code or 130
                try:rpc.call('getVersion');break
                except (OSError,ValueError):
                    if process.poll() is not None or time.monotonic()>=deadline:
                        raise RuntimeError('aria2 RPC failed to start; progress retained')
                    self.stopping.wait(.1)
            pool=ThreadPoolExecutor(max_workers=self.budget.maximum,thread_name_prefix='download-file')
            futures={pool.submit(self.file,rpc,process,entry) for entry in entries}
            codes=[];next_tick=0
            while futures:
                if self.stopping.is_set():return self.fatal_code or 130
                self.budget.update()
                if time.monotonic()>=next_tick:
                    on_tick();next_tick=time.monotonic()+5
                done=wait(futures,timeout=.5,return_when=FIRST_COMPLETED).done
                if self.fatal_code:return self.fatal_code
                for future in done:
                    futures.remove(future)
                    try:codes.append(future.result())
                    except Exception:
                        if self.fatal_code:return self.fatal_code
                        raise
            finished=True
            return 9 if 9 in codes else 1 if any(codes) else 0
        finally:
            if not finished:self.stopping.set()
            self.stop_engine(process)
            if pool is not None:pool.shutdown(wait=True,cancel_futures=True)
