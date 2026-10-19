"""Admit each aria2 file through the shared measured download budget."""
import atexit
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import http.client
import math
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
                for source in ('yandex','nextcloud','opencloud','s3'):
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
    def __init__(self, port, secret, timeout=30, metrics=None):
        self.timeout,self.metrics=timeout,metrics
        self.url=f'http://127.0.0.1:{port}/jsonrpc'
        self.secret=secret
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def call(self, method, *args):
        request=urllib.request.Request(self.url,json.dumps(dict(jsonrpc='2.0',id=1,
            method='aria2.'+method,params=['token:'+self.secret,*args])).encode(),
            headers={'Content-Type':'application/json'})
        started=time.monotonic()
        try:
            with self.opener.open(request,timeout=self.timeout) as response:row=json.load(response)
            if 'error' in row:raise RuntimeError('aria2 RPC rejected '+method)
            return row['result']
        except Exception as error:
            if self.metrics is not None:
                self.metrics.add(aria_rpc_failures=1,aria_rpc_timeouts=int(isinstance(error,TimeoutError)
                    or isinstance(getattr(error,'reason',None),TimeoutError)))
            raise
        finally:
            if self.metrics is not None:
                self.metrics.add(aria_rpc_calls=1,aria_rpc_seconds=time.monotonic()-started)



class AriaBatch:
    def __init__(self, budget, stopping=None, progress=None, rpc_timeout=30, rpc_status_retries=2, metrics=None):
        if (not isinstance(rpc_timeout,(int,float)) or isinstance(rpc_timeout,bool)
                or not math.isfinite(rpc_timeout) or rpc_timeout<=0
                or type(rpc_status_retries) is not int or rpc_status_retries<0):
            raise ValueError('Expected positive RPC timeout and nonnegative status retries')
        self.rpc_timeout,self.rpc_status_retries,self.metrics=rpc_timeout,rpc_status_retries,metrics
        self.budget=budget
        self.stopping=stopping if stopping is not None else Event()
        self.progress=progress or {}
        self.completed={}
        self.engine_lock=Lock()
        self.stop_timeout=30
        self.fatal_code=None
        self.first_error=None
        self.error_lock=Lock()

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

    def status(self, rpc, process, gid, path):
        import storage
        for attempt in range(self.rpc_status_retries+1):
            if self.stopping.is_set():raise InterruptedError('Download interrupted')
            storage.check(path)
            try:return rpc.call('tellStatus',gid,['status','completedLength','errorCode'])
            except (OSError,ValueError,RuntimeError,http.client.HTTPException) as error:
                if self.stopping.is_set():raise InterruptedError('Download interrupted') from error
                if isinstance(error,RuntimeError) or process.poll() is not None or attempt==self.rpc_status_retries:
                    raise RuntimeError('aria2 control channel lost ('+type(error).__name__+'); progress retained') from error
                if self.metrics is not None:self.metrics.add(aria_rpc_status_retries=1)
                print(f'ARIA RPC STATUS RETRY: {type(error).__name__}; retry={attempt+1}/{self.rpc_status_retries}; same download retained',flush=True)
                if self.stopping.wait(.5):raise InterruptedError('Download interrupted')

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
                except (OSError,ValueError,RuntimeError,http.client.HTTPException) as error:
                    if self.stopping.is_set():raise InterruptedError('Download interrupted') from error
                    raise RuntimeError('aria2 submission unconfirmed ('+type(error).__name__+'); progress retained') from error
                seen=self.progress.get(str(path),0)
                while not self.stopping.is_set():
                    row=self.status(rpc,process,gid,path)
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
            except Exception as error:
                if not isinstance(error,InterruptedError) and not self.stopping.is_set():
                    with self.error_lock:
                        if self.first_error is None:self.first_error=error
                raise
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
        secret=secrets.token_urlsafe(32);rpc=RPC(port,secret,timeout=self.rpc_timeout,metrics=self.metrics)
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
                rpc.timeout=min(self.rpc_timeout,max(.001,deadline-time.monotonic()))
                try:rpc.call('getVersion');break
                except (OSError,ValueError,http.client.HTTPException):
                    if self.stopping.is_set():return self.fatal_code or 130
                    if process.poll() is not None or time.monotonic()>=deadline:
                        raise RuntimeError('aria2 RPC failed to start; progress retained')
                    self.stopping.wait(.1)
            rpc.timeout=self.rpc_timeout
            pool=ThreadPoolExecutor(max_workers=self.budget.maximum,thread_name_prefix='download-file')
            futures={pool.submit(self.file,rpc,process,entry) for entry in entries}
            codes=[];next_tick=0
            while futures:
                if self.fatal_code:return self.fatal_code
                if self.first_error is not None:raise self.first_error
                if self.stopping.is_set():return 130
                self.budget.update()
                if time.monotonic()>=next_tick:
                    on_tick();next_tick=time.monotonic()+5
                done=wait(futures,timeout=.5,return_when=FIRST_COMPLETED).done
                if self.fatal_code:return self.fatal_code
                if self.first_error is not None:raise self.first_error
                if self.stopping.is_set():return 130
                for future in done:
                    futures.remove(future)
                    try:codes.append(future.result())
                    except Exception:
                        if self.fatal_code:return self.fatal_code
                        if self.first_error is not None:raise self.first_error
                        if self.stopping.is_set():return 130
                        raise
            finished=True
            return 9 if 9 in codes else 1 if any(codes) else 0
        finally:
            if not finished:self.stopping.set()
            self.stop_engine(process)
            if pool is not None:pool.shutdown(wait=True,cancel_futures=True)
