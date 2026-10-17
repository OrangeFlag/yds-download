"""Measured transfer concurrency and a process-shared file budget."""
from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
import math
import os
from pathlib import Path
from threading import Lock, RLock
import time
import uuid


def target_speed(value):
    try:
        speed = float(value)
    except (TypeError, ValueError):
        raise ValueError('Transfer target must be a finite nonnegative Mbit/s value') from None
    if isinstance(value, bool) or not math.isfinite(speed) or speed < 0:
        raise ValueError('Transfer target must be a finite nonnegative Mbit/s value')
    return speed


class UploadConcurrency:
    def __init__(self, minimum, maximum, probe_zero=False, target_mbit_s=None):
        if not 1<=minimum<=maximum:
            raise ValueError('Expected 1 <= minimum uploads <= maximum uploads')
        self.minimum, self.maximum, self.limit = minimum, maximum, minimum
        self.probe_zero=probe_zero
        self.target_mbit_s=target_speed(os.environ.get('YDS_TRANSFER_TARGET_MBIT_S', '0')
                                      if target_mbit_s is None else target_mbit_s)
        self.lock = Lock()
        self.total = 0
        self.window = None
        self.trial = None
        self.direction = 1
        self.next_probe = 0
        self.last_rate = None
        self.last_rate_time = None

    def confirmed(self, size):
        with self.lock:
            self.total += size

    def update(self, active):
        now = time.monotonic()
        with self.lock:
            total = self.total
        # An unfinished experiment cannot be compared across a workload gap.
        if active<self.limit:
            if self.trial is not None:
                self.limit = self.trial[0]
                self.trial = None
                self.next_probe = now+300
            self.window = None
            return self.limit
        draining = active>self.limit
        if draining and (self.trial is None or active!=self.trial[0]):
            self.window = None
            return self.limit
        if self.window is None or self.window[2]!=active:
            self.window = now,total,active
            return self.limit
        started, previous, _ = self.window
        if now-started<90:
            return self.limit
        rate = (total-previous)/(now-started)
        self.last_rate = rate
        self.last_rate_time = datetime.now().astimezone().isoformat()
        self.window = now,total,active
        if draining:
            self.trial = active,rate
            print(f'TRANSFER CONCURRENCY: active={active}; next_limit={self.limit}; '
                  f'confirmed_mbit_s={rate*8/1_000_000:.2f}; draining',flush=True)
            return self.limit
        if self.target_mbit_s and rate*8/1_000_000>=self.target_mbit_s:
            self.trial=None
            self.direction=1
            print(f'TRANSFER CONCURRENCY: active={active}; next_limit={self.limit}; '
                  f'confirmed_mbit_s={rate*8/1_000_000:.2f}; target_mbit_s={self.target_mbit_s:g}; target reached',flush=True)
            return self.limit
        old_limit = self.limit
        reason = 'hold'
        if self.trial is not None:
            old, baseline = self.trial
            self.trial = None
            threshold = 11 if self.limit>old else 9
            if (baseline==0 and rate==0) or rate*10<baseline*threshold:
                self.direction = -1 if self.limit>old else 1
                self.limit = old
                self.next_probe = now+300
                reason = 'rollback'
            elif self.limit in (self.minimum,self.maximum):
                self.direction *= -1
                self.next_probe = now+300
        if self.limit==old_limit and (rate>0 or self.probe_zero) and now>=self.next_probe:
            candidate = self.limit+self.direction
            if not self.minimum<=candidate<=self.maximum:
                self.direction *= -1
                candidate = self.limit+self.direction
            if self.minimum<=candidate<=self.maximum:
                self.trial = self.limit,rate
                self.limit = candidate
                reason = 'probe'
        if self.limit!=old_limit:
            self.window = None
        print(f'TRANSFER CONCURRENCY: active={active}; limit={old_limit}; '
              f'confirmed_mbit_s={rate*8/1_000_000:.2f}; next_limit={self.limit}; {reason}',flush=True)
        return self.limit


class SharedUploadBudget:
    """One adaptive limit for all cloud processes; each waiting cloud gets a turn."""
    fields=('limit','total','window','trial','direction','next_probe','last_rate','last_rate_time')

    def __init__(self, directory, name, minimum=2, maximum=8, probe_zero=False, target_mbit_s=None):
        self.target_mbit_s=UploadConcurrency(minimum,maximum,probe_zero,target_mbit_s).target_mbit_s
        self.directory=Path(directory)
        self.directory.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.name,self.minimum,self.maximum=name,minimum,maximum
        self.probe_zero=probe_zero
        self.key=uuid.uuid4().hex
        self.lease_path=self.directory/(self.key+'.member')
        self.lease=self.lease_path.open('a')
        fcntl.flock(self.lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.mutex=RLock()
        self.confirmed_bytes=0
        self.next_update=0
        self.closed=False
        try:
            with self.state():pass
        except BaseException:
            self.lease.close();self.lease_path.unlink(missing_ok=True)
            raise

    @contextmanager
    def state(self):
        from download import write_json
        with self.mutex, (self.directory/'state.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            path=self.directory/'state.json'
            state=json.loads(path.read_text()) if path.exists() else {}
            before=json.dumps(state,sort_keys=True)
            members=state.setdefault('members',{})
            for key in list(members):
                if key==self.key:continue
                lease=self.directory/(key+'.member')
                with lease.open('a') as probe:
                    try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError:continue
                    del members[key]
                    lease.unlink(missing_ok=True)
            if not members and state.get('transfer_stats'):
                # Preserve the last durable sample after a crash, before replacing its epoch.
                self.log_stats(state['transfer_stats'],path.stat().st_mtime)
            expected=dict(minimum=self.minimum,maximum=self.maximum)
            if self.probe_zero:expected['probe_zero']=True
            if self.target_mbit_s:expected['target_mbit_s']=self.target_mbit_s
            if state.get('settings')!=expected:
                if members:raise ValueError('Shared upload limits differ between processes')
                state.clear();state.update(settings=expected,members={})
                members=state['members']
            if not members:
                controller=UploadConcurrency(self.minimum,self.maximum,self.probe_zero,self.target_mbit_s)
                state['controller']={k:getattr(controller,k) for k in self.fields}
                state['transfer_stats']=dict(epoch=uuid.uuid4().hex,sources={},last_log=None)
            member=members.setdefault(self.key,dict(name=self.name,active=0,waiting=0,confirmed_bytes=0,active_slots=[],waiting_slots=[]))
            delta=max(0,self.confirmed_bytes-member['confirmed_bytes'])
            state['controller']['total']+=delta
            stats=state.setdefault('transfer_stats',dict(epoch=uuid.uuid4().hex,sources={},last_log=None))
            stats['sources'][self.name]=stats['sources'].get(self.name,0)+delta
            member['confirmed_bytes']=max(member['confirmed_bytes'],self.confirmed_bytes)
            yield state
            for row in members.values():
                row['active']=len(row['active_slots'])
                row['waiting']=len(row['waiting_slots'])
            stamp=time.time()
            emit=not members or stats['last_log'] is None or stamp-stats['last_log']>=60
            if emit:stats['last_log']=stamp
            if json.dumps(state,sort_keys=True)!=before:
                write_json(path,state)
            if emit:
                self.log_stats(stats,stamp)

    def log_stats(self,stats,stamp):
        try:
            print('TRANSFER STATS: '+json.dumps(dict(time=datetime.fromtimestamp(stamp).astimezone().isoformat(),
                budget=str(self.directory.resolve()),epoch=stats['epoch'],sources=stats['sources'])),flush=True)
        except OSError:
            pass

    def confirmed(self, size):
        with self.mutex:self.confirmed_bytes+=size

    def update(self):
        with self.mutex:
            now=time.monotonic()
            if now<self.next_update:return
            self.next_update=now+1
            with self.state() as state:
                controller=UploadConcurrency(self.minimum,self.maximum,self.probe_zero,self.target_mbit_s)
                for key in self.fields:setattr(controller,key,state['controller'][key])
                active=sum(row['active'] for row in state['members'].values())
                if active>=controller.limit or not any(row['waiting'] for row in state['members'].values()):
                    controller.update(active)
                state['controller']={k:getattr(controller,k) for k in self.fields}
                state['time']=datetime.now().astimezone().isoformat()

    @contextmanager
    def slot(self, stopping):
        if stopping.is_set():raise InterruptedError('File transfer budget interrupted')
        token=uuid.uuid4().hex
        acquired=False
        try:
            with self.state() as state:state['members'][self.key]['waiting_slots'].append(token)
            while not stopping.is_set():
                self.update()
                with self.state() as state:
                    member=state['members'][self.key]
                    active=sum(row['active'] for row in state['members'].values())
                    reserved=sum(row['waiting']>0 and row['active']==0 for key,row in state['members'].items() if key!=self.key)
                    limit=state['controller']['limit']
                    if active<limit and (member['active']==0 or active<limit-reserved):
                        member['waiting_slots'].remove(token)
                        member['active_slots'].append(token)
                        acquired=True
                if acquired:break
                stopping.wait(.5)
            if not acquired:raise InterruptedError('File transfer budget interrupted')
            yield
        finally:
            with self.state() as state:
                member=state['members'][self.key]
                for kind in ('active_slots','waiting_slots'):
                    if token in member[kind]:member[kind].remove(token)

    def close(self):
        with self.mutex:
            if self.closed:return
            try:
                with self.state() as state:state['members'].pop(self.key,None)
            finally:
                self.closed=True
                self.lease.close();self.lease_path.unlink(missing_ok=True)

    def __enter__(self):return self

    def __exit__(self,*_):self.close()

