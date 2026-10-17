"""Explicit, resumable OpenCloud uploads; no background worker is enabled."""
import argparse
from contextlib import ExitStack
import base64
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path, PurePosixPath
import random
import re
import signal
from threading import Event
import time
import urllib.error
import urllib.parse
import urllib.request

import download as d
import file_hashes
import nextcloud
import network
import storage


class Client(nextcloud.Share):
    def __init__(self, server, username, password, timeout=120):
        parsed=urllib.parse.urlsplit(server)
        if (parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.path not in ('','/') or parsed.query or parsed.fragment):
            raise ValueError('Expected an HTTPS OpenCloud server origin')
        self.server=server.rstrip('/')
        self.base=None
        self.subfolder=''
        self.nickname=username
        self.timeout=timeout
        self.authorization='Basic '+base64.b64encode((username+':'+password).encode()).decode()
        self.opener=urllib.request.build_opener(nextcloud.NoRedirect())

    def safe_url(self, url):
        url=urllib.parse.urljoin(self.server+'/',url)
        parsed=urllib.parse.urlsplit(url)
        if (parsed.scheme!='https' or parsed.netloc!=urllib.parse.urlsplit(self.server).netloc
                or parsed.username or parsed.password or parsed.fragment
                or any(ord(c)<32 for c in url)):
            raise ValueError('OpenCloud returned an unsafe URL')
        return url

    def discover(self):
        with self.request('GET',self.server+'/graph/v1.0/me/drives') as response:
            drives=json.load(response).get('value',[])
        matches=[x for x in drives if x.get('name')=='ysda-videos' and x.get('driveType')=='project']
        if len(matches)!=1:
            raise ValueError('Expected exactly one project Space named ysda-videos; refusing personal storage')
        url=self.safe_url(matches[0]['root']['webDavUrl'])
        path=urllib.parse.unquote(urllib.parse.urlsplit(url).path)
        if not re.fullmatch(r'/(?:remote.php/)?dav/spaces/[^/]+/?',path):
            raise ValueError('Invalid Space WebDAV root')
        self.base=url.rstrip('/')+'/'

    def request(self, method, path='', data=None, headers=None):
        url=self.safe_url(path) if urllib.parse.urlsplit(path).scheme else self.url(path)
        request=urllib.request.Request(url,data=data,method=method,
            headers={'Authorization':self.authorization,**(headers or {})})
        return self.opener.open(request,timeout=self.timeout)


def hashes(row):
    return {kind.lower():value.lower() for kind,value in re.findall(
        r'\b(SHA256|SHA1|MD5):([a-fA-F0-9]+)', ' '.join(row.get('checksums',[])),re.I)}


class Uploader(nextcloud.Sync):
    ledger_name='opencloud-uploads'

    def __init__(self, client, stopping, chunk_size=10_000_000, retries=2, transfers=1):
        super().__init__(None,None,stopping,nickname=client.nickname,transfers=transfers,share=client)
        if not client.nickname or client.nickname in ('.','..') or any(c in client.nickname for c in '/\\'):
            raise ValueError('Invalid OpenCloud folder name')
        if chunk_size<1 or retries<0:
            raise ValueError('Expected positive chunk size and non-negative retries')
        self.chunk_size,self.retries=chunk_size,retries
        self.folder='ingest/'+client.nickname
        self.index=None
        self.active_hashes=set()
        self.known_folders=set()

    def jobs(self, roots):
        jobs=super().jobs(roots) or []
        originals={(job[2]['source'],job[3]['path']) for job in jobs if job[5]=='original'}
        jobs=[job for job in jobs if job[5]=='original' or (job[2]['source'],job[3]['path']) not in originals]
        sources={self.read(root/'.download/source.json',{}).get('source'):(root,self.read(root/'.download/source.json',{})) for root in roots
                 if not self.read(root/'.download/catalog.json',{}).get('root')}
        seen=set()
        for root in roots:
            for row in self.read(root/'.download/recordings.json',[]):
                if row.get('source') not in sources or not row.get('match'):
                    continue
                owner,data=sources[row['source']]
                match=row['match'];path=match['course_path'].rstrip('/')+'/'+match['file']
                if (row['source'],path) in originals:
                    continue
                local=storage.saved_local(root, row['local'])
                if (local in seen or not local.is_file() or Path(str(local)+'.aria2').exists()
                        or (local.stat().st_size,local.stat().st_mtime_ns)!=(row['size'],row['mtime_ns'])):
                    continue
                if not re.fullmatch('[0-9a-f]{64}',row.get('actual_sha256','')):
                    continue
                item=dict(path=path,size=row['original_size'],sha256=row['original_sha256'],payload_sha256=row['actual_sha256'])
                paths=set()
                for manifest in (owner/'.download').glob('*-manifest.json'):
                    for original in self.read(manifest,[]):
                        if (original['path'],original.get('size'),original.get('sha256'))==(path,item['size'],item['sha256']):
                            item.update({kind:original[kind] for kind in ('sha1','md5') if original.get(kind)})
                        if (PurePosixPath(original['path']).parent==PurePosixPath(path).parent
                                and PurePosixPath(original['path']).suffix.casefold() in nextcloud.VIDEO):
                            paths.add(original['path'])
                ordinal=next((i for i,p in enumerate(sorted(paths,key=nextcloud.normalized),1) if p==path),None)
                if ordinal is None:
                    continue
                jobs.append((False,owner,data,item,local,'camrip',row['size'],None,None,ordinal,row['mtime_ns']))
                seen.add(local)
        return jobs

    def checkpoint(self, job, *, local=None):
        local=str(storage.portable_local(job[1],job[4])) if local is None else str(local)
        key=hashlib.sha256(json.dumps([self.share.url(),self.folder,job[2]['source'],job[3]['path'],job[5],
                                     local,job[6],job[10]],ensure_ascii=False).encode()).hexdigest()
        return job[1]/'.download/opencloud-uploads'/key[:2]/(key+'.json')

    def record(self, job):
        p=self.checkpoint(job)
        if p.exists():
            return json.loads(p.read_text())
        locals=[job[4]]
        history=self.read(job[1]/'.download/opencloud-uploads/mount-history.json',{})
        if history and job[4].is_relative_to(history['mount']):
            relative=job[4].relative_to(history['mount'])
            locals.extend(Path(root)/relative for root in history['previous'])
        # shortcut: moves without saved mount history restart legacy uploads; retain history for future moves.
        for local in dict.fromkeys(locals):
            previous=self.checkpoint(job,local=local)
            if previous.exists():
                return json.loads(previous.read_text())
        return {}

    def save_record(self, job, row):
        path=self.checkpoint(job)
        storage.check(path)
        path.parent.parent.mkdir(mode=0o700,exist_ok=True)
        path.parent.mkdir(mode=0o700,exist_ok=True)
        row['time']=datetime.now().astimezone().isoformat()
        d.write_json(path,row)
        storage.sync_directory(path.parent.parent)
        public={key:value for key,value in row.items() if key!='session_url'}
        d.append_events(job[1]/'.download/opencloud-uploads.jsonl',[public])

    def scan(self):
        result=[];folders=[''];seen=set()
        while folders:
            if self.stopping.is_set():raise InterruptedError('OpenCloud scan interrupted')
            folder=folders.pop()
            if folder in seen:continue
            seen.add(folder)
            self.known_folders.add(folder)
            rows=network.retry_request(lambda:self.share.listing(folder),self.stopping,target='OpenCloud catalog '+folder)
            for row in rows:
                path=row.get('download_path',row['path'])
                if row['directory']:
                    if path and path!=folder:folders.append(path)
                elif not PurePosixPath(path).name.startswith('.yds-'):
                    result.append(row)
        return result

    def remote_digest(self, row):
        digest=hashlib.sha256();total=0
        with self.share.request('GET',row.get('download_path',row['path'])) as response:
            while chunk:=response.read(1024**2):
                if self.stopping.is_set():raise InterruptedError('OpenCloud hash interrupted')
                digest.update(chunk);total+=len(chunk)
        if total!=row['size']:raise RuntimeError('Incomplete OpenCloud verification read')
        result=digest.hexdigest()
        row.setdefault('checksums',[]).append('SHA256:'+result)
        return result

    def exists(self, digest, sha1=None, md5=None):
        return any(hashes(row).get('sha256')==digest or sha1 is not None and hashes(row).get('sha1')==sha1
                   or md5 is not None and hashes(row).get('md5')==md5
                   for row in self.index)

    def name_exists(self, name, size):
        def plain(value):
            return nextcloud.normalized(re.sub(r'^\d+\s*-\s*','',PurePosixPath(value).name))
        return any(row['size']==size and plain(row['path'])==plain(name) and not hashes(row)
                   for row in self.index)

    def original_exists(self, item, root=None, source=None):
        if self.exists(item['sha256'],item.get('sha1')):return True
        md5=item.get('md5')
        path=None
        if root is not None and source:
            identity=dict(source=source,path=item['path'],size=item['size'],sha256=item['sha256'])
            key=hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()
            path=root/'.download/opencloud-source-hashes'/(key+'.json')
            cached=self.read(path,{})
            if not md5 and all(cached.get(k)==v for k,v in identity.items()):md5=cached.get('md5')
        if md5:
            item['md5']=md5.lower()
        if md5 and any(hashes(row).get('md5')==md5.lower() for row in self.index):return True
        if self.name_exists(item['path'],item['size']):return True
        candidates=[row for row in self.index if hashes(row).get('md5')]
        if not candidates:return False
        if not md5 and path is not None:
            if not md5 and any(row['size']==item['size'] for row in candidates):
                try:
                    metadata=d.api(source,item['path'],stopping=self.stopping)
                    if (metadata.get('size'),metadata.get('sha256'))==(item['size'],item['sha256']):
                        md5=metadata.get('md5')
                    if md5 and re.fullmatch('[0-9a-fA-F]{32}',md5):
                        path.parent.mkdir(mode=0o700,exist_ok=True)
                        d.write_json(path,dict(identity,md5=md5.lower()))
                except (OSError,http.client.HTTPException,ValueError) as error:
                    if isinstance(error,InterruptedError) or storage.disk_error(error):raise
                    print(f'OPENCLOUD METADATA WARNING: {item["path"]}; {type(error).__name__}; HTTP={getattr(error,"code",None)}',flush=True)
        if md5 and re.fullmatch('[0-9a-fA-F]{32}',md5):
            item['md5']=md5.lower()
            return any(hashes(row)['md5']==md5.lower() for row in candidates)
        return False

    def confirm_publication(self, row, stage, sha1):
        deadline=time.monotonic()+getattr(self.share,'timeout',120)
        delay=1
        while True:
            if self.stopping.is_set():raise InterruptedError('OpenCloud publication interrupted')
            listing=self.share.listing(str(PurePosixPath(stage).parent))
            for path in (row['target'],stage):
                found=next((x for x in listing if x['path']==path and not x['directory']),None)
                if found is None:continue
                checks=hashes(found)
                expected=dict(sha256=row['payload_sha256'],sha1=sha1,md5=row['payload_md5'])
                comparable={key:value for key,value in checks.items() if key in expected}
                if (found['size']!=row['size'] or any(value!=expected[key] for key,value in comparable.items())
                        or not comparable and self.remote_digest(found)!=row['payload_sha256']):
                    raise FileExistsError('OpenCloud target checksum conflict; refusing overwrite')
                return path
            remaining=deadline-time.monotonic()
            if remaining<=0:raise RuntimeError('OpenCloud upload not published yet; session retained')
            if self.stopping.wait(min(remaining,delay)*random.uniform(.75,1)):
                raise InterruptedError('OpenCloud publication interrupted')
            delay=min(5,delay*2)

    def run_once(self, roots, upload=False, limit=None):
        jobs=self.jobs(roots)
        print(f'OPENCLOUD PLAN: {len(jobs)} videos; upload={upload}',flush=True)
        if not upload:
            for job in jobs[:limit or 10]:print(f'OPENCLOUD CANDIDATE: {job[5]}; {job[6]} bytes; {job[3]["path"]}',flush=True)
            return 0
        if isinstance(self.share,Client) and self.share.base is None:self.share.discover()
        if self.index is None:self.index=self.scan()
        jobs.sort(key=lambda job:not bool(self.record(job).get('session_url')))
        errors=0;uploaded=0;pending=iter(jobs)
        with ThreadPoolExecutor(max_workers=self.transfers,thread_name_prefix='opencloud-upload') as pool:
            futures={}
            try:
                while True:
                    if self.upload_budget:self.upload_budget.update()
                    while (len(futures)<self.transfers and not self.stopping.is_set()
                           and (limit is None or uploaded+len(futures)<limit)):
                        job=next(pending,None)
                        if job is None:break
                        futures[pool.submit(self.run_transfer,job)]=job
                    if not futures:break
                    for future in wait(futures,timeout=.25,return_when=FIRST_COMPLETED).done:
                        job=futures.pop(future)
                        try:
                            uploaded+=bool(future.result())
                        except (OSError,http.client.HTTPException,RuntimeError,ValueError) as error:
                            if storage.disk_error(error):raise
                            errors+=1
                            print(f'OPENCLOUD ERROR: {job[3]["path"]}; {type(error).__name__}; HTTP={getattr(error,"code",None)}',flush=True)
            except BaseException:
                self.stopping.set()
                for future in futures:future.cancel()
                raise
        return 1 if errors else 0

    def transfer(self, job):
        _,root,data,item,local,variant,size,percent,_,ordinal,mtime=job
        storage.check(local)
        stat=local.stat();fingerprint=(stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns)
        if (stat.st_size,stat.st_mtime_ns)!=(size,mtime):raise RuntimeError('Local video changed')
        row=self.record(job)
        if row.get('state') in ('uploaded','existing'):return False
        relative=PurePosixPath(nextcloud.remote_path(data,item))
        suffix=(f' (partially available {percent:.2f}%)' if variant=='partial' else ' (CamRip, completeness unverified)' if variant=='camrip' else '')
        name=d.local_path(Path('/'),f'/{ordinal:03} - {relative.stem}{suffix}{local.suffix}').name
        target=self.folder+'/'+str(relative.with_name(name))
        original_exists=self.original_exists(item,root,data['source'])
        if original_exists or self.name_exists(name,size):
            if row:
                row.update(state='existing',error_type=None,http_status=None,error_reason=None,error_errno=None)
                self.save_record(job,row)
            print('OPENCLOUD EXISTS (metadata): '+item['path'],flush=True);return False
        shared=file_hashes.load(local,root=root)
        cached=row.get('local_fingerprint')==list(fingerprint) and all(
            isinstance(row.get(key),str) and re.fullmatch('[0-9a-f]{'+str(length)+'}',row[key])
            for key,length in (('payload_sha256',64),('payload_sha1',40),('payload_md5',32)))
        if shared:
            actual,sha1,md5=(shared[key] for key in file_hashes.ALGORITHMS)
            print('OPENCLOUD HASH CACHED: '+item['path'],flush=True)
        elif cached:
            actual,sha1,md5=(row[key] for key in ('payload_sha256','payload_sha1','payload_md5'))
            file_hashes.save(local,dict(zip(file_hashes.ALGORITHMS,(actual,sha1,md5))),fingerprint,root=root)
            print('OPENCLOUD HASH CACHED: '+item['path'],flush=True)
        else:
            print('OPENCLOUD HASH START: '+item['path']+f'; bytes={size}',flush=True)
            started=time.monotonic()
            def progress(amount,total):
                print(f'OPENCLOUD HASH PROGRESS: {item["path"]}; {amount}/{total} bytes',flush=True)
            shared=file_hashes.calculate(local,self.stopping,root=root,progress=progress)
            actual,sha1,md5=(shared[key] for key in file_hashes.ALGORITHMS)
            print(f'OPENCLOUD HASH DONE: {item["path"]}; bytes={size}; seconds={time.monotonic()-started:.3f}',flush=True)
        if file_hashes.fingerprint(local)!=list(fingerprint):raise RuntimeError('Local video changed during hashing')
        if variant=='original' and actual!=item['sha256'] or variant=='camrip' and actual!=item['payload_sha256']:
            raise RuntimeError('Local SHA-256 mismatch')
        stage=str(PurePosixPath(target).parent/('.yds-'+actual+'.upload'))
        if row and (row['payload_sha256'],row['target'])!=(actual,target):
            raise RuntimeError('Upload identity changed; previous session retained')
        row=row or dict(source=data['source'],path=item['path'],variant=variant,target=target,
                        payload_sha256=actual,size=size,sent_bytes=0,state='pending')
        row.update(payload_md5=md5,payload_sha1=sha1,local_fingerprint=list(fingerprint))
        if self.exists(actual,sha1,md5):
            row.update(state='existing',error_type=None,http_status=None,error_reason=None,error_errno=None)
            self.save_record(job,row)
            print('OPENCLOUD EXISTS: '+item['path'],flush=True);return False
        with self.index_lock:
            if actual in self.active_hashes or self.exists(actual,sha1,md5):
                print('OPENCLOUD DUPLICATE ACTIVE/EXISTS: '+item['path'],flush=True);return False
            self.active_hashes.add(actual)
        try:
            self.save_record(job,row)
            with self.index_lock:
                for folder in reversed(PurePosixPath(target).parents):
                    folder=str(folder)
                    if folder=='.' or folder in self.known_folders:continue
                    try:
                        with self.share.request('MKCOL',folder):pass
                    except urllib.error.HTTPError as error:
                        if error.code!=405:raise
                    self.known_folders.add(folder)
            if not row.get('session_url'):
                metadata={'filename':PurePosixPath(stage).name,'checksum':'SHA1 '+sha1,'mtime':str(mtime//10**9)}
                headers={'Tus-Resumable':'1.0.0','Upload-Length':str(size),'Content-Length':'0',
                         'Upload-Metadata':','.join(k+' '+base64.b64encode(v.encode()).decode() for k,v in metadata.items())}
                row['state']='creating';self.save_record(job,row)
                with self.share.request('POST',str(PurePosixPath(stage).parent),data=b'',headers=headers) as response:
                    if response.status!=201:raise RuntimeError('OpenCloud did not create upload')
                    row['session_url']=self.share.safe_url(response.headers['Location'])
                row['state']='uploading';self.save_record(job,row)
            print('OPENCLOUD UPLOAD START: '+target,flush=True)
            failures=0;offset=None;confirmed_offset=None
            with local.open('rb') as stream:
                while row['sent_bytes']<size:
                    if self.stopping.is_set():raise InterruptedError('OpenCloud upload interrupted')
                    storage.check(local)
                    after=local.stat()
                    if (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)!=fingerprint:raise RuntimeError('Local video changed')
                    try:
                        if offset is None:
                            try:
                                with self.share.request('HEAD',row['session_url'],headers={'Tus-Resumable':'1.0.0'}) as response:
                                    offset=int(response.headers['Upload-Offset'])
                                    if int(response.headers.get('Upload-Length',size))!=size or not 0<=offset<=size:
                                        raise RuntimeError('Invalid OpenCloud upload offset/length')
                            except urllib.error.HTTPError as error:
                                if error.code not in (404,410):raise
                                self.confirm_publication(row,stage,sha1)
                                offset=size
                            if self.upload_budget and confirmed_offset is not None:
                                self.upload_budget.confirmed(max(0,offset-confirmed_offset))
                            confirmed_offset=offset
                            row.update(sent_bytes=offset,state='uploading');self.save_record(job,row)
                        if offset==size:break
                        stream.seek(offset);chunk=stream.read(min(self.chunk_size,size-offset))
                        if len(chunk)!=min(self.chunk_size,size-offset):raise RuntimeError('Local video truncated')
                        with self.share.request('PATCH',row['session_url'],data=chunk,headers={
                            'Tus-Resumable':'1.0.0','Upload-Offset':str(offset),'Content-Type':'application/offset+octet-stream'}) as response:
                            if response.status!=204 or int(response.headers['Upload-Offset'])!=offset+len(chunk):
                                raise RuntimeError('OpenCloud did not confirm the complete chunk')
                        offset+=len(chunk)
                        if self.upload_budget:self.upload_budget.confirmed(len(chunk))
                        confirmed_offset=offset
                        row.update(sent_bytes=offset,state='uploading');self.save_record(job,row)
                        print(f'OPENCLOUD UPLOAD PROGRESS: {target}; {row["sent_bytes"]}/{size} bytes',flush=True)
                        failures=0
                    except (OSError,http.client.HTTPException) as error:
                        offset=None
                        if storage.disk_error(error) or isinstance(error,urllib.error.HTTPError) and error.code not in (408,409,429,500,502,503,504):raise
                        failures+=1
                        if failures>self.retries:raise
                        if self.stopping.wait(min(30,2**(failures-1))*random.uniform(.75,1.25)):
                            raise InterruptedError('OpenCloud upload interrupted')
            # Offset == size can precede publication; a lost MOVE may already have published the target.
            published=self.confirm_publication(row,stage,sha1)
            after=local.stat()
            if (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)!=fingerprint:raise RuntimeError('Local video changed')
            if published!=target:
                with self.share.request('MOVE',stage,headers={'Destination':self.share.url(target),'Overwrite':'F'}) as response:
                    if response.status!=201:raise RuntimeError('OpenCloud did not confirm file publication')
            row.update(state='uploaded',sent_bytes=size,error_type=None,http_status=None,error_reason=None,error_errno=None)
            self.save_record(job,row)
            self.index.append(dict(path=target,size=size,directory=False,checksums=['SHA256:'+actual]))
            print('OPENCLOUD UPLOADED: '+target,flush=True)
            return True
        except BaseException as error:
            row.update(state='pending',**nextcloud.error_details(error));self.save_record(job,row);raise
        finally:
            with self.index_lock:self.active_hashes.discard(actual)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queue',type=Path)
    parser.add_argument('--destination',type=Path,action='append',default=[],help='Course archive; repeat to add archives outside queue')
    parser.add_argument('--server',required=True)
    parser.add_argument('--username',required=True)
    parser.add_argument('--password-file',type=Path,required=True,help='App Token or enabled Basic Auth password; never passed in argv')
    parser.add_argument('--upload',action='store_true',help='Actually upload; default only lists local candidates')
    parser.add_argument('--limit',type=int,help='Stop after this many newly uploaded videos')
    parser.add_argument('--chunk-size',type=int,default=10_000_000,help='TUS chunk bytes, default 10000000')
    parser.add_argument('--timeout',type=float,default=120)
    parser.add_argument('--retries',type=int,default=2)
    parser.add_argument('--transfers',type=int,default=1,help='Simultaneous file uploads, default 1')
    parser.add_argument('--upload-budget',type=Path,help='Shared automatic total upload budget directory; --transfers/--min-transfers are global limits')
    parser.add_argument('--min-transfers',type=int,default=2,help='Minimum total uploads with --upload-budget (default: 2)')
    parser.add_argument('--poll-interval',type=float,help='Repeat upload passes after this many seconds; requires --upload')
    args=parser.parse_args()
    if (not args.queue and not args.destination or args.limit is not None and args.limit<1
            or args.chunk_size<1 or args.retries<0 or args.transfers<1 or not math.isfinite(args.timeout) or args.timeout<=0
            or args.poll_interval is not None and (not args.upload or not math.isfinite(args.poll_interval) or args.poll_interval<=0)):
        parser.error('Specify archives and positive limit/chunk-size/timeout, non-negative retries')
    roots=[Path(x['destination']).resolve() for x in storage.read_queue(args.queue)['archives'] if x.get('enabled',True)] if args.queue else []
    roots=list(dict.fromkeys(roots+[p.resolve() for p in args.destination]))
    if not roots:parser.error('No selected archives')
    if args.upload_budget and (not args.upload or not 1<=args.min_transfers<=args.transfers):
        parser.error('Shared budget requires --upload and 1 <= --min-transfers <= --transfers')
    os.umask(0o077)
    stopping=Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stopping.set())
    control=args.queue.parent if args.queue else roots[0]/'.download'
    storage.check(control)
    with storage.lock_archives(roots, 'opencloud.lock') as claim, ExitStack() as resources:
        client=Client(args.server,args.username,args.password_file.expanduser().read_text().rstrip('\r\n'),args.timeout)
        if args.upload:client.discover()
        else:client.base=client.server+'/dav/spaces/not-connected/'
        uploader=Uploader(client,stopping,args.chunk_size,args.retries,args.transfers)
        if args.upload_budget:
            uploader.upload_budget=resources.enter_context(nextcloud.SharedUploadBudget(
                args.upload_budget,'opencloud',args.min_transfers,args.transfers))
        while not stopping.is_set():
            result=uploader.run_once(claim(roots),args.upload,args.limit)
            if args.poll_interval is None:break
            if stopping.wait(args.poll_interval):break
            uploader.index=None;uploader.known_folders.clear()
        return 130 if stopping.is_set() else result


if __name__=='__main__':
    try:raise SystemExit(d.cli(main))
    except OSError as error:
        if not storage.disk_error(error):raise
        print('DISK OFFLINE: OpenCloud upload state retained',flush=True)
        raise SystemExit(storage.TEMPFAIL)
