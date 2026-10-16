"""Fetch failed course videos from a public share without trusting remote checksums."""
from contextlib import nullcontext
from datetime import datetime
import hashlib
import errno
import http.client
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
from threading import BoundedSemaphore, Lock
import time
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET

import download as d
from nextcloud import Index, VIDEO, remote_path, error_details
import recovery as r
import storage

# Bound course fallback traffic across archives in this worker process.
TRANSFERS=BoundedSemaphore(2)


class Fallback:
    def __init__(self, root, link, stopping, *, index=None, budget=None, label="NEXTCLOUD"):
        self.root,self.link,self.stopping=Path(root),link,stopping
        self.index=index if index is not None else Index(root,link,stopping=stopping)
        self.budget,self.label=budget,label
        self.index.share.timeout=60
        self.device=self.root.stat().st_dev
        self.lookup_lock=Lock()
        self.source=json.loads((self.root/'.download/source.json').read_text())
        self.is_shad=bool(re.search(r'(?:^|\W)(?:шад|shad|ysda|yds)(?:$|\W)',self.source.get('title',''),re.I))
        self.directory=self.root/('.download/'+label.lower()+'-downloads')

    def state_path(self,item):
        key=hashlib.sha256(json.dumps([self.source['source'],self.index.link,item['path'],item['size'],item['sha256']]).encode()).hexdigest()
        return self.directory/(key+'.json')

    def read(self,item):
        p=self.state_path(item)
        return json.loads(p.read_text()) if p.exists() else {}

    def changed_copies(self,row):
        return [copy['signature'] for copy in row.get('retained_copies',{}).values()
                if not Path(copy['local']).exists() or self.stamp(Path(copy['local']))!=copy['local_stat']]

    def ready(self,item):
        if not self.is_shad or PurePosixPath(item['path']).suffix.casefold() not in VIDEO:
            return False
        catalog=self.root/'.download/catalog.json'
        if self.source.get('root') or catalog.exists() and json.loads(catalog.read_text()).get('root'):
            return False
        row=self.read(item)
        if row.get('state')=='unmapped':
            return False
        if row.get('state')=='mismatch' and self.changed_copies(row):return True
        if row.get('state') in ('verified','mismatch'):
            local=Path(row['local'])
            if (local.exists() and self.stamp(local)==row.get('local_stat')
                    and not (row['state']=='mismatch' and row.get('remaining_candidates'))):
                return False
        return row.get('retry_after',0)<=time.time()

    @staticmethod
    def stamp(path):
        s=path.stat()
        return [s.st_ino,s.st_size,s.st_mtime_ns]

    @staticmethod
    def body_key(path,size):
        return hashlib.sha256(json.dumps([path,size]).encode()).hexdigest()[:16]

    def save(self,item,row):
        if row.get('remote_path') is not None:
            key=self.body_key(row['remote_path'],row['remote_size'])
            row.setdefault('bodies',{})[key]={name:row.get(name) for name in ('etag','body_complete','received_bytes')}
        row['time']=datetime.now().astimezone().isoformat()
        d.write_json(self.state_path(item),row)
        d.append_events(self.directory.with_suffix('.jsonl'),[dict(row)])

    def mismatch_path(self,item,host,digest=None,collision=0):
        original=PurePosixPath(item['path']);suffix=' (SHA mismatch, '+host+')'+original.suffix
        stem=original.stem+('~'+digest[:12] if digest else '')+('-'+str(collision) if collision else '')
        if len((stem+suffix).encode())>230:
            marker='~'+hashlib.sha256((item['path']+(digest or '')+str(collision)).encode()).hexdigest()[:16]
            stem=stem.encode()[:230-len((marker+suffix).encode())].decode(errors='ignore')+marker
        return d.local_path(self.root,str(original.with_name(stem+suffix)))

    def hash_file(self,path):
        storage.check(path)
        while not d.HASH_LOCK.acquire(timeout=.25):
            if self.stopping.is_set():raise InterruptedError('SHA interrupted')
        try:
            hashes={kind:hashlib.new(kind) for kind in ('sha256','sha1','md5')}
            with path.open('rb') as stream:
                while block:=stream.read(1024**2):
                    if self.stopping.is_set():raise InterruptedError('SHA interrupted')
                    for digest in hashes.values():digest.update(block)
                os.fsync(stream.fileno())
            return {kind:digest.hexdigest() for kind,digest in hashes.items()}
        finally:d.HASH_LOCK.release()

    def retained(self,candidate,retained_hashes,retained_digests):
        remote={kind.lower():value.lower() for kind,value in re.findall(
            r'\b(SHA256|SHA1|MD5):([a-fA-F0-9]+)',' '.join(candidate.get('checksums',[])),re.I)}
        if remote.get('sha256') in retained_hashes:return True
        for actual in retained_digests:
            common=next((kind for kind in ('sha256','sha1','md5') if remote.get(kind) and actual.get(kind)),None)
            if common and remote[common]==actual[common]:return True
        # Without a common checksum, name and size cannot establish identical bytes.
        return False

    def recover_all(self,item,event,*,retained_hashes=(),retained_digests=()):
        retained_hashes=frozenset(retained_hashes)
        retained_digests=tuple(dict(hashes) for hashes in retained_digests)
        result=None
        attempts,limit=0,1
        while attempts<limit:
            current=self.recover(item,event,retained_hashes=retained_hashes,retained_digests=retained_digests)
            if current:
                result=current
                if current['status']=='valid':return current
            attempts+=1
            row=self.read(item)
            limit=max(limit,row.get('candidate_count',1))
            if not row.get('remaining_candidates') or row.get('state') not in ('pending','mismatch'):
                break
        return result

    def recover(self,item,event,*,retained_hashes=(),retained_digests=()):
        retained_hashes=frozenset(retained_hashes)
        retained_digests=tuple(dict(hashes) for hashes in retained_digests)
        if not self.is_shad or PurePosixPath(item['path']).suffix.casefold() not in VIDEO:
            return None
        catalog=self.root/'.download/catalog.json'
        if self.source.get('root') or catalog.exists() and json.loads(catalog.read_text()).get('root'):
            return None
        storage.check(self.root)
        self.directory.mkdir(mode=0o700,exist_ok=True)
        if not re.fullmatch(r'[0-9a-fA-F]{64}',item['sha256']):
            raise ValueError('Fallback requires the original SHA-256')
        row=self.read(item)
        if row.get('state') in ('verified','mismatch','not_found','unmapped') and not self.ready(item):
            return None
        row.update(source=self.source['source'],path=item['path'],size=item['size'],sha256=item['sha256'],share=self.index.link)
        own_hashes=[]
        for copy in row.get('retained_copies',{}).values():
            local=Path(copy['local'])
            if (local.is_file() and not Path(str(local)+'.aria2').exists()
                    and self.stamp(local)==copy['local_stat']):
                own_hashes.append(dict(copy.get('actual_hashes',{}),sha256=copy['actual_sha256']))
        retained_hashes=retained_hashes|frozenset(hashes['sha256'] for hashes in own_hashes)
        retained_digests=retained_digests+tuple(own_hashes)
        acquired=False
        try:
            if self.budget is None:
                while not TRANSFERS.acquire(timeout=.25):
                    if self.stopping.is_set():raise InterruptedError('Fallback interrupted')
                acquired=True
            try:
                folder=str(PurePosixPath(remote_path(self.source,item)).parent)
            except ValueError:
                if not getattr(self.index,'global_candidates',False):
                    row['state']='unmapped';self.save(item,row);return None
                folder=None
            with self.lookup_lock:
                if (row.get('state')=='not_found' or row.get('http_status')==404) and not isinstance(self.index.folders.get(folder),Exception):
                    self.index.folders.pop(folder,None)
                candidates=self.index.candidates(self.source,item)
            if not candidates:
                row.update(state='not_found',retry_after=time.time()+3600);self.save(item,row);return None
            changed=self.changed_copies(row)
            if changed:
                row['rejected_candidates']=[signature for signature in row.get('rejected_candidates',[]) if signature not in changed]
            candidates=[c for c in candidates if [c.get('download_path',c['path']),c['size'],c.get('etag')]
                        not in row.get('rejected_candidates',[])
                        and not self.retained(c,retained_hashes,retained_digests)]
            if not candidates:
                row.update(state='mismatch' if row.get('local') else 'not_found',remaining_candidates=False,
                           retry_after=time.time()+3600);self.save(item,row);return None
            row['remaining_candidates']=len(candidates)>1
            row['candidate_count']=len(candidates)
            untried=[c for c in candidates if [c.get('download_path',c['path']),c['size'],c.get('etag')]
                     not in row.get('failed_candidates',[])]
            if not untried:
                row['failed_candidates']=[];untried=candidates
            candidate=min(untried,key=lambda c:(c.get('size')!=item['size'],c['path']))
            size=candidate['size']
            if not isinstance(size,int) or size<0:
                raise RuntimeError('Nextcloud did not provide a valid file size')
            remote=candidate.get('download_path',candidate['path'])
            key=self.body_key(remote,size)
            part=self.directory/(self.state_path(item).stem+'-'+key+'.part')
            if row.get('remote_path')!=remote or row.get('remote_size')!=size:
                body=row.get('bodies',{}).get(key,{})
                row.update({name:body.get(name) for name in ('etag','body_complete','received_bytes')})
                row.update(remote_path=remote,remote_size=size)
            row.update(state='downloading',retry_after=0,error_type=None,http_status=None,
                       error_reason=None,error_errno=None)
            self.save(item,row)
            storage.check(self.root)
            offset=part.stat().st_size if part.exists() else 0
            complete=part.exists() and offset==size and row.get('body_complete') is True
            if not complete:
                headers={'Accept-Encoding':'identity'}
                etag=row.get('etag')
                resume=offset>0 and offset<size and etag and not etag.startswith('W/')
                if resume:headers.update(Range=f'bytes={offset}-',**{'If-Range':etag})
                else:offset=0
                if shutil.disk_usage(self.root).free < size-offset+1024**3:
                    raise OSError(errno.ENOSPC,'Insufficient space for Nextcloud fallback')
                print(self.label+' DOWNLOAD START: '+item['path'],flush=True)
                with self.budget.slot(self.stopping) if self.budget is not None else nullcontext(), \
                        self.index.share.request('GET',remote,headers=headers) as response:
                    if response.status==206:
                        match=re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)',response.headers.get('Content-Range',''))
                        if (not resume or not match or tuple(map(int,match.groups()))!=(offset,size-1,size)
                                or response.headers.get('ETag')!=etag):
                            raise RuntimeError('Invalid Nextcloud resume response')
                    elif response.status==200:
                        reclaim=part.stat().st_blocks*512 if part.exists() else 0
                        if shutil.disk_usage(self.root).free < max(0,size-reclaim)+1024**3:
                            raise OSError(errno.ENOSPC,'Insufficient space to restart Nextcloud fallback')
                        offset=0
                    else:raise RuntimeError('Unexpected Nextcloud download status')
                    length=response.headers.get('Content-Length')
                    if length is not None and int(length)!=size-offset:
                        raise RuntimeError('Nextcloud response size changed')
                    row['etag']=response.headers.get('ETag');row['body_complete']=False
                    self.save(item,row)
                    with part.open('ab' if offset else 'wb') as stream:
                        if os.fstat(stream.fileno()).st_dev!=self.device:
                            raise storage.StorageUnavailable(self.root)
                        last=time.monotonic()
                        while True:
                            if self.stopping.is_set():raise InterruptedError('Fallback interrupted')
                            storage.check(self.root)
                            block=response.read(1024**2)
                            if not block:break
                            if self.budget is not None:self.budget.confirmed(len(block))
                            if offset+len(block)>size:raise RuntimeError('Nextcloud sent more bytes than advertised')
                            stream.write(block);offset+=len(block)
                            if time.monotonic()-last>=30:
                                stream.flush();row['received_bytes']=offset;self.save(item,row)
                                print(f'{self.label} DOWNLOAD PROGRESS: {item["path"]}; {offset}/{size} bytes',flush=True)
                                last=time.monotonic()
                        stream.flush();os.fsync(stream.fileno())
                    if offset!=size:raise RuntimeError('Incomplete Nextcloud response')
                row.update(body_complete=True,received_bytes=size)
                self.save(item,row)
            before=self.stamp(part);actual_hashes=self.hash_file(part);actual=actual_hashes['sha256']
            if self.stamp(part)!=before:raise RuntimeError('Fallback file changed while verifying SHA')
            matched=size==item['size'] and actual==item['sha256'].lower()
            host=urllib.parse.urlsplit(self.index.link).hostname
            target=d.target(self.root,item,event['course']) if matched else self.mismatch_path(item,host,actual)
            target.parent.mkdir(parents=True,exist_ok=True)
            verified_stat=before
            collision=0
            while True:
                try:os.link(part,target)
                except FileExistsError:
                    stamp=self.stamp(target)
                    if Path(str(target)+'.aria2').exists() or d.sha256_file(target,self.stopping)!=actual or self.stamp(target)!=stamp:
                        if matched:raise FileExistsError('Existing local file retained')
                        collision+=1;target=self.mismatch_path(item,host,actual,collision)
                        continue
                    verified_stat=stamp
                break
            storage.sync_directory(target.parent)
            stat=self.stamp(target)
            if stat!=verified_stat:raise RuntimeError('Fallback file changed before publication')
            row.update(state='verified' if matched else 'mismatch',actual_sha256=actual,local=str(target),local_stat=stat,received_bytes=size)
            if matched:
                proof=d.record(self.root/'.download',item,event['course'],event['attempt'],'valid',
                    self.label.title()+' '+host+'; SHA-256 verified',local=target,verified_stat=stat)
                if proof['status']!='valid':raise RuntimeError('Published fallback file changed')
                result=dict(event,**proof,range_recovered_complete=True,recovered_stat=stat,nextcloud_source=host)
                r.recovery_state(self.root,result,'finished')
            else:
                signature=[remote,size,candidate.get('etag')]
                row.setdefault('rejected_candidates',[]).append(signature)
                row.setdefault('retained_copies',{})[key]=dict(signature=signature,local=str(target),local_stat=stat,actual_sha256=actual,actual_hashes=actual_hashes)
                result=dict(status='mismatch',local=str(target),actual_sha256=actual,expected_sha256=item['sha256'],source=host)
                print(self.label+' SHA MISMATCH: '+str(target),flush=True)
            self.save(item,row)
            part.unlink();storage.sync_directory(part.parent)
            return result
        except InterruptedError:
            row.update(state='pending',retry_after=0);self.save(item,row);raise
        except (OSError,RuntimeError,ValueError,http.client.HTTPException,ET.ParseError) as error:
            if storage.disk_error(error):raise
            if 'candidate' in locals():
                signature=[candidate.get('download_path',candidate['path']),candidate['size'],candidate.get('etag')]
                if signature not in row.setdefault('failed_candidates',[]):row['failed_candidates'].append(signature)
            details = error_details(error)
            row.update(state='pending',retry_after=time.time()+60,**details)
            self.save(item,row)
            print(f'{self.label} DOWNLOAD RETRY: {item["path"]}; {type(error).__name__}; HTTP={details["http_status"]}; reason={details["error_reason"]!r}; errno={details["error_errno"]}',flush=True)
            return None
        finally:
            if acquired:TRANSFERS.release()
