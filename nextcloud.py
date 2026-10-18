"""Public Nextcloud listings and a separate, durable video upload worker."""
import argparse
from contextlib import ExitStack
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path, PurePosixPath
import random
import re
import signal
from threading import Event, Lock
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

import download as d
from download import local_path
from file_hashes import VIDEO
import network
import storage
from upload_concurrency import UploadConcurrency, SharedUploadBudget

PROPERTIES = b'<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:resourcetype/><d:getcontentlength/><d:getetag/><oc:checksums/></d:prop></d:propfind>'


def error_details(error):
    return dict(error_type=type(error).__name__, http_status=getattr(error, 'code', None),
                error_reason=getattr(error, 'dav_message', None) or str(getattr(error, 'reason', error)),
                error_errno=network.failure(error)['errno'])


def normalized(value):
    return unicodedata.normalize('NFC', value).strip()


def remote_path(data, item):
    path = PurePosixPath(item['path'])
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('Unsafe source path for Nextcloud')
    year_match = re.search(r'(?<!\d)(20\d\d)(?!\d)', data['title'])
    year = year_match[1] if year_match else None
    title = data['title'].casefold().replace('_',' ')
    parts = list(path.parts[1:])
    season = 'spring' if 'весна' in title or re.search(r'\bspring\b',title) else 'fall' if 'осень' in title or re.search(r'\b(?:autumn|fall)\b',title) else None
    nested = re.fullmatch(r'(?:(?:шад|shad)[ _-]*)?(весна|осень|spring|autumn|fall)(?:[ _-]*(20\d\d))?',parts[0].strip(),re.I) if parts else None
    if nested:
        nested_season = 'spring' if nested[1].casefold() in ('весна','spring') else 'fall'
        if season and season!=nested_season or year and nested[2] and year!=nested[2]:
            raise ValueError('Conflicting Nextcloud year/semester metadata')
        season = nested_season
        year = year or nested[2]
        parts.pop(0)
    if not year or not season or len(parts) < 2:
        raise ValueError('Nextcloud requires a course archive with a known year and semester')
    # Nextcloud rejects literal backslashes; keep them visible as a filename character.
    parts[-1] = parts[-1].replace('\\', '＼')
    return year+'-'+season+'/'+normalized('/'.join(parts))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def share_identity(url):
    # Both entry points serve the same shares, receipts and resumable sessions.
    parsed=urllib.parse.urlsplit(url)
    if parsed.scheme=='https' and parsed.netloc=='ru.drive.oleja.dev':
        return parsed._replace(netloc='drive.oleja.dev').geturl()
    return url


class Share:
    def __init__(self, link, timeout=120):
        parsed = urllib.parse.urlsplit(link)
        match = re.fullmatch(r'(.*?)/(?:index\.php/)?s/([A-Za-z0-9_-]+)/?', parsed.path)
        if parsed.scheme != 'https' or not parsed.netloc or parsed.username or parsed.password or not match:
            raise ValueError('Expected an HTTPS Nextcloud public share URL')
        self.base = urllib.parse.urlunsplit((parsed.scheme,parsed.netloc,match[1]+'/public.php/dav/files/'+match[2]+'/', '', ''))
        self.subfolder = urllib.parse.parse_qs(parsed.query).get('dir', [''])[0].strip('/')
        self.timeout = timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler(),NoRedirect(),network.HTTPHandler(),network.HTTPSHandler())
        self.url()

    def url(self, path=''):
        path = '/'.join(p for p in (self.subfolder,path) if p)
        if (PurePosixPath(path).is_absolute() or '..' in PurePosixPath(path).parts
                or any(ord(c)<32 for c in path) or '\\' in path):
            raise ValueError('Unsafe Nextcloud path')
        return self.base+urllib.parse.quote(path,safe='/')

    def request(self, method, path='', data=None, headers=None, uploads=False):
        url=self.url(path)
        if uploads:
            url=self.base.replace('/dav/files/','/dav/uploads/')+urllib.parse.quote(path,safe='/')
        request_headers={'X-Requested-With':'XMLHttpRequest',**(headers or {})}
        if 'Destination' in request_headers:
            # File-drop creates folders from the header before URL decoding; send UTF-8 bytes.
            request_headers['Destination']=urllib.parse.unquote(request_headers['Destination']).encode('utf8').decode('latin1')
        if getattr(self,'nickname',None):
            request_headers['X-NC-Nickname']=urllib.parse.quote(self.nickname,safe='')
        request = urllib.request.Request(url,data=data,method=method,headers=request_headers)
        return self.opener.open(request,timeout=self.timeout)

    def listing(self, folder):
        with self.request('PROPFIND',folder,data=PROPERTIES,
                          headers={'Depth':'1','Content-Type':'application/xml'}) as response:
            if response.status != 207:
                raise RuntimeError('Nextcloud did not return a WebDAV listing')
            tree = ET.fromstring(response.read())
        prefix = urllib.parse.unquote(urllib.parse.urlsplit(self.url()).path).rstrip('/')+'/'
        result = []
        for row in tree.findall('{DAV:}response'):
            href = urllib.parse.urlsplit(row.findtext('{DAV:}href',''))
            path = urllib.parse.unquote(href.path)
            if not path.startswith(prefix):
                continue
            properties = next((p.find('{DAV:}prop') for p in row.findall('{DAV:}propstat')
                               if ' 200 ' in p.findtext('{DAV:}status','')),None)
            if properties is None:
                continue
            length = properties.findtext('{DAV:}getcontentlength')
            result.append(dict(path=unicodedata.normalize('NFC', path[len(prefix):].rstrip('/')),
                download_path=path[len(prefix):].rstrip('/'),etag=properties.findtext('{DAV:}getetag'),
                directory=properties.find('{DAV:}resourcetype/{DAV:}collection') is not None,
                size=int(length) if length is not None else None,
                checksums=[c.text or '' for c in properties.findall('.//{http://owncloud.org/ns}checksum')]))
        return result


class Index:
    def __init__(self, root, link, strict=True, stopping=None):
        self.root = Path(root)
        self.share = Share(link,timeout=30)
        self.link = share_identity(self.share.url())
        self.strict = strict
        self.stopping = stopping if stopping is not None else Event()
        self.folders = {}
        self.retry_at = {}
        self.receipts_stamp = None
        self.receipts = {}

    def contains(self, data, item):
        if PurePosixPath(item['path']).suffix.casefold() not in VIDEO:
            return False
        try:
            relative = remote_path(data,item)
        except ValueError:
            if self.strict:
                raise
            print('NEXTCLOUD PATH UNMAPPED: '+item['path']+'; downloading from source',flush=True)
            return False
        receipts = self.root/'.download/nextcloud-uploads.json'
        try:
            stat = receipts.stat()
            stamp = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        except FileNotFoundError:
            stamp = None
        if stamp != self.receipts_stamp:
            indexed = {}
            for row in json.loads(receipts.read_text()).values() if stamp else ():
                if (row.get('state')=='uploaded' and row.get('variant')=='original'
                        and row.get('read_share')==self.link and row.get('http_status')==201
                        and row.get('sent_bytes')==row.get('size')
                        and row.get('payload_sha256')==row.get('sha256')):
                    key = (row.get('source'), row.get('path'), row.get('size'), row.get('sha256'))
                    indexed.setdefault(key, []).append(row)
            self.receipts = indexed
            self.receipts_stamp = stamp
        uploaded = self.receipts.get((data['source'], item['path'], item['size'], item['sha256']), [])
        candidates = self.candidates(data,item)
        conflicting = False
        for row in candidates:
            hashes = [value.split(':',1)[1].lower() for value in row.get('checksums',[])
                      if value.upper().startswith('SHA256:')]
            if row['size']==item['size'] and item['sha256'].lower() in hashes:
                return True
            # An old upload receipt must not override newer, conflicting WebDAV evidence.
            conflicting |= row['size'] != item['size'] or bool(hashes)
        # File-drop uploads can remain absent from the public listing indefinitely.
        return bool(uploaded) and not conflicting

    def candidates(self, data, item):
        relative=remote_path(data,item)
        folder=str(PurePosixPath(relative).parent)
        if folder in self.retry_at and time.monotonic()>=self.retry_at[folder]:
            self.folders.pop(folder,None)
            self.retry_at.pop(folder)
        if folder not in self.folders:
            for attempt in range(3):
                if self.stopping.is_set():
                    raise InterruptedError('Nextcloud catalog lookup interrupted')
                try:
                    self.folders[folder]=self.share.listing(folder)
                    break
                except (OSError,http.client.HTTPException,RuntimeError,ET.ParseError) as error:
                    if storage.disk_error(error):
                        raise
                    if self.stopping.is_set():
                        raise InterruptedError('Nextcloud catalog lookup interrupted') from error
                    code=getattr(error,'code',None)
                    retry_after=network.retry_after(getattr(error,'headers',None))
                    if isinstance(error,urllib.error.HTTPError):
                        error.close()
                    if code==404:
                        self.folders[folder]=[]
                        break
                    transient=(code in (429,500,502,503,504)
                               or network.failure(error)['category'] in ('routing','ambiguous')
                               or isinstance(error,ET.ParseError))
                    if not transient:
                        self.folders[folder]=error
                        self.retry_at[folder]=time.monotonic()+max(60,retry_after)
                        break
                    delay=max(2**attempt*random.uniform(.75,1.25),retry_after)
                    # Long Retry-After waits must not hold the shared lookup lock.
                    if attempt==2 or delay>5:
                        self.folders[folder]=error
                        cooldown=max(60,delay)
                        self.retry_at[folder]=time.monotonic()+cooldown
                        print(f'NEXTCLOUD LOOKUP DEFERRED: {folder}; retry_after_seconds={cooldown:g}; '
                              f'HTTP={code}; reason={str(error)!r}',flush=True)
                        break
                    print(f'NEXTCLOUD LOOKUP RETRY: {folder}; retry={attempt+1}/2; pause={delay:.2f}s; '
                          f'HTTP={code}; reason={str(error)!r}',flush=True)
                    if self.stopping.wait(delay):
                        raise InterruptedError('Nextcloud catalog lookup interrupted') from error
        if isinstance(self.folders[folder],Exception):
            if self.strict:
                raise self.folders[folder].with_traceback(None)
            print('NEXTCLOUD LOOKUP UNAVAILABLE: '+folder+'; downloading from source',flush=True)
            return []
        return [row for row in self.folders[folder] if not row['directory']
                and str(PurePosixPath(row['path']).parent)==folder
                and normalized(re.sub(r'^\d{3,} - ','',PurePosixPath(row['path']).name))==PurePosixPath(relative).name]


def saved_remote_files(state, link, source=None, saved=None):
    if not link:
        return {}
    if saved is None:
        path = state/'nextcloud-existing.json'
        if not path.exists():
            return {}
        saved = json.loads(path.read_text())
    if source is None:
        source = json.loads((state/'source.json').read_text())['source']
    if saved.get('source')!=source or saved.get('share')!=share_identity(Share(link).url()):
        return {}
    return saved.get('files', {})


def remote_saved(state, item, link):
    row = saved_remote_files(state, link).get(item['path'], {})
    return (row.get('size'), row.get('sha256')) == (item['size'], item['sha256'])


class Body:
    def __init__(self, stream, size, stopping, path):
        self.stream, self.size, self.stopping, self.path = stream,size,stopping,path
        self.sent = 0
        self.digest = hashlib.sha256()
        self.last_progress=time.monotonic()

    def read(self, amount=-1):
        if self.stopping.is_set():
            raise InterruptedError('Nextcloud upload interrupted')
        storage.check(self.path)
        data = self.stream.read(min(self.size-self.sent,1024**2 if amount<0 else amount))
        self.sent += len(data)
        self.digest.update(data)
        if time.monotonic()-self.last_progress>=30:
            print(f'NEXTCLOUD UPLOAD PROGRESS: {self.path.name}; {self.sent}/{self.size} bytes',flush=True)
            self.last_progress=time.monotonic()
        return data


class Sync:
    ledger_name = 'nextcloud-uploads'

    def __init__(self, existing, upload, stopping, timeout=120, nickname=None, chunk_size=16*1024**2, transfers=4,
                 auto_transfers=False, min_transfers=2, share=None):
        self.existing, self.share, self.stopping = existing,share if share is not None else Share(upload,timeout),stopping
        if nickname and (nickname.startswith('.') or any(ord(c)<32 or c in '/\\' for c in nickname)):
            raise ValueError('Invalid Nextcloud nickname')
        self.share.nickname=nickname
        if transfers<1:
            raise ValueError('Upload concurrency must be positive')
        self.chunk_size=chunk_size
        self.transfers=transfers
        self.concurrency=UploadConcurrency(min_transfers,transfers) if auto_transfers else None
        self.upload_budget=None
        if self.concurrency:
            print(f'NEXTCLOUD AUTO TRANSFERS: minimum={min_transfers}; maximum={transfers}; '
                  'sample_seconds=90; probe_interval_seconds=300',flush=True)
        self.index_lock=Lock()
        self.indexes={}
        self.event_indexes={}
        self.cache = {}

    def read(self, path, default=None):
        if not path.exists():
            return default
        stat = path.stat();stamp=(stat.st_ino,stat.st_size,stat.st_mtime_ns)
        if path not in self.cache or self.cache[path][0]!=stamp:
            value = json.loads(path.read_text())
            self.cache[path] = stamp,value
        return self.cache[path][1]

    def jobs(self, roots):
        jobs = []
        for root in roots:
            if self.stopping.is_set():
                return
            storage.check(root)
            state = root/'.download'
            data = self.read(state/'source.json')
            if not data:
                continue
            if self.read(state/'catalog.json',{}).get('root'):
                print('NEXTCLOUD ARCHIVE SKIPPED: personal/root catalog',flush=True)
                continue
            index=None
            if self.existing:
                index=self.indexes.get(root)
                if index is None:
                    index=Index(root,self.existing,stopping=self.stopping)
                    self.indexes[root]=index
                else:
                    current=time.monotonic()
                    index.retry_at={folder:deadline for folder,deadline in index.retry_at.items() if deadline>current}
                    # Refresh successful listings, retaining only active failure cooldowns.
                    index.folders={folder:error for folder,error in index.folders.items() if folder in index.retry_at}
            started = {row['path'] for row in self.read(state/(self.ledger_name+'.json'), {}).values()
                       if row.get('state')!='uploaded' and row.get('next_chunk',0)>0
                       and row.get('source')==data['source'] and row.get('upload_share')==share_identity(self.share.url())
                       and row.get('nickname')==self.share.nickname}
            proof_index=self.event_indexes.get(root)
            if proof_index is None:
                proof_index=d.EventIndex(state/'events.jsonl',repair=False)
                self.event_indexes[root]=proof_index
            events=dict(proof_index.refresh())
            for e in self.read(state/'local-copies.json',[]):
                if e.get('source')==data['source'] and e.get('status')=='valid':
                    events[e['path']] = e
            partial = {e['path']:e for e in self.read(state/'partial-copies.json',[]) if e.get('source')==data['source']}
            entries = {}
            for manifest in sorted(state.glob('*-manifest.json'),key=lambda p:p.stat().st_mtime_ns):
                entries.update({i['path']:i for i in self.read(manifest,[])})
            folders = {}
            for item in entries.values():
                if PurePosixPath(item['path']).suffix.casefold() not in VIDEO:
                    continue
                folders.setdefault(str(PurePosixPath(item['path']).parent),[]).append(item)
            for items in folders.values():
                for ordinal,item in enumerate(sorted(items,key=lambda i:normalized(i['path'])),1):
                    if self.stopping.is_set():
                        return
                    if PurePosixPath(item['path']).suffix.casefold() not in VIDEO:
                        continue
                    event=events.get(item['path'],{})
                    local=storage.saved_local(root, event['local']) if event.get('local') else local_path(root,item['path'])
                    variant='original'
                    expected_size=item['size']
                    same=(event.get('size'),event.get('sha256'))==(item['size'],item['sha256'])
                    valid=(same and event.get('status')=='valid' and local.is_file()
                           and (local.stat().st_size,local.stat().st_mtime_ns)==(expected_size,event.get('mtime_ns'))
                           and not Path(str(local)+'.aria2').exists())
                    percent=None
                    if not valid:
                        event=partial.get(item['path'],{})
                        if ((event.get('size'),event.get('sha256'))!=(item['size'],item['sha256'])
                                or (event.get('downloaded_bytes') or 0)*2<=item['size'] or event.get('sha256_mismatch')):
                            continue
                        local=(root/event['recovered_path']).resolve()
                        if not local.is_relative_to(root.resolve()):
                            raise ValueError('Unsafe partial video path')
                        expected_size=event['local_size']
                        if (not local.is_file() or (local.stat().st_size,local.stat().st_mtime_ns)!=(expected_size,event['mtime_ns'])):
                            continue
                        variant='partial'
                        percent=100*event['downloaded_bytes']/item['size']
                    jobs.append((item['path'] in started,root,data,item,local,variant,expected_size,percent,index,ordinal,event['mtime_ns']))
        jobs.sort(key=lambda job:not job[0])
        return jobs

    def run_transfer(self, job):
        if self.upload_budget is None:
            return self.transfer(job)
        try:
            with self.upload_budget.slot(self.stopping):
                return self.transfer(job)
        except InterruptedError:
            if not self.stopping.is_set():raise
            return False

    def run_once(self, roots):
        jobs = self.jobs(roots)
        if jobs is None:
            return
        with ThreadPoolExecutor(max_workers=self.transfers, thread_name_prefix='nextcloud-upload') as pool:
            pending=iter(jobs)
            futures=set()
            try:
                while futures or not self.stopping.is_set():
                    if self.upload_budget:self.upload_budget.update()
                    limit=self.concurrency.limit if self.concurrency else self.transfers
                    while len(futures)<limit and not self.stopping.is_set():
                        job=next(pending,None)
                        if job is None:
                            break
                        futures.add(pool.submit(self.run_transfer,job))
                    if self.concurrency:
                        self.concurrency.update(len(futures))
                    if not futures:
                        break
                    for future in wait(futures,timeout=.25,return_when=FIRST_COMPLETED).done:
                        futures.remove(future)
                        future.result()
            except BaseException:
                self.stopping.set()
                for future in futures:
                    future.cancel()
                raise

    def transfer(self, job):
        _,root,data,item,local,variant,expected_size,percent,index,ordinal,expected_mtime=job
        if self.stopping.is_set():
            return
        try:
            storage.check(local)
            if not local.is_file() or (local.stat().st_size,local.stat().st_mtime_ns)!=(expected_size,expected_mtime):
                return
            if index:
                with self.index_lock:
                    exists=index.contains(data,item)
                if exists:
                    print('NEXTCLOUD EXISTS: '+item['path'],flush=True)
                    return
            relative=PurePosixPath(remote_path(data,item))
            stem=relative.stem+(f' (partially available {percent:.2f}%)' if percent is not None else '')
            name=local_path(Path('/'),f'/{ordinal:03} - {stem}{relative.suffix}').name
            target=str(relative.with_name(name))
            self.upload(root,data,item,local,variant,target,expected_size,percent)
        except (OSError,http.client.HTTPException,RuntimeError,ValueError,ET.ParseError) as error:
            if storage.disk_error(error):
                self.stopping.set()
                raise
            details = error_details(error)
            print(f'NEXTCLOUD ERROR: {item["path"]}; {type(error).__name__}; HTTP={details["http_status"]}; reason={details["error_reason"]!r}; errno={details["error_errno"]}',flush=True)

    def save(self, state, row, record=False):
        with d.STATE_LOCK:
            ledger=json.loads(state.read_text()) if state.exists() else {}
            ledger[row['key']]=dict(row)
            if record:
                d.append_events(state.with_suffix('.jsonl'),[dict(row)])
            d.write_json(state,ledger)

    def request(self, method, path, data=None, **kwargs):
        retry = network.RequestRetry(path, self.stopping)
        server_retries = 0
        while True:
            if self.stopping.is_set():
                raise InterruptedError('Nextcloud request interrupted')
            try:
                response = self.share.request(method, path, data=data, **kwargs)
                network.GUARD.observe(path)
                return response
            except (OSError, http.client.HTTPException) as error:
                if isinstance(error, urllib.error.HTTPError) and error.code==412:
                    error.request_method=method
                    try:
                        tree=ET.fromstring(error.read(8192))
                        error.dav_message=tree.findtext('{DAV:}message') or tree.findtext('{http://sabredav.org/ns}message') or ''
                    except (OSError,http.client.HTTPException,ET.ParseError):
                        pass
                if storage.disk_error(error):
                    raise
                if isinstance(error, urllib.error.HTTPError) and error.code in (500, 502, 504):
                    delay = max(2**server_retries * random.uniform(.75, 1.25), network.retry_after(error.headers))
                    if server_retries >= 2 or delay > 60:
                        raise
                    if self.stopping.wait(delay):
                        raise InterruptedError('Nextcloud request interrupted') from error
                    server_retries += 1
                elif not retry.retry(network.failure(error, 'body' if method=='PUT' else 'connect')):
                    raise
                if isinstance(data, Body):
                    data.stream.seek(0)
                    data.sent = 0
                    data.digest = hashlib.sha256()
                details = error_details(error)
                print(f'NEXTCLOUD REQUEST RETRY: {method}; session={path}; '
                      f'{type(error).__name__}; HTTP={details["http_status"]}; '
                      f'reason={details["error_reason"]!r}; errno={details["error_errno"]}',flush=True)

    def upload(self, root, data, item, local, variant, target, size, percent, allow_session_restart=True):
        state=root/'.download/nextcloud-uploads.json'
        with d.STATE_LOCK:
            ledger=json.loads(state.read_text()) if state.exists() else {}
            stamp=local.stat();fingerprint=[stamp.st_ino,stamp.st_size,stamp.st_mtime_ns]
            key=hashlib.sha256(json.dumps([share_identity(self.share.url()),self.share.nickname,data['source'],item['path'],item['sha256'],variant,
                                        fingerprint if variant=='partial' else None]).encode()).hexdigest()
            old=ledger.get(key,{})
            journal=state.with_suffix('.jsonl')
            if old and old.get('state')!='uploaded' and journal.exists():
                completed = None
                for entry in d.iter_events(journal):
                    if entry.get('key') == key and entry.get('state') == 'uploaded':
                        completed = entry
                if completed:
                    self.save(state,completed)
                    return
            if old.get('state')=='uploaded':
                return
            row=dict(key=key,source=data['source'],path=item['path'],size=item['size'],sha256=item['sha256'],variant=variant,
                     target=target,upload_share=share_identity(self.share.url()),read_share=share_identity(Share(self.existing).url()) if self.existing else None,
                     local=str(local),local_stat=fingerprint,payload_size=size,bytes_percent=percent,state='uploading',
                     time=datetime.now().astimezone().isoformat(),nickname=self.share.nickname,
                     upload_id=old.get('upload_id') or uuid.uuid4().hex,
                     chunk_size=old.get('chunk_size',self.chunk_size),
                     next_chunk=0 if old.get('state')=='creating_collection' else old.get('next_chunk',0))
            row['sent_bytes']=min(size,row['next_chunk']*row['chunk_size'])
            self.save(state,row,record=True)
        headers={'Destination':self.share.url(target),'OC-Total-Length':str(size)}
        count=(size+row['chunk_size']-1)//row['chunk_size']
        if count>10000:
            raise ValueError('Nextcloud upload needs larger chunks for this video')
        restart=False
        try:
            if row['next_chunk']<count:
                collection_exists=False
                if row['next_chunk']:
                    try:
                        # Check existence without creating a new empty session after a lost response.
                        with self.request('PROPFIND',row['upload_id'],data=PROPERTIES,
                                headers={'Depth':'0','Content-Type':'application/xml'},uploads=True) as response:
                            if response.status!=207:
                                raise RuntimeError('Nextcloud did not confirm the upload session')
                            try:
                                tree=ET.fromstring(response.read())
                            except ET.ParseError as error:
                                raise RuntimeError('Invalid Nextcloud upload session response') from error
                            collection_exists=any(' 200 ' in prop.findtext('{DAV:}status','')
                                and prop.find('{DAV:}prop/{DAV:}resourcetype/{DAV:}collection') is not None
                                for prop in tree.findall('.//{DAV:}propstat'))
                            if not collection_exists:
                                raise RuntimeError('Nextcloud did not confirm the upload collection')
                    except urllib.error.HTTPError as error:
                        if error.code!=404:
                            raise
                        row.update(next_chunk=0,sent_bytes=0)
                if not collection_exists:
                    row['state']='creating_collection'
                    self.save(state,row)
                    try:
                        # Chunking v2 checks the cached parent before file-drop creates course folders.
                        with self.request('MKCOL',row['upload_id'],
                            headers={**headers,'Destination':self.share.url(row['upload_id']+'.upload')},uploads=True):
                            row['next_chunk']=0
                            row['sent_bytes']=0
                    except urllib.error.HTTPError as error:
                        if error.code!=405:
                            raise
                    row['state']='uploading'
                    self.save(state,row)
            print('NEXTCLOUD UPLOAD START: '+target,flush=True)
            digest=hashlib.sha256()
            with local.open('rb') as stream:
                for number in range(count):
                    if self.stopping.is_set():
                        raise InterruptedError('Nextcloud upload interrupted')
                    storage.check(local)
                    chunk=stream.read(min(row['chunk_size'],size-number*row['chunk_size']))
                    if len(chunk)!=min(row['chunk_size'],size-number*row['chunk_size']):
                        raise RuntimeError('Video truncated during upload')
                    digest.update(chunk)
                    if number<row['next_chunk']:
                        continue
                    import io
                    body=Body(io.BytesIO(chunk),len(chunk),self.stopping,local)
                    chunk_headers={**headers,'Content-Length':str(len(chunk)),
                        'OC-Checksum':'SHA256:'+hashlib.sha256(chunk).hexdigest()}
                    with self.request('PUT',row['upload_id']+f'/{number+1:05}',data=body,
                                            headers=chunk_headers,uploads=True) as response:
                        if response.status not in (200,201,204) or body.sent!=len(chunk):
                            raise RuntimeError('Nextcloud did not confirm the complete chunk')
                    after=local.stat()
                    if [after.st_ino,after.st_size,after.st_mtime_ns]!=fingerprint:
                        raise RuntimeError('Video changed during upload')
                    row.update(next_chunk=number+1,sent_bytes=min(size,(number+1)*row['chunk_size']))
                    self.save(state,row,record=True)
                    if self.concurrency:
                        self.concurrency.confirmed(len(chunk))
                    if self.upload_budget:self.upload_budget.confirmed(len(chunk))
                    print(f'NEXTCLOUD UPLOAD PROGRESS: {target}; {row["sent_bytes"]}/{size} bytes',flush=True)
            after=local.stat()
            if ([after.st_ino,after.st_size,after.st_mtime_ns]!=fingerprint
                    or variant=='original' and digest.hexdigest()!=item['sha256']):
                raise RuntimeError('Video changed during upload or was not sent completely')
            row.update(state='assembling',payload_sha256=digest.hexdigest())
            self.save(state,row)
            try:
                with self.request('MOVE',row['upload_id']+'/.file',
                    headers={**headers,'Overwrite':'F','OC-Checksum':'SHA256:'+digest.hexdigest()},uploads=True) as response:
                    if response.status!=201:
                        raise RuntimeError('Nextcloud did not confirm creation of the assembled video')
                    receipt=dict(http_status=response.status,etag=response.headers.get('ETag'),file_id=response.headers.get('OC-FileId'))
            except urllib.error.HTTPError as error:
                if error.code in (404,412):
                    row['state']='unconfirmed'
                raise
            row.update(state='uploaded',sent_bytes=size,**receipt)
            print('NEXTCLOUD UPLOADED: '+target,flush=True)
        except BaseException as error:
            row.update(state=row['state'] if row['state'] in ('creating_collection','assembling','unconfirmed') else 'pending',
                       **error_details(error))
            if (allow_session_restart and isinstance(error,urllib.error.HTTPError) and error.code==412
                    and getattr(error,'request_method',None)=='PUT'
                    and getattr(error,'dav_message','').startswith('Missing metadata for chunked upload.')):
                self.save(state,row,record=True)
                previous_id,previous_bytes=row['upload_id'],row['sent_bytes']
                row.update(state='pending',upload_id=uuid.uuid4().hex,next_chunk=0,sent_bytes=0)
                print(f'NEXTCLOUD SESSION RESET: {target}; missing server metadata; '
                      f'previous_session={previous_id}; previous_confirmed_bytes={previous_bytes}; '
                      f'new_session={row["upload_id"]}',flush=True)
                restart=True
            else:
                raise
        finally:
            row['time']=datetime.now().astimezone().isoformat()
            self.save(state,row,record=True)
        if restart:
            self.upload(root,data,item,local,variant,target,size,percent,allow_session_restart=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    scope=parser.add_mutually_exclusive_group(required=True)
    scope.add_argument('--queue',type=Path,help='Semester queue; uploads from all enabled archives')
    scope.add_argument('--destination',type=Path,help='One downloaded course archive')
    parser.add_argument('--nextcloud-existing',help='Read-only public share used to skip existing originals')
    parser.add_argument('--nextcloud-upload',required=True,help='Public file-drop share for missing originals and partial videos')
    parser.add_argument('--poll-interval',type=float,default=30)
    parser.add_argument('--timeout',type=float,default=120,help='WebDAV socket timeout, seconds')
    parser.add_argument('--nickname',help='File-request sender nickname; defaults to queue mark or yds-download')
    parser.add_argument('--chunk-size-mib',type=int,default=16,help='Resumable upload chunk size, MiB (minimum 5)')
    parser.add_argument('--transfers',type=int,default=4,help='Maximum simultaneous file uploads (default: 4)')
    parser.add_argument('--auto-transfers',action='store_true',help='Adjust file concurrency using confirmed upload speed')
    parser.add_argument('--min-transfers',type=int,default=2,help='Minimum concurrency with --auto-transfers (default: 2)')
    parser.add_argument('--upload-budget',type=Path,help='Shared automatic total upload budget directory; --transfers/--min-transfers are global limits')
    parser.add_argument('--once',action='store_true',help='Process currently available local videos once')
    args=parser.parse_args()
    if args.poll_interval<=0 or args.timeout<=0 or args.chunk_size_mib<5 or args.transfers<1:
        parser.error('Intervals and concurrency must be positive; chunk size must be at least 5 MiB')
    if (args.auto_transfers or args.upload_budget) and not 1<=args.min_transfers<=args.transfers:
        parser.error('Auto uploads require 1 <= --min-transfers <= --transfers')
    os.umask(0o077)
    stopping=Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stopping.set())
    control=args.queue if args.queue else args.destination/'.download/source.json'
    lockpath=control.with_suffix('.nextcloud.lock')
    with lockpath.open('a') as lock, storage.lock_archives([], 'nextcloud.lock') as claim, ExitStack() as resources:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        nickname=args.nickname or (json.loads(args.queue.read_text()).get('mark') if args.queue else None) or 'yds-download'
        sync=Sync(args.nextcloud_existing,args.nextcloud_upload,stopping,args.timeout,nickname,args.chunk_size_mib*1024**2,
                  transfers=args.transfers,auto_transfers=args.auto_transfers and not args.upload_budget,min_transfers=args.min_transfers)
        if args.upload_budget:
            sync.upload_budget=resources.enter_context(SharedUploadBudget(args.upload_budget,
                'nextcloud',args.min_transfers,args.transfers))
        while not stopping.is_set():
            storage.check(control)
            roots=[Path(a['destination']) for a in storage.read_queue(args.queue)['archives'] if a.get('enabled',True)] if args.queue else [args.destination.resolve()]
            sync.run_once(claim(roots))
            if args.once:
                break
            stopping.wait(args.poll_interval)


if __name__=='__main__':
    try:
        raise SystemExit(d.cli(main))
    except OSError as error:
        if not storage.disk_error(error):
            raise
        print('DISK OFFLINE: Nextcloud upload state retained',flush=True)
        raise SystemExit(storage.TEMPFAIL)
