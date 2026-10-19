"""Recover manifest originals from shared cloud metadata, then verify local SHA-256."""
import http.client
import json
from pathlib import Path, PurePosixPath
import re
from threading import Lock
import time
import unicodedata
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET

import download as d
import file_hashes
import network
from nextcloud import VIDEO, normalized, remote_path
from nextcloud_fallback import Fallback
import opencloud as oc
import recovery as r
import storage


class OpenCloudIndex:
    global_candidates=True
    catalogs={}
    catalogs_lock=Lock()

    def __init__(self, config, stopping):
        self.link=config['server'].rstrip('/')+'/?opencloud-user='+urllib.parse.quote(config['username'],safe='')
        secret=Path(config['password_file']).resolve()
        password=secret.read_text().rstrip('\r\n')
        if not password:raise ValueError('Empty OpenCloud password file')
        self.share=oc.Client(config['server'],config['username'],password,timeout=60)
        self.stopping=stopping
        self.folders={}
        key=(self.link,str(secret),tuple(Fallback.stamp(secret)))
        with self.catalogs_lock:
            if key not in self.catalogs:
                if len(self.catalogs)>=16:self.catalogs.pop(next(iter(self.catalogs)))
                self.catalogs[key]=dict(rows=None,error=None,retry_at=0,expires=0,base=None,lock=Lock())
            self.catalog=self.catalogs[key]

    def candidates(self, data, item):
        catalog=self.catalog
        with catalog['lock']:
            cooling=catalog['error'] is not None and time.monotonic()<catalog['retry_at']
            if cooling and catalog['rows'] is None:
                raise catalog['error'].with_traceback(None)
            if not cooling and (catalog['rows'] is None or catalog['error'] is not None or time.monotonic()>=catalog['expires']):
                try:
                    if catalog['base'] is None:
                        self.share.discover()
                        catalog['base']=self.share.base
                    self.share.base=catalog['base']
                    # scan traverses the selected project Space with PROPFIND only.
                    rows=oc.Uploader(self.share,self.stopping).scan()
                    catalog.update(rows=rows,error=None,retry_at=0,expires=time.monotonic()+300)
                except (OSError,ValueError,RuntimeError,http.client.HTTPException,ET.ParseError) as error:
                    if isinstance(error,InterruptedError) or storage.disk_error(error):raise
                    code=getattr(error,'code',None)
                    transient=(code in (408,429,500,502,503,504)
                               or network.failure(error)['category'] in ('routing','ambiguous')
                               or isinstance(error,ET.ParseError))
                    cooldown=max(60,network.retry_after(getattr(error,'headers',None)))
                    if isinstance(error,urllib.error.HTTPError):error.close()
                    if not transient:catalog.update(rows=None,base=None)
                    catalog.update(error=error.with_traceback(None),retry_at=time.monotonic()+cooldown)
                    print(f'OPENCLOUD CATALOG DEFERRED: cached={catalog["rows"] is not None}; '
                          f'retry_after_seconds={cooldown:g}; HTTP={code}; error={type(error).__name__}',flush=True)
                    if catalog['rows'] is None:raise
            if catalog['base'] is not None:self.share.base=catalog['base']
            try:relative=remote_path(data,item)
            except ValueError:relative=None
            result=[]
            for row in catalog['rows']:
                path=PurePosixPath(row['path'])
                if row.get('directory') or path.suffix.casefold() not in VIDEO:continue
                hashes=oc.hashes(row)
                comparable=next((kind for kind in ('sha256','sha1','md5') if hashes.get(kind) and item.get(kind)),None)
                if comparable:
                    if hashes[comparable]==item[comparable].lower():result.append(row)
                    continue
                if relative is None or row.get('size')!=item['size']:continue
                plain=normalized(re.sub(r'^\d+\s*-\s*','',path.name))
                expected=PurePosixPath(relative)
                # User prefixes may differ, but course and semester must agree.
                parent=unicodedata.normalize('NFC',str(path.parent))
                if (plain==expected.name and (parent==str(expected.parent) or parent.endswith('/'+str(expected.parent)))):
                    result.append(row)
            return result


class CloudFallback:
    def __init__(self, root, nextcloud_link, stopping, opencloud=None, budget=None, s3=None):
        self.root,self.stopping=Path(root),stopping
        self.sources=[]
        def source_budget(name):return budget.get(name) if isinstance(budget,dict) else budget
        if s3:
            from s3_fallback import S3Index
            index=S3Index(s3,stopping)
            self.sources.append(Fallback(root,index.link,stopping,index=index,budget=source_budget('s3'),label='S3'))
        if nextcloud_link:
            self.sources.append(Fallback(root,nextcloud_link,stopping,budget=source_budget('nextcloud')))
        if opencloud:
            index=OpenCloudIndex(opencloud,stopping)
            self.sources.append(Fallback(root,index.link,stopping,index=index,budget=source_budget('opencloud'),label='OPENCLOUD'))
        self.link=json.dumps([source.index.link for source in self.sources],ensure_ascii=False)
        self.manifest_stamp=None
        self.manifest={}
        self.local_proofs={}
        self.event_index=d.EventIndex(self.root/'.download/events.jsonl',by_copy=True,repair=False)
        self.event_proofs={}
        self.event_proofs_stamp=None

    def set_stopping(self, stopping):
        self.stopping=stopping
        for source in self.sources:
            source.stopping=stopping
            source.index.stopping=stopping

    def _listed(self,item):
        paths=sorted((self.root/'.download').glob('*-manifest.json'))
        stamp=[(str(path),Fallback.stamp(path)) for path in paths]
        if stamp!=self.manifest_stamp:
            entries={}
            for path in paths:
                entries.update({row['path']:row for row in json.loads(path.read_text())})
            self.manifest,self.manifest_stamp=entries,stamp
        original=self.manifest.get(item['path'],{})
        matches=(original.get('size'),original.get('sha256'))==(item['size'],item['sha256'])
        if matches:
            for kind in ('md5','sha1'):
                if original.get(kind):item[kind]=original[kind]
        return matches

    def _local(self,item,event=None,verify=True):
        target=d.local_path(self.root,item['path'])
        paths={target:[]}
        source=self.sources[0].source['source']
        with d.STATE_LOCK:
            proofs=self.event_index.refresh()
            stamp=(self.event_index.device,self.event_index.inode,self.event_index.offset)
            if stamp!=self.event_proofs_stamp:
                by_path={}
                for row in proofs.values():
                    by_path.setdefault(row['path'],[]).append(row)
                self.event_proofs,self.event_proofs_stamp=by_path,stamp
            rows=list(self.event_proofs.get(item['path'],()))
        if event:rows.append(event)
        for row in rows:
            path=storage.saved_local(self.root, row['local']) if row.get('local') else target
            paths.setdefault(path,[]).append(row)
        registry=self.root/'.download/local-copies.json'
        if registry.exists():
            for row in json.loads(registry.read_text()):
                if row.get('source')==source and row.get('path')==item['path'] and row.get('status')=='valid':
                    paths.setdefault(storage.saved_local(self.root, row['local']),[]).append(row)
        for fallback in self.sources:
            row=fallback.read(item)
            if (row.get('state')=='verified' and row.get('source')==source
                    and row.get('actual_sha256')==item['sha256']):
                paths.setdefault(storage.saved_local(self.root, row['local']),[]).append(row)
        for path,proofs in paths.items():
            if not path.is_file() or Path(str(path)+'.aria2').exists():continue
            stamp=Fallback.stamp(path)
            if stamp[1]!=item['size']:continue
            trusted=False
            for proof in proofs:
                if ((proof.get('path'),proof.get('size'),proof.get('sha256'))!=(item['path'],item['size'],item['sha256'])
                        or proof.get('source',source)!=source
                        or proof.get('status')!='valid' and proof.get('state')!='verified'
                        or proof.get('actual_sha256',item['sha256'])!=item['sha256']):continue
                saved_stat=proof.get('local_stat',proof.get('recovered_stat'))
                if proof.get('ctime_ns') is not None:
                    trusted=file_hashes.proof_current(path,proof,root=self.root)
                elif saved_stat is not None:
                    trusted=list(saved_stat)==stamp
                else:trusted=file_hashes.proof_current(path,proof,root=self.root)
                if trusted:break
            key=(str(path),item['sha256'])
            if self.local_proofs.get(key)!=stamp:
                if not trusted:
                    cached=file_hashes.load(path,root=self.root)
                    if cached and cached['sha256']==item['sha256'].lower():
                        trusted=True
                    elif not verify or d.sha256_file(path,self.stopping)!=item['sha256'].lower():
                        continue
                    if Fallback.stamp(path)!=stamp:continue
                self.local_proofs[key]=stamp
            return path,stamp
        return None

    def ready(self,item):
        return self._listed(item) and self._local(item,verify=False) is None and any(source.ready(item) for source in self.sources)

    def recover_all(self,item,event):
        if not self._listed(item):return None
        local=self._local(item,event)
        if local is not None:
            path,stamp=local
            if any(source.read(item).get('state')=='verified' and source.read(item).get('local_stat')==stamp for source in self.sources):return None
            proof=d.record(self.root/'.download',item,event['course'],event['attempt'],'valid',
                           'Existing local original; SHA-256 verified',local=path,verified_stat=stamp)
            if proof['status']!='valid':return None
            result=dict(event,**proof,range_recovered_complete=True,recovered_stat=stamp)
            r.recovery_state(self.root,result,'finished')
            return result
        # An interrupted body gets its turn before a new source's payload.
        def priority(source):
            row=source.read(item)
            if row.get('state') not in ('pending','downloading') or not row.get('remote_path'):return 1
            key=source.body_key(row['remote_path'],row['remote_size'])
            part=source.directory/(source.state_path(item).stem+'-'+key+'.part')
            return 0 if part.is_file() and part.stat().st_size>0 or row.get('received_bytes',0) else 1
        result=None
        for source in sorted(self.sources,key=priority):
            if self.stopping.is_set():raise InterruptedError('Cloud fallback interrupted')
            if not source.ready(item):continue
            retained=set();digests=[]
            for previous in self.sources:
                for copy in previous.read(item).get('retained_copies',{}).values():
                    path=storage.saved_local(self.root, copy['local'])
                    if path.is_file() and Fallback.stamp(path)==copy['local_stat']:
                        retained.add(copy['actual_sha256'])
                        digests.append(dict(copy.get('actual_hashes',{}),sha256=copy['actual_sha256']))
            current=source.recover_all(item,event,retained_hashes=retained,retained_digests=digests)
            if current:
                result=current
                if current['status']=='valid':return current
        return result
