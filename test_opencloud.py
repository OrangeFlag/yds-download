"""OpenCloud only publishes confirmed uploads and never overwrites existing videos."""
import hashlib
import gc
import sys
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
import io
import json
import os
from contextlib import contextmanager
from pathlib import Path
import tempfile
from threading import Barrier, Event, Lock
import unittest
import urllib.error
from unittest.mock import patch

import download as d
import opencloud as oc


class Response(io.BytesIO):
    def __init__(self, status=204, headers=None, body=b''):
        super().__init__(body)
        self.status, self.headers = status, headers or {}


class Server:
    nickname = 'Walterit'
    base = 'https://cloud.example/dav/spaces/project/'
    def __init__(self):
        self.payload = bytearray()
        self.calls = []
        self.files = {}
        self.drop_patch = False
        self.fail_move = False
    def url(self, path=''):
        return self.base+path
    def safe_url(self, url):
        if not url.startswith('https://cloud.example/'):
            raise ValueError('Unsafe URL')
        return url
    def listing(self, folder):
        return [dict(path=k, size=len(v), checksums=['SHA256:'+hashlib.sha256(v).hexdigest()], directory=False)
                for k,v in self.files.items() if str(Path(k).parent)==folder]
    def request(self, method, path='', data=None, headers=None):
        headers=headers or {};self.calls.append((method,path,headers))
        if method=='MKCOL': return Response(201)
        if method=='POST':
            import base64
            meta=dict(x.split(' ',1) for x in headers['Upload-Metadata'].split(','))
            self.stage=str(Path(path)/base64.b64decode(meta['filename']).decode())
            self.payload=bytearray()
            return Response(201,{'Location':'https://cloud.example/data/session'})
        if method=='HEAD':
            return Response(200,{'Upload-Offset':str(len(self.payload)), 'Upload-Length':str(self.size)})
        if method=='PATCH':
            self.assert_offset=int(headers['Upload-Offset'])
            if self.assert_offset!=len(self.payload): raise AssertionError('Wrong resume offset')
            self.payload.extend(data)
            if len(self.payload)==self.size:self.files[self.stage]=bytes(self.payload)
            if self.drop_patch:
                self.drop_patch=False
                raise urllib.error.URLError('connection lost after acceptance')
            return Response(204,{'Upload-Offset':str(len(self.payload))})
        if method=='MOVE':
            if self.fail_move:
                raise urllib.error.HTTPError(path,412,'conflict',{},None)
            self.files[headers['Destination'].removeprefix(self.base)]=self.files.pop(path)
            return Response(201)
        if method=='GET':return Response(200,body=self.files[path])
        raise AssertionError(method)


class UploadChecks(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.root=Path(tmp.name);self.state=self.root/'.download';self.state.mkdir()
        self.data=dict(source='https://disk.yandex.ru/d/test', title='ШАД осень 2024')
        self.content=b'original lecture video'
        self.item=dict(path='/Course/Lecture.mp4',size=len(self.content),sha256=hashlib.sha256(self.content).hexdigest(),file='https://example.invalid/video')
        self.local=d.local_path(self.root,self.item['path']);self.local.parent.mkdir();self.local.write_bytes(self.content)
        d.write_json(self.state/'source.json',self.data)
        d.write_json(self.state/'01-manifest.json',[self.item])
        d.append_events(self.state/'events.jsonl',[dict(self.item,status='valid',mtime_ns=self.local.stat().st_mtime_ns,local=str(self.local))])
        self.server=Server();self.server.size=len(self.content)
        self.uploader=oc.Uploader(self.server,Event(),chunk_size=7)

    def job(self):return self.uploader.jobs([self.root])[0]

    def legacy_checkpoint(self, job):
        key=hashlib.sha256(json.dumps([self.server.url(),self.uploader.folder,job[2]['source'],
            job[3]['path'],job[5],str(job[4]),job[6],job[10]],ensure_ascii=False).encode()).hexdigest()
        return job[1]/'.download/opencloud-uploads'/key[:2]/(key+'.json')

    def seed_lost_session(self, sent_bytes=7):
        self.server.timeout=0
        job=self.job()
        row=dict(source=self.data['source'],path=self.item['path'],variant='original',
                 target='ingest/Walterit/2024-fall/Course/001 - Lecture.mp4',
                 payload_sha256=self.item['sha256'],payload_sha1=hashlib.sha1(self.content).hexdigest(),
                 payload_md5=hashlib.md5(self.content).hexdigest(),size=len(self.content),
                 sent_bytes=sent_bytes,state='pending',session_url='https://cloud.example/data/lost',
                 local_fingerprint=oc.file_hashes.fingerprint(self.local))
        self.uploader.save_record(job,row);self.uploader.index=[]
        return job,row

    def test_checkpoint_updates_reuse_durable_parent_directories(self):
        job = self.job()
        row = dict(state='uploading', sent_bytes=4, session_url='https://cloud.example/private')
        parent = self.uploader.checkpoint(job).parent.parent
        real_sync = oc.storage.sync_directory
        with patch.object(oc.storage, 'sync_directory', wraps=real_sync) as sync:
            self.uploader.save_record(job, row)
            self.assertIn(parent, [call.args[0] for call in sync.call_args_list])
            sync.reset_mock()
            row['sent_bytes'] = 8
            self.uploader.save_record(job, row)
            self.assertNotIn(parent, [call.args[0] for call in sync.call_args_list],
                             'Already durable checkpoint parent was flushed again')
        self.assertEqual(json.loads(self.uploader.checkpoint(job).read_text())['sent_bytes'], 8)
        events = list(d.iter_events(self.state/'opencloud-uploads.jsonl'))
        self.assertEqual([event['sent_bytes'] for event in events], [4, 8])
        self.assertTrue(all('session_url' not in event for event in events))

    def test_checkpoint_retries_failed_parent_sync_before_confirming_progress(self):
        job = self.job()
        checkpoint = self.uploader.checkpoint(job)
        parent = checkpoint.parent.parent
        row = dict(state='uploading', sent_bytes=4)
        real_sync = oc.storage.sync_directory
        def fail_parent(path):
            if path == parent:
                raise OSError(5, 'injected directory sync failure')
            real_sync(path)
        with patch.object(oc.storage, 'sync_directory', side_effect=fail_parent):
            with self.assertRaises(OSError):
                self.uploader.save_record(job, row)
        self.assertTrue(checkpoint.parent.is_dir())
        self.assertFalse(checkpoint.exists())
        with patch.object(oc.storage, 'sync_directory', wraps=real_sync) as sync:
            self.uploader.save_record(job, row)
            self.assertIn(parent, [call.args[0] for call in sync.call_args_list])
        self.assertEqual(json.loads(checkpoint.read_text())['sent_bytes'], 4)

    def test_checkpoint_first_touch_confirms_existing_directory_entries(self):
        job = self.job()
        checkpoint = self.uploader.checkpoint(job)
        checkpoint.parent.mkdir(parents=True)
        with patch.object(oc.storage, 'sync_directory', wraps=oc.storage.sync_directory) as sync:
            self.uploader.save_record(job, dict(state='uploading', sent_bytes=4))
            self.assertIn(checkpoint.parent.parent, [call.args[0] for call in sync.call_args_list])

    def test_checkpoint_identity_survives_remount_and_reads_legacy_session(self):
        import shutil
        job=self.job()
        previous=self.legacy_checkpoint(job)
        previous.parent.mkdir(parents=True,exist_ok=True)
        row=dict(source=self.data['source'],path=self.item['path'],variant='original',
                 size=self.item['size'],payload_sha256=self.item['sha256'],
                 session_url='https://cloud.example/data/oldsession',
                 target='ingest/Walterit/2024-fall/Course/001 - Lecture.mp4',
                 state='uploading',sent_bytes=7)
        d.write_json(previous,row)
        moved=self.root/'new_mount'
        (moved/'.download').mkdir(parents=True)
        shutil.copytree(self.state/'opencloud-uploads',moved/'.download/opencloud-uploads')
        new_job=(*job[:1],moved,*job[2:4],moved/'Course/Lecture.mp4',*job[5:])
        self.assertEqual(self.uploader.checkpoint(new_job).relative_to(moved),
                         self.uploader.checkpoint(job).relative_to(self.root))
        oc.storage.migrate(self.root,moved,'uuid',self.root/'custom-state')
        self.assertEqual(self.uploader.record(new_job).get('session_url'),row['session_url'])

    def test_external_original_copy_uploads(self):
        with tempfile.TemporaryDirectory() as directory:
            external=Path(directory)/self.local.name
            self.local.rename(external)
            d.write_json(self.state/'local-copies.json',[dict(self.item,source=self.data['source'],
                status='valid',local=str(external),mtime_ns=external.stat().st_mtime_ns)])
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
            self.assertIn(self.content,self.server.files.values())

    def test_legacy_checkpoint_requires_exact_destination_and_local_version(self):
        job=self.job();path=self.legacy_checkpoint(job);path.parent.mkdir(parents=True)
        path.write_text(json.dumps(dict(source=self.data['source'],path=self.item['path'],variant='original',
            size=job[6],payload_sha256=self.item['sha256'],state='uploaded',sent_bytes=job[6])))
        self.assertEqual(self.uploader.record(job)['state'],'uploaded')
        for changed in ('nickname','base'):
            server=Server();setattr(server,changed,getattr(server,changed)+'changed')
            self.assertEqual(oc.Uploader(server,Event()).record(job),{})
        self.assertEqual(oc.Uploader(self.server,Event()).record((*job[:10],job[10]+1)),{})

    def test_legacy_partial_checkpoint_keeps_its_actual_payload_identity(self):
        job=self.job();job=(*job[:5],'partial',job[6]-1,75,*job[8:])
        path=self.legacy_checkpoint(job);path.parent.mkdir(parents=True)
        row=dict(source=self.data['source'],path=self.item['path'],variant='partial',size=job[6],
            payload_sha256='f'*64,state='uploading',sent_bytes=7,session_url='https://cloud.example/data/old')
        path.write_text(json.dumps(row))
        self.assertEqual(self.uploader.record(job),row)

    def test_webdav_scan_uses_exact_server_spelling_for_unicode_folder(self):
        raw='Cafe\u0301 '
        normalized='Café '
        def listing(folder):
            if folder=='':return [dict(path=normalized,download_path=raw,directory=True)]
            if folder==raw:return [dict(path=raw+'/video.mp4',directory=False,size=1,checksums=[])]
            raise AssertionError('Normalized folder used for PROPFIND: '+repr(folder))
        with patch.object(self.server,'listing',side_effect=listing):
            result=self.uploader.scan()
        self.assertEqual(len(result),1)

    def test_webdav_scan_preserves_trailing_space_in_directory_name(self):
        from urllib.parse import quote
        client=oc.Client('https://cloud.example','Walterit','secret')
        client.base='https://cloud.example/dav/spaces/project/'
        folder='2024-fall/Course '
        files=[f'{folder}/video-{i}.mp4' for i in range(9)]
        visited=[]
        def response(rows):
            xml='<d:multistatus xmlns:d="DAV:">'
            for path,is_dir in rows:
                href='/dav/spaces/project/'+quote(path,safe='/')+('/' if is_dir else '')
                xml+=('<d:response><d:href>'+href+'</d:href><d:propstat><d:prop>'
                      '<d:resourcetype>'+('<d:collection/>' if is_dir else '')+'</d:resourcetype>'
                      '<d:getcontentlength>1</d:getcontentlength>'
                      '</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>')
            return Response(207,body=(xml+'</d:multistatus>').encode())
        def request(method,path='',data=None,headers=None):
            visited.append(path)
            if path=='':return response([('2024-fall',True)])
            if path=='2024-fall':return response([(folder,True)])
            if path==folder:return response([(name,False) for name in files])
            raise AssertionError('Wrong WebDAV directory: '+repr(path))
        client.request=request
        uploader=oc.Uploader(client,Event())
        found=uploader.scan()
        self.assertEqual(len(found),9)
        self.assertEqual([row['path'] for row in found],files)
        self.assertEqual(visited,['','2024-fall',folder])

    def test_dry_run_makes_no_upload_requests(self):
        self.uploader.run_once([self.root])
        self.assertEqual(self.server.calls,[])

    def test_original_uses_numbered_semester_path_and_durable_receipt(self):
        self.uploader.run_once([self.root],upload=True)
        target='ingest/Walterit/2024-fall/Course/001 - Lecture.mp4'
        self.assertEqual(self.server.files[target],self.content)
        self.assertEqual(self.uploader.record(self.job())['state'],'uploaded')
        before=len(self.server.calls)
        self.uploader.run_once([self.root],upload=True)
        self.assertEqual(len(self.server.calls),before)
        self.assertTrue((self.state/'opencloud-uploads.jsonl').exists())
        self.assertEqual((self.state/'opencloud-uploads').stat().st_mode & 0o777,0o700)

    def test_lost_patch_response_resumes_from_server_offset(self):
        self.server.drop_patch=True
        self.uploader.run_once([self.root],upload=True)
        self.assertEqual(self.uploader.record(self.job())['state'],'uploaded')
        offsets=[int(h['Upload-Offset']) for m,p,h in self.server.calls if m=='PATCH']
        self.assertEqual(offsets,[0,7,14,21])
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)
        self.assertEqual(sum(m=='HEAD' for m,p,h in self.server.calls),2)

    def test_shared_budget_counts_accepted_patch_after_lost_response_only_once(self):
        with oc.nextcloud.SharedUploadBudget(self.root/'budget','opencloud',2,8) as budget:
            self.uploader.upload_budget=budget
            self.server.drop_patch=True
            self.uploader.run_once([self.root],upload=True)
            state=json.loads((self.root/'budget/state.json').read_text())
            self.assertEqual(state['controller']['total'],22)
            self.uploader.run_once([self.root],upload=True)
            self.assertEqual(json.loads((self.root/'budget/state.json').read_text())['controller']['total'],22)
            self.assertTrue(all(row['active']==row['waiting']==0 for row in state['members'].values()))

    def test_successful_patch_offsets_need_only_one_head(self):
        self.uploader.run_once([self.root],upload=True)
        self.assertEqual(sum(m=='HEAD' for m,p,h in self.server.calls),1)
        self.assertEqual(sum(m=='PATCH' for m,p,h in self.server.calls),4)

    def test_resumed_upload_reuses_verified_hashes_without_rereading_video(self):
        self.server.drop_patch=True;self.uploader.retries=0
        self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        self.assertEqual(len(self.server.payload),7)
        real_open=Path.open
        @contextmanager
        def guarded(path,*args,**kwargs):
            with real_open(path,*args,**kwargs) as stream:
                if path==self.local and (args[0] if args else kwargs.get('mode'))=='rb':
                    real_read=stream.read
                    def resumed_read(size=-1):
                        if stream.tell()<7:raise AssertionError('Unchanged video hashed again')
                        return real_read(size)
                    with patch.object(stream,'read',side_effect=resumed_read):
                        yield stream
                else:yield stream
        fresh=oc.Uploader(self.server,Event(),chunk_size=7)
        with patch.object(Path,'open',guarded):
            self.assertEqual(fresh.run_once([self.root],upload=True),0)
        self.assertEqual(fresh.record(self.job())['state'],'uploaded')
        self.assertEqual(sum(method=='POST' for method,_,_ in self.server.calls),1)
        self.assertEqual([int(headers['Upload-Offset']) for method,_,headers in self.server.calls if method=='PATCH'],[0,7,14,21])

    def test_download_verification_hashes_are_reused_for_first_upload(self):
        d.sha256_file(self.local)
        real_open=Path.open;read_bytes=[]
        @contextmanager
        def counted(path,*args,**kwargs):
            with real_open(path,*args,**kwargs) as stream:
                if path==self.local and (args[0] if args else kwargs.get('mode'))=='rb':
                    real_read=stream.read
                    def read(size=-1):
                        block=real_read(size);read_bytes.append(len(block));return block
                    with patch.object(stream,'read',side_effect=read):yield stream
                else:yield stream
        with patch.object(Path,'open',counted):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(sum(read_bytes),len(self.content))
        self.assertIn(self.content,self.server.files.values())

    def test_hash_cache_rejects_modified_video_even_when_size_and_mtime_are_restored(self):
        self.server.fail_move=True
        self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        before=self.local.stat()
        self.local.write_bytes(b'x'*len(self.content))
        os.utime(self.local,ns=(before.st_atime_ns,before.st_mtime_ns))
        self.assertNotEqual(before.st_ctime_ns,self.local.stat().st_ctime_ns)
        self.server.fail_move=False;self.server.calls.clear()
        fresh=oc.Uploader(self.server,Event(),chunk_size=7)
        self.assertEqual(fresh.run_once([self.root],upload=True),1)
        self.assertFalse(any(method=='MOVE' for method,_,_ in self.server.calls))

    def test_two_upload_slots_overlap_and_limit_never_exceeds_budget(self):
        uploader=oc.Uploader(self.server,Event(),transfers=2)
        uploader.index=[];barrier=Barrier(2);lock=Lock();calls=[]
        def transfer(job):
            with lock:calls.append(job)
            barrier.wait(timeout=3)
            return True
        with patch.object(uploader,'jobs',return_value=[self.job()]*4),patch.object(uploader,'transfer',side_effect=transfer):
            self.assertEqual(uploader.run_once([self.root],upload=True,limit=2),0)
        self.assertEqual(len(calls),2)

    def test_parallel_identical_payloads_are_uploaded_only_once(self):
        uploader=oc.Uploader(self.server,Event(),transfers=2)
        with patch.object(uploader,'jobs',return_value=[self.job()]*2):
            self.assertEqual(uploader.run_once([self.root],upload=True),0)
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)

    def test_checksum_lookup_decodes_remote_rows_once_per_index(self):
        self.uploader.index=[
            dict(path=f'other/{i}.mp4', size=len(self.content),
                 checksums=['SHA256:'+format(i,'064x'), 'MD5:'+format(i,'032x')],
                 directory=False)
            for i in range(64)]
        with patch.object(oc, 'hashes', wraps=oc.hashes) as decoded:
            for _ in range(5):
                self.assertFalse(self.uploader.exists('f'*64))
                self.assertFalse(self.uploader.name_exists('missing.mp4',len(self.content)))
                self.assertFalse(self.uploader.original_exists(
                    dict(self.item, md5='f'*32)))
            self.assertLessEqual(decoded.call_count, len(self.uploader.index))

    def test_concurrent_checksum_lookup_builds_only_one_index(self):
        self.uploader.index=[dict(path=f'other/{i}.mp4',size=i+1,
                                  checksums=['SHA256:'+format(i,'064x')],directory=False)
                             for i in range(64)]
        barrier=Barrier(4)
        original=oc.hashes
        def parse(row):
            time.sleep(.002)
            return original(row)
        def lookup(_):
            barrier.wait(timeout=5)
            return self.uploader.exists('f'*64)
        with patch.object(oc,'hashes',side_effect=parse) as parsed:
            with ThreadPoolExecutor(max_workers=4) as workers:
                self.assertEqual(list(workers.map(lookup,range(4))),[False]*4)
        self.assertEqual(parsed.call_count,len(self.uploader.index))

    def test_polling_releases_previous_checksum_index_and_catalog(self):
        class Rows(list):pass
        rows=Rows([dict(path='remote/video.mp4',size=10,
                        checksums=['SHA256:'+'a'*64],directory=False)])
        self.uploader.index=rows
        self.assertFalse(self.uploader.exists('f'*64))
        old=weakref.ref(rows)
        del rows
        password=self.root/'password.txt';password.write_text('secret')
        class Stop:
            def __init__(self):self.polls=0
            def is_set(self):return False
            def wait(self,_):
                self.polls+=1
                return self.polls>=2
        from contextlib import contextmanager
        @contextmanager
        def locked(*_):
            yield lambda roots:roots
        argv=['opencloud.py','--destination',str(self.root),'--server','https://cloud.example',
              '--username','Walterit','--password-file',str(password),
              '--upload','--poll-interval','1']
        with patch.object(oc,'Uploader',return_value=self.uploader), \
                patch.object(self.uploader,'run_once',return_value=0), \
                patch.object(oc,'Client'),patch.object(oc.storage,'lock_archives',side_effect=locked), \
                patch.object(oc.storage,'check'),patch.object(oc.signal,'signal'), \
                patch.object(oc.os,'umask'),patch.object(oc,'Event',return_value=Stop()), \
                patch.object(sys,'argv',argv):
            self.assertEqual(oc.main(),0)
        gc.collect()
        self.assertIsNone(old())

    def test_checksum_lookup_tracks_new_rows_and_replaced_catalog(self):
        known='a'*64
        md5='b'*32
        self.uploader.index=[]
        self.assertFalse(self.uploader.exists(known))
        self.uploader.index.append(dict(path='other/file.mp4',size=1,
                                        checksums=['SHA256:'+known],directory=False))
        self.assertTrue(self.uploader.exists(known))
        self.uploader.index.append(dict(path='other/another.mp4',size=2,
                                        checksums=['MD5:'+md5],directory=False))
        self.assertTrue(self.uploader.exists('z'*64,md5=md5))
        self.uploader.index=[dict(path='other/001 - Lecture.mp4',size=17,
                                  checksums=[],directory=False)]
        self.assertFalse(self.uploader.exists(known))
        self.assertTrue(self.uploader.name_exists('Lecture.mp4',17))
        self.assertFalse(self.uploader.name_exists('Lecture.mp4',18))

    def test_checksum_lookup_refreshes_after_remote_digest_is_added(self):
        self.server.files['remote/file.mp4']=self.content
        row=dict(path='remote/file.mp4',size=len(self.content),checksums=[],directory=False)
        self.uploader.index=[row]
        self.assertFalse(self.uploader.exists(self.item['sha256']))
        self.assertEqual(self.uploader.remote_digest(row),self.item['sha256'])
        self.assertTrue(self.uploader.exists(self.item['sha256']))

    def test_checksum_lookup_rebuilds_after_catalog_appends_during_iteration(self):
        added=dict(path='remote/new.mp4',size=1,checksums=['SHA256:'+'b'*64],directory=False)
        class Growing(list):
            def __iter__(self):
                yield from self[:]
                if len(self)==1:
                    self.append(added)
        self.uploader.index=Growing([dict(path='remote/old.mp4',size=1,
                                          checksums=['SHA256:'+'a'*64],directory=False)])
        self.assertFalse(self.uploader.exists('b'*64))
        self.assertTrue(self.uploader.exists('b'*64))

    def test_checksum_lookup_rebuilds_after_checksum_changes_during_iteration(self):
        self.server.files['remote/video.mp4']=self.content
        row=dict(path='remote/video.mp4',size=len(self.content),checksums=[],directory=False)
        uploader=self.uploader
        class Changing(list):
            def __iter__(self):
                yield from self[:]
                if not row['checksums']:
                    uploader.remote_digest(row)
        uploader.index=Changing([row])
        self.assertFalse(uploader.exists(self.item['sha256']))
        self.assertTrue(uploader.exists(self.item['sha256']))

    def test_conflicting_known_hash_does_not_fall_back_to_name_and_size(self):
        self.uploader.index=[dict(path='other/Lecture.mp4',size=len(self.content),
                                 checksums=['SHA256:'+'0'*64],directory=False)]
        self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)

    def test_different_algorithm_checksum_does_not_prove_same_name_and_size(self):
        # A remote SHA1 is not proof of the local SHA256: hash the local file first.
        self.uploader.index=[dict(path='other/Lecture.mp4',size=len(self.content),
                                  checksums=['SHA1:'+'0'*40],directory=False)]
        self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(sum(method=='POST' for method,_,_ in self.server.calls),1)

    def test_computed_md5_skips_existing_copy_when_yandex_metadata_has_no_md5(self):
        self.uploader.index=[dict(path='other/Lecture.mp4',size=len(self.content),
                                  checksums=['MD5:'+hashlib.md5(self.content).hexdigest()],directory=False)]
        with patch.object(d,'api',return_value=self.item):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.server.calls,[])

    def test_known_sha1_suppresses_partial_without_payload_reads(self):
        (self.state/'events.jsonl').write_text('')
        self.local.write_bytes(b'x'*len(self.content))
        item=dict(self.item,sha1=hashlib.sha1(self.content).hexdigest())
        d.write_json(self.state/'01-manifest.json',[item])
        d.write_json(self.state/'partial-copies.json',[dict(item,source=self.data['source'],
            recovered_path='Course/Lecture.mp4',local_size=len(self.content),downloaded_bytes=15,mtime_ns=self.local.stat().st_mtime_ns)])
        self.uploader.index=[dict(path='other/Lecture.mp4',size=len(self.content),checksums=['SHA1:'+item['sha1']],directory=False)]
        self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.server.calls,[])

    def test_known_md5_matches_without_name_or_size_filters(self):
        item=dict(self.item,md5=hashlib.md5(self.content).hexdigest())
        self.uploader.index=[dict(path='other/unrelated.mp4',size=1,checksums=['MD5:'+item['md5']],directory=False)]
        self.assertTrue(self.uploader.original_exists(item))

    def test_restart_retains_confirmed_session_and_move_conflict(self):
        self.server.fail_move=True
        self.uploader.run_once([self.root],upload=True)
        self.assertEqual(self.uploader.record(self.job())['state'],'pending')
        self.server.fail_move=False
        oc.Uploader(self.server,Event(),chunk_size=7).run_once([self.root],upload=True)
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)
        self.assertEqual(sum(m=='PATCH' for m,p,h in self.server.calls),4)

    def test_duplicate_anywhere_in_space_is_not_uploaded(self):
        self.uploader.index=[dict(path='another-user/renamed.mp4',size=len(self.content),checksums=['SHA256:'+self.item['sha256']],directory=False)]
        self.uploader.run_once([self.root],upload=True)
        self.assertEqual(self.server.calls,[])

    def test_same_name_and_size_in_starpheonix_is_skipped_without_download(self):
        self.uploader.index=[dict(path='ingest/starphoenix/Rust/019 - Lecture.mp4',size=len(self.content),checksums=[],directory=False)]
        self.uploader.run_once([self.root],upload=True)
        self.assertEqual(self.server.calls,[])

    def test_sha1_metadata_skips_renamed_video_without_remote_download(self):
        self.uploader.index=[dict(path='ingest/another-user/renamed.mp4',size=len(self.content),
            checksums=['SHA1:'+hashlib.sha1(self.content).hexdigest()],directory=False)]
        self.uploader.run_once([self.root],upload=True)
        self.assertEqual(self.server.calls,[])

    def test_changed_local_payload_is_not_published(self):
        self.local.write_bytes(b'x'*len(self.content))
        self.assertEqual(self.uploader.jobs([self.root]),[])

    def test_personal_archive_is_never_selected(self):
        d.write_json(self.state/'catalog.json',{'root':True})
        self.assertEqual(self.uploader.jobs([self.root]),[])

    def test_partial_is_labelled_and_never_treated_as_original(self):
        (self.state/'events.jsonl').write_text('')
        d.write_json(self.state/'partial-copies.json',[dict(self.item,source=self.data['source'],
            recovered_path='Course/Lecture.mp4',local_size=len(self.content),downloaded_bytes=15,mtime_ns=self.local.stat().st_mtime_ns)])
        self.uploader.run_once([self.root],upload=True)
        self.assertTrue(any('(partially available 68.18%)' in p for p in self.server.files))

    def test_remote_original_suppresses_partial(self):
        (self.state/'events.jsonl').write_text('')
        d.write_json(self.state/'partial-copies.json',[dict(self.item,source=self.data['source'],
            recovered_path='Course/Lecture.mp4',local_size=len(self.content),downloaded_bytes=15,mtime_ns=self.local.stat().st_mtime_ns)])
        self.uploader.index=[dict(path='organized/Lecture.mp4',size=len(self.content),checksums=['SHA256:'+self.item['sha256']],directory=False)]
        self.uploader.run_once([self.root],upload=True)
        self.assertEqual(self.server.calls,[])

    def test_yandex_md5_suppresses_renamed_original_without_any_payload_read(self):
        (self.state/'events.jsonl').write_text('')
        d.write_json(self.state/'partial-copies.json',[dict(self.item,source=self.data['source'],
            recovered_path='Course/Lecture.mp4',local_size=len(self.content),downloaded_bytes=15,mtime_ns=self.local.stat().st_mtime_ns)])
        md5=hashlib.md5(self.content).hexdigest()
        self.uploader.index=[dict(path='someone-else/completely-renamed.mp4',size=len(self.content),checksums=['MD5:'+md5],directory=False)]
        with patch.object(d,'api',return_value=dict(self.item,md5=md5)) as api:
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
            self.assertEqual(api.call_count,1)
        self.assertEqual(self.server.calls,[])

    def test_cached_manifest_md5_needs_no_yandex_request_or_payload_read(self):
        md5=hashlib.md5(self.content).hexdigest()
        d.write_json(self.state/'01-manifest.json',[dict(self.item,md5=md5)])
        self.uploader.index=[dict(path='another/renamed.mp4',size=len(self.content),checksums=['MD5:'+md5],directory=False)]
        with patch.object(d,'api',side_effect=AssertionError('unnecessary metadata request')):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.server.calls,[])

    def test_camrip_uses_its_own_archive_when_source_is_shared_across_roots(self):
        (self.state/'events.jsonl').write_text('')
        record=dict(local=str(self.local),source=self.data['source'],size=len(self.content),
            mtime_ns=self.local.stat().st_mtime_ns,actual_sha256=self.item['sha256'],
            original_sha256=self.item['sha256'],original_size=len(self.content),
            match=dict(course_path='/Course',file='Lecture.mp4',sheet='2024 осень'))
        d.write_json(self.state/'recordings.json',[record])
        with tempfile.TemporaryDirectory() as other_dir:
            other=Path(other_dir)
            state=other/'.download'
            state.mkdir()
            d.write_json(state/'source.json',self.data)
            d.write_json(state/'01-manifest.json',[])
            jobs=self.uploader.jobs([self.root,other])
        self.assertEqual(len(jobs),1)
        self.assertEqual(jobs[0][1],self.root)
        self.assertEqual(jobs[0][5],'camrip')

    def test_many_camrip_candidates_index_archive_manifests_once(self):
        (self.state/'events.jsonl').write_text('')
        second=dict(self.item,path='/Course/Second.mp4',
                    sha256=hashlib.sha256(b'second video').hexdigest(),size=12)
        second_local=d.local_path(self.root,second['path'])
        second_local.write_bytes(b'second video')
        d.write_json(self.state/'01-manifest.json',[self.item,second])
        records=[]
        for item,local in [(self.item,self.local),(second,second_local)]:
            records.append(dict(local=str(local),source=self.data['source'],
                size=item['size'],mtime_ns=local.stat().st_mtime_ns,
                actual_sha256=item['sha256'],original_sha256=item['sha256'],
                original_size=item['size'],
                match=dict(course_path='/Course',file=Path(item['path']).name,sheet='2024 осень')))
        d.write_json(self.state/'recordings.json',records)
        with patch.object(self.uploader,'read',wraps=self.uploader.read) as read:
            jobs=self.uploader.jobs([self.root])
        self.assertEqual({j[3]['path'] for j in jobs},{self.item['path'],second['path']})
        self.assertEqual(sorted(j[9] for j in jobs),[1,2])
        reads=[call for call in read.call_args_list if call.args and call.args[0]==self.state/'01-manifest.json']
        self.assertLessEqual(len(reads),2)  # super().jobs and one shared CamRip index

    def test_camrip_is_used_only_without_local_original(self):
        record=dict(local=str(self.local),source=self.data['source'],size=len(self.content),mtime_ns=self.local.stat().st_mtime_ns,
            actual_sha256=self.item['sha256'],original_sha256=self.item['sha256'],original_size=len(self.content),
            match=dict(course_path='/Course',file='Lecture.mp4',sheet='2024 осень'))
        d.write_json(self.state/'recordings.json',[record])
        self.assertEqual(len(self.uploader.jobs([self.root])),1)
        (self.state/'events.jsonl').write_text('')
        jobs=self.uploader.jobs([self.root]);self.assertEqual(len(jobs),1);self.assertEqual(jobs[0][5],'camrip')

    def test_known_manifest_hash_suppresses_camrip_without_source_requests(self):
        (self.state/'events.jsonl').write_text('')
        self.local.write_bytes(b'x'*len(self.content))
        record=dict(local=str(self.local),source=self.data['source'],size=len(self.content),mtime_ns=self.local.stat().st_mtime_ns,
            actual_sha256=hashlib.sha256(self.local.read_bytes()).hexdigest(),original_sha256=self.item['sha256'],original_size=len(self.content),
            match=dict(course_path='/Course',file='Lecture.mp4',sheet='2024 осень'))
        d.write_json(self.state/'recordings.json',[record])
        for kind in ('sha1','md5'):
            with self.subTest(kind=kind):
                digest=getattr(hashlib,kind)(self.content).hexdigest()
                d.write_json(self.state/'01-manifest.json',[dict(self.item,**{kind:digest})])
                self.uploader.index=[dict(path='other/Lecture.mp4',size=len(self.content),checksums=[kind.upper()+':'+digest],directory=False)]
                with patch.object(d,'api',side_effect=AssertionError('Known hash needs no source request')):
                    self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
                self.assertEqual(self.server.calls,[])
                for changed in (dict(self.item,size=self.item['size']+1),dict(self.item,sha256='0'*64)):
                    d.write_json(self.state/'01-manifest.json',[dict(changed,**{kind:digest})])
                    self.assertNotIn(kind,self.job()[3])

    def test_foreign_session_location_is_rejected(self):
        with self.assertRaises(ValueError):
            oc.Client('https://cloud.example','Walterit','secret').safe_url('https://attacker.example/data/upload')

    def test_personal_or_ambiguous_space_is_rejected(self):
        c=oc.Client('https://cloud.example','Walterit','secret')
        with patch.object(c,'request',return_value=Response(200,body=json.dumps({'value':[{'name':'ysda-videos','driveType':'personal'}]}).encode())):
            with self.assertRaises(ValueError):c.discover()

    def test_completed_session_disappearing_after_lost_response_still_publishes(self):
        original=self.server.request
        def request(method,path='',**kwargs):
            if method=='HEAD' and len(self.server.payload)==self.server.size:
                raise urllib.error.HTTPError(path,404,'completed',{},None)
            if method=='PATCH' and int(kwargs['headers']['Upload-Offset'])==21:
                self.server.drop_patch=True
            return original(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=request):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(self.job())['state'],'uploaded')

    def test_repeated_session_loss_is_bounded_and_keeps_the_latest_session(self):
        self.server.timeout=0
        with patch.object(self.server,'request',side_effect=lambda method,path='',**kwargs:
                (_ for _ in ()).throw(urllib.error.HTTPError(path,410,'expired',{},None)) if method in ('HEAD','PATCH')
                else Server.request(self.server,method,path,**kwargs)):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        row=self.uploader.record(self.job())
        self.assertEqual(row['state'],'pending');self.assertIn('session_url',row)
        self.assertEqual(row['session_restarts'],1)
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),2)

    def test_lost_session_reuploads_only_after_a_successful_fresh_scan(self):
        job,old=self.seed_lost_session();request=self.server.request
        def lost(method,path='',**kwargs):
            if method=='HEAD' and path==old['session_url']:
                raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',return_value=[]) as scan:
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        row=self.uploader.record(job)
        self.assertEqual(row['state'],'uploaded');self.assertEqual(row['session_restarts'],1)
        self.assertEqual(row['last_lost_session']['confirmed_bytes'],7)
        self.assertEqual(scan.call_count,1)
        self.assertEqual(self.server.files[row['target']],self.content)
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)
        self.assertNotIn(old['session_url'],(self.state/'opencloud-uploads.jsonl').read_text())

    def test_completed_lost_session_finds_a_renamed_copy_in_the_refreshed_space(self):
        job,old=self.seed_lost_session(len(self.content));request=self.server.request
        other=dict(path='other-user/renamed.mp4',size=len(self.content),directory=False,
                   checksums=['SHA256:'+self.item['sha256']])
        def lost(method,path='',**kwargs):
            if method=='HEAD':raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',return_value=[other]) as scan:
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(job)['state'],'existing')
        self.assertEqual(scan.call_count,1)
        self.assertFalse(any(m in ('POST','PATCH','GET') for m,p,h in self.server.calls))

    def test_complete_lost_session_without_any_copy_restarts_upload(self):
        job,old=self.seed_lost_session(len(self.content));request=self.server.request
        def lost(method,path='',**kwargs):
            if method=='HEAD' and path==old['session_url']:
                raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',return_value=[]):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(job)['state'],'uploaded')
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)

    def test_catalog_failure_preserves_lost_session_and_confirmed_position(self):
        job,old=self.seed_lost_session();request=self.server.request
        def lost(method,path='',**kwargs):
            if method=='HEAD':raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',side_effect=TimeoutError('catalog offline')):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        row=self.uploader.record(job)
        self.assertEqual((row['session_url'],row['sent_bytes']),(old['session_url'],old['sent_bytes']))
        self.assertNotIn('session_restarts',row)
        self.assertFalse(any(m in ('POST','PATCH') for m,p,h in self.server.calls))

    def test_late_stage_publication_avoids_reupload_after_catalog_refresh(self):
        job,old=self.seed_lost_session();request=self.server.request
        stage=str(Path(old['target']).parent/('.yds-'+self.item['sha256']+'.upload'))
        def scan():
            self.server.files[stage]=self.content
            return []
        def lost(method,path='',**kwargs):
            if method=='HEAD':raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',side_effect=scan):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(job)['state'],'uploaded')
        self.assertFalse(any(m in ('POST','PATCH') for m,p,h in self.server.calls))

    def test_session_that_reappears_during_scan_keeps_accepted_blocks(self):
        job,old=self.seed_lost_session();request=self.server.request;heads=[]
        self.server.payload=bytearray(self.content[:7])
        self.server.stage=str(Path(old['target']).parent/('.yds-'+self.item['sha256']+'.upload'))
        def lost(method,path='',**kwargs):
            if method=='HEAD':
                heads.append(path)
                if len(heads)==1:raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',return_value=[]):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(job)['state'],'uploaded')
        self.assertFalse(any(m=='POST' for m,p,h in self.server.calls))
        self.assertEqual([int(h['Upload-Offset']) for m,p,h in self.server.calls if m=='PATCH'],[7,14,21])

    def test_patch_404_uses_the_same_lost_session_recovery(self):
        job,old=self.seed_lost_session();request=self.server.request;lost=[False]
        self.server.payload=bytearray(self.content[:7])
        def respond(method,path='',**kwargs):
            if path==old['session_url'] and (method=='PATCH' or method=='HEAD' and lost[0]):
                lost[0]=True
                raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=respond),patch.object(self.uploader,'scan',return_value=[]):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(job)['session_restarts'],1)
        self.assertEqual(self.uploader.record(job)['state'],'uploaded')

    def test_lost_session_checks_other_metadata_hashes_and_unhashed_names(self):
        request=self.server.request
        copies=[dict(path='other/renamed.mp4',checksums=[kind+':'+digest]) for kind,digest in (
            ('SHA1',hashlib.sha1(self.content).hexdigest()),('MD5',hashlib.md5(self.content).hexdigest()))]
        copies.append(dict(path='other/999 - Lecture.mp4',checksums=[]))
        for copy in copies:
            with self.subTest(copy=copy):
                job,old=self.seed_lost_session()
                def lost(method,path='',**kwargs):
                    if method=='HEAD':raise urllib.error.HTTPError(path,404,'gone',{},None)
                    return request(method,path,**kwargs)
                row=dict(copy,size=len(self.content),directory=False)
                with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',return_value=[row]):
                    self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
                self.assertEqual(self.uploader.record(job)['state'],'existing')
        self.assertFalse(any(m in ('POST','PATCH','GET') for m,p,h in self.server.calls))

    def test_interrupted_catalog_refresh_preserves_lost_session(self):
        job,old=self.seed_lost_session();request=self.server.request
        def lost(method,path='',**kwargs):
            if method=='HEAD':raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',side_effect=InterruptedError('stop')):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        row=self.uploader.record(job)
        self.assertEqual((row['session_url'],row['sent_bytes']),(old['session_url'],7))
        self.assertNotIn('session_restarts',row)
        self.assertFalse(any(m=='POST' for m,p,h in self.server.calls))

    def test_completed_stage_after_restart_checkpoint_is_published_without_new_session(self):
        job,row=self.seed_lost_session(0)
        row.pop('session_url');row.update(state='session_lost',session_restarts=1)
        self.uploader.save_record(job,row)
        stage=str(Path(row['target']).parent/('.yds-'+self.item['sha256']+'.upload'))
        self.server.files[stage]=self.content
        self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(job)['state'],'uploaded')
        self.assertFalse(any(m in ('POST','PATCH') for m,p,h in self.server.calls))

    def test_catalog_refresh_is_shared_and_keeps_concurrent_publications(self):
        self.uploader.index=[];observed=self.uploader.index
        started=Event();release=Event()
        published=dict(path='our/just-published.mp4',directory=False,size=1,checksums=['SHA256:'+'a'*64])
        def scan():
            started.set()
            if not release.wait(3):raise AssertionError('refresh was not released')
            with self.uploader.index_lock:self.uploader.index.append(published)
            return []
        with patch.object(self.uploader,'scan',side_effect=scan) as scanned,ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(self.uploader.refresh_index,observed)
            self.assertTrue(started.wait(3))
            second=pool.submit(self.uploader.refresh_index,observed)
            release.set();first.result(timeout=3);second.result(timeout=3)
        self.assertEqual(scanned.call_count,1)
        self.assertEqual(self.uploader.index,[published])
        self.assertIsNot(self.uploader.index,observed)

    def test_session_restart_counts_only_new_transferred_bytes(self):
        job,old=self.seed_lost_session();request=self.server.request
        def lost(method,path='',**kwargs):
            if method=='HEAD' and path==old['session_url']:
                raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with oc.nextcloud.SharedUploadBudget(self.root/'budget','opencloud',2,8) as budget:
            self.uploader.upload_budget=budget
            with patch.object(self.server,'request',side_effect=lost),patch.object(self.uploader,'scan',return_value=[]):
                self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
            state=json.loads((self.root/'budget/state.json').read_text())
            self.assertEqual(state['controller']['total'],len(self.content))

    def test_disappearing_session_waits_for_delayed_publication_before_restart(self):
        job,row=self.seed_lost_session(len(self.content));request=self.server.request;listing=self.server.listing
        self.server.timeout=60;lookups=[]
        parent=str(Path(row['target']).parent);stage=str(Path(parent)/('.yds-'+self.item['sha256']+'.upload'))
        def delayed(folder):
            if folder==parent:
                lookups.append(folder)
                if len(lookups)==3:self.server.files[stage]=self.content
            return listing(folder)
        def lost(method,path='',**kwargs):
            if method=='HEAD':raise urllib.error.HTTPError(path,404,'gone',{},None)
            return request(method,path,**kwargs)
        with patch.object(self.server,'request',side_effect=lost),patch.object(self.server,'listing',side_effect=delayed), \
                patch.object(self.uploader,'scan',return_value=[]),patch.object(self.uploader.stopping,'wait',return_value=False):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(job)['state'],'uploaded')
        self.assertFalse(any(m in ('POST','PATCH') for m,p,h in self.server.calls))

    def test_fresh_original_suppresses_reupload_of_a_lost_partial_or_camrip(self):
        for variant in ('partial','camrip'):
            with self.subTest(variant=variant):
                job,row=self.seed_lost_session()
                job=(*job[:5],variant,*job[6:])
                row.update(variant=variant,payload_sha256='f'*64,payload_sha1='a'*40,payload_md5='b'*32)
                original=dict(path='organized/renamed.mp4',directory=False,size=len(self.content),
                              checksums=['SHA256:'+self.item['sha256']])
                with patch.object(self.uploader,'scan',return_value=[original]), \
                        patch.object(self.uploader,'session_offset',side_effect=urllib.error.HTTPError('unused',404,'gone',{},None)):
                    result=self.uploader.recover_session(job,row,'unpublished.upload',row['payload_sha1'],job[10],True)
                self.assertIsNone(result)
                self.assertEqual(self.uploader.record(job)['state'],'existing')
        self.assertFalse(any(m=='POST' for m,p,h in self.server.calls))

    def test_failed_original_metadata_preserves_lost_partial_session(self):
        failures=[urllib.error.HTTPError('unused',401,'denied',{},None),ValueError('bad JSON'),
                  dict(size=1,sha256='0'*64,md5='c'*32),dict(self.item,md5=None)]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                job,row=self.seed_lost_session();job=(*job[:5],'partial',*job[6:])
                row.update(variant='partial',payload_sha256='f'*64,payload_sha1='a'*40,payload_md5='b'*32)
                self.uploader.save_record(job,row)
                original=dict(path='organized/original.mp4',directory=False,size=self.item['size'],checksums=['MD5:'+'c'*32])
                with patch.object(self.uploader,'scan',return_value=[original]), \
                        patch.object(d,'api',side_effect=failure if isinstance(failure,Exception) else None,
                                     return_value=failure):
                    with self.assertRaises((OSError,ValueError)):
                        self.uploader.recover_session(job,row,'unpublished.upload',row['payload_sha1'],job[10],True)
                saved=self.uploader.record(job)
                self.assertEqual(saved['session_url'],'https://cloud.example/data/lost')
                self.assertEqual(saved['sent_bytes'],7)
        self.assertFalse(any(m=='POST' for m,p,h in self.server.calls))

    def test_opencloud_listing_rejects_unreadable_subtrees_but_allows_optional_missing_hash(self):
        client=oc.Client('https://cloud.example','Walterit','secret');client.base=self.server.base
        for props,accepted in [('<d:propstat><d:prop/><d:status>HTTP/1.1 403 Forbidden</d:status></d:propstat>',False),
                ('<d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>'
                 '<d:propstat><d:prop><o:checksums/></d:prop><d:status>HTTP/1.1 404 Not Found</d:status></d:propstat>',True)]:
            body=('<d:multistatus xmlns:d="DAV:" xmlns:o="http://owncloud.org/ns"><d:response>'
                  '<d:href>/dav/spaces/project/subtree/</d:href>'+props+'</d:response></d:multistatus>').encode()
            with self.subTest(accepted=accepted),patch.object(client,'request',return_value=Response(207,body=body)):
                if accepted:self.assertTrue(client.listing('')[0]['directory'])
                else:
                    with self.assertRaises(ValueError):client.listing('')

    def test_stop_or_local_change_during_final_head_preserves_old_session(self):
        job=self.job()
        for action in ('stop','change','missing'):
            with self.subTest(action=action):
                self.uploader.stopping.clear();self.local.write_bytes(self.content)
                with patch.object(self.uploader,'jobs',return_value=[job]):job,row=self.seed_lost_session()
                def lost(current):
                    if action=='stop':self.uploader.stopping.set()
                    elif action=='change':self.local.write_bytes(b'changed video')
                    else:self.local.unlink()
                    raise urllib.error.HTTPError('unused',404,'gone',{},None)
                with patch.object(self.uploader,'scan',return_value=[]),patch.object(self.uploader,'session_offset',side_effect=lost):
                    with self.assertRaises((OSError,RuntimeError)):
                        self.uploader.recover_session(job,row,'stage',row['payload_sha1'],job[10],True)
                saved=self.uploader.record(job)
                self.assertEqual(saved['session_url'],'https://cloud.example/data/lost')
                self.assertEqual(saved['sent_bytes'],7)
                self.assertNotIn('session_restarts',saved)
        self.assertFalse(any(m=='POST' for m,p,h in self.server.calls))

    def test_opencloud_listing_rejects_denied_root_without_slash_and_empty_multistatus(self):
        client=oc.Client('https://cloud.example','Walterit','secret');client.base=self.server.base
        for rows in ('<d:response><d:href>/dav/spaces/project</d:href>'
                     '<d:status>HTTP/1.1 403 Forbidden</d:status></d:response>',''):
            body=('<d:multistatus xmlns:d="DAV:">'+rows+'</d:multistatus>').encode()
            with self.subTest(rows=rows),patch.object(client,'request',return_value=Response(207,body=body)):
                with self.assertRaises(ValueError):client.listing('')

    def test_staggered_publication_waits_share_the_same_catalog_refresh(self):
        job,row=self.seed_lost_session();entered=Event();refreshed=Event()
        def publication(current,*args,**kwargs):
            if kwargs.get('wait') is False:return None
            if current['worker']=='second':
                entered.set()
                if not refreshed.wait(3):raise AssertionError('first refresh missing')
            else:
                if not entered.wait(3):raise AssertionError('second recovery missing')
            raise oc.PublicationPending('not published')
        refresh=self.uploader.refresh_index
        def refresh_index(observed):
            refresh(observed);refreshed.set()
        with patch.object(self.uploader,'confirm_publication',side_effect=publication), \
                patch.object(self.uploader,'scan',return_value=[]) as scanned, \
                patch.object(self.uploader,'refresh_index',side_effect=refresh_index), \
                patch.object(self.uploader,'session_offset',return_value=7),ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(self.uploader.recover_session,job,dict(row,worker='first'),'stage',row['payload_sha1'],job[10],True)
            second=pool.submit(self.uploader.recover_session,job,dict(row,worker='second'),'stage',row['payload_sha1'],job[10],True)
            self.assertEqual(first.result(timeout=3),7);self.assertEqual(second.result(timeout=3),7)
        self.assertEqual(scanned.call_count,1)

    def test_lost_move_response_without_remote_checksums_is_reconciled(self):
        original=self.server.request
        def request(method,path='',**kwargs):
            response=original(method,path,**kwargs)
            if method=='MOVE':raise urllib.error.URLError('lost MOVE response')
            return response
        listing=self.server.listing
        def without_checksums(folder):
            return [dict(row,checksums=[]) for row in listing(folder)]
        with patch.object(self.server,'request',side_effect=request),patch.object(self.server,'listing',side_effect=without_checksums):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        restarted=oc.Uploader(self.server,Event(),chunk_size=7)
        with patch.object(self.server,'listing',side_effect=without_checksums):
            self.assertEqual(restarted.run_once([self.root],upload=True),0)
        self.assertEqual(restarted.record(self.job())['state'],'uploaded')
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)

    def test_lost_move_is_terminal_when_whole_space_index_finds_final_file(self):
        original=self.server.request
        def request(method,path='',**kwargs):
            response=original(method,path,**kwargs)
            if method=='MOVE':raise urllib.error.URLError('lost MOVE response')
            return response
        with patch.object(self.server,'request',side_effect=request):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        restarted=oc.Uploader(self.server,Event(),chunk_size=7)
        restarted.index=[dict(path=path,size=len(value),directory=False,checksums=[]) for path,value in self.server.files.items()]
        self.assertEqual(restarted.run_once([self.root],upload=True),0)
        self.assertEqual(restarted.record(self.job())['state'],'existing')
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)

    def test_changed_yandex_identity_cannot_supply_wrong_md5(self):
        md5=hashlib.md5(self.content).hexdigest()
        self.uploader.index=[dict(path='other/renamed.mp4',size=len(self.content),checksums=['MD5:'+md5],directory=False)]
        with patch.object(d,'api',return_value=dict(self.item,sha256='0'*64,md5=md5)):
            self.assertFalse(self.uploader.original_exists(self.item,self.root,self.data['source']))
        self.assertFalse((self.state/'opencloud-source-hashes').exists())

    def test_conflicting_published_checksum_fails_without_payload_download(self):
        listing=self.server.listing
        with patch.object(self.server,'listing',side_effect=lambda folder:
                [dict(x,checksums=['SHA256:'+'0'*64]) for x in listing(folder)]):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        self.assertFalse(any(m in ('GET','MOVE') for m,p,h in self.server.calls))
        self.assertEqual(self.uploader.record(self.job())['state'],'pending')

    def test_md5_only_publication_is_confirmed_without_payload_download(self):
        listing=self.server.listing
        with patch.object(self.server,'listing',side_effect=lambda folder:
                [dict(x,checksums=['MD5:'+hashlib.md5(self.content).hexdigest()]) for x in listing(folder)]):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertFalse(any(m=='GET' for m,p,h in self.server.calls))

    def test_original_in_another_selected_root_suppresses_local_partial(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        partial=Path(tmp.name);state=partial/'.download';state.mkdir()
        local=d.local_path(partial,self.item['path']);local.parent.mkdir();local.write_bytes(self.content)
        d.write_json(state/'source.json',self.data)
        d.write_json(state/'01-manifest.json',[self.item])
        d.write_json(state/'partial-copies.json',[dict(self.item,source=self.data['source'],
            recovered_path='Course/Lecture.mp4',local_size=len(self.content),downloaded_bytes=15,mtime_ns=local.stat().st_mtime_ns)])
        jobs=self.uploader.jobs([partial,self.root])
        self.assertEqual([j[5] for j in jobs],['original'])

    def test_scan_retries_transient_read_without_restarting_completed_folders(self):
        import ssl
        calls=[]
        def listing(folder):
            calls.append(folder)
            if not folder:return [dict(path='Course',directory=True)]
            if calls.count(folder)==1:
                raise urllib.error.URLError(ssl.SSLEOFError(8,'unexpected TLS EOF'))
            return [dict(path='Course/video.mp4',directory=False,size=1)]
        with patch.object(self.server,'listing',side_effect=listing):
            rows=self.uploader.scan()
        self.assertEqual(calls,['','Course','Course'])
        self.assertEqual([r['path'] for r in rows],['Course/video.mp4'])

    def test_postprocessing_visibility_delay_does_not_repeat_upload(self):
        original=self.server.listing;calls=[]
        def listing(folder):
            calls.append(folder)
            if len(self.server.payload)==self.server.size and len(calls)<3:return []
            return original(folder)
        self.uploader.index=[]
        with patch.object(self.server,'listing',side_effect=listing),patch.object(self.uploader.stopping,'wait',return_value=False):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),0)
        self.assertEqual(self.uploader.record(self.job())['state'],'uploaded')
        self.assertEqual(sum(m=='POST' for m,p,h in self.server.calls),1)

    def test_request_redirect_does_not_expose_credentials(self):
        client=oc.Client('https://cloud.example','Walterit','secret')
        self.assertEqual(client.safe_url('/data/session'),'https://cloud.example/data/session')
        for value in ['http://cloud.example/data/session','https://cloud.example@attacker.example/data','https://attacker.example/data']:
            with self.assertRaises(ValueError):client.safe_url(value)


if __name__=='__main__':unittest.main()
