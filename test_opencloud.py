"""OpenCloud only publishes confirmed uploads and never overwrites existing videos."""
import hashlib
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

    def test_expired_session_without_published_file_is_preserved(self):
        self.server.timeout=0
        with patch.object(self.server,'request',side_effect=lambda method,path='',**kwargs:
                (_ for _ in ()).throw(urllib.error.HTTPError(path,410,'expired',{},None)) if method=='HEAD'
                else Server.request(self.server,method,path,**kwargs)):
            self.assertEqual(self.uploader.run_once([self.root],upload=True),1)
        row=self.uploader.record(self.job())
        self.assertEqual(row['state'],'pending');self.assertIn('session_url',row)

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
