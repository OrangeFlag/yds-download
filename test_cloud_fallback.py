"""Metadata-only discovery and SHA-verified recovery across cloud sources."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import io
import json
from pathlib import Path
import tempfile
from threading import Event
import unittest
from unittest.mock import patch

import download as d
import nextcloud
from nextcloud_fallback import Fallback


class Response(io.BytesIO):
    def __init__(self, body, status=200, headers=None):
        super().__init__(body)
        self.status=status
        self.headers={'ETag':'"stable"','Content-Length':str(len(body)),**(headers or {})}


class Server:
    timeout=60
    nickname="Reader"
    def __init__(self, rows, bodies):
        self.rows,self.bodies=rows,bodies
        self.discoveries=0;self.listings=[];self.gets=[]
    def discover(self):self.discoveries+=1
    def listing(self, folder):
        self.listings.append(folder)
        return [row for row in self.rows if str(Path(row['path']).parent).replace('.', '')==folder]
    def request(self, method, path='', headers=None):
        if method!='GET':raise AssertionError(method)
        self.gets.append((path,headers or {}))
        body=self.bodies[path]
        if headers and 'Range' in headers:
            offset=int(headers['Range'][6:-1])
            return Response(body[offset:],206,{'Content-Range':f'bytes {offset}-{len(body)-1}/{len(body)}'})
        return Response(body)


class CloudChecks(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name);self.state=self.root/'.download';self.state.mkdir()
        self.data=dict(source='https://disk.yandex.ru/d/test',title='ШАД осень 2023')
        d.write_json(self.state/'source.json',self.data)
        self.body=b'original video'
        self.item=dict(path='/Course/Lecture.mp4',size=len(self.body),sha256=hashlib.sha256(self.body).hexdigest(),file='https://example.invalid/video')
        d.write_json(self.state/'01-manifest.json',[self.item])
        self.event=dict(self.item,course='Course',attempt=2,status='missing')
        self.secret=self.root/'token';self.secret.write_text('app token\n')
        self.config=dict(server='https://cloud.example',username='Reader',password_file=str(self.secret))
        self.path='ingest/Other/2023-fall/Course/001 - Lecture.mp4'
        self.rows=[dict(path='ingest',directory=True),dict(path='ingest/Other',directory=True),dict(path='ingest/Other/2023-fall',directory=True),dict(path='ingest/Other/2023-fall/Course',directory=True),dict(path=self.path,directory=False,size=len(self.body),checksums=[])]
        self.server=Server(self.rows,{self.path:self.body})

    def cloud(self, link=None, budget=None):
        import cloud_fallback
        with patch.object(cloud_fallback.oc,'Client',return_value=self.server):
            return cloud_fallback.CloudFallback(self.root,link,Event(),self.config,budget)

    def test_injected_index_and_budget_count_only_new_resume_bytes(self):
        class Budget:
            active=False
            total=0
            @contextmanager
            def slot(this, stopping):
                this.active=True
                try:yield
                finally:this.active=False
            def confirmed(this, size):
                self.assertTrue(this.active);this.total+=size
            def update(this):
                self.assertTrue(this.active)
        class Interrupted(Response):
            def read(this, size=-1):
                if this.tell():raise TimeoutError('interrupted')
                return super().read(4)
        budget=Budget()
        index=nextcloud.Index(self.root,'https://drive.example/s/test',stopping=Event())
        candidate=dict(self.rows[-1],path='2023-fall/Course/001 - Lecture.mp4')
        fallback=Fallback(self.root,index.link,Event(),index=index,budget=budget,label='OPENCLOUD')
        with patch.object(index.share,'listing',return_value=[candidate]),patch.object(index.share,'request',side_effect=[Interrupted(self.body),Response(self.body[4:],206,{'Content-Range':f'bytes 4-{len(self.body)-1}/{len(self.body)}'})]):
            self.assertIsNone(fallback.recover(self.item,self.event))
            self.assertEqual(fallback.recover(self.item,self.event)['status'],'valid')
        self.assertEqual(budget.total,len(self.body))
        self.assertFalse(budget.active)
        self.assertIn('opencloud-downloads',str(fallback.directory))

    def test_opencloud_recovers_all_users_and_caches_metadata_without_exposing_token(self):
        cloud=self.cloud()
        self.assertEqual(cloud.recover_all(self.item,self.event)['status'],'valid')
        self.assertEqual(d.target(self.root,self.item,'Course').read_bytes(),self.body)
        self.assertFalse(cloud.ready(self.item))
        self.assertIsNone(cloud.recover_all(self.item,self.event))
        self.assertEqual(len(self.server.gets),1)
        self.assertEqual(self.server.discoveries,1)
        state=''.join(path.read_text() for path in self.state.rglob('*.json'))
        self.assertNotIn('app token',state+cloud.link)

    def test_hash_matches_renamed_file_but_conflict_cannot_fall_back_to_name(self):
        cloud=self.cloud();index=cloud.sources[0].index
        self.rows[-1]['checksums']=['SHA256:'+'0'*64,'MD5:'+'a'*32]
        item=dict(self.item,md5='a'*32)
        self.assertEqual(index.candidates(self.data,item),[])
        self.rows[-1]['checksums']=['SHA256:'+self.item['sha256']]
        self.rows[-1]['path']='ingest/Other/2023-fall/Course/renamed.mp4'
        self.assertEqual(index.candidates(self.data,self.item),[self.rows[-1]])
        self.assertEqual(self.server.gets,[])

    def test_md5_identity_matches_independently_of_filename(self):
        self.rows[-1].update(path='ingest/Other/2023-fall/Course/renamed.mp4',checksums=['MD5:'+hashlib.md5(self.body).hexdigest()])
        cloud=self.cloud()
        item=dict(self.item,md5=hashlib.md5(self.body).hexdigest())
        self.assertEqual(cloud.sources[0].index.candidates(self.data,item),[self.rows[-1]])

    def test_wrong_course_semester_partial_and_personal_file_never_get_a_body(self):
        cloud=self.cloud();index=cloud.sources[0].index
        self.rows.extend(dict(self.rows[-1],path=path) for path in [
            'ingest/Other/2024-fall/Course/001 - Lecture.mp4',
            'ingest/Other/2023-fall/Personal/001 - Lecture.mp4',
            'ingest/Other/2023-fall/Course/001 - Lecture (partially available).mp4'])
        self.rows.remove(self.rows[4])
        self.assertEqual(index.candidates(self.data,self.item),[])
        self.assertEqual(self.server.gets,[])

    def test_unhashed_opencloud_candidate_preserves_trailing_course_space(self):
        self.item['path']='/Course /Lecture.mp4'
        self.rows[-2]['path']='ingest/Other/2023-fall/Course '
        self.rows[-1]['path']='ingest/Other/2023-fall/Course /001 - Lecture.mp4'
        index=self.cloud().sources[0].index
        self.assertEqual(index.candidates(self.data,self.item),[self.rows[-1]])
        self.rows[-2]['path']='ingest/Other/2023-fall/Course'
        self.rows[-1]['path']='ingest/Other/2023-fall/Course/001 - Lecture.mp4'
        index.catalog['expires']=0
        self.assertEqual(index.candidates(self.data,self.item),[])

    def test_metadata_failure_is_cached_and_does_not_publish_partial_catalog(self):
        cloud=self.cloud();index=cloud.sources[0].index
        with patch.object(self.server,'listing',side_effect=TimeoutError('offline')) as listing:
            for _ in range(2):
                with self.assertRaises(TimeoutError):index.candidates(self.data,self.item)
            self.assertEqual(listing.call_count,3)
        self.assertEqual(self.server.gets,[])

    def test_existing_valid_original_skips_both_remote_sources(self):
        target=d.target(self.root,self.item,'Course');target.parent.mkdir();target.write_bytes(self.body)
        cloud=self.cloud('https://drive.example/s/test')
        with patch.object(nextcloud.Share,'listing',side_effect=AssertionError('unnecessary remote lookup')):
            self.assertEqual(cloud.recover_all(self.item,self.event)['status'],'valid')
        self.assertEqual(self.server.gets,[])
        self.assertEqual(self.server.discoveries,0)

    def test_nextcloud_mismatch_is_retained_before_opencloud_verified_original(self):
        cloud=self.cloud('https://drive.example/s/test')
        wrong=b'other video'
        row=dict(path='2023-fall/Course/001 - Lecture.mp4',directory=False,size=len(wrong),checksums=[])
        with patch.object(nextcloud.Share,'listing',return_value=[row]),patch.object(nextcloud.Share,'request',return_value=Response(wrong)):
            self.assertEqual(cloud.recover_all(self.item,self.event)['status'],'valid')
        self.assertEqual(d.target(self.root,self.item,'Course').read_bytes(),self.body)
        self.assertEqual(next((self.root/'Course').glob('*SHA mismatch*')).read_bytes(),wrong)

    def test_matching_sha_without_semester_metadata_is_still_recoverable(self):
        self.data['title']='ШАД archive'
        d.write_json(self.state/'source.json',self.data)
        self.rows[-1]['checksums']=['SHA256:'+self.item['sha256']]
        self.assertEqual(self.cloud().recover_all(self.item,self.event)['status'],'valid')

    def test_retained_copy_hash_skips_duplicate_payload_with_no_local_source_ledger(self):
        index=nextcloud.Index(self.root,'https://drive.example/s/test',stopping=Event())
        row=dict(self.rows[-1],path='2023-fall/Course/001 - Lecture.mp4',checksums=['SHA256:'+self.item['sha256']])
        fallback=Fallback(self.root,index.link,Event(),index=index)
        with patch.object(index.share,'listing',return_value=[row]),patch.object(index.share,'request',side_effect=AssertionError('duplicate downloaded')):
            self.assertIsNone(fallback.recover_all(self.item,self.event,retained_hashes={self.item['sha256']}))
            self.assertFalse(fallback.ready(self.item))

    def test_multiple_archives_share_metadata_scan_and_listing_error_cooldown(self):
        first,second=self.cloud(),self.cloud()
        self.assertEqual(first.sources[0].index.candidates(self.data,self.item),[self.rows[-1]])
        count=len(self.server.listings)
        self.assertEqual(second.sources[0].index.candidates(self.data,self.item),[self.rows[-1]])
        self.assertEqual(len(self.server.listings),count)
        self.assertEqual(self.server.discoveries,1)

    def test_old_task_uses_manifest_md5_to_recover_renamed_original(self):
        md5=hashlib.md5(self.body).hexdigest()
        d.write_json(self.state/'01-manifest.json',[dict(self.item,md5=md5)])
        renamed='ingest/Other/2023-fall/Course/renamed.mp4'
        self.rows[-1].update(path=renamed,checksums=['MD5:'+md5])
        self.server.bodies[renamed]=self.body
        self.assertEqual(self.cloud().recover_all(self.item,self.event)['status'],'valid')
        self.assertEqual(self.server.gets[0][0],renamed)

    def test_valid_event_proof_skips_body_hash(self):
        target=d.target(self.root,self.item,'Course');target.parent.mkdir();target.write_bytes(self.body)
        d.record(self.state,self.item,'Course',2,'valid','SHA-256 verified',local=target,verified_stat=Fallback.stamp(target))
        with patch.object(d,'sha256_file',side_effect=AssertionError('verified body rehashed')):
            self.assertFalse(self.cloud().ready(self.item))

    def test_valid_registry_proof_skips_body_hash(self):
        target=self.root/'existing.mp4';target.write_bytes(self.body)
        proof=dict(self.item,source=self.data['source'],status='valid',local=str(target),mtime_ns=target.stat().st_mtime_ns)
        d.write_json(self.state/'local-copies.json',[proof])
        with patch.object(d,'sha256_file',side_effect=AssertionError('verified body rehashed')):
            self.assertFalse(self.cloud().ready(self.item))

    def test_event_proofs_are_compact_and_preserve_separate_local_copies(self):
        target=d.target(self.root,self.item,'Course');target.parent.mkdir();target.write_bytes(self.body)
        proof=dict(self.item,source=self.data['source'],status='valid',local=str(target),
                   local_stat=Fallback.stamp(target),actual_sha256=self.item['sha256'],
                   range_attempts=[dict(unneeded='x'*100000)])
        d.append_events(self.state/'events.jsonl',[proof,dict(proof,local=str(self.root/'other.mp4'),status='invalid')])
        cloud=self.cloud()
        with patch.object(d,'sha256_file',side_effect=AssertionError('valid proof was lost')):
            self.assertFalse(cloud.ready(self.item))
        cached=cloud.event_index.refresh()
        self.assertEqual(len(cached),2)
        self.assertTrue(all('range_attempts' not in row and 'file' not in row for row in cached.values()))
        self.assertLess(len(json.dumps(list(cached.values()))),3000)

    def test_event_proof_refresh_reads_only_new_rows_and_invalidates_changed_proof(self):
        target=d.target(self.root,self.item,'Course');target.parent.mkdir();target.write_bytes(self.body)
        proof=dict(self.item,source=self.data['source'],status='valid',local=str(target),local_stat=Fallback.stamp(target))
        journal=self.state/'events.jsonl';d.append_events(journal,[proof]);cloud=self.cloud()
        with patch.object(d,'sha256_file',side_effect=AssertionError('unnecessary hash')):
            self.assertFalse(cloud.ready(self.item))
            offset=journal.stat().st_size
            d.append_events(journal,[dict(proof,status='invalid')])
            target.write_bytes(b'x'*len(self.body))
            with patch.object(d,'scan_events',wraps=d.scan_events) as scan,patch.object(d,'sha256_file',return_value='0'*64) as sha:
                self.assertTrue(cloud.ready(self.item))
            self.assertEqual(scan.call_args.kwargs['start'],offset)
            sha.assert_called_once()

    def test_valid_cloud_receipt_skips_body_hash(self):
        target=d.target(self.root,self.item,'Course');target.parent.mkdir();target.write_bytes(self.body)
        cloud=self.cloud();cloud.sources[0].directory.mkdir()
        cloud.sources[0].save(self.item,dict(self.item,source=self.data['source'],state='verified',local=str(target),local_stat=Fallback.stamp(target),actual_sha256=self.item['sha256']))
        with patch.object(d,'sha256_file',side_effect=AssertionError('verified body rehashed')):
            self.assertFalse(self.cloud().ready(self.item))

    def test_wrong_registry_identity_or_stat_requires_sha_check(self):
        target=self.root/'existing.mp4';target.write_bytes(b'corrupted data')
        proof=dict(self.item,source=self.data['source'],status='valid',local=str(target),mtime_ns=target.stat().st_mtime_ns)
        for changes in [dict(sha256='0'*64),dict(source='https://disk.yandex.ru/d/other'),dict(mtime_ns=0),dict(local_stat=[0,len(self.body),target.stat().st_mtime_ns])]:
            with self.subTest(changes=changes):
                d.write_json(self.state/'local-copies.json',[dict(proof,**changes)])
                self.assertTrue(self.cloud().ready(self.item))

    def test_md5_only_duplicate_wrong_payload_is_not_downloaded_twice(self):
        cloud=self.cloud('https://drive.example/s/test')
        wrong=b'wrong original'
        self.rows[-1]['checksums']=['MD5:'+hashlib.md5(wrong).hexdigest()]
        self.server.bodies[self.path]=wrong
        nc_row=dict(self.rows[-1],path='2023-fall/Course/001 - Lecture.mp4')
        with patch.object(nextcloud.Share,'listing',return_value=[nc_row]),patch.object(nextcloud.Share,'request',return_value=Response(wrong)) as get:
            self.assertEqual(cloud.recover_all(self.item,self.event)['status'],'mismatch')
        self.assertEqual(get.call_count+len(self.server.gets),1)
        retained=list((self.root/'Course').glob('*SHA mismatch*'))
        self.assertEqual(len(retained),1)
        self.assertEqual(retained[0].read_bytes(),wrong)
        receipt=next(iter(cloud.sources[0].read(self.item)['retained_copies'].values()))
        self.assertEqual(receipt['actual_hashes'],dict(sha256=hashlib.sha256(wrong).hexdigest(),sha1=hashlib.sha1(wrong).hexdigest(),md5=hashlib.md5(wrong).hexdigest()))

    def test_advertised_wrong_body_md5_does_not_suppress_good_original(self):
        cloud=self.cloud('https://drive.example/s/test')
        wrong=b'wrong original'
        original_md5=hashlib.md5(self.body).hexdigest()
        self.rows[-1]['checksums']=['MD5:'+original_md5]
        nc_row=dict(self.rows[-1],path='2023-fall/Course/001 - Lecture.mp4')
        with patch.object(nextcloud.Share,'listing',return_value=[nc_row]),patch.object(nextcloud.Share,'request',return_value=Response(wrong)):
            self.assertEqual(cloud.recover_all(self.item,self.event)['status'],'valid')
        self.assertEqual(len(self.server.gets),1)
        self.assertEqual(d.target(self.root,self.item,'Course').read_bytes(),self.body)

    def test_concurrent_items_do_not_share_retained_payload_hashes(self):
        body_b=b'wrong original'
        item_b=dict(self.item,path='/Course/LectureB.mp4',sha256=hashlib.sha256(body_b).hexdigest())
        event_b=dict(self.event,**item_b)
        d.write_json(self.state/'01-manifest.json',[self.item,item_b])
        cloud=self.cloud();fallback=cloud.sources[0]
        retained=fallback.mismatch_path(self.item,'cloud.example',item_b['sha256'])
        retained.parent.mkdir();retained.write_bytes(body_b)
        fallback.directory.mkdir()
        fallback.save(self.item,dict(self.item,source=self.data['source'],state='mismatch',local=str(retained),local_stat=Fallback.stamp(retained),remaining_candidates=True,retained_copies={'known':dict(local=str(retained),local_stat=Fallback.stamp(retained),signature=[self.path,len(body_b),None],actual_sha256=item_b['sha256'])}))
        path_b=self.path.replace('Lecture.mp4','LectureB.mp4')
        row_a=dict(self.rows[-1],checksums=['SHA256:'+item_b['sha256']])
        row_b=dict(row_a,path=path_b)
        self.server.bodies[path_b]=body_b
        b_lookup=Event();a_started=Event();release_b=Event()
        def candidates(data,item):
            if item['path']==item_b['path']:
                b_lookup.set()
                if not release_b.wait(3):raise AssertionError('B was never released')
                return [row_b]
            return [row_a]
        recover_all=fallback.recover_all
        def recover(item,event,**kwargs):
            if item['path']==self.item['path']:a_started.set()
            return recover_all(item,event,**kwargs)
        with patch.object(fallback.index,'candidates',side_effect=candidates),patch.object(fallback,'recover_all',side_effect=recover):
            with ThreadPoolExecutor(max_workers=2) as pool:
                future_b=pool.submit(cloud.recover_all,item_b,event_b)
                self.assertTrue(b_lookup.wait(3))
                future_a=pool.submit(cloud.recover_all,self.item,self.event)
                self.assertTrue(a_started.wait(3));release_b.set()
                result_b=future_b.result(timeout=3)
                self.assertIsNone(future_a.result(timeout=3))
        self.assertIsNotNone(result_b)
        self.assertEqual(result_b['status'],'valid')
        self.assertEqual(d.target(self.root,item_b,'Course').read_bytes(),body_b)
        self.assertEqual([path for path,_ in self.server.gets],[path_b])

    def test_same_source_duplicate_md5_payload_is_downloaded_once(self):
        index=nextcloud.Index(self.root,'https://drive.example/s/test',stopping=Event())
        fallback=Fallback(self.root,index.link,Event(),index=index)
        wrong=b'wrong original'
        first=dict(self.rows[-1],path='2023-fall/Course/001 - Lecture.mp4',checksums=['MD5:'+hashlib.md5(wrong).hexdigest()])
        second=dict(first,path='2023-fall/Course/002 - Lecture.mp4')
        with patch.object(index.share,'listing',return_value=[first,second]),patch.object(index.share,'request',side_effect=lambda *args,**kwargs:Response(wrong)) as get:
            self.assertEqual(fallback.recover_all(self.item,self.event)['status'],'mismatch')
        self.assertEqual(get.call_count,1)
        self.assertEqual(len(fallback.read(self.item)['retained_copies']),1)

    def test_source_budget_mapping_uses_corresponding_source(self):
        first,second=object(),object()
        cloud=self.cloud('https://drive.example/s/test',{'nextcloud':first,'opencloud':second})
        self.assertIs(cloud.sources[0].budget,first)
        self.assertIs(cloud.sources[1].budget,second)

    def test_non_manifest_identity_never_downloads(self):
        cloud=self.cloud()
        unrelated=dict(self.item,path='/Course/Personal.mp4')
        self.assertFalse(cloud.ready(unrelated))
        self.assertIsNone(cloud.recover_all(unrelated,dict(self.event,**unrelated)))
        self.assertEqual(self.server.discoveries,0)
        self.assertEqual(self.server.gets,[])

    def test_pending_opencloud_body_resumes_before_nextcloud_lookup(self):
        cloud=self.cloud('https://drive.example/s/test');fallback=cloud.sources[1]
        fallback.directory.mkdir();fallback.save(self.item,dict(state='pending',remote_path=self.path,remote_size=len(self.body),received_bytes=4,retry_after=0))
        with patch.object(nextcloud.Share,'listing',side_effect=AssertionError('pending body lost priority')):
            self.assertEqual(cloud.recover_all(self.item,self.event)['status'],'valid')
        self.assertEqual(len(self.server.gets),1)


if __name__=='__main__':unittest.main()
