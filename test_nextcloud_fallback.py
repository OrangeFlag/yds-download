"""Fallback copies are trusted only after comparing downloaded bytes with Yandex SHA."""
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from threading import Event
import unittest
from unittest.mock import patch
import urllib.error

import download as d
import nextcloud
import pipeline
import recovery
from nextcloud_fallback import Fallback


class Response(io.BytesIO):
    def __init__(self, content, status=200, headers=None):
        super().__init__(content);self.status=status
        self.headers={'Content-Length':str(len(content)),'ETag':'"stable"',**(headers or {})}


class FallbackChecks(unittest.TestCase):
    def test_fallback_urlerror_logs_reason_and_clears_it_after_success(self):
        import socket
        import time
        error = urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, 'DNS temporarily unavailable'))
        output = io.StringIO()
        fallback = Fallback(self.root, self.link, Event())
        with patch.object(nextcloud.Share, 'listing', side_effect=error), patch('sys.stdout', output):
            self.assertIsNone(fallback.recover(self.item, self.event))
        row = fallback.read(self.item)
        self.assertEqual(row['state'], 'pending')
        self.assertGreater(row['retry_after'], time.time())
        self.assertEqual(row['error_reason'], str(error.reason))
        self.assertEqual(row['error_errno'], socket.EAI_AGAIN)
        self.assertIn('DNS temporarily unavailable', output.getvalue())
        self.assertIn(f'errno={socket.EAI_AGAIN}', output.getvalue())
        with patch.object(nextcloud.Share, 'listing', return_value=[self.row]), \
                patch.object(nextcloud.Share, 'request', return_value=Response(self.content)), \
                patch.object(nextcloud.time,'monotonic',return_value=fallback.index.retry_at['2023-fall/Course']+1):
            self.assertEqual(fallback.recover(self.item, self.event)['status'], 'valid')
        row = fallback.read(self.item)
        self.assertIsNone(row['error_reason'])
        self.assertIsNone(row['error_errno'])

    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name);self.state=self.root/'.download';self.state.mkdir()
        self.data=dict(source='https://disk.yandex.ru/d/test',title='ШАД осень 2023')
        d.write_json(self.state/'source.json',self.data)
        self.content=b'original lecture'
        self.item=dict(path='/Course/2023-09-15 Лекция.mp4',size=len(self.content),
            sha256=hashlib.sha256(self.content).hexdigest(),file='https://example.invalid/original')
        self.event=dict(self.item,course='Course',attempt=3,status='missing',corrupted_path='corrupted/body.mp4')
        self.link='https://drive.oleja.dev/index.php/s/read'
        self.row=dict(path='2023-fall/Course/001 - 2023-09-15 Лекция.mp4',size=len(self.content),directory=False,checksums=[])

    def fetch(self, content=None, rows=None, response=None):
        with patch.object(nextcloud.Share,'listing',return_value=rows if rows is not None else [self.row]), \
             patch.object(nextcloud.Share,'request',return_value=response or Response(content if content is not None else self.content)):
            return Fallback(self.root,self.link,Event()).recover(self.item,self.event)

    def test_matching_sha_produces_a_verified_local_original(self):
        result=self.fetch()
        self.assertEqual(result['status'],'valid')
        self.assertEqual(d.target(self.root,self.item,'Course').read_bytes(),self.content)
        self.assertTrue(result['range_recovered_complete'])
        self.assertTrue(recovery.range_result_current(self.root,self.item,result))
        self.assertFalse(Fallback(self.root,self.link,Event()).ready(self.item))
        import file_hashes
        target=d.target(self.root,self.item,'Course')
        self.assertIsNone(file_hashes.load(target,root=self.root))
        d.sha256_file(target)
        hashes=file_hashes.load(target,root=self.root)
        self.assertEqual(hashes,{name:hashlib.new(name,self.content).hexdigest() for name in file_hashes.ALGORITHMS})

    def test_published_fallback_mutation_does_not_receive_a_hash_cache_proof(self):
        import file_hashes
        target=d.target(self.root,self.item,'Course');real_unlink=Path.unlink
        def changed(path,*args,**kwargs):
            if path.suffix=='.part' and target.exists():
                before=target.stat();target.write_bytes(b'x'*len(self.content))
                os.utime(target,ns=(before.st_atime_ns,before.st_mtime_ns))
            return real_unlink(path,*args,**kwargs)
        with patch.object(Path,'unlink',changed):
            self.fetch()
        self.assertIsNone(file_hashes.load(target,root=self.root))
        self.assertNotEqual(d.sha256_file(target),self.item['sha256'])

    def test_mismatch_preserves_separate_copy_with_source_suffix(self):
        wrong=b'different video'
        self.row['size']=len(wrong)
        result=self.fetch(wrong)
        self.assertEqual(result['status'],'mismatch')
        local=Path(result['local'])
        self.assertIn('(SHA mismatch, drive.oleja.dev)',local.name)
        self.assertEqual(local.read_bytes(),wrong)
        self.assertEqual(result['actual_sha256'],hashlib.sha256(wrong).hexdigest())
        self.assertFalse(d.target(self.root,self.item,'Course').exists())
        self.assertFalse(any(e['status']=='valid' for e in d.read_events(self.state/'events.jsonl')))
        self.assertFalse(Fallback(self.root,self.link,Event()).ready(self.item))

    def test_wrong_semester_partial_and_personal_files_are_not_downloaded(self):
        rows=[dict(self.row,path='2024-fall/Course/001 - 2023-09-15 Лекция.mp4'),
              dict(self.row,path='2023-fall/Course/001 - 2023-09-15 Лекция (partially available 70%).mp4')]
        with patch.object(nextcloud.Share,'request',side_effect=AssertionError('unrelated video downloaded')):
            with patch.object(nextcloud.Share,'listing',return_value=rows):
                self.assertIsNone(Fallback(self.root,self.link,Event()).recover(self.item,self.event))
        d.write_json(self.state/'catalog.json',dict(root=True))
        self.assertFalse(Fallback(self.root,self.link,Event()).ready(self.item))

    def test_interrupted_body_resumes_with_validated_content_range(self):
        class Interrupted(Response):
            def read(self,amount=-1):
                if self.tell():raise TimeoutError()
                return super().read(5)
        calls=[]
        def request(share,method,path='',data=None,headers=None,uploads=False):
            calls.append(headers or {})
            if len(calls)==1:return Interrupted(self.content)
            return Response(self.content[5:],206,{'Content-Range':f'bytes 5-{len(self.content)-1}/{len(self.content)}'})
        with patch.object(nextcloud.Share,'listing',return_value=[self.row]),patch.object(nextcloud.Share,'request',request):
            fallback=Fallback(self.root,self.link,Event())
            self.assertIsNone(fallback.recover(self.item,self.event))
            result=fallback.recover(self.item,self.event)
        self.assertEqual(result['status'],'valid')
        self.assertEqual(calls[1]['Range'],'bytes=5-')
        self.assertEqual(calls[1]['If-Range'],'"stable"')

    def test_false_resume_response_never_becomes_a_valid_or_mismatched_copy(self):
        class Interrupted(Response):
            def read(self,amount=-1):
                if self.tell():raise TimeoutError()
                return super().read(5)
        replies=[Interrupted(self.content),Response(self.content[5:],206,{'Content-Range':f'bytes 0-{len(self.content)-6}/{len(self.content)}'})]
        with patch.object(nextcloud.Share,'listing',return_value=[self.row]),patch.object(nextcloud.Share,'request',side_effect=replies):
            fallback=Fallback(self.root,self.link,Event())
            self.assertIsNone(fallback.recover(self.item,self.event))
            self.assertIsNone(fallback.recover(self.item,self.event))
        self.assertFalse(d.target(self.root,self.item,'Course').exists())
        self.assertFalse(list((self.root/'Course').glob('*SHA mismatch*')) if (self.root/'Course').exists() else [])

    def test_existing_local_data_is_never_overwritten(self):
        target=d.target(self.root,self.item,'Course');target.parent.mkdir();target.write_bytes(b'keep this file')
        self.assertIsNone(self.fetch())
        self.assertEqual(target.read_bytes(),b'keep this file')

    def test_background_tries_fallback_before_range_search(self):
        background=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True,nextcloud_fallback=self.link)
        self.addCleanup(background.close)
        background.submit('range',self.item,'Course',3,self.event)
        path,job=next(iter(background.jobs.items()))
        with patch.object(nextcloud.Share,'listing',return_value=[self.row]),patch.object(nextcloud.Share,'request',return_value=Response(self.content)), \
             patch.object(recovery,'finish_recovery',side_effect=AssertionError('searched Yandex despite valid fallback')):
            result=background.work(path,job)
        self.assertTrue(result['result']['event']['range_recovered_complete'])

    def test_worker_backfills_an_applied_failed_task_and_updates_course_summary(self):
        import range_queue as q
        stopping=Event()
        catalog=self.root/'catalog.json';d.write_json(catalog,self.data)
        queue=self.root/'queue.json'
        d.write_json(queue,dict(archives=[dict(destination=str(self.root),catalog=str(catalog),state='finished')]))
        d.write_json(self.state/'catalog.json',self.data)
        d.write_json(self.state/'courses.json',['Course'])
        d.write_json(self.state/'01-manifest.json',[{k:self.item[k] for k in ('path','size','sha256')}])
        d.write_json(self.state/'settings.json',dict(retries=0,tuning=pipeline.resolve(),nextcloud_existing=self.link))
        d.write_json(self.state/'nextcloud-existing.json',dict(source=self.data['source'],share=nextcloud.Share(self.link).url(),files={self.item['path']:{k:self.item[k] for k in ('size','sha256')}}))
        d.write_json(self.state/'summary.json',dict(downloads_complete=True,pending_courses=[],failed=1))
        d.write_json(self.state/'course-history.json',{'Course':dict(source=self.data['source'],state='finished',failed=1)})
        recovery.recovery_state(self.root,self.event,'finished')
        background=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True)
        background.submit('range',self.item,'Course',3,self.event)
        path,job=next(iter(background.jobs.items()));job.update(state='applied',result=dict(event=self.event))
        d.write_json(path,job);background.close()
        finish=q.finish_courses
        def finished(*args,**kwargs):
            finish(*args,**kwargs)
            if json.loads((self.state/'summary.json').read_text()).get('verified')==1:stopping.set()
        waits=0
        def wait(seconds):
            nonlocal waits
            waits+=1
            if waits>40:stopping.set()
            import time
            time.sleep(.01)
            return stopping.is_set()
        with patch.object(nextcloud.Share,'listing',return_value=[self.row]),patch.object(nextcloud.Share,'request',return_value=Response(self.content)), \
             patch.object(recovery,'finish_recovery',side_effect=AssertionError('repeated finished Yandex recovery')), \
             patch.object(q,'finish_courses',finished),patch.object(stopping,'wait',wait):
            q.run([queue],files=1,per_file=1,requests=1,rps=5,stopping=stopping,nextcloud_fallback=self.link)
        summary=json.loads((self.state/'summary.json').read_text())
        self.assertEqual(summary['verified'],1)
        self.assertTrue(summary['complete'])
        self.assertTrue(json.loads(path.read_text())['result']['event']['range_recovered_complete'])

    def test_range_ignored_response_restarts_the_body_safely(self):
        class Interrupted(Response):
            def read(self,amount=-1):
                if self.tell():raise TimeoutError()
                return super().read(5)
        with patch.object(nextcloud.Share,'listing',return_value=[self.row]), \
             patch.object(nextcloud.Share,'request',side_effect=[Interrupted(self.content),Response(self.content)]):
            fallback=Fallback(self.root,self.link,Event())
            self.assertIsNone(fallback.recover(self.item,self.event))
            self.assertEqual(fallback.recover(self.item,self.event)['status'],'valid')
        self.assertEqual(d.target(self.root,self.item,'Course').read_bytes(),self.content)

    def test_long_mismatch_name_keeps_the_source_marker(self):
        item=dict(self.item,path='/Course/'+('Лекция '*80)+'.mp4')
        destination=Fallback(self.root,self.link,Event()).mismatch_path(item,'drive.oleja.dev')
        self.assertLessEqual(len(destination.name.encode()),230)
        self.assertIn('(SHA mismatch, drive.oleja.dev)',destination.name)

    def test_metadata_outage_is_retried_before_deferring_file(self):
        calls=[]
        def listing(*args):
            calls.append(1)
            if len(calls)==1:raise TimeoutError()
            return [self.row]
        with patch.object(nextcloud.Share,'listing',listing),patch.object(nextcloud.Share,'request',return_value=Response(self.content)):
            fallback=Fallback(self.root,self.link,Event())
            self.assertEqual(fallback.recover(self.item,self.event)['status'],'valid')
        self.assertEqual(len(calls),2)

    def test_catalog_cooldown_is_shared_across_fallback_files(self):
        stopping=Event();fallback=Fallback(self.root,self.link,stopping)
        with patch.object(stopping,'wait',return_value=False), \
                patch.object(nextcloud.Share,'listing',side_effect=TimeoutError()) as listing:
            self.assertIsNone(fallback.recover(self.item,self.event))
            other=dict(self.item,path='/Course/another.mp4')
            self.assertIsNone(fallback.recover(other,self.event))
        self.assertEqual(listing.call_count,3)
        with patch.object(nextcloud.time,'monotonic',return_value=fallback.index.retry_at['2023-fall/Course']+1), \
                patch.object(nextcloud.Share,'listing',return_value=[self.row]), \
                patch.object(nextcloud.Share,'request',return_value=Response(self.content)):
            self.assertEqual(fallback.recover(self.item,self.event)['status'],'valid')

    def test_permission_failure_can_recover_after_catalog_cooldown(self):
        fallback=Fallback(self.root,self.link,Event())
        error=urllib.error.HTTPError('hidden',403,'Forbidden',{},None)
        with patch.object(nextcloud.time,'monotonic',return_value=100) as clock, \
                patch.object(nextcloud.Share,'listing',side_effect=error) as listing, \
                patch.object(nextcloud.Share,'request',return_value=Response(self.content)):
            self.assertIsNone(fallback.recover(self.item,self.event))
            self.assertIsNone(fallback.recover(self.item,self.event))
            self.assertEqual(listing.call_count,1)
            clock.return_value=161;listing.side_effect=None;listing.return_value=[self.row]
            self.assertEqual(fallback.recover(self.item,self.event)['status'],'valid')
            self.assertEqual(listing.call_count,2)

    def test_malformed_listing_is_retried_instead_of_crashing_worker(self):
        import xml.etree.ElementTree as ET
        with patch.object(nextcloud.Share,'listing',side_effect=ET.ParseError('truncated XML')):
            self.assertIsNone(Fallback(self.root,self.link,Event()).recover(self.item,self.event))
        row=Fallback(self.root,self.link,Event()).read(self.item)
        self.assertEqual(row['state'],'pending')

    def test_resuming_only_requires_space_for_remaining_bytes(self):
        from types import SimpleNamespace
        class Interrupted(Response):
            def read(self,amount=-1):
                if self.tell():raise TimeoutError()
                return super().read(5)
        with patch.object(nextcloud.Share,'listing',return_value=[self.row]), \
             patch.object(nextcloud.Share,'request',side_effect=[Interrupted(self.content),Response(self.content[5:],206,{'Content-Range':f'bytes 5-{len(self.content)-1}/{len(self.content)}'})]):
            fallback=Fallback(self.root,self.link,Event())
            self.assertIsNone(fallback.recover(self.item,self.event))
            with patch('nextcloud_fallback.shutil.disk_usage',return_value=SimpleNamespace(free=1024**3+len(self.content)-5)):
                self.assertEqual(fallback.recover(self.item,self.event)['status'],'valid')

    def test_non_shad_course_archive_is_not_downloaded(self):
        d.write_json(self.state/'source.json',dict(self.data,title='осень 2023'))
        with patch.object(nextcloud.Share,'request',side_effect=AssertionError('non-SHAD data downloaded')):
            fallback=Fallback(self.root,self.link,Event())
            self.assertFalse(fallback.ready(self.item))
            self.assertIsNone(fallback.recover(self.item,self.event))

    def test_a_mismatched_candidate_does_not_hide_a_second_valid_copy(self):
        rows=[self.row,dict(self.row,path=self.row['path'].replace('001 - ','002 - '))]
        with patch.object(nextcloud.Share,'listing',return_value=rows), \
             patch.object(nextcloud.Share,'request',side_effect=[Response(b'X'*len(self.content)),Response(self.content)]):
            fallback=Fallback(self.root,self.link,Event())
            self.assertEqual(fallback.recover(self.item,self.event)['status'],'mismatch')
            self.assertTrue(fallback.ready(self.item))
            self.assertEqual(fallback.recover(self.item,self.event)['status'],'valid')
        self.assertTrue(list((self.root/'Course').glob('*SHA mismatch*')))

    def test_applied_task_without_recovery_ledger_does_not_repeat_yandex(self):
        background=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True,nextcloud_fallback=self.link)
        self.addCleanup(background.close)
        background.submit('range',self.item,'Course',3,self.event,force=True,fallback_only=True)
        path,job=next(iter(background.jobs.items()))
        with patch.object(nextcloud.Share,'listing',return_value=[]), \
             patch.object(recovery,'finish_recovery',side_effect=AssertionError('repeated completed task')):
            background.work(path,job)

    def test_failed_first_candidate_rotates_to_the_second(self):
        rows=[self.row,dict(self.row,path=self.row['path'].replace('001 - ','002 - '))]
        calls=[]
        def request(share,method,path='',**kwargs):
            calls.append(path)
            if '001 - ' in path:raise urllib.error.HTTPError('https://example.invalid',404,'missing',{},None)
            return Response(self.content)
        with patch.object(nextcloud.Share,'listing',return_value=rows),patch.object(nextcloud.Share,'request',request):
            fallback=Fallback(self.root,self.link,Event())
            self.assertIsNone(fallback.recover(self.item,self.event))
            self.assertEqual(fallback.recover(self.item,self.event)['status'],'valid')
        self.assertIn('002 - ',calls[1])

    def test_background_checks_all_candidates_before_searching_yandex(self):
        rows=[self.row,dict(self.row,path=self.row['path'].replace('001 - ','002 - '))]
        background=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True,nextcloud_fallback=self.link)
        self.addCleanup(background.close);background.submit('range',self.item,'Course',3,self.event)
        path,job=next(iter(background.jobs.items()))
        with patch.object(nextcloud.Share,'listing',return_value=rows), \
             patch.object(nextcloud.Share,'request',side_effect=[Response(b'X'*len(self.content)),Response(self.content)]), \
             patch.object(recovery,'finish_recovery',side_effect=AssertionError('searched Yandex before exact second copy')):
            result=background.work(path,job)
        self.assertTrue(result['result']['event']['range_recovered_complete'])

    def test_changed_mismatch_copy_is_preserved_and_a_fresh_copy_is_kept(self):
        wrong=b'X'*len(self.content)
        with patch.object(nextcloud.Share,'listing',return_value=[self.row]), \
             patch.object(nextcloud.Share,'request',side_effect=[Response(wrong),Response(wrong)]) as request:
            fallback=Fallback(self.root,self.link,Event())
            first=fallback.recover(self.item,self.event)
            old=Path(first['local']);old.write_bytes(b'user changed this copy')
            self.assertTrue(fallback.ready(self.item))
            second=fallback.recover(self.item,self.event)
        self.assertEqual(request.call_count,2)
        self.assertEqual(second['status'],'mismatch')
        self.assertNotEqual(second['local'],first['local'])
        self.assertEqual(old.read_bytes(),b'user changed this copy')
        self.assertEqual(Path(second['local']).read_bytes(),wrong)

    def test_interrupted_backfill_retains_fallback_only_on_restart(self):
        background=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True,nextcloud_fallback=self.link)
        background.submit('range',self.item,'Course',3,self.event,force=True,fallback_only=True)
        path,job=next(iter(background.jobs.items()));job['state']='running';d.write_json(path,job)
        background.close()
        restarted=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True,nextcloud_fallback=self.link)
        self.addCleanup(restarted.close);restarted.submit('range',self.item,'Course',3,self.event)
        job=next(iter(restarted.jobs.values()))
        self.assertTrue(job.get('fallback_only'))
        with patch.object(nextcloud.Share,'listing',return_value=[]), \
             patch.object(recovery,'finish_recovery',side_effect=AssertionError('repeated completed Range after restart')):
            restarted.work(path,job)

    def test_explicit_retry_check_still_repeats_yandex_recovery(self):
        recovery.recovery_state(self.root,self.event,'finished')
        background=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True,nextcloud_fallback=self.link,retry_check=True)
        self.addCleanup(background.close);background.submit('range',self.item,'Course',3,self.event,force=True)
        path,job=next(iter(background.jobs.items()))
        with patch.object(nextcloud.Share,'listing',return_value=[]),patch.object(recovery,'finish_recovery') as recover:
            background.work(path,job)
        recover.assert_called_once()

    def test_changed_earlier_mismatch_retries_only_its_candidate(self):
        rows=[self.row,dict(self.row,path=self.row['path'].replace('001 - ','002 - '))]
        calls=[]
        def request(share,method,path='',**kwargs):
            calls.append(path)
            return Response((b'X' if '001 - ' in path else b'Y')*len(self.content))
        with patch.object(nextcloud.Share,'listing',return_value=rows),patch.object(nextcloud.Share,'request',request):
            fallback=Fallback(self.root,self.link,Event())
            first=fallback.recover(self.item,self.event);second=fallback.recover(self.item,self.event)
            self.assertFalse(fallback.ready(self.item))
            Path(first['local']).write_bytes(b'edited first copy')
            self.assertTrue(fallback.ready(self.item))
            restored=fallback.recover(self.item,self.event)
            self.assertFalse(fallback.ready(self.item))
        self.assertIn('001 - ',calls[-1])
        self.assertEqual(Path(first['local']).read_bytes(),b'edited first copy')
        self.assertEqual(Path(second['local']).read_bytes(),b'Y'*len(self.content))
        self.assertEqual(Path(restored['local']).read_bytes(),b'X'*len(self.content))

    def test_completed_range_is_not_repeated_when_fallback_has_no_match(self):
        recovery.recovery_state(self.root,self.event,'finished')
        background=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True,nextcloud_fallback=self.link)
        self.addCleanup(background.close);background.submit('range',self.item,'Course',3,self.event,force=True)
        path,job=next(iter(background.jobs.items()))
        with patch.object(nextcloud.Share,'listing',return_value=[]), \
             patch.object(recovery,'finish_recovery',side_effect=AssertionError('repeated completed Range')):
            background.work(path,job)


if __name__=='__main__':unittest.main()
