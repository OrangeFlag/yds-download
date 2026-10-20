"""An offline original retires local recovery without discarding retained bytes."""
import json
from threading import Event
from unittest.mock import patch
import unittest
import pipeline
import range_queue
import settings
import volume_catalog
import download as d
import test_offline_pipeline as fixtures

class OfflineRangeChecks(unittest.TestCase):
    setUp=fixtures.OfflinePipelineChecks.setUp
    catalog=fixtures.OfflinePipelineChecks.catalog
    def test_applied_partial_task_is_discovered_and_retired_without_rechecking(self):
        host=self.catalog();state=self.root/'.download';(state/'tasks').mkdir(parents=True)
        item=self.items['First'];source=self.data['source']
        body=self.root/'corrupted/old/First/video.mp4';body.parent.mkdir(parents=True);body.write_bytes(b'lec')
        event=dict(item,course='First',attempt=1,status='partial',corrupted_path=str(body.relative_to(self.root)))
        files={'source.json':dict(source=source),'catalog.json':dict(source=source),
            'settings.json':dict(retries=0,tuning=settings.resolve()),'courses.json':['First'],
            '01-manifest.json':[item],'summary.json':dict(downloads_complete=True,pending_courses=[],failed=1,complete=False),
            'course-history.json':{'First':dict(state='finished',verified=0,failed=1)},
            'queue.json':dict(archives=[dict(state='finished',destination=str(self.root),catalog=str(state/'catalog.json'))]),
            'tasks/partial.json':dict(kind='range',source=source,item=item,event=event,course='First',attempt=1,
                state='applied',result=dict(event=event))}
        for name,value in files.items():d.write_json(state/name,value)
        stopping=Event();passes=0
        def wait(_):
            nonlocal passes
            passes+=1
            if passes>=4:stopping.set()
        with patch.dict('os.environ',dict(YDS_VOLUME_CATALOG=str(host),YDS_VOLUME_UUID=fixtures.NEW)), \
                patch.object(stopping,'wait',side_effect=wait), \
                patch('recovery.finish_recovery',side_effect=AssertionError('Offline recovery started')):
            range_queue.run([state/'queue.json'],files=1,stopping=stopping)
        job=json.loads((state/'tasks/partial.json').read_text())
        self.assertEqual(job['result']['event']['status'],'stored_offline')
        summary=json.loads((state/'summary.json').read_text())
        self.assertTrue(summary['complete']);self.assertEqual(summary['offline_available'],1)
        self.assertEqual(body.read_bytes(),b'lec')

    def test_retirement_preserves_raw_body_and_can_be_reversed_when_proof_disappears(self):
        host=self.catalog();state=self.root/'.download';state.mkdir()
        bg=pipeline.Background(self.root,self.data['source'],0,settings.resolve(),defer_range=True)
        self.addCleanup(bg.close)
        item=self.items['First'];body=self.root/'corrupted/old/First/video.mp4';body.parent.mkdir(parents=True);body.write_bytes(b'lec')
        event=dict(item,course='First',attempt=1,status='partial',corrupted_path='corrupted/old/First/video.mp4')
        bg.submit('range',item,'First',1,event);path=next(iter(bg.jobs));job=json.loads(path.read_text())
        index=volume_catalog.OfflineIndex(host,fixtures.NEW)
        self.assertTrue(range_queue.retire_offline_copy(bg,path,job,index))
        self.assertEqual(body.read_bytes(),b'lec')
        job=json.loads(path.read_text());self.assertEqual(job['state'],'applied')
        self.assertEqual(job['result']['event']['status'],'stored_offline')
        self.assertEqual(job['result']['event']['corrupted_path'],event['corrupted_path'])
        self.catalog(verified=False)
        self.assertFalse(range_queue.retire_offline_copy(bg,path,job,index))
        self.assertEqual(json.loads(path.read_text())['state'],'pending')
        self.assertEqual(body.read_bytes(),b'lec')
