import io
import os
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import service_stats as stats


class StatsTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)

    def test_default_window_reads_rotated_logs_and_ignores_partial_tail(self):
        (self.root/'nextcloud.log.1').write_text('2026-10-17 10:00:00,000 NEXTCLOUD UPLOADED: old.mp4\n')
        (self.root/'nextcloud.log').write_text('2026-10-17 11:00:00,000 NEXTCLOUD UPLOADED: new.mp4\n2026-10-17 11:01:00,000 NEXTCLOUD UPLOADED: partial')
        rows=list(stats.log_rows(self.root/'nextcloud.log'))
        self.assertEqual(len(rows),2)
        self.assertEqual(len([r for r in rows if r[0]>=stats.timestamp('2026-10-17T10:30:00')]),1)

    def test_counter_windows_do_not_count_resumed_baseline_or_mix_epochs(self):
        rows=[dict(time=t,budget='/budget',epoch=e,sources={'nextcloud':n}) for t,e,n in
              [(100,'a',1000),(160,'a',1300),(170,'b',0),(230,'b',500)]]
        result=stats.traffic(rows,cutoff=None,now=230)
        self.assertEqual(result['nextcloud']['bytes'],800)
        self.assertAlmostEqual(result['nextcloud']['mbit_s'],500*8/60/1e6)
        self.assertEqual(stats.traffic(rows,cutoff=180,now=230)['nextcloud']['bytes'],0)

    def test_unreadable_rotation_warns_and_keeps_other_logs(self):
        current=self.root/'nextcloud.log';rotation=self.root/'nextcloud.log.1'
        current.write_text('2026-10-17 11:00:00,000 NEXTCLOUD UPLOADED: new.mp4\n')
        rotation.touch();real_open=Path.open;warnings=[]
        def opened(path,*args,**kwargs):
            if path==rotation:raise PermissionError('denied')
            return real_open(path,*args,**kwargs)
        with patch.object(Path,'open',opened):
            rows=list(stats.log_rows(current,warnings))
        self.assertEqual(len(rows),1)
        self.assertIn('PermissionError',warnings[0])

    def test_legacy_progress_ignores_body_buffers_and_first_unknown_prefix(self):
        rows=[(1,'NEXTCLOUD UPLOAD PROGRESS: video.mp4; 8192/16777216 bytes'),
              (2,'NEXTCLOUD UPLOAD PROGRESS: 2024-spring/Course/video.mp4; 100/300 bytes'),
              (3,'NEXTCLOUD UPLOAD PROGRESS: 2024-spring/Course/video.mp4; 200/300 bytes')]
        result=stats.log_summary(rows,'nextcloud',None)
        self.assertEqual(result['progress']['nextcloud'],100)

    def test_since_accepts_durations_and_rejects_nonpositive_values(self):
        self.assertEqual(stats.duration('15m'),900)
        self.assertEqual(stats.duration('2h'),7200)
        for value in ('0m','-1h','nope'):
            with self.assertRaises(ValueError):stats.duration(value)

    def test_output_marks_unknown_bytes_and_redacts_secrets_in_errors(self):
        result=stats.log_summary([(1,'NEXTCLOUD ERROR: file; HTTPError; reason=https://user:secret@example.org/s/token?x=secret')],'nextcloud',None)
        self.assertNotIn('secret',str(result['errors']))
        self.assertNotIn('token',str(result['errors']))

    def test_missing_user_bus_is_a_visible_warning(self):
        warnings=[]
        with patch('service_stats.subprocess.run',return_value=SimpleNamespace(returncode=1,stdout='',stderr='No bus')):
            self.assertEqual(stats.systemd(['queue'],warnings),{})
        self.assertTrue(warnings)

    def test_unreadable_disk_metadata_does_not_crash_report(self):
        warnings=[]
        with patch.object(Path,'exists',side_effect=OSError('disk disconnected')):
            self.assertFalse(stats.exists(self.root,warnings))
        self.assertTrue(warnings)

    def test_unmounted_directory_is_not_reported_as_archive_disk(self):
        cfg=dict(state_dir=str(self.root/'state'),mount_root=str(self.root),disk_uuid='missing',
                 repo=str(self.root),services={})
        with patch('storage.mounts',return_value=[]),patch('service_stats.systemd',return_value={}):
            result=stats.collect(cfg,[])
        self.assertNotIn('archive',result['disk'])
        self.assertTrue(any('UUID' in warning for warning in result['warnings']))

    def test_old_budget_slots_are_explicitly_stale(self):
        budget=self.root/'budget';budget.mkdir()
        state=budget/'state.json'
        state.write_text(json.dumps(dict(controller={'limit':8},members={'old':dict(name='yandex',active=8,waiting=3)})))
        os.utime(state,(1000,1000))
        cfg=dict(state_dir=str(self.root/'state'),mount_root=str(self.root),disk_uuid='missing',repo=str(self.root),
                 services={'queue':{'command':['python3','queue.py','--download-budget',str(budget)]}})
        with patch('storage.mounts',return_value=[]),patch('service_stats.systemd',return_value={}),patch('time.time',return_value=2000):
            result=stats.collect(cfg,[])
        self.assertTrue(result['directions']['download']['stale'])

    def test_readable_output_preserves_unknown_zero_and_stale_aggregated_slots(self):
        data=dict(now=2000,cutoff=None,summaries={name:dict(first=None,last=None,files=stats.Counter(),
                  progress=stats.Counter(),errors=stats.Counter()) for name in ('queue','range','nextcloud','opencloud')},
                  directions={'download':dict(traffic={'yandex':dict(bytes=1_500_000_000,measured=True,mbit_s=0)},
                      members=[dict(name='yandex',active=2,waiting=1),dict(name='yandex',active=1,waiting=2)],
                      controller={'limit':8},age=190,stale=True)},
                  services={},queue_states={'queue.json':{'pending':3,'finished':1}},
                  tasks={'range:pending':2},gauges={},metrics_age=None,host={},disk={},warnings=[])
        output=io.StringIO()
        with patch.object(stats,'collect',return_value=data),patch('sys.stdout',output):
            stats.show({},['queue'])
        rendered=output.getvalue()
        for heading in ('ПЕРЕДАЧИ','СЕРВИСЫ И ПАМЯТЬ','ОЧЕРЕДИ','СЕТЬ И ДИСК','ПРОБЛЕМЫ ЗА ПЕРИОД','ОХВАТ ЖУРНАЛОВ'):
            self.assertIn(heading,rendered)
        yandex=next(line for line in rendered.splitlines() if 'Yandex' in line)
        self.assertIn('≥1.5 ГБ',yandex)
        self.assertIn('0.00',yandex)
        self.assertEqual(yandex.split()[-2:],['3*','3*'])
        self.assertIn('нет данных',rendered)
        self.assertIn('УСТАРЕЛ',rendered)
        self.assertIn('включая ротации',rendered)
        self.assertIn('Нет свежего снимка Range/Recheck.',rendered)
        self.assertNotIn("{'",rendered)
        self.assertEqual(stats.size_label(0),'0.0 Б')
        self.assertEqual(stats.age_label(None),'нет данных')
        data['gauges']={'range_active_files':4,'range_task_discovery_pending_archives':2,'range_task_discovery_reads':32}
        with patch.object(stats,'collect',return_value=data),patch('sys.stdout',io.StringIO()) as output:
            stats.show({},['queue'])
        self.assertIn('Всего активных файлов: 4.',output.getvalue())
        self.assertIn('Очередь показана частично.',output.getvalue())


if __name__=='__main__':unittest.main()
