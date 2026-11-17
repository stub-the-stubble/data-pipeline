"""Scheduler policy and transaction failure tests. No network or publishing."""
import copy
from datetime import date
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'v2'))
import iari_schedule as schedule

POLICY = schedule.read_json(schedule.POLICY)
URL = 'https://creams.iari.res.in/Creams_website_bulletin/Fire_bulletin_2026/Fire_bulletin_2026r/62.RiceResidueFireBulletin_15Nov_2026_ICAR.pdf'


class ScheduleTests(unittest.TestCase):
    def test_latest_completed_day_ignores_today_and_other_hosts(self):
        html = f'<a href="{URL}">15</a><a href="{URL.replace("15Nov", "16Nov")}">16</a>'
        html += f'<a href="{URL.replace("creams.iari.res.in", "example.com")}">wrong host</a>'
        self.assertEqual(schedule.latest_bulletin(html, POLICY, date(2026, 11, 16)), ('2026-11-15', URL))

    def test_only_links_not_script_strings_are_discovered(self):
        with self.assertRaises(ValueError):
            schedule.latest_bulletin(f'<script>let url="{URL}";</script>', POLICY, date(2026, 11, 16))

    def test_missing_overdue_and_ambiguous_bulletins_hold_publication(self):
        for html, today in [('', date(2026, 11, 16)), (f'<a href="{URL}">15</a>', date(2026, 11, 20)),
                            (f'<a href="{URL}">15</a><a href="{URL.replace("62.Rice", "63.Rice")}">15</a>', date(2026, 11, 16))]:
            with self.assertRaises(ValueError): schedule.latest_bulletin(html, POLICY, today)

    def test_new_year_and_closed_season_require_review(self):
        for today in [date(2027, 11, 16), date(2026, 12, 8)]:
            with self.assertRaises(ValueError): schedule.latest_bulletin(f'<a href="{URL}">15</a>', POLICY, today)

    def test_changed_archive_is_rejected_before_extraction(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'evidence'
            with self.assertRaises(ValueError):
                schedule.extract_reviewed(target, {**POLICY, 'archive_sha256': 'bad'})
            self.assertFalse(target.exists())

    def test_nrsc_baseline_cannot_be_migrated_implicitly(self):
        days = schedule.days_through('2026-11-15')
        history = {'total': {'dates': dict.fromkeys(days, 0)}, 'sources': {'dates': dict.fromkeys(days, 'nrsc-legacy')}}
        with patch.object(schedule, 'require_histories', return_value={s: copy.deepcopy(history) for s in schedule.STATES}), \
             patch.object(schedule, 'validate_published_data'):
            with self.assertRaisesRegex(ValueError, 'mixed season'):
                schedule.require_iari_baseline(Path('unused'), POLICY)

    def test_download_failure_cannot_change_baseline(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder) / 'baseline'; base.mkdir()
            (base / 'sentinel.txt').write_text('preserve')
            before = schedule.fingerprint_tree(base)
            with patch.object(schedule, 'require_iari_baseline', return_value='2026-11-15'), \
                 patch.object(schedule, 'download', side_effect=TimeoutError('offline')):
                with self.assertRaises(TimeoutError):
                    schedule.prepare(base, Path(folder)/'work', session=object(), today=date(2026,11,16))
            self.assertEqual(schedule.fingerprint_tree(base), before)
            self.assertFalse((Path(folder)/'work/release.json').exists())

    def make_release(self, folder):
        base, work = Path(folder)/'base', Path(folder)/'work'
        (base/'PB').mkdir(parents=True); (base/'HR').mkdir()
        (work/'release/PB').mkdir(parents=True); (work/'release/HR').mkdir()
        for state in schedule.STATES:
            (base/state/'historical_data.json').write_text('{"old":1}')
            (work/'release'/state/'historical_data.json').write_text('{"new":2}')
        report = {'status':'ready', 'baseline_sha256':schedule.fingerprint_tree(base),
                  'release_sha256':schedule.fingerprint_tree(work/'release'),
                  'changed_files':['PB/historical_data.json','HR/historical_data.json']}
        (work/'release.json').write_text(json.dumps(report))
        return base, work, report

    def test_apply_rejects_stale_baseline_or_changed_candidate(self):
        for modified in ['base', 'work/release']:
            with tempfile.TemporaryDirectory() as folder:
                base, work, report = self.make_release(folder)
                (Path(folder)/modified/'PB/historical_data.json').write_text('changed')
                before = schedule.fingerprint_tree(base)
                with self.assertRaises(ValueError): schedule.apply(base, work)
                self.assertEqual(schedule.fingerprint_tree(base), before)

    def test_apply_rolls_back_if_second_file_install_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            base, work, report = self.make_release(folder)
            real_replace = schedule.os.replace
            calls = []
            def fail_second(src, dest):
                calls.append(dest)
                if len(calls) == 2: raise OSError('injected disk failure')
                return real_replace(src, dest)
            with patch.object(schedule, 'validate_published_data'), patch.object(schedule, 'require_iari_baseline'), \
                 patch.object(schedule.os, 'replace', side_effect=fail_second):
                with self.assertRaises(OSError): schedule.apply(base, work)
            self.assertEqual(schedule.fingerprint_tree(base), report['baseline_sha256'])

    def test_apply_rejects_paths_outside_state_json(self):
        with tempfile.TemporaryDirectory() as folder:
            base, work, report = self.make_release(folder)
            report['changed_files'].append('../config.json')
            (work/'release.json').write_text(json.dumps(report))
            with patch.object(schedule, 'validate_published_data'), patch.object(schedule, 'require_iari_baseline'):
                with self.assertRaises(ValueError): schedule.apply(base, work)

    def test_workflow_calls_iari_and_holds_no_change_deployment(self):
        workflow = (schedule.REPOSITORY/'.github/workflows/main.yml').read_text()
        self.assertIn('python v2/iari_schedule.py', workflow)
        self.assertNotIn('python v2/get_nrsc_data.py', workflow)
        self.assertIn("if: steps.ingest.outputs.changed == 'true'", workflow)
        self.assertIn("if: steps.release.outputs.publish == 'true'", workflow)
        self.assertIn('group: nrsc-data-release', workflow)


if __name__ == '__main__':
    unittest.main()
