import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'v2'))
import backfill
import get_nrsc_data as pipeline
import geopandas as gpd
from shapely.geometry import box


class BackfillTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.baseline = self.root / 'baseline'
        self.csv = self.root / 'csv'
        self.csv.mkdir()
        self.work = self.root / 'candidate-review'
        for state in backfill.STATES:
            (self.baseline / state).mkdir(parents=True)
            backfill.save(self.baseline / state / 'historical_data.json', {
                'total': {'dates': {'2026-10-22': 3}},
                'districts': {'A': {'dates': {'2026-10-22': 3}}},
                'unassigned': {'dates': {'2026-10-22': 0}},
            })
        for day in ['2026-10-23', '2026-10-24']:
            (self.csv / f'{day}.csv').write_text(
                'state,sat,detection_,cropmask,lat,lon,acqdate,geometry\n'
                f'PB,npp,8,1,1,1,{day},POINT (1 1)\n')
        boundaries = gpd.GeoDataFrame({'District': ['A']}, geometry=[box(0,0,2,2)], crs='EPSG:4326')
        self.boundary_patch = patch.object(pipeline, 'get_districts_geometry', return_value=boundaries)
        self.boundary_patch.start()
        self.addCleanup(self.boundary_patch.stop)

    def run_job(self, **kw):
        return backfill.run(self.baseline, self.csv, '2026-10-23', '2026-10-24', **kw)

    def test_dry_run_preserves_baseline_and_leaves_no_candidate(self):
        original = backfill.fingerprint_tree(self.baseline)
        result = self.run_job()
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(len(result['allowlist']), 6)
        self.assertFalse(self.work.exists())
        self.assertEqual(backfill.fingerprint_tree(self.baseline), original)

    def test_missing_history_refused_before_output(self):
        (self.baseline / 'HR' / 'historical_data.json').unlink()
        with self.assertRaisesRegex(ValueError, 'Missing published history'):
            self.run_job(work_dir=self.work)
        self.assertFalse(self.work.exists())

    def test_existing_date_refused(self):
        (self.baseline / 'PB' / '2026-10-23.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'existing observation'):
            self.run_job(work_dir=self.work)

    def test_current_and_future_day_refused(self):
        with patch.object(backfill.utils, 'get_todays_date', return_value='2026-10-24'):
            with self.assertRaises(ValueError): self.run_job()
            result = self.run_job(include_current_day=True)
            self.assertTrue(result['provenance']['include_current_day'])
            with self.assertRaises(ValueError): backfill.dates_between('2026-10-24','2026-10-25',True)

    def test_candidate_and_resume_preserve_hashes(self):
        result = self.run_job(work_dir=self.work)
        again = self.run_job(work_dir=self.work, resume=True)
        self.assertEqual(result['output_sha256'], again['output_sha256'])
        self.assertEqual(result['completed']['2026-10-23']['PB']['total'], 1)
        history = backfill.json_read(self.work / 'candidate/PB/historical_data.json')
        self.assertEqual(history['unassigned']['dates']['2026-10-22'], 0)
        self.assertEqual(history['total']['dates']['2026-10-22'], 3)

    def test_changed_source_and_baseline_refused_on_resume(self):
        self.run_job(work_dir=self.work)
        source = self.csv / '2026-10-23.csv'
        original = source.read_bytes()
        source.write_bytes(original + b'\n')
        with self.assertRaisesRegex(ValueError, 'changed'): self.run_job(work_dir=self.work,resume=True)
        source.write_bytes(original)
        (self.baseline / 'unrelated.txt').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'changed'): self.run_job(work_dir=self.work,resume=True)

    def test_candidate_tamper_refused(self):
        self.run_job(work_dir=self.work)
        (self.work / 'candidate/PB/2026-10-23.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'candidate changed'): self.run_job(work_dir=self.work,resume=True)

    def test_failed_day_checkpoint_resumes(self):
        real = backfill.calculate_day
        def fail(day, source, candidate):
            if day == '2026-10-24': raise ValueError('test failure')
            return real(day, source, candidate)
        with patch.object(backfill, 'calculate_day', side_effect=fail):
            with self.assertRaisesRegex(ValueError, 'test failure'): self.run_job(work_dir=self.work)
        manifest = backfill.json_read(self.work / 'manifest.json')
        self.assertEqual(manifest['status'], 'building')
        self.assertEqual(list(manifest['completed']), ['2026-10-23'])
        self.assertFalse((self.work / '.lock').exists())
        self.assertEqual(self.run_job(work_dir=self.work,resume=True)['status'], 'complete')

    def test_overlapping_output_and_lock_refused(self):
        with self.assertRaisesRegex(ValueError, 'outside'): self.run_job(work_dir=self.baseline / 'out')
        self.work.mkdir()
        (self.work / '.lock').write_text('busy')
        with self.assertRaises(FileExistsError): self.run_job(work_dir=self.work)
        self.assertTrue((self.work / '.lock').exists())

    def test_production_entrypoint_rejects_sparse_history(self):
        import argparse
        with patch.object(pipeline, 'parse_arguments', return_value=argparse.Namespace(temp=False)), \
             patch.object(pipeline.utils, 'get_save_path', return_value=self.root/'missing'):
            # A complete CLI namespace reaches preflight without any request.
            with patch.object(pipeline, 'parse_arguments', return_value=argparse.Namespace(date='2026-10-23',temp=False,csv=False)):
                with patch.object(pipeline, 'get_data_from_nrsc') as download:
                    with self.assertRaisesRegex(ValueError,'Missing published history'): pipeline.main()
                    download.assert_not_called()



class ReleaseValidationTests(unittest.TestCase):
    def test_location_labels_must_match_district_counts(self):
        history = {'total': {'dates': {'2026-10-23': 1}}, 'districts': {'A': {'dates': {'2026-10-23': 1}}}, 'unassigned': {'dates': {'2026-10-23': 0}}}
        daily = {'total': 1, 'districts': {'A': 1}, 'unassigned_total': 0, 'locations': [{'district': 'B'}]}
        with self.assertRaisesRegex(ValueError, 'Location district labels'):
            backfill.validate_day(daily, history, '2026-10-23')

if __name__ == '__main__': unittest.main()
