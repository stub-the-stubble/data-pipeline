import argparse
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'v2'))
import get_nrsc_data as pipeline
import utils
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, box


class PipelineTests(unittest.TestCase):
    def archive(self, members):
        result = io.BytesIO()
        with zipfile.ZipFile(result, 'w') as archive:
            for member in members:
                archive.writestr(member, b'test')
        result.seek(0)
        return result

    def frame(self):
        return gpd.GeoDataFrame({
            'state': ['PB'], 'sat': ['npp'], 'detection_': [8],
            'cropmask': [1], 'lat': [1.0], 'lon': [1.0],
            'acqdate': ['2026-10-23'],
        }, geometry=[Point(1, 1)], crs='EPSG:4326')

    def boundaries(self):
        return gpd.GeoDataFrame({'District': ['A']}, geometry=[box(0, 0, 2, 2)], crs='EPSG:4326')

    def test_archive_requires_all_components(self):
        for members in [['only.prj'], ['fire.shp'], ['a.shp', 'b.shp']]:
            with self.assertRaises(ValueError):
                utils.get_shp_from_zip(self.archive(members))
        members = ['nested/fire' + ext for ext in ['.shp', '.shx', '.dbf', '.prj']]
        self.assertEqual(utils.get_shp_from_zip(self.archive(members)), 'nested/fire.shp')
        with self.assertRaises(ValueError):
            utils.get_shp_from_zip(io.BytesIO(b'<html>error</html>'))

    def test_iframe_parser_validates_download(self):
        origin = 'https://bhuvan-app1.nrsc.gov.in/dir/source.php'
        self.assertEqual(pipeline.download_url("<iframe src='file.zip'></iframe>", origin),
                         'https://bhuvan-app1.nrsc.gov.in/dir/file.zip')
        for html in ['error', '<iframe src="http://other.test/file.zip">', '<iframe src="bad.php">']:
            with self.assertRaises(ValueError):
                pipeline.download_url(html, origin)

    def test_date_arguments(self):
        self.assertEqual(pipeline.validate_date('2026-10-23'), '2026-10-23')
        for value in ['2026-02-30', '20261023', '2099-01-01']:
            with self.assertRaises(argparse.ArgumentTypeError):
                pipeline.validate_date(value)

    def test_source_date_schema_and_empty_valid_source(self):
        frame = self.frame()
        self.assertEqual(len(pipeline.validate_source(frame, '2026-10-23')), 1)
        self.assertTrue(pipeline.validate_source(frame.iloc[:0], '2026-10-23').empty)
        with self.assertRaises(ValueError):
            pipeline.validate_source(frame, '2026-10-24')
        with self.assertRaises(ValueError):
            pipeline.validate_source(frame.drop(columns=['cropmask']), '2026-10-23')

    def test_thresholds_and_exact_state_filter(self):
        frame = pd.DataFrame({'cropmask': [1, 1, 1, 1, None],
            'sat': ['npp', 'npp', 'mod', 'mod', 'npp'], 'detection_': [7, 8, 30, 31, 90]})
        self.assertEqual(pipeline.filter_data(frame).index.tolist(), [1, 3])
        self.assertEqual(pipeline.filter_state(pd.DataFrame({'state': ['PB', 'PBX', None]}), 'PB').index.tolist(), [0])

    def test_district_join_and_unmapped_observation(self):
        frame = self.frame()
        result = pipeline.add_district_data(frame, self.boundaries())
        self.assertEqual(result.district.tolist(), ['A'])
        frame.geometry = [Point(10, 10)]
        result = pipeline.add_district_data(frame, self.boundaries())
        self.assertEqual(len(result), 1)
        self.assertTrue(result.district.isna().all())
        self.assertEqual(result.district_assignment.tolist(), ['unassigned'])
        self.assertEqual(result.state.tolist(), ['PB'])

    def test_overlapping_boundaries_rejected(self):
        duplicate = pd.concat([self.boundaries(), self.boundaries()], ignore_index=True)
        with self.assertRaises(ValueError):
            pipeline.add_district_data(self.frame(), duplicate)

    def test_daily_history_upsert_and_zero(self):
        with tempfile.TemporaryDirectory() as folder:
            Path(folder, 'PB').mkdir()
            history = {'total': {'dates': {'2026-10-22': 3}}, 'districts': {}}
            Path(folder, 'PB', 'historical_data.json').write_text(json.dumps(history))
            frame = pd.DataFrame({'district': ['A'], 'lat': [1.0], 'lon': [1.0]})
            for _ in range(2):
                pipeline.write_todays_date_data(frame, self.boundaries(), 'PB', '2026-10-23', folder)
            data = json.loads(Path(folder, 'PB', 'historical_data.json').read_text())
            self.assertEqual(data['total']['dates'], {'2026-10-22': 3, '2026-10-23': 1})
            self.assertEqual(data['districts']['A']['dates']['2026-10-23'], 1)
            pipeline.write_todays_date_data(frame.iloc[:0], self.boundaries(), 'PB', '2026-10-24', folder)
            daily = json.loads(Path(folder, 'PB', '2026-10-24.json').read_text())
            self.assertEqual(daily['total'], 0)
            self.assertEqual(daily['locations'], [])

    def test_unassigned_counts_preserve_total_without_inventing_history(self):
        with tempfile.TemporaryDirectory() as folder:
            Path(folder, 'PB').mkdir()
            history = {'total': {'dates': {'2026-10-22': 3}}, 'districts': {'A': {'dates': {'2026-10-22': 3}}}}
            Path(folder, 'PB', 'historical_data.json').write_text(json.dumps(history))
            frame = pd.DataFrame({'district': ['A', None], 'lat': [1.0, 3.0], 'lon': [1.0, 3.0]})
            pipeline.write_todays_date_data(frame, self.boundaries(), 'PB', '2026-10-23', folder)
            daily = json.loads(Path(folder, 'PB', '2026-10-23.json').read_text())
            updated = json.loads(Path(folder, 'PB', 'historical_data.json').read_text())
            self.assertEqual(daily['total'], 2)
            self.assertEqual(daily['unassigned_total'], 1)
            self.assertEqual(sum(daily['districts'].values()) + daily['unassigned_total'], 2)
            self.assertIsNone(daily['locations'][1]['district'])
            self.assertEqual(updated['unassigned']['dates'], {'2026-10-23': 1})
            self.assertEqual(updated['total']['dates']['2026-10-22'], 3)

    def test_unknown_district_name_cannot_silently_disappear(self):
        with tempfile.TemporaryDirectory() as folder:
            Path(folder, 'PB').mkdir()
            with self.assertRaises(ValueError):
                pipeline.write_todays_date_data(pd.DataFrame({'district': ['UNKNOWN']}), self.boundaries(), 'PB', '2026-10-23', folder)
            self.assertEqual(list(Path(folder).rglob('*.json')), [])

    def test_second_state_failure_writes_neither_state(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(pipeline, 'parse_arguments', return_value=argparse.Namespace(date='2026-10-23', temp=True, csv=False)), \
                 patch.object(utils, 'get_save_path', return_value=folder), \
                 patch.object(pipeline, 'get_data_from_nrsc', return_value=self.frame()), \
                 patch.object(pipeline, 'get_districts_geometry', return_value=self.boundaries()), \
                 patch.object(pipeline, 'add_district_data', side_effect=[self.frame().assign(district='A'), ValueError('bad boundaries')]):
                with self.assertRaises(ValueError):
                    pipeline.main()
            self.assertEqual(list(Path(folder).rglob('*.json')), [])

    def test_download_failure_never_writes_daily_json(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(pipeline, 'parse_arguments', return_value=argparse.Namespace(date='2026-10-23', temp=True, csv=False)), \
                 patch.object(utils, 'get_save_path', return_value=folder), \
                 patch.object(pipeline, 'get_data_from_nrsc', side_effect=ValueError('incomplete archive')):
                with self.assertRaises(ValueError):
                    pipeline.main()
            self.assertEqual(list(Path(folder).rglob('*.json')), [])


if __name__ == '__main__':
    unittest.main()
