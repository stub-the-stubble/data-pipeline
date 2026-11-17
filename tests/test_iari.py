import copy
from datetime import date
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'v2'))
import iari_source as source
import iari_import as importer

FIXTURES = Path(__file__).parent / 'fixtures/iari'


class IariTests(unittest.TestCase):
    def setUp(self):
        self.wfs = json.loads((FIXTURES / 'wfs-excerpt.json').read_text())
        self.summary = (FIXTURES / 'summary-tables.txt').read_text(encoding='utf-8')

    def parse(self, data=None, expected=None):
        return source.from_wfs(data if data is not None else self.wfs, '2026-11-15', expected or {'PB': 1, 'HR': 0})

    def test_frozen_official_summary_reconciles_all_days_and_districts(self):
        result = source.parse_bulletin(self.summary, '2026-11-15')
        self.assertEqual(len(result['daily']), 62)
        self.assertEqual(sum(d['PB'] for d in result['daily'].values()), 5972)
        self.assertEqual(sum(d['HR'] for d in result['daily'].values()), 242)
        self.assertEqual(result['districts']['PB']['MALERKOTLA'], 153)
        self.assertEqual(result['districts']['HR']['HANSI'], 6)

    def test_missing_daily_row_and_wrong_columns_are_rejected(self):
        for text in [self.summary.replace('17-Sep', 'xx-Sep'), self.summary.replace('Punjab Haryana Uttar Pradesh', 'Haryana Punjab Uttar Pradesh')]:
            with self.assertRaises(ValueError): source.parse_bulletin(text, '2026-11-15')

    def test_summary_must_match_district_totals(self):
        with self.assertRaises(ValueError):
            source.parse_bulletin(self.summary.replace('265 ', '266 ', 1), '2026-11-15')

    def test_future_layout_is_not_guessed(self):
        with self.assertRaises(ValueError): source.days_through('2027-11-15')

    def test_real_event_retains_publisher_identity_and_time(self):
        event = self.parse()[0]
        self.assertEqual((event['district'], event['sat'], event['acqtime']), ('AMRITSAR', 'JPSS-1', '13:36:12'))
        self.assertEqual(event['district_assignment'], 'publisher')

    def test_complete_pagination_is_required(self):
        self.wfs['numberMatched'] = 2
        with self.assertRaises(ValueError): self.parse()

    def test_missing_or_duplicate_id_rejected(self):
        duplicate = copy.deepcopy(self.wfs['features'][0])
        self.wfs['features'].append(duplicate)
        self.wfs['numberMatched'] = self.wfs['numberReturned'] = 2
        with self.assertRaises(ValueError): self.parse(expected={'PB': 2, 'HR': 0})

    def test_same_observation_in_one_state_is_not_silently_counted_twice(self):
        second = copy.deepcopy(self.wfs['features'][0]); second['id'] += '.copy'
        self.wfs['features'].append(second); self.wfs['numberMatched'] = self.wfs['numberReturned'] = 2
        with self.assertRaises(ValueError): self.parse(expected={'PB': 2, 'HR': 0})

    def test_official_cross_state_observation_is_preserved(self):
        second = copy.deepcopy(self.wfs['features'][0]); second['id'] += '.other-state'
        second['properties']['district'] = 'KAITHAL'
        self.wfs['features'].append(second); self.wfs['numberMatched'] = self.wfs['numberReturned'] = 2
        self.assertEqual(len(self.parse(expected={'PB': 1, 'HR': 1})), 2)

    def test_bad_dates_coordinates_and_satellites_fail_closed(self):
        cases = [('acq_date', '2026-11-14Z'), ('acq_date', 14112026), ('latitude', float('nan')),
                 ('latitude', 91), ('satellite', 'unknown'), ('acq_time', '25:00:00'), ('day_flag', '?')]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                data = copy.deepcopy(self.wfs); data['features'][0]['properties'][field] = value
                with self.assertRaises(ValueError): self.parse(data)

    def test_geometry_crs_and_attribute_consistency(self):
        data = copy.deepcopy(self.wfs); data['features'][0]['geometry']['coordinates'][0] += .01
        with self.assertRaises(ValueError): self.parse(data)
        data = copy.deepcopy(self.wfs); data['crs']['properties']['name'] = 'EPSG:3857'
        with self.assertRaises(ValueError): self.parse(data)

    def test_count_difference_and_unknown_target_district_rejected(self):
        with self.assertRaises(ValueError): self.parse(expected={'PB': 2, 'HR': 0})
        self.wfs['features'][0]['properties'].update(district='UNRECOGNIZED', field11='PUNJAB')
        with self.assertRaises(ValueError): self.parse(expected={'PB': 0, 'HR': 0})

    def test_bulletin_issued_following_day_uses_event_date(self):
        text = (FIXTURES / 'oct21-events.txt').read_text(encoding='utf-8')
        events = source.from_event_table(text, '2026-10-21', {'PB': 70, 'HR': 2})
        self.assertEqual(len(events), 72)
        with self.assertRaises(ValueError): source.from_event_table(text, '2026-10-22', {'PB': 70, 'HR': 2})

    def test_pdf_missing_serial_is_not_accepted(self):
        text = (FIXTURES / 'oct21-events.txt').read_text(encoding='utf-8')
        text = text.replace('1 AMRITSAR AJNALA AQUA', '9 AMRITSAR AJNALA AQUA', 1)
        with self.assertRaises(ValueError): source.from_event_table(text, '2026-10-21', {'PB': 70, 'HR': 2})

    def test_source_hash_and_path_are_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); (root/'day.json').write_bytes(b'{}')
            desc = {'path': 'day.json', 'url': 'https://creams.iari.res.in/geokrishi/geoserver/wfs', 'sha256': importer.sha(b'{}')}
            self.assertEqual(importer.source_bytes(root, desc), b'{}')
            for modified in [{**desc, 'sha256': 'bad'}, {**desc, 'path': '../day.json'}, {**desc, 'url': 'https://other.example/wfs'}]:
                with self.assertRaises(ValueError): importer.source_bytes(root, modified)

    def test_missing_layer_is_not_zero_without_bulletin_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError): importer.resolve_day(root, '2026-09-17', {}, {'PB': 0, 'HR': 0}, {})
            fallback = {'fallback': {'kind': 'bulletin-zero', 'rejected_wfs_sha256': None, 'reason': 'Daily table confirms zero'}}
            descriptor = {'url': 'https://creams.iari.res.in/Creams_website_bulletin/example.pdf', 'sha256': 'test'}
            events, provenance = importer.resolve_day(root, '2026-09-17', fallback, {'PB': 0, 'HR': 0}, descriptor)
            self.assertEqual(events, [])
            with self.assertRaises(ValueError): importer.resolve_day(root, '2026-09-17', fallback, {'PB': 1, 'HR': 0}, descriptor)

    def test_complete_season_replacement_preserves_other_history_and_aliases(self):
        original = {'total': {'dates': {'2025-10-01': 9, '2026-10-01': 8}},
                    'districts': {'GURUGRAM': {'dates': {'2025-10-01': 9, '2026-10-01': 8}}}}
        saved = copy.deepcopy(original)
        history = importer.seed_history(original, 'HR', ['2026-10-01'])
        self.assertEqual(original, saved)
        self.assertEqual(history['districts']['GURGAON']['dates'], {'2025-10-01': 9})
        self.assertEqual(history['districts']['HANSI']['dates'], {})
        self.assertEqual(history['sources']['dates']['2025-10-01'], 'nrsc-legacy')

    def test_existing_candidate_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValueError): importer.build(Path(directory)/'missing.json', directory)

    def test_failed_import_leaves_no_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); bundle = root/'source'; bundle.mkdir()
            path = bundle/'bundle.json'; path.write_text(json.dumps({'version': 1, 'end': '2026-11-15',
                'summary': {'path': 'missing.pdf', 'url': 'https://creams.iari.res.in/Creams_website_bulletin/x.pdf', 'sha256': 'bad'}}))
            output = root/'candidate'
            with self.assertRaises(ValueError): importer.build(path, output)
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
