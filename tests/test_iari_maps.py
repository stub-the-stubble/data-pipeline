"""Reference geometry checks; these polygons must never assign event counts."""
import hashlib
import json
from pathlib import Path
import unittest

from shapely.geometry import shape
from shapely.ops import unary_union

ROOT = Path(__file__).resolve().parents[1]
MAPS = ROOT / 'geojson/iari-2026'


class IariMapTests(unittest.TestCase):
    def test_catalog_validity_and_district_interiors(self):
        catalog = json.loads((ROOT / 'v2/iari_districts.json').read_text())
        provenance = json.loads((MAPS / 'provenance.json').read_text())
        for state, names in catalog.items():
            raw = (MAPS / (state + '.geojson')).read_bytes()
            self.assertEqual(hashlib.sha256(raw).hexdigest(), provenance['outputs'][state])
            features = json.loads(raw)['features']
            self.assertEqual(len(features), 23)
            geometries = {f['properties']['District']: shape(f['geometry']) for f in features}
            self.assertEqual(set(geometries), set(names))
            for name, geometry in geometries.items():
                self.assertTrue(geometry.is_valid and not geometry.is_empty, name)
                for other, other_geometry in geometries.items():
                    if name < other:
                        self.assertLess(geometry.intersection(other_geometry).area, 1e-8, name + '/' + other)

    def test_hansi_uses_three_documented_blocks_without_hisar_overlap(self):
        blocks = json.loads((MAPS / 'hansi-blocks-source.geojson').read_text())['features']
        self.assertEqual({f['properties']['block_lgd'] for f in blocks}, {487, 488, 491})
        union = unary_union([shape(f['geometry']) for f in blocks])
        districts = {f['properties']['District']: shape(f['geometry'])
                     for f in json.loads((MAPS / 'HR.geojson').read_text())['features']}
        hansi, hisar = districts['HANSI'], districts['HISAR']
        self.assertLess(hansi.symmetric_difference(union).area / union.area, 1e-6)
        self.assertLess(hansi.intersection(hisar).area, 1e-12)


if __name__ == '__main__':
    unittest.main()
