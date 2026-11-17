"""Generate display boundaries and SVG paths from documented public geography.

Hansi is the union of its three LGD blocks, whose current membership is listed
by the district administration. These outlines are for display, never for
reassigning IARI's events. The legacy NRSC boundary files remain versioned intact.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

from pyproj import Transformer
from shapely.geometry import shape, mapping
from shapely.ops import transform, unary_union

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'v2'))
from iari_source import DISTRICTS, require

ALIASES = {'MALER KOTLA': 'MALERKOTLA', 'CHARKI DADRI': 'CHARKHI DADRI',
           'GURUGRAM': 'GURGAON', 'MAHENDRAGARH': 'MAHENDERGARH', 'NUH': 'MEWAT',
           'SHAHID BHAGAT SINGH NAGAR': 'SBS NAGAR', 'SRI MUKTSAR SAHIB': 'MUKTSAR',
           'SAS NAGAR (SAHIBZADA AJIT SINGH NAGAR)': 'SAS NAGAR (MOHALI)'}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(source_path, block_path, webapp):
    source_path, block_path, webapp = Path(source_path), Path(block_path), Path(webapp)
    source, blocks = json.loads(source_path.read_text()), json.loads(block_path.read_text())
    require(source['numberMatched'] == source['numberReturned'] == len(source['features']) == 45, 'Incomplete publisher boundaries')
    require(source['crs']['properties']['name'] == 'urn:ogc:def:crs:EPSG::4326', 'Unexpected boundary CRS')
    require({f['properties']['block_lgd'] for f in blocks['features']} == {487, 488, 491}, 'Wrong Hansi block membership')
    require(len(blocks['features']) == 3, 'Duplicate Hansi block')
    geometries = {s: {} for s in DISTRICTS}
    for feature in source['features']:
        p = feature['properties']
        state = {'PUNJAB': 'PB', 'HARYANA': 'HR'}[p['state_ut']]
        name = ALIASES.get(p['district'], p['district'])
        g = shape(feature['geometry'])
        require(g.is_valid and not g.is_empty, 'Invalid publisher geometry: ' + name)
        require(name not in geometries[state], 'Duplicate district boundary')
        geometries[state][name] = g
    hansi = unary_union([shape(f['geometry']) for f in blocks['features']])
    require(hansi.is_valid and hansi.geom_type in ('Polygon', 'MultiPolygon'), 'Invalid Hansi block union')
    hisar = geometries['HR']['HISAR']
    outside_fraction = hansi.difference(hisar).area / hansi.area
    require(outside_fraction < 0.000001, 'LGD blocks materially disagree with parent Hisar outline')
    # Only remove sub-metre edge slivers at the two source datasets' seam.
    hansi = hansi.intersection(hisar)
    geometries['HR']['HANSI'] = hansi
    geometries['HR']['HISAR'] = hisar.difference(hansi)
    require(unary_union([hansi, geometries['HR']['HISAR']]).symmetric_difference(hisar).area < 1e-12,
            'Hisar/Hansi split changed the state footprint')
    repository = Path(__file__).resolve().parents[1]
    output = repository / 'geojson/iari-2026'
    output.mkdir(parents=True, exist_ok=True)
    projected = Transformer.from_crs(4326, 3857, always_xy=True).transform
    maps = {}
    for state, districts in geometries.items():
        require(set(districts) == set(DISTRICTS[state]), 'Publisher/map district catalog differs')
        require(all(g.is_valid and not g.is_empty for g in districts.values()), 'Invalid derived boundary')
        for name, geometry in districts.items():
            for other, other_geometry in districts.items():
                if name < other:
                    require(geometry.intersection(other_geometry).area < 1e-8, 'Overlapping districts: ' + name + '/' + other)
        features = [{'type': 'Feature', 'properties': {'District': name, 'STATE': state,
                     'boundary_method': 'LGD block union' if name == 'HANSI' else 'IARI/SOI (Hisar split for Hansi)' if name == 'HISAR' else 'IARI/SOI'},
                     'geometry': mapping(g)} for name, g in sorted(districts.items())]
        (output / (state + '.geojson')).write_text(json.dumps({'type': 'FeatureCollection', 'features': features}, separators=(',', ':'))+'\n')
        display = {name: transform(projected, g) for name, g in districts.items()}
        minx, miny, maxx, maxy = unary_union(list(display.values())).bounds
        scale = 760/max(maxx-minx, maxy-miny)
        ox, oy = (800-(maxx-minx)*scale)/2, (800-(maxy-miny)*scale)/2
        paths = {}
        for name, g in display.items():
            # Forty metres of display simplification reduces SVG size. Counts use
            # publisher labels and the full-resolution GeoJSON remains available.
            g = g.simplify(40, preserve_topology=True)
            polygons = [g] if g.geom_type == 'Polygon' else list(g.geoms)
            rings = [ring for polygon in polygons for ring in [polygon.exterior, *polygon.interiors]]
            paths[name] = ' '.join('M ' + ' L '.join(f'{ox+(p[0]-minx)*scale:.2f} {oy+(maxy-p[1])*scale:.2f}' for p in ring.coords) + ' Z' for ring in rings)
        maps[state] = {'bbox': '0 0 800 800', 'paths': paths}
    target = webapp / 'src/lib/data/map_paths.js'
    require(target.is_file(), 'Expected frontend map module is missing')
    target.write_text('// Generated by data-pipeline/tools/build_iari_maps.py. Display outlines only.\n'
                      'export const map_paths = ' + json.dumps(maps, indent=2) + ';\n', encoding='utf-8')
    metadata = {'publisher_layer': 'geokrishi_v:district_boundary_ll_soi',
                'publisher_url': 'https://creams.iari.res.in/geokrishi/geoserver/wfs',
                'publisher_snapshot_sha256': digest(source_path), 'lgd_blocks_sha256': digest(block_path),
                'lgd_release_url': 'https://github.com/yashveeeeeeer/india-geodata/releases/tag/admin/blocks',
                'hansi_membership_url': 'https://hansi.haryana.gov.in/subdivision-blocks/',
                'hansi_method': 'Union LGD blocks 487 (Hansi-I), 488 (Hansi-II), 491 (Narnaund); intersect with publisher Hisar and subtract from Hisar.',
                'edge_clipped_area_fraction': outside_fraction, 'display_simplification_metres': 40,
                'count_assignment': 'Publisher classification; polygons do not determine counts',
                'outputs': {state: digest(output / (state+'.geojson')) for state in DISTRICTS},
                'svg_sha256': digest(target)}
    (output / 'provenance.json').write_text(json.dumps(metadata, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({'districts': {s: len(d) for s, d in geometries.items()}, 'hansi_edge_clip_fraction': outside_fraction}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publisher-boundaries', required=True)
    parser.add_argument('--hansi-blocks', required=True)
    parser.add_argument('--webapp', required=True)
    args = parser.parse_args()
    build(args.publisher_boundaries, args.hansi_blocks, args.webapp)
