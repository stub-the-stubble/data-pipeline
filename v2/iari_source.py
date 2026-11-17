"""IARI paddy event validation. Publisher classifications never use our polygons.

The parser intentionally supports the checked 2026 bulletin layout only. A new
layout/year must be reviewed rather than silently selecting the wrong columns.
"""
from collections import Counter
from datetime import date, datetime, timedelta
import io
import json
import math
from pathlib import Path
import re

DISTRICTS = json.loads(Path(__file__).with_name('iari_districts.json').read_text())
STATES = {'PB': 'Punjab', 'HR': 'Haryana'}
ALIASES = {'SAS NAGAR': 'SAS NAGAR (MOHALI)', 'GURUGRAM': 'GURGAON',
           'MAHENDRAGARH': 'MAHENDERGARH'}
SERIES = 'iari-creams-paddy-v1'
SOURCE_URL = 'https://creams.iari.res.in/geokrishi/'
EVENT_ROW = re.compile(
    r'^\s*(\d+)\s+(.+?)\s+(JPSS-1|S-NPP|AQUA|TERRA)\s+'
    r'([\d.]+)\s+([\d.]+)\s+(\d{2}:\d{2}:\d{2})\s+([DN])\s+([\d.]+)\s*$', re.M)


def require(condition, message):
    # Do not use assert for source validation: python -O must also fail closed.
    if not condition:
        raise ValueError(message)


def days_through(end):
    last = date.fromisoformat(end)
    require(last.year == 2026 and date(2026, 9, 15) <= last <= date(2026, 11, 30),
            'Only the reviewed 2026 paddy season is supported')
    start = date(2026, 9, 15)
    return [(start + timedelta(days=i)).isoformat() for i in range((last-start).days + 1)]


def pdf_text(content):
    from pypdf import PdfReader
    require(content.startswith(b'%PDF'), 'Bulletin is not a PDF')
    reader = PdfReader(io.BytesIO(content))
    require(not reader.is_encrypted, 'Encrypted bulletin')
    return '\n'.join(page.extract_text() or '' for page in reader.pages)


def issue_date(text):
    matches = re.findall(r'Issue Date:\s*(\d{1,2}-[A-Za-z]{3}-\d{4})', text)
    require(len(matches) == 1, 'Missing or ambiguous bulletin issue date')
    return datetime.strptime(matches[0], '%d-%b-%Y').date().isoformat()


def event_date(text):
    matches = re.findall(r'Events Date:\s*(\d{1,2}-[A-Za-z]{3}-\d{4})', text)
    require(len(matches) == 1, 'Missing or ambiguous bulletin observation date')
    return datetime.strptime(matches[0], '%d-%b-%Y').date().isoformat()


def parse_bulletin(text, end):
    """Extract independent daily and district totals, checking both against sums."""
    require(event_date(text) == end and issue_date(text) >= end, 'Summary bulletin observation date differs from requested end')
    days = days_through(end)
    header = re.search(r'Punjab\s+Haryana\s+Uttar Pradesh\s+Date\s+'
                       r'(?:2020\s+2021\s+2022\s+2023\s+2024\s+2025\s+2026\s*){3}', text)
    require(header is not None, 'Unrecognized daily table columns')
    block = text[header.end():]
    total = re.search(r'^\s*Total\s+((?:\d+\s+){20}\d+)\s*$', block, re.M | re.I)
    require(total is not None, 'Missing daily table total')
    daily = {}
    for m in re.finditer(r'^\s*(\d{2}-[A-Za-z]{3})\s+((?:\d+\s+){20}\d+)\s*$', block[:total.start()], re.M):
        day = datetime.strptime(m[1] + '-2026', '%d-%b-%Y').date().isoformat()
        values = list(map(int, m[2].split()))
        require(day not in daily, 'Duplicate daily table date')
        daily[day] = {'PB': values[6], 'HR': values[13]}
    require(sorted(daily) == days, 'Bulletin daily coverage is incomplete')
    sums = list(map(int, total[1].split()))
    districts = {state: {} for state in STATES}
    # District tables have seven year columns, unlike the daily table's 21.
    for m in re.finditer(r'^\s*([A-Z][A-Z ()]+?)\s+((?:\d+\s+){6}\d+)\s*$', text, re.M):
        name = m[1].strip()
        for state, names in DISTRICTS.items():
            if name in names:
                require(name not in districts[state], 'Duplicate district summary row')
                districts[state][name] = int(m[2].split()[-1])
    for state, index in [('PB', 6), ('HR', 13)]:
        require(set(districts[state]) == set(DISTRICTS[state]), 'Missing district summary rows')
        require(sum(d[state] for d in daily.values()) == sums[index] == sum(districts[state].values()),
                'Bulletin daily/district/season totals disagree')
    return {'daily': daily, 'districts': districts, 'end': end}


def canonical_district(name):
    require(isinstance(name, str) and bool(name.strip()), 'Missing district')
    name = name.strip().upper()
    return ALIASES.get(name, name)


def validate_observation(p):
    for key, lower, upper in [('lat', -90, 90), ('lon', -180, 180), ('fire_power', 0, float('inf'))]:
        value = p[key]
        require(type(value) in (float, int) and math.isfinite(value) and lower <= value <= upper,
                'Invalid observation ' + key)
    require(p['sat'] in ('S-NPP', 'JPSS-1', 'AQUA', 'TERRA'), 'Unknown satellite')
    require(p['daynight'] in ('D', 'N'), 'Unknown day/night flag')
    require(isinstance(p['acqtime'], str) and re.fullmatch(r'\d{2}:\d{2}:\d{2}', p['acqtime']), 'Invalid acquisition time')
    datetime.strptime(p['acqtime'], '%H:%M:%S')


def observation_key(p):
    return p['lat'], p['lon'], p['sat'], p['acqtime']


def validate_counts(events, expected):
    for state in STATES:
        rows = [p for p in events if p['state'] == state]
        require(len(rows) == expected[state], 'Event count differs from bulletin: ' + state)
        require(len({observation_key(p) for p in rows}) == len(rows),
                'Repeated observation within a state needs publisher review')
    require(len({p['event_id'] for p in events}) == len(events), 'Repeated source record identity')


def from_wfs(data, day, expected):
    require(data.get('type') == 'FeatureCollection', 'Not a WFS feature collection')
    features = data.get('features')
    require(isinstance(features, list), 'Missing WFS features')
    require(type(data.get('numberMatched')) is int and type(data.get('numberReturned')) is int
            and data['numberMatched'] == data['numberReturned'] == len(features), 'Incomplete WFS pagination')
    require(data.get('crs', {}).get('properties', {}).get('name') == 'urn:ogc:def:crs:EPSG::4326', 'Unexpected WFS CRS')
    ids, events = set(), []
    for feature in features:
        identifier = feature.get('id')
        require(isinstance(identifier, str) and identifier and identifier not in ids, 'Missing or duplicate WFS ID')
        ids.add(identifier)
        p, geometry = feature['properties'], feature['geometry']
        require(geometry.get('type') == 'Point' and len(geometry.get('coordinates', [])) == 2, 'Invalid point geometry')
        event = {'event_id': day + ':' + identifier, 'lat': p['latitude'], 'lon': p['longitude'],
                 'sat': p['satellite'], 'acqtime': p['acq_time'], 'daynight': p['day_flag'],
                 'fire_power': p['fire_power'], 'district_assignment': 'publisher'}
        validate_observation(event)
        require(all(type(n) in (int, float) and math.isfinite(n) for n in geometry['coordinates']), 'Invalid geometry coordinate')
        require(abs(geometry['coordinates'][0]-event['lon']) < 1e-8
                and abs(geometry['coordinates'][1]-event['lat']) < 1e-8, 'Geometry/attribute disagreement')
        require(type(p['acq_date']) in (int, float) and p['acq_date'] == int(date.fromisoformat(day).strftime('%d%m%Y')),
                'WFS acquisition date differs from requested day')
        district = canonical_district(p['district'])
        state = next((s for s in STATES if district in DISTRICTS[s]), None)
        if state:
            if p.get('field11'):
                require(p['field11'].upper() == STATES[state].upper(), 'Publisher state/district disagreement')
            event.update(state=state, district=district, publisher_district=p['district'])
            events.append(event)
        elif p.get('field11', '').upper() in ('PUNJAB', 'HARYANA'):
            raise ValueError('Unknown publisher district in target state')
    validate_counts(events, expected)
    return events


def from_event_table(text, day, expected, corrections=()):
    """Parse a checked daily bulletin; corrections must match the exact source row."""
    # A bulletin can be published the following day (21 October was issued on
    # 22 October). Event date and table headings determine the observation day.
    require(event_date(text) == day and issue_date(text) >= day, 'Fallback bulletin has wrong observation date')
    events, used = [], set()
    for state, name in STATES.items():
        heading = f'Details of paddy residue burning events in {name} on {date.fromisoformat(day).strftime("%d-%b-%Y")}'
        if expected[state] == 0 and heading not in text:
            continue
        require(text.count(heading) == 1, 'Missing or repeated event table heading')
        block = text.split(heading)[1].split('Details of paddy residue burning events in ')[0]
        rows = EVENT_ROW.findall(block)
        require([int(r[0]) for r in rows] == list(range(1, expected[state]+1)), 'Incomplete or duplicate bulletin event rows')
        require(not re.search(r'^\s*\d+\s+[A-Z].+$', EVENT_ROW.sub('', block), re.M), 'Unparsed numbered event row')
        for serial, place, sat, lat, lon, time, flag, power in rows:
            event = {'event_id': f'{day}:bulletin:{state}:{serial}', 'state': state, 'lat': float(lat), 'lon': float(lon),
                     'sat': sat, 'acqtime': time, 'daynight': flag, 'fire_power': float(power),
                     'district_assignment': 'publisher', 'publisher_place': place}
            validate_observation(event)
            choices = [n for n in DISTRICTS[state] if place.startswith(n + ' ')]
            choices += [n for alias, n in ALIASES.items() if place.startswith(alias + ' ') and n in DISTRICTS[state]]
            correction = next((c for c in corrections if c['state'] == state and c['serial'] == int(serial)), None)
            if correction:
                require(correction['place'] == place and correction['lat'] == event['lat'] and correction['lon'] == event['lon'],
                        'Reviewed correction no longer matches publisher row')
                require(correction['district'] in DISTRICTS[state] and correction.get('reason'), 'Invalid reviewed district correction')
                event['district'] = correction['district']
                event['review_note'] = correction['reason']
                used.add((state, int(serial)))
            else:
                require(bool(choices), 'Unknown bulletin district: ' + place)
                event['district'] = max(choices, key=len)
            events.append(event)
    require(len(used) == len(corrections) and used == {(c['state'], c['serial']) for c in corrections}, 'Unused or duplicate reviewed correction')
    validate_counts(events, expected)
    return events
