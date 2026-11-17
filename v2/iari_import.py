"""Build a validated IARI season candidate offline. No git, deployment or writes to docs.

A bundle freezes public WFS/PDF bytes and their URLs/hashes. The summary PDF is
the independent count authority. Known publisher defects require an explicit,
hash-bound fallback entry; a missing URL can never silently produce a zero.
"""
import argparse
from collections import Counter, defaultdict
import copy
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import tempfile
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from iari_source import (DISTRICTS, STATES, SERIES, SOURCE_URL, ALIASES, require,
                         parse_bulletin, pdf_text, from_wfs, from_event_table, observation_key)


def sha(content):
    return hashlib.sha256(content).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def source_bytes(root, descriptor):
    source_path = root / descriptor['path']
    path = source_path.resolve()
    require(root in path.parents and path.is_file() and not source_path.is_symlink(), 'Source must be a file inside bundle')
    parsed = urlparse(descriptor['url'])
    require(parsed.scheme == 'https' and parsed.netloc == 'creams.iari.res.in', 'Unexpected source publisher')
    require(parsed.path.startswith(('/geokrishi/geoserver/wfs', '/Creams_website_bulletin/')), 'Unexpected source path')
    raw = path.read_bytes()
    require(sha(raw) == descriptor['sha256'], 'Source bytes changed: ' + descriptor['path'])
    return raw


def public_source(descriptor):
    # Local paths and machine usernames are never written into API data.
    return {k: descriptor[k] for k in ('url', 'sha256')}


def empty_history():
    return {'total': {'dates': {}}, 'districts': {}, 'unassigned': {'dates': {}}, 'sources': {'dates': {}}}


def seed_history(original, state, days):
    """Replace the complete reviewed season, preserving every other observation."""
    h = copy.deepcopy(original)
    require(isinstance(h.get('total', {}).get('dates'), dict) and isinstance(h.get('districts'), dict), 'Invalid baseline history')
    h.setdefault('unassigned', {'dates': {}})
    h.setdefault('sources', {'dates': {}})
    # Preserve provenance of older data instead of labelling an entire mixed history IARI.
    for day in h['total']['dates']:
        h['sources']['dates'].setdefault(day, 'nrsc-legacy')
    for alias, canonical in ALIASES.items():
        if alias not in h['districts']:
            continue
        old = h['districts'].pop(alias)['dates']
        target = h['districts'].setdefault(canonical, {'dates': {}})['dates']
        require(all(d not in target or target[d] == n for d, n in old.items()), 'Conflicting district alias history')
        target.update(old)
    for series in [h['total'], h['unassigned'], h['sources'], *h['districts'].values()]:
        for day in days:
            series['dates'].pop(day, None)
    for district in DISTRICTS[state]:
        h['districts'].setdefault(district, {'dates': {}})
    return h


def resolve_day(root, day, entry, expected, summary_descriptor):
    """Do not fall back on arbitrary network/parse errors without a reviewed source."""
    fallback = entry.get('fallback')
    wfs_error = None
    events = None
    if entry.get('wfs'):
        raw = source_bytes(root, entry['wfs'])  # Hash failures are never bypassed.
        try:
            events = from_wfs(json.loads(raw), day, expected)
        except (KeyError, TypeError, ValueError) as error:
            wfs_error = str(error)
    if fallback:
        require(isinstance(fallback.get('reason'), str) and fallback['reason'].strip(), 'Fallback needs review reason')
        require(fallback.get('rejected_wfs_sha256') == (entry.get('wfs') or {}).get('sha256'), 'Fallback is not bound to WFS snapshot')
        if fallback['kind'] == 'bulletin-zero':
            require(expected == {'PB': 0, 'HR': 0}, 'Cannot infer zero for a nonzero bulletin day')
            require(not events, 'Zero conflicts with WFS events')
            events, descriptor = [], summary_descriptor
        else:
            require(fallback['kind'] == 'bulletin-events', 'Unknown fallback kind')
            descriptor = fallback['bulletin']
            events = from_event_table(pdf_text(source_bytes(root, descriptor)), day, expected, fallback.get('corrections', []))
        provenance = {'method': fallback['kind'], 'source': public_source(descriptor), 'review_reason': fallback['reason']}
        if entry.get('wfs'):
            provenance['rejected_source'] = public_source(entry['wfs'])
            provenance['validation_error'] = wfs_error
        return events, provenance
    require(events is not None, 'No validated source for ' + day + ': ' + (wfs_error or 'missing layer'))
    return events, {'method': 'wfs-events', 'source': public_source(entry['wfs'])}


def validate_output(daily, history, day):
    counts = Counter(p['district'] for p in daily['locations'])
    require(len(daily['locations']) == daily['total'] == sum(daily['districts'].values()), 'Output totals do not reconcile')
    require(all(counts[n] == count for n, count in daily['districts'].items()), 'Output point/district mismatch')
    require(history['total']['dates'][day] == daily['total'], 'Output daily/history mismatch')
    require(all(history['districts'][n]['dates'][day] == c for n, c in daily['districts'].items()), 'Output district/history mismatch')


def build(bundle_path, output, baseline=None):
    bundle_path, output = Path(bundle_path).resolve(), Path(output).resolve()
    root, repository = bundle_path.parent, Path(__file__).resolve().parents[1]
    require(not output.exists(), 'Output must be a new directory; existing candidates are immutable')
    require(output != repository and repository not in output.parents, 'Candidate output must be outside repository')
    require(output != root and root not in output.parents and output not in root.parents, 'Output must be separate from source bundle')
    if baseline:
        baseline = Path(baseline).resolve()
        require(output != baseline and baseline not in output.parents and output not in baseline.parents, 'Output must be separate from baseline')
    bundle_raw = bundle_path.read_bytes()
    bundle = json.loads(bundle_raw)
    require(bundle.get('version') == 1, 'Unsupported source bundle')
    require(date.fromisoformat(bundle['end']) < datetime.now(ZoneInfo('Asia/Kolkata')).date(),
            'Only completed observation days can be imported')
    summary = parse_bulletin(pdf_text(source_bytes(root, bundle['summary'])), bundle['end'])
    days = sorted(summary['daily'])
    require(set(bundle['days']) == set(days), 'Bundle must cover the complete reviewed season')
    baseline_bytes = {s: (baseline / s / 'historical_data.json').read_bytes() for s in STATES} if baseline else {}
    histories = {s: seed_history(json.loads(baseline_bytes[s]) if baseline else empty_history(), s, days) for s in STATES}
    prepared, provenance, all_events = {}, {}, []
    district_sums = {s: Counter() for s in STATES}
    for day in days:
        events, source = resolve_day(root, day, bundle['days'][day], summary['daily'][day], bundle['summary'])
        # Cross-state duplicates are retained as official state records and disclosed.
        observations = defaultdict(list)
        for event in events:
            observations[observation_key(event)].append(event)
        overlaps = 0
        for rows in observations.values():
            if len(rows) > 1:
                overlaps += 1
                for row in rows:
                    row['shared_observation_states'] = sorted({p['state'] for p in rows})
        provenance[day] = {**source, 'overlapping_observations': overlaps}
        for state in STATES:
            rows = [p for p in events if p['state'] == state]
            counts = Counter(p['district'] for p in rows)
            district_sums[state].update(counts)
            h = histories[state]
            h['total']['dates'][day], h['unassigned']['dates'][day] = len(rows), 0
            h['sources']['dates'][day] = SERIES
            for name in DISTRICTS[state]:
                h['districts'][name]['dates'][day] = counts[name]
            daily = {'last_update': bundle['end'] + ' (IARI bulletin)',
                     'total': len(rows), 'districts': {n: counts[n] for n in DISTRICTS[state]},
                     'unassigned_total': 0, 'locations': rows,
                     'source': {'series': SERIES, 'label': 'IARI-reported paddy-residue fire events', 'url': SOURCE_URL,
                                **source, 'bulletin_check': public_source(bundle['summary'])},
                     'shared_observation_total': sum('shared_observation_states' in r for r in rows)}
            validate_output(daily, h, day)
            prepared[f'{state}/{day}.json'] = daily
        all_events.extend(events)
    for state in STATES:
        require(all(district_sums[state][n] == c for n, c in summary['districts'][state].items()), 'Season district reconciliation failed')
        h = histories[state]
        h['last_update'] = bundle['end'] + ' (IARI bulletin)'
        h['source_catalog'] = {**h.get('source_catalog', {}), SERIES: {
            'label': 'IARI-reported paddy-residue fire events', 'url': SOURCE_URL,
            'count_definition': 'Publisher-assigned state records; not unique physical fires',
            'coverage': [days[0], days[-1]], 'bulletin_check': public_source(bundle['summary'])}}
        for series in [h['total'], h['unassigned'], h['sources'], *h['districts'].values()]:
            series['dates'] = dict(sorted(series['dates'].items()))
        prepared[f'{state}/historical_data.json'] = h
    # Re-read all immutable sources after preparation. No partial candidate on failure.
    require(bundle_path.read_bytes() == bundle_raw, 'Bundle changed during import')
    descriptors = [bundle['summary']]
    for entry in bundle['days'].values():
        if entry.get('wfs'): descriptors.append(entry['wfs'])
        if entry.get('fallback', {}).get('bulletin'): descriptors.append(entry['fallback']['bulletin'])
    for descriptor in descriptors: source_bytes(root, descriptor)
    for state, raw in baseline_bytes.items():
        require((baseline / state / 'historical_data.json').read_bytes() == raw, 'Baseline changed during import')
    manifest = {'version': 1, 'status': 'validated-local-candidate', 'series': SERIES,
                'bundle_sha256': sha(bundle_raw), 'coverage': [days[0], days[-1]],
                'state_day_checks': len(days)*2, 'district_total_checks': sum(map(len, DISTRICTS.values())),
                'totals': {s: sum(district_sums[s].values()) for s in STATES}, 'daily_sources': provenance,
                'overlapping_observations': sum(p['overlapping_observations'] for p in provenance.values()),
                'implementation_sha256': {p.name: sha(p.read_bytes()) for p in Path(__file__).parent.glob('iari*') if p.is_file()},
                'baseline_sha256': {s: sha(raw) for s, raw in baseline_bytes.items()}, 'output_sha256': {}}
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='iari-stage-', dir=output.parent) as temp:
        staging = Path(temp) / 'candidate'
        staging.mkdir()
        for name, content in prepared.items():
            path = staging / name
            path.parent.mkdir(exist_ok=True)
            raw = (json.dumps(content, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()
            path.write_bytes(raw)
            manifest['output_sha256'][name] = sha(raw)
        (staging / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n', encoding='utf-8')
        # Rename a fully validated directory on the same filesystem; never overwrite.
        require(not output.exists(), 'Output appeared during import')
        os.rename(staging, output)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--baseline', help='Optional published v2 root; preserves history outside the complete season')
    args = parser.parse_args()
    result = build(args.bundle, args.output, args.baseline)
    print(json.dumps({k: result[k] for k in ('status', 'coverage', 'totals', 'state_day_checks', 'district_total_checks', 'overlapping_observations')}, indent=2))


if __name__ == '__main__':
    main()
