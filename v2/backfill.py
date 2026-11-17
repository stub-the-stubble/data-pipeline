"""Offline, candidate-only backfill. No git commands, network requests or publishing."""
import argparse
import contextlib
from collections import Counter
from datetime import date, timedelta
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile

import geopandas as gpd
import pandas as pd
import get_nrsc_data as pipeline
import utils

STATES = ('PB', 'HR')


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def json_read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def save(path, obj):
    pipeline.atomic_write(Path(path), json.dumps(obj, indent=2, sort_keys=True, allow_nan=False))


def count(value):
    return type(value) is int and value >= 0


def validate_history(history):
    if not isinstance(history, dict) or not isinstance(history.get('districts'), dict):
        raise ValueError('Invalid baseline history')
    series = [history.get('total', {}).get('dates')] + [d.get('dates') for d in history['districts'].values()]
    if not isinstance(series[0], dict) or not series[0]:
        raise ValueError('Baseline history must contain existing observations')
    if 'unassigned' in history:
        series.append(history['unassigned'].get('dates'))
    for dates in series:
        if not isinstance(dates, dict):
            raise ValueError('Invalid history date series')
        for day, value in dates.items():
            if date.fromisoformat(day).isoformat() != day or not count(value):
                raise ValueError('Invalid history date/count')


def require_histories(root):
    """Fail closed rather than replace published history with a partial checkout."""
    root = Path(root)
    histories = {}
    for state in STATES:
        path = root / state / 'historical_data.json'
        if not path.is_file():
            raise ValueError(f'Missing published history: {path}; use a full, seeded baseline')
        histories[state] = json_read(path)
        validate_history(histories[state])
    return histories


def fingerprint_tree(root):
    root = Path(root)
    result = {}
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise ValueError(f'Symlink not allowed in input/output tree: {path}')
        if path.is_file():
            result[path.relative_to(root).as_posix()] = sha(path)
    return result


def dates_between(start, end, include_current_day=False):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    today = date.fromisoformat(utils.get_todays_date())
    if first > last or last > today or (last == today and not include_current_day):
        raise ValueError('Require an ordered completed-day range; current day needs --include-current-day')
    if first.isoformat() != start or last.isoformat() != end:
        raise ValueError('Use YYYY-MM-DD dates')
    return [(first + timedelta(days=i)).isoformat() for i in range((last-first).days+1)]


def validate_day(daily, history, day):
    """Reconcile observed locations, district assignments and historical totals."""
    totals = daily['districts']
    unassigned = daily['unassigned_total']
    if not count(daily['total']) or not count(unassigned) or not all(count(n) for n in totals.values()):
        raise ValueError('Invalid generated count')
    if daily['total'] != len(daily['locations']) or daily['total'] != sum(totals.values()) + unassigned:
        raise ValueError('Generated totals do not reconcile')
    actual = Counter(p['district'] for p in daily['locations'] if p['district'] is not None)
    if set(actual) - set(totals) or any(actual[name] != n for name, n in totals.items()):
        raise ValueError('Location district labels do not match district counts')
    if unassigned != sum(p['district'] is None for p in daily['locations']):
        raise ValueError('Unassigned location count differs')
    if history['total']['dates'][day] != daily['total'] or history['unassigned']['dates'][day] != unassigned:
        raise ValueError('Daily/history totals differ')
    if any(history['districts'][name]['dates'][day] != n for name, n in totals.items()):
        raise ValueError('Daily/history districts differ')


def validate_published_data(root):
    root = Path(root)
    histories = require_histories(root)
    coverage = [set(histories[state].get('unassigned', {}).get('dates', {})) for state in STATES]
    if coverage[0] != coverage[1]:
        raise ValueError('Assignment coverage differs between states')
    for state in STATES:
        for day in coverage[0]:
            validate_day(json_read(root / state / f'{day}.json'), histories[state], day)
    return len(coverage[0]) * len(STATES)


def calculate_day(day, csv_file, candidate):
    # Each day is calculated in isolation. A failed state never changes the candidate.
    with tempfile.TemporaryDirectory(prefix='stubble-day-') as directory:
        staging = Path(directory)
        for state in STATES:
            (staging / state).mkdir()
            shutil.copyfile(candidate / state / 'historical_data.json', staging / state / 'historical_data.json')
        raw = pd.read_csv(csv_file)
        frame = gpd.GeoDataFrame(raw, geometry=gpd.GeoSeries.from_wkt(raw.geometry, crs=pipeline.CRS))
        frame = pipeline.filter_data(pipeline.validate_source(frame, day))
        summary = {}
        for state in STATES:
            boundaries = pipeline.get_districts_geometry(state)
            mapped = pipeline.add_district_data(pipeline.filter_state(frame, state), boundaries)
            with contextlib.redirect_stdout(io.StringIO()):
                pipeline.write_todays_date_data(pd.DataFrame(pipeline.remove_nrsc_columns(mapped)), boundaries, state, day, staging)
            daily = json_read(staging / state / f'{day}.json')
            validate_day(daily, json_read(staging / state / 'historical_data.json'), day)
            summary[state] = {'total': daily['total'], 'unassigned': daily['unassigned_total']}
        for state in STATES:
            for name in [f'{day}.json', 'historical_data.json']:
                pipeline.atomic_write(candidate / state / name, (staging / state / name).read_text(encoding='utf-8'))
        return summary


def run(baseline, csv_dir, start, end, work_dir=None, resume=False, include_current_day=False):
    baseline, csv_dir = Path(baseline).resolve(), Path(csv_dir).resolve()
    histories = require_histories(baseline)
    days = dates_between(start, end, include_current_day)
    for state in STATES:
        for day in days:
            if day in histories[state]['total']['dates'] or (baseline / state / f'{day}.json').exists():
                raise ValueError(f'Refusing to replace existing observation: {state}/{day}')
    input_hashes = {day: sha(csv_dir / f'{day}.csv') for day in days}
    root = Path(__file__).resolve().parent
    provenance = {
        'baseline': str(baseline), 'baseline_files': fingerprint_tree(baseline),
        'csv_dir': str(csv_dir), 'source_csv_sha256': input_hashes,
        'code_sha256': {p.name: sha(p) for p in root.glob('*.py')},
        'boundaries_sha256': fingerprint_tree(Path(pipeline.GEOJSON_PATH).resolve()),
        'start': start, 'end': end, 'include_current_day': include_current_day,
    }
    if work_dir is None:
        if resume:
            raise ValueError('--resume requires --build and --work-dir')
        with tempfile.TemporaryDirectory(prefix='stubble-backfill-') as temporary:
            return build(Path(temporary), provenance, histories, days, csv_dir, False)
    work = Path(work_dir).resolve()
    # Neither output nor input may contain the other; reject traversal and destructive overlap.
    for source in [baseline, csv_dir, root.parent]:
        if work == source or work in source.parents or source in work.parents:
            raise ValueError('Work directory must be outside the repository, baseline and source trees')
    return build(work, provenance, histories, days, csv_dir, resume)


def build(work, provenance, histories, days, csv_dir, resume):
    work.mkdir(parents=True, exist_ok=True)
    lock = work / '.lock'
    # Close the exclusive-create handle before unlinking (Windows-compatible).
    with lock.open('x'):
        pass
    try:
        return _build_locked(work, provenance, histories, days, csv_dir, resume)
    finally:
        lock.unlink()


def _build_locked(work, provenance, histories, days, csv_dir, resume):
    manifest_path = work / 'manifest.json'
    if resume:
        manifest = json_read(manifest_path)
        if manifest['provenance'] != provenance:
            raise ValueError('Resume refused: baseline, source, code or boundaries changed')
        if fingerprint_tree(work / 'candidate') != manifest['output_sha256']:
            raise ValueError('Resume refused: candidate changed or interrupted checkpoint')
    else:
        if work.exists() and any(p.name != '.lock' for p in work.iterdir()):
            raise ValueError('Work directory must be empty; use --resume for a checkpoint')
        work.mkdir(parents=True, exist_ok=True)
        manifest = {'version': 1, 'status': 'building', 'provenance': provenance, 'completed': {}, 'output_sha256': {}}
        for state in STATES:
            (work / 'candidate' / state).mkdir(parents=True)
            save(work / 'candidate' / state / 'historical_data.json', histories[state])
        manifest['output_sha256'] = fingerprint_tree(work / 'candidate')
        save(manifest_path, manifest)
    for day in days:
        if day not in manifest['completed']:
            manifest['completed'][day] = calculate_day(day, csv_dir / f'{day}.csv', work / 'candidate')
            manifest['output_sha256'] = fingerprint_tree(work / 'candidate')
            save(manifest_path, manifest)
    # Recheck immutable inputs after processing to catch edits during a run.
    if fingerprint_tree(provenance['baseline']) != provenance['baseline_files']:
        raise ValueError('Baseline changed during backfill')
    if any(sha(csv_dir / f'{day}.csv') != digest for day, digest in provenance['source_csv_sha256'].items()):
        raise ValueError('Source changed during backfill')
    for state, original in histories.items():
        updated = json_read(work / 'candidate' / state / 'historical_data.json')
        for day, value in original['total']['dates'].items():
            if updated['total']['dates'].get(day) != value:
                raise ValueError('Earlier total changed')
        for district, series in original['districts'].items():
            for day, value in series['dates'].items():
                if updated['districts'][district]['dates'].get(day) != value:
                    raise ValueError('Earlier district count changed')
    expected = {f'{state}/historical_data.json' for state in STATES}
    expected.update(f'{state}/{day}.json' for state in STATES for day in days)
    if set(manifest['output_sha256']) != expected or fingerprint_tree(work / 'candidate') != manifest['output_sha256']:
        raise ValueError('Candidate contains unexpected or changed output files')
    root = Path(__file__).resolve().parent
    if {p.name: sha(p) for p in root.glob('*.py')} != provenance['code_sha256']:
        raise ValueError('Code changed during backfill')
    if fingerprint_tree(Path(pipeline.GEOJSON_PATH).resolve()) != provenance['boundaries_sha256']:
        raise ValueError('Boundaries changed during backfill')
    manifest['status'] = 'complete'
    manifest['allowlist'] = sorted(manifest['output_sha256'])
    save(manifest_path, manifest)
    return manifest

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', required=True, help='Full published docs/v2 snapshot')
    parser.add_argument('--csv-dir', required=True, help='Frozen NRSC CSV snapshots; no network downloads')
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--build', action='store_true', help='Save a candidate; never writes baseline or publishes')
    parser.add_argument('--work-dir')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--include-current-day', action='store_true')
    args = parser.parse_args()
    if args.build != bool(args.work_dir):
        parser.error('--build and --work-dir must be used together')
    manifest = run(args.baseline, args.csv_dir, args.start, args.end, args.work_dir, args.resume, args.include_current_day)
    print(json.dumps({'mode': 'candidate' if args.build else 'dry-run', 'status': manifest['status'],
                      'completed': manifest['completed'], 'allowlist': manifest['allowlist']}, indent=2))
    if args.build:
        print('Review manifest SHA256:', sha(Path(args.work_dir) / 'manifest.json'))


if __name__ == '__main__':
    main()
