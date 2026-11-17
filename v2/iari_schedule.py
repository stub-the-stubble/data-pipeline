"""Prepare an IARI update, optionally apply validated JSON to a release checkout.

No git or deployment commands. Default operation writes only a separate work
directory. The scheduled workflow explicitly applies after all checks pass.
Reviewed historical evidence is immutable; new exceptions require human review.
"""
import argparse
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import zipfile
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backfill import fingerprint_tree, require_histories, validate_published_data
from iari_fetch import ENDPOINT, download, layer
from iari_import import build, read_json, sha, source_bytes
from iari_source import (SERIES, STATES, DISTRICTS, days_through, require,
                         parse_bulletin, pdf_text)

REPOSITORY = Path(__file__).resolve().parents[1]
POLICY = REPOSITORY / 'data/iari/policy.json'
ARCHIVE = REPOSITORY / 'data/iari/reviewed-2026.zip'


class BulletinLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.links.extend(v for k, v in attrs if k == 'href' and v)


def latest_bulletin(html, policy, today):
    """Select only actual links on the official index; never guess a PDF URL."""
    require(today.year == policy['year'], 'Season/year needs a reviewed policy update')
    require(today <= date.fromisoformat(policy['season_end']) + timedelta(days=7),
            'Season closed; review the next monitoring season before restarting')
    parser = BulletinLinks()
    parser.feed(html)
    year = policy['year']
    pattern = re.compile(rf'/Creams_website_bulletin/Fire_bulletin_{year}/Fire_bulletin_{year}r/'
                         rf'\d+\.RiceResidueFireBulletin_(\d{{2}}[A-Za-z]{{3}})_{year}_ICAR\.pdf')
    links = {}
    for href in parser.links:
        url = urljoin(policy['bulletin_index'], href)
        parsed = urlparse(url)
        if parsed.scheme != 'https' or parsed.netloc != 'creams.iari.res.in':
            continue
        match = pattern.fullmatch(parsed.path)
        if not match or parsed.query or parsed.fragment:
            continue
        day = datetime.strptime(match[1] + str(year), '%d%b%Y').date()
        if policy['season_start'] <= day.isoformat() <= policy['season_end'] and day < today:
            require(day not in links or links[day] == url, 'Ambiguous bulletin links for one date')
            links[day] = url
    require(links, 'No completed-day bulletin found; published data is unchanged')
    last = max(links)
    expected = min(today - timedelta(days=1), date.fromisoformat(policy['season_end']))
    require((expected-last).days <= policy['max_lag_days'], 'IARI bulletin is overdue; publication held')
    return last.isoformat(), links[last]


def extract_reviewed(output, policy, archive=ARCHIVE):
    """Pin all historical raw evidence, including the four reviewed PDF fallbacks."""
    require(sha(Path(archive).read_bytes()) == policy['archive_sha256'], 'Reviewed evidence archive changed')
    output = Path(output)
    output.mkdir()
    with zipfile.ZipFile(archive) as source:
        names = source.namelist()
        require(len(names) == len(set(names)), 'Duplicate archive entries')
        require(all('/' not in n and '\\' not in n and n not in ('', '.', '..') for n in names), 'Unsafe archive path')
        for name in names:
            (output / name).write_bytes(source.read(name))
    bundle = read_json(output / 'bundle.json')
    require(bundle['end'] == policy['reviewed_through'], 'Reviewed evidence/policy date differs')
    return bundle


def require_iari_baseline(baseline, policy):
    """Require the separately reviewed source migration before unattended writes."""
    histories = require_histories(baseline)
    validate_published_data(baseline)
    ends = []
    for state in STATES:
        history = histories[state]
        sources = history.get('sources', {}).get('dates', {})
        season = sorted(d for d in history['total']['dates'] if policy['season_start'] <= d <= policy['season_end'])
        require(season and season[-1] >= policy['reviewed_through'], 'Apply the reviewed IARI backfill before scheduling')
        require(season == days_through(season[-1]), 'Published IARI season has gaps')
        require(all(sources.get(day) == SERIES for day in season), 'NRSC/mixed season cannot be migrated by the scheduler')
        for day in season:
            daily = read_json(Path(baseline) / state / (day + '.json'))
            require(daily.get('source', {}).get('series') == SERIES, 'Published daily source differs from history')
            require(set(daily['districts']) == set(DISTRICTS[state]), 'Published district catalog differs')
        ends.append(season[-1])
    require(len(set(ends)) == 1, 'State observation dates differ')
    return ends[0]


def session_for_iari():
    session = requests.Session()
    session.headers['User-Agent'] = 'StubTheStubble-source-validation/1.0'
    session.mount('https://', HTTPAdapter(max_retries=Retry(total=2, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504], allowed_methods=['GET'])))
    return session


def prepare(baseline, work, session=None, today=None):
    baseline, work = Path(baseline).resolve(), Path(work).resolve()
    require(not work.exists(), 'Use a new work directory')
    require(work != REPOSITORY and REPOSITORY not in work.parents and work not in REPOSITORY.parents,
            'Work directory must be outside the repository')
    require(work != baseline and baseline not in work.parents and work not in baseline.parents,
            'Work directory must be separate from the baseline')
    policy = read_json(POLICY)
    today = today or datetime.now(ZoneInfo('Asia/Kolkata')).date()
    before = fingerprint_tree(baseline)
    published_end = require_iari_baseline(baseline, policy)
    work.mkdir(parents=True)
    owned_session = session is None
    session = session or session_for_iari()
    try:
        index = download(session, policy['bulletin_index'], work, 'bulletin-index.html')
        end, url = latest_bulletin((work / index['path']).read_text(encoding='utf-8'), policy, today)
        require(end >= published_end, 'Bulletin would roll back published coverage')
        root = work / 'bundle'
        bundle = extract_reviewed(root, policy)
        summary = download(session, url, root, 'latest-summary.pdf')
        parsed = parse_bulletin(pdf_text(source_bytes(root, summary)), end)
        # Freeze the reviewed prefix. Later bulletins still have to reconcile all
        # its daily and district totals; revisions require a new reviewed seed.
        new_days = [day for day in parsed['daily'] if day not in bundle['days']]
        if new_days:
            cap = download(session, ENDPOINT, root, 'capabilities.xml',
                {'service': 'WFS', 'version': '2.0.0', 'request': 'GetCapabilities'})
            tree = ET.fromstring((root / cap['path']).read_bytes())
            names = {x.text for x in tree.findall('.//{http://www.opengis.net/wfs/2.0}FeatureType/{http://www.opengis.net/wfs/2.0}Name')}
            for day in new_days:
                if layer(day) in names:
                    bundle['days'][day] = {'wfs': download(session, ENDPOINT, root, day + '.json', {
                        'service': 'WFS', 'version': '2.0.0', 'request': 'GetFeature',
                        'typeName': layer(day), 'outputFormat': 'application/json', 'count': 10000})}
                else:
                    require(parsed['daily'][day] == {'PB': 0, 'HR': 0}, 'Missing nonzero WFS day: ' + day)
                    bundle['days'][day] = {'fallback': {'kind': 'bulletin-zero', 'rejected_wfs_sha256': None,
                        'reason': 'No published WFS layer; current bulletin explicitly reports both states zero.'}}
        bundle.update(end=end, summary=summary)
        (root / 'bundle.json').write_text(json.dumps(bundle, indent=2)+'\n', encoding='utf-8')
        candidate = work / 'candidate'
        manifest = build(root / 'bundle.json', candidate, baseline)
        # Validate the complete archive, retaining older daily files and other API
        # content. The candidate manifest itself is never copied into public docs.
        release = work / 'release'
        shutil.copytree(baseline, release)
        expected = {f'{s}/{d}.json' for s in STATES for d in parsed['daily']}
        expected.update(f'{s}/historical_data.json' for s in STATES)
        require(set(manifest['output_sha256']) == expected, 'Unexpected candidate output paths')
        for name, digest in manifest['output_sha256'].items():
            raw = (candidate / name).read_bytes()
            require(sha(raw) == digest, 'Candidate changed during staging')
            (release / name).write_bytes(raw)
        validate_published_data(release)
        require_iari_baseline(release, policy)
        require(fingerprint_tree(baseline) == before, 'Published baseline changed during preparation')
        changed = sorted(n for n in expected if before.get(n) != manifest['output_sha256'][n])
        result = {'status': 'ready' if changed else 'unchanged', 'through': end,
                  'baseline_sha256': before, 'release_sha256': fingerprint_tree(release),
                  'changed_files': changed, 'totals': manifest['totals'],
                  'state_day_checks': manifest['state_day_checks'], 'district_total_checks': manifest['district_total_checks']}
        (work / 'release.json').write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
        return result
    finally:
        if owned_session:
            session.close()


def apply(baseline, work):
    """Install a completely validated overlay; rollback local writes on failure.

The workflow commits only after this returns. A process termination cannot
publish partial files: the checkout is disposable and no commit step runs.
"""
    baseline, work = Path(baseline).resolve(), Path(work).resolve()
    result = read_json(work / 'release.json')
    require(result['status'] in ('ready', 'unchanged'), 'Release was not validated')
    require(fingerprint_tree(baseline) == result['baseline_sha256'], 'Baseline changed before apply')
    require(fingerprint_tree(work / 'release') == result['release_sha256'], 'Staged release changed before apply')
    validate_published_data(work / 'release')
    require_iari_baseline(work / 'release', read_json(POLICY))
    changed = result['changed_files']
    pattern = re.compile(r'(PB|HR)/(historical_data|\d{4}-\d{2}-\d{2})\.json')
    require(len(changed) == len(set(changed)) and all(pattern.fullmatch(n) for n in changed), 'Invalid apply allowlist')
    after = result['release_sha256']
    before = result['baseline_sha256']
    require(set(before) <= set(after), 'Release deletes baseline files')
    require(set(changed) == {n for n in after if before.get(n) != after[n]}, 'Apply allowlist differs from release changes')
    originals = {name: (baseline / name).read_bytes() if (baseline / name).exists() else None for name in changed}
    written = []
    try:
        for name in changed:
            destination = baseline / name
            # Same-directory replace keeps individual JSON files atomic to readers.
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temp:
                temp.write((work / 'release' / name).read_bytes())
                temporary = Path(temp.name)
            try:
                os.replace(temporary, destination)
                written.append(name)
            finally:
                if temporary.exists(): temporary.unlink()
        require(fingerprint_tree(baseline) == after, 'Applied tree differs from validated release')
    except BaseException:
        for name in reversed(written):
            target = baseline / name
            if originals[name] is None:
                target.unlink()
            else:
                target.write_bytes(originals[name])
        raise
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('--apply', action='store_true', help='Explicitly install validated files in the release checkout')
    args = parser.parse_args()
    result = prepare(args.baseline, args.work_dir)
    if args.apply:
        apply(args.baseline, args.work_dir)
    print(json.dumps({k: result[k] for k in ('status', 'through', 'totals', 'state_day_checks', 'district_total_checks')}, indent=2))


if __name__ == '__main__':
    main()
