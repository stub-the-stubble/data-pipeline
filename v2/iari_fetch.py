"""Download a frozen IARI source bundle for local review; never ingest or publish.

Pass the publisher's summary PDF URL explicitly. Exceptional event-table PDFs
are supplied in a reviewed fallbacks JSON; the importer checks their issue dates,
hashes, event rows and counts independently. Unreviewed failures remain blocked.
"""
import argparse
from datetime import datetime, timezone, date
import json
from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from iari_import import sha
from iari_source import require, pdf_text, parse_bulletin, from_wfs

ENDPOINT = 'https://creams.iari.res.in/geokrishi/geoserver/wfs'


def layer(day):
    d = date.fromisoformat(day)
    return f'geokrishi_v:{d.day}_{d.strftime("%B").lower()}_{d.year}_rice'


def download(session, url, root, name, params=None):
    parsed = urlparse(url)
    require(parsed.scheme == 'https' and parsed.netloc == 'creams.iari.res.in', 'Only IARI public sources are accepted')
    response = session.get(url, params=params, timeout=(10, 60))
    response.raise_for_status()
    require(urlparse(response.url).netloc == 'creams.iari.res.in', 'Unexpected download redirect')
    (root / name).write_bytes(response.content)
    return {'path': name, 'url': response.url, 'sha256': sha(response.content),
            'fetched_at': datetime.now(timezone.utc).isoformat()}


def fetch(summary_url, end, output, reviewed=None):
    root = Path(output).resolve()
    require(not root.exists(), 'Use a new bundle directory')
    repository = Path(__file__).resolve().parents[1]
    require(repository not in root.parents and root != repository, 'Store source downloads outside repository')
    root.mkdir(parents=True)
    reviewed = reviewed or {}
    failures = {}
    with requests.Session() as session:
        session.headers['User-Agent'] = 'StubTheStubble-source-validation/1.0'
        session.mount('https://', HTTPAdapter(max_retries=Retry(total=2, backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504], allowed_methods=['GET'])))
        descriptor = download(session, summary_url, root, 'summary.pdf')
        summary = parse_bulletin(pdf_text((root / 'summary.pdf').read_bytes()), end)
        capability = download(session, ENDPOINT, root, 'capabilities.xml',
                              {'service': 'WFS', 'version': '2.0.0', 'request': 'GetCapabilities'})
        tree = ET.fromstring((root / capability['path']).read_bytes())
        names = {x.text for x in tree.findall('.//{http://www.opengis.net/wfs/2.0}FeatureType/{http://www.opengis.net/wfs/2.0}Name')}
        bundle = {'version': 1, 'end': end, 'summary': descriptor, 'capabilities': capability, 'days': {}}
        require(set(reviewed) <= set(summary['daily']), 'Review refers to a day outside this bulletin')
        for day, expected in summary['daily'].items():
            entry = {}
            if layer(day) in names:
                entry['wfs'] = download(session, ENDPOINT, root, day + '.json', {
                    'service': 'WFS', 'version': '2.0.0', 'request': 'GetFeature',
                    'typeName': layer(day), 'outputFormat': 'application/json', 'count': 10000})
                try:
                    from_wfs(json.loads((root / (day + '.json')).read_bytes()), day, expected)
                except (ValueError, KeyError, TypeError) as error:
                    failures[day] = str(error)
            elif expected == {'PB': 0, 'HR': 0}:
                entry['fallback'] = {'kind': 'bulletin-zero', 'rejected_wfs_sha256': None,
                                     'reason': 'No published layer; independently published daily table explicitly reports both states zero.'}
            else:
                failures[day] = 'No published layer for nonzero bulletin day'
            if day in reviewed:
                rule = dict(reviewed[day])
                require(rule.get('rejected_wfs_sha256') == entry.get('wfs', {}).get('sha256'), 'Reviewed WFS snapshot changed')
                if rule['kind'] == 'bulletin-events':
                    rule['bulletin'] = download(session, rule.pop('bulletin_url'), root, day + '.pdf')
                entry['fallback'] = rule
            bundle['days'][day] = entry
        (root / 'bundle.json').write_text(json.dumps(bundle, indent=2)+'\n', encoding='utf-8')
        (root / 'validation-findings.json').write_text(json.dumps(failures, indent=2)+'\n', encoding='utf-8')
    return bundle


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--summary-url', required=True)
    parser.add_argument('--through', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--reviewed-fallbacks')
    args = parser.parse_args()
    reviewed = json.loads(Path(args.reviewed_fallbacks).read_text()) if args.reviewed_fallbacks else None
    fetch(args.summary_url, args.through, args.output, reviewed)
    print('Downloaded evidence only. Review validation-findings.json before running iari_import.py.')
