"""Offline scheduled-run replay using real evidence plus an explicit synthetic day.

All writes, including --apply equivalent checks, stay in temporary directories.
No requests, git operations, credentials or deployments are used.
"""
import copy
from datetime import date, datetime
import json
from pathlib import Path
import re
import sys
import tempfile
from unittest.mock import patch
import zipfile

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'v2'))
import iari_import
import iari_schedule as schedule
from iari_source import require

FIXTURES = Path(__file__).parent / 'fixtures'
SYNTHETIC_PDF = b'%PDF synthetic scheduler integration fixture, not a publisher document'


class ReplayClock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 17, 12, tzinfo=tz)


def next_day_text():
    text = (FIXTURES/'iari/summary-tables.txt').read_text(encoding='utf-8')
    text = text.replace('Date: 15-Nov-2026', 'Date: 16-Nov-2026')
    # One additional synthetic Punjab observation in Amritsar. Retain every
    # historical row and increase the independent district and season totals.
    def district(match):
        values = match[2].split(); values[-1] = str(int(values[-1])+1)
        return match[1] + ' ' + ' '.join(values)
    text, n = re.subn(r'^(AMRITSAR)\s+((?:\d+\s+){6}\d+)\s*$', district, text, count=1, flags=re.M)
    require(n == 1, 'Synthetic district fixture changed')
    def total(match):
        values = match[1].split(); values[6] = str(int(values[6])+1)
        new = ['0']*21; new[6] = '1'
        return '16-Nov ' + ' '.join(new) + '\nTotal ' + ' '.join(values) + '\n'
    text, n = re.subn(r'^\s*Total\s+((?:\d+\s+){20}\d+)\s*$', total, text, count=1, flags=re.M)
    require(n == 1, 'Synthetic daily total fixture changed')
    return text


class FrozenSession:
    def __init__(self, policy, summary, url, extended=False, missing=False, bad_date=False):
        self.policy, self.summary, self.url = policy, summary, url
        self.extended, self.missing, self.bad_date = extended, missing, bad_date
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        response = requests.Response(); response.status_code = 200
        response.url = requests.Request('GET', url, params=params).prepare().url
        if url == self.policy['bulletin_index']:
            body = f'<a href="{self.url}">Reviewed bulletin</a>'.encode()
        elif url == self.url:
            body = SYNTHETIC_PDF if self.extended else self.summary
        elif url == schedule.ENDPOINT and params['request'] == 'GetCapabilities':
            entry = '' if self.missing else '<FeatureType><Name>geokrishi_v:16_november_2026_rice</Name></FeatureType>'
            body = ('<WFS_Capabilities xmlns="http://www.opengis.net/wfs/2.0">'+entry+'</WFS_Capabilities>').encode()
        elif url == schedule.ENDPOINT and params['request'] == 'GetFeature':
            data = json.loads((FIXTURES/'iari/wfs-excerpt.json').read_text())
            data['features'][0]['id'] = '16_november_2026_rice.1'
            data['features'][0]['properties']['acq_date'] = 15112026 if self.bad_date else 16112026
            body = json.dumps(data).encode()
        else:
            raise AssertionError('Unexpected request: ' + response.url)
        response._content = body
        return response


def main():
    policy = schedule.read_json(schedule.POLICY)
    expected = schedule.read_json(schedule.REPOSITORY/'data/iari/expected-20261115.json')
    with tempfile.TemporaryDirectory(prefix='iari-scheduled-replay-') as folder:
        root = Path(folder)
        bundle = schedule.extract_reviewed(root/'seed', policy)
        # Reuse the exact existing published baseline frozen for NRSC regression.
        recovery = FIXTURES/'recovery'
        require(schedule.sha((recovery/'inputs.zip').read_bytes()) == schedule.read_json(recovery/'expected.json')['input_archive_sha256'],
                'Published baseline archive changed')
        with zipfile.ZipFile(recovery/'inputs.zip') as archive:
            for state in schedule.STATES:
                target = root/'old'/state/'historical_data.json'
                target.parent.mkdir(parents=True)
                target.write_bytes(archive.read(f'baseline/{state}/historical_data.json'))
        baseline = root/'baseline'
        manifest = iari_import.build(root/'seed/bundle.json', baseline, root/'old')
        require(manifest['output_sha256'] == expected['output_sha256'], 'Frozen IARI candidate differs from reviewed output')
        # This represents older API content: it must survive every scheduled run.
        (baseline/'legacy-sentinel.txt').write_text('preserve older content\n')
        original = schedule.fingerprint_tree(baseline)
        summary = (root/'seed'/bundle['summary']['path']).read_bytes()
        same = FrozenSession(policy, summary, bundle['summary']['url'])
        result = schedule.prepare(baseline, root/'same', same, date(2026,11,16))
        require(result['status'] == 'unchanged', 'Unchanged bulletin creates spurious updates')
        schedule.apply(baseline, root/'same')
        require(schedule.fingerprint_tree(baseline) == original, 'No-op apply changed baseline')
        require(len(same.calls) == 2, 'Frozen prefix should not redownload every historical WFS layer')

        text = next_day_text()
        real_pdf_text = iari_import.pdf_text
        def extract(raw):
            return text if raw == SYNTHETIC_PDF else real_pdf_text(raw)
        next_url = bundle['summary']['url'].replace('62.Rice', '63.Rice').replace('15Nov', '16Nov')
        with patch.object(schedule, 'pdf_text', side_effect=extract), \
             patch.object(iari_import, 'pdf_text', side_effect=extract), \
             patch.object(iari_import, 'datetime', ReplayClock):
            session = FrozenSession(policy, summary, next_url, extended=True)
            result = schedule.prepare(baseline, root/'next', session, date(2026,11,17))
            require(result['status'] == 'ready' and result['totals'] == {'PB':5973, 'HR':242}, 'New-day counts failed')
            require(result['state_day_checks'] == 126 and result['district_total_checks'] == 46, 'Full season checks not run')
            require(schedule.fingerprint_tree(baseline) == original, 'Preparation mutated baseline')
            schedule.apply(baseline, root/'next')
            applied = schedule.fingerprint_tree(baseline)
            require(applied == result['release_sha256'], 'Applied files differ from staged candidate')
            require((baseline/'legacy-sentinel.txt').read_text() == 'preserve older content\n', 'Older content lost')
            result = schedule.prepare(baseline, root/'repeat', session, date(2026,11,17))
            require(result['status'] == 'unchanged', 'Retry was not idempotent')
            for name, options in [('missing', {'missing':True}), ('bad-date', {'bad_date':True})]:
                invalid = FrozenSession(policy, summary, next_url, extended=True, **options)
                try:
                    schedule.prepare(baseline, root/name, invalid, date(2026,11,17))
                except ValueError:
                    pass
                else:
                    raise AssertionError('Invalid source accepted: '+name)
                require(not (root/name/'release.json').exists(), 'Invalid source became publishable')
                require(schedule.fingerprint_tree(baseline) == applied, 'Invalid source changed baseline')
        print('PASS: real 62-day replay (126 exact output hashes, 5972/242); unchanged bulletin; '
              'synthetic nonzero next day; complete season reconciliation; apply; idempotent retry; '
              'missing WFS and wrong-date failures preserve baseline. No network or publication.')


if __name__ == '__main__':
    main()
