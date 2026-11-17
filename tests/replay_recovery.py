"""Replay real frozen observations in temporary storage; never publish or download."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'v2'))
import backfill


def semantic_hash(value):
    # Build timestamps are expected to differ. Retain every count, coordinate,
    # assignment and historical date when comparing Windows and Linux outputs.
    if isinstance(value, dict):
        value = {k: normalize(v) for k, v in value.items() if k != 'last_update'}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def normalize(value):
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items() if k != 'last_update'}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value


def main():
    fixtures = Path(__file__).parent / 'fixtures' / 'recovery'
    expected = json.loads((fixtures / 'expected.json').read_text())
    archive = fixtures / 'inputs.zip'
    if backfill.sha(archive) != expected['input_archive_sha256']:
        raise ValueError('Frozen input archive changed')
    with tempfile.TemporaryDirectory(prefix='recovery-replay-') as directory:
        root = Path(directory)
        with zipfile.ZipFile(archive) as source:
            for name in source.namelist():
                target = (root / name).resolve()
                if root.resolve() not in target.parents:
                    raise ValueError('Unsafe fixture path')
            source.extractall(root)
        manifest = backfill.run(root / 'baseline', root / 'csv',
                                expected['start'], expected['end'], root / 'output')
        candidate = root / 'output' / 'candidate'
        checked = backfill.validate_published_data(candidate)
        if checked != 48 or set(manifest['allowlist']) != set(expected['semantic_sha256']):
            raise ValueError('Recovered date/file coverage differs')
        for name, digest in expected['semantic_sha256'].items():
            if semantic_hash(backfill.json_read(candidate / name)) != digest:
                raise ValueError(f'Recovered observations differ: {name}')
        if manifest['completed'] != expected['completed']:
            raise ValueError('State or unassigned totals differ')
        # Resume must verify the checkpoint and leave all candidate bytes unchanged.
        resumed = backfill.run(root / 'baseline', root / 'csv', expected['start'],
                               expected['end'], root / 'output', resume=True)
        if resumed['output_sha256'] != manifest['output_sha256']:
            raise ValueError('Resume changed recovered data')
        print('PASS: 24 completed days, 48 daily files, 2 histories; all semantic hashes match.')
        print('PASS: earlier history preserved, totals reconciled, resume unchanged; no publication.')


if __name__ == '__main__':
    main()
