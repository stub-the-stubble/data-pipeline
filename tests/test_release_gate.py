"""Exercise the actual workflow shell guard, without running any writer/deployer."""
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


class ReleaseGateTests(unittest.TestCase):
    def check_gate(self, event, mode, publish, schedule, ref='refs/heads/main'):
        workflow = (Path(__file__).parents[1] / '.github/workflows/main.yml').read_text()
        guard = textwrap.dedent(workflow.split('        run: |', 1)[1].split('      - name: Setup conda', 1)[0])
        shell = 'C:/Program Files/Git/bin/bash.exe' if os.name == 'nt' else '/bin/bash'
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'output'
            env = dict(os.environ, EVENT=event, MODE=mode, ENABLED=publish,
                       SCHEDULE_ENABLED=schedule, REF=ref, GITHUB_OUTPUT=output.as_posix())
            result = subprocess.run([shell, '-c', guard], env=env, capture_output=True, text=True)
            return result.returncode, output.read_text() if output.exists() else ''

    def test_manual_publish_while_schedule_held(self):
        code, output = self.check_gate('workflow_dispatch', 'deploy-existing', 'true', '')
        self.assertEqual(code, 0)
        self.assertIn('publish=true', output)

    def test_scheduled_ingestion_requires_both_gates(self):
        for publish, schedule in [('true', ''), ('', 'true'), ('', '')]:
            self.assertNotEqual(self.check_gate('schedule', '', publish, schedule)[0], 0)
        self.assertEqual(self.check_gate('schedule', '', 'true', 'true')[0], 0)

    def test_validation_works_with_both_gates_off(self):
        code, output = self.check_gate('workflow_dispatch', 'validate', '', '', 'refs/heads/test')
        self.assertEqual(code, 0)
        self.assertIn('publish=false', output)

    def test_manual_publish_requires_main_and_publish_gate(self):
        self.assertNotEqual(self.check_gate('workflow_dispatch', 'deploy-existing', '', '')[0], 0)
        self.assertNotEqual(self.check_gate('workflow_dispatch', 'deploy-existing', 'true', '', 'refs/heads/test')[0], 0)
