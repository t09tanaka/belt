#!/usr/bin/env python3
"""Exercise the real runner with fake Cargo; reject broad-suite regressions."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pure_modules
from pure_modules import ROOT, modules, selectors


def workflow_policy(text):
    # Each checked mutation shard has exactly one execution entry. Keeping this
    # narrow makes a new direct cargo invocation a deliberate policy change.
    shard = text.split("  mutants-shard:\n", 1)[1].split("  mutants-pure:\n", 1)[0]
    runs = []
    lines = shard.splitlines()
    for i, line in enumerate(lines):
        if "run:" not in line:
            continue
        body = line.split("run:", 1)[1].strip()
        if body == "|":
            following = []
            for child in lines[i + 1:]:
                if child and len(child) - len(child.lstrip()) <= 8:
                    break
                following.append(child.strip())
            body = "\n".join(following)
        runs.append(body)
    expected = 'python3 scripts/mutation-run.py --shard "${{ matrix.shard }}/4"'
    assert runs.count(expected) == 1, "mutation must use the checked runner"
    assert len(runs) == 2, "only install and checked runner belong in mutation shard"
    assert "cargo install cargo-schemata --locked --version 0.6.0 --force" in runs[0]
    assert runs[0].count("cargo schemata") == 1 and "cargo schemata --version" in runs[0], "installation must not bypass the runner"
    assert "cargo schemata " not in runs[1].replace(expected, "")
    assert "run: python3 scripts/mutation-policy-test.py" in text, "ordinary CI must check policy"
    assert "cargo nextest run --workspace --all-targets" in text, "full test gate must remain"
    assert "run: ./scripts/coverage-gate.sh" in text, "full coverage gate must remain"


class Policy(unittest.TestCase):
    def test_manifest_rejects_empty_duplicate_or_renamed_targets(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "scripts").mkdir()
            (root / "src").mkdir()
            (root / "src/a.rs").write_text("fn a() {}")
            with patch.object(pure_modules, "ROOT", root):
                for text in ("", "src/a.rs\nsrc/a.rs\n", "src/renamed.rs\n"):
                    (root / "scripts/pure-modules.txt").write_text(text)
                    with self.subTest(text=text), self.assertRaises(ValueError):
                        pure_modules.modules()

    def test_workflow_and_rejected_bypasses(self):
        text = (ROOT / ".github/workflows/ci.yml").read_text()
        workflow_policy(text)
        for replacement in ["cargo schemata -- --all-targets", "cargo schemata -- --tests", "cargo schemata"]:
            with self.subTest(replacement=replacement), self.assertRaises(AssertionError):
                workflow_policy(text.replace('python3 scripts/mutation-run.py --shard "${{ matrix.shard }}/4"', replacement))
        with self.assertRaises(AssertionError):
            workflow_policy(text.replace("# A cached binary", "cargo schemata -- --tests\n          # A cached binary"))

    def test_actual_runner_arguments_and_failures(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            cargo = directory / "cargo"
            cargo.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
a=sys.argv[1:]
with open(os.environ['CAPTURE'], 'a') as f: f.write(json.dumps(a)+'\\n')
if a==['schemata','--version']: print('cargo-schemata 0.6.0')
elif a[0]=='test':
    if os.environ.get('EMPTY')!='1':
        if '--lib' in a:
            for selector in json.loads(os.environ['SELECTORS']): print(selector+'contract: test')
        else:
            print('config::causal: test')
            if os.environ.get('ROGUE_IT')=='1': print('resident::outputs::slow: test')
else:
    o=pathlib.Path(a[a.index('-o')+1]);o.mkdir(exist_ok=True)
    if os.environ.get('NO_REPORT')!='1':
        data={'files':{m:{'mutants':[{'status':os.environ.get('STATUS','Killed')}]} for m in json.loads(os.environ['MODULES'])}}
        (o/'report.json').write_text('{}' if os.environ.get('EMPTY_REPORT')=='1' else json.dumps(data))
    sys.exit(int(os.environ.get('MUTATION_EXIT','0')))
''')
            cargo.chmod(0o755)
            env = dict(os.environ, PATH=f"{directory}:{os.environ['PATH']}", CAPTURE=str(directory / "argv"), SELECTORS=json.dumps(selectors()), MODULES=json.dumps(modules()))
            command = [sys.executable, str(ROOT / "scripts/mutation-run.py"), "--shard", "0/4", "--jobs", "2", "--output", str(directory / "out")]
            result = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            calls = [json.loads(line) for line in (directory / "argv").read_text().splitlines()]
            expected = ["schemata"]
            for module in modules():
                expected += ["--file", module]
            expected += ["--shard", "0/4", "--jobs", "2", "--timeout", "60", "-o", str(directory / "out"), "--", "--lib", "--test", "it", "--", *selectors()]
            self.assertEqual(calls[-1], expected)
            self.assertEqual(calls[0], ["test", "--lib", "--test", "it", "--", *selectors(), "--list"])
            self.assertEqual(calls[1], ["test", "--test", "it", "--", *selectors(), "--list"])
            self.assertNotEqual(subprocess.run(command, env=dict(env, EMPTY="1"), capture_output=True).returncode, 0)
            self.assertNotEqual(subprocess.run(command, env=dict(env, ROGUE_IT="1"), capture_output=True).returncode, 0)
            self.assertEqual(subprocess.run(command, env=dict(env, MUTATION_EXIT="7"), capture_output=True).returncode, 7)
            for change in ({"EMPTY_REPORT": "1"}, {"NO_REPORT": "1"}, {"STATUS": "Timeout"}, {"STATUS": "Survived"}, {"STATUS": "Pending"}, {"STATUS": "Mystery"}):
                with self.subTest(change=change):
                    # A stale successful report must not rescue a failed run.
                    stale = {"files": {m: {"mutants": [{"status": "Killed"}]} for m in modules()}}
                    (directory / "out/report.json").write_text(json.dumps(stale))
                    self.assertNotEqual(subprocess.run(command, env=dict(env, **change), capture_output=True).returncode, 0)


if __name__ == "__main__":
    unittest.main()
