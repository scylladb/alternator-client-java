"""Execute release validation scripts without release credentials."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        source = (ROOT / ".github/actions/release-validate/action.yml").read_text()
        self.steps = re.split(r'^    - name: ', source, flags=re.M)[1:]

    def run_shell_step(self, name, env, cwd=None):
        step = next(step for step in self.steps if step.splitlines()[0] == name)
        script = textwrap.dedent(step.split("      run: |\n", 1)[1])
        return subprocess.run(["bash", "-c", script], cwd=cwd,
                              env={**os.environ, **env}, capture_output=True, text=True)

    def test_invalid_inputs_fail_before_build_and_approval(self):
        defaults = {"GITHUB_REF": "refs/heads/main", "RELEASE_TARGET_TAG": "main",
                    "RELEASE_VERSION": ""}
        cases = [({}, True), ({"RELEASE_TARGET_TAG": "1.2.3"}, True),
                 ({"RELEASE_VERSION": "3.0.1"}, True),
                 ({"GITHUB_REF": "refs/heads/other"}, False),
                 ({"RELEASE_TARGET_TAG": "other"}, False),
                 ({"RELEASE_TARGET_TAG": "1.2.3~1"}, False),
                 ({"RELEASE_VERSION": "v3.0.1"}, False),
                 ({"RELEASE_VERSION": "3.0.1-SNAPSHOT"}, False),
                 ({"RELEASE_VERSION": "3.0.1\n"}, False)]
        for overrides, valid in cases:
            with self.subTest(overrides=overrides):
                result = self.run_shell_step("Validate release inputs", {**defaults, **overrides})
                self.assertEqual(result.returncode == 0, valid, result.stdout + result.stderr)

    def test_main_advance_fails_before_approval(self):
        with tempfile.TemporaryDirectory() as temp:
            origin = Path(temp) / "origin.git"
            checkout = Path(temp) / "checkout"

            def git(*args, cwd=temp):
                return subprocess.run(["git", *map(str, args)], cwd=cwd, check=True,
                                      capture_output=True, text=True).stdout.strip()

            git("init", "--bare", "--initial-branch=main", origin)
            git("clone", origin, checkout)
            git("config", "user.name", "Release tests", cwd=checkout)
            git("config", "user.email", "release-tests@example.com", cwd=checkout)
            git("commit", "--allow-empty", "--no-gpg-sign", "-m", "Initial target", cwd=checkout)
            git("push", "origin", "main", cwd=checkout)
            env = {"RELEASE_TARGET_COMMIT": git("rev-parse", "HEAD", cwd=checkout)}
            result = self.run_shell_step("Recheck main before approval", env, cwd=checkout)
            self.assertEqual(result.returncode, 0, result.stderr)
            git("commit", "--allow-empty", "--no-gpg-sign", "-m", "Advance main", cwd=checkout)
            git("push", "origin", "main", cwd=checkout)
            result = self.run_shell_step("Recheck main before approval", env, cwd=checkout)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("main advanced during validation", result.stdout)

    def test_maven_checksum_failure_stops_before_extraction_or_path_update(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            curl = root / "curl"
            curl.write_text("#!/bin/bash\n"
                            "while [[ $# -gt 0 ]]; do\n"
                            "  if [[ $1 == --output ]]; then printf corrupt > \"$2\"; exit 0; fi\n"
                            "  shift\ndone\nexit 1\n")
            curl.chmod(0o700)
            tar = root / "tar"
            tar.write_text('#!/bin/bash\ntouch "$RUNNER_TEMP/extracted"\n')
            tar.chmod(0o700)
            result = subprocess.run(
                ["bash", str(ROOT / "scripts/release/setup-maven.sh")],
                env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                     "RUNNER_TEMP": str(root), "GITHUB_PATH": str(root / "github-path")},
                capture_output=True, text=True, timeout=10,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / "extracted").exists())
            self.assertFalse((root / "github-path").exists())


if __name__ == '__main__':
    unittest.main()
