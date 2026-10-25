"""Offline tests for signed release-note publication and reruns."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import notes


class ReleaseTests(unittest.TestCase):
    def api(self, existing=None, remote_sha="signed-tag", remote_type="tag"):
        def request(method, endpoint, body=None, allowed=()):
            if endpoint.startswith("/git/ref/tags/"):
                return {"object": {"type": remote_type, "sha": remote_sha}}
            if endpoint.startswith("/releases/tags/"):
                return existing
            if endpoint == "/releases/generate-notes":
                self.assertEqual(body["configuration_file_path"], ".github/release.yml")
                return {"body": "Filtered generated notes"}
            if endpoint == "/releases" or endpoint.startswith("/releases/42"):
                return body
            raise AssertionError(endpoint)
        return Mock(side_effect=request)

    def test_creates_plain_version_release_with_filtered_notes(self):
        api = self.api()
        created = notes.publish_release(api, "2.0.7", "signed-tag")
        self.assertEqual(created["tag_name"], "2.0.7")
        self.assertEqual(created["body"], "Filtered generated notes")
        self.assertFalse(created["draft"])
        self.assertFalse(created["prerelease"])

    def test_prerelease_is_marked(self):
        self.assertTrue(notes.publish_release(self.api(), "2.1.0-rc.1", "signed-tag")["prerelease"])

    def test_existing_published_release_is_preserved_without_writes(self):
        existing = {"id": 42, "draft": False, "body": "Maintainer notes", "assets": ["jar"]}
        api = self.api(existing)
        self.assertEqual(notes.publish_release(api, "2.0.7", "signed-tag"), existing)
        self.assertTrue(all(call.args[0] == "GET" for call in api.call_args_list))

    def test_existing_draft_keeps_curated_notes_and_is_published(self):
        api = self.api({"id": 42, "draft": True, "body": "Maintainer notes"})
        published = notes.publish_release(api, "2.0.7", "signed-tag")
        self.assertEqual(published["body"], "Maintainer notes\n\nFiltered generated notes")
        self.assertFalse(published["draft"])
        self.assertEqual(api.call_args.args[:2], ("PATCH", "/releases/42"))

    def test_unpublished_different_and_lightweight_tags_block_release(self):
        for api in (self.api(remote_sha="different"), self.api(remote_type="commit")):
            with self.assertRaisesRegex(ValueError, "differs"):
                notes.publish_release(api, "2.0.7", "signed-tag")
            self.assertEqual(api.call_count, 1)

    def test_api_failure_does_not_create_release(self):
        api = Mock(side_effect=RuntimeError("tag not found"))
        with self.assertRaisesRegex(RuntimeError, "tag not found"):
            notes.publish_release(api, "2.0.7", "signed-tag")
        self.assertEqual(api.call_count, 1)

    def test_concurrent_create_is_rerun_safe(self):
        existing = {"id": 42, "draft": False}
        api = Mock(side_effect=[{"object": {"type": "tag", "sha": "signed-tag"}},
                                None, {"body": "Generated"}, None, existing])
        self.assertEqual(notes.publish_release(api, "2.0.7", "signed-tag"), existing)

    def test_prepared_tag_is_authoritative_and_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "release.properties"
            path.write_text("# Maven release\nscm.tag=2.0.7\n")
            self.assertEqual(notes.prepared_tag(path), "2.0.7")
            for text in ("", "scm.tag=v2.0.7\n", "scm.tag=main\n",
                         "scm.tag=2.0.7\nscm.tag=2.0.8\n"):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    notes.prepared_tag(path)

    def test_public_tag_identity_survives_maven_descriptor_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            environment = Path(directory) / "environment"
            with patch.dict(notes.os.environ, {"GITHUB_ENV": str(environment)}), \
                    patch.object(notes, "prepared_tag", return_value="2.0.7"), \
                    patch.object(notes.subprocess, "check_output", return_value="a" * 40 + "\n"):
                notes.capture_prepared()
            self.assertEqual(environment.read_text(),
                             "RELEASE_NOTES_TAG=2.0.7\nRELEASE_NOTES_TAG_OBJECT=" + "a" * 40 + "\n")

    def test_workflow_runs_after_publication_and_push_but_not_dry_runs(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/release.yml").read_text()
        release = workflow.index("- name: Create GitHub release")
        self.assertGreater(release, workflow.index("- name: Perform release\n"))
        self.assertGreater(release, workflow.index("git push origin --follow-tags"))
        step = workflow[release:workflow.index("- name: Upload sanitized stdout", release)]
        self.assertIn("if: inputs.dry-run == false", step)
        self.assertIn('"$RUNNER_TEMP/release-safety/notes.py" release', step)
        self.assertIn("cp scripts/release/*.py", workflow)
        self.assertLess(workflow.index('notes.py" prepare'), workflow.index("- name: Perform release\n"))


if __name__ == "__main__":
    unittest.main()
