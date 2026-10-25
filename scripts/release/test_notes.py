"""Offline API/snapshot tests for conservative classification and release reruns."""

from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import notes


class FakeAPI:
    def __init__(self, paths=("README.md",), count=None):
        self.files = [{"filename": path, "status": "modified"} for path in paths]
        self.pr = {"head": {"sha": "head"}, "base": {"sha": "base"},
                   "changed_files": len(paths) if count is None else count}
        self.after = None
        self.calls = []
        self.reads = 0
        self.label = None
        self.label_exists = False
        self.failure = None

    def __call__(self, method, endpoint, body=None, allowed=()):
        self.calls.append((method, endpoint, body, allowed))
        if self.failure and self.failure(method, endpoint):
            raise RuntimeError("synthetic API failure")
        if endpoint == "/pulls/17":
            self.reads += 1
            return deepcopy(self.after if self.reads > 1 and self.after else self.pr)
        if endpoint.startswith("/pulls/17/files?"):
            page = int(endpoint.rsplit("=", 1)[1])
            return self.files[(page - 1) * 100:page * 100]
        if method == "DELETE":
            self.label = False
        elif method == "GET" and endpoint.startswith("/labels/"):
            return {"name": notes.OMISSION_LABEL} if self.label_exists else None
        elif method == "POST" and endpoint == "/labels":
            self.label_exists = True
        elif method == "POST" and endpoint == "/issues/17/labels":
            self.label = True
        else:
            raise AssertionError((method, endpoint))


class ClassificationTests(unittest.TestCase):
    def test_support_mixtures_and_missing_label_creation(self):
        api = FakeAPI(("README.md", "feature-specs/compression.md", "src/test/resources/fixture.json",
                       "src/integration-test/java/ClusterIT.java", ".github/workflows/release.yml",
                       "scripts/release/test_notes.py", "RELEASING.md"))
        self.assertTrue(notes.classify(api, 17))
        self.assertTrue(api.label)
        self.assertTrue(api.label_exists)

    def test_mixed_runtime_build_and_unknown_paths_remove_stale_label(self):
        for path in ("src/main/java/Client.java", "pom.xml", "Makefile", "scripts/release/signing.py",
                     "src/demo/java/Demo.java", "random.md", "LICENSE", "build/config.json"):
            with self.subTest(path=path):
                api = FakeAPI(("README.md", path))
                api.label = True
                self.assertFalse(notes.classify(api, 17))
                self.assertFalse(api.label)

    def test_rename_checks_both_paths_and_requires_previous_path(self):
        for previous, current, expected in (
            ("src/test/Old.java", "src/test/New.java", True),
            ("src/main/Client.java", "src/test/Client.java", False),
            ("src/test/Client.java", "src/main/Client.java", False),
            (None, "src/test/New.java", False),
        ):
            with self.subTest(previous=previous, current=current):
                api = FakeAPI((current,))
                api.files[0].update(status="renamed", previous_filename=previous)
                self.assertEqual(notes.classify(api, 17), expected)
                self.assertEqual(api.label, expected)

    def test_pagination_retrieves_all_files(self):
        api = FakeAPI(tuple(f"src/test/Fixture{i}.java" for i in range(201)))
        self.assertTrue(notes.classify(api, 17))
        self.assertEqual(sum("/files?" in call[1] for call in api.calls), 3)

    def test_production_change_on_later_page_remains_visible(self):
        api = FakeAPI(tuple(f"src/test/Fixture{i}.java" for i in range(100)) + ("pom.xml",))
        self.assertFalse(notes.classify(api, 17))
        self.assertFalse(api.label)

    def test_truncated_duplicate_and_empty_snapshots_fail_and_remove_label(self):
        for api in (FakeAPI(count=2), FakeAPI(("README.md", "README.md")), FakeAPI(()),
                    FakeAPI(tuple(f"src/test/{i}" for i in range(3000)), count=3001)):
            with self.subTest(count=api.pr["changed_files"]):
                with self.assertRaises(ValueError):
                    notes.classify(api, 17)
                self.assertFalse(api.label)

    def test_head_base_and_count_changes_fail_and_remove_label(self):
        for key in ("head", "base", "changed_files"):
            with self.subTest(key=key):
                api = FakeAPI()
                api.after = deepcopy(api.pr)
                api.after[key] = 2 if key == "changed_files" else {"sha": "changed"}
                with self.assertRaisesRegex(ValueError, "changed during"):
                    notes.classify(api, 17)
                self.assertFalse(api.label)

    def test_api_failures_attempt_to_remove_stale_label_and_propagate(self):
        for failed in ("/pulls/17", "/pulls/17/files?per_page=100&page=1", "/labels/" + notes.OMISSION_LABEL):
            api = FakeAPI()
            api.failure = lambda method, endpoint: method == "GET" and endpoint == failed
            with self.assertRaisesRegex(RuntimeError, "synthetic"):
                notes.classify(api, 17)
            self.assertFalse(api.label)

    def test_cleanup_failure_does_not_hide_original_error(self):
        api = FakeAPI()
        api.failure = lambda method, endpoint: True
        with self.assertRaisesRegex(RuntimeError, "synthetic"):
            notes.classify(api, 17)
        self.assertEqual(api.calls[-1][0], "DELETE")

    def test_unknown_status_and_missing_metadata_are_not_omitted(self):
        api = FakeAPI()
        api.files[0].pop("status")
        self.assertFalse(notes.classify(api, 17))
        api = FakeAPI()
        api.pr.pop("head")
        with self.assertRaises(KeyError):
            notes.classify(api, 17)
        self.assertFalse(api.label)


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
