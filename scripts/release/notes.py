#!/usr/bin/env python3
"""Conservatively classify PRs and publish notes after the signed Maven release."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


OMISSION_LABEL = "omit-from-release-notes"


def support_path(path):
    """Unknown paths and consumer build/package inputs remain in release notes."""
    if not isinstance(path, str) or any(part in ("", ".", "..") for part in path.split("/")):
        return False
    return bool(
        re.match(r"^(?:src/test|src/integration-test|feature-specs|\.github)/", path)
        or path in {"README.md", "CONTRIBUTING.md", "RELEASING.md", "SECURITY.md",
                    "CHANGELOG.md", ".gitignore", "checkstyle.xml", "scripts/release/SIGNING.md"}
        or re.fullmatch(r"scripts/release/(?:notes|test_[a-z_]+)\.py", path)
    )


def snapshot(pr):
    head, base, count = pr["head"]["sha"], pr["base"]["sha"], pr["changed_files"]
    if not head or not base or type(count) is not int or count < 1:
        raise ValueError("Invalid PR snapshot")
    return head, base, count


def classify(api, number):
    if type(number) is not int or number < 1:
        raise ValueError("Invalid PR number")

    def remove_label():
        api("DELETE", f"/issues/{number}/labels/{OMISSION_LABEL}", allowed=(404,))

    try:
        before = snapshot(api("GET", f"/pulls/{number}"))
        files = []
        # GitHub returns at most 3,000 files. Count/snapshot checks detect truncation.
        for page in range(1, 32):
            batch = api("GET", f"/pulls/{number}/files?per_page=100&page={page}")
            if not isinstance(batch, list):
                raise ValueError("Invalid PR file list")
            files.extend(batch)
            if len(batch) < 100:
                break
        if len(files) != before[2] or len({file["filename"] for file in files}) != len(files):
            raise ValueError("Incomplete or duplicate PR file list")
        omit = all(
            file.get("status") in {"added", "removed", "modified", "renamed", "copied", "changed", "unchanged"}
            and support_path(file["filename"])
            and (file["status"] != "renamed" or support_path(file.get("previous_filename")))
            and ("previous_filename" not in file or support_path(file["previous_filename"]))
            for file in files
        )
        if snapshot(api("GET", f"/pulls/{number}")) != before:
            raise ValueError("PR changed during classification")
        if omit:
            if api("GET", f"/labels/{OMISSION_LABEL}", allowed=(404,)) is None:
                api("POST", "/labels", {
                    "name": OMISSION_LABEL, "color": "ededed",
                    "description": "Support-only PR omitted from generated release notes",
                }, allowed=(422,))  # Another PR may create it concurrently.
            api("POST", f"/issues/{number}/labels", {"labels": [OMISSION_LABEL]})
        else:
            remove_label()
        return omit
    except Exception:
        try:
            remove_label()
        except Exception:
            print("Could not remove stale omission label; manual review required", file=sys.stderr)
        raise


def prepared_tag(path=Path("release.properties")):
    tags = [line.removeprefix("scm.tag=") for line in path.read_text().splitlines()
            if line.startswith("scm.tag=")]
    if len(tags) != 1 or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", tags[0]):
        raise ValueError("Expected one plain version tag from Maven release.properties")
    return tags[0]


def publish_release(api, tag, tag_object):
    """Never create a tag; require the exact verified annotated tag on GitHub."""
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", tag):
        raise ValueError("Invalid Maven release tag")
    encoded = quote(tag, safe="")
    remote = api("GET", f"/git/ref/tags/{encoded}")
    if remote["object"]["type"] != "tag" or remote["object"]["sha"] != tag_object:
        raise ValueError("Published tag differs from the verified prepared tag")
    existing = api("GET", f"/releases/tags/{encoded}", allowed=(404,))
    if existing and not existing["draft"]:
        return existing  # Preserve published notes and assets on reruns.
    generated = api("POST", "/releases/generate-notes", {
        "tag_name": tag, "configuration_file_path": ".github/release.yml",
    })
    if not isinstance(generated.get("body"), str):
        raise ValueError("Missing generated release notes")
    curated = (existing.get("body") or "").strip() if existing else ""
    body = (curated + "\n\n" if curated else "") + generated["body"]
    payload = {"tag_name": tag, "name": tag, "body": body,
               "draft": False, "prerelease": "-" in tag}
    if existing:
        return api("PATCH", f'/releases/{existing["id"]}', payload)
    created = api("POST", "/releases", payload, allowed=(422,))
    if created is None:
        # A concurrent run may have created this release after our lookup.
        created = api("GET", f"/releases/tags/{encoded}")
        if created["draft"]:
            raise ValueError("Concurrent release is still a draft; rerun this job")
    return created


class GitHub:
    def __init__(self):
        self.token = os.environ["GH_TOKEN"]
        repository = os.environ["GITHUB_REPOSITORY"]
        if not self.token or not re.fullmatch(r"[\w.-]+/[\w.-]+", repository):
            raise ValueError("Missing GitHub token or repository")
        self.base = os.environ.get("GITHUB_API_URL", "https://api.github.com") + "/repos/" + repository

    def __call__(self, method, endpoint, body=None, allowed=()):
        request = Request(self.base + endpoint, method=method,
                          data=json.dumps(body).encode() if body is not None else None,
                          headers={"Authorization": "Bearer " + self.token,
                                   "Accept": "application/vnd.github+json",
                                   "Content-Type": "application/json",
                                   "X-GitHub-Api-Version": "2022-11-28"})
        try:
            with urlopen(request, timeout=30) as response:
                return None if response.status == 204 else json.load(response)
        except HTTPError as error:
            if error.code in allowed:
                return None
            raise RuntimeError(f"GitHub {method} {endpoint} failed: {error.code}") from None


def capture_prepared():
    """Keep public tag identity outside Maven's workspace before perform cleans it."""
    tag = prepared_tag()
    tag_object = subprocess.check_output(["git", "rev-parse", "refs/tags/" + tag], text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", tag_object):
        raise ValueError("Invalid prepared tag object")
    with Path(os.environ["GITHUB_ENV"]).open("a") as environment:
        environment.write(f"RELEASE_NOTES_TAG={tag}\nRELEASE_NOTES_TAG_OBJECT={tag_object}\n")


def main():
    if sys.argv[1:] == ["prepare"]:
        capture_prepared()
    elif sys.argv[1:] == ["classify"]:
        classify(GitHub(), int(os.environ["PR_NUMBER"]))
    elif sys.argv[1:] == ["release"]:
        import signing
        tag = os.environ["RELEASE_NOTES_TAG"]
        signing.verify_tag(tag)
        publish_release(GitHub(), tag, os.environ["RELEASE_NOTES_TAG_OBJECT"])
    else:
        raise ValueError("Expected prepare, classify, or release")


if __name__ == "__main__":
    main()
