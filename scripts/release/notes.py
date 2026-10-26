#!/usr/bin/env python3
"""Publish GitHub notes after the signed Maven release."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


def checked_tag(tag):
    """Maven owns the version spelling; Git owns valid tag ref syntax."""
    subprocess.run(["git", "check-ref-format", "refs/tags/" + tag],
                   check=True, capture_output=True)
    return tag


def prepared_tag(path=Path("release.properties")):
    tags = [line.removeprefix("scm.tag=") for line in path.read_text().splitlines()
            if line.startswith("scm.tag=")]
    if len(tags) != 1:
        raise ValueError("Expected one plain version tag from Maven release.properties")
    return checked_tag(tags[0])


def find_release(api, tag):
    """Published tag lookup omits drafts; list releases to find curated drafts."""
    existing = api("GET", f"/releases/tags/{quote(tag, safe='')}", allowed=(404,))
    if existing:
        return existing
    page = 1
    while True:
        releases = api("GET", f"/releases?per_page=100&page={page}")
        if not isinstance(releases, list):
            raise ValueError("Missing release listing")
        for release in releases:
            if release["tag_name"] == tag:
                return release
        if len(releases) < 100:
            return None
        page += 1


def publish_release(api, tag, tag_object):
    """Never create a tag; require the exact verified annotated tag on GitHub."""
    checked_tag(tag)
    encoded = quote(tag, safe="")
    remote = api("GET", f"/git/ref/tags/{encoded}")
    if remote["object"]["type"] != "tag" or remote["object"]["sha"] != tag_object:
        raise ValueError("Published tag differs from the verified prepared tag")
    existing = find_release(api, tag)
    if existing and not existing["draft"]:
        return existing  # Preserve published notes and assets on reruns.
    generated = api("POST", "/releases/generate-notes", {
        "tag_name": tag, "configuration_file_path": ".github/release.yml",
    })
    if not isinstance(generated.get("body"), str):
        raise ValueError("Missing generated release notes")
    curated = (existing.get("body") or "") if existing else ""
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
    elif sys.argv[1:] == ["release"]:
        import signing
        tag = os.environ["RELEASE_NOTES_TAG"]
        signing.verify_tag(tag)
        publish_release(GitHub(), tag, os.environ["RELEASE_NOTES_TAG_OBJECT"])
    else:
        raise ValueError("Expected prepare or release")


if __name__ == "__main__":
    main()
