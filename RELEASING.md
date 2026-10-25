# GitHub release notes

The Release workflow creates a GitHub release after Maven publication and the
signed tag push succeed. It reads Maven's prepared plain version tag (for example,
`2.0.7`), verifies its signature with the pinned publisher key, and requires the
same annotated tag object on GitHub. Dry runs do not create GitHub releases.
Rerunning the notes helper preserves an existing published release and
its assets; an existing draft keeps its curated text and adds generated notes
when published. Existing signing, validation, environment approval, and release
blocker checks still apply; see [release signing](scripts/release/SIGNING.md).
If only GitHub release creation fails after package/tag publication, first verify
that Maven Central has the intended package. Recover the notes step from a trusted
checkout with the published signed tag fetched, rather than republishing Maven
artifacts or changing the tag. Set `GH_TOKEN`, `GITHUB_REPOSITORY`,
`RELEASE_NOTES_TAG`, and `RELEASE_NOTES_TAG_OBJECT` (the annotated tag object's
`git rev-parse refs/tags/VERSION` value), then run
`python3 scripts/release/notes.py release`. The helper rechecks the signature and
remote object before creating a release. During normal workflow execution these
public tag identifiers are captured after preparation, before Maven can clean its
release descriptor.

Generated notes use `.github/release.yml` and exclude PRs with
`omit-from-release-notes`. The Classify release notes workflow manages that label
for every PR, including forks and merged PRs. It omits a PR only if every reported
changed filename matches documentation, tests/fixtures, CI, or development-only
formatting configuration. The rules in `.github/labeler.yml` cover `src/test/`,
`src/integration-test/`, `feature-specs/`, `.github/`, named root documentation,
and specific development/release-note test files. Production code, `pom.xml`,
`Makefile`, demos, signing helpers, unknown paths, and consumer packaging/build
inputs remain visible. Mixed PRs remain visible regardless of author, title, or
broad housekeeping/dependency labels.

The workflow uses the pinned `actions/labeler` action. Every reported changed
filename must match the single union of support paths, so mixtures of docs,
tests, and CI qualify together. `sync-labels: true` removes the label when the
rules stop matching. Runs serialize per PR. Classification follows the stock
action's reported filename behavior, including rename and API behavior. Manual
uses of this label are also reconciled on the next run.

Before releasing, backfill relevant already merged PRs by dispatching the
**Classify release notes** workflow on `main` with each PR number, and wait for
successful runs. For example:

```sh
gh workflow run classify-release-notes.yml --ref main -f pr-number=123
```

Review any failed runs before generating notes. The action reads repository
configuration through the GitHub API, with no PR checkout or PR code execution.
