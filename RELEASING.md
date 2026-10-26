# GitHub release notes

For main releases, the Release workflow creates a GitHub release after Maven
publication and the signed tag push succeed. Historical targets retain their
existing Maven release behavior and main-only SCM push policy, and skip automatic
GitHub release creation throughout this PR; no later repair is scheduled.
The helper reads Maven's prepared plain version tag (for example,
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

The workflow keeps the pinned `actions/labeler` action. A guard reads the same
canonical YAML rules, paginates changed-file metadata, checks its count against
the PR metadata, and checks head/base revisions before and after collection.
Renames from a nonmatching path remain visible. After classification, it checks
the snapshot again and verifies the actual logical marker label (case
insensitively). The marker must match the verified support-only outcome.

Manual dispatch must run on the default branch. Both PR events and manual runs
load the guard and its dependencies from the repository default branch, never
from the PR head or an old merged PR base. Before any label write, including
failure cleanup, the workflow confirms the target is a PR. After a confirmed
PR's setup, classification, or verification fails or unexpectedly skips, it
attempts to remove an unverified omission marker without loading the helper.
Failures remain visible; API outages can prevent removal. Before/after reads do
not promise atomicity or race freedom.

Every reported changed filename must match the support-only policy. The single
union of support paths allows mixtures of docs, tests, and CI to qualify together.
`sync-labels: true` removes the label when the rules stop matching. Runs serialize
per PR. The guard adds the bounded checks above to the stock action; it does not prove
that GitHub's API reports every possible rename. Manual uses of the marker are
also reconciled on the next run. The local Jira caller skips only whole-name
marker add/remove events. Other Jira events are unchanged. Existing Jira copies
are not cleaned and other sync paths can restore them; this workflow performs no
Jira writes.

Before releasing, identify every merged PR whose commits will be included since
the previous release and backfill classification on **main**. Do not assume a
merge-triggered run has finished, or that a prior green run still describes the
current files and labels. For each PR, dispatch the **Classify release notes**
workflow, select its resulting run, and wait for completion:

```sh
gh workflow run classify-release-notes.yml --ref main -f pr-number=123
gh run list --workflow classify-release-notes.yml --event workflow_dispatch
gh run watch RUN_ID --exit-status
gh run view RUN_ID --log
```

Confirm the selected run is the one for that PR and its **Verify classification**
step succeeded. Read its final `Verified head=... base=... count=... snapshot=...
omit=...; actual label checked` log. Compare current head/base/count and the
complete file/rename list and labels, for example:

```sh
gh api repos/scylladb/alternator-client-java/pulls/123 \
  --jq '{head: .head.sha, base: .base.sha, count: .changed_files}'
gh api --paginate repos/scylladb/alternator-client-java/pulls/123/files \
  --jq '.[] | [.filename, .previous_filename, .status] | @tsv'
gh api --paginate repos/scylladb/alternator-client-java/issues/123/labels \
  --jq '.[].name'
```

Check that the file count and revisions match the log, and that every current
path and rename source qualifies before retaining the marker. Runtime, unknown,
or mixed paths must have no omission marker. Rerun classification after any
relevant change or mismatch, and investigate failed or skipped verification and
cleanup before publishing. These commands inspect GitHub metadata only; they do
not check out or execute PR code. The default-branch safeguards must be merged
before relying on their logs.

Release publication remains asynchronous. This is a manual operator check, not
an automated synchronization barrier. If verification is missed, published notes
can retain omissions or unwanted support entries. Notes-helper reruns preserve
already published notes; no automated synchronization follow-up is scheduled.
