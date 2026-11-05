# Release notes

The Release workflow publishes to Maven Central, pushes the signed release history
and `X.Y.Z` tag, verifies the three library JARs and their detached signatures, then
creates a draft GitHub Release with generated notes. It publishes the draft only
when all six uploads succeed. An existing draft or published release stops the
helper without changing its assets or notes. See
[release operations and recovery](../scripts/release/SIGNING.md).

`.github/release.yml` excludes PRs labelled `omit-from-release-notes`.
The filename policy lives in `.github/scripts/release-notes-guard.cjs`.
Every current and previous filename must qualify. Markdown documentation,
`src/test/`, CI configuration under `.github/`, release tooling under
`scripts/release/`, `Makefile`, and `.gitignore` qualify, including mixtures of
these paths. Runtime sources and resources, `pom.xml`, dependencies, executable
examples, and unknown paths remain visible. A PR mixing support and runtime or
dependency changes remains visible in its entirety. Titles, authors, and other
labels do not determine exclusion.

Classification runs on open, synchronize, reopen, edit, and close. Runs for one PR
are serialized, including manual runs. The workflow checks out trusted repository
event tooling, never PR head code or the old PR base revision. Manual dispatch
must select the default branch before checkout. It confirms the exact PR number
from the trusted event or a successful manual PR lookup before checkout or label
changes; ordinary issues and unavailable targets do not receive label writes.

The guard paginates the full unique file listing, validates rename metadata,
compares head/base revisions and counts around collection, and verifies the same
snapshot and actual label after applying its decision directly. A rename from a
nonqualifying path removes the omission label. Failed or unexpectedly
skipped setup/classification attempts omission-label removal independently of
checkout. Failures stay visible; API outages can prevent cleanup.
Snapshot comparisons and serialized runs cannot make classification atomic.

Before generating notes:

1. Identify every PR since the previous release, including merged PRs predating
   this workflow. Inspect complete paginated file metadata, including rename
   origins, uniqueness, and the changed-file count.
2. Run Actions → Classify release notes on `main` for each candidate PR, or use
   `gh workflow run label-release-notes.yml --ref main -f pull-request=123`.
3. Wait for success and confirm the collection and final verification log agree
   on head, base, count, snapshot hash, and the actual omission label.
4. Recheck candidates immediately before generating notes. Reclassify changed
   revisions, failures, and stale labels. Keep changes visible if their metadata
   is incomplete and inspect them manually.

Publication and classification are asynchronous. No release gate automatically
waits for classification. Maintainers can correct labels after inspecting the
same evidence; future runs reconcile them with the policy. Classification leaves
already published notes unchanged. Do not rerun the Release workflow to recover
an upload: a new dispatch prepares a new Maven version.
