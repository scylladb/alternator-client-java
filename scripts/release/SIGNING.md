# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `827BC62286108FB70C8B8BAFCF05F6EF40B90F46`
* Signing fingerprint: `8E43CBBEA11FDF577B6E37A743FE35C54462CC17`
* Identity: `ScyllaDB Publisher (scylladb/alternator-client-java) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:45:32+00:00`

The separate manually dispatched **Release** and **Release dry run** workflows
each first run a `validate` job on its own runner without production
signing or package-publishing credentials. It checks the release tooling and
target policy, then builds the release profile with signing disabled and runs
the existing CCM integration tests. Only Release exposes `skip-tests`; dry runs
always run project tests. Release helper tests always run. Both workflows use
Java 11 and Maven 3.9.16, downloaded with a pinned SHA-512 checksum. The normal
Java 11/17 CI matrix and Maven coordinates remain unchanged.
Validation records the exact target commit as a job output.

The `release` job depends on successful validation and uses a fresh runner. Only
this job uses the GitHub deployment environment `release`, so approval is requested
after validation finishes. It checks out the validated commit, rejects `main` if
it has advanced, and independently checks target policy before importing keys.
It does not restore the validation workspace or Maven cache. Signed preparation,
signature verification, and publication happen after approval; project tests are
not repeated with production credentials. Historical tags are resolved only by
validation, so moving a tag during the approval wait cannot change the source.

Before running it, a repository owner must create and protect that environment,
configure its protection rules and allowed deployment refs, and store
`GPG_PRIVATE_KEY`
(ASCII-armored protected signing-subkey export) and `GPG_PASSPHRASE` (single-line
passphrase) as environment secrets. Migrate any repository-level copies to this
environment and remove those copies after validating the migration. The workflow
binding does not configure protection or migrate secrets; owner setup remains
required, including before dry runs and reruns. Store `SONATYPE_TOKEN_USERNAME`
and `SONATYPE_TOKEN_PASSWORD` in the same environment. Missing tokens or the
`PLACEHOLDER_NOT_CONFIGURED` sentinel stop preparation without printing values.
Never upload the full primary private-key backup to CI. Packages, release commits,
and annotated release tags use the same signing subkey, selected by its complete
fingerprint with `!`. The primary key certifies the signing subkey and remains offline.

Locally, the `release-prepare`, `release`, and `release-dry-run` Make targets
require the active `gh` account to be `scylladb-publisher` before importing
credentials or changing the checkout; switch accounts with `gh auth switch` if
needed. Local dry runs and internal release recipes enforce the same account
requirement. In Actions, the protected `release` environment authorizes operators,
including dry runs and reruns. The triggering actor need not be the publisher
account: signing still uses the repository's pinned publisher subkey and identity.

Each target runs its Maven commands in a scoped `signing.py exec` session. The
session imports only the protected signing-subkey export, checks that primary
private material is absent, and verifies a fresh-agent signing preflight against
the pinned public certificate. Anonymous pipes supply the key import and passphrase;
neither credential is written to a temporary file or included in command-line
arguments. Runtime Git settings apply only to the command and its children,
including Maven's local checkout clones. Sessions leave checkout Git configuration
unchanged and remove their temporary keyrings on success or failure. No credentials
or signing-session state are written to `GITHUB_ENV` or retained between workflow
steps. The workflow records the public pre-preparation commit SHA there as
`RELEASE_BASE_COMMIT` for later verification.

Maven's forked builds inherit the complete signing fingerprint with `!` through
`MAVEN_OPTS`; the GPG plugin receives `MAVEN_GPG_PASSPHRASE` through its environment.
Every GPG invocation reads the current passphrase, so checks and builds can outlive
the agent's passphrase cache. The protected signing-subkey export is consumed by
the session and removed from the child environment. Maven release builds need
access to the session keyring and passphrase while signing artifacts.

For maintainer-driven release preparation:

```sh
export RELEASE_GPG_PRIVATE_KEY_FILE=/private/path/signing-private.asc
export RELEASE_GPG_PASSPHRASE_FILE=/private/path/passphrase.txt
export RELEASE_VERSION=X.Y.Z
export RELEASE_BASE_COMMIT=$(git rev-parse HEAD)
export SONATYPE_TOKEN_USERNAME=YOUR_CENTRAL_TOKEN_USERNAME
export SONATYPE_TOKEN_PASSWORD=YOUR_CENTRAL_TOKEN_PASSWORD
make release-prepare
# Run additional checks against the prepared release.
make release-dry-run
# For automatic publication, choose make release INSTEAD of make release-dry-run.
```

Maven owns version changes, release and development commits, annotated tags, and
artifact signing. The Make recipes retain sanitized Maven stdout and stderr.
Preparation verifies every new commit and the release tag against the pinned
certificate before closing its session. The workflow repeats verification with
public material only. Perform and dry-run commands verify prepared history before
opening a new signing session. Keep `RELEASE_BASE_COMMIT` set to the original
pre-preparation commit for those commands; verification fails without that trusted
boundary. Do not derive it from the release tag's parent or replace it after
preparation. Do not change or replace existing release tags. A failed verification
blocks publication.

Both jobs check out the workflow's launch SHA and preserve its current Makefile,
guard, redaction module, signing helper, and public certificate before selecting
the target source.
The release job uses the validated commit SHA instead of resolving the input ref
again. Explicit recursive Makefile selection keeps every phase on those preserved
recipes. Target POM and source files still come from the selected checkout and
must pass credential safety checks before any signing key is imported.

Complex checks can run before preparation or against signed outputs after a session
closes. Checks that need additional signatures can use their own scoped invocation:

```sh
python3 scripts/release/signing.py exec make YOUR_CHECK_TARGET
```

The low-level helper does not impose release-account policy. Wrapped commands have
no overall time limit; individual GPG operations have their own timeout. A failed
command propagates its exit status and still cleans up its session. Helper errors
identify a fixed operation and status or category without printing command arguments,
captured output, or exception payloads. `sign-files FILE...` signs and verifies a
batch of additional artifacts using one shared public keyring, preserving existing
signatures on reruns. Run `make test-release-signing` to test with disposable keys.

Consumers should obtain this public certificate from this repository at a reviewed
commit, check the certification fingerprint above through a trusted channel, and
verify release signatures. The Ubuntu keyserver can also serve the certificate:

```sh
gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys 827BC62286108FB70C8B8BAFCF05F6EF40B90F46
gpg --fingerprint 827BC62286108FB70C8B8BAFCF05F6EF40B90F46
gpg --status-fd 1 --verify ARTIFACT.asc ARTIFACT
```

Require exactly one `GOODSIG` and one `VALIDSIG` record. The `GOODSIG` key ID must
match the signing fingerprint's final 16 hexadecimal digits, and `VALIDSIG` must
contain the complete signing fingerprint above and the expected primary
fingerprint. Reject `EXPKEYSIG`, `REVKEYSIG`, `EXPSIG`, `BADSIG`, or `ERRSIG`, even
when a matching `VALIDSIG` is also present. `git verify-tag --raw TAG` and
`git verify-commit --raw COMMIT` provide the equivalent check for Git objects.
Keyserver presence alone establishes neither trust nor the expected repository
identity.

Rotate by preparing a new independent certificate, validating its protected subkey,
registering its public certificate on the publisher account, publishing the public
certificate, reviewing fingerprint/public-certificate updates together, and updating
the secret pair in a coordinated release pause. Keep old public certificates available
for historical releases. Never revoke the previous key merely to rotate it. If a key
is compromised, stop releases and use the offline revocation certificate; fix the
exposure before resuming. Do not rerun publication after a partial release without
following the repository's recovery instructions.


## Dispatch and repository prerequisites

Both workflows must be dispatched from `main`. The target accepts `main` or an
existing `X.Y.Z` tag; the optional version override must also be `X.Y.Z`. For a
historical tag, validation selects the commit preceding its release commit, and
all later work uses that exact SHA. The protected runner removes only its local
tag before Maven prepares local history. Historical targets must pass the current
credential policy; unsafe old POMs/configuration are rejected. Historical runs
publish/stage Central artifacts only: they never push replacement history or
create GitHub Releases. Existing remote tags stay unchanged. Use historical
targets only for deliberate Central recovery, respecting existing coordinates.

For `main`, the exact remote SHA must still match after validation, after approval,
and immediately before publication. Advancing or rewriting `main` requires a new
validation run. Real releases also check all open `release-blocker` issues and PRs
with pagination before validation, before approval, after approval, and just before
publication. Dry runs omit blocker checks. Signed work in both workflows shares
one concurrency group without cancelling an active job. Validation can run in
parallel; GitHub may replace pending jobs in that group.

Configure required reviewers, prevent self-review where appropriate, restrict
`release` deployments to `main`, and grant only the intended operators approval.
Keep all signing and Central credentials as environment secrets, with no
repository-level duplicates. The fresh protected runner restores no validation
workspace or Maven cache. Project tests are not repeated after credentials are
introduced. Local Make invocations retain their normal test defaults.

Enable GitHub immutable releases and protect release tags against updates and
deletion. Allow the release workflow identity to create `X.Y.Z` tags and push its
signed release/development commits to protected `main`, while retaining the
repository's review and signature policy. Repository administrators must configure
these settings; adding these workflows does not change repository settings.
Before enabling immutability, review existing drafts because immutability takes
effect on publication. Generated release notes also require the omission-label
policy and candidate PR checks in [RELEASE_NOTES.md](../../.github/RELEASE_NOTES.md).

## Publication and dry-run staging

`release:prepare` creates and verifies signed history without pushing, then saves
public metadata to `target/release-prepared.properties`. Maven deletes
`release.properties` after a successful `release:perform`; the saved copy retains
the identity for later verification and GitHub assets. Keep it with the original
`RELEASE_BASE_COMMIT`. Identity checks verify the signed history and compare the
prepared version with the tagged POM's `com.scylladb.alternator:load-balancing`
coordinates. Never derive release identity with `git describe`.

Real releases explicitly set `release.autopublish=true`. Dry runs explicitly set
`release.autopublish=false` and **upload real, unpublished Central deployments**.
They require working Central credentials and signing keys. Inspect and delete the
staged deployment in the Central portal after testing; staging consumes real
service resources. Dry runs have a read-only GitHub token, no persisted checkout
credentials, no SCM push step, and no GitHub Release step.

After successful Central publication, a `main` release pushes the signed commits
and annotated tag. The helper verifies these six files from
`target/checkout/target` using only the pinned public certificate before uploading:

* `load-balancing-X.Y.Z.jar` and `.jar.asc`
* `load-balancing-X.Y.Z-sources.jar` and `.jar.asc`
* `load-balancing-X.Y.Z-javadoc.jar` and `.jar.asc`

Existing signatures are verified and preserved. The helper refuses an existing
GitHub Release or draft, creates a new draft with generated notes and all six
assets, and publishes only after every upload succeeds. An upload failure leaves
an unpublished draft for manual recovery. Sanitized stdout/stderr logs are retained
for seven days; no private keyrings or signing credentials are uploaded.

## Partial-release recovery

Stop and establish which side effects succeeded before retrying anything. Inspect
the Central deployment, remote `main` and tag object IDs, GitHub drafts and assets,
and the run's sanitized logs. Preserve the original checkout, signed objects,
prepared metadata, base SHA, and built artifacts securely. Hosted runner workspaces
are ephemeral: preserve required recovery material before ending an operator-owned
session. Workflow artifacts contain only sanitized logs, so escalate to repository
owners if the original signed objects or built artifacts are unavailable.

* Before Central accepts a deployment, fix the failure and validate again. Delete
  an unwanted unpublished staging deployment through the Central portal.
* If Central accepted publication, do not republish the same coordinates or start
  a fresh Release dispatch merely to complete GitHub work. A command can fail
  after the server accepted it; reconcile its deployment state first.
* If SCM push failed, recover the **original signed objects** and push them after
  checking the expected remote baseline. Never force `main`, move an existing
  remote tag, recreate signed commits, or manufacture a replacement signature.
  If `main` advanced, ask repository owners to plan recovery of the original
  history; a new preparation would produce different signed objects.
* If asset signing/verification failed after the push, retain the pushed history.
  Diagnose the failure, recover the original published artifacts and signatures,
  and verify them with this repository's pinned public certificate. Never replace
  an existing signature just to make a retry pass.
* If draft creation or upload failed, inspect the existing draft and its full
  asset list. Verify the tag object, artifact hashes, and all six signatures.
  Upload only missing original files, without `--clobber`; do not replace existing
  assets. Reconcile an uncertain upload by reading the draft before retrying it.
  Check generated notes and publish that same draft only when every asset is
  present and verified. The automated helper deliberately refuses existing drafts.
* If the GitHub Release is already published, verify it and stop. Immutable
  releases and protected tags must not be deleted/recreated to retry automation.

A missing original artifact or signed object requires owner-assisted recovery;
do not silently rebuild and label a different byte sequence as the same release.
