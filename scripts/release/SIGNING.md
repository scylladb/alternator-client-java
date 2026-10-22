# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `827BC62286108FB70C8B8BAFCF05F6EF40B90F46`
* Signing fingerprint: `8E43CBBEA11FDF577B6E37A743FE35C54462CC17`
* Identity: `ScyllaDB Publisher (scylladb/alternator-client-java) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:45:32+00:00`

The release job uses the GitHub deployment environment `release`. Before running
it, a repository owner must create and protect that environment, configure its
protection rules and allowed deployment refs, and store `GPG_PRIVATE_KEY`
(ASCII-armored protected signing-subkey export) and `GPG_PASSPHRASE` (single-line
passphrase) as environment secrets. Migrate any repository-level copies to this
environment and remove those copies after validating the migration. The workflow
binding does not configure protection or migrate secrets; owner setup remains
required, including before dry runs and reruns.
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
make release-prepare
# Run additional checks against the prepared release.
make release-dry-run
# Use make release when ready for automatic publication.
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

The workflow preserves the current Makefile, guard, signing helper, and public
certificate before checking out a historical release target. Explicit recursive
Makefile selection keeps every phase on those preserved recipes. Target POM and
source files still come from the selected checkout and must pass credential safety
checks before any signing key is imported.

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
