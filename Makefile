SHELL := bash
RELEASE_GUARD ?= scripts/release/guard.py
RELEASE_SIGNING_HELPER ?= $(dir $(RELEASE_GUARD))signing.py
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c

mvn = mvn

ifdef IS_CICD
    mvn = mvn --no-transfer-progress
endif

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
RELEASE_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
BIN := $(MAKEFILE_PATH)/bin

SCYLLA_CCM_COMMIT := d15a2fab9d22fffad8a30c806a7c8e1632e58aae
SCYLLA_CCM_VENV := $(BIN)/scylla-ccm-$(SCYLLA_CCM_COMMIT)
PINNED_SCYLLA_CCM_PATH := $(SCYLLA_CCM_VENV)/bin/ccm
SCYLLA_CCM_INSTALL_LOCK := $(SCYLLA_CCM_VENV).install.lock
SCYLLA_CCM_INSTALL_MARKER := $(SCYLLA_CCM_VENV)/.install-complete
SCYLLA_CCM_PATH ?= $(PINNED_SCYLLA_CCM_PATH)
SCYLLA_VERSION ?= release:2025.2.5
SCYLLA_CCM_DIAGNOSTICS_DIR ?= $(MAKEFILE_PATH)/target/ccm

MAVEN_GPG_PASSPHRASE ?=
MAVEN_OPTS ?=

SONATYPE_TOKEN_USERNAME ?=
SONATYPE_TOKEN_PASSWORD ?=

RELEASE_SKIP_TESTS ?= false
RELEASE_TARGET_TAG ?=
RELEASE_VERSION ?=

.PHONY: clean verify lint lint-specs lint-docs lint-fix compile compile-test compile-demo test-unit test-integration test-all ccm-install release-prepare release release-dry-run checkout-one-commit-before
.PHONY: release-check-user test-release-signing _release-prepare _release _release-dry-run

clean:
	${mvn} clean

verify:
	${mvn} verify

lint:
	${mvn} license:check
	${mvn} fmt:check
	${mvn} checkstyle:check
	${mvn} compile test-compile
	$(MAKE) lint-specs
	$(MAKE) lint-docs

lint-specs:
	${mvn} test -Dtest=FeatureSpecFormatTest,FeatureSpecVectorsTest,FeatureSpecDefaultsTest,FeatureSpecCanonicalEndpointVectorsTest,FeatureSpecNodeHealthTransitionsTest

lint-docs:
	${mvn} javadoc:test-javadoc javadoc:test-aggregate javadoc:test-aggregate-jar javadoc:test-jar javadoc:test-resource-bundle
	${mvn} javadoc:jar javadoc:aggregate javadoc:aggregate-jar javadoc:resource-bundle

lint-fix:
	${mvn} license:format
	${mvn} fmt:format

compile:
	${mvn} compile

compile-test:
	${mvn} test-compile

compile-demo:
	${mvn} test-compile

test-unit:
	${mvn} test

test-integration: ccm-install
	@for command in openssl setsid kill ps; do
		executable=$$(type -P -- "$$command" || true)
		[[ -n "$$executable" && -x "$$executable" ]] || {
			echo "An external $$command executable is required for CCM integration tests" >&2
			exit 1
		}
	done
	mkdir -p -- "$(SCYLLA_CCM_DIAGNOSTICS_DIR)"
	INTEGRATION_TESTS=true \
		SCYLLA_VERSION="$(SCYLLA_VERSION)" \
		SCYLLA_CCM_PATH="$(SCYLLA_CCM_PATH)" \
		SCYLLA_CCM_DIAGNOSTICS_DIR="$(SCYLLA_CCM_DIAGNOSTICS_DIR)" \
		${mvn} test -Dtest=ClusterProvisioningIT -DfailIfNoTests=false \
		-Dtest.forkCount=1 -Dsurefire.timeout=1200
	INTEGRATION_TESTS=true \
		SCYLLA_VERSION="$(SCYLLA_VERSION)" \
		SCYLLA_CCM_PATH="$(SCYLLA_CCM_PATH)" \
		SCYLLA_CCM_DIAGNOSTICS_DIR="$(SCYLLA_CCM_DIAGNOSTICS_DIR)" \
		${mvn} test '-Dtest=**/*IT,!**/ClusterProvisioningIT' -DfailIfNoTests=false \
		-Dtest.forkCount=1 -Dsurefire.timeout=1200

test-all: test-integration

ccm-install:
	@ccm_works() {
		local executable=$$1
		[[ "$$executable" == */* ]] || executable=$$(type -P -- "$$executable" || true)
		[[ -n "$$executable" && -f "$$executable" && -x "$$executable" ]] \
			&& "$$executable" create --help >/dev/null 2>&1
	}
	install_complete() {
		[[ -f "$(SCYLLA_CCM_INSTALL_MARKER)" \
			&& ! -L "$(SCYLLA_CCM_INSTALL_MARKER)" ]] || return 1
		[[ "$$(< "$(SCYLLA_CCM_INSTALL_MARKER)")" == "$(SCYLLA_CCM_COMMIT)" ]] || return 1
		ccm_works "$(PINNED_SCYLLA_CCM_PATH)"
	}
	if [[ "$(SCYLLA_CCM_PATH)" != "$(PINNED_SCYLLA_CCM_PATH)" ]]; then
		ccm_works "$(SCYLLA_CCM_PATH)" || {
			echo "SCYLLA_CCM_PATH is not a working CCM executable: $(SCYLLA_CCM_PATH)" >&2
			exit 1
		}
		echo "Using CCM executable: $(SCYLLA_CCM_PATH)"
		exit 0
	fi
	command -v flock >/dev/null 2>&1 || {
		echo "flock is required to install scylla-ccm" >&2
		exit 1
	}
	mkdir -p -- "$(BIN)"
	exec {install_lock_fd}>"$(SCYLLA_CCM_INSTALL_LOCK)"
	flock -x "$$install_lock_fd"
	if install_complete; then
		echo "Using CCM executable: $(PINNED_SCYLLA_CCM_PATH)"
		exit 0
	fi
	command -v uv >/dev/null 2>&1 || {
		echo "uv is required to install scylla-ccm: https://docs.astral.sh/uv/" >&2
		exit 1
	}
	uv venv --clear "$(SCYLLA_CCM_VENV)"
	uv pip install --python "$(SCYLLA_CCM_VENV)/bin/python" \
		"git+https://github.com/scylladb/scylla-ccm.git@$(SCYLLA_CCM_COMMIT)"
	ccm_works "$(PINNED_SCYLLA_CCM_PATH)" || {
		echo "Installed CCM entry point failed its launch check" >&2
		exit 1
	}
	temporary_marker=$$(mktemp "$(SCYLLA_CCM_VENV)/.install-complete.XXXXXXXX")
	printf '%s\n' "$(SCYLLA_CCM_COMMIT)" > "$$temporary_marker"
	chmod 600 -- "$$temporary_marker"
	mv -fT -- "$$temporary_marker" "$(SCYLLA_CCM_INSTALL_MARKER)"
	echo "Using CCM executable: $(PINNED_SCYLLA_CCM_PATH)"

release-check-user:
	@set -eu
	if [ "$${GITHUB_ACTIONS:-}" = true ]; then
		# The release environment authorizes workflow operators independently of the signer.
		exit 0
	fi
	if ! user=$$(gh api user --jq .login); then
		echo "Release requires GitHub user scylladb-publisher. Run gh auth login or gh auth switch." >&2
		exit 1
	fi
	if [ "$$user" != scylladb-publisher ]; then
		echo "Release requires GitHub user scylladb-publisher (current: $$user)." >&2
		exit 1
	fi

release-prepare: release-check-user
	@python3 "$(RELEASE_GUARD)" check
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release-prepare

release: release-check-user
	@python3 "$(RELEASE_GUARD)" check
	@python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release

release-dry-run: release-check-user
	@python3 "$(RELEASE_GUARD)" check
	@python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release-dry-run

test-release-signing:
	python3 -m unittest discover -s scripts/release -p 'test_signing.py' -v

_release-prepare: release-check-user
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	NEXT_RELEASE=()
	if [[ -n "$${RELEASE_VERSION}" ]]; then
		NEXT_RELEASE=("-DreleaseVersion=$${RELEASE_VERSION}")
	fi
	export MAVEN_OPTS
	baseline=$$(git rev-parse HEAD)
	python3 "$(RELEASE_GUARD)" run ${mvn} -B release:prepare -DpushChanges=false "$${NEXT_RELEASE[@]}"
	python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared "$$baseline"

_release: release-check-user
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	python3 "$(RELEASE_GUARD)" run ${mvn} -B release:perform -Drelease.autopublish=true

_release-dry-run: release-check-user
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	python3 "$(RELEASE_GUARD)" run ${mvn} -B release:perform

checkout-one-commit-before: release-check-user
	@if [[ "${RELEASE_TARGET_TAG}" == 3.* ]]; then
		echo "Checking out one commit before ${RELEASE_TARGET_TAG}"
		cp -f Makefile /tmp/tmp-Makefile
		git fetch --prune --unshallow || git fetch --prune || true
		git checkout ${RELEASE_TARGET_TAG}~1
		git tag -d ${RELEASE_TARGET_TAG}
		mv -f /tmp/tmp-Makefile ./Makefile
	fi
