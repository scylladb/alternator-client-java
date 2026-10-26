// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const yaml = require('js-yaml');
const load = name => yaml.load(readFileSync(join(__dirname, '../workflows', name), 'utf8'));
const jira = load('call_jira_sync.yml');
// These known fixture strings follow Actions' case-insensitive comparisons.
// This is not an implementation of the general Actions expression language.
const condition = jira.jobs['jira-sync'].if.replace(/^\$\{\{\s*|\s*\}\}$/g, '');
const route = (action, label) => new Function('github', `return ${condition}`)({event: {
  action: action.toLowerCase(), label: {name: label?.toLowerCase()}
}});

test('Jira caller excludes only the whole logical omission-marker add/remove events', () => {
  for (const action of ['labeled', 'unlabeled']) {
    for (const label of ['omit-from-release-notes', 'Omit-from-release-notes', 'OMIT-FROM-RELEASE-NOTES']) {
      assert.equal(route(action, label), false);
    }
    for (const label of ['bug', 'omit-from-release-notes-extra', 'prefix-omit-from-release-notes']) {
      assert.equal(route(action, label), true);
    }
  }
  for (const action of ['opened', 'edited', 'ready_for_review', 'review_requested', 'closed']) {
    assert.equal(route(action), true);
    assert.equal(route(action, 'omit-from-release-notes'), true);
  }
  assert.equal(jira.jobs['jira-sync'].uses,
    'scylladb/github-automation/.github/workflows/main_pr_events_jira_sync.yml@main');
  assert.equal(jira.jobs['jira-sync'].with.caller_action, '${{ github.event.action }}');
  assert.equal(jira.jobs['jira-sync'].secrets.caller_jira_auth,
    '${{ secrets.USER_AND_KEY_FOR_JIRA_AUTOMATION }}');
});

test('release eligibility preserves main-only SCM and excludes historical targets and dry runs', () => {
  const steps = load('release.yml').jobs.release.steps;
  const push = steps.find(step => step.name === 'Push changes to SCM');
  const release = steps.find(step => step.name.startsWith('Create GitHub release'));
  assert.equal(release.if, push.if);
  const expression = release.if.replace(/^\$\{\{\s*|\s*\}\}$/g, '')
    .replaceAll('inputs.dry-run', 'inputs.dryRun').replaceAll('inputs.target-tag', 'inputs.targetTag');
  const eligible = new Function('inputs', `return ${expression}`);
  for (const [dryRun, targetTag, expected] of [[false, 'main', true], [true, 'main', false],
    [false, '2.0.7', false], [true, '2.0.7', false]]) {
    assert.equal(eligible({dryRun, targetTag}), expected);
  }
  assert.ok(steps.indexOf(release) > steps.indexOf(push));
  assert.ok(steps.indexOf(push) > steps.findIndex(step => step.name === 'Perform release'));
  assert.ok(!/always\(|continue-on-error/.test(release.if));
  assert.equal(release['continue-on-error'], undefined);
  assert.equal(push['continue-on-error'], undefined);
});
