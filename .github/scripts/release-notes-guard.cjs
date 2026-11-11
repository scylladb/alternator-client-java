// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const {createHash} = require('node:crypto');

const label = 'omit-from-release-notes';

function supportPath(filename) {
  return filename.endsWith('.md') || ['Makefile', '.gitignore'].includes(filename) ||
    ['.github/', 'scripts/release/', 'src/test/'].some(prefix => filename.startsWith(prefix));
}

function revision(pr) {
  if (!pr.head?.sha || !pr.base?.sha || !Number.isSafeInteger(pr.changed_files) ||
      pr.changed_files <= 0) throw new Error('Invalid PR revision or changed-file count');
  return {head: pr.head.sha, base: pr.base.sha, count: pr.changed_files};
}

async function readRevision(github, params) {
  return revision((await github.rest.pulls.get(params)).data);
}

async function snapshot(github, repo, number) {
  const params = {...repo, pull_number: number};
  const before = await readRevision(github, params);
  const files = await github.paginate(github.rest.pulls.listFiles, {...params, per_page: 100});
  const seen = new Set();
  const records = files.map(file => {
    if (typeof file.filename !== 'string' || !file.filename || seen.has(file.filename) ||
        !['added', 'removed', 'modified', 'renamed', 'copied', 'changed', 'unchanged'].includes(file.status) ||
        (file.previous_filename !== undefined &&
          (typeof file.previous_filename !== 'string' || !file.previous_filename)) ||
        (file.status === 'renamed' && !file.previous_filename)) {
      throw new Error('Incomplete or duplicate changed-file metadata');
    }
    seen.add(file.filename);
    return {filename: file.filename, status: file.status, previous_filename: file.previous_filename ?? null};
  }).sort((a, b) => a.filename < b.filename ? -1 : a.filename > b.filename ? 1 : 0);
  const after = await readRevision(github, params);
  if (JSON.stringify(before) !== JSON.stringify(after)) throw new Error('PR revisions changed during file collection');
  if (records.length !== before.count) throw new Error('Changed-file listing does not match expected count');
  const omit = records.every(file => supportPath(file.filename) &&
    (!file.previous_filename || supportPath(file.previous_filename)));
  const hash = createHash('sha256').update(JSON.stringify({...before, files: records})).digest('hex');
  return {...before, hash, omit};
}

async function verify(github, repo, number, expectedHash) {
  const current = await snapshot(github, repo, number);
  if (current.hash !== expectedHash) throw new Error('PR file snapshot changed since classification');
  const labels = await github.paginate(github.rest.issues.listLabelsOnIssue,
    {...repo, issue_number: number, per_page: 100});
  if (!labels.every(item => item && typeof item.name === 'string' && item.name.length > 0)) {
    throw new Error('Invalid label metadata');
  }
  if (labels.some(item => item.name.toLowerCase() === label) !== current.omit) {
    throw new Error('Actual omission label does not match verified file policy');
  }
  const after = await readRevision(github, {...repo, pull_number: number});
  if (JSON.stringify(after) !== JSON.stringify({head: current.head, base: current.base, count: current.count})) {
    throw new Error('PR revisions changed during label verification');
  }
  return current;
}

module.exports = {snapshot, verify};
