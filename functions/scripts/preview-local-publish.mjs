import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const PUBLISH_COLLECTIONS = [
  'storageSeasons',
  'storageOffers',
  'offerTemplates',
  'vehicleTypes',
  'storageAddOns',
  'storageSeasonAddOns',
  'storageConditions',
  'storageEtiquette',
  'i18nEntries'
];
const AUDIT_FIELDS = new Set(['createdAt', 'createdBy', 'updatedAt', 'updatedBy']);

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

function normalizePublishValue(value) {
  if (value === null || value === undefined) return value ?? null;
  if (typeof value?.toDate === 'function') {
    return value.toDate().toISOString();
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizePublishValue(item));
  }
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => !AUDIT_FIELDS.has(key))
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, nested]) => [key, normalizePublishValue(nested)])
    );
  }
  return value;
}

async function buildPublishSnapshot() {
  const snapshot = {};
  for (const collectionName of PUBLISH_COLLECTIONS) {
    const docs = await db.collection(collectionName).get();
    snapshot[collectionName] = {};
    docs.docs
      .sort((left, right) => left.id.localeCompare(right.id))
      .forEach((docSnap) => {
        snapshot[collectionName][docSnap.id] = normalizePublishValue(docSnap.data() || {});
      });
  }
  return snapshot;
}

function stableStringify(value) {
  return JSON.stringify(normalizePublishValue(value));
}

function buildPublishFieldDiffs(before = {}, after = {}) {
  const fields = Array.from(new Set([...Object.keys(before || {}), ...Object.keys(after || {})]))
    .filter((field) => stableStringify(before?.[field]) !== stableStringify(after?.[field]))
    .sort();
  return fields.map((field) => ({
    field,
    before: before?.[field] === undefined ? null : before[field],
    after: after?.[field] === undefined ? null : after[field]
  }));
}

function summarizePublishDiff(previousSnapshot = {}, nextSnapshot = {}) {
  const changes = [];
  const collections = Array.from(
    new Set([...Object.keys(previousSnapshot || {}), ...Object.keys(nextSnapshot || {})])
  ).sort();
  collections.forEach((collectionName) => {
    const beforeDocs = previousSnapshot?.[collectionName] || {};
    const afterDocs = nextSnapshot?.[collectionName] || {};
    const ids = Array.from(new Set([...Object.keys(beforeDocs), ...Object.keys(afterDocs)])).sort();
    ids.forEach((id) => {
      const before = beforeDocs[id];
      const after = afterDocs[id];
      if (before === undefined) {
        changes.push({ collection: collectionName, id, type: 'added', fieldDiffs: buildPublishFieldDiffs({}, after || {}) });
        return;
      }
      if (after === undefined) {
        changes.push({ collection: collectionName, id, type: 'removed', fieldDiffs: buildPublishFieldDiffs(before || {}, {}) });
        return;
      }
      if (stableStringify(before) === stableStringify(after)) return;
      changes.push({ collection: collectionName, id, type: 'changed', fieldDiffs: buildPublishFieldDiffs(before, after) });
    });
  });
  return {
    total: changes.length,
    added: changes.filter((change) => change.type === 'added').length,
    changed: changes.filter((change) => change.type === 'changed').length,
    removed: changes.filter((change) => change.type === 'removed').length,
    changes
  };
}

async function main() {
  const statusSnap = await db.collection('admin').doc('sitePublish').get();
  const previousSnapshot = statusSnap.data()?.lastPublishedSnapshot || {};
  const nextSnapshot = await buildPublishSnapshot();
  const diff = summarizePublishDiff(previousSnapshot, nextSnapshot);
  console.log(JSON.stringify(diff, null, 2));
}

main().catch((err) => {
  console.error(`Publish preview failed: ${err.message || err}`);
  process.exit(1);
});
