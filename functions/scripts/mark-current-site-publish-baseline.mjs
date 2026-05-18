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

const { projectId, profileName, useEmulators } = await loadActiveProfile();
const apply = process.argv.includes('--apply');
const requestedBy =
  process.argv
    .find((arg) => arg.startsWith('--by='))
    ?.slice('--by='.length)
    ?.trim() || 'current-site-publish-baseline';

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

async function main() {
  const nextSnapshot = await buildPublishSnapshot();
  const counts = Object.fromEntries(
    Object.entries(nextSnapshot).map(([collectionName, docs]) => [
      collectionName,
      Object.keys(docs || {}).length
    ])
  );
  const totalDocs = Object.values(counts).reduce((sum, count) => sum + count, 0);

  console.log(`Profile: ${profileName}${useEmulators ? ' (emulator)' : ' (production)'}`);
  console.log(`Project: ${projectId}`);
  console.log(`Baseline documents: ${totalDocs}`);
  Object.entries(counts).forEach(([collectionName, count]) => {
    console.log(`  ${collectionName}: ${count}`);
  });

  if (!apply) {
    console.log('Dry run only. Re-run with --apply to mark current content as already published.');
    return;
  }

  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  await db.collection('admin').doc('sitePublish').set(
    {
      lastPublishedAt: timestamp,
      lastRequestedBy: requestedBy,
      lastRequestedByUid: requestedBy,
      lastChangeReference: Date.now(),
      lastPublishDryRun: false,
      lastPublishHistoryId: null,
      lastPublishedSnapshot: nextSnapshot,
      baselineResetAt: timestamp,
      baselineResetBy: requestedBy,
      baselineResetReason: 'Mark current website/offering content as published baseline'
    },
    { merge: true }
  );

  console.log('Baseline updated. Current website/offering content should no longer appear as added changes.');
}

main().catch((err) => {
  console.error(`Publish baseline update failed: ${err.message || err}`);
  process.exit(1);
});
