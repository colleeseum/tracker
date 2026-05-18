import fs from 'node:fs/promises';
import path from 'node:path';
import admin from 'firebase-admin';
import { fileURLToPath } from 'node:url';
import { loadProfileConfig } from '../../lib/profile-config.js';
import { deserializeData, serializeData } from './utils/firestore-serialization.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '../..');
const DEFAULT_BACKUP_DIR = path.resolve(ROOT_DIR, 'backups');

const CONTENT_COLLECTIONS = [
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

function getArgValue(flag) {
  const index = process.argv.indexOf(flag);
  if (index === -1) return null;
  return process.argv[index + 1] || null;
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

function timestampForFile() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function resolveBackupPath() {
  const explicit = getArgValue('--backup-out');
  if (explicit) return path.resolve(explicit);
  return path.resolve(DEFAULT_BACKUP_DIR, `prod-website-content-before-sync-${timestampForFile()}.json`);
}

async function ensureDirectory(filePath) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
}

function requireProfile(config, name) {
  const profile = config?.profiles?.[name];
  if (!profile) {
    throw new Error(`Missing Firebase profile "${name}" in public/profile-config.js.`);
  }
  return profile;
}

function setProjectEnv(projectId) {
  process.env.GCLOUD_PROJECT = projectId;
  process.env.GCP_PROJECT = projectId;
  process.env.FIRESTORE_PROJECT_ID = projectId;
  process.env.GOOGLE_CLOUD_PROJECT = projectId;
}

function clearFirestoreEmulatorEnv() {
  delete process.env.FIRESTORE_EMULATOR_HOST;
}

function initApp(name, profile) {
  setProjectEnv(profile.projectId);
  clearFirestoreEmulatorEnv();
  return admin.initializeApp({ projectId: profile.projectId }, name);
}

async function collectCollectionDocs(db, collectionName) {
  const snapshot = await db.collection(collectionName).get();
  return snapshot.docs
    .sort((left, right) => left.id.localeCompare(right.id))
    .map((docSnap) => ({
      path: docSnap.ref.path,
      data: serializeData(docSnap.data() || {})
    }));
}

async function collectContentDocs(db) {
  const documents = [];
  for (const collectionName of CONTENT_COLLECTIONS) {
    documents.push(...(await collectCollectionDocs(db, collectionName)));
  }
  return documents.sort((left, right) => left.path.localeCompare(right.path));
}

function deserializeRestValue(value) {
  if (!value || typeof value !== 'object') return null;
  if (Object.prototype.hasOwnProperty.call(value, 'nullValue')) return null;
  if (Object.prototype.hasOwnProperty.call(value, 'booleanValue')) return Boolean(value.booleanValue);
  if (Object.prototype.hasOwnProperty.call(value, 'integerValue')) return Number(value.integerValue);
  if (Object.prototype.hasOwnProperty.call(value, 'doubleValue')) return Number(value.doubleValue);
  if (Object.prototype.hasOwnProperty.call(value, 'timestampValue')) {
    return { __datatype: 'timestamp', value: value.timestampValue };
  }
  if (Object.prototype.hasOwnProperty.call(value, 'stringValue')) return value.stringValue;
  if (Object.prototype.hasOwnProperty.call(value, 'bytesValue')) {
    return { __datatype: 'blob', base64: value.bytesValue };
  }
  if (Object.prototype.hasOwnProperty.call(value, 'referenceValue')) {
    const marker = '/documents/';
    const markerIndex = value.referenceValue.indexOf(marker);
    return {
      __datatype: 'documentReference',
      path: markerIndex === -1 ? value.referenceValue : value.referenceValue.slice(markerIndex + marker.length)
    };
  }
  if (value.geoPointValue) {
    return {
      __datatype: 'geopoint',
      latitude: value.geoPointValue.latitude,
      longitude: value.geoPointValue.longitude
    };
  }
  if (value.arrayValue) {
    return (value.arrayValue.values || []).map((entry) => deserializeRestValue(entry));
  }
  if (value.mapValue) {
    return deserializeRestFields(value.mapValue.fields || {});
  }
  return null;
}

function deserializeRestFields(fields = {}) {
  return Object.fromEntries(
    Object.entries(fields).map(([key, value]) => [key, deserializeRestValue(value)])
  );
}

function localCollectionUrl(profile, collectionName, pageToken = '') {
  const host = profile?.emulator?.host || '127.0.0.1';
  const port = profile?.emulator?.firestorePort || 8081;
  const base = `http://${host}:${port}/v1/projects/${profile.projectId}/databases/(default)/documents/${collectionName}`;
  const params = new URLSearchParams({ pageSize: '300' });
  if (pageToken) params.set('pageToken', pageToken);
  return `${base}?${params.toString()}`;
}

async function collectLocalCollectionDocs(profile, collectionName) {
  const documents = [];
  let pageToken = '';
  do {
    const response = await fetch(localCollectionUrl(profile, collectionName, pageToken), {
      headers: {
        Authorization: 'Bearer owner'
      }
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Failed to read local ${collectionName}: ${response.status} ${text}`);
    }
    const payload = await response.json();
    (payload.documents || []).forEach((doc) => {
      const marker = '/documents/';
      const markerIndex = doc.name.indexOf(marker);
      documents.push({
        path: markerIndex === -1 ? doc.name : doc.name.slice(markerIndex + marker.length),
        data: deserializeRestFields(doc.fields || {})
      });
    });
    pageToken = payload.nextPageToken || '';
  } while (pageToken);
  return documents.sort((left, right) => left.path.localeCompare(right.path));
}

async function collectLocalContentDocs(profile) {
  const documents = [];
  for (const collectionName of CONTENT_COLLECTIONS) {
    documents.push(...(await collectLocalCollectionDocs(profile, collectionName)));
  }
  return documents.sort((left, right) => left.path.localeCompare(right.path));
}

async function collectBackupDocs(db) {
  const documents = await collectContentDocs(db);
  const statusSnap = await db.collection('admin').doc('sitePublish').get();
  if (statusSnap.exists) {
    documents.push({
      path: statusSnap.ref.path,
      data: serializeData(statusSnap.data() || {})
    });
  }
  return documents.sort((left, right) => left.path.localeCompare(right.path));
}

async function writeBackup(filePath, payload) {
  await ensureDirectory(filePath);
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

async function deleteCollection(db, collectionName) {
  const snapshot = await db.collection(collectionName).get();
  if (snapshot.empty) return 0;
  let deleted = 0;
  for (let index = 0; index < snapshot.docs.length; index += 400) {
    const batch = db.batch();
    const slice = snapshot.docs.slice(index, index + 400);
    slice.forEach((docSnap) => batch.delete(docSnap.ref));
    await batch.commit();
    deleted += slice.length;
  }
  return deleted;
}

async function writeDocuments(db, documents) {
  let written = 0;
  for (let index = 0; index < documents.length; index += 400) {
    const batch = db.batch();
    const slice = documents.slice(index, index + 400);
    slice.forEach((entry) => {
      batch.set(db.doc(entry.path), deserializeData(entry.data, db), { merge: false });
    });
    await batch.commit();
    written += slice.length;
  }
  return written;
}

function normalizePublishValue(value) {
  if (value === null || value === undefined) return value ?? null;
  if (typeof value?.toDate === 'function') {
    return value.toDate().toISOString();
  }
  if (Array.isArray(value)) {
    return value.map((entry) => normalizePublishValue(entry));
  }
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => !AUDIT_FIELDS.has(key))
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, normalizePublishValue(entry)])
    );
  }
  return value;
}

async function buildPublishSnapshot(db) {
  const snapshot = {};
  for (const collectionName of CONTENT_COLLECTIONS) {
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

async function markImportedStatePublished(db, requestedBy) {
  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  await db.collection('admin').doc('sitePublish').set(
    {
      lastPublishedAt: timestamp,
      lastRequestedBy: requestedBy,
      lastRequestedByUid: requestedBy,
      lastChangeReference: Date.now(),
      lastPublishDryRun: false,
      lastPublishHistoryId: null,
      lastPublishedSnapshot: await buildPublishSnapshot(db)
    },
    { merge: true }
  );
}

async function main() {
  const apply = hasFlag('--apply');
  const config = await loadProfileConfig({ fresh: true });
  const localProfile = requireProfile(config, 'local');
  const prodProfile = requireProfile(config, 'prod');
  const backupPath = resolveBackupPath();
  const requestedBy = getArgValue('--by') || 'local-to-prod-content-sync';

  console.log(`Source: local emulator ${localProfile.projectId} (${localProfile.emulator?.host || '127.0.0.1'}:${localProfile.emulator?.firestorePort || 8081})`);
  console.log(`Target: production ${prodProfile.projectId}`);
  console.log(`Collections: ${CONTENT_COLLECTIONS.join(', ')}`);

  const sourceDocuments = await collectLocalContentDocs(localProfile);

  const prodApp = initApp('prod-target', prodProfile);
  const prodDb = admin.firestore(prodApp);
  prodDb.settings({ preferRest: true });
  const backupDocuments = await collectBackupDocs(prodDb);
  await writeBackup(backupPath, {
    version: 1,
    kind: 'website-content-targeted-backup',
    projectId: prodProfile.projectId,
    createdAt: new Date().toISOString(),
    collections: CONTENT_COLLECTIONS,
    documentCount: backupDocuments.length,
    documents: backupDocuments
  });

  console.log(`Production backup saved: ${backupPath}`);
  console.log(`Local source documents ready: ${sourceDocuments.length}`);
  console.log(`Production documents backed up: ${backupDocuments.length}`);

  if (!apply) {
    console.log('Dry run only. Re-run with --apply to delete and replace the listed production collections.');
    await prodApp.delete();
    return;
  }

  let deleted = 0;
  for (const collectionName of CONTENT_COLLECTIONS) {
    const count = await deleteCollection(prodDb, collectionName);
    deleted += count;
    console.log(`Deleted ${count} production docs from ${collectionName}.`);
  }

  const written = await writeDocuments(prodDb, sourceDocuments);
  await markImportedStatePublished(prodDb, requestedBy);
  await prodApp.delete();

  console.log(`Sync complete. Deleted ${deleted} production content docs, wrote ${written} local content docs.`);
  console.log('Updated admin/sitePublish so this imported state is the production publish baseline.');
}

main().catch((err) => {
  console.error('Website content sync failed', err);
  process.exit(1);
});
