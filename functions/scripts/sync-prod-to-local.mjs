import fs from 'node:fs/promises';
import path from 'node:path';
import admin from 'firebase-admin';
import { fileURLToPath } from 'node:url';
import { deserializeData, serializeData } from './utils/firestore-serialization.mjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '../..');
const DEFAULT_BACKUP_DIR = path.resolve(ROOT_DIR, 'backups');
const PROFILE_CONFIG_PATH = path.resolve(ROOT_DIR, 'public/profile-config.js');

async function loadProfileConfig() {
  const source = await fs.readFile(PROFILE_CONFIG_PATH, 'utf8');
  const match = source.match(/export\s+const\s+PROFILE_CONFIG\s*=\s*([\s\S]*);\s*$/);
  if (!match) {
    throw new Error('Missing PROFILE_CONFIG export in public/profile-config.js.');
  }
  return JSON.parse(match[1]);
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

function getArgValue(flag) {
  const index = process.argv.indexOf(flag);
  if (index === -1) return null;
  return process.argv[index + 1] || null;
}

function requireProfile(config, name) {
  const profile = config?.profiles?.[name];
  if (!profile) {
    throw new Error(`Missing Firebase profile "${name}" in public/profile-config.js.`);
  }
  return profile;
}

function timestampForFile() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function resolveBackupPath() {
  const explicit = getArgValue('--backup-out');
  if (explicit) return path.resolve(explicit);
  return path.resolve(DEFAULT_BACKUP_DIR, `local-before-prod-sync-${timestampForFile()}.json`);
}

async function ensureDirectory(filePath) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
}

async function writeBackup(filePath, payload) {
  await ensureDirectory(filePath);
  await fs.writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

function clearFirestoreEmulatorEnv() {
  delete process.env.FIRESTORE_EMULATOR_HOST;
}

function setProjectEnv(projectId) {
  process.env.GCLOUD_PROJECT = projectId;
  process.env.GCP_PROJECT = projectId;
  process.env.FIRESTORE_PROJECT_ID = projectId;
  process.env.GOOGLE_CLOUD_PROJECT = projectId;
}

function setFirestoreEmulatorEnv(profile) {
  const host = profile?.emulator?.host || '127.0.0.1';
  const port = profile?.emulator?.firestorePort || 8081;
  process.env.FIRESTORE_EMULATOR_HOST = `${host}:${port}`;
}

function initProdApp(profile) {
  setProjectEnv(profile.projectId);
  clearFirestoreEmulatorEnv();
  return admin.initializeApp({ projectId: profile.projectId }, 'prod-source');
}

function initLocalApp(profile) {
  setProjectEnv(profile.projectId);
  setFirestoreEmulatorEnv(profile);
  return admin.initializeApp({ projectId: profile.projectId }, 'local-target');
}

async function exportCollection(collectionRef, documents) {
  const snapshot = await collectionRef.get();
  const docs = snapshot.docs.sort((left, right) => left.id.localeCompare(right.id));
  for (const docSnap of docs) {
    documents.push({
      path: docSnap.ref.path,
      data: serializeData(docSnap.data() || {})
    });
    const subcollections = await docSnap.ref.listCollections();
    subcollections.sort((left, right) => left.id.localeCompare(right.id));
    for (const subcollection of subcollections) {
      await exportCollection(subcollection, documents);
    }
  }
}

async function collectDocuments(db) {
  const documents = [];
  const collections = await db.listCollections();
  collections.sort((left, right) => left.id.localeCompare(right.id));
  for (const collectionRef of collections) {
    await exportCollection(collectionRef, documents);
  }
  return documents.sort((left, right) => left.path.localeCompare(right.path));
}

async function deleteCollectionRecursive(collectionRef) {
  const snapshot = await collectionRef.get();
  let deleted = 0;
  for (const docSnap of snapshot.docs) {
    const subcollections = await docSnap.ref.listCollections();
    for (const subcollection of subcollections) {
      deleted += await deleteCollectionRecursive(subcollection);
    }
    await docSnap.ref.delete();
    deleted += 1;
  }
  return deleted;
}

async function deleteAllDocuments(db) {
  const collections = await db.listCollections();
  let deleted = 0;
  for (const collectionRef of collections) {
    deleted += await deleteCollectionRecursive(collectionRef);
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

async function main() {
  const apply = hasFlag('--apply');
  const config = await loadProfileConfig({ fresh: true });
  const prodProfile = requireProfile(config, 'prod');
  const localProfile = requireProfile(config, 'local');
  const backupPath = resolveBackupPath();
  const host = localProfile.emulator?.host || '127.0.0.1';
  const port = localProfile.emulator?.firestorePort || 8081;

  console.log(`Source: production ${prodProfile.projectId}`);
  console.log(`Target: local emulator ${localProfile.projectId} (${host}:${port})`);

  const prodApp = initProdApp(prodProfile);
  const prodDb = admin.firestore(prodApp);
  prodDb.settings({ preferRest: true });
  const prodDocuments = await collectDocuments(prodDb);
  await prodApp.delete();

  const localApp = initLocalApp(localProfile);
  const localDb = admin.firestore(localApp);
  localDb.settings({ ignoreUndefinedProperties: true });
  const localBackupDocuments = await collectDocuments(localDb);
  await writeBackup(backupPath, {
    version: 1,
    kind: 'local-before-prod-sync',
    projectId: localProfile.projectId,
    emulator: `${host}:${port}`,
    createdAt: new Date().toISOString(),
    documentCount: localBackupDocuments.length,
    documents: localBackupDocuments
  });

  console.log(`Production documents ready: ${prodDocuments.length}`);
  console.log(`Local backup saved: ${backupPath}`);
  console.log(`Local documents backed up: ${localBackupDocuments.length}`);

  if (!apply) {
    console.log('Dry run only. Re-run with --apply to delete local emulator data and replace it with production.');
    await localApp.delete();
    return;
  }

  const deleted = await deleteAllDocuments(localDb);
  const written = await writeDocuments(localDb, prodDocuments);
  await localApp.delete();

  console.log(`Sync complete. Deleted ${deleted} local documents, wrote ${written} production documents.`);
  console.log('Production was read only. No production data was changed.');
}

main().catch((err) => {
  console.error('Prod-to-local sync failed', err);
  process.exit(1);
});
