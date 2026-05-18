import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { profileName, profile, projectId, useEmulators } = await loadActiveProfile();

if (useEmulators) {
  const host = profile.emulator?.host || '127.0.0.1';
  const storagePort = profile.emulator?.storagePort ?? 9199;
  process.env.FIREBASE_STORAGE_EMULATOR_HOST = `${host}:${storagePort}`;
}

admin.initializeApp({
  projectId,
  storageBucket: profile.firebase?.storageBucket
});

const db = admin.firestore();
if (!useEmulators) {
  db.settings({ preferRest: true });
}

const bucket = admin.storage().bucket();
const deleteField = admin.firestore.FieldValue.delete();
const serverTimestamp = admin.firestore.FieldValue.serverTimestamp();
const dryRun = process.argv.includes('--dry-run');
const emulatorHost = profile.emulator?.host || '127.0.0.1';
const firestorePort = profile.emulator?.firestorePort ?? 8080;
const storagePort = profile.emulator?.storagePort ?? 9199;
const firestoreBaseUrl = `http://${emulatorHost}:${firestorePort}/v1/projects/${projectId}/databases/(default)/documents`;
const storageBaseUrl = `http://${emulatorHost}:${storagePort}/v0/b/${encodeURIComponent(bucket.name)}/o`;
const ownerHeaders = { Authorization: 'Bearer owner' };

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

async function deleteFiles(paths) {
  let deleted = 0;
  for (const path of paths) {
    if (dryRun) {
      deleted += 1;
      continue;
    }
    try {
      await bucket.file(path).delete({ ignoreNotFound: true });
      deleted += 1;
    } catch (error) {
      throw new Error(`Unable to delete storage file "${path}": ${error.message}`);
    }
  }
  return deleted;
}

async function listContractStorageFiles() {
  try {
    const [files] = await bucket.getFiles({ prefix: 'storage-contracts/' });
    return files.map((file) => file.name);
  } catch (error) {
    throw new Error(`Unable to list storage-contracts files: ${error.message}`);
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      ...ownerHeaders,
      ...(options.headers || {})
    }
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(payload?.error?.message || `${response.status} ${response.statusText}`);
  }
  return payload;
}

function firestoreDocumentId(documentName) {
  return String(documentName || '').split('/').pop();
}

function firestoreStringField(doc, fieldName) {
  return doc?.fields?.[fieldName]?.stringValue || '';
}

async function listEmulatorStorageRequests() {
  const documents = [];
  let pageToken = '';
  do {
    const url = new URL(`${firestoreBaseUrl}/storageRequests`);
    if (pageToken) url.searchParams.set('pageToken', pageToken);
    const payload = await fetchJson(url);
    documents.push(...(payload.documents || []));
    pageToken = payload.nextPageToken || '';
  } while (pageToken);
  return documents;
}

async function listEmulatorContractStorageFiles() {
  const files = [];
  let pageToken = '';
  do {
    const url = new URL(storageBaseUrl);
    url.searchParams.set('prefix', 'storage-contracts/');
    if (pageToken) url.searchParams.set('pageToken', pageToken);
    const payload = await fetchJson(url);
    files.push(...(payload.items || []).map((item) => item.name).filter(Boolean));
    pageToken = payload.nextPageToken || '';
  } while (pageToken);
  return files;
}

async function deleteEmulatorFiles(paths) {
  if (dryRun) return paths.length;
  let deleted = 0;
  for (const path of paths) {
    const url = `${storageBaseUrl}/${encodeURIComponent(path)}`;
    const response = await fetch(url, { method: 'DELETE', headers: ownerHeaders });
    if (!response.ok && response.status !== 404) {
      const text = await response.text();
      throw new Error(`Unable to delete storage file "${path}": ${text || response.statusText}`);
    }
    deleted += 1;
  }
  return deleted;
}

async function resetEmulatorStorageRequests(requestDocs) {
  if (dryRun) return requestDocs.length;
  const updateMasks = [
    'status',
    'contractId',
    'contractDraftUrl',
    'contractDraftPath',
    'contractDraftUploadedAt',
    'signedContractUrl',
    'signedContractPath',
    'signedContractUploadedAt',
    'contractEmailSentTo',
    'contractEmailSentBy',
    'contractEmailCc',
    'contractEmailSentAt',
    'updatedAt',
    'updatedBy'
  ];
  const now = new Date().toISOString();
  for (const doc of requestDocs) {
    const requestId = firestoreDocumentId(doc.name);
    const url = new URL(`${firestoreBaseUrl}/storageRequests/${requestId}`);
    updateMasks.forEach((fieldPath) => url.searchParams.append('updateMask.fieldPaths', fieldPath));
    await fetchJson(url, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fields: {
          status: { stringValue: 'new' },
          updatedAt: { timestampValue: now },
          updatedBy: { stringValue: 'reset-storage-contracts-script' }
        }
      })
    });
  }
  return requestDocs.length;
}

async function runEmulatorReset() {
  const requestDocs = await listEmulatorStorageRequests();
  const pathsFromRequests = unique(
    requestDocs.flatMap((doc) => [
      firestoreStringField(doc, 'contractDraftPath'),
      firestoreStringField(doc, 'signedContractPath')
    ])
  );
  const pathsFromStoragePrefix = await listEmulatorContractStorageFiles();
  const pathsToDelete = unique([...pathsFromRequests, ...pathsFromStoragePrefix]);

  console.log(`Storage requests found: ${requestDocs.length}`);
  console.log(`Contract file paths from requests: ${pathsFromRequests.length}`);
  console.log(`Contract files under storage-contracts/: ${pathsFromStoragePrefix.length}`);
  console.log(`Total unique contract files to delete: ${pathsToDelete.length}`);

  const deleted = await deleteEmulatorFiles(pathsToDelete);
  const updated = await resetEmulatorStorageRequests(requestDocs);

  console.log(`${dryRun ? 'Would delete' : 'Deleted'} contract files: ${deleted}`);
  console.log(`${dryRun ? 'Would reset' : 'Reset'} storage requests to new: ${updated}`);
}

async function resetStorageRequests(requestDocs) {
  let updated = 0;
  let batch = db.batch();
  let batchCount = 0;

  for (const docSnap of requestDocs) {
    batch.update(docSnap.ref, {
      status: 'new',
      contractId: deleteField,
      contractDraftUrl: deleteField,
      contractDraftPath: deleteField,
      contractDraftUploadedAt: deleteField,
      signedContractUrl: deleteField,
      signedContractPath: deleteField,
      signedContractUploadedAt: deleteField,
      contractEmailSentTo: deleteField,
      contractEmailSentBy: deleteField,
      contractEmailCc: deleteField,
      contractEmailSentAt: deleteField,
      updatedAt: serverTimestamp,
      updatedBy: 'reset-storage-contracts-script'
    });
    batchCount += 1;
    updated += 1;

    if (batchCount === 450) {
      if (!dryRun) await batch.commit();
      batch = db.batch();
      batchCount = 0;
    }
  }

  if (batchCount > 0 && !dryRun) {
    await batch.commit();
  }

  return updated;
}

async function main() {
  console.log(`Profile: ${profileName} (${useEmulators ? 'emulator' : 'production'})`);
  console.log(`Project: ${projectId}`);
  console.log(`Storage bucket: ${bucket.name}`);
  if (dryRun) console.log('Mode: dry-run');

  if (useEmulators) {
    await runEmulatorReset();
    return;
  }

  const requestSnapshot = await db.collection('storageRequests').get();
  const requestDocs = requestSnapshot.docs;
  const pathsFromRequests = unique(
    requestDocs.flatMap((docSnap) => {
      const data = docSnap.data() || {};
      return [data.contractDraftPath, data.signedContractPath];
    })
  );
  const pathsFromStoragePrefix = await listContractStorageFiles();
  const pathsToDelete = unique([...pathsFromRequests, ...pathsFromStoragePrefix]);

  console.log(`Storage requests found: ${requestDocs.length}`);
  console.log(`Contract file paths from requests: ${pathsFromRequests.length}`);
  console.log(`Contract files under storage-contracts/: ${pathsFromStoragePrefix.length}`);
  console.log(`Total unique contract files to delete: ${pathsToDelete.length}`);

  const deleted = await deleteFiles(pathsToDelete);
  const updated = await resetStorageRequests(requestDocs);

  console.log(`${dryRun ? 'Would delete' : 'Deleted'} contract files: ${deleted}`);
  console.log(`${dryRun ? 'Would reset' : 'Reset'} storage requests to new: ${updated}`);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
