import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

const MAX_BATCH_SIZE = 400;

function chunk(items, size = MAX_BATCH_SIZE) {
  const result = [];
  for (let i = 0; i < items.length; i += size) {
    result.push(items.slice(i, i + size));
  }
  return result;
}

async function commitOperations(ops, handler) {
  const batches = chunk(ops);
  for (const batchOps of batches) {
    const batch = db.batch();
    batchOps.forEach((payload) => handler(batch, payload));
    await batch.commit();
  }
}

function arraysEqual(a, b) {
  if (a === b) return true;
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

async function main() {
  const vehicleTypesSnap = await db.collection('vehicleTypes').get();
  if (vehicleTypesSnap.empty) {
    console.log('No vehicleTypes documents found.');
    return;
  }

  const idMap = new Map();
  const newDocs = [];

  vehicleTypesSnap.docs.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const normalizedValue =
      (typeof data.value === 'string' && data.value.trim()) ||
      (typeof data.type === 'string' && data.type.trim()) ||
      docSnap.id;
    const newRef = db.collection('vehicleTypes').doc();
    idMap.set(docSnap.id, newRef.id);
    newDocs.push({
      ref: newRef,
      data: {
        ...data,
        value: normalizedValue,
        legacyId: docSnap.id
      }
    });
  });

  await commitOperations(newDocs, (batch, payload) => {
    batch.set(payload.ref, payload.data);
  });
  console.log(`Created ${newDocs.length} vehicleTypes documents with new IDs.`);

  const offersSnap = await db.collection('storageOffers').get();
  const offerUpdates = [];
  offersSnap.docs.forEach((docSnap) => {
    const offer = docSnap.data() || {};
    const existing = Array.isArray(offer.vehicleTypes) ? offer.vehicleTypes : [];
    if (!existing.length) return;
    const remapped = existing.map((id) => idMap.get(id) || id);
    if (!arraysEqual(existing, remapped)) {
      offerUpdates.push({ ref: docSnap.ref, vehicleTypes: remapped });
    }
  });

  if (offerUpdates.length) {
    await commitOperations(offerUpdates, (batch, payload) => {
      batch.update(payload.ref, { vehicleTypes: payload.vehicleTypes });
    });
    console.log(`Updated ${offerUpdates.length} storageOffers documents with new vehicle type IDs.`);
  } else {
    console.log('No storageOffers documents required vehicle type ID updates.');
  }

  const deleteOps = vehicleTypesSnap.docs.map((docSnap) => ({ ref: docSnap.ref }));
  await commitOperations(deleteOps, (batch, payload) => {
    batch.delete(payload.ref);
  });
  console.log(`Deleted ${deleteOps.length} legacy vehicleTypes documents.`);
}

main().catch((err) => {
  console.error('Failed to normalize vehicle type document IDs', err);
  process.exit(1);
});
