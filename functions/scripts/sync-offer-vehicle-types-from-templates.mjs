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
  const templateSnap = await db.collection('offerTemplates').get();
  if (templateSnap.empty) {
    console.log('No offerTemplates documents found.');
    return;
  }

  const templateMap = new Map();
  templateSnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const types = Array.isArray(data.vehicleTypes) ? data.vehicleTypes : [];
    templateMap.set(docSnap.id, types);
  });

  const offersSnap = await db.collection('storageOffers').get();
  if (offersSnap.empty) {
    console.log('No storageOffers documents found.');
    return;
  }

  const updates = [];
  offersSnap.forEach((docSnap) => {
    const offer = docSnap.data() || {};
    const templateId = offer.templateId;
    if (!templateId) return;
    const templateTypes = templateMap.get(templateId);
    if (!templateTypes) return;
    const existing = Array.isArray(offer.vehicleTypes) ? offer.vehicleTypes : [];
    if (arraysEqual(existing, templateTypes)) return;
    updates.push({ ref: docSnap.ref, vehicleTypes: templateTypes });
  });

  if (!updates.length) {
    console.log('No storageOffers documents required vehicle type updates.');
    return;
  }

  await commitOperations(updates, (batch, payload) => {
    batch.update(payload.ref, { vehicleTypes: payload.vehicleTypes });
  });
  console.log(`Updated ${updates.length} storageOffers documents.`);
}

main().catch((err) => {
  console.error('Failed to sync offer vehicle types', err);
  process.exit(1);
});
