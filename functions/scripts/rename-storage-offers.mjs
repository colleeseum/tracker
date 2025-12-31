import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

const PRESERVED_IDS = new Set(['A1n5SgwNhoILXHYPPck5', 'A1n5SgwNholLXHYPPck5']);

async function migrateOffer(docSnap) {
  const data = docSnap.data() || {};
  const legacyId = docSnap.id;
  if (PRESERVED_IDS.has(legacyId)) {
    console.log(`Skipping preserved offer ${legacyId}`);
    return null;
  }
  const newRef = db.collection('storageOffers').doc();
  const payload = {
    ...data,
    id: data.id || legacyId,
    legacyId
  };
  await newRef.set(payload);
  await docSnap.ref.delete();
  console.log(`Migrated offer ${legacyId} -> ${newRef.id}`);
  return { oldId: legacyId, newId: newRef.id };
}

async function main() {
  const snapshot = await db.collection('storageOffers').get();
  if (snapshot.empty) {
    console.log('No storageOffers documents found.');
    return;
  }

  const migrations = [];
  for (const docSnap of snapshot.docs) {
    if (PRESERVED_IDS.has(docSnap.id)) continue;
    // Skip IDs that already look random (20 char alpha-numeric) unless explicitly listed.
    if (/^[A-Za-z0-9_-]{18,}$/.test(docSnap.id)) {
      console.log(`Skipping existing random ID ${docSnap.id}`);
      continue;
    }
    const result = await migrateOffer(docSnap);
    if (result) migrations.push(result);
  }

  if (!migrations.length) {
    console.log('No storageOffers documents required renaming.');
  } else {
    console.log(`Completed ${migrations.length} storageOffers migrations.`);
  }
}

main().catch((err) => {
  console.error('Failed to rename storageOffers documents', err);
  process.exit(1);
});
