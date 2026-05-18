import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

const slugify = (value = '') =>
  value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-');

async function main() {
  const [addonSnap, seasonSnap] = await Promise.all([
    db.collection('storageAddOns').get(),
    db.collection('storageSeasons').get()
  ]);

  if (addonSnap.empty) {
    console.log('No storageAddOns documents found.');
    return;
  }

  const seasons = seasonSnap.docs
    .map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }))
    .filter((season) => season.active !== false)
    .sort((a, b) => (a.order || 0) - (b.order || 0));
  const fallbackSeasonIds = seasons.map((season) => season.id);

  const batch = db.batch();
  let definitions = 0;
  let seasonPrices = 0;
  let deletedLegacyDocs = 0;

  addonSnap.docs.forEach((docSnap) => {
    const addon = docSnap.data() || {};
    const code = addon.code || docSnap.id;
    if (!code) return;
    const definitionRef = db.collection('storageAddOns').doc(code);
    const definitionPayload = {
      code,
      name: addon.name || { en: '', fr: '' },
      description: addon.description || { en: '', fr: '' },
      order: typeof addon.order === 'number' ? addon.order : 0,
      price: admin.firestore.FieldValue.delete(),
      seasonId: admin.firestore.FieldValue.delete(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedBy: addon.updatedBy || null
    };
    if (addon.createdAt) {
      definitionPayload.createdAt = addon.createdAt;
    }
    if (addon.createdBy) {
      definitionPayload.createdBy = addon.createdBy;
    }
    batch.set(definitionRef, definitionPayload, { merge: true });
    definitions += 1;

    const price = Number(addon.price);
    const targetSeasonIds = addon.seasonId ? [addon.seasonId] : fallbackSeasonIds;
    if (Number.isFinite(price)) {
      targetSeasonIds.forEach((seasonId) => {
        if (!seasonId) return;
        const priceId = slugify(`${seasonId}-${code}`) || `${seasonId}-${code}`;
        batch.set(
          db.collection('storageSeasonAddOns').doc(priceId),
          {
            seasonId,
            addonId: code,
            code,
            price,
            order: typeof addon.order === 'number' ? addon.order : 0,
            createdAt: addon.createdAt || admin.firestore.FieldValue.serverTimestamp(),
            createdBy: addon.createdBy || null,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            updatedBy: addon.updatedBy || null
          },
          { merge: true }
        );
        seasonPrices += 1;
      });
    }

    if (docSnap.id !== code) {
      batch.delete(docSnap.ref);
      deletedLegacyDocs += 1;
    }
  });

  await batch.commit();
  console.log(
    `Migrated ${definitions} add-on definitions and ${seasonPrices} season prices. Deleted ${deletedLegacyDocs} legacy add-on docs.`
  );
}

main().catch((err) => {
  console.error('Add-on migration failed', err);
  process.exit(1);
});
