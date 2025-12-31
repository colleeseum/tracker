import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

const MAX_BATCH_SIZE = 400;

const KNOWN_VEHICLE_TYPES = [
  { value: 'RV/Motorhome', labels: { en: 'RV/Motorhome', fr: 'VR/Camping-car' }, order: 0 },
  { value: 'Car', labels: { en: 'Car', fr: 'Voiture' }, order: 1 },
  { value: 'Truck', labels: { en: 'Truck', fr: 'Camion' }, order: 2 },
  { value: 'Motorcycle', labels: { en: 'Motorcycle', fr: 'Motocyclette' }, order: 3 },
  { value: 'Can-Am Spyder', labels: { en: 'Can-Am Spyder', fr: 'Can-Am Spyder' }, order: 4 },
  { value: 'Snowmobile', labels: { en: 'Snowmobile', fr: 'Motoneige' }, order: 5 },
  {
    value: 'Snowmobile + single trailer',
    labels: { en: 'Snowmobile + single trailer', fr: 'Motoneige + remorque simple' },
    order: 6
  },
  {
    value: 'Snowmobile + double trailer',
    labels: { en: 'Snowmobile + double trailer', fr: 'Motoneige + remorque double' },
    order: 7
  },
  { value: 'Other', labels: { en: 'Other', fr: 'Autre' }, order: 8 }
];

const slugify = (input = '') => {
  return input
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-');
};

const knownBySlug = new Map(
  KNOWN_VEHICLE_TYPES.map((entry, index) => {
    const slug = slugify(entry.value);
    return [
      slug,
      {
        value: entry.value,
        labels: entry.labels,
        order: typeof entry.order === 'number' ? entry.order : index,
        slug
      }
    ];
  })
);

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

function buildCanonicalEntry(raw, orderOffset) {
  const trimmed = raw?.trim();
  if (!trimmed) {
    return {
      value: `Vehicle type ${orderOffset + 1}`,
      labels: { en: `Vehicle type ${orderOffset + 1}`, fr: `Type de véhicule ${orderOffset + 1}` },
      order: orderOffset,
      slug: `vehicle-type-${orderOffset + 1}`
    };
  }
  const slug = slugify(trimmed);
  return {
    value: trimmed,
    labels: { en: trimmed, fr: trimmed },
    order: orderOffset,
    slug
  };
}

function normalizeKey(value = '') {
  return slugify(value);
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

async function deleteExistingVehicleTypes() {
  const snap = await db.collection('vehicleTypes').get();
  if (snap.empty) return 0;
  const ops = snap.docs.map((docSnap) => ({ ref: docSnap.ref }));
  await commitOperations(ops, (batch, payload) => batch.delete(payload.ref));
  return ops.length;
}

async function main() {
  const offersSnap = await db.collection('storageOffers').get();
  if (offersSnap.empty) {
    console.log('No storageOffers documents found.');
    return;
  }

  const removed = await deleteExistingVehicleTypes();
  if (removed > 0) {
    console.log(`Deleted ${removed} existing vehicleTypes documents.`);
  }

  const typeBuckets = new Map();
  let fallbackOrder = KNOWN_VEHICLE_TYPES.length;

  offersSnap.docs.forEach((docSnap) => {
    const offer = docSnap.data() || {};
    const vehicleTypes = Array.isArray(offer.vehicleTypes) ? offer.vehicleTypes : [];
    vehicleTypes.forEach((raw) => {
      if (!raw || typeof raw !== 'string') return;
      const normalized = normalizeKey(raw);
      let bucket = typeBuckets.get(normalized);
      if (!bucket) {
        const canonical =
          knownBySlug.get(normalized) || buildCanonicalEntry(raw, fallbackOrder++);
        bucket = {
          normalized,
          canonical,
          variants: new Set(),
          offerIds: new Set()
        };
        typeBuckets.set(normalized, bucket);
      }
      bucket.variants.add(raw.trim());
      bucket.offerIds.add(docSnap.id);
    });
  });

  const vehicleTypesCollection = db.collection('vehicleTypes');
  const variantToDocId = new Map();
  const createOps = [];
  let orderCounter = 0;

  for (const bucket of typeBuckets.values()) {
    const docRef = vehicleTypesCollection.doc();
    const payload = {
      value: bucket.canonical.value,
      labels: bucket.canonical.labels,
      slug: bucket.canonical.slug || bucket.normalized,
      order:
        typeof bucket.canonical.order === 'number'
          ? bucket.canonical.order
          : KNOWN_VEHICLE_TYPES.length + orderCounter,
      legacyValues: Array.from(bucket.variants),
      legacyOfferIds: Array.from(bucket.offerIds),
      createdAt: new Date().toISOString()
    };
    createOps.push({ ref: docRef, data: payload, variants: bucket.variants });
    orderCounter += 1;
  }

  await commitOperations(createOps, (batch, payload) => {
    batch.set(payload.ref, payload.data);
  });
  createOps.forEach((payload) => {
    payload.variants.forEach((variant) => variantToDocId.set(variant, payload.ref.id));
  });

  console.log(`Created ${createOps.length} vehicle type documents.`);

  const updateOps = [];
  offersSnap.docs.forEach((docSnap) => {
    const offer = docSnap.data() || {};
    const vehicleTypes = Array.isArray(offer.vehicleTypes) ? offer.vehicleTypes : [];
    if (!vehicleTypes.length) return;
    const mapped = vehicleTypes
      .map((raw) => variantToDocId.get(raw.trim()))
      .filter(Boolean);
    if (!mapped.length) return;
    if (arraysEqual(vehicleTypes, mapped)) return;
    updateOps.push({
      ref: docSnap.ref,
      vehicleTypes: mapped,
      legacyVehicleTypes: vehicleTypes
    });
  });

  if (updateOps.length) {
    await commitOperations(updateOps, (batch, payload) => {
      batch.update(payload.ref, {
        vehicleTypes: payload.vehicleTypes,
        legacyVehicleTypes: payload.legacyVehicleTypes
      });
    });
  }

  console.log(`Updated ${updateOps.length} storageOffers documents.`);
}

main().catch((err) => {
  console.error('Failed to build vehicle types from offers', err);
  process.exit(1);
});
