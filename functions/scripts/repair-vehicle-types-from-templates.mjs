import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

const KNOWN_TYPES = [
  { key: 'rv', value: 'RV/Motorhome', labels: { en: 'RV/Motorhome', fr: 'VR/Camping-car' }, order: 0 },
  { key: 'car', value: 'Car', labels: { en: 'Car', fr: 'Voiture' }, order: 1 },
  { key: 'truck', value: 'Truck', labels: { en: 'Truck', fr: 'Camion' }, order: 2 },
  { key: 'motorcycle', value: 'Motorcycle', labels: { en: 'Motorcycle', fr: 'Motocyclette' }, order: 3 },
  { key: 'spyder', value: 'Can-Am Spyder', labels: { en: 'Can-Am Spyder', fr: 'Can-Am Spyder' }, order: 4 },
  { key: 'snowmobile', value: 'Snowmobile', labels: { en: 'Snowmobile', fr: 'Motoneige' }, order: 5 },
  {
    key: 'snowmobile_single',
    value: 'Snowmobile + single trailer',
    labels: { en: 'Snowmobile + single trailer', fr: 'Motoneige + remorque simple' },
    order: 6
  },
  {
    key: 'snowmobile_double',
    value: 'Snowmobile + double trailer',
    labels: { en: 'Snowmobile + double trailer', fr: 'Motoneige + remorque double' },
    order: 7
  },
  { key: 'other', value: 'Other', labels: { en: 'Other', fr: 'Autre' }, order: 8 }
];

const KNOWN_BY_KEY = new Map(KNOWN_TYPES.map((entry) => [entry.key, entry]));

const slugify = (value = '') =>
  value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-');

const detectKey = (labels = []) => {
  const joined = labels.join(' ').toLowerCase();
  if (!joined) return null;
  if (joined.includes('spyder')) return 'spyder';
  if (joined.includes('snowmobile') || joined.includes('motoneige')) {
    if (joined.includes('double')) return 'snowmobile_double';
    if (joined.includes('single') || joined.includes('simple')) return 'snowmobile_single';
    return 'snowmobile';
  }
  if (
    joined.includes('trailer') ||
    joined.includes('remorque') ||
    joined.includes('motorhome') ||
    joined.includes('motorise') ||
    joined.includes('motorisé') ||
    joined.includes(' rv') ||
    joined.includes('vr')
  ) {
    return 'rv';
  }
  if (
    joined.includes('motorcycle') ||
    joined.includes('motocyclette') ||
    joined.includes(' moto')
  ) {
    return 'motorcycle';
  }
  if (joined.includes('other') || joined.includes('autre')) return 'other';
  if (
    joined.includes('car') ||
    joined.includes('truck') ||
    joined.includes('voiture') ||
    joined.includes('camion')
  ) {
    return 'car_truck';
  }
  return null;
};

async function main() {
  const templateSnap = await db.collection('offerTemplates').get();
  if (templateSnap.empty) {
    console.log('No offerTemplates found.');
    return;
  }

  const labelsByTypeId = new Map();
  templateSnap.forEach((docSnap) => {
    const data = docSnap.data() || {};
    const types = Array.isArray(data.vehicleTypes) ? data.vehicleTypes : [];
    const labelEn = data.label?.en || docSnap.id;
    const labelFr = data.label?.fr || '';
    types.forEach((id) => {
      if (!id) return;
      if (!labelsByTypeId.has(id)) labelsByTypeId.set(id, new Set());
      const bucket = labelsByTypeId.get(id);
      bucket.add(labelEn);
      if (labelFr) bucket.add(labelFr);
    });
  });

  if (!labelsByTypeId.size) {
    console.log('No vehicle types referenced in offerTemplates.');
    return;
  }

  const typeAssignments = new Map();
  const carTruckIds = [];
  const unknownIds = [];

  for (const [id, labelSet] of labelsByTypeId.entries()) {
    const labels = Array.from(labelSet).filter(Boolean);
    const key = detectKey(labels);
    if (!key) {
      unknownIds.push({ id, labels });
      continue;
    }
    if (key === 'car_truck') {
      carTruckIds.push(id);
      continue;
    }
    typeAssignments.set(id, key);
  }

  carTruckIds.sort();
  if (carTruckIds.length) {
    const [first, second, ...rest] = carTruckIds;
    if (first) typeAssignments.set(first, 'car');
    if (second) typeAssignments.set(second, 'truck');
    rest.forEach((id) => typeAssignments.set(id, 'car'));
  }

  if (unknownIds.length) {
    console.log('Unclassified vehicle type ids:', unknownIds);
  }

  const batch = db.batch();
  let updates = 0;
  typeAssignments.forEach((key, id) => {
    const entry = KNOWN_BY_KEY.get(key);
    if (!entry) return;
    const payload = {
      value: entry.value,
      labels: entry.labels,
      slug: slugify(entry.value),
      order: entry.order,
      legacyValues: [entry.value],
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    };
    batch.set(db.collection('vehicleTypes').doc(id), payload, { merge: true });
    updates += 1;
  });

  if (updates) {
    await batch.commit();
  }
  console.log(`Updated ${updates} vehicleTypes documents.`);
}

main().catch((err) => {
  console.error('Vehicle type repair failed', err);
  process.exit(1);
});
