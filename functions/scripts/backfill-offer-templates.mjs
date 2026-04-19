import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

function slugify(value = '') {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-')
    .slice(0, 64);
}

function buildTemplateKey(offer) {
  const labelEn = offer?.label?.en?.trim() || '';
  const labelFr = offer?.label?.fr?.trim() || '';
  const vehicleTypes = Array.isArray(offer?.vehicleTypes) ? offer.vehicleTypes.join('|') : '';
  const noteEn = offer?.note?.en?.trim() || '';
  const noteFr = offer?.note?.fr?.trim() || '';
  const hidden = offer?.hideInTable ? '1' : '0';
  return [labelEn, labelFr, vehicleTypes, noteEn, noteFr, hidden].join('::');
}

async function ensureTemplate(offer, templateKeyMap, usedTemplateIds) {
  const key = buildTemplateKey(offer);
  if (templateKeyMap.has(key)) {
    return templateKeyMap.get(key);
  }
  const labelEn = offer?.label?.en?.trim() || 'Offer';
  const baseSlug = slugify(labelEn) || `template-${templateKeyMap.size + 1}`;
  let slug = baseSlug;
  let attempt = 1;
  while (usedTemplateIds.has(slug)) {
    slug = `${baseSlug}-${attempt++}`;
  }
  const templateRef = db.collection('offerTemplates').doc(slug);
  const payload = {
    label: {
      en: offer?.label?.en?.trim() || '',
      fr: offer?.label?.fr?.trim() || ''
    },
    vehicleTypes: Array.isArray(offer?.vehicleTypes)
      ? offer.vehicleTypes.filter(Boolean)
      : [],
    note: {
      en: offer?.note?.en?.trim() || '',
      fr: offer?.note?.fr?.trim() || ''
    },
    hideInTable: Boolean(offer?.hideInTable),
    order: Number.isFinite(offer?.order) ? offer.order : 0,
    createdAt: offer?.createdAt || admin.firestore.FieldValue.serverTimestamp(),
    createdBy: offer?.createdBy || null,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedBy: null
  };
  await templateRef.set(payload, { merge: true });
  templateKeyMap.set(key, templateRef.id);
  usedTemplateIds.add(templateRef.id);
  return templateRef.id;
}

async function backfill() {
  console.log(`Loading offers from ${projectId}...`);
  const templateSnapshot = await db.collection('offerTemplates').get();
  const templateKeyMap = new Map();
  const usedTemplateIds = new Set();
  templateSnapshot.forEach((docSnap) => {
    usedTemplateIds.add(docSnap.id);
    const templateData = docSnap.data() || {};
    const key = buildTemplateKey(templateData);
    if (key) {
      templateKeyMap.set(key, docSnap.id);
    }
  });

  const snapshot = await db.collection('storageOffers').get();
  if (snapshot.empty) {
    console.log('No offers found.');
    return;
  }
  const updates = [];
  for (const docSnap of snapshot.docs) {
    const offer = docSnap.data();
    const templateId = await ensureTemplate(offer, templateKeyMap, usedTemplateIds);
    if (offer.templateId === templateId) continue;
    updates.push({ id: docSnap.id, templateId });
  }

  console.log(`Updating ${updates.length} offers with template references...`);
  const batch = db.batch();
  updates.forEach(({ id, templateId }) => {
    batch.update(db.collection('storageOffers').doc(id), {
      templateId,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    });
  });
  await batch.commit();
  console.log('Backfill complete.');
}

backfill()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('Backfill failed', err);
    process.exit(1);
  });
