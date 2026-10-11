import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';
import { reconcileOfferNotes, inheritedOfferNotes } from '../lib/offer-notes.js';

const { projectId, useEmulators } = await loadActiveProfile();
const apply = process.argv.includes('--apply');
if (apply && !useEmulators) throw new Error('Production review is read-only. Apply reviewed changes through Tracker.');
admin.initializeApp({ projectId });
const db = admin.firestore();
if (!useEmulators) db.settings({ preferRest: true });
const [templatesSnap, offersSnap] = await Promise.all([db.collection('offerTemplates').get(), db.collection('storageOffers').get()]);
const templates = new Map(templatesSnap.docs.map((snap) => [snap.id, snap.data()]));
const offers = offersSnap.docs.map((snap) => ({ id: snap.id, ...snap.data() }));
const changes = [];
const review = [];
for (const [id, template] of templates) {
  const associated = offers.filter((offer) => offer.templateId === id);
  if (!associated.length) continue;
  const proposals = associated.map((offer) => reconcileOfferNotes(template.note, offer.price, template.noteCustom));
  const unique = [...new Set(proposals.map((note) => JSON.stringify(note)))];
  if (unique.length !== 1) {
    review.push({ templateId: id, reason: 'Shared template has different pricing modes. Defaults are normalized per offer at export.', offerIds: associated.map((offer) => offer.id) });
    continue;
  }
  const note = proposals[0];
  if (JSON.stringify(note) !== JSON.stringify(template.note || {})) {
    changes.push({ collection: 'offerTemplates', id, before: template.note || {}, note });
    templates.set(id, { ...template, note });
  }
}
const defaults = new Set(['', 'flat rate', 'tarif fixe', 'minimum {{amount}}']);
for (const offer of offers) {
  const template = templates.get(offer.templateId);
  const note = inheritedOfferNotes(offer, template);
  if (['en', 'fr'].every((language) => (offer.note?.[language] || '') === note[language])) continue;
  const divergentCustom = ['en', 'fr'].some((language) => {
    const current = offer.note?.[language] || '';
    return current !== note[language] && (offer.noteCustom?.[language] || !defaults.has(current.trim().toLowerCase()));
  });
  if (divergentCustom) {
    review.push({ offerId: offer.id, templateId: offer.templateId, reason: 'Different custom offer note requires manual review; not overwritten.' });
    continue;
  }
  changes.push({ collection: 'storageOffers', id: offer.id, before: offer.note || {}, note });
}
console.log(JSON.stringify({ environment: useEmulators ? 'local' : 'production', apply, changes, review }, null, 2));
if (apply) {
  for (let offset = 0; offset < changes.length; offset += 400) {
    const batch = db.batch();
    changes.slice(offset, offset + 400).forEach((change) => batch.update(db.collection(change.collection).doc(change.id), { note: change.note, updatedAt: admin.firestore.FieldValue.serverTimestamp() }));
    await batch.commit();
  }
  console.log(`Applied ${changes.length} note-only updates. Prices and restrictions unchanged.`);
}
