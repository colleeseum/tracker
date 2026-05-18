import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

const UPDATES = {
  'contractSection.eyebrow': {
    en: 'Storage requests',
    fr: 'Demandes d’entreposage'
  },
  'contractSection.heading': {
    en: 'Submit a storage request in minutes',
    fr: 'Soumettez une demande d’entreposage en quelques minutes'
  },
  'contractSection.description': {
    en: 'Send your storage request and we will prepare the agreement for signature.',
    fr: 'Envoyez votre demande d’entreposage et nous preparerons le contrat pour signature.'
  },
  'contractHelper.title': {
    en: 'Storage request',
    fr: 'Demande d’entreposage'
  },
  'contractHelper.intro': {
    en:
      'Use the form below to request storage. We will follow up with the agreement once the request is reviewed. Questions or changes? <a href="mailto:storage@as-colle.com" data-contact-email>storage@as-colle.com</a>.',
    fr:
      'Utilisez le formulaire ci-dessous pour faire une demande d’entreposage. Nous vous enverrons le contrat une fois la demande revue. Questions ou modifications? <a href="mailto:storage@as-colle.com" data-contact-email>storage@as-colle.com</a>.'
  },
  'contractHelper.readerNote': {
    en: 'Submit one request per vehicle so we can prepare the right agreement and insurance checklist for you.',
    fr:
      'Soumettez une demande par vehicule afin que nous puissions preparer le bon contrat et la liste d’assurance pour vous.'
  },
  'contractHelper.mobileNote': {
    en: 'We will confirm availability and send the agreement by email.',
    fr: 'Nous confirmerons la disponibilite et enverrons le contrat par courriel.'
  },
  'form.preview': {
    en: 'Submit request',
    fr: 'Soumettre la demande'
  },
  'form.previewHint': {
    en: 'We will review your request and follow up by email.',
    fr: 'Nous reverrons votre demande et ferons un suivi par courriel.'
  }
};

async function main() {
  const batch = db.batch();
  Object.entries(UPDATES).forEach(([key, text]) => {
    const ref = db.collection('i18nEntries').doc(key);
    batch.set(
      ref,
      {
        key,
        text,
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      },
      { merge: true }
    );
  });
  await batch.commit();
  console.log(`Updated ${Object.keys(UPDATES).length} i18n entries.`);
}

main().catch((err) => {
  console.error('Update failed', err);
  process.exit(1);
});

