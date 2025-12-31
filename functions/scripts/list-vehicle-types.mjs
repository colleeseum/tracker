import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

const snap = await db.collection('vehicleTypes').get();
console.log('Docs:', snap.docs.length);
snap.docs.forEach((doc) => console.log(doc.id, doc.data()));
