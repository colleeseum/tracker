import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId } = await loadActiveProfile();
admin.initializeApp({ projectId });
const db = admin.firestore();
const snapshot = await db.collection('offerTemplates').get();
if (snapshot.empty) {
  console.log('No templates found');
} else {
  snapshot.forEach((doc) => console.log(doc.id, doc.data()));
}
