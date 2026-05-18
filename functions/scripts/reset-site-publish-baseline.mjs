import admin from 'firebase-admin';
import { loadActiveProfile } from '../../lib/profile-config.js';

const { projectId, profileName, useEmulators } = await loadActiveProfile();
const apply = process.argv.includes('--apply');

admin.initializeApp({ projectId });
const db = admin.firestore();

async function main() {
  const ref = db.collection('admin').doc('sitePublish');
  const snapshot = await ref.get();
  const data = snapshot.exists ? snapshot.data() || {} : {};
  const hasPublishedSnapshot = data.lastPublishedSnapshot && Object.keys(data.lastPublishedSnapshot).length > 0;

  console.log(`Profile: ${profileName}${useEmulators ? ' (emulator)' : ' (production)'}`);
  console.log(`Project: ${projectId}`);
  console.log(`admin/sitePublish exists: ${snapshot.exists}`);
  console.log(`lastPublishedSnapshot present: ${hasPublishedSnapshot}`);

  if (!apply) {
    console.log('Dry run only. Re-run with --apply to reset the publish baseline.');
    return;
  }

  await ref.set(
    {
      lastPublishedSnapshot: {},
      lastPublishedAt: null,
      lastRequestedBy: null,
      lastRequestedByUid: null,
      lastChangeReference: null,
      lastPublishDryRun: false,
      lastPublishHistoryId: null,
      resetAt: admin.firestore.FieldValue.serverTimestamp(),
      resetReason: 'Force current website content to appear unpublished'
    },
    { merge: true }
  );

  console.log('Reset complete. Current website/offering content should now appear as pending publish.');
}

main().catch((err) => {
  console.error(`Publish baseline reset failed: ${err.message || err}`);
  process.exit(1);
});
