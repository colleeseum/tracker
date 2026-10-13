import fs from 'node:fs/promises';
import path from 'node:path';
import admin from 'firebase-admin';
import { serializeData } from './utils/firestore-serialization.mjs';

if (process.env.FIRESTORE_EMULATOR_HOST !== '127.0.0.1:8081') throw new Error('This cleanup is restricted to the local emulator.');
admin.initializeApp({ projectId: 'tracker-187c5' });
const db = admin.firestore();
// Exact incomplete test-only IDs verified on 2026-10-13. Never select customer
// records or a collection-wide deletion based on a general missing-field filter.
const ids = [
  '69Ed36cCsn8ZT6bV1Y93', 'EfbOOlV8Hg3GAyyNlDo3', 'M0svFQwOnttqWxa5x8KQ',
  'ZgG0iVfdFgTik42JRce4', 'dx8s5Py9fXB2QZ6RZ2ct', 'fBEXubbzrQkLwnyXDdw6',
  'gedjX8xcHMaiJjZlz4KY', 'gqxdyqcZ9RFLcbbRConD', 'oIglXB0irvEUv7PyTXMH'
];
const allowedFields = new Set(['caseId', 'storageLocation', 'updatedAt', 'updatedBy']);
function assertArtifact(snap) {
  if (!snap.exists) return;
  const data = snap.data();
  if (Object.keys(data).some((key) => !allowedFields.has(key)) || !['no-season__no-client', 'referral-test__referral-test-tenant'].includes(data.caseId)) {
    throw new Error(`Refusing to remove ${snap.id}: it is no longer the verified incomplete test artifact.`);
  }
}
const before = await db.collection('storageRequests').get();
const complete = before.docs.filter((snap) => snap.data().clientId && snap.data().season && snap.data().vehicle);
const targets = await Promise.all(ids.map((id) => db.collection('storageRequests').doc(id).get()));
targets.forEach(assertArtifact);
for (const snap of targets.filter((snap) => snap.exists)) {
  if ((await snap.ref.listCollections()).length) throw new Error(`Refusing to remove ${snap.id}: it has subcollections.`);
}
const backupPath = path.resolve('backups', `local-test-artifacts-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
await fs.mkdir(path.dirname(backupPath), { recursive: true });
await fs.writeFile(backupPath, JSON.stringify({ projectId: 'tracker-187c5', environment: 'local emulator', createdAt: new Date().toISOString(), documents: targets.filter((snap) => snap.exists).map((snap) => ({ path: snap.ref.path, data: serializeData(snap.data()) })) }, null, 2), { flag: 'wx' });
const removed = await db.runTransaction(async (transaction) => {
  const fresh = await Promise.all(targets.map((snap) => transaction.get(snap.ref)));
  fresh.forEach(assertArtifact);
  fresh.forEach((snap, index) => {
    if (snap.exists && (!targets[index].exists || !snap.updateTime.isEqual(targets[index].updateTime))) throw new Error(`Artifact ${snap.id} changed after backup. Cleanup cancelled.`);
  });
  fresh.filter((snap) => snap.exists).forEach((snap) => transaction.delete(snap.ref));
  return fresh.filter((snap) => snap.exists).length;
});
const after = await db.collection('storageRequests').get();
for (const original of complete) {
  const current = after.docs.find((snap) => snap.id === original.id);
  if (!current || JSON.stringify(current.data()) !== JSON.stringify(original.data())) throw new Error(`Original request ${original.id} changed during cleanup. Review required.`);
}
const incomplete = after.docs.filter((snap) => !snap.data().clientId || !snap.data().season || !snap.data().vehicle);
console.log(JSON.stringify({ removedLocalTestArtifacts: removed, originalRequestsPreserved: complete.length, remainingRequests: after.size, incompleteRemaining: incomplete.length, backupPath }, null, 2));
