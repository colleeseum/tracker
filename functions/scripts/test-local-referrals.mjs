import assert from 'node:assert/strict';
import admin from 'firebase-admin';

if (process.env.FIRESTORE_EMULATOR_HOST !== '127.0.0.1:8081') throw new Error('Local emulator required.');
admin.initializeApp({ projectId: 'tracker-187c5' });
const db = admin.firestore();
const requestRef = db.collection('storageRequests').doc();
// Isolated disposable fixture, not a real customer request. No email workflow runs.
try {
  await requestRef.set({ clientId: 'referral-test-tenant', season: 'referral-test', status: 'new', contractAmount: 300, referredByClientIds: ['referral-test-alice', 'referral-test-bob'] });
  let request = (await requestRef.get()).data();
  assert.deepEqual(request.referredByClientIds, ['referral-test-alice', 'referral-test-bob']);
  await requestRef.set({ referredByClientIds: ['referral-test-bob'] }, { merge: true });
  request = (await requestRef.get()).data();
  assert.deepEqual(request.referredByClientIds, ['referral-test-bob']);
  assert.equal(request.contractAmount, 300);
  assert.equal(request.status, 'new');
  await requestRef.set({ referredByClientIds: [] }, { merge: true });
  assert.deepEqual((await requestRef.get()).data().referredByClientIds, []);
  console.log('Multiple referrals persist; editing/removal preserves existing request fields.');
} finally {
  await requestRef.delete();
  console.log('Disposable local referral test request removed.');
}
