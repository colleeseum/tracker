import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import admin from 'firebase-admin';
import { createClientEmailBatchService } from '../lib/client-email-batches.js';
import { createEmailSafetyPolicy, createSafeMailer } from '../lib/email-safety.js';

if (!/^(127\.0\.0\.1|localhost):\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST || '')) throw new Error('This test requires an explicitly configured local Firestore emulator.');
admin.initializeApp({ projectId: 'tracker-187c5' });
const db = admin.firestore();
const prefix = `mailing-test-${crypto.randomUUID()}`;
const ids = [`${prefix}-a`, `${prefix}-b`];
const messages = [];
const policy = createEmailSafetyPolicy({ FUNCTIONS_EMULATOR: 'true' });
const mockMailer = createSafeMailer({ sendMail: async (message) => { messages.push(message); return { accepted: [message.to] }; } }, policy);
const service = createClientEmailBatchService({ db, fieldValue: admin.firestore.FieldValue, from: 'test@example.invalid',
  routeEmail: policy.preview, sendEmail: (message) => mockMailer.sendMail(message), renderEmail: ({ body }) => `<p>${body}</p>` });
let batchId;
try {
  await Promise.all(ids.map((id, index) => db.collection('clients').doc(id).set({ name: `Mailing integration test ${index}`, email: `client-${index}@example.invalid`, emailCc: `cc-${index}@example.invalid`, preferredLanguage: index ? 'en' : 'fr', testFixture: true })));
  const preview = await service.prepare({ clientIds: ids, subject: 'Local integration test', body: 'No real email is sent.' }, 'local-test');
  batchId = preview.batchId;
  assert.equal(messages.length, 0);
  const payload = { batchId, clientId: ids[0], fingerprint: preview.fingerprint };
  await Promise.all([service.send(payload, 'local-test'), service.send(payload, 'local-test')]);
  await service.send({ ...payload, clientId: ids[1] }, 'local-test');
  const resumed = await service.get(batchId);
  assert.ok(resumed.recipients.every((recipient) => recipient.status === 'sent'));
  await service.send(payload, 'local-test');
  assert.equal(messages.length, 2);
  assert.ok(messages.every((message) => message.to === 'entrepot@as-colle.com' && !message.cc && message.envelope.to.length === 1));
  console.log('Local Firestore mailing test passed: two separate mock deliveries, persisted results, safe dev routing, no duplicate on retry. No real email sent.');
} finally {
  const cleanup = db.batch();
  ids.forEach((id) => {
    cleanup.delete(db.collection('clients').doc(id));
    if (batchId) cleanup.delete(db.collection('clientEmails').doc(`${batchId}_${id}`));
  });
  if (batchId) cleanup.delete(db.collection('clientEmailBatches').doc(batchId));
  await cleanup.commit();
  await db.terminate();
}
