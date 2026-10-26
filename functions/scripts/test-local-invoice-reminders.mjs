import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import admin from 'firebase-admin';
import { createStorageInvoiceReminderService } from '../lib/storage-invoice-reminders.js';
import { renderInvoicePdf } from '../lib/storage-invoices.js';
import { mergeClientEmailCc } from '../lib/client-email-cc.js';
import { createEmailSafetyPolicy, createSafeMailer } from '../lib/email-safety.js';

if (!/^(127\.0\.0\.1|localhost):\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST || '')) throw new Error('Explicit local Firestore emulator configuration is required.');
admin.initializeApp({ projectId: 'tracker-187c5' });
const db = admin.firestore();
const id = `INV-${crypto.randomBytes(9).toString('hex').toUpperCase()}`;
const fixtureId = `reminder-test-${crypto.randomUUID()}`;
const ref = db.collection('storageInvoices').doc(id);
const invoice = { invoiceId: id, date: '2026-10-26', locale: 'en', clientId: fixtureId, client: { name: 'Reminder test', email: 'test@example.invalid' },
  requestIds: [fixtureId], seasonId: 'winter', lineItems: [{ description: 'Test vehicle', amountCents: 46000 }], subtotalCents: 46000, depositCents: 10000,
  balanceDueCents: 36000, referralDiscountCents: 0, creditAllocations: [] };
const pdfBase64 = (await renderInvoicePdf(invoice)).toString('base64');
const messages = [];
const policy = createEmailSafetyPolicy({ FUNCTIONS_EMULATOR: 'true' });
const mailer = createSafeMailer({ sendMail: async (message) => { messages.push(message); return { accepted: [message.to] }; } }, policy);
const service = createStorageInvoiceReminderService({ db, fieldValue: admin.firestore.FieldValue, from: 'test@example.invalid',
  renderEmail: async () => ({ subject: 'Friendly reminder', text: 'Reminder test', html: '<p>Reminder test</p>', templateId: 'storage-invoice-reminder' }),
  ccForLocale: (_locale, client) => mergeClientEmailCc('office@example.invalid', client), routeEmail: policy.preview, sendEmail: (message) => mailer.sendMail(message) });
let reminderId;
try {
  await Promise.all([
    ref.set({ invoice, status: 'sent', preview: { pdfBase64 }, testFixture: true }),
    db.collection('clients').doc(fixtureId).set({ email: 'test@example.invalid', emailCc: 'partner@example.invalid', testFixture: true }),
    db.collection('storageRequests').doc(fixtureId).set({ clientId: fixtureId, status: 'stored', invoiceId: id, testFixture: true })
  ]);
  const before = (await ref.get()).data();
  const preview = await service.prepare(id); reminderId = preview.reminderId;
  assert.equal(messages.length, 0);
  assert.deepEqual((await ref.get()).data(), before);
  await service.send(preview, 'local-test');
  await service.send(preview, 'local-test');
  const after = (await ref.get()).data();
  assert.equal(messages.length, 1);
  assert.equal(messages[0].attachments[0].content.toString('base64'), pdfBase64);
  assert.equal(messages[0].to, 'entrepot@as-colle.com');
  assert.deepEqual(messages[0].envelope.to, ['entrepot@as-colle.com']);
  assert.equal(after.status, 'sent'); assert.deepEqual(after.invoice, before.invoice);
  assert.equal(after.preview.pdfBase64, pdfBase64);
  assert.equal((await db.collection('storageRequests').doc(fixtureId).get()).data().status, 'stored');
  assert.equal((await db.collection('expenses').doc(`storage-payment-${id}`).get()).exists, false);
  assert.equal((await ref.collection('reminders').doc(reminderId).get()).data().status, 'sent');
  console.log('Local reminder integration passed: exact saved PDF, safe dev routing, persisted audit, one mock email on retry, no payment/status/ledger changes. No real email sent.');
} finally {
  const cleanup = db.batch();
  cleanup.delete(ref);
  cleanup.delete(db.collection('clients').doc(fixtureId));
  cleanup.delete(db.collection('storageRequests').doc(fixtureId));
  if (reminderId) cleanup.delete(ref.collection('reminders').doc(reminderId));
  await cleanup.commit();
  await db.terminate();
}
