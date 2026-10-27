import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import admin from 'firebase-admin';
import { renderInvoicePdf } from '../lib/storage-invoices.js';

if (process.env.FIRESTORE_EMULATOR_HOST !== '127.0.0.1:8081') throw new Error('This preview only runs against the local development emulator.');
const actuallySend = process.argv.includes('--send');
const projectId = 'tracker-187c5';
const base = `http://127.0.0.1:5001/${projectId}/us-central1/`;
const encode = (value) => Buffer.from(JSON.stringify(value)).toString('base64url');
const token = `${encode({ alg: 'none', typ: 'JWT' })}.${encode({ sub: 'local-spark-preview', email: 'sergecolle@gmail.com', aud: projectId,
  iss: `https://securetoken.google.com/${projectId}`, iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 3600,
  firebase: { sign_in_provider: 'custom', identities: {} } })}.`;
async function call(name, data) {
  const response = await fetch(base + name, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ data }) });
  const payload = await response.json();
  if (!response.ok || payload.error) throw new Error(payload.error?.message || `${name}: HTTP ${response.status}`);
  return payload.result;
}
const policy = await call('getEmailDeliveryPolicy', {});
assert.equal(policy.development, true); assert.equal(policy.redirectTo, 'entrepot@as-colle.com');
admin.initializeApp({ projectId });
const db = admin.firestore();
const id = `INV-${crypto.randomBytes(9).toString('hex').toUpperCase()}`;
const fixtureId = `spark-template-test-${crypto.randomUUID()}`;
const invoiceRef = db.collection('storageInvoices').doc(id);
let reminderId; let sendingAttempted = false; let completed = false;
try {
  const invoice = { invoiceId: id, date: new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Toronto', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date()),
    locale: 'fr', clientId: fixtureId, client: { name: 'TEST SPARK - aucun paiement requis', email: 'entrepot@as-colle.com' },
    requestIds: [fixtureId], seasonId: 'test', seasonLabel: 'TEST SPARK - AUCUN PAIEMENT REQUIS',
    lineItems: [{ description: 'Facture fictive pour verifier le nouveau bandeau et le pied de page dans Spark. Aucun paiement requis.', amountCents: 10000 }],
    subtotalCents: 10000, depositCents: 0, balanceDueCents: 10000, referralDiscountCents: 0, creditAllocations: [] };
  const pdfBase64 = (await renderInvoicePdf(invoice)).toString('base64');
  await Promise.all([
    invoiceRef.set({ invoice, status: 'sent', preview: { pdfBase64 }, testFixture: true }),
    db.collection('clients').doc(fixtureId).set({ name: invoice.client.name, email: 'entrepot@as-colle.com', preferredLanguage: 'fr', testFixture: true }),
    db.collection('storageRequests').doc(fixtureId).set({ clientId: fixtureId, invoiceId: id, status: 'stored', testFixture: true })
  ]);
  const preview = await call('prepareStorageInvoiceReminder', { invoiceId: id });
  reminderId = preview.reminderId;
  assert.equal(preview.developmentEmailRedirect, true); assert.equal(preview.to, 'entrepot@as-colle.com'); assert.equal(preview.cc, '');
  assert.ok(preview.subject.startsWith('[DEV] ')); assert.match(preview.subject, /TEST SPARK/);
  assert.ok(preview.html.includes('bgcolor="#0f172a" style="background-color:#0f172a;color:#ffffff;padding:20px 24px;'));
  assert.ok(preview.html.includes('bgcolor="#0f172a" style="background-color:#0f172a;color:#94a3b8;padding:16px 24px;'));
  assert.equal(preview.pdfBase64, pdfBase64);
  console.log(`Verified dev-only recipient: ${preview.to}\nSubject: ${preview.subject}`);
  if (actuallySend) {
    sendingAttempted = true;
    const result = await call('sendStorageInvoiceReminder', { invoiceId: id, reminderId, fingerprint: preview.fingerprint });
    assert.equal(result.success, true);
    const audit = (await invoiceRef.collection('reminders').doc(reminderId).get()).data();
    assert.equal(audit.status, 'sent');
    completed = true;
    console.log('SMTP accepted one French test reminder with a clearly fictitious invoice PDF. No customer or production record was used.');
  } else console.log('Preview only. Use --send to deliver one test email.');
} finally {
  if (!sendingAttempted || completed) {
    const cleanup = db.batch(); cleanup.delete(invoiceRef); cleanup.delete(db.collection('clients').doc(fixtureId)); cleanup.delete(db.collection('storageRequests').doc(fixtureId));
    if (reminderId) cleanup.delete(invoiceRef.collection('reminders').doc(reminderId));
    await cleanup.commit();
    console.log('Temporary dev fixture records removed.');
  } else console.log(`Delivery could not be confirmed. Retained dev audit at storageInvoices/${id}/reminders/${reminderId}. Do not resend before checking it.`);
  await db.terminate();
}
