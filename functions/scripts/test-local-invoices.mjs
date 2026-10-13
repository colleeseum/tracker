import assert from 'node:assert/strict';
import admin from 'firebase-admin';
import readline from 'node:readline/promises';
import { createStorageInvoiceService, invoiceIdForRequests, referralCreditId } from '../lib/storage-invoices.js';

if (process.env.FIRESTORE_EMULATOR_HOST !== '127.0.0.1:8081') throw new Error('Local emulator required.');
admin.initializeApp({ projectId: 'tracker-187c5' });
const db = admin.firestore();
const settings = db.collection('admin').doc('appSettings');
if ((await settings.get()).data()?.referralDiscountAmount == null) {
  await settings.set({ referralDiscountAmount: 25 }, { merge: true });
  console.log('Initial local referral discount setting saved: CAD 25. No production data changed.');
}
const client = db.collection('clients').doc();
const referred = db.collection('clients').doc();
const vehicles = [db.collection('storageRequests').doc(), db.collection('storageRequests').doc()];
const referredVehicles = [db.collection('storageRequests').doc(), db.collection('storageRequests').doc()];
const receipt = db.collection('storageReceipts').doc();
const ids = vehicles.map((ref) => ref.id);
const invoice = db.collection('storageInvoices').doc(invoiceIdForRequests(ids));
const credit = db.collection('storageReferralCredits').doc(referralCreditId(client.id, referred.id));
const disposable = [invoice, credit, receipt, ...vehicles, ...referredVehicles, client, referred];
const emails = [];
const service = createStorageInvoiceService({
  db, fieldValue: admin.firestore.FieldValue, loadPricing: async () => ({}),
  resolveAmount: (request) => request.contractAmount,
  resolveCaseId: (request) => request.caseId || `${request.clientId}-${request.season}`,
  from: 'billing@example.invalid', ccForLocale: () => 'office@example.invalid',
  sendEmail: async (message) => { emails.push(message); return { accepted: [message.to] }; }
});
try {
  await client.set({ name: 'ZZ Invoice Preview Test (disposable)', email: 'invoice-test@example.invalid', preferredLanguage: 'fr' });
  await referred.set({ name: 'ZZ Referred Customer Test (disposable)', email: 'referral-test@example.invalid' });
  for (const [index, ref] of vehicles.entries()) await ref.set({ clientId: client.id, season: 'winter', caseId: `${client.id}-winter`, status: 'scheduled', contractAmount: index ? 400 : 300, vehicle: { type: index ? 'car' : 'rv', brand: 'Local test', model: `${index + 1}` } });
  for (const ref of referredVehicles) await ref.set({ clientId: referred.id, season: 'winter', caseId: `${referred.id}-winter`, status: 'new', referredByClientIds: [client.id], vehicle: { type: 'rv' } });
  await receipt.set({ clientId: client.id, receiptType: 'deposit', amount: 100, requestIds: ids });
  const preview = await service.prepare(ids);
  const configuredDiscount = (await settings.get()).data().referralDiscountAmount;
  assert.equal(preview.invoice.referralDiscountCents, Math.min(60000, Math.round(configuredDiscount * 100)));
  assert.equal(emails.length, 0);
  assert.equal((await invoice.get()).exists, false);
  console.log(JSON.stringify({ previewClientName: 'ZZ Invoice Preview Test (disposable)', requestIds: ids, balanceDueCents: preview.invoice.balanceDueCents, pdfBytes: Buffer.from(preview.pdfBase64, 'base64').length }));
  if (process.argv.includes('--preview')) {
    const prompt = readline.createInterface({ input: process.stdin, output: process.stdout });
    await prompt.question('Local preview fixture ready. Press Enter after cancelling the UI preview to continue mock-send checks and cleanup.\n');
    prompt.close();
  }
  await service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'local-test');
  assert.equal(emails.length, 1);
  assert.equal(emails[0].attachments[0].content.subarray(0, 4).toString(), '%PDF');
  assert.equal((await invoice.get()).data().status, 'sent');
  for (const ref of vehicles) assert.equal((await ref.get()).data().status, 'stored');
  await service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'local-test');
  assert.equal(emails.length, 1);
  console.log('Local transactions passed: invoice/PDF persisted, Stored status set, credit consumed once. Email sender was mocked.');
} finally {
  for (const ref of disposable) await ref.delete();
  console.log('All disposable local invoice, credit, receipt, request and client fixtures removed.');
}
