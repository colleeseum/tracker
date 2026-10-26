import fs from 'node:fs/promises';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { serializeData } from './utils/firestore-serialization.mjs';

const { values } = parseArgs({ options: {
  invoice: { type: 'string' }, vehicle: { type: 'string' }, 'retained-deposit': { type: 'string' },
  'expected-client': { type: 'string' }, 'expected-balance': { type: 'string' }, 'apply-production': { type: 'boolean', default: false },
  'reopen-only': { type: 'boolean', default: false }
} });
if (process.env.FIRESTORE_EMULATOR_HOST || process.env.FUNCTIONS_EMULATOR === 'true') throw new Error('This explicitly production-only correction must not use emulator configuration.');
if (process.env.GCLOUD_PROJECT && process.env.GCLOUD_PROJECT !== 'tracker-187c5') throw new Error('Unexpected production project.');
process.env.GCLOUD_PROJECT = 'tracker-187c5';
if (!/^INV-[A-F0-9]{18}$/.test(values.invoice || '') || !values.vehicle || values.vehicle.includes('/') || !values['expected-client'] || values['retained-deposit'] == null || values['expected-balance'] == null) throw new Error('Provide exact invoice, vehicle, retained-deposit, expected-client and expected-balance arguments.');
const retainedDepositAmount = Number(values['retained-deposit']);
const expectedBalanceCents = Math.round(Number(values['expected-balance']) * 100);
if (!Number.isFinite(retainedDepositAmount) || retainedDepositAmount < 0 || !Number.isSafeInteger(expectedBalanceCents)) throw new Error('Invalid expected amounts.');
const { reopenStorageInvoice, cancelStorageVehicle, prepareStorageInvoice } = await import('../index.js');
const { default: admin } = await import('firebase-admin');
const db = admin.firestore();
const auth = { auth: { uid: 'serge-production-correction', token: { email: 'sergecolle@gmail.com', name: 'Serge Colle' } } };
try {
  const invoiceSnap = await db.collection('storageInvoices').doc(values.invoice).get();
  const data = invoiceSnap.data();
  if (data?.status !== 'sent' || data.payment || data.invoice?.client?.name !== values['expected-client'] || !data.invoice.requestIds.includes(values.vehicle)) throw new Error('Invoice is no longer the expected unpaid client invoice. Refusing correction.');
  const requests = await Promise.all(data.invoice.requestIds.map((id) => db.collection('storageRequests').doc(id).get()));
  if (requests.some((snap) => !snap.exists || snap.data().status !== 'stored' || snap.data().invoiceId !== values.invoice || snap.data().paidInvoiceId || snap.data().invoicePendingId || snap.data().paymentPendingId)) throw new Error('Requests no longer match the expected Stored, unpaid case.');
  const ledger = await db.collection('expenses').where('storageInvoiceId', '==', values.invoice).get();
  if (!ledger.empty) throw new Error('Linked ledger entries exist. Refusing correction.');
  const line = data.invoice.lineItems.find((item) => item.requestId === values.vehicle);
  const adjustmentCents = line.amountCents - Math.round(retainedDepositAmount * 100);
  if (data.invoice.balanceDueCents - adjustmentCents !== expectedBalanceCents) throw new Error('Expected cancellation balance no longer matches the saved invoice.');
  const receipts = await Promise.all((data.invoice.depositReceiptIds || []).map((id) => db.collection('storageReceipts').doc(id).get()));
  const credits = await Promise.all((data.invoice.creditAllocations || []).map((credit) => db.collection('storageReferralCredits').doc(credit.id).get()));
  console.log(JSON.stringify({ production: true, invoiceId: values.invoice, client: values['expected-client'], requestIds: data.invoice.requestIds, cancelledVehicleId: values.vehicle,
    originalBalanceCents: data.invoice.balanceDueCents, retainedDepositCents: Math.round(retainedDepositAmount * 100), adjustmentCents: -adjustmentCents,
    expectedBalanceCents, apply: values['apply-production'], reopenOnly: values['reopen-only'], emailSent: false }));
  if (values['apply-production']) {
    const backupPath = path.resolve('backups', `production-cancellation-${values.invoice}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
    await fs.mkdir(path.dirname(backupPath), { recursive: true });
    await fs.writeFile(backupPath, JSON.stringify({ projectId: 'tracker-187c5', environment: 'production', createdAt: new Date().toISOString(), documents: [invoiceSnap, ...requests, ...receipts, ...credits].filter((snap) => snap.exists).map((snap) => ({ path: snap.ref.path, data: serializeData(snap.data()) })) }, null, 2), { flag: 'wx', mode: 0o600 });
    const reason = 'Invoice issued in error; reopen for customer-requested motorcycle cancellation, retaining the deposit.';
    const reopened = await reopenStorageInvoice.run({ invoiceId: values.invoice, reason, confirmReopen: true }, auth);
    if (!values['reopen-only']) await cancelStorageVehicle.run({ requestId: values.vehicle, retainedDepositAmount, reason, confirmCancellation: true }, auth);
    const preview = await prepareStorageInvoice.run({ requestIds: data.invoice.requestIds }, auth);
    const expectedPreviewBalanceCents = values['reopen-only'] ? data.invoice.balanceDueCents : expectedBalanceCents;
    if (preview.invoice.balanceDueCents !== expectedPreviewBalanceCents) throw new Error('Corrected preview differs from expected balance. Do not send; reconcile this case.');
    const finalRequests = await Promise.all(data.invoice.requestIds.map((id) => db.collection('storageRequests').doc(id).get()));
    console.log(JSON.stringify({ corrected: true, replacementInvoiceId: reopened.replacementInvoiceId, previewInvoiceId: preview.invoiceId,
      subtotalCents: preview.invoice.subtotalCents, depositCents: preview.invoice.depositCents, balanceDueCents: preview.invoice.balanceDueCents,
      adjustmentCents: preview.invoice.lineItems.find((item) => item.cancellationAdjustment)?.amountCents,
      statuses: finalRequests.map((snap) => ({ requestId: snap.id, status: snap.data().status })), backupPath,
      newInvoiceIssued: false, cancellationApplied: !values['reopen-only'], emailSent: false,
      note: values['reopen-only'] ? 'Invoice reopened only. Both vehicles remain Scheduled; user will apply cancellation and review/send the replacement.' : 'Corrected preview only; user must review and send/save the new invoice.' }));
  }
} finally { await admin.app().delete(); }
