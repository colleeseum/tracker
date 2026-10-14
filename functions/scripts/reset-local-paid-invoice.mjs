import fs from 'node:fs/promises';
import path from 'node:path';
import admin from 'firebase-admin';
import { serializeData } from './utils/firestore-serialization.mjs';
import { storagePaymentLedgerId } from '../lib/storage-payments.js';

if (process.env.FIRESTORE_EMULATOR_HOST !== '127.0.0.1:8081') throw new Error('Reset is restricted to the local Firestore emulator at 127.0.0.1:8081.');
const invoiceId = process.argv[2];
if (!/^INV-[A-F0-9]{18}$/.test(invoiceId || '')) throw new Error('Provide one exact invoice ID.');
admin.initializeApp({ projectId: 'tracker-187c5' });
const db = admin.firestore();
try {
  const invoiceRef = db.collection('storageInvoices').doc(invoiceId);
  const invoiceSnap = await invoiceRef.get();
  const data = invoiceSnap.data();
  if (data?.status !== 'sent' || data.payment?.status !== 'sent' || !data.invoice?.requestIds?.length) throw new Error('Only a sent invoice with a completed Paid receipt can be reset.');
  if (data.payment.ledgerEntryId) throw new Error('Payment has a ledger link. Refusing to reset accounting data.');
  const requests = await Promise.all(data.invoice.requestIds.map((id) => db.collection('storageRequests').doc(id).get()));
  const assertRequest = (snap) => {
    const request = snap.data();
    if (!snap.exists || request.status !== 'paid' || request.invoiceId !== invoiceId || request.paidInvoiceId !== invoiceId || request.invoicePendingId || request.paymentPendingId) throw new Error('Every invoiced vehicle must be Paid with no pending delivery.');
  };
  requests.forEach(assertRequest);
  const ledgerRef = db.collection('expenses').doc(storagePaymentLedgerId(invoiceId));
  const ledgerSnap = await ledgerRef.get();
  const expenses = await db.collection('expenses').get();
  if (ledgerSnap.exists || expenses.docs.some((snap) => snap.data().storageInvoiceId === invoiceId)) throw new Error('A ledger entry already exists. Refusing to reset financial records.');
  console.log(JSON.stringify({ environment: 'local emulator', invoiceId, client: data.invoice.client.name, vehicles: requests.length, resetFrom: 'paid', resetTo: 'stored', balanceCents: data.invoice.balanceDueCents, apply: process.argv.includes('--apply') }));
  if (!process.argv.includes('--apply')) process.exitCode = 0;
  else {
    const backupPath = path.resolve('backups', `local-paid-reset-${invoiceId}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
    await fs.mkdir(path.dirname(backupPath), { recursive: true });
    await fs.writeFile(backupPath, JSON.stringify({ environment: 'local emulator', invoiceId, createdAt: new Date().toISOString(), documents: [invoiceSnap, ...requests].map((snap) => ({ path: snap.ref.path, data: serializeData(snap.data()) })) }, null, 2), { flag: 'wx' });
    await db.runTransaction(async (transaction) => {
      const freshInvoice = await transaction.get(invoiceRef);
      const freshRequests = await Promise.all(requests.map((snap) => transaction.get(snap.ref)));
      const freshLedger = await transaction.get(ledgerRef);
      if (!freshInvoice.updateTime.isEqual(invoiceSnap.updateTime) || freshLedger.exists) throw new Error('Invoice or ledger changed after backup. Reset cancelled.');
      freshRequests.forEach((snap, index) => {
        assertRequest(snap);
        if (!snap.updateTime.isEqual(requests[index].updateTime)) throw new Error('A vehicle changed after backup. Reset cancelled.');
      });
      const fieldValue = admin.firestore.FieldValue;
      transaction.update(invoiceRef, { payment: fieldValue.delete(), balanceRemainingCents: data.invoice.balanceDueCents });
      freshRequests.forEach((snap) => transaction.update(snap.ref, {
        status: 'stored', paidAt: fieldValue.delete(), paidBy: fieldValue.delete(), paidInvoiceId: fieldValue.delete(),
        updatedAt: fieldValue.serverTimestamp(), updatedBy: 'local-paid-reset'
      }));
    });
    console.log(JSON.stringify({ reset: true, invoiceId, vehicles: requests.length, backupPath, originalInvoicePreserved: true, ledgerEntriesRemoved: 0, emailSent: false }));
  }
} finally { await admin.app().delete(); }
