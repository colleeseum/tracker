import crypto from 'node:crypto';
import { digest, invoiceError } from './storage-invoices.js';
import { readSkipEmail } from './email-options.js';

export function createStorageInvoiceReminderService({ db, fieldValue, renderEmail, routeEmail, ccForLocale, sendEmail, from }) {
  const invoiceRef = (id) => {
    if (!/^INV-[A-F0-9]{18}$/.test(id || '')) throw invoiceError('Select a valid saved invoice.', 'invalid-argument');
    return db.collection('storageInvoices').doc(id);
  };
  const reminderRef = (invoiceId, reminderId) => {
    if (!/^[a-f0-9-]{36}$/.test(reminderId || '')) throw invoiceError('Reopen the payment reminder preview.', 'invalid-argument');
    return db.collection('storageInvoices').doc(invoiceId).collection('reminders').doc(reminderId);
  };
  const timestamp = () => fieldValue.serverTimestamp();
  const assertUnpaid = (data) => {
    if (data?.status !== 'sent' || data.payment || !Number.isSafeInteger(data.invoice?.balanceDueCents) || data.invoice.balanceDueCents <= 0) throw invoiceError('Only an issued, unpaid invoice with a remaining balance can receive a reminder.');
    if (data.reminderPendingId) throw invoiceError('An invoice reminder is already sending. Reconcile it before continuing.');
  };
  const assertRequests = (snapshots, id) => {
    if (!snapshots.length || snapshots.some((snap) => !snap.exists || !['stored', 'picked_up', 'cancelled'].includes(snap.data().status) || snap.data().invoiceId !== id || snap.data().paidInvoiceId || snap.data().invoicePendingId || snap.data().paymentPendingId)) throw invoiceError('The invoice vehicles have changed or payment is pending. Review the case before sending a reminder.');
  };
  async function quote(id, reminderId) {
    const snap = await invoiceRef(id).get();
    const data = snap.data(); assertUnpaid(data);
    const invoice = data.invoice;
    const [clientSnap, ledger, ...requests] = await Promise.all([
      db.collection('clients').doc(invoice.clientId).get(),
      db.collection('expenses').doc(`storage-payment-${id}`).get(),
      ...invoice.requestIds.map((requestId) => db.collection('storageRequests').doc(requestId).get())
    ]);
    if (ledger.exists) throw invoiceError('A final payment is already recorded in the ledger. Review payment before sending a reminder.');
    assertRequests(requests, id);
    const pdfBase64 = data.preview?.pdfBase64;
    if (!pdfBase64 || Buffer.from(pdfBase64, 'base64').subarray(0, 4).toString() !== '%PDF') throw invoiceError('The original invoice PDF is unavailable. A reminder cannot regenerate or replace it.');
    const client = clientSnap.exists ? clientSnap.data() : invoice.client;
    const to = String(client.email || invoice.client.email || '').trim();
    if (!to) throw invoiceError('Client email is missing.');
    const email = await renderEmail(invoice);
    const delivery = routeEmail({ to, cc: ccForLocale(invoice.locale, { ...client, email: to }), subject: email.subject });
    const fingerprint = digest({ reminderId, invoice, email, delivery, pdfBase64 });
    return { data, preview: { ...email, ...delivery, invoice, invoiceId: id, reminderId, reminder: true, fingerprint, pdfBase64 } };
  }
  async function prepare(id) {
    return (await quote(id, crypto.randomUUID())).preview;
  }
  async function send({ invoiceId: id, reminderId, fingerprint, skipEmail: skipValue }, uid) {
    const skipEmail = readSkipEmail({ skipEmail: skipValue });
    const ref = invoiceRef(id);
    const auditRef = reminderRef(id, reminderId);
    const previous = (await auditRef.get()).data();
    if (['sent', 'sent_with_warning', 'skipped'].includes(previous?.status)) return { success: true, alreadySent: previous.status !== 'skipped', emailSkipped: previous.status === 'skipped' };
    if (['sending', 'delivery_uncertain'].includes(previous?.status)) throw invoiceError('Reminder delivery is pending or uncertain. Check delivery before retrying.');
    if (!from && !skipEmail) throw invoiceError('SMTP default FROM is not configured.');
    const { data, preview } = await quote(id, reminderId);
    if (fingerprint !== preview.fingerprint) throw invoiceError('The reminder or recipients changed after preview. Reopen it before sending.');
    await db.runTransaction(async (tx) => {
      const current = (await tx.get(ref)).data(); assertUnpaid(current);
      const audit = (await tx.get(auditRef)).data();
      if (audit && !['failed'].includes(audit.status)) throw invoiceError('This reminder is already sent or in progress.');
      if (digest(current.invoice) !== digest(data.invoice) || current.preview?.pdfBase64 !== preview.pdfBase64) throw invoiceError('The invoice changed. Reopen the reminder preview.');
      const [ledger, ...requests] = await Promise.all([
        tx.get(db.collection('expenses').doc(`storage-payment-${id}`)),
        ...preview.invoice.requestIds.map((requestId) => tx.get(db.collection('storageRequests').doc(requestId)))
      ]);
      if (ledger.exists) throw invoiceError('Payment is already recorded.');
      assertRequests(requests, id);
      tx.set(auditRef, { status: skipEmail ? 'skipped' : 'sending', fingerprint, to: preview.to, cc: preview.cc, subject: preview.subject,
        developmentEmailRedirect: preview.developmentEmailRedirect, templateId: preview.templateId, attemptedAt: timestamp(), attemptedBy: uid });
      if (!skipEmail) tx.set(ref, { reminderPendingId: reminderId }, { merge: true });
    });
    if (skipEmail) return { success: true, emailSkipped: true };
    let status = 'sent';
    try {
      const result = await sendEmail({ from, to: preview.to, cc: preview.cc, subject: preview.subject, html: preview.html, text: preview.text,
        attachments: [{ filename: `${id}.pdf`, content: Buffer.from(preview.pdfBase64, 'base64'), contentType: 'application/pdf' }] });
      const accepted = (result?.accepted || []).map((value) => String(value.address || value).toLowerCase());
      if (!accepted.includes(preview.to.toLowerCase())) {
        status = !accepted.length && result?.rejected?.length ? 'failed' : 'delivery_uncertain';
      } else if (result?.rejected?.length) status = 'sent_with_warning';
    } catch (error) {
      status = error.code === 'EAUTH' || (error.responseCode >= 400 && error.responseCode < 600) ? 'failed' : 'delivery_uncertain';
    }
    await db.runTransaction(async (tx) => {
      const current = (await tx.get(ref)).data();
      if (current.reminderPendingId !== reminderId) throw invoiceError('Reminder delivery needs reconciliation. Do not send it again.');
      tx.set(auditRef, { status, ...(status.startsWith('sent') ? { sentAt: timestamp(), sentBy: uid } : {}) }, { merge: true });
      tx.set(ref, { reminderPendingId: fieldValue.delete(),
        ...(status.startsWith('sent') ? { lastReminderSentAt: timestamp(), lastReminderSentBy: uid, lastReminderId: reminderId } : {}) }, { merge: true });
    });
    if (status === 'failed') throw invoiceError('The reminder was rejected. Invoice and payment status are unchanged; you may retry.');
    if (status === 'delivery_uncertain') throw invoiceError('Reminder delivery is uncertain. Check the email provider before sending another reminder.');
    return { success: true, warning: status === 'sent_with_warning' ? 'The client message was accepted, but a CC recipient was rejected. Review delivery before resending.' : '' };
  }
  return { prepare, send };
}
