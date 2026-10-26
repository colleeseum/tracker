import { PDFDocument, StandardFonts, rgb, degrees } from 'pdf-lib';
import { digest, invoiceError, invoiceDate } from './storage-invoices.js';
import { readSkipEmail } from './email-options.js';

export async function stampPaidInvoicePdf(pdfBase64, locale) {
  const pdf = await PDFDocument.load(Buffer.from(pdfBase64, 'base64'));
  const font = await pdf.embedFont(StandardFonts.HelveticaBold);
  const label = locale === 'fr' ? 'PAYÉ' : 'PAID';
  for (const page of pdf.getPages()) {
    const { width, height } = page.getSize();
    const x = width / 2 - 100;
    const y = height / 2 - 30;
    const green = rgb(0.05, 0.48, 0.25);
    page.drawRectangle({ x, y, width: 200, height: 72, borderWidth: 4, borderColor: green, borderOpacity: 0.35, rotate: degrees(-12) });
    page.drawText(label, { x: x + 28, y: y + 21, size: 43, font, color: green, opacity: 0.35, rotate: degrees(-12) });
  }
  return Buffer.from(await pdf.save());
}

export const storagePaymentLedgerId = (invoiceId) => `storage-payment-${invoiceId}`;

export function createStoragePaymentService({ db, fieldValue, sendEmail, from, renderEmail, routeEmail, resolveCaseId, resolveLedgerAccounts, ccForLocale }) {
  const refFor = (invoiceId) => {
    if (typeof invoiceId !== 'string' || !/^INV-[A-F0-9]{18}$/.test(invoiceId)) throw invoiceError('Select a valid saved invoice.', 'invalid-argument');
    return db.collection('storageInvoices').doc(invoiceId);
  };
  const requestState = (request) => digest({ status: request.status, invoiceId: request.invoiceId, clientId: request.clientId, season: request.season, contractAmount: request.contractAmount, vehicle: request.vehicle, addons: request.addons, cancellation: request.cancellation, billingRevision: request.billingRevision });
  async function load(invoiceId) {
    const snap = await refFor(invoiceId).get();
    const data = snap.data();
    if (!data || data.status !== 'sent') throw invoiceError('Send the original invoice before confirming payment.');
    if (data.reminderPendingId) throw invoiceError('An invoice reminder is sending. Wait for it to finish before confirming payment.');
    if (data.payment?.status === 'sent' || data.payment?.emailAcceptedAt || data.payment?.emailSkippedAt) return { data, recovery: true };
    if (['sending', 'delivery_uncertain'].includes(data.payment?.status)) throw invoiceError('Payment email is pending or needs reconciliation. Do not send it twice.');
    const requests = await Promise.all(data.invoice.requestIds.map((id) => db.collection('storageRequests').doc(id).get()));
    if (requests.some((request) => !request.exists || !['stored', 'cancelled'].includes(request.data().status) || request.data().invoiceId !== invoiceId || request.data().invoicePendingId || request.data().paymentPendingId)) throw invoiceError('Every active invoice vehicle must be Stored and free of pending delivery before confirming payment. Cancelled vehicles stay cancelled.');
    const allRequests = await db.collection('storageRequests').get();
    const caseId = resolveCaseId(requests[0].data());
    const caseIds = allRequests.docs.filter((request) => resolveCaseId(request.data()) === caseId).map((request) => request.id).sort();
    if (digest(caseIds) !== digest([...data.invoice.requestIds].sort())) throw invoiceError('Confirm payment for the complete invoiced case. Review added or changed vehicles first.');
    if (!Number.isSafeInteger(data.invoice.balanceDueCents) || data.invoice.balanceDueCents < 0) throw invoiceError('The saved invoice balance is invalid.');
    let ledgerAccounts = null;
    if (data.invoice.balanceDueCents > 0) {
      if ((await db.collection('expenses').doc(storagePaymentLedgerId(invoiceId)).get()).exists) throw invoiceError('A ledger entry already exists for this payment. Reconcile it before sending a receipt.');
      const accounts = await resolveLedgerAccounts();
      if (!accounts?.cashAccount?.id || !accounts?.entityAccount?.id) throw invoiceError('Configure the default cash account and Ferme Colle entity before receiving payment.');
      ledgerAccounts = { accountId: accounts.cashAccount.id, entityId: accounts.entityAccount.id };
    }
    return { data, ledgerAccounts, states: requests.map((request) => ({ requestId: request.id, fingerprint: requestState(request.data()) })) };
  }
  async function serialize(invoiceId, quote) {
    const invoice = quote.data.invoice;
    const paidDate = invoiceDate();
    const email = await renderEmail({ ...invoice, paidDate, paymentAmountCents: invoice.balanceDueCents });
    // Use the current client CC, including changes made after issuing the invoice.
    const clientSnap = ccForLocale ? await db.collection('clients').doc(invoice.clientId).get() : null;
    const client = clientSnap?.exists ? clientSnap.data() : invoice.client;
    const cc = ccForLocale ? ccForLocale(invoice.locale, { ...client, email: invoice.client.email }) : quote.data.preview.cc || '';
    const requested = { to: invoice.client.email, cc, subject: email.subject };
    const delivery = routeEmail ? routeEmail(requested) : requested;
    const pdfBase64 = (await stampPaidInvoicePdf(quote.data.preview.pdfBase64, invoice.locale)).toString('base64');
    const preview = { invoice, invoiceId, paidDate, paymentAmountCents: invoice.balanceDueCents, ledgerAccounts: quote.ledgerAccounts, ...email, ...delivery, pdfBase64, payment: true,
      fingerprint: digest({ invoice, paidDate, email, delivery, ledgerAccounts: quote.ledgerAccounts, states: quote.states, originalPdf: digest(quote.data.preview.pdfBase64), layoutVersion: 'paid-1' }) };
    // Both original and paid PDFs share the same Firestore invoice document.
    if (Buffer.byteLength(JSON.stringify({ ...quote.data, payment: { preview, requestStates: quote.states } })) > 900000) throw invoiceError('The paid receipt exceeds the invoice document size limit.');
    return preview;
  }
  async function prepare(invoiceId) {
    const quote = await load(invoiceId);
    if (quote.recovery) return { ...quote.data.payment.preview, archived: quote.data.payment.status === 'sent', alreadyAccepted: !!quote.data.payment.emailAcceptedAt, emailSkipped: !!quote.data.payment.emailSkippedAt, completionPending: quote.data.payment.status !== 'sent' };
    return { ...await serialize(invoiceId, quote), archived: false };
  }
  async function finalize(invoiceId, uid) {
    return db.runTransaction(async (transaction) => {
      const ref = refFor(invoiceId);
      const snap = await transaction.get(ref);
      const data = snap.data();
      if (data?.payment?.status === 'sent') return { success: true, invoiceId, alreadySent: true };
      if (!data?.payment?.emailAcceptedAt && !data?.payment?.emailSkippedAt) throw invoiceError('Payment receipt email must be accepted or explicitly skipped.');
      const requests = await Promise.all(data.invoice.requestIds.map((id) => transaction.get(db.collection('storageRequests').doc(id))));
      requests.forEach((request, index) => {
        if (request.data()?.paymentPendingId !== invoiceId || requestState(request.data() || {}) !== data.payment.requestStates[index].fingerprint) throw invoiceError('A vehicle changed after receipt delivery. Reconcile before completing Paid status.');
      });
      const ledgerEntryId = data.invoice.balanceDueCents > 0 ? storagePaymentLedgerId(invoiceId) : null;
      if (ledgerEntryId) {
        const ledger = await transaction.get(db.collection('expenses').doc(ledgerEntryId));
        if (ledger.exists) throw invoiceError('A ledger entry already exists for this payment. Reconcile before completing Paid status.');
        if (!data.payment.preview.ledgerAccounts?.accountId || !data.payment.preview.ledgerAccounts?.entityId) throw invoiceError('Payment ledger accounts are missing. Reconcile before completing Paid status.');
      }
      const timestamp = fieldValue.serverTimestamp();
      if (ledgerEntryId) {
        transaction.set(db.collection('expenses').doc(ledgerEntryId), {
          title: `Storage payment ${data.invoice.seasonLabel || data.invoice.seasonId} ${data.invoice.client.name} ${invoiceId}`.replace(/\s+/g, ' ').trim(),
          description: [`Invoice: ${invoiceId}`, ...data.invoice.lineItems.map((line) => line.description)].join('\n'),
          ...data.payment.preview.ledgerAccounts,
          date: data.payment.confirmedAt, entryType: 'income', category: 'Storage payment',
          categoryId: null, categoryCode: null, categoryType: 'income',
          amount: data.invoice.balanceDueCents / 100, transactionId: ledgerEntryId,
          clientId: data.invoice.clientId, storageInvoiceId: invoiceId,
          storageRequestIds: data.invoice.requestIds, storageCaseId: resolveCaseId(requests[0].data()),
          paymentStatus: null, paidAt: null, paidAccountId: null,
          tags: ['storage-payment'], interestTags: [], vendorTag: null, isReturn: false,
          receiptUrl: null, receiptStoragePath: null,
          createdAt: timestamp, createdBy: data.payment.confirmedBy || uid,
          createdByEmail: data.payment.confirmedByEmail || null, createdByName: data.payment.confirmedByName || null,
          updatedAt: timestamp, updatedBy: uid
        });
      }
      requests.forEach((request) => transaction.set(db.collection('storageRequests').doc(request.id), { ...(request.data().status === 'cancelled' ? {} : { status: 'paid', paidAt: timestamp, paidBy: uid }), paidInvoiceId: invoiceId, paymentPendingId: fieldValue.delete(), updatedAt: timestamp, updatedBy: uid }, { merge: true }));
      transaction.set(ref, { payment: { ...data.payment, status: 'sent', paidAt: timestamp, paidBy: uid, amountCents: data.invoice.balanceDueCents, ledgerEntryId, emailDeliveryStatus: data.payment.emailSkippedAt ? 'skipped' : 'accepted' }, balanceRemainingCents: 0 }, { merge: true });
      return { success: true, invoiceId };
    });
  }
  async function send({ invoiceId, fingerprint, confirmFullPayment, skipEmail: skipEmailValue }, uid, actor = {}) {
    const skipEmail = readSkipEmail({ skipEmail: skipEmailValue });
    if (confirmFullPayment !== true) throw invoiceError('Confirm that the full invoice balance has been received.');
    const quote = await load(invoiceId);
    if (quote.recovery) return finalize(invoiceId, uid);
    if (!from && !skipEmail) throw invoiceError('SMTP default FROM is not configured.');
    const preview = await serialize(invoiceId, quote);
    if (fingerprint !== preview.fingerprint) throw invoiceError('Payment receipt changed after preview. Reopen it before sending.');
    await db.runTransaction(async (transaction) => {
      const ref = refFor(invoiceId);
      const current = (await transaction.get(ref)).data();
      if (current?.reminderPendingId) throw invoiceError('An invoice reminder is sending. Wait for it to finish before confirming payment.');
      if (['sent', 'sending', 'delivery_uncertain'].includes(current?.payment?.status) || current?.payment?.emailAcceptedAt) throw invoiceError('Payment receipt is already sent or pending reconciliation.');
      if (digest(current.invoice) !== digest(quote.data.invoice) || digest(current.preview) !== digest(quote.data.preview)) throw invoiceError('Invoice changed. Reopen the payment preview.');
      const requests = await Promise.all(quote.states.map((state) => transaction.get(db.collection('storageRequests').doc(state.requestId))));
      requests.forEach((request, index) => {
        if (requestState(request.data() || {}) !== quote.states[index].fingerprint || request.data()?.paymentPendingId || request.data()?.invoicePendingId) throw invoiceError('A vehicle changed or delivery is pending. Reopen the payment preview.');
      });
      const timestamp = fieldValue.serverTimestamp();
      transaction.set(ref, { payment: { status: 'sending', preview, requestStates: quote.states, confirmedAt: timestamp, confirmedBy: uid, confirmedByEmail: actor.email || null, confirmedByName: actor.name || null,
        ...(skipEmail ? { emailSkippedAt: timestamp, emailSkippedBy: uid, emailDeliveryStatus: 'skipped' } : {}) } }, { merge: true });
      requests.forEach((request) => transaction.set(db.collection('storageRequests').doc(request.id), { paymentPendingId: invoiceId }, { merge: true }));
    });
    if (skipEmail) return finalize(invoiceId, uid);
    try {
      const result = await sendEmail({ from, to: preview.to, cc: preview.cc, subject: preview.subject, html: preview.html, text: preview.text,
        attachments: [{ filename: `${invoiceId}-${preview.invoice.locale === 'fr' ? 'paye' : 'paid'}.pdf`, content: Buffer.from(preview.pdfBase64, 'base64'), contentType: 'application/pdf' }] });
      if (Array.isArray(result?.accepted) && !result.accepted.some((address) => String(address).toLowerCase() === preview.to.toLowerCase())) throw invoiceError('Payment recipient delivery could not be confirmed.');
    } catch (error) {
      const rejected = Number(error.responseCode) >= 400 && Number(error.responseCode) < 600;
      await db.runTransaction(async (transaction) => {
        const ref = refFor(invoiceId);
        const current = (await transaction.get(ref)).data();
        transaction.set(ref, { payment: { ...current.payment, status: rejected ? 'failed' : 'delivery_uncertain' } }, { merge: true });
        if (rejected) quote.states.forEach(({ requestId }) => transaction.set(db.collection('storageRequests').doc(requestId), { paymentPendingId: fieldValue.delete() }, { merge: true }));
      });
      throw invoiceError(rejected ? 'Payment receipt email was rejected. Vehicles remain Stored; reopen the preview to retry.' : 'Payment receipt delivery is uncertain. Vehicles remain Stored; reconcile before retrying.');
    }
    await db.runTransaction(async (transaction) => {
      const ref = refFor(invoiceId);
      const current = (await transaction.get(ref)).data();
      transaction.set(ref, { payment: { ...current.payment, emailAcceptedAt: fieldValue.serverTimestamp() } }, { merge: true });
    });
    return finalize(invoiceId, uid);
  }
  async function get(invoiceId) {
    const data = (await refFor(invoiceId).get()).data();
    if (data?.payment?.status !== 'sent') throw invoiceError('Paid receipt not found.', 'not-found');
    return { ...data.payment.preview, archived: true };
  }
  return { prepare, send, get };
}
