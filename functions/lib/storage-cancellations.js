import { cents, digest, invoiceError, invoiceIdForRequests } from './storage-invoices.js';

const cancellable = new Set(['new', 'contract_ready', 'waiting_contract_deposit', 'waiting_contract', 'waiting_deposit', 'reserved', 'scheduled']);
const validId = (id) => typeof id === 'string' && id.trim() && !id.includes('/');
export function createStorageCancellationService({ db, fieldValue, resolveCaseId, loadPricing, resolveAmount, estimateDeposit }) {
  async function cancellationQuote(requestId, transaction = null) {
    if (!validId(requestId)) throw invoiceError('Select one valid vehicle.', 'invalid-argument');
    const get = (ref) => transaction ? transaction.get(ref) : ref.get();
    const [allSnap, receipts, pricing] = await Promise.all([get(db.collection('storageRequests')), get(db.collection('storageReceipts')), loadPricing()]);
    const all = allSnap.docs.map((snap) => ({ ...snap.data(), id: snap.id }));
    const request = all.find((item) => item.id === requestId);
    if (!request) throw invoiceError('Vehicle not found.', 'not-found');
    if (request.invoicePendingId || request.paymentPendingId) throw invoiceError('Complete or reconcile pending invoice/payment delivery before cancelling.');
    if (request.status === 'cancelled') return { request, alreadyCancelled: true };
    if (!cancellable.has(request.status || 'new') || request.invoiceId || request.paidInvoiceId) throw invoiceError('Reopen the unpaid invoice before cancelling an invoiced or Stored vehicle. Paid vehicles require payment reconciliation.');
    const caseId = resolveCaseId(request);
    const caseRequests = all.filter((item) => resolveCaseId(item) === caseId);
    if (caseRequests.some((item) => item.invoicePendingId || item.paymentPendingId)) throw invoiceError('Another vehicle in this case has pending invoice/payment delivery.');
    const ids = caseRequests.map((item) => item.id);
    const deposits = receipts.docs.filter((snap) => {
      const receipt = snap.data();
      return receipt.receiptType === 'deposit' && !['void', 'cancelled'].includes(receipt.status) && receipt.requestIds?.some((id) => ids.includes(id));
    });
    if (deposits.some((snap) => snap.data().requestIds.some((id) => !ids.includes(id)))) throw invoiceError('A deposit covers another case. Reconcile its allocation before cancelling.');
    const totalDepositCents = deposits.reduce((sum, snap) => sum + cents(snap.data().amount), 0);
    const alreadyRetainedCents = caseRequests.filter((item) => item.status === 'cancelled').reduce((sum, item) => sum + (item.cancellation?.retainedDepositCents || 0), 0);
    const originalAmountCents = cents(resolveAmount(request, pricing));
    const maximumRetainedCents = Math.min(originalAmountCents, Math.max(0, totalDepositCents - alreadyRetainedCents));
    return { request, originalAmountCents, totalDepositCents, maximumRetainedCents,
      suggestedRetainedCents: Math.min(maximumRetainedCents, cents(estimateDeposit(originalAmountCents / 100))),
      depositReceiptIds: deposits.map((snap) => snap.id).sort() };
  }
  async function prepare(requestId) { return cancellationQuote(requestId); }
  async function cancel({ requestId, retainedDepositAmount, reason, confirmCancellation }, uid) {
    if (confirmCancellation !== true || !String(reason || '').trim()) throw invoiceError('Confirm cancellation and provide a reason.');
    const retainedDepositCents = cents(retainedDepositAmount);
    return db.runTransaction(async (transaction) => {
      const quote = await cancellationQuote(requestId, transaction);
      if (quote.alreadyCancelled) return { success: true, alreadyCancelled: true, cancellation: quote.request.cancellation };
      if (retainedDepositCents > quote.maximumRetainedCents) throw invoiceError('The retained deposit exceeds the recorded deposit available for this case or the vehicle charge.');
      const timestamp = fieldValue.serverTimestamp();
      const cancellation = { originalAmountCents: quote.originalAmountCents, retainedDepositCents, creditCents: quote.originalAmountCents - retainedDepositCents,
        depositReceiptIds: quote.depositReceiptIds, reason: String(reason).trim(), cancelledAt: timestamp, cancelledBy: uid, previousStatus: quote.request.status || 'new' };
      transaction.update(db.collection('storageRequests').doc(requestId), { status: 'cancelled', cancellation, updatedAt: timestamp, updatedBy: uid });
      return { success: true, cancellation };
    });
  }
  async function reopen({ invoiceId, reason, confirmReopen }, uid) {
    if (!/^INV-[A-F0-9]{18}$/.test(invoiceId || '') || confirmReopen !== true || !String(reason || '').trim()) throw invoiceError('Select an invoice, confirm reopening and provide a reason.');
    return db.runTransaction(async (transaction) => {
      const ref = db.collection('storageInvoices').doc(invoiceId);
      const snap = await transaction.get(ref);
      const data = snap.data();
      if (data?.status === 'superseded') return { success: true, alreadyReopened: true, replacementInvoiceId: data.replacementInvoiceId };
      if (data?.status !== 'sent' || data.payment || !data.invoice?.requestIds?.length) throw invoiceError('Only a completed, unpaid invoice with no payment receipt can be reopened.');
      const [allSnap, ledgerSnap] = await Promise.all([transaction.get(db.collection('storageRequests')), transaction.get(db.collection('expenses'))]);
      if (ledgerSnap.docs.some((entry) => entry.data().storageInvoiceId === invoiceId || entry.id === `storage-payment-${invoiceId}`)) throw invoiceError('A ledger payment exists. Reconcile the payment before reopening this invoice.');
      const all = allSnap.docs.map((item) => ({ ...item.data(), id: item.id }));
      const ids = [...data.invoice.requestIds].sort();
      const requests = ids.map((id) => all.find((item) => item.id === id));
      if (requests.some((request) => !request || !['stored', 'cancelled'].includes(request.status) || request.invoiceId !== invoiceId || request.paidInvoiceId || request.invoicePendingId || request.paymentPendingId)) throw invoiceError('Every invoice vehicle must be Stored (or cancelled) and have no payment or pending delivery.');
      const caseId = resolveCaseId(requests[0]);
      if (digest(ids) !== digest(all.filter((request) => resolveCaseId(request) === caseId).map((request) => request.id).sort())) throw invoiceError('Reopen the complete invoiced case together.');
      const revision = (data.invoice.billingRevision || 0) + 1;
      const replacementInvoiceId = invoiceIdForRequests(ids, revision);
      if ((await transaction.get(db.collection('storageInvoices').doc(replacementInvoiceId))).exists) throw invoiceError('The replacement invoice already exists. Reconcile before reopening.');
      const credits = data.invoice.creditAllocations || [];
      const creditSnaps = await Promise.all(credits.map((credit) => transaction.get(db.collection('storageReferralCredits').doc(credit.id))));
      creditSnaps.forEach((credit, index) => {
        if (credit.data()?.reservedInvoiceId || credit.data()?.usedCents !== credits[index].beforeUsedCents + credits[index].appliedCents) throw invoiceError('Referral credit has been used by another invoice. Reconcile it before reopening.');
      });
      const timestamp = fieldValue.serverTimestamp();
      requests.forEach((request) => {
        const line = data.invoice.lineItems.find((item) => item.requestId === request.id && !item.cancellationAdjustment);
        if (!line || !Number.isSafeInteger(line.amountCents) || line.amountCents < 0) throw invoiceError('The original vehicle charge is missing or invalid.');
        transaction.update(db.collection('storageRequests').doc(request.id), {
          status: request.status === 'cancelled' ? 'cancelled' : 'scheduled',
          contractAmount: line.amountCents / 100, invoiceId: fieldValue.delete(), invoiceSentAt: fieldValue.delete(), storedAt: fieldValue.delete(),
          previousInvoiceId: invoiceId, billingRevision: revision, invoiceReopenedAt: timestamp, invoiceReopenedBy: uid,
          updatedAt: timestamp, updatedBy: uid
        });
      });
      credits.forEach((credit) => transaction.update(db.collection('storageReferralCredits').doc(credit.id), { usedCents: credit.beforeUsedCents, updatedAt: timestamp }));
      transaction.update(ref, { status: 'superseded', supersededAt: timestamp, supersededBy: uid, supersededReason: String(reason).trim(), replacementInvoiceId,
        ...(credits.length ? { referralCreditsReversedAt: timestamp } : {}) });
      return { success: true, replacementInvoiceId, requestIds: ids };
    });
  }
  return { prepare, cancel, reopen };
}
