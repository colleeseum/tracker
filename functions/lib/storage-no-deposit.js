import { digest, invoiceError } from './storage-invoices.js';

export function createStorageNoDepositService({ db, fieldValue, resolveCaseId }) {
  async function schedule({ requestIds, confirmNoDeposit }, uid) {
    if (confirmNoDeposit !== true) throw invoiceError('Confirm that this case may be scheduled without a deposit.');
    if (!Array.isArray(requestIds) || !requestIds.length || requestIds.length > 50 || requestIds.some((id) => typeof id !== 'string' || !id.trim() || id.includes('/'))) throw invoiceError('Select 1 to 50 valid storage requests.', 'invalid-argument');
    const ids = [...new Set(requestIds.map((id) => id.trim()))].sort();
    return db.runTransaction(async (transaction) => {
      const requestSnapshot = await transaction.get(db.collection('storageRequests'));
      const receiptSnapshot = await transaction.get(db.collection('storageReceipts'));
      const all = requestSnapshot.docs.map((snap) => ({ ...snap.data(), id: snap.id }));
      const requests = ids.map((id) => all.find((request) => request.id === id));
      if (requests.some((request) => !request)) throw invoiceError('A storage request was not found.', 'not-found');
      const first = requests[0];
      if (!first.clientId || !first.season || requests.some((request) => request.clientId !== first.clientId || request.season !== first.season)) throw invoiceError('Schedule one client and season together.');
      const caseId = resolveCaseId(first);
      const fullIds = all.filter((request) => resolveCaseId(request) === caseId).map((request) => request.id).sort();
      if (digest(ids) !== digest(fullIds)) throw invoiceError('Schedule the full storage case together.');
      if (requests.some((request) => request.invoicePendingId || request.paymentPendingId)) throw invoiceError('Complete or reconcile pending email delivery before scheduling.');
      if (requests.every((request) => request.status === 'scheduled' && request.depositRequired === false)) return { success: true, targetStatus: 'scheduled', alreadyScheduled: true };
      if (requests.some((request) => request.status !== 'waiting_deposit' || !request.signedContractPath)) throw invoiceError('Receive and upload every signed contract first. The case must be Waiting for Deposit.');
      const activeDeposits = receiptSnapshot.docs.filter((snap) => {
        const receipt = snap.data();
        return receipt.receiptType === 'deposit' && !['void', 'cancelled'].includes(receipt.status) && receipt.requestIds?.some((id) => ids.includes(id));
      });
      if (activeDeposits.length) throw invoiceError('This case already has a deposit receipt. Review its normal deposit workflow instead.');
      const timestamp = fieldValue.serverTimestamp();
      requests.forEach((request) => transaction.update(db.collection('storageRequests').doc(request.id), {
        status: 'scheduled', depositRequired: false, depositRequirementWaivedAt: timestamp,
        depositRequirementWaivedBy: uid, updatedAt: timestamp, updatedBy: uid
      }));
      return { success: true, targetStatus: 'scheduled' };
    });
  }
  return { schedule };
}
