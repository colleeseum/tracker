import test from 'node:test';
import assert from 'node:assert/strict';
import { createStorageNoDepositService } from './storage-no-deposit.js';
import { buildInvoiceModel } from './storage-invoices.js';

function fixture() {
  const records = new Map(['a', 'b'].map((id) => [`storageRequests/${id}`, {
    clientId: 'client', season: 'winter', status: 'waiting_deposit', signedContractPath: `signed/${id}.pdf`,
    contractAmount: id === 'a' ? 598 : 460, vehicle: { type: id === 'a' ? 'rv' : 'car' }, receiptEmailType: 'contract'
  }]));
  const writes = [];
  const db = {
    collection: (name) => ({ name, doc: (id) => ({ path: `${name}/${id}` }) }),
    runTransaction: async (handler) => {
      const pending = [];
      const result = await handler({
        get: async ({ name }) => ({ docs: [...records].filter(([path]) => path.startsWith(`${name}/`)).map(([path, data]) => ({ id: path.split('/')[1], data: () => structuredClone(data) })) }),
        update: (ref, patch) => pending.push([ref.path, patch])
      });
      pending.forEach(([path, patch]) => { assert.ok(records.has(path)); records.set(path, { ...records.get(path), ...patch }); writes.push([path, patch]); });
      return result;
    }
  };
  return { records, writes, service: createStorageNoDepositService({ db, fieldValue: { serverTimestamp: () => 'timestamp' }, resolveCaseId: (request) => `${request.season}|${request.clientId}` }) };
}

test('received contracts can schedule the complete case without creating payment or receipt records', async () => {
  const { service, records, writes } = fixture();
  await service.schedule({ requestIds: ['b', 'a'], confirmNoDeposit: true }, 'admin');
  assert.equal(records.size, 2);
  for (const id of ['a', 'b']) {
    const request = records.get(`storageRequests/${id}`);
    assert.equal(request.status, 'scheduled');
    assert.equal(request.depositRequired, false);
    assert.equal(request.depositRequirementWaivedBy, 'admin');
    assert.equal(request.depositRequirementWaivedAt, 'timestamp');
    assert.equal(request.receiptEmailType, 'contract');
    assert.equal(request.signedContractPath, `signed/${id}.pdf`);
  }
  const requests = [...records].map(([path, request]) => ({ ...request, id: path.split('/')[1] }));
  const invoice = buildInvoiceModel({ requests, allRequests: requests, client: { email: 'client@example.invalid' }, clients: new Map(), receipts: [], credits: new Map(), amountForRequest: (request) => request.contractAmount });
  assert.equal(invoice.subtotalCents, 105800);
  assert.equal(invoice.depositCents, 0);
  assert.equal(invoice.balanceDueCents, 105800);
  await service.schedule({ requestIds: ['a', 'b'], confirmNoDeposit: true }, 'admin');
  assert.equal(writes.length, 2);
});

test('confirmation, complete case, signed contracts and correct status are required by backend', async () => {
  const { service, records, writes } = fixture();
  await assert.rejects(service.schedule({ requestIds: ['a', 'b'] }, 'admin'), /Confirm/);
  await assert.rejects(service.schedule({ requestIds: ['a'], confirmNoDeposit: true }, 'admin'), /full storage case/);
  records.get('storageRequests/a').signedContractPath = '';
  await assert.rejects(service.schedule({ requestIds: ['a', 'b'], confirmNoDeposit: true }, 'admin'), /signed contract/);
  records.get('storageRequests/a').signedContractPath = 'signed/a.pdf';
  records.get('storageRequests/a').status = 'waiting_contract_deposit';
  await assert.rejects(service.schedule({ requestIds: ['a', 'b'], confirmNoDeposit: true }, 'admin'), /Waiting for Deposit/);
  assert.equal(writes.length, 0);
});

test('existing deposits and pending deliveries cannot be bypassed', async () => {
  const { service, records, writes } = fixture();
  records.set('storageReceipts/deposit', { receiptType: 'deposit', requestIds: ['a', 'b'], amount: 200 });
  await assert.rejects(service.schedule({ requestIds: ['a', 'b'], confirmNoDeposit: true }, 'admin'), /already has a deposit/);
  records.get('storageReceipts/deposit').status = 'void';
  records.get('storageRequests/a').invoicePendingId = 'pending';
  await assert.rejects(service.schedule({ requestIds: ['a', 'b'], confirmNoDeposit: true }, 'admin'), /pending email/);
  assert.equal(writes.length, 0);
});
