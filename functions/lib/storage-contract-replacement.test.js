import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
function implementation(name) {
  const start = source.indexOf(`async function ${name}(`);
  return source.slice(start, source.indexOf('\n}\n', start) + 3);
}
function fixture(status = 'stored') {
  const request = { id: 'vehicle', clientId: 'client', season: 'winter', status, contractId: 'CT-EXISTING',
    signedContractPath: 'contracts/old.pdf', signedContractUrl: 'https://example.invalid/old.pdf',
    signedContractUploadedAt: 'old-date', invoiceId: 'original-invoice', paidInvoiceId: 'paid-invoice', contractAmount: 598 };
  const records = new Map([['storageRequests/vehicle', structuredClone(request)]]);
  const context = vm.createContext({
    db: {}, auth: { currentUser: { uid: 'admin' } },
    doc: (...args) => typeof args[0] === 'string' ? `${args[0]}/revision` : args.slice(1).join('/'),
    collection: (ref, name) => `${ref}/${name}`,
    serverTimestamp: () => 'timestamp', resolveStorageCaseId: () => 'case',
    getExpectedStorageContractId: (request) => request.contractId || 'CT-NEW',
    runTransaction: async (_db, callback) => {
      const writes = [];
      const result = await callback({
        get: async (ref) => ({ exists: () => records.has(ref), data: () => structuredClone(records.get(ref)) }),
        set: (ref, value) => writes.push([ref, value, false]),
        update: (ref, value) => writes.push([ref, value, true])
      });
      writes.forEach(([ref, value, merge]) => records.set(ref, merge ? { ...records.get(ref), ...value } : value));
      return result;
    }
  });
  vm.runInContext(implementation('saveUploadedStorageContract'), context);
  return { context, records, request };
}
const upload = { path: 'contracts/new.pdf', url: 'https://example.invalid/new.pdf', contractId: 'CT-EXISTING' };

test('contract replacement preserves every advanced workflow status, amount and invoice while retaining previous file', async () => {
  for (const status of ['waiting_contract_deposit', 'waiting_contract', 'waiting_deposit', 'reserved', 'scheduled', 'stored', 'paid', 'picked_up']) {
    const { context, records, request } = fixture(status);
    await context.saveUploadedStorageContract(request, upload, { markContractReady: true });
    const saved = records.get('storageRequests/vehicle');
    assert.equal(saved.status, status);
    assert.equal(saved.contractId, 'CT-EXISTING');
    assert.equal(saved.invoiceId, 'original-invoice');
    assert.equal(saved.paidInvoiceId, 'paid-invoice');
    assert.equal(saved.contractAmount, 598);
    assert.equal(saved.signedContractPath, upload.path);
    assert.equal(saved.signedContractUrl, upload.url);
    assert.equal(saved.signedContractUploadedBy, 'admin');
    const revision = records.get('storageRequests/vehicle/contractRevisions/revision');
    assert.equal(revision.signedContractPath, request.signedContractPath);
    assert.equal(revision.signedContractUrl, request.signedContractUrl);
    assert.equal(revision.replacedBy, 'admin');
  }
});

test('first upload retains initial Contract Ready progression without inventing a revision', async () => {
  const { context, records, request } = fixture('new');
  delete request.signedContractPath; delete request.signedContractUrl;
  records.set('storageRequests/vehicle', { ...request });
  await context.saveUploadedStorageContract(request, upload, { markContractReady: true });
  assert.equal(records.get('storageRequests/vehicle').status, 'contract_ready');
  assert.equal(records.size, 1);
});

test('failed or incomplete upload never replaces existing contract metadata', async () => {
  const { context, records, request } = fixture();
  await assert.rejects(context.saveUploadedStorageContract(request, null), /did not complete/);
  await assert.rejects(context.saveUploadedStorageContract(request, { path: upload.path }), /did not complete/);
  assert.equal(records.get('storageRequests/vehicle').signedContractPath, 'contracts/old.pdf');
  assert.equal(records.size, 1);
});

test('concurrent replacement, status changes and pending invoice/payment delivery are rejected atomically', async () => {
  for (const patch of [{ signedContractPath: 'contracts/other.pdf' }, { status: 'paid' }, { invoicePendingId: 'pending' }, { paymentPendingId: 'pending' }]) {
    const { context, records, request } = fixture();
    Object.assign(records.get('storageRequests/vehicle'), patch);
    const before = JSON.stringify([...records]);
    await assert.rejects(context.saveUploadedStorageContract(request, upload, { markContractReady: true }), /changed|pending/);
    assert.equal(JSON.stringify([...records]), before);
  }
});

test('regeneration uses current saved request and stable contract ID without replacing uploaded signed copy', async () => {
  const request = { id: 'vehicle', status: 'paid', contractId: 'CT-EXISTING', signedContractPath: 'old.pdf', vehicle: { model: 'Updated model' } };
  const calls = [];
  const context = vm.createContext({
    activeStorageContractRequestId: request.id, storageContractModalError: { textContent: '' }, storageContractBusy: false,
    generatedStorageContractUrl: null, generatedStorageContractBlob: null,
    getStorageRequestById: () => request, getExpectedStorageContractId: () => request.contractId,
    updateStorageContractButtons: () => {}, renderStorageContractLinks: () => {}, storageContractModalLinks: {},
    updateDoc: async () => { throw new Error('Regenerating an existing contract must not update request metadata'); },
    generateStorageContractPdf: async (data, contractId) => { calls.push({ data, contractId }); return { blob: 'new PDF', url: 'blob:new', filename: 'contract.pdf' }; }
  });
  vm.runInContext(implementation('generateAndDownloadStorageDraftContract'), context);
  await context.generateAndDownloadStorageDraftContract({ autoDownload: false });
  assert.equal(context.storageContractModalError.textContent, '');
  assert.equal(calls[0].data.vehicle.model, 'Updated model');
  assert.equal(calls[0].contractId, 'CT-EXISTING');
  assert.equal(context.generatedStorageContractUrl, 'blob:new');
  assert.equal(request.signedContractPath, 'old.pdf');
  assert.equal(request.status, 'paid');
});

test('request editor exposes Manage contract beside the uploaded file, including later workflow stages', () => {
  const element = () => ({ children: [], handlers: {}, appendChild(child) { this.children.push(child); }, addEventListener(name, handler) { this.handlers[name] = handler; }, closest() { return this.fieldset; } });
  const editor = element();
  editor.fieldset = { hidden: true };
  const opened = [];
  const latest = { id: 'request', status: 'waiting_contract', signedContractUrl: 'https://example.invalid/current.pdf' };
  const context = vm.createContext({
    storageContractLinks: editor, document: { createElement: element },
    getStorageRequestById: () => latest, openStorageContractModal: (request) => opened.push(request)
  });
  const start = source.indexOf('function renderStorageContractLinks(');
  vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), context);
  context.renderStorageContractLinks(editor, latest);
  assert.equal(editor.fieldset.hidden, false);
  assert.equal(editor.children[0].textContent, 'View/download signed contract');
  const button = editor.children.find((child) => child.textContent === 'Manage contract');
  assert.ok(button);
  assert.equal(button.type, 'button');
  button.handlers.click();
  assert.equal(opened[0], latest);
});
