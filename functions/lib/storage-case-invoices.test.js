import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
const start = source.indexOf('function renderStorageTable()');
const implementation = source.slice(start, source.indexOf('\n}\n', start) + 3);

function render(invoiceIds, status = 'stored', hasPaidReceipt = status === 'paid') {
  const element = (tag) => ({
    tag, children: [], dataset: {}, attributes: {},
    appendChild(child) { this.children.push(child); },
    setAttribute(name, value) { this.attributes[name] = value; }
  });
  const body = element('tbody');
  const context = vm.createContext({
    document: { createElement: element }, storageTableBody: body,
    storageRequests: invoiceIds.map((invoiceId, index) => ({
      id: `request-${index}`, clientId: 'client', season: 'winter',
      status: Array.isArray(status) ? status[index] : status, vehicle: { type: 'rv' }, invoiceId, paidInvoiceId: hasPaidReceipt ? invoiceId : undefined
    })),
    storageCasesExpanded: new Set(), clientLookup: new Map([['client', { name: 'Client' }]]),
    STORAGE_STATUS_OPTIONS: [{ value: 'stored' }, { value: 'paid' }, { value: 'picked_up' }, { value: 'waiting_deposit' }, { value: 'scheduled' }, { value: 'cancelled' }],
    STORAGE_RECEIPT_EMAIL_WORKFLOWS: { missing: { buttonLabel: 'Follow up' } },
    resolveStorageCaseId: () => 'case', compareStorageRequests: () => 0,
    resolveStorageAmount: () => 100, formatCurrency: (value) => `$${value}`,
    formatSeasonDisplayLabel: () => 'Winter', deriveStorageCaseStatus: () => 'Stored',
    getStorageLocation: () => 'indoor', normalizeReferralClientIds: () => [],
    formatStorageRequestAddons: () => 'None', formatStorageStatusLabel: () => 'Stored'
  });
  vm.runInContext(implementation, context);
  context.renderStorageTable();
  return body.children;
}

function invoices(element) {
  return [element, ...element.children.flatMap(invoices)]
    .filter((child) => child.dataset.action === 'view-storage-invoice');
}

test('shared invoice appears once on the case row, never on vehicle rows', () => {
  const rows = render(['INV-SHARED', 'INV-SHARED']);
  const links = invoices(rows[0]);
  assert.equal(links.length, 1);
  assert.equal(links[0].dataset.invoiceId, 'INV-SHARED');
  assert.equal(links[0].textContent, 'Invoice (PDF)');
  assert.equal(invoices(rows[0].children[2]).length, 1);
  assert.equal(invoices(rows[0].children[6]).length, 0);
  assert.ok(rows.slice(1).every((row) => invoices(row).length === 0));
  assert.equal(rows[0].children[5].textContent, '$200');
  assert.ok(rows.slice(1).every((row) => row.children[6].children.length === 3));
  assert.ok(rows.slice(1).every((row) => row.children[6].children.some((button) => button.dataset.action === 'storage-request-contract')));
});

test('distinct historical invoices remain accessible at case level', () => {
  const rows = render(['INV-FIRST', 'INV-SECOND', 'INV-FIRST']);
  assert.deepEqual(invoices(rows[0]).map((link) => link.dataset.invoiceId), ['INV-FIRST', 'INV-SECOND']);
  assert.deepEqual(invoices(rows[0]).map((link) => link.textContent), ['Invoice 1 (PDF)', 'Invoice 2 (PDF)']);
  assert.ok(rows.slice(1).every((row) => invoices(row).length === 0));
});

test('cases without invoices retain their existing actions', () => {
  const rows = render([undefined]);
  assert.equal(invoices(rows[0]).length, 0);
  assert.equal(rows[0].children[6].children[0].dataset.action, 'storage-case-action');
});
test('a cancelled vehicle does not block the remaining Scheduled or Stored case actions', () => {
  const scheduled = render([undefined, undefined], ['scheduled', 'cancelled']);
  const scheduledActions = scheduled[0].children[6].children[0].children.map((option) => option.value);
  assert.ok(scheduledActions.includes('status:stored'));
  const stored = render(['INV-SHARED', 'INV-SHARED'], ['stored', 'cancelled']);
  const storedActions = stored[0].children[6].children[0].children.map((option) => option.value);
  assert.ok(storedActions.includes('status:paid'));
  assert.ok(storedActions.includes('reopen-invoice'));
  assert.ok(!storedActions.includes('generate-contract'));
});
test('contract-received cases offer scheduling without deposit alongside normal deposit receipt', () => {
  const rows = render([undefined, undefined], 'waiting_deposit');
  const actions = rows[0].children[6].children[0].children.map((option) => option.value);
  assert.ok(actions.includes('schedule-no-deposit'));
  assert.ok(actions.includes('receipt:deposit'));
  assert.ok(actions.includes('receipt:missing'));
});

test('Stored offers payment preview while Paid offers pickup and a case-level paid PDF', () => {
  const stored = render(['INV-SHARED']);
  const storedActions = stored[0].children[6].children[0].children.map((option) => option.value);
  assert.ok(storedActions.includes('status:paid'));
  const paid = render(['INV-SHARED', 'INV-SHARED'], 'paid');
  const actions = paid[0].children[6].children[0].children.map((option) => option.value);
  assert.ok(actions.includes('status:picked_up'));
  assert.ok(!actions.includes('status:paid'));
  const all = (element) => [element, ...element.children.flatMap(all)];
  const paidLinks = all(paid[0]).filter((element) => element.dataset.action === 'view-storage-paid');
  assert.equal(paidLinks.length, 1);
  assert.equal(paidLinks[0].textContent, 'Paid (PDF)');
  assert.equal(invoices(paid[0]).length, 0);
  assert.ok(paid.slice(1).every((row) => all(row).every((element) => element.dataset.action !== 'view-storage-paid')));
});

test('paid receipt remains the only PDF after pickup; legacy unpaid pickup retains its invoice', () => {
  const paid = render(['INV-SHARED'], 'picked_up', true);
  assert.equal(invoices(paid[0]).length, 0);
  const links = paid[0].children[2].children[1].children;
  assert.equal(links.length, 1);
  assert.equal(links[0].dataset.action, 'view-storage-paid');
  const unpaid = render(['INV-SHARED'], 'picked_up', false);
  assert.equal(invoices(unpaid[0]).length, 1);
});

function pdfFixture({ blocked = false, fail = false, missing = false } = {}) {
  const calls = [];
  const tab = { opener: {}, location: { replace: (url) => calls.push(['navigate', url]) }, close: () => calls.push(['close']) };
  const context = vm.createContext({
    window: { open: () => { calls.push(['open']); return blocked ? null : tab; } },
    getStorageInvoiceCallable: async ({ invoiceId }) => {
      calls.push(['get', invoiceId]);
      if (fail) throw new Error('Unable to load invoice');
      return { data: missing ? {} : { pdfBase64: 'JVBERg==' } };
    },
    atob: (value) => Buffer.from(value, 'base64').toString('binary'), Uint8Array, Blob,
    URL: { createObjectURL: (blob) => { calls.push(['blob', blob.type]); return 'blob:invoice'; }, revokeObjectURL: (url) => calls.push(['revoke', url]) },
    setTimeout: (callback) => { context.cleanup = callback; }
  });
  const pdfStart = source.indexOf('async function openSavedStorageInvoicePdf(');
  vm.runInContext(source.slice(pdfStart, source.indexOf('\n}\n', pdfStart) + 3), context);
  return { context, calls, tab };
}

test('saved invoice opens only its PDF in a separate tab', async () => {
  const { context, calls, tab } = pdfFixture();
  await context.openSavedStorageInvoicePdf('INV-SHARED');
  assert.deepEqual(calls, [['open'], ['get', 'INV-SHARED'], ['blob', 'application/pdf'], ['navigate', 'blob:invoice']]);
  assert.equal(tab.opener, null);
  context.cleanup();
  assert.deepEqual(calls.at(-1), ['revoke', 'blob:invoice']);
});

test('PDF loading failure closes the reserved tab', async () => {
  for (const options of [{ fail: true }, { missing: true }]) {
    const { context, calls } = pdfFixture(options);
    await assert.rejects(context.openSavedStorageInvoicePdf('INV-SHARED'));
    assert.deepEqual(calls.at(-1), ['close']);
  }
});

test('blocked popup reports an actionable error without fetching the invoice', async () => {
  const { context, calls } = pdfFixture({ blocked: true });
  await assert.rejects(context.openSavedStorageInvoicePdf('INV-SHARED'), /Allow pop-ups/);
  assert.deepEqual(calls, [['open']]);
});
