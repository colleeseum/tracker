import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { findMileageLedgerEntries } from '../../public/mileage-ledger.js';

const helpers = { dateKey: (value) => value ? String(value).slice(0, 10) : '', vendorKey: (value) => String(value || '').trim().toLowerCase() };
const entry = (extra = {}) => ({ id: 'expense-a', transactionId: 'transaction-a', entryType: 'expense', date: '2026-10-27', vendorTag: 'Store', entityId: 'farm', ...extra });
const trip = (extra = {}) => ({ id: 'trip', date: '2026-10-27', vendorTags: ['Store'], entityId: 'farm', ...extra });
test('saved source IDs open the exact ledger record even if dates, vendor or entity have since changed', () => {
  const original = entry({ date: '2025-01-01', vendorTag: 'Renamed', entityId: 'other' });
  const result = findMileageLedgerEntries(trip({ sourceExpenseId: original.id }), [original, entry({ id: 'different' })], helpers);
  assert.equal(result.kind, 'linked'); assert.deepEqual(result.entries.map((item) => item.id), ['expense-a']);
});
test('a deleted explicit source does not silently fall back to another matching transaction', () => {
  const result = findMileageLedgerEntries(trip({ sourceExpenseId: 'deleted', sourceTransactionId: 'transaction-a' }), [entry()], helpers);
  assert.equal(result.kind, 'linked'); assert.equal(result.entries.length, 0);
});
test('older trips match date/vendor/entity and keep multiple candidates instead of guessing', () => {
  const result = findMileageLedgerEntries(trip({ vendorTags: [' STORE '] }), [entry(), entry({ id: 'b' }), entry({ id: 'income', entryType: 'income' }), entry({ id: 'other-entity', entityId: 'other' }), entry({ id: 'other-day', date: '2026-10-26' }), entry({ id: 'opening', isVirtualOpening: true })], helpers);
  assert.equal(result.kind, 'matching'); assert.deepEqual(result.entries.map((item) => item.id), ['expense-a', 'b']);
  assert.equal(findMileageLedgerEntries(trip({ vendorTags: [] }), [entry()], helpers).entries.length, 0);
  assert.equal(findMileageLedgerEntries(trip({ date: null }), [entry()], helpers).entries.length, 0);
});
test('transaction-only links and legacy single-vendor trips remain supported', () => {
  assert.equal(findMileageLedgerEntries(trip({ sourceTransactionId: 'transaction-a' }), [entry()], helpers).kind, 'linked');
  assert.equal(findMileageLedgerEntries({ date: '2026-10-27', vendorTag: 'Store' }, [entry()], helpers).entries.length, 1);
});

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
function load(context, name) {
  const start = source.indexOf(`function ${name}(`);
  vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), context);
}
test('opening a linked ledger entry is read-only until explicit Save and handles missing records safely', () => {
  const opened = []; const alerts = []; const choices = [];
  const expenses = [entry(), entry({ id: 'b', transactionId: 'transaction-b' }), entry({ id: 'transfer', category: 'Transfer' })];
  const context = vm.createContext({ expenses, expenseLookup: new Map(expenses.map((item) => [item.id, item])),
    findMileageLedgerEntries, getDateKey: helpers.dateKey, normalizeVendorKey: helpers.vendorKey,
    startEditEntry: (item) => opened.push(item.id), startEditTransfer: (item) => opened.push(`transfer:${item.id}`), window: { alert: (message) => alerts.push(message) },
    registerReportDrilldown: (ids, label) => { choices.push({ ids, label }); return 'choice'; }, openReportDetailModalById: (id) => { assert.equal(id, 'choice'); }
  });
  load(context, 'openMileageLedgerEntry'); load(context, 'openMileageLedgerEntries');
  context.openMileageLedgerEntry('expense-a'); assert.deepEqual(opened, ['expense-a']);
  context.openMileageLedgerEntry('transfer'); assert.equal(opened.at(-1), 'transfer:transfer');
  context.openMileageLedgerEntry('deleted'); assert.match(alerts.at(-1), /no longer available/);
  context.openMileageLedgerEntries(trip());
  assert.equal(opened.length, 2); assert.equal(choices.length, 1);
  assert.match(choices[0].label, /Matching ledger transactions/);
  assert.deepEqual(Array.from(choices[0].ids), ['expense-a', 'b', 'transfer']);
});

test('logging from a suggestion persists its source; manual trips do not inherit it and edits preserve it', async () => {
  const writes = []; const fields = {};
  for (const name of ['mileageDateInput', 'mileagePurposeInput', 'mileageKmInput', 'mileageTagsInput', 'mileageVendorTagInput', 'mileageStartingPointSelect']) fields[name] = { value: '', focus() {} };
  const context = vm.createContext({ ...fields, editingMileageId: 'previous-edit', mileageSourceExpenseId: 'previous-source', mileageSourceTransactionId: 'old-transaction',
    mileageForm: { reset() {}, addEventListener(_event, handler) { context.submit = handler; } }, mileageSubmitButton: null, mileageCancelButton: null, mileageFormTitle: null,
    applyMileageInputDefaults() {}, updateMileageStartingPointOptions() {}, openMileageModal() {},
    closeMileageModal: () => context.resetMileageFormState(), parseMileageTags: (value) => value ? value.split(',') : [], getResolvedMileageEntityId: () => 'farm',
    auth: { currentUser: { uid: 'admin', email: 'admin@example.invalid' } }, db: {}, Timestamp: { fromDate: (date) => date.toISOString() }, serverTimestamp: () => 'timestamp',
    collection: (_db, name) => name, doc: (_db, name, id) => `${name}/${id}`,
    addDoc: async (path, data) => writes.push({ path, data }), updateDoc: async (path, data) => writes.push({ path, data }), console, window: { alert: assert.fail }
  });
  load(context, 'resetMileageFormState'); load(context, 'openMileageModalForSuggestion');
  const start = source.indexOf("mileageForm.addEventListener('submit'");
  vm.runInContext(source.slice(start, source.indexOf('\n  });', start) + 6), context);
  context.openMileageModalForSuggestion({ id: 'expense-a', transactionId: 'transaction-a', dateKey: '2026-10-27', vendorLabel: 'Store', description: 'Purchase' });
  fields.mileageKmInput.value = '12';
  await context.submit({ preventDefault() {} });
  assert.equal(writes[0].path, 'mileageLogs');
  assert.equal(writes[0].data.sourceExpenseId, 'expense-a'); assert.equal(writes[0].data.sourceTransactionId, 'transaction-a');
  assert.equal(writes[0].data.kilometres, 12);
  assert.equal(context.mileageSourceExpenseId, null);
  fields.mileageDateInput.value = '2026-10-27'; fields.mileagePurposeInput.value = 'Manual trip'; fields.mileageKmInput.value = '8';
  await context.submit({ preventDefault() {} });
  assert.equal(writes[1].data.sourceExpenseId, undefined);
  context.editingMileageId = 'trip-existing'; fields.mileagePurposeInput.value = 'Edited';
  await context.submit({ preventDefault() {} });
  assert.equal(writes[2].path, 'mileageLogs/trip-existing');
  assert.equal(Object.hasOwn(writes[2].data, 'sourceExpenseId'), false);
  assert.ok(writes.every((write) => write.path.startsWith('mileageLogs')));
});
