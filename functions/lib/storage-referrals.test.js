import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
const clients = [
  { id: 'tenant', name: 'Tenant' }, { id: 'alice', name: 'Alice' },
  { id: 'bob', name: 'Bob', nonStorageClient: true }
];
const select = {
  options: [],
  get selectedOptions() { return this.options.filter((option) => option.selected); },
  replaceChildren() { this.options = []; },
  appendChild(option) { this.options.push(option); }
};
const context = vm.createContext({
  clients, clientLookup: new Map(clients.map((client) => [client.id, client])),
  storageClientSelect: { value: 'tenant' }, storageReferredBySelect: select,
  document: { createElement: () => ({ value: '', textContent: '', selected: false }) }
});
for (const name of ['normalizeReferralClientIds', 'updateStorageReferralOptions']) {
  const start = source.indexOf(`function ${name}(`);
  assert.ok(start >= 0);
  vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), context);
}
const normalized = (value, tenant) => JSON.parse(JSON.stringify(context.normalizeReferralClientIds(value, tenant)));

test('legacy requests without referrals remain empty; malformed values do not grant references', () => {
  for (const value of [undefined, null, '', 'alice', {}, 42]) assert.deepEqual(normalized(value), []);
});
test('multiple referrals normalize, deduplicate and exclude self-referrals', () => {
  assert.deepEqual(normalized(['alice', ' bob ', 'alice', '', null, 4, 'tenant'], 'tenant'), ['alice', 'bob']);
});
test('existing referrals load and survive a client-list refresh, including non-storage clients', () => {
  context.updateStorageReferralOptions(['alice', 'bob']);
  assert.deepEqual(select.selectedOptions.map((option) => option.value), ['alice', 'bob']);
  context.updateStorageReferralOptions();
  assert.deepEqual(select.selectedOptions.map((option) => option.value), ['alice', 'bob']);
  assert.ok(!select.options.some((option) => option.value === 'tenant'));
});
test('deleted client references remain visible until explicitly removed', () => {
  context.updateStorageReferralOptions(['alice', 'deleted']);
  assert.match(select.options.find((option) => option.value === 'deleted').textContent, /Unavailable client/);
  context.updateStorageReferralOptions();
  assert.deepEqual(select.selectedOptions.map((option) => option.value), ['alice', 'deleted']);
  select.options.find((option) => option.value === 'deleted').selected = false;
  context.updateStorageReferralOptions();
  assert.deepEqual(select.selectedOptions.map((option) => option.value), ['alice']);
});
test('changing tenant removes only that tenant from the referral selection', () => {
  context.storageClientSelect.value = 'tenant';
  context.updateStorageReferralOptions(['alice', 'bob']);
  context.storageClientSelect.value = 'alice';
  context.updateStorageReferralOptions();
  assert.deepEqual(select.selectedOptions.map((option) => option.value), ['bob']);
  context.storageClientSelect.value = 'tenant';
});
