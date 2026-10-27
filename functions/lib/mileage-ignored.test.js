import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
class Element {
  constructor() {
    this.children = []; this.dataset = {}; this.attributes = {}; this.textContent = '';
    const classes = new Set(); this.classList = { add: (value) => classes.add(value), remove: (value) => classes.delete(value), contains: (value) => classes.has(value) };
  }
  set innerHTML(value) { this.markup = value; this.children = []; }
  get innerHTML() { return this.markup || ''; }
  appendChild(child) { this.children.push(child); }
  setAttribute(name, value) { this.attributes[name] = value; }
}
function fixture(entries, logs = []) {
  const records = new Map(entries.map((entry) => [entry.id, structuredClone(entry)]));
  const list = new Element(); const section = new Element(); const pending = new Element(); const ignored = new Element(); const hide = new Element();
  const alerts = []; const writes = [];
  const context = vm.createContext({
    expenses: [...records.values()], mileageLogs: logs, missingMileageSuggestions: [], ignoredMileageSuggestions: [], mileageSuggestionView: 'pending', hideMileageSuggestions: false,
    accountLookup: new Map([['cash', { name: 'Bank' }], ['farm', { name: 'Farm' }]]),
    normalizeVendorKey: (value) => String(value || '').trim().toLowerCase(), getDateKey: (value) => value ? String(value).slice(0, 10) : '',
    formatDate: (value) => value || '', formatCurrency: (value) => `$${Number(value).toFixed(2)}`, formatTagLabel: (value) => value,
    mileageSuggestionSection: section, mileageSuggestionList: list, pendingMileageSuggestionsButton: pending, ignoredMileageSuggestionsButton: ignored, dismissMileageSuggestionsButton: hide,
    mileageSuggestionTitle: new Element(), mileageSuggestionDescription: new Element(),
    document: { createElement: () => new Element() }, db: {}, auth: { currentUser: { uid: 'admin' } }, serverTimestamp: () => 'timestamp',
    doc: (_db, collection, id) => ({ collection, id }),
    updateDoc: async (ref, patch) => {
      if (!records.has(ref.id)) throw new Error('Record not found');
      writes.push({ ref, patch }); Object.assign(records.get(ref.id), patch);
    }, window: { alert: (message) => alerts.push(message) }
  });
  for (const name of ['escapeEmailTemplateHtml', 'computeMissingMileageSuggestions', 'renderMissingMileageSuggestions', 'setMissingMileageIgnored', 'ignoreMissingMileageSuggestion']) {
    const start = source.includes(`async function ${name}(`) ? source.indexOf(`async function ${name}(`) : source.indexOf(`function ${name}(`);
    assert.ok(start >= 0, name);
    vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), context);
  }
  return { context, records, list, section, pending, ignored, hide, alerts, writes };
}
const expense = (extra = {}) => ({ id: 'a', entryType: 'expense', vendorTag: 'Store', date: '2026-10-01', amount: 25, accountId: 'cash', entityId: 'farm', description: 'Purchase', ...extra });

test('ignored expenses remain reviewable even when covered by mileage or missing a vendor/date', () => {
  const { context } = fixture([
    expense({ id: 'pending', vendorTag: 'Other' }), expense({ id: 'covered' }),
    expense({ id: 'ignored-covered', ignoreMissingMileage: true }),
    expense({ id: 'ignored-no-vendor', vendorTag: '', date: null, ignoreMissingMileage: true }),
    expense({ id: 'income', entryType: 'income', ignoreMissingMileage: true })
  ], [{ date: '2026-10-01', vendorTags: ['Store'] }]);
  context.computeMissingMileageSuggestions();
  assert.deepEqual(Array.from(context.missingMileageSuggestions, (entry) => entry.id), ['pending']);
  assert.deepEqual(Array.from(context.ignoredMileageSuggestions, (entry) => entry.id), ['ignored-covered', 'ignored-no-vendor']);
});

test('ignore and restore persist only dismissal/audit fields and survive rebuilding suggestions', async () => {
  const { context, records, writes } = fixture([expense()]);
  context.computeMissingMileageSuggestions();
  await context.ignoreMissingMileageSuggestion('a');
  context.computeMissingMileageSuggestions();
  assert.equal(context.missingMileageSuggestions.length, 0); assert.equal(context.ignoredMileageSuggestions.length, 1);
  await context.setMissingMileageIgnored('a', false);
  context.computeMissingMileageSuggestions();
  assert.equal(context.missingMileageSuggestions.length, 1); assert.equal(context.ignoredMileageSuggestions.length, 0);
  assert.equal(records.get('a').ignoreMissingMileage, false);
  assert.equal(records.get('a').amount, 25); assert.equal(records.get('a').date, '2026-10-01');
  assert.equal(records.get('a').description, 'Purchase');
  assert.deepEqual(Object.keys(writes[1].patch).sort(), ['ignoreMissingMileage', 'updatedAt', 'updatedBy']);
  const reloaded = fixture([...records.values()]); reloaded.context.computeMissingMileageSuggestions();
  assert.equal(reloaded.context.missingMileageSuggestions.length, 1);
});

test('the panel and Ignored button remain available when there are no pending suggestions', () => {
  const { context, section, pending, ignored, list } = fixture([expense({ ignoreMissingMileage: true })]);
  context.computeMissingMileageSuggestions(); context.renderMissingMileageSuggestions();
  assert.equal(section.classList.contains('hidden'), false);
  assert.equal(pending.textContent, 'Pending (0)'); assert.equal(ignored.textContent, 'Ignored (1)');
  context.mileageSuggestionView = 'ignored'; context.renderMissingMileageSuggestions();
  assert.equal(ignored.attributes['aria-pressed'], 'true');
  assert.ok(list.children[0].innerHTML.includes('Restore suggestion'));
  assert.ok(list.children[0].innerHTML.includes('Open ledger transaction'));
  assert.ok(list.children[0].innerHTML.includes('data-action="view-suggestion-ledger"'));
  assert.ok(!list.children[0].innerHTML.includes('data-action="log-missing-mileage"'));
});

test('restoring an already covered expense clears ignore without generating a duplicate mileage suggestion', async () => {
  const { context, records } = fixture([expense({ ignoreMissingMileage: true })], [{ date: '2026-10-01', vendorTag: 'Store' }]);
  await context.setMissingMileageIgnored('a', false); context.computeMissingMileageSuggestions();
  assert.equal(records.get('a').ignoreMissingMileage, false);
  assert.equal(context.missingMileageSuggestions.length, 0); assert.equal(context.ignoredMileageSuggestions.length, 0);
  assert.equal(context.mileageLogs.length, 1);
});

test('restore failure does not recreate a deleted expense or remove its ignored state optimistically', async () => {
  const { context, records, alerts, writes } = fixture([expense({ ignoreMissingMileage: true })]);
  context.computeMissingMileageSuggestions(); records.delete('a');
  await context.setMissingMileageIgnored('a', false);
  assert.equal(records.has('a'), false); assert.equal(writes.length, 0);
  assert.equal(context.ignoredMileageSuggestions.length, 1);
  assert.match(alerts[0], /Unable to restore suggestion/);
});

test('ignored suggestion content is escaped and empty panels reset Hide state', () => {
  const { context, list, section } = fixture([expense({ vendorTag: '<unsafe>', description: '<script>bad()</script>', ignoreMissingMileage: true })]);
  context.computeMissingMileageSuggestions(); context.mileageSuggestionView = 'ignored'; context.renderMissingMileageSuggestions();
  assert.ok(list.children[0].innerHTML.includes('&lt;script&gt;')); assert.ok(!list.children[0].innerHTML.includes('<script>'));
  context.expenses = []; context.hideMileageSuggestions = true; context.computeMissingMileageSuggestions(); context.renderMissingMileageSuggestions();
  assert.equal(section.classList.contains('hidden'), true); assert.equal(context.hideMileageSuggestions, false);
});
