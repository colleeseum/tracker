import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createLockedReportService, lockedReportId } from './locked-reports.js';
import { createReportLockController, lockedReportDocument } from '../../public/locked-reports.js';

const filters = { type: 'balance', entityId: 'farm', year: 2025 };
const html = '<!doctype html><html><head><meta http-equiv="Content-Security-Policy" content="default-src \'none\'"><style>body{color:black}</style></head><body>Expense $123; rate 0.72</body></html>';
function fixture() {
  const records = new Map();
  const snap = (path) => ({ exists: records.has(path), id: path.split('/').at(-1), data: () => structuredClone(records.get(path)) });
  const ref = (path) => ({ path, get: async () => snap(path), collection: (name) => ({ doc: (key) => ref(`${path}/${name}/${key}`) }) });
  let queue = Promise.resolve();
  const db = { collection: (name) => ({ doc: (key) => ref(`${name}/${key}`) }), runTransaction(handler) {
    const work = queue.then(async () => {
      const writes = [];
      const result = await handler({ get: async (ref) => snap(ref.path), set: (ref, value) => writes.push([ref.path, structuredClone(value)]) });
      writes.forEach(([key, value]) => records.set(key, value)); return result;
    }); queue = work.catch(() => {}); return work;
  } };
  return { records, service: createLockedReportService({ db, fieldValue: { serverTimestamp: () => 1234567890000 } }) };
}

test('locking freezes HTML, formatting, reference inputs and actor metadata without overwriting', async () => {
  const { service, records } = fixture();
  assert.deepEqual(await service.get(filters), { locked: false });
  const inputs = { mileageRate: 0.72, ccaRate: 0.3, ledgerEntries: [{ amount: 123 }], taxCodes: [9819] };
  const first = await service.lock({ ...filters, entityName: 'Farm', html, inputs }, { uid: 'admin', email: 'admin@example.invalid' });
  inputs.mileageRate = 999; inputs.ledgerEntries[0].amount = 999;
  records.set('mileageRates/2025', { rate: 999 });
  records.set('accounts/farm', { name: 'Changed name' });
  const reopened = await service.get(filters);
  assert.equal(reopened.html, html);
  assert.equal(reopened.inputs.mileageRate, 0.72);
  assert.equal(reopened.inputs.ledgerEntries[0].amount, 123);
  assert.equal(reopened.metadata.entityName, 'Farm');
  assert.equal(reopened.metadata.lockedBy, 'admin');
  const again = await service.lock({ ...filters, html: html.replace('$123', '$999'), inputs }, { uid: 'another' });
  assert.equal(again.html, first.html);
  assert.equal(again.metadata.sha256, first.metadata.sha256);
});

test('concurrent locks preserve the first snapshot; different entities, years and report types remain separate', async () => {
  const { service } = fixture();
  const first = { ...filters, html, inputs: { version: 'first' } };
  const second = { ...filters, html: html.replace('$123', '$999'), inputs: { version: 'second' } };
  await Promise.all([service.lock(first, { uid: 'a' }), service.lock(second, { uid: 'b' })]);
  assert.equal((await service.get(filters)).inputs.version, 'first');
  for (const other of [{ ...filters, year: 2024 }, { ...filters, type: 'tags' }, { ...filters, entityId: 'other' }]) assert.deepEqual(await service.get(other), { locked: false });
});

test('multibyte large snapshots are chunked and verified; missing or tampered content fails closed', async () => {
  const { service, records } = fixture();
  const large = html.replace('</body>', `${'É🚗'.repeat(90000)}</body>`);
  await service.lock({ ...filters, html: large, inputs: { rate: 0.72 } }, { uid: 'a' });
  assert.equal((await service.get(filters)).html, large);
  const key = `lockedReports/${lockedReportId(filters)}`;
  assert.ok(records.get(key).chunkCount > 1);
  records.get(`${key}/content/0`).text += 'tampered';
  await assert.rejects(service.get(filters), /integrity/);
  records.delete(`${key}/content/0`);
  await assert.rejects(service.get(filters), /missing/);
});

test('invalid and oversized snapshots do not create partial locks', async () => {
  const { service, records } = fixture();
  for (const extra of [{ year: NaN }, { entityId: 'bad/id' }, { type: 'other' }, { inputs: null }, { html: '<html>not static</html>' }, { html: html + 'x'.repeat(4 * 1024 * 1024) }]) {
    await assert.rejects(service.lock({ ...filters, html, inputs: {}, ...extra }, { uid: 'a' }));
  }
  assert.equal(records.size, 0);
});

class Element {
  constructor() { this.value = ''; this.children = []; this.listeners = {}; this.textContent = ''; this.attributes = {}; const classes = new Set(); this.classList = { add: (value) => classes.add(value), remove: (value) => classes.delete(value), contains: (value) => classes.has(value) }; }
  appendChild(child) { this.children.push(child); }
  replaceChildren(...children) { this.children = children; }
  querySelector(selector) { return selector === 'iframe' ? this.children.find((child) => child.tagName === 'iframe') : null; }
  setAttribute(key, value) { this.attributes[key] = value; }
  addEventListener(name, handler) { this.listeners[name] = handler; }
  querySelectorAll() { return []; }
  cloneNode() { return Object.assign(new Element(), { innerHTML: this.innerHTML || '' }); }
  get outerHTML() { return `<${this.tagName || 'div'}>${this.textContent || this.innerHTML || ''}</${this.tagName || 'div'}>`; }
}
function controllerFixture(options = {}) {
  const nodes = Object.fromEntries(['output', 'status', 'lockButton', 'downloadButton'].map((key) => [key, new Element()]));
  let liveCalls = 0; let getCalls = 0;
  const state = { filters: { ...filters }, rate: 0.72, generated: true };
  const saved = { locked: true, html, inputs: { mileageRate: 0.72 }, metadata: { ...filters, entityName: 'Farm', sha256: 'original', lockedAt: 1234567890000 } };
  const controller = createReportLockController({ ...nodes,
    getFilters: () => state.filters, isGenerated: () => state.generated, canLock: () => true,
    renderLive: () => { liveCalls++; }, captureInputs: () => ({ rate: state.rate }),
    getSaved: async () => { getCalls++; return saved; }, save: async () => saved, getCss: async () => 'body{}', ...options
  });
  return { controller, nodes, state, saved, liveCalls: () => liveCalls, getCalls: () => getCalls };
}
test('locked report renders an inert static page and never invokes live calculations on data updates', async () => {
  const original = globalThis.document;
  globalThis.document = { createElement: (tagName) => Object.assign(new Element(), { tagName }) };
  try {
    const { controller, nodes, state, liveCalls, getCalls } = controllerFixture();
    await controller.render();
    const frame = nodes.output.children[0];
    assert.ok(frame.srcdoc.includes('LOCKED REPORT'));
    assert.ok(frame.srcdoc.includes('Locked on'));
    assert.ok(frame.srcdoc.includes('Expense $123; rate 0.72'));
    assert.equal(frame.attributes.sandbox, '');
    state.rate = 999;
    await controller.render(); await controller.render();
    assert.equal(liveCalls(), 0); assert.equal(getCalls(), 1);
    assert.equal(nodes.output.children[0], frame);
    assert.equal(nodes.lockButton.disabled, true);
    assert.match(nodes.status.textContent, /no recalculation/);
  } finally { globalThis.document = original; }
});

test('a missing lock allows live reports, but a failed lock check never falls back to recalculation', async () => {
  const live = controllerFixture({ getSaved: async () => ({ locked: false }) });
  await live.controller.render(); assert.equal(live.liveCalls(), 1); assert.equal(live.nodes.lockButton.disabled, false);
  const failed = controllerFixture({ getSaved: async () => { throw new Error('Offline'); } });
  await failed.controller.render(); assert.equal(failed.liveCalls(), 0); assert.equal(failed.nodes.lockButton.disabled, true);
  assert.match(failed.nodes.output.textContent, /Offline/);
});

test('locking pairs the displayed HTML with render-time reference inputs, even during async CSS loading', async () => {
  const original = globalThis.document;
  globalThis.document = { createElement: (tagName) => Object.assign(new Element(), { tagName }) };
  try {
    let resolveCss; let payload;
    const { controller, nodes, state } = controllerFixture({
      getSaved: async () => ({ locked: false }), confirmLock: () => true,
      getCss: () => new Promise((resolve) => { resolveCss = resolve; }),
      save: async (value) => {
        payload = value;
        return { locked: true, html: value.html, inputs: value.inputs, metadata: { ...value, lockedAt: 1234567890000, sha256: 'saved' } };
      }
    });
    await controller.render();
    nodes.output.innerHTML = '<section>Tax total $123.00</section>';
    const saving = nodes.lockButton.listeners.click();
    state.rate = 999; nodes.output.innerHTML = '<section>Later total $999.00</section>';
    resolveCss('body{color:black}'); await saving;
    assert.equal(payload.inputs.rate, 0.72);
    assert.ok(payload.html.includes('Tax total $123.00'));
    assert.ok(!payload.html.includes('Later total'));
    assert.ok(payload.html.includes("default-src 'none'"));
    assert.ok(payload.html.includes('body{color:black}'));
    assert.equal(nodes.lockButton.disabled, true);
    assert.ok(nodes.output.children[0].srcdoc.includes(payload.html.match(/<section>.*<\/section>/)[0]));
    assert.ok(nodes.output.children[0].srcdoc.includes('LOCKED REPORT'));
  } finally { globalThis.document = original; }
});

test('Open / generate automatically opens the matching locked report using normal report filters', async () => {
  const original = globalThis.document;
  globalThis.document = { createElement: (tagName) => Object.assign(new Element(), { tagName }) };
  try {
    const requested = [];
    const { controller, nodes, state, liveCalls, saved } = controllerFixture({ getSaved: async (filters) => { requested.push(filters); return saved; } });
    state.generated = false;
    await controller.render();
    // Same sequence as submitting the existing Open / generate form.
    controller.clearSelection(); state.filters = { ...filters }; state.generated = true;
    await controller.render();
    assert.deepEqual(requested, [filters]);
    assert.ok(nodes.output.children[0].srcdoc.includes('LOCKED REPORT'));
    // Only the initial empty-state render runs, never locked-report calculation.
    assert.equal(liveCalls(), 1);
    assert.equal(nodes.lockButton.disabled, true);
    assert.ok(nodes.status.classList.contains('report-lock-notice'));
  } finally { globalThis.document = original; }
});

test('lock notice decorates the saved report and download without modifying its frozen values', () => {
  const saved = { html, metadata: { lockedAt: 1234567890000, lockedByEmail: '<unsafe>@example.invalid' } };
  const document = lockedReportDocument(saved);
  assert.ok(document.includes('<body><aside role="status"'));
  assert.ok(document.includes('LOCKED REPORT'));
  assert.ok(document.includes('Locked on'));
  assert.ok(document.includes('America/Toronto'));
  assert.ok(document.includes('Expense $123; rate 0.72'));
  assert.ok(!document.includes('<unsafe>'));
  assert.equal(saved.html, html);
  const markup = readFileSync(new URL('../../public/index.html', import.meta.url), 'utf8');
  assert.ok(!markup.includes('locked-report-select'));
  assert.ok(!markup.includes('Choose a saved report'));
});

test('late lock responses cannot replace another selected report or a signed-out page', async () => {
  const resolvers = [];
  const { controller, state, liveCalls, nodes } = controllerFixture({ getSaved: () => new Promise((resolve) => resolvers.push(resolve)) });
  const first = controller.render(); state.filters = { ...filters, year: 2024 };
  const second = controller.render(); resolvers[1]({ locked: false }); await second;
  resolvers[0]({ locked: false }); await first; assert.equal(liveCalls(), 1);
  state.filters = { ...filters, year: 2023 };
  const third = controller.render(); controller.reset(); resolvers[2]({ locked: false }); await third;
  assert.equal(liveCalls(), 1); assert.equal(nodes.output.children.length, 0);
});

test('Firestore catch-all excludes locked snapshots, and report rendering checks the lock first', () => {
  const rules = readFileSync(new URL('../../firestore.rules', import.meta.url), 'utf8');
  assert.ok(rules.includes("allow write: if isAuthorizedUser() && collection != 'lockedReports'"));
  const app = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
  assert.match(app, /function renderReports\(\) \{\s*reportLockController\?\.render\(\);\s*\}/);
  assert.ok(app.includes('reportLogicVersion:'));
});
