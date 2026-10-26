import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { createClientEmailBatchService } from './client-email-batches.js';
import { createEmailSafetyPolicy, createSafeMailer } from './email-safety.js';
import { filterMailingClients, initializeClientMailing } from '../../public/client-mailing.js';

function fixture({ development = false, sender } = {}) {
  const records = new Map([
    ['clients/alice', { name: 'Alice', email: 'alice@example.invalid', emailCc: 'partner@example.invalid', preferredLanguage: 'fr' }],
    ['clients/bob', { name: 'Bob', email: 'bob@example.invalid', preferredLanguage: 'en', nonStorageClient: true }]
  ]);
  let sequence = 0;
  let queue = Promise.resolve();
  let failFinal = false;
  const snap = (path) => ({ id: path.split('/').at(-1), exists: records.has(path), data: () => structuredClone(records.get(path)) });
  const set = (ref, data, options) => records.set(ref.path, structuredClone(options?.merge ? { ...records.get(ref.path), ...data } : data));
  const ref = (path) => ({ path, id: path.split('/').at(-1), get: async () => snap(path), set: async (data) => set({ path }, data) });
  const db = {
    collection: (collection) => ({ doc: (id = `batch-${++sequence}`) => ref(`${collection}/${id}`) }),
    runTransaction(handler) {
      const work = queue.then(async () => {
        const writes = [];
        const result = await handler({ get: async (ref) => snap(ref.path), set: (...args) => writes.push(args) });
        if (failFinal && writes.some(([ref]) => ref.path.startsWith('clientEmails/'))) throw new Error('Database unavailable after SMTP acceptance');
        writes.forEach((args) => set(...args));
        return result;
      });
      queue = work.catch(() => {});
      return work;
    }
  };
  const messages = [];
  let policy = createEmailSafetyPolicy(development ? { FUNCTIONS_EMULATOR: 'true' } : {});
  const mailer = createSafeMailer({ sendMail: async (message) => {
    messages.push(message);
    return sender ? sender(message) : { accepted: [message.to], rejected: [] };
  } }, policy);
  const service = createClientEmailBatchService({ db, fieldValue: { serverTimestamp: () => 'timestamp' }, from: 'office@as-colle.com',
    renderEmail: (data) => JSON.stringify(data), routeEmail: (message) => policy.preview(message), sendEmail: (message) => mailer.sendMail(message) });
  const prepare = (extra = {}) => service.prepare({ clientIds: ['alice', 'bob'], subject: 'News', body: 'Hello everyone', ...extra }, 'admin');
  return { service, records, messages, prepare, failFinal: () => { failFinal = true; }, switchToDev: () => { policy = createEmailSafetyPolicy({ FUNCTIONS_EMULATOR: 'true' }); } };
}

test('preparing a mailing sends nothing and produces individual recipients with their own CC and locale', async () => {
  const { prepare, messages, records } = fixture();
  const preview = await prepare({ clientIds: ['alice', 'alice', 'bob'] });
  assert.equal(messages.length, 0);
  assert.equal(preview.recipients.length, 2);
  assert.equal(preview.recipients[0].delivery.to, 'alice@example.invalid');
  assert.equal(preview.recipients[0].delivery.cc, 'entrepot@as-colle.com, partner@example.invalid');
  assert.equal(preview.recipients[1].delivery.to, 'bob@example.invalid');
  assert.equal(preview.recipients[1].delivery.cc, 'warehouse@as-colle.com');
  assert.ok(preview.templates['fr-storage'].html.includes('"isStorageClient":true'));
  assert.ok(preview.templates['en-general'].html.includes('"isStorageClient":false'));
  assert.equal(records.get(`clientEmailBatches/${preview.batchId}`).createdBy, 'admin');
});

test('sending and concurrent retries deliver each client separately exactly once and persist results', async () => {
  const { prepare, service, messages, records } = fixture();
  const preview = await prepare();
  const payload = { batchId: preview.batchId, clientId: 'alice', fingerprint: preview.fingerprint, to: 'attacker@example.invalid' };
  await Promise.all([service.send(payload, 'admin'), service.send(payload, 'admin')]);
  await service.send(payload, 'admin');
  await service.send({ ...payload, clientId: 'bob' }, 'admin');
  assert.equal(messages.length, 2);
  assert.equal(messages[0].to, 'alice@example.invalid');
  assert.equal(messages[1].to, 'bob@example.invalid');
  assert.ok(!messages[0].html.includes('bob@example.invalid'));
  assert.ok(records.has(`clientEmails/${preview.batchId}_alice`));
  assert.deepEqual((await service.get(preview.batchId)).recipients.map((item) => item.status), ['sent', 'sent']);
});

test('dev routing suppresses every customer To/CC and the actual SMTP envelope', async () => {
  const { prepare, service, messages } = fixture({ development: true });
  const preview = await prepare();
  for (const recipient of preview.recipients) {
    assert.equal(recipient.delivery.to, 'entrepot@as-colle.com');
    assert.equal(recipient.delivery.cc, '');
    await service.send({ batchId: preview.batchId, clientId: recipient.clientId, fingerprint: preview.fingerprint }, 'admin');
  }
  assert.equal(messages.length, 2);
  for (const message of messages) {
    assert.equal(message.to, 'entrepot@as-colle.com');
    assert.equal(message.cc, undefined);
    assert.deepEqual(message.envelope.to, ['entrepot@as-colle.com']);
    assert.match(message.subject, /^\[DEV\]/);
  }
});

test('definite rejection is retryable, accepted and uncertain deliveries are never retried automatically', async () => {
  for (const kind of ['rejected', 'timeout', 'partial']) {
    let attempts = 0;
    const { prepare, service, messages } = fixture({ sender: (message) => {
      attempts++;
      if (kind === 'rejected' && attempts === 1) return { accepted: [], rejected: [message.to] };
      if (kind === 'timeout') throw Object.assign(new Error('Timeout'), { code: 'ETIMEDOUT' });
      if (kind === 'partial') return { accepted: [message.to], rejected: ['partner@example.invalid'] };
      return { accepted: [message.to] };
    } });
    const preview = await prepare();
    const payload = { batchId: preview.batchId, clientId: 'alice', fingerprint: preview.fingerprint };
    const first = await service.send(payload, 'admin');
    assert.equal(first.status, kind === 'rejected' ? 'failed' : kind === 'timeout' ? 'uncertain' : 'sent_with_warning');
    await service.send(payload, 'admin');
    assert.equal(messages.length, kind === 'rejected' ? 2 : 1);
  }
});

test('database failure after SMTP acceptance leaves a locked recipient, never a resendable one', async () => {
  const { prepare, service, messages, failFinal } = fixture();
  const preview = await prepare(); failFinal();
  const payload = { batchId: preview.batchId, clientId: 'alice', fingerprint: preview.fingerprint };
  await assert.rejects(service.send(payload, 'admin'), /Database unavailable/);
  assert.equal((await service.send(payload, 'admin')).status, 'sending');
  assert.equal(messages.length, 1);
});

test('bad IDs, invalid addresses, duplicate addresses and stale or differently routed previews are rejected', async () => {
  const { prepare, service, records, switchToDev, messages } = fixture();
  for (const extra of [{ clientIds: [] }, { clientIds: ['missing'] }, { clientIds: ['bad/id'] }, { subject: '' }, { subject: 'Header\r\ninjection' }, { body: '' }, { clientIds: Array(101).fill('alice') }]) {
    await assert.rejects(prepare(extra));
  }
  records.get('clients/bob').email = 'ALICE@example.invalid';
  await assert.rejects(prepare(), /share/);
  records.get('clients/bob').email = 'bad address';
  await assert.rejects(prepare(), /valid email/);
  records.get('clients/bob').email = 'bob@example.invalid';
  const preview = await prepare();
  const payload = { batchId: preview.batchId, clientId: 'alice', fingerprint: preview.fingerprint };
  await assert.rejects(service.send({ ...payload, fingerprint: 'bad' }, 'admin'), /preview changed/);
  await assert.rejects(service.send({ ...payload, clientId: 'constructor' }, 'admin'), /not part/);
  switchToDev();
  await assert.rejects(service.send(payload, 'admin'), /routing changed/);
  assert.equal(messages.length, 0);
});

test('season and status filters match the same request and do not duplicate clients with multiple vehicles', () => {
  const clients = [{ id: 'a', name: 'Alice', email: 'a@x' }, { id: 'b', name: 'Bob', emailCc: 'partner@x' }];
  const requests = [{ clientId: 'a', season: 'winter', status: 'stored' }, { clientId: 'a', season: 'summer', status: 'paid' }, { clientId: 'a', season: 'winter', status: 'stored' }];
  assert.deepEqual(filterMailingClients(clients, requests, { season: 'winter', status: 'stored' }).map((item) => item.id), ['a']);
  assert.equal(filterMailingClients(clients, requests, { season: 'winter', status: 'paid' }).length, 0);
  assert.deepEqual(filterMailingClients(clients, requests, { search: 'PARTNER' }).map((item) => item.id), ['b']);
});

test('mailing callables require the Tracker admin allowlist', async () => {
  const source = readFileSync(new URL('../index.js', import.meta.url), 'utf8');
  const start = source.indexOf('function clientEmailBatchCallable(');
  class HttpsError extends Error { constructor(code, message) { super(message); this.code = code; } }
  const context = vm.createContext({ functions: { https: { HttpsError, onCall: (handler) => handler } }, TRACKER_ADMIN_EMAILS: new Set(['admin@example.invalid']) });
  vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), context);
  const callable = context.clientEmailBatchCallable(async (_data, uid) => uid);
  await assert.rejects(callable({}, {}), /administrator/);
  await assert.rejects(callable({}, { auth: { token: { email: 'customer@example.invalid' } } }), /administrator/);
  assert.equal(await callable({}, { auth: { uid: 'admin', token: { email: 'admin@example.invalid' } } }), 'admin');
});

test('mailing UI requires review before send, retains selections and resumes without resending successes', async () => {
  const originalDocument = globalThis.document;
  const originalStorage = globalThis.localStorage;
  const elements = new Map();
  class Element {
    constructor() {
      this.value = ''; this.children = []; this.listeners = {}; this.disabled = false;
      const classes = new Set();
      this.classList = { add: (value) => classes.add(value), remove: (value) => classes.delete(value), toggle: (value, force) => force ? classes.add(value) : classes.delete(value), contains: (value) => classes.has(value) };
    }
    appendChild(value) { this.children.push(value); if (value.value && !this.value) this.value = value.value; }
    append(...values) { values.forEach((value) => this.appendChild(value)); }
    replaceChildren() { this.children = []; this.value = ''; }
    addEventListener(name, handler) { this.listeners[name] = handler; }
    focus() {}
    reset() {}
  }
  globalThis.document = {
    getElementById: (id) => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); },
    createElement: () => new Element(), createTextNode: (text) => ({ textContent: text })
  };
  const saved = new Map();
  globalThis.localStorage = { getItem: (key) => saved.get(key), setItem: (key, value) => saved.set(key, value) };
  const field = (id) => document.getElementById(`client-mailing-${id}`);
  try {
    const fixtureData = fixture({ development: true });
    const { service, records, messages } = fixtureData;
    initializeClientMailing({
      getClients: () => ['alice', 'bob'].map((id) => ({ id, ...records.get(`clients/${id}`) })), getRequests: () => [], getSeasons: () => [], getStatuses: () => [], getStorageKey: () => 'mailing-test',
      prepare: (data) => service.prepare(data, 'admin'), load: (data) => service.get(data.batchId), send: (data) => service.send(data, 'admin'), verifyRouting: async (message) => message
    });
    await document.getElementById('email-clients').listeners.click();
    await field('select').listeners.click();
    assert.equal(field('count').textContent, '2 selected');
    field('search').value = 'Alice'; await field('search').listeners.input();
    assert.equal(field('clients').children.length, 1);
    assert.equal(field('count').textContent, '2 selected');
    field('subject').value = 'News'; field('body').value = 'Hello';
    await field('form').listeners.submit({ preventDefault() {} });
    assert.equal(messages.length, 0);
    assert.equal(field('send').textContent, 'Send to 2 clients');
    assert.equal(field('editor').disabled, true);
    assert.ok(field('warning').textContent.includes('entrepot@as-colle.com'));
    await field('send').listeners.click();
    assert.equal(messages.length, 2);
    assert.ok(field('send').classList.contains('hidden'));
    await field('close').listeners.click();
    await document.getElementById('email-clients').listeners.click();
    await field('resume').listeners.click();
    assert.ok(field('progress').textContent.includes('2 sent'));
    await field('send').listeners.click();
    assert.equal(messages.length, 2);
  } finally {
    globalThis.document = originalDocument;
    globalThis.localStorage = originalStorage;
  }
});
