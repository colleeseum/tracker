import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { mergeClientEmailCc } from './client-email-cc.js';
import { createEmailSafetyPolicy } from './email-safety.js';

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
const backend = readFileSync(new URL('../index.js', import.meta.url), 'utf8');
const browser = vm.createContext({});
const start = source.indexOf('function mergeClientEmailCc(');
vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), browser);

test('optional client CC preserves the office copy, defaults safely, and deduplicates recipients', () => {
  for (const merge of [mergeClientEmailCc, browser.mergeClientEmailCc]) {
    assert.equal(merge('office@example.invalid'), 'office@example.invalid');
    assert.equal(merge('office@example.invalid', { emailCc: '' }), 'office@example.invalid');
    assert.equal(merge('office@example.invalid', { email: 'client@example.invalid', emailCc: ' partner@example.invalid ' }), 'office@example.invalid, partner@example.invalid');
    assert.equal(merge('office@example.invalid', { emailCc: 'OFFICE@example.invalid' }), 'office@example.invalid');
    assert.equal(merge('office@example.invalid', { email: 'client@example.invalid', emailCc: 'CLIENT@example.invalid' }), 'office@example.invalid');
    for (const emailCc of ['bad', 'a@b, c@d', 'a@b; c@d', 'a@b\r\nBcc:other@c', ['a@b']]) {
      assert.throws(() => merge('office@example.invalid', { emailCc }), /one valid email/);
    }
  }
});

test('dev suppresses the client CC and office CC, production retains both', () => {
  const message = { to: 'client@example.invalid', cc: mergeClientEmailCc('office@example.invalid', { emailCc: 'partner@example.invalid' }), subject: 'Test' };
  const dev = createEmailSafetyPolicy({ FUNCTIONS_EMULATOR: 'true' }).preview(message);
  assert.equal(dev.to, 'entrepot@as-colle.com');
  assert.equal(dev.cc, '');
  assert.equal(createEmailSafetyPolicy({}).preview(message).cc, message.cc);
});

test('all client and storage email previews and sends include the saved client CC', () => {
  assert.equal((backend.match(/const cc = getStorageConfirmationCc\(locale, client\)/g) || []).length, 4);
  assert.equal((source.match(/cc: getStorageConfirmationCc\(lang, client\)/g) || []).length, 3);
  assert.ok(source.includes('getDefaultClientLanguage()), client)'));
});

test('client CC loads, saves on create/update, and can be cleared without losing other fields', async () => {
  const fields = Object.fromEntries(['clientNameInput', 'clientLanguageSelect', 'clientPhoneInput', 'clientEmailInput', 'clientEmailCcInput', 'clientAddressInput', 'clientCityInput', 'clientProvinceSelect', 'clientPostalInput', 'clientActiveInput', 'clientNonStorageInput', 'clientNotesInput'].map((key) => [key, { value: '', checked: false, focus() {} }]));
  const writes = [];
  const context = vm.createContext({ ...fields, editingClientId: null, clientFormTitle: {}, clientFormError: {},
    clientModal: { classList: { remove() {} } }, getDefaultClientLanguage: () => 'en',
    clientForm: { reset() { Object.values(fields).forEach((field) => { field.value = ''; }); }, addEventListener(type, handler) { context.submit = handler; } },
    formatClientPhone: (value) => value, serverTimestamp: () => 'timestamp', auth: { currentUser: { uid: 'admin' } }, db: {},
    doc: (_db, collection, id) => `${collection}/${id}`, collection: (_db, name) => name,
    setDoc: async (...args) => writes.push(args), addDoc: async (...args) => writes.push(args), closeClientModal() {}
  });
  const openStart = source.indexOf('function openClientModal(');
  vm.runInContext(source.slice(openStart, source.indexOf('\n}\n', openStart) + 3), context);
  const submitStart = source.indexOf("clientForm.addEventListener('submit'");
  vm.runInContext(source.slice(submitStart, source.indexOf('\n});', submitStart) + 4), context);
  const client = { id: 'test', name: 'Camille', email: 'client@example.invalid', emailCc: 'partner@example.invalid', address: '123 Main', city: 'Ottawa', province: 'ON', postalCode: 'K1A 0B1' };
  context.openClientModal('edit', client);
  assert.equal(fields.clientEmailCcInput.value, client.emailCc);
  await context.submit({ preventDefault() {} });
  assert.equal(writes.at(-1)[0], 'clients/test');
  assert.equal(writes.at(-1)[1].emailCc, client.emailCc);
  assert.equal(writes.at(-1)[1].address, client.address);
  fields.clientEmailCcInput.value = '';
  await context.submit({ preventDefault() {} });
  assert.equal(writes.at(-1)[1].emailCc, '');
  context.editingClientId = null;
  fields.clientEmailCcInput.value = ' new@example.invalid ';
  await context.submit({ preventDefault() {} });
  assert.equal(writes.at(-1)[0], 'clients');
  assert.equal(writes.at(-1)[1].emailCc, 'new@example.invalid');
  context.openClientModal('edit', { ...client, emailCc: undefined });
  assert.equal(fields.clientEmailCcInput.value, '');
  context.openClientModal('new');
  assert.equal(fields.clientEmailCcInput.value, '');
});
