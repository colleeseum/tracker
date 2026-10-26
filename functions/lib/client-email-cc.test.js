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

test('optional client contacts load, save on create/update, and can be cleared without losing other fields', async () => {
  const fields = Object.fromEntries(['clientNameInput', 'clientLanguageSelect', 'clientPhoneInput', 'clientPhoneSecondaryInput', 'clientPhoneWhatsAppInput', 'clientPhoneSecondaryWhatsAppInput', 'clientEmailInput', 'clientEmailCcInput', 'clientAddressInput', 'clientCityInput', 'clientProvinceSelect', 'clientPostalInput', 'clientActiveInput', 'clientNonStorageInput', 'clientNotesInput'].map((key) => [key, { value: '', checked: false, focus() {} }]));
  const writes = [];
  const context = vm.createContext({ ...fields, editingClientId: null, clientFormTitle: {}, clientFormError: {},
    clientModal: { classList: { remove() {} } }, getDefaultClientLanguage: () => 'en',
    clientForm: { reset() { Object.values(fields).forEach((field) => { field.value = ''; field.checked = false; }); }, addEventListener(type, handler) { context.submit = handler; } },
    serverTimestamp: () => 'timestamp', auth: { currentUser: { uid: 'admin' } }, db: {},
    doc: (_db, collection, id) => `${collection}/${id}`, collection: (_db, name) => name,
    setDoc: async (...args) => writes.push(args), addDoc: async (...args) => writes.push(args), closeClientModal() {}
  });
  const formatStart = source.indexOf('function formatClientPhone(');
  vm.runInContext(source.slice(formatStart, source.indexOf('\n}\n', formatStart) + 3), context);
  const openStart = source.indexOf('function openClientModal(');
  vm.runInContext(source.slice(openStart, source.indexOf('\n}\n', openStart) + 3), context);
  const submitStart = source.indexOf("clientForm.addEventListener('submit'");
  vm.runInContext(source.slice(submitStart, source.indexOf('\n});', submitStart) + 4), context);
  const client = { id: 'test', name: 'Camille', phone: '514-555-1234', phoneSecondary: '613-555-4567', phoneWhatsApp: true, phoneSecondaryWhatsApp: false, email: 'client@example.invalid', emailCc: 'partner@example.invalid', address: '123 Main', city: 'Ottawa', province: 'ON', postalCode: 'K1A 0B1' };
  context.openClientModal('edit', client);
  assert.equal(fields.clientEmailCcInput.value, client.emailCc);
  assert.equal(fields.clientPhoneSecondaryInput.value, client.phoneSecondary);
  assert.equal(fields.clientPhoneWhatsAppInput.checked, true);
  assert.equal(fields.clientPhoneSecondaryWhatsAppInput.checked, false);
  await context.submit({ preventDefault() {} });
  assert.equal(writes.at(-1)[0], 'clients/test');
  assert.equal(writes.at(-1)[1].emailCc, client.emailCc);
  assert.equal(writes.at(-1)[1].address, client.address);
  assert.equal(writes.at(-1)[1].phone, client.phone);
  assert.equal(writes.at(-1)[1].phoneSecondary, client.phoneSecondary);
  assert.equal(writes.at(-1)[1].phoneWhatsApp, true);
  assert.equal(writes.at(-1)[1].phoneSecondaryWhatsApp, false);
  fields.clientEmailCcInput.value = '';
  fields.clientPhoneSecondaryInput.value = '';
  fields.clientPhoneSecondaryWhatsAppInput.checked = true;
  await context.submit({ preventDefault() {} });
  assert.equal(writes.at(-1)[1].emailCc, '');
  assert.equal(writes.at(-1)[1].phoneSecondary, '');
  assert.equal(writes.at(-1)[1].phoneSecondaryWhatsApp, false);
  assert.equal(writes.at(-1)[1].phone, client.phone);
  context.editingClientId = null;
  fields.clientEmailCcInput.value = ' new@example.invalid ';
  fields.clientPhoneSecondaryInput.value = ' (613) 555-7890 ';
  await context.submit({ preventDefault() {} });
  assert.equal(writes.at(-1)[0], 'clients');
  assert.equal(writes.at(-1)[1].emailCc, 'new@example.invalid');
  assert.equal(writes.at(-1)[1].phoneSecondary, '613-555-7890');
  assert.equal(writes.at(-1)[1].phoneSecondaryWhatsApp, true);
  context.openClientModal('edit', { ...client, emailCc: undefined, phoneSecondary: undefined, phoneWhatsApp: undefined, phoneSecondaryWhatsApp: undefined });
  assert.equal(fields.clientEmailCcInput.value, '');
  assert.equal(fields.clientPhoneSecondaryInput.value, '');
  assert.equal(fields.clientPhoneWhatsAppInput.checked, false);
  assert.equal(fields.clientPhoneSecondaryWhatsAppInput.checked, false);
  fields.clientPhoneWhatsAppInput.checked = true;
  fields.clientPhoneSecondaryWhatsAppInput.checked = true;
  context.openClientModal('new');
  assert.equal(fields.clientEmailCcInput.value, '');
  assert.equal(fields.clientPhoneSecondaryInput.value, '');
  assert.equal(fields.clientPhoneWhatsAppInput.checked, false);
  assert.equal(fields.clientPhoneSecondaryWhatsAppInput.checked, false);
});

test('second phone is optional and both numbers appear in the existing phone column', () => {
  const html = readFileSync(new URL('../../public/index.html', import.meta.url), 'utf8');
  const input = html.match(/<input\s+type="tel"\s+id="client-phone-secondary"[^>]*>/)?.[0];
  assert.ok(input);
  assert.ok(!input.includes('required'));
  assert.ok(source.includes('phone: client.phone, whatsApp: client.phoneWhatsApp === true'));
  assert.ok(source.includes('phone: client.phoneSecondary, whatsApp: client.phoneSecondaryWhatsApp === true'));
  assert.ok(source.includes("whatsApp ? ' (WhatsApp)' : ''"));
});
