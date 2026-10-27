import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { readSkipEmail, workflowEmailAudit } from './email-options.js';
import { storageIncomeFields } from './storage-income-category.js';

const source = readFileSync(new URL('../index.js', import.meta.url), 'utf8');
function fixture(status = 'new', sender = undefined) {
  const records = new Map([
    ['storageRequests/a', { clientId: 'client', season: 'winter', status, signedContractPath: 'contracts/signed.pdf', contractAmount: 600 }],
    ['clients/client', { name: 'Client', email: 'client@example.invalid' }]
  ]);
  const emails = [];
  const snap = (path) => ({ id: path.split('/').at(-1), exists: records.has(path), data: () => records.get(path) });
  const db = {
    collection: (name) => ({ doc: (id) => ({ path: `${name}/${id}`, get: async () => snap(`${name}/${id}`) }) }),
    batch: () => {
      const writes = [];
      return { set: (ref, value, options) => writes.push([ref.path, value, options]), commit: async () => writes.forEach(([path, value, options]) => records.set(path, options?.merge ? { ...records.get(path), ...value } : value)) };
    }
  };
  class HttpsError extends Error { constructor(code, message) { super(message); this.code = code; } }
  const context = vm.createContext({
    db, readSkipEmail, workflowEmailAudit, defaultFrom: sender,
    functions: { https: { onCall: (handler) => handler, HttpsError } },
    admin: { firestore: { FieldValue: { serverTimestamp: () => 'timestamp' }, Timestamp: { now: () => 'timestamp' } }, storage: () => ({ bucket: () => ({ file: () => ({ download: async () => [Buffer.from('signed contract')] }) }) }) },
    transporter: { sendMail: async (message) => emails.push(message) },
    emailSafetyPolicy: { preview: (message) => ({ ...message, developmentEmailRedirect: false }) },
    loadEmailTemplate: async () => ({}), loadEmailTemplateByIds: async () => ({ template: {}, templateId: 'receipt' }),
    normalizeLanguage: () => 'en', loadStoragePricingData: async () => ({}),
    buildTrackerConfirmationContext: () => ({ season: 'Winter 2027', leaseCost: '$600', deposit: '$100' }),
    buildContractDetailsHtml: () => 'Vehicle', getLocalizedTemplateField: () => 'Message',
    fillTemplateText: () => 'Message', fillTemplateHtml: () => '<p>Message</p>', wrapTrackerEmailHtml: () => '<html>Message</html>',
    getStorageConfirmationCc: () => '', resolveRequestAmountForEmail: () => 600, estimateDepositForAmount: () => 100,
    storageIncomeFields, loadStorageIncomeCategory: async () => ({ id: 'storage-income', label: 'Entrepot', code: 9600, type: 'income' }),
    allocateStorageReceiptId: async () => 'receipt-1', resolveFermeColleLedgerAccounts: async () => ({ cashAccount: { id: 'cash' }, entityAccount: { id: 'entity' } }),
    resolveEmailCaseId: () => 'case', buildDepositVehicleDescription: () => 'Vehicle', formatMoney: (value) => `$${value}`, console
  });
  const workflowStart = source.indexOf('const STORAGE_RECEIPT_EMAIL_WORKFLOWS =');
  vm.runInContext(source.slice(workflowStart, source.indexOf('\n};', workflowStart) + 3), context);
  for (const name of ['sendStorageConfirmationEmail', 'sendStorageContractEmail', 'sendStorageReceiptEmail']) {
    const start = source.indexOf(`export const ${name} =`);
    const handler = source.slice(start, source.indexOf('\n});', start) + 4).replace(`export const ${name}`, `globalThis.${name}`);
    vm.runInContext(handler, context);
  }
  const auth = { auth: { uid: 'admin', token: { email: 'admin@example.invalid' } } };
  return { context, records, emails, auth };
}

test('skip-email option is explicit and rejects string booleans', () => {
  assert.equal(readSkipEmail({}), false);
  assert.equal(readSkipEmail({ skipEmail: false }), false);
  assert.equal(readSkipEmail({ skipEmail: true }), true);
  assert.throws(() => readSkipEmail({ skipEmail: 'true' }), /boolean/);
});

test('confirmation and contract steps skip provider calls and do not claim an email was sent', async () => {
  for (const [name, status, prefix] of [['sendStorageConfirmationEmail', 'new', 'confirmation'], ['sendStorageContractEmail', 'contract_ready', 'contract']]) {
    const { context, records, emails, auth } = fixture(status);
    await context[name]({ requestIds: ['a'], skipEmail: true }, auth);
    const request = records.get('storageRequests/a');
    assert.equal(emails.length, 0);
    assert.equal(request[`${prefix}EmailDeliveryStatus`], 'skipped');
    assert.equal(request[`${prefix}EmailSkippedBy`], 'admin');
    assert.ok(!request[`${prefix}EmailSentAt`]);
    if (prefix === 'contract') assert.equal(request.status, 'waiting_contract_deposit');
  }
});

test('missing storage income category blocks deposit creation before email or workflow changes', async () => {
  const { context, records, emails, auth } = fixture('waiting_deposit', 'billing@example.invalid');
  context.loadStorageIncomeCategory = async () => { throw new Error('Select a storage income category'); };
  await assert.rejects(context.sendStorageReceiptEmail({ requestIds: ['a'], receiptType: 'deposit', interacReference: 'reference' }, auth), /storage income category/);
  assert.equal(emails.length, 0);
  assert.equal(records.get('storageRequests/a').status, 'waiting_deposit');
  assert.ok(!records.has('expenses/storage-deposit-receipt-1'));
});

test('all receipt steps advance normally without email, retaining deposit ledger entries and references', async () => {
  for (const [receiptType, status, target] of [['contract', 'waiting_contract_deposit', 'waiting_deposit'], ['deposit', 'waiting_deposit', 'reserved'], ['contract_deposit', 'waiting_contract_deposit', 'reserved'], ['missing', 'waiting_deposit', 'waiting_deposit']]) {
    const { context, records, emails, auth } = fixture(status);
    await context.sendStorageReceiptEmail({ requestIds: ['a'], receiptType, interacReference: 'reference', skipEmail: true }, auth);
    assert.equal(emails.length, 0);
    assert.equal(records.get('storageRequests/a').status, target);
    assert.ok(!records.get('storageRequests/a').receiptEmailSentAt);
    if (receiptType === 'deposit' || receiptType === 'contract_deposit') {
      const receipt = records.get('storageReceipts/receipt-1');
      assert.equal(receipt.emailDeliveryStatus, 'skipped');
      assert.equal(receipt.sentAt, null);
      assert.equal(receipt.interacReference, 'reference');
      assert.equal(records.get('expenses/storage-deposit-receipt-1').amount, 100);
      assert.equal(records.get('expenses/storage-deposit-receipt-1').categoryId, 'storage-income');
      assert.equal(records.get('expenses/storage-deposit-receipt-1').categoryCode, 9600);
      assert.equal(records.get('expenses/storage-deposit-receipt-1').storagePaymentStage, 'deposit');
    }
    if (receiptType === 'missing') assert.equal(records.get('storageRequests/a').followUpEmailDeliveryStatus, 'skipped');
  }
});

test('skipping cannot bypass signed contracts or deposit reference requirements', async () => {
  const { context, records, emails, auth } = fixture('waiting_contract_deposit');
  records.get('storageRequests/a').signedContractPath = '';
  await assert.rejects(context.sendStorageReceiptEmail({ requestIds: ['a'], receiptType: 'contract', skipEmail: true }, auth), /signed contract/);
  await assert.rejects(context.sendStorageReceiptEmail({ requestIds: ['a'], receiptType: 'deposit', skipEmail: true }, auth), /Interac reference/);
  assert.equal(emails.length, 0);
  assert.equal(records.size, 2);
});

test('unchecked steps continue sending normally', async () => {
  const { context, records, emails, auth } = fixture('contract_ready', 'sender@example.invalid');
  await context.sendStorageContractEmail({ requestIds: ['a'], skipEmail: false }, auth);
  assert.equal(emails.length, 1);
  assert.equal(records.get('storageRequests/a').contractEmailDeliveryStatus, 'accepted');
  assert.equal(records.get('storageRequests/a').contractEmailSentAt, 'timestamp');
});

test('storage checkbox resets for each action and button labels reflect skip and recovery', () => {
  const app = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
  const checkbox = { checked: true, disabled: false };
  const button = { disabled: false, textContent: '' };
  const hint = { textContent: '' };
  const context = vm.createContext({
    document: { getElementById: () => hint },
    storageSkipEmailCheckbox: checkbox, sendStorageConfirmationEmailButton: button,
    cancelStorageConfirmationEmailButton: null, storageConfirmationEmailBusy: false,
    pendingStorageConfirmationEmail: { emailType: 'invoice' },
    arePendingReceiptContractsUploaded: () => true, hasRequiredReceiptInteracReference: () => true,
    updateReceiptContractUploadButton: () => {}
  });
  for (const name of ['resetStorageEmailOption', 'updateStorageConfirmationEmailButtons']) {
    const start = app.indexOf(`function ${name}(`);
    vm.runInContext(app.slice(start, app.indexOf('\n}\n', start) + 3), context);
  }
  context.resetStorageEmailOption();
  assert.equal(checkbox.checked, false);
  checkbox.checked = true;
  context.updateStorageConfirmationEmailButtons();
  assert.match(button.textContent, /no email/);
  context.pendingStorageConfirmationEmail = { emailType: 'payment' };
  context.updateStorageConfirmationEmailButtons();
  assert.equal(button.textContent, 'Confirm full payment without email');
  context.hasRequiredReceiptInteracReference = () => false;
  context.updateStorageConfirmationEmailButtons();
  assert.equal(button.disabled, true);
  context.pendingStorageConfirmationEmail = { emailType: 'payment', emailSkipped: true, completionPending: true };
  context.resetStorageEmailOption(context.pendingStorageConfirmationEmail);
  context.updateStorageConfirmationEmailButtons();
  assert.equal(checkbox.checked, true);
  assert.equal(checkbox.disabled, true);
  assert.match(button.textContent, /email skipped/);
  context.pendingStorageConfirmationEmail = { emailType: 'reminder', reminder: true };
  context.resetStorageEmailOption(context.pendingStorageConfirmationEmail);
  context.updateStorageConfirmationEmailButtons();
  assert.equal(button.textContent, 'Send payment reminder');
  assert.match(hint.textContent, /No invoice, payment, vehicle status or ledger changes/);
  checkbox.checked = true;
  context.updateStorageConfirmationEmailButtons();
  assert.equal(button.textContent, 'Record reminder without email');
});
