import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import Mustache from 'mustache';
import { createStorageInvoiceReminderService } from './storage-invoice-reminders.js';
import { DEFAULT_STORAGE_INVOICE_TEMPLATE, DEFAULT_STORAGE_PAID_TEMPLATE, DEFAULT_STORAGE_INVOICE_REMINDER_TEMPLATE } from './storage-invoice-template.js';
import { renderInvoicePdf, renderInvoiceHtml, invoiceTextLines, money } from './storage-invoices.js';
import { createEmailSafetyPolicy, createSafeMailer } from './email-safety.js';
import { mergeClientEmailCc } from './client-email-cc.js';

const index = readFileSync(new URL('../index.js', import.meta.url), 'utf8');
const emailContext = vm.createContext({ Mustache, DEFAULT_STORAGE_INVOICE_TEMPLATE, DEFAULT_STORAGE_PAID_TEMPLATE, DEFAULT_STORAGE_INVOICE_REMINDER_TEMPLATE,
  renderInvoiceHtml, invoiceTextLines, formatInvoiceMoney: money, loadEmailTemplate: async () => null, functions: { https: { HttpsError: Error } } });
for (const name of ['escapeHtml', 'getLocalizedTemplateField', 'normalizeTemplateSections', 'fillLegacyTemplateTokens', 'renderMustacheTemplate', 'fillTemplateText', 'fillTemplateHtml', 'wrapTrackerEmailHtml', 'renderStorageInvoiceEmail']) {
  const start = index.includes(`async function ${name}(`) ? index.indexOf(`async function ${name}(`) : index.indexOf(`function ${name}(`);
  vm.runInContext(index.slice(start, index.indexOf('\n}\n', start) + 3), emailContext);
}
const id = 'INV-123456789012345678';
async function fixture({ locale = 'en', development = false, sender, from = 'billing@example.invalid' } = {}) {
  const invoice = { invoiceId: id, date: '2026-10-26', locale, clientId: 'client', client: { name: 'Camille', email: 'original@example.invalid' }, requestIds: ['rv', 'moto'], seasonId: 'winter', seasonLabel: 'Winter 2027',
    lineItems: [{ description: 'RV', amountCents: 46000 }, { description: 'Moto', amountCents: 18000 }, { description: 'Cancellation', amountCents: -13000, cancellationAdjustment: true }],
    subtotalCents: 51000, depositCents: 15000, balanceDueCents: 36000, creditAllocations: [], referralDiscountCents: 0 };
  const pdfBase64 = (await renderInvoicePdf(invoice)).toString('base64');
  const records = new Map([
    [`storageInvoices/${id}`, { invoice, status: 'sent', preview: { pdfBase64 } }],
    ['clients/client', { name: 'Camille', email: 'current@example.invalid', emailCc: 'partner@example.invalid' }],
    ['storageRequests/rv', { clientId: 'client', status: 'stored', invoiceId: id }],
    ['storageRequests/moto', { clientId: 'client', status: 'cancelled', invoiceId: id }],
    ['expenses/deposit', { amount: 150, receiptId: 'deposit' }]
  ]);
  const snap = (path) => ({ exists: records.has(path), id: path.split('/').at(-1), data: () => structuredClone(records.get(path)) });
  const ref = (path) => ({ path, get: async () => snap(path), collection: (name) => ({ doc: (key) => ref(`${path}/${name}/${key}`) }) });
  let queue = Promise.resolve(); let failFinal = false;
  const db = { collection: (name) => ({ doc: (key) => ref(`${name}/${key}`) }), runTransaction(handler) {
    const work = queue.then(async () => {
      const writes = [];
      const result = await handler({ get: async (ref) => snap(ref.path), set: (...args) => writes.push(args) });
      if (failFinal && writes.some(([ref, data]) => ref.path.includes('/reminders/') && data.sentAt)) throw new Error('Database failure after SMTP');
      writes.forEach(([ref, data, options]) => {
        const value = options?.merge ? { ...records.get(ref.path), ...structuredClone(data) } : structuredClone(data);
        for (const key of Object.keys(value)) if (value[key] === '__delete__') delete value[key];
        records.set(ref.path, value);
      });
      return result;
    }); queue = work.catch(() => {}); return work;
  } };
  const messages = [];
  const policy = createEmailSafetyPolicy(development ? { FUNCTIONS_EMULATOR: 'true' } : {});
  const mailer = createSafeMailer({ sendMail: async (message) => { messages.push(message); return sender ? sender(message) : { accepted: [message.to] }; } }, policy);
  const service = createStorageInvoiceReminderService({ db, fieldValue: { serverTimestamp: () => 'timestamp', delete: () => '__delete__' }, from,
    renderEmail: (invoice) => emailContext.renderStorageInvoiceEmail(invoice, false, true), routeEmail: policy.preview,
    ccForLocale: (locale, client) => mergeClientEmailCc(locale === 'fr' ? 'entrepot@as-colle.com' : 'warehouse@as-colle.com', client), sendEmail: (message) => mailer.sendMail(message) });
  return { service, records, messages, pdfBase64, invoice, db, failFinal: () => { failFinal = true; } };
}

test('friendly reminder previews use normal EN/FR branding and the exact original PDF without changing data', async () => {
  for (const locale of ['en', 'fr']) {
    const { service, records, messages, pdfBase64 } = await fixture({ locale });
    const before = structuredClone([...records]);
    const preview = await service.prepare(id);
    assert.deepEqual([...records], before);
    assert.equal(messages.length, 0);
    assert.equal(preview.pdfBase64, pdfBase64);
    assert.equal(preview.to, 'current@example.invalid');
    assert.ok(preview.cc.includes('partner@example.invalid'));
    assert.equal(preview.templateId, 'storage-invoice-reminder');
    assert.match(preview.html, /Entrepot Ferme Colle/);
    assert.ok(preview.html.includes('background:#0f172a;color:#94a3b8;padding:16px 24px;'));
    assert.ok(preview.text.includes(locale === 'fr' ? 'Si vous avez déjà payé' : 'If you have already paid'));
    assert.ok(preview.text.includes(money(36000, locale)));
    assert.ok(!preview.html.includes('{{'));
  }
});

test('sending attaches the saved invoice, is idempotent, and never changes payments, vehicles or ledger', async () => {
  const { service, records, messages, pdfBase64 } = await fixture();
  const original = structuredClone(records.get(`storageInvoices/${id}`).invoice);
  const requests = structuredClone([records.get('storageRequests/rv'), records.get('storageRequests/moto')]);
  const preview = await service.prepare(id);
  await service.send(preview, 'admin');
  await service.send(preview, 'admin');
  assert.equal(messages.length, 1);
  assert.equal(messages[0].attachments[0].content.toString('base64'), pdfBase64);
  assert.equal(messages[0].attachments[0].filename, `${id}.pdf`);
  assert.deepEqual(records.get(`storageInvoices/${id}`).invoice, original);
  assert.deepEqual([records.get('storageRequests/rv'), records.get('storageRequests/moto')], requests);
  assert.equal(records.get(`storageInvoices/${id}`).status, 'sent');
  assert.equal(records.get(`storageInvoices/${id}`).payment, undefined);
  assert.equal(records.get(`storageInvoices/${id}`).reminderPendingId, undefined);
  assert.equal([...records.keys()].filter((key) => key.startsWith('expenses/')).length, 1);
  assert.equal(records.get(`storageInvoices/${id}/reminders/${preview.reminderId}`).status, 'sent');
});

test('paid, superseded, zero-balance, ledger-paid and pending cases cannot receive reminders', async () => {
  for (const mutate of [
    (r) => { r.get(`storageInvoices/${id}`).payment = { status: 'sent' }; },
    (r) => { r.get(`storageInvoices/${id}`).payment = { status: 'failed', confirmedAt: 'already received' }; },
    (r) => { r.get(`storageInvoices/${id}`).status = 'superseded'; },
    (r) => { r.get(`storageInvoices/${id}`).invoice.balanceDueCents = 0; },
    (r) => { r.set(`expenses/storage-payment-${id}`, { amount: 360 }); },
    (r) => { r.get('storageRequests/rv').status = 'paid'; },
    (r) => { r.get('storageRequests/rv').paymentPendingId = id; },
    (r) => { r.get(`storageInvoices/${id}`).reminderPendingId = 'pending'; },
    (r) => { delete r.get(`storageInvoices/${id}`).preview.pdfBase64; }
  ]) {
    const { service, records, messages } = await fixture(); mutate(records);
    await assert.rejects(service.prepare(id)); assert.equal(messages.length, 0);
  }
});

test('payment or recipient changes after preview are revalidated before SMTP', async () => {
  for (const change of ['payment', 'cc', 'race']) {
    const { service, records, messages, db } = await fixture();
    const preview = await service.prepare(id);
    if (change === 'payment') records.get(`storageInvoices/${id}`).payment = { status: 'sent' };
    if (change === 'cc') records.get('clients/client').emailCc = 'new@example.invalid';
    if (change === 'race') {
      const transaction = db.runTransaction.bind(db);
      db.runTransaction = (handler) => { records.get(`storageInvoices/${id}`).payment = { status: 'sending' }; return transaction(handler); };
    }
    await assert.rejects(service.send(preview, 'admin'));
    assert.equal(messages.length, 0);
  }
});

test('dev reminder delivery suppresses customer To/CC and uses only the safe SMTP envelope', async () => {
  const { service, messages } = await fixture({ development: true });
  const preview = await service.prepare(id);
  assert.equal(preview.to, 'entrepot@as-colle.com'); assert.equal(preview.cc, '');
  await service.send(preview, 'admin');
  assert.equal(messages[0].to, 'entrepot@as-colle.com');
  assert.equal(messages[0].cc, undefined);
  assert.deepEqual(messages[0].envelope.to, ['entrepot@as-colle.com']);
});

test('rejected reminders may retry; uncertain delivery and post-SMTP database failures never resend', async () => {
  for (const scenario of ['rejected', 'uncertain', 'database']) {
    let attempts = 0;
    const { service, messages, failFinal } = await fixture({ sender: (message) => {
      attempts++;
      if (scenario === 'rejected' && attempts === 1) throw Object.assign(new Error('Rejected'), { responseCode: 550 });
      if (scenario === 'uncertain') throw new Error('Connection lost');
      return { accepted: [message.to] };
    } });
    const preview = await service.prepare(id);
    if (scenario === 'database') failFinal();
    await assert.rejects(service.send(preview, 'admin'));
    if (scenario === 'rejected') await service.send(preview, 'admin');
    else await assert.rejects(service.send(preview, 'admin'));
    assert.equal(messages.length, scenario === 'rejected' ? 2 : 1);
  }
});

test('concurrent sends deliver one reminder; skipping email records only a skipped audit', async () => {
  const { service, messages } = await fixture();
  const preview = await service.prepare(id);
  await Promise.allSettled([service.send(preview, 'admin'), service.send(preview, 'admin')]);
  assert.equal(messages.length, 1);
  const skipped = await fixture({ from: '' });
  const skipPreview = await skipped.service.prepare(id);
  const before = structuredClone(skipped.records.get(`storageInvoices/${id}`));
  await skipped.service.send({ ...skipPreview, skipEmail: true }, 'admin');
  assert.equal(skipped.messages.length, 0);
  assert.deepEqual(skipped.records.get(`storageInvoices/${id}`), before);
  assert.equal(skipped.records.get(`storageInvoices/${id}/reminders/${skipPreview.reminderId}`).status, 'skipped');
});

test('saved reminder templates override defaults and stay editable through the template management UI', async () => {
  emailContext.loadEmailTemplate = async () => ({ subject: { en: 'Hello {{tenant}}' }, body: { en: 'Custom reminder {{invoice_id}}\n{{invoice_details}}' } });
  try {
    const { service } = await fixture();
    assert.equal((await service.prepare(id)).subject, 'Hello Camille');
  } finally { emailContext.loadEmailTemplate = async () => null; }
  const app = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
  assert.ok(app.includes("value: 'invoice-reminder', label: 'Payment reminder'"));
  assert.ok(app.includes("templateId === 'storage-invoice-reminder'"));
  assert.ok(app.includes('reminderId: pendingStorageConfirmationEmail.reminderId'));
});
