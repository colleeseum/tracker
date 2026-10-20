import test from 'node:test';
import assert from 'node:assert/strict';
import { PDFDocument } from 'pdf-lib';
import { buildInvoiceModel, createStorageInvoiceService, referralCreditId, invoiceIdForRequests, renderInvoicePdf, renderInvoiceHtml, invoiceTextLines, invoiceDate, cents, money } from './storage-invoices.js';
import { DEFAULT_STORAGE_INVOICE_TEMPLATE, DEFAULT_STORAGE_PAID_TEMPLATE } from './storage-invoice-template.js';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import Mustache from 'mustache';
import { createEmailSafetyPolicy, createSafeMailer, DEV_EMAIL_RECIPIENT } from './email-safety.js';

function fixture() {
  const requests = [
    { id: 'vehicle-a', clientId: 'referrer', season: 'winter', status: 'scheduled', contractAmount: 300, vehicle: { type: 'rv' } },
    { id: 'vehicle-b', clientId: 'referrer', season: 'winter', status: 'scheduled', contractAmount: 400, vehicle: { type: 'car' } }
  ];
  return {
    requests, allRequests: [...requests,
      { id: 'referred-1', clientId: 'new-customer', referredByClientIds: ['referrer'], status: 'scheduled' },
      { id: 'referred-2', clientId: 'new-customer', referredByClientIds: ['referrer'], status: 'scheduled' }
    ],
    client: { name: 'Referring client', email: 'invoice@example.invalid', preferredLanguage: 'en' },
    clients: new Map([['new-customer', { name: 'New customer' }]]),
    receipts: [{ id: 'paid-deposit', receiptType: 'deposit', amount: 100, requestIds: requests.map((request) => request.id) }],
    credits: new Map(), referralDiscountAmount: 25, amountForRequest: (request) => request.contractAmount, date: '2026-10-11'
  };
}

test('invoice subtracts actual recorded deposits, not estimated deposits', () => {
  const invoice = buildInvoiceModel(fixture());
  assert.equal(invoice.subtotalCents, 70000);
  assert.equal(invoice.depositCents, 10000);
  assert.equal(invoice.referralDiscountCents, 2500);
  assert.equal(invoice.balanceDueCents, 57500);
});
test('referral discount comes from configuration and applies once per referred customer, not vehicle', () => {
  const data = fixture();
  data.referralDiscountAmount = 40;
  const invoice = buildInvoiceModel(data);
  assert.equal(invoice.creditAllocations.length, 1);
  assert.equal(invoice.referralDiscountCents, 4000);
  data.referralDiscountAmount = undefined;
  assert.throws(() => buildInvoiceModel(data), /Storage billing/);
});
test('new referred client does not receive their own referring-client discount', () => {
  const data = fixture();
  data.requests = [{ id: 'new', clientId: 'new-customer', season: 'winter', status: 'scheduled', contractAmount: 300, referredByClientIds: ['referrer'] }];
  data.receipts = [];
  assert.equal(buildInvoiceModel(data).referralDiscountCents, 0);
});
test('consumed credits do not repeat; partial credits carry forward at their original configured amount', () => {
  const data = fixture();
  data.credits.set(referralCreditId('referrer', 'new-customer'), { amountCents: 2500, usedCents: 2500 });
  assert.equal(buildInvoiceModel(data).referralDiscountCents, 0);
  data.credits.get(referralCreditId('referrer', 'new-customer')).usedCents = 1000;
  data.referralDiscountAmount = 40;
  assert.equal(buildInvoiceModel(data).referralDiscountCents, 1500);
  data.credits.clear();
  data.receipts[0].amount = 695;
  const capped = buildInvoiceModel(data);
  assert.equal(capped.balanceDueCents, 0);
  assert.equal(capped.creditAllocations[0].appliedCents, 500);
});
test('cancelled referrals, invalid amounts, partial-deposit cases and wrong statuses cannot produce invoices', () => {
  const data = fixture();
  data.allRequests.filter((request) => request.clientId === 'new-customer').forEach((request) => { request.status = 'cancelled'; });
  assert.equal(buildInvoiceModel(data).referralDiscountCents, 0);
  for (const value of [null, undefined, '', -1, NaN, Infinity, true]) assert.throws(() => cents(value));
  data.receipts[0].requestIds.push('another-vehicle');
  assert.throws(() => buildInvoiceModel(data), /full storage case/);
  data.receipts = [];
  data.requests[0].status = 'new';
  assert.throws(() => buildInvoiceModel(data), /Scheduled/);
});
test('localized invoice HTML escapes user content and PDF is valid in both languages', async () => {
  for (const language of ['en', 'fr']) {
    const data = fixture();
    data.client.preferredLanguage = language;
    data.client.name = '<script>bad()</script> Élodie 🚐';
    const invoice = buildInvoiceModel(data);
    assert.ok(!renderInvoiceHtml(invoice).includes('<script>'));
    assert.ok(invoiceTextLines(invoice).some((line) => line.includes(language === 'fr' ? 'Solde à payer' : 'Balance due')));
    const pdf = await renderInvoicePdf(invoice);
    assert.equal(pdf.subarray(0, 4).toString(), '%PDF');
    assert.ok((await PDFDocument.load(pdf)).getPageCount() >= 1);
  }
  assert.equal(invoiceDate(new Date('2026-10-12T01:00:00Z')), '2026-10-11');
  assert.equal(invoiceIdForRequests(['b', 'a', 'a']), invoiceIdForRequests(['a', 'b']));
});

function memoryDb(seed) {
  const records = new Map(Object.entries(seed).map(([key, value]) => [key, structuredClone(value)]));
  const snapshot = (path) => ({ id: path.split('/').at(-1), exists: records.has(path), data: () => records.has(path) ? structuredClone(records.get(path)) : undefined });
  const write = (path, values, options = {}) => {
    const checkArrays = (value) => {
      if (Array.isArray(value)) {
        assert.ok(!value.some(Array.isArray), 'Firestore does not accept directly nested arrays');
        value.forEach(checkArrays);
      } else if (value && typeof value === 'object') Object.values(value).forEach(checkArrays);
    };
    checkArrays(values);
    const next = options.merge ? { ...records.get(path) } : {};
    for (const [key, value] of Object.entries(values)) {
      if (value === '__delete__') delete next[key];
      else next[key] = structuredClone(value);
    }
    records.set(path, next);
  };
  const ref = (path) => ({ path, get: async () => snapshot(path), set: async (data, options) => write(path, data, options) });
  let queue = Promise.resolve();
  return {
    records,
    collection: (name) => ({ doc: (id) => ref(`${name}/${id}`), get: async () => ({ docs: [...records.keys()].filter((path) => path.startsWith(`${name}/`)).map(snapshot) }) }),
    runTransaction(handler) {
      const work = queue.then(async () => {
        const writes = [];
        const result = await handler({ get: async (reference) => snapshot(reference.path), set: (reference, values, options) => { writes.push([reference.path, values, options]); } });
        writes.forEach((args) => write(...args));
        return result;
      });
      queue = work.catch(() => {});
      return work;
    }
  };
}
function serviceFixture(sender, renderEmail, routeEmail) {
  const data = fixture();
  const seed = {
    'clients/referrer': data.client,
    'clients/new-customer': { name: 'New customer' },
    'admin/appSettings': { referralDiscountAmount: 25 },
    'storageReceipts/paid-deposit': data.receipts[0]
  };
  data.allRequests.forEach((request) => { seed[`storageRequests/${request.id}`] = request; });
  const db = memoryDb(seed);
  const emails = [];
  const service = createStorageInvoiceService({
    db, fieldValue: { serverTimestamp: () => 'timestamp', delete: () => '__delete__' },
    loadPricing: async () => ({}), resolveAmount: (request) => request.contractAmount,
    resolveCaseId: (request) => `${request.clientId}|${request.season}`,
    sendEmail: async (message) => { emails.push(message); return sender ? sender(message) : { accepted: [message.to] }; },
    from: 'billing@example.invalid', ccForLocale: () => 'office@example.invalid', renderEmail, routeEmail
  });
  return { service, db, emails, ids: data.requests.map((request) => request.id) };
}
test('preview/cancel has no writes or emails; confirmed send attaches PDF, consumes credit and marks stored once', async () => {
  const { service, db, emails, ids } = serviceFixture();
  const before = JSON.stringify([...db.records]);
  const preview = await service.prepare(ids);
  assert.equal(JSON.stringify([...db.records]), before);
  assert.equal(emails.length, 0);
  const result = await service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin');
  assert.equal(result.success, true);
  assert.equal(emails.length, 1);
  assert.equal(emails[0].attachments[0].content.subarray(0, 4).toString(), '%PDF');
  assert.deepEqual(emails[0].attachments[0].content, Buffer.from(preview.pdfBase64, 'base64'));
  assert.equal(emails[0].html, preview.html);
  assert.equal(emails[0].subject, preview.subject);
  assert.equal(emails[0].text, preview.text);
  for (const id of ids) assert.equal(db.records.get(`storageRequests/${id}`).status, 'stored');
  assert.equal(db.records.get(`storageReferralCredits/${referralCreditId('referrer', 'new-customer')}`).usedCents, 2500);
  assert.equal(db.records.get(`storageInvoices/${preview.invoiceId}`).status, 'sent');
  await service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin');
  assert.equal(emails.length, 1);
  assert.equal((await service.prepare(ids)).archived, true);
});
test('stale preview and partial-case selection cannot send or mark stored', async () => {
  const { service, db, emails, ids } = serviceFixture();
  await assert.rejects(service.prepare([ids[0]]), /full storage case/);
  const preview = await service.prepare(ids);
  db.records.get(`storageRequests/${ids[0]}`).contractAmount = 350;
  await assert.rejects(service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin'), /changed after preview/);
  assert.equal(emails.length, 0);
  assert.equal(db.records.get(`storageRequests/${ids[0]}`).status, 'scheduled');
});
test('skipping invoice email saves PDF, consumes credits once and marks Stored without claiming delivery', async () => {
  const { service, db, emails, ids } = serviceFixture(() => { throw new Error('SMTP must not be called'); });
  const preview = await service.prepare(ids);
  await service.send({ requestIds: ids, fingerprint: preview.fingerprint, skipEmail: true }, 'admin');
  assert.equal(emails.length, 0);
  const saved = db.records.get(`storageInvoices/${preview.invoiceId}`);
  assert.equal(saved.emailDeliveryStatus, 'skipped');
  assert.equal(saved.emailSkippedBy, 'admin');
  assert.ok(saved.emailSkippedAt);
  assert.ok(!saved.emailAcceptedAt && !saved.sentAt);
  assert.equal(saved.preview.pdfBase64, preview.pdfBase64);
  for (const id of ids) {
    const request = db.records.get(`storageRequests/${id}`);
    assert.equal(request.status, 'stored');
    assert.equal(request.invoiceEmailDeliveryStatus, 'skipped');
    assert.ok(!request.invoiceSentAt);
  }
  assert.equal(db.records.get(`storageReferralCredits/${referralCreditId('referrer', 'new-customer')}`).usedCents, 2500);
  await service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin');
  assert.equal(emails.length, 0);
});

test('uncertain invoice delivery cannot be bypassed by selecting skip email', async () => {
  const { service, emails, ids } = serviceFixture(() => { throw new Error('Timeout'); });
  const preview = await service.prepare(ids);
  await assert.rejects(service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin'), /uncertain/);
  await assert.rejects(service.send({ requestIds: ids, fingerprint: preview.fingerprint, skipEmail: true }, 'admin'), /reconciliation/);
  assert.equal(emails.length, 1);
});
test('no-deposit Scheduled case can send its invoice and become Stored without inventing a deposit', async () => {
  const { service, db, emails, ids } = serviceFixture();
  db.records.delete('storageReceipts/paid-deposit');
  ids.forEach((id) => { db.records.get(`storageRequests/${id}`).depositRequired = false; });
  const preview = await service.prepare(ids);
  assert.equal(preview.invoice.depositCents, 0);
  assert.equal(preview.invoice.balanceDueCents, 67500);
  await service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin');
  assert.equal(emails.length, 1);
  assert.equal(db.records.get(`storageRequests/${ids[0]}`).status, 'stored');
  assert.equal(db.records.get(`storageRequests/${ids[0]}`).depositRequired, false);
  assert.ok(![...db.records.keys()].some((key) => key.startsWith('storageReceipts/')));
});
test('concurrent confirmations cannot send duplicate invoices', async () => {
  const { service, emails, ids } = serviceFixture();
  const preview = await service.prepare(ids);
  await Promise.allSettled([1, 2].map(() => service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin')));
  assert.equal(emails.length, 1);
});
test('definitive SMTP rejection releases credit reservation and permits a reviewed retry', async () => {
  let attempts = 0;
  const { service, db, emails, ids } = serviceFixture(() => {
    if (++attempts === 1) throw Object.assign(new Error('Rejected'), { responseCode: 550 });
    return { accepted: ['invoice@example.invalid'] };
  });
  const preview = await service.prepare(ids);
  await assert.rejects(service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin'), /rejected/);
  assert.equal(db.records.get(`storageRequests/${ids[0]}`).status, 'scheduled');
  assert.equal(db.records.get(`storageReferralCredits/${referralCreditId('referrer', 'new-customer')}`).usedCents, 0);
  assert.ok(!db.records.get(`storageReferralCredits/${referralCreditId('referrer', 'new-customer')}`).reservedInvoiceId);
  const retry = await service.prepare(ids);
  await service.send({ requestIds: ids, fingerprint: retry.fingerprint }, 'admin');
  assert.equal(emails.length, 2);
  assert.equal(db.records.get(`storageRequests/${ids[0]}`).status, 'stored');
});
test('uncertain delivery stays locked instead of risking duplicate email and discount use', async () => {
  const { service, db, emails, ids } = serviceFixture(() => { throw new Error('Timeout'); });
  const preview = await service.prepare(ids);
  await assert.rejects(service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin'), /uncertain/);
  assert.equal(db.records.get(`storageInvoices/${preview.invoiceId}`).status, 'delivery_uncertain');
  assert.equal(db.records.get(`storageRequests/${ids[0]}`).status, 'scheduled');
  await assert.rejects(service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin'), /reconciliation/);
  assert.equal(emails.length, 1);
});

test('unused referral credit carries into a later invoice without being recalculated at a changed rate', async () => {
  const { service, db, ids } = serviceFixture();
  db.records.get('storageReceipts/paid-deposit').amount = 695;
  const first = await service.prepare(ids);
  assert.equal(first.invoice.referralDiscountCents, 500);
  await service.send({ requestIds: ids, fingerprint: first.fingerprint }, 'admin');
  db.records.set('storageRequests/later-vehicle', { clientId: 'referrer', season: 'summer', status: 'scheduled', contractAmount: 300, vehicle: { type: 'car' } });
  db.records.get('admin/appSettings').referralDiscountAmount = 40;
  const second = await service.prepare(['later-vehicle']);
  assert.equal(second.invoice.referralDiscountCents, 2000);
  await service.send({ requestIds: ['later-vehicle'], fingerprint: second.fingerprint }, 'admin');
  assert.equal(db.records.get(`storageReferralCredits/${referralCreditId('referrer', 'new-customer')}`).usedCents, 2500);
});
test('different invoices cannot concurrently consume the same customer referral credit', async () => {
  const { service, db, emails, ids } = serviceFixture();
  db.records.set('storageRequests/later-vehicle', { clientId: 'referrer', season: 'summer', status: 'scheduled', contractAmount: 300, vehicle: { type: 'car' } });
  const first = await service.prepare(ids);
  const second = await service.prepare(['later-vehicle']);
  await Promise.allSettled([
    service.send({ requestIds: ids, fingerprint: first.fingerprint }, 'admin'),
    service.send({ requestIds: ['later-vehicle'], fingerprint: second.fingerprint }, 'admin')
  ]);
  assert.equal(emails.length, 1);
  assert.equal(db.records.get(`storageReferralCredits/${referralCreditId('referrer', 'new-customer')}`).usedCents, 2500);
});
test('accepted email can finalize after database failure without resending', async () => {
  const { service, db, emails, ids } = serviceFixture();
  const runTransaction = db.runTransaction.bind(db);
  let count = 0;
  db.runTransaction = (handler) => ++count === 2 ? Promise.reject(new Error('Database temporarily unavailable')) : runTransaction(handler);
  const preview = await service.prepare(ids);
  await assert.rejects(service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin'), /temporarily unavailable/);
  assert.equal(emails.length, 1);
  assert.equal((await service.prepare(ids)).alreadyAccepted, true);
  await service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin');
  assert.equal(emails.length, 1);
  assert.equal(db.records.get(`storageRequests/${ids[0]}`).status, 'stored');
});

const indexSource = readFileSync(new URL('../index.js', import.meta.url), 'utf8');
const emailContext = vm.createContext({
  Mustache, DEFAULT_STORAGE_INVOICE_TEMPLATE, DEFAULT_STORAGE_PAID_TEMPLATE, renderInvoiceHtml, invoiceTextLines,
  formatInvoiceMoney: money,
  loadEmailTemplate: async () => null,
  functions: { https: { HttpsError: Error } }
});
for (const name of ['escapeHtml', 'getLocalizedTemplateField', 'normalizeTemplateSections', 'fillLegacyTemplateTokens', 'renderMustacheTemplate', 'fillTemplateText', 'fillTemplateHtml', 'wrapTrackerEmailHtml', 'renderStorageInvoiceEmail']) {
  const start = indexSource.indexOf(`async function ${name}(`) >= 0 ? indexSource.indexOf(`async function ${name}(`) : indexSource.indexOf(`function ${name}(`);
  assert.ok(start >= 0, name);
  vm.runInContext(indexSource.slice(start, indexSource.indexOf('\n}\n', start) + 3), emailContext);
}
test('invoice email uses the exact normal banner/footer wrapper in both languages', async () => {
  for (const language of ['en', 'fr']) {
    const data = fixture();
    data.client.preferredLanguage = language;
    data.client.name = '<unsafe customer>';
    const invoice = buildInvoiceModel(data);
    const email = await emailContext.renderStorageInvoiceEmail(invoice);
    assert.equal(email.templateId, 'storage-invoice');
    assert.match(email.html, /Entrepot Ferme Colle/);
    assert.ok(email.html.includes('background:#0f172a;color:#ffffff;padding:20px 24px;'));
    assert.ok(email.html.includes('background:#0f172a;color:#94a3b8;padding:16px 24px;'));
    assert.match(email.html, /entrepot\.as-colle\.com/);
    assert.ok(email.html.includes(language === 'fr' ? 'entrepot@as-colle.com' : 'warehouse@as-colle.com'));
    assert.ok(email.html.includes(language === 'fr' ? 'SOLDE À PAYER' : 'BALANCE DUE'));
    assert.equal((email.html.match(/<html /g) || []).length, 1);
    assert.ok(!email.html.includes('<unsafe customer>'));
    assert.ok(!email.html.includes('{{invoice_details}}'));
  }
});
test('saved invoice email template overrides the default while retaining normal branding', async () => {
  const override = { ...DEFAULT_STORAGE_INVOICE_TEMPLATE, subject: { en: 'Custom subject {{invoice_id}}' }, body: { en: 'Custom message for {{tenant}}\n\n{{invoice_details}}\nBalance {{balance_due}}' } };
  emailContext.loadEmailTemplate = async () => override;
  try {
    const email = await emailContext.renderStorageInvoiceEmail(buildInvoiceModel(fixture()));
    assert.match(email.subject, /^Custom subject INV-/);
    assert.match(email.html, /Custom message for Referring client/);
    assert.match(email.html, /Entrepot Ferme Colle/);
    assert.match(email.html, /BALANCE DUE/);
  } finally { emailContext.loadEmailTemplate = async () => null; }
});
test('paid email uses localized receipt template, zero remaining balance and normal banner/footer', async () => {
  for (const locale of ['en', 'fr']) {
    const data = fixture();
    data.client.preferredLanguage = locale;
    const invoice = buildInvoiceModel(data);
    const email = await emailContext.renderStorageInvoiceEmail({ ...invoice, paidDate: '2026-10-13', paymentAmountCents: 57500 }, true);
    assert.equal(email.templateId, 'storage-paid');
    assert.match(email.html, /Entrepot Ferme Colle/);
    assert.ok(email.html.includes('background:#0f172a;color:#94a3b8;padding:16px 24px;'));
    assert.match(email.subject, locale === 'fr' ? /Paiement reçu/ : /Payment received/);
    assert.match(email.text, locale === 'fr' ? /PAYÉ/ : /PAID/);
    assert.ok(email.text.includes(money(0, locale)));
    assert.ok(email.text.includes(money(57500, locale)));
    assert.ok(email.html.includes(`-${money(57500, locale)}`));
    assert.ok(email.html.includes(`${locale === 'fr' ? 'Paiement reçu' : 'Payment received'} (2026-10-13)`));
    assert.ok(email.text.includes(`${locale === 'fr' ? 'Paiement reçu' : 'Payment received'} (2026-10-13): -${money(57500, locale)}`));
    assert.equal(invoice.balanceDueCents, 57500);
    assert.ok(!email.text.includes('{{'));
  }
});
test('paid summary reconciles the screenshot amounts; ordinary invoices omit the final-payment line', () => {
  const invoice = { ...buildInvoiceModel(fixture()), subtotalCents: 105800, depositCents: 20000,
    creditAllocations: [{ referredClientName: 'First', appliedCents: 2500 }, { referredClientName: 'Second', appliedCents: 2500 }],
    referralDiscountCents: 5000, balanceDueCents: 80800, locale: 'fr' };
  assert.ok(!renderInvoiceHtml(invoice).includes('Paiement reçu'));
  const paid = { ...invoice, balanceDueCents: 0, paymentAmountCents: 80800, paidDate: '2026-10-13' };
  assert.equal(paid.subtotalCents - paid.depositCents - paid.referralDiscountCents - paid.paymentAmountCents, paid.balanceDueCents);
  assert.ok(renderInvoiceHtml(paid).includes(`-${money(80800, 'fr')}`));
  assert.ok(renderInvoiceHtml(paid).includes('Paiement reçu (2026-10-13)'));
  assert.ok(renderInvoiceHtml(paid).includes(money(0, 'fr')));
});
test('email template changes after preview require review, not silent replacement at send time', async () => {
  let message = 'Original invoice email';
  const { service, emails, ids } = serviceFixture(undefined, async () => ({ subject: message, html: `<p>${message}</p>`, text: message, templateId: 'storage-invoice' }));
  const preview = await service.prepare(ids);
  message = 'Changed invoice email';
  await assert.rejects(service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin'), /changed after preview/);
  assert.equal(emails.length, 0);
});
test('long descriptions and many invoice rows paginate without losing the balance summary', async () => {
  const invoice = buildInvoiceModel(fixture());
  invoice.lineItems = Array.from({ length: 50 }, (_, index) => ({ requestId: `${index}`, description: `${index + 1} - ${'Motorhome with a long description '.repeat(5)}`, amountCents: 10000 }));
  const pdf = await renderInvoicePdf(invoice);
  assert.ok((await PDFDocument.load(pdf)).getPageCount() > 1);
  assert.match(renderInvoiceHtml(invoice), /Amount \(CAD\)/);
  assert.match(renderInvoiceHtml(invoice), /BALANCE DUE/);
});
test('dev invoice preview, delivered email and saved invoice all use only the safe destination', async () => {
  const policy = createEmailSafetyPolicy({ FUNCTIONS_EMULATOR: 'true' });
  const smtpMessages = [];
  const protectedMailer = createSafeMailer({ sendMail: async (message) => { smtpMessages.push(message); return { accepted: [DEV_EMAIL_RECIPIENT] }; } }, policy);
  const { service, db, ids } = serviceFixture((message) => protectedMailer.sendMail(message), undefined, policy.preview);
  const preview = await service.prepare(ids);
  assert.equal(preview.to, DEV_EMAIL_RECIPIENT);
  assert.equal(preview.cc, '');
  assert.equal(preview.developmentEmailRedirect, true);
  assert.ok(preview.subject.startsWith('[DEV] '));
  await service.send({ requestIds: ids, fingerprint: preview.fingerprint }, 'admin');
  assert.equal(smtpMessages.length, 1);
  assert.equal(smtpMessages[0].to, DEV_EMAIL_RECIPIENT);
  assert.deepEqual(smtpMessages[0].envelope.to, [DEV_EMAIL_RECIPIENT]);
  assert.ok(!smtpMessages[0].cc && !smtpMessages[0].bcc);
  assert.equal(smtpMessages[0].subject, preview.subject);
  assert.equal(db.records.get(`storageInvoices/${preview.invoiceId}`).preview.to, DEV_EMAIL_RECIPIENT);
  assert.equal(db.records.get(`storageRequests/${ids[0]}`).status, 'stored');
});
