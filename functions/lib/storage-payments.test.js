import test from 'node:test';
import assert from 'node:assert/strict';
import { PDFDocument } from 'pdf-lib';
import { createStoragePaymentService, stampPaidInvoicePdf } from './storage-payments.js';
import { renderInvoicePdf, digest } from './storage-invoices.js';
import { DEFAULT_STORAGE_PAID_TEMPLATE } from './storage-invoice-template.js';
import { createEmailSafetyPolicy, createSafeMailer } from './email-safety.js';

const id = 'INV-123456789012345678';
async function fixture(sender, locale = 'en') {
  const invoice = { invoiceId: id, date: '2026-10-13', locale, clientId: 'client', client: { name: 'Test client', email: 'customer@example.invalid' }, seasonId: 'winter', requestIds: ['a', 'b'], lineItems: [{ description: 'RV', amountCents: 30000 }, { description: 'Car', amountCents: 40000 }], subtotalCents: 70000, depositCents: 10000, referralDiscountCents: 2500, creditAllocations: [{ referredClientName: 'Referral', appliedCents: 2500 }], balanceDueCents: 57500 };
  const originalPdf = (await renderInvoicePdf(invoice)).toString('base64');
  const records = new Map([
    [`storageInvoices/${id}`, { status: 'sent', invoice, preview: { invoice, pdfBase64: originalPdf, cc: 'office@example.invalid' } }],
    ...invoice.requestIds.map((requestId) => [`storageRequests/${requestId}`, { status: 'stored', invoiceId: id, clientId: 'client', season: 'winter', contractAmount: 350 }]),
    ['storageReferralCredits/credit', { usedCents: 2500 }]
  ]);
  const snapshot = (path) => ({ id: path.split('/').at(-1), exists: records.has(path), data: () => structuredClone(records.get(path)) });
  const write = (ref, value, options) => {
    const next = options?.merge ? { ...records.get(ref.path), ...structuredClone(value) } : structuredClone(value);
    for (const key of Object.keys(next)) if (next[key] === '__delete__') delete next[key];
    records.set(ref.path, next);
  };
  const ref = (path) => ({ path, get: async () => snapshot(path) });
  let queue = Promise.resolve();
  const db = {
    collection: (name) => ({ doc: (key) => ref(`${name}/${key}`), get: async () => ({ docs: [...records.keys()].filter((path) => path.startsWith(`${name}/`)).map(snapshot) }) }),
    runTransaction(handler) {
      const work = queue.then(async () => {
        const writes = [];
        const result = await handler({ get: async (reference) => snapshot(reference.path), set: (...args) => writes.push(args) });
        writes.forEach((args) => write(...args));
        return result;
      });
      queue = work.catch(() => {});
      return work;
    }
  };
  const emails = [];
  const policy = createEmailSafetyPolicy({ FUNCTIONS_EMULATOR: 'true' });
  const mailer = createSafeMailer({ sendMail: async (message) => { emails.push(message); return sender ? sender(message) : { accepted: ['entrepot@as-colle.com'] }; } }, policy);
  const service = createStoragePaymentService({ db, fieldValue: { serverTimestamp: () => 'timestamp', delete: () => '__delete__' },
    from: 'billing@as-colle.com', sendEmail: (message) => mailer.sendMail(message), routeEmail: policy.preview,
    resolveCaseId: (request) => `${request.clientId}|${request.season}`,
    renderEmail: async (invoice) => ({ subject: `${locale === 'fr' ? 'Paiement reçu' : 'Payment received'} ${invoice.invoiceId}`, html: `<p>${invoice.paymentAmountCents}</p>`, text: 'Paid in full', templateId: 'storage-paid' })
  });
  return { service, records, emails, originalPdf, db };
}

test('payment preview is read-only and preserves invoice amounts and pages in EN and FR', async () => {
  for (const locale of ['en', 'fr']) {
    const { service, records, emails, originalPdf } = await fixture(undefined, locale);
    const before = digest([...records]);
    const preview = await service.prepare(id);
    assert.equal(digest([...records]), before);
    assert.equal(emails.length, 0);
    assert.equal(preview.paymentAmountCents, 57500);
    assert.equal(preview.invoice.balanceDueCents, 57500);
    assert.equal(preview.to, 'entrepot@as-colle.com');
    assert.equal(preview.cc, '');
    assert.equal(preview.developmentEmailRedirect, true);
    assert.notEqual(preview.pdfBase64, originalPdf);
    assert.equal((await PDFDocument.load(Buffer.from(preview.pdfBase64, 'base64'))).getPageCount(), (await PDFDocument.load(Buffer.from(originalPdf, 'base64'))).getPageCount());
    assert.match(DEFAULT_STORAGE_PAID_TEMPLATE.body[locale], locale === 'fr' ? /PAYÉ/ : /PAID/);
  }
});

test('full-payment confirmation sends exactly the preview PDF and marks the complete case Paid once', async () => {
  const { service, records, emails, originalPdf } = await fixture();
  const preview = await service.prepare(id);
  await assert.rejects(service.send({ invoiceId: id, fingerprint: preview.fingerprint }, 'admin'), /Confirm/);
  await service.send({ invoiceId: id, fingerprint: preview.fingerprint, confirmFullPayment: true }, 'admin');
  assert.equal(emails.length, 1);
  assert.deepEqual(emails[0].attachments[0].content, Buffer.from(preview.pdfBase64, 'base64'));
  assert.equal(emails[0].envelope.to[0], 'entrepot@as-colle.com');
  assert.equal(emails[0].subject, preview.subject);
  for (const requestId of ['a', 'b']) {
    const request = records.get(`storageRequests/${requestId}`);
    assert.equal(request.status, 'paid');
    assert.equal(request.paidInvoiceId, id);
    assert.ok(!request.paymentPendingId);
  }
  const saved = records.get(`storageInvoices/${id}`);
  assert.equal(saved.payment.amountCents, 57500);
  assert.equal(saved.balanceRemainingCents, 0);
  assert.equal(saved.preview.pdfBase64, originalPdf);
  assert.equal(saved.invoice.balanceDueCents, 57500);
  assert.equal(records.get('storageReferralCredits/credit').usedCents, 2500);
  assert.equal((await service.get(id)).pdfBase64, preview.pdfBase64);
  await service.send({ invoiceId: id, fingerprint: preview.fingerprint, confirmFullPayment: true }, 'admin');
  assert.equal(emails.length, 1);
});

test('stale preview, unsent invoice and added case vehicles cannot be marked Paid', async () => {
  const { service, records, emails } = await fixture();
  const preview = await service.prepare(id);
  records.get('storageRequests/a').contractAmount = 400;
  await assert.rejects(service.send({ invoiceId: id, fingerprint: preview.fingerprint, confirmFullPayment: true }, 'admin'), /changed after preview/);
  records.set('storageRequests/c', { status: 'stored', clientId: 'client', season: 'winter' });
  await assert.rejects(service.prepare(id), /complete invoiced case/);
  records.get(`storageInvoices/${id}`).status = 'failed';
  await assert.rejects(service.prepare(id), /original invoice/);
  assert.equal(emails.length, 0);
});

test('definite rejection keeps Stored and allows reviewed retry; uncertain delivery stays locked', async () => {
  let rejected = true;
  const { service, records, emails } = await fixture(() => { if (rejected) throw Object.assign(new Error('Rejected'), { responseCode: 550 }); return { accepted: ['entrepot@as-colle.com'] }; });
  let preview = await service.prepare(id);
  await assert.rejects(service.send({ invoiceId: id, fingerprint: preview.fingerprint, confirmFullPayment: true }, 'admin'), /rejected/);
  assert.equal(records.get('storageRequests/a').status, 'stored');
  assert.ok(!records.get('storageRequests/a').paymentPendingId);
  rejected = false;
  preview = await service.prepare(id);
  await service.send({ invoiceId: id, fingerprint: preview.fingerprint, confirmFullPayment: true }, 'admin');
  assert.equal(emails.length, 2);
  const uncertain = await fixture(() => { throw new Error('Timeout'); });
  const pending = await uncertain.service.prepare(id);
  await assert.rejects(uncertain.service.send({ invoiceId: id, fingerprint: pending.fingerprint, confirmFullPayment: true }, 'admin'), /uncertain/);
  await assert.rejects(uncertain.service.prepare(id), /reconciliation/);
  assert.equal(uncertain.records.get('storageRequests/a').status, 'stored');
  assert.equal(uncertain.emails.length, 1);
});

test('concurrent payment confirmations do not duplicate email', async () => {
  const { service, emails } = await fixture();
  const preview = await service.prepare(id);
  await Promise.allSettled([1, 2].map(() => service.send({ invoiceId: id, fingerprint: preview.fingerprint, confirmFullPayment: true }, 'admin')));
  assert.equal(emails.length, 1);
});

test('accepted payment email completes status after a finalization failure without resending', async () => {
  const { service, records, emails, db } = await fixture();
  const preview = await service.prepare(id);
  const originalTransaction = db.runTransaction.bind(db);
  let transactions = 0;
  db.runTransaction = (handler) => {
    if (++transactions === 3) return Promise.reject(new Error('Finalization unavailable'));
    return originalTransaction(handler);
  };
  await assert.rejects(service.send({ invoiceId: id, fingerprint: preview.fingerprint, confirmFullPayment: true }, 'admin'), /Finalization unavailable/);
  assert.equal(emails.length, 1);
  assert.equal(records.get('storageRequests/a').status, 'stored');
  const recovery = await service.prepare(id);
  assert.equal(recovery.alreadyAccepted, true);
  await service.send({ invoiceId: id, fingerprint: recovery.fingerprint, confirmFullPayment: true }, 'admin');
  assert.equal(records.get('storageRequests/a').status, 'paid');
  assert.equal(emails.length, 1);
});

test('stamp applies to every page without rebuilding invoice content', async () => {
  const pdf = await PDFDocument.create();
  pdf.addPage(); pdf.addPage();
  const result = await stampPaidInvoicePdf(Buffer.from(await pdf.save()).toString('base64'), 'fr');
  assert.equal((await PDFDocument.load(result)).getPageCount(), 2);
});
