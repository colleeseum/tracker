import fs from 'node:fs/promises';
import path from 'node:path';
import { buildInvoiceModel, renderInvoicePdf } from '../lib/storage-invoices.js';
import { stampPaidInvoicePdf } from '../lib/storage-payments.js';

const output = process.argv[2] || '/private/tmp/tracker-invoice-layout';
await fs.mkdir(output, { recursive: true });
for (const locale of ['en', 'fr']) {
  const requests = [
    { id: 'sample-rv', clientId: 'sample-client', season: 'winter', status: 'scheduled', contractAmount: 460, vehicle: { typeLabel: locale === 'fr' ? 'Motorisé' : 'Motorhome', brand: 'Winnebago', model: 'Travato 59K', plate: 'ABC 123' } },
    { id: 'sample-car', clientId: 'sample-client', season: 'winter', status: 'scheduled', contractAmount: 380, vehicle: { typeLabel: locale === 'fr' ? 'Voiture' : 'Car', brand: 'Toyota', model: 'Camry', plate: 'XYZ 789' } }
  ];
  const invoice = buildInvoiceModel({
    requests, allRequests: [...requests, { clientId: 'sample-referral', status: 'scheduled', referredByClientIds: ['sample-client'] }],
    client: { name: 'Camille Tremblay', email: 'camille@example.invalid', address: '100, rue Exemple', city: 'Montréal', province: 'QC', postalCode: 'H0H 0H0', preferredLanguage: locale },
    clients: new Map([['sample-referral', { name: 'Jordan Martin' }]]),
    receipts: [{ id: 'sample-deposit', receiptType: 'deposit', amount: 150, requestIds: requests.map((request) => request.id) }],
    credits: new Map(), referralDiscountAmount: 25, amountForRequest: (request) => request.contractAmount,
    seasonLabel: locale === 'fr' ? 'Hiver 2027' : 'Winter 2027', date: '2026-10-13'
  });
  const pdf = await renderInvoicePdf(invoice);
  await fs.writeFile(path.join(output, `invoice-${locale}.pdf`), pdf);
  if (process.argv.includes('--paid')) await fs.writeFile(path.join(output, `invoice-paid-${locale}.pdf`), await stampPaidInvoicePdf(pdf.toString('base64'), locale));
  if (locale === 'en' && process.argv.includes('--stress')) {
    const stress = { ...invoice, lineItems: Array.from({ length: 12 }, (_, index) => ({ requestId: `long-${index}`, description: `Vehicle ${index + 1}: ${'A motorhome with a long description and accessories '.repeat(4)}`, amountCents: 10000 })), subtotalCents: 120000, depositCents: 15000, referralDiscountCents: 2500, balanceDueCents: 102500 };
    await fs.writeFile(path.join(output, 'invoice-stress.pdf'), await renderInvoicePdf(stress));
  }
}
console.log(`English/French sample PDFs written to ${output}. No database access or email.`);
