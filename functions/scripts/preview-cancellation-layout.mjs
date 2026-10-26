import fs from 'node:fs/promises';
import path from 'node:path';
import { buildInvoiceModel, renderInvoicePdf } from '../lib/storage-invoices.js';

const output = process.argv[2] || '/private/tmp/tracker-cancellation-layout';
await fs.mkdir(output, { recursive: true });
for (const locale of ['en', 'fr']) {
  const requests = [
    { id: 'rv', status: 'scheduled', clientId: 'sample', season: 'winter', contractAmount: 460, billingRevision: 1, previousInvoiceId: 'INV-000000000000000001', vehicle: { typeLabel: locale === 'fr' ? 'Motorisé' : 'Motorhome' } },
    { id: 'moto', status: 'cancelled', clientId: 'sample', season: 'winter', contractAmount: 180, billingRevision: 1, previousInvoiceId: 'INV-000000000000000001', vehicle: { typeLabel: locale === 'fr' ? 'Moto' : 'Motorcycle' }, cancellation: { originalAmountCents: 18000, retainedDepositCents: 5000, creditCents: 13000 } }
  ];
  const invoice = buildInvoiceModel({ requests, allRequests: requests, client: { name: 'Camille Tremblay', email: 'sample@example.invalid', preferredLanguage: locale },
    clients: new Map(), credits: new Map(), receipts: [{ id: 'deposit', receiptType: 'deposit', amount: 150, requestIds: ['rv', 'moto'] }],
    amountForRequest: (request) => request.contractAmount, seasonLabel: locale === 'fr' ? 'Hiver 2027' : 'Winter 2027', date: '2026-10-25' });
  await fs.writeFile(path.join(output, `cancellation-${locale}.pdf`), await renderInvoicePdf(invoice));
}
console.log(`Synthetic EN/FR cancellation PDFs written to ${output}. No database or email.`);
