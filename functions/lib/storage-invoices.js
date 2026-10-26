import crypto from 'node:crypto';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import { readSkipEmail } from './email-options.js';

export function invoiceError(message, code = 'failed-precondition') {
  return Object.assign(new Error(message), { code });
}
export const digest = (value) => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
export const referralCreditId = (referrer, customer) => digest([referrer, customer]);
export const invoiceIdForRequests = (ids, revision = 0) => `INV-${digest(revision ? { requestIds: [...new Set(ids)].sort(), revision } : [...new Set(ids)].sort()).slice(0, 18).toUpperCase()}`;
export function invoiceRevisionForRequests(requests) {
  const revisions = requests.map((request) => request.billingRevision ?? 0);
  if (revisions.some((value) => !Number.isSafeInteger(value) || value < 0 || value !== revisions[0])) throw invoiceError('Invoice revision differs between vehicles. Review the complete case before invoicing.');
  if (revisions[0] && requests.some((request) => !request.previousInvoiceId || request.previousInvoiceId !== requests[0].previousInvoiceId)) throw invoiceError('Replacement invoice references differ between vehicles. Review the complete case.');
  return revisions[0] || 0;
}
export const invoiceDate = (date = new Date()) => new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Toronto', year: 'numeric', month: '2-digit', day: '2-digit' }).format(date);
export function cents(value) {
  if (value == null || value === '' || typeof value === 'boolean' || !Number.isFinite(Number(value)) || Number(value) < 0) throw invoiceError('A valid non-negative billing amount is required.');
  const amount = Math.round(Number(value) * 100);
  if (!Number.isSafeInteger(amount)) throw invoiceError('Billing amount is out of range.');
  return amount;
}
const escape = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
export const money = (amount, locale) => new Intl.NumberFormat(locale === 'fr' ? 'fr-CA' : 'en-CA', { style: 'currency', currency: 'CAD' }).format(amount / 100);

export function buildInvoiceModel({ requests, allRequests, client, clients, receipts, credits, referralDiscountAmount, amountForRequest, seasonLabel, date = invoiceDate() }) {
  if (!requests.length || !requests.some((request) => request.status === 'scheduled') || requests.some((request) => !['scheduled', 'cancelled'].includes(request.status))) throw invoiceError('Every active vehicle must be Scheduled before invoicing; cancelled vehicles remain cancelled.');
  const first = requests[0];
  if (!first.clientId || !first.season) throw invoiceError('A client and season are required for invoicing.');
  if (requests.some((request) => request.clientId !== first.clientId || request.season !== first.season)) throw invoiceError('Invoice requests must belong to the same client and season.');
  if (!client?.email?.trim()) throw invoiceError('The client needs an email address before invoicing.');
  const locale = (client.preferredLanguage || first.sourceLanguage) === 'en' ? 'en' : 'fr';
  const ids = requests.map((request) => request.id).sort();
  const lineItems = requests.slice().sort((a, b) => a.id.localeCompare(b.id)).flatMap((request) => {
    const description = [request.vehicle?.typeLabel || request.vehicle?.type, request.vehicle?.brand, request.vehicle?.model, request.vehicle?.plate].filter(Boolean).join(' ') || request.id;
    if (request.status !== 'cancelled') return [{ requestId: request.id, description, amountCents: cents(amountForRequest(request)) }];
    const cancellation = request.cancellation;
    if (!cancellation || ![cancellation.originalAmountCents, cancellation.retainedDepositCents, cancellation.creditCents].every((value) => Number.isSafeInteger(value) && value >= 0)
      || cancellation.retainedDepositCents + cancellation.creditCents !== cancellation.originalAmountCents) throw invoiceError('Cancelled vehicle has an invalid cancellation adjustment.');
    return [
      { requestId: request.id, description: `${description} (${locale === 'fr' ? 'Annulé' : 'Cancelled'})`, amountCents: cancellation.originalAmountCents },
      { requestId: request.id, description: `${locale === 'fr' ? 'Annulation' : 'Cancellation adjustment'} - ${description} (${locale === 'fr' ? 'dépôt conservé' : 'deposit retained'} ${money(cancellation.retainedDepositCents, locale)})`, amountCents: -cancellation.creditCents, cancellationAdjustment: true }
    ];
  });
  const subtotalCents = lineItems.reduce((sum, line) => sum + line.amountCents, 0);
  if (!Number.isSafeInteger(subtotalCents)) throw invoiceError('Invoice total is out of range.');
  const matchingReceipts = receipts.filter((receipt) => receipt.receiptType === 'deposit' && !['void', 'cancelled'].includes(receipt.status) && receipt.requestIds?.some((id) => ids.includes(id)));
  if (matchingReceipts.some((receipt) => receipt.requestIds.some((id) => !ids.includes(id)))) throw invoiceError('A deposit covers vehicles outside this invoice. Invoice the full storage case together.');
  const depositCents = matchingReceipts.reduce((sum, receipt) => sum + cents(receipt.amount), 0);
  if (!Number.isSafeInteger(depositCents)) throw invoiceError('Deposit total is out of range.');
  if (depositCents > subtotalCents) throw invoiceError('Recorded deposits exceed the contract total. Review the payments before invoicing.');
  const retainedDepositCents = requests.filter((request) => request.status === 'cancelled').reduce((sum, request) => sum + request.cancellation.retainedDepositCents, 0);
  if (retainedDepositCents > depositCents) throw invoiceError('Retained cancellation deposits exceed the actual recorded deposits.');
  const referredIds = [...new Set(allRequests.filter((request) => request.clientId && request.clientId !== first.clientId && !['cancelled', 'rejected'].includes(request.status) && Array.isArray(request.referredByClientIds) && request.referredByClientIds.includes(first.clientId)).map((request) => request.clientId))].sort();
  let remainingCents = subtotalCents - depositCents;
  const creditAllocations = [];
  for (const referredClientId of referredIds) {
    const id = referralCreditId(first.clientId, referredClientId);
    const credit = credits.get(id);
    if (!credit && referralDiscountAmount == null) throw invoiceError('Save the referral discount in General settings > Storage billing before awarding new credits.');
    const totalCents = credit ? credit.amountCents : cents(referralDiscountAmount);
    const usedCents = credit?.usedCents || 0;
    if (!Number.isSafeInteger(totalCents) || !Number.isSafeInteger(usedCents) || totalCents < usedCents || usedCents < 0) throw invoiceError('An existing referral credit is invalid. Review it before invoicing.');
    if (credit?.reservedInvoiceId) throw invoiceError('A referral credit is already reserved by an invoice awaiting email confirmation.');
    const appliedCents = Math.min(remainingCents, totalCents - usedCents);
    if (appliedCents <= 0) continue;
    creditAllocations.push({ id, referrerClientId: first.clientId, referredClientId, referredClientName: clients.get(referredClientId)?.name || referredClientId, amountCents: totalCents, beforeUsedCents: usedCents, appliedCents });
    if (creditAllocations.length > 200) throw invoiceError('Too many referral credits for one invoice. Review the credits before invoicing.');
    remainingCents -= appliedCents;
  }
  return {
    invoiceId: invoiceIdForRequests(ids, invoiceRevisionForRequests(requests)), billingRevision: invoiceRevisionForRequests(requests),
    replacesInvoiceId: first.previousInvoiceId || null, cancelledRequestIds: requests.filter((request) => request.status === 'cancelled').map((request) => request.id),
    date, locale, currency: 'CAD', clientId: first.clientId,
    client: { name: client.name || '', email: client.email.trim(), address: client.address || '', city: client.city || '', province: client.province || '', postalCode: client.postalCode || '' },
    seasonId: first.season, seasonLabel: seasonLabel || first.season, requestIds: ids, lineItems, subtotalCents, depositCents,
    depositReceiptIds: matchingReceipts.map((receipt) => receipt.id).sort(),
    creditAllocations, referralDiscountCents: creditAllocations.reduce((sum, credit) => sum + credit.appliedCents, 0),
    balanceDueCents: remainingCents
  };
}

export function invoiceTextLines(invoice) {
  const fr = invoice.locale === 'fr';
  return [
    'Ferme Colle', `${fr ? 'Facture' : 'Invoice'} ${invoice.invoiceId}`, `${fr ? 'Date' : 'Date'}: ${invoice.date}`,
    invoice.replacesInvoiceId ? `${fr ? 'Remplace la facture' : 'Replaces invoice'}: ${invoice.replacesInvoiceId}` : '',
    invoice.client.name, invoice.client.address, [invoice.client.city, invoice.client.province, invoice.client.postalCode].filter(Boolean).join(' '),
    `${fr ? 'Saison' : 'Season'}: ${invoice.seasonLabel || invoice.seasonId}`,
    ...invoice.lineItems.map((line) => `${line.description}: ${money(line.amountCents, invoice.locale)}`),
    `${fr ? 'Total du contrat' : 'Contract total'}: ${money(invoice.subtotalCents, invoice.locale)}`,
    `${fr ? 'Dépôt reçu' : 'Deposit received'}: -${money(invoice.depositCents, invoice.locale)}`,
    ...invoice.creditAllocations.map((credit) => `${fr ? 'Rabais de recommandation' : 'Referral discount'} (${credit.referredClientName}): -${money(credit.appliedCents, invoice.locale)}`),
    ...(Number.isSafeInteger(invoice.paymentAmountCents) && invoice.paymentAmountCents >= 0
      ? [`${fr ? 'Paiement reçu' : 'Payment received'}${invoice.paidDate ? ` (${invoice.paidDate})` : ''}: -${money(invoice.paymentAmountCents, invoice.locale)}`]
      : []),
    `${fr ? 'Solde à payer' : 'Balance due'}: ${money(invoice.balanceDueCents, invoice.locale)}`,
    fr ? 'Aucun calcul de taxe supplémentaire. Le total utilise le montant convenu au contrat.' : 'No additional tax calculation. The total uses the agreed contract amount.'
  ].filter(Boolean);
}
export function renderInvoiceHtml(invoice) {
  const fr = invoice.locale === 'fr';
  const cell = 'padding:10px 12px;border-bottom:1px solid #e2e8f0;';
  const amount = `${cell}text-align:right;white-space:nowrap;`;
  const rows = invoice.lineItems.map((line) => `<tr><td style="${cell}">${escape(line.description)}</td><td style="${amount}">${money(line.amountCents, invoice.locale)}</td></tr>`).join('');
  const summary = [
    [fr ? 'Total du contrat' : 'Contract total', money(invoice.subtotalCents, invoice.locale)],
    [fr ? 'Dépôt reçu' : 'Deposit received', `-${money(invoice.depositCents, invoice.locale)}`],
    ...invoice.creditAllocations.map((credit) => [`${fr ? 'Rabais de recommandation' : 'Referral discount'} - ${credit.referredClientName}`, `-${money(credit.appliedCents, invoice.locale)}`]),
    ...(Number.isSafeInteger(invoice.paymentAmountCents) && invoice.paymentAmountCents >= 0
      ? [[`${fr ? 'Paiement reçu' : 'Payment received'}${invoice.paidDate ? ` (${invoice.paidDate})` : ''}`, `-${money(invoice.paymentAmountCents, invoice.locale)}`]]
      : [])
  ].map(([label, value]) => `<tr><td style="${cell}">${escape(label)}</td><td style="${amount}">${value}</td></tr>`).join('');
  return `<div style="border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin:16px 0;">
    <div style="background:#f8fafc;padding:16px 12px;"><strong>${fr ? 'FACTURE' : 'INVOICE'} ${escape(invoice.invoiceId)}</strong><br>${fr ? 'Date de facturation' : 'Invoice date'}: ${escape(invoice.date)}<br>${fr ? 'Saison' : 'Season'}: ${escape(invoice.seasonLabel || invoice.seasonId)}${invoice.replacesInvoiceId ? `<br>${fr ? 'Remplace la facture' : 'Replaces invoice'}: ${escape(invoice.replacesInvoiceId)}` : ''}</div>
    <div style="padding:16px 12px;"><strong>${fr ? 'Facturé à' : 'Bill to'}</strong><br>${[invoice.client.name, invoice.client.address, [invoice.client.city, invoice.client.province, invoice.client.postalCode].filter(Boolean).join(' '), invoice.client.email].filter(Boolean).map(escape).join('<br>')}</div>
    <table style="width:100%;border-collapse:collapse;font-size:14px;"><thead><tr style="background:#0f172a;color:#fff;"><th style="${cell}text-align:left;">${fr ? 'Description' : 'Description'}</th><th style="${amount}">${fr ? 'Montant (CAD)' : 'Amount (CAD)'}</th></tr></thead><tbody>${rows}${summary}<tr style="background:#eaf1f8;font-weight:700;"><td style="${cell}">${fr ? 'SOLDE À PAYER' : 'BALANCE DUE'}</td><td style="${amount}">${money(invoice.balanceDueCents, invoice.locale)}</td></tr></tbody></table>
  </div>`;
}
export async function renderInvoicePdf(invoice) {
  const pdf = await PDFDocument.create();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  pdf.setTitle(invoice.invoiceId);
  const issued = new Date(`${invoice.date}T00:00:00Z`);
  if (!Number.isNaN(issued.getTime())) { pdf.setCreationDate(issued); pdf.setModificationDate(issued); }
  const fr = invoice.locale === 'fr';
  const navy = rgb(0.059, 0.09, 0.165);
  const ink = rgb(0.09, 0.13, 0.2);
  const gray = rgb(0.39, 0.45, 0.54);
  const pale = rgb(0.96, 0.97, 0.985);
  const rule = rgb(0.88, 0.9, 0.93);
  const white = rgb(1, 1, 1);
  let page;
  let y;
  const clean = (text) => [...String(text ?? '')].map((char) => { try { font.encodeText(char); return char; } catch { return '?'; } }).join('');
  const text = (value, x, baseline, size = 10, face = font, color = ink) => page.drawText(clean(value), { x, y: baseline, size, font: face, color });
  const right = (value, edge, baseline, size = 10, face = font, color = ink) => text(value, edge - face.widthOfTextAtSize(clean(value), size), baseline, size, face, color);
  const wrap = (value, width, size = 10, face = font) => {
    const lines = [];
    let current = '';
    for (const word of clean(value).split(/\s+/).filter(Boolean)) {
      const proposal = current ? `${current} ${word}` : word;
      if (face.widthOfTextAtSize(proposal, size) <= width) { current = proposal; continue; }
      if (current) lines.push(current);
      current = '';
      for (const char of word) {
        if (current && face.widthOfTextAtSize(current + char, size) > width) { lines.push(current); current = ''; }
        current += char;
      }
    }
    if (current) lines.push(current);
    return lines.length ? lines : [''];
  };
  const newPage = () => {
    page = pdf.addPage([612, 792]);
    page.drawRectangle({ x: 0, y: 686, width: 612, height: 106, color: navy });
    text('Entrepot Ferme Colle', 48, 748, 18, bold, white);
    text(fr ? 'ENTREPOSAGE SAISONNIER DE VEHICULES' : 'SEASONAL VEHICLE STORAGE', 48, 728, 8, font, rgb(0.8, 0.84, 0.89));
    right(fr ? 'FACTURE' : 'INVOICE', 564, 748, 23, bold, white);
    text(invoice.invoiceId, 48, 704, 9, font, white);
    right(`${fr ? 'Date de facturation' : 'Invoice date'}: ${invoice.date}`, 564, 704, 9, font, white);
    y = 654;
  };
  const tableHeader = () => {
    page.drawRectangle({ x: 48, y: y - 26, width: 516, height: 26, color: navy });
    text('Description', 60, y - 17, 10, bold, white);
    right(fr ? 'Montant (CAD)' : 'Amount (CAD)', 552, y - 17, 10, bold, white);
    y -= 26;
  };
  newPage();
  const column = (label, values, x) => {
    text(label, x, 654, 9, bold, gray);
    let baseline = 633;
    for (const value of values.filter(Boolean)) {
      for (const line of wrap(value, 232, 10)) {
        if (baseline < 140) throw invoiceError('Billing address is too long. Shorten it before invoicing.');
        text(line, x, baseline);
        baseline -= 15;
      }
    }
    return baseline;
  };
  const supplierBottom = column(fr ? 'FOURNISSEUR' : 'FROM', ['Ferme Colle - Alain Colle & Serge Colle', '276, rue Dolbec', 'St-Eustache, QC J7R 6N5', '514-627-5377', fr ? 'entrepot@as-colle.com' : 'warehouse@as-colle.com'], 48);
  const clientBottom = column(fr ? 'FACTURÉ À' : 'BILL TO', [invoice.client.name, invoice.client.address, [invoice.client.city, invoice.client.province, invoice.client.postalCode].filter(Boolean).join(' '), invoice.client.email], 332);
  y = Math.min(supplierBottom, clientBottom) - 12;
  const seasonLines = wrap(`${fr ? 'Saison' : 'Season'}: ${invoice.seasonLabel || invoice.seasonId}`, 492, 10, bold);
  const seasonHeight = seasonLines.length * 14 + 18;
  page.drawRectangle({ x: 48, y: y - seasonHeight, width: 516, height: seasonHeight, color: pale });
  seasonLines.forEach((line, index) => text(line, 60, y - 17 - index * 14, 10, bold));
  y -= seasonHeight + 20;
  if (invoice.replacesInvoiceId) {
    text(`${fr ? 'Remplace la facture' : 'Replaces invoice'}: ${invoice.replacesInvoiceId}`, 48, y + 4, 9, font, gray);
    y -= 14;
  }
  tableHeader();
  const row = (label, value, { heading = false, shade = false, table = false } = {}) => {
    const face = heading ? bold : font;
    const lines = wrap(label, 378, 10, face);
    while (lines.length) {
      if (y < 110) { newPage(); if (table) tableHeader(); }
      const count = Math.min(lines.length, Math.max(1, Math.floor((y - 78 - 18) / 14)));
      const part = lines.splice(0, count);
      const height = part.length * 14 + 18;
      if (shade) page.drawRectangle({ x: 48, y: y - height, width: 516, height, color: heading ? rgb(0.9, 0.94, 0.98) : pale });
      part.forEach((line, index) => text(line, 60, y - 18 - index * 14, 10, face));
      if (!lines.length) {
        let size = heading ? 12 : 10;
        while (face.widthOfTextAtSize(clean(value), size) > 104 && size > 7) size -= 0.5;
        right(value, 552, y - 18, size, face);
      }
      page.drawLine({ start: { x: 48, y: y - height }, end: { x: 564, y: y - height }, thickness: 0.5, color: rule });
      y -= height;
    }
  };
  invoice.lineItems.forEach((line, index) => row(line.description, money(line.amountCents, invoice.locale), { table: true, shade: index % 2 === 0 }));
  y -= 18;
  row(fr ? 'Total du contrat' : 'Contract total', money(invoice.subtotalCents, invoice.locale), { heading: true });
  row(fr ? 'Dépôt reçu' : 'Deposit received', `-${money(invoice.depositCents, invoice.locale)}`);
  invoice.creditAllocations.forEach((credit) => row(`${fr ? 'Rabais de recommandation' : 'Referral discount'} - ${credit.referredClientName}`, `-${money(credit.appliedCents, invoice.locale)}`));
  y -= 12;
  row(fr ? 'SOLDE À PAYER' : 'BALANCE DUE', money(invoice.balanceDueCents, invoice.locale), { heading: true, shade: true });
  y -= 22;
  const noteLines = wrap(fr ? 'Montants selon le contrat convenu. Aucun calcul de taxe supplémentaire.' : 'Amounts follow the agreed contract. No additional tax calculation.', 516, 8.5);
  if (y - noteLines.length * 12 < 65) newPage();
  noteLines.forEach((line, index) => text(line, 48, y - index * 12, 8.5, font, gray));
  const pages = pdf.getPages();
  pages.forEach((current, index) => {
    page = current;
    page.drawLine({ start: { x: 48, y: 48 }, end: { x: 564, y: 48 }, thickness: 0.5, color: rule });
    text('entrepot.as-colle.com', 48, 32, 8.5, font, gray);
    right(`Page ${index + 1} / ${pages.length}`, 564, 32, 8.5, font, gray);
  });
  return Buffer.from(await pdf.save());
}

export function createStorageInvoiceService({ db, fieldValue, loadPricing, resolveAmount, resolveCaseId, sendEmail, from, ccForLocale, renderEmail, seasonLabelFor, routeEmail }) {
  const stateOfRequest = (request) => digest({ clientId: request.clientId, season: request.season, status: request.status, vehicle: request.vehicle, addons: request.addons, contractAmount: request.contractAmount, estimate: request.estimate, invoiceId: request.invoiceId, billingRevision: request.billingRevision, previousInvoiceId: request.previousInvoiceId, cancellation: request.cancellation });
  const serialize = async (invoice) => {
    const email = renderEmail ? await renderEmail(invoice) : { subject: `${invoice.locale === 'fr' ? 'Facture' : 'Invoice'} ${invoice.invoiceId} - Ferme Colle`, html: renderInvoiceHtml(invoice), text: invoiceTextLines(invoice).join('\n'), templateId: 'storage-invoice' };
    const requestedDelivery = { to: invoice.client.email, cc: ccForLocale(invoice.locale), subject: email.subject };
    const delivery = routeEmail ? routeEmail(requestedDelivery) : requestedDelivery;
    const preview = { invoice, invoiceId: invoice.invoiceId, fingerprint: digest({ invoice, email, delivery, layoutVersion: '20261013' }), ...email, ...delivery, pdfBase64: (await renderInvoicePdf(invoice)).toString('base64') };
    if (Buffer.byteLength(JSON.stringify({ invoice, preview })) > 900000) throw invoiceError('Invoice exceeds the document size limit. Shorten its descriptions before invoicing.');
    return preview;
  };
  const validateIds = (values) => {
    if (!Array.isArray(values) || !values.length || values.length > 50 || values.some((id) => typeof id !== 'string' || !id.trim() || id.includes('/'))) throw invoiceError('Select 1 to 50 storage requests.', 'invalid-argument');
    return [...new Set(values.map((id) => id.trim()))].sort();
  };
  async function loadQuote(values) {
    const ids = validateIds(values);
    const [requestSnap, clientSnap, receiptSnap, creditSnap, settingsSnap, pricing] = await Promise.all([
      db.collection('storageRequests').get(), db.collection('clients').get(), db.collection('storageReceipts').get(), db.collection('storageReferralCredits').get(), db.collection('admin').doc('appSettings').get(), loadPricing()
    ]);
    const allRequests = requestSnap.docs.map((snap) => ({ ...snap.data(), id: snap.id }));
    const requests = ids.map((id) => allRequests.find((request) => request.id === id));
    if (requests.some((request) => !request)) throw invoiceError('A storage request was not found.', 'not-found');
    const existing = await db.collection('storageInvoices').doc(invoiceIdForRequests(ids, invoiceRevisionForRequests(requests))).get();
    if (existing.exists && existing.data().status === 'sent') return { archived: true, existing: existing.data() };
    if (existing.data()?.status === 'superseded') throw invoiceError('This invoice was superseded. Reload the case before creating its replacement.');
    if (existing.data()?.emailAcceptedAt || existing.data()?.emailSkippedAt) return { recoverable: true, existing: existing.data() };
    if (existing.exists && ['sending', 'delivery_uncertain'].includes(existing.data().status)) throw invoiceError('Invoice delivery is already in progress or needs reconciliation. It will not be sent twice.');
    const caseId = resolveCaseId(requests[0]);
    const fullCaseIds = allRequests.filter((request) => resolveCaseId(request) === caseId).map((request) => request.id).sort();
    if (digest(ids) !== digest(fullCaseIds)) throw invoiceError('Preview and invoice the full storage case together.');
    if (requests[0].previousInvoiceId) {
      const original = (await db.collection('storageInvoices').doc(requests[0].previousInvoiceId).get()).data();
      if (original?.status !== 'superseded' || original.replacementInvoiceId !== invoiceIdForRequests(ids, invoiceRevisionForRequests(requests))) throw invoiceError('The previous invoice has not been reopened for this replacement.');
    }
    const clients = new Map(clientSnap.docs.map((snap) => [snap.id, snap.data()]));
    const credits = new Map(creditSnap.docs.map((snap) => [snap.id, snap.data()]));
    const setting = settingsSnap.data()?.referralDiscountAmount ?? null;
    const client = clients.get(requests[0].clientId);
    const locale = (client?.preferredLanguage || requests[0].sourceLanguage) === 'en' ? 'en' : 'fr';
    const invoice = buildInvoiceModel({ requests, allRequests, client, clients, receipts: receiptSnap.docs.map((snap) => ({ ...snap.data(), id: snap.id })), credits, referralDiscountAmount: setting, amountForRequest: (request) => resolveAmount(request, pricing), seasonLabel: seasonLabelFor?.(requests[0].season, locale, pricing) });
    return { invoice, states: requests.map((request) => ({ requestId: request.id, fingerprint: stateOfRequest(request) })), setting };
  }
  async function prepare(values) {
    const quote = await loadQuote(values);
    if (quote.archived) return { ...quote.existing.preview, archived: true };
    if (quote.recoverable) return { ...quote.existing.preview, archived: false, alreadyAccepted: !!quote.existing.emailAcceptedAt, emailSkipped: !!quote.existing.emailSkippedAt, completionPending: true };
    return { ...await serialize(quote.invoice), archived: false };
  }
  async function finalize(invoiceId, uid) {
    return db.runTransaction(async (transaction) => {
      const ref = db.collection('storageInvoices').doc(invoiceId);
      const snap = await transaction.get(ref);
      const data = snap.data();
      if (data?.status === 'sent') return { success: true, invoiceId, alreadySent: true };
      if (!data?.emailAcceptedAt && !data?.emailSkippedAt) throw invoiceError('Invoice email must be accepted or explicitly skipped before completing Stored status.');
      const invoice = data.invoice;
      const requestSnaps = await Promise.all(invoice.requestIds.map((id) => transaction.get(db.collection('storageRequests').doc(id))));
      requestSnaps.forEach((request, index) => {
        if (request.data()?.invoicePendingId !== invoiceId || stateOfRequest(request.data() || {}) !== data.requestStates[index].fingerprint) throw invoiceError('A request changed after invoice delivery. Reconcile it before completing Stored status.');
      });
      const creditSnaps = await Promise.all(invoice.creditAllocations.map((credit) => transaction.get(db.collection('storageReferralCredits').doc(credit.id))));
      creditSnaps.forEach((creditSnap) => { if (creditSnap.data()?.reservedInvoiceId !== invoiceId) throw invoiceError('Referral credit reservation needs reconciliation.'); });
      const timestamp = fieldValue.serverTimestamp();
      invoice.creditAllocations.forEach((credit) => transaction.set(db.collection('storageReferralCredits').doc(credit.id), { usedCents: credit.beforeUsedCents + credit.appliedCents, reservedInvoiceId: fieldValue.delete(), reservedCents: fieldValue.delete(), updatedAt: timestamp }, { merge: true }));
      invoice.requestIds.forEach((id, index) => transaction.set(db.collection('storageRequests').doc(id), { ...(requestSnaps[index].data().status === 'cancelled' ? {} : { status: 'stored', storedAt: timestamp }), invoiceId,
        ...(data.emailSkippedAt ? { invoiceEmailDeliveryStatus: 'skipped', invoiceEmailSkippedAt: data.emailSkippedAt, invoiceEmailSkippedBy: data.emailSkippedBy } : { invoiceSentAt: timestamp, invoiceEmailDeliveryStatus: 'accepted' }),
        invoicePendingId: fieldValue.delete(), updatedAt: timestamp, updatedBy: uid }, { merge: true }));
      // status remains the legacy workflow-completion marker, not proof of email delivery.
      transaction.set(ref, { status: 'sent', completedAt: timestamp, completedBy: uid,
        ...(data.emailSkippedAt ? { emailDeliveryStatus: 'skipped' } : { sentAt: timestamp, sentBy: uid, emailDeliveryStatus: 'accepted' }) }, { merge: true });
      return { success: true, invoiceId };
    });
  }
  async function send({ requestIds, fingerprint, skipEmail: skipEmailValue }, uid) {
    const skipEmail = readSkipEmail({ skipEmail: skipEmailValue });
    const ids = validateIds(requestIds);
    const quote = await loadQuote(ids);
    if (quote.archived) return { success: true, invoiceId: quote.existing.invoice.invoiceId, alreadySent: true };
    if (quote.recoverable) return finalize(quote.existing.invoice.invoiceId, uid);
    const invoiceId = quote.invoice.invoiceId;
    const invoiceRef = db.collection('storageInvoices').doc(invoiceId);
    const previous = await invoiceRef.get();
    if (previous.data()?.status === 'sent') return { success: true, invoiceId, alreadySent: true };
    if (previous.data()?.emailAcceptedAt || previous.data()?.emailSkippedAt) return finalize(invoiceId, uid);
    if (!from && !skipEmail) throw invoiceError('SMTP default FROM is not configured.');
    const preview = await serialize(quote.invoice);
    if (fingerprint !== preview.fingerprint) throw invoiceError('Invoice details changed after preview. Reopen the preview before sending.');
    const claimed = await db.runTransaction(async (transaction) => {
      const snap = await transaction.get(invoiceRef);
      if (snap.data()?.status === 'sent') return false;
      if (['sending', 'delivery_uncertain'].includes(snap.data()?.status)) throw invoiceError('Invoice delivery is already in progress or needs reconciliation.');
      const requestSnaps = await Promise.all(ids.map((id) => transaction.get(db.collection('storageRequests').doc(id))));
      const creditSnaps = await Promise.all(quote.invoice.creditAllocations.map((credit) => transaction.get(db.collection('storageReferralCredits').doc(credit.id))));
      const settings = await transaction.get(db.collection('admin').doc('appSettings'));
      if ((settings.data()?.referralDiscountAmount ?? null) !== quote.setting) throw invoiceError('Referral discount setting changed. Reopen the invoice preview.');
      requestSnaps.forEach((request, index) => { if (stateOfRequest(request.data() || {}) !== quote.states[index].fingerprint || request.data()?.invoicePendingId) throw invoiceError('A request changed or is already being invoiced. Reopen the preview.'); });
      creditSnaps.forEach((snap, index) => {
        const credit = quote.invoice.creditAllocations[index];
        if (snap.data()?.reservedInvoiceId || (snap.data()?.usedCents || 0) !== credit.beforeUsedCents || (snap.exists && snap.data().amountCents !== credit.amountCents)) throw invoiceError('A referral credit changed. Reopen the preview.');
      });
      const timestamp = fieldValue.serverTimestamp();
      transaction.set(invoiceRef, { invoice: quote.invoice, requestStates: quote.states, status: 'sending', preview, createdAt: snap.data()?.createdAt || timestamp, createdBy: uid, attemptAt: timestamp,
        ...(skipEmail ? { emailSkippedAt: timestamp, emailSkippedBy: uid, emailDeliveryStatus: 'skipped' } : {}) });
      quote.invoice.creditAllocations.forEach((credit) => transaction.set(db.collection('storageReferralCredits').doc(credit.id), { referrerClientId: credit.referrerClientId, referredClientId: credit.referredClientId, amountCents: credit.amountCents, usedCents: credit.beforeUsedCents, reservedInvoiceId: invoiceId, reservedCents: credit.appliedCents, updatedAt: timestamp }, { merge: true }));
      ids.forEach((id) => transaction.set(db.collection('storageRequests').doc(id), { invoicePendingId: invoiceId }, { merge: true }));
      return true;
    });
    if (!claimed) return { success: true, invoiceId, alreadySent: true };
    if (skipEmail) return finalize(invoiceId, uid);
    try {
      const result = await sendEmail({ from, to: preview.to, cc: preview.cc, subject: preview.subject, html: preview.html, text: preview.text || invoiceTextLines(quote.invoice).join('\n'), attachments: [{ filename: `${invoiceId}.pdf`, content: Buffer.from(preview.pdfBase64, 'base64'), contentType: 'application/pdf' }] });
      if (Array.isArray(result?.accepted) && !result.accepted.some((address) => String(address).toLowerCase() === preview.to.toLowerCase())) throw invoiceError('Recipient delivery could not be confirmed.');
    } catch (error) {
      const rejected = Number(error.responseCode) >= 400 && Number(error.responseCode) < 600;
      await db.runTransaction(async (transaction) => {
        const snap = await transaction.get(invoiceRef);
        if (snap.data()?.status !== 'sending') return;
        transaction.set(invoiceRef, { status: rejected ? 'failed' : 'delivery_uncertain', lastError: rejected ? 'Mail server rejected the invoice.' : 'Delivery could not be confirmed. Reconcile before retrying.' }, { merge: true });
        if (rejected) {
          quote.invoice.creditAllocations.forEach((credit) => transaction.set(db.collection('storageReferralCredits').doc(credit.id), { reservedInvoiceId: fieldValue.delete(), reservedCents: fieldValue.delete() }, { merge: true }));
          ids.forEach((id) => transaction.set(db.collection('storageRequests').doc(id), { invoicePendingId: fieldValue.delete() }, { merge: true }));
        }
      });
      throw invoiceError(rejected ? 'Invoice email was rejected. Vehicles remain Scheduled; you may reopen the preview and retry.' : 'Invoice delivery is uncertain. Vehicles remain Scheduled; reconcile delivery before retrying to avoid duplicates.');
    }
    await invoiceRef.set({ emailAcceptedAt: fieldValue.serverTimestamp() }, { merge: true });
    return finalize(invoiceId, uid);
  }
  async function get(invoiceId) {
    if (typeof invoiceId !== 'string' || !/^INV-[A-F0-9]{18}$/.test(invoiceId)) throw invoiceError('Invalid invoice ID.', 'invalid-argument');
    const snap = await db.collection('storageInvoices').doc(invoiceId).get();
    if (!snap.exists) throw invoiceError('Invoice not found.', 'not-found');
    return { ...snap.data().preview, status: snap.data().status };
  }
  return { prepare, send, get };
}
