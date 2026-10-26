import { digest, invoiceError } from './storage-invoices.js';
import { mergeClientEmailCc } from './client-email-cc.js';

export function createClientEmailBatchService({ db, fieldValue, renderEmail, routeEmail, sendEmail, from }) {
  const refFor = (id) => {
    if (typeof id !== 'string' || !/^[A-Za-z0-9_-]{1,100}$/.test(id)) throw invoiceError('Select a valid mailing.', 'invalid-argument');
    return db.collection('clientEmailBatches').doc(id);
  };
  const timestamp = () => fieldValue.serverTimestamp();
  const messageFor = (batch, recipient) => ({ ...batch.templates[recipient.templateKey], ...recipient.delivery });
  const view = (id, batch) => ({ batchId: id, subject: batch.subject, body: batch.body, fingerprint: batch.fingerprint,
    recipients: Object.values(batch.recipients), templates: batch.templates });
  async function prepare({ clientIds, subject, body }, uid) {
    if (!Array.isArray(clientIds) || !clientIds.length || clientIds.length > 100 || clientIds.some((id) => typeof id !== 'string' || !id || id.includes('/'))) throw invoiceError('Select 1 to 100 clients.', 'invalid-argument');
    if (typeof subject !== 'string' || !subject.trim() || subject.length > 200 || /[\r\n]/.test(subject)) throw invoiceError('Enter a subject of up to 200 characters.', 'invalid-argument');
    if (typeof body !== 'string' || !body.trim() || body.length > 10000) throw invoiceError('Enter a message of up to 10,000 characters.', 'invalid-argument');
    subject = subject.trim(); body = body.trim();
    const ids = [...new Set(clientIds)];
    const clients = await Promise.all(ids.map((id) => db.collection('clients').doc(id).get()));
    const recipients = {};
    const templates = {};
    const addresses = new Set();
    for (const snap of clients) {
      if (!snap.exists) throw invoiceError('A selected client no longer exists. Refresh the selection.');
      const client = snap.data();
      const to = String(client.email || '').trim();
      if (!/^[^\s@,;<>]+@[^\s@,;<>]+$/.test(to)) throw invoiceError(`Set a valid email for ${client.name || snap.id} before sending.`);
      if (addresses.has(to.toLowerCase())) throw invoiceError(`Several selected clients share ${to}. Select only one of those clients.`);
      addresses.add(to.toLowerCase());
      const locale = client.preferredLanguage === 'fr' ? 'fr' : 'en';
      const isStorageClient = client.nonStorageClient !== true;
      const templateKey = `${locale}-${isStorageClient ? 'storage' : 'general'}`;
      const cc = mergeClientEmailCc(locale === 'fr' ? 'entrepot@as-colle.com' : 'warehouse@as-colle.com', client);
      const delivery = routeEmail({ to, cc, subject });
      if (!templates[templateKey]) templates[templateKey] = {
        html: renderEmail({ locale, subject: delivery.subject, body, isStorageClient }), text: body
      };
      Object.defineProperty(recipients, snap.id, { enumerable: true, value: { clientId: snap.id, name: client.name || to, locale, templateKey, delivery, status: 'ready', error: '' } });
    }
    const fingerprint = digest({ subject, body, recipients, templates });
    const ref = db.collection('clientEmailBatches').doc();
    const batch = { subject, body, recipients, templates, fingerprint, createdAt: timestamp(), createdBy: uid };
    await ref.set(batch);
    return view(ref.id, batch);
  }
  async function get(id) {
    const snap = await refFor(id).get();
    if (!snap.exists) throw invoiceError('Mailing not found.', 'not-found');
    return view(snap.id, snap.data());
  }
  async function send({ batchId, clientId, fingerprint }, uid) {
    if (!from) throw invoiceError('SMTP default FROM is not configured.');
    const ref = refFor(batchId);
    const claim = await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const batch = snap.data();
      if (!batch || batch.fingerprint !== fingerprint) throw invoiceError('Mailing preview changed. Reload it before sending.');
      if (typeof clientId !== 'string' || !Object.hasOwn(batch.recipients, clientId)) throw invoiceError('Client is not part of this mailing.');
      const recipient = batch.recipients[clientId];
      if (!['ready', 'failed'].includes(recipient.status)) return { done: true, recipient };
      const message = messageFor(batch, recipient);
      // Do not send a stored dev preview through a live backend, or vice versa.
      const currentDelivery = routeEmail(message);
      if (['to', 'cc', 'bcc', 'subject', 'developmentEmailRedirect'].some((key) => currentDelivery[key] !== recipient.delivery[key])) throw invoiceError('Email routing changed. Create a new preview.');
      const recipients = { ...batch.recipients, [clientId]: { ...recipient, status: 'sending', error: '' } };
      tx.set(ref, { recipients, updatedAt: timestamp() }, { merge: true });
      return { recipient, message };
    });
    if (claim.done) return claim.recipient;
    let status = 'uncertain';
    let error = '';
    try {
      const result = await sendEmail({ from, ...claim.message });
      const accepted = (result?.accepted || []).map((value) => String(value.address || value).toLowerCase());
      if (accepted.length) {
        status = accepted.includes(claim.message.to.toLowerCase()) && !result?.rejected?.length ? 'sent' : 'sent_with_warning';
        if (status === 'sent_with_warning') error = 'Some recipients were rejected. Do not resend automatically; review delivery.';
      } else if (result?.rejected?.length) { status = 'failed'; error = 'The email server rejected all recipients. You can retry.'; }
      else error = 'Delivery was not confirmed. Review with the email provider before resending.';
    } catch (failure) {
      // A timeout/disconnect after SMTP DATA may already have delivered the email.
      status = failure.code === 'EAUTH' || (failure.responseCode >= 400 && failure.responseCode < 600) ? 'failed' : 'uncertain';
      error = status === 'failed' ? 'The email server rejected the message. Check SMTP configuration and retry.' : 'Delivery is uncertain. Review with the email provider before resending.';
    }
    return db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const batch = snap.data();
      const recipient = { ...batch.recipients[clientId], status, error };
      tx.set(ref, { recipients: { ...batch.recipients, [clientId]: recipient }, updatedAt: timestamp() }, { merge: true });
      const emailRef = db.collection('clientEmails').doc(`${batchId}_${clientId}`);
      tx.set(emailRef, { batchId, clientId, ...recipient.delivery, body: batch.body, status, error, attemptedAt: timestamp(), attemptedBy: uid,
        ...(['sent', 'sent_with_warning'].includes(status) ? { sentAt: timestamp(), sentBy: uid } : {}) });
      // A client could have been deleted while SMTP was running. Do not recreate it.
      return recipient;
    });
  }
  return { prepare, get, send };
}
