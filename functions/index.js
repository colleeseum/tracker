import functions from 'firebase-functions';
import admin from 'firebase-admin';
import nodemailer from 'nodemailer';
import crypto from 'node:crypto';
import Mustache from 'mustache';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';

const smtpHost = process.env.SMTP2GO_HOST || 'mail.smtp2go.com';
const smtpPort = Number(process.env.SMTP2GO_PORT || 587);
const smtpUser = process.env.SMTP2GO_USER;
const smtpPass = process.env.SMTP2GO_PASS;
const defaultFrom = process.env.SMTP2GO_FROM;
const restrictedDomain = process.env.MAILER_RESTRICTED_DOMAIN || '@as-colle.com';
const recaptchaSecret = process.env.RECAPTCHA_SECRET || '';
const skipCaptcha = process.env.SKIP_CAPTCHA === 'true';
const publicAllowedRecipients = (process.env.PUBLIC_ALLOWED_RECIPIENTS || '')
  .split(',')
  .map((email) => email.trim().toLowerCase())
  .filter(Boolean);
const rateLimitExemptReplyTo = (process.env.RATE_LIMIT_EXEMPT_REPLY_TO || '').toLowerCase();
const frenchSenderKeyword = (process.env.FRENCH_SENDER_KEYWORD || 'entrepot@as-colle.com').toLowerCase();
const englishSenderKeyword = (process.env.ENGLISH_SENDER_KEYWORD || 'warehouse@as-colle.com').toLowerCase();
const githubRepo = process.env.GITHUB_PUBLISH_REPO || 'colleeseum/entrepot';
const githubEventType = process.env.GITHUB_PUBLISH_EVENT || 'tracker-content-publish';
const githubTokenSecretResource =
  process.env.GITHUB_PUBLISH_TOKEN_SECRET_RESOURCE ||
  process.env.GITHUB_PUBLISH_TOKEN_SECRET ||
  'projects/1044638579272/secrets/Entrepot';
const publishCallbackTokenSecretResource =
  process.env.PUBLISH_CALLBACK_TOKEN_SECRET_RESOURCE ||
  process.env.PUBLISH_CALLBACK_TOKEN_SECRET ||
  'projects/1044638579272/secrets/TRACKER_PUBLISH_CALLBACK_TOKEN';
const publishDryRun = process.env.FUNCTIONS_EMULATOR === 'true' || process.env.PUBLISH_DRY_RUN === 'true';
const TRACKER_ADMIN_EMAILS = new Set(['sergecolle@gmail.com', 'arcolle@gmail.com']);
const PUBLISH_COLLECTIONS = [
  'storageSeasons',
  'storageOffers',
  'offerTemplates',
  'vehicleTypes',
  'storageAddOns',
  'storageSeasonAddOns',
  'storageConditions',
  'storageEtiquette',
  'i18nEntries'
];
const AUDIT_FIELDS = new Set(['createdAt', 'createdBy', 'updatedAt', 'updatedBy']);
const DEFAULT_VEHICLE_TYPE_LABELS = {
  rv: { en: 'RV / Motorhome', fr: 'VR / motorisé' },
  boat: { en: 'Boat', fr: 'Bateau' },
  trailer: { en: 'Trailer', fr: 'Remorque' },
  car: { en: 'Car', fr: 'Voiture' },
  truck: { en: 'Truck', fr: 'Camion' },
  motorcycle: { en: 'Motorcycle', fr: 'Moto' },
  spyder: { en: 'Can-Am Spyder', fr: 'Can-Am Spyder' },
  other: { en: 'Other', fr: 'Autre' }
};
const STORAGE_RECEIPT_EMAIL_WORKFLOWS = {
  contract: {
    templateIds: ['receipt-contract'],
    expectedStatuses: ['waiting_contract_deposit', 'waiting_contract'],
    targetStatusByCurrent: {
      waiting_contract_deposit: 'waiting_deposit',
      waiting_contract: 'reserved'
    },
    createsReceipt: false,
    requiresSignedContract: true
  },
  contract_deposit: {
    templateIds: ['receipt-contract-deposit'],
    expectedStatuses: ['waiting_contract_deposit'],
    targetStatus: 'reserved',
    createsReceipt: true,
    requiresSignedContract: true
  },
  deposit: {
    templateIds: ['receipt-deposit', 'receipt-deport'],
    expectedStatuses: ['waiting_contract_deposit', 'waiting_deposit'],
    targetStatusByCurrent: {
      waiting_contract_deposit: 'waiting_contract',
      waiting_deposit: 'reserved'
    },
    createsReceipt: true,
    requiresSignedContract: false
  },
  missing: {
    templateIds: ['receipt-missing'],
    expectedStatuses: ['waiting_contract_deposit', 'waiting_contract', 'waiting_deposit'],
    createsReceipt: false,
    requiresSignedContract: false,
    changesStatus: false
  }
};

admin.initializeApp();
const db = admin.firestore();

let cachedGithubToken = null;
let githubTokenLoadPromise = null;

let publishCallbackTokenLoadPromise = null;

async function loadGithubPublishToken() {
  if (cachedGithubToken) return cachedGithubToken;
  if (githubTokenLoadPromise) return githubTokenLoadPromise;

  githubTokenLoadPromise = (async () => {
    const direct = process.env.GITHUB_PUBLISH_TOKEN;
    if (direct) {
      cachedGithubToken = direct;
      return cachedGithubToken;
    }

    const secretResource = githubTokenSecretResource;
    if (!secretResource) {
      cachedGithubToken = '';
      return cachedGithubToken;
    }

    const versionName = secretResource.includes('/versions/')
      ? secretResource
      : `${secretResource}/versions/latest`;
    const client = new SecretManagerServiceClient();
    const [result] = await client.accessSecretVersion({ name: versionName });
    const token = result?.payload?.data ? result.payload.data.toString('utf8').trim() : '';
    cachedGithubToken = token;
    return cachedGithubToken;
  })()
    .catch((err) => {
      console.error('Failed to load GitHub publish token from Secret Manager.', err);
      cachedGithubToken = '';
      return cachedGithubToken;
    })
    .finally(() => {
      githubTokenLoadPromise = null;
    });

  return githubTokenLoadPromise;
}

async function loadPublishCallbackToken() {
  const direct = process.env.PUBLISH_CALLBACK_TOKEN;
  if (direct) {
    return direct.trim();
  }

  if (publishCallbackTokenLoadPromise) return publishCallbackTokenLoadPromise;

  publishCallbackTokenLoadPromise = (async () => {
    const secretResource = publishCallbackTokenSecretResource;
    if (!secretResource) return '';

    const versionName = secretResource.includes('/versions/')
      ? secretResource
      : `${secretResource}/versions/latest`;
    const client = new SecretManagerServiceClient();
    const [result] = await client.accessSecretVersion({ name: versionName });
    return result?.payload?.data ? result.payload.data.toString('utf8').trim() : '';
  })()
    .catch((err) => {
      console.error('Failed to load publish callback token from Secret Manager.', err);
      return '';
    })
    .finally(() => {
      publishCallbackTokenLoadPromise = null;
    });

  return publishCallbackTokenLoadPromise;
}

function extractBearerToken(req) {
  const header = req?.headers?.authorization || req?.headers?.Authorization || '';
  if (typeof header !== 'string') return '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function sanitizePublishJobStatus(value) {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return 'unknown';
  if (raw.length > 80) return raw.slice(0, 80);
  return raw;
}

function sanitizePublishJobDetails(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') {
    return value.length > 2000 ? value.slice(0, 2000) : value;
  }
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (typeof value === 'object') {
    try {
      return JSON.parse(JSON.stringify(value));
    } catch {
      return null;
    }
  }
  return null;
}

export const reportSitePublishJobUpdate = functions.https.onRequest(async (req, res) => {
  if (req.method !== 'POST') {
    res.status(405).send('Method Not Allowed');
    return;
  }

  const expected = await loadPublishCallbackToken();
  const presented =
    (req.headers['x-tracker-callback-token'] || '').toString().trim() ||
    extractBearerToken(req);
  if (!expected || !presented || expected !== presented) {
    res.status(403).send('Forbidden');
    return;
  }

  const publishId = (req.body?.publishId || req.query?.publishId || '').toString().trim();
  const status = sanitizePublishJobStatus(req.body?.status || req.query?.status);
  const details = sanitizePublishJobDetails(req.body?.details);
  if (!publishId) {
    res.status(400).send('Missing publishId');
    return;
  }

  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  const eventTimestamp = admin.firestore.Timestamp.now();
  const jobRef = db.collection('sitePublishJobs').doc(publishId);
  const historyRef = db.collection('sitePublishHistory').doc(publishId);
  const event = {
    at: eventTimestamp,
    status,
    details: details ?? null
  };

  const terminal = new Set([
    'deploy_succeeded',
    'deploy_failed',
    'no_changes',
    'failed'
  ]);
  const update = {
    publishId,
    status,
    updatedAt: timestamp,
    events: admin.firestore.FieldValue.arrayUnion(event)
  };
  if (terminal.has(status)) {
    update.completedAt = timestamp;
  }

  const historyPatch = {};
  if (status === 'commit_pushed' && details && typeof details.commitSha === 'string') {
    historyPatch.commitSha = details.commitSha.trim();
  }
  if (status === 'deploy_succeeded' && details && typeof details.pageUrl === 'string') {
    historyPatch.pageUrl = details.pageUrl.trim();
  }

  const ops = [jobRef.set(update, { merge: true })];
  if (Object.keys(historyPatch).length) {
    ops.push(
      historyRef.set(
        {
          ...historyPatch,
          updatedAt: timestamp
        },
        { merge: true }
      )
    );
  }
  if (status === 'deploy_succeeded' || status === 'no_changes') {
    const historySnap = await historyRef.get();
    const history = historySnap.exists ? historySnap.data() : {};
    if (history?.publishedSnapshot) {
      const statusRef = db.collection('admin').doc('sitePublish');
      ops.push(statusRef.set(
        {
          lastPublishedAt: timestamp,
          lastPublishedBy: history.requestedBy || null,
          lastRequestedBy: history.requestedBy || null,
          lastRequestedByUid: history.requestedByUid || null,
          lastChangeReference: history.latestChangeAt || null,
          lastPublishHistoryId: publishId,
          lastPublishDryRun: Boolean(history.dryRun)
        },
        { merge: true }
      ));
      // Replace the full snapshot map so documents removed from staging are also removed from the baseline.
      ops.push(statusRef.update({ lastPublishedSnapshot: history.publishedSnapshot }));
    }
  }
  await Promise.all(ops);
  res.status(200).json({ ok: true });
});

const RATE_LIMIT = 2;
const RATE_LIMIT_COLLECTION = 'emailRateLimits';

const transporter = nodemailer.createTransport({
  host: smtpHost,
  port: smtpPort,
  secure: smtpPort === 465,
  auth: {
    user: smtpUser,
    pass: smtpPass
  }
});

const currentDateKey = () => new Date().toISOString().slice(0, 10);

function normalizeRateLimitKey(key) {
  return String(key).replace(/[^\w.-]/g, '_');
}

async function verifyCaptcha(token, remoteIp) {
  if (skipCaptcha) {
    return;
  }
  if (!recaptchaSecret) {
    throw new functions.https.HttpsError('failed-precondition', 'reCAPTCHA secret is not configured.');
  }
  if (!token) {
    throw new functions.https.HttpsError('invalid-argument', 'Missing captchaToken.');
  }

  const params = new URLSearchParams();
  params.append('secret', recaptchaSecret);
  params.append('response', token);
  if (remoteIp) {
    params.append('remoteip', remoteIp);
  }

  let response;
  try {
    response = await fetch('https://www.google.com/recaptcha/api/siteverify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params
    });
  } catch (err) {
    console.error('captcha verify request failed', err);
    throw new functions.https.HttpsError('internal', 'Captcha verification failed.');
  }

  if (!response.ok) {
    const text = await response.text();
    console.error('captcha verify non-200', text);
    throw new functions.https.HttpsError('internal', 'Captcha verification failed.');
  }

  const result = await response.json();
  if (!result.success) {
    const codes = result['error-codes']?.join(', ') || 'unknown';
    throw new functions.https.HttpsError('permission-denied', `Captcha rejected (${codes}).`);
  }
}

async function enforceRateLimit(key, type) {
  if (!key) {
    return;
  }

  const ref = db
    .collection(RATE_LIMIT_COLLECTION)
    .doc(`${type}_${normalizeRateLimitKey(key)}`);
  const today = currentDateKey();

  await db.runTransaction(async (tx) => {
    const snapshot = await tx.get(ref);
    const data = snapshot.data();

    if (!snapshot.exists || data?.date !== today) {
      tx.set(ref, { date: today, count: 1 });
      return;
    }

    const count = data?.count || 0;
    if (count >= RATE_LIMIT) {
      throw new functions.https.HttpsError(
        'resource-exhausted',
        type === 'user'
          ? 'Daily email quota reached for this account.'
          : 'Daily email quota reached for this IP address.'
      );
    }

    tx.update(ref, { count: count + 1 });
  });
}

function extractIp(rawRequest = {}) {
  const forwarded = rawRequest.headers?.['x-forwarded-for'];
  if (forwarded) {
    return forwarded.split(',')[0].trim();
  }
  return rawRequest.ip || rawRequest.headers?.['fastly-client-ip'] || null;
}

function toRecipientList(value) {
  if (Array.isArray(value)) {
    return value;
  }
  if (typeof value === 'string') {
    return value.split(',').map((item) => item.trim());
  }
  return [];
}

function ensurePublicAllowedRecipients(recipients) {
  if (!publicAllowedRecipients.length) {
    throw new functions.https.HttpsError(
      'failed-precondition',
      'PUBLIC_ALLOWED_RECIPIENTS is not configured.'
    );
  }

  const disallowed = recipients.find(
    (email) => !publicAllowedRecipients.includes(email.toLowerCase())
  );
  if (disallowed) {
    throw new functions.https.HttpsError(
      'permission-denied',
      'Authentication required to contact this recipient.'
    );
  }
}

function extractEmailAddress(value) {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  const match = trimmed.match(/<([^>]+)>/);
  const email = match ? match[1] : trimmed;
  return email || null;
}

function getConfirmationLocale(sender) {
  if (!sender) {
    return 'en';
  }
  const lower = sender.toLowerCase();
  if (lower.includes(frenchSenderKeyword)) {
    return 'fr';
  }
  if (lower.includes(englishSenderKeyword)) {
    return 'en';
  }
  return 'en';
}

function buildConfirmationMessage(locale, subject) {
  const safeSubject = subject || 'votre demande';
  if (locale === 'fr') {
    return {
      subject: `Nous avons bien reçu votre demande : ${safeSubject}`,
      html: `<p>Bonjour,</p><p>Nous avons bien reçu votre demande concernant « ${safeSubject} ».</p><p>Notre équipe vous répondra sous peu.</p><p>Merci,</p><p>Site</p>`,
      text: `Bonjour,\n\nNous avons bien reçu votre demande concernant « ${safeSubject} ».\nNotre équipe vous répondra sous peu.\n\nMerci,\nSite`
    };
  }
  return {
    subject: `We received your request: ${safeSubject}`,
    html: `<p>Hello,</p><p>We received your request about “${safeSubject}”.</p><p>Our team will follow up soon.</p><p>Thanks,</p><p>Site</p>`,
    text: `Hello,\n\nWe received your request about “${safeSubject}”.\nOur team will follow up soon.\n\nThanks,\nSite`
  };
}

async function sendConfirmationEmail({ sender, replyTo, subject }) {
  const recipient = extractEmailAddress(replyTo);
  if (!recipient) {
    return;
  }
  const locale = getConfirmationLocale(sender);
  const message = buildConfirmationMessage(locale, subject);
  try {
    await transporter.sendMail({
      from: sender,
      to: recipient,
      subject: message.subject,
      html: message.html,
      text: message.text
    });
  } catch (err) {
    console.error('sendEmail confirmation error', err);
  }
}

function generateConfirmationCode() {
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const random = crypto.randomBytes(3).toString('hex').toUpperCase();
  return `FC-${date}-${random}`;
}

function normalizePublishValue(value) {
  if (value === null || value === undefined) return value ?? null;
  if (typeof value?.toDate === 'function') {
    return value.toDate().toISOString();
  }
  if (Array.isArray(value)) {
    return value.map((item) => normalizePublishValue(item));
  }
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => !AUDIT_FIELDS.has(key))
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, nested]) => [key, normalizePublishValue(nested)])
    );
  }
  return value;
}

async function buildPublishSnapshot() {
  const snapshot = {};
  for (const collectionName of PUBLISH_COLLECTIONS) {
    const docs = await db.collection(collectionName).get();
    snapshot[collectionName] = {};
    docs.docs
      .sort((left, right) => left.id.localeCompare(right.id))
      .forEach((docSnap) => {
        snapshot[collectionName][docSnap.id] = normalizePublishValue(docSnap.data() || {});
      });
  }
  return snapshot;
}

function stableStringify(value) {
  return JSON.stringify(normalizePublishValue(value));
}

function buildPublishFieldDiffs(before = {}, after = {}) {
  const fields = Array.from(new Set([...Object.keys(before || {}), ...Object.keys(after || {})]))
    .filter((field) => stableStringify(before?.[field]) !== stableStringify(after?.[field]))
    .sort();
  return fields.map((field) => ({
    field,
    before: before?.[field] === undefined ? null : before[field],
    after: after?.[field] === undefined ? null : after[field]
  }));
}

function summarizePublishDiff(previousSnapshot = {}, nextSnapshot = {}) {
  const changes = [];
  const collections = Array.from(
    new Set([...Object.keys(previousSnapshot || {}), ...Object.keys(nextSnapshot || {})])
  ).sort();
  collections.forEach((collectionName) => {
    const beforeDocs = previousSnapshot?.[collectionName] || {};
    const afterDocs = nextSnapshot?.[collectionName] || {};
    const ids = Array.from(new Set([...Object.keys(beforeDocs), ...Object.keys(afterDocs)])).sort();
    ids.forEach((id) => {
      const before = beforeDocs[id];
      const after = afterDocs[id];
      if (before === undefined) {
        changes.push({
          collection: collectionName,
          id,
          type: 'added',
          after: after || {},
          fields: Object.keys(after || {}).sort(),
          fieldDiffs: buildPublishFieldDiffs({}, after || {})
        });
        return;
      }
      if (after === undefined) {
        changes.push({
          collection: collectionName,
          id,
          type: 'removed',
          before: before || {},
          fields: Object.keys(before || {}).sort(),
          fieldDiffs: buildPublishFieldDiffs(before || {}, {})
        });
        return;
      }
      if (stableStringify(before) === stableStringify(after)) return;
      const fieldDiffs = buildPublishFieldDiffs(before, after);
      changes.push({
        collection: collectionName,
        id,
        type: 'changed',
        fields: fieldDiffs.map((entry) => entry.field),
        fieldDiffs
      });
    });
  });
  return {
    total: changes.length,
    added: changes.filter((change) => change.type === 'added').length,
    changed: changes.filter((change) => change.type === 'changed').length,
    removed: changes.filter((change) => change.type === 'removed').length,
    changes
  };
}

async function getPublishPreview() {
  const statusSnap = await db.collection('admin').doc('sitePublish').get();
  const previousSnapshot = statusSnap.data()?.lastPublishedSnapshot || {};
  const nextSnapshot = await buildPublishSnapshot();
  return {
    previousSnapshot,
    nextSnapshot,
    diff: summarizePublishDiff(previousSnapshot, nextSnapshot)
  };
}

async function upsertWebsiteClient({
  tenantName,
  tenantPhone,
  tenantEmail,
  tenantEmailLower,
  tenantAddress,
  tenantCity,
  tenantProvince,
  tenantPostal,
  formLanguage
}) {
  if (!tenantEmailLower || tenantEmailLower.includes('/')) {
    throw new functions.https.HttpsError('invalid-argument', 'Invalid email address.');
  }
  const clientId = tenantEmailLower;
  const clientDoc = db.collection('clients').doc(clientId);
  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  const clientPayload = {
    name: tenantName,
    phone: tenantPhone,
    email: tenantEmail,
    emailLower: tenantEmailLower,
    preferredLanguage: normalizeLanguage(formLanguage),
    address: tenantAddress,
    city: tenantCity,
    province: tenantProvince,
    postalCode: tenantPostal,
    active: true,
    nonStorageClient: false,
    notes: `Submitted via Entrepot website${formLanguage ? ` (${formLanguage})` : ''}`,
    updatedAt: timestamp,
    updatedBy: null
  };

  await db.runTransaction(async (transaction) => {
    const snapshot = await transaction.get(clientDoc);
    if (snapshot.exists) {
      transaction.set(clientDoc, clientPayload, { merge: true });
      return;
    }
    transaction.set(clientDoc, {
      ...clientPayload,
      createdAt: timestamp,
      createdBy: null
    });
  });

  return clientId;
}

function formatStorageRequestLine(request, locale = 'en') {
  const parts = [];
  const typeLabel = request.vehicleTypeLabel || request.vehicleType || 'Vehicle';
  if (typeLabel) parts.push(typeLabel);
  if (request.vehicleLength) {
    parts.push(locale === 'fr' ? `${request.vehicleLength} pi` : `${request.vehicleLength} ft`);
  }
  if (request.vehiclePlate) {
    const suffix = request.vehicleProv ? ` (${request.vehicleProv})` : '';
    parts.push(`${request.vehiclePlate}${suffix}`);
  } else if (request.vehicleProv) {
    parts.push(request.vehicleProv);
  }
  const addons = [];
  if (request.battery) addons.push(locale === 'fr' ? 'Charge de batterie' : 'Battery charging');
  if (request.propane) addons.push(locale === 'fr' ? 'Entreposage propane' : 'Propane storage');
  if (addons.length) {
    parts.push(addons.join(locale === 'fr' ? ' + ' : ' + '));
  }
  const seasonLabel = request.seasonLabel || request.season;
  if (seasonLabel) {
    parts.push(
      locale === 'fr' ? `Saison : ${seasonLabel}` : `Season: ${seasonLabel}`
    );
  }
  return parts.filter(Boolean).join(' • ');
}

function buildStorageRequestConfirmationMessage(locale, confirmationCode, tenant, requests) {
  const isFrench = locale === 'fr';
  const intro = isFrench
    ? `Bonjour ${tenant.tenantName || ''},`
    : `Hello ${tenant.tenantName || ''},`;
  const title = isFrench
    ? 'Nous avons bien reçu votre demande d’entreposage.'
    : 'We received your storage request.';
  const confirmationLine = isFrench
    ? `Numéro de confirmation : ${confirmationCode}`
    : `Confirmation number: ${confirmationCode}`;
  const listLabel = isFrench
    ? 'Demandes reçues :'
    : 'Requests received:';
  const items = requests.map((request, index) => {
    const line = formatStorageRequestLine(request, locale);
    return `${index + 1}. ${line}`;
  });
  const outro = isFrench
    ? 'Nous vous contacterons sous peu par courriel pour les prochaines étapes.'
    : 'We will contact you by email shortly with next steps.';
  const text = [intro, '', title, confirmationLine, '', listLabel, ...items, '', outro]
    .filter(Boolean)
    .join('\n');
  const htmlItems = requests
    .map((request, index) => `<li><strong>${index + 1}.</strong> ${formatStorageRequestLine(request, locale)}</li>`)
    .join('');
  const html = `
    <p>${intro}</p>
    <p>${title}</p>
    <p><strong>${confirmationLine}</strong></p>
    <p>${listLabel}</p>
    <ol>${htmlItems}</ol>
    <p>${outro}</p>
  `;
  const subject = isFrench
    ? `Confirmation ${confirmationCode} – demande d’entreposage`
    : `Storage request confirmation ${confirmationCode}`;
  return { subject, text, html };
}

async function sendStorageRequestConfirmationEmail({ to, locale, confirmationCode, tenant, requests }) {
  if (!to) return;
  const sender = defaultFrom;
  if (!sender) {
    throw new functions.https.HttpsError('failed-precondition', 'SMTP default FROM is not configured.');
  }
  const message = buildStorageRequestConfirmationMessage(locale, confirmationCode, tenant, requests);
  await transporter.sendMail({
    from: sender,
    to,
    subject: message.subject,
    html: message.html,
    text: message.text
  });
}

async function sendStorageRequestNotificationEmail({ confirmationCode, tenant, requests, requestIds = [], clientId = '' }) {
  const to = frenchSenderKeyword;
  if (!to) return;
  const sender = defaultFrom;
  if (!sender) {
    throw new functions.https.HttpsError('failed-precondition', 'SMTP default FROM is not configured.');
  }
  const tenantAddress = [
    tenant.tenantAddress,
    tenant.tenantCity,
    tenant.tenantProvince,
    tenant.tenantPostal
  ].filter(Boolean).join(', ');
  const requestLines = requests.map((request, index) => {
    const id = requestIds[index] || '';
    const prefix = id ? `${index + 1}. ${id} - ` : `${index + 1}. `;
    return `${prefix}${formatStorageRequestLine(request, 'fr')}`;
  });
  const subject = `Nouvelle demande d'entreposage - ${confirmationCode}`;
  const text = [
    'Une nouvelle demande d’entreposage a été soumise.',
    '',
    `Confirmation : ${confirmationCode}`,
    `Client : ${tenant.tenantName || ''}`,
    `Courriel : ${tenant.email || ''}`,
    `Téléphone : ${tenant.tenantPhone || ''}`,
    `Adresse : ${tenantAddress}`,
    clientId ? `Client ID : ${clientId}` : '',
    '',
    'Demandes :',
    ...requestLines
  ].filter(Boolean).join('\n');
  const htmlItems = requestLines
    .map((line) => `<li>${escapeHtml(line)}</li>`)
    .join('');
  const bodyHtml = `
    <p>Une nouvelle demande d’entreposage a été soumise.</p>
    <table style="border-collapse:collapse;width:100%;margin:12px 0;">
      <tr><th style="text-align:left;padding:6px 8px;border:1px solid #e2e8f0;background:#f8fafc;">Confirmation</th><td style="padding:6px 8px;border:1px solid #e2e8f0;">${escapeHtml(confirmationCode)}</td></tr>
      <tr><th style="text-align:left;padding:6px 8px;border:1px solid #e2e8f0;background:#f8fafc;">Client</th><td style="padding:6px 8px;border:1px solid #e2e8f0;">${escapeHtml(tenant.tenantName || '')}</td></tr>
      <tr><th style="text-align:left;padding:6px 8px;border:1px solid #e2e8f0;background:#f8fafc;">Courriel</th><td style="padding:6px 8px;border:1px solid #e2e8f0;">${escapeHtml(tenant.email || '')}</td></tr>
      <tr><th style="text-align:left;padding:6px 8px;border:1px solid #e2e8f0;background:#f8fafc;">Téléphone</th><td style="padding:6px 8px;border:1px solid #e2e8f0;">${escapeHtml(tenant.tenantPhone || '')}</td></tr>
      <tr><th style="text-align:left;padding:6px 8px;border:1px solid #e2e8f0;background:#f8fafc;">Adresse</th><td style="padding:6px 8px;border:1px solid #e2e8f0;">${escapeHtml(tenantAddress)}</td></tr>
      ${clientId ? `<tr><th style="text-align:left;padding:6px 8px;border:1px solid #e2e8f0;background:#f8fafc;">Client ID</th><td style="padding:6px 8px;border:1px solid #e2e8f0;">${escapeHtml(clientId)}</td></tr>` : ''}
    </table>
    <p><strong>Demandes :</strong></p>
    <ol>${htmlItems}</ol>
  `;
  const html = wrapTrackerEmailHtml({ locale: 'fr', subject, body: text, bodyHtml });
  await transporter.sendMail({ from: sender, to, subject, html, text });
}

function normalizeLanguage(value) {
  return String(value || '').toLowerCase() === 'fr' ? 'fr' : 'en';
}

function getTorontoDateToken(date = new Date()) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Toronto',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
  const parts = formatter.formatToParts(date);
  const lookup = Object.fromEntries(parts.filter((part) => part.type !== 'literal').map((part) => [part.type, part.value]));
  return `${lookup.year}${lookup.month}${lookup.day}`;
}

function sanitizeIdToken(value) {
  return String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}

function shortIdToken(value, length = 6, fallback = 'XXXXXX') {
  const normalized = sanitizeIdToken(value);
  if (!normalized) return fallback.slice(0, length);
  return normalized.slice(0, length).padEnd(length, 'X');
}

function getStorageConfirmationCc(locale) {
  return locale === 'fr' ? 'entrepot@as-colle.com' : 'warehouse@as-colle.com';
}

function escapeHtml(value) {
  return String(value || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function getLocalizedTemplateField(template, fieldName, locale) {
  const value = template?.[fieldName];
  if (!value) return '';
  if (typeof value === 'string') return value;
  return value[locale] || value.en || value.fr || '';
}

function normalizeTemplateSections(templateText, context) {
  return String(templateText || '').replace(
    /{{\s*([#^/])\s*([\w.]+)\s*}}/g,
    (match, mode, key) => {
      if (!Object.prototype.hasOwnProperty.call(context?.__sections || {}, key)) return match;
      return `{{${mode}__sections.${key}}}`;
    }
  );
}

function fillLegacyTemplateTokens(templateText, context) {
  return String(templateText || '').replace(
    /{\s*([\w.]+)\s*}|\[\[\s*([\w.]+)\s*\]\]|%%\s*([\w.]+)\s*%%/g,
    (match, braceKey, bracketKey, percentKey) => {
      const key = braceKey || bracketKey || percentKey;
      const value = context[key];
      return value === null || value === undefined ? '' : String(value);
    }
  );
}

function renderMustacheTemplate(templateText, context, options = {}) {
  const escapeValues = options.escapeValues !== false;
  const normalized = normalizeTemplateSections(templateText, context);
  if (escapeValues) return Mustache.render(normalized, context);
  const previousEscape = Mustache.escape;
  Mustache.escape = (value) => String(value ?? '');
  try {
    return Mustache.render(normalized, context);
  } finally {
    Mustache.escape = previousEscape;
  }
}

function fillTemplateText(templateText, context) {
  const rendered = renderMustacheTemplate(templateText, context, { escapeValues: false });
  return fillLegacyTemplateTokens(rendered, context);
}

function fillTemplateHtml(templateText, context, htmlContext = {}) {
  const htmlTokens = [];
  const tokenizedTemplate = normalizeTemplateSections(templateText, context).replace(
    /{{\s*([\w.]+)\s*}}/g,
    (match, key) => {
      if (Object.prototype.hasOwnProperty.call(htmlContext, key)) {
        const token = `__HTML_TOKEN_${htmlTokens.length}__`;
        htmlTokens.push(htmlContext[key] || '');
        return token;
      }
      return match;
    }
  );
  const rendered = fillLegacyTemplateTokens(renderMustacheTemplate(tokenizedTemplate, context), context);
  const withPlaceholders = rendered.replace(/\n/g, '<br>');
  return htmlTokens.reduce((html, value, index) => html.replace(`__HTML_TOKEN_${index}__`, value), withPlaceholders);
}

function formatMoney(value) {
  return new Intl.NumberFormat('en-CA', {
    style: 'currency',
    currency: 'CAD'
  }).format(Number(value) || 0);
}

function formatDateValue(value, locale = 'en') {
  if (!value) return '';
  const date = typeof value.toDate === 'function' ? value.toDate() : new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleDateString(locale === 'fr' ? 'fr-CA' : 'en-CA');
}

function getRequestStorageLocation(request, locale = 'en') {
  const isOutdoor = request?.storageLocation === 'outdoor';
  if (locale === 'fr') return isOutdoor ? 'Extérieur' : 'Intérieur';
  return isOutdoor ? 'Outdoor' : 'Indoor';
}

function estimateDepositForAmount(amount) {
  return Number.isFinite(amount) && amount > 400 ? 100 : 50;
}

function accountSupportsCash(account) {
  return account?.type === 'cash' || account?.type === 'cash_entity';
}

function accountSupportsEntity(account) {
  return account?.type === 'entity' || account?.type === 'cash_entity';
}

function normalizeAccountName(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

async function resolveFermeColleLedgerAccounts() {
  const snapshot = await db.collection('accounts').get();
  const accounts = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
  const cashAccounts = accounts.filter(accountSupportsCash);
  const entityAccounts = accounts.filter(accountSupportsEntity);
  const fermeColleEntity =
    entityAccounts.find((account) => normalizeAccountName(account.name) === 'ferme colle') ||
    entityAccounts.find((account) => normalizeAccountName(account.name).includes('ferme colle')) ||
    entityAccounts.find((account) => account.defaultEntity);
  const defaultCash =
    cashAccounts.find((account) => account.defaultCash) ||
    (cashAccounts.length === 1 ? cashAccounts[0] : null);
  if (!fermeColleEntity) {
    throw new functions.https.HttpsError('failed-precondition', 'Ferme Colle entity account was not found.');
  }
  if (!defaultCash) {
    throw new functions.https.HttpsError('failed-precondition', 'Default cash account was not found.');
  }
  return { entityAccount: fermeColleEntity, cashAccount: defaultCash };
}

function buildDepositVehicleDescription(requests, locale, pricingData, options = {}) {
  const vehicleLines = requests
    .map((request) => {
      const vehicle = request.vehicle || {};
      const type = getVehicleTypeLabelForEmail(vehicle, locale, pricingData);
      const model = [vehicle.brand, vehicle.model].filter(Boolean).join(' ');
      if (type && model) return `${type}: ${model}`;
      return type || model || '';
    })
    .filter(Boolean)
    .join('\n');
  const reference = String(options.interacReference || '').trim();
  return [
    reference ? `Interac reference: ${reference}` : '',
    'List of vehicle',
    vehicleLines
  ]
    .filter(Boolean)
    .join('\n');
}

function requestAmountValue(request) {
  if (request?.contractAmount !== null && request?.contractAmount !== undefined && request?.contractAmount !== '') {
    const amount = Number(request.contractAmount);
    if (Number.isFinite(amount)) return amount;
  }
  if (request?.estimate?.amount !== null && request?.estimate?.amount !== undefined && request?.estimate?.amount !== '') {
    const amount = Number(request.estimate.amount);
    if (Number.isFinite(amount)) return amount;
  }
  return 0;
}

function getOfferVehicleTypesForEmail(offer, templateLookup) {
  if (Array.isArray(offer?.vehicleTypes) && offer.vehicleTypes.length) return offer.vehicleTypes;
  const template = offer?.templateId ? templateLookup.get(offer.templateId) : null;
  if (Array.isArray(template?.vehicleTypes) && template.vehicleTypes.length) return template.vehicleTypes;
  return [];
}

function computeOfferPriceForEmail(offer, lengthFeet) {
  if (!offer?.price) return null;
  if (offer.price.mode === 'flat') {
    const amount = Number(offer.price.amount);
    return Number.isFinite(amount) ? amount : null;
  }
  if (offer.price.mode === 'perFoot') {
    const length = Number(lengthFeet);
    if (!Number.isFinite(length)) return null;
    const rate = Number(offer.price.rate || 0);
    const minimum = Number(offer.price.minimum || 0);
    return Math.max(length * rate, minimum);
  }
  return null;
}

function lengthMatchesRangeForEmail(lengthFeet, range) {
  if (!range) return true;
  const length = Number(lengthFeet);
  if (!Number.isFinite(length)) return false;
  if (typeof range.min === 'number' && length < range.min) return false;
  if (typeof range.max === 'number' && length > range.max) return false;
  return true;
}

async function loadStoragePricingData() {
  const [offerSnap, templateSnap, addOnSnap, seasonSnap, vehicleTypeSnap] = await Promise.all([
    db.collection('storageOffers').get(),
    db.collection('offerTemplates').get(),
    db.collection('storageSeasonAddOns').get(),
    db.collection('storageSeasons').get(),
    db.collection('vehicleTypes').get()
  ]);
  return {
    offers: offerSnap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() })),
    templateLookup: new Map(templateSnap.docs.map((docSnap) => [docSnap.id, docSnap.data()])),
    addOnPrices: addOnSnap.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() })),
    seasonLookup: new Map(seasonSnap.docs.map((docSnap) => [docSnap.id, { id: docSnap.id, ...docSnap.data() }])),
    vehicleTypeLookup: new Map(vehicleTypeSnap.docs.map((docSnap) => [docSnap.id, { id: docSnap.id, ...docSnap.data() }]))
  };
}

function formatSeasonLabelForEmail(seasonId, locale = 'en', pricingData = {}) {
  const normalizedLocale = normalizeLanguage(locale);
  const season = pricingData?.seasonLookup?.get?.(seasonId);
  return (
    season?.label?.[normalizedLocale] ||
    season?.name?.[normalizedLocale] ||
    season?.label?.en ||
    season?.name?.en ||
    season?.label?.fr ||
    season?.name?.fr ||
    seasonId ||
    ''
  );
}

function deriveSeasonCodeForEmail(seasonId, pricingData = {}) {
  const season = pricingData?.seasonLookup?.get?.(seasonId);
  const seasonLabel =
    season?.label?.en ||
    season?.label?.fr ||
    season?.name?.en ||
    season?.name?.fr ||
    seasonId;
  const normalizedLabel = String(seasonLabel || '').toLowerCase();
  const yearMatches = String(seasonLabel || '').match(/20\d{2}/g) || [];
  if (normalizedLabel.includes('winter') || normalizedLabel.includes('hiver')) {
    if (yearMatches.length >= 2) return `W${yearMatches[0].slice(2)}${yearMatches[1].slice(2)}`;
    if (yearMatches.length === 1) {
      const start = Number(yearMatches[0]);
      if (Number.isFinite(start)) return `W${String(start).slice(2)}`;
    }
  }
  if (normalizedLabel.includes('summer') || normalizedLabel.includes('ete') || normalizedLabel.includes('été')) {
    if (yearMatches.length >= 1) return `S${yearMatches[0]}`;
  }
  if (yearMatches.length >= 2) return `W${yearMatches[0].slice(2)}${yearMatches[1].slice(2)}`;
  if (yearMatches.length === 1) return `S${yearMatches[0]}`;
  return 'S0000';
}

function buildStorageReceiptIdBase(request, pricingData = {}) {
  const seasonCode = deriveSeasonCodeForEmail(request?.season || '', pricingData);
  const rawCaseId = resolveEmailCaseId(request);
  const caseClientPart = rawCaseId.includes('__') ? rawCaseId.split('__')[1] : '';
  const caseIdSource = caseClientPart || request?.clientId || rawCaseId || 'CASEXX';
  const caseIdShort = shortIdToken(caseIdSource, 6, 'CASEXX');
  return `R-${seasonCode}-${caseIdShort}-${getTorontoDateToken(new Date())}`;
}

async function allocateStorageReceiptId(request, pricingData = {}) {
  const baseId = buildStorageReceiptIdBase(request, pricingData);
  for (let index = 0; index < 100; index += 1) {
    const candidate = index === 0 ? baseId : `${baseId}-${String(index + 1).padStart(2, '0')}`;
    const snap = await db.collection('storageReceipts').doc(candidate).get();
    if (!snap.exists) return candidate;
  }
  throw new functions.https.HttpsError('resource-exhausted', `Unable to allocate receipt id for ${baseId}.`);
}

function findVehicleTypeForEmail(identifier, pricingData = {}) {
  if (!identifier) return null;
  const direct = pricingData?.vehicleTypeLookup?.get?.(identifier);
  if (direct) return direct;
  const values = pricingData?.vehicleTypeLookup?.values?.() || [];
  for (const entry of values) {
    if (
      entry?.id === identifier ||
      entry?.value === identifier ||
      entry?.slug === identifier ||
      (Array.isArray(entry?.legacyValues) && entry.legacyValues.includes(identifier))
    ) {
      return entry;
    }
  }
  return null;
}

function getVehicleTypeLabelForEmail(vehicle = {}, locale = 'en', pricingData = {}) {
  const normalizedLocale = normalizeLanguage(locale);
  const identifier = vehicle.type || vehicle.typeLabel || '';
  const entry = findVehicleTypeForEmail(identifier, pricingData);
  const defaultLabels =
    DEFAULT_VEHICLE_TYPE_LABELS[entry?.id] ||
    DEFAULT_VEHICLE_TYPE_LABELS[vehicle.type] ||
    DEFAULT_VEHICLE_TYPE_LABELS[vehicle.typeLabel] ||
    null;
  return (
    entry?.labels?.[normalizedLocale] ||
    defaultLabels?.[normalizedLocale] ||
    entry?.labels?.en ||
    defaultLabels?.en ||
    entry?.value ||
    vehicle.typeLabel ||
    vehicle.type ||
    ''
  );
}

function resolveRequestAmountForEmail(request, pricingData) {
  if (request?.contractAmount !== null && request?.contractAmount !== undefined && request?.contractAmount !== '') {
    const amount = Number(request.contractAmount);
    if (Number.isFinite(amount)) return amount;
  }
  const seasonId = request?.season || '';
  const vehicleType = request?.vehicle?.type || '';
  const storageLocation = request?.storageLocation === 'outdoor' ? 'outdoor' : 'indoor';
  const offers = (pricingData?.offers || [])
    .filter((offer) => offer.seasonId === seasonId)
    .filter((offer) => {
      const types = getOfferVehicleTypesForEmail(offer, pricingData.templateLookup);
      if (storageLocation === 'outdoor') return !types.length && offer?.price?.mode === 'flat';
      return types.includes(vehicleType);
    });
  const selectedOffer =
    offers.find((offer) => lengthMatchesRangeForEmail(request?.vehicle?.lengthFeet, offer.lengthRange)) ||
    offers[offers.length - 1] ||
    null;
  const pricedAmount = computeOfferPriceForEmail(selectedOffer, request?.vehicle?.lengthFeet);
  if (Number.isFinite(pricedAmount)) {
    let total = pricedAmount;
    if (request?.addons?.battery) total += getAddOnPriceForEmail('battery', seasonId, pricingData);
    if (request?.addons?.propane) total += getAddOnPriceForEmail('propane', seasonId, pricingData);
    return total;
  }
  return requestAmountValue(request);
}

function getAddOnPriceForEmail(code, seasonId, pricingData) {
  const match = (pricingData?.addOnPrices || []).find(
    (entry) => (entry.code === code || entry.addonId === code || entry.id === code) && entry.seasonId === seasonId
  );
  if (match) {
    const amount = Number(match.price);
    return Number.isFinite(amount) ? amount : 0;
  }
  const global = (pricingData?.addOnPrices || []).find(
    (entry) => (entry.code === code || entry.addonId === code || entry.id === code) && !entry.seasonId
  );
  const amount = Number(global?.price);
  return Number.isFinite(amount) ? amount : 0;
}

function buildEmailRequestLine(request, locale = 'en', pricingData = {}) {
  const vehicle = request?.vehicle || {};
  const parts = [
    getVehicleTypeLabelForEmail(vehicle, locale, pricingData),
    vehicle.brand || '',
    vehicle.model || '',
    vehicle.lengthFeet ? (locale === 'fr' ? `${vehicle.lengthFeet} pi` : `${vehicle.lengthFeet} ft`) : '',
    vehicle.plate || ''
  ].filter(Boolean);
  return parts.join(' - ');
}

function buildVehicleDetailRows(request, locale = 'en', pricingData = {}) {
  const vehicle = request?.vehicle || {};
  const labels = locale === 'fr'
    ? {
        vehicle: 'Vehicule',
        storageLocation: 'Lieu',
        brand: 'Marque',
        model: 'Modele',
        colour: 'Couleur',
        length: 'Longueur',
        year: 'Annee',
        plate: 'Plaque',
        insuranceCompany: 'Assureur',
        insurancePolicy: 'Police',
        insuranceExpiration: 'Expiration'
      }
    : {
        vehicle: 'Vehicle',
        storageLocation: 'Location',
        brand: 'Brand',
        model: 'Model',
        colour: 'Colour',
        length: 'Length',
        year: 'Year',
        plate: 'Plate',
        insuranceCompany: 'Insurance company',
        insurancePolicy: 'Policy number',
        insuranceExpiration: 'Expiration'
      };
  return [
    [labels.vehicle, getVehicleTypeLabelForEmail(vehicle, locale, pricingData)],
    [labels.storageLocation, getRequestStorageLocation(request, locale)],
    [labels.brand, vehicle.brand || ''],
    [labels.model, vehicle.model || ''],
    [labels.colour, vehicle.colour || ''],
    [labels.length, vehicle.lengthFeet ? `${vehicle.lengthFeet} ${locale === 'fr' ? 'pi' : 'ft'}` : ''],
    [labels.year, vehicle.year || ''],
    [labels.plate, [vehicle.plate, vehicle.province].filter(Boolean).join(' ')],
    [labels.insuranceCompany, request?.insuranceCompany || ''],
    [labels.insurancePolicy, request?.policyNumber || ''],
    [labels.insuranceExpiration, formatDateValue(request?.insuranceExpiration, locale)]
  ];
}

function buildTenantInfoText(client, locale = 'en') {
  const address = [client.address, client.city, client.province, client.postalCode].filter(Boolean).join(', ');
  if (locale === 'fr') {
    return [
      `Nom : ${client.name || ''}`,
      `Courriel : ${client.email || ''}`,
      `Telephone : ${client.phone || ''}`,
      `Adresse : ${address}`
    ].join('\n');
  }
  return [
    `Name: ${client.name || ''}`,
    `Email: ${client.email || ''}`,
    `Phone: ${client.phone || ''}`,
    `Address: ${address}`
  ].join('\n');
}

function buildVehicleInfoText(requests, locale = 'en', pricingData = {}) {
  return (requests || [])
    .map((request, index) => {
      const prefix = requests.length > 1 ? `${index + 1}. ` : '';
      return buildVehicleDetailRows(request, locale, pricingData)
        .map(([label, value], rowIndex) => `${rowIndex === 0 ? prefix : ''}${label}: ${value || ''}`)
        .join('\n');
    })
    .join('\n\n');
}

function buildVehicleInfoHtml(requests, locale = 'en', pricingData = {}) {
  const chunkPairs = (rows) => {
    const chunks = [];
    for (let index = 0; index < rows.length; index += 2) {
      chunks.push([rows[index], rows[index + 1] || null]);
    }
    return chunks;
  };
  return (requests || [])
    .map((request, index) => {
      const heading = requests.length > 1
        ? `<p style="margin:16px 0 8px;font-weight:700;">${escapeHtml(locale === 'fr' ? `Vehicule ${index + 1}` : `Vehicle ${index + 1}`)}</p>`
        : '';
      const rows = chunkPairs(buildVehicleDetailRows(request, locale, pricingData))
        .map(([left, right]) => `
          <tr>
            <th style="width:18%;text-align:left;vertical-align:top;padding:7px 10px;border:1px solid #dbe3ef;background:#f8fafc;color:#334155;font-weight:700;">${escapeHtml(left[0])}</th>
            <td style="width:32%;vertical-align:top;padding:7px 10px;border:1px solid #dbe3ef;color:#0f172a;">${escapeHtml(left[1] || '') || '&nbsp;'}</td>
            <th style="width:18%;text-align:left;vertical-align:top;padding:7px 10px;border:1px solid #dbe3ef;background:#f8fafc;color:#334155;font-weight:700;">${right ? escapeHtml(right[0]) : '&nbsp;'}</th>
            <td style="width:32%;vertical-align:top;padding:7px 10px;border:1px solid #dbe3ef;color:#0f172a;">${right ? escapeHtml(right[1] || '') || '&nbsp;' : '&nbsp;'}</td>
          </tr>
        `)
        .join('');
      return `${heading}<table role="presentation" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;margin:0 0 18px;font-size:14px;">${rows}</table>`;
    })
    .join('');
}

function resolveEmailCaseId(request) {
  const existing = String(request?.caseId || '').trim();
  if (existing) return existing;
  const season = String(request?.season || 'no-season');
  const clientId = String(request?.clientId || 'no-client');
  return `${season}__${clientId}`;
}

function buildTrackerConfirmationContext({ client, requests, locale, pricingData }) {
  const first = requests[0] || {};
  const firstVehicle = first.vehicle || {};
  const total = requests.reduce((sum, request) => sum + resolveRequestAmountForEmail(request, pricingData), 0);
  const requestLines = requests.map((request, index) => `${index + 1}. ${buildEmailRequestLine(request, locale, pricingData)}`);
  const tenantInfo = buildTenantInfoText(client, locale);
  const vehicleInfo = buildVehicleInfoText(requests, locale, pricingData);
  const vehicleInfoHtml = buildVehicleInfoHtml(requests, locale, pricingData);
  const context = {
    tenant: client.name || '',
    tenantInfo,
    tenant_info: tenantInfo,
    vehicleInfo,
    vehicle_info: vehicleInfo,
    vehicleInfoHtml,
    vehicle_info_html: vehicleInfoHtml,
    tenantName: client.name || '',
    tenantEmail: client.email || '',
    tenantPhone: client.phone || '',
    tenantAddress: client.address || '',
    tenantCity: client.city || '',
    tenantProvince: client.province || '',
    tenantPostal: client.postalCode || '',
    clientName: client.name || '',
    clientEmail: client.email || '',
    season: formatSeasonLabelForEmail(first.season || '', locale, pricingData),
    seasonId: first.season || '',
    caseId: resolveEmailCaseId(first),
    confirmationCode: first.confirmationCode || '',
    storageLocation: getRequestStorageLocation(first, locale),
    vehicleType: getVehicleTypeLabelForEmail(firstVehicle, locale, pricingData),
    vehicleBrand: firstVehicle.brand || '',
    vehicleModel: firstVehicle.model || '',
    vehicleColour: firstVehicle.colour || '',
    vehicleLength: firstVehicle.lengthFeet || '',
    vehicleYear: firstVehicle.year || '',
    vehiclePlate: firstVehicle.plate || '',
    vehicleProvince: firstVehicle.province || '',
    insuranceCompany: first.insuranceCompany || '',
    insurancePolicy: first.policyNumber || '',
    insuranceExpiration: formatDateValue(first.insuranceExpiration, locale),
    leaseCost: formatMoney(total),
    estimatedCost: formatMoney(total),
    deposit: formatMoney(requests.reduce((sum, r) => sum + estimateDepositForAmount(resolveRequestAmountForEmail(r, pricingData)), 0)),
    requestCount: String(requests.length),
    requestLines: requestLines.join('\n'),
    requests: requestLines.join('\n'),
    submittedDate: formatDateValue(first.submittedAt || first.createdAt, locale),
    today: new Date().toLocaleDateString(locale === 'fr' ? 'fr-CA' : 'en-CA')
  };

  Object.keys(context).forEach((key) => {
    const snakeKey = key.replace(/[A-Z]/g, (match) => `_${match.toLowerCase()}`);
    context[snakeKey] = context[key];
  });
  return context;
}

function wrapTrackerEmailHtml({ locale, subject, body, bodyHtml: renderedBodyHtml = null }) {
  const footerContact = locale === 'fr'
    ? '<a href="https://entrepot.as-colle.com" style="color:#94a3b8;">entrepot.as-colle.com</a> | entrepot@as-colle.com'
    : '<a href="https://entrepot.as-colle.com" style="color:#94a3b8;">entrepot.as-colle.com</a> | warehouse@as-colle.com';
  const bodyHtml = renderedBodyHtml || escapeHtml(body).replace(/\n/g, '<br>');
  return `<!doctype html>
<html lang="${locale}">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
</head>
<body style="margin:0;background:#f1f5f9;font-family:Inter,Arial,sans-serif;color:#0f172a;">
  <div style="max-width:1040px;margin:24px auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
    <div style="background:#0f172a;color:#ffffff;padding:20px 24px;">
      <div style="font-size:20px;font-weight:700;">Entrepot Ferme Colle</div>
      <div style="margin-top:4px;font-size:12px;color:#cbd5e1;text-transform:uppercase;letter-spacing:0.08em;">${locale === 'fr' ? 'ENTREPOSAGE SAISONNIER DE VEHICULES' : 'SEASONAL VEHICLE STORAGE'}</div>
    </div>
    <div style="padding:24px;">
      <h1 style="font-size:22px;font-weight:700;margin:0 0 16px;color:#111827;">${escapeHtml(subject)}</h1>
      <div style="font-size:15px;line-height:1.6;color:#1f2937;">${bodyHtml}</div>
    </div>
    <div style="background:#0f172a;color:#94a3b8;padding:16px 24px;font-size:12px;">${footerContact}</div>
  </div>
</body>
</html>`;
}

function wrapClientEmailHtml({ locale, subject, body, isStorageClient }) {
  if (isStorageClient) {
    return wrapTrackerEmailHtml({ locale, subject, body });
  }
  const bodyHtml = escapeHtml(body).replace(/\n/g, '<br>');
  return `<!doctype html>
<html lang="${locale}">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
</head>
<body style="margin:0;background:#f1f5f9;font-family:Inter,Arial,sans-serif;color:#0f172a;">
  <div style="max-width:1040px;margin:24px auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
    <div style="background:#0f172a;color:#ffffff;padding:20px 24px;">
      <div style="font-size:20px;font-weight:700;">Ferme Colle</div>
    </div>
    <div style="padding:24px;">
      <h1 style="font-size:22px;font-weight:700;margin:0 0 16px;color:#111827;">${escapeHtml(subject)}</h1>
      <div style="font-size:15px;line-height:1.6;color:#1f2937;">${bodyHtml}</div>
    </div>
  </div>
</body>
</html>`;
}

async function loadEmailTemplate(templateId) {
  const directSnap = await db.collection('emailTemplates').doc(templateId).get();
  if (directSnap.exists) return directSnap.data();
  const querySnap = await db
    .collection('emailTemplates')
    .where('templateId', '==', templateId)
    .limit(1)
    .get();
  if (querySnap.empty) return null;
  return querySnap.docs[0].data();
}

async function loadEmailTemplateByIds(templateIds) {
  const candidates = Array.isArray(templateIds) ? templateIds : [templateIds];
  for (const templateId of candidates) {
    const template = await loadEmailTemplate(templateId);
    if (template) return { template, templateId };
  }
  return { template: null, templateId: candidates[0] || '' };
}

export const sendStorageConfirmationEmail = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new functions.https.HttpsError('unauthenticated', 'Sign in to send email.');
  }
  const requestIds = Array.isArray(data?.requestIds)
    ? data.requestIds.map((id) => String(id || '').trim()).filter(Boolean)
    : [];
  if (!requestIds.length) {
    throw new functions.https.HttpsError('invalid-argument', 'At least one storage request is required.');
  }
  if (!defaultFrom) {
    throw new functions.https.HttpsError('failed-precondition', 'SMTP default FROM is not configured.');
  }

  const requestSnaps = await Promise.all(
    requestIds.map((requestId) => db.collection('storageRequests').doc(requestId).get())
  );
  const requests = requestSnaps.map((snap) => (snap.exists ? { id: snap.id, ...snap.data() } : null)).filter(Boolean);
  if (requests.length !== requestIds.length) {
    throw new functions.https.HttpsError('not-found', 'One or more storage requests were not found.');
  }
  const clientId = requests[0]?.clientId || '';
  if (!clientId || requests.some((request) => request.clientId !== clientId)) {
    throw new functions.https.HttpsError('invalid-argument', 'All selected requests must belong to the same tenant.');
  }

  const clientSnap = await db.collection('clients').doc(clientId).get();
  if (!clientSnap.exists) {
    throw new functions.https.HttpsError('not-found', 'Tenant was not found.');
  }
  const client = clientSnap.data() || {};
  const to = String(client.email || '').trim();
  if (!to) {
    throw new functions.https.HttpsError('failed-precondition', 'Tenant email is missing.');
  }

  const template = await loadEmailTemplate('request-info-confirmation');
  if (!template) {
    throw new functions.https.HttpsError('failed-precondition', 'Email template request-info-confirmation was not found.');
  }

  const locale = normalizeLanguage(client.preferredLanguage || requests[0]?.sourceLanguage);
  const pricingData = await loadStoragePricingData();
  const contextValues = buildTrackerConfirmationContext({ client, requests, locale, pricingData });
  const subject = fillTemplateText(getLocalizedTemplateField(template, 'subject', locale), contextValues);
  const rawBody = getLocalizedTemplateField(template, 'body', locale);
  const text = fillTemplateText(rawBody, contextValues);
  if (!subject || !text) {
    throw new functions.https.HttpsError('failed-precondition', 'Email template subject or body is empty.');
  }
  const cc = getStorageConfirmationCc(locale);
  const bodyHtml = fillTemplateHtml(rawBody, contextValues, {
    vehicleInfo: contextValues.vehicleInfoHtml,
    vehicle_info: contextValues.vehicle_info_html
  });
  const html = wrapTrackerEmailHtml({ locale, subject, body: text, bodyHtml });

  await transporter.sendMail({
    from: defaultFrom,
    to,
    cc,
    subject,
    html,
    text
  });

  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  const batch = db.batch();
  requests.forEach((request) => {
    batch.set(
      db.collection('storageRequests').doc(request.id),
      {
        confirmationEmailSentAt: timestamp,
        confirmationEmailSentBy: context.auth.uid,
        confirmationEmailSentTo: to,
        confirmationEmailCc: cc,
        updatedAt: timestamp,
        updatedBy: context.auth.uid
      },
      { merge: true }
    );
  });
  await batch.commit();

  return { success: true, to, cc };
});

function buildContractDetailsHtml(requests, locale, pricingData) {
  const headers = locale === 'fr'
    ? ['# Demande', 'Véhicule', 'Montant']
    : ['Request #', 'Vehicle', 'Amount'];
  const rows = requests.map((request) => {
    const v = request.vehicle || {};
    const vehicleType = getVehicleTypeLabelForEmail(v, locale, pricingData);
    const vehicleModel = [v.brand, v.model].filter(Boolean).join(' ');
    const desc = [vehicleType, vehicleModel].filter(Boolean).join(': ');
    const amount = resolveRequestAmountForEmail(request, pricingData);
    return `<tr>
      <td style="padding:8px 12px;border:1px solid #e2e8f0;">${escapeHtml(request.id || '')}</td>
      <td style="padding:8px 12px;border:1px solid #e2e8f0;">${escapeHtml(desc || '—')}</td>
      <td style="padding:8px 12px;text-align:right;border:1px solid #e2e8f0;">${Number.isFinite(amount) ? formatMoney(amount) : '—'}</td>
    </tr>`;
  });
  return `<table style="width:100%;border-collapse:collapse;margin:1rem 0;font-size:14px;">
  <thead>
    <tr style="background:#f1f5f9;">
      <th style="padding:8px 12px;text-align:left;border:1px solid #e2e8f0;">${headers[0]}</th>
      <th style="padding:8px 12px;text-align:left;border:1px solid #e2e8f0;">${headers[1]}</th>
      <th style="padding:8px 12px;text-align:right;border:1px solid #e2e8f0;">${headers[2]}</th>
    </tr>
  </thead>
  <tbody>${rows.join('')}</tbody>
</table>`;
}

export const sendStorageContractEmail = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new functions.https.HttpsError('unauthenticated', 'Sign in to send email.');
  }
  const requestIds = Array.isArray(data?.requestIds)
    ? data.requestIds.map((id) => String(id || '').trim()).filter(Boolean)
    : [];
  if (!requestIds.length) {
    throw new functions.https.HttpsError('invalid-argument', 'At least one storage request is required.');
  }
  if (!defaultFrom) {
    throw new functions.https.HttpsError('failed-precondition', 'SMTP default FROM is not configured.');
  }

  const requestSnaps = await Promise.all(
    requestIds.map((requestId) => db.collection('storageRequests').doc(requestId).get())
  );
  const requests = requestSnaps.map((snap) => (snap.exists ? { id: snap.id, ...snap.data() } : null)).filter(Boolean);
  if (requests.length !== requestIds.length) {
    throw new functions.https.HttpsError('not-found', 'One or more storage requests were not found.');
  }
  const clientId = requests[0]?.clientId || '';
  if (!clientId || requests.some((request) => request.clientId !== clientId)) {
    throw new functions.https.HttpsError('invalid-argument', 'All selected requests must belong to the same tenant.');
  }
  if (!requests.every((request) => request.status === 'contract_ready')) {
    throw new functions.https.HttpsError('failed-precondition', 'All requests must be in Contract Ready status.');
  }

  const clientSnap = await db.collection('clients').doc(clientId).get();
  if (!clientSnap.exists) {
    throw new functions.https.HttpsError('not-found', 'Tenant was not found.');
  }
  const client = clientSnap.data() || {};
  const to = String(client.email || '').trim();
  if (!to) {
    throw new functions.https.HttpsError('failed-precondition', 'Tenant email is missing.');
  }

  const template = await loadEmailTemplate('send-contracts');
  if (!template) {
    throw new functions.https.HttpsError('failed-precondition', 'Email template send-contracts was not found.');
  }

  const locale = normalizeLanguage(client.preferredLanguage || requests[0]?.sourceLanguage);
  const pricingData = await loadStoragePricingData();
  const baseContext = buildTrackerConfirmationContext({ client, requests, locale, pricingData });
  const detailsHtml = buildContractDetailsHtml(requests, locale, pricingData);
  const contextValues = {
    ...baseContext,
    saison: baseContext.season,
    details: detailsHtml,
    total_cost: baseContext.leaseCost,
    total_deposit: baseContext.deposit
  };

  const subject = fillTemplateText(getLocalizedTemplateField(template, 'subject', locale), contextValues);
  const rawBody = getLocalizedTemplateField(template, 'body', locale);
  const text = fillTemplateText(rawBody, contextValues);
  if (!subject || !text) {
    throw new functions.https.HttpsError('failed-precondition', 'Email template subject or body is empty.');
  }
  const cc = getStorageConfirmationCc(locale);
  const bodyHtml = fillTemplateHtml(rawBody, contextValues, { details: detailsHtml });
  const html = wrapTrackerEmailHtml({ locale, subject, body: text, bodyHtml });

  const attachments = [];
  for (const request of requests) {
    if (request.signedContractPath) {
      try {
        const [content] = await admin.storage().bucket().file(request.signedContractPath).download();
        const v = request.vehicle || {};
        const desc = [v.typeLabel || v.type, v.brand, v.model].filter(Boolean).join('-') || 'contract';
        attachments.push({ filename: `contract-${desc}.pdf`, content, contentType: 'application/pdf' });
      } catch (err) {
        console.warn(`Could not fetch signed contract for request ${request.id}`, err);
      }
    }
  }

  await transporter.sendMail({ from: defaultFrom, to, cc, subject, html, text, attachments });

  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  const batch = db.batch();
  requests.forEach((request) => {
    batch.set(
      db.collection('storageRequests').doc(request.id),
      {
        status: 'waiting_contract_deposit',
        contractEmailSentAt: timestamp,
        contractEmailSentBy: context.auth.uid,
        contractEmailSentTo: to,
        contractEmailCc: cc,
        updatedAt: timestamp,
        updatedBy: context.auth.uid
      },
      { merge: true }
    );
  });
  await batch.commit();

  return { success: true, to, cc };
});

export const sendStorageReceiptEmail = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new functions.https.HttpsError('unauthenticated', 'Sign in to send email.');
  }
  const receiptType = String(data?.receiptType || '').trim();
  const workflow = STORAGE_RECEIPT_EMAIL_WORKFLOWS[receiptType];
  if (!workflow) {
    throw new functions.https.HttpsError('invalid-argument', 'Unknown receipt workflow.');
  }
  const requestIds = Array.isArray(data?.requestIds)
    ? data.requestIds.map((id) => String(id || '').trim()).filter(Boolean)
    : [];
  const interacReference = String(data?.interacReference || '').trim();
  if (!requestIds.length) {
    throw new functions.https.HttpsError('invalid-argument', 'At least one storage request is required.');
  }
  if (!defaultFrom) {
    throw new functions.https.HttpsError('failed-precondition', 'SMTP default FROM is not configured.');
  }

  const requestSnaps = await Promise.all(
    requestIds.map((requestId) => db.collection('storageRequests').doc(requestId).get())
  );
  const requests = requestSnaps.map((snap) => (snap.exists ? { id: snap.id, ...snap.data() } : null)).filter(Boolean);
  if (requests.length !== requestIds.length) {
    throw new functions.https.HttpsError('not-found', 'One or more storage requests were not found.');
  }
  const clientId = requests[0]?.clientId || '';
  if (!clientId || requests.some((request) => request.clientId !== clientId)) {
    throw new functions.https.HttpsError('invalid-argument', 'All selected requests must belong to the same tenant.');
  }
  const currentStatus = String(requests[0]?.status || '');
  const expectedStatuses = workflow.expectedStatuses || (workflow.expectedStatus ? [workflow.expectedStatus] : []);
  if (!expectedStatuses.includes(currentStatus) || !requests.every((request) => request.status === currentStatus)) {
    throw new functions.https.HttpsError(
      'failed-precondition',
      `All requests must be in one of these statuses: ${expectedStatuses.join(', ')}.`
    );
  }
  const targetStatus =
    workflow.changesStatus === false
      ? currentStatus
      : workflow.targetStatusByCurrent?.[currentStatus] || workflow.targetStatus;
  if (!targetStatus) {
    throw new functions.https.HttpsError('failed-precondition', 'No target status is configured for this workflow.');
  }
  if (workflow.requiresSignedContract) {
    const missingSignedContract = requests.find((request) => !request.signedContractPath);
    if (missingSignedContract) {
      throw new functions.https.HttpsError(
        'failed-precondition',
        'Upload the signed contract before sending this confirmation email.'
      );
    }
  }
  if (workflow.createsReceipt && !interacReference) {
    throw new functions.https.HttpsError('invalid-argument', 'Interac reference number is required.');
  }

  const clientSnap = await db.collection('clients').doc(clientId).get();
  if (!clientSnap.exists) {
    throw new functions.https.HttpsError('not-found', 'Tenant was not found.');
  }
  const client = clientSnap.data() || {};
  const to = String(client.email || '').trim();
  if (!to) {
    throw new functions.https.HttpsError('failed-precondition', 'Tenant email is missing.');
  }

  const { template, templateId } = await loadEmailTemplateByIds(workflow.templateIds);
  if (!template) {
    throw new functions.https.HttpsError(
      'failed-precondition',
      `Email template ${workflow.templateIds.join(' or ')} was not found.`
    );
  }

  const locale = normalizeLanguage(client.preferredLanguage || requests[0]?.sourceLanguage);
  const pricingData = await loadStoragePricingData();
  const baseContext = buildTrackerConfirmationContext({ client, requests, locale, pricingData });
  const detailsHtml = buildContractDetailsHtml(requests, locale, pricingData);
  const depositAmount = requests.reduce(
    (sum, request) => sum + estimateDepositForAmount(resolveRequestAmountForEmail(request, pricingData)),
    0
  );
  const receiptId = workflow.createsReceipt ? await allocateStorageReceiptId(requests[0], pricingData) : '';
  const ledgerAccounts = workflow.createsReceipt ? await resolveFermeColleLedgerAccounts() : null;
  const contractReceived = receiptType === 'contract' || receiptType === 'contract_deposit' || currentStatus === 'waiting_deposit';
  const depositReceived = Boolean(workflow.createsReceipt) || currentStatus === 'waiting_contract';
  const contextValues = {
    ...baseContext,
    __sections: {
      contract: contractReceived,
      deposit: depositReceived
    },
    saison: baseContext.season,
    details: detailsHtml,
    total_cost: baseContext.leaseCost,
    total_deposit: baseContext.deposit,
    receiptId,
    receipt_id: receiptId,
    receipt_type: receiptType,
    next_status: targetStatus
  };

  const subject = fillTemplateText(getLocalizedTemplateField(template, 'subject', locale), contextValues);
  const rawBody = getLocalizedTemplateField(template, 'body', locale);
  const text = fillTemplateText(rawBody, contextValues);
  if (!subject || !text) {
    throw new functions.https.HttpsError('failed-precondition', 'Email template subject or body is empty.');
  }
  const cc = getStorageConfirmationCc(locale);
  const bodyHtml = fillTemplateHtml(rawBody, contextValues, { details: detailsHtml });
  const html = wrapTrackerEmailHtml({ locale, subject, body: text, bodyHtml });

  await transporter.sendMail({ from: defaultFrom, to, cc, subject, html, text });

  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  const batch = db.batch();
  if (workflow.createsReceipt) {
    const ledgerEntryId = `storage-deposit-${receiptId}`;
    const ledgerEntryTitle = `Deposit ${baseContext.season} ${client.name || ''} ${receiptId}`.replace(/\s+/g, ' ').trim();
    const ledgerEntryDescription = buildDepositVehicleDescription(requests, locale, pricingData, { interacReference });
    const ledgerEntryRef = db.collection('expenses').doc(ledgerEntryId);
    batch.set(db.collection('storageReceipts').doc(receiptId), {
      receiptId,
      caseId: resolveEmailCaseId(requests[0]),
      seasonId: requests[0]?.season || '',
      clientId,
      requestIds,
      receiptType: 'deposit',
      sourceWorkflow: receiptType,
      amount: depositAmount,
      amountFormatted: formatMoney(depositAmount),
      interacReference,
      ledgerEntryId,
      templateId,
      sentAt: timestamp,
      sentBy: context.auth.uid,
      sentTo: to,
      sentCc: cc,
      statusFrom: currentStatus,
      statusTo: targetStatus,
      createdAt: timestamp,
      createdBy: context.auth.uid,
      updatedAt: timestamp,
      updatedBy: context.auth.uid
    });
    batch.set(ledgerEntryRef, {
      title: ledgerEntryTitle,
      description: ledgerEntryDescription,
      accountId: ledgerAccounts.cashAccount.id,
      entityId: ledgerAccounts.entityAccount.id,
      date: admin.firestore.Timestamp.now(),
      entryType: 'income',
      category: 'Deposit',
      categoryId: null,
      categoryCode: null,
      categoryType: 'income',
      amount: Number(depositAmount.toFixed(2)),
      transactionId: ledgerEntryId,
      clientId,
      storageReceiptId: receiptId,
      interacReference,
      storageRequestIds: requestIds,
      storageCaseId: resolveEmailCaseId(requests[0]),
      paymentStatus: null,
      paidAt: null,
      paidAccountId: null,
      tags: ['storage-deposit'],
      interestTags: [],
      vendorTag: null,
      isReturn: false,
      receiptUrl: null,
      receiptStoragePath: null,
      createdAt: timestamp,
      createdBy: context.auth.uid,
      createdByEmail: context.auth.token?.email || null,
      createdByName: context.auth.token?.name || null,
      updatedAt: timestamp,
      updatedBy: context.auth.uid
    });
  }
  requests.forEach((request) => {
    const requestUpdate = {
      updatedAt: timestamp,
      updatedBy: context.auth.uid
    };
    if (workflow.changesStatus === false) {
      requestUpdate.followUpEmailSentAt = timestamp;
      requestUpdate.followUpEmailSentBy = context.auth.uid;
      requestUpdate.followUpEmailSentTo = to;
      requestUpdate.followUpEmailCc = cc;
      requestUpdate.followUpEmailTemplateId = templateId;
      requestUpdate.followUpEmailType = receiptType;
    } else {
      requestUpdate.status = targetStatus;
      requestUpdate.receiptEmailSentAt = timestamp;
      requestUpdate.receiptEmailSentBy = context.auth.uid;
      requestUpdate.receiptEmailSentTo = to;
      requestUpdate.receiptEmailCc = cc;
      requestUpdate.receiptEmailTemplateId = templateId;
      requestUpdate.receiptEmailType = receiptType;
    }
    if (workflow.createsReceipt) {
      requestUpdate.receiptId = receiptId;
      requestUpdate.lastReceiptId = receiptId;
    }
    batch.set(db.collection('storageRequests').doc(request.id), requestUpdate, { merge: true });
  });
  await batch.commit();

  return { success: true, to, cc, templateId, targetStatus, receiptId };
});

export const sendClientEmail = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new functions.https.HttpsError('unauthenticated', 'Sign in to send email.');
  }
  if (!defaultFrom) {
    throw new functions.https.HttpsError('failed-precondition', 'SMTP default FROM is not configured.');
  }
  const clientId = String(data?.clientId || '').trim();
  const subject = String(data?.subject || '').trim();
  const body = String(data?.body || '').trim();
  if (!clientId) {
    throw new functions.https.HttpsError('invalid-argument', 'Client is required.');
  }
  if (!subject) {
    throw new functions.https.HttpsError('invalid-argument', 'Email title is required.');
  }
  if (!body) {
    throw new functions.https.HttpsError('invalid-argument', 'Email body is required.');
  }

  const clientSnap = await db.collection('clients').doc(clientId).get();
  if (!clientSnap.exists) {
    throw new functions.https.HttpsError('not-found', 'Client was not found.');
  }
  const client = clientSnap.data() || {};
  const to = String(client.email || '').trim();
  if (!to) {
    throw new functions.https.HttpsError('failed-precondition', 'Client email is missing.');
  }

  const locale = normalizeLanguage(client.preferredLanguage);
  const isStorageClient = client.nonStorageClient !== true;
  const html = wrapClientEmailHtml({ locale, subject, body, isStorageClient });
  await transporter.sendMail({ from: defaultFrom, to, subject, html, text: body });

  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  const emailRef = db.collection('clientEmails').doc();
  await db.runTransaction(async (transaction) => {
    transaction.set(emailRef, {
      clientId,
      to,
      subject,
      body,
      isStorageClient,
      sentAt: timestamp,
      sentBy: context.auth.uid,
      sentByEmail: context.auth.token?.email || null,
      sentByName: context.auth.token?.name || null,
      createdAt: timestamp,
      createdBy: context.auth.uid
    });
    transaction.set(
      clientSnap.ref,
      {
        lastClientEmailSentAt: timestamp,
        lastClientEmailSentBy: context.auth.uid,
        lastClientEmailSubject: subject,
        updatedAt: timestamp,
        updatedBy: context.auth.uid
      },
      { merge: true }
    );
  });

  return { success: true, to };
});

function parseAttachmentContent(rawContent) {
  let contentType;
  let content = rawContent;

  if (typeof rawContent === 'string') {
    const dataUrlMatch = rawContent.match(/^data:([^;]+);base64,(.+)$/i);
    if (dataUrlMatch) {
      contentType = dataUrlMatch[1];
      content = Buffer.from(dataUrlMatch[2], 'base64');
      return { content, contentType };
    }

    const trimmed = rawContent.trim();
    const looksBase64 = /^[a-z0-9+/=\s]+$/i.test(trimmed) && trimmed.length % 4 === 0;
    if (looksBase64) {
      try {
        content = Buffer.from(trimmed, 'base64');
        return { content, contentType };
      } catch (err) {
        // Fallthrough to treat as plain string
      }
    }
  }

  return { content, contentType };
}

function normalizeAttachments(attachments = []) {
  return attachments.map((file, index) => {
    if (!file) {
      throw new functions.https.HttpsError('invalid-argument', `Attachment at index ${index} is undefined.`);
    }
    if (file.path) {
      return {
        filename: file.filename || file.path.split('/').pop(),
        path: file.path,
        contentType: file.contentType
      };
    }
    if (file.content != null) {
      const { content, contentType: inferredType } = parseAttachmentContent(file.content);
      return {
        filename: file.filename || `attachment-${index + 1}`,
        content,
        contentType: file.contentType || inferredType,
        cid: file.cid,
        contentDisposition: file.contentDisposition
      };
    }
    throw new functions.https.HttpsError('invalid-argument', 'Attachment requires a path or content.');
  });
}

export const sendEmail = functions.https.onCall(async (data, context) => {
  const { to, subject, html, text, from, replyTo, captchaToken, attachments = [] } = data || {};
  if (!to || !subject || (!html && !text)) {
    throw new functions.https.HttpsError('invalid-argument', 'Missing to, subject, or body.');
  }

  const sender = from || defaultFrom;
  if (!sender) {
    throw new functions.https.HttpsError('failed-precondition', 'SMTP default FROM is not configured.');
  }

  const recipients = toRecipientList(to);
  if (!recipients.length) {
    throw new functions.https.HttpsError('invalid-argument', 'Invalid recipient list.');
  }

  const normalizedReplyTo = replyTo?.trim();
  const isAuthenticated = Boolean(context.auth);
  if (!isAuthenticated) {
    ensurePublicAllowedRecipients(recipients);
  }

  const isDefaultSender = !from || from === defaultFrom;
  const isTrackerAdmin = Boolean(context.auth?.token?.isTrackerAdmin);
  if (!isDefaultSender && !isTrackerAdmin) {
    const invalid = recipients.find((email) => !email.trim().endsWith(restrictedDomain));
    if (invalid) {
      throw new functions.https.HttpsError(
        'permission-denied',
        `Emails must be sent to ${restrictedDomain}. Blocked: ${invalid}`
      );
    }
  }

  const requesterIp = extractIp(context.rawRequest);
  await verifyCaptcha(captchaToken, requesterIp);

  const applyRateLimit =
    !rateLimitExemptReplyTo ||
    !normalizedReplyTo ||
    normalizedReplyTo.toLowerCase() !== rateLimitExemptReplyTo;

  if (applyRateLimit) {
    await enforceRateLimit(context.auth?.uid, 'user');
    await enforceRateLimit(requesterIp, 'ip');
  }

  try {
    const mailOptions = {
      from: sender,
      to,
      subject,
      html,
      text,
      attachments: normalizeAttachments(attachments)
    };
    if (replyTo) {
      mailOptions.replyTo = replyTo;
    }

    await transporter.sendMail(mailOptions);
    await sendConfirmationEmail({ sender, replyTo: replyTo || normalizedReplyTo, subject });
    return { success: true };
  } catch (err) {
    console.error('sendEmail error', err);
    throw new functions.https.HttpsError('internal', err.message);
  }
});

export const getSitePublishPreview = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new functions.https.HttpsError('unauthenticated', 'Sign in to preview a publish.');
  }
  const preview = await getPublishPreview();
  return { diff: preview.diff };
});

export const requestSitePublish = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new functions.https.HttpsError('unauthenticated', 'Sign in to request a publish.');
  }
  const githubToken = publishDryRun ? '' : await loadGithubPublishToken();
  if (!publishDryRun && !githubRepo) {
    throw new functions.https.HttpsError(
      'failed-precondition',
      'Publish target repository is not configured.'
    );
  }
  if (!publishDryRun && !githubToken) {
    throw new functions.https.HttpsError(
      'failed-precondition',
      `GitHub publish token could not be loaded from ${githubTokenSecretResource}. Verify the secret exists and the function service account has Secret Manager Secret Accessor access.`
    );
  }

  const latestChangeAt = data?.latestChangeAt || null;
  const requestedBy = context.auth.token?.email || context.auth.uid;
  const historyRef = db.collection('sitePublishHistory').doc();
  const jobRef = db.collection('sitePublishJobs').doc(historyRef.id);
  const preview = await getPublishPreview();

  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  const eventTimestamp = admin.firestore.Timestamp.now();
  const jobPayload = {
    publishId: historyRef.id,
    requestedAt: timestamp,
    requestedBy,
    requestedByUid: context.auth.uid,
    status: publishDryRun ? 'dry_run_recorded' : 'dispatch_sending',
    updatedAt: timestamp,
    events: admin.firestore.FieldValue.arrayUnion({
      at: eventTimestamp,
      status: publishDryRun ? 'dry_run_recorded' : 'dispatch_sending',
      details: null
    })
  };

  if (!publishDryRun) {
    await jobRef.set(jobPayload, { merge: true });
    const response = await fetch(`https://api.github.com/repos/${githubRepo}/dispatches`, {
      method: 'POST',
      headers: {
        Accept: 'application/vnd.github+json',
        Authorization: `Bearer ${githubToken}`,
        'Content-Type': 'application/json',
        'User-Agent': 'tracker-publish'
      },
      body: JSON.stringify({
        event_type: githubEventType,
        client_payload: {
          requestedBy,
          latestChangeAt,
          publishId: historyRef.id
        }
      })
    });

    if (!response.ok) {
      const text = await response.text();
      console.error('GitHub dispatch failed', response.status, text);
      const failureTimestamp = admin.firestore.Timestamp.now();
      await jobRef.set(
        {
          status: 'dispatch_failed',
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          events: admin.firestore.FieldValue.arrayUnion({
            at: failureTimestamp,
            status: 'dispatch_failed',
            details: { httpStatus: response.status }
          }),
          completedAt: admin.firestore.FieldValue.serverTimestamp()
        },
        { merge: true }
      );
      throw new functions.https.HttpsError('internal', 'GitHub workflow dispatch failed.');
    }

    const sentTimestamp = admin.firestore.Timestamp.now();
    await jobRef.set(
      {
        status: 'dispatch_sent',
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        events: admin.firestore.FieldValue.arrayUnion({
          at: sentTimestamp,
          status: 'dispatch_sent',
          details: null
        })
      },
      { merge: true }
    );
  } else {
    await jobRef.set(jobPayload, { merge: true });
  }

  const publishPayload = {
    requestedAt: timestamp,
    requestedBy,
    requestedByUid: context.auth.uid,
    latestChangeAt,
    dryRun: publishDryRun,
    githubRepo: publishDryRun ? null : githubRepo,
    githubEventType,
    diff: preview.diff,
    publishedSnapshot: preview.nextSnapshot,
    publishJobId: historyRef.id
  };

  const batch = db.batch();
  batch.set(historyRef, publishPayload);
  batch.set(
    db.collection('admin').doc('sitePublish'),
    {
      lastRequestedBy: requestedBy,
      lastRequestedByUid: context.auth.uid,
      lastChangeReference: latestChangeAt,
      lastPublishHistoryId: historyRef.id,
      lastPublishDryRun: publishDryRun
    },
    { merge: true }
  );
  await batch.commit();

  return { ok: true, dryRun: publishDryRun, publishId: historyRef.id, diff: preview.diff };
});

function normalizeLocaleMap(value) {
  if (!value) return null;
  if (typeof value === 'string') {
    return { en: value, fr: value };
  }
  const en = typeof value.en === 'string' ? value.en : '';
  const fr = typeof value.fr === 'string' ? value.fr : '';
  if (!en && !fr) return null;
  return { en, fr };
}

function normalizePrice(price) {
  if (!price || typeof price !== 'object') return null;
  const mode = price.mode || null;
  if (!mode) return null;
  if (mode === 'flat') {
    const amount = Number(price.amount);
    return Number.isFinite(amount) ? { mode, amount } : null;
  }
  if (mode === 'perFoot') {
    const rate = Number(price.rate);
    const minimum = Number(price.minimum);
    return {
      mode,
      rate: Number.isFinite(rate) ? rate : 0,
      minimum: Number.isFinite(minimum) ? minimum : 0,
      unit: normalizeLocaleMap(price.unit) || price.unit || { en: '/ ft', fr: '/ pi' }
    };
  }
  return { mode: 'contact' };
}

function toPlainTimestamp(timestamp) {
  if (!timestamp) return null;
  if (typeof timestamp.toDate === 'function') {
    return timestamp.toDate().toISOString();
  }
  return null;
}

function normalizeOffer(offer) {
  return {
    id: offer.id,
    label: normalizeLocaleMap(offer.label) || { en: '', fr: '' },
    price: normalizePrice(offer.price),
    vehicleTypes: Array.isArray(offer.vehicleTypes)
      ? offer.vehicleTypes.filter((value) => typeof value === 'string' && value.length > 0)
      : [],
    note: normalizeLocaleMap(offer.note),
    hideInTable: Boolean(offer.hideInTable),
    order: typeof offer.order === 'number' ? offer.order : 0,
    updatedAt: toPlainTimestamp(offer.updatedAt)
  };
}

export const getWebsiteContent = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new functions.https.HttpsError('unauthenticated', 'Sign in to view staging website content.');
  }
  const email = (context.auth.token?.email || '').toLowerCase();
  if (!email || !TRACKER_ADMIN_EMAILS.has(email)) {
    throw new functions.https.HttpsError('permission-denied', 'Not authorized.');
  }

  const [
    addOnsSnap,
    seasonAddOnsSnap,
    conditionsSnap,
    etiquetteSnap,
    i18nSnap,
    seasonsSnap,
    offersSnap,
    vehicleTypesSnap
  ] = await Promise.all([
    db.collection('storageAddOns').orderBy('order').get(),
    db.collection('storageSeasonAddOns').orderBy('order').get(),
    db.collection('storageConditions').orderBy('order').get(),
    db.collection('storageEtiquette').orderBy('order').get(),
    db.collection('i18nEntries').get(),
    db.collection('storageSeasons').orderBy('order').get(),
    db.collection('storageOffers').orderBy('order').get(),
    db.collection('vehicleTypes').orderBy('order').get()
  ]);

  const STORAGE_ADDONS = addOnsSnap.docs.map((docSnap) => {
    const entry = docSnap.data() || {};
    return {
      id: docSnap.id,
      code: entry.code || docSnap.id,
      name: normalizeLocaleMap(entry.name) || { en: '', fr: '' },
      description: normalizeLocaleMap(entry.description),
      order: typeof entry.order === 'number' ? entry.order : 0,
      active: entry.active !== false,
      updatedAt: toPlainTimestamp(entry.updatedAt)
    };
  });

  const STORAGE_SEASON_ADDONS = seasonAddOnsSnap.docs.map((docSnap) => {
    const entry = docSnap.data() || {};
    return {
      id: docSnap.id,
      seasonId: entry.seasonId || null,
      code: entry.code || entry.addonId || docSnap.id,
      price: typeof entry.price === 'number' ? entry.price : Number(entry.price) || 0,
      order: typeof entry.order === 'number' ? entry.order : 0,
      active: entry.active !== false,
      updatedAt: toPlainTimestamp(entry.updatedAt)
    };
  });

  const STORAGE_CONDITIONS = conditionsSnap.docs.map((docSnap) => {
    const entry = docSnap.data() || {};
    return {
      text: entry.text || { en: '', fr: '' },
      tooltip: entry.tooltip || { en: '', fr: '' },
      order: entry.order || 0,
      updatedAt: toPlainTimestamp(entry.updatedAt)
    };
  });

  const STORAGE_ETIQUETTE = etiquetteSnap.docs.map((docSnap) => {
    const entry = docSnap.data() || {};
    return {
      text: entry.text || { en: '', fr: '' },
      tooltip: entry.tooltip || { en: '', fr: '' },
      order: entry.order || 0,
      updatedAt: toPlainTimestamp(entry.updatedAt)
    };
  });

  const offersBySeason = new Map();
  offersSnap.docs.forEach((docSnap) => {
    const offer = { id: docSnap.id, ...docSnap.data() };
    const seasonId = offer.seasonId;
    if (!seasonId) return;
    const normalizedOffer = normalizeOffer(offer);
    if (!offersBySeason.has(seasonId)) offersBySeason.set(seasonId, []);
    offersBySeason.get(seasonId).push(normalizedOffer);
  });

  const STORAGE_SEASONS = seasonsSnap.docs
    .map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }))
    .filter((season) => season.active !== false)
    .map((season) => {
      const offers = offersBySeason.get(season.id) || [];
      offers.sort((a, b) => (a.order || 0) - (b.order || 0));
      return {
        id: season.id,
        name: normalizeLocaleMap(season.name) || { en: '', fr: '' },
        seasonLabel: normalizeLocaleMap(season.label) || normalizeLocaleMap(season.name) || { en: '', fr: '' },
        timeframe: normalizeLocaleMap(season.timeframe),
        duration: normalizeLocaleMap(season.duration) || normalizeLocaleMap(season.timeframe) || null,
        dropoffWindow: normalizeLocaleMap(season.dropoffWindow),
        pickupDeadline: normalizeLocaleMap(season.pickupDeadline),
        description: normalizeLocaleMap(season.description),
        ruleTitle: normalizeLocaleMap(season.ruleTitle),
        policies: Array.isArray(season.policies)
          ? season.policies
              .map((policy) => {
                if (!policy) return null;
                if (typeof policy === 'string') return { text: { en: policy, fr: policy } };
                if (policy.text || policy.tooltip || policy.tooltipKey) {
                  const entry = {};
                  if (policy.text) entry.text = normalizeLocaleMap(policy.text) || { en: '', fr: '' };
                  if (policy.tooltip) entry.tooltip = normalizeLocaleMap(policy.tooltip);
                  if (policy.tooltipKey) entry.tooltipKey = policy.tooltipKey;
                  return entry;
                }
                const text = normalizeLocaleMap(policy);
                return text ? { text } : null;
              })
              .filter(Boolean)
          : [],
        offers,
        order: typeof season.order === 'number' ? season.order : 0,
        active: season.active !== false,
        updatedAt: toPlainTimestamp(season.updatedAt)
      };
    })
    .sort((a, b) => (a.order || 0) - (b.order || 0));

  const VEHICLE_TYPES = vehicleTypesSnap.docs
    .map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }))
    .map((entry) => {
      const labels = normalizeLocaleMap(entry.label) || { en: '', fr: '' };
      const value =
        (typeof entry.value === 'string' && entry.value.trim()) ||
        (typeof entry.type === 'string' && entry.type.trim()) ||
        entry.id ||
        labels.en ||
        labels.fr ||
        '';
      if (!value) return null;
      const legacyValues = Array.isArray(entry.legacyValues)
        ? entry.legacyValues.filter((item) => typeof item === 'string' && item.trim().length > 0)
        : [];
      return {
        id: entry.id,
        value,
        labels,
        slug: typeof entry.slug === 'string' && entry.slug.trim() ? entry.slug.trim() : null,
        legacyValues,
        order: typeof entry.order === 'number' ? entry.order : 0,
        active: entry.active !== false,
        updatedAt: toPlainTimestamp(entry.updatedAt)
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if ((a.order || 0) !== (b.order || 0)) return (a.order || 0) - (b.order || 0);
      return (a.value || '').localeCompare(b.value || '');
    });

  const I18N = {};
  i18nSnap.docs.forEach((docSnap) => {
    const entry = docSnap.data() || {};
    const key = entry.key || docSnap.id;
    if (!key) return;
    I18N[key] = entry.text || { en: '', fr: '' };
  });

  return {
    STORAGE_ADDONS,
    STORAGE_SEASON_ADDONS,
    STORAGE_CONDITIONS,
    STORAGE_ETIQUETTE,
    STORAGE_SEASONS,
    VEHICLE_TYPES,
    I18N
  };
});

export const createStorageRequest = functions.https.onCall(async (data, context) => {
  const payload = data || {};
  const normalize = (value) => (typeof value === 'string' ? value.trim() : '');
  const asNumber = (value) => {
    if (value === null || value === undefined || value === '') return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  };
  const tenantName = normalize(payload.tenantName);
  const tenantPhone = normalize(payload.tenantPhone);
  const tenantAddress = normalize(payload.tenantAddress);
  const tenantCity = normalize(payload.tenantCity);
  const tenantProvince = normalize(payload.tenantProvince);
  const tenantPostal = normalize(payload.tenantPostal);
  const tenantEmail = normalize(payload.email);
  const tenantEmailLower = tenantEmail.toLowerCase();
  const season = normalize(payload.season);
  const vehicleType = normalize(payload.vehicleType);
  const confirmationCode =
    normalize(payload.confirmationCode) || generateConfirmationCode();

  if (!tenantEmail) {
    throw new functions.https.HttpsError('invalid-argument', 'Email is required.');
  }
  if (!season || !tenantName || !tenantPhone || !tenantAddress || !tenantCity || !tenantProvince || !tenantPostal) {
    throw new functions.https.HttpsError('invalid-argument', 'Missing required tenant details.');
  }
  if (!vehicleType) {
    throw new functions.https.HttpsError('invalid-argument', 'Missing vehicle type.');
  }

  const requesterIp = extractIp(context.rawRequest);
  await verifyCaptcha(payload.captchaToken, requesterIp);

  const clientId = await upsertWebsiteClient({
    tenantName,
    tenantPhone,
    tenantEmail,
    tenantEmailLower,
    tenantAddress,
    tenantCity,
    tenantProvince,
    tenantPostal,
    formLanguage: payload.formLanguage
  });

  const insuranceExpiration = normalize(payload.insuranceExpiration);
  const insuranceDate =
    insuranceExpiration && !Number.isNaN(Date.parse(insuranceExpiration))
      ? admin.firestore.Timestamp.fromDate(new Date(insuranceExpiration))
      : null;

  const estimateAmount = asNumber(payload.estimatedCost);
  const depositEstimate = asNumber(payload.deposit);

  const requestPayload = {
    season,
    clientId,
    storageLocation: normalize(payload.storageLocation) === 'outdoor' ? 'outdoor' : 'indoor',
    vehicle: {
      type: vehicleType,
      typeLabel: normalize(payload.vehicleTypeLabel) || vehicleType || '—',
      otherType: normalize(payload.vehicleTypeOther),
      brand: normalize(payload.vehicleBrand),
      model: normalize(payload.vehicleModel),
      colour: normalize(payload.vehicleColour),
      lengthFeet: asNumber(payload.vehicleLength),
      year: asNumber(payload.vehicleYear),
      plate: normalize(payload.vehiclePlate),
      province: normalize(payload.vehicleProv)
    },
    insuranceCompany: normalize(payload.insuranceCompany),
    policyNumber: normalize(payload.insurancePolicy),
    insuranceExpiration: insuranceDate,
    status: 'new',
    addons: {
      battery: payload.battery === true,
      propane: payload.propane === true
    },
    contractAmount: null,
    estimate: {
      amount: estimateAmount,
      deposit: depositEstimate
    },
    confirmationCode,
    source: 'entrepot',
    sourceLanguage: normalize(payload.formLanguage),
    submittedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedBy: null,
    createdBy: null
  };

  const requestDoc = await db.collection('storageRequests').add(requestPayload);

  try {
    const locale = normalize(payload.formLanguage) === 'fr' ? 'fr' : 'en';
    await sendStorageRequestConfirmationEmail({
      to: tenantEmail,
      locale,
      confirmationCode,
      tenant: {
        tenantName,
        tenantPhone,
        tenantAddress,
        tenantCity,
        tenantProvince,
        tenantPostal,
        email: tenantEmail,
        formLanguage: normalize(payload.formLanguage)
      },
      requests: [
        {
          vehicleType,
          vehicleTypeLabel: normalize(payload.vehicleTypeLabel) || vehicleType || '—',
          vehicleTypeOther: normalize(payload.vehicleTypeOther),
          vehicleBrand: normalize(payload.vehicleBrand),
          vehicleModel: normalize(payload.vehicleModel),
          vehicleColour: normalize(payload.vehicleColour),
          vehicleLength: normalize(payload.vehicleLength),
          vehicleYear: normalize(payload.vehicleYear),
          vehiclePlate: normalize(payload.vehiclePlate),
          vehicleProv: normalize(payload.vehicleProv),
          battery: payload.battery === true,
          propane: payload.propane === true
        }
      ]
    });
  } catch (err) {
    console.error('Storage request confirmation email failed', err);
  }

  try {
    await sendStorageRequestNotificationEmail({
      confirmationCode,
      tenant: {
        tenantName,
        tenantPhone,
        tenantAddress,
        tenantCity,
        tenantProvince,
        tenantPostal,
        email: tenantEmail
      },
      requests: [
        {
          season,
          vehicleType,
          vehicleTypeLabel: normalize(payload.vehicleTypeLabel) || vehicleType || '—',
          vehicleTypeOther: normalize(payload.vehicleTypeOther),
          vehicleBrand: normalize(payload.vehicleBrand),
          vehicleModel: normalize(payload.vehicleModel),
          vehicleColour: normalize(payload.vehicleColour),
          vehicleLength: normalize(payload.vehicleLength),
          vehicleYear: normalize(payload.vehicleYear),
          vehiclePlate: normalize(payload.vehiclePlate),
          vehicleProv: normalize(payload.vehicleProv),
          battery: payload.battery === true,
          propane: payload.propane === true
        }
      ],
      requestIds: [requestDoc.id],
      clientId
    });
  } catch (err) {
    console.error('Storage request notification email failed', err);
  }

  return { success: true, requestId: requestDoc.id, clientId, confirmationCode };
});

export const createStorageRequests = functions.https.onCall(async (data, context) => {
  const payload = data || {};
  const normalize = (value) => (typeof value === 'string' ? value.trim() : '');
  const asNumber = (value) => {
    if (value === null || value === undefined || value === '') return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  };
  const tenant = payload.tenant || {};
  const tenantName = normalize(tenant.tenantName);
  const tenantPhone = normalize(tenant.tenantPhone);
  const tenantAddress = normalize(tenant.tenantAddress);
  const tenantCity = normalize(tenant.tenantCity);
  const tenantProvince = normalize(tenant.tenantProvince);
  const tenantPostal = normalize(tenant.tenantPostal);
  const tenantEmail = normalize(tenant.email);
  const tenantEmailLower = tenantEmail.toLowerCase();
  const season = normalize(tenant.season);

  const requests = Array.isArray(payload.requests)
    ? payload.requests.filter((entry) => entry && typeof entry === 'object')
    : [];

  if (!tenantEmail) {
    throw new functions.https.HttpsError('invalid-argument', 'Email is required.');
  }
  if (!season || !tenantName || !tenantPhone || !tenantAddress || !tenantCity || !tenantProvince || !tenantPostal) {
    throw new functions.https.HttpsError('invalid-argument', 'Missing required tenant details.');
  }
  if (!requests.length) {
    throw new functions.https.HttpsError('invalid-argument', 'At least one vehicle request is required.');
  }

  const requesterIp = extractIp(context.rawRequest);
  await verifyCaptcha(payload.captchaToken, requesterIp);

  const clientId = await upsertWebsiteClient({
    tenantName,
    tenantPhone,
    tenantEmail,
    tenantEmailLower,
    tenantAddress,
    tenantCity,
    tenantProvince,
    tenantPostal,
    formLanguage: tenant.formLanguage
  });

  const confirmationCode = generateConfirmationCode();
  const now = admin.firestore.FieldValue.serverTimestamp();
  const batch = db.batch();
  const requestIds = [];

  requests.forEach((request) => {
    const vehicleType = normalize(request.vehicleType);
    if (!vehicleType) {
      return;
    }
    const insuranceExpiration = normalize(request.insuranceExpiration);
    const insuranceDate =
      insuranceExpiration && !Number.isNaN(Date.parse(insuranceExpiration))
        ? admin.firestore.Timestamp.fromDate(new Date(insuranceExpiration))
        : null;

    const estimateAmount = asNumber(request.estimatedCost);
    const depositEstimate = asNumber(request.deposit);

    const ref = db.collection('storageRequests').doc();
    requestIds.push(ref.id);
    batch.set(ref, {
      season: normalize(request.season) || season,
      clientId,
      storageLocation: normalize(request.storageLocation) === 'outdoor' ? 'outdoor' : 'indoor',
      vehicle: {
        type: vehicleType,
        typeLabel: normalize(request.vehicleTypeLabel) || vehicleType || '—',
        otherType: normalize(request.vehicleTypeOther),
        brand: normalize(request.vehicleBrand),
        model: normalize(request.vehicleModel),
        colour: normalize(request.vehicleColour),
        lengthFeet: asNumber(request.vehicleLength),
        year: asNumber(request.vehicleYear),
        plate: normalize(request.vehiclePlate),
        province: normalize(request.vehicleProv)
      },
      insuranceCompany: normalize(request.insuranceCompany),
      policyNumber: normalize(request.insurancePolicy),
      insuranceExpiration: insuranceDate,
      status: 'new',
      addons: {
        battery: request.battery === true,
        propane: request.propane === true
      },
      contractAmount: null,
      estimate: {
        amount: estimateAmount,
        deposit: depositEstimate
      },
      confirmationCode,
      source: 'entrepot',
      sourceLanguage: normalize(tenant.formLanguage),
      submittedAt: now,
      updatedAt: now,
      createdAt: now,
      updatedBy: null,
      createdBy: null
    });
  });

  if (!requestIds.length) {
    throw new functions.https.HttpsError('invalid-argument', 'Missing vehicle type on all requests.');
  }

  await batch.commit();

  try {
    const locale = tenant.formLanguage === 'fr' ? 'fr' : 'en';
    await sendStorageRequestConfirmationEmail({
      to: tenantEmail,
      locale,
      confirmationCode,
      tenant,
      requests
    });
  } catch (err) {
    console.error('Storage request confirmation email failed', err);
  }

  try {
    await sendStorageRequestNotificationEmail({
      confirmationCode,
      tenant,
      requests,
      requestIds,
      clientId
    });
  } catch (err) {
    console.error('Storage request notification email failed', err);
  }

  return { success: true, confirmationCode, requestIds, clientId };
});
