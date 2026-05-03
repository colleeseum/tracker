import functions from 'firebase-functions';
import admin from 'firebase-admin';
import nodemailer from 'nodemailer';
import crypto from 'node:crypto';
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

admin.initializeApp();
const db = admin.firestore();

let cachedGithubToken = null;
let githubTokenLoadPromise = null;

let cachedPublishCallbackToken = null;
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
  if (cachedPublishCallbackToken) return cachedPublishCallbackToken;
  if (publishCallbackTokenLoadPromise) return publishCallbackTokenLoadPromise;

  publishCallbackTokenLoadPromise = (async () => {
    const direct = process.env.PUBLISH_CALLBACK_TOKEN;
    if (direct) {
      cachedPublishCallbackToken = direct;
      return cachedPublishCallbackToken;
    }

    const secretResource =
      process.env.PUBLISH_CALLBACK_TOKEN_SECRET_RESOURCE ||
      process.env.PUBLISH_CALLBACK_TOKEN_SECRET ||
      '';
    if (!secretResource) {
      cachedPublishCallbackToken = '';
      return cachedPublishCallbackToken;
    }

    const versionName = secretResource.includes('/versions/')
      ? secretResource
      : `${secretResource}/versions/latest`;
    const client = new SecretManagerServiceClient();
    const [result] = await client.accessSecretVersion({ name: versionName });
    const token = result?.payload?.data ? result.payload.data.toString('utf8').trim() : '';
    cachedPublishCallbackToken = token;
    return cachedPublishCallbackToken;
  })()
    .catch((err) => {
      console.error('Failed to load publish callback token from Secret Manager.', err);
      cachedPublishCallbackToken = '';
      return cachedPublishCallbackToken;
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
  const jobRef = db.collection('sitePublishJobs').doc(publishId);
  const historyRef = db.collection('sitePublishHistory').doc(publishId);
  const event = {
    at: timestamp,
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
    address: tenantAddress,
    city: tenantCity,
    province: tenantProvince,
    postalCode: tenantPostal,
    active: true,
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
  if (!publishDryRun && (!githubRepo || !githubToken)) {
    throw new functions.https.HttpsError(
      'failed-precondition',
      'Publish target is not configured. Set GITHUB_PUBLISH_REPO and either GITHUB_PUBLISH_TOKEN or GITHUB_PUBLISH_TOKEN_SECRET_RESOURCE.'
    );
  }

  const latestChangeAt = data?.latestChangeAt || null;
  const requestedBy = context.auth.token?.email || context.auth.uid;
  const historyRef = db.collection('sitePublishHistory').doc();
  const jobRef = db.collection('sitePublishJobs').doc(historyRef.id);
  const preview = await getPublishPreview();

  const timestamp = admin.firestore.FieldValue.serverTimestamp();
  const jobPayload = {
    publishId: historyRef.id,
    requestedAt: timestamp,
    requestedBy,
    requestedByUid: context.auth.uid,
    status: publishDryRun ? 'dry_run_recorded' : 'dispatch_sending',
    updatedAt: timestamp,
    events: admin.firestore.FieldValue.arrayUnion({
      at: timestamp,
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
      await jobRef.set(
        {
          status: 'dispatch_failed',
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          events: admin.firestore.FieldValue.arrayUnion({
            at: admin.firestore.FieldValue.serverTimestamp(),
            status: 'dispatch_failed',
            details: { httpStatus: response.status }
          }),
          completedAt: admin.firestore.FieldValue.serverTimestamp()
        },
        { merge: true }
      );
      throw new functions.https.HttpsError('internal', 'GitHub workflow dispatch failed.');
    }

    await jobRef.set(
      {
        status: 'dispatch_sent',
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        events: admin.firestore.FieldValue.arrayUnion({
          at: admin.firestore.FieldValue.serverTimestamp(),
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
    publishJobId: historyRef.id
  };

  const batch = db.batch();
  batch.set(historyRef, publishPayload);
  batch.set(
    db.collection('admin').doc('sitePublish'),
    {
      lastPublishedAt: admin.firestore.FieldValue.serverTimestamp(),
      lastRequestedBy: requestedBy,
      lastRequestedByUid: context.auth.uid,
      lastChangeReference: latestChangeAt,
      lastPublishHistoryId: historyRef.id,
      lastPublishDryRun: publishDryRun,
      lastPublishedSnapshot: preview.nextSnapshot
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

  return { success: true, confirmationCode, requestIds, clientId };
});
