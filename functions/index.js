import functions from 'firebase-functions';
import admin from 'firebase-admin';
import nodemailer from 'nodemailer';

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
const githubRepo = process.env.GITHUB_PUBLISH_REPO || '';
const githubToken = process.env.GITHUB_PUBLISH_TOKEN || '';
const githubEventType = process.env.GITHUB_PUBLISH_EVENT || 'tracker-content-publish';

admin.initializeApp();
const db = admin.firestore();

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

export const requestSitePublish = functions.https.onCall(async (data, context) => {
  if (!context.auth) {
    throw new functions.https.HttpsError('unauthenticated', 'Sign in to request a publish.');
  }
  if (!githubRepo || !githubToken) {
    throw new functions.https.HttpsError(
      'failed-precondition',
      'Publish target is not configured. Set GITHUB_PUBLISH_REPO and GITHUB_PUBLISH_TOKEN.'
    );
  }

  const latestChangeAt = data?.latestChangeAt || null;
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
        requestedBy: context.auth.token?.email || context.auth.uid,
        latestChangeAt
      }
    })
  });

  if (!response.ok) {
    const text = await response.text();
    console.error('GitHub dispatch failed', response.status, text);
    throw new functions.https.HttpsError('internal', 'GitHub workflow dispatch failed.');
  }

  await db
    .collection('admin')
    .doc('sitePublish')
    .set(
      {
        lastPublishedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastRequestedBy: context.auth.token?.email || context.auth.uid,
        lastChangeReference: latestChangeAt
      },
      { merge: true }
    );

  return { ok: true };
});
