export const DEV_EMAIL_RECIPIENT = 'entrepot@as-colle.com';

export function isDevelopmentEmailEnvironment(environment = process.env) {
  const enabled = (value) => String(value || '').toLowerCase() === 'true';
  return enabled(environment.FUNCTIONS_EMULATOR) || enabled(environment.USE_FIRESTORE_EMULATOR) || enabled(environment.DEV_EMAIL_MODE)
    || ['FIRESTORE_EMULATOR_HOST', 'FIREBASE_AUTH_EMULATOR_HOST', 'FIREBASE_STORAGE_EMULATOR_HOST', 'FIREBASE_EMULATOR_HUB'].some((key) => Boolean(String(environment[key] || '').trim()))
    || ['development', 'test'].includes(environment.NODE_ENV)
    || ['FIREBASE_PROFILE', 'FIREBASE_ENV', 'GCLOUD_ENV'].some((key) => ['local', 'dev', 'development', 'test'].includes(String(environment[key] || '').toLowerCase()));
}

export function createEmailSafetyPolicy(environment = process.env) {
  const development = isDevelopmentEmailEnvironment(environment);
  const subjectForDev = (subject) => String(subject || '').startsWith('[DEV] ') ? String(subject) : `[DEV] ${String(subject || '')}`;
  const unsafeHeader = /^(to|cc|bcc|reply-to|return-path|delivered-to|resent-.+|disposition-notification-to|return-receipt-to|delivery-receipt-to|x-confirm-reading-to|x-read-receipt-to|list-.+)$/i;
  function preview(message) {
    return development
      ? { to: DEV_EMAIL_RECIPIENT, cc: '', bcc: '', subject: subjectForDev(message.subject), developmentEmailRedirect: true }
      : { to: message.to, cc: message.cc || '', bcc: message.bcc || '', subject: message.subject || '', developmentEmailRedirect: false };
  }
  function prepare(message) {
    if (!development) return message;
    if (!message || typeof message !== 'object' || message.raw) throw new Error('Raw email delivery is disabled in dev. Use structured email options so recipient safety can be enforced.');
    const safe = {
      ...message, to: DEV_EMAIL_RECIPIENT, replyTo: DEV_EMAIL_RECIPIENT,
      subject: subjectForDev(message.subject),
      // Enforce the actual SMTP destination and bounce address, not just the visible headers.
      envelope: { from: DEV_EMAIL_RECIPIENT, to: [DEV_EMAIL_RECIPIENT] }
    };
    delete safe.cc;
    delete safe.bcc;
    delete safe.dsn;
    delete safe.list;
    if (Array.isArray(message.headers)) {
      safe.headers = message.headers.filter((header) => !unsafeHeader.test(String(header.key || header.name || '')));
    } else if (message.headers && typeof message.headers === 'object') {
      safe.headers = Object.fromEntries(Object.entries(message.headers).filter(([key]) => !unsafeHeader.test(key)));
    }
    return safe;
  }
  return Object.freeze({ development, redirectTo: development ? DEV_EMAIL_RECIPIENT : null, preview, prepare });
}

export function createSafeMailer(transport, policy = createEmailSafetyPolicy()) {
  return Object.freeze({ sendMail: (message, ...args) => transport.sendMail(policy.prepare(message), ...args) });
}
