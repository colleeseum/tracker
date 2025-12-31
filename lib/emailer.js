import nodemailer from 'nodemailer';

const smtpHost = process.env.SMTP2GO_HOST || 'mail.smtp2go.com';
const smtpPort = Number(process.env.SMTP2GO_PORT || 587);
const smtpUser = process.env.SMTP2GO_USER;
const smtpPass = process.env.SMTP2GO_PASS;
const defaultFrom = process.env.SMTP2GO_FROM;

if (!smtpUser || !smtpPass || !defaultFrom) {
  console.warn('[emailer] Missing SMTP2GO credentials. Set SMTP2GO_USER, SMTP2GO_PASS, SMTP2GO_FROM.');
}

const transporter = nodemailer.createTransport({
  host: smtpHost,
  port: smtpPort,
  secure: smtpPort === 465,
  auth: {
    user: smtpUser,
    pass: smtpPass
  }
});

const restrictedDomain = process.env.RESTRICTED_EMAIL_DOMAIN || '@as-colle.com';

export async function sendEmail({ to, subject, html, text, from, attachments = [] }) {
  if (!to) throw new Error('Missing "to"');
  if (!subject) throw new Error('Missing "subject"');
  if (!html && !text) throw new Error('Provide "html" or "text" body');
  const sender = from || defaultFrom;
  if (!sender) throw new Error('Missing "from" address. Set SMTP2GO_FROM or pass `from`.');

  const isDefaultSender = !from || from === defaultFrom;
  if (!isDefaultSender) {
    // Non-default sender: enforce restricted domain per requirement
    const toList = Array.isArray(to) ? to : String(to).split(',');
    const invalid = toList.find((email) => !email.trim().endsWith(restrictedDomain));
    if (invalid) {
      throw new Error(`Emails must be sent to ${restrictedDomain}. Blocked: ${invalid}`);
    }
  }

  const message = {
    from: sender,
    to,
    subject,
    html,
    text,
    attachments: attachments.map((file, index) => {
      if (!file) throw new Error(`Attachment at index ${index} is undefined`);
      if (file.path) {
        return { filename: file.filename || file.path.split('/').pop(), path: file.path, contentType: file.contentType };
      }
      if (file.content) {
        return {
          filename: file.filename || `attachment-${index + 1}`,
          content: file.content,
          encoding: file.encoding,
          contentType: file.contentType
        };
      }
      throw new Error('Attachment requires path or content');
    })
  };

  const info = await transporter.sendMail(message);
  return info;
}
