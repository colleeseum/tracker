import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { DEV_EMAIL_RECIPIENT, isDevelopmentEmailEnvironment, createEmailSafetyPolicy, createSafeMailer } from './email-safety.js';

test('every emulator and development signal independently forces safe routing', () => {
  for (const environment of [
    { FUNCTIONS_EMULATOR: 'true' }, { FIRESTORE_EMULATOR_HOST: '127.0.0.1:8081' },
    { FIREBASE_AUTH_EMULATOR_HOST: '127.0.0.1:9099' }, { FIREBASE_STORAGE_EMULATOR_HOST: 'localhost:9199' },
    { FIREBASE_EMULATOR_HUB: '127.0.0.1:4400' }, { USE_FIRESTORE_EMULATOR: 'true' },
    { FIREBASE_PROFILE: 'local' }, { FIREBASE_ENV: 'dev' }, { GCLOUD_ENV: 'test' },
    { NODE_ENV: 'development' }, { NODE_ENV: 'test' }, { DEV_EMAIL_MODE: 'true' },
    { FUNCTIONS_EMULATOR: 'true', NODE_ENV: 'production', DEV_EMAIL_MODE: 'false' }
  ]) assert.equal(isDevelopmentEmailEnvironment(environment), true);
  assert.equal(isDevelopmentEmailEnvironment({ NODE_ENV: 'production', FIREBASE_PROFILE: 'prod' }), false);
});
test('dev rewrites visible To and the SMTP envelope, removes customer CC/BCC and redirects replies/bounces', () => {
  const original = {
    to: ['Customer <customer@example.invalid>', { name: 'Second customer', address: 'other@example.invalid' }],
    cc: 'warehouse@as-colle.com', bcc: ['private@example.invalid'], from: 'sender@as-colle.com',
    replyTo: 'customer@example.invalid', subject: 'Invoice',
    envelope: { from: 'customer@example.invalid', to: ['hidden@example.invalid'] },
    dsn: { recipient: 'customer@example.invalid' }, list: { post: 'customer@example.invalid' },
    headers: { To: 'customer@example.invalid', BCC: 'secret@example.invalid', 'Reply-To': 'customer@example.invalid', 'Disposition-Notification-To': 'customer@example.invalid', 'X-Trace': 'kept' },
    attachments: [{ filename: 'invoice.pdf', content: Buffer.from('%PDF') }]
  };
  const policy = createEmailSafetyPolicy({ FUNCTIONS_EMULATOR: 'true' });
  const safe = policy.prepare(original);
  assert.equal(safe.to, DEV_EMAIL_RECIPIENT);
  assert.deepEqual(safe.envelope, { from: DEV_EMAIL_RECIPIENT, to: [DEV_EMAIL_RECIPIENT] });
  assert.equal(safe.replyTo, DEV_EMAIL_RECIPIENT);
  assert.equal(safe.subject, '[DEV] Invoice');
  assert.ok(!('cc' in safe) && !('bcc' in safe) && !('dsn' in safe) && !('list' in safe));
  assert.deepEqual(safe.headers, { 'X-Trace': 'kept' });
  assert.equal(safe.attachments, original.attachments);
  assert.equal(original.replyTo, 'customer@example.invalid');
  assert.equal(policy.prepare(safe).subject, '[DEV] Invoice');
});
test('array routing headers and raw emails cannot bypass dev safety', () => {
  const policy = createEmailSafetyPolicy({ FIREBASE_PROFILE: 'local' });
  const safe = policy.prepare({ to: 'customer@example.invalid', subject: 'Test', headers: [{ key: 'Cc', value: 'customer@example.invalid' }, { key: 'Return-Receipt-To', value: 'customer@example.invalid' }, { key: 'X-Trace', value: 'kept' }] });
  assert.deepEqual(safe.headers, [{ key: 'X-Trace', value: 'kept' }]);
  assert.throws(() => policy.prepare({ raw: 'To: customer@example.invalid\r\n\r\nHello' }), /disabled in dev/);
});
test('production options are unchanged, including normal CC/BCC and envelope', () => {
  const policy = createEmailSafetyPolicy({ NODE_ENV: 'production' });
  const original = { to: 'customer@example.invalid', cc: 'office@example.invalid', bcc: 'archive@example.invalid', replyTo: 'sender@example.invalid', subject: 'Invoice', envelope: { to: ['customer@example.invalid'] } };
  assert.equal(policy.prepare(original), original);
  assert.equal(policy.preview(original).to, original.to);
  assert.equal(policy.preview(original).cc, original.cc);
  assert.equal(policy.preview(original).developmentEmailRedirect, false);
});
test('all existing email workflows share one protected mailer, including automatic acknowledgements', async () => {
  const sent = [];
  const mailer = createSafeMailer({ sendMail: async (message) => { sent.push(message); return { accepted: message.envelope.to }; } }, createEmailSafetyPolicy({ FUNCTIONS_EMULATOR: 'true' }));
  for (const workflow of ['invoice', 'receipt', 'contract', 'client', 'website request', 'website notification', 'sendEmail', 'acknowledgement']) {
    await mailer.sendMail({ to: 'customer@example.invalid', cc: 'warehouse@as-colle.com', bcc: 'other@example.invalid', subject: workflow });
  }
  assert.equal(sent.length, 8);
  assert.ok(sent.every((message) => message.to === DEV_EMAIL_RECIPIENT && message.envelope.to.length === 1 && message.envelope.to[0] === DEV_EMAIL_RECIPIENT && !message.cc && !message.bcc));
  const source = readFileSync(new URL('../index.js', import.meta.url), 'utf8');
  assert.match(source, /const transporter = createSafeMailer\(smtpTransport, emailSafetyPolicy\)/);
  assert.equal((source.match(/nodemailer\.createTransport\(/g) || []).length, 1);
  assert.ok([...source.matchAll(/\b(\w+)\.sendMail\(/g)].every((match) => match[1] === 'transporter'));
});

const appSource = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
const start = appSource.indexOf('async function resolveEmailDeliveryPreview(');
const context = vm.createContext({ usingEmulators: true, isLocalHost: true, getEmailDeliveryPolicyCallable: async () => ({ data: { development: true, redirectTo: DEV_EMAIL_RECIPIENT } }) });
vm.runInContext(appSource.slice(start, appSource.indexOf('\n}\n', start) + 3), context);
test('dev previews show effective To, blank CC/BCC and marked subjects', async () => {
  const preview = await context.resolveEmailDeliveryPreview({ to: 'customer@example.invalid', cc: 'warehouse@as-colle.com', bcc: 'private@example.invalid', subject: 'Invoice' });
  assert.equal(preview.to, DEV_EMAIL_RECIPIENT);
  assert.equal(preview.cc, '');
  assert.equal(preview.bcc, '');
  assert.equal(preview.subject, '[DEV] Invoice');
});
test('local preview fails closed if the server reports live routing or an unexpected redirect', async () => {
  context.getEmailDeliveryPolicyCallable = async () => ({ data: { development: false } });
  await assert.rejects(context.resolveEmailDeliveryPreview({}), /live email backend/);
  context.getEmailDeliveryPolicyCallable = async () => ({ data: { development: true, redirectTo: 'customer@example.invalid' } });
  await assert.rejects(context.resolveEmailDeliveryPreview({}), /blocked/);
});
