import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import admin from 'firebase-admin';
import { createLockedReportService, lockedReportId } from '../lib/locked-reports.js';

if (!/^(127\.0\.0\.1|localhost):\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST || '')) throw new Error('Explicit local Firestore emulator configuration is required.');
admin.initializeApp({ projectId: 'tracker-187c5' });
const db = admin.firestore();
const filters = { type: 'balance', entityId: `locked-report-test-${crypto.randomUUID()}`, year: 2025 };
const id = lockedReportId(filters);
const ref = db.collection('lockedReports').doc(id);
const html = '<!doctype html><html><head><meta http-equiv="Content-Security-Policy" content="default-src \'none\'"><style>body{color:black}</style></head><body>Saved tax total $123.00</body></html>';
const service = createLockedReportService({ db, fieldValue: admin.firestore.FieldValue });
// Unsigned tokens are accepted only by the emulator. No Auth account is created.
const b64 = (value) => Buffer.from(JSON.stringify(value)).toString('base64url');
const token = `${b64({ alg: 'none', typ: 'JWT' })}.${b64({ sub: 'report-test-admin', user_id: 'report-test-admin', email: 'sergecolle@gmail.com', email_verified: true,
  aud: 'tracker-187c5', iss: 'https://securetoken.google.com/tracker-187c5', iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 3600,
  firebase: { sign_in_provider: 'custom', identities: {} } })}.`;
try {
  const saved = await service.lock({ ...filters, entityName: 'Local report test', html, inputs: { mileageRate: 0.72, ccaRate: 0.3, amount: 123 } }, { uid: 'local-test', email: 'test@example.invalid' });
  assert.equal(saved.html, html);
  assert.equal(typeof saved.metadata.lockedAt, 'number');
  const other = await service.lock({ ...filters, html: html.replace('$123', '$999'), inputs: { mileageRate: 999 } }, { uid: 'different' });
  assert.equal(other.html, html); assert.equal(other.inputs.mileageRate, 0.72);
  const base = `http://${process.env.FIRESTORE_EMULATOR_HOST}/v1/projects/tracker-187c5/databases/(default)/documents/lockedReports/${id}`;
  const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
  const read = await fetch(base, { headers });
  assert.equal(read.status, 200, await read.text());
  for (const target of [base, `${base}/content/0`]) {
    const overwrite = await fetch(target, { method: 'PATCH', headers, body: JSON.stringify({ fields: { text: { stringValue: 'tampered' } } }) });
    assert.equal(overwrite.status, 403, `Locked metadata/content must reject client overwrites: ${await overwrite.text()}`);
    const deletion = await fetch(target, { method: 'DELETE', headers });
    assert.equal(deletion.status, 403, 'Locked snapshots must reject client deletion');
  }
  assert.equal((await service.get(filters)).html, html);
  console.log('Local locked-report integration passed: permanent snapshot, unchanged rates/totals on repeat, verified content, admin client overwrite/delete denied. No existing data modified.');
} finally {
  // Remove only this test's own documents using the Admin SDK.
  const chunks = await ref.collection('content').get();
  const cleanup = db.batch(); chunks.docs.forEach((chunk) => cleanup.delete(chunk.ref)); cleanup.delete(ref);
  await cleanup.commit(); await db.terminate();
}
