import crypto from 'node:crypto';

const hash = (value) => crypto.createHash('sha256').update(value).digest('hex');
const error = (message, code = 'failed-precondition') => Object.assign(new Error(message), { code });
export function lockedReportId({ type, entityId, year }) {
  if (!['balance', 'tags'].includes(type) || typeof entityId !== 'string' || !entityId || entityId.includes('/') || !Number.isInteger(year) || year < 1900 || year > 2200) throw error('Choose a valid report type, entity and year.', 'invalid-argument');
  return hash(JSON.stringify([type, entityId, year]));
}
export function createLockedReportService({ db, fieldValue }) {
  const refFor = (filters) => db.collection('lockedReports').doc(lockedReportId(filters));
  async function get(filters) {
    const ref = refFor(filters);
    const snap = await ref.get();
    if (!snap.exists) return { locked: false };
    const data = snap.data();
    if (data.version !== 1 || !Number.isInteger(data.chunkCount) || data.chunkCount < 1 || data.chunkCount > 40) throw error('Saved report metadata is invalid. It will not be regenerated.');
    const chunks = await Promise.all(Array.from({ length: data.chunkCount }, (_, index) => ref.collection('content').doc(String(index)).get()));
    if (chunks.some((chunk) => !chunk.exists || typeof chunk.data().text !== 'string')) throw error('Saved report content is missing. It will not be regenerated.');
    const content = chunks.map((chunk) => chunk.data().text).join('');
    if (hash(content) !== data.sha256) throw error('Saved report integrity check failed. It will not be regenerated.');
    const payload = JSON.parse(content);
    return { locked: true, metadata: { ...data, id: snap.id, lockedAt: data.lockedAt?.toMillis?.() || data.lockedAt }, ...payload };
  }
  async function lock({ type, entityId, year, entityName, html, inputs }, actor) {
    const filters = { type, entityId, year };
    const ref = refFor(filters);
    if (typeof html !== 'string' || !html.includes('<html') || !html.includes('Content-Security-Policy') || !html.includes("default-src 'none'")) throw error('A self-contained static report is required.', 'invalid-argument');
    if (!inputs || typeof inputs !== 'object' || Array.isArray(inputs)) throw error('Report calculation inputs are required.', 'invalid-argument');
    const content = JSON.stringify({ html, inputs });
    const bytes = Buffer.byteLength(content);
    if (bytes > 4 * 1024 * 1024) throw error('Report exceeds the 4 MB snapshot limit. No lock was saved.');
    // Smaller documents avoid Firestore's 1 MiB limit even for multibyte text.
    const chunks = content.match(/[\s\S]{1,180000}/gu) || [];
    await db.runTransaction(async (tx) => {
      if ((await tx.get(ref)).exists) return; // Permanent and idempotent: never overwrite.
      tx.set(ref, { version: 1, type, entityId, year, entityName: String(entityName || entityId).slice(0, 300),
        lockedAt: fieldValue.serverTimestamp(), lockedBy: actor.uid, lockedByEmail: actor.email || '',
        sha256: hash(content), bytes, chunkCount: chunks.length });
      chunks.forEach((text, index) => tx.set(ref.collection('content').doc(String(index)), { text }));
    });
    return get(filters);
  }
  return { get, lock };
}
