// A client may have one optional CC address. Keep the existing office copy.
export function mergeClientEmailCc(officeCc, client = {}) {
  const raw = client.emailCc;
  if (raw != null && typeof raw !== 'string') throw new Error('Client CC must be one valid email address.');
  const emailCc = (raw || '').trim();
  if (emailCc && !/^[^\s@,;<>]+@[^\s@,;<>]+$/.test(emailCc)) throw new Error('Client CC must be one valid email address.');
  const seen = new Set([String(client.email || '').trim().toLowerCase()]);
  return [officeCc, emailCc].filter((address) => {
    const key = String(address || '').trim().toLowerCase();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  }).map((address) => address.trim()).join(', ');
}
