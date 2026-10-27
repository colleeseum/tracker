export function findMileageLedgerEntries(log, entries, { dateKey, vendorKey }) {
  const ledger = entries.filter((entry) => entry && !entry.isVirtualOpening);
  if (log.sourceExpenseId) return { kind: 'linked', entries: ledger.filter((entry) => entry.id === log.sourceExpenseId) };
  if (log.sourceTransactionId) return { kind: 'linked', entries: ledger.filter((entry) => (entry.transactionId || entry.id) === log.sourceTransactionId) };
  const date = dateKey(log.date);
  const vendors = new Set((Array.isArray(log.vendorTags) ? log.vendorTags : [log.vendorTag]).map(vendorKey).filter(Boolean));
  if (!date || !vendors.size) return { kind: 'matching', entries: [] };
  return { kind: 'matching', entries: ledger.filter((entry) => entry.entryType === 'expense' && dateKey(entry.date) === date && vendors.has(vendorKey(entry.vendorTag)) && (!log.entityId || entry.entityId === log.entityId)) };
}
