import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import admin from 'firebase-admin';
import { fileURLToPath } from 'node:url';
import { serializeData } from './utils/firestore-serialization.mjs';
import { selectStorageIncomeCategory, storageIncomeFields, generatedStoragePaymentStage } from '../lib/storage-income-category.js';

if (!/^(127\.0\.0\.1|localhost):\d+$/.test(process.env.FIRESTORE_EMULATOR_HOST || '')) throw new Error('This correction is LOCAL ONLY. Explicit loopback Firestore emulator configuration is required.');
const index = process.argv.indexOf('--year');
const year = Number(index >= 0 ? process.argv[index + 1] : NaN);
if (!Number.isInteger(year) || year < 1900 || year > 2200) throw new Error('Specify the year to correct with --year YYYY. Other years are never modified.');
const apply = process.argv.includes('--apply');
admin.initializeApp({ projectId: 'tracker-187c5' });
const db = admin.firestore();
const hash = (value) => crypto.createHash('sha256').update(JSON.stringify(serializeData(value))).digest('hex');
try {
  const settingsRef = db.collection('admin').doc('appSettings');
  const [settingsSnap, categories, entries] = await Promise.all([settingsRef.get(), db.collection('categories').get(), db.collection('expenses').get()]);
  const settings = settingsSnap.data() || {};
  const category = selectStorageIncomeCategory(categories.docs.map((snap) => ({ ...snap.data(), id: snap.id })), settings);
  const changes = []; let skippedCustom = 0;
  for (const snap of entries.docs) {
    const entry = { ...snap.data(), id: snap.id };
    const stage = generatedStoragePaymentStage(entry);
    const date = entry.date?.toDate?.() || new Date(entry.date);
    if (!stage || Number.isNaN(date.getTime()) || Number(new Intl.DateTimeFormat('en-CA', { year: 'numeric', timeZone: 'America/Toronto' }).format(date)) !== year) continue;
    if (entry.categoryId && entry.categoryId !== category.id) { skippedCustom++; continue; }
    const fields = storageIncomeFields(category, stage);
    if (Object.entries(fields).every(([key, value]) => entry[key] === value)) continue;
    changes.push({ ref: snap.ref, before: snap.data(), fields });
  }
  console.log(`Local ${year}: ${changes.length} generated ledger entries need correction to ${category.label} (configured code ${category.code}); ${skippedCustom} custom-category entries left untouched.`);
  if (!apply) { console.log('Dry run only. Use --apply to back up and correct these local records.'); }
  else if (!changes.length && settings.storageIncomeCategoryId === category.id) console.log('Already corrected. No writes.');
  else {
    const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
    const backupPath = path.join(root, 'backups', `local-storage-income-${year}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
    await fs.mkdir(path.dirname(backupPath), { recursive: true });
    await fs.writeFile(backupPath, JSON.stringify({ emulator: process.env.FIRESTORE_EMULATOR_HOST, year, createdAt: new Date().toISOString(),
      category: serializeData(category), previousSettings: serializeData(settings), entries: changes.map((change) => ({ path: change.ref.path, data: serializeData(change.before) })) }, null, 2), { mode: 0o600 });
    await db.runTransaction(async (tx) => {
      const [currentSettings, currentCategory, ...currentEntries] = await Promise.all([
        tx.get(settingsRef), tx.get(db.collection('categories').doc(category.id)), ...changes.map((change) => tx.get(change.ref))
      ]);
      if (hash(currentSettings.data() || {}) !== hash(settings) || hash(currentCategory.data()) !== hash(categories.docs.find((snap) => snap.id === category.id).data())) throw new Error('Settings/category changed. Re-run the preview.');
      currentEntries.forEach((snap, index) => { if (!snap.exists || hash(snap.data()) !== hash(changes[index].before)) throw new Error('A ledger entry changed. No corrections applied; rerun after reviewing.'); });
      changes.forEach((change) => tx.set(change.ref, { ...change.fields, updatedAt: admin.firestore.FieldValue.serverTimestamp(), updatedBy: 'local-storage-category-correction' }, { merge: true }));
      tx.set(settingsRef, { storageIncomeCategoryId: category.id, updatedAt: admin.firestore.FieldValue.serverTimestamp(), updatedBy: 'local-storage-category-correction' }, { merge: true });
    });
    const allowed = new Set(['category', 'categoryId', 'categoryCode', 'categoryType', 'storagePaymentStage', 'updatedAt', 'updatedBy']);
    for (const change of changes) {
      const after = (await change.ref.get()).data();
      for (const [key, value] of Object.entries(change.before)) if (!allowed.has(key) && hash(after[key]) !== hash(value)) throw new Error(`Verification failed: ${change.ref.path} changed ${key}`);
      if (!Object.entries(change.fields).every(([key, value]) => after[key] === value)) throw new Error('Classification verification failed.');
    }
    console.log(`Corrected and verified ${changes.length} entries. Amounts, dates, titles, descriptions and payment links unchanged. Backup: ${backupPath}`);
    console.log('Production, invoices, receipts, vehicle statuses and locked reports were not modified.');
  }
} finally { await db.terminate(); }
