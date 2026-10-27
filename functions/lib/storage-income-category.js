import { invoiceError } from './storage-invoices.js';

const normalizedLabel = (value) => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();

export function selectStorageIncomeCategory(categories, settings = {}) {
  const configured = String(settings.storageIncomeCategoryId || '').trim();
  const matches = configured ? categories.filter((category) => category.id === configured)
    : categories.filter((category) => category.type === 'income' && normalizedLabel(category.label) === 'entrepot');
  if (matches.length !== 1 || matches[0].type !== 'income') throw invoiceError('Select one valid storage income category in General settings before recording storage payments.');
  const category = matches[0];
  if (!category.label || category.code == null || category.code === '' || !Number.isInteger(Number(category.code)) || Number(category.code) < 0) throw invoiceError('The storage income category needs a valid label and tax code.');
  return category;
}

export async function loadStorageIncomeCategory(db) {
  const [settings, categories] = await Promise.all([db.collection('admin').doc('appSettings').get(), db.collection('categories').get()]);
  return selectStorageIncomeCategory(categories.docs.map((snap) => ({ ...snap.data(), id: snap.id })), settings.data() || {});
}

export function storageIncomeFields(category, stage) {
  if (!category?.id || category.type !== 'income') throw invoiceError('A configured storage income category is required.');
  selectStorageIncomeCategory([category], { storageIncomeCategoryId: category.id });
  if (!['deposit', 'final-payment'].includes(stage)) throw invoiceError('Invalid storage payment stage.');
  return { category: category.label, categoryId: category.id, categoryCode: Number(category.code), categoryType: 'income', storagePaymentStage: stage };
}

export function generatedStoragePaymentStage(entry) {
  if (entry.entryType !== 'income') return null;
  if (entry.storageReceiptId && entry.id === `storage-deposit-${entry.storageReceiptId}`) return 'deposit';
  if (entry.storageInvoiceId && entry.id === `storage-payment-${entry.storageInvoiceId}`) return 'final-payment';
  return null;
}
