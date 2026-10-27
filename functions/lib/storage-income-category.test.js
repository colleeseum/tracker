import test from 'node:test';
import assert from 'node:assert/strict';
import { selectStorageIncomeCategory, storageIncomeFields, generatedStoragePaymentStage, loadStorageIncomeCategory } from './storage-income-category.js';

test('both payment stages link to the configured category and inherit its code without hard-coding 9600', () => {
  const category = { id: 'storage', label: 'Entrepôt', type: 'income', code: 1234 };
  for (const stage of ['deposit', 'final-payment']) assert.deepEqual(storageIncomeFields(category, stage), {
    category: 'Entrepôt', categoryId: 'storage', categoryCode: 1234, categoryType: 'income', storagePaymentStage: stage
  });
  assert.equal(storageIncomeFields({ ...category, code: 0 }, 'deposit').categoryCode, 0);
  assert.equal(selectStorageIncomeCategory([category]).id, 'storage');
  assert.equal(selectStorageIncomeCategory([{ ...category, label: 'Renamed storage income' }], { storageIncomeCategoryId: 'storage' }).id, 'storage');
});

test('missing, deleted, ambiguous, expense or invalid categories stop creation rather than creating unmapped income', () => {
  const category = { id: 'storage', label: 'Entrepot', type: 'income', code: 9600 };
  assert.throws(() => selectStorageIncomeCategory([]), /valid storage income category/);
  assert.throws(() => selectStorageIncomeCategory([category, { ...category, id: 'duplicate' }]), /valid storage income category/);
  assert.throws(() => selectStorageIncomeCategory([category], { storageIncomeCategoryId: 'deleted' }), /valid storage income category/);
  assert.throws(() => selectStorageIncomeCategory([{ ...category, type: 'expense' }], { storageIncomeCategoryId: 'storage' }), /valid storage income category/);
  for (const code of [null, '', NaN, -1]) assert.throws(() => storageIncomeFields({ ...category, code }, 'deposit'), /tax code/);
});

test('category lookup reads saved configuration and the current category record', async () => {
  const db = { collection: (name) => name === 'admin'
    ? { doc: () => ({ get: async () => ({ data: () => ({ storageIncomeCategoryId: 'chosen' }) }) }) }
    : { get: async () => ({ docs: [{ id: 'chosen', data: () => ({ label: 'Rentals', type: 'income', code: 8123 }) }] }) } };
  assert.equal((await loadStorageIncomeCategory(db)).code, 8123);
});

test('correction detection requires exact generated IDs and payment references, not just a category label', () => {
  assert.equal(generatedStoragePaymentStage({ id: 'storage-deposit-R1', storageReceiptId: 'R1', entryType: 'income' }), 'deposit');
  assert.equal(generatedStoragePaymentStage({ id: 'storage-payment-INV1', storageInvoiceId: 'INV1', entryType: 'income' }), 'final-payment');
  assert.equal(generatedStoragePaymentStage({ id: 'manual', category: 'Deposit', entryType: 'income' }), null);
  assert.equal(generatedStoragePaymentStage({ id: 'storage-deposit-other', storageReceiptId: 'R1', entryType: 'income' }), null);
  assert.equal(generatedStoragePaymentStage({ id: 'storage-payment-INV1', storageInvoiceId: 'INV1', entryType: 'expense' }), null);
});
