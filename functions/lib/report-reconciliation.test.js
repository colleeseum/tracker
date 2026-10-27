import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { reconcileReport } from '../../public/report-reconciliation.js';

test('dashboard example includes T5 once and reconciles unpaid mileage to the balance change', () => {
  const result = reconcileReport({ openingBalance: 9969.74, closingBalance: 9007.67, adjustedResult: -1995.61 + 89.04, mileage: 944.50 });
  assert.equal(result.adjustedResult, -1906.57);
  assert.equal(result.ledgerChange, -962.07);
  assert.equal(result.difference, 0);
});

test('CCA, capital cash movements and unclassified entries are explicit reconciliation adjustments', () => {
  const result = reconcileReport({ openingBalance: 1000, closingBalance: 1150, adjustedResult: 550, mileage: 100, cca: 150, capitalLedgerDelta: -700, unclassifiedLedgerDelta: 50 });
  assert.equal(result.ledgerChange, 150);
  assert.equal(result.difference, 0);
  assert.equal(result.capitalLedgerDelta, -700);
  assert.equal(result.cca, 150);
});

test('rounding uses cents and unexplained differences remain visible rather than becoming plug adjustments', () => {
  assert.equal(reconcileReport({ openingBalance: 0.1, closingBalance: 0.3, adjustedResult: 0.2 }).difference, 0);
  assert.equal(reconcileReport({ openingBalance: 0, closingBalance: 100, adjustedResult: 90 }).difference, 10);
});

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
function loadFunction(context, name) {
  const start = source.indexOf(`function ${name}(`);
  vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), context);
}
test('tax section clearly distinguishes its estimate from the cash banner and does not claim T776 support', () => {
  const context = vm.createContext({
    document: { createElement: () => ({ innerHTML: '', children: [], appendChild(child) { this.children.push(child); } }) },
    formatCurrency: (amount) => Number(amount).toFixed(2)
  });
  loadFunction(context, 'buildReportTotalsSummary');
  const section = context.buildReportTotalsSummary({ total2042Income: 1000, total2042Expenses: 2995.61, net2042Income: -1995.61, netIncome: -1906.57, t5IncomeTotal: 89.04,
    reconciliation: reconcileReport({ openingBalance: 9969.74, closingBalance: 9007.67, adjustedResult: -1906.57, mileage: 944.5 }) });
  assert.ok(section.innerHTML.includes('Estimated net tax income'));
  assert.ok(section.innerHTML.includes('Overall adjusted result (includes T5)'));
  assert.ok(section.innerHTML.includes('-1906.57'));
  assert.ok(section.innerHTML.includes('T776 category mappings and deduction rules have not been configured'));
  assert.ok(section.children[0].innerHTML.includes('944.50'));
  assert.ok(section.children[0].innerHTML.includes('-962.07'));
  assert.ok(section.children[0].innerHTML.includes('Reconciled:'));
  const header = source.slice(source.indexOf('function renderLiveReports('), source.indexOf('function buildTagReportData('));
  assert.ok(header.includes('<span>Net ledger change</span>'));
  assert.ok(!header.includes('<span>Net income</span>'));
  assert.ok(!header.includes('<span>Net 2042 income</span>'));
});

test('excluded movements are restricted to this entity/year and respect capital refunds', () => {
  const context = vm.createContext({
    expenses: [
      { entityId: 'farm', date: '2026-03-01', entryType: 'expense', amount: 700, ccaPoolId: 'pool' },
      { entityId: 'farm', date: '2026-04-01', entryType: 'expense', amount: 100, isReturn: true, isCcaClass: true },
      { entityId: 'farm', date: '2026-04-01', entryType: 'income', amount: 50, metaType: 'transfer' },
      { entityId: 'farm', date: '2026-04-01', entryType: 'expense', amount: 90 },
      { entityId: 'other', date: '2026-04-01', entryType: 'expense', amount: 999, ccaPoolId: 'pool' },
      { entityId: 'farm', date: '2025-04-01', entryType: 'expense', amount: 999, ccaPoolId: 'pool' }
    ], toDateObject: (value) => new Date(value), getCategoryMetaForEntry: (entry) => ({ type: entry.metaType || entry.entryType })
  });
  loadFunction(context, 'getEntryDelta'); loadFunction(context, 'computeReportExcludedLedgerMovements');
  const result = context.computeReportExcludedLedgerMovements('farm', 2026);
  assert.equal(result.capitalLedgerDelta, -600);
  assert.equal(result.unclassifiedLedgerDelta, 50);
});
