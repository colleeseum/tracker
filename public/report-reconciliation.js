const cents = (value) => Math.round((Number(value) || 0) * 100);

export function reconcileReport({ openingBalance, closingBalance, adjustedResult, mileage = 0, cca = 0, capitalLedgerDelta = 0, unclassifiedLedgerDelta = 0 }) {
  const ledgerChange = cents(closingBalance) - cents(openingBalance);
  const expectedChange = cents(adjustedResult) + cents(mileage) + cents(cca) + cents(capitalLedgerDelta) + cents(unclassifiedLedgerDelta);
  return {
    ledgerChange: ledgerChange / 100, adjustedResult: cents(adjustedResult) / 100,
    mileage: cents(mileage) / 100, cca: cents(cca) / 100,
    capitalLedgerDelta: cents(capitalLedgerDelta) / 100, unclassifiedLedgerDelta: cents(unclassifiedLedgerDelta) / 100,
    difference: (ledgerChange - expectedChange) / 100
  };
}
