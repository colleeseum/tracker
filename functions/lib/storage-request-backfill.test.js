import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
const start = source.indexOf('async function backfillStorageRequestCaseIds(');
const implementation = source.slice(start, source.indexOf('\n}\n', start) + 3);
function fixture(records) {
  const writes = [];
  const context = vm.createContext({
    storageCaseBackfillInFlight: false, db: {}, auth: { currentUser: { uid: 'test-admin' } },
    serverTimestamp: () => 'timestamp', doc: (_db, _collection, id) => id,
    resolveStorageCaseId: (request) => `${request.season}__${request.clientId}`,
    console: { warn: () => {} },
    writeBatch: () => ({
      set: () => { throw new Error('Backfill must not create requests'); },
      update: (id, patch) => { writes.push({ id, patch }); },
      commit: async () => {
        if (writes.some(({ id }) => !records.has(id))) throw new Error('Request was deleted');
        writes.forEach(({ id, patch }) => records.set(id, { ...records.get(id), ...patch }));
      }
    })
  });
  vm.runInContext(implementation, context);
  return { context, writes };
}
test('queued backfill cannot resurrect a deleted request', async () => {
  const records = new Map();
  const { context } = fixture(records);
  await context.backfillStorageRequestCaseIds([{ id: 'deleted-fixture', clientId: 'test-client', season: 'winter' }]);
  assert.equal(records.size, 0);
  assert.equal(context.storageCaseBackfillInFlight, false);
});
test('backfill preserves existing fields while adding missing case and location', async () => {
  const request = { id: 'live', clientId: 'customer', season: 'winter', vehicle: { type: 'rv' }, status: 'scheduled', contractAmount: 460 };
  const records = new Map([['live', request]]);
  const { context } = fixture(records);
  await context.backfillStorageRequestCaseIds([request]);
  assert.equal(records.get('live').caseId, 'winter__customer');
  assert.equal(records.get('live').storageLocation, 'indoor');
  assert.equal(records.get('live').contractAmount, 460);
  assert.equal(records.get('live').status, 'scheduled');
  assert.deepEqual(records.get('live').vehicle, request.vehicle);
});
test('incomplete artifacts and already backfilled records receive no writes', async () => {
  const { context, writes } = fixture(new Map());
  await context.backfillStorageRequestCaseIds([
    { id: 'blank' },
    { id: 'missing-client', season: 'winter' },
    { id: 'missing-season', clientId: 'customer' },
    { id: 'complete', clientId: 'customer', season: 'winter', caseId: 'winter__customer', storageLocation: 'outdoor' }
  ]);
  assert.equal(writes.length, 0);
});
