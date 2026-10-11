import assert from 'node:assert/strict';

// Read-only integration checks against local data. Stop before insurance/captcha,
// client creation, request persistence, or email transmission.
if (process.env.FIRESTORE_EMULATOR_HOST !== '127.0.0.1:8081') throw new Error('Local emulator required.');
const { createStorageRequest, createStorageRequests, getWebsiteContent } = await import('../index.js');
const context = { auth: { uid: 'local-test', token: { email: 'sergecolle@gmail.com' } }, rawRequest: {} };
const tenant = { tenantName: 'Local Test', tenantPhone: '5550000000', email: 'local-test@example.invalid', tenantAddress: 'Test', tenantCity: 'Test', tenantProvince: 'QC', tenantPostal: 'H0H0H0', season: 'winter' };
// The single-offer endpoint uses flat tenant field names.
const base = { ...tenant, storageLocation: 'outdoor', vehicleLength: 25 };
for (const vehicleType of ['rv', 'motorcycle', 'snowmobile']) {
  await assert.rejects(createStorageRequest.run({ ...base, vehicleType }, context), (error) => {
    if (vehicleType === 'rv') assert.match(error.message, /Missing required insurance/);
    else assert.match(error.message, /No storage offer permits/);
    return true;
  });
  console.log(`single ${vehicleType}: eligibility ${vehicleType === 'rv' ? 'passed (stopped before writes)' : 'rejected'}`);
}
for (const vehicleType of ['motorcycle', 'snowmobile']) {
  await assert.rejects(createStorageRequests.run({ tenant, requests: [{ vehicleType, storageLocation: 'outdoor', vehicleLength: 25 }] }, context), /No storage offer permits/);
  console.log(`multi ${vehicleType}: rejected before writes`);
}
const content = await getWebsiteContent.run({}, context);
const seasons = content.STORAGE_SEASONS;
assert.ok(seasons, `Unexpected export keys: ${Object.keys(content)}`);
const outdoor = seasons.find((season) => season.id === 'winter').offers.find((offer) => offer.id === 'winter-rv-outdoor');
assert.equal(outdoor.storageLocation, 'outdoor');
assert.deepEqual(outdoor.vehicleTypes, ['rv', 'car', 'truck']);
assert.equal(outdoor.availability, 'available');
assert.equal(outdoor.price.rate, 13);
assert.equal(outdoor.price.minimum, 300);
assert.equal(seasons.find((season) => season.id === 'summer').availability, 'available');
console.log('Website callable exports explicit restrictions and normalized legacy availability.');
