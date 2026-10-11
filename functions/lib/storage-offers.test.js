import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeAvailability, getOfferStorageLocation, getOfferVehicleTypes, getRequestEligibility } from './storage-offers.js';

const pricing = () => ({
  seasonLookup: new Map([['winter', { availability: 'available' }]]),
  templateLookup: new Map([['legacy', { vehicleTypes: ['rv'] }]]),
  vehicleTypeLookup: new Map([['rv', { id: 'rv', value: 'RV/Motorhome', slug: 'rv-motorhome' }]]),
  offers: [
    { id: 'indoor', seasonId: 'winter', storageLocation: 'indoor', vehicleTypes: ['rv'], availability: 'full', price: { mode: 'perFoot', rate: 23, minimum: 460 } },
    { id: 'outdoor', seasonId: 'winter', storageLocation: 'outdoor', vehicleTypes: ['rv', 'car', 'truck'], price: { mode: 'perFoot', rate: 13, minimum: 300 } }
  ]
});
const request = (vehicleType = 'rv', storageLocation = 'outdoor') => ({ season: 'winter', vehicleType, storageLocation, vehicleLength: 25 });

test('missing and invalid availability normalize without migrating records', () => {
  for (const value of [undefined, null, '', 'FULL', 'invalid', 0, {}, []]) assert.equal(normalizeAvailability(value), 'available');
  for (const value of ['available', 'limited', 'full']) assert.equal(normalizeAvailability(value), value);
});
test('indoor full blocks indoor only; outdoor permitted types remain requestable', () => {
  const data = pricing();
  assert.equal(getRequestEligibility(request('rv', 'indoor'), data).allowed, false);
  for (const type of ['rv', 'car', 'truck', 'RV/Motorhome', 'rv-motorhome']) assert.equal(getRequestEligibility(request(type), data).allowed, true);
  for (const type of ['motorcycle', 'snowmobile', 'snowmobile_single', 'snowmobile_double']) assert.equal(getRequestEligibility(request(type), data).allowed, false);
  assert.equal(getRequestEligibility(request('rv', 'invalid'), data).allowed, false);
});
test('full season blocks every location, limited and legacy season remain requestable', () => {
  const data = pricing();
  data.seasonLookup.set('winter', { availability: 'full' });
  for (const location of ['indoor', 'outdoor']) assert.match(getRequestEligibility(request('rv', location), data).reason, /season is full/);
  for (const availability of ['available', 'limited', undefined, 'invalid']) {
    data.seasonLookup.set('winter', { availability });
    assert.equal(getRequestEligibility(request(), data).allowed, true);
  }
  data.offers[1].availability = 'full';
  assert.match(getRequestEligibility(request(), data).reason, /offer is full/);
});
test('empty allowlist denies all; missing allowlist falls back only to a configured template', () => {
  const data = pricing();
  const offer = data.offers[1];
  offer.templateId = 'legacy';
  offer.vehicleTypes = [];
  assert.deepEqual(getOfferVehicleTypes(offer, data.templateLookup), []);
  assert.equal(getRequestEligibility(request(), data).allowed, false);
  delete offer.vehicleTypes;
  assert.equal(getRequestEligibility(request(), data).allowed, true);
  delete offer.templateId;
  assert.equal(getRequestEligibility(request(), data).allowed, false);
});
test('legacy locations default safely and price fields remain untouched', () => {
  assert.equal(getOfferStorageLocation({}), 'indoor');
  assert.equal(getOfferStorageLocation({ id: 'winter-rv-outdoor' }), 'outdoor');
  assert.equal(getOfferStorageLocation({ storageLocation: 'invalid', id: 'outdoor' }), 'indoor');
  const data = pricing();
  const before = structuredClone(data.offers);
  getRequestEligibility(request(), data);
  assert.deepEqual(data.offers, before);
});
test('length restrictions cannot fall through to the last offer', () => {
  const data = pricing();
  data.offers[1].lengthRange = { max: 30 };
  assert.equal(getRequestEligibility({ ...request(), vehicleLength: 31 }, data).allowed, false);
  assert.equal(getRequestEligibility({ ...request(), vehicleLength: 30 }, data).allowed, true);
});
