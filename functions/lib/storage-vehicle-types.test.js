import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
function fixture(types, selected = '') {
  const select = { value: selected, options: [], replaceChildren() { this.options = []; }, appendChild(option) { this.options.push(option); } };
  const context = vm.createContext({
    storageVehicleTypeSelect: select, vehicleTypes: types,
    vehicleTypeLookup: new Map(types.map((type) => [type.id, type])),
    DEFAULT_VEHICLE_TYPE_LABELS: { motorcycle: { en: 'Motorcycle' } },
    document: { createElement: () => ({ value: '', textContent: '', selected: false }) }
  });
  for (const name of ['findVehicleTypeEntry', 'getVehicleTypeDisplayLabel', 'getVehicleTypeCandidates', 'updateStorageVehicleTypeOptions']) {
    const start = source.indexOf(`function ${name}(`);
    vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), context);
  }
  return { select, context };
}
const types = [
  { id: 'rv-record', value: 'rv', labels: { en: 'RV / Motorhome', fr: 'VR' } },
  { id: 'motorcycle-record', value: 'motorcycle', slug: 'moto', labels: { en: 'Motorcycle', fr: 'Moto' } },
  { id: 'snowmobile-record', value: 'snowmobile', labels: { en: 'Snowmobile', fr: 'Motoneige' } }
];

test('new requests list all configured types, including Motorcycle and types without order', () => {
  const { select, context } = fixture(types);
  context.updateStorageVehicleTypeOptions();
  assert.deepEqual(select.options.map((option) => option.value), ['', 'rv-record', 'motorcycle-record', 'snowmobile-record']);
  assert.equal(select.options[2].textContent, 'Motorcycle');
  assert.equal(select.value, '');
});
test('editing and snapshot refresh preserve canonical, slug and legacy values without duplicate options', () => {
  for (const selected of ['motorcycle-record', 'motorcycle', 'moto']) {
    const { select, context } = fixture(types, selected);
    context.updateStorageVehicleTypeOptions();
    assert.equal(select.value, selected);
    assert.equal(select.options.filter((option) => option.textContent === 'Motorcycle').length, 1);
    assert.ok(select.options.some((option) => option.selected && option.value === selected));
    context.updateStorageVehicleTypeOptions();
    assert.equal(select.value, selected);
  }
});
test('deleted vehicle type remains selectable for an existing request but is not offered on new requests', () => {
  const { select, context } = fixture(types, 'old-type');
  context.updateStorageVehicleTypeOptions();
  assert.equal(select.options.at(-1).value, 'old-type');
  assert.match(select.options.at(-1).textContent, /existing type/);
  context.updateStorageVehicleTypeOptions('');
  assert.ok(!select.options.some((option) => option.value === 'old-type'));
});
test('empty configuration does not invent available or priceable vehicle types', () => {
  const { select, context } = fixture([]);
  context.updateStorageVehicleTypeOptions();
  assert.equal(select.options.length, 1);
  assert.equal(select.options[0].value, '');
});
test('vehicle subscription includes records missing order and refreshes request options', () => {
  const start = source.indexOf('function subscribeToVehicleTypes(');
  const subscription = source.slice(start, source.indexOf('\n}\n', start) + 3);
  assert.ok(!subscription.includes("orderBy('order')"));
  assert.ok(subscription.includes('updateStorageVehicleTypeOptions();'));
});
