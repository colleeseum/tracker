import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { defaultOfferNote, reconcileOfferNotes, inheritedOfferNotes } from './offer-notes.js';

const perFoot = { mode: 'perFoot', minimum: 300, rate: 13 };
const flat = { mode: 'flat', amount: 380 };

test('flat and per-foot defaults use correct English/French and literal minimum placeholder', () => {
  assert.deepEqual(reconcileOfferNotes({}, flat), { en: 'flat rate', fr: 'tarif fixe' });
  assert.deepEqual(reconcileOfferNotes({ en: 'flat rate', fr: 'tarif fixe' }, perFoot), { en: 'minimum {{amount}}', fr: 'minimum {{amount}}' });
  assert.deepEqual(reconcileOfferNotes({ en: 'minimum {{amount}}', fr: 'minimum {{amount}}' }, flat), { en: 'flat rate', fr: 'tarif fixe' });
  assert.equal(defaultOfferNote({ mode: 'perFoot' }, 'fr'), '');
  assert.equal(defaultOfferNote({ mode: 'perFoot', minimum: 0 }, 'en'), 'minimum {{amount}}');
});
test('custom notes, including manually typed defaults and empty text, survive mode changes', () => {
  const custom = { en: 'Special pad pricing', fr: 'Tarif spécial pour la dalle' };
  assert.deepEqual(reconcileOfferNotes(custom, perFoot), custom);
  assert.deepEqual(reconcileOfferNotes(custom, flat), custom);
  const manuallyTyped = { en: '', fr: 'tarif fixe' };
  assert.deepEqual(reconcileOfferNotes(manuallyTyped, perFoot, { en: true, fr: true }), manuallyTyped);
  assert.deepEqual(reconcileOfferNotes({ en: 'Custom English', fr: 'tarif fixe' }, perFoot), { en: 'Custom English', fr: 'minimum {{amount}}' });
});
test('offers inherit current template notes rather than stale saved copies', () => {
  const offer = { price: perFoot, note: { en: 'Old copy', fr: 'Ancienne copie' } };
  const template = { note: { en: 'flat rate', fr: 'tarif fixe' } };
  assert.deepEqual(inheritedOfferNotes(offer, template), { en: 'minimum {{amount}}', fr: 'minimum {{amount}}' });
  template.note = { en: 'Current template', fr: 'Gabarit actuel' };
  assert.deepEqual(inheritedOfferNotes(offer, template), template.note);
  assert.deepEqual(inheritedOfferNotes(offer, undefined), offer.note);
  assert.deepEqual(inheritedOfferNotes({ ...offer, price: flat }, { note: { en: 'minimum {{amount}}', fr: 'minimum {{amount}}' } }), { en: 'flat rate', fr: 'tarif fixe' });
});

const source = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
const context = vm.createContext({
  offerPriceModeSelect: { value: 'flat' }, offerMinimumInput: { value: '300' },
  offerNoteEnInput: { value: '' }, offerNoteFrInput: { value: '' },
  offerTemplateSelect: { value: 'pad' },
  offerTemplateLookup: new Map([['pad', { note: { en: 'flat rate', fr: 'tarif fixe' } }]]),
  offerNoteCustom: {}
});
for (const name of ['defaultOfferNote', 'reconcileOfferNotes', 'inheritedOfferNotes', 'currentOfferNotePrice', 'loadOfferNotes', 'syncOfferDefaultNotes']) {
  const start = source.indexOf(`function ${name}(`);
  assert.ok(start >= 0);
  const end = source.indexOf('\n}\n', start) + 3;
  vm.runInContext(source.slice(start, end), context);
}
test('offer form loads inherited notes and preserves a manual edit in only its language', () => {
  vm.runInContext('loadOfferNotes()', context);
  assert.equal(context.offerNoteEnInput.value, 'flat rate');
  assert.equal(context.offerNoteFrInput.value, 'tarif fixe');
  context.offerPriceModeSelect.value = 'perFoot';
  vm.runInContext('syncOfferDefaultNotes()', context);
  assert.equal(context.offerNoteEnInput.value, 'minimum {{amount}}');
  assert.equal(context.offerNoteFrInput.value, 'minimum {{amount}}');
  context.offerNoteEnInput.value = 'Custom English';
  context.offerNoteCustom.en = true;
  context.offerPriceModeSelect.value = 'flat';
  vm.runInContext('syncOfferDefaultNotes()', context);
  assert.equal(context.offerNoteEnInput.value, 'Custom English');
  assert.equal(context.offerNoteFrInput.value, 'tarif fixe');
});
test('browser and export note normalization stay identical', () => {
  for (const price of [flat, perFoot, { mode: 'contact' }, { mode: 'perFoot' }]) {
    for (const note of [{}, { en: 'flat rate', fr: 'tarif fixe' }, { en: 'Custom', fr: 'Personnalisé' }]) {
      for (const custom of [{}, { fr: true }]) {
        assert.deepEqual(JSON.parse(JSON.stringify(context.reconcileOfferNotes(note, price, custom))), reconcileOfferNotes(note, price, custom));
      }
    }
  }
});
