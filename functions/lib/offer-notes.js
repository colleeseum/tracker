export function defaultOfferNote(price, language) {
  if (price?.mode === 'flat') return language === 'fr' ? 'tarif fixe' : 'flat rate';
  if (price?.mode === 'perFoot' && price.minimum != null && price.minimum !== '' && Number.isFinite(Number(price.minimum))) return 'minimum {{amount}}';
  return '';
}

export function reconcileOfferNotes(note = {}, price = {}, custom = {}) {
  const defaults = new Set(['', 'flat rate', 'tarif fixe', 'minimum {{amount}}']);
  return Object.fromEntries(['en', 'fr'].map((language) => {
    const text = typeof note?.[language] === 'string' ? note[language] : '';
    return [language, custom?.[language] || !defaults.has(text.trim().toLowerCase()) ? text : defaultOfferNote(price, language)];
  }));
}

export function inheritedOfferNotes(offer, template) {
  return reconcileOfferNotes(template?.note ?? offer.note, offer.price, template?.noteCustom ?? offer.noteCustom);
}
