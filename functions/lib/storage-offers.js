export function normalizeAvailability(value) {
  return ['available', 'limited', 'full'].includes(value) ? value : 'available';
}

export function getOfferStorageLocation(offer = {}) {
  if (['indoor', 'outdoor'].includes(offer.storageLocation)) return offer.storageLocation;
  // Retain the location of clearly labelled legacy outdoor offers. Never infer it from an empty allowlist.
  if (offer.storageLocation == null) {
    const label = `${offer.id || ''} ${offer.label?.en || ''} ${offer.label?.fr || ''}`;
    if (/outdoor|ext[ée]rieur/i.test(label)) return 'outdoor';
  }
  return 'indoor';
}

export function getOfferVehicleTypes(offer = {}, templateLookup = new Map()) {
  const template = templateLookup.get(offer.templateId);
  const values = Array.isArray(offer.vehicleTypes)
    ? offer.vehicleTypes
    : offer.vehicleTypes == null && Array.isArray(template?.vehicleTypes) ? template.vehicleTypes : [];
  return [...new Set(values.filter((value) => typeof value === 'string' && value.trim()).map((value) => value.trim()))];
}

export function offerSupportsVehicleType(offer, identifier, pricingData = {}) {
  if (!identifier) return false;
  const entries = [...(pricingData.vehicleTypeLookup?.values() || [])];
  const entry = entries.find((type) => [type.id, type.value, type.slug, ...(type.legacyValues || [])].includes(identifier));
  const candidates = [identifier, entry?.id, entry?.value, entry?.slug, ...(entry?.legacyValues || [])].filter(Boolean);
  return getOfferVehicleTypes(offer, pricingData.templateLookup).some((type) => candidates.includes(type));
}

export function lengthMatchesOffer(lengthValue, range) {
  if (!range) return true;
  if (lengthValue == null || lengthValue === '') return false;
  const length = Number(lengthValue);
  if (!Number.isFinite(length)) return false;
  if (typeof range.min === 'number' && (range.exclusiveMin ? length <= range.min : length < range.min)) return false;
  if (typeof range.max === 'number' && (range.exclusiveMax ? length >= range.max : length > range.max)) return false;
  return true;
}

export function getSelectedOfferForWebsiteRequest(request, pricingData = {}) {
  const location = request.storageLocation ?? 'indoor';
  if (!['indoor', 'outdoor'].includes(location)) return null;
  return (pricingData.offers || [])
    .filter((offer) => offer.seasonId === request.season && getOfferStorageLocation(offer) === location)
    .filter((offer) => offerSupportsVehicleType(offer, request.vehicleType || request.vehicle?.type, pricingData))
    .sort((a, b) => (a.order || 0) - (b.order || 0))
    .find((offer) => lengthMatchesOffer(request.vehicleLength ?? request.vehicle?.lengthFeet, offer.lengthRange)) || null;
}

export function getRequestEligibility(request, pricingData = {}) {
  const season = pricingData.seasonLookup?.get(request.season);
  if (!season || season.active === false) return { allowed: false, reason: 'This storage season is unavailable.' };
  if (normalizeAvailability(season.availability) === 'full') return { allowed: false, reason: 'This storage season is full.' };
  const offer = getSelectedOfferForWebsiteRequest(request, pricingData);
  if (!offer) return { allowed: false, reason: 'No storage offer permits this vehicle and location.' };
  if (normalizeAvailability(offer.availability) === 'full') return { allowed: false, reason: 'This storage offer is full.' };
  return { allowed: true, offer };
}
