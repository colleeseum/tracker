import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { loadActiveProfile } from '../../lib/profile-config.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const { projectId, useEmulators } = await loadActiveProfile();

let db = null;
let emulatorBaseUrl = null;

const shouldUseEmulatorRest =
  useEmulators &&
  (process.env.EXPORT_SITE_DATA_USE_EMULATOR_REST === '1' ||
    process.env.EXPORT_SITE_DATA_USE_EMULATOR_REST === 'true');

if (shouldUseEmulatorRest) {
  // Fallback path: emulator REST API (no OAuth) for environments where gRPC is flaky.
  // Note: emulator REST enforces Firestore security rules, so this only works if your
  // rules allow reads or you supply an auth token (not implemented here).
  const host = process.env.FIRESTORE_EMULATOR_HOST;
  if (!host) {
    throw new Error(
      'Missing FIRESTORE_EMULATOR_HOST while useEmulators=true. Start emulators and ensure the local profile is active.',
    );
  }
  emulatorBaseUrl = `http://${host}/v1/projects/${projectId}/databases/(default)/documents`;
} else {
  // Default path: firebase-admin (works with emulator without requiring auth and ignores rules).
  const adminModule = await import('firebase-admin');
  const admin = adminModule.default || adminModule;
  admin.initializeApp({ projectId });
  db = admin.firestore();
  // Prefer REST when talking to production to avoid gRPC edge cases.
  if (!useEmulators) {
    db.settings({ preferRest: true });
  }
}

function resolveOutputPath() {
  const outFlagIndex = process.argv.findIndex((arg) => arg === '--out');
  if (outFlagIndex !== -1 && process.argv[outFlagIndex + 1]) {
    return path.resolve(process.argv[outFlagIndex + 1]);
  }
  return path.resolve(__dirname, '../../../entrepot/static/generated/website-text.generated.js');
}

function serialize(value) {
  return JSON.stringify(value, null, 2);
}

function toPlainTimestamp(timestamp) {
  if (!timestamp) return null;
  if (typeof timestamp.toDate === 'function') {
    return timestamp.toDate().toISOString();
  }
  return null;
}

async function fetchCollection(name, orderField) {
  if (emulatorBaseUrl) {
    // Firestore REST documents use typed field encodings.
    const decodeValue = (value) => {
      if (!value || typeof value !== 'object') return null;
      if (value.stringValue !== undefined) return value.stringValue;
      if (value.booleanValue !== undefined) return Boolean(value.booleanValue);
      if (value.integerValue !== undefined) return Number(value.integerValue);
      if (value.doubleValue !== undefined) return Number(value.doubleValue);
      if (value.timestampValue !== undefined) return value.timestampValue;
      if (value.nullValue !== undefined) return null;
      if (value.mapValue) {
        const fields = value.mapValue.fields || {};
        const out = {};
        Object.entries(fields).forEach(([k, v]) => {
          out[k] = decodeValue(v);
        });
        return out;
      }
      if (value.arrayValue) {
        const values = Array.isArray(value.arrayValue.values)
          ? value.arrayValue.values
          : [];
        return values.map(decodeValue);
      }
      return null;
    };

    const decodeFields = (fields) => {
      const out = {};
      Object.entries(fields || {}).forEach(([k, v]) => {
        out[k] = decodeValue(v);
      });
      return out;
    };

    const docs = [];
    let pageToken = null;
    do {
      const url = new URL(`${emulatorBaseUrl}/${name}`);
      // Sorting is done client-side to keep the export simple and deterministic.
      url.searchParams.set('pageSize', '1000');
      if (pageToken) url.searchParams.set('pageToken', pageToken);
      const res = await fetch(url.toString());
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Failed to read emulator ${name}: ${res.status} ${body}`);
      }
      const json = await res.json();
      const pageDocs = Array.isArray(json.documents) ? json.documents : [];
      pageDocs.forEach((doc) => {
        const fullName = doc.name || '';
        const id = fullName.split('/').pop();
        docs.push({ id, ...decodeFields(doc.fields) });
      });
      pageToken = json.nextPageToken || null;
    } while (pageToken);

    if (orderField) {
      docs.sort((a, b) => {
        const av = a?.[orderField];
        const bv = b?.[orderField];
        const an = typeof av === 'number' ? av : Number.isFinite(Number(av)) ? Number(av) : null;
        const bn = typeof bv === 'number' ? bv : Number.isFinite(Number(bv)) ? Number(bv) : null;
        if (an !== null && bn !== null && an !== bn) return an - bn;
        return String(a?.id || '').localeCompare(String(b?.id || ''));
      });
    }

    return docs;
  }

  let ref = db.collection(name);
  if (orderField) {
    ref = ref.orderBy(orderField);
  }
  const snapshot = await ref.get();
  return snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
}

function normalizeLocaleMap(value) {
  if (!value) return null;
  if (typeof value === 'string') {
    return { en: value, fr: value };
  }
  const en = typeof value.en === 'string' ? value.en : '';
  const fr = typeof value.fr === 'string' ? value.fr : '';
  if (!en && !fr) {
    return null;
  }
  return { en, fr };
}

function normalizePrice(price) {
  if (!price || typeof price !== 'object') {
    return null;
  }
  const mode = price.mode || null;
  if (!mode) return null;
  if (mode === 'flat') {
    const amount = Number(price.amount);
    return Number.isFinite(amount) ? { mode, amount } : null;
  }
  if (mode === 'perFoot') {
    const rate = Number(price.rate);
    const minimum = Number(price.minimum);
    return {
      mode,
      rate: Number.isFinite(rate) ? rate : 0,
      minimum: Number.isFinite(minimum) ? minimum : 0,
      unit: normalizeLocaleMap(price.unit) || price.unit || { en: '/ ft', fr: '/ pi' }
    };
  }
  return { mode: 'contact' };
}

function normalizeLengthRange(offer) {
  const range = offer.lengthRange && typeof offer.lengthRange === 'object' ? offer.lengthRange : {};
  const normalized = {};
  const min = typeof range.min === 'number' ? range.min : typeof offer.minLength === 'number' ? offer.minLength : null;
  const max = typeof range.max === 'number' ? range.max : typeof offer.maxLength === 'number' ? offer.maxLength : null;
  if (min !== null) normalized.min = min;
  if (max !== null) normalized.max = max;
  if (range.exclusiveMin || offer.exclusiveMin) normalized.exclusiveMin = true;
  if (range.exclusiveMax || offer.exclusiveMax) normalized.exclusiveMax = true;
  if (
    normalized.min === undefined &&
    normalized.max === undefined &&
    !normalized.exclusiveMin &&
    !normalized.exclusiveMax
  ) {
    return undefined;
  }
  return normalized;
}

function normalizePolicy(policy) {
  if (!policy) return null;
  if (typeof policy === 'string') {
    return { text: { en: policy, fr: policy } };
  }
  if (policy.text || policy.tooltip || policy.tooltipKey) {
    const entry = {};
    if (policy.text) {
      entry.text = normalizeLocaleMap(policy.text) || { en: '', fr: '' };
    }
    if (policy.tooltip) {
      entry.tooltip = normalizeLocaleMap(policy.tooltip);
    }
    if (policy.tooltipKey) {
      entry.tooltipKey = policy.tooltipKey;
    }
    return entry;
  }
  const text = normalizeLocaleMap(policy);
  if (!text) return null;
  return { text };
}

function normalizeOffer(offer) {
  const normalizedPrice = normalizePrice(offer.price);
  return {
    id: offer.id,
    label: normalizeLocaleMap(offer.label) || { en: '', fr: '' },
    price: normalizedPrice,
    vehicleTypes: Array.isArray(offer.vehicleTypes)
      ? offer.vehicleTypes.filter((value) => typeof value === 'string' && value.length > 0)
      : [],
    note: normalizeLocaleMap(offer.note),
    hideInTable: Boolean(offer.hideInTable),
    order: typeof offer.order === 'number' ? offer.order : 0,
    lengthRange: normalizeLengthRange(offer),
    updatedAt: toPlainTimestamp(offer.updatedAt)
  };
}

const slugify = (value = '') =>
  value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-');

function normalizeVehicleType(entry) {
  if (!entry) return null;
  const labels = normalizeLocaleMap(entry.label) || { en: '', fr: '' };
  const value =
    (typeof entry.value === 'string' && entry.value.trim()) ||
    (typeof entry.type === 'string' && entry.type.trim()) ||
    entry.id ||
    labels.en ||
    labels.fr ||
    '';
  if (!value) return null;
  const legacyValues = Array.isArray(entry.legacyValues)
    ? entry.legacyValues.filter((item) => typeof item === 'string' && item.trim().length > 0)
    : [];
  return {
    id: entry.id,
    value,
    labels,
    slug: typeof entry.slug === 'string' && entry.slug.trim() ? entry.slug.trim() : slugify(value),
    legacyValues,
    order: typeof entry.order === 'number' ? entry.order : 0,
    active: entry.active !== false,
    updatedAt: toPlainTimestamp(entry.updatedAt)
  };
}

function normalizeSeason(season, offersBySeason) {
  const normalizedDuration =
    normalizeLocaleMap(season.duration) ||
    normalizeLocaleMap(season.timeframe) ||
    null;
  const normalizedRuleTitle = normalizeLocaleMap(season.ruleTitle);
  const policies = Array.isArray(season.policies)
    ? season.policies.map(normalizePolicy).filter(Boolean)
    : [];
  const offers = offersBySeason.get(season.id) || [];
  offers.sort((a, b) => (a.order || 0) - (b.order || 0));
  return {
    id: season.id,
    name: normalizeLocaleMap(season.name) || { en: '', fr: '' },
    seasonLabel: normalizeLocaleMap(season.label) || normalizeLocaleMap(season.name) || { en: '', fr: '' },
    timeframe: normalizeLocaleMap(season.timeframe),
    duration: normalizedDuration,
    dropoffWindow: normalizeLocaleMap(season.dropoffWindow),
    pickupDeadline: normalizeLocaleMap(season.pickupDeadline),
    description: normalizeLocaleMap(season.description),
    ruleTitle: normalizedRuleTitle,
    policies,
    offers,
    order: typeof season.order === 'number' ? season.order : 0,
    active: season.active !== false,
    updatedAt: toPlainTimestamp(season.updatedAt)
  };
}

async function main() {
  const outputPath = resolveOutputPath();

  const [addOns, seasonAddOns, conditions, etiquette, i18nDocs, seasons, offers, vehicleTypes] = await Promise.all([
    fetchCollection('storageAddOns', 'order'),
    fetchCollection('storageSeasonAddOns', 'order'),
    fetchCollection('storageConditions', 'order'),
    fetchCollection('storageEtiquette', 'order'),
    fetchCollection('i18nEntries'),
    fetchCollection('storageSeasons', 'order'),
    fetchCollection('storageOffers', 'order'),
    fetchCollection('vehicleTypes', 'order')
  ]);

  const normalizedAddOns = addOns.map((entry) => ({
    id: entry.id,
    code: entry.code || entry.id,
    name: normalizeLocaleMap(entry.name) || { en: '', fr: '' },
    description: normalizeLocaleMap(entry.description),
    order: typeof entry.order === 'number' ? entry.order : 0,
    active: entry.active !== false,
    updatedAt: toPlainTimestamp(entry.updatedAt)
  }));

  const normalizedSeasonAddOns = seasonAddOns.map((entry) => ({
    id: entry.id,
    seasonId: entry.seasonId || null,
    code: entry.code || entry.addonId || entry.id,
    price: typeof entry.price === 'number' ? entry.price : Number(entry.price) || 0,
    order: typeof entry.order === 'number' ? entry.order : 0,
    active: entry.active !== false,
    updatedAt: toPlainTimestamp(entry.updatedAt)
  }));

  const normalizedConditions = conditions.map((entry) => ({
    text: entry.text || { en: '', fr: '' },
    tooltip: entry.tooltip || { en: '', fr: '' },
    order: entry.order || 0,
    updatedAt: toPlainTimestamp(entry.updatedAt)
  }));

  const normalizedEtiquette = etiquette.map((entry) => ({
    text: entry.text || { en: '', fr: '' },
    tooltip: entry.tooltip || { en: '', fr: '' },
    order: entry.order || 0,
    updatedAt: toPlainTimestamp(entry.updatedAt)
  }));

  const offersBySeason = new Map();
  offers.forEach((offer) => {
    const seasonId = offer.seasonId;
    if (!seasonId) return;
    const normalizedOffer = normalizeOffer(offer);
    if (!offersBySeason.has(seasonId)) {
      offersBySeason.set(seasonId, []);
    }
    offersBySeason.get(seasonId).push(normalizedOffer);
  });

  const normalizedSeasons = seasons
    .filter((season) => season.active !== false)
    .map((season) => normalizeSeason(season, offersBySeason))
    .sort((a, b) => (a.order || 0) - (b.order || 0));

  const normalizedVehicleTypes = vehicleTypes
    .map((entry) => normalizeVehicleType(entry))
    .filter(Boolean)
    .sort((a, b) => {
      if ((a.order || 0) !== (b.order || 0)) {
        return (a.order || 0) - (b.order || 0);
      }
      return (a.value || '').localeCompare(b.value || '');
    });

  const i18nEntries = {};
  i18nDocs.forEach((entry) => {
    const key = entry.key || entry.id;
    if (!key) return;
    i18nEntries[key] = entry.text || { en: '', fr: '' };
  });

  const banner = `// Auto-generated by Tracker on ${new Date().toISOString()}\n// Do not edit manually. Run "node functions/scripts/export-site-data.mjs --out <path>" instead.\n\n`;
  const fileContents = `${banner}export const STORAGE_ADDONS = ${serialize(normalizedAddOns)};\n\nexport const STORAGE_SEASON_ADDONS = ${serialize(
    normalizedSeasonAddOns
  )};\n\nexport const STORAGE_CONDITIONS = ${serialize(
    normalizedConditions
  )};\n\nexport const STORAGE_ETIQUETTE = ${serialize(normalizedEtiquette)};\n\nexport const VEHICLE_TYPES = ${serialize(
    normalizedVehicleTypes
  )};\n\nexport const STORAGE_SEASONS = ${serialize(
    normalizedSeasons
  )};\n\nexport const I18N = ${serialize(i18nEntries)};\n`;

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, fileContents, 'utf8');
  console.log(`Wrote site data to ${outputPath}`);
}

main().catch((err) => {
  console.error('Export failed', err);
  process.exit(1);
});
