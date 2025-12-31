import fs from 'fs';
import path from 'path';
import vm from 'vm';
import admin from 'firebase-admin';
import { fileURLToPath } from 'url';
import { loadActiveProfile } from '../../lib/profile-config.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const { projectId } = await loadActiveProfile();

admin.initializeApp({ projectId });
const db = admin.firestore();

const siteJsPath = path.resolve(__dirname, '../../../entrepot/static/site.js');

function extractBlock(source, token, openChar, closeChar) {
  const startIndex = source.indexOf(token);
  if (startIndex === -1) {
    throw new Error(`Unable to find "${token}" in ${siteJsPath}`);
  }
  const startBrace = source.indexOf(openChar, startIndex);
  if (startBrace === -1) {
    throw new Error(`Missing opening "${openChar}" for ${token}`);
  }
  let depth = 0;
  for (let i = startBrace; i < source.length; i += 1) {
    const char = source[i];
    if (char === openChar) {
      depth += 1;
    } else if (char === closeChar) {
      depth -= 1;
      if (depth === 0) {
        return source.slice(startBrace, i + 1);
      }
    }
  }
  throw new Error(`Unable to find closing "${closeChar}" for ${token}`);
}

function evaluateExpression(code, context = {}) {
  return vm.runInNewContext(`(${code})`, context);
}

async function clearCollection(name) {
  const snapshot = await db.collection(name).get();
  if (snapshot.empty) return;
  const batch = db.batch();
  snapshot.docs.forEach((docSnap) => batch.delete(docSnap.ref));
  await batch.commit();
}

async function main() {
  const siteSource = fs.readFileSync(siteJsPath, 'utf8');
  const seasonsCode = extractBlock(siteSource, 'const SEASON_DEFINITIONS', '[', ']');
  const servicePricesCode = extractBlock(siteSource, 'const SERVICE_PRICES', '{', '}');

  const formatCurrencySandbox = (value = 0, lang = 'en') =>
    new Intl.NumberFormat(lang === 'fr' ? 'fr-CA' : 'en-CA', {
      style: 'currency',
      currency: 'CAD'
    }).format(value || 0);

  const sharedPolicyCode = extractBlock(siteSource, 'const SHARED_POLICY_CARD', '{', '}');
  const i18nCode = extractBlock(siteSource, 'const I18N', '{', '}');
  const etiquetteTooltipCode = extractBlock(siteSource, 'const ETIQUETTE_TOOLTIP_KEYS', '{', '}');

const sharedPolicyCard = evaluateExpression(sharedPolicyCode, {
  formatCurrency: formatCurrencySandbox,
  SERVICE_PRICES: evaluateExpression(servicePricesCode)
});
const i18nMap = evaluateExpression(i18nCode);
const etiquetteTooltipKeys = evaluateExpression(etiquetteTooltipCode);
const marketingCopyEntries = Object.entries(i18nMap);

  console.log(`Prepared ${marketingCopyEntries.length} i18n entries.`);

  await clearCollection('i18nEntries');

  const copyBatch = db.batch();
  marketingCopyEntries.forEach(([key, value]) => {
    const ref = db.collection('i18nEntries').doc(key);
    copyBatch.set(ref, {
      key,
      category: key.split('.')[0],
      text: {
        en: value?.en || '',
        fr: value?.fr || ''
      },
      hint: '',
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    });
  });
  await copyBatch.commit();

  console.log('Imported shared conditions, etiquette entries, and i18n strings into Firestore.');
}

main()
  .then(() => {
    console.log('Import complete.');
    process.exit(0);
  })
  .catch((err) => {
    console.error('Import failed', err);
    process.exit(1);
  });
