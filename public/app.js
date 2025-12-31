import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js';
import {
  GoogleAuthProvider,
  connectAuthEmulator,
  getAuth,
  onAuthStateChanged,
  signInWithEmailAndPassword,
  signInWithPopup,
  signOut
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js';
import {
  Timestamp,
  addDoc,
  collection,
  connectFirestoreEmulator,
  deleteDoc,
  doc,
  getFirestore,
  onSnapshot,
  orderBy,
  query,
  serverTimestamp,
  setDoc,
  updateDoc,
  writeBatch
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js';
import {
  connectFunctionsEmulator,
  getFunctions,
  httpsCallable
} from 'https://www.gstatic.com/firebasejs/10.12.2/firebase-functions.js';
import { firebaseConfig, emulatorConfig } from './firebase-config.js';

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const functionsApp = getFunctions(app);
const requestSitePublishCallable = httpsCallable(functionsApp, 'requestSitePublish');
const googleProvider = new GoogleAuthProvider();
googleProvider.setCustomParameters({ prompt: 'select_account' });
const usingEmulators = Boolean(emulatorConfig.useEmulators);

if (usingEmulators) {
  connectAuthEmulator(auth, `http://${emulatorConfig.authHost}:${emulatorConfig.authPort}`);
  connectFirestoreEmulator(db, emulatorConfig.firestoreHost, emulatorConfig.firestorePort);
  connectFunctionsEmulator(functionsApp, emulatorConfig.functionsHost, emulatorConfig.functionsPort);
}

const authSection = document.getElementById('auth-section');
const appSection = document.getElementById('app-section');
const activeUser = document.getElementById('active-user');
const signOutButton = document.getElementById('sign-out');
const headerSettingsButton = document.getElementById('header-settings-link');
const loginForm = document.getElementById('login-form');
const googleSignInButton = document.getElementById('google-sign-in');
const loginError = document.getElementById('login-error');

const navLinks = Array.from(document.querySelectorAll('.nav-link[data-view]'));
const mainNav = document.getElementById('main-nav');
const settingsNav = document.getElementById('settings-nav');
const settingsNavButtons = Array.from(settingsNav?.querySelectorAll('button[data-settings-target]') ?? []);
const closeSettingsNavButton = document.getElementById('close-settings-nav');
const panelTitle = document.getElementById('panel-title');
const panelSubtitle = document.getElementById('panel-subtitle');
const accountsView = document.getElementById('accounts-view');
const ledgerView = document.getElementById('ledger-view');
const newAccountButton = document.getElementById('new-account');
const accountBalanceStatus = document.getElementById('account-balance-status');
const newClientButton = document.getElementById('new-client');
const newStorageRequestButton = document.getElementById('new-storage-request');
const pricingPrimaryActionButton = document.getElementById('pricing-primary-action');
const pricingSecondaryActionButton = document.getElementById('pricing-secondary-action');
const requestPublishButton = document.getElementById('request-publish');
const publishStatusLabel = document.getElementById('publish-status');
const addEntryButton = document.getElementById('add-entry');
const transferButton = document.getElementById('transfer-funds');
const newCategoryButton = document.getElementById('new-category');
const accountList = document.getElementById('account-list');
const clientsView = document.getElementById('clients-view');
const clientTableBody = document.getElementById('client-table-body');
const storageView = document.getElementById('storage-view');
const storageTableBody = document.getElementById('storage-table-body');
const pricingView = document.getElementById('pricing-view');
const settingsView = document.getElementById('settings-view');
const seasonTableBody = document.getElementById('season-table-body');
const vehicleTypeTableBody = document.getElementById('vehicle-type-table-body');
const offerTableBody = document.getElementById('offer-table-body');
const addonTableBody = document.getElementById('addon-table-body');
const copyTableBody = document.getElementById('copy-table-body');
const conditionTableBody = document.getElementById('condition-table-body');
const etiquetteTableBody = document.getElementById('etiquette-table-body');
const categoryTableBody = document.getElementById('category-table-body');
const modal = document.getElementById('account-modal');
const closeModalButton = document.getElementById('close-modal');
const accountForm = document.getElementById('account-form');
const accountFormTitle = document.getElementById('account-form-title');
const accountNameInput = document.getElementById('account-name');
const accountDescriptionInput = document.getElementById('account-description');
const accountOpeningDateInput = document.getElementById('account-opening-date');
const accountOpeningInput = document.getElementById('account-opening-balance');
const accountTypeSelect = document.getElementById('account-type');
const accountDefaultSection = document.getElementById('account-defaults');
const accountDefaultCashWrapper = document.getElementById('account-default-cash-wrapper');
const accountDefaultEntityWrapper = document.getElementById('account-default-entity-wrapper');
const accountDefaultCashInput = document.getElementById('account-default-cash');
const accountDefaultEntityInput = document.getElementById('account-default-entity');
const accountFormError = document.getElementById('account-form-error');
const clientModal = document.getElementById('client-modal');
const closeClientModalButton = document.getElementById('close-client-modal');
const clientForm = document.getElementById('client-form');
const clientFormTitle = document.getElementById('client-form-title');
const clientNameInput = document.getElementById('client-name');
const clientPhoneInput = document.getElementById('client-phone');
const clientEmailInput = document.getElementById('client-email');
const clientAddressInput = document.getElementById('client-address');
const clientCityInput = document.getElementById('client-city');
const clientProvinceSelect = document.getElementById('client-province');
const clientPostalInput = document.getElementById('client-postal');
const clientActiveInput = document.getElementById('client-active');
const clientNotesInput = document.getElementById('client-notes');
const clientFormError = document.getElementById('client-form-error');
const storageModal = document.getElementById('storage-modal');
const closeStorageModalButton = document.getElementById('close-storage-modal');
const storageForm = document.getElementById('storage-form');
const storageFormTitle = document.getElementById('storage-form-title');
const storageSeasonSelect = document.getElementById('storage-season');
const storageClientSelect = document.getElementById('storage-client');
const storageVehicleTypeSelect = document.getElementById('storage-vehicle-type');
const storageVehicleBrandInput = document.getElementById('storage-vehicle-brand');
const storageVehicleModelInput = document.getElementById('storage-vehicle-model');
const storageVehicleColourInput = document.getElementById('storage-vehicle-colour');
const storageVehicleLengthInput = document.getElementById('storage-vehicle-length');
const storageVehicleYearInput = document.getElementById('storage-vehicle-year');
const storageVehiclePlateInput = document.getElementById('storage-vehicle-plate');
const storageVehicleProvinceSelect = document.getElementById('storage-vehicle-province');
const storageInsuranceCompanyInput = document.getElementById('storage-insurance-company');
const storagePolicyNumberInput = document.getElementById('storage-policy-number');
const storageInsuranceExpirationInput = document.getElementById('storage-insurance-expiration');
const storageStatusSelect = document.getElementById('storage-status');
const storageAmountInput = document.getElementById('storage-amount');
const storageAddonBatteryInput = document.getElementById('storage-addon-battery');
const storageAddonPropaneInput = document.getElementById('storage-addon-propane');
const storageFormError = document.getElementById('storage-form-error');
const seasonModal = document.getElementById('season-modal');
const closeSeasonModalButton = document.getElementById('close-season-modal');
const seasonForm = document.getElementById('season-form');
const seasonFormTitle = document.getElementById('season-form-title');
const seasonNameEnInput = document.getElementById('season-name-en');
const seasonNameFrInput = document.getElementById('season-name-fr');
const seasonLabelEnInput = document.getElementById('season-label-en');
const seasonLabelFrInput = document.getElementById('season-label-fr');
const seasonTimeframeEnInput = document.getElementById('season-timeframe-en');
const seasonTimeframeFrInput = document.getElementById('season-timeframe-fr');
const seasonDropoffEnInput = document.getElementById('season-dropoff-en');
const seasonDropoffFrInput = document.getElementById('season-dropoff-fr');
const seasonPickupEnInput = document.getElementById('season-pickup-en');
const seasonPickupFrInput = document.getElementById('season-pickup-fr');
const seasonDescriptionEnInput = document.getElementById('season-description-en');
const seasonDescriptionFrInput = document.getElementById('season-description-fr');
const seasonOrderInput = document.getElementById('season-order');
const seasonActiveInput = document.getElementById('season-active');
const seasonFormError = document.getElementById('season-form-error');
const vehicleTypeModal = document.getElementById('vehicle-type-modal');
const closeVehicleTypeModalButton = document.getElementById('close-vehicle-type-modal');
const vehicleTypeForm = document.getElementById('vehicle-type-form');
const vehicleTypeFormTitle = document.getElementById('vehicle-type-form-title');
const vehicleTypeValueInput = document.getElementById('vehicle-type-value');
const vehicleTypeLabelEnInput = document.getElementById('vehicle-type-label-en');
const vehicleTypeLabelFrInput = document.getElementById('vehicle-type-label-fr');
const vehicleTypeSlugInput = document.getElementById('vehicle-type-slug');
const vehicleTypeOrderInput = document.getElementById('vehicle-type-order');
const vehicleTypeLegacyInput = document.getElementById('vehicle-type-legacy');
const vehicleTypeFormError = document.getElementById('vehicle-type-form-error');
const offerModal = document.getElementById('offer-modal');
const closeOfferModalButton = document.getElementById('close-offer-modal');
const offerForm = document.getElementById('offer-form');
const offerFormTitle = document.getElementById('offer-form-title');
const offerSeasonSelect = document.getElementById('offer-season');
const offerLabelEnInput = document.getElementById('offer-label-en');
const offerLabelFrInput = document.getElementById('offer-label-fr');
const offerPriceModeSelect = document.getElementById('offer-price-mode');
const offerFlatAmountInput = document.getElementById('offer-flat-amount');
const offerPriceRateInput = document.getElementById('offer-price-rate');
const offerMinimumInput = document.getElementById('offer-minimum');
const offerPriceUnitEnInput = document.getElementById('offer-price-unit-en');
const offerPriceUnitFrInput = document.getElementById('offer-price-unit-fr');
const offerVehicleTypesInput = document.getElementById('offer-vehicle-types');
const offerNoteEnInput = document.getElementById('offer-note-en');
const offerNoteFrInput = document.getElementById('offer-note-fr');
const offerOrderInput = document.getElementById('offer-order');
const offerHideInput = document.getElementById('offer-hide');
const offerFormError = document.getElementById('offer-form-error');
const offerFlatAmountWrapper = document.getElementById('offer-flat-amount-wrapper');
const offerRateWrapper = document.getElementById('offer-rate-wrapper');
const offerMinimumWrapper = document.getElementById('offer-minimum-wrapper');
const offerUnitEnWrapper = document.getElementById('offer-unit-en-wrapper');
const offerUnitFrWrapper = document.getElementById('offer-unit-fr-wrapper');
const addonModal = document.getElementById('addon-modal');
const closeAddonModalButton = document.getElementById('close-addon-modal');
const addonForm = document.getElementById('addon-form');
const addonFormTitle = document.getElementById('addon-form-title');
const addonCodeInput = document.getElementById('addon-code');
const addonNameEnInput = document.getElementById('addon-name-en');
const addonNameFrInput = document.getElementById('addon-name-fr');
const addonDescriptionEnInput = document.getElementById('addon-description-en');
const addonDescriptionFrInput = document.getElementById('addon-description-fr');
const addonPriceInput = document.getElementById('addon-price');
const addonOrderInput = document.getElementById('addon-order');
const addonFormError = document.getElementById('addon-form-error');
const copyModal = document.getElementById('copy-modal');
const closeCopyModalButton = document.getElementById('close-copy-modal');
const copyForm = document.getElementById('copy-form');
const copyFormTitle = document.getElementById('copy-form-title');
const copyKeyInput = document.getElementById('copy-key');
const copyCategoryInput = document.getElementById('copy-category');
const copyTextEnInput = document.getElementById('copy-text-en');
const copyTextFrInput = document.getElementById('copy-text-fr');
const copyHintInput = document.getElementById('copy-hint');
const copyFormError = document.getElementById('copy-form-error');
const categoryModal = document.getElementById('category-modal');
const closeCategoryModalButton = document.getElementById('close-category-modal');
const categoryForm = document.getElementById('category-form');
const categoryFormTitle = document.getElementById('category-form-title');
const categoryLabelInput = document.getElementById('category-label');
const categoryTypeSelect = document.getElementById('category-type');
const categoryCodeInput = document.getElementById('category-code');
const categoryClientRequiredInput = document.getElementById('category-requires-client');
const categoryFormError = document.getElementById('category-form-error');
const conditionModal = document.getElementById('condition-modal');
const closeConditionModalButton = document.getElementById('close-condition-modal');
const conditionForm = document.getElementById('condition-form');
const conditionFormTitle = document.getElementById('condition-form-title');
const conditionTextEnInput = document.getElementById('condition-text-en');
const conditionTextFrInput = document.getElementById('condition-text-fr');
const conditionTooltipEnInput = document.getElementById('condition-tooltip-en');
const conditionTooltipFrInput = document.getElementById('condition-tooltip-fr');
const conditionOrderInput = document.getElementById('condition-order');
const conditionFormError = document.getElementById('condition-form-error');
const etiquetteModal = document.getElementById('etiquette-modal');
const closeEtiquetteModalButton = document.getElementById('close-etiquette-modal');
const etiquetteForm = document.getElementById('etiquette-form');
const etiquetteFormTitle = document.getElementById('etiquette-form-title');
const etiquetteTextEnInput = document.getElementById('etiquette-text-en');
const etiquetteTextFrInput = document.getElementById('etiquette-text-fr');
const etiquetteTooltipEnInput = document.getElementById('etiquette-tooltip-en');
const etiquetteTooltipFrInput = document.getElementById('etiquette-tooltip-fr');
const etiquetteOrderInput = document.getElementById('etiquette-order');
const etiquetteFormError = document.getElementById('etiquette-form-error');

const toggleLedgerFilterButton = document.getElementById('toggle-ledger-filter');
const ledgerFilterSummary = document.getElementById('ledger-filter-summary');
const ledgerFilterMenu = document.getElementById('ledger-filter-menu');
const ledgerFilterList = document.getElementById('ledger-filter-list');
const closeLedgerFilterButton = document.getElementById('close-ledger-filter');
const resetLedgerFilterButton = document.getElementById('reset-ledger-filter');
const ledgerTagFilterInput = document.getElementById('ledger-tag-filter');
const ledgerTableBody = document.getElementById('ledger-table-body');
const ledgerErrorModal = document.getElementById('ledger-error-modal');
const closeLedgerErrorButton = document.getElementById('close-ledger-error');

const entryModal = document.getElementById('entry-modal');
const closeEntryModalButton = document.getElementById('close-entry-modal');
const entryForm = document.getElementById('entry-form');
const entryAccountSelect = document.getElementById('entry-account');
const entryEntitySelect = document.getElementById('entry-entity');
const entryDateInput = document.getElementById('entry-date');
const entryTypeSelect = document.getElementById('entry-type');
const entryCategorySelect = document.getElementById('entry-category');
const entryClientField = document.getElementById('entry-client-field');
const entryClientSelect = document.getElementById('entry-client');
const entryAmountInput = document.getElementById('entry-amount');
const entryReturnInput = document.getElementById('entry-is-return');
const entryReturnLabel = document.getElementById('entry-return-label');
const entryDescriptionInput = document.getElementById('entry-description');
const entryFormError = document.getElementById('entry-form-error');
const tagInput = document.getElementById('entry-tag-input');
const tagSuggestionList = document.getElementById('tag-suggestion-list');
const selectedTagsContainer = document.getElementById('selected-tags');
const tagInputWrapper = document.getElementById('tag-input-wrapper');
const entryFormTitle = document.getElementById('entry-form-title');
const ledgerError = document.getElementById('ledger-error');
const transferModal = document.getElementById('transfer-modal');
const closeTransferModalButton = document.getElementById('close-transfer-modal');
const transferForm = document.getElementById('transfer-form');
const transferFromSelect = document.getElementById('transfer-from');
const transferToSelect = document.getElementById('transfer-to');
const transferDateInput = document.getElementById('transfer-date');
const transferAmountInput = document.getElementById('transfer-amount');
const transferNoteInput = document.getElementById('transfer-note');
const transferFormError = document.getElementById('transfer-form-error');
const amountPositiveColor = '#16a34a';
const amountNegativeColor = '#dc2626';
const storageSubmitButton = storageForm?.querySelector('button[type="submit"]');
const pricingTabs = document.getElementById('pricing-tabs');
const pricingPanels = Array.from(document.querySelectorAll('.pricing-panel'));
const pricingTabButtons = Array.from(pricingTabs?.querySelectorAll('button[data-panel]') ?? []);

function hideEntryModal() {
  entryModal.classList.add('hidden');
  entryForm.reset();
  entryFormError.textContent = '';
  entryAccountSelect.disabled = cashAccounts.length === 0;
  entryEntitySelect.disabled = entityAccounts.length === 0;
  editingEntryId = null;
  editingEntryTransactionId = null;
  entryFormTitle.textContent = 'Add ledger entry';
  selectedTags = [];
  renderSelectedTags();
  tagSuggestionList.classList.add('hidden');
  updateEntryCategoryOptions({ forceType: entryTypeSelect.value, preserveSelection: false });
  if (entryClientSelect) {
    entryClientSelect.value = '';
  }
  if (entryReturnInput) {
    entryReturnInput.checked = false;
  }
  updateReturnLabel();
  syncEntryClientVisibility();
  syncEntrySelectors();
}

function hideTransferModal() {
  transferModal.classList.add('hidden');
  transferForm.reset();
  transferFormError.textContent = '';
  editingTransferContext = null;
}

function generateTransactionId() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return `txn_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

let unsubscribeAccounts = null;
let accounts = [];
let accountLookup = new Map();
let cashAccounts = [];
let entityAccounts = [];
let accountAdjustments = new Map();
let entityAdjustments = new Map();
let editingAccountId = null;
let currentView = 'ledger';
let lastNonSettingsView = 'ledger';
let activeSettingsSection = null;
let ledgerAccountSelection = [];
let ledgerFilterCustom = false;
let ledgerTagFilters = [];
let lastKnownCashTotal = 0;
let lastKnownEntityTotal = 0;
let expenses = [];
let unsubscribeExpenses = null;
let tagSet = new Set();
let editingEntryId = null;
let editingEntryTransactionId = null;
let editingTransferContext = null;
let selectedTags = [];
let clients = [];
let clientLookup = new Map();
let unsubscribeClients = null;
let editingClientId = null;
let storageRequests = [];
let unsubscribeStorageRequests = null;
let editingStorageRequestId = null;
let activePricingPanel = 'seasons';
let seasons = [];
let seasonLookup = new Map();
let unsubscribeSeasons = null;
let editingSeasonId = null;
let vehicleTypes = [];
let vehicleTypeLookup = new Map();
let unsubscribeVehicleTypes = null;
let editingVehicleTypeId = null;
let offers = [];
let offerLookup = new Map();
let unsubscribeOffers = null;
let editingOfferId = null;
let addOns = [];
let unsubscribeAddOns = null;
let editingAddonId = null;
let addonPriceLookup = new Map();
let conditions = [];
let unsubscribeConditions = null;
let editingConditionId = null;
let etiquetteEntries = [];
let unsubscribeEtiquette = null;
let editingEtiquetteId = null;
let marketingCopyEntries = [];
let unsubscribeMarketingCopy = null;
let editingCopyId = null;
let categories = [];
let categoryLookup = new Map();
let unsubscribeCategories = null;
let editingCategoryId = null;
let sitePublishStatus = null;
let latestContentUpdateMs = 0;
let publishRequestInFlight = false;
let unsubscribePublishStatus = null;

const STORAGE_STATUS_OPTIONS = [
  { value: 'new', label: 'New' },
  { value: 'waiting_signature', label: 'Waiting for Signature' },
  { value: 'waiting_deposit', label: 'Waiting for Deposit' },
  { value: 'reserved', label: 'Reserved' },
  { value: 'scheduled', label: 'Scheduled' },
  { value: 'stored', label: 'Stored' },
  { value: 'picked_up', label: 'Picked-Up' }
];

const PRICING_PANEL_ACTIONS = {
  seasons: {
    primary: { label: 'Add season', handler: () => openSeasonModal('create') }
  },
  vehicleTypes: {
    primary: { label: 'Add vehicle type', handler: () => openVehicleTypeModal('create') }
  },
  offers: {
    primary: { label: 'Add offer', handler: () => openOfferModal('create') }
  },
  addons: {
    primary: { label: 'Add add-on', handler: () => openAddonModal('create') }
  },
  policies: {
    primary: { label: 'Add condition', handler: () => openConditionModal('create') }
  },
  etiquette: {
    primary: { label: 'Add etiquette note', handler: () => openEtiquetteModal('create') }
  },
  copy: {
    primary: { label: 'Add copy entry', handler: () => openCopyModal('create') }
  }
};

const VEHICLE_TYPE_OPTIONS = [
  { value: 'rv', label: 'RV / Motorhome' },
  { value: 'boat', label: 'Boat' },
  { value: 'trailer', label: 'Trailer' },
  { value: 'car', label: 'Car' },
  { value: 'other', label: 'Other' }
];

const slugify = (value = '') =>
  value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/--+/g, '-');

function showAppUI(isAuthenticated) {
  authSection.classList.toggle('hidden', isAuthenticated);
  appSection.classList.toggle('hidden', !isAuthenticated);
  activeUser.classList.toggle('hidden', !isAuthenticated);
  signOutButton.classList.toggle('hidden', !isAuthenticated);
  if (headerSettingsButton) {
    headerSettingsButton.classList.toggle('hidden', !isAuthenticated);
  }
}

function cleanAccounts() {
  accounts = [];
  accountLookup = new Map();
  accountAdjustments = new Map();
  entityAdjustments = new Map();
  editingAccountId = null;
  renderAccountList();
  closeModal();
  renderLedgerTable();
}

function cleanClients() {
  clients = [];
  clientLookup = new Map();
  editingClientId = null;
  renderClientTable();
  closeClientModal();
  updateStorageClientOptions();
}

function cleanStorageRequests() {
  storageRequests = [];
  editingStorageRequestId = null;
  renderStorageTable();
  closeStorageModal();
}

function cleanSeasonsData() {
  seasons = [];
  seasonLookup = new Map();
  editingSeasonId = null;
  renderSeasonTable();
  closeSeasonModal();
}

function cleanVehicleTypesData() {
  vehicleTypes = [];
  vehicleTypeLookup = new Map();
  editingVehicleTypeId = null;
  renderVehicleTypeTable();
  closeVehicleTypeModal();
}

function cleanOffersData() {
  offers = [];
  offerLookup = new Map();
  editingOfferId = null;
  renderOfferTable();
  closeOfferModal();
}

function cleanAddOnsData() {
  addOns = [];
  editingAddonId = null;
  renderAddonTable();
  closeAddonModal();
}

function cleanConditionsData() {
  conditions = [];
  editingConditionId = null;
  renderConditionTable();
  closeConditionModal();
}

function cleanEtiquetteData() {
  etiquetteEntries = [];
  editingEtiquetteId = null;
  renderEtiquetteTable();
  closeEtiquetteModal();
}

function cleanMarketingCopyData() {
  marketingCopyEntries = [];
  editingCopyId = null;
  renderCopyTable();
  closeCopyModal();
}

function cleanCategoriesData() {
  categories = [];
  categoryLookup = new Map();
  editingCategoryId = null;
  renderCategoryTable();
  closeCategoryModal();
  updateEntryCategoryOptions({ preserveSelection: false });
}

function typeSupportsCash(type) {
  return type === 'cash' || type === 'cash_entity';
}

function typeSupportsEntity(type) {
  return type === 'entity' || type === 'cash_entity';
}

function isCashAccount(account) {
  return typeSupportsCash(account?.type);
}

function isEntityAccount(account) {
  return typeSupportsEntity(account?.type);
}

function isCombinedAccount(account) {
  return account?.type === 'cash_entity';
}

function getDefaultCashAccountId() {
  return cashAccounts.find((account) => account.defaultCash)?.id || cashAccounts[0]?.id || '';
}

function getDefaultEntityAccountId() {
  return entityAccounts.find((account) => account.defaultEntity)?.id || entityAccounts[0]?.id || '';
}

function syncAccountDefaultVisibility() {
  if (
    !accountTypeSelect ||
    !accountDefaultSection ||
    !accountDefaultCashWrapper ||
    !accountDefaultEntityWrapper ||
    !accountDefaultCashInput ||
    !accountDefaultEntityInput
  ) {
    return;
  }
  const type = accountTypeSelect.value;
  const supportsCash = typeSupportsCash(type);
  const supportsEntity = typeSupportsEntity(type);
  accountDefaultCashWrapper.classList.toggle('hidden', !supportsCash);
  accountDefaultEntityWrapper.classList.toggle('hidden', !supportsEntity);
  if (!supportsCash) {
    accountDefaultCashInput.checked = false;
  }
  if (!supportsEntity) {
    accountDefaultEntityInput.checked = false;
  }
  accountDefaultSection.classList.toggle('hidden', !supportsCash && !supportsEntity);
}

function subscribeToAccounts() {
  cleanAccountSubscription();
  const ref = collection(db, 'accounts');
  const q = query(ref, orderBy('name'));
  unsubscribeAccounts = onSnapshot(
    q,
    (snapshot) => {
      accounts = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      accountLookup = new Map(accounts.map((account) => [account.id, account]));
      cashAccounts = accounts.filter((account) => isCashAccount(account));
      entityAccounts = accounts.filter((account) => isEntityAccount(account));
      const validIds = accounts.map((account) => account.id);
      if (!ledgerFilterCustom) {
        ledgerAccountSelection = [...validIds];
      } else {
        ledgerAccountSelection = ledgerAccountSelection.filter((id) => validIds.includes(id));
        if (!ledgerAccountSelection.length) {
          ledgerFilterCustom = false;
          ledgerAccountSelection = [...validIds];
        }
      }
      renderAccountList();
      updateLedgerAccountOptions();
      updateEntryAccountOptions();
      updateTransferAccountOptions();
      addEntryButton.disabled = cashAccounts.length === 0 || entityAccounts.length === 0;
      transferButton.disabled = cashAccounts.length < 2;
    },
    (error) => {
      accountFormError.textContent = error.message;
    }
  );
}

function subscribeToClients() {
  cleanClientSubscription();
  const ref = collection(db, 'clients');
  const q = query(ref, orderBy('name'));
  unsubscribeClients = onSnapshot(
    q,
    (snapshot) => {
      clients = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      clientLookup = new Map(clients.map((client) => [client.id, client]));
      renderClientTable();
      updateStorageClientOptions();
      updateEntryClientOptions();
    },
    (error) => {
      clientFormError.textContent = error.message;
    }
  );
}

function subscribeToStorageRequests() {
  cleanStorageSubscription();
  const ref = collection(db, 'storageRequests');
  const q = query(ref, orderBy('season'), orderBy('createdAt'));
  unsubscribeStorageRequests = onSnapshot(
    q,
    (snapshot) => {
      storageRequests = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      renderStorageTable();
    },
    (error) => {
      storageFormError.textContent = error.message;
    }
  );
}

function subscribeToSeasons() {
  cleanSeasonSubscription();
  const ref = collection(db, 'storageSeasons');
  const q = query(ref, orderBy('order'));
  unsubscribeSeasons = onSnapshot(
    q,
    (snapshot) => {
      seasons = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      seasonLookup = new Map(seasons.map((season) => [season.id, season]));
      renderSeasonTable();
      updateSeasonOptions();
      renderStorageTable();
      renderOfferTable();
      updateLatestContentTimestamp();
    },
    (error) => {
      seasonFormError.textContent = error.message;
    }
  );
}

function subscribeToVehicleTypes() {
  cleanVehicleTypeSubscription();
  const ref = collection(db, 'vehicleTypes');
  const q = query(ref, orderBy('order'));
  unsubscribeVehicleTypes = onSnapshot(
    q,
    (snapshot) => {
      vehicleTypes = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      vehicleTypeLookup = new Map(vehicleTypes.map((type) => [type.id, type]));
      renderVehicleTypeTable();
      renderStorageTable();
      updateLatestContentTimestamp();
    },
    (error) => {
      vehicleTypeFormError.textContent = error.message;
    }
  );
}

function subscribeToOffers() {
  cleanOfferSubscription();
  const ref = collection(db, 'storageOffers');
  const q = query(ref, orderBy('order'));
  unsubscribeOffers = onSnapshot(
    q,
    (snapshot) => {
      offers = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      offerLookup = new Map(offers.map((offer) => [offer.id, offer]));
      renderOfferTable();
      renderStorageTable();
      updateLatestContentTimestamp();
    },
    (error) => {
      offerFormError.textContent = error.message;
    }
  );
}

function subscribeToAddOns() {
  cleanAddonSubscription();
  const ref = collection(db, 'storageAddOns');
  const q = query(ref, orderBy('order'));
  unsubscribeAddOns = onSnapshot(
    q,
    (snapshot) => {
      addOns = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      addonPriceLookup = new Map(addOns.map((addon) => [addon.code, Number(addon.price) || 0]));
      renderAddonTable();
      renderStorageTable();
      updateLatestContentTimestamp();
    },
    (error) => {
      addonFormError.textContent = error.message;
    }
  );
}

function subscribeToMarketingCopy() {
  cleanMarketingCopySubscription();
  const ref = collection(db, 'i18nEntries');
  const q = query(ref, orderBy('key'));
  unsubscribeMarketingCopy = onSnapshot(
    q,
    (snapshot) => {
      marketingCopyEntries = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      renderCopyTable();
      updateLatestContentTimestamp();
    },
    (error) => {
      copyFormError.textContent = error.message;
    }
  );
}

function subscribeToCategories() {
  cleanCategorySubscription();
  const ref = collection(db, 'categories');
  const q = query(ref, orderBy('label'));
  unsubscribeCategories = onSnapshot(
    q,
    (snapshot) => {
      categories = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      categoryLookup = new Map(categories.map((category) => [category.id, category]));
      renderCategoryTable();
      updateEntryCategoryOptions({ preserveSelection: true });
    },
    (error) => {
      categoryFormError.textContent = error.message;
    }
  );
}

function subscribeToConditions() {
  cleanConditionSubscription();
  const ref = collection(db, 'storageConditions');
  const q = query(ref, orderBy('order'));
  unsubscribeConditions = onSnapshot(
    q,
    (snapshot) => {
      conditions = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      renderConditionTable();
      updateLatestContentTimestamp();
    },
    (error) => {
      conditionFormError.textContent = error.message;
    }
  );
}

function subscribeToEtiquette() {
  cleanEtiquetteSubscription();
  const ref = collection(db, 'storageEtiquette');
  const q = query(ref, orderBy('order'));
  unsubscribeEtiquette = onSnapshot(
    q,
    (snapshot) => {
      etiquetteEntries = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      renderEtiquetteTable();
      updateLatestContentTimestamp();
    },
    (error) => {
      etiquetteFormError.textContent = error.message;
    }
  );
}

function subscribeToPublishStatus() {
  if (unsubscribePublishStatus) {
    unsubscribePublishStatus();
  }
  const statusDoc = doc(db, 'admin', 'sitePublish');
  unsubscribePublishStatus = onSnapshot(
    statusDoc,
    (snapshot) => {
      sitePublishStatus = snapshot.exists() ? snapshot.data() : null;
      updatePublishButtonState();
    },
    (error) => {
      console.error('Failed to load publish status', error);
    }
  );
}

function cleanAccountSubscription() {
  if (unsubscribeAccounts) {
    unsubscribeAccounts();
  }
  unsubscribeAccounts = null;
}

function cleanClientSubscription() {
  if (unsubscribeClients) {
    unsubscribeClients();
  }
  unsubscribeClients = null;
}

function cleanStorageSubscription() {
  if (unsubscribeStorageRequests) {
    unsubscribeStorageRequests();
  }
  unsubscribeStorageRequests = null;
}

function cleanSeasonSubscription() {
  if (unsubscribeSeasons) {
    unsubscribeSeasons();
  }
  unsubscribeSeasons = null;
}

function cleanVehicleTypeSubscription() {
  if (unsubscribeVehicleTypes) {
    unsubscribeVehicleTypes();
  }
  unsubscribeVehicleTypes = null;
}

function cleanOfferSubscription() {
  if (unsubscribeOffers) {
    unsubscribeOffers();
  }
  unsubscribeOffers = null;
}

function cleanAddonSubscription() {
  if (unsubscribeAddOns) {
    unsubscribeAddOns();
  }
  unsubscribeAddOns = null;
}

function cleanConditionSubscription() {
  if (unsubscribeConditions) {
    unsubscribeConditions();
  }
  unsubscribeConditions = null;
}

function cleanEtiquetteSubscription() {
  if (unsubscribeEtiquette) {
    unsubscribeEtiquette();
  }
  unsubscribeEtiquette = null;
}

function cleanMarketingCopySubscription() {
  if (unsubscribeMarketingCopy) {
    unsubscribeMarketingCopy();
  }
  unsubscribeMarketingCopy = null;
}

function cleanCategorySubscription() {
  if (unsubscribeCategories) {
    unsubscribeCategories();
  }
  unsubscribeCategories = null;
}

function renderAccountList() {
  accountList.innerHTML = '';
  let cashTotal = 0;
  let entityTotal = 0;
  if (!accounts.length) {
    const li = document.createElement('li');
    li.textContent = 'No accounts yet. Add one to get started.';
    li.className = 'empty';
    accountList.appendChild(li);
    updateAccountBalanceIndicator(0, 0);
    return;
  }

  accounts.forEach((account) => {
    const li = document.createElement('li');
    const adjustmentMap = isCashAccount(account) ? accountAdjustments : entityAdjustments;
    const adjustment = adjustmentMap.get(account.id) || 0;
    const computedBalance = (Number(account.openingBalance) || 0) + adjustment;
    const badges = [];
    if (isCashAccount(account)) badges.push('<span class="badge cash">CASH</span>');
    if (isEntityAccount(account)) badges.push('<span class="badge entity">ENTITY</span>');
    const openingBalance = Number(account.openingBalance) || 0;
    const cashValue = openingBalance + (accountAdjustments.get(account.id) || 0);
    const entityValue = openingBalance + (entityAdjustments.get(account.id) || 0);
    if (isCashAccount(account)) {
      cashTotal += cashValue;
    }
    if (isEntityAccount(account)) {
      entityTotal += entityValue;
    }
    li.innerHTML = `
      <div class="account-info">
        <p class="account-name">${account.name}</p>
        <p class="account-description">${account.description || 'No description provided.'}</p>
      </div>
      <div class="account-meta">
        <div class="badge-list">${badges.join(' ')}</div>
        <p class="label">Balance</p>
        <p class="account-balance">${formatCurrency(computedBalance)}</p>
        <p class="account-opening">Opening ${formatCurrency(account.openingBalance || 0)}</p>
        <button
          type="button"
          class="icon-button"
          data-action="edit"
          data-id="${account.id}"
          aria-label="Edit ${account.name}"
        >
          <img src="icons/pencil.svg" alt="Edit ${account.name}" />
        </button>
      </div>
    `;
    accountList.appendChild(li);
  });
  updateAccountBalanceIndicator(cashTotal, entityTotal);
}

function updateAccountBalanceIndicator(cashTotal = lastKnownCashTotal, entityTotal = lastKnownEntityTotal) {
  if (!accountBalanceStatus) return;
  lastKnownCashTotal = cashTotal;
  lastKnownEntityTotal = entityTotal;
  const hasAccounts = accounts.length > 0;
  if (!hasAccounts || currentView !== 'accounts') {
    accountBalanceStatus.classList.add('hidden');
    return;
  }
  const diff = cashTotal - entityTotal;
  const balanced = Math.abs(diff) < 0.01;
  const detail = balanced
    ? 'Accounts balanced'
    : `${diff > 0 ? 'Cash exceeds entity by ' : 'Entity exceeds cash by '}${formatCurrency(Math.abs(diff))}`;
  accountBalanceStatus.textContent = detail;
  accountBalanceStatus.classList.toggle('balanced', balanced);
  accountBalanceStatus.classList.toggle('unbalanced', !balanced);
  accountBalanceStatus.classList.remove('hidden');
}

function renderClientTable() {
  if (!clientTableBody) return;
  clientTableBody.innerHTML = '';
  if (!clients.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 8;
    cell.className = 'empty';
    cell.textContent = 'No clients yet. Add one to get started.';
    row.appendChild(cell);
    clientTableBody.appendChild(row);
    return;
  }

  clients.forEach((client) => {
    const row = document.createElement('tr');

    const nameCell = document.createElement('td');
    nameCell.textContent = client.name || '—';
    row.appendChild(nameCell);

    const phoneCell = document.createElement('td');
    phoneCell.textContent = client.phone || '—';
    row.appendChild(phoneCell);

    const emailCell = document.createElement('td');
    emailCell.textContent = client.email || '—';
    row.appendChild(emailCell);

    const addressCell = document.createElement('td');
    addressCell.textContent = client.address || '—';
    row.appendChild(addressCell);

    const cityCell = document.createElement('td');
    cityCell.textContent = client.city || '—';
    row.appendChild(cityCell);

    const provinceCell = document.createElement('td');
    provinceCell.textContent = client.province || '—';
    row.appendChild(provinceCell);

    const postalCell = document.createElement('td');
    postalCell.textContent = client.postalCode || '—';
    row.appendChild(postalCell);

    const actionCell = document.createElement('td');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'icon-button';
    button.dataset.action = 'edit-client';
    button.dataset.id = client.id;
    button.setAttribute('aria-label', `Edit ${client.name || 'client'}`);
    button.innerHTML = `<img src="icons/pencil.svg" alt="Edit ${client.name || 'client'}" />`;
    actionCell.appendChild(button);
    row.appendChild(actionCell);

    clientTableBody.appendChild(row);
  });
}

function renderStorageTable() {
  if (!storageTableBody) return;
  storageTableBody.innerHTML = '';
  if (!storageRequests.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 7;
    cell.className = 'empty';
    cell.textContent = 'No storage requests yet.';
    row.appendChild(cell);
    storageTableBody.appendChild(row);
    return;
  }

  storageRequests.forEach((request) => {
    const row = document.createElement('tr');
    const tenantDisplay = clientLookup.get(request.clientId)?.name || '—';
    const addonList = [];
    if (request.addons?.battery) addonList.push('Battery charging');
    if (request.addons?.propane) addonList.push('Propane tank storage');

    const vehicle = request.vehicle || {};

    const cells = [
      request.season || '—',
      tenantDisplay,
      vehicle.typeLabel || vehicle.type || '—'
    ];

    cells.forEach((value) => {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.appendChild(cell);
    });

    const addonCell = document.createElement('td');
    addonCell.textContent = addonList.length ? addonList.join(', ') : 'None';
    row.appendChild(addonCell);

    const statusCell = document.createElement('td');
    const select = document.createElement('select');
    select.className = 'storage-status-select';
    select.dataset.id = request.id;
    STORAGE_STATUS_OPTIONS.forEach((option) => {
      const opt = document.createElement('option');
      opt.value = option.value;
      opt.textContent = option.label;
      opt.selected = request.status === option.value;
      select.appendChild(opt);
    });
    statusCell.appendChild(select);
    row.appendChild(statusCell);

    const amountCell = document.createElement('td');
    const resolvedAmount = resolveStorageAmount(request);
    amountCell.textContent = Number.isFinite(resolvedAmount) ? formatCurrency(resolvedAmount) : '—';
    row.appendChild(amountCell);

    const actionCell = document.createElement('td');
    const viewButton = document.createElement('button');
    viewButton.type = 'button';
    viewButton.className = 'icon-button';
    viewButton.dataset.action = 'view-storage';
    viewButton.dataset.id = request.id;
    viewButton.setAttribute('aria-label', `View storage request for ${tenantDisplay}`);
    viewButton.innerHTML = '<img src="icons/eye.svg" alt="View storage request" />';
    actionCell.appendChild(viewButton);

    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'icon-button';
    editButton.dataset.action = 'edit-storage';
    editButton.dataset.id = request.id;
    editButton.setAttribute('aria-label', `Edit storage request for ${tenantDisplay}`);
    editButton.innerHTML = `<img src="icons/pencil.svg" alt="Edit storage request for ${tenantDisplay}" />`;
    actionCell.appendChild(editButton);
    row.appendChild(actionCell);

    storageTableBody.appendChild(row);
  });
}

function timestampToMillis(value) {
  if (!value) return 0;
  if (typeof value.toMillis === 'function') return value.toMillis();
  if (value.seconds) return value.seconds * 1000;
  if (typeof value === 'string') return Date.parse(value) || 0;
  return 0;
}

function updateLatestContentTimestamp() {
  const candidates = [
    ...seasons,
    ...vehicleTypes,
    ...offers,
    ...addOns,
    ...conditions,
    ...etiquetteEntries,
    ...marketingCopyEntries
  ];
  const newest = candidates.reduce((max, item) => {
    const updatedAt = timestampToMillis(item.updatedAt) || timestampToMillis(item.createdAt);
    return updatedAt > max ? updatedAt : max;
  }, 0);
  latestContentUpdateMs = newest;
  updatePublishButtonState();
}

function showPricingPanel(panelId = 'seasons') {
  if (!pricingPanels.length) return;
  activePricingPanel = panelId;
  pricingPanels.forEach((panel) => {
    const match = panel.dataset.panel === panelId;
    panel.classList.toggle('hidden', !match);
  });
  pricingTabButtons.forEach((button) => {
    const isActive = button.dataset.panel === panelId;
    button.classList.toggle('active', isActive);
    button.setAttribute('aria-selected', String(isActive));
  });
  updatePricingToolbarActions();
}

function updatePricingToolbarActions() {
  if (!pricingPrimaryActionButton) return;
  if (!isPricingViewActive()) {
    pricingPrimaryActionButton.classList.add('hidden');
    pricingPrimaryActionButton.onclick = null;
    if (pricingSecondaryActionButton) {
      pricingSecondaryActionButton.classList.add('hidden');
      pricingSecondaryActionButton.onclick = null;
    }
    return;
  }
  const config = PRICING_PANEL_ACTIONS[activePricingPanel];
  if (config && config.primary) {
    pricingPrimaryActionButton.textContent = config.primary.label;
    pricingPrimaryActionButton.onclick = config.primary.handler;
    pricingPrimaryActionButton.classList.remove('hidden');
  } else {
    pricingPrimaryActionButton.classList.add('hidden');
    pricingPrimaryActionButton.onclick = null;
  }
  if (!pricingSecondaryActionButton) return;
  if (config && config.secondary) {
    pricingSecondaryActionButton.textContent = config.secondary.label;
    pricingSecondaryActionButton.onclick = config.secondary.handler;
    pricingSecondaryActionButton.classList.remove('hidden');
  } else {
    pricingSecondaryActionButton.classList.add('hidden');
    pricingSecondaryActionButton.onclick = null;
  }
}

function updatePublishButtonState() {
  if (!requestPublishButton) return;
  const showingPricing = isPricingViewActive();
  requestPublishButton.classList.toggle('hidden', !showingPricing);
  if (publishStatusLabel) {
    publishStatusLabel.classList.toggle('hidden', !showingPricing);
  }
  if (!showingPricing) return;
  const lastPublishedMillis = timestampToMillis(sitePublishStatus?.lastPublishedAt);
  const hasChanges = latestContentUpdateMs > lastPublishedMillis;
  const disabled = !hasChanges || publishRequestInFlight;
  requestPublishButton.disabled = disabled;
  if (publishStatusLabel) {
    const lastPublished = sitePublishStatus?.lastPublishedAt
      ? new Date(timestampToMillis(sitePublishStatus.lastPublishedAt)).toLocaleString()
      : 'never';
    publishStatusLabel.textContent = hasChanges
      ? `Changes pending publish (last publish: ${lastPublished}).`
      : `Website text is up to date (last publish: ${lastPublished}).`;
  }
}

function updateSeasonOptions() {
  if (!offerSeasonSelect) return;
  const previous = offerSeasonSelect.value;
  offerSeasonSelect.innerHTML = '';
  seasons.forEach((season) => {
    const option = document.createElement('option');
    option.value = season.id;
    option.textContent = season.label?.en || season.name?.en || season.id;
    offerSeasonSelect.appendChild(option);
  });
  if (previous) {
    offerSeasonSelect.value = previous;
  }
  if (!offerSeasonSelect.value && seasons.length) {
    offerSeasonSelect.value = seasons[0].id;
  }
}

function renderSeasonTable() {
  if (!seasonTableBody) return;
  seasonTableBody.innerHTML = '';
  if (!seasons.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 7;
    cell.className = 'empty';
    cell.textContent = 'No seasons yet.';
    row.appendChild(cell);
    seasonTableBody.appendChild(row);
    return;
  }
  seasons.forEach((season) => {
    const row = document.createElement('tr');
    const cells = [
      season.label?.en || season.name?.en || '—',
      season.label?.fr || season.name?.fr || '—',
      season.timeframe?.en || '—',
      Number.isFinite(season.order) ? season.order : '—',
      season.active ? 'Yes' : 'No'
    ];
    cells.forEach((value) => {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.appendChild(cell);
    });
    const actionCell = document.createElement('td');
    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'icon-button';
    editButton.dataset.action = 'edit-season';
    editButton.dataset.id = season.id;
    editButton.setAttribute('aria-label', `Edit ${season.label?.en || season.name?.en || 'season'}`);
    editButton.innerHTML = `<img src="icons/pencil.svg" alt="Edit ${season.label?.en || season.name?.en || 'season'}" />`;
    actionCell.appendChild(editButton);
    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'icon-button';
    deleteButton.dataset.action = 'delete-season';
    deleteButton.dataset.id = season.id;
    deleteButton.setAttribute('aria-label', `Delete ${season.label?.en || season.name?.en || 'season'}`);
    deleteButton.innerHTML = '<img src="icons/trash.svg" alt="Delete season" data-icon="trash" />';
    actionCell.appendChild(deleteButton);
    row.appendChild(actionCell);
    seasonTableBody.appendChild(row);
  });
}

function renderVehicleTypeTable() {
  if (!vehicleTypeTableBody) return;
  vehicleTypeTableBody.innerHTML = '';
  if (!vehicleTypes.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 7;
    cell.className = 'empty';
    cell.textContent = 'No vehicle types yet.';
    row.appendChild(cell);
    vehicleTypeTableBody.appendChild(row);
    return;
  }
  vehicleTypes.forEach((type) => {
    const row = document.createElement('tr');
    const legacyDisplay = (type.legacyValues || []).join(', ') || '—';
    const cells = [
      type.value || '—',
      type.labels?.en || '—',
      type.labels?.fr || '—',
      type.slug || '—',
      Number.isFinite(type.order) ? type.order : '—',
      legacyDisplay
    ];
    cells.forEach((value) => {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.appendChild(cell);
    });
    const actionCell = document.createElement('td');
    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'icon-button';
    editButton.dataset.action = 'edit-vehicle-type';
    editButton.dataset.id = type.id;
    editButton.setAttribute('aria-label', `Edit ${type.value || 'vehicle type'}`);
    editButton.innerHTML = `<img src="icons/pencil.svg" alt="Edit ${type.value || 'vehicle type'}" />`;
    actionCell.appendChild(editButton);
    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'icon-button';
    deleteButton.dataset.action = 'delete-vehicle-type';
    deleteButton.dataset.id = type.id;
    deleteButton.setAttribute('aria-label', `Delete ${type.value || 'vehicle type'}`);
    deleteButton.innerHTML = '<img src="icons/trash.svg" alt="Delete vehicle type" data-icon="trash" />';
    actionCell.appendChild(deleteButton);
    row.appendChild(actionCell);
    vehicleTypeTableBody.appendChild(row);
  });
}

function renderOfferTable() {
  if (!offerTableBody) return;
  offerTableBody.innerHTML = '';
  if (!offers.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 8;
    cell.className = 'empty';
    cell.textContent = 'No offers yet.';
    row.appendChild(cell);
    offerTableBody.appendChild(row);
    return;
  }

  offers.forEach((offer) => {
    const row = document.createElement('tr');
    const parentSeason = seasonLookup.get(offer.seasonId);
    const priceDisplay = (() => {
      if (offer.price?.mode === 'flat') {
        return formatCurrency(offer.price?.amount ?? 0);
      }
      if (offer.price?.mode === 'perFoot') {
        const rate = formatCurrency(offer.price?.rate ?? 0);
        const unit = offer.price?.unit?.en || '/ ft';
        return `${rate} ${unit}`;
      }
      return 'Contact';
    })();

    const minimumDisplay =
      offer.price?.mode === 'perFoot' && offer.price?.minimum != null
        ? formatCurrency(offer.price.minimum)
        : '—';

    const cells = [
      parentSeason?.label?.en || offer.seasonId || '—',
      offer.label?.en || '—',
      offer.label?.fr || '—',
      priceDisplay,
      minimumDisplay,
      offer.hideInTable ? 'Yes' : 'No'
    ];
    cells.forEach((value) => {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.appendChild(cell);
    });
    const actionCell = document.createElement('td');
    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'icon-button';
    editButton.dataset.action = 'edit-offer';
    editButton.dataset.id = offer.id;
    editButton.setAttribute('aria-label', `Edit ${offer.label?.en || 'offer'}`);
    editButton.innerHTML = `<img src="icons/pencil.svg" alt="Edit ${offer.label?.en || 'offer'}" />`;
    actionCell.appendChild(editButton);
    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'icon-button';
    deleteButton.dataset.action = 'delete-offer';
    deleteButton.dataset.id = offer.id;
    deleteButton.setAttribute('aria-label', `Delete ${offer.label?.en || 'offer'}`);
    deleteButton.innerHTML = '<img src="icons/trash.svg" alt="Delete offer" data-icon="trash" />';
    actionCell.appendChild(deleteButton);
    row.appendChild(actionCell);
    offerTableBody.appendChild(row);
  });
}

function renderAddonTable() {
  if (!addonTableBody) return;
  addonTableBody.innerHTML = '';
  if (!addOns.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 5;
    cell.className = 'empty';
    cell.textContent = 'No add-on services yet.';
    row.appendChild(cell);
    addonTableBody.appendChild(row);
    return;
  }
  addOns.forEach((addon) => {
    const row = document.createElement('tr');
    const cells = [
      addon.code || '—',
      addon.name?.en || '—',
      addon.name?.fr || '—',
      addon.price != null ? formatCurrency(addon.price) : '—'
    ];
    cells.forEach((value) => {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.appendChild(cell);
    });
    const actionCell = document.createElement('td');
    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'icon-button';
    editButton.dataset.action = 'edit-addon';
    editButton.dataset.id = addon.id;
    editButton.setAttribute('aria-label', `Edit ${addon.code || 'add-on'}`);
    editButton.innerHTML = `<img src="icons/pencil.svg" alt="Edit ${addon.code || 'add-on'}" />`;
    actionCell.appendChild(editButton);
    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'icon-button';
    deleteButton.dataset.action = 'delete-addon';
    deleteButton.dataset.id = addon.id;
    deleteButton.setAttribute('aria-label', `Delete ${addon.code || 'add-on'}`);
    deleteButton.innerHTML = '<img src="icons/trash.svg" alt="Delete add-on" data-icon="trash" />';
    actionCell.appendChild(deleteButton);
    row.appendChild(actionCell);
    addonTableBody.appendChild(row);
  });
}

function renderConditionTable() {
  if (!conditionTableBody) return;
  conditionTableBody.innerHTML = '';
  if (!conditions.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 6;
    cell.className = 'empty';
    cell.textContent = 'No conditions yet.';
    row.appendChild(cell);
    conditionTableBody.appendChild(row);
    return;
  }
  conditions.forEach((entry) => {
    const row = document.createElement('tr');
    const cells = [
      entry.text?.en || '—',
      entry.text?.fr || '—',
      entry.tooltip?.en || '—',
      entry.tooltip?.fr || '—',
      Number.isFinite(entry.order) ? entry.order : '—'
    ];
    cells.forEach((value) => {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.appendChild(cell);
    });
    const actionCell = document.createElement('td');
    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'icon-button';
    editButton.dataset.action = 'edit-condition';
    editButton.dataset.id = entry.id;
    editButton.setAttribute('aria-label', 'Edit condition');
    editButton.innerHTML = '<img src="icons/pencil.svg" alt="Edit condition" />';
    actionCell.appendChild(editButton);
    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'icon-button';
    deleteButton.dataset.action = 'delete-condition';
    deleteButton.dataset.id = entry.id;
    deleteButton.setAttribute('aria-label', 'Delete condition');
    deleteButton.innerHTML = '<img src="icons/trash.svg" alt="Delete condition" data-icon="trash" />';
    actionCell.appendChild(deleteButton);
    row.appendChild(actionCell);
    conditionTableBody.appendChild(row);
  });
}

function renderEtiquetteTable() {
  if (!etiquetteTableBody) return;
  etiquetteTableBody.innerHTML = '';
  if (!etiquetteEntries.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 6;
    cell.className = 'empty';
    cell.textContent = 'No etiquette notes yet.';
    row.appendChild(cell);
    etiquetteTableBody.appendChild(row);
    return;
  }
  etiquetteEntries.forEach((entry) => {
    const row = document.createElement('tr');
    const cells = [
      entry.text?.en || '—',
      entry.text?.fr || '—',
      entry.tooltip?.en || '—',
      entry.tooltip?.fr || '—',
      Number.isFinite(entry.order) ? entry.order : '—'
    ];
    cells.forEach((value) => {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.appendChild(cell);
    });
    const actionCell = document.createElement('td');
    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'icon-button';
    editButton.dataset.action = 'edit-etiquette';
    editButton.dataset.id = entry.id;
    editButton.setAttribute('aria-label', 'Edit etiquette note');
    editButton.innerHTML = '<img src="icons/pencil.svg" alt="Edit etiquette note" />';
    actionCell.appendChild(editButton);
    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'icon-button';
    deleteButton.dataset.action = 'delete-etiquette';
    deleteButton.dataset.id = entry.id;
    deleteButton.setAttribute('aria-label', 'Delete etiquette note');
    deleteButton.innerHTML = '<img src="icons/trash.svg" alt="Delete etiquette" data-icon="trash" />';
    actionCell.appendChild(deleteButton);
    row.appendChild(actionCell);
  etiquetteTableBody.appendChild(row);
});
}

function renderCopyTable() {
  if (!copyTableBody) return;
  copyTableBody.innerHTML = '';
  if (!marketingCopyEntries.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 6;
    cell.className = 'empty';
    cell.textContent = 'No i18n entries yet.';
    row.appendChild(cell);
    copyTableBody.appendChild(row);
    return;
  }
  marketingCopyEntries.forEach((entry) => {
    const row = document.createElement('tr');
    const cells = [
      entry.key || entry.id || '—',
      entry.category || '—',
      entry.text?.en || '—',
      entry.text?.fr || '—',
      entry.hint || '—'
    ];
    cells.forEach((value) => {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.appendChild(cell);
    });
    const actionCell = document.createElement('td');
    const editButton = document.createElement('button');
    editButton.type = 'button';
    editButton.className = 'icon-button';
    editButton.dataset.action = 'edit-copy';
    editButton.dataset.id = entry.id;
    editButton.setAttribute('aria-label', `Edit copy ${entry.key || entry.id}`);
    editButton.innerHTML = `<img src="icons/pencil.svg" alt="Edit copy" />`;
    actionCell.appendChild(editButton);
    const deleteButton = document.createElement('button');
    deleteButton.type = 'button';
    deleteButton.className = 'icon-button';
    deleteButton.dataset.action = 'delete-copy';
    deleteButton.dataset.id = entry.id;
    deleteButton.setAttribute('aria-label', `Delete copy ${entry.key || entry.id}`);
    deleteButton.innerHTML = '<img src="icons/trash.svg" alt="Delete copy" data-icon="trash" />';
    actionCell.appendChild(deleteButton);
    row.appendChild(actionCell);
    copyTableBody.appendChild(row);
  });
}

function renderCategoryTable() {
  if (!categoryTableBody) return;
  categoryTableBody.innerHTML = '';
  if (!categories.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 5;
    cell.className = 'empty';
    cell.textContent = 'No categories yet.';
    row.appendChild(cell);
    categoryTableBody.appendChild(row);
    return;
  }

  const sorted = [...categories].sort((a, b) => {
    if (a.type !== b.type) {
      return a.type.localeCompare(b.type);
    }
    const codeA = Number(a.code) || 0;
    const codeB = Number(b.code) || 0;
    if (codeA !== codeB) {
      return codeA - codeB;
    }
    return (a.label || '').localeCompare(b.label || '');
  });

  sorted.forEach((category) => {
    const row = document.createElement('tr');
    const labelCell = document.createElement('td');
    labelCell.textContent = category.label || '—';
    row.appendChild(labelCell);
    const typeCell = document.createElement('td');
    typeCell.textContent = category.type === 'income' ? 'Income' : 'Expense';
    row.appendChild(typeCell);
    const codeCell = document.createElement('td');
    codeCell.textContent = Number.isFinite(Number(category.code)) ? Number(category.code) : '—';
    row.appendChild(codeCell);
    const requiresClientCell = document.createElement('td');
    requiresClientCell.textContent = category.requiresClient ? 'Yes' : 'No';
    row.appendChild(requiresClientCell);
    const actionCell = document.createElement('td');
    actionCell.innerHTML = `
      <button type="button" class="link" data-action="edit-category" data-id="${category.id}">Edit</button>
      <button type="button" class="link" data-action="delete-category" data-id="${category.id}">Delete</button>
    `;
    row.appendChild(actionCell);
    categoryTableBody.appendChild(row);
  });
}

function updateStorageClientOptions() {
  if (!storageClientSelect) return;
  const previousValue = storageClientSelect.value;
  storageClientSelect.innerHTML = '<option value="">Select client</option>';
  clients.forEach((client) => {
    const option = document.createElement('option');
    option.value = client.id;
    option.textContent = client.name || client.email || 'Unnamed client';
    storageClientSelect.appendChild(option);
  });
  if (previousValue) {
    storageClientSelect.value = previousValue;
  }
}

function ensureLedgerAccountSelection() {
  const validIds = accounts.map((account) => account.id);
  ledgerAccountSelection = ledgerAccountSelection.filter((id) => validIds.includes(id));
  if (!ledgerAccountSelection.length && accounts.length) {
    ledgerAccountSelection = [...validIds];
  }
}

function updateLedgerFilterSummary() {
  if (!ledgerFilterSummary) return;
  const selectedNames = accounts
    .filter((account) => ledgerAccountSelection.includes(account.id))
    .map((account) => account.name);
  const accountPart =
    !ledgerFilterCustom || ledgerAccountSelection.length === accounts.length
      ? 'All accounts'
      : selectedNames.length <= 2
        ? selectedNames.join(', ')
        : `${selectedNames.slice(0, 2).join(', ')} +${selectedNames.length - 2}`;
  if (ledgerTagFilters.length) {
    ledgerFilterSummary.textContent = `${accountPart} • Tags: ${ledgerTagFilters.join(', ')}`;
  } else {
    ledgerFilterSummary.textContent = accountPart;
  }
}

function syncLedgerFilterUI() {
  if (!ledgerFilterList) return;
  const selectionSet = new Set(ledgerAccountSelection);
  ledgerFilterList.querySelectorAll('input[type="checkbox"]').forEach((input) => {
    input.checked = selectionSet.has(input.value);
  });
  if (ledgerTagFilterInput) {
    ledgerTagFilterInput.value = ledgerTagFilters.join(', ');
  }
}

function updateLedgerAccountOptions() {
  if (!ledgerFilterList) return;
  ensureLedgerAccountSelection();
  const selectionSet = new Set(ledgerAccountSelection);
  ledgerFilterList.innerHTML = '';
  accounts.forEach((account) => {
    const label = document.createElement('label');
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.value = account.id;
    input.checked = selectionSet.has(account.id);
    label.append(input, document.createTextNode(`${account.name} (${account.type === 'cash' ? 'cash' : 'entity'})`));
  ledgerFilterList.appendChild(label);
  });
  updateLedgerFilterSummary();
  renderLedgerTable();
}

function applyLedgerFilterSelection(selectedIds, { custom = true } = {}) {
  ledgerAccountSelection = selectedIds;
  ledgerFilterCustom = custom;
  ensureLedgerAccountSelection();
  syncLedgerFilterUI();
  updateLedgerFilterSummary();
  renderLedgerTable();
}

function updateEntryAccountOptions() {
  const previousAccount = entryAccountSelect.value;
  const previousEntity = entryEntitySelect.value;

  entryAccountSelect.innerHTML = '';
  cashAccounts.forEach((account) => {
    const option = document.createElement('option');
    option.value = account.id;
    option.textContent = account.name;
    entryAccountSelect.appendChild(option);
  });
  entryAccountSelect.disabled = cashAccounts.length === 0;

  const nextCashSelection =
    (previousAccount && entryAccountSelect.querySelector(`option[value="${previousAccount}"]`))
      ? previousAccount
      : getDefaultCashAccountId();
  if (nextCashSelection) {
    entryAccountSelect.value = nextCashSelection;
  }

  entryEntitySelect.innerHTML = '';
  entityAccounts.forEach((account) => {
    const option = document.createElement('option');
    option.value = account.id;
    option.textContent = account.name;
    entryEntitySelect.appendChild(option);
  });
  entryEntitySelect.disabled = entityAccounts.length === 0;

  const nextEntitySelection =
    (previousEntity && entryEntitySelect.querySelector(`option[value="${previousEntity}"]`))
      ? previousEntity
      : getDefaultEntityAccountId();
  if (nextEntitySelection) {
    entryEntitySelect.value = nextEntitySelection;
  }

  syncEntrySelectors();
}

function updateEntryClientOptions() {
  if (!entryClientSelect) return;
  const previous = entryClientSelect.value;
  entryClientSelect.innerHTML = '<option value="">Select client</option>';
  clients.forEach((client) => {
    const option = document.createElement('option');
    option.value = client.id;
    option.textContent = client.name || client.email || 'Unnamed client';
    entryClientSelect.appendChild(option);
  });
  entryClientSelect.value = previous && entryClientSelect.querySelector(`option[value="${previous}"]`) ? previous : '';
  entryClientSelect.disabled = clients.length === 0;
}

function updateEntryCategoryOptions(options = {}) {
  if (!entryCategorySelect) return;
  const activeType = options.forceType || entryTypeSelect.value || 'expense';
  const filtered = categories.filter((category) => category.type === activeType);
  const previousOption = entryCategorySelect.selectedOptions?.[0] || null;
  const previousValue = options.preserveSelection ? entryCategorySelect.value : '';
  const previousWasCustom = Boolean(previousOption?.dataset?.custom === 'true');
  const derivedFallbackLabel = previousWasCustom ? previousOption.dataset.label : undefined;
  const derivedFallbackCode = previousWasCustom ? previousOption.dataset.code : undefined;

  entryCategorySelect.innerHTML = '';
  const placeholder = document.createElement('option');
  placeholder.value = '';
  placeholder.disabled = true;
  placeholder.selected = true;
  placeholder.textContent = filtered.length ? 'Select category' : 'Add categories in Settings';
  entryCategorySelect.appendChild(placeholder);

  filtered.forEach((category) => {
    const option = document.createElement('option');
    option.value = category.id;
    option.textContent = category.code
      ? `${category.label || 'Untitled'} (${category.code})`
      : category.label || 'Untitled';
    option.dataset.label = category.label || '';
    option.dataset.code = category.code ?? '';
    option.dataset.type = category.type;
    entryCategorySelect.appendChild(option);
  });
  entryCategorySelect.disabled = filtered.length === 0;

  const targetValue = options.selectedId ?? (previousWasCustom ? '' : previousValue);
  if (targetValue) {
    entryCategorySelect.value = targetValue;
  }

  const fallbackLabel = options.fallbackLabel ?? derivedFallbackLabel;
  const fallbackCode = options.fallbackCode ?? derivedFallbackCode;
  if (!entryCategorySelect.value && fallbackLabel) {
    const fallback = document.createElement('option');
    fallback.value = '__custom';
    fallback.dataset.label = fallbackLabel;
    if (fallbackCode !== undefined) {
      fallback.dataset.code = fallbackCode;
    }
    fallback.dataset.custom = 'true';
    fallback.textContent = fallbackCode ? `${fallbackLabel} (${fallbackCode})` : fallbackLabel;
    entryCategorySelect.appendChild(fallback);
    entryCategorySelect.value = '__custom';
    entryCategorySelect.disabled = false;
  }

  if (!entryCategorySelect.value) {
    entryCategorySelect.value = '';
  }
  syncEntryClientVisibility();
}

function categoryRequiresClient(categoryId) {
  if (!categoryId || categoryId === '__custom') return false;
  const category = categoryLookup.get(categoryId);
  return Boolean(category?.requiresClient);
}

function syncEntryClientVisibility() {
  if (!entryClientField || !entryClientSelect) return;
  const requiresClient = categoryRequiresClient(entryCategorySelect?.value);
  entryClientField.classList.toggle('hidden', !requiresClient);
  entryClientSelect.required = requiresClient;
  if (!requiresClient) {
    entryClientSelect.value = '';
  }
}

function syncEntrySelectors() {
  const selectedAccount = accountLookup.get(entryAccountSelect.value);
  const selectedEntity = accountLookup.get(entryEntitySelect.value);
  if (isCombinedAccount(selectedAccount)) {
    entryEntitySelect.value = selectedAccount.id;
    entryEntitySelect.disabled = true;
  } else {
    entryEntitySelect.disabled = entityAccounts.length === 0;
    if (!isEntityAccount(accountLookup.get(entryEntitySelect.value)) && entityAccounts.length) {
      entryEntitySelect.value = entityAccounts[0].id;
    }
  }

  if (isCombinedAccount(selectedEntity)) {
    entryAccountSelect.value = selectedEntity.id;
    entryAccountSelect.disabled = true;
  } else {
    entryAccountSelect.disabled = cashAccounts.length === 0;
    if (!isCashAccount(accountLookup.get(entryAccountSelect.value)) && cashAccounts.length) {
      entryAccountSelect.value = cashAccounts[0].id;
    }
  }
}

function updateReturnLabel() {
  if (!entryReturnLabel || !entryTypeSelect) return;
  const isExpense = entryTypeSelect.value === 'expense';
  entryReturnLabel.textContent = isExpense ? 'Mark as return / credit' : 'Mark as refund / debit';
}

function updateTransferAccountOptions() {
  transferFromSelect.innerHTML = '';
  transferToSelect.innerHTML = '';
  cashAccounts.forEach((account) => {
    const optionFrom = document.createElement('option');
    optionFrom.value = account.id;
    optionFrom.textContent = account.name;
    transferFromSelect.appendChild(optionFrom);

    const optionTo = document.createElement('option');
    optionTo.value = account.id;
    optionTo.textContent = account.name;
    transferToSelect.appendChild(optionTo);
  });
  const disabled = cashAccounts.length < 2;
  transferFromSelect.disabled = disabled;
  transferToSelect.disabled = disabled;
}

function subscribeToExpensesStream() {
  cleanExpensesSubscription();
  const ref = collection(db, 'expenses');
  const q = query(ref, orderBy('date', 'desc'));
  unsubscribeExpenses = onSnapshot(
    q,
    (snapshot) => {
      expenses = snapshot.docs.map((docSnap) => ({ id: docSnap.id, ...docSnap.data() }));
      const { cashTotals, entityTotals } = calculateAdjustments(expenses);
      accountAdjustments = cashTotals;
      entityAdjustments = entityTotals;
      tagSet = new Set();
      expenses.forEach((entry) => {
        if (Array.isArray(entry.tags)) {
          entry.tags.forEach((tag) => tagSet.add(tag));
        }
      });
      updateTagSuggestions();
      if (ledgerTagFilterInput) {
        const currentValue = ledgerTagFilterInput.value;
        ledgerTagFilterInput.value = currentValue;
      }
      renderLedgerTable();
      renderAccountList();
    },
    (error) => {
      entryFormError.textContent = error.message;
    }
  );
}

function cleanExpensesSubscription() {
  if (unsubscribeExpenses) {
    unsubscribeExpenses();
  }
  unsubscribeExpenses = null;
  expenses = [];
  accountAdjustments = new Map();
  entityAdjustments = new Map();
  renderLedgerTable();
}

function calculateAdjustments(entries) {
  const cashTotals = new Map();
  const entityTotals = new Map();
  entries.forEach((entry) => {
    const delta = getEntryDelta(entry);
    const cashCurrent = cashTotals.get(entry.accountId) || 0;
    cashTotals.set(entry.accountId, cashCurrent + delta);
    if (entry.entityId) {
      const entityCurrent = entityTotals.get(entry.entityId) || 0;
      entityTotals.set(entry.entityId, entityCurrent + delta);
    }
  });
  return { cashTotals, entityTotals };
}

function getEntryDelta(entry) {
  if (!entry || entry.isVirtualOpening) return 0;
  const amount = Number(entry.amount) || 0;
  const direction = entry.entryType === 'expense' ? -1 : 1;
  const returnFactor = entry.isReturn ? -1 : 1;
  return direction * amount * returnFactor;
}

async function enforceAccountDefaults(accountId, { defaultCash, defaultEntity }) {
  if (!defaultCash && !defaultEntity) return;
  const updateMap = new Map();
  if (defaultCash) {
    accounts.forEach((account) => {
      if (account.id !== accountId && account.defaultCash) {
        const existing = updateMap.get(account.id) || {};
        existing.defaultCash = false;
        updateMap.set(account.id, existing);
      }
    });
  }
  if (defaultEntity) {
    accounts.forEach((account) => {
      if (account.id !== accountId && account.defaultEntity) {
        const existing = updateMap.get(account.id) || {};
        existing.defaultEntity = false;
        updateMap.set(account.id, existing);
      }
    });
  }
  if (!updateMap.size) return;
  const batch = writeBatch(db);
  updateMap.forEach((payload, targetId) => {
    batch.update(doc(db, 'accounts', targetId), payload);
  });
  await batch.commit();
}

function openModal(mode, account = null) {
  if (mode === 'edit' && account) {
    editingAccountId = account.id;
    accountFormTitle.textContent = `Edit ${account.name}`;
    accountNameInput.value = account.name;
    accountDescriptionInput.value = account.description || '';
    accountOpeningInput.value = Number(account.openingBalance || 0).toFixed(2);
    accountTypeSelect.value = account.type || 'entity';
    accountDefaultCashInput.checked = Boolean(account.defaultCash);
    accountDefaultEntityInput.checked = Boolean(account.defaultEntity);
    const resolvedOpeningDate = resolveAccountOpeningDate(account);
    setDateInputValue(accountOpeningDateInput, resolvedOpeningDate, true);
  } else {
    editingAccountId = null;
    accountFormTitle.textContent = 'New account';
    accountForm.reset();
    accountTypeSelect.value = account?.type || 'cash';
    accountDefaultCashInput.checked = false;
    accountDefaultEntityInput.checked = false;
    setDateInputValue(accountOpeningDateInput, new Date(), true);
  }
  accountFormError.textContent = '';
  syncAccountDefaultVisibility();
  modal.classList.remove('hidden');
  accountNameInput.focus();
}

function closeModal() {
  modal.classList.add('hidden');
  accountForm.reset();
  accountTypeSelect.value = 'entity';
  accountOpeningDateInput.value = '';
  accountDefaultCashInput.checked = false;
  accountDefaultEntityInput.checked = false;
  syncAccountDefaultVisibility();
  accountFormError.textContent = '';
  editingAccountId = null;
}

function openClientModal(mode, client = null) {
  if (mode === 'edit' && client) {
    editingClientId = client.id;
    clientFormTitle.textContent = `Edit ${client.name || 'client'}`;
    clientNameInput.value = client.name || '';
    clientPhoneInput.value = client.phone || '';
    clientEmailInput.value = client.email || '';
    clientAddressInput.value = client.address || '';
    clientCityInput.value = client.city || '';
    clientProvinceSelect.value = client.province || '';
    clientPostalInput.value = client.postalCode || '';
    if (clientActiveInput) {
      clientActiveInput.checked = client.active !== false;
    }
    if (clientNotesInput) {
      clientNotesInput.value = client.notes || '';
    }
  } else {
    editingClientId = null;
    clientFormTitle.textContent = 'New client';
    clientForm.reset();
    clientProvinceSelect.value = '';
    if (clientActiveInput) {
      clientActiveInput.checked = true;
    }
    if (clientNotesInput) {
      clientNotesInput.value = '';
    }
  }
  clientFormError.textContent = '';
  clientModal.classList.remove('hidden');
  clientNameInput.focus();
}

function closeClientModal() {
  clientModal.classList.add('hidden');
  clientForm.reset();
  clientProvinceSelect.value = '';
  if (clientActiveInput) {
    clientActiveInput.checked = true;
  }
  if (clientNotesInput) {
    clientNotesInput.value = '';
  }
  clientFormError.textContent = '';
  editingClientId = null;
}

function formatClientPhone(value) {
  if (!value) return '';
  const digits = value.replace(/\D+/g, '').slice(0, 10);
  if (digits.length < 10) return digits;
  const area = digits.slice(0, 3);
  const prefix = digits.slice(3, 6);
  const line = digits.slice(6);
  return `${area}-${prefix}-${line}`;
}

function findVehicleTypeEntry(identifier) {
  if (!identifier) return null;
  return (
    vehicleTypeLookup.get(identifier) ||
    vehicleTypes.find((entry) => {
      if (!entry) return false;
      return (
        entry.id === identifier ||
        entry.value === identifier ||
        entry.slug === identifier ||
        (Array.isArray(entry.legacyValues) && entry.legacyValues.includes(identifier))
      );
    }) ||
    null
  );
}

function getVehicleTypeCandidates(identifier) {
  if (!identifier) return [];
  const entry = findVehicleTypeEntry(identifier);
  const candidates = new Set([identifier]);
  if (entry) {
    if (entry.id) candidates.add(entry.id);
    if (entry.value) candidates.add(entry.value);
    if (entry.slug) candidates.add(entry.slug);
    if (Array.isArray(entry.legacyValues)) {
      entry.legacyValues.forEach((legacy) => legacy && candidates.add(legacy));
    }
  }
  return Array.from(candidates).filter(Boolean);
}

function offerSupportsVehicleTypeForRequest(offer, vehicleTypeId) {
  if (!offer || !Array.isArray(offer.vehicleTypes) || !offer.vehicleTypes.length) {
    return false;
  }
  const candidates = getVehicleTypeCandidates(vehicleTypeId);
  if (!candidates.length) return false;
  return candidates.some((candidate) => offer.vehicleTypes.includes(candidate));
}

function resolveSeasonId(seasonValue) {
  if (!seasonValue) return null;
  if (seasonLookup.has(seasonValue)) return seasonValue;
  const normalized = seasonValue.toString().toLowerCase();
  const match =
    seasons.find((season) => {
      if (!season) return false;
      if (season.id === seasonValue) return true;
      const nameEn = season.name?.en?.toLowerCase?.() || '';
      const nameFr = season.name?.fr?.toLowerCase?.() || '';
      const labelEn = season.label?.en?.toLowerCase?.() || '';
      const labelFr = season.label?.fr?.toLowerCase?.() || '';
      return (
        normalized === nameEn ||
        normalized === nameFr ||
        normalized === labelEn ||
        normalized === labelFr
      );
    }) || null;
  return match?.id || null;
}

function offerRequiresLength(offer) {
  if (!offer) return false;
  if (offer.price?.mode === 'perFoot') return true;
  return Boolean(offer.lengthRange);
}

function lengthMatchesRange(length, range) {
  if (!range) return true;
  if (!Number.isFinite(length)) return false;
  if (typeof range.min === 'number') {
    if (range.exclusiveMin) {
      if (!(length > range.min)) return false;
    } else if (!(length >= range.min)) {
      return false;
    }
  }
  if (typeof range.max === 'number') {
    if (range.exclusiveMax) {
      if (!(length < range.max)) return false;
    } else if (!(length <= range.max)) {
      return false;
    }
  }
  return true;
}

function computeOfferPriceValue(offer, context) {
  if (!offer || !offer.price) return null;
  if (offer.price.mode === 'contact') return null;
  if (offer.price.mode === 'flat') {
    return Number(offer.price.amount);
  }
  if (offer.price.mode === 'perFoot') {
    const length = context.length;
    if (!Number.isFinite(length)) return null;
    const minimum = Number(offer.price.minimum) || 0;
    return Math.max(length * Number(offer.price.rate || 0), minimum);
  }
  return null;
}

function getOffersForRequest(request) {
  if (!request?.season || !request?.vehicle?.type) return [];
  const seasonId = resolveSeasonId(request.season);
  if (!seasonId) return [];
  const filtered = offers
    .filter((offer) => offer.seasonId === seasonId && offerSupportsVehicleTypeForRequest(offer, request.vehicle.type))
    .sort((a, b) => {
      const orderA = Number.isFinite(a.order) ? a.order : 0;
      const orderB = Number.isFinite(b.order) ? b.order : 0;
      return orderA - orderB;
    });
  return filtered;
}

function getAddonPrice(code) {
  if (!code) return 0;
  const value = addonPriceLookup.get(code);
  return Number.isFinite(value) ? Number(value) : 0;
}

function estimateStorageAmount(request) {
  if (!request) return null;
  const seasonOffers = getOffersForRequest(request);
  if (!seasonOffers.length) return null;
  const vehicleLength = Number(request.vehicle?.lengthFeet);
  const numericLength = Number.isFinite(vehicleLength) ? vehicleLength : null;
  const needsLength = seasonOffers.some(offerRequiresLength);
  if (needsLength && !Number.isFinite(numericLength)) {
    return null;
  }
  const matched = seasonOffers.find((offer) => lengthMatchesRange(numericLength, offer.lengthRange));
  const selectedOffer = matched || seasonOffers[seasonOffers.length - 1];
  const baseAmount = computeOfferPriceValue(selectedOffer, { length: numericLength });
  if (!Number.isFinite(baseAmount)) return null;
  let total = baseAmount;
  if (request.addons?.battery) {
    total += getAddonPrice('battery');
  }
  if (request.addons?.propane) {
    total += getAddonPrice('propane');
  }
  return Number.isFinite(total) ? total : null;
}

function resolveStorageAmount(request) {
  if (!request) return null;
  const overrideAmount = Number(request.contractAmount);
  if (Number.isFinite(overrideAmount)) {
    return overrideAmount;
  }
  return estimateStorageAmount(request);
}

function formatAmountInputValue(amount) {
  if (!Number.isFinite(amount)) return '';
  return (Math.round(Number(amount) * 100) / 100).toString();
}

function syncStorageAmountInput(request) {
  if (!storageAmountInput) return;
  const overrideAmount = request && Number.isFinite(Number(request.contractAmount)) ? Number(request.contractAmount) : null;
  const suggestedAmount = request ? estimateStorageAmount(request) : null;
  const valueToDisplay = overrideAmount != null ? overrideAmount : suggestedAmount;
  storageAmountInput.value = formatAmountInputValue(valueToDisplay);
  storageAmountInput.placeholder = suggestedAmount ? `Suggested ${formatCurrency(suggestedAmount)}` : 'Auto-calculated from pricing';
}

function setStorageFormReadOnly(readOnly) {
  if (!storageForm) return;
  const fields = storageForm.querySelectorAll('input, select, textarea');
  fields.forEach((field) => {
    field.disabled = readOnly;
  });
  if (storageSubmitButton) {
    storageSubmitButton.classList.toggle('hidden', readOnly);
    storageSubmitButton.disabled = readOnly;
  }
}

function resetStorageFormFields() {
  storageForm.reset();
  storageSeasonSelect.value = '';
  storageClientSelect.value = '';
  storageVehicleTypeSelect.value = '';
  storageVehicleProvinceSelect.value = '';
  storageStatusSelect.value = 'new';
  storageAddonBatteryInput.checked = false;
  storageAddonPropaneInput.checked = false;
  storageInsuranceExpirationInput.value = '';
  syncStorageAmountInput(null);
}

function populateStorageFormFields(request) {
  storageSeasonSelect.value = request.season || '';
  storageClientSelect.value = request.clientId || '';
  storageVehicleTypeSelect.value = request.vehicle?.type || '';
  storageVehicleBrandInput.value = request.vehicle?.brand || '';
  storageVehicleModelInput.value = request.vehicle?.model || '';
  storageVehicleColourInput.value = request.vehicle?.colour || '';
  storageVehicleLengthInput.value = request.vehicle?.lengthFeet || '';
  storageVehicleYearInput.value = request.vehicle?.year || '';
  storageVehiclePlateInput.value = request.vehicle?.plate || '';
  storageVehicleProvinceSelect.value = request.vehicle?.province || '';
  storageInsuranceCompanyInput.value = request.insuranceCompany || '';
  storagePolicyNumberInput.value = request.policyNumber || '';
  storageInsuranceExpirationInput.value = request.insuranceExpiration
    ? request.insuranceExpiration.toDate
      ? request.insuranceExpiration.toDate().toISOString().slice(0, 10)
      : request.insuranceExpiration
    : '';
  storageStatusSelect.value = request.status || 'new';
  storageAddonBatteryInput.checked = Boolean(request.addons?.battery);
  storageAddonPropaneInput.checked = Boolean(request.addons?.propane);
  syncStorageAmountInput(request);
}

function openStorageModal(mode, request = null) {
  updateStorageClientOptions();
  resetStorageFormFields();
  const isEdit = mode === 'edit' && request;
  const isView = mode === 'view' && request;

  if (isEdit || isView) {
    if (isEdit) {
      editingStorageRequestId = request.id;
    } else {
      editingStorageRequestId = null;
    }
    const clientName = clientLookup.get(request.clientId)?.name || 'request';
    storageFormTitle.textContent = `${isView ? 'View' : 'Edit'} ${clientName}`;
    populateStorageFormFields(request);
    setStorageFormReadOnly(isView);
  } else {
    editingStorageRequestId = null;
    storageFormTitle.textContent = 'New storage request';
    setStorageFormReadOnly(false);
  }

  storageFormError.textContent = '';
  storageModal.classList.remove('hidden');
  if (mode === 'view' && request) {
    closeStorageModalButton?.focus();
  } else {
    storageSeasonSelect.focus();
  }
}

function closeStorageModal() {
  storageModal.classList.add('hidden');
  resetStorageFormFields();
  setStorageFormReadOnly(false);
  storageFormError.textContent = '';
  editingStorageRequestId = null;
}

function openSeasonModal(mode, season = null) {
  if (mode === 'edit' && season) {
    editingSeasonId = season.id;
    seasonFormTitle.textContent = `Edit ${season.label?.en || season.name?.en || 'season'}`;
    seasonNameEnInput.value = season.name?.en || '';
    seasonNameFrInput.value = season.name?.fr || '';
    seasonLabelEnInput.value = season.label?.en || '';
    seasonLabelFrInput.value = season.label?.fr || '';
    seasonTimeframeEnInput.value = season.timeframe?.en || '';
    seasonTimeframeFrInput.value = season.timeframe?.fr || '';
    seasonDropoffEnInput.value = season.dropoffWindow?.en || '';
    seasonDropoffFrInput.value = season.dropoffWindow?.fr || '';
    seasonPickupEnInput.value = season.pickupDeadline?.en || '';
    seasonPickupFrInput.value = season.pickupDeadline?.fr || '';
    seasonDescriptionEnInput.value = season.description?.en || '';
    seasonDescriptionFrInput.value = season.description?.fr || '';
    seasonOrderInput.value = Number.isFinite(season.order) ? season.order : 0;
    seasonActiveInput.checked = Boolean(season.active);
  } else {
    editingSeasonId = null;
    seasonFormTitle.textContent = 'New season';
    seasonForm.reset();
    seasonOrderInput.value = 0;
    seasonActiveInput.checked = false;
  }
  seasonFormError.textContent = '';
  seasonModal.classList.remove('hidden');
  seasonNameEnInput.focus();
}

function closeSeasonModal() {
  seasonModal.classList.add('hidden');
  seasonForm.reset();
  seasonOrderInput.value = 0;
  seasonActiveInput.checked = false;
  seasonFormError.textContent = '';
  editingSeasonId = null;
}

function openVehicleTypeModal(mode, entry = null) {
  if (mode === 'edit' && entry) {
    editingVehicleTypeId = entry.id;
    vehicleTypeFormTitle.textContent = `Edit ${entry.value || 'vehicle type'}`;
    vehicleTypeValueInput.value = entry.value || '';
    vehicleTypeLabelEnInput.value = entry.labels?.en || '';
    vehicleTypeLabelFrInput.value = entry.labels?.fr || '';
    vehicleTypeSlugInput.value = entry.slug || '';
    vehicleTypeOrderInput.value = Number.isFinite(entry.order) ? entry.order : 0;
    vehicleTypeLegacyInput.value = (entry.legacyValues || []).join(', ');
  } else {
    editingVehicleTypeId = null;
    vehicleTypeFormTitle.textContent = 'New vehicle type';
    vehicleTypeForm.reset();
    vehicleTypeOrderInput.value = 0;
    vehicleTypeSlugInput.value = '';
    vehicleTypeLegacyInput.value = '';
  }
  vehicleTypeFormError.textContent = '';
  vehicleTypeModal.classList.remove('hidden');
  vehicleTypeValueInput.focus();
}

function closeVehicleTypeModal() {
  if (!vehicleTypeModal) return;
  vehicleTypeModal.classList.add('hidden');
  vehicleTypeForm.reset();
  vehicleTypeOrderInput.value = 0;
  vehicleTypeSlugInput.value = '';
  vehicleTypeLegacyInput.value = '';
  vehicleTypeFormError.textContent = '';
  editingVehicleTypeId = null;
}

function openOfferModal(mode, offer = null) {
  updateSeasonOptions();
  if (mode === 'edit' && offer) {
    editingOfferId = offer.id;
    offerFormTitle.textContent = `Edit ${offer.label?.en || 'offer'}`;
    offerSeasonSelect.value = offer.seasonId || '';
    offerLabelEnInput.value = offer.label?.en || '';
    offerLabelFrInput.value = offer.label?.fr || '';
    offerPriceModeSelect.value = offer.price?.mode || 'flat';
    offerFlatAmountInput.value = offer.price?.amount ?? '';
    offerPriceRateInput.value = offer.price?.rate ?? '';
    offerMinimumInput.value = offer.price?.minimum ?? '';
    offerPriceUnitEnInput.value = offer.price?.unit?.en || '';
    offerPriceUnitFrInput.value = offer.price?.unit?.fr || '';
    offerVehicleTypesInput.value = Array.isArray(offer.vehicleTypes) ? offer.vehicleTypes.join(', ') : '';
    offerNoteEnInput.value = offer.note?.en || '';
    offerNoteFrInput.value = offer.note?.fr || '';
    offerOrderInput.value = Number.isFinite(offer.order) ? offer.order : 0;
    offerHideInput.checked = Boolean(offer.hideInTable);
  } else {
    editingOfferId = null;
    offerFormTitle.textContent = 'New offer';
    offerForm.reset();
    offerSeasonSelect.value = seasons[0]?.id || '';
    offerPriceModeSelect.value = 'flat';
    offerFlatAmountInput.value = '';
    offerPriceRateInput.value = '';
    offerMinimumInput.value = '';
    offerOrderInput.value = 0;
    offerHideInput.checked = false;
  }
  offerFormError.textContent = '';
  offerModal.classList.remove('hidden');
  offerLabelEnInput.focus();
  syncOfferPriceFields();
}

function closeOfferModal() {
  offerModal.classList.add('hidden');
  offerForm.reset();
  offerOrderInput.value = 0;
  offerHideInput.checked = false;
  offerFormError.textContent = '';
  editingOfferId = null;
  syncOfferPriceFields();
}

function syncOfferPriceFields() {
  const mode = offerPriceModeSelect.value;
  const showFlat = mode === 'flat';
  const showPerFoot = mode === 'perFoot';
  offerFlatAmountWrapper.classList.toggle('hidden', !showFlat);
  offerRateWrapper.classList.toggle('hidden', !showPerFoot);
  offerMinimumWrapper.classList.toggle('hidden', !showPerFoot);
  offerUnitEnWrapper.classList.toggle('hidden', !showPerFoot);
  offerUnitFrWrapper.classList.toggle('hidden', !showPerFoot);
}

function openAddonModal(mode, addon = null) {
  if (mode === 'edit' && addon) {
    editingAddonId = addon.id;
    addonFormTitle.textContent = `Edit ${addon.code || 'add-on'}`;
    addonCodeInput.value = addon.code || '';
    addonNameEnInput.value = addon.name?.en || '';
    addonNameFrInput.value = addon.name?.fr || '';
    addonDescriptionEnInput.value = addon.description?.en || '';
    addonDescriptionFrInput.value = addon.description?.fr || '';
    addonPriceInput.value = addon.price ?? '';
    addonOrderInput.value = Number.isFinite(addon.order) ? addon.order : 0;
    addonCodeInput.disabled = true;
  } else {
    editingAddonId = null;
    addonFormTitle.textContent = 'New add-on';
    addonForm.reset();
    addonOrderInput.value = 0;
    addonCodeInput.disabled = false;
  }
  addonFormError.textContent = '';
  addonModal.classList.remove('hidden');
  addonCodeInput.focus();
}

function closeAddonModal() {
  addonModal.classList.add('hidden');
  addonForm.reset();
  addonOrderInput.value = 0;
  addonFormError.textContent = '';
  addonCodeInput.disabled = false;
  editingAddonId = null;
}

function openConditionModal(mode, entry = null) {
  if (mode === 'edit' && entry) {
    editingConditionId = entry.id;
    conditionFormTitle.textContent = 'Edit condition';
    conditionTextEnInput.value = entry.text?.en || '';
    conditionTextFrInput.value = entry.text?.fr || '';
    conditionTooltipEnInput.value = entry.tooltip?.en || '';
    conditionTooltipFrInput.value = entry.tooltip?.fr || '';
    conditionOrderInput.value = Number.isFinite(entry.order) ? entry.order : 0;
  } else {
    editingConditionId = null;
    conditionFormTitle.textContent = 'New condition';
    conditionForm.reset();
    conditionOrderInput.value = 0;
  }
  conditionFormError.textContent = '';
  conditionModal.classList.remove('hidden');
  conditionTextEnInput.focus();
}

function closeConditionModal() {
  conditionModal.classList.add('hidden');
  conditionForm.reset();
  conditionOrderInput.value = 0;
  conditionFormError.textContent = '';
  editingConditionId = null;
}

function openEtiquetteModal(mode, entry = null) {
  if (mode === 'edit' && entry) {
    editingEtiquetteId = entry.id;
    etiquetteFormTitle.textContent = 'Edit etiquette note';
    etiquetteTextEnInput.value = entry.text?.en || '';
    etiquetteTextFrInput.value = entry.text?.fr || '';
    etiquetteTooltipEnInput.value = entry.tooltip?.en || '';
    etiquetteTooltipFrInput.value = entry.tooltip?.fr || '';
    etiquetteOrderInput.value = Number.isFinite(entry.order) ? entry.order : 0;
  } else {
    editingEtiquetteId = null;
    etiquetteFormTitle.textContent = 'New etiquette note';
    etiquetteForm.reset();
    etiquetteOrderInput.value = 0;
  }
  etiquetteFormError.textContent = '';
  etiquetteModal.classList.remove('hidden');
  etiquetteTextEnInput.focus();
}

function closeEtiquetteModal() {
  etiquetteModal.classList.add('hidden');
  etiquetteForm.reset();
  etiquetteOrderInput.value = 0;
  etiquetteFormError.textContent = '';
  editingEtiquetteId = null;
}

function openCopyModal(mode, entry = null) {
  if (mode === 'edit' && entry) {
    editingCopyId = entry.id;
    copyFormTitle.textContent = `Edit copy (${entry.key || entry.id})`;
    copyKeyInput.value = entry.key || entry.id;
    copyKeyInput.disabled = true;
    copyCategoryInput.value = entry.category || '';
    copyTextEnInput.value = entry.text?.en || '';
    copyTextFrInput.value = entry.text?.fr || '';
    copyHintInput.value = entry.hint || '';
  } else {
    editingCopyId = null;
    copyFormTitle.textContent = 'New copy entry';
    copyForm.reset();
    copyKeyInput.disabled = false;
  }
  copyFormError.textContent = '';
  copyModal.classList.remove('hidden');
  copyKeyInput.focus();
}

function closeCopyModal() {
  copyModal.classList.add('hidden');
  copyForm.reset();
  copyKeyInput.disabled = false;
  copyFormError.textContent = '';
  editingCopyId = null;
}

function openCategoryModal(mode, category = null) {
  if (mode === 'edit' && category) {
    editingCategoryId = category.id;
    categoryFormTitle.textContent = `Edit ${category.label || 'category'}`;
    categoryLabelInput.value = category.label || '';
    categoryTypeSelect.value = category.type || 'expense';
    categoryCodeInput.value = Number.isFinite(Number(category.code)) ? Number(category.code) : '';
    if (categoryClientRequiredInput) {
      categoryClientRequiredInput.checked = Boolean(category.requiresClient);
    }
  } else {
    editingCategoryId = null;
    categoryFormTitle.textContent = 'New category';
    categoryForm.reset();
    categoryTypeSelect.value = 'expense';
    if (categoryClientRequiredInput) {
      categoryClientRequiredInput.checked = false;
    }
  }
  categoryFormError.textContent = '';
  categoryModal.classList.remove('hidden');
  categoryLabelInput.focus();
}

function closeCategoryModal() {
  categoryModal.classList.add('hidden');
  categoryForm.reset();
  categoryTypeSelect.value = 'expense';
  if (categoryClientRequiredInput) {
    categoryClientRequiredInput.checked = false;
  }
  categoryFormError.textContent = '';
  editingCategoryId = null;
}

async function deleteCategoryById(categoryId) {
  const category = categoryLookup.get(categoryId);
  const label = category?.label || categoryId;
  if (!window.confirm(`Delete category "${label}"? Entries already saved will keep their label.`)) {
    return;
  }
  await deleteDoc(doc(db, 'categories', categoryId));
}

async function deleteSeasonById(seasonId) {
  const season = seasonLookup.get(seasonId);
  const label = season?.label?.en || season?.name?.en || seasonId;
  if (!window.confirm(`Delete season "${label}" and its offers? This cannot be undone.`)) return;
  const batch = writeBatch(db);
  batch.delete(doc(db, 'storageSeasons', seasonId));
  offers
    .filter((offer) => offer.seasonId === seasonId)
    .forEach((offer) => batch.delete(doc(db, 'storageOffers', offer.id)));
  await batch.commit();
}

async function deleteVehicleTypeById(typeId) {
  const entry = vehicleTypes.find((item) => item.id === typeId);
  const label = entry?.value || entry?.labels?.en || typeId;
  if (!window.confirm(`Delete vehicle type "${label}"?`)) return;
  await deleteDoc(doc(db, 'vehicleTypes', typeId));
}

async function deleteOfferById(offerId) {
  const offer = offerLookup.get(offerId);
  const label = offer?.label?.en || offerId;
  if (!window.confirm(`Delete offer "${label}"?`)) return;
  await deleteDoc(doc(db, 'storageOffers', offerId));
}

async function deleteAddonById(addonId) {
  const addon = addOns.find((item) => item.id === addonId || item.code === addonId);
  const label = addon?.name?.en || addonId;
  if (!window.confirm(`Delete add-on "${label}"?`)) return;
  await deleteDoc(doc(db, 'storageAddOns', addonId));
}

async function deleteConditionById(conditionId) {
  const entry = conditions.find((item) => item.id === conditionId);
  const label = entry?.text?.en || conditionId;
  if (!window.confirm(`Delete condition "${label}"?`)) return;
  await deleteDoc(doc(db, 'storageConditions', conditionId));
}

async function deleteEtiquetteById(entryId) {
  const entry = etiquetteEntries.find((item) => item.id === entryId);
  const label = entry?.text?.en || entryId;
  if (!window.confirm(`Delete etiquette note "${label}"?`)) return;
  await deleteDoc(doc(db, 'storageEtiquette', entryId));
}

async function deleteCopyById(copyId) {
  const entry = marketingCopyEntries.find((item) => item.id === copyId);
  const label = entry?.key || copyId;
  if (!window.confirm(`Delete i18n entry "${label}"?`)) return;
  await deleteDoc(doc(db, 'i18nEntries', copyId));
}

function formatCurrency(value) {
  const formatter = new Intl.NumberFormat('en-CA', {
    style: 'currency',
    currency: 'CAD'
  });
  return formatter.format(Number(value) || 0);
}

accountList.addEventListener('click', (event) => {
  const button = event.target.closest('button[data-action="edit"]');
  if (!button) return;
  const account = accounts.find((item) => item.id === button.dataset.id);
  if (account) {
    openModal('edit', account);
  }
});

newAccountButton.addEventListener('click', () => {
  const defaultType = 'cash';
  openModal('create', { type: defaultType });
});

closeModalButton.addEventListener('click', () => {
  closeModal();
});

modal.addEventListener('click', (event) => {
  if (event.target === modal) {
    closeModal();
  }
});

if (clientTableBody) {
  clientTableBody.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action="edit-client"]');
    if (!button) return;
    const client = clients.find((item) => item.id === button.dataset.id);
    if (client) {
      openClientModal('edit', client);
    }
  });
}

if (newClientButton) {
  newClientButton.addEventListener('click', () => {
    openClientModal('create');
  });
}

if (closeClientModalButton) {
  closeClientModalButton.addEventListener('click', () => {
    closeClientModal();
  });
}

if (clientModal) {
  clientModal.addEventListener('click', (event) => {
    if (event.target === clientModal) {
      closeClientModal();
    }
  });
}

if (requestPublishButton) {
  requestPublishButton.addEventListener('click', async () => {
    if (!latestContentUpdateMs) {
      return;
    }
    publishRequestInFlight = true;
    updatePublishButtonState();
    try {
      await requestSitePublishCallable({ latestChangeAt: latestContentUpdateMs });
      if (publishStatusLabel) {
        publishStatusLabel.textContent = 'Publish requested. GitHub will redeploy shortly.';
      }
    } catch (error) {
      console.error('Publish request failed', error);
      alert(error.message || 'Publish request failed.');
    } finally {
      publishRequestInFlight = false;
      updatePublishButtonState();
    }
  });
}

if (pricingTabs) {
  pricingTabs.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-panel]');
    if (!button) return;
    event.preventDefault();
    showPricingPanel(button.dataset.panel);
  });
  showPricingPanel(activePricingPanel);
}

if (storageTableBody) {
  storageTableBody.addEventListener('click', (event) => {
    const viewButton = event.target.closest('button[data-action="view-storage"]');
    if (viewButton) {
      const request = storageRequests.find((item) => item.id === viewButton.dataset.id);
      if (request) {
        openStorageModal('view', request);
      }
      return;
    }
    const editButton = event.target.closest('button[data-action="edit-storage"]');
    if (editButton) {
      const request = storageRequests.find((item) => item.id === editButton.dataset.id);
      if (request) {
        openStorageModal('edit', request);
      }
    }
  });

  storageTableBody.addEventListener('change', async (event) => {
    const select = event.target.closest('select.storage-status-select');
    if (!select) return;
    const requestId = select.dataset.id;
    const newStatus = select.value;
    try {
      await updateDoc(doc(db, 'storageRequests', requestId), {
        status: newStatus,
        updatedAt: serverTimestamp(),
        updatedBy: auth.currentUser?.uid || null
      });
    } catch (error) {
      select.value = storageRequests.find((item) => item.id === requestId)?.status || 'new';
      alert(error.message);
    }
  });
}

if (newStorageRequestButton) {
  newStorageRequestButton.addEventListener('click', () => {
    openStorageModal('create');
  });
}

if (closeStorageModalButton) {
  closeStorageModalButton.addEventListener('click', () => {
    closeStorageModal();
  });
}

if (storageModal) {
  storageModal.addEventListener('click', (event) => {
    if (event.target === storageModal) {
      closeStorageModal();
    }
  });
}

if (seasonTableBody) {
  seasonTableBody.addEventListener('click', (event) => {
    const editButton = event.target.closest('button[data-action="edit-season"]');
    if (editButton) {
      const season = seasons.find((item) => item.id === editButton.dataset.id);
      if (season) {
        openSeasonModal('edit', season);
      }
      return;
    }
    const deleteButton = event.target.closest('button[data-action="delete-season"]');
    if (deleteButton) {
      deleteSeasonById(deleteButton.dataset.id);
    }
  });
}

if (vehicleTypeTableBody) {
  vehicleTypeTableBody.addEventListener('click', (event) => {
    const editButton = event.target.closest('button[data-action="edit-vehicle-type"]');
    if (editButton) {
      const entry = vehicleTypes.find((item) => item.id === editButton.dataset.id);
      if (entry) {
        openVehicleTypeModal('edit', entry);
      }
      return;
    }
    const deleteButton = event.target.closest('button[data-action="delete-vehicle-type"]');
    if (deleteButton) {
      deleteVehicleTypeById(deleteButton.dataset.id);
    }
  });
}

if (closeSeasonModalButton) {
  closeSeasonModalButton.addEventListener('click', () => {
    closeSeasonModal();
  });
}

if (seasonModal) {
  seasonModal.addEventListener('click', (event) => {
    if (event.target === seasonModal) {
      closeSeasonModal();
    }
  });
}

if (closeVehicleTypeModalButton) {
  closeVehicleTypeModalButton.addEventListener('click', () => {
    closeVehicleTypeModal();
  });
}

if (vehicleTypeModal) {
  vehicleTypeModal.addEventListener('click', (event) => {
    if (event.target === vehicleTypeModal) {
      closeVehicleTypeModal();
    }
  });
}

if (offerTableBody) {
  offerTableBody.addEventListener('click', (event) => {
    const editButton = event.target.closest('button[data-action="edit-offer"]');
    if (editButton) {
      const offer = offers.find((item) => item.id === editButton.dataset.id);
      if (offer) {
        openOfferModal('edit', offer);
      }
      return;
    }
    const deleteButton = event.target.closest('button[data-action="delete-offer"]');
    if (deleteButton) {
      deleteOfferById(deleteButton.dataset.id);
    }
  });
}

if (closeOfferModalButton) {
  closeOfferModalButton.addEventListener('click', () => {
    closeOfferModal();
  });
}

if (offerModal) {
  offerModal.addEventListener('click', (event) => {
    if (event.target === offerModal) {
      closeOfferModal();
    }
  });
}

if (offerPriceModeSelect) {
  offerPriceModeSelect.addEventListener('change', syncOfferPriceFields);
  syncOfferPriceFields();
}

if (addonTableBody) {
  addonTableBody.addEventListener('click', (event) => {
    const editButton = event.target.closest('button[data-action="edit-addon"]');
    if (editButton) {
      const addon = addOns.find((item) => item.id === editButton.dataset.id);
      if (addon) {
        openAddonModal('edit', addon);
      }
      return;
    }
    const deleteButton = event.target.closest('button[data-action="delete-addon"]');
    if (deleteButton) {
      deleteAddonById(deleteButton.dataset.id);
    }
  });
}

if (closeAddonModalButton) {
  closeAddonModalButton.addEventListener('click', () => {
    closeAddonModal();
  });
}

if (addonModal) {
  addonModal.addEventListener('click', (event) => {
    if (event.target === addonModal) {
      closeAddonModal();
    }
  });
}

if (copyTableBody) {
  copyTableBody.addEventListener('click', (event) => {
    const editButton = event.target.closest('button[data-action="edit-copy"]');
    if (editButton) {
      const entry = marketingCopyEntries.find((item) => item.id === editButton.dataset.id);
      if (entry) {
        openCopyModal('edit', entry);
      }
      return;
    }
    const deleteButton = event.target.closest('button[data-action="delete-copy"]');
    if (deleteButton) {
      deleteCopyById(deleteButton.dataset.id);
    }
  });
}

if (closeCopyModalButton) {
  closeCopyModalButton.addEventListener('click', () => {
    closeCopyModal();
  });
}

if (copyModal) {
  copyModal.addEventListener('click', (event) => {
    if (event.target === copyModal) {
      closeCopyModal();
    }
  });
}

if (categoryTableBody) {
  categoryTableBody.addEventListener('click', (event) => {
    const editButton = event.target.closest('button[data-action="edit-category"]');
    if (editButton) {
      const category = categoryLookup.get(editButton.dataset.id);
      if (category) {
        openCategoryModal('edit', category);
      }
      return;
    }
    const deleteButton = event.target.closest('button[data-action="delete-category"]');
    if (deleteButton) {
      deleteCategoryById(deleteButton.dataset.id);
    }
  });
}

if (newCategoryButton) {
  newCategoryButton.addEventListener('click', () => {
    openCategoryModal('create');
  });
}

if (closeCategoryModalButton) {
  closeCategoryModalButton.addEventListener('click', () => {
    closeCategoryModal();
  });
}

if (categoryModal) {
  categoryModal.addEventListener('click', (event) => {
    if (event.target === categoryModal) {
      closeCategoryModal();
    }
  });
}

if (conditionTableBody) {
  conditionTableBody.addEventListener('click', (event) => {
    const editButton = event.target.closest('button[data-action="edit-condition"]');
    if (editButton) {
      const entry = conditions.find((item) => item.id === editButton.dataset.id);
      if (entry) {
        openConditionModal('edit', entry);
      }
      return;
    }
    const deleteButton = event.target.closest('button[data-action="delete-condition"]');
    if (deleteButton) {
      deleteConditionById(deleteButton.dataset.id);
    }
  });
}

if (closeConditionModalButton) {
  closeConditionModalButton.addEventListener('click', () => {
    closeConditionModal();
  });
}

if (conditionModal) {
  conditionModal.addEventListener('click', (event) => {
    if (event.target === conditionModal) {
      closeConditionModal();
    }
  });
}

if (etiquetteTableBody) {
  etiquetteTableBody.addEventListener('click', (event) => {
    const editButton = event.target.closest('button[data-action="edit-etiquette"]');
    if (editButton) {
      const entry = etiquetteEntries.find((item) => item.id === editButton.dataset.id);
      if (entry) {
        openEtiquetteModal('edit', entry);
      }
      return;
    }
    const deleteButton = event.target.closest('button[data-action="delete-etiquette"]');
    if (deleteButton) {
      deleteEtiquetteById(deleteButton.dataset.id);
    }
  });
}

if (closeEtiquetteModalButton) {
  closeEtiquetteModalButton.addEventListener('click', () => {
    closeEtiquetteModal();
  });
}

if (etiquetteModal) {
  etiquetteModal.addEventListener('click', (event) => {
    if (event.target === etiquetteModal) {
      closeEtiquetteModal();
    }
  });
}

if (accountTypeSelect) {
  syncAccountDefaultVisibility();
  accountTypeSelect.addEventListener('change', () => {
    syncAccountDefaultVisibility();
  });
}

accountForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  accountFormError.textContent = '';

  const name = accountNameInput.value.trim();
  const description = accountDescriptionInput.value.trim();
  const openingBalanceValue = Number(accountOpeningInput.value);
  const openingDateValue = accountOpeningDateInput.value;
  const type = accountTypeSelect.value;
  const supportsCash = typeSupportsCash(type);
  const supportsEntity = typeSupportsEntity(type);
  const defaultCash = supportsCash && accountDefaultCashInput.checked;
  const defaultEntity = supportsEntity && accountDefaultEntityInput.checked;

  if (!name) {
    accountFormError.textContent = 'Account name is required.';
    return;
  }

  if (!Number.isFinite(openingBalanceValue)) {
    accountFormError.textContent = 'Enter a valid opening balance.';
    return;
  }
  if (!openingDateValue) {
    accountFormError.textContent = 'Select an opening date.';
    return;
  }
  const openingDateDate = new Date(`${openingDateValue}T00:00:00`);
  if (Number.isNaN(openingDateDate.getTime())) {
    accountFormError.textContent = 'Enter a valid opening date.';
    return;
  }
  const normalizedName = name.toLowerCase();
  const duplicate = accounts.find(
    (account) => account.name?.toLowerCase() === normalizedName && account.id !== editingAccountId
  );
  if (duplicate) {
    accountFormError.textContent = 'An account with that name already exists.';
    return;
  }

  const payload = {
    name,
    description,
    type,
    defaultCash,
    defaultEntity,
    openingBalance: Number(openingBalanceValue.toFixed(2)),
    openingDate: Timestamp.fromDate(openingDateDate),
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  try {
    if (editingAccountId) {
      await setDoc(doc(db, 'accounts', editingAccountId), payload, { merge: true });
      await enforceAccountDefaults(editingAccountId, { defaultCash, defaultEntity });
    } else {
      const docRef = await addDoc(collection(db, 'accounts'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
      await enforceAccountDefaults(docRef.id, { defaultCash, defaultEntity });
    }
    closeModal();
  } catch (error) {
    accountFormError.textContent = error.message;
  }
});

clientForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  clientFormError.textContent = '';

  const name = clientNameInput.value.trim();
  const rawPhone = clientPhoneInput.value.trim();
  const email = clientEmailInput.value.trim();
  const address = clientAddressInput.value.trim();
  const city = clientCityInput.value.trim();
  const province = clientProvinceSelect.value;
  const postalCode = clientPostalInput.value.trim();
  const active = clientActiveInput ? clientActiveInput.checked : true;
  const notes = clientNotesInput?.value?.trim() || '';

  if (!name) {
    clientFormError.textContent = 'Client name is required.';
    return;
  }
  if (!email) {
    clientFormError.textContent = 'Email is required.';
    return;
  }
  if (!address) {
    clientFormError.textContent = 'Mailing address is required.';
    return;
  }
  if (!city) {
    clientFormError.textContent = 'City is required.';
    return;
  }
  if (!province) {
    clientFormError.textContent = 'Select a province.';
    return;
  }
  if (!postalCode) {
    clientFormError.textContent = 'Postal code is required.';
    return;
  }

  const payload = {
    name,
    phone: formatClientPhone(rawPhone),
    email,
    address,
    city,
    province,
    postalCode,
    active,
    notes,
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  try {
    if (editingClientId) {
      await setDoc(
        doc(db, 'clients', editingClientId),
        {
          ...payload,
          updatedAt: serverTimestamp(),
          updatedBy: auth.currentUser?.uid || null
        },
        { merge: true }
      );
    } else {
      await addDoc(collection(db, 'clients'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null,
        updatedAt: serverTimestamp(),
        updatedBy: auth.currentUser?.uid || null
      });
    }
    closeClientModal();
  } catch (error) {
    clientFormError.textContent = error.message;
  }
});

storageForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  storageFormError.textContent = '';

  const vehicleType = storageVehicleTypeSelect.value;
  const vehicleLabel =
      VEHICLE_TYPE_OPTIONS.find((option) => option.value === vehicleType)?.label || vehicleType || '—';
  const contractAmountValue = storageAmountInput?.value?.trim();
  const contractAmount = contractAmountValue ? Number(contractAmountValue) : null;

  const payload = {
    season: storageSeasonSelect.value,
    clientId: storageClientSelect.value,
    vehicle: {
      type: vehicleType,
      typeLabel: vehicleLabel,
      brand: storageVehicleBrandInput.value.trim(),
      model: storageVehicleModelInput.value.trim(),
      colour: storageVehicleColourInput.value.trim(),
      lengthFeet: storageVehicleLengthInput.value ? Number(storageVehicleLengthInput.value) : null,
      year: storageVehicleYearInput.value ? Number(storageVehicleYearInput.value) : null,
      plate: storageVehiclePlateInput.value.trim(),
      province: storageVehicleProvinceSelect.value
    },
    insuranceCompany: storageInsuranceCompanyInput.value.trim(),
    policyNumber: storagePolicyNumberInput.value.trim(),
    insuranceExpiration: storageInsuranceExpirationInput.value
      ? Timestamp.fromDate(new Date(`${storageInsuranceExpirationInput.value}T00:00:00`))
      : null,
    status: storageStatusSelect.value || 'new',
    addons: {
      battery: storageAddonBatteryInput.checked,
      propane: storageAddonPropaneInput.checked
    },
    contractAmount: Number.isFinite(contractAmount) ? contractAmount : null,
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  if (!payload.season) {
    storageFormError.textContent = 'Select a season.';
    return;
  }
  if (!payload.clientId) {
    storageFormError.textContent = 'Select a client.';
    return;
  }
  if (!vehicleType) {
    storageFormError.textContent = 'Select a vehicle type.';
    return;
  }
  if (contractAmountValue && !Number.isFinite(contractAmount)) {
    storageFormError.textContent = 'Enter a valid amount.';
    return;
  }

  try {
    if (editingStorageRequestId) {
      await setDoc(
        doc(db, 'storageRequests', editingStorageRequestId),
        {
          ...payload,
          updatedAt: serverTimestamp(),
          updatedBy: auth.currentUser?.uid || null
        },
        { merge: true }
      );
    } else {
      await addDoc(collection(db, 'storageRequests'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null,
        updatedAt: serverTimestamp(),
        updatedBy: auth.currentUser?.uid || null
      });
    }
    closeStorageModal();
  } catch (error) {
    storageFormError.textContent = error.message;
  }
});

seasonForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  seasonFormError.textContent = '';

  const payload = {
    name: { en: seasonNameEnInput.value.trim(), fr: seasonNameFrInput.value.trim() },
    label: { en: seasonLabelEnInput.value.trim(), fr: seasonLabelFrInput.value.trim() },
    timeframe: { en: seasonTimeframeEnInput.value.trim(), fr: seasonTimeframeFrInput.value.trim() },
    dropoffWindow: { en: seasonDropoffEnInput.value.trim(), fr: seasonDropoffFrInput.value.trim() },
    pickupDeadline: { en: seasonPickupEnInput.value.trim(), fr: seasonPickupFrInput.value.trim() },
    description: { en: seasonDescriptionEnInput.value.trim(), fr: seasonDescriptionFrInput.value.trim() },
    order: Number(seasonOrderInput.value) || 0,
    active: seasonActiveInput.checked,
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  if (!payload.name.en || !payload.name.fr) {
    seasonFormError.textContent = 'Provide both English and French names.';
    return;
  }

  try {
    if (editingSeasonId) {
      await setDoc(doc(db, 'storageSeasons', editingSeasonId), payload, { merge: true });
    } else {
      await addDoc(collection(db, 'storageSeasons'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
    }
    closeSeasonModal();
  } catch (error) {
    seasonFormError.textContent = error.message;
  }
});

if (vehicleTypeForm) {
  vehicleTypeForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    vehicleTypeFormError.textContent = '';
    const value = vehicleTypeValueInput.value.trim();
    const labelEn = vehicleTypeLabelEnInput.value.trim();
    const labelFr = vehicleTypeLabelFrInput.value.trim();
    const slugInput = vehicleTypeSlugInput.value.trim();
    const orderValue = Number(vehicleTypeOrderInput.value) || 0;
    const legacyValues = vehicleTypeLegacyInput.value
      ? vehicleTypeLegacyInput.value
          .split(',')
          .map((entry) => entry.trim())
          .filter(Boolean)
      : [];

    if (!value || !labelEn || !labelFr) {
      vehicleTypeFormError.textContent = 'Value and both labels are required.';
      return;
    }

    const payload = {
      value,
      labels: { en: labelEn, fr: labelFr },
      slug: slugInput || slugify(labelEn || value),
      order: orderValue,
      legacyValues,
      updatedAt: serverTimestamp(),
      updatedBy: auth.currentUser?.uid || null
    };

    try {
      if (editingVehicleTypeId) {
        await setDoc(doc(db, 'vehicleTypes', editingVehicleTypeId), payload, { merge: true });
      } else {
        await addDoc(collection(db, 'vehicleTypes'), {
          ...payload,
          createdAt: serverTimestamp(),
          createdBy: auth.currentUser?.uid || null
        });
      }
      closeVehicleTypeModal();
    } catch (error) {
      vehicleTypeFormError.textContent = error.message;
    }
  });
}

offerForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  offerFormError.textContent = '';

  const seasonId = offerSeasonSelect.value;
  const priceMode = offerPriceModeSelect.value;
  const price = { mode: priceMode };
  if (priceMode === 'flat') {
    price.amount = offerFlatAmountInput.value ? Number(offerFlatAmountInput.value) : null;
  } else if (priceMode === 'perFoot') {
    price.rate = offerPriceRateInput.value ? Number(offerPriceRateInput.value) : null;
    price.minimum = offerMinimumInput.value ? Number(offerMinimumInput.value) : null;
    price.unit = {
      en: offerPriceUnitEnInput.value.trim() || '/ ft',
      fr: offerPriceUnitFrInput.value.trim() || '/ pi'
    };
  }

  const payload = {
    seasonId,
    label: { en: offerLabelEnInput.value.trim(), fr: offerLabelFrInput.value.trim() },
    price,
    vehicleTypes: offerVehicleTypesInput.value
      ? offerVehicleTypesInput.value.split(',').map((type) => type.trim()).filter(Boolean)
      : [],
    note: { en: offerNoteEnInput.value.trim(), fr: offerNoteFrInput.value.trim() },
    hideInTable: offerHideInput.checked,
    order: Number(offerOrderInput.value) || 0,
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  if (!seasonId) {
    offerFormError.textContent = 'Select a season.';
    return;
  }
  if (!payload.label.en || !payload.label.fr) {
    offerFormError.textContent = 'Provide labels in both languages.';
    return;
  }
  if (priceMode === 'flat' && (price.amount == null || Number.isNaN(price.amount))) {
    offerFormError.textContent = 'Enter a flat amount.';
    return;
  }
  if (priceMode === 'perFoot') {
    if (price.rate == null || Number.isNaN(price.rate)) {
      offerFormError.textContent = 'Enter a per-foot rate.';
      return;
    }
    if (price.minimum == null || Number.isNaN(price.minimum)) {
      offerFormError.textContent = 'Enter a minimum for per-foot pricing.';
      return;
    }
  }

  try {
    if (editingOfferId) {
      await setDoc(doc(db, 'storageOffers', editingOfferId), payload, { merge: true });
    } else {
      await addDoc(collection(db, 'storageOffers'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
    }
    closeOfferModal();
  } catch (error) {
    offerFormError.textContent = error.message;
  }
});

addonForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  addonFormError.textContent = '';

  const payload = {
    code: addonCodeInput.value.trim(),
    name: { en: addonNameEnInput.value.trim(), fr: addonNameFrInput.value.trim() },
    description: { en: addonDescriptionEnInput.value.trim(), fr: addonDescriptionFrInput.value.trim() },
    price: addonPriceInput.value ? Number(addonPriceInput.value) : 0,
    order: Number(addonOrderInput.value) || 0,
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  if (!payload.code) {
    addonFormError.textContent = 'Add-on code is required.';
    return;
  }
  if (!payload.name.en || !payload.name.fr) {
    addonFormError.textContent = 'Provide English and French names.';
    return;
  }

  try {
    if (editingAddonId) {
      await setDoc(doc(db, 'storageAddOns', editingAddonId), payload, { merge: true });
    } else {
      await setDoc(doc(db, 'storageAddOns', payload.code), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
    }
    closeAddonModal();
  } catch (error) {
    addonFormError.textContent = error.message;
  }
});

conditionForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  conditionFormError.textContent = '';

  const payload = {
    text: {
      en: conditionTextEnInput.value.trim(),
      fr: conditionTextFrInput.value.trim()
    },
    tooltip: {
      en: conditionTooltipEnInput.value.trim(),
      fr: conditionTooltipFrInput.value.trim()
    },
    order: Number(conditionOrderInput.value) || 0,
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  if (!payload.text.en || !payload.text.fr) {
    conditionFormError.textContent = 'Provide condition text in both languages.';
    return;
  }

  try {
    if (editingConditionId) {
      await setDoc(doc(db, 'storageConditions', editingConditionId), payload, { merge: true });
    } else {
      await addDoc(collection(db, 'storageConditions'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
    }
    closeConditionModal();
  } catch (error) {
    conditionFormError.textContent = error.message;
  }
});

etiquetteForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  etiquetteFormError.textContent = '';

  const payload = {
    text: {
      en: etiquetteTextEnInput.value.trim(),
      fr: etiquetteTextFrInput.value.trim()
    },
    tooltip: {
      en: etiquetteTooltipEnInput.value.trim(),
      fr: etiquetteTooltipFrInput.value.trim()
    },
    order: Number(etiquetteOrderInput.value) || 0,
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  if (!payload.text.en || !payload.text.fr) {
    etiquetteFormError.textContent = 'Provide etiquette text in both languages.';
    return;
  }

  try {
    if (editingEtiquetteId) {
      await setDoc(doc(db, 'storageEtiquette', editingEtiquetteId), payload, { merge: true });
    } else {
      await addDoc(collection(db, 'storageEtiquette'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
    }
    closeEtiquetteModal();
  } catch (error) {
    etiquetteFormError.textContent = error.message;
  }
});

copyForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  copyFormError.textContent = '';

  const payload = {
    key: copyKeyInput.value.trim(),
    category: copyCategoryInput.value.trim(),
    text: {
      en: copyTextEnInput.value.trim(),
      fr: copyTextFrInput.value.trim()
    },
    hint: copyHintInput.value.trim() || '',
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  if (!payload.key) {
    copyFormError.textContent = 'Key is required.';
    return;
  }
  if (!payload.text.en || !payload.text.fr) {
    copyFormError.textContent = 'Provide both English and French text.';
    return;
  }

  try {
    if (editingCopyId) {
      await setDoc(doc(db, 'i18nEntries', editingCopyId), payload, { merge: true });
    } else {
      await setDoc(doc(db, 'i18nEntries', payload.key), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
    }
    closeCopyModal();
  } catch (error) {
    copyFormError.textContent = error.message;
  }
});

categoryForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  categoryFormError.textContent = '';

  const label = categoryLabelInput.value.trim();
  const type = categoryTypeSelect.value;
  const codeValue = Number(categoryCodeInput.value);
  const requiresClient = Boolean(categoryClientRequiredInput?.checked);

  if (!label) {
    categoryFormError.textContent = 'Label is required.';
    return;
  }
  if (type !== 'income' && type !== 'expense') {
    categoryFormError.textContent = 'Select a valid type.';
    return;
  }
  if (!Number.isFinite(codeValue)) {
    categoryFormError.textContent = 'Enter a valid numeric code.';
    return;
  }

  const payload = {
    label,
    type,
    code: codeValue,
    requiresClient,
    updatedAt: serverTimestamp(),
    updatedBy: auth.currentUser?.uid || null
  };

  try {
    if (editingCategoryId) {
      await setDoc(doc(db, 'categories', editingCategoryId), payload, { merge: true });
    } else {
      await addDoc(collection(db, 'categories'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
    }
    closeCategoryModal();
  } catch (error) {
    categoryFormError.textContent = error.message;
  }
});

if (loginForm) {
  if (usingEmulators) {
    loginForm.classList.remove('hidden');
    loginForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      loginError.textContent = '';
      const email = document.getElementById('login-email').value.trim();
      const password = document.getElementById('login-password').value;
      try {
        await signInWithEmailAndPassword(auth, email, password);
      } catch (error) {
        loginError.textContent = error.message;
      }
    });
  } else {
    loginForm.classList.add('hidden');
  }
}

if (googleSignInButton) {
  googleSignInButton.addEventListener('click', async () => {
    loginError.textContent = '';
    try {
      await signInWithPopup(auth, googleProvider);
    } catch (error) {
      loginError.textContent = error.message;
    }
  });
}


signOutButton.addEventListener('click', async () => {
  await signOut(auth);
});

onAuthStateChanged(auth, (user) => {
  if (!user) {
    showAppUI(false);
    cleanAccountSubscription();
    cleanExpensesSubscription();
    cleanClientSubscription();
    cleanStorageSubscription();
    cleanSeasonSubscription();
    cleanVehicleTypeSubscription();
    cleanOfferSubscription();
    cleanAddonSubscription();
    cleanConditionSubscription();
    cleanEtiquetteSubscription();
    cleanMarketingCopySubscription();
    cleanCategorySubscription();
    cleanAccounts();
    cleanClients();
    cleanStorageRequests();
    cleanSeasonsData();
    cleanVehicleTypesData();
    cleanOffersData();
    cleanAddOnsData();
    cleanConditionsData();
    cleanEtiquetteData();
    cleanMarketingCopyData();
    cleanCategoriesData();
    hideEntryModal();
    closeModal();
    closeClientModal();
    closeStorageModal();
    closeSeasonModal();
    closeOfferModal();
    closeAddonModal();
    ledgerAccountSelection = [];
    if (ledgerFilterMenu) {
      ledgerFilterMenu.classList.add('hidden');
    }
    if (ledgerFilterSummary) {
      ledgerFilterSummary.textContent = 'All accounts';
    }
    activeUser.textContent = '';
    return;
  }

  showAppUI(true);
  activeUser.textContent = user.displayName || user.email || 'Anonymous user';
  subscribeToAccounts();
  subscribeToClients();
  subscribeToStorageRequests();
  subscribeToSeasons();
  subscribeToVehicleTypes();
  subscribeToOffers();
  subscribeToAddOns();
  subscribeToConditions();
  subscribeToEtiquette();
  subscribeToMarketingCopy();
  subscribeToCategories();
  subscribeToPublishStatus();
  subscribeToExpensesStream();
  ledgerAccountSelection = [];
  setView('ledger');
});
navLinks.forEach((link) => {
  link.addEventListener('click', () => {
    if (link.dataset.view) {
      setView(link.dataset.view);
    }
  });
});

settingsNavButtons.forEach((button) => {
  button.addEventListener('click', () => {
    const target = button.dataset.settingsTarget;
    if (!target) return;
    if (currentView !== 'settings') {
      setView('settings');
    }
    setSettingsSection(target);
  });
});

if (closeSettingsNavButton) {
  closeSettingsNavButton.addEventListener('click', () => {
    setView(lastNonSettingsView || 'ledger');
  });
}

function setView(view) {
  if (view !== 'settings') {
    lastNonSettingsView = view;
  }
  currentView = view;
  navLinks.forEach((link) => {
    link.classList.toggle('active', link.dataset.view === view);
  });
  const showingLedger = view === 'ledger';
  const showingAccounts = view === 'accounts';
  const showingClients = view === 'clients';
  const showingStorage = view === 'storage';
  const showingPricing = view === 'pricing';
  const showingSettings = view === 'settings';
  if (mainNav) {
    mainNav.classList.toggle('hidden', showingSettings);
  }
  if (settingsNav) {
    settingsNav.classList.toggle('hidden', !showingSettings);
  }
  accountsView.classList.toggle('hidden', !showingAccounts);
  clientsView.classList.toggle('hidden', !showingClients);
  storageView.classList.toggle('hidden', !showingStorage);
  const pricingVisible = showingPricing || (showingSettings && activeSettingsSection === 'pricing');
  pricingView.classList.toggle('hidden', !pricingVisible);
  ledgerView.classList.toggle('hidden', !showingLedger);
  if (settingsView) {
    const showCategories = showingSettings && activeSettingsSection === 'categories';
    settingsView.classList.toggle('hidden', !showCategories);
  }
  if (!showingSettings) {
    activeSettingsSection = null;
    settingsNavButtons.forEach((button) => button.classList.remove('active'));
    if (settingsView) {
      settingsView.classList.add('hidden');
    }
    if (!showingPricing) {
      pricingView.classList.add('hidden');
    }
    updateSettingsActionsVisibility();
  } else {
    setSettingsSection(activeSettingsSection);
  }
  newAccountButton.classList.toggle('hidden', !showingAccounts);
  if (accountBalanceStatus) {
    if (showingAccounts) {
      updateAccountBalanceIndicator();
    } else {
      accountBalanceStatus.classList.add('hidden');
    }
  }
  newClientButton.classList.toggle('hidden', !showingClients);
  newStorageRequestButton.classList.toggle('hidden', !showingStorage);
  addEntryButton.classList.toggle('hidden', !showingLedger);
  transferButton.classList.toggle('hidden', !showingLedger);
  updatePublishButtonState();
  updatePricingToolbarActions();
  if (showingLedger) {
    panelTitle.textContent = 'Ledger';
    panelSubtitle.textContent = 'Reverse chronological table of every entry.';
    updateLedgerAccountOptions();
  } else if (showingClients) {
    panelTitle.textContent = 'Clients';
    panelSubtitle.textContent = 'Contacts with addresses and billing details.';
    renderClientTable();
  } else if (showingStorage) {
    panelTitle.textContent = 'Storage requests';
    panelSubtitle.textContent = 'Workflow for each seasonal storage tenant.';
    renderStorageTable();
  } else if (showingPricing) {
    panelTitle.textContent = 'Pricing';
    panelSubtitle.textContent = 'Manage seasons, offers, add-ons, policies, etiquette, and site copy.';
    renderSeasonTable();
    renderOfferTable();
    renderAddonTable();
    renderConditionTable();
    renderEtiquetteTable();
    renderCopyTable();
  } else if (!showingSettings) {
    panelTitle.textContent = 'Accounts';
    panelSubtitle.textContent = 'Cash, entity, and hybrid accounts.';
    renderAccountList();
  }
}

function setSettingsSection(section) {
  if (currentView !== 'settings') {
    return;
  }
  activeSettingsSection = section || null;
  settingsNavButtons.forEach((button) => {
    button.classList.toggle('active', button.dataset.settingsTarget === activeSettingsSection);
  });
  const showCategories = activeSettingsSection === 'categories';
  const showPricing = activeSettingsSection === 'pricing';
  if (settingsView) {
    settingsView.classList.toggle('hidden', !showCategories);
  }
  pricingView.classList.toggle('hidden', !(showPricing || currentView === 'pricing'));
  updateSettingsActionsVisibility();
  if (!activeSettingsSection) {
    panelTitle.textContent = 'Settings';
    panelSubtitle.textContent = 'Choose a section to manage.';
  } else if (activeSettingsSection === 'categories') {
    panelTitle.textContent = 'Categories';
    panelSubtitle.textContent = 'Manage ledger categories and client requirements.';
    renderCategoryTable();
  } else if (activeSettingsSection === 'pricing') {
    panelTitle.textContent = 'Pricing';
    panelSubtitle.textContent = 'Manage seasons, offers, add-ons, policies, etiquette, and site copy.';
    renderSeasonTable();
    renderOfferTable();
    renderAddonTable();
    renderConditionTable();
    renderEtiquetteTable();
    renderCopyTable();
  }
  updatePricingToolbarActions();
  updatePublishButtonState();
}

function updateSettingsActionsVisibility() {
  if (!newCategoryButton) return;
  const shouldShow = currentView === 'settings' && activeSettingsSection === 'categories';
  newCategoryButton.classList.toggle('hidden', !shouldShow);
}

function isPricingViewActive() {
  if (currentView === 'pricing') return true;
  return currentView === 'settings' && activeSettingsSection === 'pricing';
}

function renderLedgerTable() {
  ledgerTableBody.innerHTML = '';
  const filterSet = new Set(ledgerAccountSelection);
  const useFilter = ledgerFilterCustom && filterSet.size && filterSet.size < accounts.length;
  let filteredEntries = useFilter
    ? expenses.filter(
        (entry) => filterSet.has(entry.accountId) || (entry.entityId && filterSet.has(entry.entityId))
      )
    : expenses;
  if (ledgerTagFilters.length) {
    filteredEntries = filteredEntries.filter((entry) => {
      if (!Array.isArray(entry.tags) || !entry.tags.length) return false;
      const lowerTags = entry.tags.map((tag) => tag.toLowerCase());
      return ledgerTagFilters.every((filterTag) => lowerTags.includes(filterTag));
    });
  }
  filteredEntries = [...filteredEntries];

  if (!ledgerTagFilters.length) {
    const accountsToSeed = accounts.filter((account) => !useFilter || filterSet.has(account.id));
    accountsToSeed.forEach((account) => {
      const openingValue = Number(account.openingBalance) || 0;
      if (!Number.isFinite(openingValue)) return;
      const isCash = isCashAccount(account);
      const isEntity = isEntityAccount(account);
      const resolvedOpeningDate = resolveAccountOpeningDate(account) || new Date(0);
      const virtualEntry = {
        transactionId: `opening-${account.id}`,
        date: resolvedOpeningDate,
        accountId: account.id,
        entityId: isEntity ? account.id : null,
        entryType: 'income',
        category: 'Opening balance',
        description: 'Opening balance',
        amount: openingValue,
        isVirtualOpening: true,
        entityOnly: !isCash && isEntity
      };
      filteredEntries.unshift(virtualEntry);
    });
  }
  if (!filteredEntries.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 8;
    cell.className = 'empty';
    cell.textContent = 'No entries yet.';
    row.appendChild(cell);
    ledgerTableBody.appendChild(row);
    return;
  }

  const cashRunning = new Map();
  const entityRunning = new Map();
  const txnStripeMap = new Map();
  const txnActionsRendered = new Set();
  let stripeToggle = false;
  const stripePalette = {
    'txn-stripe-a': { background: '#ffffff', color: '#1f2937' },
    'txn-stripe-b': { background: '#e2e8f0', color: '#1f2937' }
  };
const applyStripeColors = (rowEl, stripeClass) => {
  const palette = stripePalette[stripeClass] || stripePalette['txn-stripe-a'];
  Array.from(rowEl.cells || []).forEach((cell) => {
    cell.style.backgroundColor = palette.background;
    cell.style.color = palette.color;
  });
  return palette;
};

const applyAmountColor = (rowEl, amountValue, fallbackColor) => {
  const amountCell = rowEl.querySelector('.ledger-amount');
  if (!amountCell) return;
  if (amountValue < 0) {
    amountCell.style.color = amountNegativeColor;
  } else if (amountValue > 0) {
    amountCell.style.color = amountPositiveColor;
  } else {
    amountCell.style.color = fallbackColor;
  }
};

  const getTxnActionsMarkup = (entry, txnKey) => {
    if (entry.isVirtualOpening) return '';
    if (txnActionsRendered.has(txnKey)) return '';
    txnActionsRendered.add(txnKey);
    return `
      <div class="table-actions">
        <button
          type="button"
          class="icon-button edit-txn"
          data-id="${entry.id}"
          aria-label="Edit transaction ${txnKey}"
        >
          <img src="icons/pencil.svg" alt="Edit transaction ${txnKey}" />
        </button>
        <button
          type="button"
          class="icon-button delete-txn"
          data-txn="${txnKey}"
          aria-label="Delete transaction ${txnKey}"
        >
          <img src="icons/trash.svg" alt="Delete transaction ${txnKey}" />
        </button>
      </div>
    `;
  };

  filteredEntries
    .sort((a, b) => {
      const aDate = a.date?.toMillis ? a.date.toMillis() : new Date(a.date).getTime();
      const bDate = b.date?.toMillis ? b.date.toMillis() : new Date(b.date).getTime();
      if (bDate !== aDate) {
        return bDate - aDate;
      }
      const aTxn = a.transactionId || a.id || '';
      const bTxn = b.transactionId || b.id || '';
      if (aTxn !== bTxn) {
        return aTxn.localeCompare(bTxn);
      }
      const aIsCashRow = !a.entityOnly;
      const bIsCashRow = !b.entityOnly;
      if (aIsCashRow !== bIsCashRow) {
        return aIsCashRow ? -1 : 1;
      }
      return (a.accountId || '').localeCompare(b.accountId || '');
    })
    .forEach((entry) => {
    const account = accountLookup.get(entry.accountId);
    const opening = Number(account?.openingBalance) || 0;
    const accountFinal = (accountAdjustments.get(entry.accountId) || 0) + opening;
    const previousCash = cashRunning.has(entry.accountId) ? cashRunning.get(entry.accountId) : accountFinal;
    const delta = entry.isVirtualOpening ? 0 : getEntryDelta(entry);
    const displayAmount = entry.isVirtualOpening ? Number(account?.openingBalance) || 0 : delta;
    const balance = entry.isVirtualOpening ? Number(account?.openingBalance) || 0 : previousCash;
    cashRunning.set(entry.accountId, previousCash - delta);

    const includeCash = !entry.entityOnly && (!useFilter || filterSet.has(entry.accountId));
    const includeEntity =
      entry.entityId &&
      (!useFilter || filterSet.has(entry.entityId)) &&
      (entry.entityId !== entry.accountId || entry.entityOnly);

    const txnKey = entry.transactionId || entry.id;
    if (!txnStripeMap.has(txnKey)) {
      txnStripeMap.set(txnKey, stripeToggle ? 'txn-stripe-b' : 'txn-stripe-a');
      stripeToggle = !stripeToggle;
    }
  const rowStripeClass = txnStripeMap.get(txnKey);

    if (includeCash) {
      const row = document.createElement('tr');
      row.className = 'ledger-account-row';
      row.classList.add(rowStripeClass);
      row.dataset.txnId = txnKey || '';
      const actionsMarkup = getTxnActionsMarkup(entry, txnKey);
      row.innerHTML = `
        <td>${entry.isVirtualOpening ? 'Opening balance' : txnKey}</td>
        <td>${formatDate(entry.date)}</td>
        <td>${account?.name || 'Unknown'}</td>
        <td>${renderLedgerDescription(entry)}</td>
        <td>${entry.category || ''}</td>
        <td class="ledger-amount">${formatCurrency(displayAmount)}</td>
        <td>${formatCurrency(balance)}</td>
        <td>${actionsMarkup}</td>
      `;
      const cashPalette = applyStripeColors(row, rowStripeClass);
      applyAmountColor(row, displayAmount, cashPalette.color);
      ledgerTableBody.appendChild(row);
    }

    if (includeEntity && entry.entityId) {
      const entity = accountLookup.get(entry.entityId);
      const entityOpening = Number(entity?.openingBalance) || 0;
      const entityFinal = (entityAdjustments.get(entry.entityId) || 0) + entityOpening;
      const previousEntity = entityRunning.has(entry.entityId) ? entityRunning.get(entry.entityId) : entityFinal;
      const entityDelta = entry.isVirtualOpening ? 0 : getEntryDelta(entry);
      const entityDisplayAmount = entry.isVirtualOpening ? entityOpening : entityDelta;
      const entityBalance = entry.isVirtualOpening ? entityOpening : previousEntity;
      entityRunning.set(entry.entityId, previousEntity - entityDelta);
      const entityRow = document.createElement('tr');
      entityRow.className = 'ledger-entity-row';
      entityRow.classList.add(rowStripeClass);
      entityRow.dataset.txnId = txnKey || '';
      const entityActionsMarkup = getTxnActionsMarkup(entry, txnKey);
      entityRow.innerHTML = `
        <td>${entry.isVirtualOpening ? 'Opening balance' : txnKey}</td>
        <td>${formatDate(entry.date)}</td>
        <td>${entity?.name || 'Entity'}</td>
        <td>${renderLedgerDescription(entry)}</td>
        <td>${entry.category || ''}</td>
        <td class="ledger-amount">${formatCurrency(entityDisplayAmount)}</td>
        <td>${formatCurrency(entityBalance)}</td>
        <td>${entityActionsMarkup}</td>
      `;
      const entityPalette = applyStripeColors(entityRow, rowStripeClass);
      applyAmountColor(entityRow, entityDisplayAmount, entityPalette.color);
      ledgerTableBody.appendChild(entityRow);
    }
  });
}

function toDateObject(value) {
  if (!value) return null;
  if (typeof value.toDate === 'function') {
    return value.toDate();
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date;
}

function resolveAccountOpeningDate(account) {
  return toDateObject(account?.openingDate) || toDateObject(account?.createdAt) || null;
}

function formatDate(raw) {
  const date = toDateObject(raw);
  if (!date) return '';
  return date.toLocaleDateString();
}

function renderLedgerDescription(entry) {
  const description = entry.description || '';
  const returnLabel = entry.isReturn
    ? `<span class="return-label">${entry.entryType === 'expense' ? 'Return' : 'Adjustment'}</span>`
    : '';
  const spacer = description && returnLabel ? ' ' : '';
  const clientName = entry.clientId ? clientLookup.get(entry.clientId)?.name || 'Client' : '';
  const clientLabel = clientName ? `<span class="client-label">${clientName}</span>` : '';
  const clientSpacer = (description || returnLabel) && clientLabel ? ' ' : '';
  return `${description}${spacer}${returnLabel}${clientSpacer}${clientLabel}${renderTagList(entry)}`;
}

function renderTagList(entry) {
  if (!Array.isArray(entry.tags) || !entry.tags.length) return '';
  const chips = entry.tags
    .map((tag) => `<span class="table-tag">${tag}</span>`)
    .join('');
  return `<div class="table-tags">${chips}</div>`;
}

if (toggleLedgerFilterButton) {
  toggleLedgerFilterButton.addEventListener('click', () => {
    ledgerFilterMenu.classList.toggle('hidden');
    syncLedgerFilterUI();
  });
}

if (closeLedgerFilterButton) {
  closeLedgerFilterButton.addEventListener('click', () => {
    ledgerFilterMenu.classList.add('hidden');
  });
}

if (resetLedgerFilterButton) {
  resetLedgerFilterButton.addEventListener('click', () => {
    applyLedgerFilterSelection(accounts.map((acc) => acc.id), { custom: false });
    resetLedgerTagFilters();
  });
}

if (ledgerFilterList) {
  ledgerFilterList.addEventListener('change', (event) => {
    const selected = Array.from(ledgerFilterList.querySelectorAll('input[type="checkbox"]:checked')).map(
      (input) => input.value
    );
    if (!selected.length) {
      event.target.checked = true;
      return;
    }
    applyLedgerFilterSelection(selected, { custom: true });
  });
}
if (ledgerTagFilterInput) {
  ledgerTagFilterInput.addEventListener('input', (event) => {
    setLedgerTagFiltersFromInput(event.target.value);
  });
}

function setLedgerTagFiltersFromInput(value) {
  const tags = value
    .split(',')
    .map((tag) => tag.trim().toLowerCase())
    .filter(Boolean);
  ledgerTagFilters = Array.from(new Set(tags));
  renderLedgerTable();
  updateLedgerFilterSummary();
}

function resetLedgerTagFilters() {
  ledgerTagFilters = [];
  if (ledgerTagFilterInput) {
    ledgerTagFilterInput.value = '';
  }
  renderLedgerTable();
  updateLedgerFilterSummary();
}

addEntryButton.addEventListener('click', () => {
  if (!cashAccounts.length || !entityAccounts.length) {
    entryFormError.textContent = 'Create at least one cash account and one entity before logging entries.';
    return;
  }
  entryForm.reset();
  entryFormError.textContent = '';
  editingEntryId = null;
  editingEntryTransactionId = null;
  entryFormTitle.textContent = 'Add ledger entry';
  selectedTags = [];
  renderSelectedTags();
  const defaultCashId = getDefaultCashAccountId();
  const defaultEntityId = getDefaultEntityAccountId();
  const preferredAccount =
    defaultCashId ||
    ledgerAccountSelection.find((id) => cashAccounts.some((acc) => acc.id === id)) ||
    cashAccounts[0]?.id ||
    '';
  entryAccountSelect.value = preferredAccount;
  entryEntitySelect.value = defaultEntityId || entityAccounts[0]?.id || '';
  if (entryReturnInput) {
    entryReturnInput.checked = false;
  }
  updateReturnLabel();
  setDateInputValue(entryDateInput, new Date(), true);
  updateEntryCategoryOptions({ forceType: entryTypeSelect.value, preserveSelection: false });
  updateEntryClientOptions();
  if (entryClientSelect) {
    entryClientSelect.value = '';
  }
  syncEntryClientVisibility();
  syncEntrySelectors();
  updateTagSuggestions();
  entryModal.classList.remove('hidden');
});

closeEntryModalButton.addEventListener('click', () => {
  hideEntryModal();
});

entryModal.addEventListener('click', (event) => {
  if (event.target === entryModal) {
    hideEntryModal();
  }
});

entryAccountSelect.addEventListener('change', () => {
  const selectedAccount = accountLookup.get(entryAccountSelect.value);
  if (isCombinedAccount(selectedAccount)) {
    entryEntitySelect.value = selectedAccount.id;
  }
  syncEntrySelectors();
});

entryEntitySelect.addEventListener('change', () => {
  const selectedEntity = accountLookup.get(entryEntitySelect.value);
  if (isCombinedAccount(selectedEntity)) {
    entryAccountSelect.value = selectedEntity.id;
  }
  syncEntrySelectors();
});

entryTypeSelect.addEventListener('change', () => {
  updateEntryCategoryOptions({ forceType: entryTypeSelect.value });
  syncEntryClientVisibility();
  updateReturnLabel();
});
updateReturnLabel();

if (entryCategorySelect) {
  entryCategorySelect.addEventListener('change', () => {
    syncEntryClientVisibility();
  });
}

tagInput.addEventListener('keydown', (event) => {
  if (event.key === 'Enter' || event.key === ',') {
    event.preventDefault();
    addTag(tagInput.value);
  } else if (event.key === 'Backspace' && !tagInput.value && selectedTags.length) {
    selectedTags.pop();
    renderSelectedTags();
  } else if (event.key === 'Escape') {
    tagSuggestionList.classList.add('hidden');
  }
});

tagInput.addEventListener('input', () => {
  updateTagSuggestions(tagInput.value);
});

tagInput.addEventListener('focus', () => {
  updateTagSuggestions(tagInput.value);
});

function setDateInputValue(input, rawDate, fallbackToday = false) {
  if (!input) return;
  const baseDate = toDateObject(rawDate) || (fallbackToday ? new Date() : null);
  if (!baseDate) {
    input.value = '';
    return;
  }
  const normalized = new Date(baseDate.getFullYear(), baseDate.getMonth(), baseDate.getDate());
  input.valueAsDate = normalized;
}

function startEditEntry(entry) {
  entryFormError.textContent = '';
  editingEntryId = entry.id;
  editingEntryTransactionId = entry.transactionId || entry.id;
  entryFormTitle.textContent = 'Edit ledger entry';
  entryAccountSelect.value = entry.accountId;
  entryEntitySelect.value = entry.entityId || '';
  if (!entryEntitySelect.value && entityAccounts.length) {
    entryEntitySelect.value = entityAccounts[0].id;
  }
  entryTypeSelect.value = entry.entryType;
  updateReturnLabel();
  updateEntryCategoryOptions({
    selectedId: entry.categoryId || '',
    fallbackLabel: entry.category || '',
    fallbackCode: entry.categoryCode
  });
  entryAmountInput.value = Number(entry.amount) || 0;
  if (entryReturnInput) {
    entryReturnInput.checked = Boolean(entry.isReturn);
  }
  entryDescriptionInput.value = entry.description || '';
  selectedTags = Array.isArray(entry.tags) ? [...entry.tags] : [];
  renderSelectedTags();
  setDateInputValue(entryDateInput, entry.date, true);
  updateEntryClientOptions();
  if (entryClientSelect) {
    entryClientSelect.value = entry.clientId || '';
  }
  syncEntryClientVisibility();
  syncEntrySelectors();
  updateTagSuggestions();
  entryModal.classList.remove('hidden');
}

function startEditTransfer(entry) {
  if (!entry?.transactionId) {
    startEditEntry(entry);
    return;
  }
  const transferEntries = expenses.filter((item) => item.transactionId === entry.transactionId);
  if (!transferEntries.length) {
    startEditEntry(entry);
    return;
  }
  const expenseEntry =
    transferEntries.find((item) => item.entryType === 'expense') ||
    (entry.entryType === 'expense' ? entry : null) ||
    transferEntries[0];
  const incomeEntry =
    transferEntries.find((item) => item.entryType === 'income') ||
    (entry.entryType === 'income' ? entry : null) ||
    transferEntries.find((item) => item.id !== expenseEntry.id);
  if (!expenseEntry || !incomeEntry || !expenseEntry.accountId || !incomeEntry.accountId) {
    startEditEntry(entry);
    return;
  }
  editingTransferContext = {
    transactionId: entry.transactionId,
    expenseId: expenseEntry.id,
    incomeId: incomeEntry.id
  };
  transferFormError.textContent = '';
  transferFromSelect.value = expenseEntry.accountId;
  transferToSelect.value = incomeEntry.accountId;
  const amountValue = Math.abs(Number(expenseEntry.amount) || Number(incomeEntry.amount) || 0);
  transferAmountInput.value = amountValue || '';
  transferNoteInput.value = expenseEntry.description || '';
  setDateInputValue(transferDateInput, expenseEntry.date || entry.date || incomeEntry.date, true);
  transferModal.classList.remove('hidden');
}

entryForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  entryFormError.textContent = '';

  if (!cashAccounts.length || !entityAccounts.length) {
    entryFormError.textContent = 'Create at least one cash account and one entity.';
    return;
  }

  let accountId = entryAccountSelect.value;
  let entityId = entryEntitySelect.value;
  const dateValue = entryDateInput.value;
  const entryType = entryTypeSelect.value;
  const selectedCategoryId = entryCategorySelect.value;
  const selectedCategoryOption = entryCategorySelect.options[entryCategorySelect.selectedIndex];
  const categoryLabelFromOption = selectedCategoryOption?.dataset?.label?.trim() || '';
  if (tagInput && tagInput.value.trim()) {
    addTag(tagInput.value);
  }
  const amountValue = Number(entryAmountInput.value);
  const description = entryDescriptionInput.value.trim();
  const tags = [...selectedTags];
  const selectedClientId = entryClientSelect ? entryClientSelect.value : '';
  const isReturn = Boolean(entryReturnInput?.checked);

  if (!accountId) {
    entryFormError.textContent = 'Select an account.';
    return;
  }

  if (!entityId) {
    entryFormError.textContent = 'Select an entity.';
    return;
  }

  if (!dateValue) {
    entryFormError.textContent = 'Choose a date.';
    return;
  }

  if ((!selectedCategoryId || selectedCategoryId === '') && !categoryLabelFromOption) {
    entryFormError.textContent = 'Select a category.';
    return;
  }

  if (!Number.isFinite(amountValue) || amountValue <= 0) {
    entryFormError.textContent = 'Enter a positive amount.';
    return;
  }

  const account = accountLookup.get(accountId);
  const entity = accountLookup.get(entityId);
  if (isCombinedAccount(account)) {
    entityId = accountId;
  } else if (isCombinedAccount(entity)) {
    accountId = entityId;
  }

  const isEditing = Boolean(editingEntryId);
  const transactionId = isEditing ? editingEntryTransactionId : generateTransactionId();
  const linkedCategory = categoryLookup.get(selectedCategoryId);
  const resolvedCategoryLabel = linkedCategory?.label || categoryLabelFromOption;
  const categoryCodeRaw = linkedCategory?.code ?? selectedCategoryOption?.dataset?.code;
  const categoryCodeNumber = Number(categoryCodeRaw);
  const normalizedCategoryCode = Number.isFinite(categoryCodeNumber) ? categoryCodeNumber : null;
  const requiresClientForEntry = Boolean(linkedCategory?.requiresClient);
  if (!resolvedCategoryLabel) {
    entryFormError.textContent = 'Select a category.';
    return;
  }
  if (requiresClientForEntry && !selectedClientId) {
    entryFormError.textContent = 'Select a client for this category.';
    return;
  }

  const payload = {
    accountId,
    entityId,
    date: Timestamp.fromDate(new Date(`${dateValue}T00:00:00`)),
    entryType,
    category: resolvedCategoryLabel,
    categoryId: linkedCategory?.id || null,
    categoryCode: normalizedCategoryCode,
    categoryType: linkedCategory?.type || entryType,
    amount: Number(amountValue.toFixed(2)),
    description,
    tags,
    transactionId,
    clientId: selectedClientId || null,
    isReturn
  };

  try {
    if (isEditing) {
      await updateDoc(doc(db, 'expenses', editingEntryId), {
        ...payload,
        updatedAt: serverTimestamp()
      });
    } else {
      await addDoc(collection(db, 'expenses'), {
        ...payload,
        createdAt: serverTimestamp(),
        createdBy: auth.currentUser?.uid || null
      });
    }
    hideEntryModal();
  } catch (error) {
    entryFormError.textContent = error.message;
  }
});

transferButton.addEventListener('click', () => {
  if (cashAccounts.length < 2) return;
  transferForm.reset();
  transferFormError.textContent = '';
  transferFromSelect.value = cashAccounts[0]?.id || '';
  transferToSelect.value = cashAccounts[1]?.id || '';
  setDateInputValue(transferDateInput, new Date(), true);
  editingTransferContext = null;
  transferModal.classList.remove('hidden');
});

closeTransferModalButton.addEventListener('click', () => {
  hideTransferModal();
});

transferModal.addEventListener('click', (event) => {
  if (event.target === transferModal) {
    hideTransferModal();
  }
});

transferForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  transferFormError.textContent = '';

  if (cashAccounts.length < 2) {
    transferFormError.textContent = 'You need at least two cash accounts.';
    return;
  }

  const fromId = transferFromSelect.value;
  const toId = transferToSelect.value;
  const dateValue = transferDateInput.value;
  const amountValue = Number(transferAmountInput.value);
  const note = transferNoteInput.value.trim();

  if (!fromId || !toId || fromId === toId) {
    transferFormError.textContent = 'Select two distinct cash accounts.';
    return;
  }

  if (!dateValue) {
    transferFormError.textContent = 'Choose a transfer date.';
    return;
  }

  const transferDate = new Date(`${dateValue}T00:00:00`);
  if (Number.isNaN(transferDate.getTime())) {
    transferFormError.textContent = 'Enter a valid transfer date.';
    return;
  }

  if (!Number.isFinite(amountValue) || amountValue <= 0) {
    transferFormError.textContent = 'Enter a positive amount.';
    return;
  }

  const isEditingTransfer = Boolean(editingTransferContext);
  const transferTransactionId = isEditingTransfer ? editingTransferContext.transactionId : generateTransactionId();
  const descriptionText =
    note || `Transfer ${accountLookup.get(fromId)?.name || ''} → ${accountLookup.get(toId)?.name || ''}`;
  const entryPayload = {
    date: Timestamp.fromDate(transferDate),
    category: 'Transfer',
    amount: Number(amountValue.toFixed(2)),
    description: descriptionText,
    transactionId: transferTransactionId
  };

  try {
    if (isEditingTransfer) {
      const updates = [
        updateDoc(doc(db, 'expenses', editingTransferContext.expenseId), {
          ...entryPayload,
          accountId: fromId,
          entryType: 'expense',
          updatedAt: serverTimestamp()
        }),
        updateDoc(doc(db, 'expenses', editingTransferContext.incomeId), {
          ...entryPayload,
          accountId: toId,
          entryType: 'income',
          updatedAt: serverTimestamp()
        })
      ];
      await Promise.all(updates);
    } else {
      const batch = [
        addDoc(collection(db, 'expenses'), {
          ...entryPayload,
          accountId: fromId,
          entryType: 'expense',
          createdAt: serverTimestamp(),
          createdBy: auth.currentUser?.uid || null
        }),
        addDoc(collection(db, 'expenses'), {
          ...entryPayload,
          accountId: toId,
          entryType: 'income',
          createdAt: serverTimestamp(),
          createdBy: auth.currentUser?.uid || null
        })
      ];
      await Promise.all(batch);
    }
    hideTransferModal();
  } catch (error) {
    transferFormError.textContent = error.message;
  }
});
ledgerTableBody.addEventListener('click', async (event) => {
  const deleteButton = event.target.closest('.delete-txn');
  const editButton = event.target.closest('.edit-txn');
  if (!deleteButton && !editButton) return;
  if (editButton) {
    const entry = expenses.find((item) => item.id === editButton.dataset.id);
    if (!entry) return;
    if (entry.category === 'Transfer' && entry.transactionId) {
      startEditTransfer(entry);
    } else {
      startEditEntry(entry);
    }
    return;
  }
  const txnId = deleteButton.dataset.txn;
  if (!txnId) {
    ledgerError.textContent = 'Missing transaction id.';
    ledgerErrorModal.classList.remove('hidden');
    return;
  }
  if (!window.confirm('Delete this transaction (both cash and entity entries)?')) {
    return;
  }
  try {
    const matching = expenses.filter((entry) => entry.transactionId === txnId || entry.id === txnId);
    if (matching.length) {
      await Promise.all(matching.map((entry) => deleteDoc(doc(db, 'expenses', entry.id))));
    } else {
      await deleteDoc(doc(db, 'expenses', txnId));
    }
    expenses = expenses.filter((entry) => entry.transactionId !== txnId && entry.id !== txnId);
    const { cashTotals, entityTotals } = calculateAdjustments(expenses);
    accountAdjustments = cashTotals;
    entityAdjustments = entityTotals;
    renderLedgerTable();
    renderAccountList();
    ledgerError.textContent = '';
    ledgerErrorModal.classList.add('hidden');
  } catch (error) {
    ledgerError.textContent = error.message;
    ledgerErrorModal.classList.remove('hidden');
  }
});

if (closeLedgerErrorButton) {
  closeLedgerErrorButton.addEventListener('click', () => {
    ledgerErrorModal.classList.add('hidden');
  });
}
function txnLabel(entry) {
  return entry.transactionId || entry.id || '—';
}
function renderSelectedTags() {
  selectedTagsContainer.innerHTML = '';
  selectedTags.forEach((tag) => {
    const chip = document.createElement('span');
    chip.className = 'tag-chip';
    chip.textContent = tag;
    const remove = document.createElement('button');
    remove.type = 'button';
    remove.textContent = '×';
    remove.addEventListener('click', () => {
      selectedTags = selectedTags.filter((t) => t !== tag);
      renderSelectedTags();
    });
    chip.appendChild(remove);
    selectedTagsContainer.appendChild(chip);
  });
}

function addTag(value) {
  const tag = value.trim();
  if (!tag || selectedTags.includes(tag)) return;
  selectedTags.push(tag);
  tagSet.add(tag);
  renderSelectedTags();
  tagInput.value = '';
  updateTagSuggestions();
  tagSuggestionList.classList.add('hidden');
}

function updateTagSuggestions(filter = '') {
  if (!tagSuggestionList) return;
  const pool = Array.from(tagSet).filter((tag) => !selectedTags.includes(tag));
  const normalized = filter.trim().toLowerCase();
  const matches = normalized ? pool.filter((tag) => tag.toLowerCase().includes(normalized)) : pool;
  tagSuggestionList.innerHTML = '';
  if (!matches.length) {
    tagSuggestionList.classList.add('hidden');
    return;
  }
  matches.slice(0, 8).forEach((tag) => {
    const item = document.createElement('li');
    item.textContent = tag;
    const handleSelection = (event) => {
      event.preventDefault();
      addTag(tag);
    };
    item.addEventListener('mousedown', handleSelection);
    item.addEventListener('click', handleSelection);
    tagSuggestionList.appendChild(item);
  });
  tagSuggestionList.classList.remove('hidden');
}

document.addEventListener('click', (event) => {
  if (!tagSuggestionList || !tagInputWrapper) return;
  const clickedInsideInput = tagInputWrapper.contains(event.target);
  const clickedSuggestion = tagSuggestionList.contains(event.target);
  if (!clickedInsideInput && !clickedSuggestion) {
    tagSuggestionList.classList.add('hidden');
  }
});
