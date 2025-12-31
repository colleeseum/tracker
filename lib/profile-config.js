import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const CURRENT_DIR = path.dirname(fileURLToPath(import.meta.url));
const PROFILE_CONFIG_PATH = path.resolve(CURRENT_DIR, '../public/profile-config.js');

function deepClone(data) {
  return JSON.parse(JSON.stringify(data));
}

export async function loadProfileConfig({ fresh = false } = {}) {
  const moduleUrl = pathToFileURL(PROFILE_CONFIG_PATH);
  const cacheBust = fresh ? `?v=${Date.now()}` : '';
  const profileModule = await import(`${moduleUrl.href}${cacheBust}`);
  const config = profileModule?.PROFILE_CONFIG;
  if (!config) {
    throw new Error('Missing PROFILE_CONFIG export in public/profile-config.js.');
  }
  return deepClone(config);
}

function coerceBoolean(value) {
  if (value === true || value === false) return value;
  if (value === undefined || value === null) return undefined;
  if (typeof value === 'string') {
    if (value === 'true') return true;
    if (value === 'false') return false;
  }
  return undefined;
}

export async function getActiveProfileName(config) {
  const override =
    process.env.FIREBASE_PROFILE ||
    process.env.FIREBASE_ENV ||
    process.env.GCLOUD_ENV ||
    null;
  if (override && config?.profiles?.[override]) {
    return override;
  }
  if (config?.activeProfile && config.profiles?.[config.activeProfile]) {
    return config.activeProfile;
  }
  const profileNames = config?.profiles ? Object.keys(config.profiles) : [];
  if (profileNames.length) {
    return profileNames[0];
  }
  throw new Error('No Firebase profiles found. Update public/profile-config.js.');
}

export async function getActiveProfile(options = {}) {
  const config = options.config || (await loadProfileConfig());
  const profileName = options.profileName || (await getActiveProfileName(config));
  const profile = config.profiles?.[profileName];
  if (!profile) {
    throw new Error(`Unknown Firebase profile "${profileName}".`);
  }

  const envToggle = coerceBoolean(process.env.USE_FIRESTORE_EMULATOR);
  const explicitToggle =
    typeof options.useEmulators === 'boolean' ? options.useEmulators : envToggle;

  const useEmulators =
    typeof explicitToggle === 'boolean'
      ? explicitToggle
      : Boolean(profile.useEmulators);

  return {
    config,
    profileName,
    profile: {
      ...profile,
      useEmulators
    }
  };
}

export function applyProfileToEnv(profile) {
  if (!profile?.projectId) {
    throw new Error('Active Firebase profile is missing a projectId.');
  }

  const projectId = profile.projectId;
  process.env.GCLOUD_PROJECT = projectId;
  process.env.GCP_PROJECT = projectId;
  process.env.FIRESTORE_PROJECT_ID = projectId;
  process.env.GOOGLE_CLOUD_PROJECT = projectId;

  const useEmulators = Boolean(profile.useEmulators);
  if (useEmulators) {
    const host = profile.emulator?.host || '127.0.0.1';
    const port = profile.emulator?.firestorePort ?? 8080;
    process.env.FIRESTORE_EMULATOR_HOST = `${host}:${port}`;
  } else if (process.env.FIRESTORE_EMULATOR_HOST) {
    delete process.env.FIRESTORE_EMULATOR_HOST;
  }

  return { projectId, useEmulators };
}

export async function loadActiveProfile(options = {}) {
  const { config, profileName, profile } = await getActiveProfile(options);
  const env = applyProfileToEnv(profile);
  return {
    config,
    profileName,
    profile,
    projectId: env.projectId,
    useEmulators: env.useEmulators
  };
}

export async function writeProfileConfig(config) {
  if (!config || typeof config !== 'object') {
    throw new Error('Invalid profile config payload.');
  }
  const source = `export const PROFILE_CONFIG = ${JSON.stringify(config, null, 2)};\n`;
  await fs.writeFile(PROFILE_CONFIG_PATH, source, 'utf8');
}

export async function setActiveProfile(profileName) {
  const config = await loadProfileConfig({ fresh: true });
  if (!config?.profiles?.[profileName]) {
    const available = config?.profiles ? Object.keys(config.profiles).join(', ') : 'none';
    throw new Error(`Profile "${profileName}" not found. Available: ${available}`);
  }
  if (config.activeProfile === profileName) {
    return { updated: false, profileName };
  }
  const nextConfig = {
    ...config,
    activeProfile: profileName
  };
  await writeProfileConfig(nextConfig);
  return { updated: true, profileName };
}
