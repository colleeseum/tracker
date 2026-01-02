import { PROFILE_CONFIG } from './profile-config.js';

const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1']);

const resolveProfile = () => {
  const { activeProfile, profiles } = PROFILE_CONFIG || {};
  if (!profiles || !Object.keys(profiles).length) {
    throw new Error('Missing profile configuration. Update public/profile-config.js.');
  }

  const getProfileByName = (name) => (name && profiles?.[name] ? profiles[name] : null);

  const host = typeof window !== 'undefined' ? window.location.hostname : null;
  if (host && LOCAL_HOSTS.has(host)) {
    const localProfile = getProfileByName('local');
    if (localProfile) return localProfile;
  }
  if (host && !LOCAL_HOSTS.has(host)) {
    const prodProfile = getProfileByName('prod');
    if (prodProfile) return prodProfile;
  }

  const active = getProfileByName(activeProfile);
  if (active) return active;

  const firstProfile = Object.values(profiles)[0];
  if (firstProfile) return firstProfile;

  throw new Error('Missing profile configuration. Update public/profile-config.js.');
};

const ACTIVE_PROFILE = resolveProfile();

export const firebaseConfig = ACTIVE_PROFILE.firebase;

export const emulatorConfig = {
  authHost: ACTIVE_PROFILE.emulator?.host || '127.0.0.1',
  authPort: ACTIVE_PROFILE.emulator?.authPort ?? 9099,
  firestoreHost: ACTIVE_PROFILE.emulator?.host || '127.0.0.1',
  firestorePort: ACTIVE_PROFILE.emulator?.firestorePort ?? 8080,
  functionsHost: ACTIVE_PROFILE.emulator?.host || '127.0.0.1',
  functionsPort: ACTIVE_PROFILE.emulator?.functionsPort ?? 5001,
  storageHost: ACTIVE_PROFILE.emulator?.host || '127.0.0.1',
  storagePort: ACTIVE_PROFILE.emulator?.storagePort ?? null,
  useEmulators: Boolean(ACTIVE_PROFILE.useEmulators)
};
