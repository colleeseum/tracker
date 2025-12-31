import { PROFILE_CONFIG } from './profile-config.js';

const resolveProfile = () => {
  const { activeProfile, profiles } = PROFILE_CONFIG || {};
  if (activeProfile && profiles?.[activeProfile]) {
    return profiles[activeProfile];
  }
  const firstProfile = profiles ? Object.values(profiles)[0] : null;
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
  useEmulators: Boolean(ACTIVE_PROFILE.useEmulators)
};
