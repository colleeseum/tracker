#!/usr/bin/env node
import process from 'node:process';
import { loadProfileConfig, setActiveProfile } from '../lib/profile-config.js';

const args = process.argv.slice(2);

async function showProfiles() {
  const config = await loadProfileConfig({ fresh: true });
  const current = config.activeProfile;
  const entries = Object.entries(config.profiles || {});
  if (!entries.length) {
    console.log('No profiles defined in public/profile-config.js');
    process.exit(1);
  }
  console.log('Available Firebase profiles:');
  entries.forEach(([key, profile]) => {
    const prefix = key === current ? '*' : ' ';
    const label = profile.label ? ` (${profile.label})` : '';
    const project = profile.projectId ? ` – ${profile.projectId}` : '';
    console.log(`${prefix} ${key}${label}${project}`);
  });
  console.log('\nUse `node bin/use-profile.mjs <name>` to switch the active profile.');
}

async function main() {
  if (!args.length || args[0] === 'list' || args.includes('--list')) {
    await showProfiles();
    return;
  }

  const desiredProfile = args[0];
  try {
    const { updated } = await setActiveProfile(desiredProfile);
    if (updated) {
      console.log(`Active profile updated to "${desiredProfile}".`);
    } else {
      console.log(`Profile "${desiredProfile}" is already active.`);
    }
  } catch (err) {
    console.error(err.message || err);
    process.exit(1);
  }
}

main();
