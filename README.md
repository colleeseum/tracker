# Firebase expense tracker

A lightweight Firebase web app that mirrors the structure of `depense.csv`. The current milestone focuses on rebuilding account management (sidebar, account settings, opening balances) while the ledger is temporarily commented out. The app is built with static HTML/CSS/JS, uses the Firebase Web SDK directly in the browser, and is ready to run entirely on the Firebase Emulator Suite.

## Project layout

```
.
├── depense.csv              # Source spreadsheet provided for reference
├── public/
│   ├── index.html           # UI markup (auth, ledger, summary)
│   ├── styles.css           # Basic styling
│   ├── profile-config.js    # Project/emulator profiles + active selection
│   ├── firebase-config.js   # Uses the active profile to configure Firebase
│   └── app.js               # All client-side logic
├── firebase.json            # Hosting + emulator definitions
├── firestore.rules          # Rules enforcing per-account data isolation
├── firestore.indexes.json   # Composite index for account/date queries
└── .firebaserc              # Placeholder project id (adjust if needed)
```

## Prerequisites

1. Install the Firebase CLI if you do not have it yet:
   ```bash
   npm install -g firebase-tools
   ```
2. (Optional) Login so that the emulator can reuse your credentials:
   ```bash
   firebase login
   ```

## Configure Firebase profiles

All credentials and emulator overrides live in `public/profile-config.js`. Each entry defines `firebase` (web SDK config) plus optional emulator host/ports. The `activeProfile` field controls which entry the UI and CLI scripts use by default.

List or switch profiles without editing the file manually:

```bash
# Show every profile and the current selection
node bin/use-profile.mjs list

# Activate the production profile
node bin/use-profile.mjs prod
```

Every Firestore script under `functions/scripts/` automatically loads the active profile. You can override it temporarily with an environment variable:

```bash
FIREBASE_PROFILE=prod node functions/scripts/export-site-data.mjs
```

When the active profile enables emulators, the helper sets `FIRESTORE_EMULATOR_HOST` before Firebase Admin initialises, so the scripts and the admin UI always hit the same backend.

## Run the Firebase emulators

From the repository root run:

```bash
firebase emulators:start
```

This launches Firestore (8080), Auth (9099) and Hosting (5000). The Emulator UI is enabled, so you can inspect data at `http://localhost:4000`.

Then open `http://localhost:5000` in the browser to use the app.

## Using the app

- **Create users:** Use the right-hand card to register email/password credentials in the Auth emulator. Any signed-in user can manage accounts while the ledger is on hold.
- **Sign in:** Once authenticated, the layout shows a dark navigation sidebar and a main panel that lists every account with its live balance (opening balance ± ledger activity).
- **Browse accounts:** Scroll the list to see descriptions, current balances, and whether the account lives under `Accounts` (cash) or `Entities` (non-cash). Use the sidebar links to swap between the two.
- **Add/edit accounts:** Use the “New account” button to open a modal where you set the name, type, optional description, and opening balance. Click “Edit” beside any row to revise it.
- **Ledger tab:** Switch to *Ledger* to see a reverse-chronological table. Every journal entry appears twice (once for the cash account, once for the entity) so you can see both balances change in tandem. Filter the table by opening the “Filter accounts” menu and toggling one or more accounts/entities (defaults to all). Each row carries a transaction id with a delete action that removes both sides at once.
- **Transfers:** Cash-to-cash movements are automated via the “Transfer funds” button (visible on the Ledger tab). Pick two cash accounts, enter an amount and optional note, and the app creates the paired income/expense entries for you.
- **Clients tab:** Maintain tenant contact details (address, phone, email) via the Clients view. It mirrors the account modal so you can add/edit rows quickly.
- **Storage tab:** Track seasonal storage requests (tenant info, insurance, add-ons) and move each request through the workflow statuses (New → Picked-Up).
- **Pricing tab:** Manage seasons, storage offers, and add-on service pricing (battery maintenance, propane storage, etc.) with built-in Firestore CRUD.
- **Settings:** Use the Settings link at the bottom of the sidebar to manage ledger categories (label, income/expense type, numeric code). The Ledger “Add entry” form only shows categories that match the selected entry type.

## Data model & rules

- `accounts/{accountId}` — holds `name`, `type` (`entity` or `cash`), `description`, `openingBalance`, and bookkeeping metadata (`createdAt`, `updatedAt`, `createdBy`, `updatedBy`).
- `expenses/{expenseId}` — ledger entry with `accountId` (cash source), `entityId`, `date`, `entryType` (expense/income), `category`, `amount`, `description`, and timestamps.
- `clients/{clientId}` — tenant address book with `name`, `email`, `phone`, `address`, `city`, `province`, `postalCode`, plus audit stamps.
- `storageRequests/{requestId}` — seasonal storage applications linked to a `clientId`, vehicle metadata (type, brand, model, plate, etc.), insurance details, add-on booleans, `season`, and `status` (New → Picked-Up).
- `storageSeasons/{seasonId}` — localized season metadata (names, timeframe, descriptions, ordering, active flag) used to power pricing.
- `storageOffers/{offerId}` — per-season offers with price mode (flat/perFoot/contact), vehicle type filters, localized labels/notes, ordering, and visibility flags.
- `storageAddOns/{addonId}` — add-on services (battery maintenance, propane storage, etc.) with localized copy and per-season pricing.
- `storageConditions/{conditionId}` — global “conditions” policies (localized text, optional tooltips) shown on the marketing site and in contracts.
- `storageEtiquette/{entryId}` — drop-off etiquette reminders with localized copy/tooltips.
- `i18nEntries/{copyId}` — localized site copy (navigation, hero, forms, contracts, etc.) that powers the public-facing site.
- `categories/{categoryId}` — ledger category definitions with a `label`, `type` (`income` or `expense`), and numeric `code`. The Add Entry modal only shows categories whose type matches the current entry.
- `admin/sitePublish` — bookkeeping doc that stores the last publish timestamp written by the “Publish” button.

### Seeding i18n entries from `~/personal/entrepot`

When running against the Firebase Emulator Suite, you can populate the `i18nEntries` collection from the existing static data in `~/personal/entrepot/static/site.js`:

```bash
# Make sure the Firestore emulator is running on localhost:8080 first
cd functions
node scripts/import-pricing.mjs
```

The script parses the Entrepôt site constants, clears the `i18nEntries` collection, and writes the current bilingual copy into Firestore. Re-run it whenever you update `static/site.js` to keep the admin UI in sync.

### Exporting website text for Entrepôt

To mirror Tracker data into `~/personal/entrepot/static/generated/website-text.generated.js`, run:

```bash
cd functions
node scripts/export-site-data.mjs --out ../entrepot/static/generated/website-text.generated.js
```

The script reads Firestore (`storageAddOns`, `storageConditions`, `storageEtiquette`, `i18nEntries`), serializes the latest values, and writes an ES module that Entrepôt imports at build time. Run the script locally (pointing at the emulator or production) or in CI before deploying the marketing site.

## Backing up and restoring Firestore

You can now snapshot the entire Firestore database (all collections/subcollections) and rehydrate it later.

```bash
# 1. Ensure the active Firebase profile is LOCAL before exporting.
node bin/use-profile.mjs local

# 2. Run the backup script (defaults to ../backups/firestore-backup-<timestamp>.json).
cd functions
node scripts/backup-firestore.mjs --out ../backups/firestore-backup.json
```

Restore from a backup file (pass `--drop-existing` to wipe the target project before writing):

```bash
# 1. Switch to the PROD profile so the restore targets the real project.
node bin/use-profile.mjs prod

# 2. Restore the chosen backup into production.
cd functions
node scripts/restore-firestore.mjs --in ../backups/firestore-backup.json --drop-existing

# 3. (Optional) Switch back to the local profile for day-to-day work.
node bin/use-profile.mjs local
```

Both scripts honour the active Firebase profile, so switch profiles (`node bin/use-profile.mjs prod`) before exporting/importing production data. Backups are JSON files stored in `backups/` (which is ignored by git).

### Normalizing vehicle type document IDs

Older vehicle type entries used human-readable IDs derived from their labels. To duplicate
each entry with a random Firestore ID and update every offer reference automatically, run:

```bash
cd functions
node scripts/normalize-vehicle-type-ids.mjs
```

The script copies every document in `vehicleTypes/`, writes a `value` field with the legacy ID,
updates all `storageOffers.vehicleTypes` arrays to point at the new IDs, and deletes the
legacy documents once the migration succeeds.

### Renaming offer document IDs

To replace the slug-style document IDs in `storageOffers/` with auto-generated IDs (while
retaining the `id` field for downstream consumers), run:

```bash
cd functions
node scripts/rename-storage-offers.mjs
```

Update `PRESERVED_IDS` inside the script if there are particular documents that should keep
their existing IDs.

### Building a vehicle type collection from offers

To bootstrap the new `vehicleTypes/` collection by scanning every offer, deduplicating the
type strings, and rewriting `storageOffers.vehicleTypes` to reference the new IDs, run:

```bash
cd functions
node scripts/build-vehicle-types-from-offers.mjs
```

This script deletes any existing docs in `vehicleTypes/`, seeds canonical entries (using
the default English/French labels when possible), and stores the legacy values alongside
the generated document IDs for traceability.

### GitHub automation

1. Configure the workflow in `.github/workflows/publish-site.yml`. It can be triggered manually or via the publish button (repository_dispatch `tracker-content-publish`).
2. Create the following repository secrets in Tracker:
   - `ENTREPOT_REPO`: `owner/repo` of the public site.
   - `ENTREPOT_PAT`: personal access token that can push to the Entrepôt repo.
   - `FIREBASE_SERVICE_ACCOUNT`: JSON key for a service account with Firestore read access (use `secrets` format and the workflow writes it to disk).
3. In Firebase Functions, set `GITHUB_PUBLISH_REPO`, `GITHUB_PUBLISH_TOKEN`, and (optionally) `GITHUB_PUBLISH_EVENT`. The new callable function `requestSitePublish` uses these to dispatch the GitHub workflow.
4. When you edit pricing/conditions/i18n in Tracker, the “Publish website text” button (visible on the Pricing view) becomes active once it detects a change newer than the last publish timestamp. Clicking it calls the callable function, which updates `admin/sitePublish` and triggers the GitHub workflow. The workflow exports content and pushes the generated file to the Entrepôt repository.

`firestore.rules` currently allow any authenticated user to read or update the `accounts` and `expenses` collections while workflows are being redesigned. Tighten these rules once roles are defined again.

`firestore.indexes.json` already includes the composite index (`account` + `date DESC`) required for the query used in the UI.

## Deploying (optional)

When you are ready to host the static site in Firebase Hosting:

```bash
firebase use <your-project-id>
firebase deploy --only hosting
```

Make sure the active profile (`node bin/use-profile.mjs prod`) points to the production Firebase project before deploying.

## Next ideas

- Re-enable the ledger table once the business logic is ready (the UI currently only focuses on account settings).
- Attach per-account ownership again once roles are clarified.
- Import the historical rows from `depense.csv` into Firestore after the account structure stabilises.

## Sending emails via SMTP2GO

Use the helper in `lib/emailer.js` (built on top of `nodemailer`) to send mail from both this tracker and other repositories (e.g. `~/personal/entrepot`).

1. Install the dependency once in the project that sends emails:
   ```bash
   npm install nodemailer
   ```
2. Provide credentials via the Functions dotenv files. Copy `functions/.env.example` to the appropriate target (e.g. `functions/.env.local`, `functions/.env.prod`) and fill in the values:
   ```bash
   SMTP2GO_HOST=mail.smtp2go.com
   SMTP2GO_PORT=587
   SMTP2GO_USER=apikey
   SMTP2GO_PASS=secret
   SMTP2GO_FROM="Tracker <tracker@example.com>"
   MAILER_RESTRICTED_DOMAIN=@as-colle.com
   RECAPTCHA_SECRET=your-recaptcha-secret
   # Optional: set SKIP_CAPTCHA=true only in local/test envs
   SKIP_CAPTCHA=false
   # Comma-separated list of addresses unauthenticated callers can reach
   PUBLIC_ALLOWED_RECIPIENTS=entrepot@as-colle.com,warehouse@as-colle.com
   # Optional: allow a specific replyTo address to bypass rate limits
   RATE_LIMIT_EXEMPT_REPLY_TO=serge.colle+test@gmail.com
   # Keywords that pick the confirmation language for reply-to receipts
   FRENCH_SENDER_KEYWORD=entrepot@as-colle.com
   ENGLISH_SENDER_KEYWORD=warehouse@as-colle.com
   ```
3. Obtain a reCAPTCHA token on the client (v2 checkbox or v3) and include it in every call. The Cloud Function rejects requests without a valid `captchaToken`.
4. Send an email:
   ```js
   import { sendEmail } from '../tracker/lib/emailer.js';

   await sendEmail({
     to: 'client@example.com',
     subject: 'Invoice #42',
     html: '<p>Hello!</p>',
     replyTo: 'client@example.com',
     captchaToken: tokenFromRecaptcha,
     attachments: [
       { path: '/tmp/invoice-42.pdf', filename: 'invoice.pdf' },
       { filename: 'notes.txt', content: 'Internal notes', contentType: 'text/plain' },
       // Base64 or data URLs are automatically decoded
       {
         filename: 'quote.pdf',
         content: base64StringFromClient,
         contentType: 'application/pdf'
       }
     ]
   });
   ```
   > Tip: pass `replyTo` when you need responses to go to the end user (e.g. contact forms).

> Security rule: If you pass a custom `from` address (different from `SMTP2GO_FROM`), every recipient must be under `@as-colle.com` (configurable via `RESTRICTED_EMAIL_DOMAIN`). Otherwise `sendEmail` throws and the message is not sent.
>
> Authentication behavior: unauthenticated callers can only send to the emails in `PUBLIC_ALLOWED_RECIPIENTS`. Any other recipient requires a logged-in user; admins can additionally override the `from` address.
>
> Rate limiting: the function enforces two sends/day per user and per IP. Set `RATE_LIMIT_EXEMPT_REPLY_TO` if you want a specific `replyTo` (e.g., `serge.colle+test@gmail.com`) to bypass the cap for testing.
>
> Acknowledgements: every successful send triggers a confirmation message to `replyTo`. If the sender contains `FRENCH_SENDER_KEYWORD` (default `entrepot@as-colle.com`) the acknowledgement is sent in French; otherwise the message is in English (controlled by `ENGLISH_SENDER_KEYWORD`).

This API works from `~/personal/entrepot` (just adjust the relative import path). SMTP2GO handles delivery; if Firebase adds a native email API later, swap the internals while keeping the `sendEmail` signature.

## Deploying the email Cloud Function

1. Install packages inside `functions/`:
   ```bash
   cd functions
   npm install
   cd ..
   ```
2. Before deploying, ensure the target-specific dotenv file exists (e.g. `functions/.env.prod`) with the keys shown above. The Firebase CLI loads that file automatically based on `--project` / `firebase use`.
3. For local testing you can copy the template to `functions/.env.local`, set `SKIP_CAPTCHA=true`, and run the emulators to bypass the captcha check. **Never** enable this flag in production.
4. Deploy just the backend email API:
   ```bash
   firebase deploy --only functions:sendEmail
   ```
5. From clients or other services call the callable function:
   ```js
   import { httpsCallable } from 'firebase/functions';

   const sendEmail = httpsCallable(functions, 'sendEmail');
   await sendEmail({
     to: 'client@as-colle.com',
     subject: 'Invoice',
     html: '<p>Hi!</p>',
     replyTo: 'client@as-colle.com',
     captchaToken: tokenFromRecaptcha,
     attachments: [{ path: '/tmp/invoice.pdf' }]
   });
   ```

   > To allow custom `from` addresses and unrestricted recipients, attach the `isTrackerAdmin=true` custom claim to the authenticated user. Otherwise, any custom sender is restricted to the configured domain.
