# Firebase expense tracker

A lightweight Firebase web app that mirrors the structure of `depense.csv`. The current milestone focuses on rebuilding account management (sidebar, account settings, opening balances) while the ledger is temporarily commented out. The app is built with static HTML/CSS/JS, uses the Firebase Web SDK directly in the browser, and is ready to run entirely on the Firebase Emulator Suite.

## Project layout

```
.
├── depense.csv              # Source spreadsheet provided for reference
├── public/
│   ├── index.html           # UI markup (auth, ledger, summary)
│   ├── styles.css           # Basic stylin
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

## Email several clients

On **Clients**, choose **Send email to clients**. Search or filter by storage season/status,
select up to 100 clients, enter a shared subject/message, and choose **Review email**.
The saved review shows each client's effective To/CC and an exact server-generated preview.
The banner/footer follow the client's language and storage/non-storage type; the message
itself is not translated. Each client receives a separate message with their own optional
CC plus the existing office copy. Clients sharing the same primary email must not both be
selected in the same mailing.

Choose **Send to X clients** only after review. Results are saved in `clientEmailBatches`
and `clientEmails`. **Resume last mailing** reloads results after closing or interruption.
Confirmed successes are never retried within that mailing. Definite SMTP rejections can
be retried; timeouts, partial acceptance, and interrupted finalization require delivery
review before any manual resend. Preparing/reviewing sends no email. Dev routing continues
to suppress all customer To/CC recipients and sends only to `entrepot@as-colle.com`.

For a local Firestore integration test with disposable records and a mocked sender:

```bash
FIRESTORE_EMULATOR_HOST=127.0.0.1:8081 node functions/scripts/test-local-client-mailing.mjs
```

No real emails are sent by this test; its own temporary records are cleaned up afterwards.

## Permanent report snapshots

Live financial summaries use an internal ledger banner: opening balance, net ledger
change and closing balance. Net ledger change is closing minus opening and includes
all ledger movements, including T5 interest, without deducting calculated mileage
or CCA that did not move ledger funds. A separate estimated tax section retains the
existing category mapping, includes T5 once in the overall adjusted result, and shows
a reconciliation for mileage, CCA, capital movements and unclassified movements.
Any unexplained difference is displayed explicitly. This is not a switch to T776
calculation rules. Previously locked reports retain their original contents.

In **Reports**, choose the report type, entity and year, then **Open / generate**.
Review the displayed report and choose **Lock report**. Confirmation permanently
freezes that report type/entity/year. There is no unlock or overwrite operation.
Ledger entries remain editable; later changes do not alter the saved report.

A locked report opens as a static, script-disabled page without calling the live
report calculations. Its HTML and CSS, expanded transaction details, source ledger
inputs, category mappings, mileage rates/settings and CCA inputs are captured at
lock time. The server records the locking administrator/time and a SHA-256 checksum.
Missing or damaged snapshots produce an error, never an automatic regeneration.

To reopen a locked report, select its type, entity and year and click **Open / generate**.
The saved version opens automatically with a prominent **LOCKED REPORT** notice and
lock date, including inside the static report and downloaded copy. Archived entities
remain selectable in the normal Entity dropdown. Use **Download static report** to keep
a self-contained HTML copy. Tax reference or
formatting changes in future app versions do not rebuild the saved page. Existing
reports remain live until explicitly locked. A snapshot records the reviewed client
calculation, not an independent server verification of tax arithmetic.

Snapshots are stored atomically under `lockedReports/{key}` with protected content
chunks to avoid Firestore's per-document limit. Direct client create/update/delete
is denied for this namespace, including for administrators. A report over the 4 MB
snapshot limit fails without saving a partial lock.

Local persistence and access-control check using disposable fixtures:

```bash
FIRESTORE_EMULATOR_HOST=127.0.0.1:8081 node functions/scripts/test-local-locked-reports.mjs
```

## Storage income classification

In **Settings → General → Storage billing**, select the income category used for both
storage deposits and final storage payments. New ledger entries save that category's
ID, label and current tax code; no code is hard-coded. An existing unique income
category named **Entrepot/Entrepôt** is used as the compatibility default until the
setting is saved. Missing or ambiguous configuration blocks posting rather than
creating uncategorized income. Payment previews include the category in their
fingerprint, so changing it requires a fresh preview before email or ledger posting.

The payment stage remains in the transaction title/description and is also stored as
`storagePaymentStage` (`deposit` or `final-payment`). Posting amounts and invoice totals
are unchanged: deposits are booked once when received, and only the remaining balance
is booked on final payment. Accepted legacy payment previews can finish with the
configured category without resending email.

To preview a correction of generated **local-only** ledger entries for one year:

```bash
FIRESTORE_EMULATOR_HOST=127.0.0.1:8081 node functions/scripts/correct-local-storage-income.mjs --year 2026
```

Add `--apply` only after review. It backs up the affected entries/settings, then changes
only classification/audit fields on verified generated deposit/payment records. It
leaves other years, custom-category entries, amounts, invoices and locked reports
unchanged, and refuses non-loopback/non-emulator targets.

## Unpaid invoice reminders

On a Stored storage case, choose **Payment reminder** in its action menu. Review the
friendly English/French email and the original saved invoice PDF, then choose **Send
payment reminder**. The same action is available for picked-up cases with an unpaid
invoice. Paid, superseded, zero-balance and pending-payment invoices are rejected by
the backend, including when payment occurs after opening the preview.

The `storage-invoice-reminder` template is editable in **Email templates**. Delivery
uses the current client email/CC and the normal banner/footer. The PDF is attached
byte-for-byte from the saved invoice, not regenerated. Reminder audits live under
`storageInvoices/{invoiceId}/reminders`; reminders do not change vehicle statuses,
invoice amounts, payments or ledger entries. Skipping email records only a skipped
reminder. Reusing the same preview cannot resend a completed reminder; uncertain
delivery requires provider review. Dev messages go only to `entrepot@as-colle.com`.

Local integration check with a mocked sender and disposable records:

```bash
FIRESTORE_EMULATOR_HOST=127.0.0.1:8081 node functions/scripts/test-local-invoice-reminders.mjs
```

## Run the Firebase emulators

From the repository root run:

```bash
node bin/start-dev.mjs
```

This launches Firestore (8081), Auth (9099) and Hosting (5002). The Emulator UI is enabled, so you can inspect data at `http://localhost:4000`.

Then open `http://localhost:5002` in the browser to use the app. Port 8080 is left free for the local Entrepôt site.

The dev launcher imports the saved `emulator-data/` snapshot and exports Firestore,
local Auth accounts, and Storage data there when you stop it with Ctrl+C. Allow the
export to finish before closing the terminal. The snapshot is ignored by git.
To save a snapshot while the emulators are still running, use
`firebase emulators:export emulator-data --project local --force`.

## Using the app

- **Create users:** Use the right-hand card to register email/password credentials in the Auth emulator. Any signed-in user can manage accounts while the ledger is on hold.
- **Sign in:** Once authenticated, the layout shows a dark navigation sidebar and a main panel that lists every account with its live balance (opening balance ± ledger activity).
- **Browse accounts:** Scroll the list to see descriptions, current balances, and whether the account lives under `Accounts` (cash) or `Entities` (non-cash). Use the sidebar links to swap between the two.
- **Add/edit accounts:** Use the “New account” button to open a modal where you set the name, type, optional description, and opening balance. Click “Edit” beside any row to revise it.
- **Ledger tab:** Switch to *Ledger* to see a reverse-chronological table. Every journal entry appears twice (once for the cash account, once for the entity) so you can see both balances change in tandem. Filter the table by opening the “Filter accounts” menu and toggling one or more accounts/entities (defaults to all). Each row carries a transaction id with a delete action that removes both sides at once.
- **Attach receipts:** When logging an expense you can capture/upload a receipt photo. The admin UI stores the image in Firebase Storage and shows a “View receipt” link beside the ledger description. Edit any expense later to replace or remove the attachment.
- **Transfers:** Cash-to-cash movements are automated via the “Transfer funds” button (visible on the Ledger tab). Pick two cash accounts, enter an amount and optional note, and the app creates the paired income/expense entries for you.
- **Clients tab:** Maintain tenant contact details (address, phone, email) via the Clients view. It mirrors the account modal so you can add/edit rows quickly.
- **Storage tab:** Track seasonal storage requests (tenant info, insurance, add-ons) and move each request through the workflow statuses (New → Picked-Up).
- **Pricing tab:** Manage seasons, storage offers, and add-on service pricing (battery maintenance, propane storage, etc.) with built-in Firestore CRUD.
- **Settings:** Use the Settings link at the bottom of the sidebar to manage ledger categories (label, income/expense type, numeric code). The Ledger “Add entry” form only shows categories that match the selected entry type.
- **Mileage log:** Keep CRA-friendly mileage totals. The mileage view surfaces vendor expenses without a same-day mileage log. Use **Pending** to log or ignore suggestions and **Ignored** to review dismissals. **Restore suggestion** clears the ignore flag without creating a trip or changing financial fields. A restored expense returns to Pending only when no matching trip exists. Ignored remains accessible even when Pending is empty.
- **Mileage ledger links:** Pending and Ignored suggestions include **Open ledger transaction**, opening the existing transaction editor without saving changes. Trips created from suggestions retain `sourceExpenseId` and `sourceTransactionId` and show a link icon beside their actions. Older trips can open matching date/vendor/entity transactions; multiple matches are presented for selection, never guessed or saved as an inferred association. A missing explicit source is reported rather than silently opening another transaction. Manual trips without a matching expense have no ledger link.

## Data model & rules

- `accounts/{accountId}` — holds `name`, `type` (`entity` or `cash`), `description`, `openingBalance`, and bookkeeping metadata (`createdAt`, `updatedAt`, `createdBy`, `updatedBy`).
- `expenses/{expenseId}` — ledger entry with `accountId` (cash source), `entityId`, `date`, `entryType` (expense/income), `category`, `amount`, `description`, optional tags/vendor/client references, receipt metadata (`receiptUrl`, `receiptStoragePath`), and timestamps.
- `clients/{clientId}` — tenant address book with `name`, `email`, `phone`, `address`, `city`, `province`, `postalCode`, plus audit stamps.
- `storageRequests/{requestId}` — seasonal storage applications linked to a `clientId`, vehicle metadata (type, brand, model, plate, etc.), insurance details, add-on booleans, `season`, and `status` (New → Picked-Up).
- `storageSeasons/{seasonId}` — localized season metadata (names, timeframe, descriptions, ordering, active flag) and availability (`available`, `limited`, or `full`, defaulting to `available` when missing or invalid) used to power pricing. A full season blocks new website requests without changing individual offer prices or availability.
- `storageOffers/{offerId}` — per-season offers with price mode (flat/perFoot/contact), availability (`available`, `limited`, or `full`, defaulting to `available` when absent), vehicle type filters, localized labels/notes, ordering, and visibility flags.
- `storageAddOns/{addonId}` — add-on service definitions (battery maintenance, propane storage, etc.) with localized copy.
- `storageSeasonAddOns/{seasonAddonId}` — per-season add-on pricing linked by `seasonId` and add-on `code`.
- `storageConditions/{conditionId}` — global “conditions” policies (localized text, optional tooltips) shown on the marketing site and in contracts.
- `storageEtiquette/{entryId}` — drop-off etiquette reminders with localized copy/tooltips.
- `i18nEntries/{copyId}` — localized site copy (navigation, hero, forms, contracts, etc.) that powers the public-facing site.
- `categories/{categoryId}` — ledger category definitions with a `label`, `type` (`income` or `expense`), and numeric `code`. The Add Entry modal only shows categories whose type matches the current entry.
- `admin/sitePublish` — bookkeeping doc that stores the last publish timestamp and last published content snapshot used for publish previews.
- `sitePublishHistory/{publishId}` — one document per website-text push, including who pushed it and the diff that was approved.

### Seeding i18n entries from `~/personal/entrepot`

When running against the Firebase Emulator Suite, you can populate the `i18nEntries` collection from the existing static data in `~/personal/entrepot/static/site.js`:

```bash
# Make sure the Firestore emulator is running on localhost:8081 first
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

The script reads Firestore (`storageAddOns`, `storageSeasonAddOns`, `storageConditions`, `storageEtiquette`, `i18nEntries`), serializes the latest values, and writes an ES module that Entrepôt imports at build time. Run the script locally (pointing at the emulator or production) or in CI before deploying the marketing site.

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
4. When you edit pricing/conditions/i18n in Tracker, the “Publish website text” button (visible on the Pricing view) becomes active once it detects a change newer than the last publish timestamp. Clicking it opens a diff preview against the last published snapshot. Continuing calls the callable function, writes a `sitePublishHistory` entry with the approved diff, updates `admin/sitePublish`, and triggers the GitHub workflow. The workflow exports content and pushes the generated file to the Entrepôt repository.

Local/emulator publishes run as dry-runs: they write `sitePublishHistory` and update `admin/sitePublish.lastPublishedSnapshot`, but skip the GitHub dispatch. To update the local Entrepôt generated file after a dry-run publish:

```bash
cd functions
npm run publish:site:local
```

This marks the local Firestore publish baseline and writes `entrepot/static/generated/website-text.generated.js`. The mark script refuses non-emulator profiles unless `ALLOW_NON_LOCAL_PUBLISH_MARK=true` is set intentionally.

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
## Seasonal storage restrictions

Season and offer `availability` support `available`, `limited`, and `full`. Missing or invalid values normalize to `available` without migration. A full season blocks every location; a full offer blocks only that offer. Limited remains requestable.

Offers store `storageLocation` (`indoor` or `outdoor`) and an explicit `vehicleTypes` allowlist using the existing vehicle type IDs. Empty lists allow no vehicles. Legacy offers without a list can use their configured template's list. Missing locations default to indoor, except clearly labelled legacy outdoor offers. Existing pricing is retained when editing restrictions.

The website export and both public request endpoints enforce these fields. Run unit checks with `npm --prefix functions test`. For read-only integration checks against the configured local concrete-pad offer, run:

```bash
FIRESTORE_EMULATOR_HOST=127.0.0.1:8081 GCLOUD_PROJECT=tracker-187c5 FUNCTIONS_EMULATOR=true node functions/scripts/test-local-storage.mjs
```

These integration checks stop permitted requests before persistence and email delivery.

## Storage request referrals

Storage requests can record multiple referring clients in `referredByClientIds`, an optional array of existing client IDs. Use **Referred by (clients)** in the request editor. Select more than one client with Ctrl or Command, or deselect a client to remove their referral. The tenant cannot refer themself. Referrals appear under the request's vehicle details and in its view/edit form.

Requests without this field work unchanged. Missing client records retain a visible unavailable-client entry so unrelated edits do not silently discard historical referrals. Referrals are internal Tracker metadata, not public website fields or email recipients.

## Storage invoices and referral discounts

**Cancel vehicle** is available on each uninvoiced vehicle before it is Stored. It asks for the deposit amount to keep and a reason; the backend checks actual, non-voided deposit receipts and amounts already retained for other cancellations. Cancellation keeps the request visible, stores its original charge/retained deposit/credit and never creates a refund or deletes deposit income. The invoice shows the original vehicle charge and a separate negative cancellation adjustment; cancelled vehicles never become Stored or Paid. Case actions operate on active vehicles while billing includes the whole case so grouped deposits stay intact. A fully cancelled case has no active vehicle to move to Stored and requires separate billing review if a final document is needed.

**Correct unpaid invoice** preserves the original invoice/PDF as `superseded`, returns active vehicles to Scheduled and reserves a new billing revision/invoice number for the replacement. It refuses payment receipts, linked ledger payments, pending deliveries, incomplete cases and referral credits consumed by another invoice. Original agreed prices are frozen from the old invoice. Credits applied by the old invoice are released once and applied normally on its replacement; deposit receipts and ledger income remain unchanged. Review/cancel the relevant vehicle, then use **Mark stored & invoice** to preview the new invoice referencing the old one. No correction or cancellation action sends email automatically. Production corrections must be backed up and scoped to exact IDs; `correct-storage-cancellation.mjs` is read-only unless `--apply-production` is explicitly passed.

Each vehicle row has a **Manage contract** action at every workflow stage. Use **Regenerate PDF** to download a fresh unsigned contract from the saved request details and current PDF template, keeping the existing contract ID and uploaded signed copy. Use **Replace uploaded contract** to upload a newly reviewed/signed copy. Replacements preserve the current workflow status, invoices, payments and prices; only the first upload on a New request advances it to Contract Ready. Each replacement atomically archives the previous upload's link and metadata in `storageRequests/{requestId}/contractRevisions`. Previous Storage objects are retained, and new uploads use unique paths. Concurrent contract/status changes and pending invoice/payment delivery block replacement. Regeneration and replacement do not send email or alter existing invoices and receipts. The separate contract-receipt upload step uses the same revision-preserving save operation.

Every storage email step (confirmation, contract, contract/deposit receipt, missing-item follow-up, Stored invoice and Paid receipt) offers **Do not send email** in its preview. It is unchecked for each new action. Checking it completes the same workflow and saves required PDFs, deposits, referral credits and final-payment ledger entries without invoking SMTP. Signed-contract and payment-reference requirements still apply. Audit fields record `emailDeliveryStatus: skipped` plus the admin and timestamp, rather than claiming email acceptance or sent timestamps. Recovery completes the original accepted/skipped decision without sending again. For compatibility, saved invoice/payment `status: sent` remains a workflow-completion marker; use `emailDeliveryStatus`, `emailAcceptedAt` and `emailSkippedAt` to determine actual delivery. The checkbox does not disable automatic public-website notifications or the separate manual client-email composer.

For an approved no-deposit case, first confirm **Received Contract** and upload every signed contract. While the case is **Waiting for Deposit**, choose **Schedule without deposit** and confirm the exception. The backend validates the complete case, signed contracts, absence of active deposit receipts and pending deliveries, then atomically marks all vehicles Scheduled. Each request records `depositRequired: false`, `depositRequirementWaivedAt` and `depositRequirementWaivedBy`. No email, receipt or accounting payment is created by this scheduling action. The regular deposit-received workflow stays available and unchanged.

The subsequent Stored invoice still uses actual recorded deposits only. With no deposit receipt it subtracts zero, leaving the full agreed total less any eligible referral discounts due. The approved exception does not waive any contract charges or fabricate a payment.

**Mark stored & invoice** opens an email preview plus an invoice PDF link for the complete scheduled storage case. Previewing or cancelling does not create an invoice, send email, consume referral credit, or change storage status. Confirm **Send invoice & mark stored** to email the PDF and record the vehicles as Stored. The request editor also routes a Scheduled-to-Stored transition through this preview. Existing Stored requests are not retroactively invoiced.

The invoice uses the agreed contract amounts, subtracts actual deposit receipts linked to the case, and subtracts available referring-client credits. It does not add another tax calculation or record the outstanding balance as cash received. Saved invoices and the exact PDF emailed are accessible through the top-level case's **Invoice (PDF)** link in the Vehicle Type cell. Invoice dates use America/Toronto.

After receiving the full remaining balance, choose **Mark paid & receipt** on a Stored case. Review the localized email and the original invoice PDF with a green **PAID / PAYÉ** seal, then choose **Confirm full payment & send receipt**. This records the payment amount/date/admin and zero remaining balance in `storageInvoices.payment`, and marks every invoiced vehicle Paid after the email is accepted. Partial payments are not handled by this action. The original invoice, deposits, prices and referral credits remain unchanged. The same finalization transaction creates one income entry in `expenses/storage-payment-{invoiceId}` for the remaining payment only, using the existing default cash account and Ferme Colle entity. The invoice stores `payment.ledgerEntryId`. Deposits already booked and referral discounts are not posted again. Zero-balance invoices create no cash entry. Preview/cancel and rejected or uncertain delivery create no ledger entry; retries and status recovery cannot duplicate it. Accounts are validated before sending and included in the preview fingerprint. Already-paid legacy records are not silently backfilled. The Paid case can then move to Picked-Up; the existing Stored-to-Picked-Up action remains available for legacy records.

The editable **storage-paid** email template uses the normal banner/footer and supports `{{payment_amount}}`, `{{paid_date}}`, `{{invoice_id}}`, `{{saison}}`, `{{balance_due}}` and `{{invoice_details}}`. **Paid (PDF)** reopens the saved stamped attachment at case level without emailing it. Preview/cancel is read-only. A definite rejection leaves the case Stored and permits a reviewed retry; uncertain delivery remains locked for reconciliation. Already accepted email can complete Paid status without resending. Dev receipt emails use the same fixed safe recipient as all other dev emails.

Set **General settings > Storage billing > Referral discount (CAD)** and save it. The proposed initial value is $25; billing calculations use only the saved `admin/appSettings.referralDiscountAmount`, not a hard-coded rate. A referring client earns one credit per referred customer, even when that customer has multiple vehicles or returns for another season. Multiple referring clients each have their own customer-credit record. Credits are derived from recorded client referrals on non-cancelled/non-rejected requests, not awarded separately for every vehicle. The preview lists the customers contributing each credit. New credits use the saved rate; partially used credits retain their original value and unused amounts carry forward.

`storageInvoices` stores immutable billing snapshots, email previews and attached PDF bytes. `storageReferralCredits` tracks each referring/referred client pair, applied amounts and in-flight reservations. Duplicate confirmations and concurrent invoices cannot consume a credit twice. Requests remain Scheduled when SMTP rejects the email. If delivery is uncertain, automatic retry is blocked pending reconciliation. If the mail server accepted the email but status persistence failed, reopening the preview can complete the Stored status without resending the email. An invoice pending delivery blocks edits to its associated requests.

Invoice PDFs use a branded invoice layout with supplier/client address blocks, invoice number/date, localized season, description/amount columns, separate deposit/referral deductions, a highlighted balance due, and numbered continuation pages. Supplier contact details match the existing storage contract.

Invoice emails render the editable **storage-invoice** template and reuse the same banner/footer wrapper as contract and receipt messages. A built-in English/French default appears in **Website & Offering > Email templates** even before it has a Firestore record; editing and saving it creates the override without changing other templates. Supported tokens include `{{tenant}}`, `{{invoice_id}}`, `{{invoice_date}}`, `{{saison}}`, `{{total_cost}}`, `{{total_deposit}}`, `{{referral_discount}}`, `{{balance_due}}`, and `{{invoice_details}}` (the invoice table). Both the billing snapshot and rendered email are fingerprinted: editing a template after preview requires a fresh preview. Previously sent invoices/PDFs remain unchanged.

Run `npm --prefix functions test` for invoice, credit, PDF and failure-path checks. For local Firestore integration with a mocked mail sender:

```bash
FIRESTORE_EMULATOR_HOST=127.0.0.1:8081 node functions/scripts/test-local-invoices.mjs
```

Add `--preview` to keep the disposable local case visible while inspecting its UI preview, then press Enter in the terminal to complete mock-send checks and remove all fixtures. This test never sends real email; it initializes the local discount setting to $25 only if that setting is absent.

## Development email safety

Every email workflow shares a protected SMTP sender. When running with Firebase emulator settings or a local/dev/test profile, emails go **only to `entrepot@as-colle.com`**. Customer To/CC/BCC recipients are replaced, custom SMTP envelopes are overridden, replies and bounces route to the same safe address, and subjects get a `[DEV]` prefix. Raw messages and recipient/read-receipt headers cannot bypass this safeguard. This includes invoice, contract, receipt, manual client, website notification, and acknowledgement emails.

Previews show the effective destination and a dev warning. A local preview fails closed if its backend reports live production routing. Sent-email metadata records the effective destination; invoice billing details still identify the customer without emailing them. The safeguard cannot be disabled by a client payload. Any emulator signal forces it on, even if another environment flag says production. `DEV_EMAIL_MODE=true` can additionally force safe routing in another environment. Normal production routing is unchanged when no dev/emulator signal is present.

Reload Tracker after updating the local code. The Functions emulator reloads the central protection automatically; if the preview cannot verify routing, restart it with `node bin/start-dev.mjs` and do not send until the preview shows the safe destination.

## Offer-template pricing notes

The offer editor exposes English and French template notes. Saving updates the template and synchronizes inherited notes on its associated offers without changing their prices or restrictions. Both website exports resolve notes from the current template rather than stale offer copies.

Default notes follow each offer's price mode: `flat rate` / `tarif fixe` for flat pricing, or `minimum {{amount}}` in both languages for per-foot pricing with a configured minimum. The website substitutes each offer's minimum for `{{amount}}`. Custom text and manual edits (tracked per language by `noteCustom`) survive mode changes. Use **Use pricing defaults for both notes** to explicitly discard custom text and restore defaults.

Review existing template and offer notes without writing data:

```bash
FIREBASE_PROFILE=prod USE_FIRESTORE_EMULATOR=false node functions/scripts/review-offer-notes.mjs
```

To apply safe note-only corrections in the local emulator:

```bash
FIREBASE_PROFILE=local USE_FIRESTORE_EMULATOR=true FIRESTORE_EMULATOR_HOST=127.0.0.1:8081 node functions/scripts/review-offer-notes.mjs --apply
```

The script refuses production writes and reports divergent custom notes for manual review. Shared templates with mixed pricing modes are reported rather than assigned a single incorrect static default; exports normalize their default notes per offer.

## Firebase Storage rules

Receipt photos live under `gs://<project-bucket>/receipts/**`. Protect them by adding Storage rules similar to the following before deploying:

```javascript
rules_version = '2';
service firebase.storage {
  match /b/{bucket}/o {
    match /receipts/{allPaths=**} {
      allow read, write: if request.auth != null;
    }
  }
}
```

If you also run the Firebase Storage emulator locally, add `storagePort` to `public/profile-config.js` so the UI connects to it automatically (defaults to the prod bucket when no emulator is configured).
