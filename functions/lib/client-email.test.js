import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import vm from 'node:vm';

const app = readFileSync(new URL('../../public/app.js', import.meta.url), 'utf8');
const backend = readFileSync(new URL('../index.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../../public/index.html', import.meta.url), 'utf8');
const css = readFileSync(new URL('../../public/styles.css', import.meta.url), 'utf8');
const escape = (value) => String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const context = vm.createContext({
  escapeHtml: escape, escapeEmailTemplateHtml: escape,
  normalizeLanguageCode: (value, fallback) => ['en', 'fr'].includes(value) ? value : fallback,
  getDefaultClientLanguage: () => 'en'
});
for (const [source, name] of [[app, 'buildClientEmailHtml'], [backend, 'wrapClientEmailHtml']]) {
  const start = source.indexOf(`function ${name}(`);
  assert.ok(start >= 0);
  vm.runInContext(source.slice(start, source.indexOf('\n}\n', start) + 3), context);
}

test('client email preview exactly matches sent HTML for both languages and client kinds', () => {
  for (const locale of ['en', 'fr']) {
    for (const isStorageClient of [true, false]) {
      const message = { subject: 'Information <test> & détails', body: 'Bonjour Élodie,\n\n<script>not markup</script>\nMerci.' };
      const preview = context.buildClientEmailHtml({ client: { preferredLanguage: locale, nonStorageClient: !isStorageClient }, ...message });
      const sent = context.wrapClientEmailHtml({ locale, isStorageClient, ...message });
      assert.equal(preview, sent);
      assert.ok(sent.includes('&lt;script&gt;not markup&lt;/script&gt;'));
      assert.ok(sent.includes('Bonjour Élodie,<br><br>'));
      assert.ok(sent.includes('max-width:1040px'));
      assert.ok(sent.includes('<!--[if mso]><table role="presentation" width="1040"'));
      assert.equal(sent.includes('SEASONAL VEHICLE STORAGE') || sent.includes('ENTREPOSAGE SAISONNIER'), isStorageClient);
      assert.equal(sent.includes('warehouse@as-colle.com'), isStorageClient && locale === 'en');
      assert.equal(sent.includes('entrepot@as-colle.com'), isStorageClient && locale === 'fr');
    }
  }
});

test('client email icon exists and retains the accessible button label', () => {
  assert.ok(existsSync(new URL('../../public/icons/email.png', import.meta.url)));
  assert.ok(app.includes('src="icons/email.png"'));
  assert.ok(!app.includes('src="icons/mail.svg"'));
  assert.ok(app.includes('emailButton.setAttribute(\'aria-label\''));
});

test('client email editor and sandboxed preview use the full expanded modal width', () => {
  assert.match(html, /id="client-email-body" rows="14"/);
  assert.match(html, /id="client-email-preview" class="email-template-preview-frame wide"[^>]*sandbox=""/);
  assert.match(css, /#client-email-modal \.modal-content\s*\{[^}]*max-width: 1120px/s);
  assert.match(css, /#client-email-form\s*\{[^}]*grid-template-columns: minmax\(0, 1fr\)/s);
  assert.match(css, /#client-email-body\s*\{[^}]*min-height: 18rem/s);
});
