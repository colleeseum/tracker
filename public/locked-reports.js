export const reportKey = ({ type, entityId, year }) => JSON.stringify([type, entityId, year]);

export function lockedReportDocument(saved) {
  const escape = (value) => String(value || '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
  const date = new Date(saved.metadata.lockedAt).toLocaleString('en-CA', { timeZone: 'America/Toronto', dateStyle: 'medium', timeStyle: 'short' });
  const notice = `<aside role="status" style="padding:16px 20px;margin:0 0 20px;border:2px solid #166534;border-radius:8px;background:#ecfdf5;color:#14532d;font-family:Arial,sans-serif;break-inside:avoid;"><strong style="display:block;font-size:20px;margin-bottom:6px;">LOCKED REPORT</strong><div>Locked on ${escape(date)} (America/Toronto)${saved.metadata.lockedByEmail ? ` by ${escape(saved.metadata.lockedByEmail)}` : ''}.</div><div style="margin-top:6px;">Permanent, read-only snapshot. Values and tax reference data are frozen. This report has not been recalculated.</div></aside>`;
  // Add immutable lock metadata to the presentation, never recalculate or rewrite the saved report.
  return saved.html.replace(/<body\b[^>]*>/i, (body) => body + notice);
}

export function buildStaticReportHtml({ output, css, title, details = [] }) {
  const clone = output.cloneNode(true);
  // Expand all already-calculated details and remove controls that could edit live data.
  clone.querySelectorAll('.collapsed, .hidden').forEach((node) => node.classList.remove('collapsed', 'hidden'));
  clone.querySelectorAll('script, iframe, object, embed, button, input, select, textarea, img, link, base').forEach((node) => node.remove());
  clone.querySelectorAll('a').forEach((node) => node.replaceWith(document.createTextNode(node.textContent)));
  clone.querySelectorAll('*').forEach((node) => {
    for (const attribute of [...node.attributes]) if (/^on/i.test(attribute.name) || attribute.name.startsWith('data-')) node.removeAttribute(attribute.name);
  });
  const appendix = document.createElement('section');
  appendix.className = 'card';
  for (const detail of details) {
    const heading = document.createElement('h4'); heading.textContent = detail.label; appendix.appendChild(heading);
    const table = document.createElement('table'); table.className = 'ledger-table';
    for (const entry of detail.entries) {
      const row = document.createElement('tr');
      for (const value of [entry.date, entry.description, entry.account, entry.amount]) {
        const cell = document.createElement('td'); cell.textContent = value; row.appendChild(cell);
      }
      table.appendChild(row);
    }
    appendix.appendChild(table);
  }
  const heading = document.createElement('h1'); heading.textContent = title;
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; img-src data:; font-src data:; base-uri 'none'; form-action 'none'"><title>${heading.textContent.replace(/[<>&]/g, '')}</title><style>${css.replace(/<\/style/gi, '<\\/style')}
body{padding:20px;background:#f8fafc} .report-entry-list,.chip-entries{display:block!important} .report-detail-row{display:table-row!important} button{display:none!important} @media print{body{padding:0;background:white}}
</style></head><body>${heading.outerHTML}<p>Permanent report snapshot. Values and reference inputs were frozen when locked.</p>${clone.innerHTML}${details.length ? appendix.outerHTML : ''}</body></html>`;
}

export function createReportLockController({ getFilters, isGenerated, isVisible = () => true, canLock, renderLive, captureInputs, getSaved, save, output, status, lockButton, downloadButton, getCss, confirmLock = (message) => window.confirm(message) }) {
  const cache = new Map();
  let sequence = 0;
  let busy = false;
  let activeArchive = null;
  let displayed = null;
  let live = null;
  const showError = (error) => { output.textContent = error.message || 'Unable to verify report lock. Live regeneration is blocked.'; status.textContent = 'Report unavailable. No recalculation was performed.'; lockButton.disabled = true; };
  async function render() {
    if (busy) return;
    const token = ++sequence;
    if (!isVisible()) return;
    const filters = { ...(activeArchive || getFilters()) };
    status.classList.remove('report-lock-notice');
    lockButton.disabled = true; downloadButton.classList.add('hidden'); live = null;
    if (!filters.entityId || (!activeArchive && !isGenerated())) { displayed = null; status.textContent = ''; renderLive(); return; }
    const key = reportKey(filters);
    try {
      if (!cache.has(key)) {
        status.textContent = 'Checking saved report…';
        output.textContent = 'Checking whether this report is locked before generating it.';
        cache.set(key, getSaved(filters));
      }
      const saved = await cache.get(key);
      if (token !== sequence) return;
      if (saved.locked) cache.set(key, saved);
      else cache.delete(key); // Never reuse an unlocked result after another admin may lock it.
      if (saved.locked) {
        activeArchive = { type: saved.metadata.type, entityId: saved.metadata.entityId, year: saved.metadata.year };
        const unchanged = displayed?.metadata?.sha256 === saved.metadata.sha256 && output.querySelector('iframe');
        displayed = saved;
        if (!unchanged) {
          const frame = document.createElement('iframe');
          frame.className = 'locked-report-frame'; frame.title = 'Locked report snapshot'; frame.setAttribute('sandbox', ''); frame.srcdoc = lockedReportDocument(saved);
          output.replaceChildren(frame);
        }
        status.classList.add('report-lock-notice');
        status.textContent = `LOCKED REPORT: ${saved.metadata.entityName} · ${saved.metadata.year}. Locked on ${new Date(saved.metadata.lockedAt).toLocaleString('en-CA', { timeZone: 'America/Toronto' })}. Read-only saved version; no recalculation.`;
        downloadButton.classList.remove('hidden');
      } else {
        displayed = null;
        renderLive();
        if (canLock()) live = { filters, inputs: JSON.parse(JSON.stringify(captureInputs())) };
        lockButton.disabled = !live;
        status.textContent = 'Live report. Lock it to preserve these values and reference inputs permanently.';
      }
    } catch (error) { cache.delete(key); if (token === sequence) showError(error); }
  }
  lockButton.addEventListener('click', async () => {
    if (busy || !live) return;
    const snapshot = live;
    const name = snapshot.inputs.entityName || snapshot.filters.entityId;
    if (!confirmLock(`Permanently lock ${name}, ${snapshot.filters.year}, ${snapshot.filters.type === 'tags' ? 'Tag summary' : 'Financial summary'}? The saved report cannot be overwritten or regenerated. Ledger entries remain editable, but later changes will not affect this snapshot.`)) return;
    busy = true; ++sequence; lockButton.disabled = true; status.textContent = 'Saving permanent snapshot…';
    let failure = null;
    try {
      // Capture DOM before awaiting CSS/network, paired with the inputs frozen at render time.
      const content = output.cloneNode(true);
      const css = await getCss();
      const html = buildStaticReportHtml({ output: content, css, title: `${name} · ${snapshot.filters.year} · ${snapshot.filters.type === 'tags' ? 'Tag summary' : 'Financial summary'}`, details: snapshot.inputs.details });
      const saved = await save({ ...snapshot.filters, entityName: name, html, inputs: snapshot.inputs });
      cache.set(reportKey(snapshot.filters), saved); activeArchive = snapshot.filters;
    } catch (error) { failure = error; cache.delete(reportKey(snapshot.filters)); }
    finally {
      busy = false; await render();
      if (failure) status.textContent = `${failure.message || 'Lock could not be confirmed.'} Check the saved report before retrying.`;
    }
  });
  downloadButton.addEventListener('click', () => {
    if (!displayed) return;
    const url = URL.createObjectURL(new Blob([lockedReportDocument(displayed)], { type: 'text/html;charset=utf-8' }));
    const link = document.createElement('a'); link.href = url; link.download = `locked-${displayed.metadata.type}-${displayed.metadata.year}-${displayed.metadata.id}.html`; link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
  return {
    render,
    clearSelection() { activeArchive = null; },
    updateLocks() {
      for (const [key, value] of cache) { if (!value?.locked) cache.delete(key); }
      render();
    },
    reset() { ++sequence; cache.clear(); activeArchive = null; displayed = null; live = null; output.replaceChildren(); lockButton.disabled = true; downloadButton.classList.add('hidden'); status.classList.remove('report-lock-notice'); status.textContent = ''; },
    fail(error) { ++sequence; cache.clear(); live = null; showError(error); }
  };
}
