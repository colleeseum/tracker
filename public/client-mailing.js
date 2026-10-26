export function filterMailingClients(clients, requests, { search = '', season = '', status = '' } = {}) {
  const query = search.trim().toLowerCase();
  return clients.filter((client) => {
    if (query && ![client.name, client.email, client.emailCc].some((value) => String(value || '').toLowerCase().includes(query))) return false;
    return (!season && !status) || requests.some((request) => request.clientId === client.id && (!season || request.season === season) && (!status || (request.status || 'new') === status));
  }).sort((a, b) => String(a.name || '').localeCompare(String(b.name || '')));
}

export function initializeClientMailing({ getClients, getRequests, getSeasons, getStatuses, getStorageKey, prepare, load, send, verifyRouting }) {
  const el = (id) => document.getElementById(`client-mailing-${id}`);
  const modal = el('modal');
  const selected = new Set();
  let batch = null;
  let busy = false;
  const remembered = () => { try { return localStorage.getItem(getStorageKey()); } catch { return null; } };
  const remember = (id) => { try { localStorage.setItem(getStorageKey(), id); } catch { /* Resume remains available until the modal is closed. */ } };
  const setBusy = (value) => {
    busy = value;
    for (const id of ['close', 'resume', 'prepare', 'send', 'edit']) el(id).disabled = value;
    el('editor').disabled = value || Boolean(batch);
  };
  const visible = () => filterMailingClients(getClients(), getRequests(), { search: el('search').value, season: el('season').value, status: el('status').value });
  const renderClients = () => {
    const list = el('clients');
    list.replaceChildren();
    for (const client of visible()) {
      const label = document.createElement('label');
      label.className = 'checkbox';
      const checkbox = document.createElement('input');
      checkbox.type = 'checkbox';
      checkbox.checked = selected.has(client.id);
      checkbox.disabled = !client.email;
      checkbox.addEventListener('change', () => {
        if (checkbox.checked && selected.size >= 100) { checkbox.checked = false; el('error').textContent = 'Select at most 100 clients.'; return; }
        if (checkbox.checked) selected.add(client.id); else selected.delete(client.id);
        el('count').textContent = `${selected.size} selected`;
      });
      label.append(checkbox, document.createTextNode(`${client.name || 'Unnamed client'} · ${client.email || 'Missing email'}${client.active === false ? ' (inactive)' : ''}`));
      list.appendChild(label);
    }
    if (!list.children.length) list.textContent = 'No matching clients.';
    el('count').textContent = `${selected.size} selected`;
  };
  const option = (select, value, text) => {
    const item = document.createElement('option'); item.value = value; item.textContent = text; select.appendChild(item);
  };
  const renderPreview = () => {
    const recipient = batch?.recipients.find((item) => item.clientId === el('preview-client').value);
    el('preview').srcdoc = recipient ? batch.templates[recipient.templateKey].html : '';
  };
  const renderBatch = () => {
    el('review').classList.remove('hidden');
    el('prepare').classList.add('hidden');
    el('edit').classList.toggle('hidden', batch.recipients.some((item) => item.status !== 'ready'));
    el('subject').value = batch.subject; el('body').value = batch.body;
    el('editor').disabled = true;
    const rows = el('results'); rows.replaceChildren();
    const previewClient = el('preview-client').value;
    el('preview-client').replaceChildren();
    const labels = { ready: 'Ready', sending: 'In progress / check delivery before retrying', sent: 'Sent', sent_with_warning: 'Sent with warning', failed: 'Rejected / retry available', uncertain: 'Uncertain / manual review required' };
    for (const recipient of batch.recipients) {
      const row = document.createElement('tr');
      for (const text of [recipient.name, recipient.delivery.to, recipient.delivery.cc || 'None', `${labels[recipient.status] || recipient.status}${recipient.error ? `: ${recipient.error}` : ''}`]) {
        const cell = document.createElement('td'); cell.textContent = text; row.appendChild(cell);
      }
      rows.appendChild(row);
      option(el('preview-client'), recipient.clientId, `${recipient.name} (${recipient.locale.toUpperCase()})`);
    }
    if (batch.recipients.some((item) => item.clientId === previewClient)) el('preview-client').value = previewClient;
    const pending = batch.recipients.filter((item) => ['ready', 'failed'].includes(item.status));
    el('send').classList.toggle('hidden', !pending.length);
    el('send').textContent = batch.recipients.some((item) => item.status !== 'ready') ? `Send / retry ${pending.length} remaining` : `Send to ${pending.length} clients`;
    const sent = batch.recipients.filter((item) => ['sent', 'sent_with_warning'].includes(item.status)).length;
    const uncertain = batch.recipients.filter((item) => ['uncertain', 'sending'].includes(item.status)).length;
    el('progress').textContent = `${batch.recipients.length} clients: ${sent} sent, ${pending.length} remaining, ${uncertain} requiring delivery review.`;
    const dev = batch.recipients[0]?.delivery.developmentEmailRedirect;
    el('warning').textContent = dev ? 'DEV: each message goes only to entrepot@as-colle.com. All customer To and CC addresses are suppressed.' : '';
    el('warning').classList.toggle('hidden', !dev);
    renderPreview();
  };
  const validateRouting = async (value) => {
    const first = value.recipients[0]?.delivery;
    if (!first) throw new Error('No recipients found.');
    const verified = await verifyRouting(first);
    if (verified.developmentEmailRedirect !== first.developmentEmailRedirect) throw new Error('Mailing routing differs from this environment. Create a new preview.');
    for (const recipient of value.recipients) {
      if (recipient.delivery.developmentEmailRedirect !== verified.developmentEmailRedirect || (verified.developmentEmailRedirect && (recipient.delivery.to !== verified.to || recipient.delivery.cc))) throw new Error('Unsafe dev recipient routing. Sending is blocked.');
    }
  };
  const resetReview = () => {
    batch = null;
    el('review').classList.add('hidden'); el('prepare').classList.remove('hidden');
    el('send').classList.add('hidden'); el('edit').classList.add('hidden');
    el('warning').classList.add('hidden'); el('progress').textContent = ''; el('error').textContent = '';
    el('editor').disabled = false;
  };
  document.getElementById('email-clients').addEventListener('click', () => {
    if (busy) return;
    resetReview(); selected.clear(); el('form').reset();
    el('season').replaceChildren(); option(el('season'), '', 'All seasons');
    getSeasons().forEach((season) => option(el('season'), season.id, season.label?.en || season.label?.fr || season.id));
    el('status').replaceChildren(); option(el('status'), '', 'All statuses');
    getStatuses().forEach((status) => option(el('status'), status.value, status.label));
    el('resume').classList.toggle('hidden', !remembered());
    modal.classList.remove('hidden'); renderClients(); el('search').focus();
  });
  el('close').addEventListener('click', () => { if (!busy) modal.classList.add('hidden'); });
  for (const id of ['search', 'season', 'status']) el(id).addEventListener(id === 'search' ? 'input' : 'change', renderClients);
  el('select').addEventListener('click', () => {
    for (const client of visible()) if (client.email && selected.size < 100) selected.add(client.id);
    renderClients();
  });
  el('clear').addEventListener('click', () => { selected.clear(); renderClients(); });
  el('preview-client').addEventListener('change', renderPreview);
  el('edit').addEventListener('click', resetReview);
  el('resume').addEventListener('click', async () => {
    const id = remembered(); if (!id || busy) return;
    setBusy(true); el('error').textContent = '';
    try { const saved = await load({ batchId: id }); await validateRouting(saved); batch = saved; selected.clear(); saved.recipients.forEach((item) => selected.add(item.clientId)); renderClients(); renderBatch(); }
    catch (error) { el('error').textContent = error.message || 'Unable to load mailing.'; }
    finally { setBusy(false); }
  });
  el('form').addEventListener('submit', async (event) => {
    event.preventDefault(); if (busy || batch) return;
    if (!selected.size) { el('error').textContent = 'Select at least one client.'; return; }
    setBusy(true); el('error').textContent = '';
    try {
      const value = await prepare({ clientIds: [...selected], subject: el('subject').value.trim(), body: el('body').value.trim() });
      await validateRouting(value); batch = value; remember(batch.batchId); renderBatch();
    } catch (error) { el('error').textContent = error.message || 'Unable to prepare email.'; }
    finally { setBusy(false); }
  });
  el('send').addEventListener('click', async () => {
    if (busy || !batch) return;
    setBusy(true); el('error').textContent = '';
    try {
      await validateRouting(batch);
      for (const recipient of batch.recipients) {
        if (!['ready', 'failed'].includes(recipient.status)) continue;
        const result = await send({ batchId: batch.batchId, clientId: recipient.clientId, fingerprint: batch.fingerprint });
        Object.assign(recipient, result); renderBatch();
      }
    } catch (error) {
      el('error').textContent = `${error.message || 'Sending interrupted.'} Reload the saved mailing to check results before retrying.`;
      try { batch = await load({ batchId: batch.batchId }); renderBatch(); } catch { /* Keep the saved batch ID for later reconciliation. */ }
    } finally { setBusy(false); }
  });
}
