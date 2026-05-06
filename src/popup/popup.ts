import type { JobResult, RunState, Settings } from '../types'

// ── DOM refs ──────────────────────────────────────────────────────────────────
const btnRun = document.getElementById('btn-run') as HTMLButtonElement
const btnStop = document.getElementById('btn-stop') as HTMLButtonElement
const btnOpen = document.getElementById('btn-open') as HTMLButtonElement
const statusPill = document.getElementById('status-pill') as HTMLDivElement
const banner = document.getElementById('banner') as HTMLDivElement
const lastRunEl = document.getElementById('last-run') as HTMLDivElement
const lastRunWhen = document.getElementById('last-run-when') as HTMLSpanElement
const nextRunText = document.getElementById('next-run-text') as HTMLSpanElement

const scheduleEnabled = document.getElementById('schedule-enabled') as HTMLInputElement
const scheduleTime = document.getElementById('schedule-time') as HTMLInputElement
const backendUrl = document.getElementById('backend-url') as HTMLInputElement
const qboEnvSandbox = document.getElementById('qbo-env-sandbox') as HTMLInputElement
const qboEnvProduction = document.getElementById('qbo-env-production') as HTMLInputElement
let refreshTimer: number | undefined

// ── helpers ───────────────────────────────────────────────────────────────────

function setStatusPill(text: string, kind: '' | 'ok' | 'warn' | 'err' = '') {
  statusPill.textContent = text
  statusPill.className = `status-dot ${kind}`
}

function showBanner(html: string, kind: '' | 'err' = '') {
  banner.innerHTML = html
  banner.className = `banner ${kind}`
  banner.style.display = 'flex'
}
function hideBanner() {
  banner.style.display = 'none'
}

function formatRelativeTime(ts: number): string {
  const diff = Date.now() - ts
  const s = Math.round(diff / 1000)
  if (s < 60) return `${s}s ago`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min ago`
  const h = Math.round(m / 60)
  if (h < 24) return `${h}h ago`
  return new Date(ts).toLocaleDateString()
}

function formatTime(ts: number): string {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function scheduleRefresh(isRunning: boolean) {
  if (refreshTimer != null) window.clearTimeout(refreshTimer)
  refreshTimer = isRunning ? window.setTimeout(refresh, 1500) : undefined
}

// ── render ────────────────────────────────────────────────────────────────────

function renderLastRun(lastRun: JobResult | null | undefined, nextRun: number | null) {
  // schedule next run
  if (nextRun) {
    const d = new Date(nextRun)
    nextRunText.textContent = `Next: ${d.toLocaleString([], { weekday: 'short', hour: '2-digit', minute: '2-digit' })}`
  } else {
    nextRunText.textContent = 'Off'
  }

  // last run
  if (!lastRun) {
    lastRunEl.innerHTML = `<div class="meta" style="margin:0;padding:6px 0;color:var(--muted)">No runs yet.</div>`
    lastRunWhen.textContent = ''
    setStatusPill('Ready')
    return
  }

  lastRunWhen.textContent = formatRelativeTime(lastRun.timestamp)

  // Pill state from last run
  if (lastRun.error === 'Not logged in') {
    setStatusPill('Login required', 'err')
    showBanner(
      `<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2L1 21h22L12 2zm1 15h-2v-2h2v2zm0-4h-2V8h2v5z"/></svg>
       Please log into <strong>QuickBooks Online</strong> in the open tab and try again.`,
      'err'
    )
  } else if (!lastRun.ok) {
    setStatusPill('Error', 'err')
    showBanner(`<strong>Error:</strong> ${lastRun.error ?? 'Unknown'}`, 'err')
  } else {
    setStatusPill('Idle', 'ok')
    hideBanner()
  }

  // Stat cards
  const matchedClass = lastRun.matchedCount > 0 ? 'ok' : 'muted'
  const sentClass = lastRun.uncategorizedSent > 0 ? 'ok' : 'muted'
  const skippedClass = lastRun.skippedCount > 0 ? 'warn' : 'muted'

  const triggerBadge =
    lastRun.trigger === 'scheduled'
      ? '<span class="badge ok">scheduled</span>'
      : '<span class="badge muted">manual</span>'

  const backendBadge = lastRun.backendStatus.startsWith('OK')
    ? `<span class="badge ok">backend ✓</span>`
    : lastRun.backendStatus.startsWith('Backend')
      ? `<span class="badge err">backend ✕</span>`
      : `<span class="badge muted">${lastRun.backendStatus.includes('no uncategorized') ? 'no rows' : 'n/a'}</span>`

  lastRunEl.innerHTML = `
    <div class="stats">
      <div class="stat"><div class="num ${matchedClass}">${lastRun.matchedCount}</div><span class="lbl">matched</span></div>
      <div class="stat"><div class="num ${sentClass}">${lastRun.uncategorizedSent}</div><span class="lbl">sent</span></div>
      <div class="stat"><div class="num ${skippedClass}">${lastRun.skippedCount}</div><span class="lbl">skipped</span></div>
    </div>
    <div class="meta">
      <span>${triggerBadge} ${backendBadge}</span>
      <span>${formatTime(lastRun.timestamp)} · ${(lastRun.duration / 1000).toFixed(1)}s</span>
    </div>
    ${lastRun.details
      ? `<details>
           <summary>View details</summary>
           <div>
             ${lastRun.details.autoMatched.length
               ? `<strong>Matched (${lastRun.details.autoMatched.filter(m => m.success).length}/${lastRun.details.autoMatched.length}):</strong><br/>` +
                 lastRun.details.autoMatched.map(m =>
                   `${m.success ? '✓' : '✗'} ${m.date} — ${m.description || m.matchInfo} (${m.amount})${m.error ? ` <em style="color:var(--error)">${m.error}</em>` : ''}`
                 ).join('<br/>')
               : ''}
             ${lastRun.details.skipped.length
               ? `<br/><br/><strong>Skipped (${lastRun.details.skipped.length}):</strong><br/>` +
                 lastRun.details.skipped.slice(0, 8).map(s =>
                   `• ${s.date} — ${s.description || '(no description)'} <em>(${s.reason})</em>`
                 ).join('<br/>')
               : ''}
           </div>
         </details>
         <details>
           <summary>View logs (${lastRun.details.logs?.length ?? 0})</summary>
           <div style="font-family:ui-monospace,Menlo,monospace;white-space:pre-wrap;font-size:10.5px;background:#0d1117;color:#d1d5db;padding:8px;border-radius:4px;max-height:200px;overflow:auto">${(lastRun.details.logs ?? []).map(l => l.replace(/[<>&]/g, c => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c]!))).join('\n')}</div>
         </details>`
      : ''}
  `
}

function renderSettings(s: Settings) {
  scheduleEnabled.checked = s.scheduleEnabled
  scheduleTime.value = s.scheduleTime
  backendUrl.value = s.backendUrl
  qboEnvSandbox.checked = s.qboEnvironment !== 'production'
  qboEnvProduction.checked = s.qboEnvironment === 'production'
  btnOpen.textContent = s.qboEnvironment === 'production' ? 'Open Production QBO ↗' : 'Open Sandbox QBO ↗'
}

function renderRunState(runState: RunState | null | undefined) {
  const isRunning = runState?.running === true
  const isStopping = runState?.stopRequested === true

  btnRun.disabled = isRunning
  btnRun.classList.toggle('loading', isRunning)
  ;(btnRun.querySelector('.label') as HTMLElement).textContent = isRunning ? 'Running...' : 'Run Now'

  btnStop.hidden = !isRunning
  btnStop.disabled = !isRunning || isStopping
  btnStop.textContent = isStopping ? 'Stopping...' : 'Stop'

  if (isRunning) {
    setStatusPill(isStopping ? 'Stopping' : 'Running', 'warn')
    showBanner(
      isStopping
        ? '<strong>Stop requested.</strong> Finishing the current safe checkpoint.'
        : '<strong>Running...</strong> You can close this popup. The toolbar badge stays active until the job finishes.'
    )
  }

  scheduleRefresh(isRunning)
}

// ── messaging ─────────────────────────────────────────────────────────────────

function send<T = unknown>(msg: object): Promise<T> {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage(msg, (resp) => {
      if (chrome.runtime.lastError) return reject(chrome.runtime.lastError)
      resolve(resp as T)
    })
  })
}

async function refresh() {
  try {
    const { lastRun, nextRun, runState } = await send<{
      lastRun: JobResult | null
      nextRun: number | null
      runState: RunState
    }>({
      type: 'GET_LAST_RUN',
    })
    renderLastRun(lastRun, nextRun)
    renderRunState(runState)
  } catch (e) {
    setStatusPill('Background not ready', 'warn')
  }
}

async function loadSettings() {
  const { settings } = await send<{ settings: Settings }>({ type: 'GET_SETTINGS' })
  renderSettings(settings)
}

async function saveSettings() {
  const settings: Settings = {
    scheduleEnabled: scheduleEnabled.checked,
    scheduleTime: scheduleTime.value || '09:00',
    backendUrl: backendUrl.value.trim() || 'http://localhost:3001/api/llm/qbo-categorize',
    qboEnvironment: qboEnvProduction.checked ? 'production' : 'sandbox',
  }
  await send({ type: 'SET_SETTINGS', settings })
  await refresh()
}

// ── events ────────────────────────────────────────────────────────────────────

btnRun.addEventListener('click', async () => {
  btnRun.classList.add('loading')
  btnRun.disabled = true
  btnStop.hidden = false
  btnStop.disabled = false
  ;(btnRun.querySelector('.label') as HTMLElement).textContent = 'Running...'
  setStatusPill('Running', 'warn')
  showBanner('<strong>Running...</strong> You can close this popup. The toolbar badge stays active until the job finishes.')

  try {
    await send({ type: 'RUN_NOW' })
  } finally {
    btnRun.classList.remove('loading')
    btnRun.disabled = false
    ;(btnRun.querySelector('.label') as HTMLElement).textContent = 'Run Now'
    await refresh()
  }
})

btnStop.addEventListener('click', async () => {
  btnStop.disabled = true
  btnStop.textContent = 'Stopping...'
  setStatusPill('Stopping', 'warn')
  showBanner('<strong>Stop requested.</strong> Finishing the current safe checkpoint.')
  await send({ type: 'STOP_RUN' })
  await refresh()
})

btnOpen.addEventListener('click', async () => {
  await send({ type: 'OPEN_QBO' })
  window.close()
})

scheduleEnabled.addEventListener('change', saveSettings)
scheduleTime.addEventListener('change', saveSettings)
backendUrl.addEventListener('change', saveSettings)
qboEnvSandbox.addEventListener('change', saveSettings)
qboEnvProduction.addEventListener('change', saveSettings)

// ── init ─────────────────────────────────────────────────────────────────────

loadSettings().then(refresh)
