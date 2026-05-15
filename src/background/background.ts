import { DEFAULT_SETTINGS } from '../types'
import type { ApplyEvent, JobResult, PageResult, RunState, Settings, Suggestion } from '../types'
import { resolveClinic } from './clinic-resolver'

const QBO_TARGETS: Record<Settings['qboEnvironment'], { openUrl: string; tabMatchUrl: string; label: string }> = {
  sandbox: {
    openUrl: 'https://sandbox.qbo.intuit.com/app/banking',
    tabMatchUrl: 'https://sandbox.qbo.intuit.com/app/banking*',
    label: 'QuickBooks sandbox',
  },
  production: {
    openUrl: 'https://qbo.intuit.com/app/banking?jobId=accounting',
    tabMatchUrl: 'https://qbo.intuit.com/app/banking*',
    label: 'QuickBooks production',
  },
}
const ALARM_NAME = 'qbo-daily-run'
type ActiveRun = {
  id: string
  startedAt: number
  trigger: 'manual' | 'scheduled'
  tabId?: number
  stopRequested: boolean
}

const IDLE_RUN_STATE: RunState = {
  running: false,
  stopRequested: false,
  startedAt: null,
  trigger: null,
}

let activeRun: ActiveRun | null = null

// ── Settings ──────────────────────────────────────────────────────────────────

async function getSettings(): Promise<Settings> {
  const { settings } = await chrome.storage.local.get('settings')
  return { ...DEFAULT_SETTINGS, ...(settings ?? {}), scheduleEnabled: false }
}

async function setSettings(s: Settings) {
  await chrome.storage.local.set({ settings: { ...s, scheduleEnabled: false } })
}

async function rescheduleAlarm(s: Settings) {
  await chrome.alarms.clear(ALARM_NAME)
  if (!s.scheduleEnabled) return

  const [hh, mm] = s.scheduleTime.split(':').map(Number)
  const next = new Date()
  next.setHours(hh, mm, 0, 0)
  if (next.getTime() <= Date.now()) next.setDate(next.getDate() + 1)

  await chrome.alarms.create(ALARM_NAME, {
    when: next.getTime(),
    periodInMinutes: 24 * 60,
  })
}

// ── Notifications & badge ─────────────────────────────────────────────────────

function notify(title: string, message: string, isError = false) {
  chrome.notifications.create({
    type: 'basic',
    iconUrl: chrome.runtime.getURL('icon128.png'),
    title,
    message,
    priority: isError ? 2 : 1,
  })
}

function setBadge(text: string, color = '#2ca01c') {
  chrome.action.setBadgeText({ text })
  chrome.action.setBadgeBackgroundColor({ color })
}

async function setRunState(state: RunState) {
  await chrome.storage.local.set({ runState: state })
}

async function clearRunState() {
  await setRunState(IDLE_RUN_STATE)
}

async function markRunning(run: ActiveRun) {
  await setRunState({
    running: true,
    stopRequested: run.stopRequested,
    startedAt: run.startedAt,
    trigger: run.trigger,
    tabId: run.tabId,
  })
  setBadge(run.stopRequested ? 'STOP' : 'RUN', run.stopRequested ? '#D97706' : '#00A3E0')
  await chrome.action.setTitle({
    title: run.stopRequested ? 'QBO Categorizer stopping...' : 'QBO Categorizer running...',
  })
}

function makeInProgressResult(trigger: 'manual' | 'scheduled'): JobResult {
  return {
    ok: false,
    trigger,
    timestamp: Date.now(),
    duration: 0,
    error: 'Run already in progress',
    loggedIn: false,
    totalRows: 0,
    matchedCount: 0,
    uncategorizedSent: 0,
    skippedCount: 0,
    appliedCount: 0,
    applyFailedCount: 0,
    backendStatus: 'running',
  }
}

// ── Tab handling ──────────────────────────────────────────────────────────────

function getQboTarget(settings: Settings) {
  return QBO_TARGETS[settings.qboEnvironment] ?? QBO_TARGETS.sandbox
}

function isQboBankingUrl(url: string | undefined, settings: Settings) {
  if (!url) return false
  const target = getQboTarget(settings)
  return url.startsWith(target.openUrl.split('?')[0])
}

async function openOrFocusQBO(focus: boolean, settings: Settings): Promise<chrome.tabs.Tab | null> {
  const target = getQboTarget(settings)
  const [activeTab] = await chrome.tabs.query({ active: true, currentWindow: true })
  if (activeTab?.id != null && isQboBankingUrl(activeTab.url, settings)) {
    return activeTab
  }

  const tabs = await chrome.tabs.query({ url: target.tabMatchUrl })
  if (tabs[0]?.id != null) {
    if (focus) {
      await chrome.tabs.update(tabs[0].id, { active: true })
      if (tabs[0].windowId != null) await chrome.windows.update(tabs[0].windowId, { focused: true })
    }
    return tabs[0]
  }
  return chrome.tabs.create({ url: target.openUrl, active: focus })
}

async function requestStopRun() {
  if (!activeRun) {
    await clearRunState()
    setBadge('')
    return false
  }

  activeRun.stopRequested = true
  await markRunning(activeRun)

  if (activeRun.tabId != null) {
    try {
      await chrome.scripting.executeScript({
        target: { tabId: activeRun.tabId },
        func: () => {
          ;(window as Window & typeof globalThis & { __qboCategorizerStop?: boolean }).__qboCategorizerStop = true
        },
      })
    } catch {
      // The tab may still be loading or may no longer allow injection. The background job
      // still observes stopRequested between major phases.
    }
  }

  return true
}

function waitForTabComplete(tabId: number, timeoutMs = 30000): Promise<boolean> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      chrome.tabs.onUpdated.removeListener(listener)
      resolve(false)
    }, timeoutMs)

    const listener = (id: number, info: chrome.tabs.TabChangeInfo) => {
      if (id === tabId && info.status === 'complete') {
        clearTimeout(timer)
        chrome.tabs.onUpdated.removeListener(listener)
        resolve(true)
      }
    }

    chrome.tabs.get(tabId, (tab) => {
      if (tab?.status === 'complete') {
        clearTimeout(timer)
        resolve(true)
      } else {
        chrome.tabs.onUpdated.addListener(listener)
      }
    })
  })
}

// ── In-page extractor + categorizer (self-contained) ─────────────────────────
// Runs inside the QBO tab via chrome.scripting.executeScript.
// Returns PageResult.

async function inPageWorker(): Promise<PageResult> {
  const pageWindow = window as Window & typeof globalThis & { __qboCategorizerStop?: boolean }
  pageWindow.__qboCategorizerStop = false
  const assertNotStopped = () => {
    if (pageWindow.__qboCategorizerStop) throw new Error('QBO_CATEGORIZER_STOPPED')
  }
  const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))
  const wait = async (ms: number) => {
    const started = Date.now()
    while (Date.now() - started < ms) {
      assertNotStopped()
      await sleep(Math.min(250, ms - (Date.now() - started)))
    }
    assertNotStopped()
  }
  const text = (el: Element | null | undefined) => el?.textContent?.trim() ?? ''

  function readRealmId(): string | null {
    // 1. Query param on the banking URL
    const url = new URL(location.href)
    const fromQuery = url.searchParams.get('realmId')
    if (fromQuery) return fromQuery
    // 2. window.qboeUser?.realm (some QBO builds)
    const w = window as unknown as { qboeUser?: { realm?: string | number } }
    const fromGlobal = w.qboeUser?.realm
    if (fromGlobal != null) return String(fromGlobal)
    // 3. Cookies — this build of QBO stores it as qbo.currentcompanyid
    const cookies = document.cookie.split(';').map((c) => c.trim())
    for (const c of cookies) {
      const eq = c.indexOf('=')
      if (eq < 0) continue
      const name = c.slice(0, eq)
      const value = decodeURIComponent(c.slice(eq + 1))
      if (name === 'qbo.currentcompanyid' && /^\d+$/.test(value)) return value
      // 4. Fallback: shell.ctx.id has format "authId=...&companyId=..."
      if (name === 'shell.ctx.id') {
        const m = value.match(/companyId=(\d+)/)
        if (m) return m[1]
      }
    }
    return null
  }
  const realmId = readRealmId()

  // ── Logging ──────────────────────────────────────────────────────────────────
  const logs: string[] = []
  const log = (msg: string) => {
    const line = `[${new Date().toISOString().slice(11, 23)}] ${msg}`
    logs.push(line)
    console.log('[QBO Categorizer]', line)
  }

  log(`worker v1.8 started · url=${location.href}`)
  log(`realmId=${realmId ?? '(none)'}`)

  // Bail early if not on the banking page
  if (!location.href.includes('/app/banking')) {
    log('NOT on /app/banking — assuming login required')
    return {
      loggedIn: false,
      url: location.href,
      realmId,
      totalRows: 0,
      autoMatched: [],
      uncategorized: [],
      skipped: [],
      logs,
    }
  }

  // Wait for the virtualized table (up to 12 s)
  let scroller: HTMLElement | null = null
  for (let i = 0; i < 24; i++) {
    scroller = document.querySelector<HTMLElement>('.ids-table__virtualized-container')
    if (scroller) break
    await wait(500)
  }
  if (!scroller) {
    log('FAIL — table never appeared after 12s')
    return {
      loggedIn: false,
      url: location.href,
      realmId,
      totalRows: 0,
      autoMatched: [],
      uncategorized: [],
      skipped: [],
      logs,
    }
  }
  log(`table found · scrollHeight=${scroller.scrollHeight}`)

  // ── Helpers ─────────────────────────────────────────────────────────────────

  function getPendingCount(): number | null {
    // The "Pending (25)" segment label
    const buttons = document.querySelectorAll<HTMLElement>('[data-testid^="segmentedBTN"]')
    for (const b of Array.from(buttons)) {
      const t = b.textContent?.trim() ?? ''
      const m = t.match(/Pending\s*\((\d+)\)/i)
      if (m) return parseInt(m[1])
    }
    return null
  }

  function mkOpts(el: HTMLElement): MouseEventInit & PointerEventInit {
    const r = el.getBoundingClientRect()
    return {
      bubbles: true, cancelable: true, view: window, button: 0, buttons: 1,
      clientX: r.left + r.width / 2, clientY: r.top + r.height / 2,
      composed: true,
      pointerType: 'mouse', pointerId: 1, isPrimary: true,
    }
  }

  function fireHoverChain(el: HTMLElement) {
    const o = mkOpts(el)
    el.dispatchEvent(new PointerEvent('pointerover', o))
    el.dispatchEvent(new MouseEvent('mouseover', o))
    el.dispatchEvent(new PointerEvent('pointerenter', o))
    el.dispatchEvent(new MouseEvent('mouseenter', o))
    el.dispatchEvent(new PointerEvent('pointermove', o))
    el.dispatchEvent(new MouseEvent('mousemove', o))
  }

  // Re-finds the freshest match button in the cell. Handles both:
  //   1. Old phantom UI: button.action-phantom.match-action
  //   2. New ComboLink UI: button.idsLinkActionButton with text "Match"
  function findMatchBtn(cell: HTMLElement): HTMLButtonElement | null {
    // Old UI variations
    const old =
      cell.querySelector<HTMLButtonElement>('.match-action:not(.action-phantom)') ??
      cell.querySelector<HTMLButtonElement>('button.match-action') ??
      cell.querySelector<HTMLButtonElement>('.match-action')
    if (old) return old

    // New ComboLink UI — primary link button whose text is "Match"
    const linkBtns = cell.querySelectorAll<HTMLButtonElement>('.idsLinkActionButton')
    for (const b of Array.from(linkBtns)) {
      if (/^Match$/i.test((b.textContent || '').trim())) return b
    }

    // Last-resort: any button with aria-label containing "match"
    return cell.querySelector<HTMLButtonElement>('button[aria-label*="match" i]') ?? null
  }

  // Locate and click the Match button. Handles two UIs:
  //   - old phantom (needs hover-to-upgrade dance)
  //   - new ComboLink (regular button, just click)
  async function activateAndClickMatch(
    row: HTMLElement
  ): Promise<{ ok: boolean; method: string; details: string }> {
    const cell = row.querySelector<HTMLElement>('.idsTable__cell.action')
    if (!cell) return { ok: false, method: 'no-cell', details: 'action cell not found' }

    // Find match button (either UI)
    let btn = findMatchBtn(cell)
    let polls = 0
    if (!btn) {
      fireHoverChain(row)
      fireHoverChain(cell)
      for (polls = 1; polls <= 10; polls++) {
        await wait(200)
        btn = findMatchBtn(cell)
        if (btn) break
      }
    }
    if (!btn) {
      const buttons = cell.querySelectorAll('button')
      const summary = Array.from(buttons).map((b) => `class="${b.className}" text="${(b.textContent || '').trim()}"`).join(' | ') || '(none)'
      return {
        ok: false,
        method: 'no-match-btn',
        details: `Match button never found after ${polls} polls. Buttons: ${summary}`,
      }
    }

    const isPhantom = btn.classList.contains('action-phantom')
    const uiKind = isPhantom ? 'old-phantom' : 'new-combolink'

    if (isPhantom) {
      // Old UI: hover the row to wake Morpheus, then re-find (DOM may swap)
      fireHoverChain(row)
      fireHoverChain(cell)
      fireHoverChain(btn)
      for (let i = 0; i < 8; i++) {
        await wait(80)
        const fresh = findMatchBtn(cell)
        if (fresh && fresh.isConnected && !fresh.classList.contains('action-phantom')) {
          btn = fresh
          break
        }
        if (fresh && fresh.isConnected) btn = fresh
      }
      if (!btn || !btn.isConnected) {
        return { ok: false, method: 'phantom-vanished', details: 'phantom button gone after hover' }
      }
    }
    // New UI (no action-phantom class): click directly with NO hover.
    // Hovering the new ComboLink button triggers a React re-render that
    // detaches it — so we skip hover entirely.

    // Click — full event sequence
    const o = mkOpts(btn)
    btn.dispatchEvent(new PointerEvent('pointerdown', o))
    btn.dispatchEvent(new MouseEvent('mousedown', o))
    await wait(50)
    btn.dispatchEvent(new PointerEvent('pointerup', o))
    btn.dispatchEvent(new MouseEvent('mouseup', o))
    btn.dispatchEvent(new MouseEvent('click', o))

    try { btn.click() } catch {}

    return {
      ok: true,
      method: `${uiKind}-click (polls=${polls})`,
      details: `class="${btn.className.slice(0, 80)}..." text="${(btn.textContent || '').trim()}" connected=${btn.isConnected}`,
    }
  }

  function findPostBtn(cell: HTMLElement): HTMLButtonElement | null {
    const old =
      cell.querySelector<HTMLButtonElement>('.post-action:not(.action-phantom)') ??
      cell.querySelector<HTMLButtonElement>('button.post-action') ??
      cell.querySelector<HTMLButtonElement>('.post-action')
    if (old) return old
    const linkBtns = cell.querySelectorAll<HTMLButtonElement>('.idsLinkActionButton')
    for (const b of Array.from(linkBtns)) {
      if (/^Post$/i.test((b.textContent || '').trim())) return b
    }
    return cell.querySelector<HTMLButtonElement>('button[aria-label*="post" i]') ?? null
  }

  async function activateAndClickPost(
    row: HTMLElement
  ): Promise<{ ok: boolean; method: string; details: string }> {
    const cell = row.querySelector<HTMLElement>('.idsTable__cell.action')
    if (!cell) return { ok: false, method: 'no-cell', details: 'action cell not found' }

    let btn = findPostBtn(cell)
    let polls = 0
    if (!btn) {
      fireHoverChain(row)
      fireHoverChain(cell)
      for (polls = 1; polls <= 10; polls++) {
        await wait(200)
        btn = findPostBtn(cell)
        if (btn) break
      }
    }
    if (!btn) {
      const buttons = cell.querySelectorAll('button')
      const summary = Array.from(buttons).map((b) => `class="${b.className}" text="${(b.textContent || '').trim()}"`).join(' | ') || '(none)'
      return { ok: false, method: 'no-post-btn', details: `Post button never found after ${polls} polls. Buttons: ${summary}` }
    }

    const isPhantom = btn.classList.contains('action-phantom')
    const uiKind = isPhantom ? 'old-phantom' : 'new-combolink'

    if (isPhantom) {
      fireHoverChain(row)
      fireHoverChain(cell)
      fireHoverChain(btn)
      for (let i = 0; i < 8; i++) {
        await wait(80)
        const fresh = findPostBtn(cell)
        if (fresh && fresh.isConnected && !fresh.classList.contains('action-phantom')) { btn = fresh; break }
        if (fresh && fresh.isConnected) btn = fresh
      }
      if (!btn || !btn.isConnected) return { ok: false, method: 'phantom-vanished', details: 'phantom post button gone after hover' }
    }

    const o = mkOpts(btn)
    btn.dispatchEvent(new PointerEvent('pointerdown', o))
    btn.dispatchEvent(new MouseEvent('mousedown', o))
    await wait(50)
    btn.dispatchEvent(new PointerEvent('pointerup', o))
    btn.dispatchEvent(new MouseEvent('mouseup', o))
    btn.dispatchEvent(new MouseEvent('click', o))
    try { btn.click() } catch {}

    return {
      ok: true,
      method: `${uiKind}-post-click (polls=${polls})`,
      details: `class="${btn.className.slice(0, 80)}..." text="${(btn.textContent || '').trim()}" connected=${btn.isConnected}`,
    }
  }

  // Open the multi-match overlay for a row, pick the first radio option, and click Match.
  // Called after single-match auto-clicks and before the uncategorized /qbo-suggest flow.
  async function resolveMultiMatchOverlay(
    row: HTMLElement,
    bankRow: RawRow,
  ): Promise<{ ok: boolean; method: string; details: string }> {
    // Walk an element's React fiber tree (and optionally up DOM ancestors) to find
    // and invoke an onClick / onMouseDown handler directly. This bypasses the DOM
    // event system entirely — necessary because QBO's React ignores synthetic events
    // dispatched from extension scripts (isTrusted=false is rejected by some handlers,
    // and React 17+ event delegation requires the event to bubble from within the root).
    function fireReactHandler(startEl: HTMLElement): boolean {
      const rect = startEl.getBoundingClientRect()
      const fakeEvent = {
        type: 'click', bubbles: true, cancelable: true, isTrusted: false,
        target: startEl, currentTarget: startEl as EventTarget,
        clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2,
        preventDefault() {}, stopPropagation() {}, stopImmediatePropagation() {},
        nativeEvent: new MouseEvent('click', { bubbles: true, cancelable: true }),
        persist() {},
      }
      // Walk up to 6 DOM levels from the trigger element (but stop before the row itself)
      for (let el: HTMLElement | null = startEl, domDepth = 0; el && domDepth < 6; el = el.parentElement, domDepth++) {
        const fk = Object.keys(el).find(k => k.startsWith('__reactFiber') || k.startsWith('__reactInternalInstance'))
        if (!fk) continue
        let fiber = (el as any)[fk]
        let fd = 0
        while (fiber && fd < 25) {
          const p = fiber.memoizedProps
          if (p) {
            for (const evName of ['onClick', 'onMouseDown', 'onPointerDown']) {
              if (typeof p[evName] === 'function') {
                try {
                  p[evName]({ ...fakeEvent, currentTarget: el, type: evName === 'onClick' ? 'click' : evName === 'onMouseDown' ? 'mousedown' : 'pointerdown' })
                  log(`  [multi-match] React ${evName} fired on ${el.tagName}.${String(el.className).split(' ')[0]} (domDepth=${domDepth} fiberDepth=${fd})`)
                  return true
                } catch (e) {
                  log(`  [multi-match] React ${evName} threw: ${(e as Error).message}`)
                }
              }
            }
          }
          fiber = fiber.return; fd++
        }
      }
      return false
    }

    const categoryCell = row.querySelector<HTMLElement>('.idsTable__cell.category')
    let triggerEl: HTMLElement | null =
      categoryCell?.querySelector<HTMLElement>('.matchedInvoicedPlaceholder')
      ?? categoryCell?.querySelector<HTMLElement>('.matchedInvoicedText')
      ?? row.querySelector<HTMLButtonElement>('.sparkleWithPlaceholder__sparkleVisible')

    // If we found a container div, prefer its inner anchor/button (the real click target)
    if (triggerEl) {
      const inner = triggerEl.querySelector<HTMLElement>('a, button, [role="button"], [tabindex]')
      if (inner) triggerEl = inner
    }

    if (!triggerEl) {
      return { ok: false, method: 'no-trigger', details: '"Suggested matches found" element not found in row' }
    }

    const triggerMethod = triggerEl.className.split(' ')[0] || triggerEl.tagName
    const triggerText = (triggerEl.textContent ?? '').trim().slice(0, 60)
    const triggerHtml = triggerEl.outerHTML.slice(0, 200)
    log(`  [multi-match] trigger: ${triggerEl.tagName}.${triggerMethod} text="${triggerText}"`)
    log(`  [multi-match] trigger html: ${triggerHtml}`)

    // Step 1: Hover chain to ensure phantom buttons are visible
    fireHoverChain(row)
    if (categoryCell) fireHoverChain(categoryCell)
    fireHoverChain(triggerEl)
    await wait(150)

    // Step 2: React fiber direct invocation (primary method — bypasses isTrusted check)
    const firedViaFiber = fireReactHandler(triggerEl)
    log(`  [multi-match] fireReactHandler=${firedViaFiber}`)

    // Step 3: Synthetic DOM events + native .click() as fallback
    const oTrigger = mkOpts(triggerEl)
    triggerEl.dispatchEvent(new PointerEvent('pointerdown', oTrigger))
    triggerEl.dispatchEvent(new MouseEvent('mousedown', oTrigger))
    triggerEl.dispatchEvent(new PointerEvent('pointerup', oTrigger))
    triggerEl.dispatchEvent(new MouseEvent('mouseup', oTrigger))
    triggerEl.dispatchEvent(new MouseEvent('click', oTrigger))
    try { triggerEl.click() } catch {}

    // Step 4: Also try focusing + Enter key (some React components listen to keydown)
    try {
      triggerEl.focus()
      await wait(50)
      triggerEl.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', bubbles: true, cancelable: true }))
      triggerEl.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', code: 'Enter', bubbles: true, cancelable: true }))
    } catch {}

    // Wait for the overlay to appear. Watch for BOTH DOM insertions AND visibility changes.
    await wait(1500)

    // Check what appeared in the DOM — look for any overlay/panel/dialog
    const allOverlayLike = Array.from(document.querySelectorAll<HTMLElement>(
      '.redesign-overlay, [role="dialog"], [class*="overlay"], [class*="panel"], [class*="match-suggestion"]'
    )).map(el => `${el.tagName}.${String(el.className).slice(0, 40)}`)
    log(`  [multi-match] overlay-like elements in DOM (${allOverlayLike.length}): ${allOverlayLike.slice(0, 10).join(' | ')}`)

    // Wait for the Match|Categorize toggle to appear inside the overlay
    let matchToggleBtn: HTMLElement | null = null
    for (let i = 0; i < 15; i++) {
      matchToggleBtn =
        document.querySelector<HTMLElement>('[role="button"][aria-label="Match"]')
        ?? document.querySelector<HTMLElement>('[aria-label="Match"][aria-pressed]')
        ?? document.querySelector<HTMLElement>('button[class*="match"][class*="toggle" i]')
      if (matchToggleBtn) break
      await wait(200)
    }

    if (!matchToggleBtn) {
      log(`  [multi-match] toggle never appeared — checking for direct radios`)
    } else {
      const isPressed = matchToggleBtn.getAttribute('aria-pressed') === 'true'
      log(`  [multi-match] toggle found aria-pressed=${isPressed}`)
      if (!isPressed) {
        // Try fiber invocation on the toggle too
        const toggleFired = fireReactHandler(matchToggleBtn)
        if (!toggleFired) {
          const oToggle = mkOpts(matchToggleBtn)
          matchToggleBtn.dispatchEvent(new MouseEvent('click', oToggle))
          try { matchToggleBtn.click() } catch {}
        }
        await wait(400)
      }
    }

    // Wait for radio inputs (match options) to appear
    let firstRadio: HTMLInputElement | null = null
    let overlayEl: HTMLElement | null = null
    for (let i = 0; i < 20; i++) {
      firstRadio = document.querySelector<HTMLInputElement>('input[type="radio"]')
      if (firstRadio) {
        overlayEl = firstRadio.closest<HTMLElement>('.redesign-overlay, [role="dialog"], [class*="overlay"]')
        break
      }
      await wait(200)
    }

    if (!firstRadio) {
      // Single-match case: overlay opened in Match mode but shows a direct Match button
      // (no radio selection needed because QBO already identified the one correct match).
      // This happens when a row's match state changes between harvest time and process time
      // (e.g. other rows being matched reduces the candidates to 1).
      if (matchToggleBtn) {
        const overlayContainer = matchToggleBtn.closest<HTMLElement>(
          '.redesign-overlay-main-row-content, .redesign-overlay, [role="dialog"]'
        ) ?? document.body
        const directMatchBtn = Array.from(
          overlayContainer.querySelectorAll<HTMLElement>('button, [role="button"]')
        ).find((b) => {
          const rect = b.getBoundingClientRect()
          if (rect.width === 0 || rect.height === 0) return false
          // Exclude the Match/Categorize tab buttons (they have aria-pressed)
          if (b.getAttribute('aria-pressed') !== null) return false
          const lbl = (b.getAttribute('aria-label') ?? '').toLowerCase()
          const t = (b.textContent ?? '').trim().toLowerCase()
          return lbl === 'post transaction' || t === 'match'
        }) ?? null

        if (directMatchBtn) {
          log(`  [multi-match] single-match mode: clicking "${(directMatchBtn.textContent ?? '').trim()}" [${directMatchBtn.getAttribute('aria-label')}] directly (no radio needed)`)
          const postFired = fireReactHandler(directMatchBtn)
          if (!postFired) {
            const oBtn = mkOpts(directMatchBtn)
            directMatchBtn.dispatchEvent(new MouseEvent('click', oBtn))
            try { directMatchBtn.click() } catch {}
          }
          let stillFindable2 = true
          for (let i = 0; i < 25; i++) {
            await wait(200)
            if (findRowByKey(bankRow) == null) { stillFindable2 = false; break }
          }
          return {
            ok: !stillFindable2,
            method: `${triggerMethod}(single-direct)`,
            details: stillFindable2
              ? 'row still present after single-match direct click'
              : 'matched via single-match direct Match button',
          }
        }
      }

      // Still nothing — log diagnostics and give up
      const visibleBtns = Array.from(document.querySelectorAll<HTMLElement>('button, [role="button"]')).filter(b => {
        const r = b.getBoundingClientRect(); return r.width > 0 && r.height > 0
      }).map(b => `"${(b.textContent ?? '').trim().slice(0, 20)}"[${b.getAttribute('aria-label') ?? ''}]`).slice(0, 10)
      log(`  [multi-match] no radios + no direct Match btn. Visible interactive: ${visibleBtns.join(' | ')}`)
      try { document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true })) } catch {}
      return { ok: false, method: triggerMethod, details: `no radio inputs appeared (fiber=${firedViaFiber})` }
    }

    // Read the label for logging
    const radioRow = firstRadio.closest('tr')
    const cells = radioRow?.querySelectorAll('td') ?? []
    const chosenLabel = cells[1] ? (cells[1].querySelector('a')?.textContent?.trim() ?? cells[1].textContent?.trim() ?? '') : ''
    const chosenAmount = cells[2] ? cells[2].textContent?.trim() ?? '' : ''
    const chosenVendor = cells[3] ? cells[3].textContent?.trim() ?? '' : ''
    log(`  [multi-match] overlay="${overlayEl?.className?.slice(0, 60) ?? 'doc-level'}" · first option: "${chosenLabel}" vendor="${chosenVendor}" amount="${chosenAmount}"`)

    // Click the first radio — try fiber then fall back to synthetic events
    const radioFired = fireReactHandler(firstRadio)
    if (!radioFired) {
      const oRadio = mkOpts(firstRadio)
      firstRadio.dispatchEvent(new MouseEvent('click', oRadio))
      firstRadio.checked = true
      firstRadio.dispatchEvent(new Event('change', { bubbles: true }))
      try { firstRadio.click() } catch {}
    }
    await wait(400)

    // Find the Match / Post button
    const searchRoot = overlayEl ?? document
    const postBtn =
      searchRoot.querySelector<HTMLButtonElement>('button[aria-label="Post transaction"]') ??
      Array.from(searchRoot.querySelectorAll<HTMLButtonElement>('button')).find((b) => {
        if (b.closest('[role="list"]')) return false
        const lbl = (b.getAttribute('aria-label') ?? '').toLowerCase()
        const t = (b.textContent ?? '').trim().toLowerCase()
        return lbl.includes('post') || lbl.includes('match') || t === 'match'
      }) ?? null

    if (!postBtn) {
      try { document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true })) } catch {}
      return { ok: false, method: triggerMethod, details: 'Match/Post button not found after radio click' }
    }

    log(`  [multi-match] clicking post btn: "${(postBtn.textContent ?? '').trim()}" [${postBtn.getAttribute('aria-label')}] disabled=${postBtn.disabled}`)

    const postFired = fireReactHandler(postBtn)
    if (!postFired) {
      const oBtn = mkOpts(postBtn)
      postBtn.dispatchEvent(new MouseEvent('click', oBtn))
      try { postBtn.click() } catch {}
    }

    // Poll for the row to disappear (up to 5s)
    let stillFindable = true
    for (let i = 0; i < 25; i++) {
      await wait(200)
      if (findRowByKey(bankRow) == null) { stillFindable = false; break }
    }

    return {
      ok: !stillFindable,
      method: `multi-match-overlay(${triggerMethod})`,
      details: stillFindable
        ? `row still present after clicking Match for "${chosenLabel}"`
        : `matched to "${chosenLabel}" (${chosenVendor} ${chosenAmount})`,
    }
  }

  // ── per-row extraction helpers ──────────────────────────────────────────────

  type RawRow = {
    rowIndex: string
    page: number
    date: string
    description: string
    spent: string
    received: string
    type: 'expense' | 'income' | ''
    fromTo: string
    category: string
    matchInfo: string
    action: string
    hasSparkle: boolean
    isSingleMatch: boolean
    isMultiMatch: boolean
    isUncategorized: boolean
    isAutoPost: boolean
  }

  function parseRow(row: HTMLElement, page: number): RawRow | null {
    const rowIndex = row.getAttribute('aria-rowindex') ?? row.dataset.index ?? ''
    if (!rowIndex) return null

    const cell = (cls: string) => row.querySelector<HTMLElement>(`.${cls}`)

    const date = text(cell('txnDate'))
    const description = text(cell('description')?.querySelector('.description-tooltip'))
    const spentRaw = text(cell('spent')?.querySelector('.pull-right'))
    const receivedRaw = text(cell('received')?.querySelector('.received-amount'))
    const spent = spentRaw.replace(/\s+/g, '')
    const received = receivedRaw.replace(/\s+/g, '')

    const fromToRaw =
      cell('payee')?.querySelector<HTMLButtonElement>('button[aria-label]')?.getAttribute('aria-label') ?? ''
    const fromTo = /^Select (vendor|customer)$/i.test(fromToRaw) ? '' : fromToRaw

    const categoryCell = cell('category')
    const matchInfo = text(categoryCell?.querySelector('.matchedInvoicedText'))
    // Category label in the new redesign-enabled-qf UI lives in one of two places:
    //   1. Phantom button aria-label: <button class="QuickfillsPhantom qbo-quickfills-ui-account" aria-label="Uncategorized Income">
    //   2. Hydrated container data-props JSON: <div class="QuickfillsContainer" data-props='{"placeholder":"Uncategorized Income",...}'>
    const phantomCatBtn = categoryCell?.querySelector<HTMLButtonElement>(
      '.QuickfillsPhantom.qbo-quickfills-ui-account',
    )
    let categoryLabel = phantomCatBtn?.getAttribute('aria-label') ?? ''
    if (!categoryLabel) {
      const container = categoryCell?.querySelector<HTMLElement>('.QuickfillsContainer[data-props]')
      const raw = container?.getAttribute('data-props')
      if (raw) {
        try {
          const parsed = JSON.parse(raw)
          if (typeof parsed?.placeholder === 'string') categoryLabel = parsed.placeholder
        } catch {
          // ignore
        }
      }
    }
    // Determine action — handles both old phantom UI and new ComboLink UI
    let action = ''
    const actionCell = cell('action')

    // Old phantom UI
    const phantomBtn = actionCell?.querySelector<HTMLButtonElement>('.action-phantom')
    if (phantomBtn?.classList.contains('match-action')) action = 'Match'
    else if (phantomBtn?.classList.contains('post-action')) action = 'Post'

    // New ComboLink UI: read text of primary link button
    if (!action && actionCell) {
      const linkBtns = actionCell.querySelectorAll<HTMLButtonElement>('.idsLinkActionButton')
      for (const b of Array.from(linkBtns)) {
        const t = (b.textContent || '').trim()
        if (/^Match$/i.test(t)) { action = 'Match'; break }
        if (/^Post$/i.test(t))  { action = 'Post';  break }
      }
    }

    // Last-resort text fallback (old DOM)
    if (!action) action = text(actionCell?.querySelector('.phantom-link-text'))

    const hasSparkle = !!categoryCell?.querySelector('.sparkleWithPlaceholder__sparkleVisible')
    const isMultiMatch = /\d+\s*\|\s*Suggested matches found/i.test(matchInfo)
    const isSingleMatch = hasSparkle && matchInfo.length > 0 && !isMultiMatch
    const isUncategorized = /^Uncategorized\s+(Expense|Income)$/i.test(categoryLabel)
    // Already categorized + Post action: just needs the Post button clicked
    const isAutoPost = action === 'Post' && !isUncategorized && categoryLabel.length > 0

    if (!date && !spent && !received) return null

    return {
      rowIndex,
      page,
      date,
      description,
      spent,
      received,
      type: received ? 'income' : spent ? 'expense' : '',
      fromTo,
      category: categoryLabel,
      matchInfo,
      action,
      hasSparkle,
      isSingleMatch,
      isMultiMatch,
      isUncategorized,
      isAutoPost,
    }
  }

  // Primary lookup: O(1) CSS query by aria-rowindex (virtualizer keeps this stable).
  function findRowByIndex(rowIndex: string): HTMLElement | null {
    if (!rowIndex) return null
    return document.querySelector<HTMLElement>(`.idsTable__row[aria-rowindex="${rowIndex}"]`)
      ?? document.querySelector<HTMLElement>(`.idsTable__row[data-index="${rowIndex}"]`)
  }

  // Content-based lookup — works even after QBO re-indexes rows following removals.
  // Uses the broad `.idsTable__row[aria-rowindex]` selector because the scoped
  // `#table-body-main tbody` container loses its `tbody` after row removals.
  function findRowByContent(target: RawRow): HTMLElement | null {
    const rows = document.querySelectorAll<HTMLElement>('.idsTable__row[aria-rowindex]')
    for (const row of Array.from(rows)) {
      const date = text(row.querySelector('.txnDate'))
      const desc = text(row.querySelector('.description-tooltip'))
      const spent = text(row.querySelector('.spent .pull-right')).replace(/\s+/g, '')
      const received = text(row.querySelector('.received-amount')).replace(/\s+/g, '')
      if (
        date === target.date &&
        desc === target.description &&
        spent === target.spent &&
        received === target.received
      ) {
        return row
      }
    }
    return null
  }

  // Content is primary (stable even after re-indexing); aria-rowindex only as fallback
  // for duplicate-content rows that are otherwise indistinguishable.
  function findRowByKey(target: RawRow): HTMLElement | null {
    return findRowByContent(target) ?? findRowByIndex(target.rowIndex)
  }

  // Scrolls the virtualized container until the target row is rendered, then centers it.
  // Uses rowIndex to estimate the target's scroll position so we jump directly
  // rather than scanning linearly — avoids leaving the container at a bad scrollTop.
  async function findRowWithScroll(target: RawRow): Promise<HTMLElement | null> {
    const sc = document.querySelector<HTMLElement>('.ids-table__virtualized-container')
    if (!sc) {
      log(`    [scroll] FAIL: scroller element gone`)
      return null
    }

    // Quick check — row may already be rendered
    let row = findRowByKey(target)
    if (row) {
      log(`    [scroll] quick-found in current view, centering`)
      row.scrollIntoView({ block: 'center', behavior: 'auto' })
      await wait(200)
      const re = findRowByKey(target)
      log(`    [scroll] post-center re-find: ${re ? 'OK' : 'lost (row detached)'}`)
      return re ?? row
    }

    const { scrollHeight, clientHeight } = sc
    log(`    [scroll] not in view (scrollTop=${sc.scrollTop}, scrollHeight=${scrollHeight}, clientHeight=${clientHeight}) — jumping`)

    // Estimate target position from rowIndex. Row 2 is the first data row.
    // totalRows ≈ scrollHeight / avgRowHeight; avgRowHeight ≈ scrollHeight / rendered-row count.
    const renderedRows = document.querySelectorAll<HTMLElement>('.idsTable__row[aria-rowindex]')
    const maxIdx = renderedRows.length > 0
      ? Math.max(...Array.from(renderedRows).map(r => Number(r.getAttribute('aria-rowindex') ?? '0')))
      : 0
    const estimatedTotalRows = maxIdx > 2 ? maxIdx - 1 : 20
    const rowHeight = scrollHeight / estimatedTotalRows
    const targetIdx = Number(target.rowIndex) || 2
    const estimatedOffset = Math.max(0, (targetIdx - 2) * rowHeight - clientHeight / 2)

    // Try 4 jump positions: estimated, top, middle, bottom
    const jumps = [estimatedOffset, 0, scrollHeight / 2, scrollHeight]
    for (const pos of jumps) {
      sc.scrollTop = Math.min(scrollHeight, Math.max(0, pos))
      await wait(300)
      row = findRowByKey(target)
      if (row) {
        log(`    [scroll] found via jump to ${pos.toFixed(0)}, scrollTop=${sc.scrollTop}`)
        row.scrollIntoView({ block: 'center', behavior: 'auto' })
        await wait(200)
        return findRowByKey(target) ?? row
      }
    }

    // Fine-grained scan as last resort — scroll from top in small steps
    log(`    [scroll] jump failed, falling back to scan`)
    sc.scrollTop = 0
    await wait(250)
    const step = Math.max(100, Math.floor(clientHeight * 0.5))
    for (let attempt = 0; attempt < 50; attempt++) {
      row = findRowByKey(target)
      if (row) {
        log(`    [scroll] scan found at attempt ${attempt}, scrollTop=${sc.scrollTop}`)
        const rRect = row.getBoundingClientRect()
        const cRect = sc.getBoundingClientRect()
        sc.scrollTop += rRect.top - cRect.top - cRect.height / 2 + rRect.height / 2
        await wait(220)
        return findRowByKey(target) ?? row
      }
      if (sc.scrollTop + clientHeight >= scrollHeight - 5) {
        log(`    [scroll] scan reached bottom at attempt ${attempt} without finding`)
        break
      }
      sc.scrollTop = Math.min(scrollHeight, sc.scrollTop + step)
      await wait(200)
    }

    // Final diagnostic: log what rows ARE rendered so we can see why the match failed
    const rendered = Array.from(document.querySelectorAll<HTMLElement>('.idsTable__row[aria-rowindex]'))
      .map(r => `idx=${r.getAttribute('aria-rowindex')} date="${text(r.querySelector('.txnDate'))}" desc="${text(r.querySelector('.description-tooltip')).slice(0,20)}"`)
    log(`    [scroll] FAILED. Rendered rows (${rendered.length}): ${rendered.slice(0,8).join(' | ')}`)
    log(`    [scroll] target: idx=${target.rowIndex} date="${target.date}" desc="${target.description}" spent="${target.spent}" received="${target.received}"`)

    return null
  }

  async function harvestCurrentPage(page: number): Promise<RawRow[]> {
    const collected = new Map<string, RawRow>()

    function pass() {
      document.querySelectorAll<HTMLElement>('.idsTable__row[aria-rowindex]').forEach((row) => {
        const idx = row.getAttribute('aria-rowindex') ?? row.dataset.index ?? ''
        if (!idx || collected.has(idx)) return
        const parsed = parseRow(row, page)
        if (parsed) collected.set(idx, parsed)
      })
    }

    const sc = document.querySelector<HTMLElement>('.ids-table__virtualized-container')
    if (!sc) return []

    sc.scrollTop = 0
    await wait(300)
    pass()

    let pos = 250
    while (pos < sc.scrollHeight) {
      sc.scrollTop = pos
      await wait(180)
      pass()
      pos += 250
    }
    sc.scrollTop = sc.scrollHeight
    await wait(300)
    pass()
    sc.scrollTop = 0
    await wait(400)  // give virtualizer time to re-render top rows before clicking

    const sorted = [...collected.values()].sort((a, b) => Number(a.rowIndex) - Number(b.rowIndex))
    log(`page ${page}: harvested ${sorted.length} rows`)
    return sorted
  }

  // ── main loop with pagination ──────────────────────────────────────────────

  const autoMatched: PageResult['autoMatched'] = []
  const uncategorizedAll: PageResult['uncategorized'] = []
  const skippedAll: PageResult['skipped'] = []
  let totalRows = 0
  let pageNum = 1

  while (true) {
    const pageRows = await harvestCurrentPage(pageNum)
    totalRows += pageRows.length

    // Per-row classification log
    for (const r of pageRows) {
      log(
        `  row#${r.rowIndex} | ${r.date} | "${r.description || '(no desc)'}" | spent=${r.spent || '-'} received=${r.received || '-'} ` +
        `| action=${r.action} | sparkle=${r.hasSparkle} | single=${r.isSingleMatch} | multi=${r.isMultiMatch} | uncat=${r.isUncategorized} | autopost=${r.isAutoPost} ` +
        `| matchInfo="${r.matchInfo}" | category="${r.category}"`
      )
    }

    // ── Inner helper: process one auto-matchable row, return true if confirmed ──
    async function processOneRow(r: RawRow): Promise<boolean> {
      if (r.isSingleMatch) {
        log(`→ MATCH attempt for row#${r.rowIndex} (${r.date} ${r.description || r.matchInfo}) [action=${r.action || 'unknown'}]`)
        const liveRow = await findRowWithScroll(r)
        if (!liveRow) {
          log(`  ✗ row not found even after scrolling`)
          autoMatched.push({ rowIndex: r.rowIndex, date: r.date, description: r.description,
            amount: r.spent || r.received, matchInfo: r.matchInfo, success: false, error: 'row not found after scroll' })
          return false
        }
        const pendingBefore = getPendingCount()
        log(`  pending count before: ${pendingBefore ?? '?'}`)
        const clickResult = await activateAndClickMatch(liveRow)
        log(`  click via ${clickResult.method} (${clickResult.details})`)
        await wait(1800)
        const pendingAfter = getPendingCount()
        const stillFindable = findRowByKey(r) != null
        const countDropped = pendingBefore != null && pendingAfter != null && pendingAfter < pendingBefore
        const realSuccess = countDropped || (!stillFindable && pendingBefore != null)
        log(`  verify · pendingAfter=${pendingAfter ?? '?'} · stillInDOM=${stillFindable} · MATCH ${realSuccess ? 'CONFIRMED' : 'NOT confirmed'}`)
        autoMatched.push({ rowIndex: r.rowIndex, date: r.date, description: r.description,
          amount: r.spent || r.received, matchInfo: r.matchInfo, success: realSuccess,
          method: clickResult.method,
          error: realSuccess ? undefined : `pending ${pendingBefore ?? '?'}→${pendingAfter ?? '?'}, row ${stillFindable ? 'still present' : 'gone but count unchanged'}` })
        return realSuccess
      }

      if (r.isMultiMatch) {
        log(`→ MULTI-MATCH attempt for row#${r.rowIndex} (${r.date} "${r.description || r.matchInfo}") matchInfo="${r.matchInfo}"`)
        const liveRow = await findRowWithScroll(r)
        if (!liveRow) {
          log(`  ✗ row not found even after scrolling`)
          autoMatched.push({ rowIndex: r.rowIndex, date: r.date, description: r.description,
            amount: r.spent || r.received, matchInfo: r.matchInfo, success: false, error: 'row not found after scroll' })
          return false
        }
        const pendingBefore = getPendingCount()
        log(`  pending count before: ${pendingBefore ?? '?'}`)
        const mmResult = await resolveMultiMatchOverlay(liveRow, r)
        log(`  ${mmResult.ok ? '✓' : '✗'} ${mmResult.method}: ${mmResult.details}`)
        await wait(500)
        const pendingAfter = getPendingCount()
        const stillFindable = findRowByKey(r) != null
        const countDropped = pendingBefore != null && pendingAfter != null && pendingAfter < pendingBefore
        const realSuccess = mmResult.ok && (countDropped || !stillFindable)
        log(`  verify · pendingAfter=${pendingAfter ?? '?'} · stillInDOM=${stillFindable} · MATCH ${realSuccess ? 'CONFIRMED' : 'NOT confirmed'}`)
        autoMatched.push({ rowIndex: r.rowIndex, date: r.date, description: r.description,
          amount: r.spent || r.received, matchInfo: r.matchInfo, success: realSuccess,
          method: mmResult.method, error: realSuccess ? undefined : mmResult.details })
        return realSuccess
      }

      if (r.isAutoPost) {
        log(`→ AUTO-POST attempt for row#${r.rowIndex} (${r.date} "${r.description}") category="${r.category}"`)
        const liveRow = await findRowWithScroll(r)
        if (!liveRow) {
          log(`  ✗ row not found after scrolling`)
          autoMatched.push({ rowIndex: r.rowIndex, date: r.date, description: r.description,
            amount: r.spent || r.received, matchInfo: r.matchInfo, success: false, error: 'row not found after scroll' })
          return false
        }
        const pendingBefore = getPendingCount()
        log(`  pending count before: ${pendingBefore ?? '?'}`)
        const postResult = await activateAndClickPost(liveRow)
        log(`  ${postResult.ok ? '●' : '✗'} ${postResult.method}: ${postResult.details}`)
        await wait(1000)
        const pendingAfter = getPendingCount()
        const stillFindable = findRowByKey(r) != null
        const countDropped = pendingBefore != null && pendingAfter != null && pendingAfter < pendingBefore
        const realSuccess = postResult.ok && (countDropped || !stillFindable)
        log(`  verify · pendingAfter=${pendingAfter ?? '?'} · stillInDOM=${stillFindable} · POST ${realSuccess ? 'CONFIRMED' : 'NOT confirmed'}`)
        autoMatched.push({ rowIndex: r.rowIndex, date: r.date, description: r.description,
          amount: r.spent || r.received, matchInfo: r.matchInfo, success: realSuccess,
          method: postResult.method, error: realSuccess ? undefined : postResult.details })
        return realSuccess
      }

      return false
    }

    // ── Main pass: process all rows from harvest ──────────────────────────────
    // Content key = stable identity used to track what's already been processed
    // (aria-rowindex is NOT stable — QBO re-indexes rows after each removal).
    const rowContentKey = (r: RawRow) => `${r.date}|${r.description}|${r.spent}|${r.received}`
    const confirmedKeys = new Set<string>()

    for (const r of pageRows) {
      if (r.isSingleMatch || r.isMultiMatch || r.isAutoPost) {
        const ok = await processOneRow(r)
        if (ok) confirmedKeys.add(rowContentKey(r))
      } else if (r.isUncategorized) {
        uncategorizedAll.push({ rowIndex: r.rowIndex, page: r.page, date: r.date,
          description: r.description, spent: r.spent, received: r.received, type: r.type,
          fromTo: r.fromTo, category: r.category, matchInfo: r.matchInfo, action: r.action })
      } else {
        skippedAll.push({ rowIndex: r.rowIndex, date: r.date, description: r.description,
          reason: r.action === 'Match' ? 'no AI suggestion' : 'other' })
      }
    }

    // ── Retry passes: re-harvest and process anything still processable ───────
    for (let retryPass = 1; retryPass <= 2; retryPass++) {
      await wait(700)
      const retryRows = await harvestCurrentPage(pageNum)
      const toRetry = retryRows.filter(r =>
        (r.isSingleMatch || r.isMultiMatch || r.isAutoPost) &&
        !confirmedKeys.has(rowContentKey(r))
      )
      if (toRetry.length === 0) {
        log(`[retry ${retryPass}] no remaining processable rows`)
        break
      }
      log(`[retry ${retryPass}] ${toRetry.length} rows still processable — retrying`)
      let retrySucceeded = 0
      for (const r of toRetry) {
        const ok = await processOneRow(r)
        if (ok) { confirmedKeys.add(rowContentKey(r)); retrySucceeded++ }
      }
      log(`[retry ${retryPass}] ${retrySucceeded}/${toRetry.length} confirmed`)
      if (retrySucceeded === 0) break  // no progress — stop to avoid loop
    }

    // Pagination
    const nav = document.querySelector('[data-testid="pagination"]')
    if (!nav) break
    const buttons = nav.querySelectorAll<HTMLButtonElement>('button')
    const nextBtn = buttons[buttons.length - 1]
    const isDisabled = !nextBtn || nextBtn.disabled || nextBtn.classList.contains('IconControl-disabled-8b7fde6')
    if (isDisabled) break
    nextBtn.click()
    await wait(1500)
    pageNum++
    if (pageNum > 20) break
  }

  log(`DONE · matched=${autoMatched.filter(m => m.success).length}/${autoMatched.length} · uncategorized=${uncategorizedAll.length} · skipped=${skippedAll.length}`)

  return {
    loggedIn: true,
    url: location.href,
    realmId,
    totalRows,
    autoMatched,
    uncategorized: uncategorizedAll,
    skipped: skippedAll,
    logs,
  }
}

async function applyCategoriesInPage(
  suggestions: {
    rowIndex: string
    suggestedCategory: string
    categoryId: string | null
    // Stable per-row identifiers from the original scrape — used to find the
    // row even if QBO has re-rendered and reshuffled aria-rowindex (which
    // happens after the Match auto-click pass at the start of the run).
    date: string
    description: string
    spent: string
    received: string
  }[]
): Promise<{
  rowIndex: string
  status: 'applied' | 'failed'
  suggestedCategory: string
  categoryId: string | null
  error: string | null
}[]> {
  const pageWindow = window as Window & typeof globalThis & { __qboCategorizerStop?: boolean }
  const assertNotStopped = () => {
    if (pageWindow.__qboCategorizerStop) throw new Error('QBO_CATEGORIZER_STOPPED')
  }
  const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))
  const wait = async (ms: number) => {
    const started = Date.now()
    while (Date.now() - started < ms) {
      assertNotStopped()
      await sleep(Math.min(250, ms - (Date.now() - started)))
    }
    assertNotStopped()
  }
  const text = (el: Element | null | undefined) => el?.textContent?.trim() ?? ''

  function findRowByRowIndex(rowIndex: string): HTMLElement | null {
    const rows = document.querySelectorAll<HTMLElement>('#table-body-main tbody .idsTable__row')
    for (const row of Array.from(rows)) {
      const idx = row.getAttribute('aria-rowindex') ?? row.dataset.index ?? ''
      if (idx === rowIndex) return row
    }
    return null
  }

  function findRowByContent(target: {
    date: string
    description: string
    spent: string
    received: string
  }): HTMLElement | null {
    const norm = (s: string) => s.replace(/\s+/g, '')
    const rows = document.querySelectorAll<HTMLElement>('#table-body-main tbody .idsTable__row')
    for (const row of Array.from(rows)) {
      const date = (row.querySelector('.txnDate')?.textContent ?? '').trim()
      const desc = (row.querySelector('.description-tooltip')?.textContent ?? '').trim()
      const spent = norm(row.querySelector('.spent .pull-right')?.textContent ?? '')
      const received = norm(row.querySelector('.received-amount')?.textContent ?? '')
      if (
        date === target.date &&
        desc === target.description &&
        spent === norm(target.spent) &&
        received === norm(target.received)
      ) {
        return row
      }
    }
    return null
  }

  async function ensureRowVisible(
    rowIndex: string,
    content?: { date: string; description: string; spent: string; received: string },
  ): Promise<HTMLElement | null> {
    const sc = document.querySelector<HTMLElement>('.ids-table__virtualized-container')
    if (!sc) return null

    // Lookup helper that prefers content-match (stable across re-renders) but
    // falls back to aria-rowindex when content isn't provided.
    const lookup = (): HTMLElement | null =>
      (content ? findRowByContent(content) : null) ?? findRowByRowIndex(rowIndex)

    let row = lookup()
    if (row) {
      row.scrollIntoView({ block: 'center', behavior: 'auto' })
      await wait(180)
      return lookup() ?? row
    }
    sc.scrollTop = 0
    await wait(220)
    const step = Math.max(120, Math.floor(sc.clientHeight * 0.6))
    for (let attempt = 0; attempt < 40; attempt++) {
      row = lookup()
      if (row) {
        row.scrollIntoView({ block: 'center', behavior: 'auto' })
        await wait(220)
        return lookup() ?? row
      }
      const atBottom = sc.scrollTop + sc.clientHeight >= sc.scrollHeight - 5
      if (atBottom) break
      sc.scrollTop = Math.min(sc.scrollHeight, sc.scrollTop + step)
      await wait(180)
    }
    return null
  }

  // QBO redesign-enabled-qf UI has two states for the category cell:
  //   1. Phantom: <button class="QuickfillsPhantom qbo-quickfills-ui-account"
  //              aria-label="Uncategorized Income">...</button>
  //   2. Hydrated: <div class="QuickfillsContainer" data-props='{"placeholder":"Uncategorized Income",...}'>
  //              with an <input aria-label="Select category">
  function phantomCategoryButton(row: HTMLElement): HTMLButtonElement | null {
    return row.querySelector<HTMLButtonElement>(
      '.category .QuickfillsPhantom.qbo-quickfills-ui-account',
    )
  }

  function hydratedCategoryContainer(row: HTMLElement): HTMLElement | null {
    return row.querySelector<HTMLElement>('.category .QuickfillsContainer[data-props]')
  }

  function readPlaceholderFromDataProps(container: HTMLElement): string {
    const raw = container.getAttribute('data-props')
    if (!raw) return ''
    try {
      const parsed = JSON.parse(raw)
      return typeof parsed?.placeholder === 'string' ? parsed.placeholder : ''
    } catch {
      return ''
    }
  }

  // The category cell goes through three DOM states:
  //   1. Phantom (uncategorized):   .QuickfillsPhantom.qbo-quickfills-ui-account
  //   2. Hydrated typeahead-open:   .QuickfillsContainer[data-props]
  //   3. Categorized (post-click):  bare .qbo-quickfills-ui-account (no Phantom)
  function bareCategoryButton(row: HTMLElement): HTMLButtonElement | null {
    return row.querySelector<HTMLButtonElement>(
      '.category .qbo-quickfills-ui-account:not(.QuickfillsPhantom)',
    )
  }

  function isUncategorized(row: HTMLElement): boolean {
    const phantom = phantomCategoryButton(row)
    if (phantom) {
      const label = phantom.getAttribute('aria-label') ?? ''
      return /^Uncategorized\s+(Expense|Income)$/i.test(label)
    }
    const container = hydratedCategoryContainer(row)
    if (container) {
      const placeholder = readPlaceholderFromDataProps(container)
      return /^Uncategorized\s+(Expense|Income)$/i.test(placeholder)
    }
    const bare = bareCategoryButton(row)
    if (bare) {
      const label = bare.getAttribute('aria-label') ?? ''
      return /^Uncategorized\s+(Expense|Income)$/i.test(label)
    }
    return false
  }

  // Returns the row's current category name. Empty string if cell shows
  // "Uncategorized …" or is unreadable. Used to distinguish QBO's side-effect
  // categorization (auto-applied to similar-vendor rows) from a real failure.
  function readCurrentCategory(row: HTMLElement): string {
    const phantom = phantomCategoryButton(row)
    if (phantom) {
      const label = phantom.getAttribute('aria-label') ?? ''
      return /^Uncategorized\s+(Expense|Income)$/i.test(label) ? '' : label
    }
    const container = hydratedCategoryContainer(row)
    if (container) {
      const placeholder = readPlaceholderFromDataProps(container)
      if (placeholder && !/^Uncategorized\s+(Expense|Income)$/i.test(placeholder)) {
        return placeholder
      }
    }
    const bare = bareCategoryButton(row)
    if (bare) {
      const label = bare.getAttribute('aria-label') ?? ''
      if (label && !/^Uncategorized\s+(Expense|Income)$/i.test(label)) return label
      const txt = bare.textContent?.trim() ?? ''
      if (txt && !/^Uncategorized\s+(Expense|Income)$/i.test(txt)) return txt
    }
    const input = row.querySelector<HTMLInputElement>(
      '.category input[aria-label="Select category"]',
    )
    if (input?.value) return input.value.trim()
    return ''
  }

  async function openDropdown(row: HTMLElement): Promise<HTMLElement | null> {
    // Step 1: if phantom button is present, click to hydrate into the typeahead.
    const phantom = phantomCategoryButton(row)
    if (phantom) {
      phantom.click()
      await wait(700)
    }

    // Step 2: find the typeahead input.
    let input: HTMLInputElement | null =
      row.querySelector('input[aria-label="Select category"]') as HTMLInputElement | null
    for (let i = 0; i < 10 && !input; i++) {
      await wait(120)
      input =
        (row.querySelector('input[aria-label="Select category"]') as HTMLInputElement | null) ??
        (document.querySelector('input[aria-label="Select category"]') as HTMLInputElement | null)
    }
    if (!input) return null

    // Step 3: click + focus opens the listbox (input event alone does nothing).
    input.focus()
    input.click()

    // Step 4: wait for the listbox to render.
    for (let i = 0; i < 20; i++) {
      const panel = document.querySelector<HTMLElement>('ul[role="listbox"]')
      if (panel && panel.offsetParent !== null) return panel
      await wait(120)
    }
    return null
  }

  async function selectCategoryOption(
    panel: HTMLElement,
    suggestion: { suggestedCategory: string; categoryId: string | null },
  ): Promise<boolean> {
    const targetLower = suggestion.suggestedCategory.trim().toLowerCase()
    const normalize = (s: string) =>
      s.toLowerCase().replace(/\s+/g, ' ').trim()
    const targetNorm = normalize(suggestion.suggestedCategory)
    const options = panel.querySelectorAll<HTMLElement>(
      'li[role="option"].quickfillMenuItem',
    )

    // Pass 1: exact leaf-span match (most reliable).
    for (const opt of Array.from(options)) {
      for (const span of Array.from(opt.querySelectorAll<HTMLElement>('span'))) {
        if (span.children.length > 0) continue
        const txt = normalize(span.textContent ?? '')
        if (txt === targetNorm || txt.toLowerCase() === targetLower) {
          opt.click()
          return true
        }
      }
    }

    // Pass 2: case-insensitive leaf-span "starts with" — QBO sometimes wraps
    // names with type/parent suffixes inside a single span depending on locale
    // (e.g., "Office Expenses & Supplies (Expense)").
    for (const opt of Array.from(options)) {
      for (const span of Array.from(opt.querySelectorAll<HTMLElement>('span'))) {
        if (span.children.length > 0) continue
        const txt = normalize(span.textContent ?? '')
        if (txt.startsWith(targetNorm)) {
          opt.click()
          return true
        }
      }
    }

    // Pass 3: any element inside the LI whose direct text equals the target.
    for (const opt of Array.from(options)) {
      for (const el of Array.from(opt.querySelectorAll<HTMLElement>('*'))) {
        if (el.children.length > 0) continue
        const txt = normalize(el.textContent ?? '')
        if (txt === targetNorm) {
          opt.click()
          return true
        }
      }
    }

    return false
  }

  async function verifySaved(row: HTMLElement): Promise<boolean> {
    // QBO can take 3-4s to confirm a category change in the For Review row,
    // especially on larger COAs. Poll for up to 6s.
    for (let i = 0; i < 50; i++) {
      if (!isUncategorized(row)) return true
      await wait(120)
    }
    return false
  }

  function mkPointerOpts(el: HTMLElement): MouseEventInit & PointerEventInit {
    const r = el.getBoundingClientRect()
    return {
      bubbles: true,
      cancelable: true,
      view: window,
      button: 0,
      buttons: 1,
      clientX: r.left + r.width / 2,
      clientY: r.top + r.height / 2,
      composed: true,
      pointerType: 'mouse',
      pointerId: 1,
      isPrimary: true,
    }
  }

  function fireHoverChain(el: HTMLElement) {
    const o = mkPointerOpts(el)
    el.dispatchEvent(new PointerEvent('pointerover', o))
    el.dispatchEvent(new MouseEvent('mouseover', o))
    el.dispatchEvent(new PointerEvent('pointerenter', o))
    el.dispatchEvent(new MouseEvent('mouseenter', o))
    el.dispatchEvent(new PointerEvent('pointermove', o))
    el.dispatchEvent(new MouseEvent('mousemove', o))
  }

  // QBO's "Post" action button lives in the same action cell as Match.
  // Two UI variants — old phantom (.action-phantom.post-action) and new
  // ComboLink (.idsLinkActionButton with text "Post").
  function findPostBtn(cell: HTMLElement): HTMLButtonElement | null {
    const phantom =
      cell.querySelector<HTMLButtonElement>('.post-action:not(.action-phantom)') ??
      cell.querySelector<HTMLButtonElement>('button.post-action') ??
      cell.querySelector<HTMLButtonElement>('.post-action')
    if (phantom) return phantom
    const linkBtns = cell.querySelectorAll<HTMLButtonElement>('.idsLinkActionButton')
    for (const b of Array.from(linkBtns)) {
      if (/^Post$/i.test((b.textContent || '').trim())) return b
    }
    return null
  }

  // Click the row's Post action. Returns true if the row disappears from the
  // table (= QBO accepted the post), false otherwise.
  async function clickPostAndVerify(
    row: HTMLElement,
    content?: { date: string; description: string; spent: string; received: string },
  ): Promise<{ ok: boolean; error: string | null }> {
    const cell = row.querySelector<HTMLElement>('.idsTable__cell.action')
    if (!cell) return { ok: false, error: 'no action cell' }

    let btn = findPostBtn(cell)
    if (!btn) {
      // Phantom buttons hydrate on row hover. Wake it up and re-find.
      fireHoverChain(row)
      fireHoverChain(cell)
      for (let i = 0; i < 8; i++) {
        await wait(120)
        btn = findPostBtn(cell)
        if (btn) break
      }
    }
    if (!btn) return { ok: false, error: 'post button not found' }

    const isPhantom = btn.classList.contains('action-phantom')
    if (isPhantom) {
      // Old phantom UI: hover hand-off swaps the DOM element. Re-find after.
      fireHoverChain(row)
      fireHoverChain(cell)
      fireHoverChain(btn)
      for (let i = 0; i < 8; i++) {
        await wait(80)
        const fresh = findPostBtn(cell)
        if (fresh && fresh.isConnected && !fresh.classList.contains('action-phantom')) {
          btn = fresh
          break
        }
        if (fresh && fresh.isConnected) btn = fresh
      }
      if (!btn || !btn.isConnected) return { ok: false, error: 'phantom post button vanished' }
    }

    // Try clicking up to twice. After the first click, poll for ~5s. If the
    // row hasn't gone, hover + click again. Catches the case where the first
    // click landed on a stale phantom element.
    const fireClick = (target: HTMLButtonElement) => {
      const o = mkPointerOpts(target)
      target.dispatchEvent(new PointerEvent('pointerdown', o))
      target.dispatchEvent(new MouseEvent('mousedown', o))
      target.dispatchEvent(new PointerEvent('pointerup', o))
      target.dispatchEvent(new MouseEvent('mouseup', o))
      target.dispatchEvent(new MouseEvent('click', o))
      try { target.click() } catch {}
    }

    const isGone = (): boolean => {
      if (!row.isConnected) return true
      if (content && !findRowByContent(content)) return true
      return false
    }

    fireClick(btn)
    for (let i = 0; i < 40; i++) {
      await wait(125)
      if (isGone()) return { ok: true, error: null }
    }

    // First attempt didn't confirm — hover the row again and retry the click.
    const rowAfter =
      (content ? findRowByContent(content) : null) ?? (row.isConnected ? row : null)
    if (!rowAfter) return { ok: true, error: null } // row gone but not detected above
    const cellAfter = rowAfter.querySelector<HTMLElement>('.idsTable__cell.action')
    if (cellAfter) {
      fireHoverChain(rowAfter)
      fireHoverChain(cellAfter)
      await wait(200)
      const btn2 = findPostBtn(cellAfter)
      if (btn2) {
        fireClick(btn2)
        for (let i = 0; i < 40; i++) {
          await wait(125)
          if (isGone()) return { ok: true, error: null }
        }
      }
    }
    return { ok: false, error: 'post not confirmed after retry' }
  }

  // Click-outside to dismiss any open listbox so the next row's interaction starts clean.
  // Waits up to 1.5s for [role="listbox"] to disappear from the document.
  async function waitForListboxClosed(): Promise<void> {
    // Best-effort: blur active element and click on a benign area.
    try {
      ;(document.activeElement as HTMLElement | null)?.blur?.()
      document.body.click()
    } catch {
      // ignore
    }
    for (let i = 0; i < 15; i++) {
      const panel = document.querySelector<HTMLElement>('ul[role="listbox"]')
      if (!panel || panel.offsetParent === null) return
      await wait(100)
    }
  }

  const events: {
    rowIndex: string
    status: 'applied' | 'failed'
    suggestedCategory: string
    categoryId: string | null
    error: string | null
  }[] = []

  for (const s of suggestions) {
    assertNotStopped()

    const content = { date: s.date, description: s.description, spent: s.spent, received: s.received }

    let row = await ensureRowVisible(s.rowIndex, content)
    if (!row) {
      events.push({ rowIndex: s.rowIndex, status: 'failed', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'row not found' })
      continue
    }

    // Two independent steps: (1) set the category if needed; (2) click Post.
    // Both must happen even when QBO auto-applied the right category to a
    // sibling row — otherwise the row gets the category but stays in Pending.
    let categoryNote: string | null = null

    if (isUncategorized(row)) {
      // We need to set the category ourselves.
      const panel = await openDropdown(row)
      if (!panel) {
        events.push({ rowIndex: s.rowIndex, status: 'failed', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'dropdown did not open' })
        continue
      }
      const picked = await selectCategoryOption(panel, s)
      if (!picked) {
        await waitForListboxClosed()
        events.push({ rowIndex: s.rowIndex, status: 'failed', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'option not found' })
        continue
      }
      row = await ensureRowVisible(s.rowIndex, content) ?? row
      const ok = await verifySaved(row)
      if (!ok) {
        events.push({ rowIndex: s.rowIndex, status: 'failed', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'save not confirmed' })
        await waitForListboxClosed()
        continue
      }
      await waitForListboxClosed()
    } else {
      // Row already has a category. Three sub-cases:
      //   a) Matches our suggestion (QBO auto-applied from a sibling)
      //   b) Different but valid category (QBO's vendor-pattern guess) → still
      //      post it; the row has a reasonable category, just not ours
      //   c) Unreadable category → skip (we can't tell what's there)
      const current = readCurrentCategory(row)
      if (!current) {
        events.push({
          rowIndex: s.rowIndex,
          status: 'failed',
          suggestedCategory: s.suggestedCategory,
          categoryId: s.categoryId,
          error: 'row already categorized but unreadable',
        })
        continue
      }
      const matches = current.trim().toLowerCase() === s.suggestedCategory.trim().toLowerCase()
      categoryNote = matches
        ? 'auto-applied by QBO'
        : `pre-categorized as "${current}" (our suggestion was "${s.suggestedCategory}") — posting anyway`
    }

    // Always click Post — whether we set the category or QBO did.
    row = await ensureRowVisible(s.rowIndex, content) ?? row
    const post = await clickPostAndVerify(row, content)
    if (post.ok) {
      events.push({
        rowIndex: s.rowIndex,
        status: 'applied',
        suggestedCategory: s.suggestedCategory,
        categoryId: s.categoryId,
        error: categoryNote,
      })
    } else {
      events.push({
        rowIndex: s.rowIndex,
        status: 'applied',
        suggestedCategory: s.suggestedCategory,
        categoryId: s.categoryId,
        error: categoryNote
          ? `${categoryNote}; post failed: ${post.error}`
          : `category set, post failed: ${post.error}`,
      })
    }
  }

  return events
}

// ── Job runner ────────────────────────────────────────────────────────────────

async function runJob(trigger: 'manual' | 'scheduled'): Promise<JobResult> {
  const start = Date.now()
  if (activeRun) return makeInProgressResult(trigger)

  const run: ActiveRun = {
    id: `${start}-${Math.random().toString(36).slice(2)}`,
    startedAt: start,
    trigger,
    stopRequested: false,
  }
  activeRun = run
  await markRunning(run)

  try {
  const settings = await getSettings()
  const focus = trigger === 'manual'
  const target = getQboTarget(settings)

  const tab = await openOrFocusQBO(focus, settings)
  if (!tab?.id) return finalize({ start, trigger, error: `Could not open ${target.label} tab` })
  run.tabId = tab.id
  await markRunning(run)

  if (run.stopRequested) return finalize({ start, trigger, error: 'Stopped', backendStatus: 'stopped' })

  await waitForTabComplete(tab.id)
  await new Promise((r) => setTimeout(r, 2500))  // SPA settle
  if (run.stopRequested) return finalize({ start, trigger, error: 'Stopped', backendStatus: 'stopped' })

  const loadedTab = await chrome.tabs.get(tab.id)
  if (!loadedTab.url?.includes('/app/banking')) {
    return finalize({
      start,
      trigger,
      error: 'Not logged in',
      details: {
        loggedIn: false,
        url: loadedTab.url ?? '',
        realmId: null,
        totalRows: 0,
        autoMatched: [],
        uncategorized: [],
        skipped: [],
        logs: [`Opened ${target.label}, but QuickBooks redirected away from /app/banking. Log in and run again.`],
      },
    })
  }

  let pageResult: PageResult | undefined
  try {
    const [r] = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      func: inPageWorker,
    })
    pageResult = r?.result as PageResult | undefined
  } catch (e) {
    const message = (e as Error).message
    if (run.stopRequested || message.includes('QBO_CATEGORIZER_STOPPED')) {
      return finalize({ start, trigger, error: 'Stopped', backendStatus: 'stopped' })
    }
    return finalize({ start, trigger, error: `Script injection failed: ${message}` })
  }

  if (run.stopRequested) return finalize({ start, trigger, error: 'Stopped', backendStatus: 'stopped' })

  if (!pageResult) {
    return finalize({ start, trigger, error: 'No result from page' })
  }

  // Mirror the in-page logs into the SW console for easy debugging
  console.group(`%c[QBO Run · ${trigger}]`, 'color:#2ca01c;font-weight:bold')
  ;(pageResult.logs ?? []).forEach((line) => console.log(line))
  console.groupEnd()

  if (!pageResult.loggedIn) {
    setBadge('!', '#e55353')
    notify(
      'QBO not logged in',
      'Please log into QuickBooks Online to run the categorizer.',
      true
    )
    return finalize({
      start,
      trigger,
      error: 'Not logged in',
      details: pageResult,
    })
  }

  // Resolve clinic from realmId (extension-side cache + /clinics/by-realm/:realmId)
  const resolved = await resolveClinic(
    pageResult.realmId,
    settings.apiBaseUrl,
    settings.extensionApiKey,
  )

  let suggestions: Suggestion[] = []
  let backendStatus = 'no uncategorized rows to send'
  let uncategorizedSent = 0

  if (pageResult.uncategorized.length > 0) {
    if (run.stopRequested) return finalize({ start, trigger, error: 'Stopped', details: pageResult, backendStatus: 'stopped' })
    if (!resolved) {
      backendStatus = pageResult.realmId
        ? 'No BookKeep clinic matches this QBO company. Add it in BookKeep first.'
        : 'realmId not found in QBO tab — cannot match to a clinic.'
    } else {
      try {
        const res = await fetch(
          `${settings.apiBaseUrl}/llm/qbo-suggest?clinic_slug=${encodeURIComponent(resolved.clinicSlug)}`,
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'X-Extension-Api-Key': settings.extensionApiKey,
            },
            body: JSON.stringify({
              clinic_slug: resolved.clinicSlug,
              source: 'qbo-extractor',
              qboEnvironment: settings.qboEnvironment,
              trigger,
              transactions: pageResult.uncategorized,
            }),
          },
        )
        if (res.ok) {
          const json = await res.json()
          suggestions = (json.suggestions ?? []) as Suggestion[]
          uncategorizedSent = pageResult.uncategorized.length
          backendStatus = `OK — ${uncategorizedSent} sent · ${suggestions.filter((s) => s.accepted).length} accepted`
        } else {
          backendStatus = `Backend HTTP ${res.status}`
        }
      } catch (e) {
        backendStatus = `Backend error: ${(e as Error).message}`
      }
    }
  }

  // ── Apply-back: drive QBO category dropdown for accepted suggestions ─────────
  let appliedCount = 0
  let applyFailedCount = 0
  if (resolved && suggestions.length > 0) {
    const accepted = suggestions.filter((s) => s.accepted)
    if (accepted.length > 0 && !run.stopRequested && tab.id != null) {
      // Enrich each accepted suggestion with the row content scraped earlier so
      // applyCategoriesInPage can find the row by stable attributes (date +
      // description + amounts) even after the Match auto-click pass has
      // renumbered aria-rowindex values.
      const enrich = (s: typeof accepted[number]) => {
        const orig = pageResult.uncategorized.find((u) => u.rowIndex === s.rowIndex)
        return {
          rowIndex: s.rowIndex,
          suggestedCategory: s.suggestedCategory,
          categoryId: s.categoryId,
          date: orig?.date ?? '',
          description: orig?.description ?? '',
          spent: orig?.spent ?? '',
          received: orig?.received ?? '',
        }
      }

      let events: ApplyEvent[] = []
      try {
        const [r1] = await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          func: applyCategoriesInPage,
          args: [accepted.map(enrich)],
        })
        const firstPass = (r1?.result ?? []) as Omit<ApplyEvent, 'confidence' | 'source' | 'qboTransactionId'>[]
        events = firstPass.map((e) => {
          const s = accepted.find((x) => x.rowIndex === e.rowIndex)!
          return { ...e, confidence: s.confidence, source: s.source, qboTransactionId: null }
        })

        // Retry-once for failures
        const failures = events.filter((e) => e.status === 'failed')
        if (failures.length > 0 && !run.stopRequested) {
          await new Promise((r) => setTimeout(r, 500))
          const retryEnriched = failures.map((e) => {
            const orig = accepted.find((a) => a.rowIndex === e.rowIndex)
            return enrich(orig!)
          })
          const [r2] = await chrome.scripting.executeScript({
            target: { tabId: tab.id },
            func: applyCategoriesInPage,
            args: [retryEnriched],
          })
          const retryPass = (r2?.result ?? []) as Omit<ApplyEvent, 'confidence' | 'source' | 'qboTransactionId'>[]
          for (const retry of retryPass) {
            const slot = events.findIndex((e) => e.rowIndex === retry.rowIndex)
            if (slot >= 0) {
              const s = accepted.find((x) => x.rowIndex === retry.rowIndex)!
              events[slot] = { ...retry, confidence: s.confidence, source: s.source, qboTransactionId: null }
            }
          }
        }
      } catch (e) {
        const msg = (e as Error).message
        if (run.stopRequested || msg.includes('QBO_CATEGORIZER_STOPPED')) {
          backendStatus = `${backendStatus} · apply stopped`
        } else {
          backendStatus = `${backendStatus} · apply error: ${msg}`
        }
      }

      appliedCount = events.filter((e) => e.status === 'applied').length
      applyFailedCount = events.filter((e) => e.status === 'failed').length

      // POST audit log
      if (events.length > 0) {
        try {
          await fetch(
            `${settings.apiBaseUrl}/llm/qbo-applied?clinic_slug=${encodeURIComponent(resolved.clinicSlug)}`,
            {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'X-Extension-Api-Key': settings.extensionApiKey,
              },
              body: JSON.stringify({ clinic_slug: resolved.clinicSlug, events }),
            },
          )
        } catch (e) {
          backendStatus = `${backendStatus} · audit POST failed: ${(e as Error).message}`
        }
      }
    }
  }

  setBadge('')

  const okMatched = pageResult.autoMatched.filter((m) => m.success).length
  notify(
    'QBO sync complete',
    `${okMatched} matched · ${appliedCount} auto-applied · ${applyFailedCount} apply-failed · ${pageResult.skipped.length} skipped`,
  )

  return finalize({
    start,
    trigger,
    error: null,
    details: pageResult,
    backendStatus,
    uncategorizedSent,
    appliedCount,
    applyFailedCount,
  })
  } finally {
    if (activeRun?.id === run.id) activeRun = null
    await clearRunState()
    setBadge('')
    await chrome.action.setTitle({ title: 'QBO Categorizer' })
  }
}

function finalize({
  start,
  trigger,
  error,
  details,
  backendStatus,
  uncategorizedSent,
  appliedCount,
  applyFailedCount,
}: {
  start: number
  trigger: 'manual' | 'scheduled'
  error: string | null
  details?: PageResult
  backendStatus?: string
  uncategorizedSent?: number
  appliedCount?: number
  applyFailedCount?: number
}): JobResult {
  const result: JobResult = {
    ok: !error,
    trigger,
    timestamp: start,
    duration: Date.now() - start,
    error,
    loggedIn: details?.loggedIn ?? false,
    totalRows: details?.totalRows ?? 0,
    matchedCount: details?.autoMatched.filter((m) => m.success).length ?? 0,
    uncategorizedSent: uncategorizedSent ?? 0,
    skippedCount: details?.skipped.length ?? 0,
    appliedCount: appliedCount ?? 0,
    applyFailedCount: applyFailedCount ?? 0,
    backendStatus: backendStatus ?? (error ?? 'not run'),
    details,
  }
  chrome.storage.local.set({ lastRun: result })
  return result
}

// ── Wire up listeners ────────────────────────────────────────────────────────

chrome.runtime.onInstalled.addListener(async () => {
  await clearRunState()
  const settings = await getSettings()
  await rescheduleAlarm(settings)
})

chrome.runtime.onStartup.addListener(async () => {
  await clearRunState()
  const settings = await getSettings()
  await rescheduleAlarm(settings)
})

chrome.alarms.onAlarm.addListener(async (alarm) => {
  if (alarm.name === ALARM_NAME) {
    const settings = await getSettings()
    if (settings.scheduleEnabled) {
      await runJob('scheduled')
    } else {
      await chrome.alarms.clear(ALARM_NAME)
    }
  }
})

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  ;(async () => {
    try {
      if (msg?.type === 'RUN_NOW') {
        const result = await runJob('manual')
        sendResponse({ ok: true, result })
      } else if (msg?.type === 'GET_SETTINGS') {
        sendResponse({ ok: true, settings: await getSettings() })
      } else if (msg?.type === 'SET_SETTINGS') {
        await setSettings(msg.settings)
        await rescheduleAlarm(msg.settings)
        sendResponse({ ok: true })
      } else if (msg?.type === 'GET_LAST_RUN') {
        const { lastRun, runState } = await chrome.storage.local.get(['lastRun', 'runState'])
        const alarms = await chrome.alarms.get(ALARM_NAME)
        sendResponse({ ok: true, lastRun, runState: runState ?? IDLE_RUN_STATE, nextRun: alarms?.scheduledTime ?? null })
      } else if (msg?.type === 'STOP_RUN') {
        const stopped = await requestStopRun()
        sendResponse({ ok: true, stopped })
      } else if (msg?.type === 'OPEN_QBO') {
        const settings = await getSettings()
        const tab = await openOrFocusQBO(true, settings)
        sendResponse({ ok: true, tabId: tab?.id })
      } else {
        sendResponse({ ok: false, error: 'unknown message' })
      }
    } catch (e) {
      sendResponse({ ok: false, error: (e as Error).message })
    }
  })()
  return true
})
