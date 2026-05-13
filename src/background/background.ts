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
    isUncategorized: boolean
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
      isUncategorized,
    }
  }

  function findRowByKey(target: RawRow): HTMLElement | null {
    const rows = document.querySelectorAll<HTMLElement>('#table-body-main tbody .idsTable__row')
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

  // Scrolls the virtualized container until the target row is rendered, then centers it
  async function findRowWithScroll(target: RawRow): Promise<HTMLElement | null> {
    const sc = document.querySelector<HTMLElement>('.ids-table__virtualized-container')
    if (!sc) {
      log(`    [scroll] FAIL: scroller element gone`)
      return null
    }

    // Quick check current view
    let row = findRowByKey(target)
    if (row) {
      log(`    [scroll] quick-found in current view, centering`)
      row.scrollIntoView({ block: 'center', behavior: 'auto' })
      await wait(200)
      const re = findRowByKey(target)
      log(`    [scroll] post-center re-find: ${re ? 'OK' : 'lost (row detached)'}`)
      return re ?? row  // fall back to original ref if re-find detaches
    }

    log(`    [scroll] not in view (scrollTop=${sc.scrollTop}, scrollHeight=${sc.scrollHeight}, clientHeight=${sc.clientHeight}) — scanning`)

    // Force scroll to top, wait for virtualizer to re-render
    sc.scrollTop = 0
    await wait(250)

    const step = Math.max(120, Math.floor(sc.clientHeight * 0.6))
    let attempt = 0

    while (attempt < 50) {
      row = findRowByKey(target)
      if (row) {
        log(`    [scroll] found at attempt ${attempt}, scrollTop=${sc.scrollTop}`)
        const rRect = row.getBoundingClientRect()
        const cRect = sc.getBoundingClientRect()
        const delta = rRect.top - cRect.top - cRect.height / 2 + rRect.height / 2
        sc.scrollTop += delta
        await wait(220)
        return findRowByKey(target) ?? row
      }

      const atBottom = sc.scrollTop + sc.clientHeight >= sc.scrollHeight - 5
      if (atBottom) {
        log(`    [scroll] reached bottom at attempt ${attempt} without finding — giving up`)
        break
      }

      sc.scrollTop = Math.min(sc.scrollHeight, sc.scrollTop + step)
      await wait(200)
      attempt++
    }

    return null
  }

  async function harvestCurrentPage(page: number): Promise<RawRow[]> {
    const collected = new Map<string, RawRow>()

    function pass() {
      document.querySelectorAll<HTMLElement>('#table-body-main tbody .idsTable__row').forEach((row) => {
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
        `| action=${r.action} | sparkle=${r.hasSparkle} | single=${r.isSingleMatch} | uncat=${r.isUncategorized} ` +
        `| matchInfo="${r.matchInfo}" | category="${r.category}"`
      )
    }

    // Auto-match single-suggestion AI rows
    for (const r of pageRows) {
      if (r.isSingleMatch) {
        log(`→ MATCH attempt for row#${r.rowIndex} (${r.date} ${r.description || r.matchInfo}) [action=${r.action || 'unknown'}]`)
        const liveRow = await findRowWithScroll(r)
        if (!liveRow) {
          log(`  ✗ row not found even after scrolling`)
          autoMatched.push({
            rowIndex: r.rowIndex,
            date: r.date,
            description: r.description,
            amount: r.spent || r.received,
            matchInfo: r.matchInfo,
            success: false,
            error: 'row not found after scroll',
          })
          continue
        }
        // Read pending count BEFORE attempting click
        const pendingBefore = getPendingCount()
        log(`  pending count before: ${pendingBefore ?? '?'}`)

        const clickResult = await activateAndClickMatch(liveRow)
        log(`  click via ${clickResult.method} (${clickResult.details})`)

        // Wait longer for QBO to apply the match server-side
        await wait(1800)

        // Verify two ways: pending count dropped, OR row disappeared from DOM by key
        const pendingAfter = getPendingCount()
        const stillFindable = findRowByKey(r) != null
        const countDropped =
          pendingBefore != null && pendingAfter != null && pendingAfter < pendingBefore
        const realSuccess = countDropped || (!stillFindable && pendingBefore != null)

        log(`  verify · pendingAfter=${pendingAfter ?? '?'} · stillInDOM=${stillFindable} · MATCH ${realSuccess ? 'CONFIRMED' : 'NOT confirmed'}`)

        autoMatched.push({
          rowIndex: r.rowIndex,
          date: r.date,
          description: r.description,
          amount: r.spent || r.received,
          matchInfo: r.matchInfo,
          success: realSuccess,
          method: clickResult.method,
          error: realSuccess
            ? undefined
            : `pending ${pendingBefore ?? '?'}→${pendingAfter ?? '?'}, row ${stillFindable ? 'still present' : 'gone but count unchanged'}`,
        })
      } else if (r.isUncategorized) {
        uncategorizedAll.push({
          rowIndex: r.rowIndex,
          page: r.page,
          date: r.date,
          description: r.description,
          spent: r.spent,
          received: r.received,
          type: r.type,
          fromTo: r.fromTo,
          category: r.category,
          matchInfo: r.matchInfo,
          action: r.action,
        })
      } else {
        let reason = 'other'
        if (r.action === 'Match' && r.hasSparkle && /Suggested matches found/i.test(r.matchInfo))
          reason = 'multi-match (skipped per safety policy)'
        else if (r.action === 'Match') reason = 'no AI suggestion'
        else if (r.action === 'Post' && !r.isUncategorized) reason = 'already categorized'
        skippedAll.push({
          rowIndex: r.rowIndex,
          date: r.date,
          description: r.description,
          reason,
        })
      }
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
  async function clickPostAndVerify(row: HTMLElement): Promise<{ ok: boolean; error: string | null }> {
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

    const o = mkPointerOpts(btn)
    btn.dispatchEvent(new PointerEvent('pointerdown', o))
    btn.dispatchEvent(new MouseEvent('mousedown', o))
    await wait(50)
    btn.dispatchEvent(new PointerEvent('pointerup', o))
    btn.dispatchEvent(new MouseEvent('mouseup', o))
    btn.dispatchEvent(new MouseEvent('click', o))
    try { btn.click() } catch {}

    // QBO removes the row from the Pending list once Post succeeds.
    // Poll for up to 4s for the row to detach.
    for (let i = 0; i < 32; i++) {
      await wait(125)
      if (!row.isConnected) return { ok: true, error: null }
    }
    return { ok: false, error: 'post not confirmed (row still present)' }
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

    // Re-resolve the row by aria-rowindex right before each operation. The previous
    // apply may have triggered a re-render, so a cached reference could be stale.
    let row = await ensureRowVisible(s.rowIndex, { date: s.date, description: s.description, spent: s.spent, received: s.received })
    if (!row) {
      events.push({ rowIndex: s.rowIndex, status: 'failed', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'row not found' })
      continue
    }
    if (!isUncategorized(row)) {
      // QBO may have already categorized this row (auto-suggested from a sibling
      // we just applied). Treat as success if the current category matches our
      // suggestion; otherwise log the actual disagreement.
      const current = readCurrentCategory(row)
      if (current && current.trim().toLowerCase() === s.suggestedCategory.trim().toLowerCase()) {
        events.push({ rowIndex: s.rowIndex, status: 'applied', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'auto-applied by QBO' })
      } else {
        events.push({
          rowIndex: s.rowIndex,
          status: 'failed',
          suggestedCategory: s.suggestedCategory,
          categoryId: s.categoryId,
          error: current ? `pre-categorized as "${current}"` : 'row already categorized',
        })
      }
      continue
    }

    const panel = await openDropdown(row)
    if (!panel) {
      events.push({ rowIndex: s.rowIndex, status: 'failed', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'dropdown did not open' })
      continue
    }

    const picked = await selectCategoryOption(panel, s)
    if (!picked) {
      // Close any leftover listbox before moving on.
      await waitForListboxClosed()
      events.push({ rowIndex: s.rowIndex, status: 'failed', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'option not found' })
      continue
    }

    // Re-resolve the row again — option click can detach the original element.
    row = await ensureRowVisible(s.rowIndex, { date: s.date, description: s.description, spent: s.spent, received: s.received }) ?? row
    const ok = await verifySaved(row)
    if (!ok) {
      events.push({ rowIndex: s.rowIndex, status: 'failed', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: 'save not confirmed' })
      await waitForListboxClosed()
      continue
    }

    // Close any open listbox before clicking Post.
    await waitForListboxClosed()

    // Re-resolve once more — the category save may have re-rendered the row.
    row = await ensureRowVisible(s.rowIndex, { date: s.date, description: s.description, spent: s.spent, received: s.received }) ?? row
    const post = await clickPostAndVerify(row)
    if (post.ok) {
      events.push({ rowIndex: s.rowIndex, status: 'applied', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: null })
    } else {
      // Category is set in QBO but the row wasn't posted. Still a partial success
      // — surface the post failure so the audit log captures it.
      events.push({ rowIndex: s.rowIndex, status: 'applied', suggestedCategory: s.suggestedCategory, categoryId: s.categoryId, error: `category set, post failed: ${post.error}` })
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
