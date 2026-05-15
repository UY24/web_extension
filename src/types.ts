export interface Transaction {
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
}

export interface MatchAttempt {
  rowIndex: string
  date: string
  description: string
  amount: string
  matchInfo: string
  success: boolean
  method?: string
  error?: string
}

export interface SkippedRow {
  rowIndex: string
  date: string
  description: string
  reason: string
}

export interface Suggestion {
  rowIndex: string
  suggestedCategory: string
  categoryId: string | null
  confidence: number
  source: 'vendor-match' | 'llm'
  accepted: boolean
}

export interface ApplyEvent {
  rowIndex: string
  status: 'applied' | 'failed'
  suggestedCategory: string
  categoryId: string | null
  confidence: number
  source: 'vendor-match' | 'llm'
  error: string | null
  qboTransactionId: string | null
}

export interface PageResult {
  loggedIn: boolean
  url: string
  realmId: string | null
  totalRows: number
  autoMatched: MatchAttempt[]
  uncategorized: Transaction[]
  skipped: SkippedRow[]
  logs: string[]
}

export interface JobResult {
  ok: boolean
  trigger: 'manual' | 'scheduled'
  timestamp: number
  duration: number
  error: string | null
  loggedIn: boolean
  totalRows: number
  matchedCount: number
  uncategorizedSent: number
  skippedCount: number
  appliedCount: number
  applyFailedCount: number
  backendStatus: string
  details?: PageResult
}

export interface RunState {
  running: boolean
  stopRequested: boolean
  startedAt: number | null
  trigger: 'manual' | 'scheduled' | null
  tabId?: number
}

export type QBOEnvironment = 'sandbox' | 'production'

export interface Settings {
  backendUrl: string
  apiBaseUrl: string
  extensionApiKey: string
  qboEnvironment: QBOEnvironment
  scheduleEnabled: boolean
  scheduleTime: string
}

// Defaults split by deployment target. Local dev points at localhost; production
// points at the deployed Vercel backend. The extensionApiKey default is the
// shared local-dev secret — works out of the box for local testing without
// touching the popup. For production, the user must set their own key via the
// popup (or the request will be rejected by the backend).
export const DEV_DEFAULTS = {
  apiBaseUrl: 'http://localhost:3001/api',
  backendUrl: 'http://localhost:3001/api/llm/qbo-categorize',
  extensionApiKey: 'b36991cadedd2bf95c71b4dee381d47f5cf61791dd2a44003b43edf26a74c98c',
} as const

export const PROD_DEFAULTS = {
  apiBaseUrl: 'https://abacus-be.vercel.app/api',
  backendUrl: 'https://abacus-be.vercel.app/api/llm/qbo-categorize',
  extensionApiKey: '',
} as const

export const DEFAULT_SETTINGS: Settings = {
  backendUrl: DEV_DEFAULTS.backendUrl,
  apiBaseUrl: DEV_DEFAULTS.apiBaseUrl,
  extensionApiKey: DEV_DEFAULTS.extensionApiKey,
  qboEnvironment: 'sandbox',
  scheduleEnabled: false,
  scheduleTime: '09:00',
}
