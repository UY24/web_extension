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

export interface PageResult {
  loggedIn: boolean
  url: string
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
  backendStatus: string
  details?: PageResult
}

export interface Settings {
  backendUrl: string
  scheduleEnabled: boolean
  scheduleTime: string  // "HH:MM"
}

export const DEFAULT_SETTINGS: Settings = {
  backendUrl: 'http://localhost:3001/api/llm/qbo-categorize',
  scheduleEnabled: false,
  scheduleTime: '09:00',
}
