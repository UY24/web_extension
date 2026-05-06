const express = require('express')
const cors = require('cors')

const app = express()
const PORT = 3001

app.use(cors())
app.use(express.json({ limit: '5mb' }))

app.post('/transactions', (req, res) => {
  const { source, trigger, count, transactions } = req.body

  console.log(
    `\n[${new Date().toISOString()}] ${source} (${trigger ?? 'unknown'}) — ${count} uncategorized`
  )
  console.table(
    transactions.map(({ page, date, description, spent, received, type, fromTo, category, action }) => ({
      page, date, description, spent, received, type, fromTo, category, action,
    }))
  )

  res.json({ ok: true, received: count, processedAt: new Date().toISOString() })
})

app.get('/health', (_req, res) => res.json({ ok: true }))

app.listen(PORT, () => {
  console.log(`QBO backend listening on http://localhost:${PORT}`)
  console.log(`POST /transactions  — receives uncategorized rows only`)
})
