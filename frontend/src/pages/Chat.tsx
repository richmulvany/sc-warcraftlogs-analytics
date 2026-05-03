import { useEffect, useRef, useState } from 'react'
import { ChevronDown, ChevronRight, Database, MessageCircleQuestion, Send } from 'lucide-react'
import clsx from 'clsx'
import { AppLayout } from '../components/layout/AppLayout'
import { Card, CardBody, CardHeader, CardTitle } from '../components/ui/Card'
import { ChatbotConfigError, postChat, type ChatResponse } from '../lib/chatbotClient'

interface Turn {
  id: string
  question: string
  pending: boolean
  response?: ChatResponse
  errorMessage?: string
  startedAt: number
  endedAt?: number
}

const SUGGESTIONS = [
  'Who dies most often on each boss?',
  'Which bosses are we wiping on most?',
  'Who has the highest Mythic+ score trend?',
  'Which players have the best parse percentiles on Mythic?',
  'What are our most common killing blows?',
]

function elapsedLabel(turn: Turn): string {
  const end = turn.endedAt ?? Date.now()
  const seconds = Math.max(0, (end - turn.startedAt) / 1000)
  return seconds < 1 ? `${(seconds * 1000).toFixed(0)} ms` : `${seconds.toFixed(1)} s`
}

function makeId(): string {
  return Math.random().toString(36).slice(2, 10)
}

export function Chat() {
  const [question, setQuestion] = useState('')
  const [turns, setTurns] = useState<Turn[]>([])
  const [busy, setBusy] = useState(false)
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [turns])

  useEffect(() => () => abortRef.current?.abort(), [])

  async function ask(text: string) {
    const trimmed = text.trim()
    if (!trimmed || busy) return
    const id = makeId()
    setTurns(prev => [
      ...prev,
      { id, question: trimmed, pending: true, startedAt: Date.now() },
    ])
    setQuestion('')
    setBusy(true)
    const controller = new AbortController()
    abortRef.current = controller
    try {
      const response = await postChat(trimmed, controller.signal)
      setTurns(prev => prev.map(t => (t.id === id ? { ...t, response, pending: false, endedAt: Date.now() } : t)))
    } catch (err) {
      const message = err instanceof ChatbotConfigError
        ? `${err.message} The backend usually runs at http://localhost:8000 — start it with \`uvicorn backend.app.main:app --reload\`.`
        : err instanceof Error
          ? err.message
          : String(err)
      setTurns(prev => prev.map(t => (t.id === id ? { ...t, errorMessage: message, pending: false, endedAt: Date.now() } : t)))
    } finally {
      setBusy(false)
      abortRef.current = null
    }
  }

  function handleSubmit(event: React.FormEvent) {
    event.preventDefault()
    ask(question)
  }

  function handleKeyDown(event: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault()
      ask(question)
    }
  }

  return (
    <AppLayout title="Ask" subtitle="Natural-language queries against the governed Gold layer" wide>
      <div className="flex flex-col h-full min-h-0">
        <div ref={scrollRef} className="flex-1 overflow-y-auto px-4 pt-4 pb-2 md:px-5 md:pt-5 space-y-4 min-h-0">
          {turns.length === 0 ? (
            <EmptyState onSuggest={ask} disabled={busy} />
          ) : (
            turns.map(turn => <TurnCard key={turn.id} turn={turn} />)
          )}
        </div>

        <form
          onSubmit={handleSubmit}
          className="flex-shrink-0 border-t border-ctp-surface0 bg-ctp-mantle/60 backdrop-blur px-4 py-3 md:px-5 md:py-4"
        >
          <div className="flex items-end gap-2 max-w-4xl mx-auto w-full">
            <textarea
              value={question}
              onChange={e => setQuestion(e.target.value)}
              onKeyDown={handleKeyDown}
              rows={1}
              placeholder="Ask about progression, parses, wipes, Mythic+, preparation…"
              className="flex-1 resize-none rounded-xl border border-ctp-surface1 bg-ctp-surface0/60 px-3 py-2.5 text-sm text-ctp-text placeholder:text-ctp-overlay0 focus:border-ctp-mauve focus:outline-none focus:ring-1 focus:ring-ctp-mauve min-h-[44px] max-h-40"
              disabled={busy}
            />
            <button
              type="submit"
              disabled={busy || !question.trim()}
              className={clsx(
                'flex items-center gap-1.5 rounded-xl border px-3 py-2 text-sm font-medium transition-colors',
                busy || !question.trim()
                  ? 'border-ctp-surface1 bg-ctp-surface0/40 text-ctp-overlay0 cursor-not-allowed'
                  : 'border-ctp-mauve/40 bg-ctp-mauve/15 text-ctp-mauve hover:bg-ctp-mauve/25'
              )}
            >
              <Send className="w-3.5 h-3.5" />
              <span className="hidden sm:inline">{busy ? 'Thinking…' : 'Ask'}</span>
            </button>
          </div>
          <p className="text-[10px] font-mono text-ctp-overlay0 mt-2 max-w-4xl mx-auto">
            Read-only. Queries run against <code>03_gold.sc_analytics</code>; only governed tables are reachable.
          </p>
        </form>
      </div>
    </AppLayout>
  )
}

function EmptyState({ onSuggest, disabled }: { onSuggest: (q: string) => void; disabled: boolean }) {
  return (
    <div className="max-w-3xl mx-auto py-8 md:py-12 space-y-6">
      <div className="flex items-center gap-3">
        <div className="w-9 h-9 rounded-xl bg-ctp-mauve/15 border border-ctp-mauve/30 flex items-center justify-center">
          <MessageCircleQuestion className="w-4 h-4 text-ctp-mauve" />
        </div>
        <div>
          <h2 className="text-base font-semibold text-ctp-text">Ask the analytics dataset</h2>
          <p className="text-xs text-ctp-overlay1 mt-0.5">
            Each answer cites the gold tables it consulted and the SQL it ran.
          </p>
        </div>
      </div>
      <Card>
        <CardHeader><CardTitle>Try one of these</CardTitle></CardHeader>
        <CardBody className="space-y-1.5">
          {SUGGESTIONS.map(s => (
            <button
              key={s}
              type="button"
              onClick={() => onSuggest(s)}
              disabled={disabled}
              className="w-full text-left text-sm text-ctp-subtext1 hover:text-ctp-text bg-ctp-surface0/40 hover:bg-ctp-surface0 rounded-lg px-3 py-2 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {s}
            </button>
          ))}
        </CardBody>
      </Card>
    </div>
  )
}

function TurnCard({ turn }: { turn: Turn }) {
  return (
    <div className="max-w-4xl mx-auto space-y-2">
      <div className="text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0">You</div>
      <div className="text-sm text-ctp-text bg-ctp-surface0/40 rounded-lg px-3 py-2 whitespace-pre-wrap">
        {turn.question}
      </div>

      <div className="text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0 pt-1">
        Assistant{turn.pending ? ' · thinking…' : ` · ${elapsedLabel(turn)}`}
      </div>

      {turn.pending ? (
        <Card>
          <CardBody>
            <div className="flex items-center gap-2 text-sm text-ctp-overlay1">
              <span className="w-1.5 h-1.5 rounded-full bg-ctp-mauve animate-pulse" />
              <span>Selecting tables, generating SQL, executing…</span>
            </div>
          </CardBody>
        </Card>
      ) : turn.errorMessage ? (
        <Card>
          <CardBody>
            <p className="text-sm text-ctp-red">Error</p>
            <p className="text-xs text-ctp-subtext0 mt-1 whitespace-pre-wrap">{turn.errorMessage}</p>
          </CardBody>
        </Card>
      ) : turn.response ? (
        <ResponseCard response={turn.response} />
      ) : null}
    </div>
  )
}

function ResponseCard({ response }: { response: ChatResponse }) {
  return (
    <Card>
      <CardBody className="space-y-3">
        <p className="text-sm text-ctp-text whitespace-pre-wrap leading-relaxed">{response.answer}</p>

        {response.tables_used.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5">
            <Database className="w-3 h-3 text-ctp-overlay1" />
            <span className="text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0">Tables</span>
            {response.tables_used.map(table => (
              <code
                key={table}
                className="text-[10px] font-mono bg-ctp-surface0/60 text-ctp-blue rounded px-1.5 py-0.5"
              >
                {table}
              </code>
            ))}
          </div>
        )}

        {response.caveats.length > 0 && (
          <ul className="text-xs text-ctp-yellow space-y-0.5">
            {response.caveats.map((caveat, idx) => (
              <li key={idx} className="flex gap-1.5">
                <span className="text-ctp-overlay0">•</span>
                <span>{caveat}</span>
              </li>
            ))}
          </ul>
        )}

        {response.sql && <CollapsibleBlock title="Generated SQL" defaultOpen={false}>
          <pre className="text-[11px] font-mono leading-relaxed text-ctp-subtext1 whitespace-pre-wrap break-all">
            {response.sql}
          </pre>
        </CollapsibleBlock>}

        {response.rows.length > 0 && (
          <CollapsibleBlock title={`Result rows (${response.rows.length})`} defaultOpen={false}>
            <RowsPreview rows={response.rows} />
          </CollapsibleBlock>
        )}

        {response.error && (
          <p className="text-xs text-ctp-red mt-1 break-words">Error: {response.error}</p>
        )}
      </CardBody>
    </Card>
  )
}

function CollapsibleBlock({
  title,
  children,
  defaultOpen,
}: {
  title: string
  children: React.ReactNode
  defaultOpen: boolean
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="border border-ctp-surface1 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center gap-1.5 px-3 py-2 text-[11px] font-mono uppercase tracking-wide text-ctp-overlay1 hover:text-ctp-subtext1 hover:bg-ctp-surface0/40 transition-colors"
      >
        {open ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
        <span>{title}</span>
      </button>
      {open && <div className="px-3 pb-3 pt-1">{children}</div>}
    </div>
  )
}

function RowsPreview({ rows }: { rows: Record<string, unknown>[] }) {
  const visible = rows.slice(0, 50)
  const columns = Array.from(
    visible.reduce<Set<string>>((set, row) => {
      Object.keys(row).forEach(k => set.add(k))
      return set
    }, new Set())
  )
  return (
    <div className="overflow-x-auto -mx-1">
      <table className="min-w-full text-[11px] font-mono">
        <thead>
          <tr className="text-left text-ctp-overlay1 border-b border-ctp-surface1">
            {columns.map(c => (
              <th key={c} className="px-2 py-1 font-medium whitespace-nowrap">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visible.map((row, i) => (
            <tr key={i} className="border-b border-ctp-surface0 last:border-0">
              {columns.map(c => (
                <td key={c} className="px-2 py-1 text-ctp-subtext1 whitespace-nowrap">
                  {formatCell(row[c])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > visible.length && (
        <p className="text-[10px] text-ctp-overlay0 mt-2 px-2">
          Showing first {visible.length} of {rows.length} rows.
        </p>
      )}
    </div>
  )
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toFixed(2)
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
