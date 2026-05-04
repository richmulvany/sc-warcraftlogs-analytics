import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Check,
  ChevronDown,
  ChevronRight,
  Database,
  History,
  MessageCircleQuestion,
  PanelLeftClose,
  PanelLeftOpen,
  Plus,
  RotateCcw,
  Send,
  Sparkles,
  ThumbsDown,
  ThumbsUp,
  Trash2,
  X,
} from 'lucide-react'
import clsx from 'clsx'
import { AppLayout } from '../components/layout/AppLayout'
import { useChat, type ChatSession, type ChatTurn, type ChatTurnStep } from '../context/ChatContext'
import { getChatbotMeta, type ChatResponse } from '../lib/chatbotClient'

const SUGGESTIONS = [
  'Who dies most often on each boss?',
  'Which bosses are we wiping on most?',
  'Who has the highest Mythic+ score trend?',
  'Which players have the best parse percentiles on Mythic?',
  'What are our most common killing blows?',
  'Who has the strongest attendance this tier?',
]

let openedChatThisRuntime = false

function elapsedLabel(turn: ChatTurn): string {
  const end = turn.endedAt ?? Date.now()
  const seconds = Math.max(0, (end - turn.startedAt) / 1000)
  return seconds < 1 ? `${(seconds * 1000).toFixed(0)} ms` : `${seconds.toFixed(1)} s`
}

function relativeTime(value: number): string {
  const seconds = Math.max(0, Math.floor((Date.now() - value) / 1000))
  if (seconds < 60) return 'now'
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h`
  const days = Math.floor(hours / 24)
  return `${days}d`
}

export function Chat() {
  const {
    sessions,
    activeSession,
    activeSessionId,
    pendingCount,
    setActiveSessionId,
    startNewSession,
    deleteSession,
    ask,
    submitFeedback,
  } = useChat()
  const [question, setQuestion] = useState('')
  const [historyOpen, setHistoryOpen] = useState(false)
  const [chatbotModel, setChatbotModel] = useState<string | null>(null)
  const [privacyDismissed, setPrivacyDismissed] = useState(
    () => sessionStorage.getItem('sc-chat-privacy-dismissed') === '1',
  )
  const hasSentMessage = activeSession.turns.some(t => !t.pending) || activeSession.turns.length > 0
  const showPrivacyNotice = !privacyDismissed && !hasSentMessage && question.trim().length > 0
  const dismissPrivacy = () => {
    sessionStorage.setItem('sc-chat-privacy-dismissed', '1')
    setPrivacyDismissed(true)
  }
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const activeBusy = activeSession.turns.some(turn => turn.pending)

  useEffect(() => {
    let cancelled = false
    getChatbotMeta()
      .then(meta => {
        if (!cancelled) setChatbotModel(meta.model)
      })
      .catch(() => {
        if (!cancelled) setChatbotModel(null)
      })
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (openedChatThisRuntime) return
    openedChatThisRuntime = true
    if (activeSession.turns.length > 0) {
      startNewSession()
    }
  }, [activeSession.turns.length, startNewSession])

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [activeSession.turns])

  async function submit(text: string) {
    const trimmed = text.trim()
    if (!trimmed || activeBusy) return
    setQuestion('')
    await ask(trimmed, activeSessionId)
  }

  function handleSubmit(event: React.FormEvent) {
    event.preventDefault()
    submit(question)
  }

  function handleKeyDown(event: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault()
      submit(question)
    }
  }

  return (
    <AppLayout title="Ask" subtitle="Ask the chatbot to utilise natural-language queries against the guild data" wide fullBleed>
      <div className="h-full min-h-0 flex flex-col md:flex-row bg-ctp-base">
        <HistoryPanel
          sessions={sessions}
          activeSessionId={activeSessionId}
          pendingCount={pendingCount}
          open={historyOpen}
          onToggle={() => setHistoryOpen(open => !open)}
          onSelect={setActiveSessionId}
          onNew={startNewSession}
          onDelete={deleteSession}
        />

        <section className="flex-1 min-w-0 min-h-0 flex flex-col">
          <div ref={scrollRef} className="flex-1 min-h-0 overflow-y-auto px-4 py-5 md:px-8 lg:px-10">
            <div className="mx-auto w-full max-w-4xl space-y-5">
              {activeSession.turns.length === 0 ? (
                <EmptyState onSuggest={submit} disabled={activeBusy} />
              ) : (
                activeSession.turns.map(turn => (
                  <TurnMessage
                    key={turn.id}
                    turn={turn}
                    onFeedback={submitFeedback}
                  />
                ))
              )}
            </div>
          </div>

          {showPrivacyNotice && (
            <div className="flex-shrink-0 px-4 pt-2 md:px-8 lg:px-10">
              <div className="mx-auto flex w-full max-w-4xl items-start gap-2 rounded-xl border border-ctp-red/40 bg-ctp-red/10 px-3 py-2 text-xs text-ctp-red">
                <span className="flex-1 leading-snug">
                  Your question is sent to an OpenAI API. Do not paste personal information.{' '}
                  <a
                    href="https://openai.com/policies/privacy-policy/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="underline hover:text-ctp-text"
                  >
                    OpenAI privacy policy
                  </a>
                </span>
                <button
                  type="button"
                  onClick={dismissPrivacy}
                  aria-label="Dismiss privacy notice"
                  className="flex h-5 w-5 flex-shrink-0 items-center justify-center rounded hover:bg-ctp-red/20"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>
          )}

          <form
            onSubmit={handleSubmit}
            className="flex-shrink-0 border-t border-ctp-surface0 bg-ctp-base/95 px-4 pt-3 pb-[max(0.75rem,env(safe-area-inset-bottom))] md:px-8 md:pb-3 lg:px-10"
          >
            <div className="mx-auto flex w-full max-w-4xl items-end gap-2 rounded-2xl border border-ctp-surface1 bg-ctp-surface0/65 p-2 shadow-card focus-within:border-ctp-teal/70 focus-within:ring-1 focus-within:ring-ctp-teal/30">
              <textarea
                value={question}
                onChange={e => setQuestion(e.target.value)}
                onKeyDown={handleKeyDown}
                rows={1}
                placeholder="Ask about progression, parses, wipes, Mythic+, preparation..."
                className="min-h-[44px] max-h-40 flex-1 resize-none bg-transparent px-2 py-2.5 text-sm leading-relaxed text-ctp-text placeholder:text-ctp-overlay0 focus:outline-none"
                disabled={activeBusy}
              />
              <button
                type="submit"
                disabled={activeBusy || !question.trim()}
                aria-label="Send message"
                title={activeBusy ? 'Assistant is responding' : 'Send message'}
                className={clsx(
                  'flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-xl border transition-colors',
                  activeBusy || !question.trim()
                    ? 'border-ctp-surface1 bg-ctp-surface0/40 text-ctp-overlay0 cursor-not-allowed'
                    : 'border-ctp-teal/40 bg-ctp-teal/15 text-ctp-teal hover:bg-ctp-teal/25'
                )}
              >
                <Send className="h-4 w-4" />
              </button>
            </div>
            <div
              className={clsx(
                'mx-auto mt-2 w-full max-w-4xl text-right text-[10px] font-mono uppercase tracking-wide text-ctp-overlay1',
                !chatbotModel && 'opacity-60'
              )}
            >
              Powered by {chatbotModel ?? 'gpt model'}
            </div>
          </form>
        </section>
      </div>
    </AppLayout>
  )
}

function HistoryPanel({
  sessions,
  activeSessionId,
  pendingCount,
  open,
  onToggle,
  onSelect,
  onNew,
  onDelete,
}: {
  sessions: ChatSession[]
  activeSessionId: string
  pendingCount: number
  open: boolean
  onToggle: () => void
  onSelect: (id: string) => void
  onNew: () => void
  onDelete: (id: string) => void
}) {
  return (
    <aside className={clsx(
      'flex-shrink-0 border-b border-ctp-surface0 bg-ctp-mantle/55 transition-all duration-200 md:border-b-0 md:border-r',
      open ? 'md:w-72' : 'md:w-14'
    )}>
      <div className={clsx(
        'flex items-center gap-2 px-3 py-3 md:py-4',
        !open && 'md:flex-col'
      )}>
        <button
          type="button"
          onClick={onToggle}
          aria-label={open ? 'Collapse Chat History' : 'Expand Chat History'}
          title={open ? 'Collapse Chat History' : 'Expand Chat History'}
          className="flex h-8 w-8 items-center justify-center rounded-xl border border-ctp-teal/30 bg-ctp-teal/15 text-ctp-teal hover:bg-ctp-teal/25"
        >
          {open ? <PanelLeftClose className="h-4 w-4" /> : <PanelLeftOpen className="h-4 w-4" />}
        </button>
        {open ? (
          <>
            <div className="min-w-0 flex-1">
              <p className="text-sm font-semibold text-ctp-text">Chat History</p>
              {pendingCount > 0 && <p className="text-[11px] font-mono text-ctp-teal">{pendingCount} running</p>}
            </div>
            <button
              type="button"
              onClick={onNew}
              aria-label="New chat"
              title="New chat"
              className="flex h-8 w-8 items-center justify-center rounded-lg text-ctp-subtext0 hover:bg-ctp-surface0 hover:text-ctp-text"
            >
              <Plus className="h-4 w-4" />
            </button>
          </>
        ) : (
          <>
            <button
              type="button"
              onClick={onNew}
              aria-label="New chat"
              title="New chat"
              className="hidden h-8 w-8 items-center justify-center rounded-lg text-ctp-subtext0 hover:bg-ctp-surface0 hover:text-ctp-text md:flex"
            >
              <Plus className="h-4 w-4" />
            </button>
            <div className="hidden flex-1 items-center justify-center md:flex">
              <span className="[writing-mode:vertical-rl] rotate-180 text-[10px] font-mono uppercase tracking-[0.14em] text-ctp-overlay1">
                Chat History
              </span>
            </div>
            {pendingCount > 0 && <span className="hidden h-2 w-2 rounded-full bg-ctp-teal md:block" />}
          </>
        )}
      </div>

      {open && <div className="flex gap-2 overflow-x-auto px-3 pb-3 md:block md:h-[calc(100vh-12rem)] md:overflow-y-auto md:overflow-x-hidden md:space-y-1">
        {sessions.map(session => {
          const active = session.id === activeSessionId
          const pending = session.turns.some(turn => turn.pending)
          return (
            <div
              key={session.id}
              className={clsx(
                'group flex min-w-[220px] items-center gap-1 rounded-xl border transition-colors md:w-full md:min-w-0',
                active
                  ? 'border-ctp-teal/30 bg-ctp-teal/10'
                  : 'border-transparent bg-transparent hover:bg-ctp-surface0/55'
              )}
            >
              <button
                type="button"
                onClick={() => onSelect(session.id)}
                className="flex min-w-0 flex-1 items-center gap-2 px-3 py-2 text-left"
              >
                <span className={clsx(
                  'h-2 w-2 flex-shrink-0 rounded-full',
                  pending ? 'bg-ctp-teal animate-pulse' : active ? 'bg-ctp-teal' : 'bg-ctp-overlay0'
                )} />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-medium text-ctp-text">{session.title}</span>
                  <span className="block truncate text-[11px] font-mono text-ctp-overlay1">
                    {session.turns.length} {session.turns.length === 1 ? 'turn' : 'turns'} · {relativeTime(session.updatedAt)}
                  </span>
                </span>
              </button>
              {sessions.length > 1 && (
                <button
                  type="button"
                  title="Delete chat"
                  aria-label="Delete chat"
                  onClick={event => {
                    event.stopPropagation()
                    onDelete(session.id)
                  }}
                  className="mr-2 hidden h-7 w-7 flex-shrink-0 items-center justify-center rounded-lg text-ctp-overlay0 hover:bg-ctp-surface1 hover:text-ctp-red group-hover:flex"
                >
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              )}
            </div>
          )
        })}
      </div>}
    </aside>
  )
}

function EmptyState({ onSuggest, disabled }: { onSuggest: (q: string) => void; disabled: boolean }) {
  return (
    <div className="mx-auto max-w-3xl py-8 md:py-14">
      <div className="mb-7 rounded-2xl border border-ctp-surface1 bg-ctp-surface0/35 px-5 py-5 shadow-card">
        <div className="mb-4 flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-2xl border border-ctp-teal/30 bg-ctp-teal/15">
          <MessageCircleQuestion className="h-4 w-4 text-ctp-teal" />
          </div>
          <div>
            <h2 className="text-base font-semibold text-ctp-text">Start a conversation with the guild data</h2>
            <p className="mt-0.5 text-xs text-ctp-overlay1">
              Ask the chatbot a question and it will translate it to SQL and fetch your answer.
            </p>
          </div>
        </div>
        <p className="text-sm leading-relaxed text-ctp-subtext1">
          Try asking about deaths, boss progress, raid attendance, player performance, preparation, or Mythic+ trends.
          The answer will include the SQL query and result rows for when they are useful to inspect.
        </p>
      </div>
      <div className="mb-3 flex items-center gap-2 text-[11px] font-mono uppercase tracking-wide text-ctp-overlay1">
        <History className="h-3.5 w-3.5 text-ctp-teal" />
        <span>Suggested starts</span>
      </div>
      <div className="grid gap-2 sm:grid-cols-2">
        {SUGGESTIONS.map(s => (
          <button
            key={s}
            type="button"
            onClick={() => onSuggest(s)}
            disabled={disabled}
            className="min-h-[48px] rounded-xl border border-ctp-surface1 bg-ctp-surface0/45 px-3 py-2 text-left text-sm text-ctp-subtext1 transition-colors hover:border-ctp-teal/30 hover:bg-ctp-teal/10 hover:text-ctp-text disabled:cursor-not-allowed disabled:opacity-50"
          >
            {s}
          </button>
        ))}
      </div>
    </div>
  )
}

function TurnMessage({
  turn,
  onFeedback,
}: {
  turn: ChatTurn
  onFeedback: (turnId: string, effective: boolean) => Promise<void>
}) {
  const [feedbackBusy, setFeedbackBusy] = useState<null | 'up' | 'down'>(null)
  const [feedbackError, setFeedbackError] = useState<string | null>(null)
  const [confirmedAt, setConfirmedAt] = useState<number | null>(null)
  const [lastEffective, setLastEffective] = useState<boolean | null>(null)

  async function handleFeedback(effective: boolean) {
    if (!turn.response?.sql || feedbackBusy) return
    setFeedbackBusy(effective ? 'up' : 'down')
    setFeedbackError(null)
    setLastEffective(effective)
    try {
      await onFeedback(turn.id, effective)
      setConfirmedAt(Date.now())
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      setFeedbackError(message)
    } finally {
      setFeedbackBusy(null)
    }
  }

  // Auto-fade the "Feedback accepted" pill ~2.5s after success. The button
  // ring colour stays so the user can still see which way they voted.
  useEffect(() => {
    if (!confirmedAt) return
    const timer = window.setTimeout(() => setConfirmedAt(null), 2500)
    return () => window.clearTimeout(timer)
  }, [confirmedAt])

  return (
    <article className="space-y-4">
      <div className="flex justify-end">
        <div className="max-w-[86%] rounded-2xl rounded-br-md bg-ctp-surface0 px-4 py-3 text-sm leading-relaxed text-ctp-text shadow-card whitespace-pre-wrap">
          {turn.question}
        </div>
      </div>

      <div className="flex gap-3">
        <div className="mt-1 flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-xl border border-ctp-teal/30 bg-ctp-teal/15">
          <Sparkles className="h-3.5 w-3.5 text-ctp-teal" />
        </div>
        <div className="min-w-0 flex-1 space-y-2">
          <div className="text-[11px] font-mono text-ctp-overlay1">
            Assistant{turn.pending ? ' · thinking...' : ` · ${elapsedLabel(turn)}`}
          </div>

          {turn.pending ? (
            <StepStream steps={turn.steps ?? []} />
          ) : turn.errorMessage ? (
            <div className="rounded-2xl rounded-tl-md border border-ctp-red/20 bg-ctp-red/10 px-4 py-3">
              <p className="text-sm font-medium text-ctp-red">Error</p>
              <p className="mt-1 whitespace-pre-wrap text-xs leading-relaxed text-ctp-subtext0">{turn.errorMessage}</p>
            </div>
          ) : turn.response ? (
            <>
              {turn.steps && turn.steps.length > 0 && (
                <CollapsibleBlock
                  title={`Steps (${turn.steps.length})`}
                  defaultOpen={false}
                >
                  <StepStream steps={turn.steps} compact />
                </CollapsibleBlock>
              )}
              <ResponseBlock
                response={turn.response}
                feedbackBusy={feedbackBusy}
                feedbackError={feedbackError}
                confirmedVisible={confirmedAt !== null}
                lastEffective={lastEffective}
                onFeedback={handleFeedback}
                onDismissError={() => setFeedbackError(null)}
              />
            </>
          ) : null}
        </div>
      </div>
    </article>
  )
}

const PHASE_LABELS: Record<ChatTurnStep['phase'], string> = {
  selecting_tables: 'Selecting tables',
  memory_reuse: 'Reusing approved memory',
  writing_sql: 'Writing SQL',
  executing_sql: 'Executing query',
  writing_answer: 'Writing answer',
}

function StepStream({ steps, compact = false }: { steps: ChatTurnStep[]; compact?: boolean }) {
  if (steps.length === 0) {
    return (
      <div className="flex items-center gap-2 rounded-2xl rounded-tl-md border border-ctp-surface1 bg-ctp-surface0/35 px-4 py-3 text-sm text-ctp-overlay1">
        <TypingDots />
        <span>Connecting to assistant…</span>
      </div>
    )
  }
  return (
    <div
      className={clsx(
        'rounded-2xl rounded-tl-md border border-ctp-surface1 bg-ctp-surface0/35 px-4 py-3 text-xs',
        compact && 'border-transparent bg-transparent px-0 py-0',
      )}
    >
      <ul className="space-y-1.5">
        {steps.map((step, idx) => (
          <StepRow key={idx} step={step} />
        ))}
      </ul>
    </div>
  )
}

function StepRow({ step }: { step: ChatTurnStep }) {
  const label = PHASE_LABELS[step.phase] ?? step.phase
  const attempt = step.attempt && step.attempt > 1 ? ` (attempt ${step.attempt})` : ''
  const colour =
    step.status === 'error'
      ? 'text-ctp-red'
      : step.status === 'done'
        ? 'text-ctp-green'
        : 'text-ctp-teal'
  const icon =
    step.status === 'running' ? (
      <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-ctp-teal" />
    ) : step.status === 'done' ? (
      <Check className="h-3 w-3 text-ctp-green" />
    ) : (
      <span className="text-ctp-red">!</span>
    )
  return (
    <li className="flex items-start gap-2 font-mono">
      <span className="mt-0.5 flex h-3 w-3 flex-shrink-0 items-center justify-center">{icon}</span>
      <span className={clsx('leading-snug', colour)}>
        {label}
        {attempt}
        {step.status === 'error' && step.error && (
          <span className="ml-2 break-all text-[10px] text-ctp-overlay1">— {step.error}</span>
        )}
      </span>
    </li>
  )
}

function ResponseBlock({
  response,
  feedbackBusy,
  feedbackError,
  confirmedVisible,
  lastEffective,
  onFeedback,
  onDismissError,
}: {
  response: ChatResponse
  feedbackBusy: null | 'up' | 'down'
  feedbackError: string | null
  confirmedVisible: boolean
  lastEffective: boolean | null
  onFeedback: (effective: boolean) => void
  onDismissError: () => void
}) {
  const upChosen = response.feedback === 'effective'
  const downChosen = response.feedback === 'ineffective'
  return (
    <div className="rounded-2xl rounded-tl-md border border-ctp-surface1 bg-ctp-surface0/35 px-4 py-3 shadow-card">
      <div className="space-y-3">
        <p className="whitespace-pre-wrap text-sm leading-relaxed text-ctp-text">
          <InlineBold text={response.answer} />
        </p>

        {response.tables_used.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5">
            <Database className="h-3 w-3 text-ctp-overlay1" />
            <span className="text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0">Tables</span>
            {response.tables_used.map(table => (
              <code
                key={table}
                className="rounded bg-ctp-base/70 px-1.5 py-0.5 font-mono text-[10px] text-ctp-teal"
              >
                {table}
              </code>
            ))}
          </div>
        )}

        {response.caveats.length > 0 && (
          <ul className="space-y-0.5 text-xs text-ctp-yellow">
            {response.caveats.map((caveat, idx) => (
              <li key={idx} className="flex gap-1.5">
                <span className="text-ctp-overlay0">-</span>
                <span>{caveat}</span>
              </li>
            ))}
          </ul>
        )}

        {response.sql && <CollapsibleBlock title="Generated SQL" defaultOpen={false}>
          <pre className="whitespace-pre-wrap break-all font-mono text-[11px] leading-relaxed text-ctp-subtext1">
            {response.sql}
          </pre>
        </CollapsibleBlock>}

        {response.rows.length > 0 && (
          <CollapsibleBlock title={`Result rows (${response.rows.length})`} defaultOpen={false}>
            <RowsPreview rows={response.rows} />
          </CollapsibleBlock>
        )}

        {response.error && (
          <p className="mt-1 break-words text-xs text-ctp-red">Error: {response.error}</p>
        )}

        {response.sql && (
          <div className="flex flex-wrap items-center gap-2 border-t border-ctp-surface1/70 pt-2">
            <span className="text-[10px] font-mono uppercase tracking-wide text-ctp-overlay0">
              Was this answer helpful?
            </span>
            <button
              type="button"
              onClick={() => onFeedback(true)}
              disabled={feedbackBusy !== null}
              aria-label="Mark answer helpful"
              title="Mark answer helpful"
              className={clsx(
                'flex h-7 w-7 items-center justify-center rounded-lg border transition-all duration-150 active:scale-95',
                upChosen
                  ? 'border-ctp-green/40 bg-ctp-green/20 text-ctp-green ring-1 ring-ctp-green/30'
                  : 'border-transparent text-ctp-overlay1 hover:bg-ctp-surface1 hover:text-ctp-green',
                feedbackBusy === 'up' && 'cursor-wait animate-pulse ring-2 ring-ctp-green/40'
              )}
            >
              {upChosen && feedbackBusy === null ? (
                <Check className="h-3.5 w-3.5" />
              ) : (
                <ThumbsUp className="h-3.5 w-3.5" />
              )}
            </button>
            <button
              type="button"
              onClick={() => onFeedback(false)}
              disabled={feedbackBusy !== null}
              aria-label="Mark answer not helpful"
              title="Mark answer not helpful"
              className={clsx(
                'flex h-7 w-7 items-center justify-center rounded-lg border transition-all duration-150 active:scale-95',
                downChosen
                  ? 'border-ctp-red/40 bg-ctp-red/20 text-ctp-red ring-1 ring-ctp-red/30'
                  : 'border-transparent text-ctp-overlay1 hover:bg-ctp-surface1 hover:text-ctp-red',
                feedbackBusy === 'down' && 'cursor-wait animate-pulse ring-2 ring-ctp-red/40'
              )}
            >
              {downChosen && feedbackBusy === null ? (
                <Check className="h-3.5 w-3.5" />
              ) : (
                <ThumbsDown className="h-3.5 w-3.5" />
              )}
            </button>
            {confirmedVisible && (
              <span
                className={clsx(
                  'feedback-pop-in inline-flex items-center gap-1 rounded-full px-2 py-1 text-[10px] font-mono uppercase tracking-wide',
                  lastEffective
                    ? 'bg-ctp-green/15 text-ctp-green'
                    : 'bg-ctp-red/15 text-ctp-red'
                )}
              >
                <Check className="h-3 w-3" />
                Feedback recorded
              </span>
            )}
            {feedbackError && (
              <span className="inline-flex items-center gap-2 text-[10px] font-mono text-ctp-red">
                Couldn't save feedback
                <button
                  type="button"
                  onClick={() => {
                    onDismissError()
                    if (lastEffective !== null) onFeedback(lastEffective)
                  }}
                  className="inline-flex items-center gap-1 rounded border border-ctp-red/40 bg-ctp-red/10 px-1.5 py-0.5 uppercase tracking-wide hover:bg-ctp-red/20"
                >
                  <RotateCcw className="h-3 w-3" /> Retry
                </button>
              </span>
            )}
            {response.from_memory && (
              <span className="ml-auto text-[10px] font-mono text-ctp-teal">memory</span>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function InlineBold({ text }: { text: string }) {
  const parts = useMemo(() => text.split(/(\*\*[^*]+\*\*)/g), [text])
  return (
    <>
      {parts.map((part, index) => {
        if (part.startsWith('**') && part.endsWith('**') && part.length > 4) {
          return <strong key={index} className="font-semibold text-ctp-text">{part.slice(2, -2)}</strong>
        }
        return <span key={index}>{part}</span>
      })}
    </>
  )
}

function TypingDots() {
  return (
    <span className="flex items-center gap-1" aria-hidden="true">
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-ctp-teal" />
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-ctp-teal [animation-delay:120ms]" />
      <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-ctp-teal [animation-delay:240ms]" />
    </span>
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
    <div className="overflow-hidden rounded-lg border border-ctp-surface1">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        className="flex w-full items-center gap-1.5 px-3 py-2 text-[11px] font-mono uppercase tracking-wide text-ctp-overlay1 transition-colors hover:bg-ctp-surface0/40 hover:text-ctp-subtext1"
      >
        {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
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
    <div className="-mx-1 overflow-x-auto">
      <table className="min-w-full font-mono text-[11px]">
        <thead>
          <tr className="border-b border-ctp-surface1 text-left text-ctp-overlay1">
            {columns.map(c => (
              <th key={c} className="whitespace-nowrap px-2 py-1 font-medium">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visible.map((row, i) => (
            <tr key={i} className="border-b border-ctp-surface0 last:border-0">
              {columns.map(c => (
                <td key={c} className="whitespace-nowrap px-2 py-1 text-ctp-subtext1">
                  {formatCell(row[c])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > visible.length && (
        <p className="mt-2 px-2 text-[10px] text-ctp-overlay0">
          Showing first {visible.length} of {rows.length} rows.
        </p>
      )}
    </div>
  )
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return '-'
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toFixed(2)
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
