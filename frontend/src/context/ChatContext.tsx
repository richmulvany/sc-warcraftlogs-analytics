import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import { ChatbotConfigError, postChat, postChatFeedback, type ChatResponse } from '../lib/chatbotClient'

export interface ChatTurn {
  id: string
  question: string
  pending: boolean
  response?: ChatResponse
  errorMessage?: string
  startedAt: number
  endedAt?: number
}

export interface ChatSession {
  id: string
  title: string
  createdAt: number
  updatedAt: number
  turns: ChatTurn[]
}

interface ChatContextValue {
  sessions: ChatSession[]
  activeSession: ChatSession
  activeSessionId: string
  pendingCount: number
  oldestPendingStartedAt?: number
  lastSettledAt?: number
  setActiveSessionId: (id: string) => void
  startNewSession: () => string
  deleteSession: (id: string) => void
  ask: (text: string, sessionId?: string) => Promise<void>
  submitFeedback: (turnId: string, effective: boolean) => Promise<void>
}

const STORAGE_KEY = 'sc-analytics-chat-sessions-v1'
const ChatContext = createContext<ChatContextValue | null>(null)

function makeId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
}

function createSession(): ChatSession {
  const now = Date.now()
  return {
    id: makeId(),
    title: 'New chat',
    createdAt: now,
    updatedAt: now,
    turns: [],
  }
}

function normaliseStoredSessions(raw: string | null): ChatSession[] {
  if (!raw) return [createSession()]
  try {
    const parsed = JSON.parse(raw) as ChatSession[]
    if (!Array.isArray(parsed) || parsed.length === 0) return [createSession()]
    return parsed.map(session => ({
      ...session,
      title: session.title || 'New chat',
      turns: (session.turns || []).map(turn => (
        turn.pending
          ? {
              ...turn,
              pending: false,
              endedAt: turn.endedAt ?? Date.now(),
              errorMessage: 'This request was interrupted by a page reload. Please ask again.',
            }
          : turn
      )),
    }))
  } catch {
    return [createSession()]
  }
}

function generateTitle(question: string): string {
  const cleaned = question
    .replace(/[?!.]+$/g, '')
    .replace(/\s+/g, ' ')
    .trim()
  if (!cleaned) return 'New chat'

  const words = cleaned.split(' ')
  const short = words.slice(0, 8).join(' ')
  return short.length > 54 ? `${short.slice(0, 51).trim()}...` : short
}

function errorMessageFor(err: unknown): string {
  if (err instanceof ChatbotConfigError) {
    return `${err.message} The backend usually runs at http://localhost:8000; start it with \`uvicorn backend.app.main:app --reload\`.`
  }
  if (err instanceof Error) return err.message
  return String(err)
}

export function ChatProvider({ children }: { children: React.ReactNode }) {
  const [sessions, setSessions] = useState<ChatSession[]>(() => normaliseStoredSessions(localStorage.getItem(STORAGE_KEY)))
  const [activeSessionId, setActiveSessionId] = useState(() => sessions[0]?.id ?? createSession().id)

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(sessions))
  }, [sessions])

  const startNewSession = useCallback(() => {
    const session = createSession()
    setSessions(prev => [session, ...prev])
    setActiveSessionId(session.id)
    return session.id
  }, [])

  const deleteSession = useCallback((id: string) => {
    setSessions(prev => {
      const next = prev.filter(session => session.id !== id)
      const replacement = next.length > 0 ? next : [createSession()]
      setActiveSessionId(current => current === id ? replacement[0].id : current)
      return replacement
    })
  }, [])

  const ask = useCallback(async (text: string, targetSessionId?: string) => {
    const trimmed = text.trim()
    if (!trimmed) return

    const sessionId = targetSessionId ?? activeSessionId
    const turnId = makeId()
    const startedAt = Date.now()

    setSessions(prev => prev.map(session => {
      if (session.id !== sessionId) return session
      const firstTurn = session.turns.length === 0
      return {
        ...session,
        title: firstTurn || session.title === 'New chat' ? generateTitle(trimmed) : session.title,
        updatedAt: startedAt,
        turns: [
          ...session.turns,
          { id: turnId, question: trimmed, pending: true, startedAt },
        ],
      }
    }))

    try {
      const response = await postChat(trimmed)
      const endedAt = Date.now()
      setSessions(prev => prev.map(session => {
        if (session.id !== sessionId) return session
        return {
          ...session,
          updatedAt: endedAt,
          turns: session.turns.map(turn => (
            turn.id === turnId
              ? { ...turn, response, pending: false, endedAt }
              : turn
          )),
        }
      }))
    } catch (err) {
      const endedAt = Date.now()
      const message = errorMessageFor(err)
      setSessions(prev => prev.map(session => {
        if (session.id !== sessionId) return session
        return {
          ...session,
          updatedAt: endedAt,
          turns: session.turns.map(turn => (
            turn.id === turnId
              ? { ...turn, errorMessage: message, pending: false, endedAt }
              : turn
          )),
        }
      }))
    }
  }, [activeSessionId])

  const submitFeedback = useCallback(async (turnId: string, effective: boolean) => {
    const target = sessions
      .flatMap(session => session.turns)
      .find(turn => turn.id === turnId)
    const responseId = target?.response?.response_id
    if (!target || !responseId) return

    await postChatFeedback({
      responseId,
      effective,
      question: target.question,
      sql: target.response?.sql,
    })

    setSessions(prev => prev.map(session => ({
      ...session,
      turns: session.turns.map(turn => (
        turn.id === turnId && turn.response
          ? {
              ...turn,
              response: {
                ...turn.response,
                feedback: effective ? 'effective' : 'ineffective',
              },
            }
          : turn
      )),
    })))
  }, [sessions])

  const activeSession = sessions.find(session => session.id === activeSessionId) ?? sessions[0] ?? createSession()
  const pendingCount = sessions.reduce(
    (count, session) => count + session.turns.filter(turn => turn.pending).length,
    0
  )
  const pendingTurns = sessions.flatMap(session => session.turns.filter(turn => turn.pending))
  const settledTurns = sessions.flatMap(session => session.turns.filter(turn => !turn.pending && turn.endedAt))
  const oldestPendingStartedAt = pendingTurns.length
    ? Math.min(...pendingTurns.map(turn => turn.startedAt))
    : undefined
  const lastSettledAt = settledTurns.length
    ? Math.max(...settledTurns.map(turn => turn.endedAt ?? 0))
    : undefined

  const value = useMemo<ChatContextValue>(() => ({
    sessions,
    activeSession,
    activeSessionId: activeSession.id,
    pendingCount,
    oldestPendingStartedAt,
    lastSettledAt,
    setActiveSessionId,
    startNewSession,
    deleteSession,
    ask,
    submitFeedback,
  }), [activeSession, ask, deleteSession, lastSettledAt, oldestPendingStartedAt, pendingCount, sessions, startNewSession, submitFeedback])

  return <ChatContext.Provider value={value}>{children}</ChatContext.Provider>
}

export function useChat() {
  const value = useContext(ChatContext)
  if (!value) throw new Error('useChat must be used within ChatProvider')
  return value
}
