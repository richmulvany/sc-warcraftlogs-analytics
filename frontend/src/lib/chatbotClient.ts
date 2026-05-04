// Thin client for the FastAPI chatbot backend. Mirrors dashboardDataClient
// in spirit: read the base URL from env, return typed responses, surface
// configuration errors clearly. Run the backend locally with
// `uvicorn backend.app.main:app --reload` for development.

// IMPORTANT: Vite only inlines env vars whose name starts with `VITE_`. The
// matching backend env var is CHATBOT_BACKEND_API_KEY (used in Azure); the
// frontend equivalent must be VITE_CHATBOT_API_KEY because Vite ignores
// non-prefixed names regardless of what they're called on the backend.
const baseUrl = import.meta.env.VITE_CHATBOT_API_URL?.replace(/\/$/, '') ?? ''
const apiKey = import.meta.env.VITE_CHATBOT_API_KEY ?? ''

export interface ChatRow {
  [key: string]: unknown
}

export interface ChatResponse {
  answer: string
  sql?: string | null
  tables_used: string[]
  rows: ChatRow[]
  caveats: string[]
  error?: string | null
  response_id?: string | null
  from_memory?: boolean
  feedback?: 'effective' | 'ineffective'
}

export interface ChatbotMeta {
  model: string
}

export class ChatbotConfigError extends Error {}

export async function getChatbotMeta(): Promise<ChatbotMeta> {
  if (!baseUrl) {
    throw new ChatbotConfigError(
      'Chatbot backend is not configured. Set VITE_CHATBOT_API_URL (must use the VITE_ prefix so Vite inlines it).'
    )
  }
  const headers: Record<string, string> = {}
  if (apiKey) headers['X-API-Key'] = apiKey
  const response = await fetch(`${baseUrl}/chat/meta`, { headers })
  if (!response.ok) {
    const detail = await response.text().catch(() => '')
    throw new Error(
      `Chatbot metadata returned ${response.status} ${response.statusText}${detail ? `: ${detail}` : ''}`
    )
  }
  return response.json()
}

export async function postChat(question: string, signal?: AbortSignal): Promise<ChatResponse> {
  if (!baseUrl) {
    throw new ChatbotConfigError(
      'Chatbot backend is not configured. Set VITE_CHATBOT_API_URL (must use the VITE_ prefix so Vite inlines it).'
    )
  }
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (apiKey) headers['X-API-Key'] = apiKey
  const response = await fetch(`${baseUrl}/chat`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ question }),
    signal,
  })
  if (!response.ok) {
    const detail = await response.text().catch(() => '')
    if (response.status === 401) {
      throw new Error(
        'Chatbot backend rejected the request (401). Check VITE_CHATBOT_API_KEY matches the backend CHATBOT_BACKEND_API_KEY.'
      )
    }
    throw new Error(
      `Chatbot returned ${response.status} ${response.statusText}${detail ? `: ${detail}` : ''}`
    )
  }
  return response.json()
}

export type ChatStreamPhase =
  | 'selecting_tables'
  | 'memory_reuse'
  | 'writing_sql'
  | 'executing_sql'
  | 'writing_answer'

export interface ChatStreamStep {
  type: 'step'
  phase: ChatStreamPhase
  status: 'running' | 'done' | 'error'
  attempt?: number
  detail?: unknown
  error?: string
}

export interface ChatStreamFinal {
  type: 'final'
  response: ChatResponse
}

export type ChatStreamEvent = ChatStreamStep | ChatStreamFinal | { type: 'error'; detail: string }

export async function streamChat(
  question: string,
  {
    onEvent,
    signal,
  }: { onEvent: (event: ChatStreamEvent) => void; signal?: AbortSignal },
): Promise<ChatResponse> {
  if (!baseUrl) {
    throw new ChatbotConfigError(
      'Chatbot backend is not configured. Set VITE_CHATBOT_API_URL (must use the VITE_ prefix so Vite inlines it).',
    )
  }
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    Accept: 'text/event-stream',
  }
  if (apiKey) headers['X-API-Key'] = apiKey
  const response = await fetch(`${baseUrl}/chat/stream`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ question }),
    signal,
  })
  if (!response.ok || !response.body) {
    const detail = await response.text().catch(() => '')
    throw new Error(
      `Chatbot stream returned ${response.status} ${response.statusText}${detail ? `: ${detail}` : ''}`,
    )
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let final: ChatResponse | null = null

  // Parse SSE frames: each frame ends in a blank line and contains
  // "event: <name>" and "data: <json>" lines.
  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let boundary = buffer.indexOf('\n\n')
    while (boundary !== -1) {
      const frame = buffer.slice(0, boundary)
      buffer = buffer.slice(boundary + 2)
      let name: string | null = null
      let data = ''
      for (const line of frame.split('\n')) {
        if (line.startsWith('event:')) name = line.slice(6).trim()
        else if (line.startsWith('data:')) data += line.slice(5).trim()
      }
      if (data) {
        try {
          const parsed = JSON.parse(data)
          if (name === 'final') {
            final = parsed as ChatResponse
            onEvent({ type: 'final', response: final })
          } else if (name === 'step') {
            onEvent({ type: 'step', ...(parsed as Omit<ChatStreamStep, 'type'>) })
          } else if (name === 'error') {
            onEvent({ type: 'error', detail: parsed.detail ?? '' })
          }
        } catch {
          // ignore malformed frame
        }
      }
      boundary = buffer.indexOf('\n\n')
    }
  }

  if (!final) {
    throw new Error('Chatbot stream ended without a final response.')
  }
  return final
}

export async function postChatFeedback({
  responseId,
  effective,
  question,
  sql,
}: {
  responseId: string
  effective: boolean
  question?: string
  sql?: string | null
}): Promise<void> {
  if (!baseUrl) {
    throw new ChatbotConfigError(
      'Chatbot backend is not configured. Set VITE_CHATBOT_API_URL (must use the VITE_ prefix so Vite inlines it).'
    )
  }
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (apiKey) headers['X-API-Key'] = apiKey
  const response = await fetch(`${baseUrl}/chat/feedback`, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      response_id: responseId,
      effective,
      question,
      sql,
    }),
  })
  if (!response.ok) {
    const detail = await response.text().catch(() => '')
    throw new Error(
      `Feedback returned ${response.status} ${response.statusText}${detail ? `: ${detail}` : ''}`
    )
  }
}
