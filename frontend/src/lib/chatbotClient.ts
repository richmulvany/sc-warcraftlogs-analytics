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
}

export class ChatbotConfigError extends Error {}

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
