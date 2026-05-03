// Thin client for the FastAPI chatbot backend. Mirrors dashboardDataClient
// in spirit: read the base URL from env, return typed responses, surface
// configuration errors clearly. Run the backend locally with
// `uvicorn backend.app.main:app --reload` for development.

const baseUrl = import.meta.env.VITE_CHATBOT_API_URL?.replace(/\/$/, '') ?? ''

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
      'Chatbot backend is not configured. Set VITE_CHATBOT_API_URL.'
    )
  }
  const response = await fetch(`${baseUrl}/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ question }),
    signal,
  })
  if (!response.ok) {
    const detail = await response.text().catch(() => '')
    throw new Error(
      `Chatbot returned ${response.status} ${response.statusText}${detail ? `: ${detail}` : ''}`
    )
  }
  return response.json()
}
