import { useState, useRef, useEffect } from 'react'
import Anthropic from '@anthropic-ai/sdk'
import './Conversation.css'

const SCENARIOS = [
  { id: 'restaurant', label: 'At a Restaurant', description: 'Order food, ask for the bill, handle dietary needs' },
  { id: 'directions', label: 'Asking for Directions', description: 'Find your hotel, navigate the city' },
  { id: 'shopping', label: 'Shopping', description: 'Ask prices, sizes, and make purchases' },
  { id: 'hotel', label: 'At the Hotel', description: 'Check in, request things, handle issues' },
  { id: 'smalltalk', label: 'Small Talk', description: 'Greet locals, introduce yourself, chat casually' },
]

const SCENARIO_LABELS = Object.fromEntries(SCENARIOS.map(s => [s.id, s.label]))

function buildSystemPrompt(scenario) {
  return `You are a friendly, encouraging Italian language tutor. The user is an English speaker who knows Spanish fluently and is preparing for a trip to Italy in 2 months. They know zero Italian right now.

Your job is to help them practice conversational Italian for travel.

For every response you give:
1. Reply in Italian appropriate to the scenario — keep sentences short and practical
2. Add an English translation immediately after, in [square brackets]
3. When a word or phrase is similar to Spanish, point it out briefly (e.g., "💡 'grazie' — similar to 'gracias' in Spanish!")
4. If the user makes a grammatical mistake or uses English when they should try Italian, gently correct them and explain why, then continue the conversation
5. Keep responses concise — this is a travel conversation practice, not a lecture
6. Use realistic Italian that a tourist would actually hear or say in Italy

The user has chosen this practice scenario: **${SCENARIO_LABELS[scenario] || scenario}**

Start by briefly setting the scene in Italian (with translation), then invite the user to respond. Keep the energy positive and encouraging — mistakes are part of learning!`
}

export default function Conversation() {
  const [scenario, setScenario] = useState('')
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [started, setStarted] = useState(false)
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  async function startConversation() {
    if (!scenario) return
    const apiKey = import.meta.env.VITE_ANTHROPIC_API_KEY
    if (!apiKey) {
      setError('No API key found. Create italian_app/.env with VITE_ANTHROPIC_API_KEY=your_key')
      return
    }
    setError('')
    setMessages([])
    setStarted(true)
    setLoading(true)

    try {
      const client = new Anthropic({ apiKey, dangerouslyAllowBrowser: true })
      const response = await client.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 600,
        system: buildSystemPrompt(scenario),
        messages: [{ role: 'user', content: 'Start the scenario.' }],
      })
      const text = response.content[0].text
      setMessages([{ role: 'assistant', text }])
    } catch (e) {
      setError(`Error: ${e.message}`)
      setStarted(false)
    } finally {
      setLoading(false)
    }
  }

  async function sendMessage() {
    if (!input.trim() || loading) return
    const userText = input.trim()
    setInput('')

    const newMessages = [...messages, { role: 'user', text: userText }]
    setMessages(newMessages)
    setLoading(true)
    setError('')

    try {
      const apiKey = import.meta.env.VITE_ANTHROPIC_API_KEY
      const client = new Anthropic({ apiKey, dangerouslyAllowBrowser: true })

      const apiMessages = newMessages.map(m => ({
        role: m.role,
        content: m.text,
      }))

      const response = await client.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 600,
        system: buildSystemPrompt(scenario),
        messages: apiMessages,
      })
      const text = response.content[0].text
      setMessages(prev => [...prev, { role: 'assistant', text }])
    } catch (e) {
      setError(`Error: ${e.message}`)
    } finally {
      setLoading(false)
    }
  }

  function reset() {
    setStarted(false)
    setMessages([])
    setInput('')
    setError('')
    setScenario('')
  }

  function handleKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  // Scenario picker
  if (!started) {
    return (
      <div className="conv-picker">
        <h2 className="conv-title">Conversation Practice</h2>
        <p className="conv-subtitle">
          Choose a scenario and practice speaking Italian with an AI tutor.
          It'll draw parallels to Spanish to help you learn faster!
        </p>

        <div className="scenario-grid">
          {SCENARIOS.map(s => (
            <button
              key={s.id}
              className={`scenario-card ${scenario === s.id ? 'selected' : ''}`}
              onClick={() => setScenario(s.id)}
            >
              <span className="scenario-label">{s.label}</span>
              <span className="scenario-desc">{s.description}</span>
            </button>
          ))}
        </div>

        {error && <p className="conv-error">{error}</p>}

        <button
          className="conv-start-btn"
          onClick={startConversation}
          disabled={!scenario}
        >
          Start Practice →
        </button>
      </div>
    )
  }

  // Chat UI
  return (
    <div className="conv-chat">
      <div className="conv-chat-header">
        <span className="conv-scenario-tag">{SCENARIO_LABELS[scenario]}</span>
        <button className="conv-reset-btn" onClick={reset}>← Change Scenario</button>
      </div>

      <div className="conv-messages">
        {messages.map((m, i) => (
          <div key={i} className={`conv-message ${m.role}`}>
            <div className="conv-bubble">
              {m.text.split('\n').map((line, j) => (
                <p key={j}>{line}</p>
              ))}
            </div>
            <span className="conv-role-label">{m.role === 'assistant' ? 'Tutor' : 'You'}</span>
          </div>
        ))}
        {loading && (
          <div className="conv-message assistant">
            <div className="conv-bubble conv-loading">
              <span className="dot" /><span className="dot" /><span className="dot" />
            </div>
          </div>
        )}
        {error && <p className="conv-error">{error}</p>}
        <div ref={bottomRef} />
      </div>

      <div className="conv-input-row">
        <textarea
          className="conv-input"
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Type in Italian or English... (Enter to send)"
          rows={2}
          disabled={loading}
        />
        <button
          className="conv-send-btn"
          onClick={sendMessage}
          disabled={loading || !input.trim()}
        >
          Send
        </button>
      </div>
      <p className="conv-tip">Tip: Try typing in Italian — even broken Italian! The tutor will help you improve.</p>
    </div>
  )
}
