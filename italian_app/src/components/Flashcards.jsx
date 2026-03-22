import { useState, useMemo } from 'react'
import { phrases, categories } from '../data/phrases'
import './Flashcards.css'

function shuffle(arr) {
  const a = [...arr]
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]]
  }
  return a
}

export default function Flashcards() {
  const [category, setCategory] = useState('all')
  const [index, setIndex] = useState(0)
  const [flipped, setFlipped] = useState(false)
  const [order, setOrder] = useState(null) // null = original order

  const filtered = useMemo(() => {
    const base = category === 'all' ? phrases : phrases.filter(p => p.category === category)
    return order ? order.filter(p => category === 'all' || p.category === category) : base
  }, [category, order])

  const card = filtered[index] || null

  function handleCategory(cat) {
    setCategory(cat)
    setIndex(0)
    setFlipped(false)
    setOrder(null)
  }

  function handleShuffle() {
    const base = category === 'all' ? phrases : phrases.filter(p => p.category === category)
    setOrder(shuffle(base))
    setIndex(0)
    setFlipped(false)
  }

  function go(dir) {
    setIndex(i => Math.max(0, Math.min(filtered.length - 1, i + dir)))
    setFlipped(false)
  }

  if (!card) return <p>No cards found.</p>

  return (
    <div className="flashcards">
      <div className="fc-controls">
        <select
          value={category}
          onChange={e => handleCategory(e.target.value)}
          className="fc-select"
        >
          {categories.map(c => (
            <option key={c.id} value={c.id}>{c.label}</option>
          ))}
        </select>
        <button className="fc-btn-secondary" onClick={handleShuffle}>Shuffle</button>
      </div>

      <p className="fc-progress">{index + 1} / {filtered.length}</p>

      <div
        className={`fc-card ${flipped ? 'flipped' : ''}`}
        onClick={() => setFlipped(f => !f)}
        role="button"
        tabIndex={0}
        onKeyDown={e => e.key === 'Enter' || e.key === ' ' ? setFlipped(f => !f) : null}
        aria-label={flipped ? 'Back of card — click to flip' : 'Front of card — click to flip'}
      >
        <div className="fc-card-inner">
          {/* FRONT */}
          <div className="fc-face fc-front">
            <span className="fc-label">English</span>
            <p className="fc-english">{card.english}</p>
            <p className="fc-hint">Click to reveal Italian</p>
          </div>

          {/* BACK */}
          <div className="fc-face fc-back">
            <div className="fc-back-content">
              <div className="fc-italian-block">
                <span className="fc-label">Italian</span>
                <p className="fc-italian">{card.italian}</p>
                <p className="fc-phonetic">/{card.phonetic}/</p>
              </div>

              {card.spanish && (
                <div className="fc-spanish-block">
                  <span className="fc-label fc-label-es">Spanish (you know this!)</span>
                  <p className="fc-spanish">{card.spanish}</p>
                </div>
              )}

              {card.note && (
                <div className="fc-note">
                  <span>Tip: </span>{card.note}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="fc-nav">
        <button
          className="fc-btn"
          onClick={() => go(-1)}
          disabled={index === 0}
        >
          ← Previous
        </button>
        <button
          className="fc-btn fc-btn-primary"
          onClick={() => setFlipped(f => !f)}
        >
          {flipped ? 'Show English' : 'Show Italian'}
        </button>
        <button
          className="fc-btn"
          onClick={() => go(1)}
          disabled={index === filtered.length - 1}
        >
          Next →
        </button>
      </div>
    </div>
  )
}
