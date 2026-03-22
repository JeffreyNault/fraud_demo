import { useState } from 'react'
import Flashcards from './components/Flashcards'
import Conversation from './components/Conversation'
import Pronunciation from './components/Pronunciation'
import './App.css'

const TABS = [
  { id: 'flashcards', label: 'Flashcards' },
  { id: 'conversation', label: 'Conversation Practice' },
  { id: 'pronunciation', label: 'Pronunciation Guide' },
]

function App() {
  const [activeTab, setActiveTab] = useState('flashcards')

  return (
    <div className="app">
      <header className="app-header">
        <div className="flag-strip">
          <div className="flag-green" />
          <div className="flag-white" />
          <div className="flag-red" />
        </div>
        <h1 className="app-title">Impara l'Italiano</h1>
        <p className="app-subtitle">Learn Italian for Your Trip to Italy</p>
        <nav className="tab-nav">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              className={`tab-btn ${activeTab === tab.id ? 'active' : ''}`}
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </header>

      <main className="app-main">
        {activeTab === 'flashcards' && <Flashcards />}
        {activeTab === 'conversation' && <Conversation />}
        {activeTab === 'pronunciation' && <Pronunciation />}
      </main>
    </div>
  )
}

export default App
