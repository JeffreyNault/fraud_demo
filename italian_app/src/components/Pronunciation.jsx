import './Pronunciation.css'

const vowels = [
  { letter: 'a', sound: 'ah', example: 'pasta', note: 'Same as Spanish — clear and open' },
  { letter: 'e', sound: 'eh', example: 'bene (well)', note: 'Always clear, never reduced — like Spanish' },
  { letter: 'i', sound: 'ee', example: 'vino (wine)', note: 'Same as Spanish "i"' },
  { letter: 'o', sound: 'oh', example: 'solo (alone)', note: 'Round and open — like Spanish' },
  { letter: 'u', sound: 'oo', example: 'uno (one)', note: 'Always "oo" — never "yoo" like English' },
]

const consonants = [
  {
    combo: 'c + e / i',
    sound: '"ch"',
    examples: [{ word: 'cena', meaning: 'dinner', phonetic: 'CHEH-na' }, { word: 'ciao', meaning: 'hi/bye', phonetic: 'chow' }],
    spanish: 'Like "ch" in some dialects, but NOT like the "th" or "s" of standard Spanish',
    color: '#ce2b37',
  },
  {
    combo: 'c + a / o / u',
    sound: '"k"',
    examples: [{ word: 'casa', meaning: 'house', phonetic: 'KA-za' }, { word: 'conto', meaning: 'bill', phonetic: 'KON-to' }],
    spanish: 'Same hard "k" as Spanish',
    color: '#009246',
  },
  {
    combo: 'ch',
    sound: 'always "k"',
    examples: [{ word: 'chi', meaning: 'who', phonetic: 'kee' }, { word: 'chiave', meaning: 'key', phonetic: 'KYAH-veh' }],
    spanish: 'DIFFERENT from Spanish "ch" — Italian "ch" is always hard "k"',
    color: '#ce2b37',
  },
  {
    combo: 'g + e / i',
    sound: '"j" (soft)',
    examples: [{ word: 'gelato', meaning: 'ice cream', phonetic: 'jeh-LAH-to' }, { word: 'giro', meaning: 'tour', phonetic: 'JEE-ro' }],
    spanish: 'Like English "j" — softer than Spanish "j" (which is more guttural)',
    color: '#ce2b37',
  },
  {
    combo: 'g + a / o / u',
    sound: 'hard "g"',
    examples: [{ word: 'gatto', meaning: 'cat', phonetic: 'GAT-to' }, { word: 'gondola', meaning: 'gondola', phonetic: 'GON-do-la' }],
    spanish: 'Same hard "g" as Spanish',
    color: '#009246',
  },
  {
    combo: 'gh',
    sound: 'always hard "g"',
    examples: [{ word: 'spaghetti', meaning: 'spaghetti', phonetic: 'spa-GET-tee' }, { word: 'ghiaccio', meaning: 'ice', phonetic: 'GYAT-cho' }],
    spanish: '"gh" keeps the "g" hard before e/i — same idea as "gu" in Spanish',
    color: '#009246',
  },
  {
    combo: 'gli',
    sound: '"lly" (like million)',
    examples: [{ word: 'gli', meaning: 'the (plural)', phonetic: 'lyee' }, { word: 'figlio', meaning: 'son', phonetic: 'FEEL-yo' }],
    spanish: 'No Spanish equivalent — closest is "ll" in some Latin American accents',
    color: '#ce2b37',
  },
  {
    combo: 'gn',
    sound: '"ny"',
    examples: [{ word: 'gnocchi', meaning: 'gnocchi', phonetic: 'NYOK-kee' }, { word: 'bagno', meaning: 'bathroom', phonetic: 'BAN-yo' }],
    spanish: 'Like Spanish "ñ" — this one is easy for Spanish speakers!',
    color: '#009246',
  },
  {
    combo: 'sc + e / i',
    sound: '"sh"',
    examples: [{ word: 'scena', meaning: 'scene', phonetic: 'SHEH-na' }, { word: 'uscita', meaning: 'exit', phonetic: 'oo-SHEE-ta' }],
    spanish: 'No direct equivalent in Spanish — new sound to learn',
    color: '#ce2b37',
  },
  {
    combo: 'z',
    sound: '"ts" or "dz"',
    examples: [{ word: 'pizza', meaning: 'pizza', phonetic: 'PEET-tsa' }, { word: 'zero', meaning: 'zero', phonetic: 'DZEH-ro' }],
    spanish: 'DIFFERENT from Spanish "z" — Italian "z" = "ts" or "dz"',
    color: '#ce2b37',
  },
  {
    combo: 'r',
    sound: 'rolled',
    examples: [{ word: 'Roma', meaning: 'Rome', phonetic: 'ROH-ma' }, { word: 'arrivederci', meaning: 'goodbye', phonetic: 'ah-ree-veh-DER-chee' }],
    spanish: 'Same as Spanish "r" — you already know this one!',
    color: '#009246',
  },
]

const doubleCons = [
  { pair: ['pala', 'palla'], meanings: ['shovel', 'ball'] },
  { pair: ['nono', 'nonno'], meanings: ['ninth', 'grandfather'] },
  { pair: ['fato', 'fatto'], meanings: ['fate', 'done/made'] },
  { pair: ['caro', 'carro'], meanings: ['dear/expensive', 'cart/car'] },
]

export default function Pronunciation() {
  return (
    <div className="pronunciation">
      <h2 className="pron-title">Italian Pronunciation Guide</h2>
      <p className="pron-intro">
        Good news: Italian vowels are <strong>identical to Spanish</strong> — always clear and consistent.
        Italian also rolls its R's just like Spanish. Focus on the consonant combos below that differ.
      </p>

      {/* Vowels */}
      <section className="pron-section">
        <h3 className="pron-section-title">Vowels — Same as Spanish!</h3>
        <div className="vowel-grid">
          {vowels.map(v => (
            <div key={v.letter} className="vowel-card">
              <span className="vowel-letter">{v.letter}</span>
              <span className="vowel-sound">/{v.sound}/</span>
              <span className="vowel-example">e.g. <em>{v.example}</em></span>
              <span className="vowel-note">{v.note}</span>
            </div>
          ))}
        </div>
      </section>

      {/* Key consonant combos */}
      <section className="pron-section">
        <h3 className="pron-section-title">Key Consonant Combinations</h3>
        <div className="legend">
          <span className="badge badge-new">New for Spanish speakers</span>
          <span className="badge badge-same">Same as Spanish</span>
        </div>
        <div className="cons-list">
          {consonants.map(c => (
            <div key={c.combo} className={`cons-row ${c.color === '#009246' ? 'same' : 'new'}`}>
              <div className="cons-combo">{c.combo}</div>
              <div className="cons-detail">
                <div className="cons-sound">Sounds like: <strong>{c.sound}</strong></div>
                <div className="cons-examples">
                  {c.examples.map(ex => (
                    <span key={ex.word} className="cons-example">
                      <em>{ex.word}</em> ({ex.meaning}) → /{ex.phonetic}/
                    </span>
                  ))}
                </div>
                <div className="cons-spanish">{c.spanish}</div>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Double consonants */}
      <section className="pron-section">
        <h3 className="pron-section-title">Double Consonants — Critical in Italian!</h3>
        <p className="pron-intro">
          Unlike Spanish, double consonants in Italian are held <strong>twice as long</strong>.
          Getting this wrong can completely change the meaning:
        </p>
        <div className="double-grid">
          {doubleCons.map(d => (
            <div key={d.pair[0]} className="double-card">
              <div className="double-pair">
                <span className="double-word single">{d.pair[0]}</span>
                <span className="double-meaning">({d.meanings[0]})</span>
              </div>
              <div className="double-vs">vs</div>
              <div className="double-pair">
                <span className="double-word double">{d.pair[1]}</span>
                <span className="double-meaning">({d.meanings[1]})</span>
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Stress */}
      <section className="pron-section">
        <h3 className="pron-section-title">Word Stress</h3>
        <p className="pron-intro">
          Like Spanish, Italian usually stresses the <strong>second-to-last syllable</strong>:
        </p>
        <div className="stress-examples">
          <div className="stress-item"><em>pa</em><strong>sta</strong> → pa-<strong>STA</strong></div>
          <div className="stress-item"><em>buon</em><strong>gior</strong>no → bwon-<strong>JOR</strong>-no</div>
          <div className="stress-item"><em>gra</em><strong>zie</strong> → <strong>GRAT</strong>-syeh</div>
          <div className="stress-item"><em>ar</em>ri<em>ve</em><strong>der</strong>ci → ah-ree-veh-<strong>DER</strong>-chee</div>
        </div>
        <p className="pron-note">
          When a word breaks this pattern, it usually carries a written accent mark: caffè, città (city), perché (why/because).
        </p>
      </section>
    </div>
  )
}
