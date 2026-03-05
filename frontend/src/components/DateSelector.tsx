import type { DateSummary } from '../types'

const REGIME_BADGE: Record<string, { bg: string; text: string }> = {
  BULL: { bg: 'bg-accent-green/10', text: 'text-accent-green' },
  BEAR: { bg: 'bg-accent-red/10', text: 'text-accent-red' },
  CHOP: { bg: 'bg-accent-yellow/10', text: 'text-accent-yellow' },
}

interface Props {
  dates: DateSummary[]
  selected: string
  onSelect: (d: string) => void
  regimeFilter: string
  onRegimeFilter: (r: string) => void
}

export default function DateSelector({ dates, selected, onSelect, regimeFilter, onRegimeFilter }: Props) {
  const filtered = regimeFilter ? dates.filter(d => d.regime === regimeFilter) : dates

  return (
    <div className="flex flex-col gap-2">
      <span className="text-xs text-accent-cyan uppercase tracking-wider font-semibold">Select Date</span>

      {/* Regime filter */}
      <div className="flex gap-1.5 mb-1">
        {['', 'BULL', 'BEAR', 'CHOP'].map(r => (
          <button
            key={r}
            onClick={() => onRegimeFilter(r)}
            className={`px-2 py-1 text-[9px] font-bold tracking-wider rounded transition-all ${
              regimeFilter === r
                ? 'bg-accent-cyan/10 text-accent-cyan border border-accent-cyan/30'
                : 'text-gray-500 hover:text-gray-300 border border-transparent'
            }`}
          >
            {r || 'ALL'}
          </button>
        ))}
      </div>

      {/* Date list */}
      <div className="flex flex-col gap-0.5 max-h-[calc(100vh-200px)] overflow-y-auto">
        {filtered.map(d => {
          const badge = REGIME_BADGE[d.regime] || REGIME_BADGE.CHOP
          const active = d.date === selected
          return (
            <button
              key={d.date}
              onClick={() => onSelect(d.date)}
              className={`flex items-center justify-between px-2 py-1.5 rounded text-left transition-all ${
                active
                  ? 'bg-accent-cyan/10 border border-accent-cyan/30 text-accent-cyan font-semibold'
                  : 'border border-transparent text-gray-500 hover:text-gray-300 hover:bg-bg-hover'
              }`}
            >
              <span className="text-[11px]">{d.date}</span>
              <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[8px] font-bold tracking-wider ${badge.bg} ${badge.text}`}>
                {d.regime}
              </span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
