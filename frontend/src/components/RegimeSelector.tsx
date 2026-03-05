import type { Regime } from '../types'

const REGIME_STYLES: Record<Regime, { bg: string; text: string }> = {
  BULL: { bg: 'bg-accent-green/10', text: 'text-accent-green' },
  BEAR: { bg: 'bg-accent-red/10', text: 'text-accent-red' },
  CHOP: { bg: 'bg-accent-yellow/10', text: 'text-accent-yellow' },
}

interface Props {
  selected: Regime
  onSelect: (r: Regime) => void
}

export default function RegimeSelector({ selected, onSelect }: Props) {
  return (
    <div className="flex gap-2">
      {(['BULL', 'BEAR', 'CHOP'] as Regime[]).map(r => {
        const style = REGIME_STYLES[r]
        const active = selected === r
        return (
          <button
            key={r}
            onClick={() => onSelect(r)}
            className={`px-3 py-1.5 text-[10px] font-bold tracking-wider rounded transition-all ${
              active
                ? `${style.bg} ${style.text} border border-current/30`
                : 'text-gray-500 hover:text-gray-300 border border-transparent'
            }`}
          >
            {r}
          </button>
        )
      })}
    </div>
  )
}
