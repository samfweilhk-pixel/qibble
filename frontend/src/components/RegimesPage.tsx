import { useState, useEffect } from 'react'
import { ResponsiveContainer, ComposedChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, Legend, BarChart, Bar, Cell } from 'recharts'
import { fetchRegimes, fetchRegimeDailyAll, fetchVolumeTrend } from '../api'
import ChartCard from './ChartCard'

const REGIME_COLORS: Record<string, string> = { BULL: '#00ff88', BEAR: '#ff3366', CHOP: '#ffd700' }

export default function RegimesPage() {
  const [regimes, setRegimes] = useState<any>(null)
  const [regimeDaily, setRegimeDaily] = useState<any>(null)
  const [volTrend, setVolTrend] = useState<any>(null)
  const [selectedPeriods, setSelectedPeriods] = useState<string[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([fetchRegimes(), fetchRegimeDailyAll(), fetchVolumeTrend()])
      .then(([r, rd, vt]) => {
        setRegimes(r)
        setRegimeDaily(rd)
        setVolTrend(vt)
        // Auto-select first 3 periods
        const keys = Object.keys(rd).slice(0, 3)
        setSelectedPeriods(keys)
        setLoading(false)
      })
  }, [])

  if (loading) return <div className="flex items-center justify-center py-20"><div className="text-accent-cyan text-sm animate-pulse">Loading regimes...</div></div>

  const periods = regimes?.periods || []
  const summary = regimes?.summary || {}

  // Regime stats cards
  const regimeStats = ['BULL', 'BEAR', 'CHOP'].map(r => {
    const periodsList = periods.filter((p: any) => p.regime === r)
    const avgReturn = periodsList.length > 0 ? periodsList.reduce((s: number, p: any) => s + p.return_pct, 0) / periodsList.length : 0
    return { regime: r, n_periods: periodsList.length, n_days: summary[r]?.n_days || 0, pct: summary[r]?.pct || 0, avg_return: avgReturn }
  })

  // Period overlay data
  const OVERLAY_COLORS = ['#00d4ff', '#ff3366', '#00ff88', '#ffd700', '#7c3aed', '#ff8c00']

  const togglePeriod = (key: string) => {
    setSelectedPeriods(prev =>
      prev.includes(key) ? prev.filter(k => k !== key) : [...prev, key]
    )
  }

  return (
    <div className="flex flex-1 overflow-hidden">
      {/* Sidebar — period selector */}
      <aside className="hidden md:block w-64 border-r border-border p-3 flex-shrink-0 overflow-y-auto">
        <span className="text-xs text-accent-cyan uppercase tracking-wider font-semibold block mb-2">Select Periods</span>
        <div className="flex flex-col gap-0.5">
          {Object.entries(regimeDaily || {}).map(([key, val]: [string, any], idx) => {
            const active = selectedPeriods.includes(key)
            const color = OVERLAY_COLORS[selectedPeriods.indexOf(key) % OVERLAY_COLORS.length]
            const regimeColor = REGIME_COLORS[val.regime]
            return (
              <button key={key} onClick={() => togglePeriod(key)}
                className={`flex items-center gap-2 px-2 py-1.5 rounded text-left text-[10px] transition-all ${
                  active ? 'bg-bg-hover border border-border-bright' : 'border border-transparent hover:bg-bg-hover'
                }`}
              >
                <div className="w-2 h-2 rounded-sm" style={{ backgroundColor: active ? color : '#333' }} />
                <span className="text-gray-400 flex-1">{val.start.slice(5)} → {val.end.slice(5)}</span>
                <span className="font-bold" style={{ color: regimeColor }}>{val.regime[0]}</span>
              </button>
            )
          })}
        </div>
      </aside>

      <main className="flex-1 overflow-y-auto p-3 md:p-4 flex flex-col gap-3 md:gap-4">
        {/* Regime summary cards */}
        <div className="grid grid-cols-3 gap-3">
          {regimeStats.map(r => (
            <div key={r.regime} className="card p-3">
              <div className="text-[10px] uppercase tracking-wider font-bold mb-1" style={{ color: REGIME_COLORS[r.regime] }}>
                {r.regime}
              </div>
              <div className="text-sm font-semibold text-gray-200">{r.pct}%</div>
              <div className="text-[9px] text-gray-500">{r.n_days} days / {r.n_periods} periods</div>
              <div className="text-[9px] text-gray-500">Avg return: {r.avg_return >= 0 ? '+' : ''}{r.avg_return.toFixed(1)}%</div>
            </div>
          ))}
        </div>

        {/* Period flow overlay */}
        <ChartCard
          title="Regime Flow Overlay"
          description="Compares buying pressure vs price across different market regimes. Shows whether flow leads or follows price in bull, bear, and chop markets."
          height="h-[250px] md:h-[350px]"
        >
          <ResponsiveContainer>
            <ComposedChart>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
              <XAxis dataKey="day_num" type="number" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: 'Day within period', position: 'bottom', fill: '#555', fontSize: 9 }} />
              <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${v.toFixed(0)}%`} />
              <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
              {selectedPeriods.map((key, idx) => {
                const pd = regimeDaily[key]
                if (!pd) return null
                const color = OVERLAY_COLORS[idx % OVERLAY_COLORS.length]
                return (
                  <Line key={key} data={pd.days} dataKey="cum_return" stroke={color} strokeWidth={1.5} dot={false}
                    name={`${pd.regime} ${pd.start.slice(5)}→${pd.end.slice(5)}`} isAnimationActive={false} />
                )
              })}
              <Legend wrapperStyle={{ fontSize: 9 }} />
              <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11 }} labelStyle={{ color: '#00d4ff' }}
                formatter={(v: number) => [`${v.toFixed(2)}%`]}
                labelFormatter={l => `Day ${l}`}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Volume trend */}
        <ChartCard
          title="Volume Trend (5 Years)"
          description="How trading activity has changed over 5 years. Rising volume during rallies = strong conviction. Falling volume during rallies = weak hands."
          height="h-[200px] md:h-[280px]"
        >
          <ResponsiveContainer>
            <ComposedChart data={volTrend}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
              <XAxis dataKey="date_str" tick={{ fontSize: 9, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} interval={Math.floor((volTrend?.length || 1) / 8)} />
              <YAxis yAxisId="vol" orientation="left" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${(v / 1000).toFixed(0)}k`} />
              <YAxis yAxisId="px" orientation="right" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `$${(v / 1000).toFixed(0)}k`} />
              <Area yAxisId="vol" dataKey="roll_vol_30d" fill="#7c3aed" fillOpacity={0.15} stroke="#7c3aed" strokeWidth={1} isAnimationActive={false} name="30d Avg Vol" />
              <Line yAxisId="px" dataKey="close_px" stroke="#00d4ff" dot={false} strokeWidth={1.5} isAnimationActive={false} name="Price" />
              <Legend wrapperStyle={{ fontSize: 10 }} />
              <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11 }} labelStyle={{ color: '#00d4ff' }}
                formatter={(v: number, name: string) => {
                  if (name === 'Price') return [`$${v.toLocaleString()}`, name]
                  return [`${v.toFixed(0)} BTC`, name]
                }}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>
      </main>
    </div>
  )
}
