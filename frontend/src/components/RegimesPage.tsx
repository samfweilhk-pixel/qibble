import { useState, useEffect } from 'react'
import { ResponsiveContainer, ComposedChart, BarChart, Bar, Line, Area, XAxis, YAxis, CartesianGrid, Tooltip, Cell } from 'recharts'
import { fetchRegimes, fetchVolumeTrend } from '../api'
import ChartCard from './ChartCard'

const REGIME_COLORS: Record<string, string> = { BULL: '#00ff88', BEAR: '#ff3366', CHOP: '#ffd700' }

export default function RegimesPage() {
  const [regimes, setRegimes] = useState<any>(null)
  const [volTrend, setVolTrend] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([fetchRegimes(), fetchVolumeTrend()])
      .then(([r, vt]) => {
        setRegimes(r)
        setVolTrend(vt)
        setLoading(false)
      })
  }, [])

  if (loading) return <div className="flex items-center justify-center py-20"><div className="text-accent-cyan text-sm animate-pulse">Loading regimes...</div></div>

  const periods = regimes?.periods || []
  const summary = regimes?.summary || {}

  const regimeStats = ['BULL', 'BEAR', 'CHOP'].map(r => {
    const periodsList = periods.filter((p: any) => p.regime === r)
    const avgReturn = periodsList.length > 0 ? periodsList.reduce((s: number, p: any) => s + p.return_pct, 0) / periodsList.length : 0
    return { regime: r, n_periods: periodsList.length, n_days: summary[r]?.n_days || 0, pct: summary[r]?.pct || 0, avg_return: avgReturn }
  })

  return (
    <div className="flex-1 overflow-y-auto p-3 md:p-4 flex flex-col gap-3 md:gap-4">
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

      {/* Regime Timeline */}
      <ChartCard
        title="Regime Timeline"
        description="BTC price colored by market regime. Green = Bull (30-day return above +10%, price trending up). Red = Bear (30-day return below -10%, price trending down). Yellow = Chop (sideways, between -10% and +10%). A regime must persist for 14+ days before switching to avoid false signals from short-term swings."
        height="h-[260px] md:h-[360px]"
      >
        <div className="flex gap-4 mb-2 text-[10px]">
          <span style={{ color: '#00ff88' }}>● Bull (up 10%+)</span>
          <span style={{ color: '#ff3366' }}>● Bear (down 10%+)</span>
          <span style={{ color: '#ffd700' }}>● Chop (sideways)</span>
        </div>
        <ResponsiveContainer>
          <BarChart data={volTrend} margin={{ top: 5, right: 15, left: 15, bottom: 35 }} barCategoryGap={0} barGap={0}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis dataKey="date_str" tick={{ fontSize: 9, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} interval={Math.floor((volTrend?.length || 1) / 8)} label={{ value: 'Date', position: 'bottom', fill: '#666', fontSize: 10, dy: 15 }} />
            <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `$${(v / 1000).toFixed(0)}k`} label={{ value: 'BTC Price (USD)', angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9, dx: -5, style: { textAnchor: 'middle' } }} />
            <Bar dataKey="close_px" isAnimationActive={false}>
              {(volTrend || []).map((d: any, i: number) => (
                <Cell key={i} fill={REGIME_COLORS[d.regime] || '#555'} opacity={0.7} />
              ))}
            </Bar>
            <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} itemStyle={{ color: '#e5e5e5' }} labelStyle={{ color: '#00d4ff' }}
              formatter={(v: number) => [`$${v.toLocaleString()}`, 'Price']}
              labelFormatter={(l: string) => {
                const d = (volTrend || []).find((x: any) => x.date_str === l)
                return d ? `${l} (${d.regime})` : l
              }}
            />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Regime History Table */}
      <ChartCard
        title="Regime History"
        description="Every detected regime period with full dates, duration, and total BTC price change. Regimes are detected using 30-day rolling returns with a 14-day persistence filter to avoid whipsaw."
        height=""
      >
        <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
          <table className="w-full text-[11px]">
            <thead className="sticky top-0 bg-[#0d0d14]">
              <tr className="text-gray-500 border-b border-border">
                <th className="text-left py-2 px-3">Regime</th>
                <th className="text-left py-2 px-3">Start</th>
                <th className="text-left py-2 px-3">End</th>
                <th className="text-right py-2 px-3">Days</th>
                <th className="text-right py-2 px-3">Return</th>
              </tr>
            </thead>
            <tbody>
              {periods.map((p: any, i: number) => (
                <tr key={i} className="border-b border-border/30 hover:bg-bg-hover/30">
                  <td className="py-1.5 px-3 font-bold" style={{ color: REGIME_COLORS[p.regime] }}>{p.regime}</td>
                  <td className="py-1.5 px-3 text-gray-400">{p.start}</td>
                  <td className="py-1.5 px-3 text-gray-400">{p.end}</td>
                  <td className="py-1.5 px-3 text-gray-400 text-right">{p.n_days}</td>
                  <td className="py-1.5 px-3 text-right font-semibold" style={{ color: p.return_pct >= 0 ? '#00ff88' : '#ff3366' }}>
                    {p.return_pct >= 0 ? '+' : ''}{p.return_pct.toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ChartCard>

      {/* Volume Trend */}
      <ChartCard
        title="Volume Trend (5 Years)"
        description="Daily BTC trading volume (purple area, left axis) vs BTC price (blue line, right axis) over 5 years. Volume is measured in BTC, not dollars — when BTC price is lower, the same dollar amount of trading equals more BTC, which is why BTC-denominated volume can appear higher during lower-price periods."
        height="h-[200px] md:h-[280px]"
      >
        <ResponsiveContainer>
          <ComposedChart data={volTrend} margin={{ top: 5, right: 15, left: 15, bottom: 35 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis dataKey="date_str" tick={{ fontSize: 9, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} interval={Math.floor((volTrend?.length || 1) / 8)} label={{ value: 'Date', position: 'bottom', fill: '#666', fontSize: 10, dy: 15 }} />
            <YAxis yAxisId="vol" orientation="left" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${(v / 1000).toFixed(0)}k`} label={{ value: 'Volume (BTC)', angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9, dx: -5, style: { textAnchor: 'middle' } }} />
            <YAxis yAxisId="px" orientation="right" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `$${(v / 1000).toFixed(0)}k`} label={{ value: 'Price (USD)', angle: 90, position: 'insideRight', fill: '#555', fontSize: 9, dx: 5, style: { textAnchor: 'middle' } }} />
            <Area yAxisId="vol" dataKey="roll_vol_30d" fill="#7c3aed" fillOpacity={0.15} stroke="#7c3aed" strokeWidth={1} isAnimationActive={false} name="30d Avg Vol" />
            <Line yAxisId="px" dataKey="close_px" stroke="#00d4ff" dot={false} strokeWidth={1.5} isAnimationActive={false} name="Price" />
            <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} itemStyle={{ color: '#e5e5e5' }} labelStyle={{ color: '#00d4ff' }}
              formatter={(v: number, name: string) => {
                if (name === 'Price') return [`$${v.toLocaleString()}`, name]
                return [`${v.toFixed(0)} BTC`, name]
              }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  )
}
