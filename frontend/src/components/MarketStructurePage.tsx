import { useState, useEffect } from 'react'
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, ScatterChart, Scatter, Cell, ComposedChart, Line, Area, Legend } from 'recharts'
import { fetchFlowTod, fetchSessionPerf, fetchSessionFlowFwd, fetchWhaleActivity, fetchFlowClassification } from '../api'
import RegimeSelector from './RegimeSelector'
import ChartCard from './ChartCard'
import type { Regime } from '../types'

const SESSION_COLORS: Record<string, string> = { ASIA: '#7c3aed', EUROPE: '#00d4ff', US: '#ffd700' }

export default function MarketStructurePage() {
  const [regime, setRegime] = useState<Regime>('BULL')
  const [tod, setTod] = useState<any>(null)
  const [sessPerf, setSessPerf] = useState<any>(null)
  const [sessFwd, setSessFwd] = useState<any>(null)
  const [whale, setWhale] = useState<any>(null)
  const [flowClass, setFlowClass] = useState<any>(null)
  const [todMetric, setTodMetric] = useState<'avg_net_flow' | 'avg_bar_imb' | 'avg_volume' | 'avg_trade_size'>('avg_net_flow')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([fetchFlowTod(), fetchSessionPerf(), fetchSessionFlowFwd(), fetchWhaleActivity(), fetchFlowClassification()])
      .then(([t, sp, sf, w, fc]) => { setTod(t); setSessPerf(sp); setSessFwd(sf); setWhale(w); setFlowClass(fc); setLoading(false) })
  }, [])

  if (loading) return <div className="flex items-center justify-center py-20"><div className="text-accent-cyan text-sm animate-pulse">Loading market structure...</div></div>

  const todData = tod?.[regime] || []
  const sessData = sessPerf?.[regime]
  const sessFwdData = sessFwd?.[regime]
  const flowClassData = flowClass?.[regime]

  const metricLabels: Record<string, string> = {
    avg_net_flow: 'NET FLOW',
    avg_bar_imb: 'IMBALANCE',
    avg_volume: 'VOLUME',
    avg_trade_size: 'AVG TRADE SIZE',
  }

  const metricYLabels: Record<string, string> = {
    avg_net_flow: 'Net Flow (BTC)',
    avg_bar_imb: 'Imbalance',
    avg_volume: 'Volume (BTC)',
    avg_trade_size: 'Avg Trade Size (BTC)',
  }

  // Session performance bars
  const sessChartData = sessData
    ? ['ASIA', 'EUROPE', 'US'].map(s => ({ session: s, return_pct: sessData[s]?.avg_return || 0, volume: sessData[s]?.avg_volume || 0 }))
    : []

  // Flow classification bars
  const classChartData = flowClassData
    ? [
        { label: 'Aligned', pct: flowClassData.aligned_pct, fill: '#00ff88' },
        { label: 'Divergent', pct: flowClassData.divergent_pct, fill: '#ff3366' },
        { label: 'Neutral', pct: flowClassData.neutral_pct, fill: '#555' },
      ]
    : []

  return (
    <div className="flex-1 overflow-y-auto p-3 md:p-4 flex flex-col gap-3 md:gap-4">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <RegimeSelector selected={regime} onSelect={setRegime} />
      </div>

      {/* Time-of-Day Profile */}
      <ChartCard
        title="Time-of-Day Profile"
        description="Average trading patterns by hour of day across all days in this regime. Shows when buyers vs sellers tend to dominate, and when volume is heaviest."
        height="h-[220px] md:h-[300px]"
      >
        <div className="flex gap-2 mb-2">
          {Object.entries(metricLabels).map(([k, v]) => (
            <button key={k} onClick={() => setTodMetric(k as any)}
              className={`px-2 py-1 text-[8px] font-bold tracking-wider rounded ${todMetric === k ? 'bg-accent-cyan/10 text-accent-cyan border border-accent-cyan/30' : 'text-gray-500 border border-transparent'}`}
            >{v}</button>
          ))}
        </div>
        <ResponsiveContainer>
          <BarChart data={todData} margin={{ top: 5, right: 10, left: 10, bottom: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis dataKey="hour" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={h => `${String(h).padStart(2, '0')}:00`} label={{ value: 'Hour (UTC)', position: 'bottom', fill: '#555', fontSize: 9, dy: 10 }} />
            <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: metricYLabels[todMetric], angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9, dx: -5 }} />
            {todMetric === 'avg_net_flow' || todMetric === 'avg_bar_imb' ? (
              <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
            ) : null}
            <Bar dataKey={todMetric} isAnimationActive={false}>
              {todData.map((d: any, i: number) => (
                <Cell key={i} fill={
                  todMetric === 'avg_net_flow' || todMetric === 'avg_bar_imb'
                    ? (d[todMetric] >= 0 ? '#00ff88' : '#ff3366')
                    : d.hour < 8 ? '#7c3aed' : d.hour < 14 ? '#00d4ff' : '#ffd700'
                } opacity={0.8} />
              ))}
            </Bar>
            <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} labelStyle={{ color: '#00d4ff' }}
              labelFormatter={h => `${String(h).padStart(2, '0')}:00 UTC`}
              formatter={(v: number) => {
                if (todMetric === 'avg_bar_imb') return [`${(v * 100).toFixed(2)}%`, 'Imbalance']
                if (todMetric === 'avg_net_flow') return [`${v.toFixed(2)} BTC`, 'Net Flow']
                if (todMetric === 'avg_volume') return [`${v.toFixed(2)} BTC`, 'Volume']
                return [`${v.toFixed(4)} BTC`, 'Avg Trade Size']
              }}
            />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 md:gap-4">
        {/* Session Performance */}
        <ChartCard
          title="Session Performance"
          description="Which part of the world drives BTC's daily move? Shows average return during each trading session."
          height="h-[180px] md:h-[220px]"
        >
          <ResponsiveContainer>
            <BarChart data={sessChartData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
              <XAxis dataKey="session" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} />
              <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${v.toFixed(2)}%`} label={{ value: 'Avg Return %', angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9, dx: -5 }} />
              <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
              <Bar dataKey="return_pct" isAnimationActive={false}>
                {sessChartData.map((d, i) => (
                  <Cell key={i} fill={SESSION_COLORS[d.session]} opacity={0.8} />
                ))}
              </Bar>
              <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} labelStyle={{ color: '#00d4ff' }}
                formatter={(v: number) => [`${v.toFixed(3)}%`, 'Avg Return']}
              />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Flow Classification */}
        <ChartCard
          title="Daily Flow Classification"
          description="On how many days does buying pressure actually move price up? 'Aligned' = flow and price move together. 'Divergent' = they move opposite — often a reversal signal."
          height="h-[180px] md:h-[220px]"
        >
          <ResponsiveContainer>
            <BarChart data={classChartData} layout="vertical" margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
              <XAxis type="number" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${v}%`} label={{ value: '% of Days', position: 'bottom', fill: '#555', fontSize: 9 }} />
              <YAxis type="category" dataKey="label" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} width={70} />
              <Bar dataKey="pct" isAnimationActive={false}>
                {classChartData.map((d, i) => (
                  <Cell key={i} fill={d.fill} opacity={0.8} />
                ))}
              </Bar>
              <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} labelStyle={{ color: '#00d4ff' }}
                formatter={(v: number) => [`${v.toFixed(1)}%`, '% of Days']}
              />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Session Flow → Next Session */}
      {sessFwdData && (
        <ChartCard
          title="Session Flow → Next Session Return"
          description="Does buying pressure in one session predict the next session's price move? Each dot is one day. A clear upward slope means the flow signal carries over. The correlation score ranges from -1 to +1: closer to +1 = strong predictive link, near 0 = no link."
          height="h-[200px] md:h-[260px]"
        >
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 h-full">
            {['asia_to_europe', 'europe_to_us'].map(pair => {
              const data = sessFwdData[pair]
              if (!data) return null
              const fromSession = pair === 'asia_to_europe' ? 'Asia' : 'Europe'
              const toSession = pair === 'asia_to_europe' ? 'Europe' : 'US'
              const corrStr = data.correlation.toFixed(3)
              const corrStrength = Math.abs(data.correlation) < 0.05 ? 'very weak' : Math.abs(data.correlation) < 0.15 ? 'weak' : 'moderate'
              return (
                <div key={pair} className="flex flex-col">
                  <span className="text-[9px] text-gray-400 mb-1">{fromSession} Flow → {toSession} Return — correlation: {corrStr} ({corrStrength})</span>
                  <ResponsiveContainer>
                    <ScatterChart margin={{ top: 5, right: 10, left: 10, bottom: 20 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
                      <XAxis dataKey="flow" type="number" tick={{ fontSize: 9, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} name="Flow" label={{ value: `${fromSession} Net Flow (BTC)`, position: 'bottom', fill: '#555', fontSize: 8, dy: 10 }} />
                      <YAxis dataKey="return" type="number" tick={{ fontSize: 9, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} name="Return %" label={{ value: `${toSession} Return %`, angle: -90, position: 'insideLeft', fill: '#555', fontSize: 8, dx: -5 }} />
                      <Scatter data={data.points} fill="#00d4ff" opacity={0.4} isAnimationActive={false} />
                      <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} labelStyle={{ color: '#00d4ff' }}
                        formatter={(v: number, name: string) => {
                          if (name === 'Flow') return [`${v.toFixed(1)} BTC`, 'Net Flow']
                          return [`${v.toFixed(3)}%`, 'Return']
                        }}
                      />
                    </ScatterChart>
                  </ResponsiveContainer>
                </div>
              )
            })}
          </div>
        </ChartCard>
      )}

      {/* Whale Activity */}
      <ChartCard
        title="Whale Activity"
        description="Detects when average trade size spikes above normal — a sign of large players entering. Compared against the prior 30 days of typical trade sizes."
        height="h-[200px] md:h-[260px]"
      >
        <ResponsiveContainer>
          <ComposedChart data={whale}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis dataKey="date_str" tick={{ fontSize: 9, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} interval={Math.floor((whale?.length || 1) / 8)} />
            <YAxis yAxisId="z" orientation="left" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: 'Z-score', angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9 }} />
            <YAxis yAxisId="px" orientation="right" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `$${(v / 1000).toFixed(0)}k`} />
            <ReferenceLine yAxisId="z" y={2} stroke="#ff3366" strokeDasharray="4 4" label={{ value: '2σ', fill: '#ff3366', fontSize: 9 }} />
            <ReferenceLine yAxisId="z" y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
            <Bar yAxisId="z" dataKey="whale_z" isAnimationActive={false}>
              {(whale || []).map((d: any, i: number) => (
                <Cell key={i} fill={d.whale_z > 2 ? '#ff8c00' : '#7c3aed'} opacity={d.whale_z > 2 ? 0.9 : 0.3} />
              ))}
            </Bar>
            <Line yAxisId="px" dataKey="close" stroke="#00d4ff" dot={false} strokeWidth={1} opacity={0.5} isAnimationActive={false} />
            <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} labelStyle={{ color: '#00d4ff' }}
              formatter={(v: number, name: string) => {
                if (name === 'close') return [`$${v.toLocaleString()}`, 'Price']
                return [`${v.toFixed(2)}σ`, 'Whale Z']
              }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  )
}
