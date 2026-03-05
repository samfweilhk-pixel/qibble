import { useState, useEffect } from 'react'
import { ResponsiveContainer, ComposedChart, BarChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, Cell, Legend } from 'recharts'
import { fetchCorrDivergence, fetchLeadLag, fetchFlowExtremes, fetchFlowPersistence } from '../api'
import RegimeSelector from './RegimeSelector'
import ChartCard from './ChartCard'
import type { Regime } from '../types'

export default function FlowSignalsPage() {
  const [regime, setRegime] = useState<Regime>('BULL')
  const [divergence, setDivergence] = useState<any>(null)
  const [leadLag, setLeadLag] = useState<any>(null)
  const [extremes, setExtremes] = useState<any>(null)
  const [persistence, setPersistence] = useState<any>(null)
  const [threshold, setThreshold] = useState('-1.5')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([fetchCorrDivergence(), fetchLeadLag(), fetchFlowExtremes(), fetchFlowPersistence()])
      .then(([div, ll, ext, per]) => {
        setDivergence(div)
        setLeadLag(ll)
        setExtremes(ext)
        setPersistence(per)
        setLoading(false)
      })
  }, [])

  if (loading) return <div className="flex items-center justify-center py-20"><div className="text-accent-cyan text-sm animate-pulse">Loading flow signals...</div></div>

  const divData = divergence?.[regime]?.thresholds?.[threshold]
  const llData = leadLag?.[regime]?.bars || []
  const extData = extremes?.[regime]
  const perData = persistence?.[regime] || []

  return (
    <div className="flex-1 overflow-y-auto p-3 md:p-4 flex flex-col gap-3 md:gap-4">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <RegimeSelector selected={regime} onSelect={setRegime} />
      </div>

      {/* Divergence */}
      <ChartCard
        title="Flow-Price Divergence"
        description="Detects when buying/selling pressure disconnects from price. Green = buyers active but price flat or down (expect bounce). Red = sellers active but price flat or up (expect drop). Shows average price move over next 1-10 minutes after each event."
        height="h-[220px] md:h-[300px]"
      >
        <div className="flex gap-2 mb-2">
          {['-1.0', '-1.5', '-2.0'].map(t => (
            <button key={t} onClick={() => setThreshold(t)}
              className={`px-2 py-1 text-[9px] font-bold tracking-wider rounded ${threshold === t ? 'bg-accent-cyan/10 text-accent-cyan border border-accent-cyan/30' : 'text-gray-500 border border-transparent'}`}
            >{t}σ</button>
          ))}
        </div>
        <ResponsiveContainer>
          <ComposedChart>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis dataKey="bar" type="number" domain={[1, 10]} tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: 'Minutes ahead', position: 'bottom', fill: '#555', fontSize: 9 }} />
            <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${v.toFixed(0)} bps`} />
            <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
            {divData?.bullish?.curve && (
              <Line data={divData.bullish.curve} dataKey="avg_bps" stroke="#00ff88" strokeWidth={2} dot={{ r: 3, fill: '#00ff88' }} name={`Bullish (${divData.bullish.n})`} isAnimationActive={false} />
            )}
            {divData?.bearish?.curve && (
              <Line data={divData.bearish.curve} dataKey="avg_bps" stroke="#ff3366" strokeWidth={2} dot={{ r: 3, fill: '#ff3366' }} name={`Bearish (${divData.bearish.n})`} isAnimationActive={false} />
            )}
            <Legend wrapperStyle={{ fontSize: 10 }} />
            <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11 }} labelStyle={{ color: '#00d4ff' }} formatter={(v: number) => [`${v.toFixed(1)} bps`]} />
          </ComposedChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Lead-Lag */}
      <ChartCard
        title="Lead-Lag Cross-Correlation"
        description="Does buying pressure predict future price moves? Bars to the right of zero = flow leads price (predictive). Bars to the left = price moves first, flow follows (reactive). Taller bar = stronger relationship."
        height="h-[180px] md:h-[240px]"
      >
        <ResponsiveContainer>
          <BarChart data={llData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis dataKey="lag" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: 'Lag (minutes)', position: 'bottom', fill: '#555', fontSize: 9 }} />
            <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} />
            <ReferenceLine x={0} stroke="#555" strokeDasharray="4 4" />
            <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
            <Bar dataKey="corr" isAnimationActive={false}>
              {llData.map((d: any, i: number) => (
                <Cell key={i} fill={d.lag >= 0 ? '#00d4ff' : '#7c3aed'} opacity={0.8} />
              ))}
            </Bar>
            <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11 }} labelStyle={{ color: '#00d4ff' }}
              formatter={(v: number) => [`${v.toFixed(4)}`]}
              labelFormatter={(l) => `Lag: ${l} min`}
            />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 md:gap-4">
        {/* Flow Extremes */}
        <ChartCard
          title="Extreme Flow Events"
          description="What happens to price after unusually large buying or selling? The curve shows average price change 1-10 minutes after flow spikes to 2x normal."
          height="h-[200px] md:h-[260px]"
        >
          <ResponsiveContainer>
            <ComposedChart>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
              <XAxis dataKey="bar" type="number" domain={[1, 10]} tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} />
              <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${v.toFixed(0)} bps`} />
              <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
              {extData?.fwd_curve_buy && (
                <Line data={extData.fwd_curve_buy} dataKey="avg_bps" stroke="#00ff88" strokeWidth={2} dot={{ r: 3, fill: '#00ff88' }} name={`Buy ext (${extData.n_buy})`} isAnimationActive={false} />
              )}
              {extData?.fwd_curve_sell && (
                <Line data={extData.fwd_curve_sell} dataKey="avg_bps" stroke="#ff3366" strokeWidth={2} dot={{ r: 3, fill: '#ff3366' }} name={`Sell ext (${extData.n_sell})`} isAnimationActive={false} />
              )}
              <Legend wrapperStyle={{ fontSize: 10 }} />
              <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11 }} labelStyle={{ color: '#00d4ff' }} formatter={(v: number) => [`${v.toFixed(1)} bps`]} />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Flow Persistence */}
        <ChartCard
          title="Flow Persistence (ACF)"
          description="How long does a burst of buying or selling last? High bars at lag 1-5 = pressure continues for several minutes. Bars near zero = each minute is independent."
          height="h-[200px] md:h-[260px]"
        >
          <ResponsiveContainer>
            <BarChart data={perData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
              <XAxis dataKey="lag" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: 'Lag (minutes)', position: 'bottom', fill: '#555', fontSize: 9 }} />
              <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} />
              <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
              <Bar dataKey="acf" fill="#7c3aed" opacity={0.8} isAnimationActive={false} />
              <Tooltip contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11 }} labelStyle={{ color: '#00d4ff' }}
                formatter={(v: number) => [`${v.toFixed(4)}`]}
                labelFormatter={(l) => `Lag: ${l} min`}
              />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </div>
  )
}
