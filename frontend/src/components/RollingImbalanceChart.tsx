import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine } from 'recharts'
import ChartCard from './ChartCard'
import type { Bar } from '../types'

export default function RollingImbalanceChart({ bars }: { bars: Bar[] }) {
  const ticks = bars.filter((_, i) => i % 60 === 0).map(b => b.time)

  return (
    <ChartCard
      title="Rolling Imbalance (20-bar)"
      description="Average buy/sell imbalance over the last 20 minutes (smoothed version of the Bar Imbalance chart). Positive = buying has dominated recently. Negative = selling has dominated. This filters out minute-to-minute noise to show the broader pressure trend."
      height="h-[150px] md:h-[200px]"
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={bars} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
          <defs>
            <linearGradient id="imbGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#7c3aed" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#7c3aed" stopOpacity={0.05} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
          <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} ticks={ticks} />
          <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: 'Imbalance', angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9, dx: -5 }} />
          <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
          <Area dataKey="rolling_imb_20" stroke="#7c3aed" strokeWidth={2} fill="url(#imbGrad)" isAnimationActive={false} />
          <Tooltip
            contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} itemStyle={{ color: '#e5e5e5' }}
            labelStyle={{ color: '#00d4ff' }}
            formatter={(val: number) => [`${(val * 100).toFixed(1)}%`, 'Rolling Imb']}
          />
        </AreaChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}
