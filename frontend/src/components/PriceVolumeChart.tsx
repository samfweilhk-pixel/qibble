import { ResponsiveContainer, ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine } from 'recharts'
import ChartCard from './ChartCard'
import type { Bar as BarType } from '../types'

const SESSION_LINES = [
  { time: '00:00', label: 'Asia' },
  { time: '08:00', label: 'Europe' },
  { time: '14:00', label: 'US' },
]

export default function PriceVolumeChart({ bars }: { bars: BarType[] }) {
  const ticks = bars.filter((_, i) => i % 60 === 0).map(b => b.time)

  return (
    <ChartCard
      title="Price + Volume"
      description="BTC price and total trading volume per minute. Tall bars = heavy trading activity."
    >
      <ResponsiveContainer>
        <ComposedChart data={bars}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
          <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} ticks={ticks} />
          <YAxis yAxisId="vol" orientation="left" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${v.toFixed(0)}`} label={{ value: 'Volume (BTC)', angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9, dx: -5 }} />
          <YAxis yAxisId="px" orientation="right" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} domain={['auto', 'auto']} tickFormatter={v => `$${v.toLocaleString()}`} label={{ value: 'Price (USD)', angle: 90, position: 'insideRight', fill: '#555', fontSize: 9, dx: 5 }} />
          {SESSION_LINES.map(s => (
            <ReferenceLine key={s.time} x={s.time} yAxisId="vol" stroke="#2a2a3e" strokeDasharray="4 4" label={{ value: s.label, position: 'top', fill: '#555', fontSize: 9 }} />
          ))}
          <Bar yAxisId="vol" dataKey="volume" fill="#00d4ff" opacity={0.2} isAnimationActive={false} />
          <Line yAxisId="px" dataKey="close" stroke="#00d4ff" dot={false} strokeWidth={1.5} isAnimationActive={false} />
          <Tooltip
            contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }} itemStyle={{ color: '#e5e5e5' }}
            labelStyle={{ color: '#00d4ff' }}
            formatter={(val: number, name: string) => {
              if (name === 'close') return [`$${val.toLocaleString(undefined, { minimumFractionDigits: 2 })}`, 'Price']
              return [`${val.toFixed(2)} BTC`, 'Volume']
            }}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}
