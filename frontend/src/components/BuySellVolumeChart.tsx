import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts'
import ChartCard from './ChartCard'
import type { Bar as BarType } from '../types'

export default function BuySellVolumeChart({ bars }: { bars: BarType[] }) {
  const ticks = bars.filter((_, i) => i % 60 === 0).map(b => b.time)

  return (
    <ChartCard
      title="Buy / Sell Volume"
      description="Who's more aggressive each minute — buyers or sellers? Green = buyers hitting the ask. Red = sellers hitting the bid."
      height="h-[160px] md:h-[220px]"
    >
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={bars} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
          <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} ticks={ticks} />
          <YAxis tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: 'Volume (BTC)', angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9, dx: -5 }} />
          <Bar dataKey="buy_vol" stackId="vol" fill="#00ff88" opacity={0.8} name="Buy Vol" isAnimationActive={false} />
          <Bar dataKey="sell_vol" stackId="vol" fill="#ff3366" opacity={0.8} name="Sell Vol" isAnimationActive={false} />
          <Tooltip
            contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11, color: '#e5e5e5' }}
            labelStyle={{ color: '#00d4ff' }}
            formatter={(val: number, name: string) => [`${val.toFixed(3)} BTC`, name]}
          />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}
