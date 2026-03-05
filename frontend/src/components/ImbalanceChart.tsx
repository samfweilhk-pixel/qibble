import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, Cell } from 'recharts'
import ChartCard from './ChartCard'
import type { Bar as BarType } from '../types'

export default function ImbalanceChart({ bars }: { bars: BarType[] }) {
  const ticks = bars.filter((_, i) => i % 60 === 0).map(b => b.time)

  return (
    <ChartCard
      title="Bar Imbalance"
      description="Balance between buyers and sellers per minute. +1 = all buyers. -1 = all sellers. Near 0 = balanced."
      height="h-[180px] md:h-[240px]"
    >
      <ResponsiveContainer>
        <BarChart data={bars}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
          <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} ticks={ticks} />
          <YAxis domain={[-1, 1]} tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} />
          <ReferenceLine y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
          <Bar dataKey="bar_imb" isAnimationActive={false}>
            {bars.map((b, i) => (
              <Cell key={i} fill={b.bar_imb >= 0 ? '#00ff88' : '#ff3366'} />
            ))}
          </Bar>
          <Tooltip
            contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11 }}
            labelStyle={{ color: '#00d4ff' }}
            formatter={(val: number) => [`${(val * 100).toFixed(1)}%`, 'Imbalance']}
          />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}
