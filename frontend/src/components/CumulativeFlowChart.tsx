import { ResponsiveContainer, ComposedChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, Legend } from 'recharts'
import ChartCard from './ChartCard'
import type { Bar } from '../types'

export default function CumulativeFlowChart({ bars }: { bars: Bar[] }) {
  const ticks = bars.filter((_, i) => i % 60 === 0).map(b => b.time)

  return (
    <ChartCard
      title="CVD vs Price Return"
      description="Cumulative buying pressure vs price change since midnight UTC. When the purple area rises but the yellow line drops, buyers are accumulating but price hasn't followed yet."
      height="h-[200px] md:h-[280px]"
    >
      <ResponsiveContainer>
        <ComposedChart data={bars}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
          <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} ticks={ticks} />
          <YAxis yAxisId="flow" orientation="left" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} label={{ value: 'CVD (BTC)', angle: -90, position: 'insideLeft', fill: '#555', fontSize: 9, dx: -5 }} />
          <YAxis yAxisId="ret" orientation="right" tick={{ fontSize: 10, fill: '#555' }} tickLine={false} axisLine={{ stroke: '#1e1e2e' }} tickFormatter={v => `${v.toFixed(1)}%`} label={{ value: 'Return %', angle: 90, position: 'insideRight', fill: '#555', fontSize: 9, dx: 5 }} />
          <ReferenceLine yAxisId="flow" y={0} stroke="#2a2a3e" strokeDasharray="3 3" />
          <Area yAxisId="flow" dataKey="cum_flow" fill="#7c3aed" fillOpacity={0.15} stroke="#7c3aed" strokeWidth={1.5} name="CVD (BTC)" isAnimationActive={false} />
          <Line yAxisId="ret" dataKey="cum_return" stroke="#ffd700" dot={false} strokeWidth={2} name="Return %" isAnimationActive={false} />
          <Legend wrapperStyle={{ fontSize: 10 }} />
          <Tooltip
            contentStyle={{ background: '#111118', border: '1px solid #2a2a3e', borderRadius: 6, fontSize: 11 }}
            labelStyle={{ color: '#00d4ff' }}
            formatter={(val: number, name: string) => {
              if (name === 'Return %') return [`${val.toFixed(3)}%`, name]
              return [`${val.toFixed(2)} BTC`, name]
            }}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}
