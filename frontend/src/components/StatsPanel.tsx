import type { DayStats } from '../types'

function StatBox({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] uppercase tracking-wider text-gray-500">{label}</span>
      <span className={`text-sm font-semibold ${color || 'text-gray-200'}`}>{value}</span>
    </div>
  )
}

export default function StatsPanel({ stats }: { stats: DayStats }) {
  const retColor = stats.return_pct >= 0 ? 'text-accent-green glow-green' : 'text-accent-red glow-red'
  const flowColor = stats.net_flow >= 0 ? 'text-accent-green' : 'text-accent-red'
  const imbColor = stats.day_imb >= 0 ? 'text-accent-green' : 'text-accent-red'
  const buyRatio = stats.total_vol > 0 ? (stats.total_buy / stats.total_vol * 100).toFixed(1) : '0'

  return (
    <div className="card p-4">
      <div className="flex flex-wrap gap-x-6 gap-y-3">
        <StatBox label="Return" value={`${stats.return_pct >= 0 ? '+' : ''}${stats.return_pct.toFixed(2)}%`} color={retColor} />
        <StatBox label="Net Flow" value={`${stats.net_flow >= 0 ? '+' : ''}${stats.net_flow.toFixed(2)} BTC`} color={flowColor} />
        <StatBox label="Day Imbalance" value={`${stats.day_imb >= 0 ? '+' : ''}${(stats.day_imb * 100).toFixed(1)}%`} color={imbColor} />
        <StatBox label="Volume" value={`${stats.total_vol.toFixed(0)} BTC`} />
        <StatBox label="Buy Ratio" value={`${buyRatio}%`} color={Number(buyRatio) > 50 ? 'text-accent-green' : 'text-accent-red'} />
        <StatBox label="Avg Trade" value={`${stats.avg_trade_size.toFixed(4)} BTC`} />
        <StatBox label="Trades" value={stats.total_trades.toLocaleString()} />
        <StatBox label="Regime" value={stats.regime} color={
          stats.regime === 'BULL' ? 'text-accent-green' : stats.regime === 'BEAR' ? 'text-accent-red' : 'text-accent-yellow'
        } />
      </div>
    </div>
  )
}
