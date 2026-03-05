export interface DateSummary {
  date: string
  regime: string
  total_vol: number
  total_buy: number
  total_sell: number
  net_flow: number
  open: number
  close: number
  high: number
  low: number
  return_pct: number
  day_imb: number
  n_bars: number
  avg_trade_size: number
  total_trades: number
}

export interface Bar {
  time: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  quote_volume: number
  buy_vol: number
  sell_vol: number
  net_flow: number
  bar_imb: number
  cum_flow: number
  cum_return: number
  rolling_imb_20: number
  num_trades: number
  avg_trade_size: number
}

export interface DayStats {
  total_vol: number
  total_buy: number
  total_sell: number
  net_flow: number
  open: number
  close: number
  high: number
  low: number
  return_pct: number
  n_bars: number
  day_imb: number
  total_trades: number
  avg_trade_size: number
  regime: string
}

export interface DayData {
  bars: Bar[]
  stats: DayStats
}

export interface RegimePeriod {
  regime: string
  start: string
  end: string
  n_days: number
  avg_price: number
  start_price: number
  end_price: number
  return_pct: number
}

export interface FwdCurvePoint {
  bar: number
  avg_bps: number
  n: number
}

export type Regime = 'BULL' | 'BEAR' | 'CHOP'
