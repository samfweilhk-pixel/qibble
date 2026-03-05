const BASE = '/api'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

export const fetchDates = () => get<any[]>('/dates')
export const fetchDay = (date: string) => get<any>(`/day/${date}`)
export const fetchRegimes = () => get<any>('/regimes')
export const fetchRegimeDailyAll = () => get<any>('/regime-daily-all')
export const fetchIntradayCorr = () => get<any>('/intraday-correlation-all')
export const fetchLeadLag = () => get<any>('/lead-lag-all')
export const fetchFlowExtremes = () => get<any>('/flow-extremes-all')
export const fetchCorrDivergence = () => get<any>('/corr-divergence-all')
export const fetchFlowTod = () => get<any>('/flow-tod-all')
export const fetchSessionPerf = () => get<any>('/session-performance-all')
export const fetchSessionFlowFwd = () => get<any>('/session-flow-fwd-all')
export const fetchWhaleActivity = () => get<any>('/whale-activity')
export const fetchFlowPersistence = () => get<any>('/flow-persistence-all')
export const fetchFlowClassification = () => get<any>('/flow-classification-all')
export const fetchVolumeTrend = () => get<any>('/volume-trend')
