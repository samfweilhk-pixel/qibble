import { useState, useEffect } from 'react'
import { fetchDates, fetchDay } from './api'
import type { DateSummary, DayData } from './types'
import DateSelector from './components/DateSelector'
import StatsPanel from './components/StatsPanel'
import PriceVolumeChart from './components/PriceVolumeChart'
import CumulativeFlowChart from './components/CumulativeFlowChart'
import BuySellVolumeChart from './components/BuySellVolumeChart'
import ImbalanceChart from './components/ImbalanceChart'
import RollingImbalanceChart from './components/RollingImbalanceChart'
import FlowSignalsPage from './components/FlowSignalsPage'
import MarketStructurePage from './components/MarketStructurePage'
import RegimesPage from './components/RegimesPage'

type Page = 'daily' | 'signals' | 'structure' | 'regimes'

const NAV_ITEMS: { key: Page; label: string }[] = [
  { key: 'daily', label: 'Daily Flow' },
  { key: 'signals', label: 'Flow Signals' },
  { key: 'structure', label: 'Market Structure' },
  { key: 'regimes', label: 'Regimes' },
]

export default function App() {
  const [page, setPage] = useState<Page>('daily')
  const [dates, setDates] = useState<DateSummary[]>([])
  const [selectedDate, setSelectedDate] = useState('')
  const [dayData, setDayData] = useState<DayData | null>(null)
  const [loading, setLoading] = useState(false)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [regimeFilter, setRegimeFilter] = useState('')

  useEffect(() => {
    fetchDates().then(d => {
      setDates(d)
      if (d.length > 0) setSelectedDate(d[0].date)
    })
  }, [])

  useEffect(() => {
    if (!selectedDate) return
    setLoading(true)
    fetchDay(selectedDate).then(d => {
      setDayData(d)
      setLoading(false)
    })
  }, [selectedDate])

  return (
    <div className="min-h-screen flex flex-col">
      {/* Header */}
      <header className="border-b border-border">
        <div className="px-4 md:px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <button
              onClick={() => setSidebarOpen(true)}
              className="md:hidden text-gray-400 hover:text-accent-cyan p-1 -ml-1"
              aria-label="Open sidebar"
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
              </svg>
            </button>
            <div className="flex items-center gap-2">
              <span className="text-lg font-bold text-accent-cyan glow-cyan tracking-wide">QIBBLE</span>
              <span className="text-[10px] text-gray-500 hidden sm:inline">BTC Flow Analytics</span>
            </div>
          </div>
          <div className="hidden sm:flex items-center gap-4">
            <span className="text-[10px] text-accent-purple/60 uppercase tracking-widest">All times UTC</span>
            <span className="text-[10px] text-gray-600">{dates.length} days loaded</span>
          </div>
        </div>

        {/* Nav tabs */}
        <nav className="px-4 md:px-6 flex gap-0 overflow-x-auto">
          {NAV_ITEMS.map(item => (
            <button
              key={item.key}
              onClick={() => { setPage(item.key); setSidebarOpen(false) }}
              className={`relative px-3 md:px-5 py-2.5 text-xs tracking-wide transition-all whitespace-nowrap ${
                page === item.key ? 'text-accent-cyan' : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              <span className="font-semibold uppercase">{item.label}</span>
              {page === item.key && (
                <div className="absolute bottom-0 left-0 right-0 h-[2px] bg-accent-cyan shadow-[0_0_8px_rgba(0,212,255,0.5)]" />
              )}
            </button>
          ))}
        </nav>
      </header>

      {/* Page content */}
      {page === 'daily' && (
        <div className="flex flex-1 overflow-hidden">
          {/* Mobile sidebar drawer */}
          {sidebarOpen && (
            <div className="fixed inset-0 z-40 md:hidden">
              <div className="absolute inset-0 bg-black/60" onClick={() => setSidebarOpen(false)} />
              <aside className="absolute left-0 top-0 bottom-0 w-72 bg-[#0a0a0f] border-r border-border p-3 overflow-y-auto z-50">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-xs text-accent-cyan uppercase tracking-wider font-semibold">Select Date</span>
                  <button onClick={() => setSidebarOpen(false)} className="text-gray-400 hover:text-white p-1" aria-label="Close sidebar">
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                </div>
                <DateSelector dates={dates} selected={selectedDate} onSelect={(d) => { setSelectedDate(d); setSidebarOpen(false) }} regimeFilter={regimeFilter} onRegimeFilter={setRegimeFilter} />
              </aside>
            </div>
          )}
          {/* Desktop sidebar */}
          <aside className="hidden md:block w-64 border-r border-border p-3 flex-shrink-0 overflow-y-auto">
            <DateSelector dates={dates} selected={selectedDate} onSelect={setSelectedDate} regimeFilter={regimeFilter} onRegimeFilter={setRegimeFilter} />
          </aside>
          <main className="flex-1 overflow-y-auto p-3 md:p-4 flex flex-col gap-3 md:gap-4">
            {loading && (
              <div className="flex items-center justify-center py-20">
                <div className="text-accent-cyan text-sm animate-pulse">Loading...</div>
              </div>
            )}
            {!loading && dayData && dayData.bars.length > 0 && (
              <>
                <StatsPanel stats={dayData.stats} />
                <PriceVolumeChart bars={dayData.bars} />
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 md:gap-4">
                  <CumulativeFlowChart bars={dayData.bars} />
                  <BuySellVolumeChart bars={dayData.bars} />
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 md:gap-4">
                  <ImbalanceChart bars={dayData.bars} />
                  <RollingImbalanceChart bars={dayData.bars} />
                </div>
              </>
            )}
            {!loading && dayData && dayData.bars.length === 0 && (
              <div className="flex items-center justify-center py-20">
                <div className="text-gray-600 text-sm">No data for {selectedDate}</div>
              </div>
            )}
          </main>
        </div>
      )}

      {page === 'signals' && <FlowSignalsPage />}
      {page === 'structure' && <MarketStructurePage />}
      {page === 'regimes' && <RegimesPage />}
    </div>
  )
}
