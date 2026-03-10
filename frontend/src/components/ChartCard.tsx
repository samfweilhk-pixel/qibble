interface Props {
  title: string
  description: string
  children: React.ReactNode
  height?: string
}

export default function ChartCard({ title, description, children, height = 'h-[220px] md:h-[300px]' }: Props) {
  return (
    <div className="card p-4 pb-10 overflow-visible">
      <h3 className="text-xs uppercase tracking-wider text-accent-cyan mb-1">{title}</h3>
      <p className="text-[9px] text-gray-400 mb-3 tracking-wide leading-relaxed">{description}</p>
      <div className={`${height} overflow-visible`}>{children}</div>
    </div>
  )
}
