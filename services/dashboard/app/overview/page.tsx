'use client'
import { useState, useEffect } from 'react'
import useSWR from 'swr'
import { LineChart, Line, ResponsiveContainer, Tooltip, YAxis } from 'recharts'

const fetcher = (url: string) => fetch(url).then(r => r.json())

function TrustCard({ dataset, history }: { dataset: Record<string, number | string>; history: number[] }) {
  const score = Number(dataset.composite ?? 0)
  const color = score > 0.8 ? 'text-green-400' : score > 0.6 ? 'text-yellow-400' : 'text-red-400'
  const border = score > 0.8 ? 'border-green-900' : score > 0.6 ? 'border-yellow-900' : 'border-red-900'
  const chartData = history.map((v, i) => ({ i, v }))

  return (
    <div className={`bg-gray-900 border rounded-lg p-4 ${border}`}>
      <div className="text-xs text-gray-400 truncate mb-1">{dataset.name}</div>
      <div className={`text-2xl font-bold ${color}`}>{(score * 100).toFixed(1)}%</div>
      <div className="text-xs text-gray-500 mt-1">
        fresh {((Number(dataset.freshness) || 0) * 100).toFixed(0)}% ·
        complete {((Number(dataset.completeness) || 0) * 100).toFixed(0)}%
      </div>
      {chartData.length > 1 && (
        <div className="h-10 mt-3">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <YAxis domain={[0, 1]} hide />
              <Tooltip
                formatter={(v: number) => `${(v * 100).toFixed(1)}%`}
                labelFormatter={() => ''}
                contentStyle={{ background: '#111', border: '1px solid #333', fontSize: 11 }}
              />
              <Line type="monotone" dataKey="v" stroke="#60a5fa" dot={false} strokeWidth={1.5} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  )
}

export default function OverviewPage() {
  const { data: trust } = useSWR('/api/trust', fetcher, { refreshInterval: 5000 })
  const { data: incidents } = useSWR('/api/incidents?status=open&limit=100', fetcher, { refreshInterval: 5000 })

  const [history, setHistory] = useState<Record<string, number[]>>({})
  useEffect(() => {
    if (!trust?.datasets) return
    setHistory(prev => {
      const next: Record<string, number[]> = { ...prev }
      for (const d of trust.datasets) {
        const arr = [...(prev[d.name] || []), Number(d.composite)]
        next[d.name] = arr.slice(-20)
      }
      return next
    })
  }, [trust])

  const datasets = trust?.datasets ?? []
  const lag = trust?.kafka_lag ?? 0
  const activeCount = Array.isArray(incidents) ? incidents.length : 0

  return (
    <div>
      <div className="flex items-center gap-3 mb-6">
        <h1 className="text-xl font-semibold">Overview</h1>
        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${activeCount > 0 ? 'bg-red-900 text-red-300' : 'bg-green-900 text-green-300'}`}>
          {activeCount} active incidents
        </span>
      </div>

      <div className="grid grid-cols-2 gap-4 mb-8">
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-5">
          <div className="text-xs text-gray-400 mb-1">Kafka Consumer Lag</div>
          <div className={`text-3xl font-bold ${lag > 10000 ? 'text-red-400' : lag > 5000 ? 'text-yellow-400' : 'text-green-400'}`}>
            {lag.toLocaleString()}
          </div>
          <div className="text-xs text-gray-500 mt-1">nexus-anomaly-detector</div>
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-5">
          <div className="text-xs text-gray-400 mb-1">Active Incidents</div>
          <div className={`text-3xl font-bold ${activeCount > 0 ? 'text-red-400' : 'text-green-400'}`}>
            {activeCount}
          </div>
          <div className="text-xs text-gray-500 mt-1">open</div>
        </div>
      </div>

      <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">Trust Scores</h2>
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-5">
        {datasets.map((d: Record<string, number | string>) => (
          <TrustCard key={String(d.name)} dataset={d} history={history[String(d.name)] || []} />
        ))}
        {datasets.length === 0 && (
          <div className="col-span-5 text-gray-600 text-sm">Loading…</div>
        )}
      </div>
    </div>
  )
}
