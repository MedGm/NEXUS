'use client'
import useSWR from 'swr'
import { LineChart, Line, YAxis, ResponsiveContainer, Tooltip } from 'recharts'

const fetcher = (url: string) => fetch(url).then(r => r.json())

const STATUS_STYLE: Record<string, string> = {
  Healthy:   'bg-green-900 text-green-300',
  Degrading: 'bg-yellow-900 text-yellow-300',
  Flagged:   'bg-orange-900 text-orange-300',
  Shadow:    'bg-blue-900 text-blue-300',
  Promoted:  'bg-purple-900 text-purple-300',
  Dismissed: 'bg-gray-800 text-gray-400',
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${STATUS_STYLE[status] || 'bg-gray-800 text-gray-400'}`}>
      {status}
    </span>
  )
}

function Sparkline({ data }: { data: number[] }) {
  const chartData = data.map((v, i) => ({ i, v }))
  return (
    <div className="h-10 w-32">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData}>
          <YAxis domain={[0, 1]} hide />
          <Tooltip
            formatter={(v: number) => v.toFixed(3)}
            contentStyle={{ background: '#111', border: '1px solid #333', fontSize: 10 }}
          />
          <Line type="monotone" dataKey="v" stroke="#60a5fa" dot={false} strokeWidth={1.5} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export default function ModelsPage() {
  const { data } = useSWR('/api/models', fetcher, { refreshInterval: 15000 })
  const models = Array.isArray(data) ? data : []

  return (
    <div>
      <h1 className="text-xl font-semibold mb-2">Model Aging</h1>
      <p className="text-gray-500 text-sm mb-6">
        Per-model anomaly score trends and drift status. Promote via Airflow&apos;s{' '}
        <a href="http://localhost:8080" target="_blank" rel="noreferrer" className="text-blue-400 hover:underline">
          promote_model_dag
        </a>.
      </p>

      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-gray-400 text-xs uppercase tracking-wider">
              <th className="text-left px-4 py-3">Model</th>
              <th className="text-left px-4 py-3">Status</th>
              <th className="text-left px-4 py-3">Score Trend (2h)</th>
              <th className="text-left px-4 py-3">Avg Score</th>
              <th className="text-left px-4 py-3">Action</th>
            </tr>
          </thead>
          <tbody>
            {models.map((m: Record<string, unknown>) => {
              const trend = (m.score_trend as number[]) || []
              const avg = trend.length
                ? (trend.reduce((s, v) => s + v, 0) / trend.length).toFixed(3)
                : '—'
              return (
                <tr key={String(m.name)} className="border-b border-gray-800">
                  <td className="px-4 py-3 font-medium text-gray-200">{String(m.name)}</td>
                  <td className="px-4 py-3"><StatusBadge status={String(m.status)} /></td>
                  <td className="px-4 py-3">
                    {trend.length > 1 ? <Sparkline data={trend} /> : <span className="text-gray-600">No data</span>}
                  </td>
                  <td className="px-4 py-3 text-gray-300 font-mono text-xs">{avg}</td>
                  <td className="px-4 py-3">
                    <a
                      href="http://localhost:8080/dags/promote_model_dag"
                      target="_blank"
                      rel="noreferrer"
                      className="text-xs px-3 py-1 bg-gray-800 hover:bg-gray-700 text-gray-300 rounded transition-colors"
                    >
                      Promote in Airflow →
                    </a>
                  </td>
                </tr>
              )
            })}
            {models.length === 0 && (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-600">Loading model data…</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
