'use client'
import useSWR from 'swr'
import Link from 'next/link'

const fetcher = (url: string) => fetch(url).then(r => r.json())

function SeverityBadge({ severity }: { severity: string }) {
  const cls = severity === 'HIGH'
    ? 'bg-red-900 text-red-300'
    : severity === 'MEDIUM'
    ? 'bg-yellow-900 text-yellow-300'
    : 'bg-gray-800 text-gray-400'
  return <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${cls}`}>{severity}</span>
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
      status === 'open' ? 'bg-blue-900 text-blue-300' : 'bg-gray-800 text-gray-400'
    }`}>{status}</span>
  )
}

export default function IncidentsPage() {
  const { data } = useSWR('/api/incidents?limit=50', fetcher, { refreshInterval: 10000 })
  const incidents = Array.isArray(data) ? data : []

  return (
    <div>
      <h1 className="text-xl font-semibold mb-6">Incidents</h1>
      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800 text-gray-400 text-xs uppercase tracking-wider">
              <th className="text-left px-4 py-3">Started</th>
              <th className="text-left px-4 py-3">Severity</th>
              <th className="text-left px-4 py-3">Status</th>
              <th className="text-left px-4 py-3">Nodes</th>
              <th className="text-left px-4 py-3">Root Cause</th>
            </tr>
          </thead>
          <tbody>
            {incidents.map((inc: Record<string, unknown>) => (
              <tr key={String(inc.id)} className="border-b border-gray-800 hover:bg-gray-800 cursor-pointer">
                <td className="px-4 py-3">
                  <Link href={`/incidents/${inc.id}`} className="block text-blue-400 hover:underline">
                    {inc.started_at ? new Date(String(inc.started_at)).toLocaleString() : '—'}
                  </Link>
                </td>
                <td className="px-4 py-3"><SeverityBadge severity={String(inc.severity || '')} /></td>
                <td className="px-4 py-3"><StatusBadge status={String(inc.status || '')} /></td>
                <td className="px-4 py-3 text-gray-300">{String(inc.node_count ?? 0)}</td>
                <td className="px-4 py-3 text-gray-400 font-mono text-xs truncate max-w-xs">
                  {inc.root_cause_node_id ? String(inc.root_cause_node_id).slice(0, 12) + '…' : '—'}
                </td>
              </tr>
            ))}
            {incidents.length === 0 && (
              <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-600">No incidents</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
