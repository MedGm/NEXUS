'use client'
import { useState, useCallback } from 'react'
import useSWR from 'swr'
import dynamic from 'next/dynamic'
import Link from 'next/link'

const ForceGraph2D = dynamic(() => import('react-force-graph-2d'), { ssr: false })

const fetcher = (url: string) => fetch(url).then(r => r.json())

const NODE_COLORS: Record<string, string> = {
  Deploy:          '#3b82f6',
  SchemaChange:    '#a855f7',
  KafkaLag:        '#f97316',
  ErrorRateSpike:  '#ef4444',
  TrustDrop:       '#eab308',
  PredictionDrift: '#ec4899',
}

function buildGraphData(graphJson: Record<string, unknown[]>) {
  const nodes = (graphJson.nodes as Record<string, unknown>[]).map(n => ({
    id: String(n.node_id),
    label: `${n.type}\n${n.dataset_id}`,
    color: NODE_COLORS[String(n.type)] || '#6b7280',
    ...n,
  }))
  const links = (graphJson.edges as Record<string, unknown>[]).map(e => ({
    source: String(e.source_id),
    target: String(e.target_id),
    label: String(e.rule_id),
  }))
  return { nodes, links }
}

export default function IncidentDetailPage({ params }: { params: { id: string } }) {
  const { data: incident, mutate } = useSWR(`/api/incidents/${params.id}`, fetcher)
  const [resolving, setResolving] = useState(false)
  const [resolved, setResolved] = useState(false)

  const handleResolve = useCallback(async () => {
    setResolving(true)
    await fetch(`/api/incidents/${params.id}`, { method: 'POST' })
    setResolved(true)
    setResolving(false)
    mutate()
  }, [params.id, mutate])

  if (!incident) return <div className="text-gray-500 p-8">Loading…</div>

  const graphJson = incident.causal_graph_json as Record<string, unknown[]>
  const graphData = graphJson ? buildGraphData(graphJson) : { nodes: [], links: [] }

  return (
    <div>
      <div className="flex items-center gap-3 mb-6">
        <Link href="/incidents" className="text-gray-500 hover:text-gray-300 text-sm">← Incidents</Link>
        <span className="text-gray-700">/</span>
        <h1 className="text-xl font-semibold">{String(params.id).slice(0, 8)}…</h1>
        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
          incident.severity === 'HIGH' ? 'bg-red-900 text-red-300'
          : incident.severity === 'MEDIUM' ? 'bg-yellow-900 text-yellow-300'
          : 'bg-gray-800 text-gray-400'}`}>
          {incident.severity}
        </span>
      </div>

      <div className="grid grid-cols-2 gap-6">
        <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
          <div className="text-xs text-gray-400 px-4 py-2 border-b border-gray-800">Causal Graph</div>
          <div className="h-96 bg-gray-950">
            {graphData.nodes.length > 0 && (
              <ForceGraph2D
                graphData={graphData}
                nodeLabel="label"
                nodeColor={(n: Record<string, unknown>) => String(n.color)}
                linkLabel="label"
                linkDirectionalArrowLength={6}
                linkDirectionalArrowRelPos={1}
                backgroundColor="#030712"
                width={540}
                height={384}
              />
            )}
          </div>
        </div>

        <div className="flex flex-col gap-4">
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-400 mb-2">LLM Summary</div>
            <p className="text-sm text-gray-200 leading-relaxed">
              {incident.llm_summary || <span className="text-gray-600 italic">No summary. Set OPENAI_API_KEY to enable.</span>}
            </p>
          </div>

          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-400 mb-2">Details</div>
            <div className="grid grid-cols-2 gap-2 text-sm">
              <span className="text-gray-500">Status</span><span>{incident.status}</span>
              <span className="text-gray-500">Nodes</span><span>{incident.node_count}</span>
              <span className="text-gray-500">Started</span>
              <span className="text-xs">{incident.started_at ? new Date(String(incident.started_at)).toLocaleString() : '—'}</span>
            </div>
          </div>

          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-400 mb-2">Nodes ({graphData.nodes.length})</div>
            <div className="flex flex-col gap-1 max-h-32 overflow-y-auto">
              {graphData.nodes.map((n: Record<string, unknown>) => (
                <div key={String(n.id)} className="flex items-center gap-2 text-xs">
                  <span className="w-2 h-2 rounded-full shrink-0" style={{ background: String(n.color) }} />
                  <span className="text-gray-300">{String(n.type)}</span>
                  <span className="text-gray-500">{String(n.dataset_id)}</span>
                </div>
              ))}
            </div>
          </div>

          {incident.status === 'open' && !resolved && (
            <button
              onClick={handleResolve}
              disabled={resolving}
              className="w-full py-2 bg-green-800 hover:bg-green-700 disabled:opacity-50 text-green-100 text-sm font-medium rounded-lg transition-colors"
            >
              {resolving ? 'Resolving…' : 'Mark Resolved'}
            </button>
          )}
          {resolved && <div className="text-center text-sm text-green-400">Incident resolved ✓</div>}
        </div>
      </div>
    </div>
  )
}
