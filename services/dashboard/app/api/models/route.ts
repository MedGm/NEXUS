import { NextResponse } from 'next/server'
import getConfig from 'next/config'
import { Client } from 'pg'

const { serverRuntimeConfig: cfg } = getConfig()

const EVENT_TYPES = [
  'OrderPlaced', 'PaymentProcessed', 'SessionStarted',
  'RecommendationServed', 'PriceSnapshot',
]

export async function GET() {
  const client = new Client({ connectionString: cfg.POSTGRES_META_DSN })
  try {
    await client.connect()
    const [trendsRes, flagsRes] = await Promise.all([
      client.query(`
        SELECT dataset_id, anomaly_score
        FROM models.inference_log
        WHERE timestamp > NOW() - INTERVAL '2 hours'
        ORDER BY timestamp ASC
      `),
      client.query(`
        SELECT DISTINCT ON (event_type) event_type, status
        FROM models.model_drift_flags
        ORDER BY event_type, detected_at DESC
      `),
    ])
    const flagMap: Record<string, string> = {}
    for (const r of flagsRes.rows) {
      flagMap[r.event_type] = r.status.charAt(0).toUpperCase() + r.status.slice(1)
    }
    const scoreMap: Record<string, number[]> = {}
    for (const r of trendsRes.rows) {
      if (!scoreMap[r.dataset_id]) scoreMap[r.dataset_id] = []
      scoreMap[r.dataset_id].push(parseFloat(r.anomaly_score))
    }
    const models = EVENT_TYPES.map(name => ({
      name,
      status: flagMap[name] || 'Healthy',
      score_trend: (scoreMap[name] || []).slice(-200),
    }))
    return NextResponse.json(models)
  } catch (e) {
    console.error('models route error:', e)
    return NextResponse.json([], { status: 503 })
  } finally {
    await client.end().catch(() => {})
  }
}
