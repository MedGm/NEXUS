import { NextResponse } from 'next/server'
import getConfig from 'next/config'

const { serverRuntimeConfig: cfg } = getConfig()

function parseTrustMetrics(text: string): Record<string, number | string>[] {
  const datasets: Record<string, Record<string, number | string>> = {}
  for (const line of text.split('\n')) {
    if (line.startsWith('#') || !line.trim()) continue
    const m = line.match(/nexus_trust_(\w+)\{dataset="([^"]+)"\}\s+([\d.e+\-]+)/)
    if (!m) continue
    const [, metric, dataset, value] = m
    if (!datasets[dataset]) datasets[dataset] = { name: dataset }
    datasets[dataset][metric] = parseFloat(value)
  }
  return Object.values(datasets)
}

export async function GET() {
  try {
    const [metricsRes, lagRes] = await Promise.all([
      fetch(`${cfg.TRUST_API_URL}/metrics`, { cache: 'no-store' }),
      fetch(
        `${cfg.PROMETHEUS_URL}/api/v1/query?query=sum(kafka_consumergroup_lag%7Bconsumergroup%3D%22nexus-anomaly-detector%22%7D)`,
        { cache: 'no-store' }
      ),
    ])
    const metricsText = await metricsRes.text()
    const lagJson = await lagRes.json()
    const datasets = parseTrustMetrics(metricsText)
    const lagResult = lagJson?.data?.result?.[0]
    const kafka_lag = lagResult ? parseInt(lagResult.value[1]) : 0
    return NextResponse.json({ datasets, kafka_lag })
  } catch {
    return NextResponse.json({ datasets: [], kafka_lag: 0 }, { status: 503 })
  }
}
