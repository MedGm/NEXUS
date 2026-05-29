import { NextRequest, NextResponse } from 'next/server'
import getConfig from 'next/config'

const { serverRuntimeConfig: cfg } = getConfig()

export async function GET() {
  try {
    const res = await fetch(`${cfg.SIMULATOR_URL}/control`, { cache: 'no-store' })
    return NextResponse.json(await res.json())
  } catch {
    return NextResponse.json(
      { base_rate: 10, burst_probability: 0.05, burst_size: 500 },
      { status: 503 }
    )
  }
}

export async function POST(req: NextRequest) {
  try {
    const body = await req.json()
    const res = await fetch(`${cfg.SIMULATOR_URL}/control`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    return NextResponse.json(await res.json(), { status: res.status })
  } catch {
    return NextResponse.json({ error: 'control unavailable' }, { status: 503 })
  }
}
