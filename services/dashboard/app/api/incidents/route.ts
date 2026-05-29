import { NextRequest, NextResponse } from 'next/server'
import getConfig from 'next/config'

const { serverRuntimeConfig: cfg } = getConfig()

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url)
    const status = searchParams.get('status')
    const limit = searchParams.get('limit') || '20'
    const qs = status ? `?status=${status}&limit=${limit}` : `?limit=${limit}`
    const res = await fetch(`${cfg.INCIDENT_API_URL}/incidents${qs}`, { cache: 'no-store' })
    return NextResponse.json(await res.json())
  } catch {
    return NextResponse.json([], { status: 503 })
  }
}
