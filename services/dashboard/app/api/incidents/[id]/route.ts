import { NextRequest, NextResponse } from 'next/server'
import getConfig from 'next/config'

const { serverRuntimeConfig: cfg } = getConfig()

export async function GET(
  _req: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const res = await fetch(`${cfg.INCIDENT_API_URL}/incidents/${params.id}`, { cache: 'no-store' })
    if (!res.ok) return NextResponse.json(null, { status: res.status })
    return NextResponse.json(await res.json())
  } catch {
    return NextResponse.json(null, { status: 503 })
  }
}

export async function POST(
  _req: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const res = await fetch(
      `${cfg.INCIDENT_API_URL}/incidents/${params.id}/resolve`,
      { method: 'POST' }
    )
    return NextResponse.json(await res.json(), { status: res.status })
  } catch {
    return NextResponse.json({ error: 'failed' }, { status: 503 })
  }
}
