'use client'
import { useState, useEffect } from 'react'
import useSWR from 'swr'
import * as Slider from '@radix-ui/react-slider'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'

const fetcher = (url: string) => fetch(url).then(r => r.json())

function SliderField({
  label, value, min, max, step, unit, onChange,
}: {
  label: string; value: number; min: number; max: number;
  step: number; unit: string; onChange: (v: number) => void
}) {
  return (
    <div>
      <div className="flex justify-between text-sm mb-2">
        <span className="text-gray-300">{label}</span>
        <span className="text-blue-400 font-mono">{value}{unit}</span>
      </div>
      <Slider.Root
        min={min} max={max} step={step} value={[value]}
        onValueChange={([v]) => onChange(v)}
        className="relative flex items-center select-none h-5 w-full"
      >
        <Slider.Track className="bg-gray-700 relative grow rounded h-1">
          <Slider.Range className="absolute bg-blue-500 rounded h-full" />
        </Slider.Track>
        <Slider.Thumb className="block w-4 h-4 bg-white rounded-full shadow-md focus:outline-none focus:ring-2 focus:ring-blue-500" />
      </Slider.Root>
      <div className="flex justify-between text-xs text-gray-600 mt-1">
        <span>{min}{unit}</span><span>{max}{unit}</span>
      </div>
    </div>
  )
}

export default function FailureLabPage() {
  const { data: currentConfig } = useSWR('/api/control', fetcher)

  const [baseRate, setBaseRate] = useState(10)
  const [burstProb, setBurstProb] = useState(0.05)
  const [burstSize, setBurstSize] = useState(500)
  const [applying, setApplying] = useState(false)
  const [applied, setApplied] = useState(false)

  useEffect(() => {
    if (!currentConfig) return
    setBaseRate(currentConfig.base_rate ?? 10)
    setBurstProb(currentConfig.burst_probability ?? 0.05)
    setBurstSize(currentConfig.burst_size ?? 500)
  }, [currentConfig])

  const { data: trust } = useSWR('/api/trust', fetcher, { refreshInterval: 5000 })
  const [anomalyHistory, setAnomalyHistory] = useState<{ t: number; avg: number }[]>([])
  useEffect(() => {
    if (!trust?.datasets?.length) return
    const avg =
      trust.datasets.reduce((s: number, d: Record<string, number>) => s + (d.composite ?? 1), 0) /
      trust.datasets.length
    setAnomalyHistory(prev => [...prev, { t: Date.now(), avg: 1 - avg }].slice(-60))
  }, [trust])

  const handleApply = async () => {
    setApplying(true)
    await fetch('/api/control', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ base_rate: baseRate, burst_probability: burstProb, burst_size: burstSize }),
    })
    setApplying(false)
    setApplied(true)
    setTimeout(() => setApplied(false), 3000)
  }

  const chartData = anomalyHistory.map((p, i) => ({
    i, anomaly: Math.round(p.avg * 1000) / 1000,
  }))

  return (
    <div>
      <h1 className="text-xl font-semibold mb-2">Failure Lab</h1>
      <p className="text-gray-500 text-sm mb-8">
        Adjust simulator parameters live. Trust scores and incidents react within seconds.
        Phase 07 will add fault-injection replay from MinIO snapshots.
      </p>

      <div className="grid grid-cols-2 gap-8">
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-6 flex flex-col gap-6">
          <h2 className="text-sm font-medium text-gray-300">Simulator Parameters</h2>
          <SliderField label="Base Rate" value={baseRate} min={1} max={200} step={1} unit=" ev/s" onChange={setBaseRate} />
          <SliderField label="Burst Probability" value={burstProb} min={0} max={1} step={0.01} unit="" onChange={setBurstProb} />
          <SliderField label="Burst Size" value={burstSize} min={10} max={1000} step={10} unit=" ev" onChange={setBurstSize} />
          <button
            onClick={handleApply}
            disabled={applying}
            className="mt-2 w-full py-2.5 bg-blue-700 hover:bg-blue-600 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors"
          >
            {applying ? 'Applying…' : applied ? 'Applied ✓' : 'Apply'}
          </button>
          {currentConfig && (
            <div className="text-xs text-gray-600 border-t border-gray-800 pt-4">
              <div className="font-medium text-gray-500 mb-1">Live config</div>
              <div>base_rate: {currentConfig.base_rate}</div>
              <div>burst_probability: {currentConfig.burst_probability}</div>
              <div>burst_size: {currentConfig.burst_size}</div>
            </div>
          )}
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-lg p-6">
          <h2 className="text-sm font-medium text-gray-300 mb-4">
            Anomaly Pressure <span className="text-gray-600 font-normal">(1 − avg trust, 5s)</span>
          </h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="i" hide />
                <YAxis domain={[0, 1]} tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`}
                  tick={{ fontSize: 10, fill: '#6b7280' }} width={40} />
                <Tooltip
                  formatter={(v: number) => [`${(v * 100).toFixed(1)}%`, 'anomaly pressure']}
                  contentStyle={{ background: '#111', border: '1px solid #333', fontSize: 11 }}
                />
                <Line type="monotone" dataKey="anomaly" stroke="#ef4444" dot={false} strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <p className="text-xs text-gray-600 mt-3">
            Increase base_rate to stress the pipeline. Increase burst_probability to trigger anomaly spikes.
          </p>
        </div>
      </div>
    </div>
  )
}
