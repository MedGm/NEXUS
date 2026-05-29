/** @type {import('next').NextConfig} */
const nextConfig = {
  serverRuntimeConfig: {
    TRUST_API_URL: process.env.TRUST_API_URL || 'http://localhost:8000',
    INCIDENT_API_URL: process.env.INCIDENT_API_URL || 'http://localhost:8002',
    PROMETHEUS_URL: process.env.PROMETHEUS_URL || 'http://localhost:9090',
    POSTGRES_META_DSN: process.env.POSTGRES_META_DSN || '',
    SIMULATOR_URL: process.env.SIMULATOR_URL || 'http://localhost:8003',
  },
}
module.exports = nextConfig
