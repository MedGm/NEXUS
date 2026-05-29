import type { Metadata } from 'next'
import Link from 'next/link'
import './globals.css'

export const metadata: Metadata = { title: 'NEXUS', description: 'ML Platform Dashboard' }

const NAV = [
  { href: '/overview',    label: 'Overview'    },
  { href: '/incidents',   label: 'Incidents'   },
  { href: '/failure-lab', label: 'Failure Lab' },
  { href: '/models',      label: 'Models'      },
]

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="bg-gray-950 text-gray-100 flex min-h-screen">
        <nav className="w-52 shrink-0 bg-gray-900 border-r border-gray-800 flex flex-col p-4 gap-1">
          <div className="text-white font-bold text-lg tracking-tight mb-6 px-2">NEXUS</div>
          {NAV.map(({ href, label }) => (
            <Link
              key={href}
              href={href}
              className="px-3 py-2 rounded text-sm text-gray-400 hover:text-white hover:bg-gray-800 transition-colors"
            >
              {label}
            </Link>
          ))}
        </nav>
        <main className="flex-1 p-8 overflow-auto min-h-screen">{children}</main>
      </body>
    </html>
  )
}
