import { EntitySummaryRow } from '../../api'
import { formatNumber, formatDate, capitalise } from '../../utils/format'
import { Badge } from '../ui/Badge'

interface SummaryTableProps {
  data: EntitySummaryRow[]
}

export function SummaryTable({ data }: SummaryTableProps) {
  const sorted = [...data].sort((a, b) => b.total_count - a.total_count)

  return (
    <div className="bg-surface-2 rounded-xl shadow-card border border-white/5 overflow-hidden">
      <table className="w-full text-xs font-mono">
        <thead>
          <tr className="border-b border-white/5">
            <th className="text-left px-5 py-3 text-slate-500 font-medium uppercase tracking-widest">Category</th>
            <th className="text-right px-5 py-3 text-slate-500 font-medium uppercase tracking-widest">Total</th>
            <th className="text-right px-5 py-3 text-slate-500 font-medium uppercase tracking-widest hidden sm:table-cell">Unique</th>
            <th className="text-right px-5 py-3 text-slate-500 font-medium uppercase tracking-widest hidden md:table-cell">Latest</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr
              key={row.category}
              className="border-b border-white/5 last:border-0 hover:bg-white/[0.02] transition-colors"
            >
              <td className="px-5 py-3 text-slate-300 flex items-center gap-2">
                <span
                  className="w-2 h-2 rounded-full inline-block flex-shrink-0"
                  style={{ backgroundColor: `hsl(${(i * 60 + 210) % 360}, 70%, 60%)` }}
                />
                {capitalise(row.category)}
              </td>
              <td className="px-5 py-3 text-right text-slate-200">{formatNumber(row.total_count)}</td>
              <td className="px-5 py-3 text-right text-slate-400 hidden sm:table-cell">{formatNumber(row.unique_count)}</td>
              <td className="px-5 py-3 text-right hidden md:table-cell">
                <Badge variant="info">{formatDate(row.latest_created_at)}</Badge>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
