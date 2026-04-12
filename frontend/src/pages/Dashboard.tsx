import { PageLayout } from '../components/layout/PageLayout'
import { Section } from '../components/layout/Section'
import { StatCard } from '../components/ui/StatCard'
import { LoadingState } from '../components/ui/LoadingState'
import { ErrorState } from '../components/ui/ErrorState'
import { CategoryBarChart } from '../components/charts/CategoryBarChart'
import { SummaryTable } from '../components/charts/SummaryTable'
import { useEntitySummary } from '../hooks/useEntitySummary'
import { formatNumber, formatRelativeTime } from '../utils/format'

export function Dashboard() {
  const { data, loading, error } = useEntitySummary()

  const totalEntities = data?.data.reduce((sum, r) => sum + r.total_count, 0) ?? 0
  const totalUnique = data?.data.reduce((sum, r) => sum + r.unique_count, 0) ?? 0
  const categoryCount = data?.data.length ?? 0

  return (
    <PageLayout title="pipeline-dashboard" subtitle="gold layer">
      {/* Pipeline provenance */}
      {data && (
        <p className="text-xs font-mono text-slate-600 mb-8">
          gold layer · last export {formatRelativeTime(data.exported_at)} ·{' '}
          {formatNumber(data.record_count)} rows
        </p>
      )}

      {/* KPI strip */}
      <Section title="overview">
        {loading ? (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <LoadingState rows={1} />
            <LoadingState rows={1} />
            <LoadingState rows={1} />
            <LoadingState rows={1} />
          </div>
        ) : error ? (
          <ErrorState message={error} />
        ) : (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <StatCard
              label="Total entities"
              value={formatNumber(totalEntities)}
              trend="neutral"
            />
            <StatCard
              label="Unique entities"
              value={formatNumber(totalUnique)}
              trend="neutral"
            />
            <StatCard
              label="Categories"
              value={categoryCount}
              trend="neutral"
            />
            <StatCard
              label="Data freshness"
              value={data ? formatRelativeTime(data.exported_at) : '—'}
              subValue="nightly export"
            />
          </div>
        )}
      </Section>

      {/* Chart + table */}
      <Section title="breakdown">
        {loading ? (
          <LoadingState rows={4} />
        ) : error ? (
          <ErrorState message={error} />
        ) : data?.data.length ? (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <CategoryBarChart data={data.data} />
            <SummaryTable data={data.data} />
          </div>
        ) : (
          <div className="text-center py-16 text-slate-600 font-mono text-sm">
            no data available yet — run the pipeline to populate gold tables
          </div>
        )}
      </Section>
    </PageLayout>
  )
}
