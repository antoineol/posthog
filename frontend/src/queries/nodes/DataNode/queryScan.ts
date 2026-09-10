import { humanFriendlyNumber } from 'lib/utils/numbers'

import { QueryScanRange, QueryScanStatus, QueryScanSummary, QueryScanWarning } from '~/queries/schema/schema-general'
import { integer } from '~/queries/schema/type-utils'
import { DashboardTile, InsightShortId, QueryBasedInsightModel } from '~/types'

// The query viewset has no generated client, so this mirrors `QueryScanResponseSerializer` in
// `posthog/api/query.py`.
export interface QueryScanApiResponse {
    status: QueryScanStatus
    warnings: QueryScanWarning[]
    events_in_range: integer | null
    range: QueryScanRange | null
    killed: boolean
}

export interface QueryScanState {
    summary: QueryScanSummary
    findings: QueryScanWarning[]
    cacheKey: string | null
}

export interface QueryScanPollResult {
    cacheKey: string
    scan: QueryScanApiResponse
}

interface ScanCarrier {
    query_scan?: QueryScanSummary
    cache_key?: string
    warnings?: unknown
}

function asObject(value: unknown): Record<string, unknown> | null {
    return value !== null && typeof value === 'object' ? (value as Record<string, unknown>) : null
}

function asCarrier(value: unknown): ScanCarrier | null {
    const object = asObject(value)
    return object && asObject(object.query_scan) ? (object as ScanCarrier) : null
}

// A killed run has no response, so its scan rides on the error: on `extra` for a blocking run and
// on `query_status` for an async one.
function errorScanCarrier(responseErrorObject: unknown): ScanCarrier | null {
    const body = asObject(asObject(responseErrorObject)?.data)
    if (!body) {
        return null
    }
    return asCarrier(body.extra) ?? asCarrier(body.query_status)
}

export function queryScanFindings(warnings: unknown): QueryScanWarning[] {
    if (!Array.isArray(warnings)) {
        return []
    }
    return warnings.filter((warning): warning is QueryScanWarning => asObject(warning)?.type === 'query_scan')
}

// Returns nothing unless the flag mode is `show`, which keeps every surface silent while the
// feature only logs.
export function resolveQueryScan(
    response: unknown,
    responseErrorObject: unknown,
    polled: QueryScanPollResult | null
): QueryScanState | null {
    const carrier = asCarrier(response) ?? errorScanCarrier(responseErrorObject)
    const summary = carrier?.query_scan
    if (!summary || summary.mode !== 'show') {
        return null
    }
    const cacheKey = typeof carrier?.cache_key === 'string' ? carrier.cache_key : null
    // A poll outlives the run that started it, so a result for an earlier query would otherwise
    // decorate whatever response is on screen when it lands.
    if (!polled || polled.cacheKey !== cacheKey) {
        return { summary, findings: queryScanFindings(carrier?.warnings), cacheKey }
    }
    const { scan } = polled
    return {
        summary: {
            ...summary,
            status: scan.status,
            events_in_range: scan.events_in_range ?? undefined,
            range: scan.range ?? undefined,
            killed: scan.killed,
        },
        findings: [...queryScanFindings(carrier?.warnings), ...scan.warnings],
        cacheKey,
    }
}

function formatRows(rows: integer): string {
    return humanFriendlyNumber(rows)
}

function formatSeconds(durationMs: integer): string {
    return humanFriendlyNumber(durationMs / 1000, 1, 1)
}

export function queryScanStatLine(summary: QueryScanSummary): string {
    const rows = formatRows(summary.rows_read)
    const seconds = formatSeconds(summary.duration_ms)
    if (summary.killed) {
        return `ClickHouse stopped it after ${seconds} s, having read ${rows} rows.`
    }
    const line = `Read ${rows} rows in ${seconds} s.`
    if (summary.status === 'done' && summary.events_in_range != null) {
        return `${line} ${formatRows(summary.events_in_range)} events in the date range.`
    }
    return line
}

export function queryScanTileTooltip(summary: QueryScanSummary, findingCount: number, showAdvice: boolean): string {
    const tooltip = `Slow query: ${formatRows(summary.rows_read)} rows in ${formatSeconds(
        summary.duration_ms
    )} s on the last run.`
    if (!showAdvice || findingCount === 0) {
        return tooltip
    }
    if (findingCount === 1) {
        return `${tooltip} 1 thing to change. Open the insight to see it.`
    }
    return `${tooltip} ${findingCount} things to change. Open the insight to see them.`
}

// A `filters` finding is fixed on the insight's date range, not in the SQL, so there is nothing in
// the query for the assistant to change.
export function fixableQueryScanFindings(findings: QueryScanWarning[]): QueryScanWarning[] {
    return findings.filter((finding) => finding.reason !== 'filters')
}

/** The message "Fix with AI" sends to the assistant. */
export function queryScanAssistantPrompt(findings: QueryScanWarning[]): string {
    return [
        'Make this query faster without changing what it answers.',
        '',
        'Here is what the slow query analysis found:',
        ...findings.map((finding, index) => `${index + 1}. ${finding.message} Suggested change: ${finding.fix}`),
        '',
        'Before you propose a rewrite, run exploratory queries to learn what the data looks like, for example which events satisfy the other conditions in the WHERE clause over the last 7 days, and how many rows each candidate change would read. Then propose the rewritten query and say what each change does to the results.',
        '',
        'Never invent event names or dates. If you cannot tell which events the question is about, say so and leave a `-- fill in the events this question is about` comment in the SQL where the filter goes.',
    ].join('\n')
}

export interface QueryScanDashboardEntry {
    tileId: number
    shortId: InsightShortId
    name: string
    findingCount: number
}

export interface QueryScanDashboardSummary {
    entries: QueryScanDashboardEntry[]
    /** Tracks which tiles are slow and how much they have to change, so a dismissed banner returns when that set moves. */
    signature: string
}

/** The insights on a dashboard whose last fresh run has advice for the viewer. */
export function queryScanDashboardSummary(tiles: DashboardTile<QueryBasedInsightModel>[]): QueryScanDashboardSummary {
    const entries: QueryScanDashboardEntry[] = []
    for (const tile of tiles) {
        const insight = tile.insight
        if (!insight || insight.deleted) {
            continue
        }
        // A killed run has no result to carry the scan, so it arrives on the query status instead.
        const summary: QueryBasedInsightModel['query_scan'] = insight.query_scan ?? insight.query_status?.query_scan
        if (summary?.mode !== 'show') {
            continue
        }
        const findingCount = queryScanFindings(summary.warnings).length
        if (findingCount === 0) {
            continue
        }
        entries.push({
            tileId: tile.id,
            shortId: insight.short_id,
            name: insight.name || insight.derived_name || 'Untitled',
            findingCount,
        })
    }
    return {
        entries,
        signature: entries
            .map((entry) => `${entry.tileId}:${entry.findingCount}`)
            .sort()
            .join(','),
    }
}
