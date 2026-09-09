import { humanFriendlyNumber } from 'lib/utils/numbers'

import { QueryScanRange, QueryScanStatus, QueryScanSummary, QueryScanWarning } from '~/queries/schema/schema-general'
import { integer } from '~/queries/schema/type-utils'

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

/** A polled scan and the run it was polled for. */
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
