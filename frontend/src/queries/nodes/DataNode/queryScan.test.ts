import { QueryScanSummary, QueryScanWarning } from '~/queries/schema/schema-general'
import { DashboardTile, InsightShortId, QueryBasedInsightModel } from '~/types'

import { queryScanDashboardSummary, queryScanTileStatLine } from './queryScan'

const SUMMARY: QueryScanSummary = {
    mode: 'show',
    rows_read: 8_400_000_000,
    duration_ms: 19_000,
    status: 'done',
}

const FINDING: QueryScanWarning = {
    type: 'query_scan',
    kind: 'no_event_filter',
    message: 'This query read every event in its date range.',
    fix: 'Add an event filter naming the events this question is about.',
    clause: "event != 'x'",
    rows_read: 8_400_000_000,
    duration_ms: 19_000,
}

function tile(id: number, insight: Partial<QueryBasedInsightModel> | null): DashboardTile<QueryBasedInsightModel> {
    return { id, color: null, insight: insight ? (insight as QueryBasedInsightModel) : undefined }
}

function slowInsight(
    shortId: string,
    name: string,
    findings: number,
    summary: Partial<QueryScanSummary> = {}
): Partial<QueryBasedInsightModel> {
    return {
        short_id: shortId as InsightShortId,
        name,
        query_scan: { ...SUMMARY, ...summary, warnings: Array(findings).fill(FINDING) },
    }
}

describe('queryScan', () => {
    describe('queryScanDashboardSummary', () => {
        it('names only the insights whose last run has advice', () => {
            const { entries } = queryScanDashboardSummary([
                tile(1, null),
                tile(2, slowInsight('aaa', 'Active users', 2)),
                tile(3, slowInsight('bbb', 'Fast enough', 0)),
                tile(4, slowInsight('ccc', 'Only logging', 1, { mode: 'log_only' })),
                tile(5, { ...slowInsight('ddd', 'Deleted', 1), deleted: true }),
                tile(6, { short_id: 'eee' as InsightShortId, derived_name: 'Pageview count' }),
            ])

            expect(entries).toEqual([{ tileId: 2, shortId: 'aaa', name: 'Active users', findingCount: 2 }])
        })

        it('falls back to the derived name, then to Untitled', () => {
            const { entries } = queryScanDashboardSummary([
                tile(1, { ...slowInsight('aaa', '', 1), derived_name: 'Pageview count' }),
                tile(2, slowInsight('bbb', '', 1)),
            ])

            expect(entries.map((entry) => entry.name)).toEqual(['Pageview count', 'Untitled'])
        })

        it('signs the slow tiles and their counts, ignoring tile order', () => {
            const first = tile(2, slowInsight('aaa', 'Active users', 2))
            const second = tile(10, slowInsight('bbb', 'Slow SQL', 1))

            expect(queryScanDashboardSummary([first, second]).signature).toEqual(
                queryScanDashboardSummary([second, first]).signature
            )
            expect(queryScanDashboardSummary([first, second]).signature).not.toEqual(
                queryScanDashboardSummary([first, tile(10, slowInsight('bbb', 'Slow SQL', 3))]).signature
            )
        })
    })

    describe('queryScanTileStatLine', () => {
        it.each([
            {
                label: 'a run that finished',
                summary: {},
                expected: 'This tile read 8,400,000,000 rows in 19.0 s on its last run.',
            },
            {
                label: 'a run ClickHouse stopped',
                summary: { killed: true },
                expected: "ClickHouse stopped this tile's last run after 19.0 s, having read 8,400,000,000 rows.",
            },
        ])('reports $label', ({ summary, expected }) => {
            expect(queryScanTileStatLine({ ...SUMMARY, ...summary })).toEqual(expected)
        })
    })
})
