import { MOCK_DEFAULT_USER } from 'lib/api.mock'

import '@testing-library/jest-dom'

import { cleanup, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Provider } from 'kea'

import { userLogic } from 'scenes/userLogic'

import { uiCustomizationLogic } from '~/layout/uiCustomizationLogic'
import { QueryScanSummary, QueryScanWarning } from '~/queries/schema/schema-general'
import { initKeaTests } from '~/test/init'

import { QueryScanState, resolveQueryScan } from './queryScan'
import { QueryScanBanner } from './QueryScanBanner'

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

const START_DATE_FINDING: QueryScanWarning = {
    ...FINDING,
    kind: 'no_start_date',
    message: 'This query has no start date.',
    fix: 'Add a start date on `timestamp`.',
}

const INSIGHT_SIDE_FINDING: QueryScanWarning = {
    ...FINDING,
    kind: 'no_start_date',
    reason: 'filters',
    message: 'No date range is set for this insight or dashboard.',
    fix: 'Set a date range on the insight or the dashboard.',
}

function state(summary: Partial<QueryScanSummary>, findings: QueryScanWarning[] = []): QueryScanState {
    return { summary: { ...SUMMARY, ...summary }, findings, cacheKey: 'cache-key' }
}

describe('QueryScanBanner', () => {
    beforeEach(() => {
        initKeaTests()
        uiCustomizationLogic().mount()
    })

    afterEach(() => cleanup())

    function seedAdviceHidden(hidden: boolean): void {
        userLogic.actions.loadUserSuccess({
            ...MOCK_DEFAULT_USER,
            ui_configuration: { version: 1, hide_query_scan_advice: hidden },
        })
    }

    it('renders nothing while the team is only logging scans', () => {
        expect(resolveQueryScan({ query_scan: { ...SUMMARY, mode: 'log_only' } }, null, null)).toBeNull()
    })

    it('renders the stat line without a banner when there is nothing to change', () => {
        seedAdviceHidden(false)
        render(
            <Provider>
                <QueryScanBanner queryScan={state({ events_in_range: 1_000 })} onFixWithAI={jest.fn()} />
            </Provider>
        )

        expect(screen.getByText('Read 8,400,000,000 rows in 19.0 s. 1,000 events in the date range.')).toBeVisible()
        expect(screen.queryByText(FINDING.message)).not.toBeInTheDocument()
    })

    it('hands the fixer a numbered list of the fixes it can make', async () => {
        seedAdviceHidden(false)
        const onFixWithAI = jest.fn()
        render(
            <Provider>
                <QueryScanBanner
                    queryScan={state({}, [INSIGHT_SIDE_FINDING, FINDING, START_DATE_FINDING])}
                    onFixWithAI={onFixWithAI}
                />
            </Provider>
        )

        expect(screen.getByText(FINDING.message)).toBeVisible()
        await userEvent.click(screen.getByText('Fix with AI'))
        expect(onFixWithAI).toHaveBeenCalledWith(`1. ${FINDING.fix}\n2. ${START_DATE_FINDING.fix}`)
    })

    it('drops the fixer when every finding is fixed on the insight', () => {
        seedAdviceHidden(false)
        render(
            <Provider>
                <QueryScanBanner queryScan={state({}, [INSIGHT_SIDE_FINDING])} onFixWithAI={jest.fn()} />
            </Provider>
        )

        expect(screen.getByText(INSIGHT_SIDE_FINDING.message)).toBeVisible()
        expect(screen.queryByText('Fix with AI')).not.toBeInTheDocument()
    })

    it('keeps the stat line but drops the advice when the toggle is off', () => {
        seedAdviceHidden(true)
        render(
            <Provider>
                <QueryScanBanner queryScan={state({}, [FINDING])} onFixWithAI={jest.fn()} />
            </Provider>
        )

        expect(screen.getByText('Read 8,400,000,000 rows in 19.0 s.')).toBeVisible()
        expect(screen.queryByText(FINDING.message)).not.toBeInTheDocument()
    })

    it('says the run was stopped rather than what it read', () => {
        seedAdviceHidden(false)
        render(
            <Provider>
                <QueryScanBanner queryScan={state({ killed: true, events_in_range: 1_000 })} />
            </Provider>
        )

        expect(screen.getByText('ClickHouse stopped it after 19.0 s, having read 8,400,000,000 rows.')).toBeVisible()
    })
})
