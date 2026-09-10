import { QueryScanSummary, QueryScanWarning } from '~/queries/schema/schema-general'

import { queryScanTileStatLine } from './queryScan'
import { QueryScanFindingList } from './QueryScanFindingList'

export interface QueryScanTileTooltipProps {
    summary: QueryScanSummary
    findings: QueryScanWarning[]
    showAdvice: boolean
}

export function QueryScanTileTooltip({ summary, findings, showAdvice }: QueryScanTileTooltipProps): JSX.Element {
    return (
        <div className="flex flex-col gap-1">
            <span>{queryScanTileStatLine(summary)}</span>
            {showAdvice && findings.length > 0 && <QueryScanFindingList findings={findings} />}
        </div>
    )
}
