import clsx from 'clsx'
import { useValues } from 'kea'

import { IconSparkles } from '@posthog/icons'

import { LemonBanner } from 'lib/lemon-ui/LemonBanner'
import { LemonButton } from 'lib/lemon-ui/LemonButton'

import { uiCustomizationLogic } from '~/layout/uiCustomizationLogic'

import { QueryScanState, queryScanStatLine } from './queryScan'

export interface QueryScanBannerProps {
    queryScan: QueryScanState | null
    /** Hands the findings' fix texts to the SQL fixer, numbered. Left out where there is no editor to write into. */
    onFixWithAI?: (instruction: string) => void
    fixWithAILoading?: boolean
    className?: string
}

/** A finding marks SQL with backticks, the way the assistant reads it. Render those spans as code. */
function withInlineCode(message: string): JSX.Element {
    return (
        <>
            {message
                .split('`')
                .map((part, index) =>
                    index % 2 === 1 ? <code key={index}>{part}</code> : <span key={index}>{part}</span>
                )}
        </>
    )
}

export function QueryScanBanner({
    queryScan,
    onFixWithAI,
    fixWithAILoading,
    className,
}: QueryScanBannerProps): JSX.Element | null {
    const { showQueryScanAdvice } = useValues(uiCustomizationLogic)

    if (!queryScan) {
        return null
    }

    const { summary, findings } = queryScan
    const showFindings = summary.status === 'done' && findings.length > 0 && showQueryScanAdvice
    // A `filters` finding is fixed on the insight's date range, not in the SQL, so handing it to
    // the fixer would ask it to change a query that is already right.
    const fixableFindings = findings.filter((finding) => finding.reason !== 'filters')

    return (
        <div className={clsx('flex flex-col gap-2 shrink-0', className)} data-attr="query-scan">
            <span className="text-xs text-secondary">{queryScanStatLine(summary)}</span>
            {showFindings && (
                <LemonBanner type="warning">
                    <ul className="list-disc pl-5">
                        {findings.map((finding, index) => (
                            <li key={`${finding.kind}-${index}`}>{withInlineCode(finding.message)}</li>
                        ))}
                    </ul>
                    {onFixWithAI && fixableFindings.length > 0 && (
                        <LemonButton
                            className="mt-2"
                            type="secondary"
                            size="small"
                            icon={<IconSparkles />}
                            loading={fixWithAILoading}
                            onClick={() =>
                                onFixWithAI(
                                    fixableFindings.map((finding, index) => `${index + 1}. ${finding.fix}`).join('\n')
                                )
                            }
                            data-attr="query-scan-fix-with-ai"
                        >
                            Fix with AI
                        </LemonButton>
                    )}
                </LemonBanner>
            )}
        </div>
    )
}
