import clsx from 'clsx'
import { useValues } from 'kea'

import { IconSparkles } from '@posthog/icons'

import { LemonBanner } from 'lib/lemon-ui/LemonBanner'
import { LemonButton } from 'lib/lemon-ui/LemonButton'

import { uiCustomizationLogic } from '~/layout/uiCustomizationLogic'

import { QueryScanState, queryScanStatLine } from './queryScan'

export interface QueryScanBannerProps {
    queryScan: QueryScanState | null
    /** Hands the findings' fix texts to the SQL fixer. Left out where there is no editor to write into. */
    onFixWithAI?: (instruction: string) => void
    fixWithAILoading?: boolean
    className?: string
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

    return (
        <div className={clsx('flex flex-col gap-2 shrink-0', className)} data-attr="query-scan">
            <span className="text-xs text-secondary">{queryScanStatLine(summary)}</span>
            {showFindings && (
                <LemonBanner type="warning">
                    <ul className="list-disc pl-5">
                        {findings.map((finding, index) => (
                            <li key={`${finding.kind}-${index}`}>
                                {finding.message}
                                {finding.clause && (
                                    <div className="mt-1 overflow-x-auto">
                                        <code className="text-xs whitespace-pre">{finding.clause}</code>
                                    </div>
                                )}
                            </li>
                        ))}
                    </ul>
                    {onFixWithAI && (
                        <LemonButton
                            className="mt-2"
                            type="secondary"
                            size="small"
                            icon={<IconSparkles />}
                            loading={fixWithAILoading}
                            onClick={() => onFixWithAI(findings.map((finding) => finding.fix).join(' '))}
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
