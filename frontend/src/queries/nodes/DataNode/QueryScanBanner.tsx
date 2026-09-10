import clsx from 'clsx'
import { useValues } from 'kea'

import { IconSparkles } from '@posthog/icons'

import { LemonBanner } from 'lib/lemon-ui/LemonBanner'
import { LemonButton } from 'lib/lemon-ui/LemonButton'

import { uiCustomizationLogic } from '~/layout/uiCustomizationLogic'

import { QueryScanState, fixableQueryScanFindings, queryScanStatLine } from './queryScan'

export interface QueryScanBannerProps {
    queryScan: QueryScanState | null
    /** Opens the assistant on the findings. Left out where there is no editor to write into. */
    onFixWithAI?: () => void
    className?: string
}

/** A finding marks SQL with backticks, the way the assistant reads it. */
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

export function QueryScanBanner({ queryScan, onFixWithAI, className }: QueryScanBannerProps): JSX.Element | null {
    const { showQueryScanAdvice } = useValues(uiCustomizationLogic)

    if (!queryScan) {
        return null
    }

    const { summary, findings } = queryScan
    const showFindings = summary.status === 'done' && findings.length > 0 && showQueryScanAdvice
    const fixableFindings = fixableQueryScanFindings(findings)

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
                            onClick={onFixWithAI}
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
