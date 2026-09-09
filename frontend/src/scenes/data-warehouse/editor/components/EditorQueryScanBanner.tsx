import { useActions, useValues } from 'kea'

import { dataNodeLogic } from '~/queries/nodes/DataNode/dataNodeLogic'
import { QueryScanBanner } from '~/queries/nodes/DataNode/QueryScanBanner'

import { fixSQLErrorsLogic } from '../fixSQLErrorsLogic'
import { sqlEditorLogic } from '../sqlEditorLogic'

/** The query scan of the editor's last run, with its advice wired to the SQL fixer. */
export function EditorQueryScanBanner(): JSX.Element | null {
    const { queryScan } = useValues(dataNodeLogic)
    const { queryInput, selectedConnectionId } = useValues(sqlEditorLogic)
    const { fixErrors } = useActions(sqlEditorLogic)
    const { responseLoading: fixErrorsLoading } = useValues(fixSQLErrorsLogic)

    return (
        <QueryScanBanner
            className="m-2"
            queryScan={queryScan}
            fixWithAILoading={fixErrorsLoading}
            onFixWithAI={(instruction) => fixErrors(queryInput ?? '', undefined, selectedConnectionId, instruction)}
        />
    )
}
