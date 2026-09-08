import { describe, expect, it } from 'vitest'
import { z } from 'zod'

import { createExecTool } from '@/tools/exec'
import { GENERATED_TOOL_MAP } from '@/tools/generated'
import { getToolDefinitions } from '@/tools/toolDefinitions'
import type { Context, Tool, ToolBase, ZodObjectAny } from '@/tools/types'

/**
 * The mirror of `nested-query-input-example.test.ts`: a query wrapper takes the
 * query fields as its own top-level parameters, so an example written as a
 * document — the whole query nested under `query`, or a `properties` group
 * object where the parameter is a flat array — teaches a call the tool rejects.
 *
 * These keep every documented example a payload the tool accepts, through both
 * routes a caller reaches it by: the tool schema directly, and the `exec` CLI,
 * which validates the same input behind `call <tool> <json>`.
 */

const TOP_LEVEL_QUERY_TOOLS = [
    'query-trends',
    'query-funnel',
    'query-retention',
    'query-stickiness',
    'query-paths',
    'query-lifecycle',
] as const

const definitions = getToolDefinitions()

/** Every fenced ```json block in a description, parsed; unparseable blocks are
 *  skipped because a description may show a fragment rather than a whole call. */
function jsonExamples(description: string): unknown[] {
    const examples: unknown[] = []
    for (const match of description.matchAll(/```json\n([\s\S]*?)```/g)) {
        try {
            examples.push(JSON.parse(match[1]!))
        } catch {
            continue
        }
    }
    return examples
}

const mockContext = { getDistinctId: async () => 'test-distinct-id' } as unknown as Context

/** The generated tool with its handler replaced, so a `call` exercises the
 *  validation gate without reaching the API. */
function asExecutableTool(name: string, base: ToolBase<ZodObjectAny>): Tool<ZodObjectAny> {
    return {
        ...base,
        title: name,
        description: definitions[name]?.description ?? '',
        scopes: [],
        annotations: { destructiveHint: false, idempotentHint: true, openWorldHint: false, readOnlyHint: true },
        handler: async () => ({ results: [] }),
    }
}

describe('tools that take their query fields at the top level', () => {
    it.each(TOP_LEVEL_QUERY_TOOLS)('%s takes no nested query parameter', (name) => {
        // Guards the premise: were one of these to grow a `query` wrapper, its
        // examples would have to nest, and the cases below would be backwards.
        const schema = z.toJSONSchema(GENERATED_TOOL_MAP[name]!().schema, { io: 'input' }) as Record<string, unknown>
        const properties = schema['properties'] as Record<string, unknown> | undefined

        expect(properties && 'query' in properties).toBeFalsy()
    })

    it.each(TOP_LEVEL_QUERY_TOOLS)('%s documents examples its own schema accepts', (name) => {
        const tool = GENERATED_TOOL_MAP[name]!()
        const examples = jsonExamples(definitions[name]?.description ?? '')

        expect(examples.length, `${name} needs at least one \`\`\`json example`).toBeGreaterThan(0)
        for (const example of examples) {
            const result = tool.schema.safeParse(example)
            expect(result.success, `${name} documents an example it rejects: ${JSON.stringify(result)}`).toBe(true)
        }
    })

    it.each(TOP_LEVEL_QUERY_TOOLS)('%s accepts its documented examples through the exec CLI', async (name) => {
        const exec = createExecTool(
            [asExecutableTool(name, GENERATED_TOOL_MAP[name]!())],
            mockContext,
            'test description',
            'test command reference',
            undefined
        )

        for (const example of jsonExamples(definitions[name]?.description ?? '')) {
            const output = await exec
                .handler(mockContext, { command: `call ${name} ${JSON.stringify(example)}` })
                .catch((error: Error) => `rejected: ${error.message}`)

            expect(String(output)).not.toContain('Invalid input')
        }
    })

    it('documents both a single-series and a multi-series trends example', () => {
        // The two shapes callers ask for. A description that shows only the
        // elaborate ones leaves the minimal call to be guessed.
        const series = jsonExamples(definitions['query-trends']?.description ?? '')
            .filter((example): example is { series: unknown[] } => Array.isArray((example as any)?.series))
            .map((example) => example.series.length)

        expect(series).toContain(1)
        expect(series.some((count) => count > 1)).toBe(true)
    })
})
