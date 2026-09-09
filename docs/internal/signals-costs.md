# Signal processing costs

Signals accumulate costs during processing.
Grouping always publishes each signal with its costs at that point.
Research or implementation can re-emit the same signal with updated costs.
No stage looks up costs retrospectively in product analytics.

```json
{
  "token_cost": { "research": 12, "implementation": 0 },
  "compute_cost": { "research": 0, "implementation": 0 }
}
```

Every amount is an integer number of cents.
`research` includes emission checks, safety, grouping, repository selection, and research.
An implementation run charges only the signal that triggered it, not every signal in the report.

## Pricing

`signal_costs.token_usage_to_spend` maps returned token usage to cents using the gateway model catalog.
It accounts for uncached input, output, cache reads, and cache writes.
Catalog rates remain strings; Decimal arithmetic is local to that conversion.
Each accepted response rounds to the nearest cent, with halves rounded up.
Only integer cents enter signal metadata or cross a stage boundary.

`normalise_cost(model, spend)` is a passthrough hook for future pricing policy.
`add_cost` applies it when recording spend; `merge_costs` combines amounts that were already normalized.
Failed internal attempts are not passed on to the user.
Catalog requests use a ten-second timeout without SDK retries.
The catalog cache expires after one hour; a failed refresh can reuse cached prices and tries again after one minute.
An unpriced model raises rather than silently reporting zero.

`TaskRun.get_current_spend()` is deliberately a placeholder returning zero token and compute cents.
Repository selection, research, and implementation call it through the Tasks facade.
Task-backed costs remain zero until runtime accounting replaces the placeholder.
Embedding API usage is not priced by this mapping.

## Publication and handoffs

- Grouping assigns the signal to its report and always sends it through the embedding worker with its accumulated metadata.
- The batch waits for these initial publications to become visible in ClickHouse before dispatching research or processing another batch.
- This wait covers embedding ingestion only, not research or implementation.
- Signals needing further work also have an S3 handoff at `signals/processing/<team_id>/<signal_id>.json`.
- Research reads report context from ClickHouse and updates the triggering signal's cost metadata in its S3 handoff.
- If implementation starts, the updated handoff passes to its finalizer; otherwise, the research stage re-emits the updated signal.
- Implementation finalizers use short status polls and workflow timers. Once the implementation workflow closes, they add its spend and re-emit the signal.

Within-batch matching uses the existing in-memory batch context.
Later batches search ClickHouse, where the initial grouping publication is already visible.
There is no team-wide pending-handoff registry, S3 semantic-search overlay, or grouping release signal.
S3 handoffs carry signal data and cost metadata, not embeddings.

The first promotion, ordered by assigned signal count, owns the initial research pass's cost.
A later pass charges the handoff that crosses the next research bucket.
Other signals retain their own costs, and arrivals below the next bucket do not trigger another research pass.
Implementation runs carry their owning handoff key in protected run state.

Each re-emission preserves the signal ID and original timestamp so ClickHouse updates the existing logical signal.
The handoff's `finalized` marker records that its final-stage update was sent; the initial grouping publication does not set it.
Signals without implementation work can be finalized in batches of up to 20.
All publications use the embedding worker; later cost updates need no separate ClickHouse visibility wait.
Unsafe or deleted reports emit deleted signal metadata.

Add an object-storage lifecycle rule before rollout to expire `signals/processing/` handoffs after a few days.

## Temporal compatibility

`signals-stage-handoffs-v1` gates the cost-aware workflow path.
Histories without that patch retain the original emission and summary path.
Activity fields have defaults for older payloads.
Tests cover the initial batch visibility barrier, later-stage re-emission, and replay of a summary history recorded with the patch disabled.
