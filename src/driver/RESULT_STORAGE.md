
## Fixed state budget and genuine results

Driver V7 keeps the 64 MiB state and Wire budgets. A Wire and its bounded
ownership/replay envelope have separate size limits. After all existing genuine
receipt checks pass, a settlement whose resident receipt, outcome publication,
join or derived input copies exceed state capacity commits a typed
`ResultStorageFailure::DriverCapacity`. Invalid receipts still reject unchanged.
Progress checks each bounded publication/join batch and each newly ready task
before materializing further fanout, rather than building every output copy first.

The exact worker releases its lifetime permit only when the host has durably
committed and applied that transition. Core retains the original receipt digest
and genuine statuses in immutable task job order, available through
`unavailable_worker_receipt`. The full original Wire remains in the Settle
transition. Core never changes the worker's real success to failure or publishes
empty successful outputs in place of unavailable values.

The failure halts the whole run: unclaimed work becomes
`CoreDecision::InfrastructureBlocked`, new readiness and joins stop, and other
claimed owners receive normal cancellation requests. Their permits remain held
through ACK until an actual receipt or an exact never-dispatched host abort.
Already expanded origins close without publishing a synthetic matrix aggregate.
`shard_join_availability` explicitly reports unavailable aggregation; this is not
missing genuine test coverage. Existing settled workers and published outcomes
are retained. The user cancellation flag remains independent of storage failure.

Every admitted claim reserves its bounded receipt-status terminal using the
actual escaped owner and immutable job count, including after dispatch and
cancellation ACK. Admission rejects before consuming these guarantees. The
reserve also covers unstarted terminals, outstanding cancellation actions and
the final completion envelope. State capacity rejection cannot prove that a
worker stopped; only the validated receipt releases that owner.

## Required host adapter behavior

The ordinary Runner has not switched to Driver as part of this slice. A future
universal adapter must retain each actual TaskResult and full committed Wire,
check `infrastructure_failure` before declaring run success, and emit a separate
run infrastructure error/nonzero exit even when all actual workers succeeded.
Report those workers with their genuine statuses and logs; report never-started
tasks as infrastructure-blocked without inventing worker results. Never treat
`Finished`, an absent `outcome`, or an absent convenience `shard_join` as success.
Never hydrate missing outputs as empty values or dispatch further work using
outcomes retained from before the halt. Durable journal write failure means no
apply, permit-release report, or dependent dispatch. Cancellation delivery,
worker joining and the original host cleanup/report responsibilities remain.

This explicit fixed-budget failure policy may stop unrelated pending work. It
does not supply durable-reference hydration, success-preserving progress for
arbitrarily large valid results, codec support beyond the Wire limit, or fenced
termination of unreachable workers. No legacy scheduler fallback is implied.
