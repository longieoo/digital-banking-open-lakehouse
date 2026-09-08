# ADR 006: Redpanda with shared batch/stream conformance

## Context
Replays and overlapping batch/stream events must not create extra money.
## Decision
Use one Kafka-compatible Redpanda broker. Version-based event IDs and a shared
single-host Bronze-writer lock serialize immutable source-version merges. Invoke
the existing Spark Silver transformation, then deterministic SQL risk rules.
## Alternatives
A separate streaming model risks inconsistent reversal/dedup logic. A distributed
stateful risk engine is beyond the educational scope.
## Consequences
Checkpoint/merge replay is at-least-once, not cross-table exactly-once. Retained
history is recomputed per microbatch, simple but bounded by demo volume. Lateness
is classified against the prior maximum event time for newly observed IDs; old
timely events are not retrospectively relabeled late.
