# Rule-based real-time transaction monitoring

Redpanda is the Kafka-compatible event transport. The producer keys by account so
ordering is partition-local, never global. Three partitions illustrate parallel
consumption. The producer uses acknowledgments and idempotence, but application
replays remain possible: event ID hashes the transaction ID and canonical UTC source
version. The Spark checkpoint records offsets; retain it across restarts.

Start the batch demo first (accounts and Bronze must exist). Then start Redpanda,
create the topic, run the producer and `streaming/consumer.py` in Spark. Producer
events intentionally overlap batch data and include out-of-order replay/duplicates.
The microbatch persists an event audit, merges immutable Bronze versions and invokes
the existing Silver conformance function. This extra Bronze step is deliberate:
batch and stream converge without two different reversal or validation engines.

Rules use real Spark SQL: configurable high amount, three successful outgoing
transfers in five event-time minutes, and three failures in five minutes. Alert ID
is transaction/rule; reruns upsert rather than duplicate. Alerts record observations
and are not automatically retracted when source status changes. This is not fraud
classification or machine learning.

Late events are retained and recomputed, not dropped. A ten-minute lateness audit
compares newly observed events to the maximum from prior microbatches. Deduplication lives in Iceberg
and survives checkpoint replay. There is no claim of bounded Spark watermark state:
this educational implementation rescans retained history per microbatch and needs
retention/incremental window state before high-volume use. A failed microbatch may
leave earlier table commits; retry repairs them before checkpoint advancement.

Local Docker validation on 2026-09-16/17 delivered 230 messages (225 unique events),
restarted the consumer with its checkpoint, and replayed the producer without
increasing unique events or Bronze versions. The small fixture produces no alerts
at the default threshold; the Spark integration test verifies all three rules with
controlled events, including the exact amount threshold boundary.
