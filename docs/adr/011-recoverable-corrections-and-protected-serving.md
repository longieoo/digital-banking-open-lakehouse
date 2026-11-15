# ADR 011 Recoverable corrections and protected serving

Status: Accepted, 2026-11-15.

The release runner previously invoked dbt outside certification's synchronization,
so a second runner could rebuild a schema while another exported or certified it.
Use a reentrant per-release process lock across every mutating release stage,
recheck status under that lock, stage exports before their final rename, and
validate the model inventory against the project SQL files. Keep the global
publication lock only for the shared pointer and source writer coordination.

Streaming rule output has two meanings: current truth and historical observation.
Store them separately. Current alerts retract on corrections through an atomic
MERGE; the observation audit is written first with retry-stable identities.
Evaluate affected old/new accounts, with a full repair on policy change/bootstrap.

FX has no provider revision timestamp, so a date-midnight version cannot represent
same-day corrections. Persist locally observed content versions before durable
ingestion; equivalent replay reuses the version and a content reversion gets a new
version. Do not claim these local timestamps are source-authored revisions.

Expose an explicit protected-serving mode with optional rotating file/env Bearer
credentials, origin/host guards and minimal liveness. Keep anonymous mode limited
to trusted local use; TLS, rate limits and backend-service identity remain external
deployment requirements. These changes close concrete correctness/security defects
without claiming general CDC, distributed transactions or production HA.
