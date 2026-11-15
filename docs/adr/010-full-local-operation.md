# ADR 010 Full local operation and explicit external acceptance

Status: Accepted, 2026-11-15.

The frozen architecture remains unchanged. Complete the operational and consumption
path using a local read-only dashboard over certified exports, an HTTP metrics
exporter, bounded live FX extraction, dedicated FX scheduling, verified product
backup/restore and conservative retention planning. These functions use the same
release contract rather than introducing another mutable reporting database.

Readers receive a configured root explicitly. Bundle verification checks paths,
symbolic links, hashes, CSV shape and row counts. New certifications record their
model inventory. Restores never publish implicitly. Readiness verifies exported
content rather than trusting status alone. Source revision fallback and full Python
package discovery permit distribution without Git history. Docker contexts exclude
runtime, private environment variants and Terraform state/providers.

The full lifecycle is executable on a dedicated Compose project. Seed is explicit;
stop preserves persistent state. Streaming can drain available offsets for finite
acceptance or use a continuous worker with preserved checkpoint. No unrelated Docker
project is modified to fit memory limits.

This does not claim general CDC, incremental Gold, distributed coordination/HA,
authenticated shared-service security, cloud deployment, native Power BI execution
or full infrastructure disaster recovery. Those are distinct acceptance scopes.
The repository contains cloud preflight and an opt-in synthetic object probe so
the account owner can supply identity/configuration and collect actual evidence.
