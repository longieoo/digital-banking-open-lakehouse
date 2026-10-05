# ADR 008: Fetch daily real FX through Frankfurter v2

Date: 2026-10-05. Status: accepted.

The fixed-date v1 request failed with HTTP 403. The public v2 single-pair route
works in this environment without credentials and returns the latest available
USD/EUR daily reference rate. Use explicit JSON/User-Agent headers and retain
Decimal parsing, validation, LIVE provenance and separate live/fixture tables.
HTTP failures remain explicit; never substitute fixture data for live data.

Add --fetch-only so API access can be verified independently of Docker. Existing
ingestion semantics remain unchanged. No scheduler, Gold currency conversion or
intraday quote support is introduced. Default v2 rates blend provider observations.
