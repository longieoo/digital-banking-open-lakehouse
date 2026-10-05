# Public exchange-rate interface

[Frankfurter v2](https://frankfurter.dev/) exposes public daily reference rates
without an API key. Live extraction uses
`https://api.frankfurter.dev/v2/rate/USD/EUR` with explicit JSON/User-Agent headers.
The previous fixed-date v1 request returned HTTP 403 in this environment.
The v2 request succeeded on 2026-10-05; it returns the latest available business
date, which may precede today on weekends or holidays. This is a daily reference
rate, not an intraday executable bank quote. The default v2 feed blends providers.

Fetch and validate without Docker: `python -m bank.external --live --fetch-only`.
Fetch and ingest into raw/Bronze: `python -m bank.external --live` with the normal
lakehouse environment and services running. Repeating this command fetches the
latest available rate; this change does not install a recurring scheduler.
The ingestion source version is the business date: same-day provider corrections
are not currently tracked as new versions. Use a provider-specific feed and a
revision-aware ingestion contract before relying on intraday corrections.

`frankfurter-fixture.json` is an invented deterministic interface fixture, not an
actual market quote. `python -m bank.external` ingests it into a clearly named
Bronze fx_rates_fixture table through the same durable raw/manifest workflow.
`--live` uses the public endpoint and fails explicitly if unavailable; it never
silently substitutes the fixture. Live data uses a separate fx_rates_live table.
Rates are not used to fabricate converted balances; Gold keeps native currency.
