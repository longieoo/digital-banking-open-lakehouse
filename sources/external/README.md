# Public exchange-rate interface

[Frankfurter v1](https://frankfurter.dev/v1/) exposes public historical JSON reference
rates. Live request attempted 2026-09-08:
`https://api.frankfurter.dev/v1/2026-01-30?base=USD&symbols=EUR` returned HTTP 403.
Live extraction is **UNAVAILABLE** in this environment; no live rates are claimed.

`frankfurter-fixture.json` is an invented deterministic interface fixture, not an
actual market quote. `python -m bank.external` ingests it into a clearly named
Bronze fx_rates_fixture table through the same durable raw/manifest workflow.
`--live` uses the public endpoint and fails explicitly if unavailable; it never
silently substitutes the fixture. Live data uses a separate fx_rates_live table.
Rates are not used to fabricate converted balances; Gold keeps native currency.
