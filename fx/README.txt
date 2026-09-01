BahtFlow FX Daily Sparse Source
================================

Source:
- Bank of Thailand reference rates via Frankfurter (provider=BOT)
- Currencies: EUR and USD
- Rate convention: THB per 1 unit of foreign currency

Layout:
fx_daily/YYYY/MM/fx_YYYYMMDD.csv

Behavior:
- One CSV per BOT-published rate date.
- Each daily CSV contains exactly 2 rows: EUR and USD.
- No artificial files are created for weekends or holidays.
- No carry-forward logic is applied in this source package.
- Carry-forward / effective-date logic belongs in the downstream transformation layer.

Coverage:
- First published date: 2025-07-08
- Last published date: 2026-07-21
- Published-date files: 251
- Data rows: 502

Original source file in GitHub is unchanged.
