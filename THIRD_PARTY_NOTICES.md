# Third-party notices

AAS-authored code is licensed under Apache-2.0. Third-party components retain
their own licenses; this file does not relicense them or any provider data.

## Vibe-Trading

Source: [HKUDS/Vibe-Trading](https://github.com/HKUDS/Vibe-Trading).
License: MIT, Copyright (c) 2026 Vibe-Trading Contributors.

| AAS material | Source and changes |
| --- | --- |
| `src/aegis_alpha/data/sec_periods.py` | Adapted reporting-period identity from `agent/backtest/loaders/sec_frames.py` at revision `fb5013c2e37ff992ce2e76f0e19219d631eebc2b`. Uses standard-library dates, rejects invalid/reversed intervals, preserves absent starts and exposes explicit cadence. Its source file retains the full MIT notice. |

### MIT notice for the Vibe-Trading-derived material

MIT License

Copyright (c) 2026 Vibe-Trading Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## exchange_calendars

Source: [gerrymanoim/exchange_calendars](https://github.com/gerrymanoim/exchange_calendars),
release 4.13.2. License: Apache-2.0.

| AAS material | Source and changes |
| --- | --- |
| `src/aegis_alpha/storage/calendar_declarations/xnys.json`, `xkrx.json` | Session dates and local hours for 1990-01-01..2027-12-31 taken from the library's `XNYS` and `XKRX` schedules by `scripts/calendar_declarations.py`, which does not import the library at runtime and is not an AAS dependency. The script restates each schedule as hour regimes, closed regime weekdays and sessions with other hours, and applies AAS corrections it lists with their evidence: KRX election-day and 2026 holiday closures, one KRX session evidenced by traded volume, KRX CSAT-day hours 2021-2026, and regular hours for one KRX session. Behavior checks: `tests/storage/test_calendar_declaration.py` against published holidays, early closes and late opens. |

## Data and strategy rights

The software license grants no rights to market/provider datasets, collected
strategy definitions, private strategy implementations, performance records,
holdings or credentials. Those remain outside this repository and its images.
Public examples and tests use synthetic inputs. Each operator is responsible
for the rights and access scope of data they supply.
