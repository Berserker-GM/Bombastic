# College notice fixtures

Each `noticeNN.txt` is a raw notice, kept exactly as received (typos and emojis included).
Each `noticeNN.expected.json` is the hand-written expected extraction for it.

## Schema (same keys in every file, `null` when absent)

title, content, actionable, registration_opens, date, start, end,
deadline, deadline_extended, venue, link

- Dates are ISO: `YYYY-MM-DD`, or `YYYY-MM-DDTHH:MM` (24h) when the notice gives a time.
- If a notice gives no time, the value is date-only. Never invent a time.
- `date` = a single-day event. `start`/`end` = a multi-day event.
- `deadline` = the original deadline. `deadline_extended` = the new one.
- `actionable: false` means nothing for a calendar/radar (all date fields null).

## Test settings

- Fixed TODAY = 2026-10-11 (Sunday). Never use the real clock in tests.
- Year-less dates resolve to TODAY's year. Relative dates ("this Friday") resolve from TODAY.
- Compare `title` and `content` fuzzily (or skip them). Compare every other field exactly.

## What each fixture covers

| File | Case |
|------|------|
| 01 | Event date only mentioned, no deadline |
| 02 | Multi-day event + deadline, no year, no times, typo in title |
| 03 | Deadline with a time + inline extension (extension has no time) |
| 04 | Not actionable (circular is an attachment) |
| 05 | Standalone extension notice (original deadline not stated) |
| 06 | Many dates: registration opens, deadline, event |
| 07 | Relative date ("this Friday") |

Notices 05, 06 and 07 are synthetic. Replace them with real notices when you get some.
