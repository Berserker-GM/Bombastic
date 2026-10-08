# SPEC

## Goal
Answer: "What important opportunities or deadlines that matter to me appeared or changed today?"
Cloud job scans sources once daily (12:00-17:00 IST). Desktop tray client pulls a personalized feed.

## Core entities
- Source: id, kind (api|scrape|college), name, config JSON, enabled,
  health_status (healthy|degraded|failed|stale|disabled), last_success_at, last_failure_at,
  consecutive_failures, last_error
- Event (normalized): id, source_id, external_id (nullable), fingerprint, category
  (contest|hackathon|college|scholarship|club|placement|other), title, url (the "View source" link),
  start_at, end_at, registration_deadline, location, mode (online|offline|hybrid|unknown),
  prize_text, prize_amount_inr (nullable), organizer, sponsors JSON, eligibility,
  team_min, team_max, tags JSON,
  lifecycle (upcoming|registration_open|registration_closed|ongoing|completed|postponed|cancelled|unknown),
  confidence JSON (field -> 0..1; a missing key means 1.0, the default for API sources),
  extra JSON (source-specific fields, plus other_dates: [{type, label, at, confidence}]),
  first_seen_at, last_seen_at, content_hash
  UNIQUE(source_id, external_id) when external_id is not null.
- EventEvidence: id, event_id, field, source_url, source_text (verbatim quote), confidence, captured_at
  (required for every LLM-extracted date, deadline, eligibility and location)
- EventVersion: id, event_id, seen_at, changed_fields JSON (field -> [old, new]),
  importance (important|minor), snapshot JSON
- UserPrefs: user_id, JSON (modes, locations, min_prize, student_only, companies, topics,
  contest_platforms, calendar_mode auto|confirm)
- UserEventState: user_id, event_id, state (new|seen|ignored|saved), relevance (0-100),
  urgency (critical|soon|upcoming|later|none), reasons JSON, notified_at
  ("added to calendar" is derived from CalendarLink, not stored here)
- CalendarLink: user_id, event_id, google_event_id, last_synced_hash
- ScanRun: id, started_at, finished_at, status (running|done), per_source JSON:
  {source: {result: ok_new|ok_nothing_new|unreachable|malformed, counts, error}}

## Rules
- All datetimes stored UTC, displayed in user's timezone (default Asia/Kolkata).
- Same real-world thing must never create two Events. Match order: (source, external_id) ->
  fingerprint (normalized title + date proximity) -> else new.
- Any change to a tracked field creates an EventVersion. Changes to IMPORTANT_FIELDS
  (registration_deadline, start_at, end_at, location, mode, eligibility, lifecycle, prize_amount_inr)
  are importance=important and notify the user. All other changes are minor (history only).
- Unchanged content_hash means skip all downstream work (saves LLM cost). Titles are
  whitespace/case-normalized before hashing so cosmetic differences are not changes.
- Three independent signals, never merged into one number:
  relevance (how much the user cares), confidence (how sure we are the data is right),
  urgency (how soon the key date is). Key date = registration_deadline for college/hackathon,
  start_at for contests. Urgency buckets: critical <24h, soon <3d, upcoming <7d, later >=7d.
- Low confidence never hides an event. It only changes the label shown to the user.
- College-category events always get relevance 100 and are never hidden.
- Saved events are never hidden by filters. Ignored events never appear.
- Never guess. If a field is not explicitly supported by the source, store null/unknown.
  A date that is merely mentioned in a notice is not a deadline. Missing beats confidently wrong.
- Every source run ends in exactly one of four results: ok_new, ok_nothing_new, unreachable, malformed.
  "0 results" never stands in for a failure. A source that normally returns events and returns none
  is malformed ("suspiciously empty"). A failed run never marks existing events as gone.
- A failing source never fails the whole scan. It is recorded in ScanRun and Source health and shown to the user.
- Source health: healthy = last run ok; degraded = 1-2 consecutive failures; failed = 3+;
  stale = no success in 48h; disabled = enabled is false.
- LLM is used only for unstructured text (college notices). APIs use deterministic parsing.
  The LLM is never called when content_hash is unchanged. LLM output is validated against a Pydantic
  schema, retried once, then recorded as a failure. Every extracted date must carry a verbatim quote
  that exists in the source text, or the field is dropped.