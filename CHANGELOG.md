# Changelog

## 0.5.1 - 2026-07-15

- Added a paste-ready Streamer.bot C# connector and setup guide.
- Added generic Streamer.bot triggers for events, snapshots, connection state,
  and replay resynchronization.
- Added automatic top-level payload variables while preserving complete JSON
  for unknown and nested fields.
- Added persistent replay-cursor recovery for Streamer.bot reconnects.
- Included consumer integrations in source and portable Windows releases.

## 0.5.0 - 2026-07-15

- Added a generic synchronous Event Bus for journals and companion files.
- Added exact raw archiving with archive and production modes.
- Added RAM-first live state and atomic `latest.json` output.
- Added automatic ingestion of all current and future companion JSON files.
- Added journal rollover, checkpoint recovery, and Windows file-lock retries.
- Added localhost HTTP state endpoints.
- Added ordered WebSocket delivery for every event, independent of archive
  filters.
- Added reconnect cursors, bounded replay, resynchronization snapshots, and
  slow-client isolation.
- Added Windows portable-build support and release documentation.
