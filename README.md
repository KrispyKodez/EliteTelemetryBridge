# EliteTelemetryBridge

EliteTelemetryBridge is a Windows-first, open middleware bridge for Elite
Dangerous. It captures every journal event and companion JSON update once, then
offers stable files, HTTP snapshots, and a live WebSocket stream to tools such
as Streamer.bot, OBS integrations, VoiceAttack, chatbots, virtual crew members,
Home Assistant, and custom Python or C# clients.

Unknown Frontier events are never rejected or filtered from processing. If a
new event appears tomorrow, its complete JSON object automatically travels
through the Event Bus and WebSocket stream.

## Privacy warning

Journal and state data can contain a commander name, missions, cargo, location,
communications, and other gameplay information. Never publish the `output`
directory or a populated `telemetry-settings.json`. Both are excluded by the
included `.gitignore`.

## Portable Windows release

1. Download and extract `EliteTelemetryBridge-v0.5.1-windows-x64.zip`.
2. Optionally edit `telemetry-settings.json`.
3. Run `EliteTelemetryBridge.exe`.
4. Leave the console window open while playing.
5. Press `Ctrl+C` for a graceful shutdown.

Python is not required for the portable release. Settings and output are kept
beside the executable.

## Running from source

Python 3.11 or newer is required; releases are tested with Python 3.13.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item telemetry-settings.example.json telemetry-settings.json
.\.venv\Scripts\python.exe app.py
```

Without an override, journals are discovered at:

```text
%USERPROFILE%\Saved Games\Frontier Developments\Elite Dangerous
```

## Interfaces

The default local service is available only on the same computer.

| Interface | Address |
| --- | --- |
| Service description | `http://127.0.0.1:8765/` |
| Health | `http://127.0.0.1:8765/health` |
| Complete state | `http://127.0.0.1:8765/state` |
| Commander | `http://127.0.0.1:8765/commander` |
| Ship | `http://127.0.0.1:8765/ship` |
| Cargo | `http://127.0.0.1:8765/cargo` |
| Live events | `ws://127.0.0.1:8765/events` |

### WebSocket protocol

A new connection receives:

1. `hello` with the current and oldest replayable sequence.
2. `snapshot` containing complete current state.
3. An ordered `event` message for every new journal or companion event.

Events retain the complete parsed Frontier payload:

```json
{
  "message_type": "event",
  "sequence": 1842,
  "event_id": "Journal.2026-01-01T000000.01.log:476213",
  "source": "journal",
  "source_file": "Journal.2026-01-01T000000.01.log",
  "event_type": "Docked",
  "timestamp": "2026-01-01T00:00:00Z",
  "event": {
    "event": "Docked",
    "StationName": "Example Station",
    "StarSystem": "Example System"
  }
}
```

Store the last successfully handled sequence and reconnect with:

```text
ws://127.0.0.1:8765/events?after=1842
```

The bridge replays retained events. If the cursor is too old or belongs to a
previous runtime buffer, it sends `resync_required` followed by a fresh
snapshot. Slow clients are disconnected instead of blocking journal capture.

Archive filters do not affect WebSocket delivery. `Music`, `ReceiveText`, and
deduplicated `ShipLocker` events are still delivered live.

### Streamer.bot

Streamer.bot does not need Python and does not read the journal files. Run the
bridge separately, then install the paste-ready C# connector from
`integrations/streamerbot`. It turns every generic WebSocket event into a
Streamer.bot custom trigger while preserving the complete payload and replay
cursor.

The connector setup guide documents the available trigger variables and
recovery behavior. The application itself remains contained in `app.py`; the
integration directory contains only consumer examples.

## Output files

```text
output/
  raw/          Append-only journal archives
  state/
    latest.json Complete current state snapshot
  companions/   Exact current companion-file mirrors
  cache/        Recovery checkpoints
```

Production archive mode skips `Music` and `ReceiveText`, and deduplicates
semantically identical `ShipLocker` events. Archive mode stores every journal
line exactly as Elite wrote it. Every event is processed internally in either
mode.

## Important settings

| Setting | Default | Purpose |
| --- | --- | --- |
| `journal_directory` | automatic | Override Elite's journal directory |
| `archive_mode` | `production` | Use `archive` for a complete raw archive |
| `show_raw_json` | `false` | Print full event JSON to the console |
| `read_existing_events_on_first_run` | `true` | Ingest existing journals chronologically |
| `api_enabled` | `true` | Enable HTTP and WebSocket interfaces |
| `api_host` | `127.0.0.1` | Local bind address |
| `api_port` | `8765` | Shared HTTP/WebSocket port |
| `api_replay_buffer_events` | `2000` | In-memory reconnect history |
| `api_client_queue_events` | `256` | Per-client backpressure limit |

Binding to anything other than localhost can expose private telemetry to the
network. Authentication is not implemented in version 0.5.0.

## Development

Install development dependencies and run the persistent test suite:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Create clean source and portable Windows releases:

```powershell
.\build-release.ps1
```

Generated artifacts are written to `release/` and are intentionally excluded
from source control.

## Architecture

```text
Elite Dangerous
  -> Journal and companion readers
  -> Event Bus
     -> Raw archive
     -> RAM state engine
     -> Ordered live-delivery hub
        -> HTTP snapshots
        -> WebSocket clients
```

Readers have no knowledge of downstream clients. Future plugins can subscribe
to the Event Bus without modifying journal ingestion.

## License and disclaimer

EliteTelemetryBridge is released under the MIT License. It is an independent
community project and is not affiliated with or endorsed by Frontier
Developments. Elite Dangerous is a trademark of Frontier Developments plc.
