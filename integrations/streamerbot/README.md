# Streamer.bot connector

Streamer.bot does not run the Python application or read Elite journal files.
The bridge runs separately, and this C# module consumes its language-neutral
WebSocket API.

The connector requires Streamer.bot 0.2.5 or newer because it uses custom code
triggers with argument dictionaries.

## Install

1. Start `EliteTelemetryBridge.exe` (or `python app.py`) and confirm
   `http://127.0.0.1:8765/health` opens.
2. In Streamer.bot, create an enabled action named
   `EliteTelemetryBridge Connector`.
3. Add `Core > C# > Execute C# Code` to that action.
4. Paste the entire contents of `EliteTelemetryBridge.cs` into the editor.
5. Name the code sub-action `EliteTelemetryBridge Connector`.
6. Enable **Precompile on Application Start**.
7. Select **Find Refs**, then **Save and Compile**. Streamer.bot includes
   Newtonsoft.Json; Find Refs should also select the required .NET WebSocket
   assemblies.

`Init()` starts the connection automatically. The action itself does not need a
normal trigger and does not need to be executed manually.

After the first successful compile, these triggers appear under
`Custom > EliteTelemetryBridge`:

- `Elite Telemetry Event`
- `Elite Telemetry Snapshot`
- `Elite Telemetry Connection Changed`
- `Elite Telemetry Resync Required`

Add `Elite Telemetry Event` as the trigger for any Streamer.bot action that
should react to Elite Dangerous. Check `%eliteEventType%` when an action only
wants a particular event, such as `Docked` or `FSDJump`.

## Event variables

Every event includes these stable variables:

| Variable | Contents |
| --- | --- |
| `%eliteEventType%` | Journal or companion event name |
| `%eliteSequence%` | Ordered delivery/reconnect cursor |
| `%eliteSource%` | `journal` or `companion` |
| `%eliteSourceFile%` | Original source filename |
| `%eliteTimestamp%` | Frontier timestamp when available |
| `%elitePayloadJson%` | Complete original parsed event object |
| `%eliteMessageJson%` | Complete bridge WebSocket envelope |

Every top-level payload field is also exposed automatically with an `elite_`
prefix. For example, a `Docked` event can provide:

```text
%elite_StationName%
%elite_StarSystem%
%elite_MarketID%
```

Nested objects and arrays are exposed as compact JSON strings. The complete
payload is always retained in `%elitePayloadJson%`, including future unknown
fields.

## Example action condition

For an action that should only run when docking:

1. Add the `Elite Telemetry Event` trigger.
2. Add an `If/Else` sub-action before the chatbot or crew response.
3. Compare `%eliteEventType%` with `Docked`.
4. Use `%elite_StationName%` and `%elite_StarSystem%` in later sub-actions.

If strict event order matters, place the receiving actions in a non-concurrent
Streamer.bot queue. The connector publishes events in bridge sequence order;
Streamer.bot controls how triggered actions are scheduled.

## Recovery and globals

The connector persists `%eliteBridgeLastSequence%` and reconnects with the
bridge replay cursor. If that cursor is older than the bridge's in-memory replay
window, the connector fires `Elite Telemetry Resync Required`, then applies the
fresh snapshot sent by the bridge.

Useful Streamer.bot globals:

| Global | Persistence | Contents |
| --- | --- | --- |
| `eliteBridgeWebSocketUrl` | Persisted | Connector URL; defaults to localhost |
| `eliteBridgeLastSequence` | Persisted | Last accepted sequence |
| `eliteBridgeConnected` | Temporary | Current connection state |
| `eliteBridgeLatestStateJson` | Temporary | Most recent full snapshot |
| `eliteBridgeLastEventType` | Temporary | Most recent event type |
| `eliteBridgeLastEventJson` | Temporary | Most recent payload JSON |

If the bridge uses a different local port, change the persisted
`eliteBridgeWebSocketUrl` global and recompile the connector. Do not expose the
current unauthenticated bridge API to the internet.
