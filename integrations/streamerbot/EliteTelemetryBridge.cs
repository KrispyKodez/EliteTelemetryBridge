using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// Paste this complete file into one Streamer.bot "Execute C# Code" sub-action.
// Enable "Precompile on Application Start" so Init() owns the connection.
public class CPHInline
{
#if ELITE_TELEMETRYBRIDGE_COMPILE_TEST
    // Allows the repository's external compile harness to supply a CPH stub.
    public dynamic CPH { get; set; }
#endif

    private const string DefaultWebSocketUrl = "ws://127.0.0.1:8765/events";
    private const string EventTrigger = "eliteTelemetryBridge.event";
    private const string SnapshotTrigger = "eliteTelemetryBridge.snapshot";
    private const string ConnectionTrigger = "eliteTelemetryBridge.connection";
    private const string ResyncTrigger = "eliteTelemetryBridge.resync";
    private const int MaximumMessageBytes = 16 * 1024 * 1024;

    private readonly object _socketLock = new object();
    private CancellationTokenSource _cancellation;
    private Task _worker;
    private ClientWebSocket _activeSocket;
    private string _baseWebSocketUrl = DefaultWebSocketUrl;
    private long _lastSequence;
    private bool _connected;
    private bool _reportedUnavailable;

    public void Init()
    {
        CPH.RegisterCustomTrigger(
            "Elite Telemetry Event",
            EventTrigger,
            new[] { "EliteTelemetryBridge" }
        );
        CPH.RegisterCustomTrigger(
            "Elite Telemetry Snapshot",
            SnapshotTrigger,
            new[] { "EliteTelemetryBridge" }
        );
        CPH.RegisterCustomTrigger(
            "Elite Telemetry Connection Changed",
            ConnectionTrigger,
            new[] { "EliteTelemetryBridge" }
        );
        CPH.RegisterCustomTrigger(
            "Elite Telemetry Resync Required",
            ResyncTrigger,
            new[] { "EliteTelemetryBridge" }
        );

        string configuredUrl = CPH.GetGlobalVar<string>(
            "eliteBridgeWebSocketUrl",
            true
        );
        if (!string.IsNullOrWhiteSpace(configuredUrl))
        {
            _baseWebSocketUrl = configuredUrl.Trim();
        }
        else
        {
            CPH.SetGlobalVar(
                "eliteBridgeWebSocketUrl",
                DefaultWebSocketUrl,
                true
            );
        }

        long? savedSequence = CPH.GetGlobalVar<long?>(
            "eliteBridgeLastSequence",
            true
        );
        _lastSequence = Math.Max(0, savedSequence ?? 0);
        CPH.SetGlobalVar("eliteBridgeConnected", false, false);

        _cancellation = new CancellationTokenSource();
        _worker = Task.Run(() => RunConnectionLoopAsync(_cancellation.Token));
    }

    public void Dispose()
    {
        if (_cancellation == null)
        {
            return;
        }

        _cancellation.Cancel();

        lock (_socketLock)
        {
            if (_activeSocket != null)
            {
                _activeSocket.Abort();
            }
        }

        try
        {
            if (_worker != null)
            {
                _worker.Wait(3000);
            }
        }
        catch (AggregateException)
        {
            // Cancellation and socket aborts are expected during shutdown.
        }

        CPH.SetGlobalVar("eliteBridgeConnected", false, false);
        _cancellation.Dispose();
        _cancellation = null;
    }

    // The module starts from Init(). Manual execution is intentionally a no-op.
    public bool Execute()
    {
        return true;
    }

    private async Task RunConnectionLoopAsync(CancellationToken cancellation)
    {
        int retrySeconds = 1;

        while (!cancellation.IsCancellationRequested)
        {
            ClientWebSocket socket = null;
            string disconnectReason = "connection closed";
            bool opened = false;

            try
            {
                socket = new ClientWebSocket();
                socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);

                lock (_socketLock)
                {
                    _activeSocket = socket;
                }

                string connectionUrl = BuildConnectionUrl(_lastSequence);
                await socket.ConnectAsync(
                    new Uri(connectionUrl),
                    cancellation
                ).ConfigureAwait(false);

                opened = true;
                _connected = true;
                _reportedUnavailable = false;
                retrySeconds = 1;
                PublishConnectionChanged(true, "connected", connectionUrl);

                await ReceiveMessagesAsync(socket, cancellation)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                disconnectReason = "connector stopped";
            }
            catch (Exception exception)
            {
                disconnectReason = exception.Message;
                if (!_reportedUnavailable)
                {
                    CPH.LogWarn(
                        "EliteTelemetryBridge is unavailable; reconnecting: "
                        + exception.Message
                    );
                    _reportedUnavailable = true;
                }
                else
                {
                    CPH.LogDebug(
                        "EliteTelemetryBridge reconnect failed: "
                        + exception.Message
                    );
                }
            }
            finally
            {
                lock (_socketLock)
                {
                    if (ReferenceEquals(_activeSocket, socket))
                    {
                        _activeSocket = null;
                    }
                }

                if (opened || _connected)
                {
                    _connected = false;
                    PublishConnectionChanged(
                        false,
                        disconnectReason,
                        BuildConnectionUrl(_lastSequence)
                    );
                }

                if (socket != null)
                {
                    socket.Dispose();
                }
            }

            if (cancellation.IsCancellationRequested)
            {
                break;
            }

            try
            {
                await Task.Delay(
                    TimeSpan.FromSeconds(retrySeconds),
                    cancellation
                ).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            retrySeconds = Math.Min(30, retrySeconds * 2);
        }
    }

    private async Task ReceiveMessagesAsync(
        ClientWebSocket socket,
        CancellationToken cancellation
    )
    {
        byte[] buffer = new byte[16 * 1024];

        while (
            socket.State == WebSocketState.Open
            && !cancellation.IsCancellationRequested
        )
        {
            using (MemoryStream messageBuffer = new MemoryStream())
            {
                WebSocketReceiveResult result;

                do
                {
                    result = await socket.ReceiveAsync(
                        new ArraySegment<byte>(buffer),
                        cancellation
                    ).ConfigureAwait(false);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        return;
                    }

                    if (result.Count > 0)
                    {
                        messageBuffer.Write(buffer, 0, result.Count);
                    }

                    if (messageBuffer.Length > MaximumMessageBytes)
                    {
                        throw new InvalidDataException(
                            "Bridge WebSocket message exceeded 16 MiB."
                        );
                    }
                }
                while (!result.EndOfMessage);

                if (result.MessageType != WebSocketMessageType.Text)
                {
                    continue;
                }

                string messageJson = Encoding.UTF8.GetString(
                    messageBuffer.ToArray()
                );
                HandleMessage(messageJson);
            }
        }
    }

    private void HandleMessage(string messageJson)
    {
        JObject message;

        try
        {
            JsonSerializerSettings settings = new JsonSerializerSettings
            {
                DateParseHandling = DateParseHandling.None
            };
            message = JsonConvert.DeserializeObject<JObject>(
                messageJson,
                settings
            );
        }
        catch (Exception exception)
        {
            CPH.LogWarn(
                "EliteTelemetryBridge sent invalid JSON: " + exception.Message
            );
            return;
        }

        if (message == null)
        {
            return;
        }

        string messageType = ValueAsString(message["message_type"]);

        if (messageType == "hello")
        {
            CPH.SetGlobalVar(
                "eliteBridgeVersion",
                ValueAsString(message["version"]),
                false
            );
            CPH.SetGlobalVar(
                "eliteBridgeCurrentSequence",
                ValueAsLong(message["current_sequence"]),
                false
            );
            return;
        }

        if (messageType == "snapshot")
        {
            HandleSnapshot(message, messageJson);
            return;
        }

        if (messageType == "resync_required")
        {
            HandleResync(message, messageJson);
            return;
        }

        if (messageType == "event")
        {
            HandleEvent(message, messageJson);
        }
    }

    private void HandleSnapshot(JObject message, string messageJson)
    {
        long sequence = ValueAsLong(message["sequence"]);
        JToken state = message["state"];
        string stateJson = state == null
            ? "{}"
            : state.ToString(Formatting.None);

        Dictionary<string, object> triggerArguments =
            new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                { "eliteMessageType", "snapshot" },
                { "eliteSequence", sequence },
                { "eliteStateJson", stateJson },
                { "eliteMessageJson", messageJson }
            };

        CPH.SetGlobalVar("eliteBridgeLatestStateJson", stateJson, false);
        CPH.TriggerCodeEvent(SnapshotTrigger, triggerArguments);
        SaveSequence(sequence);
    }

    private void HandleResync(JObject message, string messageJson)
    {
        Dictionary<string, object> triggerArguments =
            new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                { "eliteMessageType", "resync_required" },
                {
                    "eliteRequestedAfter",
                    ValueAsLong(message["requested_after"])
                },
                {
                    "eliteCurrentSequence",
                    ValueAsLong(message["current_sequence"])
                },
                {
                    "eliteOldestAvailableSequence",
                    ValueAsLong(message["oldest_available_sequence"])
                },
                { "eliteMessageJson", messageJson }
            };

        CPH.LogWarn(
            "EliteTelemetryBridge replay cursor expired; applying a fresh "
            + "state snapshot."
        );
        CPH.TriggerCodeEvent(ResyncTrigger, triggerArguments);
    }

    private void HandleEvent(JObject message, string messageJson)
    {
        long sequence = ValueAsLong(message["sequence"]);

        // A replay can overlap a previously acknowledged message. Do not fire
        // Streamer.bot actions twice for the same bridge sequence.
        if (sequence <= _lastSequence)
        {
            return;
        }

        JToken payload = message["event"];
        string payloadJson = payload == null
            ? "{}"
            : payload.ToString(Formatting.None);

        Dictionary<string, object> triggerArguments =
            new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                { "eliteMessageType", "event" },
                { "eliteSequence", sequence },
                { "eliteEventId", ValueAsString(message["event_id"]) },
                { "eliteEventType", ValueAsString(message["event_type"]) },
                { "eliteSource", ValueAsString(message["source"]) },
                { "eliteSourceFile", ValueAsString(message["source_file"]) },
                { "eliteTimestamp", ValueAsString(message["timestamp"]) },
                {
                    "eliteBridgeTimestamp",
                    ValueAsString(message["bridge_timestamp"])
                },
                { "elitePayloadJson", payloadJson },
                { "eliteMessageJson", messageJson }
            };

        JObject payloadObject = payload as JObject;
        if (payloadObject != null)
        {
            AddPayloadArguments(triggerArguments, payloadObject);
        }

        CPH.SetGlobalVar(
            "eliteBridgeLastEventType",
            ValueAsString(message["event_type"]),
            false
        );
        CPH.SetGlobalVar(
            "eliteBridgeLastEventJson",
            payloadJson,
            false
        );

        CPH.TriggerCodeEvent(EventTrigger, triggerArguments);
        SaveSequence(sequence);
    }

    private void AddPayloadArguments(
        Dictionary<string, object> triggerArguments,
        JObject payload
    )
    {
        foreach (JProperty property in payload.Properties())
        {
            string baseName = "elite_" + SanitizeVariablePart(property.Name);
            string variableName = baseName;
            int suffix = 2;

            while (triggerArguments.ContainsKey(variableName))
            {
                variableName = baseName + "_" + suffix;
                suffix += 1;
            }

            triggerArguments[variableName] = ArgumentValue(property.Value);
        }
    }

    private static object ArgumentValue(JToken value)
    {
        if (value == null || value.Type == JTokenType.Null)
        {
            return null;
        }

        JValue scalar = value as JValue;
        if (scalar != null)
        {
            return scalar.Value;
        }

        // Nested objects and arrays remain lossless JSON strings that can be
        // parsed by any downstream Streamer.bot action when needed.
        return value.ToString(Formatting.None);
    }

    private static string SanitizeVariablePart(string value)
    {
        StringBuilder result = new StringBuilder(value.Length);

        foreach (char character in value)
        {
            bool safe =
                (character >= 'a' && character <= 'z')
                || (character >= 'A' && character <= 'Z')
                || (character >= '0' && character <= '9')
                || character == '_';
            result.Append(safe ? character : '_');
        }

        return result.Length == 0 ? "field" : result.ToString();
    }

    private void SaveSequence(long sequence)
    {
        if (sequence < 0)
        {
            return;
        }

        _lastSequence = sequence;
        CPH.SetGlobalVar("eliteBridgeLastSequence", sequence, true);
        CPH.SetGlobalVar("eliteBridgeCurrentSequence", sequence, false);
    }

    private void PublishConnectionChanged(
        bool connected,
        string reason,
        string connectionUrl
    )
    {
        CPH.SetGlobalVar("eliteBridgeConnected", connected, false);

        Dictionary<string, object> triggerArguments =
            new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                { "eliteConnected", connected },
                { "eliteConnectionReason", reason ?? string.Empty },
                { "eliteWebSocketUrl", connectionUrl },
                { "eliteSequence", _lastSequence }
            };

        CPH.TriggerCodeEvent(ConnectionTrigger, triggerArguments);
    }

    private string BuildConnectionUrl(long afterSequence)
    {
        if (afterSequence <= 0)
        {
            return _baseWebSocketUrl;
        }

        string separator = _baseWebSocketUrl.Contains("?") ? "&" : "?";
        return _baseWebSocketUrl
            + separator
            + "after="
            + afterSequence.ToString(
                System.Globalization.CultureInfo.InvariantCulture
            );
    }

    private static string ValueAsString(JToken value)
    {
        if (value == null || value.Type == JTokenType.Null)
        {
            return string.Empty;
        }

        return value.Type == JTokenType.String
            ? (string)value
            : value.ToString(Formatting.None);
    }

    private static long ValueAsLong(JToken value)
    {
        if (value == null || value.Type == JTokenType.Null)
        {
            return 0;
        }

        long parsed;
        return long.TryParse(
            value.ToString(),
            System.Globalization.NumberStyles.Integer,
            System.Globalization.CultureInfo.InvariantCulture,
            out parsed
        )
            ? parsed
            : 0;
    }
}
