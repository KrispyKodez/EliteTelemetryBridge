from __future__ import annotations

import asyncio
import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from aiohttp import ClientSession

from app import (
    ApiServer,
    ArchiveManager,
    BridgeEvent,
    CompanionStore,
    DeliveryHub,
    EventBus,
    JournalTailer,
    OutputManager,
    StateEngine,
    WebSocketClient,
    read_json_object,
)


def journal_event(
    payload: dict,
    raw_text: str,
    start: int,
    end: int,
    filename: str = "Journal.test.log",
) -> BridgeEvent:
    return BridgeEvent(
        source="journal",
        name=str(payload.get("event", "Unknown")),
        payload=payload,
        raw_text=raw_text,
        timestamp=str(payload.get("timestamp", "")),
        source_file=filename,
        start_position=start,
        end_position=end,
    )


class PipelineTests(unittest.TestCase):
    def test_production_pipeline_preserves_data_and_delivers_every_event(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            bus = EventBus()
            state = StateEngine()
            archive = ArchiveManager(
                root / "raw",
                "production",
                ["Music", "ReceiveText"],
                ["ShipLocker"],
            )
            delivery = DeliveryHub(100)
            companion_store = CompanionStore(root / "companions")
            delivered: list[dict] = []
            delivery.set_dispatcher(
                lambda sequence, text: delivered.append(json.loads(text))
            )

            bus.subscribe("archive", archive.handle)
            bus.subscribe("companions", companion_store.handle)
            bus.subscribe("state", state.handle)
            bus.subscribe("delivery", delivery.handle, critical=False)

            position = 0

            def publish(payload: dict) -> str:
                nonlocal position
                raw = json.dumps(payload, separators=(",", ":")) + "\r\n"
                start = position
                position += len(raw.encode("utf-8"))
                bus.publish(journal_event(payload, raw, start, position))
                return raw

            publish(
                {
                    "timestamp": "1",
                    "event": "Music",
                    "MusicTrack": "Test",
                }
            )
            first_locker = publish(
                {
                    "timestamp": "2",
                    "event": "ShipLocker",
                    "Items": [{"Name": "x", "Count": 1}],
                }
            )
            publish(
                {
                    "timestamp": "3",
                    "event": "ShipLocker",
                    "Items": [{"Name": "x", "Count": 1}],
                }
            )
            future = publish(
                {
                    "timestamp": "4",
                    "event": "UnknownFutureFrontierEvent",
                    "Anything": {"works": True},
                }
            )

            companion_raw = (
                "{\r\n  \"timestamp\": \"5\",\r\n"
                "  \"Future\": true\r\n}"
            )
            bus.publish(
                BridgeEvent(
                    source="companion",
                    name="FutureCompanion",
                    payload=json.loads(companion_raw),
                    raw_text=companion_raw,
                    source_file="FutureCompanion.json",
                )
            )

            raw_path = root / "raw" / "Journal.test.jsonl"
            with raw_path.open("r", encoding="utf-8", newline="") as source:
                self.assertEqual(source.read(), first_locker + future)

            companion_path = root / "companions" / "FutureCompanion.json"
            with companion_path.open(
                "r", encoding="utf-8", newline=""
            ) as source:
                self.assertEqual(source.read(), companion_raw)

            statistics = archive.snapshot_statistics()
            self.assertEqual(statistics["events_seen"], 4)
            self.assertEqual(statistics["events_archived"], 2)
            self.assertEqual(statistics["events_filtered"], 1)
            self.assertEqual(statistics["events_deduplicated"], 1)
            self.assertEqual(state.event_counts["Music"], 1)
            self.assertEqual(state.event_counts["ShipLocker"], 2)
            self.assertTrue(
                state.last_event_by_type["UnknownFutureFrontierEvent"][
                    "Anything"
                ]["works"]
            )
            self.assertEqual(len(delivered), 5)
            self.assertEqual(
                [message["event_type"] for message in delivered[:4]],
                [
                    "Music",
                    "ShipLocker",
                    "ShipLocker",
                    "UnknownFutureFrontierEvent",
                ],
            )

            output = OutputManager(
                output_dir=root,
                state_engine=state,
                archive_manager=archive,
                delivery_hub=delivery,
                event_bus=bus,
                flush_interval=60,
                write_split_state_files=False,
            )
            output.flush_if_dirty(force=True)
            latest = read_json_object(root / "state" / "latest.json")
            self.assertEqual(latest["bridge"]["delivery_sequence"], 5)
            self.assertTrue(
                latest["companions"]["FutureCompanion.json"]["Future"]
            )

    def test_archive_mode_keeps_filtered_and_repeated_events(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = ArchiveManager(
                root,
                "archive",
                ["Music"],
                ["ShipLocker"],
            )
            raw_values: list[str] = []
            position = 0

            for payload in (
                {"timestamp": "1", "event": "Music"},
                {"timestamp": "2", "event": "ShipLocker", "Items": [1]},
                {"timestamp": "3", "event": "ShipLocker", "Items": [1]},
            ):
                raw = json.dumps(payload) + "\r\n"
                start = position
                position += len(raw.encode("utf-8"))
                archive.handle(journal_event(payload, raw, start, position))
                raw_values.append(raw)

            with (root / "Journal.test.jsonl").open(
                "r", encoding="utf-8", newline=""
            ) as source:
                self.assertEqual(source.read(), "".join(raw_values))

            statistics = archive.snapshot_statistics()
            self.assertEqual(statistics["events_archived"], 3)
            self.assertEqual(statistics["events_filtered"], 0)
            self.assertEqual(statistics["events_deduplicated"], 0)


class RecoveryTests(unittest.TestCase):
    @staticmethod
    def build_runtime(
        journal_directory: Path,
        output_directory: Path,
        checkpoint: dict,
        state: StateEngine,
        archive: ArchiveManager,
        delivery: DeliveryHub,
    ) -> tuple[JournalTailer, OutputManager]:
        bus = EventBus()
        bus.subscribe("archive", archive.handle)
        bus.subscribe("state", state.handle)
        bus.subscribe("delivery", delivery.handle, critical=False)
        tailer = JournalTailer(
            journal_dir=journal_directory,
            output_dir=output_directory,
            event_bus=bus,
            state_engine=state,
            archive_manager=archive,
            checkpoint=checkpoint,
            show_raw_json=False,
            read_existing_events_on_first_run=True,
            capture_companion_files=False,
            delivery_hub=delivery,
        )
        output = OutputManager(
            output_dir=output_directory,
            state_engine=state,
            archive_manager=archive,
            delivery_hub=delivery,
            event_bus=bus,
            flush_interval=60,
            write_split_state_files=False,
        )
        return tailer, output

    def test_checkpoint_rebuilds_unflushed_state_without_redelivery(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            journals = root / "journals"
            output_directory = root / "output"
            journals.mkdir()
            journal = journals / "Journal.2026-01-01T000000.01.log"
            first = '{"timestamp":"1","event":"First"}\n'
            second = '{"timestamp":"2","event":"Second"}\n'
            journal.write_text(first, encoding="utf-8")

            state_one = StateEngine()
            archive_one = ArchiveManager(
                output_directory / "raw", "production", [], []
            )
            delivery_one = DeliveryHub(20)
            tailer_one, output_one = self.build_runtime(
                journals,
                output_directory,
                {},
                state_one,
                archive_one,
                delivery_one,
            )

            with redirect_stdout(io.StringIO()):
                tailer_one.process_new_lines()
            output_one.flush_if_dirty(force=True)

            with journal.open("a", encoding="utf-8") as destination:
                destination.write(second)

            with redirect_stdout(io.StringIO()):
                tailer_one.process_new_lines()
                tailer_one.close()

            checkpoint = read_json_object(
                output_directory / "cache" / "telemetry-checkpoint.json"
            )
            self.assertEqual(
                checkpoint["delivery_state"]["last_sequence"], 2
            )
            raw_path = (
                output_directory
                / "raw"
                / "Journal.2026-01-01T000000.01.jsonl"
            )
            raw_before = raw_path.read_bytes()

            state_two = StateEngine()
            restored, _ = state_two.restore(
                output_directory / "state" / "latest.json"
            )
            self.assertTrue(restored)
            self.assertEqual(state_two.restored_delivery_sequence, 1)
            delivery_two = DeliveryHub(
                20,
                checkpoint["delivery_state"],
                recovered_through_checkpoint=True,
            )
            archive_two = ArchiveManager(
                output_directory / "raw",
                "production",
                [],
                [],
                checkpoint["archive_state"],
                recovered_through_checkpoint=True,
            )
            tailer_two, output_two = self.build_runtime(
                journals,
                output_directory,
                checkpoint,
                state_two,
                archive_two,
                delivery_two,
            )

            with redirect_stdout(io.StringIO()):
                tailer_two.process_new_lines()
                tailer_two.close()

            self.assertEqual(state_two.event_counts["First"], 1)
            self.assertEqual(state_two.event_counts["Second"], 1)
            self.assertEqual(delivery_two.current_sequence(), 2)
            self.assertEqual(raw_path.read_bytes(), raw_before)
            output_two.flush_if_dirty(force=True)
            rebuilt = read_json_object(
                output_directory / "state" / "latest.json"
            )
            self.assertEqual(rebuilt["last_event"]["event"], "Second")
            self.assertEqual(rebuilt["bridge"]["delivery_sequence"], 2)


class ApiTests(unittest.TestCase):
    def test_http_websocket_replay_and_resync(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            bus = EventBus()
            state = StateEngine()
            archive = ArchiveManager(
                root / "raw",
                "production",
                ["Music", "ReceiveText"],
                ["ShipLocker"],
            )
            delivery = DeliveryHub(2)
            bus.subscribe("archive", archive.handle)
            bus.subscribe("state", state.handle)
            bus.subscribe("delivery", delivery.handle, critical=False)
            server = ApiServer(
                host="127.0.0.1",
                port=0,
                client_queue_events=4,
                event_bus=bus,
                state_engine=state,
                archive_manager=archive,
                delivery_hub=delivery,
            )
            server.start()

            try:
                asyncio.run(
                    self._exercise_api(server, bus, archive, delivery)
                )
            finally:
                server.close()

    async def _exercise_api(
        self,
        server: ApiServer,
        bus: EventBus,
        archive: ArchiveManager,
        delivery: DeliveryHub,
    ) -> None:
        position = 0

        def publish(payload: dict) -> None:
            nonlocal position
            raw = json.dumps(payload) + "\n"
            start = position
            position += len(raw.encode("utf-8"))
            bus.publish(journal_event(payload, raw, start, position))

        async with ClientSession() as session:
            async with session.get(server.http_url + "/health") as response:
                self.assertEqual(response.status, 200)
                health = await response.json()
                self.assertEqual(health["status"], "ok")

            websocket = await session.ws_connect(server.websocket_url)
            hello = await websocket.receive_json(timeout=3)
            snapshot = await websocket.receive_json(timeout=3)
            self.assertEqual(hello["message_type"], "hello")
            self.assertEqual(snapshot["message_type"], "snapshot")

            music = {
                "timestamp": "1",
                "event": "Music",
                "MusicTrack": "Test",
            }
            publish(music)
            live = await websocket.receive_json(timeout=3)
            self.assertEqual(live["event"], music)
            music_sequence = live["sequence"]
            self.assertEqual(
                archive.snapshot_statistics()["events_filtered"], 1
            )
            await websocket.close()

            publish(
                {
                    "timestamp": "2",
                    "event": "Docked",
                    "StationName": "Replay Station",
                }
            )
            replay = await session.ws_connect(
                server.websocket_url + f"?after={music_sequence}"
            )
            replay_hello = await replay.receive_json(timeout=3)
            replay_event = await replay.receive_json(timeout=3)
            self.assertEqual(replay_hello["message_type"], "hello")
            self.assertEqual(replay_event["event_type"], "Docked")
            await replay.close()

            for index in range(3):
                publish(
                    {
                        "timestamp": str(index + 3),
                        "event": f"FutureEvent{index}",
                    }
                )

            resync = await session.ws_connect(
                server.websocket_url + "?after=0"
            )
            self.assertEqual(
                (await resync.receive_json(timeout=3))["message_type"],
                "hello",
            )
            self.assertEqual(
                (await resync.receive_json(timeout=3))["message_type"],
                "resync_required",
            )
            new_snapshot = await resync.receive_json(timeout=3)
            self.assertEqual(new_snapshot["message_type"], "snapshot")
            self.assertEqual(
                new_snapshot["state"]["last_event"]["event"],
                "FutureEvent2",
            )
            await resync.close()

            async with session.get(server.http_url + "/state") as response:
                current = await response.json()
                self.assertEqual(
                    current["bridge"]["delivery_sequence"],
                    delivery.current_sequence(),
                )

            async with session.get(
                server.http_url + "/events?after=nope"
            ) as response:
                self.assertEqual(response.status, 400)

    def test_slow_client_is_dropped_without_blocking_delivery(self) -> None:
        class SlowSocket:
            closed = False
            close_calls = 0

            async def close(self, **kwargs: object) -> None:
                self.closed = True
                self.close_calls += 1

        async def exercise() -> None:
            bus = EventBus()
            state = StateEngine()
            archive = ArchiveManager(Path("unused"), "production", [], [])
            delivery = DeliveryHub(2)
            server = ApiServer(
                "127.0.0.1",
                0,
                1,
                bus,
                state,
                archive,
                delivery,
            )
            socket = SlowSocket()
            client = WebSocketClient(
                1,
                socket,
                asyncio.Queue(maxsize=1),
                1,
            )
            server.clients[1] = client
            server._broadcast_now(1, "{}")
            server._broadcast_now(2, "{}")
            await asyncio.sleep(0)
            self.assertEqual(server.dropped_slow_clients, 1)
            self.assertEqual(socket.close_calls, 1)
            self.assertNotIn(1, server.clients)

        asyncio.run(exercise())


if __name__ == "__main__":
    unittest.main()
