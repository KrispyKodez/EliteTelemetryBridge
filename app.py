from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TextIO

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

try:
    from aiohttp import WSMsgType, web
except ImportError:
    WSMsgType = None
    web = None


APP_NAME = "Krispiez Elite Telemetry Bridge"
VERSION = "0.5.1"
SETTINGS_FILE = "telemetry-settings.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def app_directory() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent

    return Path(__file__).resolve().parent


def safe_write_text(path: Path, text: str, retries: int = 8) -> None:
    """Atomically replace a text file, with Windows-friendly retries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    last_error: OSError | None = None

    for attempt in range(retries):
        try:
            with temporary.open("w", encoding="utf-8", newline="") as output:
                output.write(text)
                output.flush()
            os.replace(temporary, path)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.03 * (attempt + 1))

    try:
        temporary.unlink(missing_ok=True)
    except OSError:
        pass

    if last_error is not None:
        raise last_error


def safe_append_text(path: Path, text: str, retries: int = 8) -> None:
    """Append raw journal text without changing its whitespace or newlines."""
    path.parent.mkdir(parents=True, exist_ok=True)
    last_error: OSError | None = None

    for attempt in range(retries):
        try:
            with path.open("a", encoding="utf-8", newline="") as output:
                output.write(text)
                output.flush()
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.03 * (attempt + 1))

    if last_error is not None:
        raise last_error


def json_text(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False)


def safe_write_json(path: Path, value: Any) -> None:
    safe_write_text(path, json_text(value))


def read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    return value if isinstance(value, dict) else {}


def load_settings(app_dir: Path) -> dict[str, Any]:
    path = app_dir / SETTINGS_FILE
    defaults: dict[str, Any] = {
        "journal_directory": "",
        "output_directory": "output",
        "archive_mode": "production",
        "production_archive_skip_events": ["Music", "ReceiveText"],
        "production_archive_deduplicate_events": ["ShipLocker"],
        "show_raw_json": True,
        "read_existing_events_on_first_run": True,
        "poll_interval_seconds": 2.0,
        "state_flush_interval_seconds": 0.25,
        "capture_companion_files": True,
        "write_split_state_files": False,
        "api_enabled": True,
        "api_host": "127.0.0.1",
        "api_port": 8765,
        "api_replay_buffer_events": 2000,
        "api_client_queue_events": 256,
    }

    if not path.exists():
        safe_write_json(path, defaults)
        return defaults

    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to read {path}: {exc}") from exc

    if not isinstance(loaded, dict):
        raise RuntimeError(f"{path} must contain a JSON object.")

    settings = defaults | loaded
    archive_mode = str(settings.get("archive_mode", "production")).lower()

    if archive_mode not in {"production", "archive"}:
        raise RuntimeError(
            "archive_mode must be either 'production' or 'archive'."
        )

    settings["archive_mode"] = archive_mode
    return settings


def saved_games_directory() -> Path:
    user_profile = os.environ.get("USERPROFILE")

    if not user_profile:
        raise RuntimeError("Windows USERPROFILE was not found.")

    candidates = [
        Path(user_profile) / "Saved Games",
        Path(user_profile) / "OneDrive" / "Saved Games",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


def resolve_journal_directory(settings: dict[str, Any]) -> Path:
    configured = str(settings.get("journal_directory", "")).strip()

    if configured:
        path = Path(os.path.expandvars(configured)).expanduser()
    else:
        path = (
            saved_games_directory()
            / "Frontier Developments"
            / "Elite Dangerous"
        )

    if not path.exists():
        raise RuntimeError(
            "Elite Dangerous journal folder was not found.\n"
            f"Expected: {path}\n"
            f"Set journal_directory in {SETTINGS_FILE}."
        )

    return path.resolve()


def resolve_output_directory(
    app_dir: Path,
    settings: dict[str, Any],
) -> Path:
    configured = str(settings.get("output_directory", "output")).strip()
    expanded = Path(os.path.expandvars(configured)).expanduser()
    path = expanded if expanded.is_absolute() else app_dir / expanded
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def journals_in_order(journal_dir: Path) -> list[Path]:
    try:
        journals = list(journal_dir.glob("Journal.*.log"))
    except OSError:
        return []

    return sorted(journals, key=lambda path: path.name.casefold())


@dataclass(frozen=True, slots=True)
class BridgeEvent:
    """A source-neutral envelope passed through the event bus."""

    source: str
    name: str
    payload: Any
    raw_text: str
    timestamp: str = ""
    source_file: str = ""
    start_position: int = 0
    end_position: int = 0
    recovery_replay: bool = False
    parse_error: str = ""


class EventBus:
    """Synchronous core pipeline; future plugins can subscribe here."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._subscribers: list[
            tuple[str, Callable[[BridgeEvent], None], bool]
        ] = []

    def subscribe(
        self,
        name: str,
        callback: Callable[[BridgeEvent], None],
        *,
        critical: bool = True,
    ) -> None:
        with self.lock:
            self._subscribers.append((name, callback, critical))

    def publish(self, event: BridgeEvent) -> None:
        with self.lock:
            for name, callback, critical in tuple(self._subscribers):
                try:
                    callback(event)
                except Exception as exc:
                    if critical:
                        raise
                    print(f"Non-critical subscriber {name!r} failed: {exc}")


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    sequence: int
    text: str


class DeliveryHub:
    """Assigns ordered cursors and retains a bounded reconnect buffer."""

    def __init__(
        self,
        replay_buffer_events: int,
        recovered_state: dict[str, Any] | None = None,
        recovered_through_checkpoint: bool = False,
    ) -> None:
        recovered_state = recovered_state or {}
        self.lock = threading.RLock()

        try:
            recovered_sequence = int(
                recovered_state.get("last_sequence", 0) or 0
            )
        except (TypeError, ValueError):
            recovered_sequence = 0

        self.last_sequence = max(0, recovered_sequence)
        self.recovered_through_checkpoint = recovered_through_checkpoint
        self.replay_buffer: deque[DeliveryRecord] = deque(
            maxlen=max(1, replay_buffer_events)
        )
        self.dispatcher: Callable[[int, str], None] | None = None
        self.events_published = 0

    def handle(self, event: BridgeEvent) -> None:
        if event.recovery_replay and self.recovered_through_checkpoint:
            return

        with self.lock:
            self.last_sequence += 1
            sequence = self.last_sequence
            message: dict[str, Any] = {
                "message_type": "event",
                "sequence": sequence,
                "event_id": (
                    f"{event.source_file}:{event.end_position}"
                    if event.source == "journal"
                    else f"{event.source_file}:{sequence}"
                ),
                "bridge_timestamp": utc_now_iso(),
                "source": event.source,
                "source_file": event.source_file,
                "event_type": event.name,
                "timestamp": event.timestamp,
                "event": event.payload,
            }

            if event.source == "journal":
                message["source_position"] = {
                    "start": event.start_position,
                    "end": event.end_position,
                }

            if event.parse_error:
                message["parse_error"] = event.parse_error
                message["raw_text"] = event.raw_text

            text = json.dumps(
                message,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            self.replay_buffer.append(DeliveryRecord(sequence, text))
            self.events_published += 1
            dispatcher = self.dispatcher

        if dispatcher is not None:
            try:
                dispatcher(sequence, text)
            except Exception as exc:
                print(f"WebSocket dispatch warning: {exc}")

    def set_dispatcher(
        self,
        dispatcher: Callable[[int, str], None] | None,
    ) -> None:
        with self.lock:
            self.dispatcher = dispatcher

    def current_sequence(self) -> int:
        with self.lock:
            return self.last_sequence

    def replay_plan(
        self,
        after_sequence: int,
    ) -> tuple[bool, list[str], int, int]:
        """Return resync flag, replay messages, current, and oldest cursor."""
        with self.lock:
            current = self.last_sequence
            oldest = (
                self.replay_buffer[0].sequence
                if self.replay_buffer
                else current + 1
            )

            resync_required = (
                after_sequence > current
                or (
                    after_sequence < current
                    and (
                        not self.replay_buffer
                        or after_sequence < oldest - 1
                    )
                )
            )

            if resync_required:
                return True, [], current, oldest

            messages = [
                record.text
                for record in self.replay_buffer
                if record.sequence > after_sequence
            ]
            return False, messages, current, oldest

    def checkpoint_state(self) -> dict[str, Any]:
        with self.lock:
            return {"last_sequence": self.last_sequence}

    def statistics(self) -> dict[str, int]:
        with self.lock:
            return {
                "current_sequence": self.last_sequence,
                "oldest_available_sequence": (
                    self.replay_buffer[0].sequence
                    if self.replay_buffer
                    else self.last_sequence + 1
                ),
                "buffered_events": len(self.replay_buffer),
                "buffer_capacity": self.replay_buffer.maxlen or 0,
                "events_published": self.events_published,
            }


class ArchiveManager:
    """Raw journal subscriber with lossless and production policies."""

    def __init__(
        self,
        raw_dir: Path,
        mode: str,
        skip_events: list[str],
        deduplicate_events: list[str],
        recovered_state: dict[str, Any] | None = None,
        recovered_through_checkpoint: bool = False,
    ) -> None:
        self.raw_dir = raw_dir
        self.mode = mode
        self.skip_events = {str(name).casefold() for name in skip_events}
        self.deduplicate_events = {
            str(name).casefold() for name in deduplicate_events
        }
        self.lock = threading.RLock()
        self.recovered_through_checkpoint = recovered_through_checkpoint
        self.last_deduplication_fingerprints: dict[str, str] = {}
        self.statistics: dict[str, Any] = {
            "mode": mode,
            "events_seen": 0,
            "events_archived": 0,
            "events_filtered": 0,
            "events_deduplicated": 0,
            "malformed_lines": 0,
            "bytes_archived": 0,
            "last_archived_utc": "",
            "by_event": {},
        }

        if recovered_state:
            self._restore(recovered_state)

    def _restore(self, recovered: dict[str, Any]) -> None:
        statistics = recovered.get("statistics", recovered)

        if isinstance(statistics, dict):
            for key in self.statistics:
                if key in statistics:
                    self.statistics[key] = statistics[key]

        fingerprints = recovered.get("deduplication_fingerprints", {})

        if isinstance(fingerprints, dict):
            self.last_deduplication_fingerprints = {
                str(key): str(value) for key, value in fingerprints.items()
            }

        self.statistics["mode"] = self.mode

    def handle(self, event: BridgeEvent) -> None:
        if event.source != "journal":
            return

        with self.lock:
            if event.recovery_replay and self.recovered_through_checkpoint:
                return

            event_stats = self._event_statistics(event.name)

            if (
                self.mode == "production"
                and event.name.casefold() in self.skip_events
            ):
                self._record_seen(event, event_stats)
                self.statistics["events_filtered"] += 1
                event_stats["filtered"] += 1
                return

            fingerprint = self._deduplication_candidate(event)

            if (
                fingerprint is not None
                and self.last_deduplication_fingerprints.get(
                    event.name.casefold()
                )
                == fingerprint
            ):
                self._record_seen(event, event_stats)
                self.statistics["events_deduplicated"] += 1
                event_stats["deduplicated"] += 1
                return

            if not event.recovery_replay:
                raw_path = self.raw_dir / f"{Path(event.source_file).stem}.jsonl"
                safe_append_text(raw_path, event.raw_text)

            # Commit counters and deduplication state only after a durable
            # append succeeds. A locked archive file can then be retried safely.
            self._record_seen(event, event_stats)

            if fingerprint is not None:
                self.last_deduplication_fingerprints[
                    event.name.casefold()
                ] = fingerprint

            encoded_size = len(event.raw_text.encode("utf-8"))
            self.statistics["events_archived"] += 1
            self.statistics["bytes_archived"] += encoded_size
            self.statistics["last_archived_utc"] = utc_now_iso()
            event_stats["archived"] += 1

    def _record_seen(
        self,
        event: BridgeEvent,
        event_stats: dict[str, int],
    ) -> None:
        self.statistics["events_seen"] += 1
        event_stats["seen"] += 1

        if event.parse_error:
            self.statistics["malformed_lines"] += 1

    def _deduplication_candidate(self, event: BridgeEvent) -> str | None:
        event_key = event.name.casefold()

        if (
            self.mode != "production"
            or event_key not in self.deduplicate_events
        ):
            return None

        return self._deduplication_fingerprint(event.payload)

    @staticmethod
    def _deduplication_fingerprint(payload: Any) -> str:
        if isinstance(payload, dict):
            payload = {
                key: value
                for key, value in payload.items()
                if key != "timestamp"
            }

        return json.dumps(
            payload,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def _event_statistics(self, event_name: str) -> dict[str, int]:
        by_event = self.statistics.setdefault("by_event", {})
        current = by_event.get(event_name)

        if not isinstance(current, dict):
            current = {
                "seen": 0,
                "archived": 0,
                "filtered": 0,
                "deduplicated": 0,
            }
            by_event[event_name] = current

        return current

    def snapshot_statistics(self) -> dict[str, Any]:
        with self.lock:
            result = {
                key: value
                for key, value in self.statistics.items()
                if key != "by_event"
            }
            result["by_event"] = {
                name: dict(values)
                for name, values in self.statistics.get("by_event", {}).items()
            }
            return result

    def checkpoint_state(self) -> dict[str, Any]:
        with self.lock:
            return {
                "statistics": self.snapshot_statistics(),
                "deduplication_fingerprints": dict(
                    self.last_deduplication_fingerprints
                ),
            }


class CompanionStore:
    """Mirrors every current and future JSON companion file exactly."""

    def __init__(self, companion_dir: Path) -> None:
        self.companion_dir = companion_dir

    def handle(self, event: BridgeEvent) -> None:
        if event.source != "companion":
            return

        safe_write_text(self.companion_dir / event.source_file, event.raw_text)


class StateEngine:
    """In-memory live state, updated by generic bus events."""

    STATE_SECTIONS = (
        "commander",
        "ship",
        "location",
        "status",
        "cargo",
        "fuel",
        "navigation",
        "missions",
        "materials",
        "ranks",
        "progress",
        "reputation",
        "engineers",
        "statistics",
        "companions",
        "last_event",
        "last_event_by_type",
    )

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.dirty = True
        self.generation = 0
        self.started_utc = utc_now_iso()
        self.updated_utc = self.started_utc
        self.recovery_journal_file = ""
        self.recovery_journal_position = 0
        self.restored_delivery_sequence = 0

        self.last_event: dict[str, Any] = {}
        self.last_event_by_type: dict[str, dict[str, Any]] = {}
        self.event_counts: defaultdict[str, int] = defaultdict(int)
        self.commander: dict[str, Any] = {}
        self.ship: dict[str, Any] = {}
        self.location: dict[str, Any] = {}
        self.status: dict[str, Any] = {}
        self.cargo: dict[str, Any] = {}
        self.fuel: dict[str, Any] = {}
        self.navigation: dict[str, Any] = {}
        self.missions: dict[str, dict[str, Any]] = {}
        self.materials: dict[str, Any] = {}
        self.ranks: dict[str, Any] = {}
        self.progress: dict[str, Any] = {}
        self.reputation: dict[str, Any] = {}
        self.engineers: dict[str, Any] = {}
        self.statistics: dict[str, Any] = {}
        self.companions: dict[str, Any] = {}

    def restore(self, path: Path) -> tuple[bool, dict[str, Any]]:
        snapshot = read_json_object(path)
        bridge = snapshot.get("bridge", {})
        recovery = bridge.get("recovery", {}) if isinstance(bridge, dict) else {}

        if not (
            isinstance(recovery, dict)
            and isinstance(recovery.get("journal_file"), str)
            and recovery.get("journal_file")
            and isinstance(recovery.get("journal_position"), int)
        ):
            return False, {}

        with self.lock:
            for section in self.STATE_SECTIONS:
                value = snapshot.get(section)
                if isinstance(value, dict):
                    setattr(self, section, value)

            counts = snapshot.get("event_counts", {})
            self.event_counts = defaultdict(
                int,
                {
                    str(name): int(count)
                    for name, count in counts.items()
                    if isinstance(count, int)
                }
                if isinstance(counts, dict)
                else {},
            )
            self.updated_utc = str(
                bridge.get("updated_utc", self.updated_utc)
            )
            self.recovery_journal_file = recovery["journal_file"]
            self.recovery_journal_position = recovery["journal_position"]
            try:
                restored_sequence = int(
                    bridge.get("delivery_sequence", 0) or 0
                )
            except (TypeError, ValueError):
                restored_sequence = 0

            self.restored_delivery_sequence = max(0, restored_sequence)
            self.dirty = False

        archive_statistics = snapshot.get("archive_statistics", {})
        return True, (
            archive_statistics
            if isinstance(archive_statistics, dict)
            else {}
        )

    def handle(self, bridge_event: BridgeEvent) -> None:
        if bridge_event.source == "journal":
            self._process_journal_event(bridge_event)
        elif bridge_event.source == "companion":
            self._process_companion_event(bridge_event)

    def _process_journal_event(self, bridge_event: BridgeEvent) -> None:
        payload = bridge_event.payload

        if isinstance(payload, dict):
            event = payload
        else:
            event = {
                "event": bridge_event.name,
                "value": payload,
            }

            if bridge_event.parse_error:
                event["raw"] = bridge_event.raw_text.rstrip("\r\n")
                event["parse_error"] = bridge_event.parse_error

        with self.lock:
            event_name = bridge_event.name
            self.last_event = event
            self.last_event_by_type[event_name] = event
            self.event_counts[event_name] += 1
            self.recovery_journal_file = bridge_event.source_file
            self.recovery_journal_position = bridge_event.end_position
            self.updated_utc = utc_now_iso()

            self._update_common_fields(event)
            self._dispatch(event_name, event)
            self._changed()

    def _process_companion_event(self, bridge_event: BridgeEvent) -> None:
        filename = bridge_event.source_file
        value = bridge_event.payload

        with self.lock:
            self.companions[filename] = value
            lowered = filename.casefold()

            if lowered == "cargo.json" and isinstance(value, dict):
                self.cargo["used"] = value.get(
                    "Count", self.cargo.get("used", 0)
                )
                self.cargo["inventory"] = value.get(
                    "Inventory", self.cargo.get("inventory", [])
                )
            elif lowered == "navroute.json" and isinstance(value, dict):
                route = value.get("Route", [])
                self.navigation["route"] = route
                self.navigation["route_jump_count"] = (
                    len(route) if isinstance(route, list) else 0
                )
            elif lowered == "status.json" and isinstance(value, dict):
                self.status["raw"] = value
                fuel = value.get("Fuel")

                if isinstance(fuel, dict):
                    self.fuel["main"] = fuel.get(
                        "FuelMain", self.fuel.get("main", 0)
                    )
                    self.fuel["reserve"] = fuel.get(
                        "FuelReservoir", self.fuel.get("reserve", 0)
                    )

            self.updated_utc = utc_now_iso()
            self._changed()

    def recovery_position(self) -> tuple[str, int]:
        with self.lock:
            return (
                self.recovery_journal_file,
                self.recovery_journal_position,
            )

    def prepare_snapshot(
        self,
        archive_statistics: dict[str, Any],
        delivery_sequence: int = 0,
        *,
        force: bool = False,
    ) -> tuple[int, str] | None:
        """Serialize once under the state lock; no deepcopy is required."""
        with self.lock:
            if not self.dirty and not force:
                return None

            snapshot = self._snapshot_value(
                archive_statistics,
                delivery_sequence,
            )
            return self.generation, json_text(snapshot)

    def serialize_snapshot(
        self,
        archive_statistics: dict[str, Any],
        delivery_sequence: int,
    ) -> str:
        with self.lock:
            return json_text(
                self._snapshot_value(
                    archive_statistics,
                    delivery_sequence,
                )
            )

    def serialize_section(self, section: str) -> str:
        with self.lock:
            value = getattr(self, section)
            return json_text(value)

    def health_metadata(self) -> dict[str, Any]:
        with self.lock:
            return {
                "started_utc": self.started_utc,
                "updated_utc": self.updated_utc,
                "journal_file": self.recovery_journal_file,
                "journal_position": self.recovery_journal_position,
            }

    def _snapshot_value(
        self,
        archive_statistics: dict[str, Any],
        delivery_sequence: int,
    ) -> dict[str, Any]:
        return {
            "bridge": {
                "name": APP_NAME,
                "version": VERSION,
                "started_utc": self.started_utc,
                "updated_utc": self.updated_utc,
                "delivery_sequence": delivery_sequence,
                "recovery": {
                    "journal_file": self.recovery_journal_file,
                    "journal_position": self.recovery_journal_position,
                },
            },
            "commander": self.commander,
            "ship": self.ship,
            "location": self.location,
            "status": self.status,
            "cargo": self.cargo,
            "fuel": self.fuel,
            "navigation": self.navigation,
            "missions": self.missions,
            "materials": self.materials,
            "engineers": self.engineers,
            "statistics": self.statistics,
            "companions": self.companions,
            "last_event": self.last_event,
            "last_event_by_type": self.last_event_by_type,
            "event_counts": dict(self.event_counts),
            "archive_statistics": archive_statistics,
            "ranks": self.ranks,
            "progress": self.progress,
            "reputation": self.reputation,
        }

    def acknowledge_snapshot(self, generation: int) -> None:
        with self.lock:
            if self.generation == generation:
                self.dirty = False

    def mark_dirty(self) -> None:
        with self.lock:
            self.dirty = True

    def _changed(self) -> None:
        self.generation += 1
        self.dirty = True

    def _update_common_fields(self, event: dict[str, Any]) -> None:
        if "Commander" in event:
            self.commander["name"] = event["Commander"]
        if event.get("event") == "Commander" and "Name" in event:
            self.commander["name"] = event["Name"]
        if "Credits" in event:
            self.commander["credits"] = event["Credits"]
        if "StarSystem" in event:
            self.location["star_system"] = event["StarSystem"]
        if "SystemAddress" in event:
            self.location["system_address"] = event["SystemAddress"]
        if "Body" in event:
            self.location["body"] = event["Body"]
        if "BodyName" in event:
            self.location["body"] = event["BodyName"]
        if "StationName" in event:
            self.location["station"] = event["StationName"]
        if "ShipName" in event:
            self.ship["name"] = event["ShipName"]
        if "ShipIdent" in event:
            self.ship["ident"] = event["ShipIdent"]
        if "Ship_Localised" in event:
            self.ship["type"] = event["Ship_Localised"]
        elif "Ship" in event:
            self.ship["type"] = event["Ship"]
        elif "ShipType" in event:
            self.ship["type"] = event["ShipType"]
        if "CargoCapacity" in event:
            self.cargo["capacity"] = event["CargoCapacity"]

    def _dispatch(self, event_name: str, event: dict[str, Any]) -> None:
        handler = getattr(self, f"_event_{event_name.lower()}", None)

        if callable(handler):
            handler(event)

    def _event_loadgame(self, event: dict[str, Any]) -> None:
        self.commander["name"] = event.get(
            "Commander", self.commander.get("name", "Unknown")
        )
        self.commander["credits"] = event.get(
            "Credits", self.commander.get("credits", 0)
        )
        self.status["docked"] = bool(event.get("Docked", False))
        self.status["landed"] = bool(event.get("Landed", False))

    def _event_loadout(self, event: dict[str, Any]) -> None:
        self.ship["modules"] = event.get("Modules", [])
        self.ship["hull_value"] = event.get("HullValue")
        self.ship["modules_value"] = event.get("ModulesValue")
        self.ship["rebuy"] = event.get("Rebuy")
        fuel_capacity = event.get("FuelCapacity")

        if isinstance(fuel_capacity, dict):
            self.fuel["capacity"] = fuel_capacity

    def _event_location(self, event: dict[str, Any]) -> None:
        self.location["body_type"] = event.get("BodyType")
        self.location["station_type"] = event.get("StationType")
        self.status["docked"] = bool(event.get("Docked", False))
        self.status["landed"] = bool(event.get("Landed", False))
        self.status["in_supercruise"] = False

    def _event_docked(self, event: dict[str, Any]) -> None:
        self.status.update(docked=True, landed=False, in_supercruise=False)

    def _event_undocked(self, event: dict[str, Any]) -> None:
        self.status["docked"] = False
        self.location["station"] = ""

    def _event_touchdown(self, event: dict[str, Any]) -> None:
        self.status["landed"] = True

    def _event_liftoff(self, event: dict[str, Any]) -> None:
        self.status["landed"] = False

    def _event_supercruiseentry(self, event: dict[str, Any]) -> None:
        self.status.update(
            in_supercruise=True,
            docked=False,
            landed=False,
        )

    def _event_supercruiseexit(self, event: dict[str, Any]) -> None:
        self.status["in_supercruise"] = False

    def _event_fsdjump(self, event: dict[str, Any]) -> None:
        self.status.update(
            in_supercruise=True,
            docked=False,
            landed=False,
        )
        self.location["station"] = ""
        self.navigation["last_jump"] = event
        self.fuel["main"] = event.get("FuelLevel", self.fuel.get("main"))
        self.fuel["last_jump_used"] = event.get("FuelUsed")

    def _event_startjump(self, event: dict[str, Any]) -> None:
        self.navigation["jump_target"] = event

    def _event_fsdtarget(self, event: dict[str, Any]) -> None:
        self.navigation["fsd_target"] = event

    def _event_navrouteclear(self, event: dict[str, Any]) -> None:
        self.navigation["route"] = []
        self.navigation["route_jump_count"] = 0

    def _event_disembark(self, event: dict[str, Any]) -> None:
        self.status["on_foot"] = True

    def _event_embark(self, event: dict[str, Any]) -> None:
        self.status["on_foot"] = False

    def _event_cargo(self, event: dict[str, Any]) -> None:
        self.cargo["used"] = event.get("Count", self.cargo.get("used", 0))

        if "Inventory" in event:
            self.cargo["inventory"] = event["Inventory"]

    def _event_cargodepot(self, event: dict[str, Any]) -> None:
        self.cargo["last_depot_update"] = event

    def _event_fuelscoop(self, event: dict[str, Any]) -> None:
        self.fuel["main"] = event.get("Total", self.fuel.get("main"))
        self.fuel["last_scoop"] = event

    def _event_reservoirreplenished(self, event: dict[str, Any]) -> None:
        self.fuel["main"] = event.get("FuelMain", self.fuel.get("main"))
        self.fuel["reserve"] = event.get(
            "FuelReservoir", self.fuel.get("reserve")
        )

    def _event_refuelall(self, event: dict[str, Any]) -> None:
        self.fuel["last_refuel"] = event

    def _event_refuelpartial(self, event: dict[str, Any]) -> None:
        self.fuel["last_refuel"] = event

    def _event_repair(self, event: dict[str, Any]) -> None:
        self.ship["last_repair"] = event

    def _event_repairall(self, event: dict[str, Any]) -> None:
        self.ship["last_repair"] = event
        self.ship["hull_percent"] = 100.0

    def _event_rank(self, event: dict[str, Any]) -> None:
        self.ranks = event

    def _event_progress(self, event: dict[str, Any]) -> None:
        self.progress = event

    def _event_reputation(self, event: dict[str, Any]) -> None:
        self.reputation = event

    def _event_materials(self, event: dict[str, Any]) -> None:
        self.materials = event

    def _event_statistics(self, event: dict[str, Any]) -> None:
        self.statistics = event

    def _event_engineerprogress(self, event: dict[str, Any]) -> None:
        engineers = event.get("Engineers")

        if isinstance(engineers, list):
            self.engineers = {
                str(item.get("Engineer", item.get("EngineerID", index))): item
                for index, item in enumerate(engineers)
                if isinstance(item, dict)
            }
        else:
            key = str(
                event.get("Engineer", event.get("EngineerID", "unknown"))
            )
            self.engineers[key] = event

    def _event_missions(self, event: dict[str, Any]) -> None:
        rebuilt: dict[str, dict[str, Any]] = {}

        for field, status in (
            ("Active", "active"),
            ("Failed", "failed"),
            ("Complete", "completed"),
        ):
            missions = event.get(field, [])

            if not isinstance(missions, list):
                continue

            for index, mission in enumerate(missions):
                if not isinstance(mission, dict):
                    continue
                mission_id = str(mission.get("MissionID", f"{field}-{index}"))
                rebuilt[mission_id] = {"status": status, "summary": mission}

        self.missions = rebuilt

    def _event_missionaccepted(self, event: dict[str, Any]) -> None:
        mission_id = str(event.get("MissionID", "unknown"))
        self.missions[mission_id] = {"status": "active", "accepted": event}

    def _event_missioncompleted(self, event: dict[str, Any]) -> None:
        mission_id = str(event.get("MissionID", "unknown"))
        mission = self.missions.setdefault(mission_id, {})
        mission["status"] = "completed"
        mission["completed"] = event

    def _event_missionfailed(self, event: dict[str, Any]) -> None:
        mission_id = str(event.get("MissionID", "unknown"))
        mission = self.missions.setdefault(mission_id, {})
        mission["status"] = "failed"
        mission["failed"] = event

    def _event_missionabandoned(self, event: dict[str, Any]) -> None:
        mission_id = str(event.get("MissionID", "unknown"))
        mission = self.missions.setdefault(mission_id, {})
        mission["status"] = "abandoned"
        mission["abandoned"] = event


class OutputManager:
    def __init__(
        self,
        output_dir: Path,
        state_engine: StateEngine,
        archive_manager: ArchiveManager,
        delivery_hub: DeliveryHub,
        event_bus: EventBus,
        flush_interval: float,
        write_split_state_files: bool,
    ) -> None:
        self.state_dir = output_dir / "state"
        self.state_engine = state_engine
        self.archive_manager = archive_manager
        self.delivery_hub = delivery_hub
        self.event_bus = event_bus
        self.flush_interval = flush_interval
        self.write_split_state_files = write_split_state_files
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        self.thread = threading.Thread(
            target=self._loop,
            name="StateOutputWriter",
            daemon=True,
        )
        self.thread.start()

    def _loop(self) -> None:
        while not self.stop_event.wait(self.flush_interval):
            self.flush_if_dirty()

    def flush_if_dirty(self, *, force: bool = False) -> None:
        try:
            with self.event_bus.lock:
                archive_statistics = (
                    self.archive_manager.snapshot_statistics()
                )
                prepared = self.state_engine.prepare_snapshot(
                    archive_statistics,
                    self.delivery_hub.current_sequence(),
                    force=force,
                )

            if prepared is None:
                return

            generation, text = prepared
            safe_write_text(self.state_dir / "latest.json", text)

            if self.write_split_state_files:
                stable_snapshot = json.loads(text)

                for key, value in stable_snapshot.items():
                    if key == "bridge":
                        continue
                    safe_write_json(self.state_dir / f"{key}.json", value)

            self.state_engine.acknowledge_snapshot(generation)
        except Exception as exc:
            self.state_engine.mark_dirty()
            print(
                f"[{datetime.now():%H:%M:%S.%f}] State write warning: {exc}"
            )

    def close(self) -> None:
        self.stop_event.set()

        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=3)

        self.flush_if_dirty(force=True)


@dataclass(slots=True)
class WebSocketClient:
    client_id: int
    websocket: Any
    queue: asyncio.Queue[str]
    next_live_sequence: int


class ApiServer:
    """Local HTTP snapshots and ordered WebSocket event delivery."""

    def __init__(
        self,
        host: str,
        port: int,
        client_queue_events: int,
        event_bus: EventBus,
        state_engine: StateEngine,
        archive_manager: ArchiveManager,
        delivery_hub: DeliveryHub,
    ) -> None:
        self.host = host
        self.port = port
        self.bound_port = port
        self.client_queue_events = max(1, client_queue_events)
        self.event_bus = event_bus
        self.state_engine = state_engine
        self.archive_manager = archive_manager
        self.delivery_hub = delivery_hub

        self.loop: asyncio.AbstractEventLoop | None = None
        self.runner: Any = None
        self.site: Any = None
        self.thread: threading.Thread | None = None
        self.ready_event = threading.Event()
        self.startup_error: BaseException | None = None
        self.clients: dict[int, WebSocketClient] = {}
        self.next_client_id = 1
        self.dropped_slow_clients = 0

    @property
    def http_url(self) -> str:
        return f"http://{self.host}:{self.bound_port}"

    @property
    def websocket_url(self) -> str:
        return f"ws://{self.host}:{self.bound_port}/events"

    def start(self) -> None:
        if web is None:
            raise RuntimeError(
                "The local API requires aiohttp. Install it with: "
                "python -m pip install \"aiohttp>=3.11,<4\""
            )

        self.thread = threading.Thread(
            target=self._thread_main,
            name="TelemetryApiServer",
            daemon=True,
        )
        self.thread.start()

        if not self.ready_event.wait(timeout=10):
            raise RuntimeError("The local API did not start within 10 seconds.")

        if self.startup_error is not None:
            raise RuntimeError(
                f"Unable to start the local API on {self.host}:{self.port}: "
                f"{self.startup_error}"
            ) from self.startup_error

    def _thread_main(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        try:
            self.loop.run_until_complete(self._setup_async())
            self.delivery_hub.set_dispatcher(self._dispatch_from_thread)
            self.ready_event.set()
            self.loop.run_forever()
        except BaseException as exc:
            self.startup_error = exc
            self.ready_event.set()
        finally:
            self.delivery_hub.set_dispatcher(None)

            if self.runner is not None:
                try:
                    self.loop.run_until_complete(self.runner.cleanup())
                except Exception:
                    pass

            pending = asyncio.all_tasks(self.loop)

            for task in pending:
                task.cancel()

            if pending:
                self.loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )

            self.loop.close()

    async def _setup_async(self) -> None:
        application = web.Application()
        application.router.add_get("/", self._root_handler)
        application.router.add_get("/health", self._health_handler)
        application.router.add_get("/state", self._state_handler)
        application.router.add_get("/ship", self._section_handler)
        application.router.add_get("/cargo", self._section_handler)
        application.router.add_get("/commander", self._section_handler)
        application.router.add_get("/events", self._websocket_handler)

        self.runner = web.AppRunner(application, access_log=None)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()

        server = getattr(self.site, "_server", None)
        sockets = getattr(server, "sockets", None)

        if sockets:
            self.bound_port = int(sockets[0].getsockname()[1])

    async def _root_handler(self, request: Any) -> Any:
        return web.json_response(
            {
                "name": APP_NAME,
                "version": VERSION,
                "http": {
                    "health": "/health",
                    "state": "/state",
                    "ship": "/ship",
                    "cargo": "/cargo",
                    "commander": "/commander",
                },
                "websocket": {
                    "events": "/events",
                    "reconnect": "/events?after=<sequence>",
                },
            },
            headers={"Cache-Control": "no-store"},
        )

    async def _health_handler(self, request: Any) -> Any:
        with self.event_bus.lock:
            delivery = self.delivery_hub.statistics()
            state = self.state_engine.health_metadata()

        return web.json_response(
            {
                "status": "ok",
                "name": APP_NAME,
                "version": VERSION,
                "state": state,
                "delivery": delivery,
                "websocket_clients": len(self.clients),
                "dropped_slow_clients": self.dropped_slow_clients,
            },
            headers={"Cache-Control": "no-store"},
        )

    async def _state_handler(self, request: Any) -> Any:
        with self.event_bus.lock:
            sequence = self.delivery_hub.current_sequence()
            archive_statistics = (
                self.archive_manager.snapshot_statistics()
            )
            text = self.state_engine.serialize_snapshot(
                archive_statistics,
                sequence,
            )

        return self._json_text_response(text, sequence)

    async def _section_handler(self, request: Any) -> Any:
        section = request.path.lstrip("/")

        with self.event_bus.lock:
            sequence = self.delivery_hub.current_sequence()
            text = self.state_engine.serialize_section(section)

        return self._json_text_response(text, sequence)

    @staticmethod
    def _json_text_response(text: str, sequence: int) -> Any:
        return web.Response(
            text=text,
            content_type="application/json",
            charset="utf-8",
            headers={
                "Cache-Control": "no-store",
                "X-EliteBridge-Sequence": str(sequence),
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def _websocket_handler(self, request: Any) -> Any:
        after_text = request.query.get("after")
        after_sequence: int | None = None

        if after_text is not None:
            try:
                after_sequence = int(after_text)
            except ValueError:
                return web.json_response(
                    {"error": "after must be a non-negative integer"},
                    status=400,
                )

            if after_sequence < 0:
                return web.json_response(
                    {"error": "after must be a non-negative integer"},
                    status=400,
                )

        websocket = web.WebSocketResponse(
            heartbeat=30,
            autoping=True,
        )
        await websocket.prepare(request)
        queue: asyncio.Queue[str] = asyncio.Queue(
            maxsize=self.client_queue_events
        )

        with self.event_bus.lock:
            current_sequence = self.delivery_hub.current_sequence()
            archive_statistics = (
                self.archive_manager.snapshot_statistics()
            )
            state_text = self.state_engine.serialize_snapshot(
                archive_statistics,
                current_sequence,
            )
            delivery_stats = self.delivery_hub.statistics()
            replay_messages: list[str] = []
            resync_required = False

            if after_sequence is not None:
                (
                    resync_required,
                    replay_messages,
                    current_sequence,
                    oldest_sequence,
                ) = self.delivery_hub.replay_plan(after_sequence)
            else:
                oldest_sequence = delivery_stats[
                    "oldest_available_sequence"
                ]

            initial_messages = [
                json.dumps(
                    {
                        "message_type": "hello",
                        "name": APP_NAME,
                        "version": VERSION,
                        "current_sequence": current_sequence,
                        "oldest_available_sequence": oldest_sequence,
                        "replay_buffer_events": delivery_stats[
                            "buffer_capacity"
                        ],
                    },
                    separators=(",", ":"),
                )
            ]

            if after_sequence is None or resync_required:
                if resync_required:
                    initial_messages.append(
                        json.dumps(
                            {
                                "message_type": "resync_required",
                                "requested_after": after_sequence,
                                "current_sequence": current_sequence,
                                "oldest_available_sequence": oldest_sequence,
                            },
                            separators=(",", ":"),
                        )
                    )

                initial_messages.append(
                    "{\"message_type\":\"snapshot\",\"sequence\":"
                    f"{current_sequence},\"state\":{state_text}}}"
                )
            else:
                initial_messages.extend(replay_messages)

            client_id = self.next_client_id
            self.next_client_id += 1
            client = WebSocketClient(
                client_id=client_id,
                websocket=websocket,
                queue=queue,
                next_live_sequence=current_sequence + 1,
            )
            self.clients[client_id] = client

        sender = asyncio.create_task(
            self._websocket_sender(client, initial_messages)
        )

        try:
            async for message in websocket:
                if message.type == WSMsgType.TEXT:
                    try:
                        incoming = json.loads(message.data)
                    except json.JSONDecodeError:
                        continue

                    if (
                        isinstance(incoming, dict)
                        and incoming.get("message_type") == "ping"
                    ):
                        try:
                            queue.put_nowait(
                                json.dumps(
                                    {
                                        "message_type": "pong",
                                        "sequence": (
                                            self.delivery_hub.current_sequence()
                                        ),
                                    },
                                    separators=(",", ":"),
                                )
                            )
                        except asyncio.QueueFull:
                            pass
                elif message.type in {
                    WSMsgType.CLOSE,
                    WSMsgType.CLOSING,
                    WSMsgType.CLOSED,
                    WSMsgType.ERROR,
                }:
                    break
        finally:
            self.clients.pop(client_id, None)
            sender.cancel()

            try:
                await sender
            except asyncio.CancelledError:
                pass

        return websocket

    async def _websocket_sender(
        self,
        client: WebSocketClient,
        initial_messages: list[str],
    ) -> None:
        try:
            for text in initial_messages:
                await client.websocket.send_str(text)

            while not client.websocket.closed:
                text = await client.queue.get()
                await client.websocket.send_str(text)
        except asyncio.CancelledError:
            raise
        except Exception:
            if not client.websocket.closed:
                await client.websocket.close()

    def _dispatch_from_thread(self, sequence: int, text: str) -> None:
        loop = self.loop

        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(
                self._broadcast_now,
                sequence,
                text,
            )

    def _broadcast_now(self, sequence: int, text: str) -> None:
        for client_id, client in tuple(self.clients.items()):
            if sequence < client.next_live_sequence:
                continue

            try:
                client.queue.put_nowait(text)
                client.next_live_sequence = sequence + 1
            except asyncio.QueueFull:
                self.clients.pop(client_id, None)
                self.dropped_slow_clients += 1
                asyncio.create_task(
                    client.websocket.close(
                        code=1013,
                        message=b"Client queue exceeded; reconnect with after",
                    )
                )

    def close(self) -> None:
        self.delivery_hub.set_dispatcher(None)
        loop = self.loop

        if loop is None or not loop.is_running():
            if self.thread is not None and self.thread.is_alive():
                self.thread.join(timeout=5)
            return

        future = asyncio.run_coroutine_threadsafe(
            self._close_clients_async(),
            loop,
        )

        try:
            future.result(timeout=5)
        except Exception:
            pass

        loop.call_soon_threadsafe(loop.stop)

        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=5)

    async def _close_clients_async(self) -> None:
        clients = list(self.clients.values())
        self.clients.clear()

        for client in clients:
            await client.websocket.close(
                code=1001,
                message=b"Bridge shutting down",
            )


class JournalTailer:
    def __init__(
        self,
        journal_dir: Path,
        output_dir: Path,
        event_bus: EventBus,
        state_engine: StateEngine,
        archive_manager: ArchiveManager,
        checkpoint: dict[str, Any],
        show_raw_json: bool,
        read_existing_events_on_first_run: bool,
        capture_companion_files: bool,
        delivery_hub: DeliveryHub | None = None,
    ) -> None:
        self.journal_dir = journal_dir
        self.event_bus = event_bus
        self.state_engine = state_engine
        self.archive_manager = archive_manager
        self.delivery_hub = delivery_hub
        self.show_raw_json = show_raw_json
        self.read_existing_events_on_first_run = (
            read_existing_events_on_first_run
        )
        self.capture_companion_files = capture_companion_files
        self.checkpoint_path = (
            output_dir / "cache" / "telemetry-checkpoint.json"
        )
        self.checkpoint = checkpoint
        self.recovery_checkpoint_file = str(
            checkpoint.get("journal_file", "")
        )
        self.recovery_checkpoint_position = int(
            checkpoint.get("position", 0) or 0
        )

        self.active_path: Path | None = None
        self.active_file: TextIO | None = None
        self.position = 0
        self.checkpoint_dirty = False
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.companion_mtimes: dict[str, int] = {}

    def process_new_lines(self, *, final: bool = False) -> None:
        with self.lock:
            self._ensure_active_journal()

            while self.active_file is not None and self.active_path is not None:
                processing_failed = False
                waiting_for_newline = False
                has_newer = self._next_journal() is not None

                while not self.stop_event.is_set():
                    line_start = self.active_file.tell()
                    line = self.active_file.readline()

                    if not line:
                        self.active_file.seek(line_start)
                        break

                    complete_line = line.endswith(("\n", "\r"))

                    if not complete_line and not (has_newer or final):
                        self.active_file.seek(line_start)
                        waiting_for_newline = True
                        break

                    line_end = self.active_file.tell()

                    if not line.strip():
                        self._advance(line_end)
                        continue

                    bridge_event = self._journal_event(
                        line,
                        line_start,
                        line_end,
                    )

                    try:
                        self.event_bus.publish(bridge_event)
                    except Exception as exc:
                        self.active_file.seek(line_start)
                        print(
                            f"Journal processing warning at "
                            f"{self.active_path.name}:{line_start}: {exc}"
                        )
                        processing_failed = True
                        break

                    self._advance(line_end)
                    print(f"[{bridge_event.timestamp}] {bridge_event.name}")

                    if self.show_raw_json:
                        print(line.rstrip("\r\n"))

                if processing_failed:
                    break

                next_journal = self._next_journal()

                if next_journal is None:
                    break

                if waiting_for_newline:
                    # The rollover appeared after the read began. Re-read the
                    # old journal once with the new file as proof it is final.
                    continue

                self._save_checkpoint()

                if not self._open_journal(next_journal, 0):
                    break

            self._save_checkpoint()

    def process_companion_files(self) -> None:
        if not self.capture_companion_files:
            return

        with self.lock:
            for path in sorted(
                self.journal_dir.glob("*.json"),
                key=lambda item: item.name.casefold(),
            ):
                try:
                    mtime = path.stat().st_mtime_ns
                except OSError:
                    continue

                if self.companion_mtimes.get(path.name) == mtime:
                    continue

                stable = self._read_stable_companion(path)

                if stable is None:
                    continue

                raw_text, value, stable_mtime = stable
                event = BridgeEvent(
                    source="companion",
                    name=path.stem,
                    payload=value,
                    raw_text=raw_text,
                    timestamp=(
                        str(value.get("timestamp", ""))
                        if isinstance(value, dict)
                        else ""
                    ),
                    source_file=path.name,
                )

                try:
                    self.event_bus.publish(event)
                except Exception as exc:
                    print(f"Companion processing warning for {path.name}: {exc}")
                    continue

                self.companion_mtimes[path.name] = stable_mtime
                self.checkpoint_dirty = True

            self._save_checkpoint()

    def _ensure_active_journal(self) -> None:
        if self.active_file is not None and self.active_path is not None:
            return

        journals = journals_in_order(self.journal_dir)

        if not journals:
            return

        by_name = {path.name: path for path in journals}
        state_file, state_position = self.state_engine.recovery_position()
        checkpoint_file = str(self.checkpoint.get("journal_file", ""))

        if state_file in by_name:
            self._open_journal(by_name[state_file], state_position)
        elif checkpoint_file in by_name:
            # Rebuild RAM from the current journal. The archive subscriber
            # recognizes the checkpoint replay and does not append it again.
            self._open_journal(by_name[checkpoint_file], 0)
        elif state_file or checkpoint_file:
            # A cleanup tool may have removed the checkpointed journal. Start
            # at the first newer file so subsequent journals are not skipped.
            boundary = (state_file or checkpoint_file).casefold()
            newer = [
                path for path in journals
                if path.name.casefold() > boundary
            ]

            if newer:
                self._open_journal(newer[0], 0)
            else:
                latest = journals[-1]

                try:
                    position = latest.stat().st_size
                except OSError:
                    position = 0

                self._open_journal(latest, position)
        else:
            if self.read_existing_events_on_first_run:
                self._open_journal(journals[0], 0)
            else:
                latest = journals[-1]
                try:
                    position = latest.stat().st_size
                except OSError:
                    position = 0
                self._open_journal(latest, position)

    def _open_journal(
        self,
        path: Path,
        position: int,
        retries: int = 8,
    ) -> bool:
        new_file: TextIO | None = None
        last_error: OSError | None = None

        for attempt in range(retries):
            try:
                new_file = path.open(
                    "r",
                    encoding="utf-8",
                    errors="replace",
                    buffering=1,
                    newline="",
                )

                try:
                    new_file.seek(max(0, position))
                except (OSError, ValueError):
                    new_file.seek(0)

                break
            except OSError as exc:
                last_error = exc

                if new_file is not None:
                    new_file.close()
                    new_file = None

                time.sleep(0.03 * (attempt + 1))

        if new_file is None:
            print(f"Journal open warning for {path.name}: {last_error}")
            return False

        self._close_active_file()
        self.active_path = path
        self.active_file = new_file
        self.position = new_file.tell()
        self.checkpoint_dirty = True
        print(f"\nActive journal: {path.name}")
        print(f"Starting position: {self.position:,}\n")
        return True

    def _next_journal(self) -> Path | None:
        if self.active_path is None:
            return None

        active_name = self.active_path.name.casefold()

        for path in journals_in_order(self.journal_dir):
            if path.name.casefold() > active_name:
                return path

        return None

    def _journal_event(
        self,
        raw_text: str,
        start_position: int,
        end_position: int,
    ) -> BridgeEvent:
        parse_error = ""

        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            payload = None
            parse_error = str(exc)

        if isinstance(payload, dict):
            name = str(payload.get("event", "Unknown"))
            timestamp = str(payload.get("timestamp", ""))
        elif parse_error:
            name = "MalformedJournalLine"
            timestamp = ""
        else:
            name = "UnknownJournalEvent"
            timestamp = ""

        return BridgeEvent(
            source="journal",
            name=name,
            payload=payload,
            raw_text=raw_text,
            timestamp=timestamp,
            source_file=(self.active_path.name if self.active_path else ""),
            start_position=start_position,
            end_position=end_position,
            recovery_replay=self._is_recovery_replay(
                self.active_path.name if self.active_path else "",
                end_position,
            ),
            parse_error=parse_error,
        )

    def _is_recovery_replay(self, journal_file: str, position: int) -> bool:
        checkpoint_file = self.recovery_checkpoint_file
        checkpoint_position = self.recovery_checkpoint_position

        if not checkpoint_file:
            return False

        current_key = journal_file.casefold()
        checkpoint_key = checkpoint_file.casefold()
        return current_key < checkpoint_key or (
            current_key == checkpoint_key and position <= checkpoint_position
        )

    def _advance(self, position: int) -> None:
        self.position = position
        self.checkpoint_dirty = True

    def _save_checkpoint(self) -> None:
        if not self.checkpoint_dirty or self.active_path is None:
            return

        value = {
            "journal_file": self.active_path.name,
            "position": self.position,
            "updated_utc": utc_now_iso(),
            "archive_state": self.archive_manager.checkpoint_state(),
        }

        if self.delivery_hub is not None:
            value["delivery_state"] = self.delivery_hub.checkpoint_state()

        try:
            safe_write_json(self.checkpoint_path, value)
        except OSError as exc:
            print(f"Checkpoint write warning: {exc}")
            return

        self.checkpoint = value
        self.checkpoint_dirty = False

    @staticmethod
    def _read_stable_companion(
        path: Path,
        retries: int = 5,
    ) -> tuple[str, Any, int] | None:
        for attempt in range(retries):
            try:
                before = path.stat()
                with path.open(
                    "r",
                    encoding="utf-8",
                    errors="strict",
                    newline="",
                ) as source:
                    raw_text = source.read()
                value = json.loads(raw_text)
                after = path.stat()

                if (
                    before.st_mtime_ns == after.st_mtime_ns
                    and before.st_size == after.st_size
                ):
                    return raw_text, value, after.st_mtime_ns
            except (OSError, UnicodeError, json.JSONDecodeError):
                pass

            time.sleep(0.02 * (attempt + 1))

        return None

    def _close_active_file(self) -> None:
        if self.active_file is not None:
            self.active_file.close()
            self.active_file = None

    def close(self) -> None:
        with self.lock:
            self.stop_event.set()
            self._save_checkpoint()
            self._close_active_file()


class JournalEventHandler(FileSystemEventHandler):
    def __init__(self, tailer: JournalTailer) -> None:
        self.tailer = tailer

    def on_created(self, event: FileSystemEvent) -> None:
        self._handle(event)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._handle(event)

    def on_moved(self, event: FileSystemEvent) -> None:
        self.tailer.process_new_lines()
        self.tailer.process_companion_files()

    def _handle(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return

        name = Path(event.src_path).name

        if name.startswith("Journal.") and name.endswith(".log"):
            self.tailer.process_new_lines()
        elif name.casefold().endswith(".json"):
            self.tailer.process_companion_files()


def string_list_setting(settings: dict[str, Any], name: str) -> list[str]:
    value = settings.get(name, [])

    if not isinstance(value, list):
        raise RuntimeError(f"{name} must be a JSON array of event names.")

    return [str(item) for item in value]


def main() -> int:
    app_dir = app_directory()
    settings = load_settings(app_dir)
    journal_dir = resolve_journal_directory(settings)
    output_dir = resolve_output_directory(app_dir, settings)
    raw_dir = output_dir / "raw"
    state_dir = output_dir / "state"
    companion_dir = output_dir / "companions"
    cache_dir = output_dir / "cache"

    for directory in (raw_dir, state_dir, companion_dir, cache_dir):
        directory.mkdir(parents=True, exist_ok=True)

    poll_interval = max(
        0.25,
        float(settings.get("poll_interval_seconds", 2.0)),
    )
    flush_interval = max(
        0.05,
        float(settings.get("state_flush_interval_seconds", 0.25)),
    )
    checkpoint_path = cache_dir / "telemetry-checkpoint.json"
    checkpoint = read_json_object(checkpoint_path)

    event_bus = EventBus()
    state_engine = StateEngine()
    state_restored, latest_archive_statistics = state_engine.restore(
        state_dir / "latest.json"
    )
    checkpoint_archive_state = checkpoint.get("archive_state", {})
    has_checkpoint_archive_state = isinstance(
        checkpoint_archive_state, dict
    ) and bool(checkpoint_archive_state)
    recovered_archive_state = (
        checkpoint_archive_state
        if has_checkpoint_archive_state
        else latest_archive_statistics
    )
    checkpoint_delivery_state = checkpoint.get("delivery_state", {})
    has_checkpoint_delivery_state = isinstance(
        checkpoint_delivery_state, dict
    ) and bool(checkpoint_delivery_state)
    recovered_delivery_state = (
        checkpoint_delivery_state
        if has_checkpoint_delivery_state
        else {
            "last_sequence": state_engine.restored_delivery_sequence,
        }
    )

    archive_manager = ArchiveManager(
        raw_dir=raw_dir,
        mode=str(settings["archive_mode"]),
        skip_events=string_list_setting(
            settings, "production_archive_skip_events"
        ),
        deduplicate_events=string_list_setting(
            settings, "production_archive_deduplicate_events"
        ),
        recovered_state=(
            recovered_archive_state
            if isinstance(recovered_archive_state, dict)
            else {}
        ),
        recovered_through_checkpoint=has_checkpoint_archive_state,
    )
    companion_store = CompanionStore(companion_dir)
    delivery_hub = DeliveryHub(
        replay_buffer_events=max(
            1,
            int(settings.get("api_replay_buffer_events", 2000)),
        ),
        recovered_state=recovered_delivery_state,
        recovered_through_checkpoint=has_checkpoint_delivery_state,
    )

    # Critical subscribers are ordered so durable storage happens before RAM
    # is advanced. Network delivery is non-critical and cannot block capture.
    event_bus.subscribe("raw_archive", archive_manager.handle)
    event_bus.subscribe("companion_store", companion_store.handle)
    event_bus.subscribe("state_engine", state_engine.handle)
    event_bus.subscribe(
        "live_delivery",
        delivery_hub.handle,
        critical=False,
    )

    output_manager = OutputManager(
        output_dir=output_dir,
        state_engine=state_engine,
        archive_manager=archive_manager,
        delivery_hub=delivery_hub,
        event_bus=event_bus,
        flush_interval=flush_interval,
        write_split_state_files=bool(
            settings.get("write_split_state_files", False)
        ),
    )
    tailer = JournalTailer(
        journal_dir=journal_dir,
        output_dir=output_dir,
        event_bus=event_bus,
        state_engine=state_engine,
        archive_manager=archive_manager,
        checkpoint=checkpoint,
        show_raw_json=bool(settings.get("show_raw_json", True)),
        read_existing_events_on_first_run=bool(
            settings.get("read_existing_events_on_first_run", True)
        ),
        capture_companion_files=bool(
            settings.get("capture_companion_files", True)
        ),
        delivery_hub=delivery_hub,
    )
    api_enabled = bool(settings.get("api_enabled", True))
    api_host = str(settings.get("api_host", "127.0.0.1")).strip()
    api_port = int(settings.get("api_port", 8765))

    if not 1 <= api_port <= 65535:
        raise RuntimeError("api_port must be between 1 and 65535.")

    api_server = (
        ApiServer(
            host=api_host,
            port=api_port,
            client_queue_events=max(
                1,
                int(settings.get("api_client_queue_events", 256)),
            ),
            event_bus=event_bus,
            state_engine=state_engine,
            archive_manager=archive_manager,
            delivery_hub=delivery_hub,
        )
        if api_enabled
        else None
    )

    print("=" * 56)
    print(f" {APP_NAME}")
    print(f" Python telemetry bridge v{VERSION}")
    print("=" * 56)
    print()
    print(f"Journal folder : {journal_dir}")
    print(f"Output folder  : {output_dir}")
    print(f"Archive mode   : {settings['archive_mode']}")
    print(f"Live state     : {state_dir / 'latest.json'}")
    print(f"State restored : {'yes' if state_restored else 'no'}")

    if api_server is not None:
        print(f"HTTP API       : http://{api_host}:{api_port}/state")
        print(f"WebSocket      : ws://{api_host}:{api_port}/events")
    else:
        print("HTTP/WebSocket : disabled")

    if api_host not in {"127.0.0.1", "localhost", "::1"}:
        print("WARNING         : API is exposed beyond this computer.")

    print()

    observer = Observer()
    observer_started = False
    shutdown_event = threading.Event()
    previous_handlers: dict[signal.Signals, Any] = {}

    def request_shutdown(signum: int, frame: Any) -> None:
        shutdown_event.set()

    for signal_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        current_signal = getattr(signal, signal_name, None)

        if current_signal is not None:
            previous_handlers[current_signal] = signal.signal(
                current_signal, request_shutdown
            )

    try:
        # Catch up state and companions before exposing the first new snapshot.
        tailer.process_new_lines()
        tailer.process_companion_files()
        output_manager.flush_if_dirty(force=True)
        output_manager.start()

        if api_server is not None:
            api_server.start()

        observer.schedule(
            JournalEventHandler(tailer),
            str(journal_dir),
            recursive=False,
        )
        observer.start()
        observer_started = True

        print("Capturing every journal event through the event bus.")
        print("State is buffered in RAM and flushed on a timer.")

        if api_server is not None:
            print(f"HTTP state: {api_server.http_url}/state")
            print(f"WebSocket events: {api_server.websocket_url}")

        print("Press Ctrl+C to stop.\n")

        while not shutdown_event.wait(poll_interval):
            # Low-frequency reconciliation covers coalesced Windows events.
            tailer.process_new_lines()
            tailer.process_companion_files()
    except KeyboardInterrupt:
        shutdown_event.set()
    finally:
        print("\nStopping telemetry bridge...")

        if observer_started and observer.is_alive():
            observer.stop()
            observer.join(timeout=5)

        # A complete non-newline-terminated final record is accepted here.
        tailer.process_new_lines(final=True)
        tailer.close()
        output_manager.close()

        if api_server is not None:
            api_server.close()

        for current_signal, previous in previous_handlers.items():
            signal.signal(current_signal, previous)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
