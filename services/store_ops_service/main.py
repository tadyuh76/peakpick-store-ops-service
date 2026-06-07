from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from shared.event_bus import InMemoryEventBus, RabbitMQEventBus, build_event_bus
from shared.events import EventEnvelope, EventType, new_event
from shared.logging import configure_logging, log_event
from shared.settings import get_settings


settings = get_settings("store-ops-service")
logger = configure_logging(settings.service_name)
board: dict[str, dict[str, object]] = {}
BOARD_EVENT_TYPES = (
    EventType.PICKUP_SLOT_RESERVED,
    EventType.ORDER_PREPARING,
    EventType.ORDER_PLACED_IN_SLOT,
    EventType.ORDER_READY,
    EventType.ORDER_PICKED_UP,
)


class PickupRequest(BaseModel):
    token: str


def pickup_token(order_id: str) -> str:
    digest = hashlib.sha1(order_id.encode("utf-8")).hexdigest()[:6].upper()
    return f"PK-{digest}"


def _require_status(item: dict[str, object], allowed: set[str], action: str) -> None:
    current = str(item["status"])
    if current not in allowed:
        allowed_list = ", ".join(sorted(allowed))
        raise HTTPException(
            status_code=409,
            detail=f"Cannot {action} order from status {current}. Expected: {allowed_list}",
        )


def _database_enabled() -> bool:
    return bool(settings.database_url)


def _timestamp(value: object) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _apply_board_event(
    state: dict[str, dict[str, object]],
    event_type: str,
    aggregate_id: str,
    correlation_id: str,
    payload: dict[str, object],
    occurred_at: object,
) -> None:
    status_by_event = {
        EventType.PICKUP_SLOT_RESERVED: "SlotAssigned",
        EventType.ORDER_PREPARING: "Preparing",
        EventType.ORDER_PLACED_IN_SLOT: "PlacedInSlot",
        EventType.ORDER_READY: "ReadyForPickup",
        EventType.ORDER_PICKED_UP: "Completed",
    }
    status = status_by_event.get(event_type)
    if not status:
        return

    existing = state.get(aggregate_id, {})
    state[aggregate_id] = {
        "order_id": aggregate_id,
        "slot_id": payload.get("slot_id", existing.get("slot_id", "")),
        "pickup_window": payload.get("pickup_window", existing.get("pickup_window", "")),
        "status": status,
        "token": payload.get("token", existing.get("token")),
        "correlation_id": str(correlation_id or payload.get("correlation_id", existing.get("correlation_id", ""))),
        "updated_at": _timestamp(occurred_at),
    }


async def _list_board() -> list[dict[str, object]]:
    if not _database_enabled():
        return list(board.values())
    hydrated = await asyncio.to_thread(_list_board_from_event_log_sync)
    board.clear()
    board.update({str(item["order_id"]): item for item in hydrated})
    return hydrated


def _list_board_from_event_log_sync() -> list[dict[str, object]]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT event_type, aggregate_id, correlation_id, payload, occurred_at
            FROM event_log
            WHERE event_type = ANY(%s)
            ORDER BY occurred_at ASC, created_at ASC
            """,
            ([str(event_type) for event_type in BOARD_EVENT_TYPES],),
        ).fetchall()

    state: dict[str, dict[str, object]] = {}
    for row in rows:
        payload = row["payload"]
        if isinstance(payload, dict):
            _apply_board_event(
                state,
                str(row["event_type"]),
                str(row["aggregate_id"]),
                str(row["correlation_id"]),
                payload,
                row["occurred_at"],
            )
    return list(state.values())


async def _require_board_item(
    order_id: str,
    state: dict[str, dict[str, object]],
) -> dict[str, object]:
    if order_id not in state and _database_enabled():
        await _list_board()
    if order_id not in state:
        raise HTTPException(status_code=404, detail="Order is not on the staff board")
    return state[order_id]


async def handle_pickup_slot_reserved(
    event: EventEnvelope,
    state: dict[str, dict[str, object]] = board,
) -> None:
    _apply_board_event(
        state,
        str(event.event_type),
        event.aggregate_id,
        event.correlation_id,
        event.payload,
        datetime.now(UTC),
    )


async def mark_preparing(
    order_id: str,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    state: dict[str, dict[str, object]] = board,
) -> dict[str, object]:
    item = await _require_board_item(order_id, state)
    _require_status(item, {"SlotAssigned"}, "mark preparing")
    item["status"] = "Preparing"
    item["updated_at"] = datetime.now(UTC).isoformat()
    await event_bus.publish(
        new_event(
            EventType.ORDER_PREPARING,
            aggregate_id=order_id,
            source=settings.service_name,
            payload=dict(item),
            correlation_id=str(item["correlation_id"]),
        )
    )
    return item


async def mark_ready(
    order_id: str,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    state: dict[str, dict[str, object]] = board,
) -> dict[str, object]:
    item = await _require_board_item(order_id, state)
    _require_status(item, {"Preparing"}, "mark ready")
    item["status"] = "PlacedInSlot"
    item["updated_at"] = datetime.now(UTC).isoformat()
    await event_bus.publish(
        new_event(
            EventType.ORDER_PLACED_IN_SLOT,
            aggregate_id=order_id,
            source=settings.service_name,
            payload=dict(item),
            correlation_id=str(item["correlation_id"]),
        )
    )
    item["status"] = "ReadyForPickup"
    item["token"] = pickup_token(order_id)
    item["updated_at"] = datetime.now(UTC).isoformat()
    await event_bus.publish(
        new_event(
            EventType.ORDER_READY,
            aggregate_id=order_id,
            source=settings.service_name,
            payload=dict(item),
            correlation_id=str(item["correlation_id"]),
        )
    )
    await event_bus.publish(
        new_event(
            EventType.NOTIFICATION_REQUESTED,
            aggregate_id=order_id,
            source=settings.service_name,
            payload={
                "order_id": order_id,
                "channel": "demo",
                "message": f"Order {order_id} is ready at slot {item['slot_id']}. Token: {item['token']}",
            },
            correlation_id=str(item["correlation_id"]),
        )
    )
    return item


async def verify_pickup(
    order_id: str,
    token: str,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    state: dict[str, dict[str, object]] = board,
) -> dict[str, object]:
    item = await _require_board_item(order_id, state)
    _require_status(item, {"ReadyForPickup"}, "verify pickup")
    expected_token = str(item["token"]).strip().upper()
    provided_token = token.strip().upper()
    if expected_token != provided_token:
        raise HTTPException(status_code=400, detail="Invalid pickup token")
    now = datetime.now(UTC).isoformat()
    item["status"] = "Completed"
    item["verified_at"] = now
    item["picked_up_at"] = now
    item["updated_at"] = now
    await event_bus.publish(
        new_event(
            EventType.ORDER_PICKED_UP,
            aggregate_id=order_id,
            source=settings.service_name,
            payload=dict(item),
            correlation_id=str(item["correlation_id"]),
        )
    )
    return item

@asynccontextmanager
async def lifespan(app: FastAPI):
    event_bus = build_event_bus(settings)
    await event_bus.connect()
    await event_bus.subscribe(
        EventType.PICKUP_SLOT_RESERVED,
        handle_pickup_slot_reserved,
        queue_name=f"{settings.service_name}.pickup-slot-reserved",
    )
    app.state.event_bus = event_bus
    log_event(logger, settings.service_name, "event subscriptions ready", bus=settings.event_bus)
    try:
        yield
    finally:
        await event_bus.close()


app = FastAPI(
    title="PeakPick Store Operations Service",
    version="0.1.0",
    description="Staff board, preparation status, and pickup verification.",
    lifespan=lifespan,
)


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    return {
        "status": "ok",
        "service": settings.service_name,
        "event_bus_connected": request.app.state.event_bus.is_connected,
    }


@app.get("/board")
async def get_board(status: str | None = None) -> list[dict[str, object]]:
    items = await _list_board()
    if status:
        items = [item for item in items if item["status"] == status]
    return sorted(items, key=lambda item: (str(item["pickup_window"]), str(item["slot_id"])))


@app.get("/board/{order_id}")
async def get_board_item(order_id: str) -> dict[str, object]:
    return await _require_board_item(order_id, board)


@app.post("/orders/{order_id}/preparing")
async def preparing(order_id: str, request: Request) -> dict[str, object]:
    item = await mark_preparing(order_id, request.app.state.event_bus)
    log_event(logger, settings.service_name, "order preparing", order_id=order_id)
    return item


@app.post("/orders/{order_id}/ready")
async def ready(order_id: str, request: Request) -> dict[str, object]:
    item = await mark_ready(order_id, request.app.state.event_bus)
    log_event(logger, settings.service_name, "order ready", order_id=order_id)
    return item


@app.post("/orders/{order_id}/pickup")
async def pickup(order_id: str, payload: PickupRequest, request: Request) -> dict[str, object]:
    item = await verify_pickup(order_id, payload.token, request.app.state.event_bus)
    log_event(logger, settings.service_name, "order picked up", order_id=order_id)
    return item
