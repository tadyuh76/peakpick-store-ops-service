from __future__ import annotations

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


class PickupRequest(BaseModel):
    token: str


def pickup_token(order_id: str) -> str:
    digest = hashlib.sha1(order_id.encode("utf-8")).hexdigest()[:6].upper()
    return f"PK-{digest}"


async def handle_pickup_slot_reserved(
    event: EventEnvelope,
    state: dict[str, dict[str, object]] = board,
) -> None:
    state[event.aggregate_id] = {
        "order_id": event.aggregate_id,
        "slot_id": event.payload["slot_id"],
        "pickup_window": event.payload["pickup_window"],
        "status": "SlotAssigned",
        "token": None,
        "correlation_id": event.correlation_id,
        "updated_at": datetime.now(UTC).isoformat(),
    }


async def mark_preparing(
    order_id: str,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    state: dict[str, dict[str, object]] = board,
) -> dict[str, object]:
    item = _require_board_item(order_id, state)
    item["status"] = "Preparing"
    item["updated_at"] = datetime.now(UTC).isoformat()
    await event_bus.publish(
        new_event(
            EventType.ORDER_PREPARING,
            aggregate_id=order_id,
            source=settings.service_name,
            payload=item,
            correlation_id=str(item["correlation_id"]),
        )
    )
    return item


async def mark_ready(
    order_id: str,
    event_bus: InMemoryEventBus | RabbitMQEventBus,
    state: dict[str, dict[str, object]] = board,
) -> dict[str, object]:
    item = _require_board_item(order_id, state)
    item["status"] = "PlacedInSlot"
    item["updated_at"] = datetime.now(UTC).isoformat()
    await event_bus.publish(
        new_event(
            EventType.ORDER_PLACED_IN_SLOT,
            aggregate_id=order_id,
            source=settings.service_name,
            payload=item,
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
            payload=item,
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
    item = _require_board_item(order_id, state)
    if item["token"] != token:
        raise HTTPException(status_code=400, detail="Invalid pickup token")
    item["status"] = "Completed"
    item["updated_at"] = datetime.now(UTC).isoformat()
    await event_bus.publish(
        new_event(
            EventType.ORDER_PICKED_UP,
            aggregate_id=order_id,
            source=settings.service_name,
            payload=item,
            correlation_id=str(item["correlation_id"]),
        )
    )
    return item


def _require_board_item(order_id: str, state: dict[str, dict[str, object]]) -> dict[str, object]:
    if order_id not in state:
        raise HTTPException(status_code=404, detail="Order is not on the staff board")
    return state[order_id]


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
async def get_board() -> list[dict[str, object]]:
    return list(board.values())


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
