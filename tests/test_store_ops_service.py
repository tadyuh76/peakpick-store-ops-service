import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from services.store_ops_service.main import app, board, mark_preparing, mark_ready, verify_pickup
from shared.event_bus import InMemoryEventBus


client = TestClient(app)


@pytest.fixture
def staff_board() -> dict[str, dict[str, object]]:
    return {
        "order-100": {
            "order_id": "order-100",
            "slot_id": "P-01",
            "pickup_window": "12:00-12:15",
            "status": "SlotAssigned",
            "token": None,
            "correlation_id": "55555555-5555-5555-5555-555555555555",
            "updated_at": "2026-06-09T00:00:00+00:00",
        }
    }


@pytest.fixture
def demo_board() -> None:
    board.clear()
    board["order-1"] = {
        "order_id": "order-1",
        "slot_id": "P-01",
        "pickup_window": "12:00-12:15",
        "status": "Preparing",
        "token": None,
        "correlation_id": "a",
        "updated_at": "2026-06-09T00:00:00+00:00",
    }
    board["order-2"] = {
        "order_id": "order-2",
        "slot_id": "P-02",
        "pickup_window": "12:00-12:15",
        "status": "ReadyForPickup",
        "token": "PK-ABCDEF",
        "correlation_id": "b",
        "updated_at": "2026-06-09T00:01:00+00:00",
    }
    yield
    board.clear()


@pytest.mark.asyncio
async def test_ready_requires_preparing(staff_board: dict[str, dict[str, object]]) -> None:
    bus = InMemoryEventBus()
    await bus.connect()

    with pytest.raises(HTTPException) as exc_info:
        await mark_ready("order-100", bus, staff_board)

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_pickup_requires_ready_status(staff_board: dict[str, dict[str, object]]) -> None:
    bus = InMemoryEventBus()
    await bus.connect()
    staff_board["order-100"]["token"] = "PK-123456"

    with pytest.raises(HTTPException) as exc_info:
        await verify_pickup("order-100", "PK-123456", bus, staff_board)

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_staff_lifecycle_moves_in_order(staff_board: dict[str, dict[str, object]]) -> None:
    bus = InMemoryEventBus()
    await bus.connect()

    preparing = await mark_preparing("order-100", bus, staff_board)
    assert preparing["status"] == "Preparing"

    ready = await mark_ready("order-100", bus, staff_board)
    assert ready["status"] == "ReadyForPickup"
    assert ready["token"]

    completed = await verify_pickup("order-100", str(ready["token"]), bus, staff_board)
    assert completed["status"] == "Completed"


def test_board_can_filter_by_status(demo_board: None) -> None:
    response = client.get("/board?status=ReadyForPickup")

    assert response.status_code == 200
    assert [item["order_id"] for item in response.json()] == ["order-2"]


def test_board_lookup_returns_one_order(demo_board: None) -> None:
    response = client.get("/board/order-2")

    assert response.status_code == 200
    assert response.json()["order_id"] == "order-2"


@pytest.mark.asyncio
async def test_pickup_records_verified_at_and_normalizes_token(
    staff_board: dict[str, dict[str, object]],
) -> None:
    bus = InMemoryEventBus()
    await bus.connect()

    await mark_preparing("order-100", bus, staff_board)
    ready = await mark_ready("order-100", bus, staff_board)
    completed = await verify_pickup("order-100", str(ready["token"]).lower(), bus, staff_board)

    assert completed["status"] == "Completed"
    assert completed["verified_at"]
    assert completed["picked_up_at"]
