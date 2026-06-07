import pytest

from services.analytics_service.main import event_counts, handle_any_event, recent_events
from services.inventory_service.main import handle_order_paid as reserve_inventory
from services.notification_service.main import handle_notification_requested
from services.slot_service.main import handle_order_paid as reserve_slot
from services.slot_service.main import handle_status_event
from services.store_ops_service.main import (
    handle_pickup_slot_reserved,
    mark_preparing,
    mark_ready,
    verify_pickup,
)
from shared.event_bus import InMemoryEventBus
from shared.events import EventType, new_event


@pytest.mark.asyncio
async def test_member3_ready_to_pickup_flow_has_inventory_notification_and_analytics() -> None:
    bus = InMemoryEventBus()
    await bus.connect()
    event_counts.clear()
    recent_events.clear()

    inventory_stock = {"coffee": 5, "snack": 5}
    inventory_reservations = {}
    slot_state = {}
    staff_board = {}
    notifications = []

    await bus.subscribe(
        EventType.ORDER_PAID,
        lambda event: reserve_inventory(event, bus, inventory_stock, inventory_reservations),
    )
    await bus.subscribe(EventType.ORDER_PAID, lambda event: reserve_slot(event, bus, slot_state))
    await bus.subscribe(
        EventType.PICKUP_SLOT_RESERVED,
        lambda event: handle_pickup_slot_reserved(event, staff_board),
    )
    for event_type in (
        EventType.ORDER_PREPARING,
        EventType.ORDER_PLACED_IN_SLOT,
        EventType.ORDER_READY,
        EventType.ORDER_PICKED_UP,
    ):
        await bus.subscribe(event_type, lambda event: handle_status_event(event, slot_state))
    await bus.subscribe(
        EventType.NOTIFICATION_REQUESTED,
        lambda event: handle_notification_requested(event, notifications),
    )
    await bus.subscribe("*", handle_any_event)

    await bus.publish(
        new_event(
            EventType.ORDER_PAID,
            aggregate_id="order-member3",
            source="order-service",
            payload={
                "order_id": "order-member3",
                "customer_name": "Member 3 Demo",
                "items": [{"sku": "coffee", "quantity": 1}, {"sku": "snack", "quantity": 1}],
                "pickup_window": "09:30-09:35",
                "payment_status": "Paid",
                "order_status": "Paid",
            },
            correlation_id="66666666-6666-6666-6666-666666666666",
        )
    )

    await mark_preparing("order-member3", bus, staff_board)
    ready = await mark_ready("order-member3", bus, staff_board)
    completed = await verify_pickup("order-member3", str(ready["token"]), bus, staff_board)

    assert inventory_reservations["order-member3"]["status"] == "Reserved"
    assert notifications[-1]["order_id"] == "order-member3"
    assert completed["status"] == "Completed"
    assert slot_state["order-member3"]["status"] == "Available"
    assert event_counts["InventoryReserved"] == 1
    assert event_counts["OrderReady"] == 1
    assert event_counts["OrderPickedUp"] == 1
