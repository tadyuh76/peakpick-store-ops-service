# PeakPick Store Operations Service

Store Operations Service là microservice cho bảng xử lý của nhân viên cửa hàng.

## Database Riêng

Service này sở hữu database `peakpick_store_ops` với bảng:

- `event_log`

Board của nhân viên được dựng từ các domain event đã consume.

## Event

Nhận event:

- `PickupSlotReserved`
- `InventoryShortageDetected`

Phát event:

- `OrderPreparing`
- `OrderReady`
- `NotificationRequested`
- `OrderPickedUp`

## Chạy Local

```bash
pip install -r requirements.txt
uvicorn services.store_ops_service.main:app --reload --port 8004
```
