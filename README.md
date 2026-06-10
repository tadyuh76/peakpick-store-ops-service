# PeakPick Store Operations Service

Owns the staff board projection and publishes preparation, ready, notification, and pickup events.

Owned database tables:

- local `event_log`

Run locally:

```bash
pip install -r requirements.txt
uvicorn services.store_ops_service.main:app --reload --port 8004
```
