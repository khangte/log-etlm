import json

msg = {
    "ts": "2025-12-03T12:00:00Z",
    "svc": "auth",
    "lvl": "INFO",
    "rid": "req_123",
    "met": "POST",
    "path": "/v1/login",
    "st": 200,
    "lat": 30.5,
    "evt": "LoginSucceeded"
}

payload = json.dumps(msg)
print(len(payload.encode("utf-8")), "bytes")
