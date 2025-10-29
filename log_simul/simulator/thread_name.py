import random

THREAD_POOLS = {
    "auth":   [f"nio-8080-exec-{i}" for i in range(1, 17)],
    "order":  [f"nio-8081-exec-{i}" for i in range(1, 17)],
    "payment":[f"nio-8082-exec-{i}" for i in range(1, 17)],
    "notify": [f"nio-8083-exec-{i}" for i in range(1, 17)],
}

def next_thread(service: str) -> str:
    pool = THREAD_POOLS.get(service, [])
    return random.choice(pool)
