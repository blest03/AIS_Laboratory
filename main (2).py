"""
Order Event-Sourced Service (ЛР6: CQRS + Event Sourcing).

Write Side: POST /orders, /orders/{id}/pay, /orders/{id}/cancel.
  Каждая команда: загрузить события → rehydrate → проверить правила →
  добавить событие в Event Store → опубликовать в Kafka.

Event Store: in-memory Dict[orderId, List[event]] + JSON-персистентность
             в /data/events.json (переживает рестарт контейнера).

Read Side: in-memory read_model, обновляется АСИНХРОННО через Kafka-консьюмер
           (projection handler). GET /orders/{id} читает ТОЛЬКО из read_model.

Eventual consistency: сразу после POST read-модель может ещё не успеть
обновиться → GET вернёт 202 (Retry-After), а не 404.
"""

import os
import json
import socket
import uuid
import time
import threading
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, Field
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------- Config ----------
CONSUL_URL        = os.getenv("CONSUL_URL", "http://consul:8500")
CONSUL_REGISTER   = f"{CONSUL_URL}/v1/agent/service/register"
CONSUL_DEREGISTER = f"{CONSUL_URL}/v1/agent/service/deregister"

SERVICE_ID   = os.getenv("SERVICE_ID", "order-es-1")
SERVICE_NAME = "order-es-service"
PORT         = int(os.getenv("PORT", "8000"))
HOST_IP      = os.getenv("HOST_IP") or socket.gethostbyname(socket.gethostname())

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "orders-events")
EVENTS_FILE  = os.getenv("EVENTS_FILE", "/data/events.json")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] [order-es] %(message)s")
log = logging.getLogger("order-es")

# ==================================================================
# EVENT STORE — единственный источник истины (state НЕ хранится)
# ==================================================================
events_store: Dict[str, List[dict]] = {}
_es_lock = threading.Lock()
_next_order_id = 1000


def _load_events() -> None:
    global events_store, _next_order_id
    if not os.path.exists(EVENTS_FILE):
        return
    try:
        with open(EVENTS_FILE) as f:
            events_store = json.load(f)
        if events_store:
            _next_order_id = max(int(oid) for oid in events_store.keys()) + 1
        log.info("Loaded %d orders (%d events) from %s",
                 len(events_store), sum(len(v) for v in events_store.values()), EVENTS_FILE)
    except Exception as e:
        log.warning("Failed to load events: %s", e)


def _save_events() -> None:
    try:
        os.makedirs(os.path.dirname(EVENTS_FILE), exist_ok=True)
        with open(EVENTS_FILE, "w") as f:
            json.dump(events_store, f, indent=2)
    except Exception as e:
        log.warning("Failed to save events: %s", e)


# ==================================================================
# READ MODEL — проекция, оптимизированная под GET-запросы
# ==================================================================
read_model: Dict[str, dict] = {}
_rm_lock = threading.Lock()


# ==================================================================
# KAFKA — producer (publish) и consumer (projection)
# ==================================================================
producer: Optional[KafkaProducer] = None
_producer_lock = threading.Lock()


def get_producer() -> KafkaProducer:
    global producer
    with _producer_lock:
        if producer is None:
            last_err = None
            for attempt in range(30):
                try:
                    producer = KafkaProducer(
                        bootstrap_servers=KAFKA_BROKER,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        acks="all",
                        retries=3,
                        api_version=(3, 7, 0),
                    )
                    log.info("Kafka producer connected to %s", KAFKA_BROKER)
                    break
                except NoBrokersAvailable as e:
                    last_err = e
                    log.warning("Kafka not ready (%d/30), retry in 2s", attempt + 1)
                    time.sleep(2)
            if producer is None:
                raise RuntimeError(f"Cannot connect to Kafka: {last_err}")
    return producer


def publish_event(event: dict) -> None:
    try:
        p = get_producer()
        p.send(KAFKA_TOPIC, event)
        p.flush(timeout=5)
        log.info("Published %s order=%s v%d → %s",
                 event["eventType"], event["orderId"], event["version"], KAFKA_TOPIC)
    except Exception as e:
        # событие уже в Event Store → в проде здесь был бы outbox + retry
        log.error("Kafka publish failed: %s", e)


# ==================================================================
# AGGREGATE — rehydration + бизнес-правила
# ==================================================================
class OrderAggregate:
    """Не хранит состояние само по себе — только собирает его из событий."""

    def __init__(self, order_id: str):
        self.id = order_id
        self.status: Optional[str] = None       # None | CREATED | PAID | CANCELLED
        self.user_id: Optional[str] = None
        self.total_amount: float = 0.0
        self.items: List[dict] = []
        self.created_at: Optional[str] = None
        self.version: int = 0
        self.pending_events: List[dict] = []    # новые события до persist

    @classmethod
    def from_events(cls, order_id: str, events: List[dict]) -> "OrderAggregate":
        """Rehydration: проигрываем события слева направо."""
        agg = cls(order_id)
        for ev in events:
            agg.apply(ev)
        return agg

    def apply(self, event: dict) -> None:
        """Чистая функция: применяет событие к состоянию."""
        et = event["eventType"]
        p  = event["payload"]
        if et == "OrderCreated":
            self.status       = "CREATED"
            self.user_id      = p["userId"]
            self.total_amount = float(p["totalAmount"])
            self.items        = p["items"]
            self.created_at   = event["timestamp"]
        elif et == "OrderPaid":
            self.status = "PAID"
        elif et == "OrderCancelled":
            self.status = "CANCELLED"
        else:
            log.warning("Unknown event type: %s", et)
        self.version = event["version"]

    def _raise_event(self, event_type: str, payload: dict) -> None:
        event = {
            "eventId":   str(uuid.uuid4()),
            "orderId":   self.id,
            "eventType": event_type,
            "payload":   payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version":   self.version + 1,
        }
        self.pending_events.append(event)
        self.apply(event)

    # ---------- команды (бизнес-правила) ----------
    def create(self, user_id: str, items: List[dict], total_amount: float) -> None:
        if self.status is not None:
            raise ValueError("Order already exists")
        if total_amount < 0:
            raise ValueError("totalAmount must be >= 0")
        self._raise_event("OrderCreated", {
            "userId": user_id, "items": items, "totalAmount": total_amount,
        })

    def pay(self) -> None:
        if self.status is None:
            raise ValueError("Order not found")
        if self.status == "PAID":
            raise ValueError("Order already paid")
        if self.status == "CANCELLED":
            raise ValueError("Cannot pay cancelled order")
        self._raise_event("OrderPaid", {"amount": self.total_amount})

    def cancel(self, reason: str = "User requested") -> None:
        if self.status is None:
            raise ValueError("Order not found")
        if self.status == "CANCELLED":
            raise ValueError("Order already cancelled")
        self._raise_event("OrderCancelled", {"reason": reason})


def load_aggregate(order_id: str) -> OrderAggregate:
    with _es_lock:
        events = list(events_store.get(order_id, []))
    return OrderAggregate.from_events(order_id, events)


def persist_events(agg: OrderAggregate) -> None:
    if not agg.pending_events:
        return
    with _es_lock:
        events_store.setdefault(agg.id, []).extend(agg.pending_events)
        _save_events()
    for ev in agg.pending_events:
        publish_event(ev)
    agg.pending_events.clear()


# ==================================================================
# PROJECTION HANDLER — асинхронно обновляет read-модель из Kafka
# ==================================================================
def apply_to_read_model(event: dict) -> None:
    oid = event["orderId"]
    et  = event["eventType"]
    p   = event["payload"]
    with _rm_lock:
        if et == "OrderCreated":
            read_model[oid] = {
                "orderId":     oid,
                "userId":      p["userId"],
                "items":       p["items"],
                "totalAmount": float(p["totalAmount"]),
                "status":      "CREATED",
                "createdAt":   event["timestamp"],
                "updatedAt":   event["timestamp"],
                "version":     event["version"],
            }
        elif et == "OrderPaid" and oid in read_model:
            read_model[oid]["status"]    = "PAID"
            read_model[oid]["updatedAt"] = event["timestamp"]
            read_model[oid]["version"]   = event["version"]
        elif et == "OrderCancelled" and oid in read_model:
            read_model[oid]["status"]    = "CANCELLED"
            read_model[oid]["updatedAt"] = event["timestamp"]
            read_model[oid]["version"]   = event["version"]
    log.info("Read model updated: order %s → %s (v%d)",
             oid, et, event["version"])


def projection_consumer_loop() -> None:
    """Unique group_id → при каждом старте читаем с earliest и пересобираем RM."""
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=f"order-es-projection-{uuid.uuid4()}",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                api_version=(3, 7, 0),
            )
            log.info("Projection consumer started (topic=%s)", KAFKA_TOPIC)
            for msg in consumer:
                try:
                    apply_to_read_model(msg.value)
                except Exception as e:
                    log.error("Projection error: %s", e)
        except Exception as e:
            log.warning("Projection consumer error: %s; retry in 3s", e)
            time.sleep(3)


# ==================================================================
# CONSUL
# ==================================================================
def consul_register() -> None:
    payload = {
        "ID": SERVICE_ID, "Name": SERVICE_NAME,
        "Address": HOST_IP, "Port": PORT,
        "Check": {"HTTP": f"http://{HOST_IP}:{PORT}/health",
                  "Interval": "5s", "Timeout": "2s",
                  "DeregisterCriticalServiceAfter": "30s"},
    }
    for attempt in range(15):
        try:
            if requests.put(CONSUL_REGISTER, json=payload, timeout=3).status_code == 200:
                log.info("Registered in Consul as %s", SERVICE_ID)
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    log.warning("Consul registration failed — continuing anyway")


# ==================================================================
# FASTAPI
# ==================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_events()
    consul_register()
    threading.Thread(target=projection_consumer_loop, daemon=True).start()
    try:
        yield
    finally:
        try:
            requests.put(f"{CONSUL_DEREGISTER}/{SERVICE_ID}", timeout=3)
        except Exception:
            pass


app = FastAPI(
    title="Order Event-Sourced Service (ЛР6)",
    version="1.0.0",
    description=(
        "CQRS + Event Sourcing. "
        "Write Side: POST /orders, /orders/{id}/pay, /orders/{id}/cancel. "
        "Read Side: GET /orders, GET /orders/{id} — только из read-модели. "
        "События публикуются в Kafka topic orders-events."
    ),
    lifespan=lifespan,
    root_path="/api/es",
)


class OrderItemIn(BaseModel):
    productId: int
    quantity: int = Field(..., ge=1)


class OrderIn(BaseModel):
    userId: str = Field(..., min_length=1)
    items: List[OrderItemIn]
    totalAmount: float = Field(..., ge=0)


# ---------------- WRITE SIDE (команды) ----------------
@app.post("/orders", status_code=201, tags=["commands"])
def create_order(order: OrderIn):
    global _next_order_id
    with _es_lock:
        new_id = str(_next_order_id)
        _next_order_id += 1

    agg = load_aggregate(new_id)
    try:
        agg.create(order.userId, [it.dict() for it in order.items], order.totalAmount)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    persist_events(agg)
    return {"orderId": new_id, "status": agg.status, "version": agg.version}


@app.post("/orders/{order_id}/pay", tags=["commands"])
def pay_order(order_id: str):
    agg = load_aggregate(order_id)
    if agg.status is None:
        raise HTTPException(status_code=404, detail="Order not found")
    try:
        agg.pay()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    persist_events(agg)
    return {"orderId": order_id, "status": agg.status, "version": agg.version}


@app.post("/orders/{order_id}/cancel", tags=["commands"])
def cancel_order(order_id: str, reason: str = Query("User requested")):
    agg = load_aggregate(order_id)
    if agg.status is None:
        raise HTTPException(status_code=404, detail="Order not found")
    try:
        agg.cancel(reason)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    persist_events(agg)
    return {"orderId": order_id, "status": agg.status, "version": agg.version}


# ---------------- READ SIDE (запросы) ----------------
@app.get("/orders/{order_id}", tags=["queries"])
def get_order(order_id: str, response: Response):
    with _rm_lock:
        if order_id in read_model:
            return {"data": read_model[order_id]}

    # В read-модели ещё нет — проверяем, существует ли в Event Store
    with _es_lock:
        has_events = order_id in events_store

    if has_events:
        response.headers["Retry-After"] = "1"
        raise HTTPException(
            status_code=202,
            detail="Order exists but not yet projected to read model. "
                   "Retry in ~1s (eventual consistency).",
        )
    raise HTTPException(status_code=404, detail="Order not found")


@app.get("/orders", tags=["queries"])
def list_orders(userId: Optional[str] = Query(None)):
    with _rm_lock:
        items = list(read_model.values())
    if userId:
        items = [o for o in items if o["userId"] == userId]
    items.sort(key=lambda o: o["createdAt"])
    return {"count": len(items), "data": items}


# ---------------- DEBUG: Event Store inspection ----------------
@app.get("/orders/{order_id}/events", tags=["debug"])
def get_events(order_id: str):
    with _es_lock:
        evs = list(events_store.get(order_id, []))
    if not evs:
        raise HTTPException(status_code=404, detail="No events for order")
    return {"orderId": order_id, "count": len(evs), "events": evs}


@app.get("/orders/{order_id}/state", tags=["debug"])
def get_rehydrated_state(order_id: str):
    """Состояние, восстановленное из событий (НЕ из read-модели)."""
    agg = load_aggregate(order_id)
    if agg.status is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return {
        "orderId": agg.id, "status": agg.status, "version": agg.version,
        "userId": agg.user_id, "totalAmount": agg.total_amount, "items": agg.items,
    }


@app.get("/health", tags=["system"])
def health():
    with _es_lock:
        total_events = sum(len(v) for v in events_store.values())
    with _rm_lock:
        rm_size = len(read_model)
    return {
        "status": "ok", "service": SERVICE_ID,
        "events": total_events, "orders": len(events_store),
        "read_model_size": rm_size,
    }