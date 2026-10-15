"""
Order Service — заказы пользователей (in-memory).
Студент A.
"""

import os
import socket
import time
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List

import requests
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
CONSUL_REGISTER = f"{CONSUL_URL}/v1/agent/service/register"
CONSUL_DEREGISTER = f"{CONSUL_URL}/v1/agent/service/deregister"

SERVICE_ID = os.getenv("SERVICE_ID", "order-1")
SERVICE_NAME = "order-service"
PORT = int(os.getenv("PORT", "8000"))
HOST_IP = os.getenv("HOST_IP") or socket.gethostbyname(socket.gethostname())

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(SERVICE_ID)

# In-memory заказы. items содержат только productId и quantity —
# детали товаров подтягивает GraphQL Gateway (агрегация).
orders_db: Dict[int, dict] = {
    101: {"id": 101, "userId": "123", "createdAt": "2024-01-01T10:00:00Z",
          "items": [{"productId": 1, "quantity": 2}, {"productId": 3, "quantity": 1}]},
    102: {"id": 102, "userId": "123", "createdAt": "2024-01-05T14:30:00Z",
          "items": [{"productId": 1, "quantity": 1}, {"productId": 5, "quantity": 2}]},
    103: {"id": 103, "userId": "123", "createdAt": "2024-01-10T09:15:00Z",
          "items": [{"productId": 2, "quantity": 1}]},
    104: {"id": 104, "userId": "456", "createdAt": "2024-01-03T11:00:00Z",
          "items": [{"productId": 4, "quantity": 1}, {"productId": 6, "quantity": 1}]},
    105: {"id": 105, "userId": "789", "createdAt": "2024-01-07T16:45:00Z",
          "items": [{"productId": 7, "quantity": 1},
                    {"productId": 8, "quantity": 3},
                    {"productId": 10, "quantity": 1}]},
}


def consul_register_with_retry(payload: dict, retries: int = 15, delay: float = 2.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            r = requests.put(CONSUL_REGISTER, json=payload, timeout=3)
            if r.status_code == 200:
                log.info("Registered in Consul as %s", SERVICE_ID)
                return
        except requests.RequestException as e:
            log.warning("Consul not ready (%d/%d): %s", attempt, retries, e)
        time.sleep(delay)
    raise RuntimeError("Failed to register in Consul after retries")


def consul_deregister() -> None:
    try:
        requests.put(f"{CONSUL_DEREGISTER}/{SERVICE_ID}", timeout=3)
        log.info("Deregistered from Consul: %s", SERVICE_ID)
    except requests.RequestException as e:
        log.warning("Consul deregister failed: %s", e)


def build_consul_payload() -> dict:
    return {
        "ID": SERVICE_ID,
        "Name": SERVICE_NAME,
        "Address": HOST_IP,
        "Port": PORT,
        "Check": {
            "HTTP": f"http://{HOST_IP}:{PORT}/health",
            "Interval": "5s",
            "Timeout": "2s",
            "DeregisterCriticalServiceAfter": "30s",
        },
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    consul_register_with_retry(build_consul_payload())
    try:
        yield
    finally:
        consul_deregister()


app = FastAPI(
    title="Order Service",
    version="1.0.0",
    description=(
        "Заказы пользователей. Возвращает только `productId` и `quantity` — "
        "детали товара агрегирует GraphQL Gateway через Catalog Service."
    ),
    lifespan=lifespan,
    root_path="/api/orders",
)


class OrderItemIn(BaseModel):
    productId: int = Field(..., description="ID товара из Catalog")
    quantity: int = Field(..., ge=1, description="Количество")


class OrderIn(BaseModel):
    userId: str = Field(..., min_length=1, description="ID пользователя")
    items: List[OrderItemIn] = Field(..., description="Позиции заказа")


@app.get("/health", tags=["system"])
def health_check():
    """Health-check для Consul."""
    return {"status": "ok", "service": SERVICE_ID}


@app.get("/orders", tags=["orders"])
def list_orders(userId: str = Query(..., description="ID пользователя")):
    """Все заказы пользователя. Товары — только ID и количество."""
    user_orders = sorted(
        (o for o in orders_db.values() if o["userId"] == userId),
        key=lambda o: o["createdAt"],
    )
    return {"userId": userId, "count": len(user_orders), "data": user_orders}


@app.get("/orders/{order_id}", tags=["orders"])
def get_order(order_id: int):
    """Заказ по ID."""
    if order_id not in orders_db:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"data": orders_db[order_id]}


@app.post("/orders", status_code=201, tags=["orders"])
def create_order(order: OrderIn):
    """Создать заказ."""
    new_id = (max(orders_db.keys()) + 1) if orders_db else 1001
    new_order = {
        "id": new_id,
        "userId": order.userId,
        "createdAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": [{"productId": it.productId, "quantity": it.quantity} for it in order.items],
    }
    orders_db[new_id] = new_order
    return {"data": new_order}