"""
GraphQL Gateway — единая точка агрегации Order + Catalog.
Студент A.

Ключевое требование ЛР3: резолвер ordersByUserId реально ходит
в Order Service, затем — в Catalog Service за деталями товаров.
Для избежания N+1 используется DataLoader (batch по ?ids=).
"""

import os
import logging
from typing import List, Optional

import httpx
import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.dataloader import DataLoader
from strawberry.types import Info
from fastapi import FastAPI, Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("graphql-gateway")

ORDER_URL = os.getenv("ORDER_URL", "http://order:8000")
CATALOG_URL = os.getenv("CATALOG_URL", "http://catalog-1:8001")
PORT = int(os.getenv("PORT", "8000"))
HTTP_TIMEOUT = 5.0


# ---------- REST-клиенты (реальные вызовы) ----------

async def fetch_orders_by_user(user_id: str) -> List[dict]:
    """Резолвер верхнего уровня: идём в Order Service."""
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(f"{ORDER_URL}/orders", params={"userId": user_id})
        r.raise_for_status()
        return r.json()["data"]


async def load_products_batch(product_ids: List[int]) -> List[Optional[dict]]:
    """
    Batch-загрузчик товаров из Catalog. ОДИН HTTP-запрос на батч — решает N+1.
    Порядок ответа строго соответствует порядку входных ключей.
    """
    if not product_ids:
        return []
    unique_ids = list({int(i) for i in product_ids})
    ids_param = ",".join(str(i) for i in unique_ids)
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(f"{CATALOG_URL}/products", params={"ids": ids_param})
        r.raise_for_status()
        items = r.json()["data"]
    by_id = {int(p["id"]): p for p in items}
    log.info("DataLoader: batched %d unique product ids in 1 request", len(unique_ids))
    return [by_id.get(int(i)) for i in product_ids]


# ---------- GraphQL-схема ----------

@strawberry.type(description="Товар из сервиса Catalog.")
class Product:
    id: strawberry.ID = strawberry.field(description="Уникальный ID товара")
    name: str = strawberry.field(description="Название товара")
    price: float = strawberry.field(description="Цена товара")


@strawberry.type(description="Позиция заказа: товар + количество.")
class OrderItem:
    # Private — поле хранится в объекте, но не отдаётся наружу; используется для DataLoader
    product_id: strawberry.Private[int]
    quantity: int = strawberry.field(description="Количество единиц товара")

    @strawberry.field(
        description="Данные товара, подгружаются из Catalog батчем через DataLoader"
    )
    async def product(self, info: Info) -> Optional[Product]:
        loader: DataLoader = info.context["product_loader"]
        raw = await loader.load(self.product_id)
        if raw is None:
            return None
        return Product(
            id=strawberry.ID(str(raw["id"])),
            name=raw["name"],
            price=float(raw["price"]),
        )


@strawberry.type(description="Заказ пользователя с позициями.")
class Order:
    id: strawberry.ID = strawberry.field(description="ID заказа")
    user_id: strawberry.ID = strawberry.field(name="userId", description="ID пользователя")
    created_at: str = strawberry.field(name="createdAt", description="Дата создания (ISO 8601)")
    items: List[OrderItem] = strawberry.field(description="Позиции заказа")


@strawberry.type
class Query:
    @strawberry.field(
        name="ordersByUserId",
        description=(
            "Получить все заказы пользователя с деталями товаров. "
            "Резолвер агрегирует данные: сначала Order Service, затем Catalog Service "
            "(батчем через DataLoader, чтобы избежать N+1)."
        ),
    )
    async def orders_by_user_id(self, info: Info, userId: strawberry.ID) -> List[Order]:
        raw_orders = await fetch_orders_by_user(str(userId))
        orders: List[Order] = []
        for o in raw_orders:
            items = [
                OrderItem(product_id=int(it["productId"]), quantity=int(it["quantity"]))
                for it in o["items"]
            ]
            orders.append(
                Order(
                    id=strawberry.ID(str(o["id"])),
                    user_id=strawberry.ID(str(o["userId"])),
                    created_at=o["createdAt"],
                    items=items,
                )
            )
        return orders


schema = strawberry.Schema(query=Query)


# ---------- FastAPI + GraphQLRouter ----------

async def get_context(request: Request) -> dict:
    """
    Свежий DataLoader на каждый GraphQL-запрос.
    Это обязательное условие корректной работы батчинга: загрузчик должен
    собирать ключи в пределах одного запроса.
    """
    return {"product_loader": DataLoader(load_fn=load_products_batch)}


graphql_app = GraphQLRouter(
    schema,
    context_getter=get_context,
    graphql_ide="apollo-sandbox",  # Playground доступен по GET /graphql
)

app = FastAPI(title="GraphQL Gateway", version="1.0.0")
app.include_router(graphql_app, prefix="/graphql")


@app.get("/health")
def health():
    return {"status": "ok", "service": "graphql-gateway"}


@app.get("/")
def index():
    return {"message": "GraphQL Gateway", "endpoint": "/graphql"}