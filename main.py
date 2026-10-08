import os
import time
import json
import uuid
import urllib.request
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "catalog")
SERVICE_ID = os.getenv("SERVICE_ID", "catalog-1")
INSTANCE_NAME = os.getenv("INSTANCE_NAME", SERVICE_ID)
SERVICE_PORT = int(os.getenv("PORT", "8000"))
SERVICE_ADDR = os.getenv("SERVICE_ADDR", SERVICE_ID)

products_db: Dict[str, dict] = {}


class ProductCreate(BaseModel):
    name: str
    price: float
    description: Optional[str] = None


class ProductUpdate(BaseModel):
    name: Optional[str] = None
    price: Optional[float] = None
    description: Optional[str] = None


app = FastAPI(title=f"Catalog Service ({INSTANCE_NAME})", version="1.0.0")


def register_in_consul():
    url = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/register"
    payload = {
        "Name": SERVICE_NAME,
        "ID": SERVICE_ID,
        "Address": SERVICE_ADDR,
        "Port": SERVICE_PORT,
        "Check": {
            "HTTP": f"http://{SERVICE_ADDR}:{SERVICE_PORT}/health",
            "Interval": "10s",
            "Timeout": "2s",
        },
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="PUT",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"[consul] Registered {SERVICE_ID}: {resp.status}")
    except Exception as e:
        print(f"[consul] Registration failed: {e}")


def deregister_from_consul():
    url = f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/deregister/{SERVICE_ID}"
    req = urllib.request.Request(url, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=5):
            print(f"[consul] Deregistered {SERVICE_ID}")
    except Exception as e:
        print(f"[consul] Deregistration failed: {e}")


@app.on_event("startup")
def on_startup():
    time.sleep(3)
    register_in_consul()


@app.on_event("shutdown")
def on_shutdown():
    deregister_from_consul()


@app.get("/health")
def health():
    return {"status": "ok", "instance_id": INSTANCE_NAME}


@app.get("/products")
def list_products():
    return {
        "instance_id": INSTANCE_NAME,
        "count": len(products_db),
        "products": [{"id": pid, **data} for pid, data in products_db.items()],
    }


@app.post("/products", status_code=201)
def create_product(product: ProductCreate):
    pid = str(uuid.uuid4())
    products_db[pid] = product.model_dump()
    return {"id": pid, "instance_id": INSTANCE_NAME, **products_db[pid]}


@app.get("/products/{product_id}")
def get_product(product_id: str):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"id": product_id, "instance_id": INSTANCE_NAME, **products_db[product_id]}


@app.put("/products/{product_id}")
def update_product(product_id: str, product: ProductUpdate):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    update_data = {k: v for k, v in product.model_dump().items() if v is not None}
    products_db[product_id].update(update_data)
    return {"id": product_id, "instance_id": INSTANCE_NAME, **products_db[product_id]}


@app.delete("/products/{product_id}", status_code=204)
def delete_product(product_id: str):
    if product_id not in products_db:
        raise HTTPException(status_code=404, detail="Product not found")
    del products_db[product_id]
    return None