import os
import socket
import time
import logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

import requests
import jwt
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel

# ---------- Настройки ----------
CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
CONSUL_REGISTER = f"{CONSUL_URL}/v1/agent/service/register"
CONSUL_DEREGISTER = f"{CONSUL_URL}/v1/agent/service/deregister"

SERVICE_ID = os.getenv("SERVICE_ID", "auth-1")
SERVICE_NAME = "auth-service"
PORT = int(os.getenv("PORT", "8000"))
HOST_IP = os.getenv("HOST_IP") or socket.gethostbyname(socket.gethostname())

SECRET_KEY = os.getenv("JWT_SECRET", "dev_secret_change_me_in_prod")
JWT_TTL_HOURS = int(os.getenv("JWT_TTL_HOURS", "1"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(SERVICE_ID)

# ---------- In-memory хранилище (упрощение для лабы) ----------
users_db: dict[str, str] = {}

# ---------- Service Discovery helpers ----------
def consul_register_with_retry(payload: dict, retries: int = 15, delay: float = 2.0) -> None:
    """Регистрация в Consul с ретраями: сервис может стартовать раньше Consul."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.put(CONSUL_REGISTER, json=payload, timeout=3)
            if r.status_code == 200:
                log.info("Registered in Consul as %s", SERVICE_ID)
                return
            log.warning("Consul register status=%s body=%s", r.status_code, r.text)
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
        "Tags": [
            "traefik.enable=true",
            # --- Router ---
            "traefik.http.routers.auth.rule=PathPrefix(`/auth`)",
            "traefik.http.routers.auth.entrypoints=web",
            "traefik.http.routers.auth.service=auth-service",
            # --- Service (load-balancer) ---
            "traefik.http.services.auth-service.loadbalancer.server.port=8000",
        ],
        "Check": {
            "HTTP": f"http://{HOST_IP}:{PORT}/auth/health",
            "Interval": "3s",
            "Timeout": "2s",
            "DeregisterCriticalServiceAfter": "15s",
        },
    }


# ---------- Lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    consul_register_with_retry(build_consul_payload())
    try:
        yield
    finally:
        consul_deregister()


app = FastAPI(title="Auth Service", lifespan=lifespan)

# ---------- Models ----------
class UserAuth(BaseModel):
    username: str
    password: str


# ---------- Routes ----------
@app.get("/auth/health")
def health_check():
    return {"status": "ok", "service": SERVICE_ID}


@app.post("/auth/register")
def register(user: UserAuth):
    if user.username in users_db:
        raise HTTPException(status_code=400, detail="User already exists")
    users_db[user.username] = user.password
    return {"message": "User registered successfully", "service": SERVICE_ID}


@app.post("/auth/login")
def login(user: UserAuth):
    if users_db.get(user.username) != user.password:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    now = datetime.now(timezone.utc)
    payload = {
        "sub": user.username,
        "iat": now,
        "exp": now + timedelta(hours=JWT_TTL_HOURS),
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return {"access_token": token, "token_type": "Bearer", "service": SERVICE_ID}


@app.get("/auth/me")
def get_me(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")

    token = authorization.split(" ", 1)[1]
    try:
        decoded = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    return {"user": decoded["sub"], "service": SERVICE_ID, "message": "You have access!"}