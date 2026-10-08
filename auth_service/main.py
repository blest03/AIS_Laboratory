"""
Auth Service — аутентификация и JWT-токены.
Студент A: Auth & Security.

Эндпоинты (внутри контейнера): /register, /login, /me, /health
Через Nginx доступны как /auth/register, /auth/login, /auth/me, /auth/health
"""

import os
import socket
import time
import logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

import requests
import jwt
from passlib.context import CryptContext
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field


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

# ---------- Хэширование паролей (passlib + bcrypt) ----------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ---------- In-memory хранилище: username -> {"password_hash": str} ----------
users_db: dict[str, dict] = {}


# ---------- Service Discovery ----------
def consul_register_with_retry(payload: dict, retries: int = 15, delay: float = 2.0) -> None:
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
        "Check": {
            "HTTP": f"http://{HOST_IP}:{PORT}/health",
            "Interval": "5s",
            "Timeout": "2s",
            "DeregisterCriticalServiceAfter": "30s",
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


app = FastAPI(
    title="Auth Service",
    lifespan=lifespan,
    root_path="/auth",  # корректная генерация URL в Swagger за Nginx
)


# ---------- Models ----------
class UserAuth(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


# ---------- Routes ----------
@app.get("/health")
def health_check():
    return {"status": "ok", "service": SERVICE_ID}


@app.post("/register")
def register(user: UserAuth):
    if user.username in users_db:
        raise HTTPException(status_code=400, detail="User already exists")
    users_db[user.username] = {"password_hash": pwd_context.hash(user.password)}
    return {"message": "User registered successfully", "service": SERVICE_ID}


@app.post("/login")
def login(user: UserAuth):
    record = users_db.get(user.username)
    if not record or not pwd_context.verify(user.password, record["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    now = datetime.now(timezone.utc)
    payload = {
        "sub": user.username,
        "iat": now,
        "exp": now + timedelta(hours=JWT_TTL_HOURS),
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return {"access_token": token, "token_type": "Bearer", "service": SERVICE_ID}


@app.get("/me")
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