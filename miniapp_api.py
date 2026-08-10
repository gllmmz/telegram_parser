"""
FastAPI-приложение для Telegram Mini App. Работает в ТОМ ЖЕ процессе и event loop,
что и bot.py (см. main() в bot.py) — использует напрямую его глобальное состояние
(user_databases, parsed_channels_history, client_pool и т.д.) и функции, без IPC.

Аутентификация — через Telegram WebApp initData (заголовок X-Telegram-Init-Data),
подпись проверяется по алгоритму Telegram:
https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
"""
import asyncio
import hashlib
import hmac
import json
import re
import time
import uuid
from types import SimpleNamespace
from urllib.parse import parse_qsl

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

import bot as botmod

INITDATA_MAX_AGE = 24 * 3600  # старше суток initData не принимаем (защита от повторного использования утёкшей ссылки)
JOB_TTL_SECONDS = 3600  # завершённые джобы старше часа чистим из памяти при создании новых
MAX_CHANNELS_PER_JOB = 30
CHANNEL_RE = re.compile(r'^(?:https?://)?(?:t\.me/|telegram\.me/)?@?[a-zA-Z0-9_]{4,32}/?$')


def _validate_init_data(init_data: str) -> dict:
    try:
        pairs = parse_qsl(init_data, strict_parsing=True)
    except ValueError:
        raise HTTPException(401, "Некорректный initData")

    data = dict(pairs)
    received_hash = data.pop("hash", None)
    if not received_hash:
        raise HTTPException(401, "Отсутствует hash в initData")

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    secret_key = hmac.new(b"WebAppData", botmod.BOT_TOKEN.encode(), hashlib.sha256).digest()
    computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        raise HTTPException(401, "Неверная подпись initData")

    try:
        auth_date = int(data.get("auth_date", "0"))
    except ValueError:
        auth_date = 0
    if time.time() - auth_date > INITDATA_MAX_AGE:
        raise HTTPException(401, "initData устарел, перезапусти мини-апп")

    try:
        user = json.loads(data["user"])
    except (KeyError, json.JSONDecodeError):
        raise HTTPException(401, "Нет данных пользователя в initData")

    return user


async def get_current_user_id(x_telegram_init_data: str = Header(..., alias="X-Telegram-Init-Data")) -> int:
    user = _validate_init_data(x_telegram_init_data)
    return int(user["id"])


class ClearRequest(BaseModel):
    which: str  # 'parsed' | 'found'


class ParsingStartRequest(BaseModel):
    channels: list[str]
    posts: int
    min_subs: int = 0
    max_subs: int = 10_000_000


class MarkContactedRequest(BaseModel):
    user_id: int  # id найденного человека (ключ записи), не текущего пользователя мини-аппа
    channel: str
    contacted: bool


class BroadcastStartRequest(BaseModel):
    user_ids: list[int]  # id получателей (broadcast_candidates), не текущего пользователя
    text: str


def create_app(bot) -> FastAPI:
    """bot — экземпляр telegram.Bot (app.bot из main()), нужен для run_parsing_job."""
    app = FastAPI()
    active_jobs: dict[str, dict] = {}
    user_start_locks: dict[int, asyncio.Lock] = {}

    def _lock_for_user(user_id: int) -> asyncio.Lock:
        lock = user_start_locks.get(user_id)
        if lock is None:
            lock = asyncio.Lock()
            user_start_locks[user_id] = lock
        return lock

    def _cleanup_old_jobs():
        now = time.time()
        stale = [
            jid for jid, job in active_jobs.items()
            if job.get("status") in ("done", "error") and now - job.get("created_at", now) > JOB_TTL_SECONDS
        ]
        for jid in stale:
            del active_jobs[jid]

    @app.get("/api/me")
    async def api_me(user_id: int = Depends(get_current_user_id)):
        return {
            "user_id": user_id,
            "is_admin": botmod.is_admin(user_id),
            "has_paid_access": botmod.has_paid_access(user_id),
            "free_trials_left": botmod.free_trials_left(user_id),
            "has_access": botmod.has_access(user_id),
            "status_text": botmod.access_status_text(user_id),
        }

    @app.get("/api/database/parsed")
    async def api_database_parsed(sort: str = "new", user_id: int = Depends(get_current_user_id)):
        data = botmod.parsed_channels_history.get(str(user_id), [])
        data = sorted(data, key=lambda x: x.get('last_parsed_at', 0), reverse=(sort != 'old'))
        return {"items": data}

    @app.get("/api/database/found")
    async def api_database_found(sort: str = "new", user_id: int = Depends(get_current_user_id)):
        data = botmod.user_databases.get(str(user_id), [])
        data = sorted(data, key=lambda x: x.get('found_at', 0), reverse=(sort != 'old'))
        return {"items": data}

    @app.post("/api/database/clear")
    async def api_database_clear(body: ClearRequest, user_id: int = Depends(get_current_user_id)):
        if body.which not in ("parsed", "found"):
            raise HTTPException(400, "which должен быть 'parsed' или 'found'")
        user_id_str = str(user_id)
        if body.which == "parsed":
            botmod.parsed_channels_history[user_id_str] = []
            await botmod.save_parsed_channels_async(botmod.parsed_channels_history)
        else:
            botmod.user_databases[user_id_str] = []
            await botmod.save_databases_async(botmod.user_databases)
        return {"ok": True}

    @app.post("/api/database/mark_contacted")
    async def api_mark_contacted(body: MarkContactedRequest, user_id: int = Depends(get_current_user_id)):
        items = botmod.user_databases.get(str(user_id), [])
        for item in items:
            if item.get("user_id") == body.user_id and item.get("channel") == body.channel:
                item["contacted"] = body.contacted
                await botmod.save_databases_async(botmod.user_databases)
                return {"ok": True}
        raise HTTPException(404, "Запись не найдена")

    @app.get("/api/tariffs")
    async def api_tariffs():
        return {
            "tariffs": [
                {"months": m, "price": p, "discount": d, "savings": s}
                for m, p, d, s in botmod.TARIFFS
            ],
            "contact": "kushher",
        }

    @app.post("/api/parsing/start")
    async def api_parsing_start(body: ParsingStartRequest, user_id: int = Depends(get_current_user_id)):
        channels = list(dict.fromkeys(c.strip().lstrip('@') for c in body.channels if c.strip()))
        if not channels:
            raise HTTPException(400, "Не указаны каналы")
        if len(channels) > MAX_CHANNELS_PER_JOB:
            raise HTTPException(400, f"Слишком много каналов за раз (максимум {MAX_CHANNELS_PER_JOB})")
        invalid = [c for c in channels if not CHANNEL_RE.match(c)]
        if invalid:
            raise HTTPException(400, f"Некорректный формат канала: {', '.join(invalid[:5])}")
        if not (0 <= body.posts <= 400):
            raise HTTPException(400, "Число постов должно быть от 0 до 400")

        min_subs, max_subs = max(0, body.min_subs), max(0, body.max_subs)
        if min_subs > max_subs:
            min_subs, max_subs = max_subs, min_subs

        if not await botmod.is_subscribed(user_id, SimpleNamespace(bot=bot)):
            raise HTTPException(
                403,
                f"Чтобы пользоваться ботом, подпишись на канал {botmod.REQUIRED_CHANNEL_LINK}",
            )

        # Проверка лимита бесплатных попыток и его списание должны быть атомарны относительно
        # друг друга — иначе несколько параллельных запросов от одного пользователя все пройдут
        # проверку has_access() до того, как первый из них спишет попытку.
        async with _lock_for_user(user_id):
            if not botmod.has_access(user_id):
                raise HTTPException(403, "Бесплатные запросы закончились. Оформи подписку.")
            if not botmod.has_paid_access(user_id):
                await botmod.consume_trial_use(user_id)

            _cleanup_old_jobs()
            job_id = uuid.uuid4().hex
            active_jobs[job_id] = {
                "user_id": user_id, "status": "queued", "percent": 0,
                "posts_done": 0, "total_posts": body.posts * len(channels),
                "found": 0, "error": None, "results": [], "rejected": [],
                "created_at": time.time(),
            }

        def on_progress(snapshot: dict):
            job = active_jobs.get(job_id)
            if job is not None:
                job.update(snapshot)

        async def worker():
            try:
                async with botmod.parse_semaphore:
                    active_jobs[job_id]["status"] = "running"
                    outcome = await botmod.run_parsing_job(
                        SimpleNamespace(bot=bot), user_id, user_id,
                        channels, body.posts, min_subs, max_subs,
                        notify_chat=False, on_progress=on_progress,
                    )
                active_jobs[job_id]["status"] = "done"
                active_jobs[job_id]["results"] = (outcome or {}).get("results", [])
                active_jobs[job_id]["rejected"] = (outcome or {}).get("rejected", [])
            except Exception as e:
                active_jobs[job_id]["status"] = "error"
                active_jobs[job_id]["error"] = str(e)

        asyncio.create_task(worker())
        return {"job_id": job_id}

    @app.get("/api/parsing/status/{job_id}")
    async def api_parsing_status(job_id: str, user_id: int = Depends(get_current_user_id)):
        job = active_jobs.get(job_id)
        if job is None or job["user_id"] != user_id:
            raise HTTPException(404, "Задача не найдена")
        return job

    @app.get("/api/broadcast/candidates")
    async def api_broadcast_candidates(user_id: int = Depends(get_current_user_id)):
        return {"items": botmod.broadcast_candidates(str(user_id))}

    @app.post("/api/broadcast/start")
    async def api_broadcast_start(body: BroadcastStartRequest, user_id: int = Depends(get_current_user_id)):
        if not body.text.strip():
            raise HTTPException(400, "Текст рассылки пустой")
        if not body.user_ids:
            raise HTTPException(400, "Не выбраны получатели")
        if len(body.user_ids) > botmod.MAX_BROADCAST_RECIPIENTS:
            raise HTTPException(
                400, f"Слишком много получателей за раз (максимум {botmod.MAX_BROADCAST_RECIPIENTS})"
            )
        if not await botmod.is_user_account_connected(user_id):
            raise HTTPException(403, "Личный аккаунт не подключён")

        by_id = {c["user_id"]: c for c in botmod.broadcast_candidates(str(user_id))}
        wanted = list(dict.fromkeys(body.user_ids))  # без повторов, порядок сохраняем
        recipients = [by_id[uid] for uid in wanted if uid in by_id]
        if not recipients:
            raise HTTPException(400, "Получатели не найдены в базе")

        _cleanup_old_jobs()
        job_id = uuid.uuid4().hex
        active_jobs[job_id] = {
            "user_id": user_id, "status": "running", "sent": 0, "failed": 0,
            "total": len(recipients), "done": 0, "stopped_reason": None, "error": None,
            "created_at": time.time(),
        }

        def on_progress(snapshot: dict):
            job = active_jobs.get(job_id)
            if job is not None:
                job.update(snapshot)

        async def worker():
            try:
                result = await botmod.run_broadcast_job(
                    user_id, recipients, body.text, on_progress=on_progress
                )
                active_jobs[job_id]["status"] = "done"
                active_jobs[job_id]["sent"] = result["sent"]
                active_jobs[job_id]["failed"] = result["failed"]
                active_jobs[job_id]["stopped_reason"] = result.get("stopped_reason")
            except Exception as e:
                active_jobs[job_id]["status"] = "error"
                active_jobs[job_id]["error"] = str(e)

        asyncio.create_task(worker())
        return {"job_id": job_id}

    @app.get("/api/broadcast/status/{job_id}")
    async def api_broadcast_status(job_id: str, user_id: int = Depends(get_current_user_id)):
        job = active_jobs.get(job_id)
        if job is None or job["user_id"] != user_id:
            raise HTTPException(404, "Задача не найдена")
        return job

    return app
