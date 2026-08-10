import asyncio
import copy
import html
import random
import re
import json
import os
import time
import traceback
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetRepliesRequest
from telethon.tl.types import Channel, User
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError,
    MsgIdInvalidError, PhoneNumberInvalidError, PhoneCodeInvalidError,
    PhoneCodeExpiredError, SessionPasswordNeededError, PasswordHashInvalidError,
    PeerFloodError,
)

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, BotCommand
from telegram.error import TimedOut, NetworkError
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler
)

# ================== НАСТРОЙКИ ==================
load_dotenv()

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Не задана переменная окружения {name}. "
            f"Создай файл .env на основе .env.example и заполни его."
        )
    return value

BOT_TOKEN = _require_env("BOT_TOKEN")
API_ID = int(_require_env("API_ID"))
API_HASH = _require_env("API_HASH")
# Несколько Telethon-сессий (аккаунтов) через запятую — опционально, для админских
# команд (/grant по @username) и как запасной пул. Основной парсинг идёт через
# личный аккаунт пользователя.
SESSION_NAMES = [s.strip() for s in os.getenv("SESSION_NAMES", "").split(",") if s.strip()]
# Telegram user_id владельца бота — только он может выдавать/отзывать доступ (/grant, /revoke).
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
# Канал, подписка на который обязательна для использования бота. Бот должен быть
# добавлен в этот канал администратором — иначе Telegram не даст проверять статус подписки.
REQUIRED_CHANNEL_USERNAME = "kushhher"
REQUIRED_CHANNEL_LINK = "https://t.me/kushhher"

DATABASE_FILE = "user_databases.json"
PARSED_CHANNELS_FILE = "parsed_channels_history.json"
ACCESS_FILE = "access_grants.json"
TRIAL_FILE = "trial_usage.json"
FREE_TRIAL_LIMIT = 3  # столько парсингов новый пользователь может сделать бесплатно, без подписки

# Тарифы подписки: (месяцев, цена в рублях, скидка %, выгода в рублях относительно помесячной цены).
TARIFFS = [
    (1, 2390, 0, 0),
    (3, 6453, 10, 717),
    (6, 11472, 20, 2868),
    (12, 20076, 30, 8604),
]
CHANNEL_CACHE_FILE = "channel_subs_cache.json"
CHANNEL_CACHE_TTL_SECONDS = 24 * 3600
USER_CACHE_FILE = "user_info_cache.json"
USER_CACHE_TTL_SECONDS = 24 * 3600
COMMENT_CONCURRENCY = 10
MAX_COMMENTS_PER_POST = 3000
PROGRESS_EDIT_INTERVAL = 1.2
MAX_CONCURRENT_PARSES = 8
CALL_TIMEOUT = 60
MAX_FLOOD_WAIT = 120
MAX_CHANNELS_PER_PARSE = 30  # тот же лимит, что и в мини-аппе (miniapp_api.MAX_CHANNELS_PER_JOB)

# Рассылка найденным людям — идёт через личный аккаунт пользователя (Bot API не может
# написать первым тому, кто не писал боту). Это реальные сообщения незнакомым людям,
# поэтому: задержка между отправками (не быстрее, чем живой человек тыкает "написать"),
# жёсткий потолок получателей за один прогон, и немедленная остановка при PeerFloodError
# (Telegram явно сигналит "хватит спамить" — продолжать значит рисковать аккаунтом).
MAX_BROADCAST_RECIPIENTS = 100
BROADCAST_DELAY_MIN = 4.0
BROADCAST_DELAY_MAX = 9.0
BROADCAST_EDIT_INTERVAL = 2.0

# Папка с сессиями пользователей (каждый user_id → свой .session файл)
USER_SESSIONS_DIR = Path("user_sessions")
USER_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
# ===============================================

# Состояния диалогов
CHANNELS, POSTS, SUBS_RANGE = range(3)
CONNECT_PHONE, CONNECT_CODE, CONNECT_PASSWORD = range(3, 6)
BROADCAST_SELECT, BROADCAST_TEXT, BROADCAST_CONFIRM = range(6, 9)

telethon_clients = [
    TelegramClient(name, API_ID, API_HASH, connection_retries=10, retry_delay=3)
    for name in SESSION_NAMES
]


class ClientPool:
    """Раздаёт Telethon-аккаунты (общие сессии) по кругу — только для админских
    команд и запасного сценария. Основной парсинг использует личный аккаунт юзера."""

    def __init__(self, clients: list):
        self.clients = clients
        self._next = 0

    @property
    def size(self) -> int:
        return len(self.clients)

    def label_for(self, client) -> str:
        if not self.clients:
            return ""
        try:
            idx = self.clients.index(client)
            return f"акк.{idx + 1}" if self.size > 1 else ""
        except ValueError:
            return "личный"

    def pick(self):
        if not self.clients:
            raise RuntimeError("Нет общих Telethon-сессий")
        client = self.clients[self._next % self.size]
        self._next += 1
        return client


client_pool = ClientPool(telethon_clients)
parse_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PARSES)
active_parses = 0

db_lock = asyncio.Lock()
parsed_channels_lock = asyncio.Lock()
channel_cache_lock = asyncio.Lock()
user_cache_lock = asyncio.Lock()
access_lock = asyncio.Lock()
trial_lock = asyncio.Lock()

# Живые клиенты пользователей: user_id -> TelegramClient (уже подключённые)
_user_clients: dict[int, TelegramClient] = {}
_user_clients_lock = asyncio.Lock()


# ================== БАЗЫ / КЭШИ ==================

def load_databases():
    if os.path.exists(DATABASE_FILE):
        try:
            with open(DATABASE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_databases(data):
    with open(DATABASE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_databases_async(data):
    async with db_lock:
        snapshot = copy.deepcopy(data)
        await asyncio.to_thread(save_databases, snapshot)

user_databases = load_databases()


def load_parsed_channels():
    if os.path.exists(PARSED_CHANNELS_FILE):
        try:
            with open(PARSED_CHANNELS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_parsed_channels(data):
    with open(PARSED_CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_parsed_channels_async(data):
    async with parsed_channels_lock:
        snapshot = copy.deepcopy(data)
        await asyncio.to_thread(save_parsed_channels, snapshot)

parsed_channels_history = load_parsed_channels()


async def record_parsed_channels(user_id: int, channels: list[str]):
    key = str(user_id)
    existing = {item['channel']: item for item in parsed_channels_history.get(key, [])}
    now = time.time()
    for ch in channels:
        existing[ch] = {'channel': ch, 'last_parsed_at': now}
    parsed_channels_history[key] = list(existing.values())
    await save_parsed_channels_async(parsed_channels_history)


def load_channel_cache():
    if os.path.exists(CHANNEL_CACHE_FILE):
        try:
            with open(CHANNEL_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_channel_cache(data):
    with open(CHANNEL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_channel_cache_async(data):
    async with channel_cache_lock:
        snapshot = copy.deepcopy(data)
        await asyncio.to_thread(save_channel_cache, snapshot)

channel_subs_cache = load_channel_cache()


def load_user_cache():
    if os.path.exists(USER_CACHE_FILE):
        try:
            with open(USER_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_user_cache(data):
    with open(USER_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_user_cache_async(data):
    async with user_cache_lock:
        snapshot = copy.deepcopy(data)
        await asyncio.to_thread(save_user_cache, snapshot)

user_info_cache = load_user_cache()


def load_access():
    if os.path.exists(ACCESS_FILE):
        try:
            with open(ACCESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_access(data):
    with open(ACCESS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_access_async(data):
    async with access_lock:
        snapshot = copy.deepcopy(data)
        await asyncio.to_thread(save_access, snapshot)

access_grants = load_access()


def load_trial_usage():
    if os.path.exists(TRIAL_FILE):
        try:
            with open(TRIAL_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_trial_usage(data):
    with open(TRIAL_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_trial_usage_async(data):
    async with trial_lock:
        snapshot = copy.deepcopy(data)
        await asyncio.to_thread(save_trial_usage, snapshot)

trial_usage = load_trial_usage()


# ================== ДОСТУП / ПОДПИСКА ==================

def is_admin(user_id: int) -> bool:
    return ADMIN_USER_ID != 0 and user_id == ADMIN_USER_ID


async def is_subscribed(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if is_admin(user_id):
        return True
    try:
        member = await context.bot.get_chat_member(f"@{REQUIRED_CHANNEL_USERNAME}", user_id)
        return member.status not in ("left", "kicked")
    except Exception as e:
        print(f"⚠️ Не удалось проверить подписку на @{REQUIRED_CHANNEL_USERNAME} для {user_id}: {e}")
        return True


def subscribe_gate_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("✅ Я подписался")]], resize_keyboard=True)


def has_paid_access(user_id: int) -> bool:
    if is_admin(user_id):
        return True
    grant = access_grants.get(str(user_id))
    return grant is not None and time.time() < grant['until']


def free_trials_left(user_id: int) -> int:
    used = trial_usage.get(str(user_id), 0)
    return max(0, FREE_TRIAL_LIMIT - used)


async def consume_trial_use(user_id: int) -> int:
    key = str(user_id)
    trial_usage[key] = trial_usage.get(key, 0) + 1
    await save_trial_usage_async(trial_usage)
    return trial_usage[key]


def has_access(user_id: int) -> bool:
    return has_paid_access(user_id) or free_trials_left(user_id) > 0


def access_status_text(user_id: int) -> str:
    if is_admin(user_id):
        return "Владелец бота — доступ без ограничений"

    grant = access_grants.get(str(user_id))
    if grant is not None and time.time() < grant['until']:
        until_str = time.strftime('%d.%m.%Y', time.localtime(grant['until']))
        days = int((grant['until'] - time.time()) // 86400) + 1
        return f"Активна до {until_str} (осталось ~{days} дн.)"

    trials_left = free_trials_left(user_id)
    if trials_left > 0:
        return f"Бесплатных запросов осталось: {trials_left} из {FREE_TRIAL_LIMIT}"

    if grant is not None:
        until_str = time.strftime('%d.%m.%Y', time.localtime(grant['until']))
        return f"Подписка истекла {until_str}. Для продления — /tariffs"

    return "Бесплатные запросы закончились. Для оформления подписки — /tariffs"


# ================== СЕССИИ ПОЛЬЗОВАТЕЛЕЙ ==================

def user_session_path(user_id: int) -> str:
    return str(USER_SESSIONS_DIR / str(user_id))


def has_session_file(user_id: int) -> bool:
    """Есть ли на диске файл сессии (ещё не значит, что она авторизована)."""
    base = user_session_path(user_id)
    return os.path.exists(base + ".session")


async def is_user_account_connected(user_id: int) -> bool:
    """Проверяет, что у пользователя есть валидная авторизованная Telethon-сессия."""
    async with _user_clients_lock:
        client = _user_clients.get(user_id)
        if client is not None:
            try:
                if not client.is_connected():
                    await client.connect()
                if await client.is_user_authorized():
                    return True
            except Exception as e:
                print(f"⚠️ is_user_account_connected({user_id}) live client: {e}")
            # Сессия битая — убираем из кэша
            try:
                await client.disconnect()
            except Exception:
                pass
            _user_clients.pop(user_id, None)

    if not has_session_file(user_id):
        return False

    client = TelegramClient(
        user_session_path(user_id), API_ID, API_HASH,
        connection_retries=5, retry_delay=2,
    )
    try:
        await client.connect()
        ok = await client.is_user_authorized()
        if ok:
            async with _user_clients_lock:
                _user_clients[user_id] = client
            return True
        await client.disconnect()
        # Файл есть, но не авторизован — удаляем мусор
        _remove_session_files(user_id)
        return False
    except Exception as e:
        print(f"⚠️ is_user_account_connected({user_id}): {e}")
        try:
            await client.disconnect()
        except Exception:
            pass
        return False


def _remove_session_files(user_id: int):
    base = user_session_path(user_id)
    for suffix in (".session", ".session-journal"):
        path = base + suffix
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            print(f"⚠️ не удалось удалить {path}: {e}")


async def get_user_client(user_id: int) -> TelegramClient | None:
    """Возвращает подключённый авторизованный клиент пользователя или None."""
    if not await is_user_account_connected(user_id):
        return None
    async with _user_clients_lock:
        return _user_clients.get(user_id)


async def disconnect_user_client(user_id: int):
    async with _user_clients_lock:
        client = _user_clients.pop(user_id, None)
    if client is not None:
        try:
            await client.disconnect()
        except Exception:
            pass


async def create_login_client(user_id: int) -> TelegramClient:
    """Создаёт (или пересоздаёт) клиент для процесса входа. Старую сессию сбрасываем."""
    await disconnect_user_client(user_id)
    _remove_session_files(user_id)
    client = TelegramClient(
        user_session_path(user_id), API_ID, API_HASH,
        connection_retries=5, retry_delay=2,
    )
    await client.connect()
    return client


# ================== УТИЛИТЫ TELETHON ==================

async def resolve_user(identifier: str) -> tuple[int | None, str | None, str | None]:
    identifier = identifier.strip().lstrip('@')
    if identifier.isdigit():
        return int(identifier), identifier, None
    if not telethon_clients:
        return None, None, "Нет доступных аккаунтов для поиска по username. Укажи числовой user_id."

    last_error = None
    for idx, client in enumerate(telethon_clients):
        await ensure_connected(client, client_pool.label_for(client) or f"акк.{idx + 1}")
        try:
            entity = await with_timeout(client.get_entity(identifier), f"resolve @{identifier}")
        except UsernameNotOccupiedError:
            return None, None, f"Пользователь @{identifier} не найден."
        except Exception as e:
            last_error = e
            continue

        if not isinstance(entity, User):
            return None, None, f"@{identifier} — это не пользователь (канал или группа?)."
        name = f"{entity.first_name or ''} {entity.last_name or ''}".strip() or f"@{identifier}"
        return entity.id, name, None

    return None, None, f"Не удалось найти @{identifier}: {last_error}"


async def with_timeout(coro, label: str, seconds: float = CALL_TIMEOUT):
    try:
        return await asyncio.wait_for(coro, timeout=seconds)
    except asyncio.TimeoutError:
        print(f"⚠️ TIMEOUT ({seconds}s): {label}")
        raise


async def ensure_connected(client: TelegramClient, label: str = ""):
    if not client.is_connected():
        print(f"⚠️ Клиент {label} отключён, переподключаюсь...")
        try:
            await with_timeout(client.connect(), f"reconnect({label})")
        except Exception as e:
            print(f"❌ Не удалось переподключить клиента {label}: {e}")


async def sleep_flood_wait(seconds: int, label: str) -> bool:
    if seconds > MAX_FLOOD_WAIT:
        print(f"⚠️ FloodWait {seconds}s слишком долгий, пропускаю: {label}")
        return False
    print(f"⏳ FloodWait {seconds}s: {label}")
    await asyncio.sleep(seconds + 1)
    return True


def extract_channel_links(text: str) -> list[str]:
    if not text:
        return []
    patterns = [
        r'(?:https?://)?(?:t\.me|telegram\.me)/(?:s/)?@?([a-zA-Z0-9_]{4,32})',
        r'@([a-zA-Z0-9_]{4,32})',
    ]
    found = set()
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            username = match.group(1).lower()
            if username not in ('joinchat', 'addstickers', 'share', 'proxy', 'iv', 's', 'boost'):
                found.add(username)
    return list(found)


client_subs_flood_until: dict[int, float] = {}
client_broken_until: dict[int, float] = {}
CLIENT_BROKEN_COOLDOWN = 45


async def get_channel_subscribers(username: str, client: TelegramClient) -> int | None:
    flood_until = client_subs_flood_until.get(id(client))
    if flood_until and time.time() < flood_until:
        return None
    try:
        entity = await with_timeout(client.get_entity(username), f"get_entity({username})")
        if isinstance(entity, Channel) and entity.broadcast:
            full = await with_timeout(client(GetFullChannelRequest(entity)), f"GetFullChannel({username})")
            return full.full_chat.participants_count
    except (UsernameNotOccupiedError, ChannelPrivateError, ValueError, TypeError):
        return None
    except FloodWaitError as e:
        if e.seconds > MAX_FLOOD_WAIT:
            client_subs_flood_until[id(client)] = time.time() + e.seconds
        if await sleep_flood_wait(e.seconds, f"get_channel_subscribers({username})"):
            return await get_channel_subscribers(username, client)
        return None
    except Exception as e:
        print(f"⚠️ get_channel_subscribers({username}) на {client_pool.label_for(client)}: {type(e).__name__}: {e}")
        client_broken_until[id(client)] = time.time() + CLIENT_BROKEN_COOLDOWN
        return None
    return None


async def get_entity_with_retry(entity_ref, client: TelegramClient):
    try:
        return await with_timeout(client.get_entity(entity_ref), f"get_entity({entity_ref})")
    except FloodWaitError as e:
        if await sleep_flood_wait(e.seconds, f"get_entity_with_retry({entity_ref})"):
            return await with_timeout(client.get_entity(entity_ref), f"get_entity({entity_ref}) retry")
        raise
    except ValueError:
        raise
    except Exception as e:
        print(f"⚠️ get_entity_with_retry({entity_ref}) на {client_pool.label_for(client)}: {type(e).__name__}: {e}")
        client_broken_until[id(client)] = time.time() + CLIENT_BROKEN_COOLDOWN
        raise


async def fetch_all_replies(entity, msg_id: int, client: TelegramClient, limit: int = 500):
    comments = []
    offset_id = 0
    while len(comments) < limit:
        page_size = min(100, limit - len(comments))
        try:
            result = await with_timeout(
                client(GetRepliesRequest(
                    peer=entity,
                    msg_id=msg_id,
                    offset_id=offset_id,
                    offset_date=None,
                    add_offset=0,
                    limit=page_size,
                    max_id=0,
                    min_id=0,
                    hash=0
                )),
                f"GetReplies(msg={msg_id}, offset={offset_id})",
            )
        except FloodWaitError as e:
            if await sleep_flood_wait(e.seconds, f"fetch_all_replies(msg={msg_id})"):
                continue
            break
        if not result.messages:
            break
        comments.extend(result.messages)
        offset_id = result.messages[-1].id
        if len(result.messages) < page_size:
            break
    return comments


async def get_user_bio_and_channels(user, client: TelegramClient) -> tuple[str, list[str]]:
    cache_key = str(user.id)
    cached = user_info_cache.get(cache_key)
    if cached is not None and (time.time() - cached['ts']) < USER_CACHE_TTL_SECONDS:
        return cached['bio'], cached['channels']

    bio = getattr(user, 'about', None) or ""
    personal_channel_username = None
    extra_text = ""

    try:
        full = await with_timeout(client(GetFullUserRequest(user)), f"GetFullUser({user.id})")
        bio = full.full_user.about or ""

        business_intro = getattr(full.full_user, 'business_intro', None)
        if business_intro:
            extra_text = f"{getattr(business_intro, 'title', '') or ''} {getattr(business_intro, 'description', '') or ''}"

        personal_channel_id = getattr(full.full_user, 'personal_channel_id', None)
        if personal_channel_id:
            personal_channel = next(
                (c for c in full.chats if c.id == personal_channel_id),
                None
            )
            if personal_channel is not None and getattr(personal_channel, 'username', None):
                personal_channel_username = personal_channel.username.lower()
    except FloodWaitError as e:
        if await sleep_flood_wait(e.seconds, f"get_user_bio_and_channels({user.id})"):
            return await get_user_bio_and_channels(user, client)
    except Exception as e:
        print(f"⚠️ get_user_bio_and_channels({user.id}) на {client_pool.label_for(client)}: {type(e).__name__}: {e}")
        client_broken_until[id(client)] = time.time() + CLIENT_BROKEN_COOLDOWN

    channels = []
    if personal_channel_username:
        channels.append(personal_channel_username)
    for ch in extract_channel_links(f"{bio} {extra_text}"):
        if ch not in channels:
            channels.append(ch)

    user_info_cache[cache_key] = {'bio': bio, 'channels': channels, 'ts': time.time()}
    return bio, channels


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}ч {m}м"
    if m:
        return f"{m}м {s}с"
    return f"{s}с"


class ProgressTracker:
    def __init__(
        self, status_msg, total_posts: int, channels_total: int, account_label: str = "",
        on_update: "callable | None" = None,
    ):
        self.status_msg = status_msg
        self.total_posts = max(total_posts, 1)
        self.channels_total = channels_total
        self.account_label = f" [{account_label}]" if account_label else ""
        self.on_update = on_update
        self.posts_done = 0
        self.channels_done = 0
        self.channel_found_counts = {}
        self.start_time = time.monotonic()
        self.last_edit_time = 0.0
        self.last_text = None

    async def _edit(self, text: str, force: bool = False):
        if self.status_msg is None:
            return
        now = time.monotonic()
        if not force and (now - self.last_edit_time) < PROGRESS_EDIT_INTERVAL:
            return
        if text == self.last_text:
            return
        self.last_edit_time = now
        self.last_text = text
        try:
            await asyncio.wait_for(self.status_msg.edit_text(text, parse_mode="HTML"), timeout=15)
        except Exception as e:
            print(f"⚠️ ProgressTracker._edit failed ({type(e).__name__}): {e}")

    def _notify(self, *, status: str, percent: int, found: int, elapsed: float, eta_text: str = ""):
        if self.on_update is None:
            return
        try:
            self.on_update({
                'status': status,
                'percent': percent,
                'posts_done': self.posts_done,
                'total_posts': self.total_posts,
                'channels_done': self.channels_done,
                'channels_total': self.channels_total,
                'found': found,
                'elapsed': elapsed,
                'eta_text': eta_text,
            })
        except Exception as e:
            print(f"⚠️ ProgressTracker._notify failed ({type(e).__name__}): {e}")

    async def channel_started(self, channel: str, channel_idx: int):
        if self.channels_total > 1:
            return
        text = f"⚡️ <b>Парсинг</b>{self.account_label} — @{channel}\nЗагружаю посты и комментарии..."
        await self._edit(text, force=True)

    async def channel_done(self):
        self.channels_done += 1

    def _bar(self, fraction: float) -> str:
        bar_len = 10
        filled = int(bar_len * fraction)
        return "█" * filled + "░" * (bar_len - filled)

    async def post_done(self, channel: str, channel_idx: int, posts_in_channel: int, found_count: int):
        self.posts_done += 1
        self.channel_found_counts[channel_idx] = found_count
        total_found = sum(self.channel_found_counts.values())

        now = time.monotonic()
        elapsed = now - self.start_time
        fraction = min(self.posts_done / self.total_posts, 1.0)
        percent = int(fraction * 100)

        eta_text = "считаю…"
        if fraction > 0.03:
            eta_seconds = elapsed / fraction - elapsed
            eta_text = f"~{format_duration(eta_seconds)}"

        bar = self._bar(fraction)

        if self.channels_total == 1:
            text = (
                f"⚡️ <b>Парсинг</b>{self.account_label} — @{channel}\n\n"
                f"<b>{percent}%</b>  <code>[{bar}]</code>\n"
                f"Пост {self.posts_done} из {posts_in_channel}\n\n"
                f"⏱ {format_duration(elapsed)} · осталось {eta_text}\n"
                f"✨ Найдено: <b>{total_found}</b>"
            )
        else:
            text = (
                f"⚡️ <b>Парсинг {self.channels_total} каналов</b>{self.account_label}\n\n"
                f"<b>{percent}%</b>  <code>[{bar}]</code>\n"
                f"Готово каналов: {self.channels_done} из {self.channels_total}\n\n"
                f"⏱ {format_duration(elapsed)} · осталось {eta_text}\n"
                f"✨ Найдено: <b>{total_found}</b>"
            )
        force = self.posts_done >= self.total_posts
        await self._edit(text, force=force)
        self._notify(status='running', percent=percent, found=total_found, elapsed=elapsed, eta_text=eta_text)

    async def finish(self, found_count: int):
        elapsed = time.monotonic() - self.start_time
        text = (
            f"✅ <b>Готово за {format_duration(elapsed)}</b>\n"
            f"✨ Найдено: <b>{found_count}</b>"
        )
        await self._edit(text, force=True)
        self._notify(status='done', percent=100, found=found_count, elapsed=elapsed)


async def parse_channel(
    channel_link: str, posts_limit: int, min_subs: int, max_subs: int,
    *, client: TelegramClient, seen_users: set, tracker: "ProgressTracker",
    channel_idx: int, channels_total: int, on_new_results: "callable | None" = None,
):
    results = []
    filtered_out = []
    total_comments = 0
    users_with_channels = 0
    channels_found = 0
    saved_up_to = 0
    switch_count = 0
    channel_seen_this_attempt = set()
    sem = asyncio.Semaphore(COMMENT_CONCURRENCY)

    print(f"▶ parse_channel start: @{channel_link} (постов запрошено: {posts_limit})")

    entity = None
    last_error: object = "не удалось открыть канал"
    # Для личного аккаунта пул может быть пуст — пробуем только переданный client
    candidates = [client]
    if client_pool.clients:
        candidates = [client] + [c for c in client_pool.clients if c is not client]

    for candidate in candidates:
        flood_until = client_subs_flood_until.get(id(candidate))
        if flood_until and time.time() < flood_until:
            continue
        try:
            await ensure_connected(candidate, client_pool.label_for(candidate))
            entity = await with_timeout(candidate.get_entity(channel_link), f"get_entity(@{channel_link})")
            client = candidate
            break
        except (UsernameNotOccupiedError, ChannelPrivateError, ValueError) as e:
            last_error = e
            break
        except FloodWaitError as e:
            last_error = e
            if e.seconds > MAX_FLOOD_WAIT:
                client_subs_flood_until[id(candidate)] = time.time() + e.seconds
            continue
        except Exception as e:
            last_error = e
            continue

    if entity is None:
        print(f"❌ @{channel_link}: не удалось открыть канал: {last_error}")
        return [], [], f"❌ Не удалось открыть канал: {last_error}"

    await tracker.channel_started(channel_link, channel_idx)

    try:
        messages = await with_timeout(
            client.get_messages(entity, limit=posts_limit), f"get_messages(@{channel_link})"
        )
    except Exception as e:
        print(f"❌ @{channel_link}: не удалось получить посты: {e}")
        return [], [], f"❌ Не удалось получить посты: {e}"
    posts_in_channel = len(messages)
    print(f"  @{channel_link}: получено {posts_in_channel} постов")

    async def ensure_healthy_client():
        nonlocal client, switch_count, entity
        if time.time() >= client_broken_until.get(id(client), 0):
            return
        if not client_pool.clients:
            return
        for candidate in client_pool.clients:
            if candidate is client or time.time() < client_broken_until.get(id(candidate), 0):
                continue
            await ensure_connected(candidate, client_pool.label_for(candidate))
            try:
                new_entity = await with_timeout(
                    candidate.get_entity(channel_link), f"get_entity(@{channel_link}) после переключения"
                )
            except Exception as e:
                print(f"⚠️ @{channel_link}: {client_pool.label_for(candidate)} тоже не смог переоткрыть канал: {type(e).__name__}: {e}")
                client_broken_until[id(candidate)] = time.time() + CLIENT_BROKEN_COOLDOWN
                continue
            print(f"⚠️ @{channel_link}: переключаюсь на другой аккаунт (проблемы с сессией у {client_pool.label_for(client)})")
            client = candidate
            entity = new_entity
            switch_count += 1
            return

    async def get_cached_subs(username: str):
        cached = channel_subs_cache.get(username)
        if cached is not None and (time.time() - cached['ts']) < CHANNEL_CACHE_TTL_SECONDS:
            return cached['subs']
        await ensure_healthy_client()
        subs = await get_channel_subscribers(username, client)
        channel_subs_cache[username] = {'subs': subs, 'ts': time.time()}
        return subs

    async def process_comment(comment):
        nonlocal total_comments, users_with_channels, channels_found

        sender_id = getattr(comment, 'sender_id', None) or getattr(comment, 'from_id', None)
        if not sender_id:
            return
        if hasattr(sender_id, 'user_id'):
            sender_id = sender_id.user_id

        if sender_id in seen_users:
            return
        seen_users.add(sender_id)
        channel_seen_this_attempt.add(sender_id)

        async with sem:
            total_comments += 1

            try:
                user = await get_entity_with_retry(sender_id, client)
            except Exception:
                return

            if not isinstance(user, User) or user.bot or getattr(user, 'deleted', False):
                return

            bio, candidate_channels = await get_user_bio_and_channels(user, client)
            if not candidate_channels:
                return

            users_with_channels += 1

            for ch_username in candidate_channels:
                channels_found += 1
                subs = await get_cached_subs(ch_username)

                if subs is None:
                    continue

                if min_subs <= subs <= max_subs:
                    results.append({
                        'user_id': user.id,
                        'username': user.username,
                        'first_name': user.first_name or "",
                        'last_name': user.last_name or "",
                        'bio': bio[:300],
                        'channel': ch_username,
                        'subscribers': subs,
                    })
                else:
                    filtered_out.append({'channel': ch_username, 'subscribers': subs})

    async def collect_via_iter(msg_id: int):
        result = []
        async for comment in client.iter_messages(entity, reply_to=msg_id, limit=MAX_COMMENTS_PER_POST):
            result.append(comment)
        return result

    async def process_all_posts(report_progress: bool):
        nonlocal saved_up_to
        for msg in messages:
            comments = []
            await ensure_healthy_client()

            try:
                comments = await with_timeout(
                    collect_via_iter(msg.id), f"iter_messages(msg={msg.id})", seconds=CALL_TIMEOUT * 10,
                )
            except FloodWaitError as e:
                await sleep_flood_wait(e.seconds, f"iter_messages(msg={msg.id})")
            except MsgIdInvalidError:
                pass
            except Exception as e:
                print(f"⚠️ iter_messages(msg={msg.id}) на {client_pool.label_for(client)}: {type(e).__name__}: {e}")
                client_broken_until[id(client)] = time.time() + CLIENT_BROKEN_COOLDOWN

            if not comments and getattr(msg, 'replies', None) and msg.replies.replies > 0:
                try:
                    comments = await with_timeout(
                        fetch_all_replies(entity, msg.id, client, limit=MAX_COMMENTS_PER_POST),
                        f"fetch_all_replies(msg={msg.id})", seconds=CALL_TIMEOUT * 10,
                    )
                except MsgIdInvalidError:
                    pass
                except Exception as e:
                    print(f"⚠️ fetch_all_replies(msg={msg.id}) на {client_pool.label_for(client)}: {type(e).__name__}: {e}")
                    client_broken_until[id(client)] = time.time() + CLIENT_BROKEN_COOLDOWN

            if comments:
                try:
                    await with_timeout(
                        asyncio.gather(*(process_comment(c) for c in comments)),
                        f"process_comment batch (msg={msg.id}, n={len(comments)})",
                        seconds=CALL_TIMEOUT * 10,
                    )
                except Exception as e:
                    print(f"⚠️ @{channel_link} msg={msg.id}: обработка комментариев прервана: {e}")

            if report_progress:
                await tracker.post_done(channel_link, channel_idx, posts_in_channel, len(results))

            if on_new_results is not None and len(results) > saved_up_to:
                await on_new_results(results[saved_up_to:])
                saved_up_to = len(results)

    await process_all_posts(report_progress=True)

    if not results and switch_count >= 3:
        seen_users -= channel_seen_this_attempt
        channel_seen_this_attempt.clear()
        soonest_free = min(
            (client_broken_until.get(id(c), 0) for c in client_pool.clients), default=0
        ) if client_pool.clients else 0
        delay = max(0.0, min(soonest_free - time.time(), CLIENT_BROKEN_COOLDOWN))
        print(f"⚠️ @{channel_link}: {switch_count} переключений аккаунта и 0 найдено — "
              f"жду {delay:.0f}с и пробую собрать комментарии ещё раз")
        if delay > 0:
            await asyncio.sleep(delay)
        await process_all_posts(report_progress=False)

    info = (f"Комментариев: {total_comments} | "
            f"С каналами (био/профиль): {users_with_channels} | "
            f"Каналов найдено: {channels_found} | "
            f"Подошло: {len(results)}")

    return results, filtered_out, info


# ================== КЛАВИАТУРЫ ==================

def main_menu_keyboard():
    keyboard = [
        [KeyboardButton("🔍 Новый парсинг")],
        [KeyboardButton("📁 Моя база каналов"), KeyboardButton("👤 Аккаунт")],
        [KeyboardButton("💎 Тарифы"), KeyboardButton("🛠 Поддержка")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def cancel_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("❌ Отмена")]], resize_keyboard=True)


def connect_phone_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📱 Отправить мой номер", request_contact=True)],
            [KeyboardButton("❌ Отмена")],
        ],
        resize_keyboard=True,
    )


def code_keyboard():
    """Цифровая клавиатура 0–9 + стереть + отмена."""
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("1"), KeyboardButton("2"), KeyboardButton("3")],
            [KeyboardButton("4"), KeyboardButton("5"), KeyboardButton("6")],
            [KeyboardButton("7"), KeyboardButton("8"), KeyboardButton("9")],
            [KeyboardButton("⌫"), KeyboardButton("0"), KeyboardButton("❌ Отмена")],
        ],
        resize_keyboard=True,
    )


def database_submenu_keyboard():
    keyboard = [
        [KeyboardButton("📡 Спарсенные каналы"), KeyboardButton("✨ Найденные каналы")],
        [KeyboardButton("◀️ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def database_list_keyboard(which: str = 'found'):
    keyboard = [[KeyboardButton("🔀 Сортировка"), KeyboardButton("🗑 Очистить базу")]]
    if which == 'found':
        keyboard.append([KeyboardButton("📣 Рассылка")])
    keyboard.append([KeyboardButton("◀️ Назад")])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def sort_keyboard():
    keyboard = [
        [KeyboardButton("🆕 Сначала новые"), KeyboardButton("📜 Сначала старые")],
        [KeyboardButton("◀️ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def confirm_clear_keyboard():
    keyboard = [
        [KeyboardButton("✅ Да, очистить")],
        [KeyboardButton("◀️ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


# ================== UI КОДА ВХОДА ==================

def _code_slots_html(digits: list[str], length: int = 5) -> str:
    """Слоты под цифры кода (заполненные + пустые ⬜). Длина кода у Telegram не
    фиксирована — обычно 5 цифр, но бывает и иначе (см. sent.type.length)."""
    slots = []
    for i in range(length):
        if i < len(digits):
            slots.append(f"<b>{html.escape(digits[i])}</b>")
        else:
            slots.append("⬜")
    return "  ".join(slots)


def code_prompt_text(digits: list[str], length: int = 5) -> str:
    return (
        "🔐 <b>Введите код из Telegram</b>\n\n"
        f"{_code_slots_html(digits, length)}\n\n"
        "Код пришёл в приложение Telegram или по SMS.\n"
        "Нажимайте цифры на клавиатуре ниже."
    )


# ================== ОБРАБОТЧИКИ МЕНЮ ==================

async def replace_last_message(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str, text: str, **kwargs):
    chat_id = update.effective_chat.id

    prev_id = context.user_data.get(key)
    if prev_id is not None:
        try:
            await context.bot.delete_message(chat_id, prev_id)
        except Exception:
            pass

    try:
        await update.message.delete()
    except Exception:
        pass

    msg = await context.bot.send_message(chat_id, text, **kwargs)
    context.user_data[key] = msg.message_id
    return msg


async def parsing_step_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    return await replace_last_message(update, context, 'parsing_step_msg_id', text, **kwargs)


async def after_account_ready(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """После успешного подключения аккаунта — проверка подписки, затем главное меню."""
    user_id = update.effective_user.id
    if not await is_subscribed(user_id, context):
        await context.bot.send_message(
            update.effective_chat.id,
            "🔒 Чтобы пользоваться ботом, подпишись на канал:\n"
            f"{REQUIRED_CHANNEL_LINK}\n\n"
            "После подписки нажми «✅ Я подписался».",
            reply_markup=subscribe_gate_keyboard(),
        )
        return ConversationHandler.END

    await context.bot.send_message(
        update.effective_chat.id,
        "👋 Готово! Аккаунт подключён.\n\n"
        "Выбери, что хочешь сделать 👇",
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # 1) Приветствие
    await replace_last_message(
        update, context, 'start_msg_id',
        "👋 Привет!\n\n"
        "Я нахожу людей, которые комментируют посты в Telegram-каналах, "
        "и каналы, которые они указали у себя в профиле.",
        reply_markup=ReplyKeyboardRemove(),
    )

    # 2) Подключение аккаунта (ДО проверки подписки)
    if not await is_user_account_connected(user_id):
        await context.bot.send_message(
            chat_id,
            "🔌 <b>Для работы подключите аккаунт</b>\n\n"
            "Парсинг выполняется через ваш Telegram-аккаунт.\n"
            "Отправьте номер телефона в международном формате "
            "(например <code>+79001234567</code>) "
            "или нажмите кнопку ниже.",
            parse_mode="HTML",
            reply_markup=connect_phone_keyboard(),
        )
        return CONNECT_PHONE

    # 3) Обязательная подписка
    if not await is_subscribed(user_id, context):
        await context.bot.send_message(
            chat_id,
            "🔒 Чтобы пользоваться ботом, подпишись на канал:\n"
            f"{REQUIRED_CHANNEL_LINK}\n\n"
            "После подписки нажми «✅ Я подписался».",
            reply_markup=subscribe_gate_keyboard(),
        )
        return ConversationHandler.END

    # 4) Главное меню
    await context.bot.send_message(
        chat_id,
        "Выбери, что хочешь сделать 👇",
        reply_markup=main_menu_keyboard(),
    )
    return ConversationHandler.END


async def connect_get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()

    if text == "❌ Отмена":
        await replace_last_message(
            update, context, 'start_msg_id',
            "Подключение отменено. Чтобы начать — /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    # Номер из контакта или текстом
    phone = None
    if update.message.contact and update.message.contact.phone_number:
        phone = update.message.contact.phone_number
        # Контакт от Telegram иногда без «+»
        if not phone.startswith("+"):
            phone = "+" + phone
    else:
        # Нормализуем ввод
        cleaned = re.sub(r"[\s\-()]", "", text)
        if cleaned.startswith("00"):
            cleaned = "+" + cleaned[2:]
        if re.fullmatch(r"\+?\d{10,15}", cleaned):
            phone = cleaned if cleaned.startswith("+") else "+" + cleaned

    if not phone:
        await update.message.reply_text(
            "Не распознал номер. Пришли в формате <code>+79001234567</code> "
            "или нажми «📱 Отправить мой номер».",
            parse_mode="HTML",
            reply_markup=connect_phone_keyboard(),
        )
        return CONNECT_PHONE

    # Удаляем сообщение пользователя с номером (приватность)
    try:
        await update.message.delete()
    except Exception:
        pass

    status = await context.bot.send_message(chat_id, "⏳ Отправляю код…")

    try:
        client = await create_login_client(user_id)
        sent = await client.send_code_request(phone)
        # Длина кода у Telegram не всегда 5 — зависит от способа доставки, сервер
        # присылает её в sent.type.length. Если атрибута нет (редкие типы вроде
        # звонка-паттерна) — берём стандартные 5.
        code_length = getattr(sent.type, 'length', None) or 5
        context.user_data['login_phone'] = phone
        context.user_data['login_phone_code_hash'] = sent.phone_code_hash
        context.user_data['login_client'] = client  # держим до sign_in
        context.user_data['code_digits'] = []
        context.user_data['code_length'] = code_length
    except PhoneNumberInvalidError:
        await status.edit_text("❌ Неверный номер телефона. Попробуй ещё раз.")
        await context.bot.send_message(
            chat_id,
            "Отправьте номер в международном формате, например <code>+79001234567</code>.",
            parse_mode="HTML",
            reply_markup=connect_phone_keyboard(),
        )
        return CONNECT_PHONE
    except FloodWaitError as e:
        await status.edit_text(f"⏳ Telegram просит подождать {e.seconds} сек. Попробуй позже.")
        await context.bot.send_message(chat_id, "Нажми /start, когда сможешь.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    except Exception as e:
        print(f"❌ send_code_request: {type(e).__name__}: {e}")
        await status.edit_text(f"❌ Не удалось отправить код: {e}")
        await context.bot.send_message(
            chat_id,
            "Попробуй другой номер или /start позже.",
            reply_markup=connect_phone_keyboard(),
        )
        return CONNECT_PHONE

    try:
        await status.delete()
    except Exception:
        pass

    code_msg = await context.bot.send_message(
        chat_id,
        code_prompt_text([], code_length),
        parse_mode="HTML",
        reply_markup=code_keyboard(),
    )
    context.user_data['code_msg_id'] = code_msg.message_id
    return CONNECT_CODE


async def connect_get_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()

    # Удаляем нажатие пользователя — остаётся только «висящее» сообщение с кодом
    try:
        await update.message.delete()
    except Exception:
        pass

    if text == "❌ Отмена":
        await _cleanup_login(context, user_id)
        code_msg_id = context.user_data.pop('code_msg_id', None)
        if code_msg_id:
            try:
                await context.bot.delete_message(chat_id, code_msg_id)
            except Exception:
                pass
        await context.bot.send_message(
            chat_id,
            "Подключение отменено. Чтобы начать — /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    digits: list[str] = context.user_data.setdefault('code_digits', [])
    code_length = context.user_data.get('code_length', 5)

    if text == "⌫":
        if digits:
            digits.pop()
        await _update_code_message(context, chat_id)
        return CONNECT_CODE

    if text not in "0123456789" or len(text) != 1:
        # Игнорируем мусор, просто обновляем подсказку
        await _update_code_message(context, chat_id)
        return CONNECT_CODE

    if len(digits) >= code_length:
        return CONNECT_CODE

    digits.append(text)
    await _update_code_message(context, chat_id)

    if len(digits) < code_length:
        return CONNECT_CODE

    # набрали нужное число цифр — пробуем войти
    code = "".join(digits)
    phone = context.user_data.get('login_phone')
    phone_code_hash = context.user_data.get('login_phone_code_hash')
    client: TelegramClient | None = context.user_data.get('login_client')

    if not client or not phone or not phone_code_hash:
        await context.bot.send_message(chat_id, "❌ Сессия входа потеряна. Начни заново — /start")
        await _cleanup_login(context, user_id)
        return ConversationHandler.END

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
    except SessionPasswordNeededError:
        # 2FA
        code_msg_id = context.user_data.pop('code_msg_id', None)
        if code_msg_id:
            try:
                await context.bot.edit_message_text(
                    "🔐 На аккаунте включена двухфакторная защита.\n"
                    "Отправь пароль облачного пароля (2FA) текстом.",
                    chat_id=chat_id,
                    message_id=code_msg_id,
                )
            except Exception:
                await context.bot.send_message(
                    chat_id,
                    "🔐 На аккаунте включена двухфакторная защита.\n"
                    "Отправь пароль облачного пароля (2FA) текстом.",
                )
        await context.bot.send_message(
            chat_id,
            "Введи пароль 2FA:",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Отмена")]], resize_keyboard=True),
        )
        return CONNECT_PASSWORD
    except PhoneCodeInvalidError:
        context.user_data['code_digits'] = []
        await _update_code_message(context, chat_id, extra="\n\n❌ Неверный код. Введи ещё раз.")
        return CONNECT_CODE
    except PhoneCodeExpiredError:
        await _cleanup_login(context, user_id)
        code_msg_id = context.user_data.pop('code_msg_id', None)
        if code_msg_id:
            try:
                await context.bot.delete_message(chat_id, code_msg_id)
            except Exception:
                pass
        await context.bot.send_message(
            chat_id,
            "❌ Код устарел. Начни заново — /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END
    except FloodWaitError as e:
        await _cleanup_login(context, user_id)
        await context.bot.send_message(
            chat_id,
            f"⏳ Telegram просит подождать {e.seconds} сек. Попробуй позже — /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END
    except Exception as e:
        print(f"❌ sign_in code: {type(e).__name__}: {e}")
        await _cleanup_login(context, user_id)
        await context.bot.send_message(
            chat_id,
            f"❌ Ошибка входа: {e}\nПопробуй /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    # Успешный вход
    return await _finish_login(update, context, client, user_id)


async def connect_get_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()

    try:
        await update.message.delete()
    except Exception:
        pass

    if text == "❌ Отмена":
        await _cleanup_login(context, user_id)
        await context.bot.send_message(
            chat_id,
            "Подключение отменено. Чтобы начать — /start",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    client: TelegramClient | None = context.user_data.get('login_client')
    if not client:
        await context.bot.send_message(chat_id, "❌ Сессия входа потеряна. /start")
        return ConversationHandler.END

    try:
        await client.sign_in(password=text)
    except PasswordHashInvalidError:
        await context.bot.send_message(
            chat_id,
            "❌ Неверный пароль 2FA. Попробуй ещё раз или ❌ Отмена.",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Отмена")]], resize_keyboard=True),
        )
        return CONNECT_PASSWORD
    except Exception as e:
        print(f"❌ sign_in password: {type(e).__name__}: {e}")
        await _cleanup_login(context, user_id)
        await context.bot.send_message(
            chat_id,
            f"❌ Ошибка: {e}\n/start",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    return await _finish_login(update, context, client, user_id)


async def _update_code_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, extra: str = ""):
    code_msg_id = context.user_data.get('code_msg_id')
    digits = context.user_data.get('code_digits', [])
    code_length = context.user_data.get('code_length', 5)
    if not code_msg_id:
        return
    text = code_prompt_text(digits, code_length) + extra
    try:
        await context.bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=code_msg_id,
            parse_mode="HTML",
        )
    except Exception as e:
        # Иногда Telegram ругается на «message is not modified»
        if "not modified" not in str(e).lower():
            print(f"⚠️ _update_code_message: {e}")


async def _cleanup_login(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    client = context.user_data.pop('login_client', None)
    if client is not None:
        try:
            await client.disconnect()
        except Exception:
            pass
    context.user_data.pop('login_phone', None)
    context.user_data.pop('login_phone_code_hash', None)
    context.user_data.pop('code_digits', None)
    context.user_data.pop('code_msg_id', None)
    context.user_data.pop('code_length', None)
    # Неавторизованную сессию удаляем
    if not await is_user_account_connected(user_id):
        _remove_session_files(user_id)


async def _finish_login(update: Update, context: ContextTypes.DEFAULT_TYPE, client: TelegramClient, user_id: int):
    chat_id = update.effective_chat.id

    # Сохраняем клиент в кэш
    async with _user_clients_lock:
        _user_clients[user_id] = client

    me = await client.get_me()
    name = f"{me.first_name or ''} {me.last_name or ''}".strip() or "аккаунт"
    uname = f"@{me.username}" if me.username else f"id{me.id}"

    # Чистим «висящее» сообщение с кодом
    code_msg_id = context.user_data.pop('code_msg_id', None)
    if code_msg_id:
        try:
            await context.bot.delete_message(chat_id, code_msg_id)
        except Exception:
            pass

    context.user_data.pop('login_phone', None)
    context.user_data.pop('login_phone_code_hash', None)
    context.user_data.pop('code_digits', None)
    context.user_data.pop('code_length', None)
    context.user_data.pop('login_client', None)

    await context.bot.send_message(
        chat_id,
        f"✅ Аккаунт подключён: <b>{html.escape(name)}</b> ({html.escape(uname)})",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )

    # Дальше — подписка / меню
    return await after_account_ready(update, context)


async def new_parsing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await is_user_account_connected(user_id):
        await parsing_step_reply(
            update, context,
            "🔌 Сначала подключите аккаунт.\nНажми /start",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END

    if not await is_subscribed(user_id, context):
        await parsing_step_reply(
            update, context,
            "🔒 Чтобы пользоваться ботом, подпишись на канал:\n"
            f"{REQUIRED_CHANNEL_LINK}\n\n"
            "После подписки нажми «✅ Я подписался».",
            reply_markup=subscribe_gate_keyboard(),
        )
        return ConversationHandler.END

    if not has_access(user_id):
        await parsing_step_reply(
            update, context,
            "🔒 <b>Бесплатные запросы закончились.</b>\n\n"
            "Посмотри варианты и оформи доступ — «💎 Тарифы» или /tariffs.",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END

    await parsing_step_reply(
        update, context,
        "🔍 <b>Новый парсинг</b>\n\n"
        "Пришли ссылку(и) на канал(ы).\n"
        "Можно несколько через пробел или с новой строки.\n\n"
        "Пример:\n<code>https://t.me/durov</code>\n<code>@channelname</code>",
        parse_mode="HTML",
        reply_markup=cancel_keyboard(),
    )
    return CHANNELS


def _chunk_parts(parts: list[str], limit: int = 4000, sep: str = "") -> list[str]:
    """Группирует уже отформатированные HTML-блоки в сообщения ≤limit символов
    (через sep), никогда не разрезая блок посередине. Наивная нарезка по фиксированной
    длине рвёт HTML-теги (<b>...</b>) — Telegram в ответ шлёт 'Can't parse entities:
    can't find end tag' / 'unclosed start tag', и апдейт падает необработанным."""
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for part in parts:
        extra = len(part) + (len(sep) if current else 0)
        if current and current_len + extra > limit:
            chunks.append(sep.join(current))
            current, current_len, extra = [], 0, len(part)
        current.append(part)
        current_len += extra
    if current:
        chunks.append(sep.join(current))
    return chunks


def render_parsed_list(user_id_str: str, sort_order: str):
    data = parsed_channels_history.get(user_id_str, [])
    if not data:
        return None
    data = sorted(data, key=lambda x: x.get('last_parsed_at', 0), reverse=(sort_order != 'old'))
    header = f"📡 <b>Спарсенные каналы</b> ({len(data)} шт.):\n\n"
    entries = []
    for i, item in enumerate(data, 1):
        when = time.strftime('%d.%m.%Y', time.localtime(item.get('last_parsed_at', 0)))
        entries.append(f"{i}. https://t.me/{html.escape(item['channel'])} — последний раз {when}\n")
    return header, entries


def render_found_list(user_id_str: str, sort_order: str):
    data = user_databases.get(user_id_str, [])
    if not data:
        return None
    data = sorted(data, key=lambda x: x.get('found_at', 0), reverse=(sort_order != 'old'))
    header = f"✨ <b>Найденные каналы</b> ({len(data)} шт.):\n\n"
    entries = []
    for i, item in enumerate(data, 1):
        name = html.escape(f"{item.get('first_name', '')} {item.get('last_name', '')}".strip()) or "Без имени"
        uname = html.escape(f"@{item['username']}") if item.get('username') else f"id{item['user_id']}"
        when = time.strftime('%d.%m.%Y %H:%M', time.localtime(item.get('found_at', 0)))
        mark = "✅" if item.get('contacted') else "⬜"
        entries.append(
            f"{mark} {i}. <b>{name}</b> ({uname})\n"
            f"   Канал: https://t.me/{html.escape(item['channel'])} — {item['subscribers']:,} подп.\n"
            f"   Найден: {when}\n\n"
        )
    return header, entries


LIST_RENDERERS = {
    'parsed': (render_parsed_list, "📡 Здесь пока пусто.\nТут появятся каналы, которые ты отправлял на парсинг."),
    'found': (render_found_list, "✨ Здесь пока пусто.\nТут появятся каналы, найденные в результате парсинга."),
}


async def show_database_list(update: Update, context: ContextTypes.DEFAULT_TYPE, which: str):
    context.user_data['db_screen'] = 'list'
    context.user_data['db_current_list'] = which
    sort_order = context.user_data.get('db_sort_order', 'new')
    user_id_str = str(update.effective_user.id)

    renderer, empty_text = LIST_RENDERERS[which]
    result = renderer(user_id_str, sort_order)

    if result is None:
        await replace_last_message(
            update, context, 'database_msg_id', empty_text, reply_markup=database_list_keyboard(which)
        )
        return

    header, entries = result
    chunks = _chunk_parts([header] + entries)

    if len(chunks) > 1:
        context.user_data.pop('database_msg_id', None)
        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode="HTML")
        await update.message.reply_text("⬆️ Список выше.", reply_markup=database_list_keyboard(which))
    else:
        await replace_last_message(
            update, context, 'database_msg_id', chunks[0],
            parse_mode="HTML", reply_markup=database_list_keyboard(which)
        )


async def my_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['db_screen'] = 'submenu'
    await replace_last_message(
        update, context, 'database_msg_id',
        "📁 <b>Моя база</b>\n\nЧто посмотреть?",
        parse_mode="HTML",
        reply_markup=database_submenu_keyboard()
    )


async def show_parsed_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_database_list(update, context, 'parsed')


async def show_found_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_database_list(update, context, 'found')


async def show_sort_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['db_screen'] = 'sort'
    await replace_last_message(
        update, context, 'database_msg_id',
        "🔀 Как сортировать?",
        reply_markup=sort_keyboard()
    )


async def _apply_sort(update: Update, context: ContextTypes.DEFAULT_TYPE, order: str):
    context.user_data['db_sort_order'] = order
    which = context.user_data.get('db_current_list', 'found')
    await show_database_list(update, context, which)


async def apply_sort_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _apply_sort(update, context, 'new')


async def apply_sort_old(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _apply_sort(update, context, 'old')


async def show_clear_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['db_screen'] = 'confirm_clear'
    which = context.user_data.get('db_current_list', 'found')
    label = {
        'parsed': "спарсенных каналов",
        'found': "найденных каналов",
    }[which]
    await replace_last_message(
        update, context, 'database_msg_id',
        f"🗑 Точно очистить базу {label}?\nОтменить это будет нельзя.",
        reply_markup=confirm_clear_keyboard()
    )


async def do_clear_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    which = context.user_data.get('db_current_list', 'found')
    user_id_str = str(update.effective_user.id)
    if which == 'parsed':
        parsed_channels_history[user_id_str] = []
        await save_parsed_channels_async(parsed_channels_history)
    else:
        user_databases[user_id_str] = []
        await save_databases_async(user_databases)
    await show_database_list(update, context, which)


async def database_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    screen = context.user_data.get('db_screen', 'main')

    if screen == 'submenu':
        context.user_data.pop('db_screen', None)
        await replace_last_message(
            update, context, 'database_msg_id',
            "Главное меню:",
            reply_markup=main_menu_keyboard()
        )
    elif screen == 'list':
        context.user_data['db_screen'] = 'submenu'
        await replace_last_message(
            update, context, 'database_msg_id',
            "📁 <b>Моя база</b>\n\nЧто посмотреть?",
            parse_mode="HTML",
            reply_markup=database_submenu_keyboard()
        )
    else:
        which = context.user_data.get('db_current_list', 'found')
        await show_database_list(update, context, which)


# ================== РАССЫЛКА ==================

def _broadcast_select_prompt(candidates: list[dict]) -> list[str]:
    header = f"📣 <b>Рассылка</b> — выбери получателей ({len(candidates)} чел.):\n\n"
    entries = []
    for i, item in enumerate(candidates, 1):
        name = html.escape(f"{item.get('first_name', '')} {item.get('last_name', '')}".strip()) or "Без имени"
        uname = f"@{item['username']}" if item.get('username') else f"id{item['user_id']}"
        mark = "✅" if item.get('contacted') else "⬜"
        entries.append(f"{mark} {i}. {name} ({html.escape(uname)})\n")
    return _chunk_parts([header] + entries)


def _parse_selection(text: str, total: int) -> list[int] | None:
    """Парсит '1,3,5-9' / 'все' в отсортированный список уникальных индексов (1-based).
    None, если не распознано ни одного валидного номера."""
    text = text.strip().lower()
    if text in ("все", "всё", "all"):
        return list(range(1, total + 1))

    result: set[int] = set()
    for part in re.split(r"[,\s]+", text):
        if not part:
            continue
        m = re.fullmatch(r"(\d+)\s*-\s*(\d+)", part)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a > b:
                a, b = b, a
            result.update(n for n in range(a, b + 1) if 1 <= n <= total)
        elif part.isdigit():
            n = int(part)
            if 1 <= n <= total:
                result.add(n)
    return sorted(result) if result else None


async def start_broadcast_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    which = context.user_data.get('db_current_list', 'found')

    if which != 'found':
        await update.message.reply_text("📣 Рассылка доступна только для раздела «Найденные каналы».")
        return ConversationHandler.END

    if not await is_user_account_connected(user_id):
        await update.message.reply_text("🔌 Сначала подключите личный аккаунт.\nНажми /start")
        return ConversationHandler.END

    candidates = broadcast_candidates(str(user_id))
    if not candidates:
        await update.message.reply_text("✨ Здесь пока пусто — сначала запусти парсинг и найди кого-то.")
        return ConversationHandler.END

    context.user_data['broadcast_candidates'] = candidates
    context.user_data.pop('database_msg_id', None)

    for chunk in _broadcast_select_prompt(candidates):
        await context.bot.send_message(chat_id, chunk, parse_mode="HTML")

    await context.bot.send_message(
        chat_id,
        "Напиши номера через запятую и/или диапазоны (например: <code>1,3,5-9</code>) "
        f"или слово «все» (максимум {MAX_BROADCAST_RECIPIENTS} за раз).",
        parse_mode="HTML",
        reply_markup=cancel_keyboard(),
    )
    return BROADCAST_SELECT


async def connect_broadcast_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "❌ Отмена":
        context.user_data.pop('broadcast_candidates', None)
        await update.message.reply_text("Рассылка отменена.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    candidates = context.user_data.get('broadcast_candidates', [])
    indices = _parse_selection(text, len(candidates))

    if not indices:
        await update.message.reply_text(
            "Не понял выбор. Пришли номера через запятую и/или диапазоны "
            "(например 1,3,5-9) или слово «все».",
            reply_markup=cancel_keyboard(),
        )
        return BROADCAST_SELECT

    if len(indices) > MAX_BROADCAST_RECIPIENTS:
        await update.message.reply_text(
            f"⚠️ Выбрано слишком много ({len(indices)} чел.) — за один раз можно не "
            f"больше {MAX_BROADCAST_RECIPIENTS}, чтобы не словить ограничения от "
            f"Telegram. Сократи диапазон и пришли ещё раз.",
            reply_markup=cancel_keyboard(),
        )
        return BROADCAST_SELECT

    context.user_data['broadcast_selected'] = [candidates[i - 1] for i in indices]
    context.user_data.pop('broadcast_candidates', None)
    await update.message.reply_text(
        f"Выбрано: {len(indices)} чел.\n\nНапиши текст рассылки обычным сообщением:",
        reply_markup=cancel_keyboard(),
    )
    return BROADCAST_TEXT


async def connect_broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "❌ Отмена":
        context.user_data.pop('broadcast_selected', None)
        await update.message.reply_text("Рассылка отменена.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    if not text:
        await update.message.reply_text("Текст пустой — пришли ещё раз.", reply_markup=cancel_keyboard())
        return BROADCAST_TEXT

    selected = context.user_data.get('broadcast_selected', [])
    context.user_data['broadcast_text'] = text
    preview = text if len(text) <= 500 else text[:500] + "…"

    await update.message.reply_text(
        f"📣 Разослать этот текст {len(selected)} получателям?\n\n"
        f"—————\n{preview}\n—————\n\n"
        "⚠️ Это реальные сообщения с твоего личного Telegram-аккаунта людям, которые "
        "тебе не писали. Отправляем с паузами между сообщениями и сразу остановимся, "
        "если Telegram сам просигналит, что это похоже на спам — но риск ограничений "
        "на аккаунт всё равно есть, особенно при частых больших рассылках.",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("✅ Разослать")], [KeyboardButton("❌ Отмена")]], resize_keyboard=True
        ),
    )
    return BROADCAST_CONFIRM


async def connect_broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    if text != "✅ Разослать":
        context.user_data.pop('broadcast_selected', None)
        context.user_data.pop('broadcast_text', None)
        await update.message.reply_text("Рассылка отменена.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    selected = context.user_data.pop('broadcast_selected', [])
    broadcast_text = context.user_data.pop('broadcast_text', '')

    if not selected or not broadcast_text:
        await update.message.reply_text(
            "❌ Сессия рассылки потеряна, начни заново.", reply_markup=main_menu_keyboard()
        )
        return ConversationHandler.END

    status_msg = await update.message.reply_text(
        f"📣 Отправляю 0 из {len(selected)}…", reply_markup=ReplyKeyboardRemove()
    )

    result = await run_broadcast_job(user_id, selected, broadcast_text, status_msg=status_msg)

    reason_text = {
        'peer_flood': (
            "\n\n⚠️ Telegram посчитал рассылку похожей на спам и мы сами остановили "
            "отправку — дальше слать не стали, чтобы не рисковать аккаунтом."
        ),
        'flood_wait': "\n\n⏳ Остановлено из-за долгого лимита от Telegram (FloodWait).",
        'no_client': "\n\n❌ Личный аккаунт не подключён.",
    }.get(result.get('stopped_reason'), "")

    try:
        await status_msg.edit_text(
            f"✅ Рассылка завершена.\nОтправлено: {result['sent']}\nОшибок: {result['failed']}{reason_text}"
        )
    except Exception:
        pass
    await context.bot.send_message(chat_id, "Готово 👇", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


async def account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    connected = await is_user_account_connected(user.id)
    if connected:
        client = await get_user_client(user.id)
        try:
            me = await client.get_me()
            acc_line = f"Подключён: {me.first_name or ''} {me.last_name or ''}".strip()
            if me.username:
                acc_line += f" (@{me.username})"
        except Exception:
            acc_line = "Подключён (сессия активна)"
    else:
        acc_line = "Не подключён — нажми /start"

    await replace_last_message(
        update, context, 'account_msg_id',
        f"👤 <b>Твой аккаунт</b>\n\n"
        f"ID: <code>{user.id}</code>\n"
        f"Имя: {html.escape(user.full_name)}\n"
        f"Юзернейм: @{user.username if user.username else 'нет'}\n\n"
        f"Telegram для парсинга: <b>{html.escape(acc_line)}</b>\n"
        f"Статус подписки: <b>{access_status_text(user.id)}</b>",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


def _money(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def _tariff_line(months: int, price: int, discount: int, savings: int) -> str:
    period = "месяц" if months == 1 else ("месяца" if months in (2, 3, 4) else "месяцев")
    bonus = f" <i>(скидка {discount}%, выгода {_money(savings)} ₽)</i>" if discount else ""
    return f"• {months} {period} — <b>{_money(price)} ₽</b>{bonus}"


async def tariffs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tariff_lines = "\n".join(_tariff_line(*t) for t in TARIFFS)
    await replace_last_message(
        update, context, 'tariffs_msg_id',
        "💎 <b>Тарифы</b>\n"
        "Парсинг работает по подписке.\n\n"
        f"📦 <b>Варианты подписки:</b>\n{tariff_lines}\n\n"
        "💳 <b>Как оформить:</b>\n"
        "Напиши в поддержку — @kushher\n"
        "Доступ выдаётся вручную после оплаты.\n\n"
        "📊 <b>Проверить статус подписки:</b>\n"
        "«👤 Аккаунт» или /status",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await replace_last_message(
        update, context, 'support_msg_id',
        "🛠 <b>Поддержка</b>\n\n"
        "По всем вопросам пиши сюда:\n"
        "@kushher",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


# ================== ПАРСИНГ (диалог) ==================

async def get_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "❌ Отмена":
        await parsing_step_reply(update, context, "Отменено.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    links = re.findall(
        r'(?:(?:https?://)?(?:t\.me|telegram\.me)/(?:s/)?@?|@)([a-zA-Z0-9_]{4,32})',
        text, re.IGNORECASE,
    )
    if not links:
        await parsing_step_reply(update, context, "Не нашёл ссылок. Попробуй ещё раз или нажми ❌ Отмена.", reply_markup=cancel_keyboard())
        return CHANNELS

    links = list(dict.fromkeys(links))
    if len(links) > MAX_CHANNELS_PER_PARSE:
        await parsing_step_reply(
            update, context,
            f"Слишком много каналов за раз ({len(links)}) — максимум {MAX_CHANNELS_PER_PARSE}. "
            "Пришли поменьше или раздели на несколько запусков.",
            reply_markup=cancel_keyboard(),
        )
        return CHANNELS

    context.user_data['channels'] = links
    await parsing_step_reply(
        update, context,
        f"Каналы: {', '.join('@'+c for c in context.user_data['channels'])}\n\n"
        "Сколько последних постов смотреть? (число от 0 до 400)",
        reply_markup=cancel_keyboard()
    )
    return POSTS


async def get_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "❌ Отмена":
        await parsing_step_reply(update, context, "Отменено.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    if not text.isdigit() or not (0 <= int(text) <= 400):
        await parsing_step_reply(update, context, "Введи число от 0 до 400.", reply_markup=cancel_keyboard())
        return POSTS

    context.user_data['posts'] = int(text)
    await parsing_step_reply(
        update, context,
        "Укажи диапазон подписчиков.\n\n"
        "Примеры:\n"
        "<code>1000-10000</code>\n"
        "<code>5000</code> (от 5000 и выше)\n"
        "<code>0-5000</code>",
        parse_mode="HTML",
        reply_markup=cancel_keyboard()
    )
    return SUBS_RANGE


async def get_subs_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "❌ Отмена":
        await parsing_step_reply(update, context, "Отменено.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    min_subs = 0
    max_subs = 10_000_000

    if "-" in text:
        parts = text.replace(" ", "").split("-")
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            min_subs = int(parts[0])
            max_subs = int(parts[1])
        else:
            await parsing_step_reply(update, context, "Неправильный формат. Пример: 1000-10000", reply_markup=cancel_keyboard())
            return SUBS_RANGE
    elif text.isdigit():
        min_subs = int(text)
    else:
        await parsing_step_reply(update, context, "Неправильный формат. Пример: 1000-10000 или 5000", reply_markup=cancel_keyboard())
        return SUBS_RANGE

    if min_subs > max_subs:
        min_subs, max_subs = max_subs, min_subs

    posts = context.user_data['posts']
    channels = context.user_data['channels']
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    prev_id = context.user_data.pop('parsing_step_msg_id', None)
    if prev_id is not None:
        try:
            await context.bot.delete_message(chat_id, prev_id)
        except Exception:
            pass
    try:
        await update.message.delete()
    except Exception:
        pass

    # Личный аккаунт обязателен
    client = await get_user_client(user_id)
    if client is None:
        await context.bot.send_message(
            chat_id,
            "🔌 Аккаунт не подключён или сессия истекла. Нажми /start",
            reply_markup=main_menu_keyboard(),
        )
        return ConversationHandler.END

    if active_parses >= MAX_CONCURRENT_PARSES:
        await context.bot.send_message(
            chat_id,
            f"⏳ Сейчас уже выполняется {active_parses} парсинг(-ов) от других пользователей "
            f"(лимит {MAX_CONCURRENT_PARSES}).\nВстал в очередь — начну автоматически, как только освободится слот."
        )

    trial_note = None
    if not has_paid_access(user_id):
        used_count = await consume_trial_use(user_id)
        trial_note = f"🎁 Бесплатный запрос {used_count} из {FREE_TRIAL_LIMIT}"

    async with parse_semaphore:
        await run_parsing_job(
            context, chat_id, user_id, channels, posts, min_subs, max_subs, trial_note,
            client=client,
        )

    return ConversationHandler.END


async def run_parsing_job(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int,
    channels: list[str], posts: int, min_subs: int, max_subs: int,
    trial_note: str | None = None,
    *, client: TelegramClient | None = None,
    notify_chat: bool = True, on_progress: "callable | None" = None,
):
    global active_parses

    if client is None:
        client = await get_user_client(user_id)
    if client is None:
        if notify_chat:
            await context.bot.send_message(
                chat_id,
                "🔌 Аккаунт не подключён. Нажми /start",
                reply_markup=main_menu_keyboard(),
            )
        return {'results': [], 'rejected': []}

    await ensure_connected(client, "личный")
    active_parses += 1
    try:
        await record_parsed_channels(user_id, channels)
        status_msg = None
        if notify_chat:
            await context.bot.send_message(chat_id, "⏳ Запускаю парсинг...", reply_markup=ReplyKeyboardRemove())

            trial_line = f"\n{trial_note}" if trial_note else ""
            status_msg = await context.bot.send_message(
                chat_id,
                f"⚡️ <b>Запускаю парсинг</b>{trial_line}\n\n"
                f"📡 Каналов: <b>{len(channels)}</b>\n"
                f"📄 Постов: <b>{posts}</b>\n"
                f"👥 Подписчики: <b>{min_subs:,} – {max_subs:,}</b>\n\n"
                "Это может занять несколько минут…",
                parse_mode="HTML",
            )

        all_results = []
        all_filtered_out = []
        debug_info = []

        seen_users = set()
        tracker = ProgressTracker(
            status_msg, total_posts=posts * len(channels), channels_total=len(channels),
            account_label="личный",
            on_update=on_progress,
        )

        user_id_str = str(user_id)
        if user_id_str not in user_databases:
            user_databases[user_id_str] = []

        pairs_before_job = {(item['user_id'], item['channel']) for item in user_databases[user_id_str]}

        async def on_new_results(new_batch):
            existing_pairs = {(item['user_id'], item['channel']) for item in user_databases[user_id_str]}
            added = False
            for r in {(x['user_id'], x['channel']): x for x in new_batch}.values():
                pair = (r['user_id'], r['channel'])
                if pair not in existing_pairs:
                    r['found_at'] = time.time()
                    user_databases[user_id_str].append(r)
                    existing_pairs.add(pair)
                    added = True
            if added:
                await save_databases_async(user_databases)

        for idx, ch in enumerate(channels, 1):
            channel_timeout = max(300, posts * 30)
            try:
                results, filtered_out, info = await asyncio.wait_for(
                    parse_channel(
                        ch, posts, min_subs, max_subs,
                        client=client, seen_users=seen_users, tracker=tracker,
                        channel_idx=idx, channels_total=len(channels),
                        on_new_results=on_new_results,
                    ),
                    timeout=channel_timeout,
                )
            except asyncio.TimeoutError:
                print(f"⚠️ @{ch}: общий таймаут канала ({channel_timeout}s), пропускаю")
                results, filtered_out, info = [], [], f"❌ Таймаут обработки канала ({channel_timeout}s)"
            await tracker.channel_done()
            all_results.extend(results)
            all_filtered_out.extend(filtered_out)
            debug_info.append(f"@{ch}: {info}")

        await save_channel_cache_async(channel_subs_cache)
        await save_user_cache_async(user_info_cache)
        await tracker.finish(len(all_results))

        unique = {(r['user_id'], r['channel']): r for r in all_results}.values()
        unique = sorted(unique, key=lambda x: x['subscribers'], reverse=True)

        by_pair_in_db = {(item['user_id'], item['channel']): item for item in user_databases[user_id_str]}
        new_pairs = {pair for pair in by_pair_in_db if pair not in pairs_before_job}
        for r in unique:
            pair = (r['user_id'], r['channel'])
            r['found_at'] = by_pair_in_db.get(pair, {}).get('found_at', time.time())
            r['is_new'] = pair in new_pairs

        rejected = {}
        for item in all_filtered_out:
            ch = item['channel']
            if ch not in rejected:
                rejected[ch] = {'channel': ch, 'subscribers': item['subscribers'], 'count': 0}
            rejected[ch]['count'] += 1
        rejected_list = sorted(rejected.values(), key=lambda x: x['subscribers'], reverse=True)

        if notify_chat:
            debug_header = "📊 <b>Статистика:</b>\n"
            debug_entries = [html.escape(line) + "\n" for line in debug_info]
            for chunk in _chunk_parts([debug_header] + debug_entries, sep=""):
                await context.bot.send_message(chat_id, chunk, parse_mode="HTML")

            if not unique:
                await context.bot.send_message(
                    chat_id,
                    "Никого не нашёл по заданным критериям.",
                    reply_markup=main_menu_keyboard()
                )
            else:
                new_count = len(new_pairs)
                already_count = len(unique) - new_count
                unique_people = len({r['user_id'] for r in unique})
                report_header = (
                    f"✅ <b>Найдено {len(unique)} результатов</b> (людей: {unique_people}) "
                    f"(новых: {new_count}, уже было в базе: {already_count}):\n"
                )
                report_entries = []
                for i, r in enumerate(unique, 1):
                    status_icon = "✅" if (r['user_id'], r['channel']) in new_pairs else "❌"
                    name = html.escape(f"{r['first_name']} {r['last_name']}".strip()) or "Без имени"
                    if r['username']:
                        person_link = f"https://t.me/{r['username']}"
                        uname = f"@{r['username']}"
                    else:
                        person_link = f"tg://user?id={r['user_id']}"
                        uname = f"id{r['user_id']}"

                    channel_link = f"https://t.me/{html.escape(r['channel'])}"
                    bio = html.escape(r['bio'][:150])
                    found_when = time.strftime('%d.%m.%Y %H:%M', time.localtime(r.get('found_at', time.time())))

                    report_entries.append(
                        f"{status_icon} <b>{i}. {name}</b> ({uname})\n"
                        f"👤 Профиль: {person_link}\n"
                        f"📢 Канал: {channel_link}\n"
                        f"👥 Подписчиков: <b>{r['subscribers']:,}</b>\n"
                        f"🕒 Найден: {found_when}\n"
                        f"📝 Био: {bio}{'...' if len(r['bio']) > 150 else ''}\n"
                    )

                for chunk in _chunk_parts([report_header] + report_entries, sep="\n"):
                    await context.bot.send_message(chat_id, chunk, parse_mode="HTML")

                await context.bot.send_message(
                    chat_id,
                    "Готово! Новые результаты (✅) сохранены в «Моя база каналов». "
                    "Отмеченные ❌ там уже были раньше.",
                    reply_markup=main_menu_keyboard()
                )

            if rejected_list:
                rejected_header = f"📛 <b>Не подошли по подписчикам ({len(rejected_list)}):</b>\n"
                rejected_entries = []
                for item in rejected_list:
                    mention = f"упомянут у {item['count']} чел." if item['count'] > 1 else "упомянут у 1 чел."
                    rejected_entries.append(
                        f"@{html.escape(item['channel'])} — <b>{item['subscribers']:,}</b> подп. ({mention})"
                    )

                for chunk in _chunk_parts([rejected_header] + rejected_entries, sep="\n"):
                    await context.bot.send_message(chat_id, chunk, parse_mode="HTML")

        return {'results': unique, 'rejected': rejected_list}
    finally:
        active_parses -= 1


def broadcast_candidates(user_id_str: str) -> list[dict]:
    """Уникальные люди (по user_id) из базы найденных — один и тот же человек мог
    найтись через разные каналы, но писать ему в рассылке нужно только один раз."""
    seen: dict[int, dict] = {}
    for item in user_databases.get(user_id_str, []):
        uid = item['user_id']
        if uid not in seen or item.get('found_at', 0) > seen[uid].get('found_at', 0):
            seen[uid] = item
    return sorted(seen.values(), key=lambda x: x.get('found_at', 0), reverse=True)


async def run_broadcast_job(
    user_id: int, recipients: list[dict], text: str,
    *, status_msg=None, on_progress: "callable | None" = None,
) -> dict:
    """Рассылает text каждому получателю личным сообщением через личный Telethon-
    аккаунт пользователя (Bot API так не может — бот не имеет права писать первым
    тому, кто с ним не переписывался). status_msg — сообщение бота, которое живьём
    редактируем прогрессом; on_progress — синхронный колбэк для мини-аппа (job dict)."""
    client = await get_user_client(user_id)
    if client is None:
        return {'sent': 0, 'failed': 0, 'stopped_reason': 'no_client', 'errors': []}

    user_id_str = str(user_id)
    total = len(recipients)
    sent = failed = 0
    errors: list[str] = []
    stopped_reason = None
    contacted_ids: set[int] = set()
    last_edit = 0.0

    async def report(done: int, force: bool = False):
        nonlocal last_edit
        if on_progress:
            try:
                on_progress({'sent': sent, 'failed': failed, 'total': total, 'done': done})
            except Exception as e:
                print(f"⚠️ broadcast on_progress: {e}")
        if status_msg is not None:
            now = time.monotonic()
            if force or now - last_edit >= BROADCAST_EDIT_INTERVAL:
                last_edit = now
                try:
                    await status_msg.edit_text(
                        f"📣 Отправляю {done} из {total}… (успешно: {sent}, ошибок: {failed})"
                    )
                except Exception:
                    pass

    for idx, r in enumerate(recipients, 1):
        try:
            await with_timeout(client.send_message(r['user_id'], text), f"broadcast({r['user_id']})")
            sent += 1
            contacted_ids.add(r['user_id'])
        except PeerFloodError:
            # Telegram явно сигналит "хватит писать незнакомцам" — продолжать значит
            # рисковать ограничением личного аккаунта пользователя. Останавливаемся.
            stopped_reason = 'peer_flood'
            await report(idx, force=True)
            break
        except FloodWaitError as e:
            if await sleep_flood_wait(e.seconds, f"broadcast({r['user_id']})"):
                try:
                    await with_timeout(
                        client.send_message(r['user_id'], text), f"broadcast retry({r['user_id']})"
                    )
                    sent += 1
                    contacted_ids.add(r['user_id'])
                except Exception as e2:
                    failed += 1
                    errors.append(f"{r.get('username') or r['user_id']}: {e2}")
            else:
                stopped_reason = 'flood_wait'
                await report(idx, force=True)
                break
        except Exception as e:
            failed += 1
            errors.append(f"{r.get('username') or r['user_id']}: {type(e).__name__}: {e}")
            print(f"⚠️ broadcast({r['user_id']}): {type(e).__name__}: {e}")

        await report(idx, force=(idx == total))

        if idx < total and stopped_reason is None:
            await asyncio.sleep(random.uniform(BROADCAST_DELAY_MIN, BROADCAST_DELAY_MAX))

    if contacted_ids:
        items = user_databases.get(user_id_str, [])
        changed = False
        for item in items:
            if item['user_id'] in contacted_ids and not item.get('contacted'):
                item['contacted'] = True
                changed = True
        if changed:
            await save_databases_async(user_databases)

    return {'sent': sent, 'failed': failed, 'stopped_reason': stopped_reason, 'errors': errors[:10]}


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await parsing_step_reply(update, context, "Отменено.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


# ================== АДМИНСКИЕ КОМАНДЫ (доступ) ==================

async def grant_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    args = context.args
    if len(args) != 2 or not args[1].isdigit():
        await update.effective_message.reply_text("Формат: /grant <user_id или @username> <дней>")
        return

    target_id, display_name, err = await resolve_user(args[0])
    if err:
        await update.effective_message.reply_text(f"❌ {err}")
        return

    days = int(args[1])
    if days <= 0:
        await update.effective_message.reply_text("Число дней должно быть больше нуля.")
        return

    until = time.time() + days * 86400
    access_grants[str(target_id)] = {'until': until, 'granted_at': time.time()}
    await save_access_async(access_grants)

    until_str = time.strftime('%d.%m.%Y', time.localtime(until))
    await update.effective_message.reply_text(
        f"✅ Доступ выдан {display_name} (id={target_id}) на {days} дн. (до {until_str})."
    )

    try:
        await context.bot.send_message(
            target_id,
            f"🎉 Тебе выдан доступ к парсингу на {days} дн. (до {until_str}).\n"
            f"Нажми «🔍 Новый парсинг» или отправь /parsing, чтобы начать."
        )
    except Exception as e:
        await update.effective_message.reply_text(f"(не смог уведомить пользователя лично: {e})")


async def revoke_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    args = context.args
    if len(args) != 1:
        await update.effective_message.reply_text("Формат: /revoke <user_id или @username>")
        return

    target_id, display_name, err = await resolve_user(args[0])
    if err:
        await update.effective_message.reply_text(f"❌ {err}")
        return

    if str(target_id) in access_grants:
        del access_grants[str(target_id)]
        await save_access_async(access_grants)
        await update.effective_message.reply_text(f"🚫 Доступ у {display_name} (id={target_id}) отозван.")
    else:
        await update.effective_message.reply_text(f"У {display_name} (id={target_id}) и так не было доступа.")


async def disconnect_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь может отключить свой аккаунт командой /disconnect."""
    user_id = update.effective_user.id
    await disconnect_user_client(user_id)
    _remove_session_files(user_id)
    await update.effective_message.reply_text(
        "🔌 Аккаунт отключён. Чтобы подключить снова — /start",
        reply_markup=ReplyKeyboardRemove(),
    )


# ================== ЗАПУСК ==================

async def call_with_retry(coro_factory, label: str, attempts: int = 6, delay: float = 5.0):
    """Повторяет сетевой вызов к Telegram Bot API при обрыве соединения — на проде
    бывают кратковременные обрывы TCP до серверов Telegram (тикет в поддержку
    хостинга), и без ретрая единственный обрыв на старте убивает весь процесс.
    coro_factory — функция без аргументов, возвращающая новую корутину на каждую
    попытку (одну и ту же корутину повторно awaitить нельзя)."""
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return await coro_factory()
        except (TimedOut, NetworkError) as e:
            last_exc = e
            print(f"⚠️ {label}: попытка {attempt}/{attempts} не удалась ({type(e).__name__}: {e})")
            if attempt < attempts:
                await asyncio.sleep(delay)
    raise last_exc


async def start_client(client: TelegramClient, name: str):
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(
            f"Сессия '{name}' не авторизована. Сначала выполни для неё вход через "
            f"login_helper.py (см. README/инструкцию), потом перезапусти бота."
        )
    me = await client.get_me()
    print(f"  [{name}] авторизован как {me.first_name} (id={me.id})")


async def main():
    # На проде бывают кратковременные обрывы TCP до Telegram (см. тикет в поддержку
    # хостинга) — стандартный таймаут httpx (5с) на нестабильной сети срабатывает
    # слишком рано. Даём больше запаса; на быстрой сети это ничего не меняет.
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .connect_timeout(20)
        .read_timeout(20)
        .write_timeout(20)
        .pool_timeout(20)
        .get_updates_connect_timeout(20)
        .get_updates_read_timeout(20)
        .build()
    )

    # Диалог: /start → (при необходимости) телефон → код → 2FA → меню
    # + парсинг
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            MessageHandler(filters.Regex("^✅ Я подписался$"), start),
            MessageHandler(filters.Regex("^🔍 Новый парсинг$"), new_parsing),
            CommandHandler("parsing", new_parsing),
            MessageHandler(filters.Regex("^📣 Рассылка$"), start_broadcast_select),
        ],
        states={
            CONNECT_PHONE: [
                MessageHandler(filters.CONTACT, connect_get_phone),
                MessageHandler(filters.TEXT & ~filters.COMMAND, connect_get_phone),
            ],
            CONNECT_CODE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, connect_get_code),
            ],
            CONNECT_PASSWORD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, connect_get_password),
            ],
            CHANNELS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_channels)],
            POSTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_posts)],
            SUBS_RANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_subs_range)],
            BROADCAST_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, connect_broadcast_selection)],
            BROADCAST_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, connect_broadcast_text)],
            BROADCAST_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, connect_broadcast_confirm)],
        },
        fallbacks=[
            MessageHandler(filters.Regex("^❌ Отмена$"), cancel),
            CommandHandler("cancel", cancel),
            CommandHandler("start", start),
        ],
        allow_reentry=True,
    )
    app.add_handler(conv_handler)

    app.add_handler(CommandHandler("status", account))
    app.add_handler(CommandHandler("database", my_database))
    app.add_handler(CommandHandler("tariffs", tariffs))
    app.add_handler(CommandHandler("support", support))
    app.add_handler(CommandHandler("grant", grant_access))
    app.add_handler(CommandHandler("revoke", revoke_access))
    app.add_handler(CommandHandler("disconnect", disconnect_account))
    app.add_handler(MessageHandler(filters.Regex("^📁 Моя база каналов$"), my_database))
    app.add_handler(MessageHandler(filters.Regex("^👤 Аккаунт$"), account))
    app.add_handler(MessageHandler(filters.Regex("^💎 Тарифы$"), tariffs))
    app.add_handler(MessageHandler(filters.Regex("^🛠 Поддержка$"), support))

    app.add_handler(MessageHandler(filters.Regex("^📡 Спарсенные каналы$"), show_parsed_channels))
    app.add_handler(MessageHandler(filters.Regex("^✨ Найденные каналы$"), show_found_channels))
    app.add_handler(MessageHandler(filters.Regex("^🔀 Сортировка$"), show_sort_menu))
    app.add_handler(MessageHandler(filters.Regex("^🆕 Сначала новые$"), apply_sort_new))
    app.add_handler(MessageHandler(filters.Regex("^📜 Сначала старые$"), apply_sort_old))
    app.add_handler(MessageHandler(filters.Regex("^🗑 Очистить базу$"), show_clear_confirm))
    app.add_handler(MessageHandler(filters.Regex("^✅ Да, очистить$"), do_clear_database))
    app.add_handler(MessageHandler(filters.Regex("^◀️ Назад$"), database_back))

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
        print(f"❌ Необработанное исключение: {context.error}")
        traceback.print_exception(type(context.error), context.error, context.error.__traceback__)
        if isinstance(update, Update) and update.effective_chat:
            try:
                await context.bot.send_message(
                    update.effective_chat.id,
                    "⚠️ Что-то пошло не так при обработке запроса. Попробуй ещё раз или напиши в поддержку — @kushher.",
                )
            except Exception:
                pass

    app.add_error_handler(error_handler)

    await call_with_retry(app.initialize, "app.initialize()")

    # Mini App API (если модуль есть)
    try:
        import uvicorn
        from miniapp_api import create_app
        fastapi_app = create_app(bot=app.bot)
        api_server = uvicorn.Server(uvicorn.Config(fastapi_app, host="127.0.0.1", port=8001, log_level="warning"))
        api_task = asyncio.create_task(api_server.serve())
        api_task.add_done_callback(
            lambda t: print(f"❌ Mini App API упал: {t.exception()}") if not t.cancelled() and t.exception() else None
        )
        print("Mini App API запущен на 127.0.0.1:8001")
    except ImportError:
        print("miniapp_api не найден — пропускаю Mini App API")

    if telethon_clients:
        print(f"Запускаю общие Telethon-сессии ({len(telethon_clients)})...")
        # Общие сессии — только запасной пул для админских команд, не критичны для
        # основного парсинга (он идёт через личный аккаунт пользователя). Поэтому
        # поломка одной сессии (протухший FloodWait, AuthKeyDuplicatedError и т.п.)
        # не должна ронять весь процесс — просто выкидываем её из пула.
        results = await asyncio.gather(
            *(start_client(client, name) for client, name in zip(telethon_clients, SESSION_NAMES)),
            return_exceptions=True,
        )
        working = []
        for client, name, result in zip(list(telethon_clients), SESSION_NAMES, results):
            if isinstance(result, Exception):
                print(f"⚠️ Общая сессия '{name}' не поднялась, пропускаю: {type(result).__name__}: {result}")
                try:
                    await client.disconnect()
                except Exception:
                    pass
            else:
                working.append(client)
        telethon_clients[:] = working
        if working:
            print(f"Общих сессий поднято: {len(working)}/{len(SESSION_NAMES)}")
        else:
            print("⚠️ Ни одна общая сессия не поднялась — парсинг только через личные аккаунты пользователей.")
    else:
        print("Общие Telethon-сессии не заданы (SESSION_NAMES пуст) — парсинг только через личные аккаунты пользователей.")

    await call_with_retry(
        lambda: app.bot.set_my_commands([
            BotCommand("start", "Запустить бота / подключить аккаунт"),
            BotCommand("parsing", "Новый парсинг"),
            BotCommand("status", "Статус аккаунта"),
            BotCommand("database", "Моя база каналов"),
            BotCommand("tariffs", "Тарифы и доступ"),
            BotCommand("support", "Поддержка"),
            BotCommand("disconnect", "Отключить Telegram-аккаунт"),
            BotCommand("cancel", "Отмена"),
        ]),
        "set_my_commands",
    )

    await call_with_retry(
        lambda: app.bot.set_my_description(
            "👋 Привет! Я ищу комментаторов Telegram-каналов и каналы, "
            "которые они указали у себя в профиле.\n\nНажми Start, чтобы начать."
        ),
        "set_my_description",
    )

    print("Официальный бот запускается...")
    # ВАЖНО: start_polling() запускает фоновый цикл поллинга — если он успел частично
    # стартовать и всё равно кинул исключение, повторный вызов создаст ВТОРОЙ такой
    # же цикл в этом же процессе, и они будут бесконечно конфликтовать друг с другом
    # (Conflict: terminated by other getUpdates request). Поэтому здесь БЕЗ ретрая —
    # если сеть подвела именно тут, пусть падает весь процесс, а systemd поднимет
    # его заново с чистого листа (Restart=always в юните).
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    print("Бот успешно запущен!")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
