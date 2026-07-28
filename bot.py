import asyncio
import re
import json
import os
import time
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.functions.messages import GetRepliesRequest
from telethon.tl.types import Channel, User
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, BotCommand
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
# Несколько Telethon-сессий (аккаунтов) через запятую — парсинг распределяется между ними,
# сколько сессий, столько парсингов может идти одновременно. Новую сессию сначала нужно
# авторизовать через login_helper.py, иначе бот откажется стартовать.
SESSION_NAMES = [s.strip() for s in os.getenv("SESSION_NAMES", "parser_session").split(",") if s.strip()]
# Telegram user_id владельца бота — только он может выдавать/отзывать доступ (/grant, /revoke).
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))

DATABASE_FILE = "user_databases.json"
ACCESS_FILE = "access_grants.json"
CHANNEL_CACHE_FILE = "channel_subs_cache.json"
CHANNEL_CACHE_TTL_SECONDS = 24 * 3600  # сутки — потом число подписчиков считаем устаревшим
USER_CACHE_FILE = "user_info_cache.json"
USER_CACHE_TTL_SECONDS = 24 * 3600  # сутки — потом био/каналы пользователя считаем устаревшими
COMMENT_CONCURRENCY = 5  # сколько комментаторов обрабатываем параллельно (на один аккаунт, на один канал)
PROGRESS_EDIT_INTERVAL = 2.5  # не чаще раза в N секунд редактируем статус-сообщение
# Сколько парсингов может идти одновременно. Может быть БОЛЬШЕ числа аккаунтов —
# тогда часть аккаунтов будет параллельно обслуживать не один, а два (и больше)
# парсинга сразу. Это осознанный компромисс: больше параллельности для людей,
# но выше шанс словить flood-wait (а в худшем случае — подозрение/бан) на тех
# аккаунтах, что делят нагрузку. Если увидишь частые FloodWaitError — либо
# добавь ещё аккаунтов (см. login_helper.py), либо опусти это число обратно
# к количеству аккаунтов.
MAX_CONCURRENT_PARSES = 5
CALL_TIMEOUT = 60  # секунд максимум на один сетевой вызов (Telethon/Bot API) — защита от зависаний
MAX_FLOOD_WAIT = 120  # дольше этого не спим по FloodWaitError — лучше пропустить, чем занять слот на часы
# ===============================================

# Состояния
CHANNELS, POSTS, SUBS_RANGE = range(3)

telethon_clients = [
    TelegramClient(name, API_ID, API_HASH, connection_retries=10, retry_delay=3)
    for name in SESSION_NAMES
]


class ClientPool:
    """Раздаёт Telethon-аккаунты парсингам по кругу (round-robin). Если MAX_CONCURRENT_PARSES
    больше числа аккаунтов, один аккаунт может достаться нескольким одновременным парсингам."""

    def __init__(self, clients: list):
        self.clients = clients
        self._next = 0

    @property
    def size(self) -> int:
        return len(self.clients)

    def label_for(self, client) -> str:
        idx = self.clients.index(client)
        return f"акк.{idx + 1}" if self.size > 1 else ""

    def pick(self):
        client = self.clients[self._next % self.size]
        self._next += 1
        return client


client_pool = ClientPool(telethon_clients)
parse_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PARSES)
active_parses = 0  # для текста "встал в очередь"

# Файл базы общий на всех пользователей бота — пишем через lock, чтобы записи не затирали друг друга
db_lock = asyncio.Lock()
channel_cache_lock = asyncio.Lock()
user_cache_lock = asyncio.Lock()
access_lock = asyncio.Lock()


# Загрузка базы
def load_databases():
    if os.path.exists(DATABASE_FILE):
        try:
            with open(DATABASE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_databases(data):
    with open(DATABASE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_databases_async(data):
    async with db_lock:
        await asyncio.to_thread(save_databases, data)

user_databases = load_databases()


# Постоянный кэш подписчиков каналов (переживает перезапуски бота и разные парсинги)
def load_channel_cache():
    if os.path.exists(CHANNEL_CACHE_FILE):
        try:
            with open(CHANNEL_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_channel_cache(data):
    with open(CHANNEL_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_channel_cache_async(data):
    async with channel_cache_lock:
        await asyncio.to_thread(save_channel_cache, data)

channel_subs_cache = load_channel_cache()


# Постоянный кэш био/каналов пользователей — если один и тот же человек комментирует
# в разных парсингах (или у разных заказчиков), второй раз его не запрашиваем у Telegram
def load_user_cache():
    if os.path.exists(USER_CACHE_FILE):
        try:
            with open(USER_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_user_cache(data):
    with open(USER_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_user_cache_async(data):
    async with user_cache_lock:
        await asyncio.to_thread(save_user_cache, data)

user_info_cache = load_user_cache()


# Платный доступ к парсингу — владелец бота выдаёт его вручную командой /grant
# после оплаты (вне бота, любым способом). Доступ подписочный: истекает по дате.
def load_access():
    if os.path.exists(ACCESS_FILE):
        try:
            with open(ACCESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_access(data):
    with open(ACCESS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def save_access_async(data):
    async with access_lock:
        await asyncio.to_thread(save_access, data)

access_grants = load_access()


def is_admin(user_id: int) -> bool:
    return ADMIN_USER_ID != 0 and user_id == ADMIN_USER_ID


def has_access(user_id: int) -> bool:
    if is_admin(user_id):
        return True  # владельцу подписка не нужна
    grant = access_grants.get(str(user_id))
    return grant is not None and time.time() < grant['until']


def access_status_text(user_id: int) -> str:
    if is_admin(user_id):
        return "Владелец бота — доступ без ограничений"
    grant = access_grants.get(str(user_id))
    if grant is None:
        return "Нет доступа. Для оформления подписки — /tariffs"
    until_str = time.strftime('%d.%m.%Y', time.localtime(grant['until']))
    remaining = grant['until'] - time.time()
    if remaining <= 0:
        return f"Подписка истекла {until_str}. Для продления — /tariffs"
    days = int(remaining // 86400) + 1
    return f"Активна до {until_str} (осталось ~{days} дн.)"


async def resolve_user(identifier: str) -> tuple[int | None, str | None, str | None]:
    """Принимает @username или числовой user_id админской команды.
    Возвращает (user_id, отображаемое_имя, текст_ошибки)."""
    identifier = identifier.strip().lstrip('@')
    if identifier.isdigit():
        return int(identifier), identifier, None
    if not telethon_clients:
        return None, None, "Нет доступных аккаунтов для поиска по username."
    try:
        entity = await with_timeout(telethon_clients[0].get_entity(identifier), f"resolve @{identifier}")
    except Exception as e:
        return None, None, f"Не удалось найти @{identifier}: {e}"
    if not isinstance(entity, User):
        return None, None, f"@{identifier} — это не пользователь (канал или группа?)."
    name = f"{entity.first_name or ''} {entity.last_name or ''}".strip() or f"@{identifier}"
    return entity.id, name, None


async def with_timeout(coro, label: str, seconds: float = CALL_TIMEOUT):
    """Оборачивает любой сетевой вызов (Telethon/Bot API) таймаутом — без этого
    зависший вызов блокирует задачу навсегда и держит занятым слот парсинга."""
    try:
        return await asyncio.wait_for(coro, timeout=seconds)
    except asyncio.TimeoutError:
        print(f"⚠️ TIMEOUT ({seconds}s): {label}")
        raise


async def sleep_flood_wait(seconds: int, label: str) -> bool:
    """Спит по FloodWaitError, но не дольше MAX_FLOOD_WAIT — иначе один долгий
    флуд-вейт держит занятым слот парсинга часами. Возвращает False, если решили
    не ждать (вызывающий код должен сдаться и пойти дальше)."""
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


async def get_channel_subscribers(username: str, client: TelegramClient) -> int | None:
    try:
        entity = await with_timeout(client.get_entity(username), f"get_entity({username})")
        # Только настоящие каналы (broadcast), супергруппы/чаты отсеиваем
        if isinstance(entity, Channel) and entity.broadcast:
            full = await with_timeout(client(GetFullChannelRequest(entity)), f"GetFullChannel({username})")
            return full.full_chat.participants_count
    except (UsernameNotOccupiedError, ChannelPrivateError, ValueError, TypeError):
        return None
    except FloodWaitError as e:
        if await sleep_flood_wait(e.seconds, f"get_channel_subscribers({username})"):
            return await get_channel_subscribers(username, client)
        return None
    except Exception:
        return None
    return None


async def get_entity_with_retry(entity_ref, client: TelegramClient):
    try:
        return await with_timeout(client.get_entity(entity_ref), f"get_entity({entity_ref})")
    except FloodWaitError as e:
        if await sleep_flood_wait(e.seconds, f"get_entity_with_retry({entity_ref})"):
            return await with_timeout(client.get_entity(entity_ref), f"get_entity({entity_ref}) retry")
        raise


async def fetch_all_replies(entity, msg_id: int, client: TelegramClient, limit: int = 500):
    """Постранично собирает комментарии к посту (Telegram отдаёт максимум ~100 за раз)."""
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
    """Возвращает био и список username каналов: привязанный к профилю канал,
    каналы из био и каналы из описания Telegram Business (отдельное поле профиля).
    Результат кэшируется на диске — тот же человек в другом парсинге не запрашивается заново."""
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
    except Exception:
        pass

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
    """Живой статус парсинга: текущий канал, прогресс-бар, ETA. Редактирует одно сообщение."""

    def __init__(self, status_msg, total_posts: int, channels_total: int, account_label: str = ""):
        self.status_msg = status_msg
        self.total_posts = max(total_posts, 1)
        self.channels_total = channels_total
        self.account_label = f" [{account_label}]" if account_label else ""
        self.posts_done = 0
        self.channels_done = 0
        self.channel_found_counts = {}  # channel_idx -> найдено (для общего счётчика при параллельных каналах)
        self.start_time = time.monotonic()
        self.last_edit_time = 0.0
        self.last_text = None

    async def _edit(self, text: str, force: bool = False):
        now = time.monotonic()
        if not force and (now - self.last_edit_time) < PROGRESS_EDIT_INTERVAL:
            return
        if text == self.last_text:
            return
        self.last_edit_time = now
        self.last_text = text
        try:
            await asyncio.wait_for(self.status_msg.edit_text(text), timeout=15)
        except Exception as e:
            print(f"⚠️ ProgressTracker._edit failed ({type(e).__name__}): {e}")

    async def channel_started(self, channel: str, channel_idx: int):
        # Каналы обрабатываются параллельно — при нескольких каналах не дёргаем статус
        # на старте каждого отдельно, иначе он мигал бы между разными каналами.
        if self.channels_total > 1:
            return
        text = f"⏳ Парсинг{self.account_label}: @{channel}\nЗагружаю посты и комментарии..."
        await self._edit(text, force=True)

    async def channel_done(self):
        self.channels_done += 1

    def _bar(self, fraction: float) -> str:
        bar_len = 10
        filled = int(bar_len * fraction)
        return "▓" * filled + "░" * (bar_len - filled)

    async def post_done(self, channel: str, channel_idx: int, post_idx: int, posts_in_channel: int, found_count: int):
        self.posts_done += 1
        self.channel_found_counts[channel_idx] = found_count
        total_found = sum(self.channel_found_counts.values())

        now = time.monotonic()
        elapsed = now - self.start_time
        fraction = min(self.posts_done / self.total_posts, 1.0)
        percent = int(fraction * 100)

        eta_text = "считаю..."
        if fraction > 0.03:
            eta_seconds = elapsed / fraction - elapsed
            eta_text = format_duration(eta_seconds)

        bar = self._bar(fraction)

        if self.channels_total == 1:
            text = (
                f"⏳ Парсинг{self.account_label}: @{channel}\n"
                f"Пост {post_idx}/{posts_in_channel}\n"
                f"[{bar}] {percent}%\n"
                f"Прошло: {format_duration(elapsed)} | Осталось: ~{eta_text}\n"
                f"Найдено подходящих: {total_found}"
            )
        else:
            text = (
                f"⏳ Парсинг {self.channels_total} каналов параллельно{self.account_label}\n"
                f"Обработано каналов: {self.channels_done}/{self.channels_total}\n"
                f"[{bar}] {percent}% по постам\n"
                f"Прошло: {format_duration(elapsed)} | Осталось: ~{eta_text}\n"
                f"Найдено подходящих: {total_found}"
            )
        force = self.posts_done >= self.total_posts
        await self._edit(text, force=force)

    async def finish(self, found_count: int):
        elapsed = time.monotonic() - self.start_time
        text = (
            f"✅ Парсинг завершён за {format_duration(elapsed)}\n"
            f"Найдено подходящих: {found_count}"
        )
        await self._edit(text, force=True)


async def parse_channel(
    channel_link: str, posts_limit: int, min_subs: int, max_subs: int,
    *, client: TelegramClient, seen_users: set, tracker: "ProgressTracker",
    channel_idx: int, channels_total: int,
):
    results = []
    filtered_out = []  # каналы, которые нашлись, но не подошли по диапазону подписчиков
    total_comments = 0
    users_with_channels = 0
    channels_found = 0
    # Каналы одного запроса теперь обрабатываются параллельно одним и тем же аккаунтом —
    # чтобы суммарная нагрузка на аккаунт не росла пропорционально числу каналов,
    # делим "бюджет" параллельности комментаторов между ними.
    sem = asyncio.Semaphore(max(1, COMMENT_CONCURRENCY // channels_total))

    print(f"▶ parse_channel start: @{channel_link} (постов запрошено: {posts_limit})")

    try:
        entity = await with_timeout(client.get_entity(channel_link), f"get_entity(@{channel_link})")
    except Exception as e:
        print(f"❌ @{channel_link}: не удалось открыть канал: {e}")
        return [], [], f"❌ Не удалось открыть канал: {e}"

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

    async def get_cached_subs(username: str):
        cached = channel_subs_cache.get(username)
        if cached is not None and (time.time() - cached['ts']) < CHANNEL_CACHE_TTL_SECONDS:
            return cached['subs']
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
                    break
                else:
                    filtered_out.append({'channel': ch_username, 'subscribers': subs})

    async def collect_via_iter(msg_id: int):
        result = []
        async for comment in client.iter_messages(entity, reply_to=msg_id, limit=500):
            result.append(comment)
        return result

    for post_idx, msg in enumerate(messages, 1):
        comments = []

        # Способ 1
        try:
            comments = await with_timeout(collect_via_iter(msg.id), f"iter_messages(msg={msg.id})")
        except FloodWaitError as e:
            await sleep_flood_wait(e.seconds, f"iter_messages(msg={msg.id})")
        except Exception:
            pass

        # Способ 2 (постранично, если способ 1 не сработал)
        if not comments and getattr(msg, 'replies', None) and msg.replies.replies > 0:
            try:
                comments = await with_timeout(
                    fetch_all_replies(entity, msg.id, client, limit=500),
                    f"fetch_all_replies(msg={msg.id})", seconds=CALL_TIMEOUT * 10,
                )
            except Exception:
                pass

        if comments:
            try:
                await with_timeout(
                    asyncio.gather(*(process_comment(c) for c in comments)),
                    f"process_comment batch (msg={msg.id}, n={len(comments)})",
                    seconds=CALL_TIMEOUT * 10,
                )
            except Exception as e:
                print(f"⚠️ @{channel_link} msg={msg.id}: обработка комментариев прервана: {e}")

        await tracker.post_done(channel_link, channel_idx, post_idx, posts_in_channel, len(results))

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


# ================== ОБРАБОТЧИКИ МЕНЮ ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я парсер комментаторов Telegram-каналов.\n\n"
        "Выбери действие в меню:",
        reply_markup=main_menu_keyboard()
    )
    return ConversationHandler.END


async def new_parsing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not has_access(update.effective_user.id):
        await update.message.reply_text(
            "🔒 <b>Парсинг доступен по подписке.</b>\n\n"
            "Посмотри варианты и оформи доступ — «💎 Тарифы» или /tariffs.",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard()
        )
        return ConversationHandler.END

    await parsing_step_reply(
        update, context,
        "🔍 <b>Новый парсинг</b>\n\n"
        "Пришли ссылку(и) на канал(ы).\n"
        "Можно несколько через пробел или с новой строки.\n\n"
        "Пример:\n<code>https://t.me/durov</code>\n<code>@channelname</code>",
        parse_mode="HTML",
        reply_markup=cancel_keyboard()
    )
    return CHANNELS


async def my_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    data = user_databases.get(user_id, [])

    if not data:
        await update.message.reply_text(
            "📁 Твоя база пока пуста.\n"
            "Найденные каналы будут сохраняться сюда после парсинга.",
            reply_markup=main_menu_keyboard()
        )
        return

    text = f"📁 <b>Твоя база каналов</b> ({len(data)} шт.):\n\n"
    for i, item in enumerate(data[-30:], 1):  # последние 30
        name = f"{item.get('first_name', '')} {item.get('last_name', '')}".strip() or "Без имени"
        uname = f"@{item['username']}" if item.get('username') else f"id{item['user_id']}"
        text += (
            f"{i}. <b>{name}</b> ({uname})\n"
            f"   Канал: https://t.me/{item['channel']} — {item['subscribers']:,} подп.\n\n"
        )

    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await update.message.reply_text(text[i:i+4000], parse_mode="HTML")
    else:
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=main_menu_keyboard())


async def account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"👤 <b>Твой аккаунт</b>\n\n"
        f"ID: <code>{user.id}</code>\n"
        f"Имя: {user.full_name}\n"
        f"Юзернейм: @{user.username if user.username else 'нет'}\n\n"
        f"Статус подписки: <b>{access_status_text(user.id)}</b>",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


async def tariffs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "💎 <b>Тарифы</b>\n"
        "Парсинг работает по подписке.\n\n"
        "📦 <b>Варианты подписки:</b>\n"
        "• 1 месяц — <b>2 390 ₽</b>\n"
        "• 3 месяца — <b>6 790 ₽</b> <i>(скидка 5%)</i>\n"
        "• 6 месяцев — <b>12 990 ₽</b> <i>(скидка 10%)</i>\n"
        "• 12 месяцев — <b>22 990 ₽</b> <i>(скидка 20%)</i>\n\n"
        "💳 <b>Как оформить:</b>\n"
        "Напиши в поддержку — 👉 https://t.me/kjwami\n"
        "Доступ выдаётся вручную после оплаты.\n\n"
        "📊 <b>Проверить статус подписки:</b>\n"
        "«👤 Аккаунт» или /status",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🛠 <b>Поддержка</b>\n\n"
        "По всем вопросам пиши сюда:\n"
        "👉 https://t.me/kjwami",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard()
    )


# ================== ПАРСИНГ (диалог) ==================
#
# В этом диалоге (выбор каналов -> число постов -> диапазон подписчиков) бот
# автоматически чистит за собой: удаляет и своё предыдущее сообщение-шаг, и
# сообщение пользователя, оставляя только актуальный шаг. Больше нигде в боте
# (меню, аккаунт, база, поддержка, сами результаты парсинга) такого удаления нет.

async def parsing_step_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    chat_id = update.effective_chat.id

    prev_id = context.user_data.get('parsing_step_msg_id')
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
    context.user_data['parsing_step_msg_id'] = msg.message_id
    return msg


async def get_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    if text == "❌ Отмена":
        await parsing_step_reply(update, context, "Отменено.", reply_markup=main_menu_keyboard())
        return ConversationHandler.END

    links = re.findall(r'(?:https?://)?(?:t\.me/|telegram\.me/)?@?([a-zA-Z0-9_]+)', text)
    if not links:
        await parsing_step_reply(update, context, "Не нашёл ссылок. Попробуй ещё раз или нажми ❌ Отмена.", reply_markup=cancel_keyboard())
        return CHANNELS

    context.user_data['channels'] = list(dict.fromkeys(links))
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

    # Парсим диапазон
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

    # Финальный шаг диалога — чистим за собой (прошлый вопрос + ответ), но дальше
    # сообщения парсинга (статус, прогресс, результаты) уже НЕ удаляются, это
    # больше не диалог, а отчёт, который должен остаться у пользователя.
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

    if active_parses >= MAX_CONCURRENT_PARSES:
        await context.bot.send_message(
            chat_id,
            f"⏳ Сейчас уже выполняется {active_parses} парсинг(-ов) от других пользователей "
            f"(лимит {MAX_CONCURRENT_PARSES}).\nВстал в очередь — начну автоматически, как только освободится слот."
        )

    async with parse_semaphore:
        await run_parsing_job(context, chat_id, user_id, channels, posts, min_subs, max_subs)

    return ConversationHandler.END


async def run_parsing_job(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int,
    channels: list[str], posts: int, min_subs: int, max_subs: int,
):
    global active_parses
    client = client_pool.pick()
    active_parses += 1
    try:
        # Сообщение с reply_markup (в т.ч. ReplyKeyboardRemove) Telegram НЕ даёт
        # редактировать вообще никогда — поэтому клавиатуру убираем отдельным,
        # одноразовым сообщением, а статус парсинга шлём уже без reply_markup,
        # чтобы его можно было редактировать (прогресс-бар).
        await context.bot.send_message(chat_id, "⏳ Запускаю парсинг...", reply_markup=ReplyKeyboardRemove())

        status_msg = await context.bot.send_message(
            chat_id,
            f"⏳ Начинаю парсинг...\n"
            f"Каналов: {len(channels)}\n"
            f"Постов: {posts}\n"
            f"Подписчики: {min_subs:,} – {max_subs:,}\n\n"
            "Это может занять несколько минут..."
        )

        all_results = []
        all_filtered_out = []
        debug_info = []

        # Общий на весь парсинг список "уже видели" — не обрабатываем повторно одного
        # и того же комментатора, даже если он встречается в разных обрабатываемых каналах.
        # Подписчики каналов кэшируются отдельно, в channel_subs_cache — она постоянная
        # (на диске, живёт между разными парсингами и перезапусками бота).
        seen_users = set()
        tracker = ProgressTracker(
            status_msg, total_posts=posts * len(channels), channels_total=len(channels),
        )

        # Каналы одного запроса обрабатываются параллельно (одним и тем же аккаунтом) —
        # быстрее для многоканальных запросов ценой более высокой пиковой нагрузки на
        # аккаунт в моменте (см. деление COMMENT_CONCURRENCY в parse_channel).
        async def run_one_channel(idx: int, ch: str):
            # Последний рубеж защиты: что бы ни случилось внутри parse_channel,
            # канал не может держать слот парсинга дольше этого времени.
            channel_timeout = max(300, posts * 30)
            try:
                results, filtered_out, info = await asyncio.wait_for(
                    parse_channel(
                        ch, posts, min_subs, max_subs,
                        client=client, seen_users=seen_users, tracker=tracker,
                        channel_idx=idx, channels_total=len(channels),
                    ),
                    timeout=channel_timeout,
                )
            except asyncio.TimeoutError:
                print(f"⚠️ @{ch}: общий таймаут канала ({channel_timeout}s), пропускаю")
                results, filtered_out, info = [], [], f"❌ Таймаут обработки канала ({channel_timeout}s)"
            await tracker.channel_done()
            return ch, results, filtered_out, info

        channel_results = await asyncio.gather(
            *(run_one_channel(idx, ch) for idx, ch in enumerate(channels, 1))
        )

        for ch, results, filtered_out, info in channel_results:
            all_results.extend(results)
            all_filtered_out.extend(filtered_out)
            debug_info.append(f"@{ch}: {info}")

        await save_channel_cache_async(channel_subs_cache)
        await save_user_cache_async(user_info_cache)
        await tracker.finish(len(all_results))

        # Убираем дубли
        unique = {r['user_id']: r for r in all_results}.values()
        unique = sorted(unique, key=lambda x: x['subscribers'], reverse=True)

        # Сохраняем в базу пользователя, запоминая какие записи новые
        user_id_str = str(user_id)
        if user_id_str not in user_databases:
            user_databases[user_id_str] = []

        existing_ids = {item['user_id'] for item in user_databases[user_id_str]}
        new_ids = set()
        for r in unique:
            if r['user_id'] not in existing_ids:
                user_databases[user_id_str].append(r)
                new_ids.add(r['user_id'])

        await save_databases_async(user_databases)

        # Статистика
        debug_text = "\n".join(debug_info)
        await context.bot.send_message(chat_id, f"📊 <b>Статистика:</b>\n{debug_text}", parse_mode="HTML")

        if not unique:
            await context.bot.send_message(
                chat_id,
                "Никого не нашёл по заданным критериям.",
                reply_markup=main_menu_keyboard()
            )
        else:
            new_count = len(new_ids)
            already_count = len(unique) - new_count
            report = [
                f"✅ <b>Найдено {len(unique)} человек</b> "
                f"(новых: {new_count}, уже было в базе: {already_count}):\n"
            ]
            for i, r in enumerate(unique, 1):
                status_icon = "✅" if r['user_id'] in new_ids else "❌"
                name = f"{r['first_name']} {r['last_name']}".strip() or "Без имени"
                if r['username']:
                    person_link = f"https://t.me/{r['username']}"
                    uname = f"@{r['username']}"
                else:
                    person_link = f"tg://user?id={r['user_id']}"
                    uname = f"id{r['user_id']}"

                channel_link = f"https://t.me/{r['channel']}"

                report.append(
                    f"{status_icon} <b>{i}. {name}</b> ({uname})\n"
                    f"👤 Профиль: {person_link}\n"
                    f"📢 Канал: {channel_link}\n"
                    f"👥 Подписчиков: <b>{r['subscribers']:,}</b>\n"
                    f"📝 Био: {r['bio'][:150]}{'...' if len(r['bio']) > 150 else ''}\n"
                )

            full = "\n".join(report)
            for i in range(0, len(full), 4000):
                await context.bot.send_message(chat_id, full[i:i+4000], parse_mode="HTML")

            await context.bot.send_message(
                chat_id,
                "Готово! Новые результаты (✅) сохранены в «Моя база каналов». "
                "Отмеченные ❌ там уже были раньше.",
                reply_markup=main_menu_keyboard()
            )

        # Отдельный отчёт по каналам, которые нашлись, но не подошли по подписчикам
        if all_filtered_out:
            rejected = {}
            for item in all_filtered_out:
                ch = item['channel']
                if ch not in rejected:
                    rejected[ch] = {'subscribers': item['subscribers'], 'count': 0}
                rejected[ch]['count'] += 1

            rejected_list = sorted(rejected.items(), key=lambda x: x[1]['subscribers'], reverse=True)

            report = [f"📛 <b>Не подошли по подписчикам ({len(rejected_list)}):</b>\n"]
            for ch, data in rejected_list:
                mention = f"упомянут у {data['count']} чел." if data['count'] > 1 else "упомянут у 1 чел."
                report.append(
                    f"@{ch} — <b>{data['subscribers']:,}</b> подп. ({mention})"
                )

            full = "\n".join(report)
            for i in range(0, len(full), 4000):
                await context.bot.send_message(chat_id, full[i:i+4000], parse_mode="HTML")
    finally:
        active_parses -= 1


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await parsing_step_reply(update, context, "Отменено.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


# ================== АДМИНСКИЕ КОМАНДЫ (доступ) ==================

async def grant_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return  # молча игнорируем — команда не для обычных пользователей

    args = context.args
    if len(args) != 2 or not args[1].isdigit():
        await update.message.reply_text("Формат: /grant <user_id или @username> <дней>")
        return

    target_id, display_name, err = await resolve_user(args[0])
    if err:
        await update.message.reply_text(f"❌ {err}")
        return

    days = int(args[1])
    if days <= 0:
        await update.message.reply_text("Число дней должно быть больше нуля.")
        return

    until = time.time() + days * 86400
    access_grants[str(target_id)] = {'until': until, 'granted_at': time.time()}
    await save_access_async(access_grants)

    until_str = time.strftime('%d.%m.%Y', time.localtime(until))
    await update.message.reply_text(
        f"✅ Доступ выдан {display_name} (id={target_id}) на {days} дн. (до {until_str})."
    )

    try:
        await context.bot.send_message(
            target_id,
            f"🎉 Тебе выдан доступ к парсингу на {days} дн. (до {until_str}).\n"
            f"Нажми «🔍 Новый парсинг» или отправь /parsing, чтобы начать."
        )
    except Exception as e:
        await update.message.reply_text(f"(не смог уведомить пользователя лично: {e})")


async def revoke_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Формат: /revoke <user_id или @username>")
        return

    target_id, display_name, err = await resolve_user(args[0])
    if err:
        await update.message.reply_text(f"❌ {err}")
        return

    if str(target_id) in access_grants:
        del access_grants[str(target_id)]
        await save_access_async(access_grants)
        await update.message.reply_text(f"🚫 Доступ у {display_name} (id={target_id}) отозван.")
    else:
        await update.message.reply_text(f"У {display_name} (id={target_id}) и так не было доступа.")


# ================== ЗАПУСК ==================

async def start_client(client: TelegramClient, name: str):
    # Явная проверка авторизации вместо client.start() — если сессия не авторизована,
    # client.start() уйдёт в интерактивный запрос телефона/кода прямо в консоли и
    # подвесит бота (особенно опасно, если он запущен в фоне без консоли).
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(
            f"Сессия '{name}' не авторизована. Сначала выполни для неё вход через "
            f"login_helper.py (см. README/инструкцию), потом перезапусти бота."
        )
    me = await client.get_me()
    print(f"  [{name}] авторизован как {me.first_name} (id={me.id})")


async def main():
    print(f"Запускаю Telethon-сессии ({len(telethon_clients)})...")
    await asyncio.gather(*(
        start_client(client, name) for client, name in zip(telethon_clients, SESSION_NAMES)
    ))
    print("Все сессии авторизованы!")

    # concurrent_updates: без этого PTB обрабатывает апдейты строго по одному —
    # пока у одного пользователя идёт долгий парсинг, бот не отвечает остальным.
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).build()

    # Главное меню (кнопки) + команды (/…) дублируют одни и те же действия —
    # кому-то удобнее кнопкой, кому-то командой из меню в левом нижнем углу.
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", account))
    app.add_handler(CommandHandler("database", my_database))
    app.add_handler(CommandHandler("tariffs", tariffs))
    app.add_handler(CommandHandler("support", support))
    app.add_handler(CommandHandler("grant", grant_access))
    app.add_handler(CommandHandler("revoke", revoke_access))
    app.add_handler(MessageHandler(filters.Regex("^📁 Моя база каналов$"), my_database))
    app.add_handler(MessageHandler(filters.Regex("^👤 Аккаунт$"), account))
    app.add_handler(MessageHandler(filters.Regex("^💎 Тарифы$"), tariffs))
    app.add_handler(MessageHandler(filters.Regex("^🛠 Поддержка$"), support))

    # Диалог парсинга — запустить можно кнопкой или командой /parsing
    conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^🔍 Новый парсинг$"), new_parsing),
            CommandHandler("parsing", new_parsing),
        ],
        states={
            CHANNELS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_channels)],
            POSTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_posts)],
            SUBS_RANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_subs_range)],
        },
        fallbacks=[
            MessageHandler(filters.Regex("^❌ Отмена$"), cancel),
            CommandHandler("cancel", cancel),
        ],
        allow_reentry=True
    )
    app.add_handler(conv_handler)

    # Меню команд в левом нижнем углу чата (кнопка "Меню" рядом с полем ввода)
    await app.bot.set_my_commands([
        BotCommand("start", "Запустить бота"),
        BotCommand("parsing", "Новый парсинг"),
        BotCommand("status", "Статус аккаунта"),
        BotCommand("database", "Моя база каналов"),
        BotCommand("tariffs", "Тарифы и доступ"),
        BotCommand("support", "Поддержка"),
        BotCommand("cancel", "Остановить анализ"),
    ])

    print("Официальный бот запускается...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    print("Бот успешно запущен!")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())