"""
Авторизация дополнительной Telethon-сессии (второй, третий... аккаунт для парсинга).
Логин двухшаговый (Telegram присылает код на телефон, поэтому один процесс не подходит):

  1) python login_helper.py send-code <session_name> <phone>
     Пример: python login_helper.py send-code parser_session_2 +79991234567
     -> Telegram пришлёт код в приложение/смс на этот номер.

  2) python login_helper.py sign-in <code> [password]
     Пример: python login_helper.py sign-in 12345
     Если у аккаунта включена облачная 2FA-защита, скрипт попросит пароль отдельным
     сообщением NEED_PASSWORD — тогда повтори с паролем вторым аргументом.

После успешного входа сессия сохраняется в <session_name>.session — её имя нужно
добавить в SESSION_NAMES в .env (через запятую), чтобы бот начал её использовать.
"""
import sys
import os
import json
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

load_dotenv()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]

STATE_FILE = ".login_state.json"


async def send_code(session_name: str, phone: str):
    client = TelegramClient(session_name, API_ID, API_HASH)
    await client.connect()
    if await client.is_user_authorized():
        print(f"ALREADY_AUTHORIZED: сессия '{session_name}' уже авторизована, ничего делать не нужно.")
        await client.disconnect()
        return

    sent = await client.send_code_request(phone)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "session_name": session_name,
            "phone": phone,
            "phone_code_hash": sent.phone_code_hash,
        }, f)
    print(f"CODE_SENT: код отправлен на {phone}. Дальше выполни: python login_helper.py sign-in <код>")
    await client.disconnect()


async def sign_in(code: str, password: str = None):
    if not os.path.exists(STATE_FILE):
        print("ERROR: сначала выполни send-code.")
        return

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        state = json.load(f)

    client = TelegramClient(state["session_name"], API_ID, API_HASH)
    await client.connect()
    try:
        try:
            await client.sign_in(
                phone=state["phone"], code=code, phone_code_hash=state["phone_code_hash"]
            )
        except SessionPasswordNeededError:
            if not password:
                print("NEED_PASSWORD: на аккаунте включена 2FA. Повтори: "
                      "python login_helper.py sign-in <код> <пароль>")
                return
            await client.sign_in(password=password)

        me = await client.get_me()
        print(f"AUTHORIZED: вошли как {me.first_name} (@{me.username}, id={me.id}), "
              f"сессия сохранена в {state['session_name']}.session")
        os.remove(STATE_FILE)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1]
    if action == "send-code":
        asyncio.run(send_code(sys.argv[2], sys.argv[3]))
    elif action == "sign-in":
        pwd = sys.argv[3] if len(sys.argv) > 3 else None
        asyncio.run(sign_in(sys.argv[2], pwd))
    else:
        print(__doc__)
        sys.exit(1)
