import asyncio
import csv
import json
import re
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Optional, Dict

from telethon import TelegramClient, errors, functions
from telethon.tl import types

# ---- UI COLORS ----
G = "\033[92m"  # Green
Y = "\033[93m"  # Yellow
R = "\033[91m"  # Red
B = "\033[94m"  # Blue
C = "\033[96m"  # Cyan
W = "\033[0m"   # White/Reset
BOLD = "\033[1m"

def banner():
    print(f"{B}{BOLD}" + "="*50)
    print(f"   TELEGRAM PREMIUM SENDER v2.0")
    print(f"="*50 + f"{W}\n")

# ==== CREDENTIALS FROM FILE ====
CRED_FILE = Path(__file__).with_name("credentials.json")

if not CRED_FILE.exists():
    print(f"{R}❌ credentials.json not found.{W}")
    sys.exit(1)

data = json.loads(CRED_FILE.read_text(encoding="utf-8"))

try:
    API_ID = int(data["API_ID"])
    API_HASH = str(data["API_HASH"])
    SESSION = str(data.get("SESSION", "premium_sender"))
except KeyError as e:
    print(f"{R}❌ Missing key in credentials.json: {e}{W}")
    sys.exit(1)

# ----- PROXY LOADER -----
PROXY_FILE = Path(__file__).with_name("proxies.txt")

def load_proxy_from_file():
    if not PROXY_FILE.exists(): return None
    lines = [x.strip() for x in PROXY_FILE.read_text(encoding="utf-8").splitlines() if x.strip()]
    if not lines: return None
    line = random.choice(lines)
    parts = line.split(":")
    if len(parts) < 4: return None
    print(f"{C}🌐 Proxy: {parts[0]}:{parts[1]}{W}")
    return ("socks5", parts[0], int(parts[1]), True, parts[2], parts[3])

proxy = load_proxy_from_file()
SENT_DB_FILE = Path("sent_db.json")
SENT_LOG_FILE = Path("sent_log.csv")
spambot_checks_count = 0

# ---------- Logic Functions ----------
def read_message_template() -> str:
    path = Path("message.txt")
    if not path.exists(): raise FileNotFoundError("message.txt not found")
    return path.read_text(encoding="utf-8").rstrip("\n")

def fill_firstname(template: str, first_name: Optional[str]) -> str:
    return re.sub(r"\[Firstname\]", (first_name or "there"), template, flags=re.IGNORECASE)

def normalize_for_compare(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()

async def already_sent_similar(client: TelegramClient, entity, text: str, limit: int = 400) -> bool:
    target_norm = normalize_for_compare(text)
    async for msg in client.iter_messages(entity, from_user="me", limit=limit):
        if normalize_for_compare(getattr(msg, "message", "")) == target_norm:
            return True
    return False

def load_sent_db() -> Dict:
    if SENT_DB_FILE.exists():
        try: return json.loads(SENT_DB_FILE.read_text(encoding="utf-8"))
        except: pass
    return {"users": {}}

def save_sent_db(db: Dict) -> None:
    SENT_DB_FILE.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

def mark_user_sent(db: Dict, user: types.User, account_id: int) -> None:
    now = int(datetime.now(timezone.utc).timestamp())
    key = str(user.id)
    u = db["users"].get(key, {})
    u["usernames"] = list(set((u.get("usernames", [])) + ([user.username] if user.username else [])))
    u["last_sent"] = now
    u.setdefault("by_accounts", []).append(account_id)
    db["users"][key] = u

async def check_spambot_and_status(client: TelegramClient) -> bool:
    global spambot_checks_count
    try:
        await client.send_message("SpamBot", "/start")
        await asyncio.sleep(2)
        async for m in client.iter_messages("SpamBot", limit=1):
            text = m.message or ""
            if "no limits" in text.lower() or "free from any" in text.lower():
                spambot_checks_count += 1
                if spambot_checks_count >= 2:
                    print(f"\n{R}{BOLD}🛑 EXIT: SpamBot confirmed no limits twice. Closing script.{W}")
                    await client.disconnect()
                    sys.exit(0)
                return False
            spambot_checks_count = 0
            return True
        return False
    except: return False

async def fetch_all_pending_global(client: TelegramClient, channel: types.Channel) -> List[types.User]:
    all_users = []
    offset_date = 0
    offset_user = types.InputUserEmpty()
    print(f"{C}🔄 Fetching all join requests...{W}")
    while True:
        res = await client(functions.messages.GetChatInviteImportersRequest(
            peer=channel, requested=True, offset_date=offset_date, offset_user=offset_user, limit=100
        ))
        if not res.users: break
        all_users.extend(res.users)
        last_imp = res.importers[-1]
        offset_date = last_imp.date
        last_u = next((u for u in res.users if u.id == last_imp.user_id), None)
        if last_u: offset_user = types.InputUser(user_id=last_u.id, access_hash=last_u.access_hash)
        else: break
        if len(res.importers) < 100: break
    return all_users

# ---------- Main Loop ----------
async def main():
    banner()
    client = TelegramClient(SESSION, API_ID, API_HASH, proxy=proxy)
    await client.start(password=lambda: input(f"{Y}Enter 2FA: {W}"))
    
    me = await client.get_me()
    print(f"{G}✅ Logged in as: {BOLD}{me.first_name}{W}\n")

    print(f"{B}Choose Source:{W}")
    print(f" 1) Contacts\n 2) Join Requests\n 3) Channel Subscribers")
    choice = input(f"{BOLD}Selection: {W}").strip()

    recipients: List[types.User] = []
    if choice == "2":
        dialogs = await client.get_dialogs()
        admins = [d.entity for d in dialogs if isinstance(d.entity, types.Channel) and (d.entity.creator or d.entity.admin_rights)]
        for i, ch in enumerate(admins, 1): print(f" {i}) {ch.title}")
        sel = int(input(f"{BOLD}Select Channel: {W}")) - 1
        recipients = await fetch_all_pending_global(client, admins[sel])
    elif choice == "3":
        dialogs = await client.get_dialogs()
        admins = [d.entity for d in dialogs if isinstance(d.entity, types.Channel) and (d.entity.creator or d.entity.admin_rights)]
        for i, ch in enumerate(admins, 1): print(f" {i}) {ch.title}")
        sel = int(input(f"{BOLD}Select Channel: {W}")) - 1
        async for u in client.iter_participants(admins[sel]):
            if isinstance(u, types.User) and not (u.bot or u.deleted or u.id == me.id): recipients.append(u)
    else:
        res = await client(functions.contacts.GetContactsRequest(hash=0))
        recipients = [u for u in res.users if not (u.bot or u.deleted or u.id == me.id)]

    if not recipients:
        print(f"{R}No users found.{W}")
        return

    total_all = len(recipients)
    limit_val = input(f"{Y}Total found: {total_all}. Limit to how many? (Enter for all): {W}")
    if limit_val: recipients = recipients[:int(limit_val)]
    
    total_run = len(recipients)
    base_delay = float(input(f"{Y}Set base interval (min 1.0): {W}") or 1.0)
    if base_delay < 1.0: base_delay = 1.0

    sent_db = load_sent_db()
    message_template = read_message_template()

    print(f"\n{G}{BOLD}🚀 Starting Sender...{W}\n")

    for i, user in enumerate(recipients, 1):
        label = f"@{user.username}" if user.username else (user.first_name or "Unknown")
        counter = f"{B}[{i}/{total_run}]{W}"
        
        if str(user.id) in sent_db.get("users", {}):
            print(f"{counter} {Y}⏭️ Skipping {label} (Already in DB){W}")
            continue

        try:
            msg = fill_firstname(message_template, user.first_name)
            if await already_sent_similar(client, user, msg):
                print(f"{counter} {Y}⏭️ Skipping {label} (Similar sent recently){W}")
                mark_user_sent(sent_db, user, me.id)
                continue

            await client.send_message(user, msg)
            print(f"{counter} {G}✅ Sent to {BOLD}{label}{W}")
            
            mark_user_sent(sent_db, user, me.id)
            save_sent_db(sent_db)

            if i < total_run:
                delay = random.uniform(base_delay, base_delay + 1.5)
                print(f"   {C}⏳ Waiting for {delay:.1f}s...{W}")
                await asyncio.sleep(delay)

        except (errors.FloodWaitError, errors.RPCError) as e:
            print(f"\n{counter} {R}⚠️ Error: {e}{W}")
            is_limited = await check_spambot_and_status(client)
            if isinstance(e, errors.FloodWaitError):
                print(f"{Y}Sleeping for {e.seconds}s...{W}")
                await asyncio.sleep(e.seconds)
        except Exception as e:
            print(f"{counter} {R}❌ Fail: {e}{W}")

    print(f"\n{G}{BOLD}🏁 All Done!{W}")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
