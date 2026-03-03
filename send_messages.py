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

# ==== CREDENTIALS FROM FILE ====
CRED_FILE = Path(__file__).with_name("credentials.json")

if not CRED_FILE.exists():
    raise FileNotFoundError("credentials.json not found.")

data = json.loads(CRED_FILE.read_text(encoding="utf-8"))

try:
    API_ID = int(data["API_ID"])
    API_HASH = str(data["API_HASH"])
    SESSION = str(data.get("SESSION", "premium_sender"))
except KeyError as e:
    raise KeyError(f"Missing key in credentials.json: {e}")

# ----- PROXY LOADER -----
PROXY_FILE = Path(__file__).with_name("proxies.txt")

def load_proxy_from_file():
    if not PROXY_FILE.exists():
        return None
    lines = [x.strip() for x in PROXY_FILE.read_text(encoding="utf-8").splitlines() if x.strip()]
    if not lines:
        return None
    line = random.choice(lines)
    parts = line.split(":")
    if len(parts) < 4:
        return None
    return ("socks5", parts[0], int(parts[1]), True, parts[2], parts[3])

proxy = load_proxy_from_file()

# Persistent stores
SENT_DB_FILE = Path("sent_db.json")
SENT_LOG_FILE = Path("sent_log.csv")

# Global counter for SpamBot logic
spambot_checks_count = 0

# ---------- Message template ----------
def read_message_template() -> str:
    path = Path("message.txt")
    if not path.exists():
        raise FileNotFoundError("message.txt not found")
    return path.read_text(encoding="utf-8").rstrip("\n")

def fill_firstname(template: str, first_name: Optional[str]) -> str:
    return re.sub(r"\[Firstname\]", (first_name or "there"), template, flags=re.IGNORECASE)

# ---------- Duplicate-check normalization ----------
def normalize_for_compare(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()

async def already_sent_similar(client: TelegramClient, entity, text: str, limit: int = 400) -> bool:
    target_norm = normalize_for_compare(text)
    async for msg in client.iter_messages(entity, from_user="me", limit=limit):
        if normalize_for_compare(getattr(msg, "message", "")) == target_norm:
            return True
    return False

# ---------- Global "do-not-message" registry ----------
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

def user_already_sent_global(db: Dict, user_id: int) -> bool:
    return str(user_id) in db.get("users", {})

def append_sent_log_row(account_id: int, user: types.User, label: str) -> None:
    new_file = not SENT_LOG_FILE.exists()
    with SENT_LOG_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["utc_time", "account_id", "user_id", "label", "username"])
        w.writerow([datetime.now(timezone.utc).isoformat(), account_id, user.id, label, user.username or ""])

# ---------- Presence Helpers ----------
def presence_bucket(user: types.User) -> int:
    st = user.status
    if isinstance(st, types.UserStatusOnline): return 0
    if isinstance(st, types.UserStatusRecently): return 1
    if isinstance(st, types.UserStatusOffline): return 2
    if isinstance(st, types.UserStatusLastWeek): return 3
    if isinstance(st, types.UserStatusLastMonth): return 4
    return 5

def last_seen_ts(user: types.User) -> float:
    st = user.status
    if isinstance(st, types.UserStatusOffline):
        return getattr(st, "was_online", datetime.min).timestamp()
    return datetime.now(timezone.utc).timestamp() if isinstance(st, types.UserStatusOnline) else 0.0

def human_last_seen(user: types.User) -> str:
    st = user.status
    if isinstance(st, types.UserStatusOnline): return "online"
    if isinstance(st, types.UserStatusRecently): return "recently"
    return "offline/hidden"

def display_tag(user: types.User) -> str:
    return f"@{user.username}" if user.username else (user.first_name or "Unknown")

# ---------- SpamBot logic ----------
async def check_spambot_and_status(client: TelegramClient) -> bool:
    """
    Returns True if limits were found and potentially cleared.
    Closes script if no limits found twice in a row.
    """
    global spambot_checks_count
    try:
        await client.send_message("SpamBot", "/start")
        await asyncio.sleep(2)
        async for m in client.iter_messages("SpamBot", limit=1):
            text = m.message or ""
            if "no limits" in text.lower() or "free from any" in text.lower():
                spambot_checks_count += 1
                if spambot_checks_count >= 2:
                    print("🛑 No limits reported by SpamBot twice. Closing script.")
                    await client.disconnect()
                    sys.exit(0)
                return False
            
            # Reset counter if limits actually exist
            spambot_checks_count = 0
            
            match = re.search(r"limited until\s+(.*?)\s+UTC", text, re.IGNORECASE)
            if match:
                print(f"⚠️ Account is limited until {match.group(1)} UTC")
                return False
        return False
    except Exception as e:
        print(f"Error checking SpamBot: {e}")
        return False

# ---------- Join Requests Logic ----------
async def list_admin_channels(client: TelegramClient) -> List[types.Channel]:
    admin_channels = []
    async for d in client.iter_dialogs():
        if isinstance(d.entity, types.Channel) and (d.entity.creator or d.entity.admin_rights):
            admin_channels.append(d.entity)
    return admin_channels

async def count_pending_requests_for_channel(client: TelegramClient, channel: types.Channel) -> int:
    try:
        res = await client(functions.messages.GetChatInviteImportersRequest(
            peer=channel, requested=True, limit=1
        ))
        return getattr(res, "count", 0)
    except: return 0

async def fetch_all_pending_global(client: TelegramClient, channel: types.Channel) -> List[types.User]:
    """Fetches ALL pending requests by paginating until end."""
    all_users = []
    offset_date = 0
    offset_user = types.InputUserEmpty()
    
    print("🔄 Fetching all pending requests (this may take a moment)...")
    while True:
        res = await client(functions.messages.GetChatInviteImportersRequest(
            peer=channel,
            requested=True,
            offset_date=offset_date,
            offset_user=offset_user,
            limit=100
        ))
        
        if not res.users:
            break
            
        all_users.extend(res.users)
        
        # Prepare for next page
        last_importer = res.importers[-1]
        offset_date = last_importer.date
        
        # Find the user object for the last importer to get access_hash
        last_user = next((u for u in res.users if u.id == last_importer.user_id), None)
        if last_user:
            offset_user = types.InputUser(user_id=last_user.id, access_hash=last_user.access_hash)
        else:
            break # Should not happen

        if len(res.importers) < 100:
            break
            
    # Remove duplicates and sort by ID (as proxy for join order) or leave as is for "recent first"
    return all_users

# ---------- Main Execution ----------
async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH, proxy=proxy)
    await client.start(password=lambda: input("Enter 2FA password (if enabled): "))
    
    me = await client.get_me()
    my_id = me.id

    print("\n1) Contacts | 2) Join Requests | 3) Subscribers")
    choice = input("Choice: ").strip()

    message_template = read_message_template()
    recipients: List[types.User] = []

    if choice == "2":
        channels = await list_admin_channels(client)
        for i, ch in enumerate(channels, 1):
            print(f"{i}) {ch.title}")
        sel = int(input("Select channel: ")) - 1
        target_ch = channels[sel]
        recipients = await fetch_all_pending_global(client, target_ch)
    elif choice == "3":
        channels = await list_admin_channels(client)
        for i, ch in enumerate(channels, 1): print(f"{i}) {ch.title}")
        sel = int(input("Select channel: ")) - 1
        async for u in client.iter_participants(channels[sel]):
            if isinstance(u, types.User) and not (u.bot or u.deleted or u.id == my_id):
                recipients.append(u)
    else:
        res = await client(functions.contacts.GetContactsRequest(hash=0))
        recipients = [u for u in res.users if not (u.bot or u.deleted or u.id == my_id)]

    if not recipients:
        print("No recipients found.")
        return

    # Sort recipients (Choice 2 is already mostly chronological from API)
    if choice != "2":
        recipients.sort(key=lambda u: (presence_bucket(u), -last_seen_ts(u)))

    print(f"Total recipients: {len(recipients)}")
    limit_val = input("Limit (Enter for all): ")
    if limit_val:
        recipients = recipients[:int(limit_val)]

    # Delay setting
    while True:
        try:
            base_delay = float(input("\nSet base interval (min 1.0s): ").strip())
            if base_delay < 1.0:
                print("❌ Min 1 second.")
                continue
            break
        except: pass

    sent_db = load_sent_db()
    successes = 0

    for i, user in enumerate(recipients):
        label = display_tag(user)
        print(f"[{i+1}/{len(recipients)}] Processing {label}...")

        if user_already_sent_global(sent_db, user.id):
            print(f"⏭️ Already in DB.")
            continue

        try:
            msg = fill_firstname(message_template, user.first_name)
            
            if await already_sent_similar(client, user, msg):
                print(f"⏭️ Similar message exists.")
                mark_user_sent(sent_db, user, my_id)
                continue

            await client.send_message(user, msg)
            print(f"✅ Sent.")
            successes += 1
            
            mark_user_sent(sent_db, user, my_id)
            save_sent_db(sent_db)
            append_sent_log_row(my_id, user, label)

            await asyncio.sleep(random.uniform(base_delay, base_delay + 2))

        except (errors.FloodWaitError, errors.RPCError) as e:
            print(f"⚠️ Error/Wait: {e}")
            await check_spambot_status(client)
            if isinstance(e, errors.FloodWaitError):
                await asyncio.sleep(e.seconds)
        except Exception as e:
            print(f"❌ Failed: {e}")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
