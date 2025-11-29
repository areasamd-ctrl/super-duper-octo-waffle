import asyncio
import csv
import json
import re
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Optional, Dict

from telethon import TelegramClient, errors, functions
from telethon.tl import types

# ==== HARD-CODED CREDENTIALS (as requested) ====
API_ID = 31868724
API_HASH = "62dbabacb3190bfed893d44727b24dc2"
SESSION = "premium_sender"
# ===============================================

# Persistent stores
SENT_DB_FILE = Path("sent_db.json")
SENT_LOG_FILE = Path("sent_log.csv")

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
        try:
            return json.loads(SENT_DB_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"users": {}}

def save_sent_db(db: Dict) -> None:
    SENT_DB_FILE.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

def mark_user_sent(db: Dict, user: types.User, account_id: int) -> None:
    now = int(datetime.now(timezone.utc).timestamp())
    key = str(user.id)
    u = db["users"].get(key)
    if not u:
        db["users"][key] = {
            "usernames": [user.username] if user.username else [],
            "first_seen": now,
            "last_sent": now,
            "by_accounts": [account_id]
        }
    else:
        if user.username and user.username not in (u.get("usernames") or []):
            u.setdefault("usernames", []).append(user.username)
        u["last_sent"] = now
        if account_id not in (u.get("by_accounts") or []):
            u.setdefault("by_accounts", []).append(account_id)

def user_already_sent_global(db: Dict, user_id: int) -> bool:
    return str(user_id) in db.get("users", {})

def append_sent_log_row(account_id: int, user: types.User, label: str) -> None:
    new_file = not SENT_LOG_FILE.exists()
    with SENT_LOG_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["utc_time", "account_id", "user_id", "label", "username"])
        w.writerow([
            datetime.now(timezone.utc).isoformat(),
            account_id,
            user.id,
            label,
            user.username or ""
        ])

# ---------- Presence & ordering ----------
def _to_timestamp(dt_or_int) -> float:
    if isinstance(dt_or_int, datetime):
        return dt_or_int.timestamp()
    if isinstance(dt_or_int, (int, float)):
        return float(dt_or_int)
    return 0.0

def presence_bucket(user: types.User) -> int:
    """
    Priority buckets:
    0: Online
    1: Recently
    2: Exact last-seen (offline with timestamp)
    3: Last week
    4: Last month
    5: Unknown/hidden
    """
    st = user.status
    if isinstance(st, types.UserStatusOnline):
        return 0
    if isinstance(st, types.UserStatusRecently):
        return 1
    if isinstance(st, types.UserStatusOffline):
        return 2
    if isinstance(st, types.UserStatusLastWeek):
        return 3
    if isinstance(st, types.UserStatusLastMonth):
        return 4
    return 5

def last_seen_ts(user: types.User) -> float:
    st = user.status
    if isinstance(st, types.UserStatusOffline):
        return _to_timestamp(getattr(st, "was_online", None))
    if isinstance(st, types.UserStatusOnline):
        return datetime.now(timezone.utc).timestamp()
    return 0.0

def human_last_seen(user: types.User) -> str:
    st = user.status
    if isinstance(st, types.UserStatusOnline):
        return "online"
    if isinstance(st, types.UserStatusOffline):
        was: Optional[datetime] = getattr(st, "was_online", None)
        if isinstance(was, datetime):
            delta = datetime.now(timezone.utc) - was
            secs = int(delta.total_seconds())
            if secs < 60: return f"{secs}s ago"
            mins = secs // 60
            if mins < 60: return f"{mins}m ago"
            hours = mins // 60
            if hours < 24: return f"{hours}h ago"
            days = hours // 24
            return f"{days}d ago"
        return "last seen (unknown)"
    if isinstance(st, types.UserStatusRecently):
        return "recently"
    if isinstance(st, types.UserStatusLastWeek):
        return "last week"
    if isinstance(st, types.UserStatusLastMonth):
        return "last month"
    return "unknown"

def display_tag(user: types.User) -> str:
    if user.username:
        return f"@{user.username}"
    if user.first_name:
        return user.first_name
    if user.phone:
        return user.phone
    return "Unknown"

# ---------- SpamBot checker on FloodWait ----------
LIMIT_MSG_RE = re.compile(
    r"limited until\s+(\d{1,2}\s+\w+\s+\d{4}),\s*(\d{1,2}:\d{2})\s+UTC",
    re.IGNORECASE
)

async def check_spambot_and_maybe_clear_limit(client: TelegramClient) -> bool:
    """
    DM /start to @SpamBot, parse the 'limited until ... UTC' line.
    If current UTC minute equals release minute AND now >= release time,
    send 'OK' and return True (continue immediately). Else return False.
    """
    try:
        await client.send_message("SpamBot", "/start")
        await asyncio.sleep(1.5)
        async for m in client.iter_messages("SpamBot", limit=1):
            text = m.message or ""
            match = LIMIT_MSG_RE.search(text)
            if not match:
                return False
            date_str, time_str = match.groups()
            dt = None
            for fmt in ("%d %b %Y %H:%M", "%d %B %Y %H:%M"):
                try:
                    dt = datetime.strptime(f"{date_str} {time_str}", fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    pass
            if not dt:
                return False
            now_utc = datetime.now(timezone.utc)
            if now_utc.minute == dt.minute and now_utc >= dt:
                await client.send_message("SpamBot", "OK")
                await asyncio.sleep(1.0)
                return True
            return False
    except Exception:
        return False

# ---------- Admin channels & GLOBAL pending join requests (no invite links) ----------
async def list_admin_channels(client: TelegramClient) -> List[types.Channel]:
    admin_channels: List[types.Channel] = []
    async for d in client.iter_dialogs():
        ent = d.entity
        if isinstance(ent, types.Channel) and d.is_channel:
            if ent.creator or ent.admin_rights:
                admin_channels.append(ent)
    return admin_channels

async def count_pending_requests_for_channel(client: TelegramClient, channel: types.Channel) -> int:
    try:
        res = await client(functions.messages.GetChatInviteImportersRequest(
            peer=channel,
            requested=True,
            offset_date=0,
            offset_user=types.InputUserEmpty(),
            limit=1
        ))
        return int(getattr(res, "count", 0) or 0)
    except errors.RPCError:
        return 0

async def fetch_all_pending_global(
    client: TelegramClient,
    channel: types.Channel,
    page_size: int = 200
) -> Tuple[List[types.User], Dict[int, int]]:
    """
    Fetch ALL pending join requests (global list, no link).
    Returns (users, importer_dates) where importer_dates[user_id] = unix_ts of request time.
    """
    users_by_id: Dict[int, types.User] = {}
    request_ts: Dict[int, int] = {}

    offset_date: int = 0
    offset_user = types.InputUserEmpty()

    while True:
        res = await client(functions.messages.GetChatInviteImportersRequest(
            peer=channel,
            requested=True,
            offset_date=offset_date,
            offset_user=offset_user,
            limit=page_size
        ))

        # Map users
        for u in res.users:
            users_by_id[u.id] = u

        importers = getattr(res, "importers", []) or []
        if not importers:
            break

        # capture importer request time
        for imp in importers:
            u_id = getattr(imp, "user_id", None)
            d = getattr(imp, "date", None)
            if isinstance(d, datetime):
                ts = int(d.replace(tzinfo=timezone.utc).timestamp())
            else:
                ts = int(d or 0)
            if u_id:
                request_ts[u_id] = ts

        last = importers[-1]
        last_date = getattr(last, "date", None)
        if isinstance(last_date, datetime):
            offset_date = int(last_date.replace(tzinfo=timezone.utc).timestamp())
        else:
            offset_date = int(last_date or 0)

        next_id = getattr(last, "user_id", None)
        next_hash = 0
        for u in res.users:
            if u.id == next_id:
                next_hash = getattr(u, "access_hash", 0)
                break
        if next_id and next_hash:
            offset_user = types.InputUser(user_id=next_id, access_hash=next_hash)
        else:
            offset_user = types.InputUserEmpty()

        if len(importers) < page_size:
            break

    return list(users_by_id.values()), request_ts

def sort_requesters(users: List[types.User], importer_ts: Dict[int, int]) -> List[types.User]:
    """
    Final ordering:
      1) Presence bucket (Online -> Recently -> Exact last-seen -> Last week -> Last month -> Unknown)
      2) Within same presence bucket, sort by importer date DESC (most recent request first)
      3) As a tie-breaker for the 'Exact last-seen' bucket, use last-seen DESC
    """
    def key(u: types.User):
        bucket = presence_bucket(u)
        req_ts = importer_ts.get(u.id, 0)
        ls_ts = last_seen_ts(u)
        # sort: bucket asc, req_ts desc, ls_ts desc
        return (bucket, -req_ts, -ls_ts)

    return sorted(users, key=key)

# ---------- Main ----------
async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()  # phone + code on first run

    me = await client.get_me()
    my_id = me.id if isinstance(me, types.User) else 0

    print("\nChoose recipients source:")
    print("  1) My Telegram contacts (sorted by presence)")
    print("  2) Pending join requests (global list, most recent first)")
    choice = input("Enter 1 or 2: ").strip()

    # Load message (exact formatting preserved)
    message_template = read_message_template()

    recipients_entities: List[types.User] = []

    if choice == "2":
        # ----- Global Join Requests (no invite links) -----
        admin_channels = await list_admin_channels(client)
        if not admin_channels:
            print("❌ No admin channels found on this account.")
            await client.disconnect()
            return

        # Show channels with accurate pending counts
        channel_info = []
        print("\nChecking channels for pending join requests…")
        for ch in admin_channels:
            pending = await count_pending_requests_for_channel(client, ch)
            title = getattr(ch, "title", "Unnamed Channel")
            print(f"  - {title}: {pending} pending")
            channel_info.append((ch, pending))

        print("\nSelect a channel:")
        for i, (ch, pending) in enumerate(channel_info, start=1):
            title = getattr(ch, "title", "Unnamed Channel")
            print(f"  {i}) {title} ({pending} requests)")
        sel = input("Enter number: ").strip()
        try:
            sel_idx = max(1, int(sel)) - 1
            ch, _pending = channel_info[sel_idx]
        except Exception:
            print("❌ Invalid selection.")
            await client.disconnect()
            return

        # Fetch all pending (global) + sort by your priority
        try:
            req_users, importer_ts = await fetch_all_pending_global(client, ch)
        except errors.RPCError as e:
            print(f"❌ Could not fetch pending requests: {e}")
            await client.disconnect()
            return

        if not req_users:
            print("ℹ️ No pending join requests found for that channel.")
            await client.disconnect()
            return

        recipients_entities = sort_requesters(req_users, importer_ts)
        print(f"\nQueued {len(recipients_entities)} requester(s). Top 10:")
        preview = "\n".join([f"  - {display_tag(u)} ({human_last_seen(u)})" for u in recipients_entities[:10]])
        print(preview if preview else "  (none)")

    else:
        # ----- My Contacts (presence order) -----
        result = await client(functions.contacts.GetContactsRequest(hash=0))
        raw_contacts: List[types.User] = result.users

        cleaned: List[types.User] = []
        for u in raw_contacts:
            if not isinstance(u, types.User):
                continue
            if u.bot or u.deleted:
                continue
            if me and u.id == my_id:
                continue
            cleaned.append(u)

        # presence priority, then last-seen desc within exact bucket
        def contact_key(u: types.User):
            return (presence_bucket(u), -last_seen_ts(u))
        recipients_entities = sorted(cleaned, key=contact_key)

        preview = "\n".join([f"  - {display_tag(u)} ({human_last_seen(u)})" for u in recipients_entities[:10]])
        print(f"\nTop of the queue:\n{preview}\n...")

        limit_str = input("Limit how many to message this run? (Enter for all): ").strip()
        if limit_str:
            try:
                limit = max(0, int(limit_str))
                recipients_entities = recipients_entities[:limit]
            except ValueError:
                pass

    # Load / init global DB
    sent_db = load_sent_db()

    # Delay input (base + random up to +15)
    while True:
        try:
            base_delay = float(input("\nSet base interval in seconds (min 120): ").strip())
            if base_delay < 120:
                print("❌ Must be at least 120 seconds.")
                continue
            break
        except ValueError:
            print("❌ Enter a valid number, e.g., 120")

    min_delay = base_delay
    max_delay = base_delay + 15
    print(f"\n💤 Each message will delay randomly between {min_delay:.0f}s and {max_delay:.0f}s.\n")

    recipients_iter = [(u, display_tag(u)) for u in recipients_entities]
    total = len(recipients_iter)

    successes = 0
    failures = 0
    skips_chatdup = 0
    skips_global = 0

    for idx in range(total):
        print(f"[{idx+1}/{total}] Preparing…")
        try:
            user, label = recipients_iter[idx]
            entity = user

            # Global "already messaged" guard (across all accounts)
            if user_already_sent_global(sent_db, user.id):
                print(f"⏭️ Skipped {label} (in global sent_db)")
                skips_global += 1
                await asyncio.sleep(5)
                continue

            # Personalize & duplicate-check (per-chat)
            first_name = entity.first_name if isinstance(entity, types.User) else None
            msg = fill_firstname(read_message_template(), first_name)

            if await already_sent_similar(client, entity, msg, limit=400):
                print(f"⏭️ Skipped {label} (similar message already sent in this chat)")
                skips_chatdup += 1
                await asyncio.sleep(5)
                # Still mark globally to protect other accounts
                mark_user_sent(sent_db, user, my_id)
                save_sent_db(sent_db)
                append_sent_log_row(my_id, user, label)
                continue

            # Send
            await client.send_message(entity, msg)
            print(f"✅ Sent to {label} ({human_last_seen(entity)})")
            successes += 1

            # Update global DB & log
            mark_user_sent(sent_db, user, my_id)
            save_sent_db(sent_db)
            append_sent_log_row(my_id, user, label)

            # Randomized delay after successful sends
            if idx < total - 1:
                delay = random.uniform(min_delay, max_delay)
                print(f"⏳ Waiting {delay:.1f}s before next message…")
                await asyncio.sleep(delay)

        except errors.FloodWaitError as e:
            print(f"⚠️ FloodWait for {e.seconds}s detected. Checking @SpamBot…")
            cleared = await check_spambot_and_maybe_clear_limit(client)
            if cleared:
                print("✅ SpamBot says limits lifted — continuing.")
                continue
            else:
                wait_time = int(e.seconds) + 1
                print(f"⏳ Waiting {wait_time}s due to FloodWait…")
                await asyncio.sleep(wait_time)

        except Exception as e:
            print(f"❌ Failed: {e}")
            failures += 1
            if idx < total - 1:
                delay = random.uniform(min_delay, max_delay)
                print(f"⏳ Waiting {delay:.1f}s before next message…")
                await asyncio.sleep(delay)

    print(
        f"\n✅ Done. Sent: {successes}, "
        f"Skipped (global): {skips_global}, "
        f"Skipped (chat-duplicate): {skips_chatdup}, "
        f"Failed: {failures}"
    )
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
