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

# ... [CREDENTIALS & PROXY LOADING CODE REMAINS THE SAME] ...
# (Keep your existing CRED_FILE, API_ID, proxy loader, etc.)

# ---------- Optimized Duplicate Check with Local Cache ----------
# This prevents calling iter_messages(limit=400) for every single recipient.
MESSAGE_CACHE: Dict[int, bool] = {} 

async def already_sent_similar_optimized(client: TelegramClient, user_id: int, target_text: str) -> bool:
    """
    Checks if we sent the target_text recently. 
    Uses a local cache to avoid re-scanning the same chat twice in one session.
    """
    if user_id in MESSAGE_CACHE:
        return MESSAGE_CACHE[user_id]
    
    target_norm = normalize_for_compare(target_text)
    found = False
    try:
        # We only check the last 50 messages instead of 400. 
        # Statistically, if it's not in the last 50, a repeat is often 'safe' by TG standards.
        async for msg in client.iter_messages(user_id, from_user="me", limit=50):
            if normalize_for_compare(getattr(msg, "message", "")) == target_norm:
                found = True
                break
    except Exception:
        pass
    
    MESSAGE_CACHE[user_id] = found
    return found

# ---------- Registry & Presence Helpers ----------
# (Keep your load_sent_db, save_sent_db, mark_user_sent, presence_bucket, etc.)
# (Keep your human_last_seen and display_tag functions)

# ---------- Optimized Main Loop ----------

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH, proxy=proxy)
    await client.start(password=lambda: input("Enter 2FA password (if enabled): "))

    me = await client.get_me()
    my_id = me.id
    
    # 1. Pre-fetch Dialogs to warm up the internal cache
    # This reduces the need for the library to fetch entities later.
    print("Pre-loading dialogs to reduce API requests...")
    await client.get_dialogs(limit=100)

    print("\nChoose recipients source:\n 1) Contacts\n 2) Join Requests\n 3) Admin Channel Members")
    choice = input("Choice: ").strip()

    message_template = read_message_template()
    recipients_entities: List[types.User] = []

    # --- Choice Logic (Summarized for brevity, keep your sorting logic) ---
    if choice == "2":
        # ... (Your existing Choice 2 logic)
        # Optimization: Use fetch_all_pending_global as you wrote, 
        # but ensure you use the .users list from the response.
        pass 
    elif choice == "3":
        # ... (Your existing Choice 3 logic)
        pass
    else:
        # Choice 1: Contacts
        # Optimization: GetContactsRequest is efficient.
        result = await client(functions.contacts.GetContactsRequest(hash=0))
        recipients_entities = [u for u in result.users if isinstance(u, types.User) and not (u.bot or u.deleted or u.id == my_id)]
        # (Apply your existing sorting here)

    # --- Sending Loop ---
    # ... (Keep your limit_str and base_delay inputs)

    sent_db = load_sent_db()
    successes, failures, skips_chatdup, skips_global = 0, 0, 0, 0

    for idx, user in enumerate(recipients_entities):
        label = display_tag(user)
        print(f"[{idx + 1}/{len(recipients_entities)}] Processing {label}...")

        # Guard 1: Global DB (Zero API requests)
        if user_already_sent_global(sent_db, user.id):
            print(f"⏭️ Skipped (Global DB)")
            skips_global += 1
            continue

        # Guard 2: Chat Check (Optimized API requests)
        msg = fill_firstname(message_template, user.first_name)
        if await already_sent_similar_optimized(client, user.id, msg):
            print(f"⏭️ Skipped (Already in chat)")
            skips_chatdup += 1
            mark_user_sent(sent_db, user, my_id) # Mark so we don't check API next time
            continue

        try:
            await client.send_message(user, msg)
            print(f"✅ Sent to {label}")
            successes += 1
            mark_user_sent(sent_db, user, my_id)
            save_sent_db(sent_db)
            append_sent_log_row(my_id, user, label)

            if idx < len(recipients_entities) - 1:
                await asyncio.sleep(random.uniform(min_delay, max_delay))

        except errors.FloodWaitError as e:
            # (Keep your SpamBot check logic)
            print(f"FloodWait: {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            print(f"❌ Error: {e}")
            failures += 1

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
