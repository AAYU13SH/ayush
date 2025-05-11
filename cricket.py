'''Okay, I've analyzed the issues and requests. Here's a breakdown of the changes and the modified code.

**Summary of Changes:**

1.  **1v1 Join & Callback Structure Fix:**
    *   The callback query handler (`handle_callback_query`) had a structural issue where 1v1-specific actions (`join_1v1`, `toss`, `batorbowl`, `num` for 1v1) were not correctly nested within the `elif game_type == '1v1':` block. This has been **fixed**. This was likely the primary reason "1v1 player can't join" or subsequent 1v1 actions failed.

2.  **Profile Command Image Fix (`/setpfp` and `/profile`):**
    *   `handle_setpfp`: Modified to store the `message.file.id` (a string file ID that Telethon uses) instead of `message.photo.id` (a long integer). This should resolve the "Cannot use X as file" error when sending the profile picture.
    *   Added a check in `handle_setpfp` to ensure `reply_message.file` and `reply_message.file.id` are available.

3.  **Broadcast Error Handling:**
    *   The existing broadcast loop in `handle_broadcast` is designed to continue even if individual messages fail to send (including handling `FloodWaitError`). No structural changes were made here as it already aligns with the request not to stop on single errors. The user might have misinterpreted a `FloodWaitError` pause.

4.  **Team Mode - Host Selects Batter/Bowler (Covers Strike Change):**
    *   After a wicket (if innings continues), the state changes to `STATE_TEAM_HOST_SELECT_BATTER`. The host must select the next batter.
    *   After an over is completed (if innings continues), the state changes to `STATE_TEAM_HOST_SELECT_BATTER` first, then `STATE_TEAM_HOST_SELECT_BOWLER`. The host selects who bats and bowls for the new over.
    *   The `create_player_selection_keyboard` will need to be used more frequently. A filter has been added to it to exclude players already marked as 'out' when selecting batters.

5.  **Host `/kick` User from Game:**
    *   New command `/kick_player [user_id_or_reply]` (aliased as `/kick`) added.
    *   Only the game host can use this command.
    *   It removes the specified player from their team.
    *   Adjusts team counts and max wickets for the affected team.
    *   If the kicked player was the current batter or bowler, the host is prompted to select a new one.
    *   If a team becomes empty, the game is cancelled.

6.  **Game Creator Cancel & Leave Team Button:**
    *   **Cancel Logic (`/cancel`):**
        *   For **team games**, only the `host_id` (game creator) can now use `/cancel` at any point.
        *   For **1v1 games**, only `p1_id` (game creator) can now use `/cancel` at any point. (This is a strict interpretation of "only game creator").
    *   **"Leave Team" Button:**
        *   Added a "Leave Team" button in the team joining phase (`STATE_TEAM_WAITING`).
        *   Players (not the host if it compromises game start) can leave a team they've joined.
        *   Callback `team_leave:<team_id>:<game_id>` handles this.

**Modified `cricket_telethon_team.py`:**
```python
# --- START OF FILE cricket_telethon_team.py ---'''

import asyncio
import random
import logging
from uuid import uuid4
import os
import html
import urllib.parse
import math # For calculating overs
import time # for monotonic and match IDs

from telethon import TelegramClient, events, Button
from telethon.errors import UserNotParticipantError, MessageNotModifiedError, MessageIdInvalidError, QueryIdInvalidError, BotMethodInvalidError, FloodWaitError
from telethon.tl.types import InputPeerUser, PeerUser, ReplyInlineMarkup, MessageReplyHeader # etc.
from telethon.tl.functions.messages import EditMessageRequest # Import specific requests if needed
from telethon.utils import get_peer_id, get_display_name

from pymongo import MongoClient, ReturnDocument # Keep sync Pymongo for now, Motor is alternative
from datetime import datetime, timezone

# --- Bot Configuration ---
# Replace with your actual credentials or use environment variables
API_ID = os.environ.get("API_ID", 25695711) # Replace with your API ID (integer)
API_HASH = os.environ.get("API_HASH", "f20065cc26d4a31bf0efc0b44edaffa9") # Replace with your API Hash (string)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "7906407273:AAHe77DY7TI9gmzsH-UM6k1vB9xDLRa_534") # Your bot token
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://yesvashisht:yash2005@clusterdf.yagj9ok.mongodb.net/?retryWrites=true&w=majority&appName=Clusterdf") # Replace with your MongoDB URI
MONGO_DB_NAME = "tct_cricket_bot_db" # Use a distinct DB?

# --- Game Configuration ---
DEFAULT_PLAYERS_PER_TEAM = 2
MAX_PLAYERS_PER_TEAM = 11 # Absolute maximum
DEFAULT_OVERS = 100 # Default overs per innings for team games
DEFAULT_OVERS_1V1 = 100 # Default overs per innings for 1v1 games
WIN_CREDITS = 200 # Credits awarded for winning a match

# --- Admin Configuration ---
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "6293455550,6265981509")
try:
    xmods = {int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',') if admin_id.strip()}
except ValueError:
    print("ERROR: Invalid ADMIN_IDS format. Please provide comma-separated integers.")
    xmods = set()

# --- Basic Validation ---
if not API_ID or not API_HASH or API_HASH == "YOUR_API_HASH" or not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN":
    print("ERROR: Please configure API_ID, API_HASH, and BOT_TOKEN (preferably via environment variables).")
    exit()
if not MONGO_URI or MONGO_URI == "YOUR_MONGODB_URI":
     print("ERROR: Please configure MONGO_URI (preferably via environment variables).")
     # exit() # Decide if you want to exit or run without DB

# --- Initialize Telethon Client ---
client = TelegramClient('bot_session', API_ID, API_HASH)
bot_info = None
games_lock = asyncio.Lock()

# --- Database Setup ---
mongo_client = None
db = None
users_collection = None
try:
    print("Connecting to MongoDB...")
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000, connectTimeoutMS=10000)
    mongo_client.admin.command('ping')
    db = mongo_client[MONGO_DB_NAME]
    users_collection = db.users
    print("Successfully connected to MongoDB.")
    print("Recommendation: Ensure indexes exist on 'runs', 'wickets', 'matches_played', 'credits'.")
except Exception as e:
    print(f"ERROR: Could not connect to MongoDB: {e}")
    print("Warning: Bot running without database. Stats and Credits features will be disabled.")
    users_collection = None
    db = None
    mongo_client = None

# --- Game States ---
# (States remain the same for 1v1)
STATE_WAITING = "WAITING"
STATE_TOSS = "TOSS"
STATE_BAT_BOWL = "BAT_BOWL"
STATE_P1_BAT = "P1_BAT"
STATE_P1_BOWL_WAIT = "P1_BOWL_WAIT"
STATE_P2_BAT = "P2_BAT"
STATE_P2_BOWL_WAIT = "P2_BOWL_WAIT"
STATE_1V1_ENDED = "1V1_ENDED"

# Team Game States
STATE_TEAM_HOST_JOIN_WAIT = "TEAM_HOST_JOIN_WAIT"
STATE_TEAM_WAITING = "TEAM_WAITING" # Players can join/leave
STATE_TEAM_TOSS_CALL = "TEAM_TOSS_CALL"
STATE_TEAM_BAT_BOWL_CHOICE = "TEAM_BAT_BOWL_CHOICE"
STATE_TEAM_HOST_SELECT_BATTER = "TEAM_HOST_SELECT_BATTER" # Host selects batter (start of innings, after wicket, start of new over)
STATE_TEAM_HOST_SELECT_BOWLER = "TEAM_HOST_SELECT_BOWLER" # Host selects bowler (start of innings, after batter selected for new over)
STATE_TEAM_BATTING = "TEAM_BATTING" # Current batter's turn
STATE_TEAM_BOWLING_WAIT = "TEAM_BOWLING_WAIT" # Current bowler's turn
STATE_TEAM_ENDED = "TEAM_ENDED"


# --- In-memory storage for active games ---
games = {} # game_id -> game_data dictionary

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- How to Play Text ---
HOW_TO_PLAY_TEXT = """<b>🎮 How to Play TCT Cricket Bot 🏏</b>

<b><u>Game Modes:</u></b>
1️⃣  <b>1v1 Cricket:</b>
    - Start with: <code>/cricket</code> in a group.
    - One player starts, another joins.
    - A coin toss decides who calls (Player 1).
    - The toss winner chooses to Bat or Bowl first.
    - <b>Gameplay:</b>
        - <b>Batter:</b> Select a run (1-6) using inline buttons.
        - <b>Bowler:</b> Select a delivery (1-6) using inline buttons.
        - If Batter's number == Bowler's number -> <b>OUT!</b> Innings ends.
        - If Batter's number != Bowler's number -> Batter scores that many runs.
    - Innings 1 ends when the batter is OUT or max overs are completed.
    - Innings 2: The other player bats, chasing the target set in Innings 1.
    - The game ends when the second batter is OUT, target is chased, or max overs are bowled.

2️⃣  <b>Team Cricket (NvN):</b>
    - Start with: <code>/team_cricket [N]</code> in a group (e.g., <code>/team_cricket 3</code> for 3v3. Default is 2v2). Max {MAX_PLAYERS_PER_TEAM} per team.
    - The player who starts is the <b>Host</b> and <u>must join Team A first</u>.
    - Other players can then join Team A or Team B (or leave if they joined) until teams are full or the Host starts.
    - <b>Minimum:</b> 1 player per team required to start.
    - The Host (captain of Team A) calls the toss.
    - The winning captain (from either team) chooses to Bat or Bowl.
    - The Host then selects the first batter for the batting team and first bowler for the bowling team.
    - <b>Gameplay:</b>
        - Similar to 1v1: Batter picks a number, then Bowler picks a number.
        - OUT or RUNS are determined.
        - When a batter gets out (and innings continues), the Host selects the next batter from available players.
        - When an over is complete (6 balls, and innings continues), the Host selects the next batter and then the next bowler.
    - Innings 1 ends when all wickets are down (equal to team size at start) or max overs are completed.
    - Innings 2: The other team bats, chasing the target.

<b><u>General Tips:</u></b>
- Use <code>/start</code> in DM with the bot to register. This is required to play and use features like credits.
- Use <code>/help</code> for a list of commands.
- Use <code>/guide</code> for this How to Play message.
- Use <code>/cancel</code> to cancel a game you started (group).
- For team games, the HOST has special responsibilities for player selection and can use <code>/kick_player</code> to remove someone from the game.
- Pay attention to whose turn it is! The bot will prompt you.
- Leaderboards (<code>/lead_runs</code>, etc.) and <code>/profile</code> are available to track stats and credits.
- Set your profile picture for the bot using <code>/setpfp</code> (reply to an image).
- Try your luck with <code>/flip h 100</code> or <code>/dice o 50</code> to bet credits!

Enjoy the game! 🎉
""".format(MAX_PLAYERS_PER_TEAM=MAX_PLAYERS_PER_TEAM)


# --- Helper Functions (Mostly Unchanged, minor logging additions if any) ---
def get_player_mention(user_id, name):
    safe_name = html.escape(name or f"User {user_id}")
    try: user_id_str = str(int(user_id))
    except ValueError: user_id_str = str(user_id)
    return f'<a href="tg://user?id={user_id_str}">{safe_name}</a>'

async def get_user_name_from_event(event):
    try:
        sender = await event.get_sender()
        if sender:
            first = sender.first_name or ""; last = sender.last_name or ""
            full_name = (first + " " + last).strip()
            return full_name if full_name else f"User_{event.sender_id}"
        else: return f"User_{event.sender_id}"
    except Exception:
        logger.warning(f"Could not get sender details for {event.sender_id}", exc_info=False)
        return f"User_{event.sender_id}"

async def safe_send_message(chat_id, text, **kwargs):
    try: return await client.send_message(chat_id, text, parse_mode='html', **kwargs)
    except Exception as e: logger.error(f"Send fail C:{chat_id} E:{e}", exc_info=False); return None

async def safe_reply(event, text, **kwargs):
    try:
        reply_to_id = event.message_id if hasattr(event, 'message_id') and event.message_id else event.id
        return await client.send_message(event.chat_id, text, reply_to=reply_to_id, parse_mode='html', **kwargs)
    except Exception as e:
        logger.error(f"Reply fail C:{event.chat_id} M:{event.id if hasattr(event, 'id') else 'N/A'} E:{e}", exc_info=False)
        # Fallback to sending without reply_to if reply fails
        return await safe_send_message(event.chat_id, text, parse_mode='html', **kwargs)


async def safe_edit_message(chat_id, message_id, text, **kwargs):
    if not message_id: return None
    try: return await client.edit_message(chat_id, message_id, text, parse_mode='html', **kwargs)
    except MessageNotModifiedError: logger.debug(f"Msg {message_id} not modified."); pass
    except (MessageIdInvalidError, BotMethodInvalidError) as e: logger.warning(f"Cannot edit msg {message_id} C:{chat_id}: {e}"); return None
    except Exception as e: logger.error(f"Edit fail M:{message_id} C:{chat_id}: {e}", exc_info=False); return None

async def safe_answer_callback(event, text=None, alert=False):
    try: await event.answer(text, alert=alert)
    except QueryIdInvalidError: logger.warning(f"Query ID invalid {event.id}")
    except Exception as e: logger.error(f"Callback answer fail {event.id}: {e}")

# --- Database Helpers (Modified for new fields) ---
def get_user_data(user_id):
    if users_collection is None: return None
    try: return users_collection.find_one({"_id": str(user_id)})
    except Exception as e: logger.error(f"DB fetch user {user_id}: {e}", exc_info=True); return None

def register_user_sync(user_id, full_name, username):
    if users_collection is None: return False
    user_id_str = str(user_id); now = datetime.now(timezone.utc)
    safe_full_name = full_name or f"User {user_id_str}"
    user_doc = {
        "$set": {"full_name": safe_full_name, "username": username or "", "last_seen": now},
        "$setOnInsert": {
            "_id": user_id_str, "runs": 0, "wickets": 0, "achievements": [],
            "registered_at": now, "matches_played": 0,
            "credits": 0,
            "pfp_file_id": None,
            "total_wins": 0,
            "total_times_out": 0,
            "total_balls_faced_overall": 0,
            "inning_scores_history": [],
            "bowling_figures_history": []
        }
    }
    try:
        result = users_collection.update_one({"_id": user_id_str}, user_doc, upsert=True)
        return result.upserted_id is not None or result.matched_count > 0
    except Exception as e: logger.error(f"DB register user {user_id_str}: {e}", exc_info=True); return False

async def register_user_telethon(user):
    if not user: return False
    return await asyncio.to_thread(register_user_sync, user.id, get_display_name(user), user.username)

def add_runs_sync(user_id, runs_to_add):
    if users_collection is None or runs_to_add <= 0: return False
    user_id_str = str(user_id)
    try:
        res = users_collection.update_one({"_id": user_id_str}, {"$inc": {"runs": runs_to_add}}, upsert=False)
        if res.matched_count == 0: logger.warning(f"DB: Add runs fail, user {user_id_str} not found"); return False
        return True
    except Exception as e: logger.error(f"DB add runs {user_id_str}: {e}", exc_info=True); return False

def add_wicket_sync(user_id):
    if users_collection is None: return False
    user_id_str = str(user_id)
    try:
        res = users_collection.update_one({"_id": user_id_str}, {"$inc": {"wickets": 1}}, upsert=False)
        if res.matched_count == 0: logger.warning(f"DB: Add wicket fail, user {user_id_str} not found"); return False
        return True
    except Exception as e: logger.error(f"DB add wicket {user_id_str}: {e}", exc_info=True); return False

def increment_matches_played_sync(user_id_list):
    if users_collection is None or not user_id_list: return False
    try:
        user_id_str_list = [str(uid) for uid in user_id_list]
        res = users_collection.update_many({"_id": {"$in": user_id_str_list}}, {"$inc": {"matches_played": 1}})
        # logger.info(f"DB: Incremented matches_played for {res.modified_count} users.")
        return res.modified_count > 0
    except Exception as e: logger.error(f"DB inc matches {user_id_list}: {e}", exc_info=True); return False

# --- NEW: Credits DB Helper ---
def add_credits_sync(user_id, credits_to_add):
    if users_collection is None:
        logger.warning("DB unavailable for credits operation.")
        return False
    if credits_to_add == 0: return True # No change needed, considered success

    user_id_str = str(user_id)
    try:
        user_doc = users_collection.find_one({"_id": user_id_str})
        if not user_doc:
            logger.warning(f"DB: Add/deduct credits fail, user {user_id_str} not found.")
            return False # User must exist

        current_credits = user_doc.get("credits", 0)
        if credits_to_add < 0 and current_credits < abs(credits_to_add):
            logger.info(f"DB: User {user_id_str} has insufficient credits ({current_credits}) for deduction {abs(credits_to_add)}.")
            return "insufficient"

        res = users_collection.update_one(
            {"_id": user_id_str},
            {"$inc": {"credits": credits_to_add}}
        )
        return res.matched_count > 0
    except Exception as e:
        logger.error(f"DB add/deduct credits for {user_id_str} ({credits_to_add}): {e}", exc_info=True)
        return False

async def add_credits_to_user(user_id, credits_amount):
    return await asyncio.to_thread(add_credits_sync, user_id, credits_amount)

# Async wrappers (Mostly Unchanged)
async def add_runs_to_user(user_id, runs): await asyncio.to_thread(add_runs_sync, user_id, runs)
async def add_wicket_to_user(user_id): await asyncio.to_thread(add_wicket_sync, user_id)
async def increment_matches_played(user_ids): await asyncio.to_thread(increment_matches_played_sync, user_ids)


# --- Leaderboard/Profile Helpers (Unchanged except get_user_rank if credits rank is added) ---
async def get_user_rank(user_id, field):
    if users_collection is None: return None
    user_data = await asyncio.to_thread(get_user_data, user_id)
    if not user_data or user_data.get(field, 0) <= 0: return None # Don't rank 0 scores
    try:
        score = user_data[field]
        count = await asyncio.to_thread(users_collection.count_documents, {field: {"$gt": score}})
        return count + 1
    except Exception as e: logger.error(f"DB rank fail U:{user_id} F:{field}: {e}", exc_info=True); return None

def _get_leaderboard_text_sync(field, top_n=10):
    if users_collection is None: return None, "⚠️ Database unavailable."
    try:
        # Ensure field exists and is > 0 for leaderboard inclusion
        users = list(users_collection.find({field: {"$gt": 0}}, {"_id": 1, "full_name": 1, field: 1}).sort(field, -1).limit(top_n))
        if not users: return None, f"No significant {field.replace('_', ' ')} recorded yet." # Changed message
        return users, None
    except Exception as e: logger.error(f"DB lead fail F:{field}: {e}", exc_info=True); return None, f"⚠️ Error fetching {field} leaderboard."

async def display_leaderboard(event, field, title):
    top_users, error_msg = await asyncio.to_thread(_get_leaderboard_text_sync, field)
    if error_msg: return await safe_reply(event, error_msg)
    if not top_users: return await safe_reply(event, f"No significant {field.replace('_', ' ')} recorded yet to display on the leaderboard.")

    medals = ['🥇', '🥈', '🥉'] + ['4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟']
    txt = f"🏆 <b>{title}:</b>\n\n"
    for i, u in enumerate(top_users):
        prefix = medals[i] if i < len(medals) else f"{i+1}."
        mention = get_player_mention(u['_id'], u.get('full_name'))
        score = u.get(field, 0)
        txt += f"{prefix} {mention} - <b>{score}</b> {field.replace('_', ' ').capitalize()}\n"

    if event.is_private: await safe_send_message(event.chat_id, txt)
    else:
        bot_uname = bot_info.username if bot_info else None
        if bot_uname:
            payload = f"show_lead_{field}"
            url = f"https://t.me/{bot_uname}?start={payload}"
            link_txt = title.replace("Top 10 ", "")
            markup = client.build_reply_markup([Button.url(f"📊 View {link_txt} (DM)", url)])
            await safe_reply(event, "Leaderboards are best viewed privately. Click below!", buttons=markup)
        else: await safe_reply(event, "Leaderboard available in DM (couldn't get my username for a link).")


# --- Game Cleanup (Unchanged) ---
async def cleanup_game(game_id, chat_id, reason="ended"):
    global games
    game_data = None
    async with games_lock: game_data = games.pop(game_id, None)
    if game_data:
        logger.info(f"Cleaning up game {game_id} C:{chat_id} (Reason: {reason})")
        msg_id = game_data.get('message_id')
        last_txt = game_data.get('last_text', "Game ended.")
        if reason != "finished normally" and msg_id:
            cleanup_txt = last_txt + f"\n\n<i>(Game session closed: {reason})</i>"
            await safe_edit_message(chat_id, msg_id, cleanup_txt, buttons=None)
    else: logger.warning(f"Cleanup attempt for non-existent game {game_id}")

# --- Format Overs (Unchanged) ---
def format_overs(balls):
    if balls < 0: balls = 0
    overs = balls // 6
    balls_in_over = balls % 6
    return f"{overs}.{balls_in_over}"

# --- Keyboard Generation ---
def create_standard_keyboard(game_id):
    btns = [Button.inline(str(i), data=f"num:{i}:{game_id}") for i in range(1, 7)]
    return [btns[:3], btns[3:]]

def create_host_join_keyboard(game_id):
    return [[Button.inline("Join Team A (Host)", data=f"team_join:A:{game_id}")]]

def create_join_team_keyboard(game_id, game_data, current_user_id=None):
    max_p = game_data['max_players_per_team']
    a_count = len(game_data['teams']['A']['players'])
    b_count = len(game_data['teams']['B']['players'])

    btn_a_text = f"Join Team A ({a_count}/{max_p})"
    btn_b_text = f"Join Team B ({b_count}/{max_p})"
    
    user_in_team_a = current_user_id in game_data['teams']['A']['players']
    user_in_team_b = current_user_id in game_data['teams']['B']['players']

    row1_buttons = []
    if not user_in_team_a and not user_in_team_b: # Not in any team
        if a_count < max_p:
            row1_buttons.append(Button.inline(btn_a_text, data=f"team_join:A:{game_id}"))
        else:
            row1_buttons.append(Button.inline(f"Team A ({a_count}/{max_p}) FULL", data="noop"))
        if b_count < max_p:
            row1_buttons.append(Button.inline(btn_b_text, data=f"team_join:B:{game_id}"))
        else:
            row1_buttons.append(Button.inline(f"Team B ({b_count}/{max_p}) FULL", data="noop"))
    elif user_in_team_a:
        # Host cannot leave Team A if it makes the team empty and game needs host in A
        can_leave_A = True
        if current_user_id == game_data.get('host_id') and len(game_data['teams']['A']['players']) == 1:
             pass # Host can't leave if they are the only one in A, no leave button for A

        if can_leave_A :
            row1_buttons.append(Button.inline("Leave Team A", data=f"team_leave:A:{game_id}"))
        if b_count < max_p and not user_in_team_b: # Show join B if not full and user not in B
             row1_buttons.append(Button.inline(btn_b_text, data=f"team_join:B:{game_id}"))
        elif len(row1_buttons) == 0: # If only option was leave and can't, or other team full
             row1_buttons.append(Button.inline(f"Team A ({a_count}/{max_p})", data="noop"))


    elif user_in_team_b:
        row1_buttons.append(Button.inline("Leave Team B", data=f"team_leave:B:{game_id}"))
        if a_count < max_p and not user_in_team_a: # Show join A if not full and user not in A
            row1_buttons.append(Button.inline(btn_a_text, data=f"team_join:A:{game_id}"))
        elif len(row1_buttons) == 0:
             row1_buttons.append(Button.inline(f"Team B ({b_count}/{max_p})", data="noop"))


    buttons = [row1_buttons] if row1_buttons else []


    host_id = game_data.get('host_id')
    host_in_a = host_id in game_data['teams']['A']['players'] if host_id else False
    can_start = host_in_a and a_count >= 1 and b_count >= 1

    if can_start and game_data['state'] == STATE_TEAM_WAITING:
         buttons.append([Button.inline("▶️ Start Game (Host Only)", data=f"start_game::{game_id}")])
    return buttons


def create_team_batbowl_keyboard(game_id):
     return [[Button.inline("Bat 🏏", data=f"team_batorbowl:bat:{game_id}"), Button.inline("Bowl 🧤", data=f"team_batorbowl:bowl:{game_id}")]]

def create_team_toss_keyboard(game_id):
    return [[Button.inline("Heads", data=f"team_toss:H:{game_id}"), Button.inline("Tails", data=f"team_toss:T:{game_id}")]]

def create_player_selection_keyboard(game_id, team_id, game_data, action_prefix):
    buttons = []
    row = []
    # Get players from the specified team's names dictionary
    players_dict = game_data['teams'][team_id]['names']
    player_stats_overall = game_data['teams'][team_id]['player_stats']

    player_items = players_dict.items() if isinstance(players_dict, dict) else []
    
    for p_id, p_name in player_items:
        safe_name = html.escape(p_name[:20])
        
        # If selecting a batter, only show players who are not out
        if action_prefix == "sel_bat":
            player_specific_stats = player_stats_overall.get(p_id, {})
            if player_specific_stats.get('is_out', False):
                continue # Skip this player as they are out

        row.append(Button.inline(safe_name, data=f"{action_prefix}:{p_id}:{game_id}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row: buttons.append(row)
    
    if not buttons and action_prefix == "sel_bat": # No non-out batters found
        return [[Button.inline("No available batters", data="noop")]]
    elif not buttons: # No players found for other selections (e.g. bowler)
        return [[Button.inline("Error: No players found", data="noop")]]
    return buttons


# =============================================
# --- Command Handlers ---
# =============================================

# --- Start Command (Modified for new help items) ---
@client.on(events.NewMessage(pattern='/start'))
async def handle_start(event):
    user_id = event.sender_id; chat_id = event.chat_id; sender = await event.get_sender()
    if not sender: logger.warning(f"/start from unknown sender {user_id}"); return
    full_name = get_display_name(sender); username = sender.username
    mention = get_player_mention(user_id, full_name); is_private = event.is_private

    if is_private and len(event.message.text.split()) > 1:
        try:
             payload = event.message.text.split(' ', 1)[1]
             logger.info(f"User {user_id} start payload: {payload}")
             if payload == 'show_lead_runs': await display_leaderboard(event, "runs", "Top 10 Run Scorers"); return
             elif payload == 'show_lead_wickets': await display_leaderboard(event, "wickets", "Top 10 Wicket Takers"); return
             elif payload == 'show_lead_matches_played': await display_leaderboard(event, "matches_played", "Top 10 Most Active Players"); return
             elif payload == 'show_help': await handle_help(event); return
             elif payload == 'show_guide': await handle_how_to_play(event); return # New
             else: logger.info(f"Unhandled start payload: {payload}")
        except IndexError: pass

    if not is_private:
         if users_collection is not None: await register_user_telethon(sender)
         start_msg = (f"Hi {mention}! 👋\nUse <code>/team_cricket</code> [size] or <code>/cricket</code> in a group.\nUse /start in my DM for stats & to use all features.")
         buttons = None; bot_uname = bot_info.username if bot_info else None
         if bot_uname: buttons = client.build_reply_markup([Button.url("Open DM", f"https://t.me/{bot_uname}?start=from_group")])
         await safe_reply(event, start_msg, buttons=buttons); return

    if users_collection is None: await safe_reply(event, f"Hi {mention}! Welcome!\n⚠️ DB offline, stats & credits disabled."); return

    is_new_user_check_before_reg = await asyncio.to_thread(get_user_data, user_id) is None
    reg_success = await register_user_telethon(sender)

    if reg_success:
        markup = client.build_reply_markup([[Button.url('Channel', 'https://t.me/TCTCRICKET'), Button.url('Group', 'https://t.me/+SIzIYQeMsRsyOWM1')]], inline_only=True)
        payload_handled = event.message.text.startswith(('/start show_', '/start from_group'))

        welcome_message_text = ""
        if not is_new_user_check_before_reg and not payload_handled :
             welcome_message_text = (f"Welcome back, {mention}!\n\nUse /help or /guide.\nCheck stats & credits: <code>/profile</code>\nLeaderboards: <code>/lead_runs /lead_wickets /lead_matches</code>")
        elif is_new_user_check_before_reg:
            welcome_message_text = (f"Welcome {mention} to TCT BOT!\nYou are now registered & can earn credits by playing.\n\nUse /help for commands or /guide to learn how to play.\nCheck stats: <code>/profile</code>")
            logger.info(f"New user reg: {full_name} ({user_id})")
            try:
                admin_mention = get_player_mention(user_id, full_name)
                for admin_id_val in xmods: await safe_send_message(admin_id_val, f"➕ New user: {admin_mention} (<code>{user_id}</code>)", link_preview=False)
            except Exception as e: logger.error(f"Admin notify fail: {e}")
        else: # Deep link handled or other specific start payload
             if not payload_handled: # If it wasn't a known payload, send generic welcome back
                  welcome_message_text = (f"Welcome back, {mention}!\n\nUse /help or /guide.\nCheck stats & credits: <code>/profile</code>\nLeaderboards: <code>/lead_runs /lead_wickets /lead_matches</code>")
             else: # Payload was handled, no further welcome message needed here.
                 return 

        if welcome_message_text: # Only send if there's a message to send
            await safe_send_message(chat_id, welcome_message_text, buttons=markup, link_preview=False)
    else: await safe_reply(event, f"{mention}, there was an error during registration/update. Please try /start again.")


# --- Help Command (Updated for new/removed commands) ---
@client.on(events.NewMessage(pattern='/help'))
async def handle_help(event):
    user_id = event.sender_id; is_admin = user_id in xmods; sender = await event.get_sender()
    mention = get_player_mention(user_id, get_display_name(sender)) if sender else f"User {user_id}"
    user_cmds = f"""<b><u>User Commands:</u></b>
<code>/start</code> - Register (DM) & welcome.
<code>/help</code> - This help message.
<code>/guide</code> - Detailed How to Play instructions.
<code>/team_cricket</code> [N] - Start NvN game (group, e.g., /team_cricket 3 for 3v3, default 2v2). Max {MAX_PLAYERS_PER_TEAM} per team.
<code>/cricket</code> - Start 1v1 game (group).
<code>/cancel</code> - Cancel a game you started (group).
<code>/profile</code> - View your stats, credits & achievements (reply to view others').
<code>/setpfp</code> - Reply to an image to set it as your bot profile picture.
<code>/lead_runs</code> / <code>/lead_wickets</code> / <code>/lead_matches</code> - Leaderboards.
<code>/ping</code> - Check bot status.
<code>/kick_player</code> - (Host only) Kick player from team game. Reply or use <code>/kick_player [user_id]</code>.

<b><u>Betting Commands (Use Credits):</u></b>
<code>/flip &lt;h/t&gt; &lt;amount&gt;</code> - Flip a coin (e.g., /flip h 100).
<code>/dice &lt;o/e&gt; &lt;amount&gt;</code> - Roll a die for Odd/Even (e.g., /dice o 50)."""

    admin_cmds = """
<b><u>Admin Commands:</u></b>
<code>/achieve</code> [id] <t> | <code>/remove_achieve</code> [id] <t>
<code>/broad</code> <msg> | <code>/set_runs</code> [id] <amt>
<code>/set_wickets</code> [id] <amt> | <code>/set_credits</code> [id] <amt>
<code>/clear_stats</code> [id] (Caution!) | <code>/force_cancel</code> <game_id>
<code>/user_count</code> | <code>/db_stats</code>""" 

    help_txt = f"Hello {mention}! Commands:\n\n" + user_cmds
    if is_admin: help_txt += "\n\n" + admin_cmds
    help_txt += "\n\n<i>[N]/[id] optional if replying to a user's message.</i>"
    is_deep_link_help = event.message.text == "/start show_help"

    if not event.is_private and not is_deep_link_help:
        bot_uname = bot_info.username if bot_info else None
        buttons = None
        if bot_uname: buttons = client.build_reply_markup([Button.url("Open DM for Full Help", f"https://t.me/{bot_uname}?start=show_help")])
        await safe_reply(event, "Check DM for full command list.", buttons=buttons)
    else:
        await safe_send_message(event.chat_id, help_txt)

# --- NEW: Guide Command ---
@client.on(events.NewMessage(pattern='/guide'))
async def handle_how_to_play(event):
    # Add a button to open DM if in group for better viewing
    if not event.is_private:
        bot_uname = bot_info.username if bot_info else None
        buttons = None
        if bot_uname:
            buttons = client.build_reply_markup([Button.url("Open DM for Guide", f"https://t.me/{bot_uname}?start=show_guide")])
        await safe_reply(event, "The How to Play guide is best viewed in DM. Click below or use /guide in my DM.", buttons=buttons)
    else:
        await safe_send_message(event.chat_id, HOW_TO_PLAY_TEXT)

# --- Profile Command (CORRECTED reply_to logic for all send operations) ---
@client.on(events.NewMessage(pattern='/profile'))
async def handle_profile(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable. Profile and credits system offline.")

    source_user_id = event.sender_id 
    target_user_id = event.sender_id 

    if event.is_reply:
        reply_msg = await event.get_reply_message()
        if reply_msg and reply_msg.sender_id:
            target_user_id = reply_msg.sender_id
    
    try:
        target_user_entity = await client.get_entity(target_user_id)
        target_display_name = get_display_name(target_user_entity) if target_user_entity else f"User {target_user_id}"
    except Exception:
        target_display_name = f"User {target_user_id}"
    target_mention = get_player_mention(target_user_id, target_display_name)

    source_user_entity = await event.get_sender()
    source_display_name = get_display_name(source_user_entity) if source_user_entity else f"User {source_user_id}"

    reply_to_id_for_handler_response = event.id 
    if event.is_reply and event.reply_to_msg_id:
        reply_to_id_for_handler_response = event.reply_to_msg_id

    user_data_task = asyncio.to_thread(get_user_data, target_user_id)
    runs_rank_task = get_user_rank(target_user_id, "runs")
    wickets_rank_task = get_user_rank(target_user_id, "wickets")
    matches_rank_task = get_user_rank(target_user_id, "matches_played")
    credits_rank_task = get_user_rank(target_user_id, "credits")

    user_data = await user_data_task
    if not user_data:
        txt = f"{target_mention} is not registered. They need to /start me in DM first."
        if source_user_id == target_user_id: 
             txt = f"You aren't registered yet, {target_mention}. Please /start me in DM."
        await client.send_message(event.chat_id, txt, reply_to=reply_to_id_for_handler_response, parse_mode='html')
        return

    runs_rank = await runs_rank_task
    wickets_rank = await wickets_rank_task
    matches_rank = await matches_rank_task
    credits_rank = await credits_rank_task

    pfp_file_id = user_data.get("pfp_file_id") # This should be a string file_id now
    runs = user_data.get("runs", 0)
    wickets = user_data.get("wickets", 0)
    matches = user_data.get("matches_played", 0)
    credits_val = user_data.get("credits", 0)
    achievements = user_data.get("achievements", [])
    reg_date = user_data.get("registered_at")
    reg_date_str = reg_date.strftime("%d %b %Y") if reg_date else "N/A"

    runs_rank_d = f"#{runs_rank}" if runs_rank else "N/A"
    wickets_rank_d = f"#{wickets_rank}" if wickets_rank else "N/A"
    matches_rank_d = f"#{matches_rank}" if matches_rank else "N/A"
    credits_rank_d = f"#{credits_rank}" if credits_rank else "N/A"

    profile_caption = f"👤 <b>{target_display_name}</b>\n"
    profile_caption += f"🆔 User ID: <code>{target_user_id}</code>\n"
    profile_caption += "━━━━━━━━━━━━━━━━━━\n"
    profile_caption += f"🏏 Runs: <b>{runs}</b> (Rank: {runs_rank_d})\n"
    profile_caption += f"🎯 Wickets: <b>{wickets}</b> (Rank: {wickets_rank_d})\n"
    profile_caption += f"🏟️ Matches: <b>{matches}</b> (Rank: {matches_rank_d})\n"
    profile_caption += f"💰 Credits: <b>{credits_val}</b> (Rank: {credits_rank_d})\n"
    profile_caption += f"🗓️ Joined: {reg_date_str}\n"
    profile_caption += "━━━━━━━━━━━━━━━━━━\n"

    if achievements:
        profile_caption += f"🏅 <b>Achievements ({len(achievements)}):</b>\n"
        for ach in sorted(achievements):
            profile_caption += f"  ✧ <code>{html.escape(str(ach))}</code>\n"
    else:
        profile_caption += "🏅 <b>Achievements:</b> <i>None yet.</i>\n"
    
    if source_user_id != target_user_id:
        source_player_mention = get_player_mention(source_user_id, source_display_name)
        profile_caption += f"\n<i>(Requested by {source_player_mention})</i>"

    if pfp_file_id: # pfp_file_id is now expected to be a string file_id
        try:
            # Using the stored string file_id directly
            await client.send_file(event.chat_id, file=pfp_file_id, caption=profile_caption, parse_mode='html', reply_to=reply_to_id_for_handler_response)
        except Exception as e:
            logger.warning(f"Failed to send profile with PFP (file_id: {pfp_file_id}) for {target_user_id}: {e}. Sending text only.")
            # Clear the invalid pfp_file_id from DB if it consistently fails
            if "Cannot use" in str(e) or "FILE_ID_INVALID" in str(e).upper():
                logger.info(f"Clearing invalid pfp_file_id {pfp_file_id} for user {target_user_id}")
                users_collection.update_one({"_id": str(target_user_id)}, {"$set": {"pfp_file_id": None}})
            await client.send_message(event.chat_id, profile_caption, reply_to=reply_to_id_for_handler_response, parse_mode='html')
    else:
        await client.send_message(event.chat_id, profile_caption, reply_to=reply_to_id_for_handler_response, parse_mode='html')

# --- Leaderboard Commands (Unchanged) ---
@client.on(events.NewMessage(pattern='/lead_runs'))
async def handle_lead_runs(event): await display_leaderboard(event, "runs", "Top 10 Run Scorers")
@client.on(events.NewMessage(pattern='/lead_wickets'))
async def handle_lead_wickets(event): await display_leaderboard(event, "wickets", "Top 10 Wicket Takers")
@client.on(events.NewMessage(pattern='/lead_matches'))
async def handle_lead_matches(event): await display_leaderboard(event, "matches_played", "Top 10 Most Active")

# --- Ping Command (Unchanged) ---
@client.on(events.NewMessage(pattern='/ping'))
async def handle_ping(event):
    start_t = time.monotonic_ns(); ping_msg = await safe_reply(event, "⏳ Pinging...")
    send_t = time.monotonic_ns();
    if not ping_msg: return
    send_lat = (send_t - start_t) // 1_000_000
    db_stat = "N/A"; db_lat = None
    if mongo_client is not None and db is not None:
        db_st = time.monotonic_ns()
        try: await asyncio.to_thread(db.command, 'ping'); db_et = time.monotonic_ns(); db_lat = (db_et - db_st) // 1_000_000; db_stat = "Connected ✅"
        except Exception as db_e: logger.warning(f"Ping DB fail: {db_e}"); db_stat = "Error ❌"
    elif mongo_client is None: db_stat = "Disconnected ⚠️"
    edit_st = time.monotonic_ns()
    ping_txt = f"🏓 <b>Pong!</b>\n⏱️ API Latency: <code>{send_lat} ms</code>\n🗄️ Database: <code>{db_stat}</code>"
    if db_lat is not None: ping_txt += f" (Ping: <code>{db_lat} ms</code>)"
    await safe_edit_message(ping_msg.chat_id, ping_msg.id, ping_txt)
    edit_et = time.monotonic_ns(); edit_lat = (edit_et - edit_st) // 1_000_000; total_lat = (edit_et - start_t) // 1_000_000
    logger.info(f"Ping: Total={total_lat}ms, Send={send_lat}ms, DB={db_lat}ms, Edit={edit_lat}ms")

# --- Start Team Cricket Command (Modified for new scorecard data AND creation_timestamp) ---
@client.on(events.NewMessage(pattern=r'/team_cricket(?: (\d+))?'))
async def start_team_cricket(event):
    global games
    host_id = event.sender_id
    chat_id = event.chat_id
    sender = await event.get_sender()
    if not sender: return
    host_name_initial = get_display_name(sender)

    if event.is_private: return await safe_reply(event, "Team games are for group chats only.")
    if users_collection is None: return await safe_reply(event, "⚠️ DB offline, cannot start games or use credits.")

    user_data = await asyncio.to_thread(get_user_data, host_id)
    if not user_data: return await safe_reply(event, f"{get_player_mention(host_id, host_name_initial)}, please /start me in DM first to use all features.")
    host_name = user_data.get("full_name", host_name_initial)

    players_per_team = DEFAULT_PLAYERS_PER_TEAM
    match_size_str = "Default"
    try:
        match_obj = event.pattern_match.group(1)
        if match_obj:
            match_size_str = match_obj
            requested_size = int(match_obj)
            if 1 <= requested_size <= MAX_PLAYERS_PER_TEAM: players_per_team = requested_size
            else: await safe_reply(event, f"Team size must be 1-{MAX_PLAYERS_PER_TEAM}. Using {players_per_team}v{players_per_team}.")
    except (ValueError, TypeError): pass
    
    game_id_to_create = str(uuid4())
    new_game_data = None
    start_text = None
    markup = None

    async with games_lock:
        for gid, gdata in games.items():
             if gdata['chat_id'] == chat_id:
                 involved_ids = set(); gtype = gdata.get('game_type')
                 if gtype == 'team': involved_ids.add(gdata.get('host_id')); involved_ids.update(gdata['teams']['A']['players'], gdata['teams']['B']['players'])
                 elif gtype == '1v1': involved_ids.update([p.get('id') for p in [gdata.get('player1'), gdata.get('player2')] if p and p.get('id')])
                 involved_ids.discard(None)
                 if host_id in involved_ids:
                     logger.warning(f"User {host_id} tried start /team_cricket but in G:{gid}")
                     return await safe_reply(event, "You are already in a game in this chat! Use /cancel.")

        new_game_data = {
            'game_id': game_id_to_create, 'game_type': 'team', 'chat_id': chat_id, 'message_id': None,
            'host_id': host_id, 'state': STATE_TEAM_HOST_JOIN_WAIT,
            'max_players_per_team': players_per_team,
            'actual_players_team_A': 0, 'actual_players_team_B': 0,
            'max_wickets_team_A': players_per_team, 'max_wickets_team_B': players_per_team,
            'overs_per_innings': DEFAULT_OVERS, 'max_balls': DEFAULT_OVERS * 6,
            'teams': {'A': {'players': [], 'names': {}, 'score': 0, 'wickets': 0, 'player_stats': {}},
                      'B': {'players': [], 'names': {}, 'score': 0, 'wickets': 0, 'player_stats': {}}},
            'innings': 1, 'balls_bowled_this_inning': 0, 'balls_this_over': 0, # Added balls_this_over
            'balls_bowled_inning1': 0, 'balls_bowled_inning2': 0,
            'current_batting_team': None, 'current_bowling_team': None,
            'current_batter_id': None, 'current_bowler_id': None,
            'batter_choice': None, 'target': None, 'last_text': "",
            'creation_timestamp': time.time(), 
            'monotonic_created_at': time.monotonic(),
            'toss_winner_team': None, 'choice': None, 'last_out_player_id': None,
            'inning_stats': {
                1: {'fours': 0, 'sixes': 0, 'dots': 0, 'runs_this_over_list': [], 'overs_completed_runs': []},
                2: {'fours': 0, 'sixes': 0, 'dots': 0, 'runs_this_over_list': [], 'overs_completed_runs': []}
            },
            'match_stats': {'total_fours': 0, 'total_sixes': 0, 'total_dots': 0, 'best_over_runs': -1, 'runs_in_overs': []},
            'first_batting_team_id': None
        }
        games[game_id_to_create] = new_game_data
        markup = client.build_reply_markup(create_host_join_keyboard(game_id_to_create))
        start_text = f"⚔️ New {players_per_team}v{players_per_team} Team Cricket!\nHost: <b>{html.escape(host_name)}</b>\n\nHost, please join Team A first:"
        new_game_data['last_text'] = start_text

    if new_game_data and start_text:
        logger.info(f"Created TEAM game {game_id_to_create} ({players_per_team}v{players_per_team}) in C:{chat_id}")
        sent_message = await safe_send_message(chat_id, start_text, buttons=markup)
        if sent_message:
            async with games_lock:
                if game_id_to_create in games: games[game_id_to_create]["message_id"] = sent_message.id
        else:
            logger.error(f"Fail send team game msg {game_id_to_create}, cleaning up.")
            async with games_lock: games.pop(game_id_to_create, None)
    else: logger.error(f"Failed to prepare team game C:{chat_id}")

# --- Start 1v1 Cricket Command (Modified for new scorecard data AND creation_timestamp) ---
@client.on(events.NewMessage(pattern='/cricket'))
async def start_1v1_cricket(event):
    global games
    p1_id = event.sender_id
    chat_id = event.chat_id
    sender = await event.get_sender()
    if not sender: return
    p1_name_initial = get_display_name(sender)

    if event.is_private: return await safe_reply(event, "1v1 games are for group chats only.")
    if users_collection is None: return await safe_reply(event, "⚠️ DB offline, cannot start games or use credits.")

    user_data = await asyncio.to_thread(get_user_data, p1_id)
    if not user_data: return await safe_reply(event, f"{get_player_mention(p1_id, p1_name_initial)}, please /start me in DM first to use all features.")
    p1_name = user_data.get("full_name", p1_name_initial)

    logger.info(f"User {p1_name}({p1_id}) initiated /cricket in C:{chat_id}")
    game_id_to_create = str(uuid4())
    new_game_data = None; start_text = None; markup = None

    async with games_lock:
        for gid, gdata in games.items():
             if gdata['chat_id'] == chat_id:
                 involved_ids = set(); gtype = gdata.get('game_type')
                 if gtype == 'team': involved_ids.add(gdata.get('host_id')); involved_ids.update(gdata['teams']['A']['players'], gdata['teams']['B']['players'])
                 elif gtype == '1v1': involved_ids.update([p.get('id') for p in [gdata.get('player1'), gdata.get('player2')] if p and p.get('id')])
                 involved_ids.discard(None)
                 if p1_id in involved_ids:
                     logger.warning(f"User {p1_id} tried start /cricket but in G:{gid}")
                     return await safe_reply(event, "You are already in a game in this chat! Use /cancel.")

        new_game_data = {
            'game_id': game_id_to_create, 'game_type': '1v1', 'chat_id': chat_id, 'message_id': None,
            'state': STATE_WAITING, 'overs_per_innings': DEFAULT_OVERS_1V1, 'max_balls': DEFAULT_OVERS_1V1 * 6,
            'player1': {'id': p1_id, 'name': p1_name, 'score': 0, 'balls_faced': 0, 'balls_bowled': 0, 'wickets_taken': 0, 'fours':0, 'sixes':0},
            'player2': None,
            'innings': 1, 'balls_bowled_this_inning': 0,
            'balls_bowled_inning1': 0, 'balls_bowled_inning2': 0,
            'balls_this_over': 0,
            'current_batting_team': None, 'current_bowling_team': None, # Not used in 1v1, but kept for consistency
            'current_batter_id': None, 'current_bowler_id': None,
            'batter_choice': None, 'target': None, 'last_text': "",
            'creation_timestamp': time.time(), 
            'monotonic_created_at': time.monotonic(),
            'toss_winner_id': None, 'choice': None,
            'inning_stats': {
                1: {'fours': 0, 'sixes': 0, 'dots': 0, 'runs_this_over_list': [], 'overs_completed_runs': []},
                2: {'fours': 0, 'sixes': 0, 'dots': 0, 'runs_this_over_list': [], 'overs_completed_runs': []}
            },
            'match_stats': {'total_fours': 0, 'total_sixes': 0, 'total_dots': 0, 'best_over_runs': -1, 'runs_in_overs': []},
            'first_batter_id': None
        }
        games[game_id_to_create] = new_game_data
        markup = client.build_reply_markup([[Button.inline("Join Game", data=f"join_1v1::{game_id_to_create}")]])
        start_text = f"⚔️ New 1v1 Cricket Game started by <b>{html.escape(p1_name)}</b>!\nWaiting for an opponent..."
        new_game_data['last_text'] = start_text

    if new_game_data and start_text:
        logger.info(f"Created 1v1 game {game_id_to_create} in C:{chat_id}")
        sent_message = await safe_send_message(chat_id, start_text, buttons=markup)
        if sent_message:
            async with games_lock:
                if game_id_to_create in games: games[game_id_to_create]["message_id"] = sent_message.id
        else:
            logger.error(f"Fail send 1v1 game msg {game_id_to_create}, cleaning up.")
            async with games_lock: games.pop(game_id_to_create, None)
    else: logger.error(f"Failed prepare 1v1 game C:{chat_id}")


# --- Cancel Command (MODIFIED for creator-only cancel) ---
@client.on(events.NewMessage(pattern='/cancel'))
async def handle_cancel(event):
    user_id = event.sender_id; chat_id = event.chat_id; sender = await event.get_sender()
    if not sender: return
    canceller_name = get_display_name(sender); game_to_cancel_id = None
    can_cancel_this_game = False

    if event.is_private: return await safe_reply(event, "Cancel in the group chat where the game was started.")

    async with games_lock:
        for gid, gdata in list(games.items()): # Iterate over a copy for safe removal
            if gdata['chat_id'] == chat_id:
                gtype = gdata.get('game_type')
                
                if gtype == 'team':
                    host_id_game = gdata.get('host_id')
                    if user_id == host_id_game: # Only host can cancel team game
                        can_cancel_this_game = True
                elif gtype == '1v1':
                    p1_id_game = gdata.get('player1', {}).get('id')
                    if user_id == p1_id_game: # Only P1 (creator) can cancel 1v1 game
                        can_cancel_this_game = True
                
                if can_cancel_this_game:
                    game_to_cancel_id = gid
                    break 
    
    if game_to_cancel_id:
        logger.info(f"User {user_id} (creator) cancelling game {game_to_cancel_id}")
        # Retrieve game message ID before popping for cleanup text.
        # last_text and message_id are already part of cleanup_game.
        await cleanup_game(game_to_cancel_id, chat_id, reason=f"cancelled by game creator {html.escape(canceller_name)}")
        await safe_reply(event, f"✅ Game cancelled by its creator, <b>{html.escape(canceller_name)}</b>.")
    else: 
        await safe_reply(event, "You cannot cancel this game. Only the game creator can cancel, or no active game found linked to you as creator in this chat.")


# --- Kick Player Command (NEW) ---
@client.on(events.NewMessage(pattern=r'/kick_player(?: (\d+))?|/kick(?: (\d+))?'))
async def handle_kick_player(event):
    global games
    host_kicker_id = event.sender_id
    chat_id = event.chat_id
    
    if event.is_private:
        return await safe_reply(event, "This command can only be used in a group chat during a team game.")

    target_user_id_to_kick = None
    try:
        # Check both match groups from the regex for /kick_player and /kick
        match1 = event.pattern_match.group(1)
        match2 = event.pattern_match.group(2)
        
        if match1: target_user_id_to_kick = int(match1)
        elif match2: target_user_id_to_kick = int(match2)
        elif event.is_reply:
            reply_msg = await event.get_reply_message()
            if reply_msg and reply_msg.sender_id:
                target_user_id_to_kick = reply_msg.sender_id
        else:
            return await safe_reply(event, "Usage: <code>/kick_player [user_id]</code> or reply to a user's message with <code>/kick_player</code>.")
    except ValueError:
        return await safe_reply(event, "Invalid user ID format.")

    if not target_user_id_to_kick:
        return await safe_reply(event, "Could not determine the target user to kick.")

    game_id_affected = None
    game_data_affected = None
    kicker_name = get_player_mention(host_kicker_id, await get_user_name_from_event(event))
    
    async with games_lock:
        for gid, gdata in games.items():
            if gdata['chat_id'] == chat_id and gdata.get('game_type') == 'team' and gdata.get('host_id') == host_kicker_id:
                game_id_affected = gid
                game_data_affected = gdata
                break
        
        if not game_data_affected:
            return await safe_reply(event, f"{kicker_name}, you are not the host of an active team game in this chat, or no team game found.")

        if host_kicker_id == target_user_id_to_kick:
            return await safe_reply(event, "Host cannot kick themselves. Use /cancel to end the game.")

        kicked_from_team_id = None
        kicked_player_name = None

        for team_id_check in ['A', 'B']:
            if target_user_id_to_kick in game_data_affected['teams'][team_id_check]['players']:
                kicked_from_team_id = team_id_check
                kicked_player_name = game_data_affected['teams'][team_id_check]['names'].get(target_user_id_to_kick, f"User {target_user_id_to_kick}")
                
                game_data_affected['teams'][team_id_check]['players'].remove(target_user_id_to_kick)
                if target_user_id_to_kick in game_data_affected['teams'][team_id_check]['names']:
                    del game_data_affected['teams'][team_id_check]['names'][target_user_id_to_kick]
                if target_user_id_to_kick in game_data_affected['teams'][team_id_check]['player_stats']:
                    del game_data_affected['teams'][team_id_check]['player_stats'][target_user_id_to_kick]
                
                game_data_affected[f'actual_players_team_{team_id_check}'] = len(game_data_affected['teams'][team_id_check]['players'])
                game_data_affected[f'max_wickets_team_{team_id_check}'] = game_data_affected[f'actual_players_team_{team_id_check}']
                break
        
        if not kicked_from_team_id:
            return await safe_reply(event, f"Player <code>{target_user_id_to_kick}</code> is not part of the current game.")

        kicked_player_mention = get_player_mention(target_user_id_to_kick, kicked_player_name)
        await safe_reply(event, f"👢 {kicked_player_mention} has been kicked from Team {kicked_from_team_id} by the host {kicker_name}.")
        
        # Check if game can continue
        if game_data_affected[f'actual_players_team_A'] == 0 or game_data_affected[f'actual_players_team_B'] == 0:
            await safe_send_message(chat_id, "A team is now empty. Game cancelled due to insufficient players.")
            await cleanup_game(game_id_affected, chat_id, reason="team became empty after kick")
            return

        # Handle if kicked player was current batter/bowler
        next_state_after_kick = game_data_affected['state']
        prompt_message = ""
        
        if game_data_affected.get('current_batter_id') == target_user_id_to_kick:
            game_data_affected['current_batter_id'] = None
            if game_data_affected['state'] not in [STATE_TEAM_ENDED, STATE_TEAM_HOST_JOIN_WAIT, STATE_TEAM_WAITING]:
                 next_state_after_kick = STATE_TEAM_HOST_SELECT_BATTER
                 prompt_message = f"\nKicked player was batting. Host, please select a new batter for Team {game_data_affected['current_batting_team']}."
        
        if game_data_affected.get('current_bowler_id') == target_user_id_to_kick:
            game_data_affected['current_bowler_id'] = None
            if game_data_affected['state'] not in [STATE_TEAM_ENDED, STATE_TEAM_HOST_JOIN_WAIT, STATE_TEAM_WAITING]:
                # If batter also became none, selecting batter takes precedence
                if next_state_after_kick != STATE_TEAM_HOST_SELECT_BATTER:
                    next_state_after_kick = STATE_TEAM_HOST_SELECT_BOWLER
                    prompt_message = f"\nKicked player was bowling. Host, please select a new bowler for Team {game_data_affected['current_bowling_team']}."
        
        game_data_affected['state'] = next_state_after_kick

        # Update game message
        new_text = ""
        new_keyboard = None
        if game_data_affected['state'] == STATE_TEAM_WAITING:
            players_txt = format_team_players_for_ui(game_data_affected)
            host_m = get_player_mention(game_data_affected['host_id'], game_data_affected['teams']['A']['names'].get(game_data_affected['host_id']))
            new_text = f"⚔️ {game_data_affected['max_players_per_team']}v{game_data_affected['max_players_per_team']} Team Cricket!\nHost: {host_m}\n\n{players_txt}\n\nWaiting for players... ({kicked_player_mention} was kicked)"
            new_keyboard = create_join_team_keyboard(game_id_affected, game_data_affected, host_kicker_id) # Pass any ID for context
        
        elif game_data_affected['state'] == STATE_TEAM_HOST_SELECT_BATTER:
            host_m = get_player_mention(game_data_affected['host_id'], game_data_affected['teams']['A']['names'].get(game_data_affected['host_id']))
            bat_team_select = game_data_affected['current_batting_team']
            new_text = f"{game_data_affected['last_text'].split('➡️')[0]}\n{prompt_message}\n➡️ Host ({host_m}), select batter for Team {bat_team_select}:"
            new_keyboard = create_player_selection_keyboard(game_id_affected, bat_team_select, game_data_affected, "sel_bat")
        
        elif game_data_affected['state'] == STATE_TEAM_HOST_SELECT_BOWLER:
            host_m = get_player_mention(game_data_affected['host_id'], game_data_affected['teams']['A']['names'].get(game_data_affected['host_id']))
            bowl_team_select = game_data_affected['current_bowling_team']
            new_text = f"{game_data_affected['last_text'].split('➡️')[0]}\n{prompt_message}\n➡️ Host ({host_m}), select bowler for Team {bowl_team_select}:"
            new_keyboard = create_player_selection_keyboard(game_id_affected, bowl_team_select, game_data_affected, "sel_bowl")
        
        else: # Game in progress, just update status
            status_txt_kick, _, _ = format_team_game_status(game_data_affected)
            new_text = f"{game_data_affected['last_text'].split('---')[0]}\n{status_txt_kick}\n{prompt_message}\nTurn continues..."
            if game_data_affected['state'] == STATE_TEAM_BATTING:
                batter_m_kick = get_player_mention(game_data_affected['current_batter_id'], game_data_affected['teams'][game_data_affected['current_batting_team']]['names'].get(game_data_affected['current_batter_id']))
                new_text = f"{status_txt_kick}\n➡️ {batter_m_kick}, select shot (1-6):"
                new_keyboard = create_standard_keyboard(game_id_affected)
            elif game_data_affected['state'] == STATE_TEAM_BOWLING_WAIT:
                bowler_m_kick = get_player_mention(game_data_affected['current_bowler_id'], game_data_affected['teams'][game_data_affected['current_bowling_team']]['names'].get(game_data_affected['current_bowler_id']))
                new_text = f"{status_txt_kick}\n➡️ {bowler_m_kick}, select delivery (1-6):"
                new_keyboard = create_standard_keyboard(game_id_affected)


        if new_text and game_data_affected.get("message_id"):
            game_data_affected['last_text'] = new_text
            await safe_edit_message(chat_id, game_data_affected["message_id"], new_text, buttons=client.build_reply_markup(new_keyboard) if new_keyboard else None)

# =============================================
# --- Central Callback Query Handler (MAJOR CHANGES for game logic, data tracking) ---
# =============================================
@client.on(events.CallbackQuery)
async def handle_callback_query(event):
    global games
    user_id = event.sender_id; chat_id = event.chat_id; message_id = event.message_id
    try: data = event.data.decode('utf-8')
    except Exception as e: logger.warning(f"Callback decode fail U:{user_id} E:{e}"); return await safe_answer_callback(event, "Decode Error", alert=True)

    logger.debug(f"Callback: Data='{data}', User={user_id}, Chat={chat_id}, Msg={message_id}")
    if data == "noop": return await safe_answer_callback(event, "Option unavailable.")

    try:
        parts = data.split(":")
        action = parts[0]
        value = parts[1] if len(parts) > 1 else None
        game_id = parts[2] if len(parts) > 2 else (parts[1] if action=="start_game" and len(parts)>1 else None) # Fix for start_game which had game_id as value
        if action == "start_game" and len(parts) > 1: value = None # Correct value for start_game

        if not game_id: logger.debug(f"Callback without game_id: {data}"); return await safe_answer_callback(event)
        if value == '_': value = None
        numeric_value = int(value) if value is not None and value.isdigit() else None
    except Exception as e: logger.warning(f"Callback parse error: {data} - {e}"); return await safe_answer_callback(event, "Parse Error.")

    db_updates = []; game_ended_flag = False; final_text = None; final_keyboard = None
    msg_needs_update = False; player_ids_to_inc_match = []
    winning_player_ids_for_credits = [] 

    async with games_lock:
        g = games.get(game_id)

        if not g:
            try: await client.edit_message(chat_id, message_id, "Game ended or not found.", buttons=None)
            except Exception as edit_err: logger.warning(f"Couldn't edit message {message_id} for ended game {game_id}: {edit_err}")
            return await safe_answer_callback(event, "This game is no longer active.", alert=True)

        if message_id != g.get("message_id"):
            return await safe_answer_callback(event, "Use buttons on the latest game message.", alert=True)

        state = g['state']; game_type = g['game_type']
        host_id = g.get('host_id'); p1_id = g.get('player1', {}).get('id')
        p2_id = g.get('player2', {}).get('id') if g.get('player2') else None
        player_name = "Unknown"
        sender_entity = await event.get_sender(); player_name = get_display_name(sender_entity) if sender_entity else f"User {user_id}"
        player_mention = get_player_mention(user_id, player_name)

        # ==================================
        # --- TEAM GAME CALLBACK LOGIC ---
        # ==================================
        if game_type == 'team':
            # --- Host Joins ---
            if action == "team_join" and state == STATE_TEAM_HOST_JOIN_WAIT and value == 'A':
                if user_id != host_id: return await safe_answer_callback(event, "Waiting for host.")
                udata = await asyncio.to_thread(get_user_data, user_id)
                if not udata: await safe_answer_callback(event,"DB Error",alert=True); return # Should not happen if host started
                p_name_join = udata.get('full_name', player_name)
                g['teams']['A']['players'].append(user_id); g['teams']['A']['names'][user_id] = p_name_join
                if 'player_stats' not in g['teams']['A']: g['teams']['A']['player_stats'] = {}
                g['teams']['A']['player_stats'][user_id] = {'runs': 0,'balls_faced': 0,'wickets_taken': 0,'balls_bowled': 0,'is_out': False, 'fours':0, 'sixes':0}
                g['state'] = STATE_TEAM_WAITING
                temp_kb = create_join_team_keyboard(game_id, g, user_id); players_txt = format_team_players_for_ui(g)
                host_m = get_player_mention(host_id, g['teams']['A']['names'].get(host_id))
                temp_text = f"⚔️ {g['max_players_per_team']}v{g['max_players_per_team']} Team Cricket!\nHost: {host_m}\n\n{players_txt}\n\nWaiting for players to join or leave..."
                g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_kb; msg_needs_update=True; await safe_answer_callback(event, "Joined! Waiting for others...")

            # --- Player Joins ---
            elif action == "team_join" and state == STATE_TEAM_WAITING and value in ['A', 'B']:
                target_team = value; max_p = g['max_players_per_team']
                team_players = g['teams'][target_team]['players']; other_players = g['teams']['B' if target_team == 'A' else 'A']['players']
                if user_id in team_players or user_id in other_players: return await safe_answer_callback(event, "You are already in a team!")
                if len(team_players) >= max_p: return await safe_answer_callback(event, f"Team {target_team} is full!")
                if user_id == host_id and target_team == 'B': return await safe_answer_callback(event, "Host must be in Team A.")
                
                udata = await asyncio.to_thread(get_user_data, user_id)
                if not udata:
                    join_kb = create_join_team_keyboard(game_id, g, user_id)
                    err_text = g['last_text'] + f"\n\n{player_mention} please /start me in DM first to use all features."
                    final_text=err_text; final_keyboard=join_kb; msg_needs_update=True; await safe_answer_callback(event); return
                
                p_name_join = udata.get('full_name', player_name)
                g['teams'][target_team]['players'].append(user_id); g['teams'][target_team]['names'][user_id] = p_name_join
                if 'player_stats' not in g['teams'][target_team]: g['teams'][target_team]['player_stats'] = {}
                g['teams'][target_team]['player_stats'][user_id] = {'runs': 0,'balls_faced': 0,'wickets_taken': 0,'balls_bowled': 0,'is_out': False, 'fours':0, 'sixes':0}
                
                players_txt_ui = format_team_players_for_ui(g)
                host_m_ui = get_player_mention(host_id, g['teams']['A']['names'].get(host_id, 'Host?'))
                temp_text_base = f"⚔️ {g['max_players_per_team']}v{g['max_players_per_team']} Team Cricket!\nHost: {host_m_ui}\n\n{players_txt_ui}\n\n"
                temp_text = temp_text_base + "Waiting for players to join or leave..."
                temp_keyboard = create_join_team_keyboard(game_id, g, user_id)
                g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True; await safe_answer_callback(event, f"You joined Team {target_team}!")

            # --- Player Leaves Team (Before Game Start) ---
            elif action == "team_leave" and state == STATE_TEAM_WAITING and value in ['A', 'B']:
                team_to_leave = value
                if user_id not in g['teams'][team_to_leave]['players']:
                    return await safe_answer_callback(event, "You are not in that team to leave.")

                # Host cannot leave Team A if it makes Team A empty and host is required for game start logic
                if user_id == host_id and team_to_leave == 'A' and len(g['teams']['A']['players']) == 1:
                    return await safe_answer_callback(event, "Host cannot leave Team A if they are the only member. /cancel if needed.", alert=True)

                g['teams'][team_to_leave]['players'].remove(user_id)
                if user_id in g['teams'][team_to_leave]['names']: del g['teams'][team_to_leave]['names'][user_id]
                if user_id in g['teams'][team_to_leave]['player_stats']: del g['teams'][team_to_leave]['player_stats'][user_id]
                
                players_txt_ui = format_team_players_for_ui(g)
                host_m_ui = get_player_mention(host_id, g['teams']['A']['names'].get(host_id, 'Host?'))
                temp_text_base = f"⚔️ {g['max_players_per_team']}v{g['max_players_per_team']} Team Cricket!\nHost: {host_m_ui}\n\n{players_txt_ui}\n\n"
                temp_text = temp_text_base + f"{player_mention} left Team {team_to_leave}. Waiting for players..."
                temp_keyboard = create_join_team_keyboard(game_id, g, user_id) # Pass user_id for contextual buttons
                g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True; 
                await safe_answer_callback(event, f"You left Team {team_to_leave}.")


            # --- Host Starts Game ---
            elif action == "start_game" and state == STATE_TEAM_WAITING:
                if user_id != host_id: return await safe_answer_callback(event, "Only host can start.")
                a_players = g['teams']['A']['players']; b_players = g['teams']['B']['players']
                a_count = len(a_players); b_count = len(b_players)
                if not (a_count >= 1 and b_count >= 1): return await safe_answer_callback(event, "Need >=1 player per team.", alert=True)
                g['actual_players_team_A'] = a_count; g['actual_players_team_B'] = b_count
                g['max_wickets_team_A'] = a_count; g['max_wickets_team_B'] = b_count # Max wickets is actual team size at start
                g['state'] = STATE_TEAM_TOSS_CALL
                
                # Captain of Team A (usually host) calls the toss
                caller_id = g['teams']['A']['players'][0] # Assuming first player in list is captain
                caller_name = g['teams']['A']['names'].get(caller_id, f"Captain A")
                caller_mention = get_player_mention(caller_id, caller_name)
                
                players_txt = format_team_players_for_ui(g) # Show final teams
                temp_text = f"⚔️ {a_count}v{b_count} Team Cricket Started!\n{players_txt}\n\nCoin Toss: {caller_mention} (Team A Captain), call Heads or Tails:"
                temp_keyboard = create_team_toss_keyboard(game_id)
                g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True; await safe_answer_callback(event)

            # --- Toss Call ---
            elif action == "team_toss" and state == STATE_TEAM_TOSS_CALL:
                caller_id = g['teams']['A']['players'][0] 
                caller_name = g['teams']['A']['names'].get(caller_id)
                if user_id != caller_id: return await safe_answer_callback(event, f"Waiting for {html.escape(caller_name)} to call the toss.")
                
                choice_call=value; flip_result=random.choice(['H','T']); toss_heads=(flip_result=='H')
                host_team_won_toss=(choice_call==flip_result)
                
                g['toss_winner_team'] = 'A' if host_team_won_toss else 'B'
                
                winner_captain_id = g['teams'][g['toss_winner_team']]['players'][0]
                winner_captain_name = g['teams'][g['toss_winner_team']]['names'].get(winner_captain_id, f"Captain {g['toss_winner_team']}")
                winner_mention=get_player_mention(winner_captain_id, winner_captain_name)
                
                g['state']=STATE_TEAM_BAT_BOWL_CHOICE
                players_txt_ui=format_team_players_for_ui(g)
                toss_txt = (f"⚔️ {g['actual_players_team_A']}v{g['actual_players_team_B']} Team Cricket\n{players_txt_ui}\n\n"
                            f"Toss Call: {html.escape(choice_call)}, Coin: <b>{'Heads' if toss_heads else 'Tails'}</b>!\n"
                            f"Team {g['toss_winner_team']} (Captain: {winner_mention}) won the toss.\n➡️ {winner_mention}, choose Bat or Bowl:")
                temp_kb=create_team_batbowl_keyboard(game_id); 
                g['last_text']=toss_txt; final_text=toss_txt; final_keyboard=temp_kb; msg_needs_update=True; await safe_answer_callback(event)

            # --- Bat/Bowl Choice ---
            elif action == "team_batorbowl" and state == STATE_TEAM_BAT_BOWL_CHOICE:
                toss_winner_team_id=g.get('toss_winner_team');
                if not toss_winner_team_id: return await safe_answer_callback(event,"Internal Error: Toss winner not set.",alert=True)
                
                chooser_id = g['teams'][toss_winner_team_id]['players'][0] # Captain makes choice
                chooser_name = g['teams'][toss_winner_team_id]['names'].get(chooser_id)
                if user_id != chooser_id: return await safe_answer_callback(event, f"Waiting for {html.escape(chooser_name)} (Captain of Team {toss_winner_team_id}) to choose.")
                
                choice_made=value; # 'bat' or 'bowl'
                if choice_made not in ['bat','bowl']: return await safe_answer_callback(event,"Invalid choice (bat/bowl).")
                
                bat_first_team_id = toss_winner_team_id if choice_made == 'bat' else ('B' if toss_winner_team_id == 'A' else 'A')
                bowl_first_team_id = 'B' if bat_first_team_id == 'A' else 'A'
                
                g['choice'] = choice_made 
                g['first_batting_team_id'] = bat_first_team_id
                g.update({'current_batting_team': bat_first_team_id, 
                          'current_bowling_team': bowl_first_team_id, 
                          'state': STATE_TEAM_HOST_SELECT_BATTER, # Host selects first batter
                          'innings': 1, 
                          'balls_bowled_this_inning': 0, 
                          'balls_this_over': 0,
                          'current_batter_id': None, # Reset for host selection
                          'current_bowler_id': None  # Reset for host selection
                          })
                for t_id_loop in ['A','B']: g['teams'][t_id_loop]['score']=0; g['teams'][t_id_loop]['wickets']=0 # Reset scores/wickets
                
                host_m_ui=get_player_mention(host_id,g['teams']['A']['names'].get(host_id)) # Host is always from Team A initially
                sel_txt=(f"Team {toss_winner_team_id} chose to <b>{choice_made.upper()}</b>.\n"
                         f"Team {bat_first_team_id} will bat first.\n\n"
                         f"➡️ Host ({host_m_ui}), select 1st batter for Team {bat_first_team_id}:")
                sel_kb=create_player_selection_keyboard(game_id, bat_first_team_id, g, "sel_bat")
                g['last_text']=sel_txt; final_text=sel_txt; final_keyboard=sel_kb; msg_needs_update=True; await safe_answer_callback(event)

            # --- Host Select Batter ---
            elif action == "sel_bat" and state == STATE_TEAM_HOST_SELECT_BATTER:
                if user_id != host_id: return await safe_answer_callback(event, "Waiting for Host to select batter.")
                try: selected_batter_id = int(value)
                except (ValueError, TypeError): return await safe_answer_callback(event, "Invalid batter selection.", alert=True)
                
                bat_team_current=g['current_batting_team']
                if selected_batter_id not in g['teams'][bat_team_current]['players']: 
                    return await safe_answer_callback(event,"Selected player is not in the current batting team.", alert=True)
                
                # Check if player is already out
                if g['teams'][bat_team_current]['player_stats'].get(selected_batter_id, {}).get('is_out', False):
                    return await safe_answer_callback(event, "This player is already out. Select another.", alert=True)

                g['current_batter_id'] = selected_batter_id
                g['state'] = STATE_TEAM_HOST_SELECT_BOWLER # Next, select bowler
                
                host_m_ui=get_player_mention(host_id,g['teams']['A']['names'].get(host_id))
                bowl_team_current=g['current_bowling_team']
                batter_selected_name = g['teams'][bat_team_current]['names'].get(selected_batter_id, f"Batter {selected_batter_id}")
                
                sel_txt=(f"Batter {html.escape(batter_selected_name)} (Team {bat_team_current}) selected.\n\n"
                         f"➡️ Host ({host_m_ui}), select 1st bowler for Team {bowl_team_current} (or next bowler if new over):")
                sel_kb=create_player_selection_keyboard(game_id, bowl_team_current, g, "sel_bowl")
                g['last_text']=sel_txt; final_text=sel_txt; final_keyboard=sel_kb; msg_needs_update=True; await safe_answer_callback(event)


            # --- Host Select Bowler ---
            elif action == "sel_bowl" and state == STATE_TEAM_HOST_SELECT_BOWLER:
                if user_id != host_id: return await safe_answer_callback(event, "Waiting for Host to select bowler.")
                try: selected_bowler_id = int(value)
                except (ValueError, TypeError): return await safe_answer_callback(event, "Invalid bowler selection.", alert=True)

                bowl_team_current=g['current_bowling_team']
                if selected_bowler_id not in g['teams'][bowl_team_current]['players']: 
                    return await safe_answer_callback(event,"Selected player is not in the current bowling team.", alert=True)
                
                g['current_bowler_id'] = selected_bowler_id
                g['state'] = STATE_TEAM_BATTING # Ready to play
                
                status_txt, batter_m_ui, bowler_m_ui = format_team_game_status(g)
                play_txt = f"Alright, let's play!\n\n{status_txt}\n\n➡️ {batter_m_ui}, select your shot (1-6):"
                play_kb = create_standard_keyboard(game_id)
                g['last_text']=play_txt; final_text=play_txt; final_keyboard=play_kb; msg_needs_update=True; await safe_answer_callback(event)


            # --- Gameplay Number Input (Team) ---
            elif action == "num" and state in [STATE_TEAM_BATTING, STATE_TEAM_BOWLING_WAIT]:
                if numeric_value is None or not (1 <= numeric_value <= 6): return await safe_answer_callback(event, "Invalid input (1-6).", alert=True)
                
                batter_id_current=g['current_batter_id']; bowler_id_current=g['current_bowler_id']
                bat_team_id_current=g['current_batting_team']; bowl_team_id_current=g['current_bowling_team']
                
                if not batter_id_current or not bowler_id_current:
                    logger.error(f"G:{game_id} Missing batter/bowler ID. B:{batter_id_current}, BWL:{bowler_id_current}, State:{state}")
                    # Try to recover by asking host to select again
                    g['state'] = STATE_TEAM_HOST_SELECT_BATTER if not batter_id_current else STATE_TEAM_HOST_SELECT_BOWLER
                    host_m_ui=get_player_mention(host_id,g['teams']['A']['names'].get(host_id))
                    rec_txt = f"Error: Player selection lost. Host ({host_m_ui}), please re-select "
                    rec_txt += "batter." if g['state'] == STATE_TEAM_HOST_SELECT_BATTER else "bowler."
                    rec_kb_team = create_player_selection_keyboard(game_id, g['current_batting_team'] if g['state'] == STATE_TEAM_HOST_SELECT_BATTER else g['current_bowling_team'], g, "sel_bat" if g['state'] == STATE_TEAM_HOST_SELECT_BATTER else "sel_bowl")
                    final_text=rec_txt; final_keyboard=rec_kb_team; msg_needs_update=True; await safe_answer_callback(event, "Selection error, host to reselect.", alert=True); return


                batter_stats = g['teams'][bat_team_id_current]['player_stats'][batter_id_current]
                bowler_stats = g['teams'][bowl_team_id_current]['player_stats'][bowler_id_current]
                batter_name_disp = g['teams'][bat_team_id_current]['names'].get(batter_id_current, f"Batter {batter_id_current}")
                bowler_name_disp = g['teams'][bowl_team_id_current]['names'].get(bowler_id_current, f"Bowler {bowler_id_current}")

                current_inning_num_val = g['innings']
                current_inning_stats_obj = g['inning_stats'][current_inning_num_val]

                if state == STATE_TEAM_BATTING:
                    if user_id != batter_id_current: return await safe_answer_callback(event, f"Waiting for {html.escape(batter_name_disp)} to bat.")
                    g['batter_choice'] = numeric_value; g['state'] = STATE_TEAM_BOWLING_WAIT
                    status_txt_ui, _, bowler_m_ui = format_team_game_status(g, batter_played=True)
                    temp_text = f"{status_txt_ui}\n\n➡️ {bowler_m_ui}, select your delivery (1-6):"
                    temp_kb = create_standard_keyboard(game_id); 
                    g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_kb; msg_needs_update=True; 
                    await safe_answer_callback(event, f"You played {numeric_value}. Waiting for bowler...")

                elif state == STATE_TEAM_BOWLING_WAIT:
                    if user_id != bowler_id_current: return await safe_answer_callback(event, f"Waiting for {html.escape(bowler_name_disp)} to bowl.")
                    bat_num_choice = g.get('batter_choice')
                    if bat_num_choice is None: # Batter's choice lost
                        g['state']=STATE_TEAM_BATTING; 
                        btr_m_ui = get_player_mention(batter_id_current,batter_name_disp); status_txt_ui,_,_ = format_team_game_status(g)
                        err_txt = f"⚠️ Error: Batter's choice was lost.\n{status_txt_ui}\n➡️ {btr_m_ui}, please play your shot again:"; 
                        err_kb = create_standard_keyboard(game_id); 
                        g['last_text']=err_txt; final_text=err_txt; final_keyboard=err_kb; msg_needs_update=True; 
                        await safe_answer_callback(event,"Error! Batter needs to play again.",alert=True); return

                    bowl_num_delivery = numeric_value
                    g['balls_bowled_this_inning'] += 1
                    g['balls_this_over'] = g.get('balls_this_over', 0) + 1
                    current_inning_stats_obj['runs_this_over_list'].append(0) # Placeholder, updated if runs scored

                    batter_stats['balls_faced'] += 1
                    bowler_stats['balls_bowled'] += 1
                    innings_ended_this_ball_flag = False
                    batter_m_disp = get_player_mention(batter_id_current, batter_name_disp); bowler_m_disp = get_player_mention(bowler_id_current, bowler_name_disp)
                    result_prefix_text = f"{batter_m_disp} (chose <code>{bat_num_choice}</code>) | {bowler_m_disp} (bowled <code>{bowl_num_delivery}</code>)\n\n"
                    result_txt_display = ""

                    if bat_num_choice == bowl_num_delivery: # OUT
                        g['teams'][bat_team_id_current]['wickets'] += 1; batter_stats['is_out'] = True; bowler_stats['wickets_taken'] += 1; g['last_out_player_id'] = batter_id_current
                        wickets_fallen = g['teams'][bat_team_id_current]['wickets']; max_wickets_for_team = g.get(f'max_wickets_team_{bat_team_id_current}')
                        
                        result_txt_display = result_prefix_text + f"💥 <b>OUT!</b> ({wickets_fallen}/{max_wickets_for_team} Wickets for Team {bat_team_id_current})\n"
                        db_updates.append({'type':'wicket','user_id':bowler_id_current})
                        current_inning_stats_obj['dots'] += 1; g['match_stats']['total_dots'] +=1
                        current_inning_stats_obj['runs_this_over_list'][-1] = 0 # Wicket is 0 runs

                        if wickets_fallen >= max_wickets_for_team:
                            innings_ended_this_ball_flag = True; result_txt_display += "Innings End! (All out for Team " + bat_team_id_current + ")\n"
                    else: # RUNS
                        runs_scored_ball = bat_num_choice; g['teams'][bat_team_id_current]['score']+=runs_scored_ball; batter_stats['runs']+=runs_scored_ball
                        result_txt_display = result_prefix_text + f"🏏 <b>{runs_scored_ball}</b> runs scored! Team {bat_team_id_current} score: {g['teams'][bat_team_id_current]['score']}/{g['teams'][bat_team_id_current]['wickets']}\n"
                        db_updates.append({'type':'runs','user_id':batter_id_current,'value':runs_scored_ball})
                        current_inning_stats_obj['runs_this_over_list'][-1] = runs_scored_ball

                        if runs_scored_ball == 4: current_inning_stats_obj['fours'] += 1; batter_stats['fours'] +=1; g['match_stats']['total_fours'] +=1
                        elif runs_scored_ball == 6: current_inning_stats_obj['sixes'] += 1; batter_stats['sixes'] +=1; g['match_stats']['total_sixes'] +=1
                        elif runs_scored_ball == 0: current_inning_stats_obj['dots'] += 1; g['match_stats']['total_dots'] +=1

                        if g['innings'] == 2 and g['teams'][bat_team_id_current]['score'] >= g['target']:
                            innings_ended_this_ball_flag = True; game_ended_flag = True; result_txt_display += "Target Chased! Game Over!\n"; g['state']=STATE_TEAM_ENDED
                            player_ids_to_inc_match.extend(g['teams']['A']['players']+g['teams']['B']['players'])
                            winning_player_ids_for_credits.extend(g['teams'][bat_team_id_current]['players'])

                    if g['balls_bowled_this_inning'] >= g['max_balls'] and not innings_ended_this_ball_flag:
                        innings_ended_this_ball_flag = True; result_txt_display += "Innings End! (Maximum overs bowled)\n"

                    over_completed_this_ball_team = False
                    if g['balls_this_over'] >= 6 or innings_ended_this_ball_flag : 
                        over_completed_this_ball_team = True
                        runs_in_this_over_val = sum(current_inning_stats_obj['runs_this_over_list'])
                        current_inning_stats_obj['overs_completed_runs'].append(runs_in_this_over_val)
                        g['match_stats']['runs_in_overs'].append(runs_in_this_over_val)
                        if runs_in_this_over_val > g['match_stats']['best_over_runs']: g['match_stats']['best_over_runs'] = runs_in_this_over_val
                        current_inning_stats_obj['runs_this_over_list'] = [] 
                        
                        if not innings_ended_this_ball_flag : result_txt_display += "\n✨ **Over Complete!** ✨\n"
                        if g['balls_this_over'] >= 6 : g['balls_this_over'] = 0 
                    
                    # Innings End / Game End / Continue Logic
                    if innings_ended_this_ball_flag:
                        g['balls_bowled_inning' + str(g['innings'])] = g['balls_bowled_this_inning']
                        if g['innings'] == 1: # First innings ended
                            g['target'] = g['teams'][bat_team_id_current]['score'] + 1
                            result_txt_display += f"Target for Team {bowl_team_id_current} to chase: <b>{g['target']}</b>"
                            # Swap batting/bowling teams
                            g['current_batting_team'], g['current_bowling_team'] = bowl_team_id_current, bat_team_id_current
                            g['innings'] = 2; g['balls_bowled_this_inning'] = 0; g['balls_this_over'] = 0; g['batter_choice'] = None
                            g['current_batter_id'] = None; g['current_bowler_id'] = None # Host will select
                            g['state'] = STATE_TEAM_HOST_SELECT_BATTER # Host selects first batter for 2nd innings
                        else: # Innings 2 ended
                            game_ended_flag = True
                            if not result_txt_display.endswith("Game Over!\n"): result_txt_display += "\n<b>Game Over!</b>"
                            g['state'] = STATE_TEAM_ENDED
                            player_ids_to_inc_match.extend(g['teams']['A']['players'] + g['teams']['B']['players'])
                            if not winning_player_ids_for_credits: # If not won by chasing
                                inn1_bat_team_final_score = g['teams'][g['first_batting_team_id']]['score']
                                inn2_bat_team_final_score = g['teams'][bat_team_id_current]['score'] # bat_team_id_current is the one batting in inn2
                                if inn1_bat_team_final_score > inn2_bat_team_final_score:
                                    winning_player_ids_for_credits.extend(g['teams'][g['first_batting_team_id']]['players'])
                                # Tie case: no credits for win (already handled by score comparison)
                    
                    else: # Innings continues (ball played, not ended innings)
                        g['batter_choice'] = None # Reset batter choice for next ball
                        if bat_num_choice == bowl_num_delivery: # OUT, host selects next batter
                            g['current_batter_id'] = None # Host will select
                            g['state'] = STATE_TEAM_HOST_SELECT_BATTER
                        elif over_completed_this_ball_team: # Over completed, host selects new batter then bowler
                            g['current_batter_id'] = None; g['current_bowler_id'] = None
                            g['state'] = STATE_TEAM_HOST_SELECT_BATTER
                        else: # Runs scored, same batter continues, same bowler (unless over was completed by this ball but handled above)
                            g['state'] = STATE_TEAM_BATTING
                    
                    # Format Message for next action or game end
                    if game_ended_flag:
                        final_text = format_final_scorecard(g) 
                        final_keyboard = None
                    elif g['state'] == STATE_TEAM_HOST_SELECT_BATTER: # Innings ended, new innings OR wicket OR over complete
                        host_m_ui=get_player_mention(host_id,g['teams']['A']['names'].get(host_id))
                        next_bat_team_id_sel = g['current_batting_team']
                        action_reason = "New innings."
                        if g['last_out_player_id'] and not innings_ended_this_ball_flag : action_reason = "Wicket fell."
                        elif over_completed_this_ball_team and not innings_ended_this_ball_flag: action_reason = "Over completed."
                        
                        final_text = f"{result_txt_display}\n{action_reason}\n\n➡️ Host ({host_m_ui}), select batter for Team {next_bat_team_id_sel}:"
                        final_keyboard=create_player_selection_keyboard(game_id, next_bat_team_id_sel, g, "sel_bat")
                    
                    # This state (STATE_TEAM_HOST_SELECT_BOWLER) is reached via sel_bat callback now
                    # elif g['state'] == STATE_TEAM_HOST_SELECT_BOWLER: ... 

                    elif g['state'] == STATE_TEAM_BATTING : # Innings continues, same batter or new batter selected, now batter's turn
                        status_txt_ui, next_batter_m_ui, _ = format_team_game_status(g)
                        final_text = f"{result_txt_display}\n{status_txt_ui}\n\n➡️ {next_batter_m_ui}, select shot (1-6):"
                        final_keyboard=create_standard_keyboard(game_id)
                    else: # Fallback / Error
                        final_text = f"{result_txt_display}\n\nInternal State Error. Current state: {g['state']}"
                        final_keyboard = None
                        if g['state'] != STATE_TEAM_ENDED: 
                            g['state'] = STATE_TEAM_ENDED; game_ended_flag = True
                            player_ids_to_inc_match.extend(g['teams']['A']['players']+g['teams']['B']['players'])
                            final_text += "\n" + format_final_scorecard(g)

                    g['last_text']=final_text; msg_needs_update=True; await safe_answer_callback(event)
        
        # ================================
        # --- 1V1 GAME CALLBACK LOGIC --- (STRUCTURAL FIX + Scorecard data)
        # ================================
        elif game_type == '1v1': # <<< ENSURE THIS elif IS CORRECTLY PLACED
            p1_obj = g['player1'] # Ensure p1_obj and p2_obj are defined if game_type is 1v1
            p2_obj = g['player2'] # Will be None if P2 hasn't joined yet

            # --- Player 2 Joins ---
            if action == "join_1v1" and state == STATE_WAITING:
                if user_id == p1_id: return await safe_answer_callback(event, "You started this game.")
                if p2_obj is not None: return await safe_answer_callback(event, "This game is already full (2 players).") # Check p2_obj directly
                
                udata=await asyncio.to_thread(get_user_data, user_id)
                if not udata:
                    join_kb_1v1=client.build_reply_markup([[Button.inline("Join Game",data=f"join_1v1::{game_id}")]]) # Note: game_id has :: here
                    err_text=g['last_text']+f"\n\n{player_mention} please /start me in DM first to use all features."
                    final_text=err_text; final_keyboard=join_kb_1v1; msg_needs_update=True; await safe_answer_callback(event); return
                
                p2_name_join=udata.get('full_name', player_name)
                g['player2']={'id':user_id,'name':p2_name_join,'score':0,'balls_faced':0,'balls_bowled':0,'wickets_taken':0, 'fours':0, 'sixes':0}
                p2_obj = g['player2'] # Update p2_obj after assignment
                
                g['state']=STATE_TOSS
                p1_m_ui=get_player_mention(p1_id,g['player1']['name']); p2_m_ui=get_player_mention(user_id,p2_name_join)
                toss_text_1v1=f"⚔️ 1v1 Game Ready!\n{p1_m_ui} vs {p2_m_ui}\n\nTime for the Toss!\n➡️ {p1_m_ui} (P1), please call Heads or Tails:"
                toss_kb_1v1=[[Button.inline("Heads",data=f"toss:H:{game_id}"),Button.inline("Tails",data=f"toss:T:{game_id}")]]
                g['last_text']=toss_text_1v1; final_text=toss_text_1v1; final_keyboard=toss_kb_1v1; msg_needs_update=True; await safe_answer_callback(event, "You've joined the game!")

            # --- Toss Call (1v1) ---
            elif action == "toss" and state == STATE_TOSS:
                if not p1_obj or not p2_obj: return await safe_answer_callback(event, "Error: Player data missing for toss.", alert=True)
                if user_id != p1_id: return await safe_answer_callback(event, f"Waiting for {html.escape(p1_obj['name'])} (P1) to call the toss.")
                
                choice_call_1v1=value; flip_result_1v1=random.choice(['H','T']); toss_heads_1v1=(flip_result_1v1=='H')
                p1_won_toss=(choice_call_1v1==flip_result_1v1)
                winner_id_toss = p1_id if p1_won_toss else p2_obj['id']
                winner_name_toss = p1_obj['name'] if p1_won_toss else p2_obj['name']
                winner_m_ui=get_player_mention(winner_id_toss,winner_name_toss)
                
                g['toss_winner_id']=winner_id_toss; g['state']=STATE_BAT_BOWL
                p1_m_disp=get_player_mention(p1_id,p1_obj['name']); p2_m_disp=get_player_mention(p2_obj['id'],p2_obj['name'])
                toss_res_text=(f"⚔️ 1v1: {p1_m_disp} vs {p2_m_disp}\n"
                               f"Toss Call: {html.escape(choice_call_1v1)}, Coin landed on: <b>{'Heads' if toss_heads_1v1 else 'Tails'}</b>!\n"
                               f"{winner_m_ui} won the toss.\n➡️ {winner_m_ui}, please choose to Bat or Bowl first:")
                bb_kb_1v1=[[Button.inline("Bat 🏏",data=f"batorbowl:bat:{game_id}"),Button.inline("Bowl 🧤",data=f"batorbowl:bowl:{game_id}")]]
                g['last_text']=toss_res_text; final_text=toss_res_text; final_keyboard=bb_kb_1v1; msg_needs_update=True; await safe_answer_callback(event)

            # --- Bat/Bowl Choice (1v1) ---
            elif action == "batorbowl" and state == STATE_BAT_BOWL:
                toss_winner_id_val=g.get('toss_winner_id');
                if not p1_obj or not p2_obj: return await safe_answer_callback(event, "Error: Player data missing for bat/bowl choice.", alert=True)
                if user_id != toss_winner_id_val: 
                    winner_name_for_wait = p1_obj['name'] if toss_winner_id_val == p1_obj['id'] else p2_obj['name']
                    return await safe_answer_callback(event,f"Waiting for {html.escape(winner_name_for_wait)} (toss winner) to choose.")
                
                choice_made_1v1=value; # 'bat' or 'bowl'
                if choice_made_1v1 not in ['bat','bowl']: return await safe_answer_callback(event,"Invalid choice (must be bat or bowl).")
                
                bat_first_player_id = toss_winner_id_val if choice_made_1v1=='bat' else (p2_obj['id'] if toss_winner_id_val==p1_obj['id'] else p1_obj['id'])
                bowl_first_player_id = p2_obj['id'] if bat_first_player_id==p1_obj['id'] else p1_obj['id']
                
                g['choice']=choice_made_1v1 
                g['first_batter_id'] = bat_first_player_id 

                g.update({'current_batter_id':bat_first_player_id,
                          'current_bowler_id':bowl_first_player_id,
                          'state':STATE_P1_BAT if bat_first_player_id==p1_obj['id'] else STATE_P2_BAT, # Sets P1_BAT or P2_BAT
                          'innings':1, 'balls_bowled_this_inning':0, 'balls_this_over': 0})
                
                current_batter_obj_disp = p1_obj if g['current_batter_id'] == p1_obj['id'] else p2_obj
                toss_winner_name_disp_ui = p1_obj['name'] if toss_winner_id_val == p1_obj['id'] else p2_obj['name']

                status_txt_1v1,batter_m_1v1,_ = format_1v1_game_status(g)
                play_txt_1v1=(f"{get_player_mention(toss_winner_id_val, toss_winner_name_disp_ui)} chose to <b>{choice_made_1v1.upper()}</b>.\n"
                              f"{get_player_mention(bat_first_player_id, current_batter_obj_disp['name'])} will bat first.\n\n"
                              f"{status_txt_1v1}\n\n➡️ {batter_m_1v1}, select your shot (1-6):")
                play_kb_1v1=create_standard_keyboard(game_id); 
                g['last_text']=play_txt_1v1; final_text=play_txt_1v1; final_keyboard=play_kb_1v1; msg_needs_update=True; await safe_answer_callback(event)


            # --- Number Input (1v1 Gameplay) ---
            elif action == "num" and state in [STATE_P1_BAT,STATE_P1_BOWL_WAIT,STATE_P2_BAT,STATE_P2_BOWL_WAIT]:
                if numeric_value is None or not(1<=numeric_value<=6): return await safe_answer_callback(event,"Invalid input (1-6).",alert=True)
                if not p1_obj or not p2_obj: return await safe_answer_callback(event, "Error: Player data missing for gameplay.", alert=True)

                batter_id_1v1 = g['current_batter_id']; bowler_id_1v1 = g['current_bowler_id']
                batter_obj_1v1 = p1_obj if batter_id_1v1 == p1_obj['id'] else p2_obj
                bowler_obj_1v1 = p1_obj if bowler_id_1v1 == p1_obj['id'] else p2_obj
                
                batter_name_1v1=batter_obj_1v1['name']; bowler_name_1v1=bowler_obj_1v1['name']
                is_p1_bat_state_now=(state == STATE_P1_BAT); is_p2_bat_state_now=(state == STATE_P2_BAT)
                is_bat_state_1v1 = is_p1_bat_state_now or is_p2_bat_state_now
                is_bowl_state_1v1=(state in [STATE_P1_BOWL_WAIT,STATE_P2_BOWL_WAIT])
                
                current_inning_num_1v1 = g['innings']
                current_inning_stats_1v1 = g['inning_stats'][current_inning_num_1v1]

                if is_bat_state_1v1:
                    if user_id!=batter_id_1v1: return await safe_answer_callback(event,f"Waiting for {html.escape(batter_name_1v1)} to bat.")
                    g['batter_choice']=numeric_value
                    g['state']=STATE_P1_BOWL_WAIT if is_p1_bat_state_now else STATE_P2_BOWL_WAIT # Switch to bowler's wait state
                    
                    status_txt_bat,_,bowler_m_bat=format_1v1_game_status(g,batter_played=True)
                    temp_text_bat=f"{status_txt_bat}\n\n➡️ {bowler_m_bat}, select your delivery (1-6):"; 
                    temp_kb_bat=create_standard_keyboard(game_id);
                    g['last_text']=temp_text_bat; final_text=temp_text_bat; final_keyboard=temp_kb_bat; msg_needs_update=True; 
                    await safe_answer_callback(event,f"You played {numeric_value}. Waiting for bowler...")
                
                elif is_bowl_state_1v1:
                    if user_id!=bowler_id_1v1: return await safe_answer_callback(event,f"Waiting for {html.escape(bowler_name_1v1)} to bowl.")
                    bat_num_choice_1v1=g.get('batter_choice');
                    if bat_num_choice_1v1 is None: # Batter choice lost
                        g['state']=STATE_P1_BAT if state == STATE_P1_BOWL_WAIT else STATE_P2_BAT # Back to batter's turn
                        btr_m_err=get_player_mention(batter_id_1v1,batter_name_1v1); status_txt_err,_,_=format_1v1_game_status(g)
                        err_txt_1v1=f"⚠️ Error: Batter's choice was lost.\n{status_txt_err}\n➡️ {btr_m_err}, please play your shot again:"; 
                        err_kb_1v1=create_standard_keyboard(game_id); 
                        g['last_text']=err_txt_1v1; final_text=err_txt_1v1; final_keyboard=err_kb_1v1; msg_needs_update=True; 
                        await safe_answer_callback(event,"Error! Batter needs to play again.",alert=True); return

                    bowl_num_delivery_1v1 = numeric_value
                    g['balls_bowled_this_inning'] += 1
                    g['balls_this_over'] = g.get('balls_this_over', 0) + 1
                    current_inning_stats_1v1['runs_this_over_list'].append(0) 

                    batter_obj_1v1['balls_faced'] += 1; bowler_obj_1v1['balls_bowled'] += 1
                    innings_ended_1v1_flag=False;
                    batter_m_disp_1v1 = get_player_mention(batter_id_1v1, batter_name_1v1); 
                    bowler_m_disp_1v1 = get_player_mention(bowler_id_1v1, bowler_name_1v1)
                    result_prefix_1v1 = f"{batter_m_disp_1v1} (chose <code>{bat_num_choice_1v1}</code>) | {bowler_m_disp_1v1} (bowled <code>{bowl_num_delivery_1v1}</code>)\n\n"
                    result_txt_1v1 = ""

                    if bat_num_choice_1v1 == bowl_num_delivery_1v1: # OUT
                        innings_ended_1v1_flag=True; 
                        # batter_obj_1v1['wickets_taken'] = 1 # This was incorrect, batter doesn't take their own wicket.
                        bowler_obj_1v1['wickets_taken'] +=1 # Bowler gets the wicket stat.
                        result_txt_1v1 = result_prefix_1v1 + "💥 <b>OUT! Innings End!</b>\n"
                        db_updates.append({'type':'wicket','user_id':bowler_id_1v1})
                        current_inning_stats_1v1['dots'] += 1; g['match_stats']['total_dots'] +=1
                        current_inning_stats_1v1['runs_this_over_list'][-1] = 0
                    else: # RUNS
                        runs_scored_1v1=bat_num_choice_1v1; batter_obj_1v1['score']+=runs_scored_1v1
                        result_txt_1v1 = result_prefix_1v1 + f"🏏 <b>{runs_scored_1v1}</b> runs scored! Score: {batter_obj_1v1['score']}/0\n" 
                        db_updates.append({'type':'runs','user_id':batter_id_1v1,'value':runs_scored_1v1})
                        current_inning_stats_1v1['runs_this_over_list'][-1] = runs_scored_1v1

                        if runs_scored_1v1 == 4: current_inning_stats_1v1['fours'] += 1; batter_obj_1v1['fours'] +=1; g['match_stats']['total_fours'] +=1
                        elif runs_scored_1v1 == 6: current_inning_stats_1v1['sixes'] += 1; batter_obj_1v1['sixes'] +=1; g['match_stats']['total_sixes'] +=1
                        elif runs_scored_1v1 == 0: current_inning_stats_1v1['dots'] += 1; g['match_stats']['total_dots'] +=1
                        
                        if g['innings']==2 and batter_obj_1v1['score']>=g['target']:
                            innings_ended_1v1_flag=True; game_ended_flag=True; result_txt_1v1+="Target Chased! Game Over!\n"; g['state']=STATE_1V1_ENDED; 
                            player_ids_to_inc_match.extend([p1_obj['id'],p2_obj['id']])
                            winning_player_ids_for_credits.append(batter_id_1v1)

                    if g['balls_bowled_this_inning'] >= g['max_balls'] and not innings_ended_1v1_flag:
                        innings_ended_1v1_flag=True; result_txt_1v1+="Innings End! (Maximum overs bowled)\n"

                    over_completed_this_ball_1v1_flag = False
                    if g['balls_this_over'] >= 6 or innings_ended_1v1_flag:
                        over_completed_this_ball_1v1_flag = True
                        runs_in_this_over_1v1_val = sum(current_inning_stats_1v1['runs_this_over_list'])
                        current_inning_stats_1v1['overs_completed_runs'].append(runs_in_this_over_1v1_val)
                        g['match_stats']['runs_in_overs'].append(runs_in_this_over_1v1_val)
                        if runs_in_this_over_1v1_val > g['match_stats']['best_over_runs']: g['match_stats']['best_over_runs'] = runs_in_this_over_1v1_val
                        current_inning_stats_1v1['runs_this_over_list'] = []
                        if not innings_ended_1v1_flag : result_txt_1v1 += "\n✨ **Over Complete!** ✨\n"
                        if g['balls_this_over'] >= 6 : g['balls_this_over'] = 0
                    
                    if innings_ended_1v1_flag and not game_ended_flag: # Innings ended, but game not over (i.e., 1st innings ended)
                        g['balls_bowled_inning' + str(g['innings'])] = g['balls_bowled_this_inning']
                        if g['innings'] == 1:
                            g['target'] = batter_obj_1v1['score']+1; result_txt_1v1+=f"Target for {html.escape(bowler_obj_1v1['name'])} to chase: <b>{g['target']}</b>";
                            # Swap batter and bowler
                            g['current_batter_id'],g['current_bowler_id']=bowler_id_1v1,batter_id_1v1
                            g['innings']=2; g['balls_bowled_this_inning']=0; g['balls_this_over'] = 0
                            g['state']=STATE_P1_BAT if bowler_id_1v1==p1_obj['id'] else STATE_P2_BAT; 
                            g['batter_choice']=None
                        else: # Innings 2 ended (e.g. max overs reached before target or wicket)
                            game_ended_flag=True;
                            if not result_txt_1v1.endswith("Game Over!\n"): result_txt_1v1+="\n<b>Game Over!</b>"
                            g['state']=STATE_1V1_ENDED; player_ids_to_inc_match.extend([p1_obj['id'],p2_obj['id']])
                            if not winning_player_ids_for_credits : # If target not chased by batter_obj_1v1 (who was P2's turn)
                                if batter_obj_1v1['score'] < g['target'] -1 : # Target not reached, and not a tie
                                    winning_player_ids_for_credits.append(bowler_id_1v1) # Bowler of this innings (P1) wins
                    
                    elif not game_ended_flag: # Game continues (current innings continues)
                        g['batter_choice']=None
                        # Batter's turn again (same batter, same bowler unless over completed)
                        g['state']=STATE_P1_BAT if state == STATE_P1_BOWL_WAIT else STATE_P2_BAT

                    # Format message for 1v1
                    if game_ended_flag: 
                        final_text = format_final_scorecard(g)
                        final_keyboard = None
                    else: # Game continues, set up for next ball
                        status_txt_next,next_batter_m_next,_ = format_1v1_game_status(g); 
                        final_text=f"{result_txt_1v1}\n{status_txt_next}\n\n➡️ {next_batter_m_next}, select your shot (1-6):"; 
                        final_keyboard=create_standard_keyboard(game_id)

                    g['last_text']=final_text; msg_needs_update=True; await safe_answer_callback(event)
            
            else: # Fallback for unhandled 1v1 actions
                 logger.warning(f"Unhandled 1v1 callback: Action='{action}', State='{state}', GID='{game_id}'")
                 await safe_answer_callback(event, "Internal: Unhandled 1v1 action.", alert=True)
        
        else: # Fallback for unknown game_type
            logger.error(f"Unknown game_type '{game_type}' in callback for GID='{game_id}'")
            await safe_answer_callback(event, "Internal Error: Unknown game type.", alert=True)


    # --- End of Async With games_lock ---

    if db_updates and users_collection is not None:
        tasks = [add_runs_to_user(up['user_id'],up['value']) if up['type']=='runs' else add_wicket_to_user(up['user_id']) for up in db_updates]
        if tasks: await asyncio.gather(*tasks)
    
    if msg_needs_update and final_text is not None:
         markup = client.build_reply_markup(final_keyboard) if final_keyboard else None
         await safe_edit_message(chat_id, message_id, final_text, buttons=markup)

    if game_ended_flag:
        logger.info(f"Game {game_id} ended. Processing post-game actions.")
        if player_ids_to_inc_match and users_collection is not None:
            await increment_matches_played(player_ids_to_inc_match)
        
        if winning_player_ids_for_credits and users_collection is not None:
            for winner_id_credit in winning_player_ids_for_credits:
                credit_awarded = await add_credits_to_user(winner_id_credit, WIN_CREDITS)
                if credit_awarded == True: 
                    logger.info(f"Awarded {WIN_CREDITS} credits to winner {winner_id_credit} in G:{game_id}")
                elif credit_awarded == "insufficient": 
                     logger.error(f"Credit award error 'insufficient' for {winner_id_credit} G:{game_id}")
                else: 
                     logger.error(f"Failed to award credits to {winner_id_credit} G:{game_id}")

        await cleanup_game(game_id, chat_id, reason="finished normally")


# --- Helper: Format Team Players for UI (Unchanged) ---
def format_team_players_for_ui(game_data):
    g = game_data; text = ""; max_p = g.get('max_players_per_team', '?')
    for team_id in ['A', 'B']:
        team_info = g['teams'].get(team_id, {}); players = team_info.get('players', []); names = team_info.get('names', {}); count = len(players)
        text += f"<b>Team {team_id} ({count}/{max_p}):</b>"
        if players:
            text += "\n"; player_mentions = [f"  • {get_player_mention(p_id, names.get(p_id, f'User {p_id}'))}" for p_id in players]
            text += "\n".join(player_mentions) + "\n"
        else: text += " <i>(Empty)</i>\n"
        if team_id == 'A': text += "\n"
    return text.strip()


# --- Game Status Formatting (Unchanged) ---
def format_team_game_status(game_data, batter_played=False):
     g = game_data
     bat_team = g.get('current_batting_team'); bowl_team = g.get('current_bowling_team')
     batter_id = g.get('current_batter_id'); bowler_id = g.get('current_bowler_id')
     innings = g.get('innings', 1); balls = g.get('balls_bowled_this_inning', 0); 
     max_overs_display = g.get('overs_per_innings', DEFAULT_OVERS) 
     overs_str = format_overs(balls); max_overs_str_disp = str(max_overs_display) 

     state = g.get('state')
     # Handle states where batter/bowler might not be set yet for selection
     if state == STATE_TEAM_HOST_SELECT_BATTER: 
         return f"Host is selecting batter for Team {bat_team}...", "N/A", "N/A"
     if state == STATE_TEAM_HOST_SELECT_BOWLER:
         sel_batter_id = g.get('current_batter_id'); sel_bat_team = g.get('current_batting_team')
         sel_batter_name = "N/A"; sel_batter_mention = "N/A"
         if sel_batter_id and sel_bat_team and sel_batter_id in g['teams'][sel_bat_team]['names']:
              sel_batter_name = g['teams'][sel_bat_team]['names'].get(sel_batter_id, f"Batter {sel_batter_id}")
              sel_batter_mention = get_player_mention(sel_batter_id, sel_batter_name)
         return f"Host is selecting bowler for Team {bowl_team}...\nBatter on strike: {sel_batter_mention}", sel_batter_mention, "N/A"
     
     if not bat_team or not bowl_team or batter_id is None or bowler_id is None:
         logger.warning(f"G:{g.get('game_id')} format_team_game_status error: Missing bat_team/bowl_team/batter_id/bowler_id. BT:{bat_team}, BWT:{bowl_team}, BID:{batter_id}, BWID:{bowler_id}, State:{state}")
         return "Status Error (Players not set)", "N/A", "N/A"

     batter_name = g['teams'][bat_team]['names'].get(batter_id, f"Btr {batter_id}")
     bowler_name = g['teams'][bowl_team]['names'].get(bowler_id, f"Bwl {bowler_id}")
     batter_m = get_player_mention(batter_id, batter_name)
     bowler_m = get_player_mention(bowler_id, bowler_name)
     score = g['teams'][bat_team].get('score', 0); wickets = g['teams'][bat_team].get('wickets', 0)
     max_wickets_disp = g.get(f'max_wickets_team_{bat_team}', g.get('actual_players_team_' + bat_team, 1))

     status = f"<b>--- Innings {innings} | Ov: {overs_str}/{max_overs_str_disp} ---</b>"
     target = g.get('target')
     if target: status += f" | Target: <b>{target}</b>"
     status += "\n\n"
     status += f"🏏 <b>Batting: Team {bat_team}</b> [<code>{score}/{wickets}</code> Wkts (Max: {max_wickets_disp})]\n"
     status += f"   On Strike: {batter_m}"
     if batter_played: status += " (Played shot)"
     status += "\n"
     status += f"🧤 <b>Bowling: Team {bowl_team}</b>\n"
     status += f"   Bowler: {bowler_m}\n"
     return status, batter_m, bowler_m

def format_1v1_game_status(game_data, batter_played=False):
    g = game_data
    batter_id = g.get('current_batter_id'); bowler_id = g.get('current_bowler_id')
    p1 = g.get('player1'); p2 = g.get('player2')
    if not batter_id or not bowler_id or not p1 or not p2: return "Status Error (Players not set)", "N/A", "N/A"

    innings = g.get('innings', 1); balls = g.get('balls_bowled_this_inning', 0)
    max_overs_display = g.get('overs_per_innings', DEFAULT_OVERS_1V1)
    overs_str = format_overs(balls); max_overs_str_disp = str(max_overs_display)

    batter = p1 if batter_id == p1['id'] else p2
    bowler = p1 if bowler_id == p1['id'] else p2
    batter_m = get_player_mention(batter_id, batter.get('name', 'Batter?'))
    bowler_m = get_player_mention(bowler_id, bowler.get('name', 'Bowler?'))
    score = batter.get('score', 0)

    status = f"<b>--- Innings {innings} | Ov: {overs_str}/{max_overs_str_disp} ---</b>"
    target = g.get('target')
    if target: status += f" | Target: <b>{target}</b>"
    status += "\n\n"
    status += f"🏏 Batter: {batter_m} [<code>{score}/0</code> Wkts]" # 1v1 ends on first wicket
    if batter_played: status += " (Played shot)"
    status += f"\n🧤 Bowler: {bowler_m}\n"
    return status, batter_m, bowler_m

# --- Scorecard & Winner Determination (MODIFIED for date fix) ---
def format_final_scorecard(game_data):
    g = game_data
    game_type = g.get('game_type')
    
    actual_creation_time = g.get('creation_timestamp', time.time()) 
    match_id_val = int(actual_creation_time) 
    match_date = datetime.fromtimestamp(actual_creation_time).strftime("%d %b %Y") 
    
    scorecard_text = f"🏏 MATCH COMPLETE #M{match_id_val}\n"
    scorecard_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    scorecard_text += f"📊 CLASSIC MODE | {match_date}\n\n"

    winner_text_result = "Match result pending determination."

    if game_type == 'team':
        scorecard_text += "👥 TEAM LINEUPS\n"
        team_a_players = g['teams']['A']['players']
        team_b_players = g['teams']['B']['players']

        # Use actual_players_team_A/B for initial team display if available and names populated
        team_a_captain_name = "Team A"
        if g['teams']['A']['names']: # Check if names are populated
            team_a_captain_name = g['teams']['A']['names'].get(g['teams']['A']['players'][0] if g['teams']['A']['players'] else None, "Team A Captain")
        
        team_b_captain_name = "Team B"
        if g['teams']['B']['names']:
            team_b_captain_name = g['teams']['B']['names'].get(g['teams']['B']['players'][0] if g['teams']['B']['players'] else None, "Team B Captain")

        first_bat_id = g.get('first_batting_team_id')
        if not first_bat_id: # Fallback if game ended very early
            first_bat_id = 'A' # Default assumption
        
        second_bat_id = 'B' if first_bat_id == 'A' else 'A'
        
        first_bat_display_name = team_a_captain_name if first_bat_id == 'A' else team_b_captain_name
        second_bat_display_name = team_a_captain_name if second_bat_id == 'A' else team_b_captain_name

        scorecard_text += f"🔵 {html.escape(first_bat_display_name)} (Batting First)\n"
        scorecard_text += f"🔴 {html.escape(second_bat_display_name)} (Bowling First)\n\n"

        scorecard_text += "📝 SCORECARD\n"
        for i_num in [1, 2]:
            batting_team_id_current_loop = first_bat_id if i_num == 1 else second_bat_id
            # Ensure team_data exists, even if empty (e.g., if game ended before second innings started)
            team_data = g['teams'].get(batting_team_id_current_loop, {'score': 0, 'wickets': 0, 'players': [], 'names': {}})
            inning_s = g['inning_stats'].get(i_num, {'fours': 0, 'sixes': 0}) # Default if somehow missing
            
            balls_bowled_inn = g.get(f'balls_bowled_inning{i_num}', 0)
            overs_played_str = format_overs(balls_bowled_inn)
            current_team_score = team_data.get('score',0)
            current_team_wickets = team_data.get('wickets',0)
            run_rate = (current_team_score / (balls_bowled_inn / 6)) if balls_bowled_inn > 0 else 0.00
            
            current_team_actual_players = team_data.get('players', [])
            current_team_names_dict = team_data.get('names',{})
            captain_or_team_name_display = current_team_names_dict.get(current_team_actual_players[0], batting_team_id_current_loop) if current_team_actual_players and current_team_names_dict else batting_team_id_current_loop

            scorecard_text += f"┌─ INNINGS {i_num} ({html.escape(captain_or_team_name_display)})\n"
            scorecard_text += f"│ {current_team_score}/{current_team_wickets} ({overs_played_str})\n"
            scorecard_text += f"│ 📈 RR: {run_rate:.2f}\n"
            fours_inn = inning_s.get('fours',0); sixes_inn = inning_s.get('sixes',0)
            scorecard_text += f"│ 🎯 4s: {fours_inn} | 💥 6s: {sixes_inn}\n"
            scorecard_text += f"└─ Total Runs: {current_team_score}\n\n"
        
        winner_text_result = determine_team_winner(g, for_scorecard=True)


    elif game_type == '1v1':
        p1 = g.get('player1'); p2 = g.get('player2')
        if not p1 or not p2: # Should not happen if game ended normally
            scorecard_text += "Error: Player data incomplete for 1v1 scorecard.\n"
            winner_text_result = "Result undetermined due to data error."
        else:
            scorecard_text += "👥 PLAYER LINEUPS\n"
            first_batter_obj = p1 if g.get('first_batter_id') == p1['id'] else p2
            second_batter_obj = p2 if g.get('first_batter_id') == p1['id'] else p1

            scorecard_text += f"🔵 {html.escape(first_batter_obj['name'])} (Batting First)\n"
            scorecard_text += f"🔴 {html.escape(second_batter_obj['name'])} (Bowling First)\n\n"

            scorecard_text += "📝 SCORECARD\n"
            for i_num in [1, 2]:
                current_batter_obj_loop = first_batter_obj if i_num == 1 else second_batter_obj
                
                balls_bowled_inn = g.get(f'balls_bowled_inning{i_num}', 0)
                overs_played_str = format_overs(balls_bowled_inn)
                current_score_loop = current_batter_obj_loop.get('score', 0)
                run_rate = (current_score_loop / (balls_bowled_inn / 6)) if balls_bowled_inn > 0 else 0.00
                
                scorecard_text += f"┌─ INNINGS {i_num} ({html.escape(current_batter_obj_loop['name'])})\n"
                
                # Wicket display logic for 1v1
                wickets_display_1v1 = "0" 
                # If innings ended before max_balls AND target not met (for inn2), it's an OUT
                if i_num == 1: # 1st Innings
                    # Wicket if balls_bowled < max_balls (implies out, as 1v1 ends on 1 wicket)
                    if balls_bowled_inn < g['max_balls']: wickets_display_1v1 = "1"
                elif i_num == 2: # 2nd Innings
                    # Wicket if balls_bowled < max_balls AND target not chased
                    if balls_bowled_inn < g['max_balls'] and current_score_loop < g.get('target', float('inf')):
                         wickets_display_1v1 = "1"
                
                scorecard_text += f"│ {current_score_loop}/{wickets_display_1v1} ({overs_played_str})\n"
                scorecard_text += f"│ 📈 RR: {run_rate:.2f}\n"
                fours_inn = current_batter_obj_loop.get('fours',0); sixes_inn = current_batter_obj_loop.get('sixes',0)
                scorecard_text += f"│ 🎯 4s: {fours_inn} | 💥 6s: {sixes_inn}\n"
                scorecard_text += f"└─ Total Runs: {current_score_loop}\n\n"
            
            winner_text_result = determine_1v1_winner(g, for_scorecard=True)

    # Match Stats (Common)
    ms = g.get('match_stats', {}) # Default to empty dict
    total_balls_match = g.get('balls_bowled_inning1',0) + g.get('balls_bowled_inning2',0)
    
    total_score_match = 0
    if game_type == 'team':
        total_score_match = g['teams'].get('A',{}).get('score',0) + g['teams'].get('B',{}).get('score',0)
    elif game_type == '1v1' and g.get('player1') and g.get('player2'):
        total_score_match = g['player1'].get('score',0) + g['player2'].get('score',0)
        
    avg_rr_match = (total_score_match / (total_balls_match / 6)) if total_balls_match > 0 else 0.00

    scorecard_text += "📊 MATCH STATS\n"
    scorecard_text += f"• 📈 Average RR: {avg_rr_match:.2f}\n"
    scorecard_text += f"• ⭕ Dot Balls: {ms.get('total_dots',0)}\n"
    scorecard_text += f"• 🎯 Total Boundaries (4s): {ms.get('total_fours',0)}\n"
    scorecard_text += f"• 💥 Total Sixes: {ms.get('total_sixes',0)}\n"
    best_over_val = ms.get('best_over_runs', -1)
    best_over_display = f"{best_over_val} runs" if best_over_val != -1 else "N/A"
    scorecard_text += f"• ⚡ Best Over: {best_over_display}\n\n"

    scorecard_text += f"🏆 RESULT\n{winner_text_result}\n"
    return scorecard_text


def determine_team_winner(game_data, for_scorecard=False):
    g = game_data
    if g.get('state') != STATE_TEAM_ENDED and not for_scorecard: return "Game outcome pending."
    try:
        team_a_score = g['teams']['A'].get('score', 0)
        team_b_score = g['teams']['B'].get('score', 0)
        first_bat_team_id = g.get('first_batting_team_id')
        target = g.get('target') # Target is score of team batting first + 1

        if not first_bat_team_id or target is None: # Game ended prematurely or data missing
            # Simple comparison if target logic failed
            if team_a_score > team_b_score: return f"🎉 Team A wins by {team_a_score - team_b_score} runs! 🏆"
            elif team_b_score > team_a_score: return f"🎉 Team B wins by {team_b_score - team_a_score} runs! 🏆"
            elif team_a_score == team_b_score and (g.get('balls_bowled_inning1',0) > 0 or g.get('balls_bowled_inning2',0) > 0) : return "🤝 It's a TIE!" # Tie if scores equal and game played
            else: return "Match outcome unclear (possibly ended before completion)."


        inn1_bat_score = g['teams'][first_bat_team_id]['score']
        inn2_bat_team_id = 'B' if first_bat_team_id == 'A' else 'A'
        inn2_bat_score = g['teams'][inn2_bat_team_id]['score']
        
        team_a_captain_name = g['teams']['A']['names'].get(g['teams']['A']['players'][0] if g['teams']['A']['players'] else None, "Team A")
        team_b_captain_name = g['teams']['B']['names'].get(g['teams']['B']['players'][0] if g['teams']['B']['players'] else None, "Team B")

        inn1_bat_team_display_name = team_a_captain_name if first_bat_team_id == 'A' else team_b_captain_name
        inn2_bat_team_display_name = team_a_captain_name if inn2_bat_team_id == 'A' else team_b_captain_name


        if inn2_bat_score >= target: # Team batting second chased target
            wickets_left = g['actual_players_team_' + inn2_bat_team_id] - g['teams'][inn2_bat_team_id]['wickets']
            return f"🎉 Team {html.escape(inn2_bat_team_display_name)} wins by {wickets_left} wicket{'s' if wickets_left != 1 else ''}! 🏆"
        elif inn2_bat_score == target - 1: # Scores level
            return f"🤝 It's a TIE!"
        else: # Team batting first defended target
            runs_margin = (target - 1) - inn2_bat_score
            return f"🎉 Team {html.escape(inn1_bat_team_display_name)} wins by {runs_margin} run{'s' if runs_margin != 1 else ''}! 🏆"
    except Exception as e:
         logger.error(f"Error in determine_team_winner G:{g.get('game_id','?')}: {e}", exc_info=True)
         return "Error determining winner."

def determine_1v1_winner(game_data, for_scorecard=False):
    g = game_data
    if g.get('state') != STATE_1V1_ENDED and not for_scorecard: return "Game outcome pending."
    try:
        p1 = g.get('player1'); p2 = g.get('player2'); target = g.get('target')
        if not p1 or not p2 or target is None: return "Game ended unexpectedly or data missing."

        first_batter_id = g.get('first_batter_id')
        inn1_batter = p1 if first_batter_id == p1['id'] else p2
        inn2_batter = p2 if first_batter_id == p1['id'] else p1
        
        inn1_score = inn1_batter.get('score',0)
        inn2_score = inn2_batter.get('score', 0)

        if inn2_score >= target: # Player 2 (chasing) won
            winner_name = inn2_batter.get('name')
            # 1v1, effectively won by 1 wicket if target chased
            return f"🎉 {html.escape(winner_name)} wins by chasing the target! 🏆"
        elif inn2_score == target - 1: # Scores level
            return f"🤝 It's a TIE!"
        else: # Player 1 (defending) won
            winner_name = inn1_batter.get('name')
            runs_margin = (target - 1) - inn2_score
            return f"🎉 {html.escape(winner_name)} wins by {runs_margin} run{'s' if runs_margin != 1 else ''}! 🏆"
    except Exception as e:
        logger.error(f"Error in determine_1v1_winner G:{g.get('game_id','?')}: {e}", exc_info=True)
        return "Error determining winner."


# --- Admin Commands (Modified for set_credits, setpfp) ---
@client.on(events.NewMessage(pattern='/user_count', from_users=xmods))
async def handle_user_count(event): # Unchanged
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    try: count = await asyncio.to_thread(users_collection.count_documents, {}); await safe_reply(event, f"👥 Users: <b>{count}</b>") # Removed +200
    except Exception as e: await safe_reply(event, f"Error: {e}"); logger.error(f"Err count users: {e}", exc_info=True)

@client.on(events.NewMessage(pattern=r'/set_runs(?: (\d+))? (\d+)', from_users=xmods))
async def handle_set_runs(event): # Unchanged
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    target_user_id=None; runs_to_set=0
    try:
        parts=event.pattern_match.groups()
        if parts[0]: target_user_id=int(parts[0])
        elif event.is_reply: reply_msg=await event.get_reply_message(); target_user_id=reply_msg.sender_id if reply_msg else None
        else: return await safe_reply(event,"Usage: /set_runs [user_id] <amount> or reply.")
        runs_to_set=int(parts[1]); assert runs_to_set>=0
    except: return await safe_reply(event,"Invalid format/value.")
    if not target_user_id: return await safe_reply(event,"No target user.")
    target_id_str=str(target_user_id)
    def _set(uid, runs):
        if users_collection is None: return False
        try: return users_collection.update_one({"_id":uid},{"$set":{"runs":runs}}).matched_count>0
        except Exception as e: logger.error(f"DB set runs err {uid}: {e}"); return False
    success = await asyncio.to_thread(_set, target_id_str, runs_to_set)
    if success:
        try: user_ent=await client.get_entity(target_user_id); mention=get_player_mention(target_user_id,get_display_name(user_ent))
        except: mention=f"User <code>{target_user_id}</code>"
        await safe_reply(event, f"✅ Runs for {mention} set to <b>{runs_to_set}</b>.")
    else: await safe_reply(event, f"⚠️ Failed for user <code>{target_user_id}</code> (not registered?).")

@client.on(events.NewMessage(pattern=r'/set_wickets(?: (\d+))? (\d+)', from_users=xmods))
async def handle_set_wickets(event): # Unchanged
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    target_user_id = None; wickets_to_set = 0
    try:
        parts = event.pattern_match.groups()
        if parts[0]: target_user_id = int(parts[0])
        elif event.is_reply: reply_msg = await event.get_reply_message(); target_user_id = reply_msg.sender_id if reply_msg else None
        else: return await safe_reply(event, "Usage: <code>/set_wickets [user_id] <amount></code> or reply.")
        wickets_to_set = int(parts[1]);
        if wickets_to_set < 0: return await safe_reply(event, "Wicket amount cannot be negative.")
    except (ValueError, TypeError, IndexError): return await safe_reply(event, "Invalid format or value.")
    if not target_user_id: return await safe_reply(event, "Could not determine target user.")
    target_id_str = str(target_user_id)
    def _set_db_wickets(uid, wickets):
        if users_collection is None: return False, "DB Unavailable"
        try: res = users_collection.update_one({"_id": uid},{"$set": {"wickets": wickets}}); return res.matched_count > 0, None
        except Exception as e: logger.error(f"DB set wickets err {uid}: {e}"); return False, str(e)
    success, err = await asyncio.to_thread(_set_db_wickets, target_id_str, wickets_to_set)
    if success:
        try: user_ent = await client.get_entity(target_user_id); mention = get_player_mention(target_user_id, get_display_name(user_ent))
        except Exception: mention = f"User <code>{target_user_id}</code>"
        await safe_reply(event, f"✅ Wickets for {mention} set to <b>{wickets_to_set}</b>.")
    else:
        err_msg = f"⚠️ Failed to set wickets for <code>{target_user_id}</code>."; err_msg += f" (DB Error: {html.escape(err)})" if err else " (User not found/registered?)."
        await safe_reply(event, err_msg)

# --- NEW: Set Credits Admin Command ---
@client.on(events.NewMessage(pattern=r'/set_credits(?: (\d+))? (-?\d+)', from_users=xmods))
async def handle_set_credits(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    target_user_id = None; credits_to_set = 0
    try:
        parts = event.pattern_match.groups()
        if parts[0]: target_user_id = int(parts[0])
        elif event.is_reply: reply_msg = await event.get_reply_message(); target_user_id = reply_msg.sender_id if reply_msg else None
        else: return await safe_reply(event, "Usage: <code>/set_credits [user_id] <amount></code> or reply.")
        credits_to_set = int(parts[1]) # Amount can be negative to deduct
    except (ValueError, TypeError, IndexError): return await safe_reply(event, "Invalid format or value.")
    if not target_user_id: return await safe_reply(event, "Could not determine target user.")
    
    target_id_str = str(target_user_id)
    def _set_db_credits(uid, credits_val):
        if users_collection is None: return False, "DB Unavailable"
        try:
            # Ensure user exists before setting credits
            user_doc = users_collection.find_one({"_id": uid})
            if not user_doc: # User not found, maybe register them with 0 initial values then set credits?
                           # For now, strict: user must exist.
                return False, "User not found in DB"
            
            res = users_collection.update_one({"_id": uid}, {"$set": {"credits": credits_val}})
            return res.matched_count > 0, None
        except Exception as e: logger.error(f"DB set credits err {uid}: {e}"); return False, str(e)

    success, err = await asyncio.to_thread(_set_db_credits, target_id_str, credits_to_set)
    if success:
        try: user_ent = await client.get_entity(target_user_id); mention = get_player_mention(target_user_id, get_display_name(user_ent))
        except Exception: mention = f"User <code>{target_user_id}</code>"
        await safe_reply(event, f"✅ Credits for {mention} set to <b>{credits_to_set}</b>.")
    else:
        err_msg = f"⚠️ Failed to set credits for <code>{target_user_id}</code>."; 
        err_msg += f" (Reason: {html.escape(err)})" if err else " (Operation failed or user not found)."
        await safe_reply(event, err_msg)


async def get_target_id_and_text(event, command_name): # Unchanged
    target_user_id = None; text_content = None
    # Split carefully to handle cases like "/command text" when replying, or "/command id text"
    command_parts = event.text.split(maxsplit=1) # Split off the command itself
    
    args_part = command_parts[1] if len(command_parts) > 1 else ""
    
    if event.is_reply:
        reply_msg = await event.get_reply_message()
        if not (reply_msg and reply_msg.sender_id):
            await safe_reply(event, "Invalid reply target for command.")
            return None, None
        target_user_id = reply_msg.sender_id
        text_content = args_part.strip() # Text is whatever remains after /command
    else: # Not a reply, User ID must be first argument
        arg_split = args_part.split(maxsplit=1)
        if not arg_split or not arg_split[0].isdigit():
            await safe_reply(event, f"Usage: <code>/{command_name} [user_id] &lt;text&gt;</code> or reply <code>/{command_name} &lt;text&gt;</code>")
            return None, None
        target_user_id = int(arg_split[0])
        if len(arg_split) > 1:
            text_content = arg_split[1].strip()
            
    if not text_content: # text_content might be "" if only ID was provided or no text after command in reply
        await safe_reply(event, f"Please provide the text/achievement to add/remove for user {target_user_id}.")
        return target_user_id, None # Return ID even if text is missing, so caller knows who it was for
        
    return target_user_id, text_content

@client.on(events.NewMessage(pattern=r'/achieve(?: (\d+))?( .*)?', from_users=xmods))
async def handle_achieve(event): # Unchanged
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    target_user_id, achievement_text = await get_target_id_and_text(event, "achieve")
    if not target_user_id or not achievement_text: return
    target_id_str = str(target_user_id)
    def _add_achieve(uid, achieve_txt):
        if users_collection is None: return False, "DB Unavailable"
        try:
             res = users_collection.update_one({"_id": uid},{"$addToSet": {"achievements": achieve_txt}})
             user_exists = users_collection.count_documents({"_id": uid}) > 0
             if not user_exists: return False, "User not found"
             return True, res.modified_count > 0 # modified_count > 0 if new item added
        except Exception as e: logger.error(f"DB add achievement err {uid}: {e}"); return False, str(e)
    success, modified_status = await asyncio.to_thread(_add_achieve, target_id_str, achievement_text) # Renamed 'modified' to 'modified_status'
    
    user_mention_text = f"User <code>{target_user_id}</code>"
    try: 
        user_entity_ach = await client.get_entity(target_user_id)
        user_mention_text = get_player_mention(target_user_id, get_display_name(user_entity_ach) if user_entity_ach else f"User {target_user_id}")
    except: pass # Keep default mention if entity fetch fails

    safe_achieve_text_disp = html.escape(achievement_text)

    if success:
        if modified_status: # True if a new achievement was actually added
            await safe_reply(event, f"✅ Added achievement '<code>{safe_achieve_text_disp}</code>' to {user_mention_text}.")
        else: # False if achievement already existed or DB op didn't modify (e.g. user found but achieve exists)
            await safe_reply(event, f"☑️ Achievement '<code>{safe_achieve_text_disp}</code>' likely already exists for {user_mention_text} (or no change made).")
    else: # success is False
        # 'modified_status' here would be the error message string from _add_achieve
        err_reason = modified_status if isinstance(modified_status, str) else "Unknown DB Error or User Not Found"
        await safe_reply(event, f"⚠️ Failed to add achievement for {user_mention_text}. Reason: {err_reason}")


@client.on(events.NewMessage(pattern=r'/remove_achieve(?: (\d+))?( .*)?', from_users=xmods))
async def handle_remove_achieve(event): # Unchanged logic, just var names for clarity
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    target_user_id, achievement_text = await get_target_id_and_text(event, "remove_achieve")
    if not target_user_id or not achievement_text: return
    target_id_str = str(target_user_id)
    
    def _remove_achieve_db(uid, achieve_txt_rem):
        if users_collection is None: return False, "DB Unavailable"
        try:
             res_rem = users_collection.update_one({"_id": uid},{"$pull": {"achievements": achieve_txt_rem}})
             user_exists_rem = users_collection.count_documents({"_id": uid}) > 0
             if not user_exists_rem: return False, "User not found"
             return True, res_rem.modified_count > 0 # True if item was found and removed
        except Exception as e: logger.error(f"DB remove achievement err {uid}: {e}"); return False, str(e)
        
    success_rem, removed_status = await asyncio.to_thread(_remove_achieve_db, target_id_str, achievement_text)

    user_mention_text_rem = f"User <code>{target_user_id}</code>"
    try: 
        user_entity_rem = await client.get_entity(target_user_id)
        user_mention_text_rem = get_player_mention(target_user_id, get_display_name(user_entity_rem) if user_entity_rem else f"User {target_user_id}")
    except: pass

    safe_achieve_text_rem_disp = html.escape(achievement_text)

    if success_rem:
        if removed_status: # True if achievement was found and removed
            await safe_reply(event, f"✅ Removed achievement '<code>{safe_achieve_text_rem_disp}</code>' from {user_mention_text_rem}.")
        else: # False if achievement was not found on user or DB op didn't modify
            await safe_reply(event, f"☑️ Achievement '<code>{safe_achieve_text_rem_disp}</code>' not found for {user_mention_text_rem} (or no change made).")
    else: # success_rem is False
        err_reason_rem = removed_status if isinstance(removed_status, str) else "Unknown DB Error or User Not Found"
        await safe_reply(event, f"⚠️ Failed to remove achievement for {user_mention_text_rem}. Reason: {err_reason_rem}")


@client.on(events.NewMessage(pattern='/broad(?: |$)(.*)', from_users=xmods))
async def handle_broadcast(event): 
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    message_text = event.pattern_match.group(1).strip()
    if not message_text: return await safe_reply(event, "Please provide a message to broadcast after <code>/broad</code>.")
    
    # Confirmation step
    confirm_msg_obj = await safe_reply(event, f"⚠️ You are about to broadcast the following message to ALL registered users:\n\n<pre>{html.escape(message_text)}</pre>\n\nReply '<b>yes</b>' (case-insensitive) to this message within 30 seconds to confirm, or '<b>no</b>' / anything else to cancel.")
    if not confirm_msg_obj: return await safe_reply(event, "Error sending confirmation message for broadcast.")

    try:
        async with client.conversation(event.chat_id, timeout=30, exclusive=True, reply_to=confirm_msg_obj.id) as conv:
            response_msg = await conv.get_reply()
            if response_msg.text.lower() != 'yes':
                await conv.send_message("Broadcast cancelled by user.")
                await safe_edit_message(event.chat_id, confirm_msg_obj.id, "Broadcast confirmation was 'no' or invalid. Cancelled.")
                return
            # User confirmed with 'yes'
            await conv.send_message("Confirmation 'yes' received. Preparing to broadcast...")
    except asyncio.TimeoutError:
        await safe_edit_message(event.chat_id, confirm_msg_obj.id, "Broadcast confirmation timed out. Cancelled.")
        return
    except Exception as e_conv:
        logger.error(f"Broadcast conversation error: {e_conv}")
        await safe_edit_message(event.chat_id, confirm_msg_obj.id, f"Error during confirmation: {e_conv}. Broadcast cancelled.")
        return

    await safe_edit_message(event.chat_id, confirm_msg_obj.id, "Confirmation received. Starting broadcast process...")
    
    sent_count = 0; failed_count = 0; blocked_count = 0; total_users_query = 0
    start_time_bc = time.monotonic()
    
    status_update_msg = await safe_reply(event, f"Broadcasting initializing... Querying users.")
    last_update_time = start_time_bc

    try:
        all_user_ids_cursor = users_collection.find({}, {"_id": 1})
        # Convert cursor to list to get total_users_query count upfront, but be mindful of memory for very large user bases
        # For extremely large DBs, iterating cursor directly is better but makes upfront total count harder.
        user_id_docs = list(all_user_ids_cursor) 
        total_users_query = len(user_id_docs)
        
        if status_update_msg: 
            await safe_edit_message(event.chat_id, status_update_msg.id, f"Broadcasting to {total_users_query} users... Sent: 0, Failed: 0")

        for i, user_doc in enumerate(user_id_docs):
            user_id_str_bc = user_doc["_id"]
            try:
                user_id_int_bc = int(user_id_str_bc)
                await client.send_message(user_id_int_bc, message_text, parse_mode='html')
                sent_count += 1
            except FloodWaitError as e_flood:
                 wait_time_flood = e_flood.seconds + random.uniform(1,5) # Add small random buffer
                 logger.warning(f"Broadcast: Flood wait encountered. Sleeping for {wait_time_flood:.2f} seconds...")
                 if status_update_msg: await safe_edit_message(event.chat_id, status_update_msg.id, f"Flood Wait... Sleeping {wait_time_flood:.0f}s... ({sent_count}/{total_users_query})")
                 await asyncio.sleep(wait_time_flood)
                 # Retry sending to the current user after sleep
                 try: 
                     await client.send_message(user_id_int_bc, message_text, parse_mode='html')
                     sent_count +=1
                 except Exception as e_retry: # If retry also fails
                     logger.warning(f"Broadcast: Retry after flood wait failed for {user_id_int_bc}: {e_retry}")
                     failed_count +=1
                     if "USER_IS_BLOCKED" in str(e_retry).upper() or "USER_DEACTIVATED" in str(e_retry).upper() or "PEER_ID_INVALID" in str(e_retry).upper():
                        blocked_count += 1
            except (UserNotParticipantError, ValueError, TypeError) as e_known_fail:
                # These are expected failures for users who might have left, etc.
                logger.debug(f"Broadcast: Failed to send to {user_id_str_bc} (expected type): {e_known_fail}")
                failed_count += 1
            except Exception as e_send: # Catch other exceptions during send
                logger.warning(f"Broadcast: Error sending to {user_id_str_bc}: {e_send}")
                failed_count += 1
                # Check for common strings indicating user blocked/deactivated
                if "USER_IS_BLOCKED" in str(e_send).upper() or \
                   "USER_DEACTIVATED" in str(e_send).upper() or \
                   "PEER_ID_INVALID" in str(e_send).upper() or \
                   "BOT_BLOCKED_BY_USER" in str(e_send).upper() : # Added another common one
                    blocked_count += 1
            
            current_time_bc = time.monotonic()
            if status_update_msg and (current_time_bc - last_update_time > 5 or (i + 1) == total_users_query) : # Update every 5 secs or on last user
                status_text = f"Broadcasting... {i+1}/{total_users_query} processed.\nSent: {sent_count}, Failed: {failed_count} (Blocked/Invalid: {blocked_count})"
                await safe_edit_message(event.chat_id, status_update_msg.id, status_text)
                last_update_time = current_time_bc
            
            if i % 50 == 0 and i > 0: # Small pause every 50 messages to be nice to API
                await asyncio.sleep(random.uniform(0.5, 1.5))

    except Exception as db_err_bc: 
        await safe_reply(event, f"Error fetching users for broadcast: {db_err_bc}")
        if status_update_msg: await safe_edit_message(event.chat_id, status_update_msg.id, "Broadcast failed: DB error.")
        return
        
    duration_bc = round(time.monotonic() - start_time_bc, 2)
    final_status_text = (f"✅ Broadcast Complete!\n"
                         f"Processed: {total_users_query} users\n"
                         f"Successfully Sent: {sent_count}\n"
                         f"Failed to Send: {failed_count}\n"
                         f"(Including Blocked/Invalid: {blocked_count})\n"
                         f"Duration: {duration_bc} seconds")
    
    if status_update_msg: await safe_edit_message(event.chat_id, status_update_msg.id, final_status_text)
    else: await safe_reply(event, final_status_text)


# --- NEW: /setpfp Command ---
@client.on(events.NewMessage(pattern='/setpfp'))
async def handle_setpfp(event):
    if not event.is_reply:
        return await safe_reply(event, "Please reply to an image with <code>/setpfp</code> to set it as your profile picture for the bot.")
    if users_collection is None:
        return await safe_reply(event, "⚠️ DB unavailable. Cannot save profile picture.")

    reply_message = await event.get_reply_message()
    if not reply_message or not (reply_message.photo or (reply_message.document and reply_message.document.mime_type.startswith("image/"))):
        return await safe_reply(event, "The replied message does not contain a photo or an image document.")

    user_id_str = str(event.sender_id)
    file_id_to_store = None

    if reply_message.photo and hasattr(reply_message.file, 'id'): # reply_message.file should exist if .photo exists
        file_id_to_store = reply_message.file.id
        logger.info(f"Attempting to use reply_message.file.id for PFP: {file_id_to_store} for user {user_id_str}")
    elif reply_message.document and reply_message.document.mime_type.startswith("image/") and hasattr(reply_message.file, 'id'):
        file_id_to_store = reply_message.file.id
        logger.info(f"Attempting to use reply_message.file.id from image document for PFP: {file_id_to_store} for user {user_id_str}")
    else:
        # Fallback or if .file.id is not directly available (older Telethon versions or unusual media)
        # This part might still lead to "Cannot use X as file" if only photo.id is stored.
        # The goal is to get a string file_id.
        if reply_message.photo:
            # Try to get a string file_id from photo sizes if message.file.id was not available
            # This is more complex and less reliable than message.file.id
            logger.warning(f"Could not get message.file.id for photo for user {user_id_str}. PFP might not work reliably.")
            # For now, we will not store if message.file.id is not found, to avoid storing problematic IDs.
            # Alternatively, one could try to download and re-upload to get a stable file_id.
            # file_id_to_store = reply_message.photo.id # Storing this long ID is often problematic for resending.
            return await safe_reply(event, "Could not obtain a stable file reference for this photo. Try a different image or ensure it's sent as a standard photo.")
        else: # Should not reach here if initial check passed
             return await safe_reply(event, "Failed to process the image for PFP.")


    if not isinstance(file_id_to_store, str):
        logger.error(f"PFP file_id_to_store is not a string: {file_id_to_store} (type: {type(file_id_to_store)}) for user {user_id_str}. Aborting PFP set.")
        return await safe_reply(event, "⚠️ Failed to get a valid file reference for the image. PFP not set.")

    try:
        users_collection.update_one(
            {"_id": user_id_str},
            {"$set": {"pfp_file_id": file_id_to_store}} 
        )
        await safe_reply(event, "✅ Your profile picture has been updated for the bot!")
    except Exception as e:
        logger.error(f"Failed to set PFP in DB for user {user_id_str}: {e}", exc_info=True)
        await safe_reply(event, "⚠️ Failed to update your profile picture due to a database error.")


# --- NEW: Betting Commands ---
@client.on(events.NewMessage(pattern=r'/flip (h|t|heads|tails) (\d+)'))
async def handle_flip(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB and Credits system unavailable.")
    user_id = event.sender_id; sender_entity = await event.get_sender()
    user_mention = get_player_mention(user_id, get_display_name(sender_entity) if sender_entity else f"User {user_id}")
    try:
        choice_str = event.pattern_match.group(1).lower()
        amount = int(event.pattern_match.group(2))
    except: return await safe_reply(event, f"Usage: <code>/flip &lt;h/t&gt; &lt;amount&gt;</code>")
    if amount <= 0: return await safe_reply(event, "Bet amount must be > 0.")

    user_data = await asyncio.to_thread(get_user_data, user_id)
    if not user_data: return await safe_reply(event, f"{user_mention}, please /start me in DM first to use credits.")
    current_credits = user_data.get("credits", 0)
    if current_credits < amount: return await safe_reply(event, f"{user_mention}, you only have {current_credits} credits. Not enough to bet {amount}.")

    deduction_result = await add_credits_to_user(user_id, -amount) # Deduct first
    if deduction_result == "insufficient": return await safe_reply(event, f"Insufficient credits confirmed by DB (should have been caught).") 
    if not deduction_result: return await safe_reply(event, f"Failed to process your bet (deduction step), {user_mention}. Please try again.")

    user_choice_is_heads = choice_str.startswith("h")
    actual_flip_is_heads = random.choice([True, False])
    actual_flip_display = "Heads" if actual_flip_is_heads else "Tails"
    
    result_message = f"🪙 Coin Flip for {user_mention}!\nBet Amount: {amount} credits\n"
    result_message += f"You Chose: <b>{'Heads' if user_choice_is_heads else 'Tails'}</b>\n"
    result_message += f"Coin Landed On: <b>{actual_flip_display}</b>!\n\n"
    
    new_balance_after_deduction = current_credits - amount # Balance after initial deduction
    
    if user_choice_is_heads == actual_flip_is_heads: # Win
        winnings_credited = amount * 2 # Give back original bet + profit
        payout_result = await add_credits_to_user(user_id, winnings_credited)
        if payout_result:
            new_balance_after_win = new_balance_after_deduction + winnings_credited
            result_message += f"🎉 Congratulations! You won {amount} credits (total {winnings_credited} credited back)!\n"
            result_message += f"Your new balance: {new_balance_after_win} credits."
        else:
            result_message += f"💔 You won, but there was an error crediting your winnings. Please contact an admin. Your balance (after deduction): {new_balance_after_deduction}."
            logger.error(f"Flip win: Failed to credit winnings {winnings_credited} to user {user_id}")
    else: # Loss
        result_message += f"💔 Unlucky! You lost {amount} credits.\n"
        result_message += f"Your new balance: {new_balance_after_deduction} credits."
        
    await safe_reply(event, result_message)

@client.on(events.NewMessage(pattern=r'/dice (o|e|odd|even) (\d+)'))
async def handle_dice(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB and Credits system unavailable.")
    user_id = event.sender_id; sender_entity = await event.get_sender()
    user_mention = get_player_mention(user_id, get_display_name(sender_entity) if sender_entity else f"User {user_id}")
    try:
        choice_str = event.pattern_match.group(1).lower()
        amount = int(event.pattern_match.group(2))
    except: return await safe_reply(event, f"Usage: <code>/dice &lt;o/e&gt; &lt;amount&gt;</code>")
    if amount <= 0: return await safe_reply(event, "Bet amount must be > 0.")

    user_data = await asyncio.to_thread(get_user_data, user_id)
    if not user_data: return await safe_reply(event, f"{user_mention}, please /start me in DM first to use credits.")
    current_credits = user_data.get("credits", 0)
    if current_credits < amount: return await safe_reply(event, f"{user_mention}, you only have {current_credits} credits. Not enough to bet {amount}.")

    deduction_result = await add_credits_to_user(user_id, -amount) # Deduct first
    if deduction_result == "insufficient": return await safe_reply(event, f"Insufficient credits confirmed by DB (should have been caught).")
    if not deduction_result: return await safe_reply(event, f"Failed to process your bet (deduction step), {user_mention}. Please try again.")

    user_choice_is_odd = choice_str.startswith("o")
    dice_roll_val = random.randint(1, 6)
    actual_roll_is_odd = (dice_roll_val % 2 != 0)
    
    result_message = f"🎲 Dice Roll for {user_mention}!\nBet Amount: {amount} credits\n"
    result_message += f"You Chose: <b>{'Odd' if user_choice_is_odd else 'Even'}</b>\n"
    result_message += f"The Die Rolled: <b>{dice_roll_val}</b> ({'Odd' if actual_roll_is_odd else 'Even'})!\n\n"
    
    new_balance_after_deduction = current_credits - amount

    if user_choice_is_odd == actual_roll_is_odd: # Win
        winnings_credited_dice = amount * 2 
        payout_result_dice = await add_credits_to_user(user_id, winnings_credited_dice)
        if payout_result_dice:
            new_balance_after_win_dice = new_balance_after_deduction + winnings_credited_dice
            result_message += f"🎉 Congratulations! You won {amount} credits (total {winnings_credited_dice} credited back)!\n"
            result_message += f"Your new balance: {new_balance_after_win_dice} credits."
        else:
            result_message += f"💔 You won, but there was an error crediting your winnings. Please contact an admin. Your balance (after deduction): {new_balance_after_deduction}."
            logger.error(f"Dice win: Failed to credit winnings {winnings_credited_dice} to user {user_id}")
    else: # Loss
        result_message += f"💔 Unlucky! You lost {amount} credits.\n"
        result_message += f"Your new balance: {new_balance_after_deduction} credits."
        
    await safe_reply(event, result_message)

# --- Main Execution (Unchanged) ---
async def main():
    global bot_info
    try:
        logger.info("Starting bot...")
        await client.start(bot_token=BOT_TOKEN)
        bot_info = await client.get_me()
        logger.info(f"Bot logged in as @{bot_info.username} (ID: {bot_info.id})")
        if mongo_client is not None and db is not None:
             try: await asyncio.to_thread(db.command, 'ping'); logger.info("MongoDB connection confirmed.")
             except Exception as e: logger.error(f"MongoDB check failed after start: {e}")
        elif mongo_client is None: logger.warning("Bot running without DB connection.")
        logger.info("Bot is ready...")
        await client.run_until_disconnected()
    except Exception as e: logger.critical(f"Critical error during execution: {e}", exc_info=True)
    finally:
        logger.info("Bot is stopping...")
        if client.is_connected():
            await client.disconnect(); logger.info("Telethon client disconnected.")
        if mongo_client is not None:
            try: mongo_client.close(); logger.info("MongoDB connection closed.")
            except Exception as e: logger.error(f"Error closing MongoDB: {e}")
        logger.info("Bot stopped.")

if __name__ == '__main__':
    try: import uvloop; uvloop.install(); logger.info("Using uvloop.")
    except ImportError: logger.info("uvloop not found, using default asyncio loop.")
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Shutdown requested.")

# --- END OF FILE ---
