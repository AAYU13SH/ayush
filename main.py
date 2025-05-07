# --- START OF FILE cricket_telethon_team.py ---

import asyncio
import random
import logging
from uuid import uuid4
import os
import html
import urllib.parse
import math # For calculating overs

from telethon import TelegramClient, events, Button
from telethon.errors import UserNotParticipantError, MessageNotModifiedError, MessageIdInvalidError, QueryIdInvalidError, BotMethodInvalidError
from telethon.tl.types import InputPeerUser, PeerUser, ReplyInlineMarkup, MessageReplyHeader # etc.
from telethon.tl.functions.messages import EditMessageRequest # Import specific requests if needed
from telethon.utils import get_peer_id, get_display_name

from pymongo import MongoClient, ReturnDocument # Keep sync Pymongo for now, Motor is alternative
from datetime import datetime, timezone
import time # for monotonic
import traceback # For logging errors to Telegram group
from telethon.errors import ChatAdminRequiredError, UserIsBlockedError, PeerIdInvalidError # Ensure these specific ones are included for the log sender

# --- Bot Configuration ---
# Replace with your actual credentials or use environment variables
API_ID = os.environ.get("API_ID", 25695711) # Replace with your API ID (integer)
API_HASH = os.environ.get("API_HASH", "f20065cc26d4a31bf0efc0b44edaffa9") # Replace with your API Hash (string)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "7906407273:AAHe77DY7TI9gmzsH-UM6k1vB9xDLRa_534") # Your bot token
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://yesvashisht:yash2005@clusterdf.yagj9ok.mongodb.net/?retryWrites=true&w=majority&appName=Clusterdf") # Replace with your MongoDB URI
MONGO_DB_NAME = "tct_cricket_bot_db_telethon" # Use a distinct DB?
# --- Bot Configuration ---
# ... (your existing API_ID, API_HASH, etc.)
LOG_GROUP_ID_STR = os.environ.get("-1002676791646") # Your Telegram Group/Channel ID for logs (e.g., -1001234567890)
LOG_GROUP_ID = -1002676791646
if LOG_GROUP_ID_STR and LOG_GROUP_ID_STR.strip():
    try:
        LOG_GROUP_ID = int(LOG_GROUP_ID_STR)
        # Test message if client were available, but it's not yet here.
        # print(f"INFO: Logging to Telegram Group/Channel ID: {LOG_GROUP_ID} enabled.")
    except ValueError:
        print(f"ERROR: Invalid LOG_GROUP_ID format: '{LOG_GROUP_ID_STR}'. Must be an integer. Logging to group disabled.")
        LOG_GROUP_ID = None
# else:
#    print("INFO: LOG_GROUP_ID not set. Logging to Telegram group disabled.") # This can be noisy, so optional.

# --- Game Configuration ---
DEFAULT_PLAYERS_PER_TEAM = 2
MAX_PLAYERS_PER_TEAM = 11 # Absolute maximum
DEFAULT_OVERS = 100 # Default overs per innings for team games
DEFAULT_OVERS_1V1 = 100 # Default overs per innings for 1v1 games

# --- Admin Configuration ---
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "6293455550,6265981509,7740827258,6620360093")
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
    print("Recommendation: Ensure indexes exist on 'runs', 'wickets', 'matches_played'.")
except Exception as e:
    print(f"ERROR: Could not connect to MongoDB: {e}")
    print("Warning: Bot running without database. Stats features will be disabled.")
    users_collection = None
    db = None
    mongo_client = None

# --- Game States ---
# 1v1 States
STATE_WAITING = "WAITING"
STATE_TOSS = "TOSS"
STATE_BAT_BOWL = "BAT_BOWL"
STATE_P1_BAT = "P1_BAT"
STATE_P1_BOWL_WAIT = "P1_BOWL_WAIT"
STATE_P2_BAT = "P2_BAT"
STATE_P2_BOWL_WAIT = "P2_BOWL_WAIT"
STATE_1V1_INNINGS_BREAK = "1V1_INNINGS_BREAK" # Might not be needed if logic transitions directly
STATE_1V1_ENDED = "1V1_ENDED"

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
    - Innings 1 ends when the batter is OUT or max overs are completed (currently set high).
    - Innings 2: The other player bats, chasing the target set in Innings 1.
    - The game ends when the second batter is OUT, target is chased, or max overs are bowled.

2️⃣  <b>Team Cricket (NvN):</b>
    - Start with: <code>/team_cricket [N]</code> in a group (e.g., <code>/team_cricket 3</code> for 3v3. Default is 2v2). Max {MAX_PLAYERS_PER_TEAM} per team.
    - The player who starts is the <b>Host</b> and <u>must join Team A first</u>.
    - Other players can then join Team A or Team B until teams are full or the Host starts.
    - <b>Minimum:</b> 1 player per team required to start.
    - The Host (captain of Team A) calls the toss.
    - The winning captain (from either team) chooses to Bat or Bowl.
    - The Host then selects the first batter for the batting team and first bowler for the bowling team.
    - <b>Gameplay:</b>
        - Similar to 1v1: Batter picks a number, then Bowler picks a number.
        - OUT or RUNS are determined.
        - When a batter gets out, the Host is prompted to select the next batter from non-out players.
        - When an over is complete (6 balls), the next bowler from the bowling team is automatically selected in rotation.
    - Innings 1 ends when all wickets are down (equal to team size at start) or max overs are completed.
    - Innings 2: The other team bats, chasing the target.
    - <b>Rebat:</b> If a player is marked OUT, the HOST can reply <code>/rebat</code> to that player's message (who got out) to mark them for 'rebat' on the scorecard. This is a visual note for score disputes and doesn't bring the player back.

<b><u>General Tips:</u></b>
- Use <code>/start</code> in DM with the bot to register.
- Use <code>/help</code> for a list of commands.
- Use <code>/cancel</code> to cancel a game you are part of in a group (host can cancel pre-game, any participant during game).
- Pay attention to whose turn it is! The bot will prompt you.
- For team games, the HOST has special responsibilities for player selection.
- Leaderboards (<code>/lead_runs</code>, etc.) and <code>/profile</code> are available to track stats.

Enjoy the game! 🎉
""".format(MAX_PLAYERS_PER_TEAM=MAX_PLAYERS_PER_TEAM) # Allows dynamic MAX_PLAYERS_PER_TEAM

# Team States
STATE_TEAM_HOST_JOIN_WAIT = "TEAM_HOST_JOIN_WAIT" # Host needs to join Team A
STATE_TEAM_WAITING = "TEAM_WAITING" # Waiting for other players (at least 1 per team required to start)
STATE_TEAM_READY_TO_START = "TEAM_READY_TO_START" # Deprecated? Use check in WAITING state instead. Kept for potential future use.
STATE_TEAM_TOSS_CALL = "TEAM_TOSS_CALL"
STATE_TEAM_BAT_BOWL_CHOICE = "TEAM_BAT_BOWL_CHOICE"
STATE_TEAM_HOST_SELECT_BATTER = "TEAM_HOST_SELECT_BATTER" # Host selects first batter
STATE_TEAM_HOST_SELECT_BOWLER = "TEAM_HOST_SELECT_BOWLER" # Host selects first bowler
STATE_TEAM_BATTING = "TEAM_BATTING"
STATE_TEAM_BOWLING_WAIT = "TEAM_BOWLING_WAIT"
STATE_TEAM_INNINGS_BREAK = "TEAM_INNINGS_BREAK" # Might not be needed
STATE_TEAM_ENDED = "TEAM_ENDED"

# --- In-memory storage for active games ---
games = {} # game_id -> game_data dictionary

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def log_event_to_telegram(event_type: str, message: str, user_id: int = None, chat_id: int = None, game_id: str = None, e: Exception = None, extra_info: str = None):
    """Sends a formatted log message to the configured Telegram log group."""
    if not LOG_GROUP_ID or not client or not client.is_connected():
        return

    log_prefix = f"<b>[{event_type.upper()}]</b> "
    bot_username_str = f"🤖 <code>@{bot_info.username}</code> " if bot_info and bot_info.username else "🤖 "
    log_prefix = bot_username_str + log_prefix

    log_msg_parts = [f"{log_prefix}{html.escape(message)}"]
    if game_id: log_msg_parts.append(f"  🎮 G_ID: <code>{game_id}</code>")
    if chat_id:
        chat_type_str = "Unknown"
        try:
            cid_str = str(chat_id)
            if cid_str.startswith("-100"): chat_type_str = "SG/Ch"
            elif cid_str.startswith("-"): chat_type_str = "Group"
            elif int(cid_str) > 0: chat_type_str = "User"
        except (ValueError, TypeError): pass
        log_msg_parts.append(f"  👥 C_ID: <code>{chat_id}</code> ({chat_type_str})")
    if user_id: log_msg_parts.append(f"  👤 U_ID: <code>{user_id}</code>")
    if extra_info: log_msg_parts.append(f"  ℹ️ Info: {html.escape(extra_info)}")


    if e: # If an exception object is passed
        exc_info = traceback.format_exc()
        # Keep traceback relatively short for Telegram message
        short_traceback = "\n".join(exc_info.splitlines()[-6:]) # Last 6 lines might be more useful
        log_msg_parts.append(f"  ‼️ Ex: <code>{html.escape(type(e).__name__)}: {html.escape(str(e))}</code>\n  <pre><code class=\"language-text\">{html.escape(short_traceback)}</code></pre>")

    final_log_msg = "\n".join(log_msg_parts)
    if len(final_log_msg) > 4000: # Telegram message limit is 4096, give some buffer
        final_log_msg = final_log_msg[:4000] + "\n... (truncated)"

    try:
        await client.send_message(LOG_GROUP_ID, final_log_msg, parse_mode='html', link_preview=False)
    except (ChatAdminRequiredError, UserIsBlockedError, PeerIdInvalidError, ValueError) as log_err:
        logger.error(f"TELEGRAM LOG FAIL (Common): Cannot send to LOG_GROUP_ID {LOG_GROUP_ID}. Error: {type(log_err).__name__} - {log_err}. Disabling TG logs for this session to prevent spam.")
        # global LOG_GROUP_ID # Declare global if you want to modify it
        # LOG_GROUP_ID = None # Simple way to disable for session
    except Exception as log_err:
        logger.error(f"TELEGRAM LOG FAIL (Other): Group {LOG_GROUP_ID}: {type(log_err).__name__} - {log_err}", exc_info=False)



# --- Helper Functions ---
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
        reply_to = event.message_id if hasattr(event, 'message_id') else event.id
        return await client.send_message(event.chat_id, text, reply_to=reply_to, parse_mode='html', **kwargs)
    except Exception as e: logger.error(f"Reply fail C:{event.chat_id} M:{event.id} E:{e}", exc_info=False); return await safe_send_message(event.chat_id, text, **kwargs)

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

# --- Database Helpers ---
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
        "$setOnInsert": {"_id": user_id_str, "runs": 0, "wickets": 0, "achievements": [], "registered_at": now, "matches_played": 0}
    }
    try:
        result = users_collection.update_one({"_id": user_id_str}, user_doc, upsert=True)
        registered = result.upserted_id is not None or result.matched_count > 0
        # if registered: logger.info(f"User {user_id_str} ('{safe_full_name}') registered/updated.")
        return registered
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
        if res.modified_count > 0: logger.info(f"DB: Incremented matches_played for {res.modified_count} users: {user_id_str_list}.")
        elif res.matched_count == 0: logger.warning(f"DB: No users matched for matches_played increment: {user_id_str_list}.")
        return res.modified_count > 0
    except Exception as e: logger.error(f"DB inc matches {user_id_list}: {e}", exc_info=True); return False

# Async wrappers
async def add_runs_to_user(user_id, runs): await asyncio.to_thread(add_runs_sync, user_id, runs)
async def add_wicket_to_user(user_id): await asyncio.to_thread(add_wicket_sync, user_id)
async def increment_matches_played(user_ids): await asyncio.to_thread(increment_matches_played_sync, user_ids)

# --- Leaderboard/Profile Helpers ---
async def get_user_rank(user_id, field):
    if users_collection is None: return None
    user_data = await asyncio.to_thread(get_user_data, user_id)
    if not user_data or user_data.get(field, 0) <= 0: return None
    try:
        score = user_data[field]
        count = await asyncio.to_thread(users_collection.count_documents, {field: {"$gt": score}})
        return count + 1
    except Exception as e: logger.error(f"DB rank fail U:{user_id} F:{field}: {e}", exc_info=True); return None

def _get_leaderboard_text_sync(field, top_n=10):
    if users_collection is None: return None, "⚠️ Database unavailable."
    try:
        users = list(users_collection.find({field: {"$gt": 0}}, {"_id": 1, "full_name": 1, field: 1}).sort(field, -1).limit(top_n))
        if not users: return None, f"No {field} recorded yet."
        return users, None
    except Exception as e: logger.error(f"DB lead fail F:{field}: {e}", exc_info=True); return None, f"⚠️ Error fetching {field} leaderboard."

async def display_leaderboard(event, field, title):
    top_users, error_msg = await asyncio.to_thread(_get_leaderboard_text_sync, field)
    if error_msg: return await safe_reply(event, error_msg)
    if not top_users: return await safe_reply(event, f"No {field} recorded yet.")

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

# --- Game Cleanup ---
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

# --- Format Overs ---
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

def create_join_team_keyboard(game_id, game_data):
    max_p = game_data['max_players_per_team']
    a_count = len(game_data['teams']['A']['players'])
    b_count = len(game_data['teams']['B']['players'])

    btn_a = Button.inline(f"Join Team A ({a_count}/{max_p})", data=f"team_join:A:{game_id}") if a_count < max_p else Button.inline(f"Team A ({a_count}/{max_p}) FULL", data="noop")

    # Prevent Host (always in A) from joining B
    host_id = game_data.get('host_id')
    host_in_a = host_id in game_data['teams']['A']['players']
    can_join_b = b_count < max_p and (not host_in_a or host_id != game_data['teams']['A']['players'][0] if host_in_a and game_data['teams']['A']['players'] else True) # Simpler: just check size if host check complicated

    btn_b = Button.inline(f"Join Team B ({b_count}/{max_p})", data=f"team_join:B:{game_id}") if b_count < max_p else Button.inline(f"Team B ({b_count}/{max_p}) FULL", data="noop")

    buttons = [[btn_a, btn_b]]

    # Show Start button if Host is in A, and both teams have at least 1 player
    host_id = game_data.get('host_id')
    host_in_a = host_id in game_data['teams']['A']['players'] if host_id else False
    can_start = host_in_a and a_count >= 1 and b_count >= 1

    # Only allow starting from WAITING state
    if can_start and game_data['state'] == STATE_TEAM_WAITING:
         buttons.append([Button.inline("▶️ Start Game (Host Only)", data=f"start_game::{game_id}")])

    return buttons

def create_team_batbowl_keyboard(game_id):
     return [[Button.inline("Bat 🏏", data=f"team_batorbowl:bat:{game_id}"), Button.inline("Bowl 🧤", data=f"team_batorbowl:bowl:{game_id}")]]

def create_team_toss_keyboard(game_id):
    return [[Button.inline("Heads", data=f"team_toss:H:{game_id}"), Button.inline("Tails", data=f"team_toss:T:{game_id}")]]

def create_player_selection_keyboard(game_id, team_id, players_dict, action_prefix):
    """Creates buttons to select a player from a team."""
    buttons = []
    row = []
    # Ensure players_dict is iterable
    player_items = players_dict.items() if isinstance(players_dict, dict) else []
    for p_id, p_name in player_items:
        safe_name = html.escape(p_name[:20]) # Truncate long names for button
        row.append(Button.inline(safe_name, data=f"{action_prefix}:{p_id}:{game_id}"))
        if len(row) == 2: # Max 2 buttons per row
            buttons.append(row)
            row = []
    if row: buttons.append(row) # Add remaining buttons
    # Add a Cancel/Skip button maybe?
    # buttons.append([Button.inline("Cancel Selection", data=f"cancel_select::{game_id}")])
    return buttons if buttons else [[Button.inline("Error: No players found", data="noop")]] # Handle empty team case





# =============================================
# --- Command Handlers ---
# =============================================

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
             elif payload == 'show_help': await handle_help(event); return # Handle help deep link
             else: logger.info(f"Unhandled start payload: {payload}")
        except IndexError: pass # No payload

    if not is_private:
         if users_collection is not None: await register_user_telethon(sender)
         start_msg = (f"Hi {mention}! 👋\nUse <code>/team_cricket</code> [size] or <code>/cricket</code> in a group.\nUse /start in my DM for stats.")
         buttons = None; bot_uname = bot_info.username if bot_info else None
         if bot_uname: buttons = client.build_reply_markup([Button.url("Open DM", f"https://t.me/{bot_uname}?start=from_group")])
         await safe_reply(event, start_msg, buttons=buttons); return

    if users_collection is None: await safe_reply(event, f"Hi {mention}! Welcome!\n⚠️ DB offline, stats disabled."); return
    reg_success = await register_user_telethon(sender)
    user_data = await asyncio.to_thread(get_user_data, user_id) # Check if exists AFTER attempting registration
    if reg_success:
        markup = client.build_reply_markup([[Button.url('Channel', 'https://t.me/TCTCRICKET'), Button.url('Group', 'https://t.me/+SIzIYQeMsRsyOWM1')]], inline_only=True)
        # Check if it was a registration or just an update
        # user_data will be None if this was the first registration attempt within this handler
        is_new_user = user_data is None # Or check result.upserted_id in register_user_sync if needed
        payload_handled = event.message.text.startswith('/start show_') # Check if payload was handled

        if not is_new_user and not payload_handled : # Welcome back, unless deep link handled
             welcome = (f"Welcome back, {mention}!\n\nUse /help or check stats:\n<code>/profile /my_achievement /lead_runs /lead_wickets</code>")
        elif is_new_user: # First time registration
            welcome = (f"Welcome {mention} to TCT BOT!\nYou are now registered.\n\nUse /help or check stats:\n<code>/profile /my_achievement /lead_runs /lead_wickets</code>")
            logger.info(f"New user reg: {full_name} ({user_id})")
            try: # Notify admin
                admin_mention = get_player_mention(user_id, full_name)
                for admin_id in xmods: await safe_send_message(admin_id, f"➕ New user: {admin_mention} (<code>{user_id}</code>)", link_preview=False)
            except Exception as e: logger.error(f"Admin notify fail: {e}")
        else: # Deep link handled, don't send welcome again
             return

        await safe_send_message(chat_id, welcome, buttons=markup, link_preview=False)
    else: await safe_reply(event, f"{mention}, there was an error during registration/update.")

@client.on(events.NewMessage(pattern='/guide'))
async def handle_how_to_play(event):
    """Displays a guide on how to play the game."""
    # Ensure users are registered if you want to get their name nicely, or just send
    # sender = await event.get_sender()
    # mention = get_player_mention(event.sender_id, get_display_name(sender) if sender else f"User {event.sender_id}")
    # await safe_reply(event, f"Hey {mention}!\n\n{HOW_TO_PLAY_TEXT}")
    await safe_reply(event, HOW_TO_PLAY_TEXT)

@client.on(events.NewMessage(pattern='/help'))
async def handle_help(event):
    user_id = event.sender_id; is_admin = user_id in xmods; sender = await event.get_sender()
    mention = get_player_mention(user_id, get_display_name(sender)) if sender else f"User {user_id}"
    user_cmds = """<b><u>User Commands:</u></b>
<code>/start</code> - Register (DM) & welcome.
<code>/help</code> - This help.
<code>/team_cricket</code> [N] - Start NvN game (group, e.g. /team_cricket 3 for 3v3, default 2v2). Max 5 per team.
<code>/cricket</code> - Start 1v1 game (group).
<code>/cancel</code> - Cancel your current game (group).
<code>/rebat</code> - Reply to an OUT player's message in game to mark for scorecard (Host only).
<code>/profile</code> - View stats (reply for others).
<code>/my_achievement</code> - View ranks/achievements (reply for others).
<code>/lead_runs</code> <code>/lead_wickets</code> <code>/lead_matches</code> - Leaderboards.
<code>/ping</code> - Check bot status."""
    admin_cmds = """
<b><u>Admin Commands:</u></b>
<code>/achieve</code> [id] <t> | <code>/remove_achieve</code> [id] <t>
<code>/broad</code> <msg> | <code>/set_runs</code> [id] <amt>
<code>/set_wickets</code> [id] <amt> | <code>/clear_stats</code> [id]
<code>/force_cancel</code> <game_id> | <code>/user_count</code> | <code>/db_stats</code>"""
    help_txt = f"Hello {mention}! Commands:\n\n" + user_cmds
    if is_admin: help_txt += "\n\n" + admin_cmds
    help_txt += "\n\n<i>[N]/[id] optional if replying.</i>"
    # Send help in DM if triggered by deep link /start show_help
    is_deep_link_help = event.message.text == "/start show_help"
    if not event.is_private and not is_deep_link_help:
        bot_uname = bot_info.username if bot_info else None
        buttons = None
        if bot_uname: buttons = client.build_reply_markup([Button.url("Open DM for Full Help", f"https://t.me/{bot_uname}?start=show_help")])
        await safe_reply(event, "Check DM for full command list.", buttons=buttons)
    else: # Send in DM or if triggered by deep link
        await safe_send_message(event.chat_id, help_txt) # Use send_message in DM

# --- Profile Command ---
@client.on(events.NewMessage(pattern='/profile'))
async def handle_profile(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")

    target_user_id = event.sender_id
    target_mention_name = await get_user_name_from_event(event)
    source_mention = get_player_mention(target_user_id, target_mention_name) # User who ran command

    # Check for reply
    if event.is_reply:
        reply_msg = await event.get_reply_message();
        if reply_msg and reply_msg.sender_id:
             target_user_id = reply_msg.sender_id
             # Fetch name safely
             try: r_sender = await client.get_entity(target_user_id)
             except Exception: r_sender = None
             target_mention_name = get_display_name(r_sender) if r_sender else f"User {target_user_id}"
        else:
             target_user_id = None # Indicate invalid reply target

    if not target_user_id: return await safe_reply(event, "Could not identify target user from reply.")

    # Fetch data
    user_data = await asyncio.to_thread(get_user_data, target_user_id)
    target_mention = get_player_mention(target_user_id, target_mention_name)

    if not user_data:
        txt = f"{target_mention}, you aren't registered. /start me." if event.sender_id == target_user_id else f"User {target_mention} not registered."
        return await safe_reply(event, txt)

    # Display data
    runs = user_data.get("runs", 0); wickets = user_data.get("wickets", 0); matches = user_data.get("matches_played", 0)
    reg_date = user_data.get("registered_at"); reg_date_str = reg_date.strftime("%Y-%m-%d") if reg_date else "N/A"
    profile_txt = f"""📊 <b>Profile: {target_mention}</b>
🏏 Runs: <b>{runs}</b> | 🎯 Wkts: <b>{wickets}</b>
🏟️ Matches: <b>{matches}</b> | 🗓️ Joined: <b>{reg_date_str}</b>"""
    if event.sender_id != target_user_id: profile_txt += f"\n<i>(Req by {source_mention})</i>"
    await safe_reply(event, profile_txt)


# --- MyAchievement Command ---
@client.on(events.NewMessage(pattern='/my_achievement'))
async def handle_my_achievement(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")

    target_user_id = event.sender_id
    target_mention_name = await get_user_name_from_event(event)
    source_mention = get_player_mention(target_user_id, target_mention_name)

    # Check for reply
    if event.is_reply:
        reply_msg = await event.get_reply_message();
        if reply_msg and reply_msg.sender_id:
             target_user_id = reply_msg.sender_id
             try: r_sender = await client.get_entity(target_user_id)
             except Exception: r_sender = None
             target_mention_name = get_display_name(r_sender) if r_sender else f"User {target_user_id}"
        else:
             target_user_id = None
    if not target_user_id: return await safe_reply(event, "Could not identify target user from reply.")

    # Fetch data & ranks concurrently
    user_data_task = asyncio.to_thread(get_user_data, target_user_id)
    runs_rank_task = get_user_rank(target_user_id, "runs")
    wickets_rank_task = get_user_rank(target_user_id, "wickets")
    matches_rank_task = get_user_rank(target_user_id, "matches_played")

    user_data = await user_data_task
    runs_rank = await runs_rank_task
    wickets_rank = await wickets_rank_task
    matches_rank = await matches_rank_task

    target_mention = get_player_mention(target_user_id, target_mention_name)
    if not user_data:
        txt = f"{target_mention}, you aren't registered. /start me." if event.sender_id == target_user_id else f"User {target_mention} not registered."
        return await safe_reply(event, txt)

    # Format output
    runs = user_data.get("runs", 0); wickets = user_data.get("wickets", 0); matches = user_data.get("matches_played", 0)
    achievements = user_data.get("achievements", [])
    runs_rank_d = f"#{runs_rank}" if runs_rank else "N/A"; wickets_rank_d = f"#{wickets_rank}" if wickets_rank else "N/A"; matches_rank_d = f"#{matches_rank}" if matches_rank else "N/A"

    stats_txt = f"""📊 Stats & Ranks: {target_mention}:
  🏏 Runs: <b>{runs}</b> (Rank: <code>{runs_rank_d}</code>)
  🎯 Wickets: <b>{wickets}</b> (Rank: <code>{wickets_rank_d}</code>)
  🏟️ Matches: <b>{matches}</b> (Rank: <code>{matches_rank_d}</code>)"""
    achieve_txt = f"\n\n🏆 <b>Achievements</b> ({len(achievements)})"
    if achievements: achieve_txt += ":\n" + "\n".join([f"  🏅 <code>{html.escape(str(a))}</code>" for a in sorted(achievements)])
    else: achieve_txt += ":\n  <i>None yet.</i>"
    final_txt = stats_txt + achieve_txt
    if event.sender_id != target_user_id: final_txt += f"\n\n<i>(Req by {source_mention})</i>"
    await safe_reply(event, final_txt, link_preview=False)

# --- Leaderboard Commands ---
@client.on(events.NewMessage(pattern='/lead_runs'))
async def handle_lead_runs(event): await display_leaderboard(event, "runs", "Top 10 Run Scorers")
@client.on(events.NewMessage(pattern='/lead_wickets'))
async def handle_lead_wickets(event): await display_leaderboard(event, "wickets", "Top 10 Wicket Takers")
@client.on(events.NewMessage(pattern='/lead_matches'))
async def handle_lead_matches(event): await display_leaderboard(event, "matches_played", "Top 10 Most Active")

# --- Ping Command ---
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

# --- Start Team Cricket Command ---
@client.on(events.NewMessage(pattern=r'/team_cricket(?: (\d+))?'))
async def start_team_cricket(event):
    global games
    host_id = event.sender_id
    chat_id = event.chat_id
    sender = await event.get_sender()
    if not sender: return
    host_name = get_display_name(sender)

    if event.is_private: return await safe_reply(event, "Team games are for group chats only.")
    if users_collection is None: return await safe_reply(event, "⚠️ DB offline, cannot start games.")

    user_data = await asyncio.to_thread(get_user_data, host_id)
    if not user_data: return await safe_reply(event, f"{get_player_mention(host_id, host_name)}, please /start me in DM first.")
    host_name = user_data.get("full_name", host_name)

    players_per_team = DEFAULT_PLAYERS_PER_TEAM
    try:
        match = event.pattern_match.group(1)
        if match:
            requested_size = int(match)
            if 1 <= requested_size <= MAX_PLAYERS_PER_TEAM: players_per_team = requested_size
            else: await safe_reply(event, f"Team size must be 1-{MAX_PLAYERS_PER_TEAM}. Using {players_per_team}.")
    except (ValueError, TypeError): pass

    logger.info(f"Host {host_name}({host_id}) initiated /team_cricket ({players_per_team}v{players_per_team}) in C:{chat_id}")

    game_id_to_create = str(uuid4())
    new_game_data = None
    start_text = None
    markup = None

    async with games_lock:
        # Check if host is already involved
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
            'max_players_per_team': players_per_team, # Requested size
            'actual_players_team_A': 0, 'actual_players_team_B': 0, # Actual size at start
            'max_wickets_team_A': players_per_team, 'max_wickets_team_B': players_per_team, # Initial max, updated at start
            'overs_per_innings': DEFAULT_OVERS, 'max_balls': DEFAULT_OVERS * 6,
            'teams': {'A': {'players': [], 'names': {}, 'score': 0, 'wickets': 0, 'player_stats': {}},
                      'B': {'players': [], 'names': {}, 'score': 0, 'wickets': 0, 'player_stats': {}}},
            'innings': 1, 'balls_bowled_this_inning': 0,
            'balls_bowled_inning1': 0, 'balls_bowled_inning2': 0, # For scorecard
            'current_batting_team': None, 'current_bowling_team': None,
            'current_batter_id': None, 'current_bowler_id': None,
            'batter_choice': None, 'target': None, 'last_text': "", 'created_at': time.monotonic(),
            'toss_winner_team': None, 'choice': None, 'last_out_player_id': None,
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

# --- Start 1v1 Cricket Command ---
@client.on(events.NewMessage(pattern='/cricket'))
async def start_1v1_cricket(event):
    global games
    p1_id = event.sender_id
    chat_id = event.chat_id
    sender = await event.get_sender()
    if not sender: return
    p1_name = get_display_name(sender)

    if event.is_private: return await safe_reply(event, "1v1 games are for group chats only.")
    if users_collection is None: return await safe_reply(event, "⚠️ DB offline, cannot start games.")

    user_data = await asyncio.to_thread(get_user_data, p1_id)
    if not user_data: return await safe_reply(event, f"{get_player_mention(p1_id, p1_name)}, please /start me in DM first.")
    p1_name = user_data.get("full_name", p1_name)

    logger.info(f"User {p1_name}({p1_id}) initiated /cricket in C:{chat_id}")

    game_id_to_create = str(uuid4())
    new_game_data = None
    start_text = None
    markup = None

    async with games_lock:
        # Check if player is already involved
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
            'player1': {'id': p1_id, 'name': p1_name, 'score': 0, 'balls_faced': 0, 'balls_bowled': 0, 'wickets_taken': 0}, # Add wickets_taken
            'player2': None,
            'innings': 1, 'balls_bowled_this_inning': 0,
            'balls_bowled_inning1': 0, 'balls_bowled_inning2': 0, # For scorecard
            'balls_this_over': 0, # <-- ADD THIS FIELD, initialize to 0
            'current_batting_team': None, 'current_bowling_team': None,
            'batter_choice': None, 'target': None, 'last_text': "", 'created_at': time.monotonic(),
            'toss_winner_id': None, 'choice': None,
        }
        games[game_id_to_create] = new_game_data
        markup = client.build_reply_markup([[Button.inline("Join Game", data=f"join_1v1::{game_id_to_create}")]])
        start_text = f"⚔️ New 1v1 Cricket Game started by <b>{html.escape(p1_name)}</b>!\nWaiting for an opponent..."
        new_game_data['last_text'] = start_text

    if new_game_data and start_text:
        logger.info(f"Created 1v1 game {game_id_to_create} in C:{chat_id}")
        await log_event_to_telegram("GAME_1V1_CREATE", f"Player {p1_name} ({p1_id}) created 1v1 game.", user_id=p1_id, chat_id=chat_id, game_id=game_id_to_create)
        sent_message = await safe_send_message(chat_id, start_text, buttons=markup)
        if sent_message:
            async with games_lock:
                if game_id_to_create in games: games[game_id_to_create]["message_id"] = sent_message.id
        else:
            logger.error(f"Fail send 1v1 game msg {game_id_to_create}, cleaning up.")
            async with games_lock: games.pop(game_id_to_create, None)
    else: logger.error(f"Failed prepare 1v1 game C:{chat_id}")


# --- Cancel Command ---
@client.on(events.NewMessage(pattern='/cancel'))
async def handle_cancel(event):
    user_id = event.sender_id; chat_id = event.chat_id; sender = await event.get_sender()
    if not sender: return
    canceller_name = get_display_name(sender); game_to_cancel_id = None

    if event.is_private: return await safe_reply(event, "Cancel in the group chat.")

    async with games_lock:
        for gid, gdata in list(games.items()): # Use list to allow removal during iteration safely if needed (though cleanup handles it)
            if gdata['chat_id'] == chat_id:
                involved_ids = set(); is_host = False; is_participant = False
                gtype = gdata.get('game_type')
                state = gdata.get('state')

                if gtype == 'team':
                    host_id = gdata.get('host_id')
                    is_host = user_id == host_id
                    involved_ids.add(host_id)
                    involved_ids.update(gdata['teams']['A']['players'], gdata['teams']['B']['players'])
                    is_participant = user_id in involved_ids
                    can_cancel_early = is_host and state in [STATE_TEAM_HOST_JOIN_WAIT, STATE_TEAM_WAITING]
                elif gtype == '1v1':
                    p1_id = gdata.get('player1', {}).get('id')
                    p2_id = gdata.get('player2', {}).get('id') if gdata.get('player2') else None
                    involved_ids.update([p1_id, p2_id])
                    is_participant = user_id in involved_ids
                    can_cancel_early = (user_id == p1_id) and state == STATE_WAITING

                involved_ids.discard(None)

                if is_participant or can_cancel_early:
                     game_to_cancel_id = gid
                     break

    if game_to_cancel_id:
        logger.info(f"User {user_id} cancelling game {game_to_cancel_id}")
        await cleanup_game(game_to_cancel_id, chat_id, reason=f"cancelled by {html.escape(canceller_name)}")
        await safe_reply(event, f"✅ Game cancelled by <b>{html.escape(canceller_name)}</b>.")
    else: await safe_reply(event, "You aren't in an active game in this chat or cannot cancel it now.")


# --- Rebat Command ---
@client.on(events.NewMessage(pattern='/rebat'))
async def handle_rebat(event):
    if not event.is_reply: return await safe_reply(event, "Reply /rebat to the message of the player who got OUT.")
    if event.is_private: return await safe_reply(event, "Use /rebat in the group chat where the game is.")

    host_id = event.sender_id
    chat_id = event.chat_id
    replied_msg = await event.get_reply_message()
    if not replied_msg or not replied_msg.sender_id: return await safe_reply(event, "Invalid reply.")

    target_player_id = replied_msg.sender_id
    try: target_player_entity = await client.get_entity(target_player_id)
    except Exception: target_player_entity = None
    target_player_name = get_display_name(target_player_entity) if target_player_entity else f"User {target_player_id}"

    game_id_to_modify = None
    updated = False
    err_msg = None

    async with games_lock:
        active_game_found = False
        for gid, g in games.items(): # Use 'g' for game_data
            if g['chat_id'] == chat_id and g.get('game_type') == 'team':
                active_game_found = True # Found a team game in this chat
                if g.get('host_id') != host_id:
                    err_msg = "Only the host of this game can use /rebat."
                    continue # Check next game if user is in multiple (shouldn't happen with start checks)

                target_player_team = None
                if target_player_id in g['teams']['A']['players']: target_player_team = 'A'
                elif target_player_id in g['teams']['B']['players']: target_player_team = 'B'

                if not target_player_team:
                    err_msg = "Replied user is not playing in this game."
                    continue

                player_stats = g['teams'][target_player_team]['player_stats'].get(target_player_id)
                # Check if player is OUT (important: only mark OUT players)
                if player_stats and player_stats.get('is_out'):
                    if not player_stats.get('is_rebat'): # Only allow one rebat mark per player
                        player_stats['is_rebat'] = True
                        game_id_to_modify = gid
                        updated = True
                        logger.info(f"Host {host_id} marked player {target_player_id} for rebat in G:{gid}")
                    else:
                        err_msg = f"{target_player_name} has already been marked for rebat."
                    break # Found game and player, action taken or error found
                elif player_stats: # Player found but not out
                     err_msg = f"{target_player_name} is not currently marked as out."
                     break # Found game, but condition not met
                else: # Player in list but no stats? Should not happen
                    logger.error(f"G:{gid} Player {target_player_id} found in team list but not in player_stats!")
                    err_msg = "Internal error: Player stats missing."
                    break
        # End of loop checking games
        if not active_game_found:
            err_msg = "No active team game found in this chat."
        elif not game_id_to_modify and not err_msg: # Loop finished, no match, no specific error set
             err_msg = "Could not find the specified player or game status is unsuitable for /rebat."

    # Send feedback outside lock
    if updated:
        await safe_reply(event, f"✅ Marked <b>{html.escape(target_player_name)}</b> for rebat on the scorecard.")
    elif err_msg:
        await safe_reply(event, f"⚠️ {err_msg}")
    # else: Should be covered by err_msg logic
# =============================================
# --- Central Callback Query Handler ---
# =============================================
@client.on(events.CallbackQuery)
async def handle_callback_query(event):
    global games # Ensure access to global games dict
    user_id = event.sender_id; chat_id = event.chat_id; message_id = event.message_id
    try: data = event.data.decode('utf-8')
    except Exception as e: logger.warning(f"Callback decode fail U:{user_id} E:{e}"); return await safe_answer_callback(event, "Decode Error", alert=True)

    logger.debug(f"Callback: Data='{data}', User={user_id}, Chat={chat_id}, Msg={message_id}")

    if data == "noop": return await safe_answer_callback(event, "Option unavailable.")

    try: # Parse callback data
        parts = data.split(":")
        action = parts[0]
        value = parts[1] if len(parts) > 1 else None
        game_id = parts[2] if len(parts) > 2 else None
        if not game_id: logger.debug(f"Callback without game_id: {data}"); return await safe_answer_callback(event)
        if value == '_': value = None
        numeric_value = int(value) if value is not None and value.isdigit() else None
    except Exception as e: logger.warning(f"Callback parse error: {data} - {e}"); return await safe_answer_callback(event, "Parse Error.")

    # Prepare variables for actions outside lock
    db_updates = []; game_ended_flag = False; final_text = None; final_keyboard = None
    msg_needs_update = False; player_ids_to_inc_match = []

    async with games_lock:
        g = games.get(game_id) # Use 'g' for game_data

        if not g:
            # Edit the message to indicate the game is over if we can find the original message ID
            try:
                # Attempt to edit the specific message the button was attached to
                await client.edit_message(chat_id, message_id, "Game ended or not found.", buttons=None)
                msg_needs_update = False # Already updated
            except Exception as edit_err:
                logger.warning(f"Couldn't edit message {message_id} for ended game {game_id}: {edit_err}")
                # Don't set final_text/keyboard as we couldn't edit
            await safe_answer_callback(event, "This game is no longer active.", alert=True) # Still inform user

        # Check if the callback is from the current game message ID
        elif message_id != g.get("message_id"):
            return await safe_answer_callback(event, "Use buttons on the latest game message.", alert=True)

        else: # Game exists and message ID matches
            state = g['state']; game_type = g['game_type']
            host_id = g.get('host_id'); p1_id = g.get('player1', {}).get('id')
            p2_id = g.get('player2', {}).get('id') if g.get('player2') else None

            # Get Player Name logic
            player_name = "Unknown"
            if game_type == 'team': player_name = g['teams']['A']['names'].get(user_id) or g['teams']['B']['names'].get(user_id)
            elif game_type == '1v1':
                if user_id == p1_id: player_name = g['player1']['name']
                elif user_id == p2_id: player_name = g['player2']['name']
            if player_name == "Unknown" or player_name is None:
                 sender_entity = await event.get_sender(); player_name = get_display_name(sender_entity) if sender_entity else f"User {user_id}"
            player_mention = get_player_mention(user_id, player_name)

            # ==================================
            # --- TEAM GAME CALLBACK LOGIC ---
            # ==================================
            if game_type == 'team':
                # --- Host Joins ---
                if action == "team_join" and state == STATE_TEAM_HOST_JOIN_WAIT and value == 'A':
                    if user_id != host_id: return await safe_answer_callback(event, "Waiting for host.")
                    if user_id in g['teams']['A']['players']: logger.warning(f"Host {user_id} multi-join G:{game_id}"); return await safe_answer_callback(event, "Already joined.")
                    udata = await asyncio.to_thread(get_user_data, user_id)
                    if not udata: logger.error(f"Host DB error G:{game_id}"); final_text="Host DB Error"; msg_needs_update=True; await safe_answer_callback(event,"DB Error",alert=True); return # Exit lock early on error
                    p_name = udata.get('full_name', player_name)
                    g['teams']['A']['players'].append(user_id); g['teams']['A']['names'][user_id] = p_name
                    if 'player_stats' not in g['teams']['A']: g['teams']['A']['player_stats'] = {}
                    g['teams']['A']['player_stats'][user_id] = {'runs': 0,'balls_faced': 0,'wickets_taken': 0,'balls_bowled': 0,'is_rebat': False,'is_out': False}
                    g['state'] = STATE_TEAM_WAITING; logger.info(f"Host {p_name}({user_id}) joined A G:{game_id}. St->WAITING")
                    temp_kb = create_join_team_keyboard(game_id, g); players_txt = format_team_players_for_ui(g) # Now calls the defined function
                    host_m = get_player_mention(host_id, g['teams']['A']['names'].get(host_id))
                    temp_text = f"⚔️ {g['max_players_per_team']}v{g['max_players_per_team']} Team Cricket!\nHost: {host_m}\n\n{players_txt}\n\nWaiting for players..."
                    g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_kb; msg_needs_update=True; await safe_answer_callback(event, "Joined! Waiting...")

                # --- Player Joins ---
                elif action == "team_join" and state == STATE_TEAM_WAITING and value in ['A', 'B']:
                    target_team = value; max_p = g['max_players_per_team']
                    team_players = g['teams'][target_team]['players']; other_players = g['teams']['B' if target_team == 'A' else 'A']['players']
                    if user_id in team_players or user_id in other_players: return await safe_answer_callback(event, "Already in a team!")
                    if len(team_players) >= max_p: return await safe_answer_callback(event, f"Team {target_team} full!")
                    if user_id == host_id and target_team == 'B': return await safe_answer_callback(event, "Host must be in Team A.")
                    udata = await asyncio.to_thread(get_user_data, user_id)
                    if not udata: join_kb = create_join_team_keyboard(game_id, g); err_text = g['last_text'] + f"\n\n{player_mention} please /start me first."; final_text=err_text; final_keyboard=join_kb; msg_needs_update=True; await safe_answer_callback(event); return # Exit lock early
                    p_name = udata.get('full_name', player_name)
                    g['teams'][target_team]['players'].append(user_id); g['teams'][target_team]['names'][user_id] = p_name
                    if 'player_stats' not in g['teams'][target_team]: g['teams'][target_team]['player_stats'] = {}
                    g['teams'][target_team]['player_stats'][user_id] = {'runs': 0,'balls_faced': 0,'wickets_taken': 0,'balls_bowled': 0,'is_rebat': False,'is_out': False}
                    logger.info(f"Player {p_name}({user_id}) joined {target_team} G:{game_id}")
                    a_count=len(g['teams']['A']['players']); b_count=len(g['teams']['B']['players'])
                    temp_text_base = f"⚔️ {g['max_players_per_team']}v{g['max_players_per_team']} Team Cricket!\nHost: {get_player_mention(host_id, g['teams']['A']['names'].get(host_id))}\n\n{format_team_players_for_ui(g)}\n\n" # Calls defined function
                    temp_text = temp_text_base + "Waiting for players..."
                    temp_keyboard = create_join_team_keyboard(game_id, g)
                    g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True; await safe_answer_callback(event, f"Joined Team {target_team}!")

                # --- Host Starts Game ---
                elif action == "start_game" and state == STATE_TEAM_WAITING:
                    if user_id != host_id: return await safe_answer_callback(event, "Only host can start.")
                    a_players = g['teams']['A']['players']; b_players = g['teams']['B']['players']
                    a_count = len(a_players); b_count = len(b_players)
                    if not (a_count >= 1 and b_count >= 1): return await safe_answer_callback(event, "Need >=1 player per team.", alert=True)
                    g['actual_players_team_A'] = a_count; g['actual_players_team_B'] = b_count
                    g['max_wickets_team_A'] = a_count; g['max_wickets_team_B'] = b_count # Max wkts = actual players
                    g['state'] = STATE_TEAM_TOSS_CALL; caller_id = a_players[0] # Host calls
                    caller_name = g['teams']['A']['names'].get(caller_id); caller_mention = get_player_mention(caller_id, caller_name)
                    players_txt = format_team_players_for_ui(g) # Show final teams
                    temp_text = f"⚔️ {a_count}v{b_count} Team Cricket Started!\n{players_txt}\n\nCoin Toss: {caller_mention}, call H or T:"
                    temp_keyboard = create_team_toss_keyboard(game_id); logger.info(f"G:{game_id} started by host ({a_count}v{b_count}). St->TOSS_CALL")
                    g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True; await safe_answer_callback(event)

                # --- Toss Call ---
                elif action == "team_toss" and state == STATE_TEAM_TOSS_CALL:
                    caller_id = g['teams']['A']['players'][0] # Host calls
                    caller_name = g['teams']['A']['names'].get(caller_id)
                    if user_id != caller_id: return await safe_answer_callback(event, f"Wait for {html.escape(caller_name)}.")
                    choice=value; flip=random.choice(['H','T']); heads=flip=='H'; host_wins=(choice==flip)
                    winner_team_id='A' if host_wins else 'B'; g['toss_winner_team']=winner_team_id
                    winner_captain_id=g['teams'][winner_team_id]['players'][0]; winner_captain_name=g['teams'][winner_team_id]['names'].get(winner_captain_id)
                    winner_mention=get_player_mention(winner_captain_id,winner_captain_name)
                    g['state']=STATE_TEAM_BAT_BOWL_CHOICE; logger.info(f"G:{game_id} Toss: {choice}v{flip}. Win:{winner_team_id}. St->BAT_BOWL_CHOICE")
                    players_txt=format_team_players_for_ui(g); toss_txt = (f"⚔️ {g['actual_players_team_A']}v{g['actual_players_team_B']} Team Cricket\n{players_txt}\n\n"
                                f"Coin: <b>{'Heads' if heads else 'Tails'}</b>! Team {winner_team_id} won.\n➡️ {winner_mention}, choose Bat/Bowl:")
                    temp_kb=create_team_batbowl_keyboard(game_id); g['last_text']=toss_txt; final_text=toss_txt; final_keyboard=temp_kb; msg_needs_update=True; await safe_answer_callback(event)

                # --- Bat/Bowl Choice ---
                elif action == "team_batorbowl" and state == STATE_TEAM_BAT_BOWL_CHOICE:
                    toss_winner_team=g.get('toss_winner_team');
                    if not toss_winner_team: logger.error(f"G:{game_id} Toss winner missing!"); return await safe_answer_callback(event,"Internal Error",alert=True)
                    chooser_id = g['teams'][toss_winner_team]['players'][0]; chooser_name = g['teams'][toss_winner_team]['names'].get(chooser_id)
                    if user_id != chooser_id: return await safe_answer_callback(event, f"Wait for {html.escape(chooser_name)}.")
                    choice=value;
                    if choice not in ['bat','bowl']: return await safe_answer_callback(event,"Invalid choice")
                    bat_first = toss_winner_team if choice == 'bat' else ('B' if toss_winner_team == 'A' else 'A')
                    bowl_first = 'B' if bat_first == 'A' else 'A'; g['choice'] = choice
                    g.update({'current_batting_team': bat_first, 'current_bowling_team': bowl_first, 'state': STATE_TEAM_HOST_SELECT_BATTER, 'innings': 1, 'balls_bowled_this_inning': 0, 'balls_this_over': 0}) # Reset counters
                    for t in ['A','B']: g['teams'][t]['score']=0; g['teams'][t]['wickets']=0 # Ensure score reset
                    logger.info(f"G:{game_id} Team {toss_winner_team} chose {choice}. Bat:{bat_first}. St->HOST_SELECT_BATTER")
                    host_m=get_player_mention(host_id,g['teams']['A']['names'].get(host_id))
                    sel_txt=(f"Team {toss_winner_team} chose <b>{choice.upper()}</b>.\n➡️ Host ({host_m}), select 1st batter for Team {bat_first}:")
                    sel_kb=create_player_selection_keyboard(game_id,bat_first,g['teams'][bat_first]['names'],"sel_bat")
                    g['last_text']=sel_txt; final_text=sel_txt; final_keyboard=sel_kb; msg_needs_update=True; await safe_answer_callback(event)

                # --- Host Select Batter ---
                elif action == "sel_bat" and state == STATE_TEAM_HOST_SELECT_BATTER:
                    if user_id != host_id: return await safe_answer_callback(event, "Wait for Host.")
                    try: selected_batter_id = int(value)
                    except: return await safe_answer_callback(event, "Invalid selection.")
                    bat_team=g['current_batting_team']
                    if selected_batter_id not in g['teams'][bat_team]['players']: return await safe_answer_callback(event,"Player not in batting team.", alert=True)
                    g['current_batter_id'] = selected_batter_id; g['state'] = STATE_TEAM_HOST_SELECT_BOWLER
                    logger.info(f"G:{game_id} Host sel batter {selected_batter_id}. St->HOST_SELECT_BOWLER")
                    host_m=get_player_mention(host_id,g['teams']['A']['names'].get(host_id)); bowl_team=g['current_bowling_team']
                    sel_txt=(f"Batter selected.\n➡️ Host ({host_m}), select 1st bowler for Team {bowl_team}:")
                    sel_kb=create_player_selection_keyboard(game_id,bowl_team,g['teams'][bowl_team]['names'],"sel_bowl")
                    g['last_text']=sel_txt; final_text=sel_txt; final_keyboard=sel_kb; msg_needs_update=True; await safe_answer_callback(event)

                # --- Host Select Bowler ---
                elif action == "sel_bowl" and state == STATE_TEAM_HOST_SELECT_BOWLER:
                    if user_id != host_id: return await safe_answer_callback(event, "Wait for Host.")
                    try: selected_bowler_id = int(value)
                    except: return await safe_answer_callback(event, "Invalid selection.")
                    bowl_team=g['current_bowling_team']
                    if selected_bowler_id not in g['teams'][bowl_team]['players']: return await safe_answer_callback(event,"Player not in bowling team.", alert=True)
                    g['current_bowler_id'] = selected_bowler_id; g['state'] = STATE_TEAM_BATTING
                    logger.info(f"G:{game_id} Host sel bowler {selected_bowler_id}. St->BATTING")
                    status_txt, batter_m, bowler_m = format_team_game_status(g)
                    play_txt = f"Alright, let's play!\n\n{status_txt}\n\n➡️ {batter_m}, select shot (1-6):"
                    play_kb = create_standard_keyboard(game_id)
                    g['last_text']=play_txt; final_text=play_txt; final_keyboard=play_kb; msg_needs_update=True; await safe_answer_callback(event)

                # --- Gameplay Number Input ---
                elif action == "num" and state in [STATE_TEAM_BATTING, STATE_TEAM_BOWLING_WAIT]:
                    if numeric_value is None or not (1 <= numeric_value <= 6): return await safe_answer_callback(event, "Invalid input (1-6).", alert=True)
                    batter_id=g['current_batter_id']; bowler_id=g['current_bowler_id']
                    bat_team=g['current_batting_team']; bowl_team=g['current_bowling_team']
                    batter_stats = g['teams'][bat_team]['player_stats'].get(batter_id)
                    bowler_stats = g['teams'][bowl_team]['player_stats'].get(bowler_id)
                    if not batter_id or not bowler_id or batter_stats is None or bowler_stats is None:
                        logger.error(f"G:{game_id} State error: Missing player IDs/stats. B:{batter_id}, BWL:{bowler_id}"); return await safe_answer_callback(event,"Internal game error.",alert=True)
                    batter_name = g['teams'][bat_team]['names'].get(batter_id, f"Btr {batter_id}")
                    bowler_name = g['teams'][bowl_team]['names'].get(bowler_id, f"Bwl {bowler_id}")

                    # --- Batter's Turn ---
                    if state == STATE_TEAM_BATTING:
                        if user_id != batter_id: return await safe_answer_callback(event, f"Waiting for {html.escape(batter_name)}.")
                        if g.get('batter_choice') is not None: return await safe_answer_callback(event, "Played, waiting...")
                        g['batter_choice'] = numeric_value; g['state'] = STATE_TEAM_BOWLING_WAIT
                        logger.info(f"G:{game_id} Batter {batter_name}({user_id}) chose {numeric_value}. St->BOWLING_WAIT")
                        status_txt, _, bowler_m = format_team_game_status(g, batter_played=True)
                        temp_text = f"{status_txt}\n\n➡️ {bowler_m}, select delivery (1-6):"
                        temp_kb = create_standard_keyboard(game_id); g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_kb; msg_needs_update=True; await safe_answer_callback(event, f"Played {numeric_value}. Waiting...")

                    # --- Bowler's Turn ---
                    elif state == STATE_TEAM_BOWLING_WAIT:
                        if user_id != bowler_id: return await safe_answer_callback(event, f"Waiting for {html.escape(bowler_name)}.")
                        bat_num = g.get('batter_choice')
                        if bat_num is None: # Error case
                            g['state']=STATE_TEAM_BATTING; logger.error(f"G:{game_id} Batter choice missing!"); btr_m = get_player_mention(batter_id,batter_name); status_txt,_,_ = format_team_game_status(g)
                            err_txt = f"⚠️ Error: Choice lost.\n{status_txt}\n➡️ {btr_m}, play again:"; err_kb = create_standard_keyboard(game_id); g['last_text']=err_txt; final_text=err_txt; final_keyboard=err_kb; msg_needs_update=True; await safe_answer_callback(event,"Error! Batter play again.",alert=True); return

                        bowl_num = numeric_value; g['balls_bowled_this_inning'] += 1
                        # Use current_balls *after* incrementing for checks below
                        current_balls = g['balls_bowled_this_inning']
                        batter_stats['balls_faced'] += 1; bowler_stats['balls_bowled'] += 1
                        innings_ended_this_ball = False; over_completed = False # Reset flags for this ball
                        # Max balls check is now less relevant with high DEFAULT_OVERS, but kept for robustness
                        max_balls = g.get('max_balls', DEFAULT_OVERS * 6)
                        batter_m = get_player_mention(batter_id, batter_name); bowler_m = get_player_mention(bowler_id, bowler_name)
                        result_prefix = f"{batter_m} <code>{bat_num}</code> | {bowler_m} <code>{bowl_num}</code>\n\n"
                        result_txt = "" # Initialize result text for this ball

                        # --- Process Ball Result (OUT or RUNS) ---
                        if bat_num == bowl_num:
                            # --- OUT ---
                            g['teams'][bat_team]['wickets'] += 1; batter_stats['is_out'] = True; bowler_stats['wickets_taken'] += 1; g['last_out_player_id'] = batter_id
                            wickets = g['teams'][bat_team]['wickets']; max_wickets = g.get(f'max_wickets_team_{bat_team}', g.get('actual_players_team_' + bat_team, 1)) # Use actual player count
                            result_txt = result_prefix + f"💥 <b>OUT!</b> ({wickets}/{max_wickets} Wkts)\n"
                            logger.info(f"G:{game_id} OUT! B:{batter_name}({batter_id}), Wkt:{wickets}. Bwl:{bowler_name}({bowler_id})"); db_updates.append({'type':'wicket','user_id':bowler_id})

                            # Check Innings End Conditions (Wickets, Last Player)
                            if wickets >= max_wickets:
                                innings_ended_this_ball = True; result_txt += "Innings End! (All out)\n"; logger.info(f"G:{game_id} Inn End: All Out")
                            else: # Check if last player just got out
                                players_in_team = g['teams'][bat_team]['players']
                                non_out_players = sum(1 for p_id in players_in_team if not g['teams'][bat_team]['player_stats'].get(p_id, {}).get('is_out'))
                                if non_out_players == 0:
                                    innings_ended_this_ball = True; result_txt += "Innings End! (Last player out)\n"; logger.info(f"G:{game_id} Inn End: Last player")
                            # Note: Overs check removed as primary end condition

                        else:
                            # --- RUNS ---
                            runs=bat_num; g['teams'][bat_team]['score']+=runs; batter_stats['runs']+=runs; score=g['teams'][bat_team]['score']; wickets=g['teams'][bat_team]['wickets']
                            result_txt = result_prefix + f"🏏 <b>{runs}</b> runs! Score: {score}/{wickets}\n"
                            logger.info(f"G:{game_id} Runs:{runs}. B:{batter_name}. Score:{score}/{wickets}"); db_updates.append({'type':'runs','user_id':batter_id,'value':runs})

                            # Check Innings End Conditions after Runs (Target Chased in Innings 2)
                            if g['innings'] == 2 and score >= g['target']:
                                innings_ended_this_ball = True; game_ended_flag = True; result_txt += "Target Chased! Game Over!\n"; g['state']=STATE_TEAM_ENDED
                                player_ids_to_inc_match.extend(g['teams']['A']['players']+g['teams']['B']['players']); logger.info(f"G:{game_id} GameOver(Target). St->ENDED")
                        # --- End Process Ball Result ---

                        # --- INNINGS END / GAME END / CONTINUE LOGIC ---
                        if innings_ended_this_ball:
                            # Record the total balls bowled for the innings that just concluded
                            g['balls_bowled_inning' + str(g['innings'])] = current_balls

                            if g['innings'] == 1:
                                # --- Transition from Innings 1 to Innings 2 ---
                                logger.info(f"G:{game_id} End of Innings 1 detected.")
                                score = g['teams'][bat_team]['score']
                                g['target'] = score + 1
                                result_txt += f"Target: <b>{g['target']}</b>"
                                g['current_batting_team'], g['current_bowling_team'] = bowl_team, bat_team
                                g['innings'] = 2
                                g['balls_bowled_this_inning'] = 0
                                g['balls_this_over'] = 0 # Reset over counter for Innings 2
                                g['batter_choice'] = None
                                g['state'] = STATE_TEAM_HOST_SELECT_BATTER
                                logger.info(f"G:{game_id} Inn 1 end processing complete. Target:{g['target']}. State -> HOST_SELECT_BATTER")
                            else:
                                # --- Innings 2 Ended -> Game Over ---
                                logger.info(f"G:{game_id} End of Innings 2 detected. Game Over.")
                                game_ended_flag = True
                                if not result_txt.endswith("Game Over!\n"): result_txt += "\n<b>Game Over!</b>" # Add if not already there
                                g['state'] = STATE_TEAM_ENDED
                                player_ids_to_inc_match.extend(g['teams']['A']['players'] + g['teams']['B']['players'])
                                logger.info(f"G:{game_id} Game Over processing complete. State -> ENDED")

                        else: # Innings continues
                            # --- FIND NEXT BATTER (only if last ball was OUT) ---
                            if bat_num == bowl_num: # Check if the ball resulted in an OUT
                                logger.debug(f"G:{game_id} Innings continues after OUT, finding next batter.")
                                players_in_team = g['teams'][bat_team]['players']
                                current_bat_idx = -1
                                try: current_bat_idx = players_in_team.index(batter_id)
                                except ValueError: logger.error(f"G:{game_id} Batter {batter_id} not in team {players_in_team}!")
                                next_batter_id = None
                                if current_bat_idx != -1:
                                     num_players = len(players_in_team)
                                     for i in range(1, num_players):
                                         check_idx = (current_bat_idx + i) % num_players
                                         potential_next_id = players_in_team[check_idx]
                                         if not g['teams'][bat_team]['player_stats'].get(potential_next_id,{}).get('is_out'):
                                             next_batter_id = potential_next_id; break
                                if next_batter_id:
                                    g['current_batter_id'] = next_batter_id
                                    g['batter_choice'] = None
                                    g['state'] = STATE_TEAM_BATTING
                                    logger.info(f"G:{game_id} After Wicket. Next batter is {next_batter_id}. State -> BATTING")
                                else:
                                    # --- Safety Net: No non-out batters left ---
                                    logger.error(f"G:{game_id} SAFETY NET: No next batter found after OUT! Forcing innings end.")
                                    innings_ended_this_ball = True # Force flag again
                                    result_txt += "(Error: No available batters found)\n"
                                    g['balls_bowled_inning' + str(g['innings'])] = current_balls # Store balls
                                    if g['innings'] == 2:
                                        game_ended_flag = True; g['state'] = STATE_TEAM_ENDED
                                        player_ids_to_inc_match.extend(g['teams']['A']['players'] + g['teams']['B']['players'])
                                        logger.info(f"G:{game_id} Safety Net: Game Over (Inn 2). State -> ENDED")
                                    else: # Force transition to Innings 2
                                        logger.info(f"G:{game_id} Safety Net: Transitioning to Innings 2.")
                                        score = g['teams'][bat_team]['score']; g['target'] = score + 1
                                        g['current_batting_team'], g['current_bowling_team'] = bowl_team, bat_team
                                        g['innings'] = 2; g['balls_bowled_this_inning'] = 0; g['balls_this_over'] = 0; g['batter_choice'] = None
                                        g['state'] = STATE_TEAM_HOST_SELECT_BATTER
                                        logger.info(f"G:{game_id} Safety Net: Inn 1 end. Target:{g['target']}. State -> HOST_SELECT_BATTER")
                        # --- END INNINGS CONTINUE / NEXT BATTER LOGIC ---

                        # --- Post Ball Processing & Over/Bowler Change Logic ---
                        # This section executes *after* the ball's result and any innings/batter changes have been determined.
                        if not game_ended_flag and not innings_ended_this_ball:
                            g['balls_this_over'] = g.get('balls_this_over', 0) + 1 # Increment balls in this over
                            logger.debug(f"G:{game_id} Ball {g['balls_this_over']} of over.")

                            # Check if over is completed (6 balls bowled)
                            if g['balls_this_over'] >= 6:
                                over_completed = True
                                g['balls_this_over'] = 0 # Reset for next over
                                logger.info(f"G:{game_id} Over completed.")
                                result_txt += "\n✨ **Over Complete!** ✨\n" # Add over complete message here

                                # --- Select Next Bowler (Automatic Cycle) ---
                                available_bowlers = g['teams'][bowl_team].get('players', [])
                                if len(available_bowlers) > 1:
                                    try:
                                        current_bowl_idx = available_bowlers.index(bowler_id)
                                        next_bowl_idx = (current_bowl_idx + 1) % len(available_bowlers)
                                        next_bowler_id = available_bowlers[next_bowl_idx]
                                        g['current_bowler_id'] = next_bowler_id # Update bowler for the next over
                                        logger.debug(f"G:{game_id} New bowler for next over: {next_bowler_id}")
                                    except ValueError:
                                        logger.error(f"G:{game_id} Current bowler {bowler_id} not in team {available_bowlers}!")
                                else:
                                    logger.debug(f"G:{game_id} Only one bowler in team {bowl_team}, cannot cycle.")
                                # --- End Bowler Selection ---

                        # --- Format Message ---
                        temp_keyboard = None # Default to no keyboard
                        if game_ended_flag:
                            temp_text = f"{result_txt}\n\n{format_final_scorecard(g)}"
                        elif innings_ended_this_ball and g['state'] == STATE_TEAM_HOST_SELECT_BATTER: # Innings 1 ended, prompt host
                            host_m=get_player_mention(host_id,g['teams']['A']['names'].get(host_id)); next_bat_team=g['current_batting_team']
                            temp_text = f"{result_txt}\n\n➡️ Host ({host_m}), select 1st batter for Team {next_bat_team}:"
                            temp_keyboard=create_player_selection_keyboard(game_id,next_bat_team,g['teams'][next_bat_team]['names'],"sel_bat")
                        elif not innings_ended_this_ball: # Game continues, prompt batter
                             # Make sure state is BATTING if innings continues
                             if g['state'] != STATE_TEAM_BATTING:
                                 g['state'] = STATE_TEAM_BATTING # Ensure state is correct
                                 g['batter_choice'] = None     # Ensure batter choice is cleared
                                 logger.debug(f"G:{game_id} Correcting state to BATTING for next ball prompt.")
                             status_txt, next_batter_m, _ = format_team_game_status(g) # Get status AFTER potential bowler change
                             temp_text = f"{result_txt}\n{status_txt}\n\n➡️ {next_batter_m}, select shot (1-6):"
                             temp_keyboard=create_standard_keyboard(game_id)
                        else: # Fallback for unexpected state
                             logger.error(f"G:{game_id} Unexpected state after ball format. State:{g['state']}, Ended:{game_ended_flag}, InnEnd:{innings_ended_this_ball}")
                             temp_text = f"{result_txt}\n\nInternal State Error. Game might be stuck."
                             # Attempt to gracefully end if stuck
                             if g['state'] != STATE_TEAM_ENDED:
                                 g['state'] = STATE_TEAM_ENDED; game_ended_flag = True;
                                 player_ids_to_inc_match.extend(g['teams']['A']['players']+g['teams']['B']['players'])
                                 temp_text += "\n" + format_final_scorecard(g) # Show scorecard on forced end

                        # Update game state and prepare message update
                        g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True; await safe_answer_callback(event)

            # ================================
            # --- 1V1 GAME CALLBACK LOGIC ---
            # ================================
            elif game_type == '1v1':
                 # --- Player 2 Joins ---
                 if action == "join_1v1" and state == STATE_WAITING:
                     if user_id == p1_id: return await safe_answer_callback(event, "You started.")
                     if p2_id is not None: return await safe_answer_callback(event, "Game full.")
                     udata=await asyncio.to_thread(get_user_data, user_id)
                     if not udata: join_kb=client.build_reply_markup([[Button.inline("Join",data=f"join_1v1::{game_id}")]]); err_text=g['last_text']+f"\n\n{player_mention} please /start me first."; final_text=err_text; final_keyboard=join_kb; msg_needs_update=True; await safe_answer_callback(event); return
                     p2_name=udata.get('full_name', player_name)
                     g['player2']={'id':user_id,'name':p2_name,'score':0,'balls_faced':0,'balls_bowled':0,'wickets_taken':0}; g['state']=STATE_TOSS; logger.info(f"P2 {p2_name}({user_id}) joined 1v1 G:{game_id}. St->TOSS")
                     p1_m=get_player_mention(p1_id,g['player1']['name']); p2_m=get_player_mention(user_id,p2_name)
                     toss_text=f"⚔️ 1v1 Ready!\n{p1_m} vs {p2_m}\n\n Toss: {p1_m}, call H or T:"; toss_kb=[[Button.inline("Heads",data=f"toss:H:{game_id}"),Button.inline("Tails",data=f"toss:T:{game_id}")]];
                     g['last_text']=toss_text; final_text=toss_text; final_keyboard=toss_kb; msg_needs_update=True; await safe_answer_callback(event, "Joined!")

                 # --- Toss Call (1v1) ---
                 elif action == "toss" and state == STATE_TOSS:
                     if user_id != p1_id: return await safe_answer_callback(event, "Wait P1.")
                     choice=value; flip=random.choice(['H','T']); heads=flip=='H'; p1_wins=(choice==flip); winner_id=p1_id if p1_wins else p2_id
                     winner_name=g['player1']['name'] if p1_wins else g['player2']['name']; winner_m=get_player_mention(winner_id,winner_name)
                     g['toss_winner_id']=winner_id; g['state']=STATE_BAT_BOWL; logger.info(f"G:{game_id}(1v1) Toss:{choice}v{flip}. Win:{winner_id}. St->BAT_BOWL")
                     p1_m=get_player_mention(p1_id,g['player1']['name']); p2_m=get_player_mention(p2_id,g['player2']['name'])
                     toss_res=(f"⚔️ 1v1: {p1_m} vs {p2_m}\nCoin:<b>{'Heads' if heads else 'Tails'}</b>! {winner_m} won.\n➡️ {winner_m}, choose Bat/Bowl:")
                     bb_kb=[[Button.inline("Bat 🏏",data=f"batorbowl:bat:{game_id}"),Button.inline("Bowl 🧤",data=f"batorbowl:bowl:{game_id}")]];
                     g['last_text']=toss_res; final_text=toss_res; final_keyboard=bb_kb; msg_needs_update=True; await safe_answer_callback(event)

                 # --- Bat/Bowl Choice (1v1) ---
                 elif action == "batorbowl" and state == STATE_BAT_BOWL:
                     toss_winner=g.get('toss_winner_id');
                     if user_id != toss_winner: return await safe_answer_callback(event,"Wait winner.")
                     choice=value;
                     if choice not in ['bat','bowl']: return await safe_answer_callback(event,"Invalid choice")
                     bat_first_id=toss_winner if choice=='bat' else (p2_id if toss_winner==p1_id else p1_id)
                     bowl_first_id=p2_id if bat_first_id==p1_id else p1_id; g['choice']=choice
                     g.update({'current_batter_id':bat_first_id,'current_bowler_id':bowl_first_id,'state':STATE_P1_BAT if bat_first_id==p1_id else STATE_P2_BAT,'innings':1,'balls_bowled_this_inning':0, 'balls_this_over': 0}) # Reset counters
                     logger.info(f"G:{game_id}(1v1) Win:{toss_winner} chose {choice}. Bat:{bat_first_id}. St->{g['state']}")
                     status_txt,batter_m,_ = format_1v1_game_status(g)
                     play_txt=f"{get_player_mention(toss_winner,player_name)} chose <b>{choice.upper()}</b>.\n\n{status_txt}\n\n➡️ {batter_m}, select shot (1-6):"
                     play_kb=create_standard_keyboard(game_id); g['last_text']=play_txt; final_text=play_txt; final_keyboard=play_kb; msg_needs_update=True; await safe_answer_callback(event)

                 # --- Number Input (1v1 Gameplay) ---
                 elif action == "num" and state in [STATE_P1_BAT,STATE_P1_BOWL_WAIT,STATE_P2_BAT,STATE_P2_BOWL_WAIT]:
                     if numeric_value is None or not(1<=numeric_value<=6): return await safe_answer_callback(event,"Invalid(1-6).",alert=True)
                     batter_id=g['current_batter_id']; bowler_id=g['current_bowler_id']
                     batter=g['player1'] if batter_id==p1_id else g['player2']; bowler=g['player1'] if bowler_id==p1_id else g['player2']
                     if not batter or not bowler: logger.error(f"G:{game_id}(1v1) Missing player obj!"); return await safe_answer_callback(event,"Internal Error",alert=True)
                     batter_name=batter['name']; bowler_name=bowler['name']; is_p1_state=(state in [STATE_P1_BAT,STATE_P1_BOWL_WAIT])
                     is_bat_st=(state in [STATE_P1_BAT,STATE_P2_BAT]); is_bowl_st=(state in [STATE_P1_BOWL_WAIT,STATE_P2_BOWL_WAIT])

                     if is_bat_st: # Batter's turn
                         if user_id!=batter_id: return await safe_answer_callback(event,f"Wait {html.escape(batter_name)}.")
                         if g.get('batter_choice') is not None: return await safe_answer_callback(event,"Played, waiting...")
                         g['batter_choice']=numeric_value; g['state']=STATE_P1_BOWL_WAIT if is_p1_state else STATE_P2_BOWL_WAIT
                         logger.info(f"G:{game_id}(1v1) Bat {batter_name}({user_id}):{numeric_value}. St->{g['state']}")
                         status_txt,_,bowler_m=format_1v1_game_status(g,batter_played=True)
                         temp_text=f"{status_txt}\n\n➡️ {bowler_m}, select delivery(1-6):"; temp_kb=create_standard_keyboard(game_id);
                         g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_kb; msg_needs_update=True; await safe_answer_callback(event,f"Played {numeric_value}. Wait...")
                     elif is_bowl_st: # Bowler's turn
                         if user_id!=bowler_id: return await safe_answer_callback(event,f"Wait {html.escape(bowler_name)}.")
                         bat_num=g.get('batter_choice');
                         if bat_num is None: # Error
                             g['state']=STATE_P1_BAT if is_p1_state else STATE_P2_BAT; logger.error(f"G:{game_id}(1v1) Bat choice missing!"); btr_m=get_player_mention(batter_id,batter_name); status_txt,_,_=format_1v1_game_status(g)
                             err_txt=f"⚠️ Error:Choice lost.\n{status_txt}\n➡️ {btr_m}, play again:"; err_kb=create_standard_keyboard(game_id); g['last_text']=err_txt; final_text=err_txt; final_keyboard=err_kb; msg_needs_update=True; await safe_answer_callback(event,"Error! Batter play again.",alert=True); return

                         bowl_num = numeric_value; g['balls_bowled_this_inning'] += 1
                         current_balls = g['balls_bowled_this_inning'] # Use after incrementing
                         batter['balls_faced'] += 1; bowler['balls_bowled'] += 1
                         innings_ended=False;
                         # max_balls check less relevant with high DEFAULT_OVERS_1V1
                         max_balls = g.get('max_balls', DEFAULT_OVERS_1V1 * 6)
                         batter_m = get_player_mention(batter_id, batter_name); bowler_m = get_player_mention(bowler_id, bowler_name)
                         result_prefix = f"{batter_m} <code>{bat_num}</code> | {bowler_m} <code>{bowl_num}</code>\n\n"
                         result_txt = ""

                         if bat_num == bowl_num: # OUT
                             innings_ended=True; batter['wickets']=1; bowler['wickets_taken']=1 # Store for scorecard
                             result_txt = result_prefix + "💥 <b>OUT! Innings End!</b>\n"; logger.info(f"G:{game_id}(1v1) OUT! B:{batter_name}, Bwl:{bowler_name}")
                             db_updates.append({'type':'wicket','user_id':bowler_id})
                         else: # RUNS
                             runs=bat_num; batter['score']+=runs; score=batter['score']
                             result_txt = result_prefix + f"🏏 <b>{runs}</b> runs! Score:{score}/0\n"; logger.info(f"G:{game_id}(1v1) Runs:{runs}. B:{batter_name}. Score:{score}"); db_updates.append({'type':'runs','user_id':batter_id,'value':runs})
                             # Check end conditions after runs
                             if g['innings']==2 and score>=g['target']:
                                 innings_ended=True; game_ended_flag=True; result_txt+="Target Chased! Game Over!\n"; g['state']=STATE_1V1_ENDED; player_ids_to_inc_match.extend([p1_id,p2_id]); logger.info(f"G:{game_id}(1v1) GameOver(Target). St->ENDED")
                             elif current_balls >= max_balls: # Still check overs as fallback
                                 innings_ended=True; result_txt+="Innings End! (Overs)\n"; logger.info(f"G:{game_id}(1v1) Inn End: Overs")

                         # Innings End Logic (1v1)
                         if innings_ended and not game_ended_flag:
                             g['balls_bowled_inning' + str(g['innings'])] = current_balls # Store balls
                             if g['innings'] == 1: # Transition Inn 2
                                 g['target'] = batter['score']+1; result_txt+=f"Target:{g['target']}"; g['current_batter_id'],g['current_bowler_id']=bowler_id,batter_id
                                 g['innings']=2; g['balls_bowled_this_inning']=0; g['state']=STATE_P1_BAT if bowler_id==p1_id else STATE_P2_BAT; g['batter_choice']=None; g['balls_this_over']=0; # Reset over count
                                 logger.info(f"G:{game_id}(1v1) Inn 1 end. Tgt:{g['target']}. St->{g['state']}")
                             else: # Game End (Inn 2 ended by Out/Overs)
                                 game_ended_flag=True;
                                 if not result_txt.endswith("Game Over!\n"): result_txt+="\n<b>Game Over!</b>"
                                 g['state']=STATE_1V1_ENDED; player_ids_to_inc_match.extend([p1_id,p2_id]); logger.info(f"G:{game_id}(1v1) GameOver(Inn 2 End). St->ENDED")
                         elif not game_ended_flag: # Game continues
                             g['batter_choice']=None; g['state']=STATE_P1_BAT if is_p1_state else STATE_P2_BAT # Back to batting state

                         # Format Message
                         temp_keyboard = None
                         if game_ended_flag: temp_text = f"{result_txt}\n\n{format_final_scorecard(g)}"
                         else: # Game continues, prompt next batter
                             status_txt,next_batter_m,_ = format_1v1_game_status(g); temp_text=f"{result_txt}\n{status_txt}\n\n➡️ {next_batter_m}, select shot(1-6):"; temp_keyboard=create_standard_keyboard(game_id)

                         g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True; await safe_answer_callback(event)

            else: # Unknown Game Type
                logger.error(f"Callback for Unknown game_type '{game_type}' G:{game_id}"); await safe_answer_callback(event,"Unknown Game Type Error")

    # --- End of Async With games_lock ---

    # --- Post-Lock Operations ---
    if db_updates and users_collection is not None:
        tasks = [add_runs_to_user(up['user_id'],up['value']) if up['type']=='runs' else add_wicket_to_user(up['user_id']) for up in db_updates]
        if tasks: await asyncio.gather(*tasks)
    if msg_needs_update and final_text is not None:
         markup = client.build_reply_markup(final_keyboard) if final_keyboard else None
         await safe_edit_message(chat_id, message_id, final_text, buttons=markup)
    # Removed redundant safe_answer_callback here, it's called within the logic now

    if game_ended_flag:
        logger.info(f"Game {game_id} ended. Cleanup.")
        if player_ids_to_inc_match and users_collection is not None: logger.info(f"Inc matches {player_ids_to_inc_match} G:{game_id}"); await increment_matches_played(player_ids_to_inc_match)
        await cleanup_game(game_id, chat_id, reason="finished normally")


# --- ADD THIS FUNCTION DEFINITION ---
# Place it near other helper/formatting functions

def format_team_players_for_ui(game_data):
    """Formats the list of players in each team for display in the game message."""
    g = game_data
    text = ""
    max_p = g.get('max_players_per_team', '?') # Get max players for display

    for team_id in ['A', 'B']:
        team_info = g['teams'].get(team_id, {})
        players = team_info.get('players', [])
        names = team_info.get('names', {})
        count = len(players)
        text += f"<b>Team {team_id} ({count}/{max_p}):</b>" # Show count/max

        if players:
            text += "\n" # Add newline only if players exist
            player_mentions = []
            for p_id in players:
                p_name = names.get(p_id, f"User {p_id}")
                mention = get_player_mention(p_id, p_name)
                player_mentions.append(f"  • {mention}") # Use bullet points
            text += "\n".join(player_mentions) + "\n"
        else:
            text += " <i>(Empty)</i>\n" # Indicate empty team

        # Add spacing between teams if it's Team A
        if team_id == 'A':
            text += "\n"

    return text.strip() # Remove leading/trailing whitespace

# --- End of new function definition ---

# --- Game Status / Scorecard Formatting ---
def format_team_game_status(game_data, batter_played=False):
     """Generates the main status text for a TEAM game message."""
     g = game_data # Alias for brevity
     bat_team = g.get('current_batting_team'); bowl_team = g.get('current_bowling_team')
     batter_id = g.get('current_batter_id'); bowler_id = g.get('current_bowler_id')
     innings = g.get('innings', 1); balls = g.get('balls_bowled_this_inning', 0); max_balls = g.get('max_balls', DEFAULT_OVERS*6)
     overs_str = format_overs(balls); max_overs_str = format_overs(max_balls)

     # Handle setup states where players might not be selected yet
     state = g.get('state')
     if state == STATE_TEAM_HOST_SELECT_BATTER: return "Selecting first batter...", "N/A", "N/A"
     if state == STATE_TEAM_HOST_SELECT_BOWLER:
         # Batter should be selected by now
         sel_batter_id = g.get('current_batter_id')
         sel_bat_team = g.get('current_batting_team')
         sel_batter_name = "N/A"
         sel_batter_mention = "N/A"
         if sel_batter_id and sel_bat_team:
              sel_batter_name = g['teams'][sel_bat_team]['names'].get(sel_batter_id, f"Btr {sel_batter_id}")
              sel_batter_mention = get_player_mention(sel_batter_id, sel_batter_name)
         return f"Selecting first bowler...\nBatter: {sel_batter_mention}", sel_batter_mention, "N/A"
     # Handle cases where game starts but players somehow missing (shouldn't happen)
     if not bat_team or not bowl_team or batter_id is None or bowler_id is None:
         logger.warning(f"G:{g.get('game_id','?')} format status error: Missing info. State:{state}"); return "Status Error", "N/A", "N/A"

     batter_name = g['teams'][bat_team]['names'].get(batter_id, f"Btr {batter_id}")
     bowler_name = g['teams'][bowl_team]['names'].get(bowler_id, f"Bwl {bowler_id}")
     batter_m = get_player_mention(batter_id, batter_name)
     bowler_m = get_player_mention(bowler_id, bowler_name)

     score = g['teams'][bat_team].get('score', 0)
     wickets = g['teams'][bat_team].get('wickets', 0)
     # Use actual player count for max wickets if available, else requested size
     max_wickets = g.get(f'max_wickets_team_{bat_team}', g.get('max_players_per_team', 1))

     status = f"<b>--- Innings {innings} | Ov: {overs_str}/{max_overs_str} ---</b>"
     target = g.get('target')
     if target: status += f" | Target: <b>{target}</b>"
     status += "\n\n"
     status += f"🏏 <b>Batting: Team {bat_team}</b> [<code>{score}/{wickets}</code> Wkts]\n"
     status += f"   On Strike: {batter_m}"
     if batter_played: status += " (Played)"
     status += "\n"
     status += f"🧤 <b>Bowling: Team {bowl_team}</b>\n"
     status += f"   Bowler: {bowler_m}\n"
     return status, batter_m, bowler_m

def format_1v1_game_status(game_data, batter_played=False):
    """Generates the main status text for a 1V1 game message."""
    g = game_data # Alias
    batter_id = g.get('current_batter_id'); bowler_id = g.get('current_bowler_id')
    p1 = g.get('player1'); p2 = g.get('player2')
    if not batter_id or not bowler_id or not p1 or not p2:
        logger.warning(f"G:{g.get('game_id','?')}(1v1) format status error. State:{g.get('state')}"); return "Status Error", "N/A", "N/A"

    innings = g.get('innings', 1); balls = g.get('balls_bowled_this_inning', 0); max_balls = g.get('max_balls', DEFAULT_OVERS_1V1*6)
    overs_str = format_overs(balls); max_overs_str = format_overs(max_balls)

    batter = p1 if batter_id == p1['id'] else p2
    bowler = p1 if bowler_id == p1['id'] else p2
    batter_m = get_player_mention(batter_id, batter.get('name', 'Batter?'))
    bowler_m = get_player_mention(bowler_id, bowler.get('name', 'Bowler?'))

    score = batter.get('score', 0)

    status = f"<b>--- Innings {innings} | Ov: {overs_str}/{max_overs_str} ---</b>"
    target = g.get('target')
    if target: status += f" | Target: <b>{target}</b>"
    status += "\n\n"
    status += f"🏏 Batter: {batter_m} [<code>{score}/0</code>]" # 1v1 ends on first wicket
    if batter_played: status += " (Played)"
    status += f"\n🧤 Bowler: {bowler_m}\n"
    return status, batter_m, bowler_m
def format_final_scorecard(game_data):
    """Generates the detailed final scorecard with improved formatting."""
    g = game_data
    game_type = g.get('game_type')
    scorecard = "📊 <b>Final Scorecard</b> 📊\n\n"

    try: # Wrap in try-except
        if game_type == 'team':
            teams = {'A': g['teams']['A'], 'B': g['teams']['B']}

            # Determine batting order for titles
            inn1_bat_team_id, inn2_bat_team_id = None, None
            toss_winner = g.get('toss_winner_team')
            choice = g.get('choice')
            if toss_winner and choice:
                 inn1_bat_team_id = toss_winner if choice == 'bat' else ('B' if toss_winner == 'A' else 'A')
                 inn2_bat_team_id = 'B' if inn1_bat_team_id == 'A' else 'A'
            # Add fallback if needed

            # Determine max name length dynamically for padding
            max_name_len = 6 # Minimum width for "Player"
            for team_data in teams.values():
                 for name in team_data.get('names', {}).values():
                      max_name_len = max(max_name_len, len(name))
            max_name_len = min(max_name_len, 16) # Limit max width

            # Define column widths (adjust as needed)
            w_player = max_name_len + 1 # +1 for space
            w_runs = 5   # Runs
            w_balls = 3  # B
            w_wickets = 4 # Wkts
            w_overs = 5  # Ov

            header_format = f"{{:<{w_player}}}{{:>{w_runs}}}{{:>{w_balls}}}{{:>{w_wickets}}}{{:>{w_overs}}}"
            player_line_format = f"{{:<{w_player}}}{{:>{w_runs}}}{{:>{w_balls}}}{{:>{w_wickets}}}{{:>{w_overs}}}"

            header = header_format.format("Player", "Runs", "B", "Wkts", "Ov")

            team_outputs = {}

            for team_id in ['A', 'B']:
                team = teams.get(team_id, {})
                is_inn1_batter = team_id == inn1_bat_team_id
                balls_batted = g.get('balls_bowled_inning1' if is_inn1_batter else 'balls_bowled_inning2', 0)
                # Correct balls if innings ended mid-way
                if g.get('innings') == (1 if is_inn1_batter else 2) and balls_batted == 0:
                     balls_batted = g.get('balls_bowled_this_inning', 0)

                overs_batted_str = format_overs(balls_batted)
                title = f"<b>Team {team_id}: {team.get('score',0)}/{team.get('wickets',0)}</b> ({overs_batted_str} Ov)"
                lines = [title, f"<code>{header}</code>"] # Add header line

                team_players = team.get('players', [])
                player_stats_dict = team.get('player_stats', {})
                player_names_dict = team.get('names', {})

                if not team_players:
                    lines.append("  <i>- No players -</i>")
                else:
                    # Batting stats first
                    lines.append(" -- Batting --")
                    for p_id in team_players:
                        stats = player_stats_dict.get(p_id, {})
                        if stats.get('balls_faced', 0) > 0: # Only show batters who faced balls
                            name = player_names_dict.get(p_id, f"User {p_id}")
                            name_display = name[:w_player-1].ljust(w_player-1) # Truncate and pad
                            runs = str(stats.get('runs', 0))
                            balls_faced = str(stats.get('balls_faced', 0))
                            out_status = ""
                            if stats.get('is_out'):
                                out_status = " (Out)"
                                if stats.get('is_rebat'): out_status = " (Out_R)" # Rebat marker

                            # Format using fixed widths (adjust Runs/Balls display if needed)
                            bat_line = f"<code> {name_display} {runs:>{w_runs-1}} {balls_faced:>{w_balls}}</code>{out_status}"
                            lines.append(bat_line)

                    # Bowling stats second
                    lines.append(" -- Bowling --")
                    bowlers_found = False
                    for p_id in team_players:
                        stats = player_stats_dict.get(p_id, {})
                        if stats.get('balls_bowled', 0) > 0: # Only show bowlers who bowled
                            bowlers_found = True
                            name = player_names_dict.get(p_id, f"User {p_id}")
                            name_display = name[:w_player-1].ljust(w_player-1)
                            wickets_taken = str(stats.get('wickets_taken', 0))
                            overs_bowled_str = format_overs(stats.get('balls_bowled', 0))

                            bowl_line = f"<code> {name_display} {'-':>{w_runs-1}} {'-':>{w_balls}} {wickets_taken:>{w_wickets}} {overs_bowled_str:>{w_overs}}</code>"
                            lines.append(bowl_line)
                    if not bowlers_found:
                         lines.append("  <i>- Did not bowl -</i>")


                team_outputs[team_id] = lines

            # Combine team outputs side-by-side (or one after another if too wide)
            # Simple approach: One after another for clarity
            scorecard += "\n".join(team_outputs.get('A', ["Team A Error"]))
            scorecard += "\n\n" # Separator
            scorecard += "\n".join(team_outputs.get('B', ["Team B Error"]))
            scorecard += f"\n\n{determine_team_winner(g)}" # Result at the end

        elif game_type == '1v1':
            # (Keep 1v1 formatting as it was simpler, or apply similar principles)
            p1 = g.get('player1'); p2 = g.get('player2')
            if not p1 or not p2: return scorecard + "Incomplete player data."
            p1_n=html.escape(p1.get('name','P1')[:15]); p2_n=html.escape(p2.get('name','P2')[:15]); max_l=max(len(p1_n),len(p2_n),6)
            scorecard += f"<b>{p1_n.ljust(max_l)} vs {p2_n.ljust(max_l)}</b>\n"

            inn1_batter_id = g.get('current_bowler_id') if g.get('innings')==2 else g.get('current_batter_id')
            if not inn1_batter_id: inn1_batter_id = g.get('toss_winner_id') if g.get('choice')=='bat' else (p2['id'] if g.get('toss_winner_id')==p1['id'] else p1['id'])

            inn1_batter = p1 if inn1_batter_id == p1['id'] else p2
            inn1_bowler = p2 if inn1_batter_id == p1['id'] else p1
            balls_inn1 = g.get('balls_bowled_inning1', g.get('balls_bowled_this_inning',0) if g.get('innings')==1 else 0)

            scorecard += f"\n<i>Innings 1:</i> ({format_overs(balls_inn1)} Ov)\n"
            scorecard += f" 🏏 {get_player_mention(inn1_batter['id'],inn1_batter['name'])}: <b>{inn1_batter.get('score',0)}</b> ({inn1_batter.get('balls_faced',0)} balls)\n"
            scorecard += f" 🧤 Bowler: {get_player_mention(inn1_bowler['id'],inn1_bowler['name'])} (Wkts: {inn1_bowler.get('wickets_taken',0)})\n"

            if g.get('target'):
                inn2_batter = p1 if inn1_batter_id == p2.get('id') else p2
                inn2_bowler = p2 if inn1_batter_id == p2.get('id') else p1
                balls_inn2 = g.get('balls_bowled_inning2', g.get('balls_bowled_this_inning',0) if g.get('innings')==2 else 0)
                scorecard += f"\n<i>Innings 2:</i> (Target: {g['target']}, {format_overs(balls_inn2)} Ov)\n"
                scorecard += f" 🏏 {get_player_mention(inn2_batter['id'],inn2_batter['name'])}: <b>{inn2_batter.get('score',0)}</b> ({inn2_batter.get('balls_faced',0)} balls)\n"
                scorecard += f" 🧤 Bowler: {get_player_mention(inn2_bowler['id'],inn2_bowler['name'])} (Wkts: {inn2_bowler.get('wickets_taken',0)})\n"

            scorecard += f"\n{determine_1v1_winner(g)}" # Result at the end
        else: scorecard += "Scorecard unavailable."

    except Exception as e:
        logger.error(f"Error formatting scorecard G:{g.get('game_id','?')}: {e}", exc_info=True)
        scorecard += "\n\n⚠️ Error generating scorecard details."
    return scorecard

# --- Winner Determination ---
def determine_team_winner(game_data):
    g = game_data
    if g.get('state') != STATE_TEAM_ENDED: return "Game outcome pending."
    try: # Add try-except for safety
        team_a = g['teams']['A']; team_b = g['teams']['B']
        target = g.get('target'); innings = g.get('innings')
        if innings == 2 and target is not None:
            # Determine who batted second based on initial choice
            inn1_bat_team = None; inn2_bat_team = None
            toss_winner = g.get('toss_winner_team')
            choice = g.get('choice')
            if toss_winner and choice:
                 inn1_bat_team = toss_winner if choice == 'bat' else ('B' if toss_winner == 'A' else 'A')
                 inn2_bat_team = 'B' if inn1_bat_team == 'A' else 'A'
            else: # Fallback if info missing
                 logger.warning(f"G:{g['game_id']} Missing toss/choice info for winner calc.")
                 # Simple comparison, might be wrong if game ended early
                 return "Outcome Unknown (Data Error)" if team_a.get('score',0) == team_b.get('score',0) else f"Team {'A' if team_a.get('score',0)>team_b.get('score',0) else 'B'} likely won (Data Error)"

            chasing_score = g['teams'][inn2_bat_team].get('score', 0)
            defending_score = g['teams'][inn1_bat_team].get('score', 0)

            if chasing_score >= target: return f"🏆 Team <b>{inn2_bat_team}</b> wins!"
            elif chasing_score == target - 1: return f"🤝 It's a TIE!"
            else: runs_margin = target - 1 - chasing_score; return f"🏆 Team <b>{inn1_bat_team}</b> wins by {runs_margin} runs!"
        else: return "Game ended prematurely or state error."
    except Exception as e:
         logger.error(f"Error in determine_team_winner G:{g.get('game_id','?')}: {e}", exc_info=True)
         return "Error determining winner."

def determine_1v1_winner(game_data):
    g = game_data
    if g.get('state') != STATE_1V1_ENDED: return "Game outcome pending."
    try: # Add try-except
        p1 = g.get('player1'); p2 = g.get('player2'); target = g.get('target')
        if not p1 or not p2 or target is None: return "Game ended unexpectedly (missing data)."

        inn1_batter_id = g.get('current_bowler_id') if g.get('innings')==2 else g.get('current_batter_id') # Who batted first
        if not inn1_batter_id: # Fallback if state missing
             inn1_batter_id = g.get('toss_winner_id') if g.get('choice')=='bat' else (p2['id'] if g.get('toss_winner_id')==p1['id'] else p1['id'])

        inn2_batter = p1 if inn1_batter_id == p2.get('id') else p2
        inn1_batter = p2 if inn1_batter_id == p2.get('id') else p1
        inn2_score = inn2_batter.get('score', 0)

        if inn2_score >= target: winner_id = inn2_batter.get('id'); winner_name = inn2_batter.get('name')
        elif inn2_score == target - 1: return f"🤝 It's a TIE!"
        else: winner_id = inn1_batter.get('id'); winner_name = inn1_batter.get('name')

        winner_mention = get_player_mention(winner_id, winner_name)
        return f"🏆 {winner_mention} wins!"
    except Exception as e:
        logger.error(f"Error in determine_1v1_winner G:{g.get('game_id','?')}: {e}", exc_info=True)
        return "Error determining winner."

# --- Admin Commands (Placeholders/Examples - Keep as before) ---
@client.on(events.NewMessage(pattern='/user_count', from_users=xmods))
async def handle_user_count(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    try: count = await asyncio.to_thread(users_collection.count_documents, {}); await safe_reply(event, f"👥 Users: <b>{count}</b>")
    except Exception as e: await safe_reply(event, f"Error: {e}"); logger.error(f"Err count users: {e}", exc_info=True)

@client.on(events.NewMessage(pattern=r'/set_runs(?: (\d+))? (\d+)', from_users=xmods))
async def handle_set_runs(event): # Basic example, needs error handling refinement
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
        try: user=await client.get_entity(target_user_id); mention=get_player_mention(target_user_id,get_display_name(user))
        except: mention=f"User <code>{target_user_id}</code>"
        await safe_reply(event, f"✅ Runs for {mention} set to <b>{runs_to_set}</b>.")
    else: await safe_reply(event, f"⚠️ Failed for user <code>{target_user_id}</code> (not registered?).")

# --- Admin Commands ---

@client.on(events.NewMessage(pattern=r'/set_wickets(?: (\d+))? (\d+)', from_users=xmods))
async def handle_set_wickets(event):
    """Admin command to set a user's total wickets."""
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")

    target_user_id = None
    wickets_to_set = 0
    try:
        parts = event.pattern_match.groups()
        # Check if user ID is provided directly after the command
        if parts[0]:
            target_user_id = int(parts[0])
        # Check if it's a reply to get the target user
        elif event.is_reply:
            reply_msg = await event.get_reply_message()
            target_user_id = reply_msg.sender_id if reply_msg else None
        else: # No ID and no reply
            return await safe_reply(event, "Usage: <code>/set_wickets [user_id] <amount></code> or reply.")

        # Get the amount (always the last group in this pattern)
        wickets_to_set = int(parts[1])
        if wickets_to_set < 0: # Wickets cannot be negative
             return await safe_reply(event, "Wicket amount cannot be negative.")

    except (ValueError, TypeError, IndexError):
        return await safe_reply(event, "Invalid format or value. Usage: <code>/set_wickets [user_id] <amount></code> or reply.")

    if not target_user_id:
        return await safe_reply(event, "Could not determine target user.")

    target_id_str = str(target_user_id)

    # Define the database update function (can run in thread)
    def _set_db_wickets(uid, wickets):
        if users_collection is None: return False, "DB Unavailable"
        try:
            res = users_collection.update_one(
                {"_id": uid},
                {"$set": {"wickets": wickets}} # Update the 'wickets' field
            )
            # Check if a document was actually matched (user exists)
            return res.matched_count > 0, None
        except Exception as e:
            logger.error(f"DB set wickets err {uid}: {e}")
            return False, str(e) # Return error message

    # Execute the DB update in a separate thread
    success, err = await asyncio.to_thread(_set_db_wickets, target_id_str, wickets_to_set)

    # Send feedback to the admin
    if success:
        try: # Try to get user mention for better feedback
            user = await client.get_entity(target_user_id)
            mention = get_player_mention(target_user_id, get_display_name(user))
        except Exception: # Fallback if user cannot be fetched
            mention = f"User <code>{target_user_id}</code>"
        await safe_reply(event, f"✅ Wickets for {mention} set to <b>{wickets_to_set}</b>.")
    else:
        err_msg = f"⚠️ Failed to set wickets for <code>{target_user_id}</code>."
        if err:
            err_msg += f" (DB Error: {html.escape(err)})"
        else: # Implies matched_count was 0
            err_msg += " (User not found/registered?)."
        await safe_reply(event, err_msg)

# Add this handler function to your script with the other admin commands.

# --- NEW/REVISED ADMIN HANDLERS ---

async def get_target_id_and_text(event, command_name):
    """Helper to parse target user ID and remaining text from admin commands."""
    target_user_id = None
    text_content = None
    parts = event.text.split(maxsplit=2)
    # Format: /command <target_id> <text> OR reply /command <text>

    if len(parts) >= 2:
        if parts[1].isdigit(): # Check if second part is an ID
            target_user_id = int(parts[1])
            if len(parts) > 2:
                text_content = parts[2].strip()
        else: # Assume second part is the start of the text, target from reply
             if event.is_reply:
                 reply_msg = await event.get_reply_message()
                 if reply_msg and reply_msg.sender_id:
                     target_user_id = reply_msg.sender_id
                 else:
                     await safe_reply(event, "Invalid reply target.")
                     return None, None
             else: # No ID given and no reply
                 await safe_reply(event, f"Usage: <code>/{command_name} [user_id] <text></code> or reply <code>/{command_name} <text></code>")
                 return None, None
             text_content = event.text.split(maxsplit=1)[1].strip() # Text is everything after command

    elif event.is_reply: # Only /command in reply, text required
        reply_msg = await event.get_reply_message()
        if reply_msg and reply_msg.sender_id:
            target_user_id = reply_msg.sender_id
        else:
            await safe_reply(event, "Invalid reply target.")
            return None, None
        # No text provided after command in reply case
        await safe_reply(event, f"Please provide the text/achievement after <code>/{command_name}</code> when replying.")
        return None, None
    else: # Just /command, no ID, no reply
        await safe_reply(event, f"Usage: <code>/{command_name} [user_id] <text></code> or reply <code>/{command_name} <text></code>")
        return None, None

    if not text_content:
         await safe_reply(event, f"Please provide the text/achievement to add/remove.")
         return target_user_id, None

    return target_user_id, text_content

@client.on(events.NewMessage(pattern=r'/achieve(?: (\d+))?( .*)?', from_users=xmods))
async def handle_achieve(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")

    target_user_id, achievement_text = await get_target_id_and_text(event, "achieve")

    if not target_user_id or not achievement_text:
        return # Error message handled by helper

    target_id_str = str(target_user_id)

    def _add_achieve(uid, achieve_txt):
        if users_collection is None: return False, "DB Unavailable"
        try:
             # Use $addToSet to add only if it doesn't exist
             res = users_collection.update_one(
                 {"_id": uid},
                 {"$addToSet": {"achievements": achieve_txt}}
             )
             # Check if user exists even if achievement was already there
             user_exists = users_collection.count_documents({"_id": uid}) > 0
             if not user_exists: return False, "User not found"
             return True, res.modified_count > 0 # Return success and whether it was actually added
        except Exception as e:
            logger.error(f"DB add achievement err {uid}: {e}")
            return False, str(e)

    success, modified = await asyncio.to_thread(_add_achieve, target_id_str, achievement_text)

    if success:
        try: user = await client.get_entity(target_user_id)
        except Exception: user = None
        mention = get_player_mention(target_user_id, get_display_name(user) if user else f"User {target_user_id}")
        safe_achieve_text = html.escape(achievement_text)
        if modified:
             await safe_reply(event, f"✅ Added achievement '<code>{safe_achieve_text}</code>' to {mention}.")
        else:
             await safe_reply(event, f"☑️ Achievement '<code>{safe_achieve_text}</code>' already exists for {mention} (or no change needed).")
    else:
        # Error message might come from _add_achieve or default
        err_msg = modified if isinstance(modified, str) else "User not found or DB Error"
        await safe_reply(event, f"⚠️ Failed to add achievement for <code>{target_user_id}</code>. Reason: {err_msg}")


@client.on(events.NewMessage(pattern=r'/remove_achieve(?: (\d+))?( .*)?', from_users=xmods))
async def handle_remove_achieve(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")

    target_user_id, achievement_text = await get_target_id_and_text(event, "remove_achieve")

    if not target_user_id or not achievement_text:
        return # Error message handled by helper

    target_id_str = str(target_user_id)

    def _remove_achieve(uid, achieve_txt):
        if users_collection is None: return False, "DB Unavailable"
        try:
             # Use $pull to remove the achievement
             res = users_collection.update_one(
                 {"_id": uid},
                 {"$pull": {"achievements": achieve_txt}}
             )
             # Check if user exists
             user_exists = users_collection.count_documents({"_id": uid}) > 0
             if not user_exists: return False, "User not found"
             return True, res.modified_count > 0 # Return success and whether it was actually removed
        except Exception as e:
            logger.error(f"DB remove achievement err {uid}: {e}")
            return False, str(e)

    success, modified = await asyncio.to_thread(_remove_achieve, target_id_str, achievement_text)

    if success:
        try: user = await client.get_entity(target_user_id)
        except Exception: user = None
        mention = get_player_mention(target_user_id, get_display_name(user) if user else f"User {target_user_id}")
        safe_achieve_text = html.escape(achievement_text)
        if modified:
             await safe_reply(event, f"✅ Removed achievement '<code>{safe_achieve_text}</code>' from {mention}.")
        else:
             await safe_reply(event, f"☑️ Achievement '<code>{safe_achieve_text}</code>' not found for {mention} (or no change needed).")
    else:
        err_msg = modified if isinstance(modified, str) else "User not found or DB Error"
        await safe_reply(event, f"⚠️ Failed to remove achievement for <code>{target_user_id}</code>. Reason: {err_msg}")


@client.on(events.NewMessage(pattern='/broad(?: |$)(.*)', from_users=xmods))
async def handle_broadcast(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")

    message_text = event.pattern_match.group(1).strip()
    if not message_text:
        return await safe_reply(event, "Please provide a message to broadcast after <code>/broad</code>.")

    # Confirmation (optional but recommended)
    confirm_msg = await safe_reply(event, f"⚠️ About to broadcast:\n\n<code>{html.escape(message_text)}</code>\n\nTo ALL users. Reply 'yes' to confirm within 30 seconds.")
    if not confirm_msg: return await safe_reply(event, "Error sending confirmation.")

    try:
        async with client.conversation(event.chat_id, timeout=30) as conv:
            response = await conv.get_reply(message=confirm_msg)
            if response.text.lower() != 'yes':
                await safe_edit_message(event.chat_id, confirm_msg.id, "Broadcast cancelled.")
                return
    except asyncio.TimeoutError:
        await safe_edit_message(event.chat_id, confirm_msg.id, "Broadcast confirmation timed out. Cancelled.")
        return
    except Exception as e:
        logger.error(f"Broadcast conversation error: {e}")
        await safe_edit_message(event.chat_id, confirm_msg.id, f"Error during confirmation: {e}. Broadcast cancelled.")
        return

    await safe_edit_message(event.chat_id, confirm_msg.id, "Confirmation received. Starting broadcast...")
    logger.info(f"Admin {event.sender_id} initiated broadcast: {message_text[:50]}...")

    sent_count = 0
    failed_count = 0
    blocked_count = 0
    total_users = 0
    start_time = time.monotonic()

    # Fetch users in batches if needed, but simple iteration for now
    try:
        all_user_ids_cursor = users_collection.find({}, {"_id": 1})
        all_user_ids = [doc["_id"] for doc in all_user_ids_cursor] # Load all into memory
        total_users = len(all_user_ids)
        logger.info(f"Broadcasting to {total_users} users...")
        await log_event_to_telegram("BROADCAST_START", f"Admin {event.sender_id} initiated broadcast.", user_id=event.sender_id, extra_info=f"Msg: {message_text[:100]}")

        status_msg = await safe_reply(event, f"Broadcasting... 0/{total_users} sent.")
        last_update = start_time

        for i, user_id_str in enumerate(all_user_ids):
            try:
                user_id = int(user_id_str)
                # Use send_message directly to user ID
                await client.send_message(user_id, message_text, parse_mode='html')
                sent_count += 1
                # Optional: Add a small delay to avoid hitting flood limits
                # await asyncio.sleep(0.1)
            except (UserNotParticipantError, ValueError, TypeError) as e: # User might not be accessible, bad ID format
                logger.warning(f"Broadcast skip user {user_id_str}: {e}")
                failed_count += 1
            except Exception as e: # Catch broader errors like FloodWaitError, UserIsBlocked, etc.
                logger.warning(f"Broadcast fail user {user_id_str}: {type(e).__name__} - {e}")
                if "User is blocked" in str(e): # Heuristic check for blocked
                     blocked_count += 1
                failed_count += 1
                # Handle FloodWaitError specifically if needed
                if isinstance(e, errors.FloodWaitError):
                     wait_time = e.seconds + 2 # Add buffer
                     logger.warning(f"Flood wait encountered. Sleeping for {wait_time} seconds...")
                     if status_msg: await safe_edit_message(event.chat_id, status_msg.id, f"Flood Wait... Sleeping {wait_time}s...")
                     await asyncio.sleep(wait_time)
                     if status_msg: await safe_edit_message(event.chat_id, status_msg.id, f"Broadcasting... {sent_count}/{total_users} sent ({failed_count} failed).")

            # Update status periodically
            current_time = time.monotonic()
            if status_msg and current_time - last_update > 5: # Update every 5 seconds
                await safe_edit_message(event.chat_id, status_msg.id, f"Broadcasting... {sent_count}/{total_users} sent ({failed_count} failed).")
                
                await log_event_to_telegram("BROADCAST_END", f"Broadcast by {event.sender_id} finished. Sent: {sent_count}, Failed: {failed_count}, Blocked: {blocked_count}", user_id=event.sender_id)
                last_update = current_time

    except Exception as db_err:
        logger.error(f"Error fetching users for broadcast: {db_err}")
        await safe_reply(event, f"Error fetching users: {db_err}")
        return

    end_time = time.monotonic()
    duration = round(end_time - start_time, 2)
    final_status = f"✅ Broadcast Complete!\n" \
                   f"Sent: {sent_count}\n" \
                   f"Failed (Incl. Blocked): {failed_count}\n" \
                   f"(Blocked estimate: {blocked_count})\n" \
                   f"Total Users Queried: {total_users}\n" \
                   f"Duration: {duration}s"
    logger.info(f"Broadcast finished: {final_status.replace('<b>','').replace('</b>','')}") # Log plain text
    if status_msg: await safe_edit_message(event.chat_id, status_msg.id, final_status)
    else: await safe_reply(event, final_status)

# --- End of Admin Commands ---

# --- Main Execution ---
# --- Main Execution ---
async def main():
    global bot_info
    global LOG_GROUP_ID # Ensure main can potentially modify if needed (e.g. disable on critical startup error)
    try:
        logger.info("Starting bot...")
        await client.start(bot_token=BOT_TOKEN)
        bot_info = await client.get_me()
        logger.info(f"Bot logged in as @{bot_info.username} (ID: {bot_info.id})")

        # <<<--- YOUR SNIPPET GOES HERE --- START --->>>
        startup_log_message_parts = [f"Bot @{bot_info.username} started successfully."] # Use a list to build the message
        db_status_for_log = "Unknown" # Default

        if mongo_client is not None and db is not None:
             try:
                 await asyncio.to_thread(db.command, 'ping')
                 logger.info("MongoDB connection confirmed.")
                 db_status_for_log = "OK"
                 startup_log_message_parts.append("Database connection: OK.")
             except Exception as e:
                 logger.error(f"MongoDB check failed after start: {e}", exc_info=True) # Good to have exc_info for console
                 db_status_for_log = f"FAILED ({type(e).__name__})"
                 startup_log_message_parts.append(f"Database connection: {db_status_for_log}.")
                 # Log this specific DB error to Telegram immediately if client is up
                 await log_event_to_telegram("DB_ERROR", "MongoDB connection check FAILED AFTER bot start.", e=e, extra_info="This might affect functionality.")
        elif mongo_client is None:
             logger.warning("Bot running without DB connection.")
             db_status_for_log = "Not Connected"
             startup_log_message_parts.append("Database: NOT CONNECTED (DB features disabled).")
        
        # Construct the final startup log message for Telegram
        final_startup_log_message = " ".join(startup_log_message_parts)
        await log_event_to_telegram("BOT_START", final_startup_log_message, extra_info=f"DB Status: {db_status_for_log}")
        # <<<--- YOUR SNIPPET GOES HERE --- END --->>>


        if not LOG_GROUP_ID:
            logger.warning("Telegram logging group ID (LOG_GROUP_ID) is not set. Logs will not be sent to a Telegram group.")
        else:
            logger.info(f"Attempting to log events to Telegram Group ID: {LOG_GROUP_ID}")
            # Test log (optional, can be noisy on every start)
            # await log_event_to_telegram("SYSTEM_TEST", f"Bot @{bot_info.username} logging system operational.")


        logger.info("Bot is ready and listening for events...")
        await client.run_until_disconnected()
    except Exception as e:
        logger.critical(f"Critical error during bot execution: {e}", exc_info=True)
        if client and client.is_connected(): # Check if client exists and is connected
            await log_event_to_telegram("CRITICAL_ERROR", f"Bot @{bot_info.username if bot_info else 'UnknownBot'} encountered a CRITICAL error. Execution might stop or be unstable.", e=e)
    finally:
        logger.info("Bot is stopping...")
        bot_name_for_shutdown = bot_info.username if bot_info and bot_info.username else 'UnknownBot'
        if client and client.is_connected():
            stop_message = f"Bot @{bot_name_for_shutdown} is shutting down."
            await log_event_to_telegram("BOT_STOP", stop_message)
            try:
                await client.disconnect()
                logger.info("Telethon client disconnected.")
            except Exception as e_disc:
                logger.error(f"Error during Telethon client disconnect: {e_disc}")
        else:
            logger.info("Telethon client was already disconnected or not started.")

        if mongo_client is not None:
            try:
                mongo_client.close()
                logger.info("MongoDB connection closed.")
            except Exception as e_mongo_close:
                logger.error(f"Error closing MongoDB connection: {e_mongo_close}")
        logger.info(f"Bot @{bot_name_for_shutdown} stopped.")

if __name__ == '__main__':
    # try: import uvloop; uvloop.install(); logger.info("Using uvloop for event loop.")
    # except ImportError: logger.info("uvloop not found, using default asyncio event loop.")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by KeyboardInterrupt.")
    except Exception as main_run_ex:
        logger.critical(f"CRITICAL Unhandled error in asyncio.run(main()): {main_run_ex}", exc_info=True)
