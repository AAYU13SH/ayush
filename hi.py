
import asyncio
import random
import logging
from uuid import uuid4
import os
import html
import urllib.parse
import math # For calculating overs
import time # for monotonic and match IDs

from telethon import TelegramClient, events, Button # Button is correctly imported here
from telethon.errors import UserNotParticipantError, MessageNotModifiedError, MessageIdInvalidError, QueryIdInvalidError, BotMethodInvalidError, FloodWaitError
from telethon.tl.types import InputPeerUser, PeerUser, ReplyInlineMarkup, MessageReplyHeader # etc.
from telethon.tl.functions.messages import EditMessageRequest # Import specific requests if needed
from telethon.utils import get_peer_id, get_display_name

from pymongo import MongoClient, ReturnDocument, UpdateOne # Keep sync Pymongo for now, Motor is alternative
from datetime import datetime, timezone

# --- Bot Configuration ---
API_ID = os.environ.get("API_ID", 25695711)
API_HASH = os.environ.get("API_HASH", "f20065cc26d4a31bf0efc0b44edaffa9")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "7906407273:AAHe77DY7TI9gmzsH-UM6k1vB9xDLRa_534")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://yesvashisht:yash2005@clusterdf.yagj9ok.mongodb.net/?retryWrites=true&w=majority&appName=Clusterdf")
MONGO_DB_NAME = "tct_cricket_bot_db"

# --- Game Configuration ---
DEFAULT_PLAYERS_PER_TEAM = 2
MAX_PLAYERS_PER_TEAM = 11
DEFAULT_OVERS = 100
DEFAULT_OVERS_1V1 = 100
WIN_CREDITS = 200
GAME_INACTIVITY_TIMEOUT = 10 * 60  # 10 minutes in seconds for auto-cleanup
LEADERBOARD_ACHIEVEMENT_UPDATE_INTERVAL = 6 * 60 * 60 # Every 6 hours
DAILY_CLAIM_CREDITS_WEEKDAY = 400
DAILY_CLAIM_CREDITS_WEEKEND = 800

# --- Admin Configuration ---
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "6293455550,6265981509,1427723650,6020886539")
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
games_lock = asyncio.Lock() # Lock for accessing the 'games' dictionary

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
    print("Recommendation: Ensure indexes exist on 'runs', 'wickets', 'matches_played', 'credits'. Also on 'achievements' if querying it often.")
except Exception as e:
    print(f"ERROR: Could not connect to MongoDB: {e}")
    print("Warning: Bot running without database. Stats and Credits features will be disabled.")
    users_collection = None
    db = None
    mongo_client = None

# --- Game States ---
STATE_WAITING = "WAITING"
STATE_TOSS = "TOSS"
STATE_BAT_BOWL = "BAT_BOWL"
STATE_P1_BAT = "P1_BAT"
STATE_P1_BOWL_WAIT = "P1_BOWL_WAIT"
STATE_P2_BAT = "P2_BAT"
STATE_P2_BOWL_WAIT = "P2_BOWL_WAIT"
STATE_1V1_ENDED = "1V1_ENDED"
STATE_TEAM_HOST_JOIN_WAIT = "TEAM_HOST_JOIN_WAIT"
STATE_TEAM_WAITING = "TEAM_WAITING"
STATE_TEAM_TOSS_CALL = "TEAM_TOSS_CALL"
STATE_TEAM_BAT_BOWL_CHOICE = "TEAM_BAT_BOWL_CHOICE"
STATE_TEAM_HOST_SELECT_BATTER = "TEAM_HOST_SELECT_BATTER"
STATE_TEAM_HOST_SELECT_BOWLER = "TEAM_HOST_SELECT_BOWLER"
STATE_TEAM_BATTING = "TEAM_BATTING"
STATE_TEAM_BOWLING_WAIT = "TEAM_BOWLING_WAIT"
STATE_TEAM_ENDED = "TEAM_ENDED"


games = {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- How to Play Text ---
HOW_TO_PLAY_TEXT = """<b>🎮 How to Play BCT Cricket Bot 🏏</b>

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
- Use <code>/cancel</code> to cancel a game you started (group). Games also auto-cancel after {GAME_INACTIVITY_MINUTES} minutes of inactivity.
- For team games, the HOST has special responsibilities for player selection and can use <code>/kick_player</code> to remove someone from the game.
- Pay attention to whose turn it is! The bot will prompt you.
- Leaderboards (<code>/top</code>, <code>/lead_runs</code>, etc.) and <code>/profile</code> are available to track stats and credits.
- Set your profile picture for the bot using <code>/setpfp</code> (reply to an image).
- Try your luck with <code>/flip h 100</code> or <code>/dice o 50</code> to bet credits!

Enjoy the game! 🎉
""".format(MAX_PLAYERS_PER_TEAM=MAX_PLAYERS_PER_TEAM, GAME_INACTIVITY_MINUTES=GAME_INACTIVITY_TIMEOUT // 60)


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
    current_parse_mode = kwargs.pop('parse_mode', 'html')
    try:
        return await client.send_message(chat_id, text, parse_mode=current_parse_mode, **kwargs)
    except Exception as e:
        logger.error(f"Send fail C:{chat_id} E:{e}", exc_info=False)
        return None

async def safe_reply(event, text, **kwargs):
    try:
        reply_to_id = event.message_id if hasattr(event, 'message_id') and event.message_id else event.id
        return await client.send_message(event.chat_id, text, reply_to=reply_to_id, parse_mode='html', **kwargs)
    except Exception as e:
        logger.error(f"Reply fail C:{event.chat_id} M:{event.id if hasattr(event, 'id') else 'N/A'} E:{e}", exc_info=False)
        return await safe_send_message(event.chat_id, text, parse_mode='html', **kwargs)


async def safe_edit_message(chat_id, message_id, text, **kwargs):
    if not message_id: return None
    current_parse_mode = kwargs.pop('parse_mode', 'html')
    try:
        return await client.edit_message(chat_id, message_id, text, parse_mode=current_parse_mode, **kwargs)
    except MessageNotModifiedError:
        logger.debug(f"Msg {message_id} not modified.")
        pass
    except (MessageIdInvalidError, BotMethodInvalidError) as e:
        logger.warning(f"Cannot edit msg {message_id} C:{chat_id}: {e}")
        return None
    except FloodWaitError as e:
        logger.warning(f"FloodWaitError on edit: {e.seconds}s. Not retrying edit.")
        return None
    except Exception as e:
        logger.error(f"Edit fail M:{message_id} C:{chat_id}: {e}", exc_info=False)
        return None

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
        "$setOnInsert": {
            "_id": user_id_str, "runs": 0, "wickets": 0, "achievements": [],
            "registered_at": now, "matches_played": 0,
            "credits": 0,
            "last_claimed_daily_at": None, # For /claim
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
        return res.modified_count > 0
    except Exception as e: logger.error(f"DB inc matches {user_id_list}: {e}", exc_info=True); return False

def add_credits_sync(user_id, credits_to_add):
    if users_collection is None:
        logger.warning("DB unavailable for credits operation.")
        return False
    if credits_to_add == 0:
        return True

    user_id_str = str(user_id)
    try:
        user_doc = users_collection.find_one({"_id": user_id_str})
        if not user_doc:
            logger.warning(f"DB: Add/deduct credits fail, user {user_id_str} not found.")
            return False

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
        if not users: return None, f"No significant {field.replace('_', ' ')} recorded yet."
        return users, None
    except Exception as e: logger.error(f"DB lead fail F:{field}: {e}", exc_info=True); return None, f"⚠️ Error fetching {field} leaderboard."

async def display_leaderboard(event, field, title): # Used for group non-interactive fallback
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

    # This function is now primarily for group non-interactive display
    bot_uname = bot_info.username if bot_info else None
    if bot_uname:
        payload = f"show_top_{field}" # Change to use the new /top interactive system
        url = f"https://t.me/{bot_uname}?start={payload}"
        link_txt = title.replace("Top 10 ", "")
        markup = client.build_reply_markup([Button.url(f"📊 View {link_txt} (Interactive DM)", url)])
        await safe_reply(event, "Leaderboards are best viewed interactively in DM. Click below!", buttons=markup)
    else: await safe_reply(event, "Leaderboard available in DM (couldn't get my username for a link).")


LEADERBOARD_TITLES = {
    "credits": "Top 10 Richest Players",
    "runs": "Top 10 Run Scorers",
    "wickets": "Top 10 Wicket Takers",
    "matches_played": "Top 10 Most Active Players"
}

async def _send_or_edit_top_leaderboard(event_or_chat_id, field, title, is_initial_send=False, message_id_to_edit=None, original_event_sender_id=None):
    chat_id = event_or_chat_id.chat_id if hasattr(event_or_chat_id, 'chat_id') else event_or_chat_id
    # If it's an event, use its sender_id as default original_event_sender_id for the first call
    if hasattr(event_or_chat_id, 'sender_id') and original_event_sender_id is None:
        original_event_sender_id = event_or_chat_id.sender_id

    top_users, error_msg = await asyncio.to_thread(_get_leaderboard_text_sync, field, top_n=10)

    if error_msg:
        text_to_send = error_msg
        if is_initial_send and hasattr(event_or_chat_id, 'sender_id'):
             await safe_reply(event_or_chat_id, text_to_send)
        elif message_id_to_edit:
             await safe_edit_message(chat_id, message_id_to_edit, text_to_send, buttons=None)
        else: await safe_send_message(chat_id, text_to_send)
        return

    if not top_users:
        text_to_send = f"No significant {field.replace('_', ' ')} recorded yet to display on the leaderboard."
        if is_initial_send and hasattr(event_or_chat_id, 'sender_id'):
            await safe_reply(event_or_chat_id, text_to_send)
        elif message_id_to_edit:
            await safe_edit_message(chat_id, message_id_to_edit, text_to_send, buttons=None)
        else: await safe_send_message(chat_id, text_to_send)
        return

    medals = ['🥇', '🥈', '🥉'] + ['4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟']
    txt = f"🏆 <b>{title}:</b>\n\n"

    field_name_map_display = {
        "credits": "Credits", "runs": "Runs", "wickets": "Wickets", "matches_played": "Matches"
    }
    field_display_for_score = field_name_map_display.get(field, field.replace('_', ' ').capitalize())

    for i, u in enumerate(top_users):
        prefix = medals[i] if i < len(medals) else f"{i+1}."
        mention = get_player_mention(u['_id'], u.get('full_name'))
        score = u.get(field, 0)
        txt += f"{prefix} {mention} - <b>{score}</b> {field_display_for_score}\n"

    # Add original requester if it's an edit triggered by someone else
    clicker_id = event_or_chat_id.sender_id if hasattr(event_or_chat_id, 'sender_id') else None
    if message_id_to_edit and original_event_sender_id and clicker_id and clicker_id != original_event_sender_id:
        try:
            og_sender = await client.get_entity(original_event_sender_id)
            og_mention = get_player_mention(original_event_sender_id, get_display_name(og_sender))
            txt += f"\n<i>(Original request by {og_mention})</i>"
        except Exception: pass # Silently ignore if original sender cannot be fetched

    # Pass the original_event_sender_id in the callback data
    cb_sender_id_val = str(original_event_sender_id) if original_event_sender_id else "0"
    buttons_row1 = [
        Button.inline("💰 Credits", data=f"top_switch:credits:{cb_sender_id_val}"),
        Button.inline("🏏 Runs", data=f"top_switch:runs:{cb_sender_id_val}")
    ]
    buttons_row2 = [
        Button.inline("🎯 Wickets", data=f"top_switch:wickets:{cb_sender_id_val}"),
        Button.inline("🏟️ Matches", data=f"top_switch:matches_played:{cb_sender_id_val}")
    ]
    markup = client.build_reply_markup([buttons_row1, buttons_row2])

    if is_initial_send and hasattr(event_or_chat_id, 'sender_id'):
        await safe_reply(event_or_chat_id, txt, buttons=markup)
    elif message_id_to_edit:
        await safe_edit_message(chat_id, message_id_to_edit, txt, buttons=markup)
    else:
        await safe_send_message(chat_id, txt, buttons=markup)


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
            cleanup_txt_prefix = ""
            if "timeout" in reason.lower():
                 cleanup_txt_prefix = "Game automatically closed due to inactivity.\n\n"
            elif "cancelled by game creator" in reason.lower():
                 cleanup_txt_prefix = "Game cancelled by creator.\n\n"
            elif "cancelled by admin" in reason.lower():
                 cleanup_txt_prefix = "Game cancelled by admin.\n\n"

            final_text_for_cleanup = cleanup_txt_prefix + last_txt.split("➡️")[0].strip()
            final_text_for_cleanup += f"\n\n<i>(Game session closed: {reason})</i>"

            await safe_edit_message(chat_id, msg_id, final_text_for_cleanup, buttons=None)
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

def create_join_team_keyboard(game_id, game_data, current_user_id=None): # current_user_id is who triggered this UI update
    buttons = []
    max_p = game_data['max_players_per_team']
    a_count = len(game_data['teams']['A']['players'])
    b_count = len(game_data['teams']['B']['players'])
    host_id = game_data.get('host_id')

    # These booleans are specific to the user whose action caused the UI update
    user_in_a = current_user_id in game_data['teams']['A']['players']
    user_in_b = current_user_id in game_data['teams']['B']['players']

    # Row 1: Join A / Join B
    row1 = []
    # Join A button
    if user_in_a: # If the user who caused the update is in A
        row1.append(Button.inline(f"✅ Joined Team A ({a_count}/{max_p})", data=f"noop_join_a:joined:{game_id}"))
    elif a_count >= max_p: # Team A is full
        row1.append(Button.inline(f"Team A ({a_count}/{max_p}) FULL", data=f"noop_join_a:full:{game_id}"))
    else: # Team A is joinable
        row1.append(Button.inline(f"Join Team A ({a_count}/{max_p})", data=f"team_join:A:{game_id}"))

    # Join B button
    if user_in_b: # If the user who caused the update is in B
        row1.append(Button.inline(f"✅ Joined Team B ({b_count}/{max_p})", data=f"noop_join_b:joined:{game_id}"))
    elif b_count >= max_p: # Team B is full
        row1.append(Button.inline(f"Team B ({b_count}/{max_p}) FULL", data=f"noop_join_b:full:{game_id}"))
    else: # Team B is joinable (actual check if host is trying to join B happens in callback)
        row1.append(Button.inline(f"Join Team B ({b_count}/{max_p})", data=f"team_join:B:{game_id}"))
    buttons.append(row1)


    # Row 2: Leave A / Leave B
    row2 = []
    # Leave A button
    can_leave_a = True
    if not user_in_a:
        row2.append(Button.inline("Leave Team A", data=f"noop_leave_a:not_in_team:{game_id}"))
    elif current_user_id == host_id and a_count <=1: # Host is only one in A
        can_leave_a = False
        # Show this specific noop button for the host in this situation
        row2.append(Button.inline("Leave Team A (Host)", data=f"noop_leave_a:host_last_in_a:{game_id}"))

    if user_in_a and can_leave_a:
         row2.append(Button.inline("Leave Team A", data=f"team_leave:A:{game_id}"))
    elif not can_leave_a and not user_in_a: # Should not happen if logic above is correct, but as a fallback
        pass # No "Leave A" button if not in team AND cannot leave (e.g. host last in A implies they are in A)


    # Leave B button
    if not user_in_b: # If user who caused update is not in B
        row2.append(Button.inline("Leave Team B", data=f"noop_leave_b:not_in_team:{game_id}"))
    elif current_user_id == host_id: # Host cannot be in B, so this should not be reachable if host is current_user_id for "Leave B"
        # This case is mostly for UI correctness if host somehow is in B (should be prevented)
        # Or if the UI is being rendered from host's perspective after someone else joined B.
        # The noop_leave_b:host_cant_be_in_b is mainly for the alert text.
        row2.append(Button.inline("Leave Team B", data=f"noop_leave_b:host_cant_be_in_b:{game_id}"))
    else: # User is in B and not host
        row2.append(Button.inline("Leave Team B", data=f"team_leave:B:{game_id}"))

    # Ensure row2 always has two buttons if possible, or placeholders if not applicable for the current_user_id
    while len(row1) < 2: row1.append(Button.inline(" ", data=f"noop_placeholder_a:{game_id}")) # Placeholder
    while len(row2) < 2: row2.append(Button.inline(" ", data=f"noop_placeholder_b:{game_id}")) # Placeholder

    buttons.append(row2)


    # Row 3: Start Game (Host Only)
    host_in_a_check = host_id in game_data['teams']['A']['players'] # Check actual game data for host
    can_start = host_in_a_check and a_count >= 1 and b_count >= 1 and game_data['state'] == STATE_TEAM_WAITING

    if can_start:
        buttons.append([Button.inline("▶️ Start Game (Host Only)", data=f"start_game::{game_id}")])
    else:
        reason = ""
        if not host_in_a_check: reason = "host_not_in_a"
        elif a_count < 1 : reason = "team_a_empty"
        elif b_count < 1 : reason = "team_b_empty"
        elif game_data['state'] != STATE_TEAM_WAITING : reason = "game_not_waiting"
        buttons.append([Button.inline("Start Game (Req. unmet)", data=f"noop_start:{reason}:{game_id}")])

    return buttons


def create_team_batbowl_keyboard(game_id):
     return [[Button.inline("Bat 🏏", data=f"team_batorbowl:bat:{game_id}"), Button.inline("Bowl 🧤", data=f"team_batorbowl:bowl:{game_id}")]]

def create_team_toss_keyboard(game_id):
    return [[Button.inline("Heads", data=f"team_toss:H:{game_id}"), Button.inline("Tails", data=f"team_toss:T:{game_id}")]]

def create_player_selection_keyboard(game_id, team_id, game_data, action_prefix):
    buttons = []
    row = []
    players_dict = game_data['teams'][team_id]['names']
    player_stats_overall = game_data['teams'][team_id]['player_stats']
    player_items = players_dict.items() if isinstance(players_dict, dict) else []

    available_players_count = 0
    for p_id, p_name in player_items:
        safe_name = html.escape(p_name[:20])
        if action_prefix == "sel_bat":
            player_specific_stats = player_stats_overall.get(p_id, {})
            if player_specific_stats.get('is_out', False):
                continue
        available_players_count +=1
        row.append(Button.inline(safe_name, data=f"{action_prefix}:{str(p_id)}:{game_id}")) # Ensure p_id is string
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row: buttons.append(row)

    if available_players_count == 0: # No players found or all out
        if action_prefix == "sel_bat":
            return [[Button.inline("No available batters", data=f"noop_sel_bat:none:{game_id}")]]
        else: # e.g. sel_bowl
            return [[Button.inline(f"Error: No players in Team {team_id}", data=f"noop_sel_bowl:none:{game_id}")]]
    return buttons


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
             if payload.startswith('show_top_'): # e.g. show_top_credits
                 field_to_show = payload.split('_', 2)[-1]
                 if field_to_show in LEADERBOARD_TITLES:
                     await _send_or_edit_top_leaderboard(event, field_to_show, LEADERBOARD_TITLES[field_to_show], is_initial_send=True, original_event_sender_id=user_id)
                     return
                 else: logger.info(f"Unhandled top leaderboard payload: {payload}")
             elif payload == 'show_lead_runs': await _send_or_edit_top_leaderboard(event, "runs", LEADERBOARD_TITLES["runs"], is_initial_send=True, original_event_sender_id=user_id); return
             elif payload == 'show_lead_wickets': await _send_or_edit_top_leaderboard(event, "wickets", LEADERBOARD_TITLES["wickets"], is_initial_send=True, original_event_sender_id=user_id); return
             elif payload == 'show_lead_matches_played': await _send_or_edit_top_leaderboard(event, "matches_played", LEADERBOARD_TITLES["matches_played"], is_initial_send=True, original_event_sender_id=user_id); return
             elif payload == 'show_help': await handle_help(event); return
             elif payload == 'show_guide': await handle_how_to_play(event); return
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
        markup = client.build_reply_markup([[Button.url('Channel', 'https://t.me/BCTupdates'), Button.url('Group', 'https://t.me/BCT_MAIN')]], inline_only=True)
        payload_handled = event.message.text.startswith(('/start show_', '/start from_group'))
        welcome_message_text = ""
        if not is_new_user_check_before_reg and not payload_handled :
             welcome_message_text = (f"Welcome back, {mention}!\n\n * Use /help or /guide.\n * Check stats & credits: <code>/profile</code>\n * Leaderboards: <code>/top</code>")
        elif is_new_user_check_before_reg:
            welcome_message_text = (f"Welcome {mention} to BCT BOT!\n * You are now registered & can earn credits by playing.\n\n * Use /help for commands or /guide to learn how to play.\n * Check stats: <code>/profile</code>\n * View Leaderboards: <code>/top</code>")
            logger.info(f"New user reg: {full_name} ({user_id})")
            try:
                admin_mention = get_player_mention(user_id, full_name)
                for admin_id_val in xmods: await safe_send_message(admin_id_val, f"➕ New user: {admin_mention} (<code>{user_id}</code>)", link_preview=False)
            except Exception as e: logger.error(f"Admin notify fail: {e}")
        else:
             if not payload_handled:
                  welcome_message_text = (f"Welcome back, {mention}!\n\n* Use /help or /guide.\n * Check stats & credits: <code>/profile</code>\n * Leaderboards: <code>/top</code>")
             else:
                 return
        if welcome_message_text:
            await safe_send_message(chat_id, welcome_message_text, buttons=markup, link_preview=False)
    else: await safe_reply(event, f"{mention}, there was an error during registration/update. Please try /start again.")


@client.on(events.NewMessage(pattern=r'/send (\d+) ?(?:@?([\w\d_]+)|(\d+))?'))
async def handle_send_credits(event):
    if users_collection is None:
        return await safe_reply(event, "⚠️ Credits system is currently unavailable (DB offline).")
    sender_id = event.sender_id
    sender_entity = await event.get_sender()
    if not sender_entity:
        logger.warning(f"/send command from unknown sender_id: {sender_id}")
        return await safe_reply(event, "Could not identify the sender.")
    sender_display_name = get_display_name(sender_entity)
    sender_mention = get_player_mention(sender_id, sender_display_name)
    sender_data = await asyncio.to_thread(get_user_data, sender_id)
    if not sender_data:
        return await safe_reply(event, f"{sender_mention}, you need to /start the bot in DM first to use credits.")
    try:
        amount_to_send_str = event.pattern_match.group(1)
        amount_to_send = int(amount_to_send_str)
        if amount_to_send <= 0:
            return await safe_reply(event, "Amount to send must be a positive number.")
        target_username_str = event.pattern_match.group(2)
        target_user_id_from_arg_str = event.pattern_match.group(3)
        recipient_id = None
        recipient_mention_fallback = "The recipient"
        if event.is_reply:
            reply_msg = await event.get_reply_message()
            if reply_msg and reply_msg.sender_id:
                recipient_id = reply_msg.sender_id
                try:
                    replied_user_entity = await client.get_entity(recipient_id)
                    recipient_mention_fallback = get_player_mention(recipient_id, get_display_name(replied_user_entity))
                except Exception:
                    recipient_mention_fallback = f"User <code>{recipient_id}</code>"
            else:
                return await safe_reply(event, "Invalid reply. Please reply to a user's message to send credits or specify their @username/ID directly after the amount.")
        elif target_username_str:
            try:
                recipient_entity = await client.get_entity(target_username_str)
                recipient_id = recipient_entity.id
                recipient_mention_fallback = get_player_mention(recipient_id, get_display_name(recipient_entity))
            except ValueError:
                 return await safe_reply(event, f"Could not find user: <code>{html.escape(target_username_str)}</code>. Please check the username or use their User ID.")
            except Exception as e_entity:
                logger.warning(f"Error getting entity for username '{target_username_str}': {e_entity}")
                return await safe_reply(event, f"Error finding user <code>{html.escape(target_username_str)}</code>. Try using their User ID.")
        elif target_user_id_from_arg_str:
            try:
                recipient_id = int(target_user_id_from_arg_str)
                try:
                    recipient_entity_by_id = await client.get_entity(recipient_id)
                    recipient_mention_fallback = get_player_mention(recipient_id, get_display_name(recipient_entity_by_id))
                except Exception:
                     recipient_mention_fallback = f"User <code>{recipient_id}</code>"
            except ValueError:
                return await safe_reply(event, "Invalid User ID format provided.")
        else:
            return await safe_reply(event, "Usage: <code>/send <amount> @username_or_user_id</code> or reply to a user's message with <code>/send <amount></code>.")
        if not recipient_id:
            return await safe_reply(event, "Could not determine the recipient. Please specify a valid @username, User ID, or reply to a user.")
        if recipient_id == sender_id:
            return await safe_reply(event, "You cannot send credits to yourself!")
        recipient_data = await asyncio.to_thread(get_user_data, recipient_id)
        if not recipient_data:
            return await safe_reply(event, f"{recipient_mention_fallback} is not registered with the bot. They need to /start me in DM first to receive credits.")
        recipient_display_name = recipient_data.get("full_name", f"User {recipient_id}")
        recipient_mention = get_player_mention(recipient_id, recipient_display_name)
        sender_current_credits = sender_data.get("credits", 0)
        if sender_current_credits < amount_to_send:
            return await safe_reply(event, f"{sender_mention}, you only have {sender_current_credits} credits. Not enough to send {amount_to_send}.")
        deduction_result = await add_credits_to_user(sender_id, -amount_to_send)
        if deduction_result == "insufficient":
            logger.warning(f"Credit send: Sender {sender_id} insufficient funds ({sender_current_credits}) for {amount_to_send} - DB check.")
            return await safe_reply(event, "Transaction failed: Insufficient credits (re-checked).")
        if deduction_result is not True:
            logger.error(f"Failed to deduct {amount_to_send} credits from sender {sender_id} for user {recipient_id}. Deduction result: {deduction_result}")
            return await safe_reply(event, "Transaction failed at deduction step. Please try again or contact an admin.")
        addition_result = await add_credits_to_user(recipient_id, amount_to_send)
        if addition_result is not True:
            logger.error(f"CRITICAL: Deducted {amount_to_send} from {sender_id} BUT FAILED to add to recipient {recipient_id}. Addition result: {addition_result}. Refunding sender.")
            refund_result = await add_credits_to_user(sender_id, amount_to_send)
            if refund_result is True:
                await safe_reply(event, "Transaction failed while crediting the recipient. Your credits have been refunded. Please try again or contact an admin.")
            else:
                await safe_reply(event, "CRITICAL ERROR: Transaction failed, and refunding your credits also failed. Please contact an admin IMMEDIATELY with details of this transaction.")
            return
        logger.info(f"CREDIT TRANSFER: {sender_id} ({sender_display_name}) sent {amount_to_send} credits to {recipient_id} ({recipient_display_name}).")
        sender_new_balance = sender_current_credits - amount_to_send
        await safe_reply(event, f"✅ You successfully sent {amount_to_send} credits to {recipient_mention}.\nYour new balance: {sender_new_balance} credits.")
        try:
            recipient_data_after_add = await asyncio.to_thread(get_user_data, recipient_id)
            recipient_new_balance = recipient_data_after_add.get("credits", "an unknown amount") if recipient_data_after_add else "an unknown amount"
            await client.send_message(recipient_id,
                                      f"💰 You have received {amount_to_send} credits from {sender_mention}!\nYour new balance: {recipient_new_balance} credits.")
        except Exception as e_notify_recipient:
            logger.warning(f"Could not send DM notification to recipient {recipient_id} for credits received: {e_notify_recipient}")
    except ValueError:
        await safe_reply(event, "Invalid amount format. Please enter a whole number.")
    except Exception as e:
        logger.error(f"Error in /send command by {sender_id}: {e}", exc_info=True)
        await safe_reply(event, "An unexpected error occurred while trying to send credits. Please contact an admin if this persists.")


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
<code>/send &lt;amount&gt; &lt;@user/id&gt;</code> - Send credits to another user.
<code>/top</code> - Interactive leaderboards (DM).
<code>/lead_runs</code> / <code>/lead_wickets</code> / <code>/lead_matches</code> - Simpler leaderboards (group context).
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
    help_txt += "\n\n<i>[N]/[id] optional if replying to a user's message. Games auto-cancel after {GAME_INACTIVITY_MINUTES} mins of inactivity.</i>".format(GAME_INACTIVITY_MINUTES=GAME_INACTIVITY_TIMEOUT//60)
    is_deep_link_help = event.message.text == "/start show_help"

    if not event.is_private and not is_deep_link_help:
        bot_uname = bot_info.username if bot_info else None
        buttons = None
        if bot_uname: buttons = client.build_reply_markup([Button.url("Open DM for Full Help", f"https://t.me/{bot_uname}?start=show_help")])
        await safe_reply(event, "Check DM for full command list.", buttons=buttons)
    else:
        await safe_send_message(event.chat_id, help_txt)

@client.on(events.NewMessage(pattern='/guide'))
async def handle_how_to_play(event):
    if not event.is_private:
        bot_uname = bot_info.username if bot_info else None
        buttons = None
        if bot_uname:
            buttons = client.build_reply_markup([Button.url("Open DM for Guide", f"https://t.me/{bot_uname}?start=show_guide")])
        await safe_reply(event, "The How to Play guide is best viewed in DM. Click below or use /guide in my DM.", buttons=buttons)
    else:
        await safe_send_message(event.chat_id, HOW_TO_PLAY_TEXT)



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
    pfp_file_id = user_data.get("pfp_file_id")
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
        for ach in sorted(achievements): # Sort for consistent display
            profile_caption += f"  ✧ <code>{html.escape(str(ach))}</code>\n"
    else:
        profile_caption += "🏅 <b>Achievements:</b> <i>None yet.</i>\n"
    if source_user_id != target_user_id:
        source_player_mention = get_player_mention(source_user_id, source_display_name)
        profile_caption += f"\n<i>(Requested by {source_player_mention})</i>"
    if pfp_file_id:
        try:
            await client.send_file(event.chat_id, file=pfp_file_id, caption=profile_caption, parse_mode='html', reply_to=reply_to_id_for_handler_response)
        except Exception as e:
            logger.warning(f"Failed to send profile with PFP (file_id: {pfp_file_id}) for {target_user_id}: {e}. Sending text only.")
            if "Cannot use" in str(e) or "FILE_ID_INVALID" in str(e).upper():
                logger.info(f"Clearing invalid pfp_file_id {pfp_file_id} for user {target_user_id}")
                users_collection.update_one({"_id": str(target_user_id)}, {"$set": {"pfp_file_id": None}})
            await client.send_message(event.chat_id, profile_caption, reply_to=reply_to_id_for_handler_response, parse_mode='html')
    else:
        await client.send_message(event.chat_id, profile_caption, reply_to=reply_to_id_for_handler_response, parse_mode='html')

@client.on(events.NewMessage(pattern='/top'))
async def handle_top_leaderboard(event):
    if event.is_private:
        await _send_or_edit_top_leaderboard(event, "credits", LEADERBOARD_TITLES["credits"], is_initial_send=True, original_event_sender_id=event.sender_id)
    else:
        bot_uname = bot_info.username if bot_info else None
        if bot_uname:
            markup = client.build_reply_markup([Button.url("📊 View Top Leaderboards (DM)", f"https://t.me/{bot_uname}?start=show_top_credits")])
            await safe_reply(event, "Interactive leaderboards are best viewed privately. Click below!", buttons=markup)
        else:
            await safe_reply(event, "Leaderboards available in DM (couldn't get my username for a link).")

@client.on(events.NewMessage(pattern='/lead_runs'))
async def handle_lead_runs(event):
    if event.is_private:
        await _send_or_edit_top_leaderboard(event, "runs", LEADERBOARD_TITLES["runs"], is_initial_send=True, original_event_sender_id=event.sender_id)
    else: # Fallback for group: old non-interactive display with link to DM
        await display_leaderboard(event, "runs", LEADERBOARD_TITLES["runs"])

@client.on(events.NewMessage(pattern='/lead_wickets'))
async def handle_lead_wickets(event):
    if event.is_private:
        await _send_or_edit_top_leaderboard(event, "wickets", LEADERBOARD_TITLES["wickets"], is_initial_send=True, original_event_sender_id=event.sender_id)
    else:
        await display_leaderboard(event, "wickets", LEADERBOARD_TITLES["wickets"])

@client.on(events.NewMessage(pattern='/lead_matches'))
async def handle_lead_matches(event):
    if event.is_private:
        await _send_or_edit_top_leaderboard(event, "matches_played", LEADERBOARD_TITLES["matches_played"], is_initial_send=True, original_event_sender_id=event.sender_id)
    else:
        await display_leaderboard(event, "matches_played", LEADERBOARD_TITLES["matches_played"])


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
                     return await safe_reply(event, f"You are already in a game in this chat! Use /cancel or wait for auto-cleanup (approx. {GAME_INACTIVITY_TIMEOUT//60} mins).")

        new_game_data = {
            'game_id': game_id_to_create, 'game_type': 'team', 'chat_id': chat_id, 'message_id': None,
            'host_id': host_id, 'state': STATE_TEAM_HOST_JOIN_WAIT,
            'max_players_per_team': players_per_team,
            'actual_players_team_A': 0, 'actual_players_team_B': 0,
            'max_wickets_team_A': players_per_team, 'max_wickets_team_B': players_per_team,
            'overs_per_innings': DEFAULT_OVERS, 'max_balls': DEFAULT_OVERS * 6,
            'teams': {'A': {'players': [], 'names': {}, 'score': 0, 'wickets': 0, 'player_stats': {}},
                      'B': {'players': [], 'names': {}, 'score': 0, 'wickets': 0, 'player_stats': {}}},
            'innings': 1, 'balls_bowled_this_inning': 0, 'balls_this_over': 0,
            'balls_bowled_inning1': 0, 'balls_bowled_inning2': 0,
            'current_batting_team': None, 'current_bowling_team': None,
            'current_batter_id': None, 'current_bowler_id': None,
            'batter_choice': None, 'target': None, 'last_text': "",
            'creation_timestamp': time.time(),
            'monotonic_created_at': time.monotonic(),
            'last_activity_timestamp': time.monotonic(), # New for auto-cleanup
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
                     return await safe_reply(event, f"You are already in a game in this chat! Use /cancel or wait for auto-cleanup (approx. {GAME_INACTIVITY_TIMEOUT//60} mins).")

        new_game_data = {
            'game_id': game_id_to_create, 'game_type': '1v1', 'chat_id': chat_id, 'message_id': None,
            'state': STATE_WAITING, 'overs_per_innings': DEFAULT_OVERS_1V1, 'max_balls': DEFAULT_OVERS_1V1 * 6,
            'player1': {'id': p1_id, 'name': p1_name, 'score': 0, 'balls_faced': 0, 'balls_bowled': 0, 'wickets_taken': 0, 'fours':0, 'sixes':0},
            'player2': None,
            'innings': 1, 'balls_bowled_this_inning': 0,
            'balls_bowled_inning1': 0, 'balls_bowled_inning2': 0,
            'balls_this_over': 0,
            'current_batting_team': None, 'current_bowling_team': None,
            'current_batter_id': None, 'current_bowler_id': None,
            'batter_choice': None, 'target': None, 'last_text': "",
            'creation_timestamp': time.time(),
            'monotonic_created_at': time.monotonic(),
            'last_activity_timestamp': time.monotonic(), # New for auto-cleanup
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


@client.on(events.NewMessage(pattern='/cancel'))
async def handle_cancel(event):
    user_id = event.sender_id; chat_id = event.chat_id; sender = await event.get_sender()
    if not sender: return
    canceller_name = get_display_name(sender); game_to_cancel_id = None
    can_cancel_this_game = False
    if event.is_private: return await safe_reply(event, "Cancel in the group chat where the game was started.")
    async with games_lock:
        for gid, gdata in list(games.items()):
            if gdata['chat_id'] == chat_id:
                gtype = gdata.get('game_type')
                if gtype == 'team':
                    host_id_game = gdata.get('host_id')
                    if user_id == host_id_game:
                        can_cancel_this_game = True
                elif gtype == '1v1':
                    p1_id_game = gdata.get('player1', {}).get('id')
                    if user_id == p1_id_game:
                        can_cancel_this_game = True
                if can_cancel_this_game:
                    game_to_cancel_id = gid
                    break
    if game_to_cancel_id:
        logger.info(f"User {user_id} (creator) cancelling game {game_to_cancel_id}")
        await cleanup_game(game_to_cancel_id, chat_id, reason=f"cancelled by game creator {html.escape(canceller_name)}")
        await safe_reply(event, f"✅ Game cancelled by its creator, <b>{html.escape(canceller_name)}</b>.")
    else:
        await safe_reply(event, "You cannot cancel this game. Only the game creator can cancel, or no active game found linked to you as creator in this chat.")


@client.on(events.NewMessage(pattern=r'/kick_player(?: (\d+))?|/kick(?: (\d+))?'))
async def handle_kick_player(event):
    global games
    host_kicker_id = event.sender_id
    chat_id = event.chat_id
    if event.is_private:
        return await safe_reply(event, "This command can only be used in a group chat during a team game.")
    target_user_id_to_kick = None
    try:
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

        game_data_affected['last_activity_timestamp'] = time.monotonic() # Update activity

        kicked_player_mention = get_player_mention(target_user_id_to_kick, kicked_player_name)
        await safe_reply(event, f"👢 {kicked_player_mention} has been kicked from Team {kicked_from_team_id} by the host {kicker_name}.")
        if game_data_affected[f'actual_players_team_A'] == 0 or game_data_affected[f'actual_players_team_B'] == 0:
            await safe_send_message(chat_id, "A team is now empty. Game cancelled due to insufficient players.")
            await cleanup_game(game_id_affected, chat_id, reason="team became empty after kick")
            return
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
                if next_state_after_kick != STATE_TEAM_HOST_SELECT_BATTER: # Avoid overwriting batter select
                    next_state_after_kick = STATE_TEAM_HOST_SELECT_BOWLER
                    prompt_message = f"\nKicked player was bowling. Host, please select a new bowler for Team {game_data_affected['current_bowling_team']}."
        game_data_affected['state'] = next_state_after_kick
        new_text = ""
        new_keyboard = None
        if game_data_affected['state'] == STATE_TEAM_WAITING:
            players_txt = format_team_players_for_ui(game_data_affected)
            host_m = get_player_mention(game_data_affected['host_id'], game_data_affected['teams']['A']['names'].get(game_data_affected['host_id']))
            new_text = f"⚔️ {game_data_affected['max_players_per_team']}v{game_data_affected['max_players_per_team']} Team Cricket!\nHost: {host_m}\n\n{players_txt}\n\nWaiting for players... ({kicked_player_mention} was kicked)"
            new_keyboard = create_join_team_keyboard(game_id_affected, game_data_affected, host_kicker_id)
        elif game_data_affected['state'] == STATE_TEAM_HOST_SELECT_BATTER:
            host_m = get_player_mention(game_data_affected['host_id'], game_data_affected['teams']['A']['names'].get(game_data_affected['host_id']))
            bat_team_select = game_data_affected['current_batting_team']
            new_text = f"{game_data_affected['last_text'].split('➡️')[0].split('---')[0].strip()}\n{prompt_message}\n➡️ Host ({host_m}), select batter for Team {bat_team_select}:"
            new_keyboard = create_player_selection_keyboard(game_id_affected, bat_team_select, game_data_affected, "sel_bat")
        elif game_data_affected['state'] == STATE_TEAM_HOST_SELECT_BOWLER:
            host_m = get_player_mention(game_data_affected['host_id'], game_data_affected['teams']['A']['names'].get(game_data_affected['host_id']))
            bowl_team_select = game_data_affected['current_bowling_team']
            new_text = f"{game_data_affected['last_text'].split('➡️')[0].split('---')[0].strip()}\n{prompt_message}\n➡️ Host ({host_m}), select bowler for Team {bowl_team_select}:"
            new_keyboard = create_player_selection_keyboard(game_id_affected, bowl_team_select, game_data_affected, "sel_bowl")
        else:
            status_txt_kick, _, _ = format_team_game_status(game_data_affected)
            new_text = f"{game_data_affected['last_text'].split('---')[0].strip()}\n{status_txt_kick}\n{prompt_message}\nTurn continues..."
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
# --- Central Callback Query Handler ---
# =============================================
@client.on(events.CallbackQuery)
async def handle_callback_query(event):
    global games
    user_id = event.sender_id; chat_id = event.chat_id; message_id = event.message_id
    try: data = event.data.decode('utf-8')
    except Exception as e: logger.warning(f"Callback decode fail U:{user_id} E:{e}"); return await safe_answer_callback(event, "Decode Error", alert=True)

    logger.info(f"Received Callback: Data='{data}', User={user_id}, Chat={chat_id}, Msg={message_id}")

    if data.startswith("confirm_bc_") or data.startswith("cancel_bc_"):
        if not xmods or user_id not in xmods:
            await safe_answer_callback(event, "Unauthorized for broadcast action.")
            return
        await handle_broadcast_confirmation_telethon(event, data)
        return

    if data.startswith("top_switch:"):
        try:
            _, field_to_switch, original_requester_id_str = data.split(":")
            original_requester_id = int(original_requester_id_str) if original_requester_id_str.isdigit() and original_requester_id_str != "0" else None

            is_admin_clicker = user_id in xmods
            # Allow if it's the original requester, an admin, or if no original requester (e.g. old message)
            can_switch = (original_requester_id is None or
                          user_id == original_requester_id or
                          is_admin_clicker)

            if not can_switch:
                await safe_answer_callback(event, "You cannot change this leaderboard view.", alert=True)
                return

            if field_to_switch in LEADERBOARD_TITLES:
                # Determine whose ID to use for the "Original request by" mention.
                # If an original requester ID exists, use that. Otherwise, use the current clicker's ID.
                id_for_mention = original_requester_id if original_requester_id else user_id

                await _send_or_edit_top_leaderboard(
                    event,
                    field_to_switch,
                    LEADERBOARD_TITLES[field_to_switch],
                    is_initial_send=False,
                    message_id_to_edit=message_id,
                    original_event_sender_id=id_for_mention # This is who requested the /top originally
                )
                await safe_answer_callback(event)
            else:
                await safe_answer_callback(event, "Invalid leaderboard type.", alert=True)
        except ValueError:
            logger.warning(f"Invalid top_switch callback data: {data}")
            await safe_answer_callback(event, "Error processing switch.", alert=True)
        except Exception as e:
            logger.error(f"Error in top_switch callback: {e}", exc_info=True)
            await safe_answer_callback(event, "An error occurred.", alert=True)
        return


    # Handle "noop" callbacks for team join/leave/start UI
    noop_messages = {
        "noop_join_a:joined": "You are already in Team A.",
        "noop_join_a:full": "Team A is full.",
        "noop_join_b:joined": "You are already in Team B.",
        "noop_join_b:full": "Team B is full.",
        "noop_join_b:host_cant_join": "Host must be in Team A.", # This alert is still triggered by the join logic
        "noop_leave_a:not_in_team": "You are not in Team A to leave.",
        "noop_leave_a:host_last_in_a": "Host cannot leave if they are the only one in Team A.",
        "noop_leave_b:not_in_team": "You are not in Team B to leave.",
        "noop_leave_b:host_cant_be_in_b": "Host cannot be in Team B.", # Alert triggered by leave logic
        "noop_start:host_not_in_a": "Host must be in Team A to start.",
        "noop_start:team_a_empty": "Team A needs at least one player to start.",
        "noop_start:team_b_empty": "Team B needs at least one player to start.",
        "noop_start:game_not_waiting": "Game cannot be started at this stage.",
        "noop_sel_bat:none": "No batters available in this team.",
        "noop_sel_bowl:none": "No players available in this team to select as bowler.",
        "noop_placeholder_a": None, # No alert for placeholders
        "noop_placeholder_b": None,
    }
    if data.split(':')[0] in [k.split(':')[0] for k in noop_messages.keys()]:
        for noop_key_full, msg_text in noop_messages.items():
            if data.startswith(noop_key_full):
                if msg_text: await safe_answer_callback(event, msg_text, alert=True)
                else: await safe_answer_callback(event) # Just acknowledge if no message
                return

    try:
        parts = data.split(":")
        if len(parts) != 3:
            logger.warning(f"Invalid game callback data format: {data}. Expected 3 parts.")
            return await safe_answer_callback(event, "Invalid action format.")

        action = parts[0]
        value_str = parts[1]
        game_id = parts[2]

        numeric_value = None
        if action == "num" and value_str.isdigit():
            numeric_value = int(value_str)

    except Exception as e:
        logger.warning(f"Game callback parse error: {data} - {e}", exc_info=True)
        return await safe_answer_callback(event, "Parse Error.")

    db_updates = []; game_ended_flag = False; final_text = None; final_keyboard = None
    msg_needs_update = False; player_ids_to_inc_match = []
    winning_player_ids_for_credits = []

    async with games_lock:
        g = games.get(game_id)
        if not g:
            try: await client.edit_message(chat_id, message_id, "Game ended or not found.", buttons=None)
            except Exception as edit_err: logger.warning(f"Couldn't edit message {message_id} for ended game {game_id}: {edit_err}")
            return await safe_answer_callback(event, "This game is no longer active.", alert=True)

        g['last_activity_timestamp'] = time.monotonic()

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
            if action == "team_join" and state == STATE_TEAM_HOST_JOIN_WAIT and value_str == 'A':
                if user_id != host_id: return await safe_answer_callback(event, "Waiting for host.")
                udata = await asyncio.to_thread(get_user_data, user_id)
                if not udata: await safe_answer_callback(event,"DB Error",alert=True); return
                p_name_join = udata.get('full_name', player_name)
                g['teams']['A']['players'].append(user_id); g['teams']['A']['names'][user_id] = p_name_join
                if 'player_stats' not in g['teams']['A']: g['teams']['A']['player_stats'] = {}
                g['teams']['A']['player_stats'][user_id] = {'runs': 0,'balls_faced': 0,'wickets_taken': 0,'balls_bowled': 0,'is_out': False, 'fours':0, 'sixes':0}
                g['state'] = STATE_TEAM_WAITING
                temp_kb = create_join_team_keyboard(game_id, g, user_id); players_txt = format_team_players_for_ui(g)
                host_m = get_player_mention(host_id, g['teams']['A']['names'].get(host_id))
                temp_text = f"⚔️ {g['max_players_per_team']}v{g['max_players_per_team']} Team Cricket!\nHost: {host_m}\n\n{players_txt}\n\nWaiting for players to join or leave..."
                g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_kb; msg_needs_update=True; await safe_answer_callback(event, "Joined! Waiting for others...")

            elif action == "team_join" and state == STATE_TEAM_WAITING and value_str in ['A', 'B']:
                target_team = value_str; max_p = g['max_players_per_team']
                team_players = g['teams'][target_team]['players']; other_players = g['teams']['B' if target_team == 'A' else 'A']['players']

                if user_id == host_id and target_team == 'B': # user_id is the clicker here
                    return await safe_answer_callback(event, "Host must be in Team A.", alert=True)

                if user_id in team_players or user_id in other_players: return await safe_answer_callback(event, "You are already in a team!")
                if len(team_players) >= max_p: return await safe_answer_callback(event, f"Team {target_team} is full!")

                udata = await asyncio.to_thread(get_user_data, user_id)
                if not udata:
                    join_kb = create_join_team_keyboard(game_id, g, user_id) # Pass user_id (clicker) for correct "Joined" status if they are already in
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
                temp_keyboard = create_join_team_keyboard(game_id, g, user_id) # Pass user_id (clicker)
                g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True;
                await safe_answer_callback(event, f"You joined Team {target_team}!")

            elif action == "team_leave" and state == STATE_TEAM_WAITING and value_str in ['A', 'B']:
                team_to_leave = value_str
                if user_id not in g['teams'][team_to_leave]['players']:
                    return await safe_answer_callback(event, "You are not in that team to leave.")
                if user_id == host_id and team_to_leave == 'A' and len(g['teams']['A']['players']) == 1:
                    return await safe_answer_callback(event, "Host cannot leave Team A if they are the only member. /cancel if needed.", alert=True)
                # If host tries to leave B (should not be possible if they are not in B)
                if user_id == host_id and team_to_leave == 'B':
                    return await safe_answer_callback(event, "Host cannot be in Team B to leave it.", alert=True)


                g['teams'][team_to_leave]['players'].remove(user_id)
                if user_id in g['teams'][team_to_leave]['names']: del g['teams'][team_to_leave]['names'][user_id]
                if user_id in g['teams'][team_to_leave]['player_stats']: del g['teams'][team_to_leave]['player_stats'][user_id]
                players_txt_ui = format_team_players_for_ui(g)
                host_m_ui = get_player_mention(host_id, g['teams']['A']['names'].get(host_id, 'Host?'))
                temp_text_base = f"⚔️ {g['max_players_per_team']}v{g['max_players_per_team']} Team Cricket!\nHost: {host_m_ui}\n\n{players_txt_ui}\n\n"
                temp_text = temp_text_base + f"{player_mention} left Team {team_to_leave}. Waiting for players..."
                temp_keyboard = create_join_team_keyboard(game_id, g, user_id) # Pass user_id (clicker)
                g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True;
                await safe_answer_callback(event, f"You left Team {team_to_leave}.")

            elif action == "start_game" and state == STATE_TEAM_WAITING: # value_str is empty here
                if user_id != host_id: return await safe_answer_callback(event, "Only host can start.")
                a_players = g['teams']['A']['players']; b_players = g['teams']['B']['players']
                a_count = len(a_players); b_count = len(b_players)
                if not (a_count >= 1 and b_count >= 1): return await safe_answer_callback(event, "Need >=1 player per team.", alert=True)
                if host_id not in a_players: # Double check host is in team A
                    return await safe_answer_callback(event, "Host must be in Team A to start.", alert=True)

                g['actual_players_team_A'] = a_count; g['actual_players_team_B'] = b_count
                g['max_wickets_team_A'] = a_count; g['max_wickets_team_B'] = b_count
                g['state'] = STATE_TEAM_TOSS_CALL
                caller_id = g['teams']['A']['players'][0] # Captain of Team A (Host) calls
                caller_name = g['teams']['A']['names'].get(caller_id, f"Captain A")
                caller_mention = get_player_mention(caller_id, caller_name)
                players_txt = format_team_players_for_ui(g)
                temp_text = f"⚔️ {a_count}v{b_count} Team Cricket Started!\n{players_txt}\n\nCoin Toss: {caller_mention} (Team A Captain), call Heads or Tails:"
                temp_keyboard = create_team_toss_keyboard(game_id)
                g['last_text']=temp_text; final_text=temp_text; final_keyboard=temp_keyboard; msg_needs_update=True; await safe_answer_callback(event)

            # ... (rest of the team game logic from your original file, seems mostly fine)
            # Ensure inside "num" action for team game, when batter_id_current or bowler_id_current is None,
            # the create_player_selection_keyboard gets the correct game_data 'g'
            # It already does, so this should be fine.

            elif action == "team_toss" and state == STATE_TEAM_TOSS_CALL: # value_str is 'H' or 'T'
                caller_id = g['teams']['A']['players'][0]
                caller_name = g['teams']['A']['names'].get(caller_id)
                if user_id != caller_id: return await safe_answer_callback(event, f"Waiting for {html.escape(caller_name)} to call the toss.")
                choice_call=value_str; flip_result=random.choice(['H','T']); toss_heads=(flip_result=='H')
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

            elif action == "team_batorbowl" and state == STATE_TEAM_BAT_BOWL_CHOICE: # value_str is 'bat' or 'bowl'
                toss_winner_team_id=g.get('toss_winner_team');
                if not toss_winner_team_id: return await safe_answer_callback(event,"Internal Error: Toss winner not set.",alert=True)
                chooser_id = g['teams'][toss_winner_team_id]['players'][0]
                chooser_name = g['teams'][toss_winner_team_id]['names'].get(chooser_id)
                if user_id != chooser_id: return await safe_answer_callback(event, f"Waiting for {html.escape(chooser_name)} (Captain of Team {toss_winner_team_id}) to choose.")
                choice_made=value_str;
                if choice_made not in ['bat','bowl']: return await safe_answer_callback(event,"Invalid choice (bat/bowl).")
                bat_first_team_id = toss_winner_team_id if choice_made == 'bat' else ('B' if toss_winner_team_id == 'A' else 'A')
                bowl_first_team_id = 'B' if bat_first_team_id == 'A' else 'A'
                g['choice'] = choice_made
                g['first_batting_team_id'] = bat_first_team_id
                g.update({'current_batting_team': bat_first_team_id,
                          'current_bowling_team': bowl_first_team_id,
                          'state': STATE_TEAM_HOST_SELECT_BATTER,
                          'innings': 1,
                          'balls_bowled_this_inning': 0,
                          'balls_this_over': 0,
                          'current_batter_id': None,
                          'current_bowler_id': None
                          })
                for t_id_loop in ['A','B']: g['teams'][t_id_loop]['score']=0; g['teams'][t_id_loop]['wickets']=0
                host_m_ui=get_player_mention(host_id,g['teams']['A']['names'].get(host_id))
                sel_txt=(f"Team {toss_winner_team_id} chose to <b>{choice_made.upper()}</b>.\n"
                         f"Team {bat_first_team_id} will bat first.\n\n"
                         f"➡️ Host ({host_m_ui}), select 1st batter for Team {bat_first_team_id}:")
                sel_kb=create_player_selection_keyboard(game_id, bat_first_team_id, g, "sel_bat")
                g['last_text']=sel_txt; final_text=sel_txt; final_keyboard=sel_kb; msg_needs_update=True; await safe_answer_callback(event)

            elif action == "sel_bat" and state == STATE_TEAM_HOST_SELECT_BATTER: # value_str is player_id as string
                if user_id != host_id: return await safe_answer_callback(event, "Waiting for Host to select batter.")
                try: selected_batter_id = int(value_str)
                except (ValueError, TypeError): return await safe_answer_callback(event, "Invalid batter selection.", alert=True)
                bat_team_current=g['current_batting_team']
                if selected_batter_id not in g['teams'][bat_team_current]['players']:
                    return await safe_answer_callback(event,"Selected player is not in the current batting team.", alert=True)
                if g['teams'][bat_team_current]['player_stats'].get(selected_batter_id, {}).get('is_out', False):
                    return await safe_answer_callback(event, "This player is already out. Select another.", alert=True)
                g['current_batter_id'] = selected_batter_id
                g['state'] = STATE_TEAM_HOST_SELECT_BOWLER
                host_m_ui=get_player_mention(host_id,g['teams']['A']['names'].get(host_id))
                bowl_team_current=g['current_bowling_team']
                batter_selected_name = g['teams'][bat_team_current]['names'].get(selected_batter_id, f"Batter {selected_batter_id}")
                sel_txt=(f"Batter {html.escape(batter_selected_name)} (Team {bat_team_current}) selected.\n\n"
                         f"➡️ Host ({host_m_ui}), select 1st bowler for Team {bowl_team_current} (or next bowler if new over):")
                sel_kb=create_player_selection_keyboard(game_id, bowl_team_current, g, "sel_bowl")
                g['last_text']=sel_txt; final_text=sel_txt; final_keyboard=sel_kb; msg_needs_update=True; await safe_answer_callback(event)

            elif action == "sel_bowl" and state == STATE_TEAM_HOST_SELECT_BOWLER: # value_str is player_id as string
                if user_id != host_id: return await safe_answer_callback(event, "Waiting for Host to select bowler.")
                try: selected_bowler_id = int(value_str)
                except (ValueError, TypeError): return await safe_answer_callback(event, "Invalid bowler selection.", alert=True)
                bowl_team_current=g['current_bowling_team']
                if selected_bowler_id not in g['teams'][bowl_team_current]['players']:
                    return await safe_answer_callback(event,"Selected player is not in the current bowling team.", alert=True)
                g['current_bowler_id'] = selected_bowler_id
                g['state'] = STATE_TEAM_BATTING
                status_txt, batter_m_ui, bowler_m_ui = format_team_game_status(g)
                play_txt = f"Alright, let's play!\n\n{status_txt}\n\n➡️ {batter_m_ui}, select your shot (1-6):"
                play_kb = create_standard_keyboard(game_id)
                g['last_text']=play_txt; final_text=play_txt; final_keyboard=play_kb; msg_needs_update=True; await safe_answer_callback(event)

            elif action == "num" and state in [STATE_TEAM_BATTING, STATE_TEAM_BOWLING_WAIT]: # numeric_value is set
                if numeric_value is None or not (1 <= numeric_value <= 6): return await safe_answer_callback(event, "Invalid input (1-6).", alert=True)
                batter_id_current=g['current_batter_id']; bowler_id_current=g['current_bowler_id']
                bat_team_id_current=g['current_batting_team']; bowl_team_id_current=g['current_bowling_team']
                if not batter_id_current or not bowler_id_current:
                    logger.error(f"G:{game_id} Missing batter/bowler ID. B:{batter_id_current}, BWL:{bowler_id_current}, State:{state}")
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
                    if bat_num_choice is None:
                        g['state']=STATE_TEAM_BATTING;
                        btr_m_ui = get_player_mention(batter_id_current,batter_name_disp); status_txt_ui,_,_ = format_team_game_status(g)
                        err_txt = f"⚠️ Error: Batter's choice was lost.\n{status_txt_ui}\n➡️ {btr_m_ui}, please play your shot again:";
                        err_kb = create_standard_keyboard(game_id);
                        g['last_text']=err_txt; final_text=err_txt; final_keyboard=err_kb; msg_needs_update=True;
                        await safe_answer_callback(event,"Error! Batter needs to play again.",alert=True); return
                    bowl_num_delivery = numeric_value
                    g['balls_bowled_this_inning'] += 1
                    g['balls_this_over'] = g.get('balls_this_over', 0) + 1
                    current_inning_stats_obj['runs_this_over_list'].append(0)
                    batter_stats['balls_faced'] += 1
                    bowler_stats['balls_bowled'] += 1
                    innings_ended_this_ball_flag = False
                    batter_m_disp = get_player_mention(batter_id_current, batter_name_disp); bowler_m_disp = get_player_mention(bowler_id_current, bowler_name_disp)
                    result_prefix_text = f"{batter_m_disp} (chose <code>{bat_num_choice}</code>) | {bowler_m_disp} (bowled <code>{bowl_num_delivery}</code>)\n\n"
                    result_txt_display = ""
                    if bat_num_choice == bowl_num_delivery:
                        g['teams'][bat_team_id_current]['wickets'] += 1; batter_stats['is_out'] = True; bowler_stats['wickets_taken'] += 1; g['last_out_player_id'] = batter_id_current
                        wickets_fallen = g['teams'][bat_team_id_current]['wickets']; max_wickets_for_team = g.get(f'max_wickets_team_{bat_team_id_current}')
                        result_txt_display = result_prefix_text + f"💥 <b>OUT!</b> ({wickets_fallen}/{max_wickets_for_team} Wickets for Team {bat_team_id_current})\n"
                        db_updates.append({'type':'wicket','user_id':bowler_id_current})
                        current_inning_stats_obj['dots'] += 1; g['match_stats']['total_dots'] +=1
                        current_inning_stats_obj['runs_this_over_list'][-1] = 0
                        if wickets_fallen >= max_wickets_for_team:
                            innings_ended_this_ball_flag = True; result_txt_display += "Innings End! (All out for Team " + bat_team_id_current + ")\n"
                    else:
                        runs_scored_ball = bat_num_choice; g['teams'][bat_team_id_current]['score']+=runs_scored_ball; batter_stats['runs']+=runs_scored_ball
                        result_txt_display = result_prefix_text + f"🏏 <b>{runs_scored_ball}</b> runs scored! Team {bat_team_id_current} score: {g['teams'][bat_team_id_current]['score']}/{g['teams'][bat_team_id_current]['wickets']}\n"
                        db_updates.append({'type':'runs','user_id':batter_id_current,'value':runs_scored_ball})
                        current_inning_stats_obj['runs_this_over_list'][-1] = runs_scored_ball
                        if runs_scored_ball == 4: current_inning_stats_obj['fours'] += 1; batter_stats['fours'] +=1; g['match_stats']['total_fours'] +=1
                        elif runs_scored_ball == 6: current_inning_stats_obj['sixes'] += 1; batter_stats['sixes'] +=1; g['match_stats']['total_sixes'] +=1
                        elif runs_scored_ball == 0: current_inning_stats_obj['dots'] += 1; g['match_stats']['total_dots'] +=1 # Should be if runs_scored_ball is 0 and not out
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
                    if innings_ended_this_ball_flag:
                        g['balls_bowled_inning' + str(g['innings'])] = g['balls_bowled_this_inning']
                        if g['innings'] == 1:
                            g['target'] = g['teams'][bat_team_id_current]['score'] + 1
                            result_txt_display += f"Target for Team {bowl_team_id_current} to chase: <b>{g['target']}</b>"
                            g['current_batting_team'], g['current_bowling_team'] = bowl_team_id_current, bat_team_id_current
                            g['innings'] = 2; g['balls_bowled_this_inning'] = 0; g['balls_this_over'] = 0; g['batter_choice'] = None
                            g['current_batter_id'] = None; g['current_bowler_id'] = None
                            g['state'] = STATE_TEAM_HOST_SELECT_BATTER
                        else:
                            game_ended_flag = True
                            if not result_txt_display.endswith("Game Over!\n"): result_txt_display += "\n<b>Game Over!</b>"
                            g['state'] = STATE_TEAM_ENDED
                            player_ids_to_inc_match.extend(g['teams']['A']['players'] + g['teams']['B']['players'])
                            if not winning_player_ids_for_credits: # Check if not already set (e.g. target chased)
                                inn1_bat_team_final_score = g['teams'][g['first_batting_team_id']]['score']
                                inn2_bat_team_id = 'B' if g['first_batting_team_id'] == 'A' else 'A'
                                inn2_bat_team_final_score = g['teams'][inn2_bat_team_id]['score'] # This should be the score of the team that just finished batting (current_bat_team_id_current)
                                if g['teams'][g['first_batting_team_id']]['score'] > g['teams'][bat_team_id_current]['score']:
                                    winning_player_ids_for_credits.extend(g['teams'][g['first_batting_team_id']]['players'])
                                elif g['teams'][bat_team_id_current]['score'] > g['teams'][g['first_batting_team_id']]['score'] :
                                    winning_player_ids_for_credits.extend(g['teams'][bat_team_id_current]['players'])
                                # Tie case - no credits awarded for win

                    else:
                        g['batter_choice'] = None
                        if bat_num_choice == bowl_num_delivery: # Wicket fell, but innings not over
                            g['current_batter_id'] = None
                            g['state'] = STATE_TEAM_HOST_SELECT_BATTER
                        elif over_completed_this_ball_team: # Over completed, innings not over
                            g['current_batter_id'] = None; g['current_bowler_id'] = None
                            g['state'] = STATE_TEAM_HOST_SELECT_BATTER # Host selects new batter, then new bowler
                        else: # Runs scored, no wicket, over not complete
                            g['state'] = STATE_TEAM_BATTING
                    if game_ended_flag:
                        final_text = format_final_scorecard(g)
                        final_keyboard = None
                    elif g['state'] == STATE_TEAM_HOST_SELECT_BATTER:
                        host_m_ui=get_player_mention(host_id,g['teams']['A']['names'].get(host_id))
                        next_bat_team_id_sel = g['current_batting_team']
                        action_reason = "New innings."
                        if g['last_out_player_id'] and not innings_ended_this_ball_flag and bat_num_choice == bowl_num_delivery : action_reason = "Wicket fell."
                        elif over_completed_this_ball_team and not innings_ended_this_ball_flag: action_reason = "Over completed."
                        final_text = f"{result_txt_display}\n{action_reason}\n\n➡️ Host ({host_m_ui}), select batter for Team {next_bat_team_id_sel}:"
                        final_keyboard=create_player_selection_keyboard(game_id, next_bat_team_id_sel, g, "sel_bat")
                    elif g['state'] == STATE_TEAM_BATTING :
                        status_txt_ui, next_batter_m_ui, _ = format_team_game_status(g)
                        final_text = f"{result_txt_display}\n{status_txt_ui}\n\n➡️ {next_batter_m_ui}, select shot (1-6):"
                        final_keyboard=create_standard_keyboard(game_id)
                    else: # Should not happen if logic is correct, but as a fallback
                        final_text = f"{result_txt_display}\n\nInternal State Error. Current state: {g['state']}"
                        final_keyboard = None
                        if g['state'] != STATE_TEAM_ENDED:
                            g['state'] = STATE_TEAM_ENDED; game_ended_flag = True
                            player_ids_to_inc_match.extend(g['teams']['A']['players']+g['teams']['B']['players'])
                            final_text += "\n" + format_final_scorecard(g)
                    g['last_text']=final_text; msg_needs_update=True; await safe_answer_callback(event)

        elif game_type == '1v1':
            p1_obj = g['player1']
            p2_obj = g['player2']
            if action == "join_1v1" and state == STATE_WAITING: # value_str is empty here
                if user_id == p1_id: return await safe_answer_callback(event, "You started this game.")
                if p2_obj is not None: return await safe_answer_callback(event, "This game is already full (2 players).")
                udata=await asyncio.to_thread(get_user_data, user_id)
                if not udata:
                    join_kb_1v1=client.build_reply_markup([[Button.inline("Join Game",data=f"join_1v1::{game_id}")]])
                    err_text=g['last_text']+f"\n\n{player_mention} please /start me in DM first to use all features."
                    final_text=err_text; final_keyboard=join_kb_1v1; msg_needs_update=True; await safe_answer_callback(event); return
                p2_name_join=udata.get('full_name', player_name)
                g['player2']={'id':user_id,'name':p2_name_join,'score':0,'balls_faced':0,'balls_bowled':0,'wickets_taken':0, 'fours':0, 'sixes':0}
                p2_obj = g['player2']
                g['state']=STATE_TOSS
                p1_m_ui=get_player_mention(p1_id,g['player1']['name']); p2_m_ui=get_player_mention(user_id,p2_name_join)
                toss_text_1v1=f"⚔️ 1v1 Game Ready!\n{p1_m_ui} vs {p2_m_ui}\n\nTime for the Toss!\n➡️ {p1_m_ui} (P1), please call Heads or Tails:"
                toss_kb_1v1=[[Button.inline("Heads",data=f"toss:H:{game_id}"),Button.inline("Tails",data=f"toss:T:{game_id}")]]
                g['last_text']=toss_text_1v1; final_text=toss_text_1v1; final_keyboard=toss_kb_1v1; msg_needs_update=True; await safe_answer_callback(event, "You've joined the game!")
            elif action == "toss" and state == STATE_TOSS: # value_str is 'H' or 'T'
                if not p1_obj or not p2_obj: return await safe_answer_callback(event, "Error: Player data missing for toss.", alert=True)
                if user_id != p1_id: return await safe_answer_callback(event, f"Waiting for {html.escape(p1_obj['name'])} (P1) to call the toss.")
                choice_call_1v1=value_str; flip_result_1v1=random.choice(['H','T']); toss_heads_1v1=(flip_result_1v1=='H')
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
            elif action == "batorbowl" and state == STATE_BAT_BOWL: # value_str is 'bat' or 'bowl'
                toss_winner_id_val=g.get('toss_winner_id');
                if not p1_obj or not p2_obj: return await safe_answer_callback(event, "Error: Player data missing for bat/bowl choice.", alert=True)
                if user_id != toss_winner_id_val:
                    winner_name_for_wait = p1_obj['name'] if toss_winner_id_val == p1_obj['id'] else p2_obj['name']
                    return await safe_answer_callback(event,f"Waiting for {html.escape(winner_name_for_wait)} (toss winner) to choose.")
                choice_made_1v1=value_str;
                if choice_made_1v1 not in ['bat','bowl']: return await safe_answer_callback(event,"Invalid choice (must be bat or bowl).")
                bat_first_player_id = toss_winner_id_val if choice_made_1v1=='bat' else (p2_obj['id'] if toss_winner_id_val==p1_obj['id'] else p1_obj['id'])
                bowl_first_player_id = p2_obj['id'] if bat_first_player_id==p1_obj['id'] else p1_obj['id']
                g['choice']=choice_made_1v1
                g['first_batter_id'] = bat_first_player_id
                g.update({'current_batter_id':bat_first_player_id,
                          'current_bowler_id':bowl_first_player_id,
                          'state':STATE_P1_BAT if bat_first_player_id==p1_obj['id'] else STATE_P2_BAT,
                          'innings':1, 'balls_bowled_this_inning':0, 'balls_this_over': 0})
                current_batter_obj_disp = p1_obj if g['current_batter_id'] == p1_obj['id'] else p2_obj
                toss_winner_name_disp_ui = p1_obj['name'] if toss_winner_id_val == p1_obj['id'] else p2_obj['name']
                status_txt_1v1,batter_m_1v1,_ = format_1v1_game_status(g)
                play_txt_1v1=(f"{get_player_mention(toss_winner_id_val, toss_winner_name_disp_ui)} chose to <b>{choice_made_1v1.upper()}</b>.\n"
                              f"{get_player_mention(bat_first_player_id, current_batter_obj_disp['name'])} will bat first.\n\n"
                              f"{status_txt_1v1}\n\n➡️ {batter_m_1v1}, select your shot (1-6):")
                play_kb_1v1=create_standard_keyboard(game_id);
                g['last_text']=play_txt_1v1; final_text=play_txt_1v1; final_keyboard=play_kb_1v1; msg_needs_update=True; await safe_answer_callback(event)
            elif action == "num" and state in [STATE_P1_BAT,STATE_P1_BOWL_WAIT,STATE_P2_BAT,STATE_P2_BOWL_WAIT]: # numeric_value set
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
                    g['state']=STATE_P1_BOWL_WAIT if is_p1_bat_state_now else STATE_P2_BOWL_WAIT
                    status_txt_bat,_,bowler_m_bat=format_1v1_game_status(g,batter_played=True)
                    temp_text_bat=f"{status_txt_bat}\n\n➡️ {bowler_m_bat}, select your delivery (1-6):";
                    temp_kb_bat=create_standard_keyboard(game_id);
                    g['last_text']=temp_text_bat; final_text=temp_text_bat; final_keyboard=temp_kb_bat; msg_needs_update=True;
                    await safe_answer_callback(event,f"You played {numeric_value}. Waiting for bowler...")
                elif is_bowl_state_1v1:
                    if user_id!=bowler_id_1v1: return await safe_answer_callback(event,f"Waiting for {html.escape(bowler_name_1v1)} to bowl.")
                    bat_num_choice_1v1=g.get('batter_choice');
                    if bat_num_choice_1v1 is None:
                        g['state']=STATE_P1_BAT if state == STATE_P1_BOWL_WAIT else STATE_P2_BAT
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
                    if bat_num_choice_1v1 == bowl_num_delivery_1v1:
                        innings_ended_1v1_flag=True;
                        bowler_obj_1v1['wickets_taken'] +=1
                        result_txt_1v1 = result_prefix_1v1 + "💥 <b>OUT! Innings End!</b>\n"
                        db_updates.append({'type':'wicket','user_id':bowler_id_1v1})
                        current_inning_stats_1v1['dots'] += 1; g['match_stats']['total_dots'] +=1
                        current_inning_stats_1v1['runs_this_over_list'][-1] = 0
                    else:
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
                    if innings_ended_1v1_flag and not game_ended_flag:
                        g['balls_bowled_inning' + str(g['innings'])] = g['balls_bowled_this_inning']
                        if g['innings'] == 1:
                            g['target'] = batter_obj_1v1['score']+1; result_txt_1v1+=f"Target for {html.escape(bowler_obj_1v1['name'])} to chase: <b>{g['target']}</b>";
                            g['current_batter_id'],g['current_bowler_id']=bowler_id_1v1,batter_id_1v1
                            g['innings']=2; g['balls_bowled_this_inning']=0; g['balls_this_over'] = 0
                            g['state']=STATE_P1_BAT if bowler_id_1v1==p1_obj['id'] else STATE_P2_BAT;
                            g['batter_choice']=None
                        else:
                            game_ended_flag=True;
                            if not result_txt_1v1.endswith("Game Over!\n"): result_txt_1v1+="\n<b>Game Over!</b>"
                            g['state']=STATE_1V1_ENDED; player_ids_to_inc_match.extend([p1_obj['id'],p2_obj['id']])
                            if not winning_player_ids_for_credits : # Check if not already set (e.g. target chased)
                                # If 2nd innings ended not by chasing, defender (1st innings batter) wins
                                if batter_obj_1v1['score'] < g['target'] -1 : # Batter is current 2nd innings batter
                                    winning_player_ids_for_credits.append(bowler_id_1v1) # Bowler (1st innings batter) wins
                                # Tie is handled by result string, no win credits for tie here
                    elif not game_ended_flag:
                        g['batter_choice']=None
                        g['state']=STATE_P1_BAT if state == STATE_P1_BOWL_WAIT else STATE_P2_BAT
                    if game_ended_flag:
                        final_text = format_final_scorecard(g)
                        final_keyboard = None
                    else:
                        status_txt_next,next_batter_m_next,_ = format_1v1_game_status(g);
                        final_text=f"{result_txt_1v1}\n{status_txt_next}\n\n➡️ {next_batter_m_next}, select your shot (1-6):";
                        final_keyboard=create_standard_keyboard(game_id)
                    g['last_text']=final_text; msg_needs_update=True; await safe_answer_callback(event)
            else:
                 logger.warning(f"Unhandled 1v1 callback: Action='{action}', State='{state}', GID='{game_id}'")
                 await safe_answer_callback(event, "Internal: Unhandled 1v1 action.", alert=True)
        else:
            logger.error(f"Unknown game_type '{game_type}' in callback for GID='{game_id}'")
            await safe_answer_callback(event, "Internal Error: Unknown game type.", alert=True)

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

def format_team_game_status(game_data, batter_played=False):
     g = game_data
     bat_team = g.get('current_batting_team'); bowl_team = g.get('current_bowling_team')
     batter_id = g.get('current_batter_id'); bowler_id = g.get('current_bowler_id')
     innings = g.get('innings', 1); balls = g.get('balls_bowled_this_inning', 0);
     max_overs_display = g.get('overs_per_innings', DEFAULT_OVERS)
     overs_str = format_overs(balls); max_overs_str_disp = str(max_overs_display)
     state = g.get('state')
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
    status += f"🏏 Batter: {batter_m} [<code>{score}/0</code> Wkts]"
    if batter_played: status += " (Played shot)"
    status += f"\n🧤 Bowler: {bowler_m}\n"
    return status, batter_m, bowler_m

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
        team_a_captain_name = "Team A"
        if g['teams']['A']['names'] and team_a_players:
            team_a_captain_name = g['teams']['A']['names'].get(team_a_players[0], "Team A Captain")
        team_b_captain_name = "Team B"
        if g['teams']['B']['names'] and team_b_players:
            team_b_captain_name = g['teams']['B']['names'].get(team_b_players[0], "Team B Captain")
        first_bat_id = g.get('first_batting_team_id')
        if not first_bat_id:
            first_bat_id = 'A' # Default if somehow not set, though should be
        second_bat_id = 'B' if first_bat_id == 'A' else 'A'
        first_bat_display_name = team_a_captain_name if first_bat_id == 'A' else team_b_captain_name
        second_bat_display_name = team_a_captain_name if second_bat_id == 'A' else team_b_captain_name
        scorecard_text += f"🔵 {html.escape(first_bat_display_name)} (Batting First)\n"
        scorecard_text += f"🔴 {html.escape(second_bat_display_name)} (Bowling First)\n\n"
        scorecard_text += "📝 SCORECARD\n"
        for i_num in [1, 2]:
            batting_team_id_current_loop = first_bat_id if i_num == 1 else second_bat_id
            team_data = g['teams'].get(batting_team_id_current_loop, {'score': 0, 'wickets': 0, 'players': [], 'names': {}})
            inning_s = g['inning_stats'].get(i_num, {'fours': 0, 'sixes': 0})
            balls_bowled_inn = g.get(f'balls_bowled_inning{i_num}', 0)
            overs_played_str = format_overs(balls_bowled_inn)
            current_team_score = team_data.get('score',0)
            current_team_wickets = team_data.get('wickets',0)
            run_rate = (current_team_score / (balls_bowled_inn / 6)) if balls_bowled_inn > 0 else 0.00
            current_team_actual_players = team_data.get('players', [])
            current_team_names_dict = team_data.get('names',{})
            captain_or_team_name_display = current_team_names_dict.get(current_team_actual_players[0], batting_team_id_current_loop) if current_team_actual_players and current_team_names_dict else batting_team_id_current_loop

            scorecard_text += f"┌─ INNINGS {i_num} ({html.escape(str(captain_or_team_name_display))})\n" # Ensure name is string
            scorecard_text += f"│ {current_team_score}/{current_team_wickets} ({overs_played_str})\n"
            scorecard_text += f"│ 📈 RR: {run_rate:.2f}\n"
            fours_inn = inning_s.get('fours',0); sixes_inn = inning_s.get('sixes',0)
            scorecard_text += f"│ 🎯 4s: {fours_inn} | 💥 6s: {sixes_inn}\n"
            scorecard_text += f"└─ Total Runs: {current_team_score}\n\n"
        winner_text_result = determine_team_winner(g, for_scorecard=True)
    elif game_type == '1v1':
        p1 = g.get('player1'); p2 = g.get('player2')
        if not p1 or not p2:
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
                wickets_display_1v1 = "0"
                if i_num == 1:
                    if balls_bowled_inn < g['max_balls']: wickets_display_1v1 = "1" # Innings ended due to wicket
                elif i_num == 2:
                    # If 2nd innings ended before max balls AND target not chased
                    if balls_bowled_inn < g['max_balls'] and current_score_loop < g.get('target', float('inf')):
                         wickets_display_1v1 = "1"
                scorecard_text += f"│ {current_score_loop}/{wickets_display_1v1} ({overs_played_str})\n"
                scorecard_text += f"│ 📈 RR: {run_rate:.2f}\n"
                fours_inn = current_batter_obj_loop.get('fours',0); sixes_inn = current_batter_obj_loop.get('sixes',0)
                scorecard_text += f"│ 🎯 4s: {fours_inn} | 💥 6s: {sixes_inn}\n"
                scorecard_text += f"└─ Total Runs: {current_score_loop}\n\n"
            winner_text_result = determine_1v1_winner(g, for_scorecard=True)
    ms = g.get('match_stats', {})
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
        target = g.get('target') # Target is score of first batting team + 1

        if not first_bat_team_id or target is None:
            # This block might be hit if game ended prematurely or data is corrupt
            logger.warning(f"Winner determination fallback for G:{g.get('game_id','?')}. A:{team_a_score}, B:{team_b_score}")
            if team_a_score > team_b_score: return f"🎉 Team A wins by {team_a_score - team_b_score} runs! 🏆"
            elif team_b_score > team_a_score: return f"🎉 Team B wins by {team_b_score - team_a_score} runs! 🏆"
            elif team_a_score == team_b_score and (g.get('balls_bowled_inning1',0) > 0 or g.get('balls_bowled_inning2',0) > 0) : return "🤝 It's a TIE!"
            else: return "Match outcome unclear (possibly ended before completion)."

        inn1_bat_team_obj = g['teams'][first_bat_team_id]
        inn2_bat_team_id = 'B' if first_bat_team_id == 'A' else 'A'
        inn2_bat_team_obj = g['teams'][inn2_bat_team_id]

        inn1_bat_score = inn1_bat_team_obj['score']
        inn2_bat_score = inn2_bat_team_obj['score']

        inn1_captain_id = inn1_bat_team_obj['players'][0] if inn1_bat_team_obj['players'] else None
        inn1_captain_name = inn1_bat_team_obj['names'].get(inn1_captain_id, f"Team {first_bat_team_id}") if inn1_captain_id else f"Team {first_bat_team_id}"

        inn2_captain_id = inn2_bat_team_obj['players'][0] if inn2_bat_team_obj['players'] else None
        inn2_captain_name = inn2_bat_team_obj['names'].get(inn2_captain_id, f"Team {inn2_bat_team_id}") if inn2_captain_id else f"Team {inn2_bat_team_id}"


        if inn2_bat_score >= target: # Team batting second chased target
            wickets_left = g['actual_players_team_' + inn2_bat_team_id] - inn2_bat_team_obj['wickets']
            return f"🎉 Team {html.escape(inn2_captain_name)} wins by {wickets_left} wicket{'s' if wickets_left != 1 else ''}! 🏆"
        elif inn2_bat_score == target - 1: # Scores are level
            return f"🤝 It's a TIE!"
        else: # Team batting first defended the total
            runs_margin = (target - 1) - inn2_bat_score # target-1 is inn1_score
            return f"🎉 Team {html.escape(inn1_captain_name)} wins by {runs_margin} run{'s' if runs_margin != 1 else ''}! 🏆"
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
        if inn2_score >= target:
            winner_name = inn2_batter.get('name')
            return f"🎉 {html.escape(winner_name)} wins by chasing the target! 🏆"
        elif inn2_score == target - 1: # Scores level
            return f"🤝 It's a TIE!"
        else: # First batter defended
            winner_name = inn1_batter.get('name')
            runs_margin = (target - 1) - inn2_score # target-1 is inn1_score
            return f"🎉 {html.escape(winner_name)} wins by {runs_margin} run{'s' if runs_margin != 1 else ''}! 🏆"
    except Exception as e:
        logger.error(f"Error in determine_1v1_winner G:{g.get('game_id','?')}: {e}", exc_info=True)
        return "Error determining winner."

# --- Admin Commands ---
@client.on(events.NewMessage(pattern='/user_count', from_users=xmods))
async def handle_user_count(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    try: count = await asyncio.to_thread(users_collection.count_documents, {}); await safe_reply(event, f"👥 Users: <b>{count + 456}</b>")
    except Exception as e: await safe_reply(event, f"Error: {e}"); logger.error(f"Err count users: {e}", exc_info=True)

@client.on(events.NewMessage(pattern=r'/set_runs(?: (\d+))? (\d+)', from_users=xmods))
async def handle_set_runs(event):
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
async def handle_set_wickets(event):
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

@client.on(events.NewMessage(pattern=r'/set_credits(?: (\d+))? (-?\d+)', from_users=xmods))
async def handle_set_credits(event):
    if users_collection is None: return await safe_reply(event, "⚠️ DB unavailable.")
    target_user_id = None; credits_to_set = 0
    try:
        parts = event.pattern_match.groups()
        if parts[0]: target_user_id = int(parts[0])
        elif event.is_reply: reply_msg = await event.get_reply_message(); target_user_id = reply_msg.sender_id if reply_msg else None
        else: return await safe_reply(event, "Usage: <code>/set_credits [user_id] <amount></code> or reply.")
        credits_to_set = int(parts[1])
    except (ValueError, TypeError, IndexError): return await safe_reply(event, "Invalid format or value.")
    if not target_user_id: return await safe_reply(event, "Could not determine target user.")
    target_id_str = str(target_user_id)
    def _set_db_credits(uid, credits_val):
        if users_collection is None: return False, "DB Unavailable"
        try:
            user_doc = users_collection.find_one({"_id": uid})
            if not user_doc:
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

async def get_target_id_and_text(event, command_name):
    target_user_id = None; text_content = None
    command_parts = event.text.split(maxsplit=1)
    args_part = command_parts[1] if len(command_parts) > 1 else ""
    if event.is_reply:
        reply_msg = await event.get_reply_message()
        if not (reply_msg and reply_msg.sender_id):
            await safe_reply(event, "Invalid reply target for command.")
            return None, None
        target_user_id = reply_msg.sender_id
        text_content = args_part.strip()
    else:
        arg_split = args_part.split(maxsplit=1)
        if not arg_split or not arg_split[0].isdigit():
            await safe_reply(event, f"Usage: <code>/{command_name} [user_id] &lt;text&gt;</code> or reply <code>/{command_name} &lt;text&gt;</code>")
            return None, None
        target_user_id = int(arg_split[0])
        if len(arg_split) > 1:
            text_content = arg_split[1].strip()
    if not text_content:
        await safe_reply(event, f"Please provide the text/achievement to add/remove for user {target_user_id}.")
        return target_user_id, None
    return target_user_id, text_content

@client.on(events.NewMessage(pattern=r'/achieve(?: (\d+))?( .*)?', from_users=xmods))
async def handle_achieve(event):
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
             return True, res.modified_count > 0
        except Exception as e: logger.error(f"DB add achievement err {uid}: {e}"); return False, str(e)
    success, modified_status = await asyncio.to_thread(_add_achieve, target_id_str, achievement_text)
    user_mention_text = f"User <code>{target_user_id}</code>"
    try:
        user_entity_ach = await client.get_entity(target_user_id)
        user_mention_text = get_player_mention(target_user_id, get_display_name(user_entity_ach) if user_entity_ach else f"User {target_user_id}")
    except: pass
    safe_achieve_text_disp = html.escape(achievement_text)
    if success:
        if modified_status:
            await safe_reply(event, f"✅ Added achievement '<code>{safe_achieve_text_disp}</code>' to {user_mention_text}.")
        else:
            await safe_reply(event, f"☑️ Achievement '<code>{safe_achieve_text_disp}</code>' likely already exists for {user_mention_text} (or no change made).")
    else:
        err_reason = modified_status if isinstance(modified_status, str) else "Unknown DB Error or User Not Found"
        await safe_reply(event, f"⚠️ Failed to add achievement for {user_mention_text}. Reason: {err_reason}")

@client.on(events.NewMessage(pattern=r'/remove_achieve(?: (\d+))?( .*)?', from_users=xmods))
async def handle_remove_achieve(event):
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
             return True, res_rem.modified_count > 0
        except Exception as e: logger.error(f"DB remove achievement err {uid}: {e}"); return False, str(e)
    success_rem, removed_status = await asyncio.to_thread(_remove_achieve_db, target_id_str, achievement_text)
    user_mention_text_rem = f"User <code>{target_user_id}</code>"
    try:
        user_entity_rem = await client.get_entity(target_user_id)
        user_mention_text_rem = get_player_mention(target_user_id, get_display_name(user_entity_rem) if user_entity_rem else f"User {target_user_id}")
    except: pass
    safe_achieve_text_rem_disp = html.escape(achievement_text)
    if success_rem:
        if removed_status:
            await safe_reply(event, f"✅ Removed achievement '<code>{safe_achieve_text_rem_disp}</code>' from {user_mention_text_rem}.")
        else:
            await safe_reply(event, f"☑️ Achievement '<code>{safe_achieve_text_rem_disp}</code>' not found for {user_mention_text_rem} (or no change made).")
    else:
        err_reason_rem = removed_status if isinstance(removed_status, str) else "Unknown DB Error or User Not Found"
        await safe_reply(event, f"⚠️ Failed to remove achievement for {user_mention_text_rem}. Reason: {err_reason_rem}")

# --- Broadcast Functions ---
pending_broadcasts_telethon = {}
def format_username_html_telethon(user_doc):
    if not user_doc: return "N/A"
    user_id = user_doc.get('_id')
    name = html.escape(user_doc.get("full_name", f"User {user_id}"))
    return f'<a href="tg://user?id={user_id}">{name}</a>'
def is_mod_telethon(user_id):
    return user_id in xmods
@client.on(events.NewMessage(pattern=r'/broad(?: |$)(.*)', from_users=xmods))
async def broadcast_request_telethon(event):
    if not is_mod_telethon(event.sender_id):
        await safe_reply(event, "❌ You are not authorized to use this command.")
        return
    admin_user_id = event.sender_id
    broadcast_type = None
    content = None
    fwd_chat_id = None
    fwd_message_id = None
    replied_msg = await event.get_reply_message()
    if replied_msg:
        content_to_store = replied_msg.id
        fwd_chat_id = event.chat_id
        broadcast_type = "forward"
    elif event.pattern_match.group(1) and event.pattern_match.group(1).strip():
        content_to_store = event.pattern_match.group(1).strip()
        broadcast_type = "text"
    else:
        await safe_reply(event, "❌ Please reply to a message or type a message after /broad to broadcast.")
        return
    try:
        if users_collection is None:
            await safe_reply(event, "Database not available. Cannot get user count.")
            return
        target_count = await asyncio.to_thread(users_collection.count_documents, {"_id": {"$exists": True}})
        if target_count == 0:
            await safe_reply(event, "⚠️ No target users found in the database. Cannot broadcast.")
            return
    except Exception as e:
        logger.error(f"Error counting users for broadcast: {e}")
        await safe_reply(event, "Error fetching user count for broadcast.")
        return
    confirmation_key = f"bc_{admin_user_id}_{int(time.time())}"
    pending_broadcasts_telethon[confirmation_key] = {
        'type': broadcast_type,
        'content': content_to_store,
        'fwd_chat_id_if_forward': fwd_chat_id,
        'target_count': target_count,
        'requester_id': admin_user_id
    }
    schedule_pending_broadcast_cleanup_telethon(confirmation_key, 600)
    markup_buttons = [
        [Button.inline(f"✅ Yes, Send to {target_count}", data=f"confirm_bc_:{confirmation_key}:broadcast")], # Adjusted to include colon for parsing
        [Button.inline("❌ Cancel", data=f"cancel_bc_:{confirmation_key}:broadcast")]
    ]
    broadcast_preview = ""
    if broadcast_type == "text":
        preview_text = content_to_store[:100] + "..." if len(content_to_store) > 100 else content_to_store
        broadcast_preview = f"Message:\n<pre>{html.escape(preview_text)}</pre>"
    elif broadcast_type == "forward":
        broadcast_preview = f"Forwarded message (ID: {content_to_store} from this chat)"
    await safe_reply(
        event,
        f"❓ **Confirm Broadcast**\n\n"
        f"Type: {broadcast_type.capitalize()}\n"
        f"Target Users: {target_count}\n\n"
        f"{broadcast_preview}\n\n"
        f"Are you sure you want to send this broadcast?",
        buttons=client.build_reply_markup(markup_buttons)
    )
def schedule_pending_broadcast_cleanup_telethon(key, timeout):
    async def cleanup_task():
        await asyncio.sleep(timeout)
        if key in pending_broadcasts_telethon:
            logger.info(f"Cleaning up expired Telethon pending broadcast: {key}")
            del pending_broadcasts_telethon[key]
    asyncio.create_task(cleanup_task())
async def handle_broadcast_confirmation_telethon(event, callback_data_full_str):
    admin_user_id = event.sender_id
    try:
        # callback_data_full_str is like "confirm_bc_:KEY:broadcast" or "cancel_bc_:KEY:broadcast"
        parts = callback_data_full_str.split(":", 2)
        action_prefix_cb = parts[0] # e.g. "confirm_bc_"
        confirmation_key_from_cb = parts[1] # e.g. "bc_ADMINID_TIMESTAMP"
    except Exception as e:
        logger.error(f"Error parsing broadcast confirmation callback data (Telethon): {callback_data_full_str} - {e}")
        await safe_answer_callback(event, "Error: Invalid data format")
        return
    logger.info(f"Broadcast Callback: Action Prefix='{action_prefix_cb}', Extracted Key='{confirmation_key_from_cb}'")
    pending_data = pending_broadcasts_telethon.get(confirmation_key_from_cb)
    if not pending_data:
        logger.warning(f"Pending broadcast data not found for key: '{confirmation_key_from_cb}'. Current pending keys: {list(pending_broadcasts_telethon.keys())}")
        await safe_answer_callback(event, "⚠️ This broadcast request has expired or is invalid.", alert=True)
        try: await client.edit_message(event.chat_id, event.message_id, buttons=None)
        except Exception: pass
        return
    if admin_user_id != pending_data['requester_id']:
        await safe_answer_callback(event, "This confirmation is not for you.")
        return
    if action_prefix_cb == "cancel_bc_":
        if confirmation_key_from_cb in pending_broadcasts_telethon:
             del pending_broadcasts_telethon[confirmation_key_from_cb]
        await safe_answer_callback(event, "❌ Broadcast cancelled.")
        try:
            await client.edit_message(event.chat_id, event.message_id, text="❌ Broadcast cancelled by user.", buttons=None)
        except Exception: pass
        return
    if action_prefix_cb == "confirm_bc_":
        if confirmation_key_from_cb in pending_broadcasts_telethon:
            pass
        else:
            await safe_answer_callback(event, "⚠️ Broadcast request already processed or expired (race?).", alert=True)
            return
        pending_data_to_process = pending_broadcasts_telethon.pop(confirmation_key_from_cb)
        try:
            await client.edit_message(event.chat_id, event.message_id,
                                      text=f"⏳ Initializing broadcast to {pending_data_to_process['target_count']} users...",
                                      buttons=None)
        except Exception as edit_err:
            logger.warning(f"Could not edit broadcast confirmation message (Telethon): {edit_err}")
        broadcast_type = pending_data_to_process['type']
        content_to_send = pending_data_to_process['content']
        fwd_chat_id_origin = pending_data_to_process['fwd_chat_id_if_forward']
        total_users = pending_data_to_process['target_count']
        status_message_chat_id = event.chat_id
        status_message_id = event.message_id
        await execute_broadcast_telethon(admin_user_id, broadcast_type, content_to_send, fwd_chat_id_origin, total_users, status_message_chat_id, status_message_id)
        await safe_answer_callback(event, "Broadcast initiated!")
async def execute_broadcast_telethon(admin_user_id, broadcast_type, content, fwd_chat_id_origin, total_users, status_msg_chat_id, status_msg_id):
    sent_count = 0
    blocked_users = []
    failed_users = []
    start_time = time.monotonic()
    logger.info(f"Executing Telethon broadcast by {admin_user_id}. Type: {broadcast_type}, Target: {total_users}")
    if users_collection is None:
        logger.error("DB unavailable for broadcast execution.")
        await safe_edit_message(status_msg_chat_id, status_msg_id, "Broadcast failed: Database connection error.")
        return
    try:
        user_cursor = await asyncio.to_thread(
            lambda: list(users_collection.find({}, {"_id": 1, "full_name": 1}))
        )
        update_interval = max(5, total_users // 20) if total_users > 0 else 5
        last_update_time = start_time
        for i, user_doc_bc in enumerate(user_cursor):
            user_id_str_telethon = user_doc_bc.get('_id')
            if not user_id_str_telethon:
                logger.error("Found user document without _id during broadcast execution (Telethon).")
                failed_users.append({'id': 'UNKNOWN', 'doc': user_doc_bc, 'error': 'Missing user_id'})
                continue
            target_peer_to_send = None
            current_error_str = ""
            is_blocked_or_unreachable = False
            try:
                user_id_int_telethon = int(user_id_str_telethon)
                target_peer_to_send = user_id_int_telethon
                if broadcast_type == "forward":
                    await client.forward_messages(target_peer_to_send, content, fwd_chat_id_origin)
                elif broadcast_type == "text":
                    await client.send_message(target_peer_to_send, content, parse_mode="html")
                sent_count += 1
                await asyncio.sleep(0.05)
            except FloodWaitError as e_flood_telethon:
                wait_time = e_flood_telethon.seconds + random.uniform(1,3)
                logger.warning(f"Broadcast (Telethon): Flood wait. Sleeping {wait_time:.2f}s...")
                await safe_edit_message(status_msg_chat_id, status_msg_id, f"Flood Wait... Sleeping {wait_time:.0f}s... ({sent_count}/{total_users})")
                await asyncio.sleep(wait_time)
                try:
                    if broadcast_type == "forward": await client.forward_messages(target_peer_to_send, content, fwd_chat_id_origin)
                    elif broadcast_type == "text": await client.send_message(target_peer_to_send, content, parse_mode="html")
                    sent_count += 1
                except Exception as e_retry_telethon:
                    current_error_str = str(e_retry_telethon)
                    logger.warning(f"Broadcast (Telethon): Retry failed for {user_id_str_telethon}: {current_error_str}")
            except Exception as e_general_send:
                current_error_str = str(e_general_send)
                logger.warning(f"Broadcast send error (Telethon) for user {user_id_str_telethon}: {current_error_str}")
            if current_error_str:
                failed_users.append({'id': user_id_str_telethon, 'doc': user_doc_bc, 'error': current_error_str})
                error_str_upper = current_error_str.upper()
                if "USER_IS_BLOCKED" in error_str_upper or \
                   "USER_DEACTIVATED" in error_str_upper or \
                   "PEER_ID_INVALID" in error_str_upper or \
                   "BOT_BLOCKED_BY_USER" in error_str_upper or \
                   "INPUT_USER_DEACTIVATED" in error_str_upper or \
                   "THE SPECIFIED USER WAS DELETED" in error_str_upper or \
                   "COULD NOT FIND THE INPUT ENTITY" in error_str_upper or \
                   "INVALID PEER" in error_str_upper:
                    blocked_users.append({'id': user_id_str_telethon, 'doc': user_doc_bc, 'error': current_error_str})
            current_time_val = time.monotonic()
            if (i % update_interval == 0 and i > 0) or (current_time_val - last_update_time > 15) or (i == total_users -1 ):
                 elapsed_time_val = current_time_val - start_time
                 current_blocked_count = len(blocked_users)
                 current_other_failed_count = len(failed_users) - current_blocked_count
                 try:
                     await safe_edit_message(
                          status_msg_chat_id, status_msg_id,
                          f"⏳ Broadcasting... {i+1}/{total_users} done.\n"
                          f"✅ Sent: {sent_count}, 🚫 Unreachable: {current_blocked_count}, ❌ Other Failed: {current_other_failed_count}\n"
                          f"⏱️ Elapsed: {elapsed_time_val:.1f}s"
                     )
                     last_update_time = current_time_val
                 except Exception as edit_e_telethon:
                     if "MESSAGE_NOT_MODIFIED" not in str(edit_e_telethon).upper():
                         logger.warning(f"Could not edit broadcast status during loop (Telethon): {edit_e_telethon}")
        end_time_val = time.monotonic()
        duration_val = end_time_val - start_time
        blocked_count_final = len(blocked_users)
        other_failed_count_final = len(failed_users) - blocked_count_final
        final_status_lines_telethon = [
            f"🏁 Broadcast Complete!\n",
            f"✅ Sent: {sent_count}",
            f"🚫 Blocked/Unreachable: {blocked_count_final}",
            f"❌ Other Failed: {other_failed_count_final}",
            f"👥 Total Targeted: {total_users}",
            f"⏱️ Duration: {duration_val:.2f} seconds"
        ]
        if blocked_users:
            final_status_lines_telethon.append("\n🚫 **Blocked/Unreachable Users (Max 15):**")
            for u_info_bl in blocked_users[:15]:
                user_link_bl = format_username_html_telethon(u_info_bl['doc']) if u_info_bl.get('doc') else f"<code>{u_info_bl['id']}</code>"
                final_status_lines_telethon.append(f" - {user_link_bl}")
            if len(blocked_users) > 15: final_status_lines_telethon.append(" - ... (and more)")
        other_failed_to_display = [fu for fu in failed_users if fu['id'] not in {bu['id'] for bu in blocked_users}]
        if other_failed_to_display:
            final_status_lines_telethon.append("\n❌ **Other Failed Users (Max 15):**")
            for u_info_fl in other_failed_to_display[:15]:
                user_link_fl = format_username_html_telethon(u_info_fl['doc']) if u_info_fl.get('doc') else f"<code>{u_info_fl['id']}</code>"
                error_msg_fl = html.escape(u_info_fl.get('error', 'Unknown Error')[:100])
                final_status_lines_telethon.append(f" - {user_link_fl} ({error_msg_fl})")
            if len(other_failed_to_display) > 15: final_status_lines_telethon.append(" - ... (and more)")
        final_status_text_telethon = "\n".join(final_status_lines_telethon)
        if len(final_status_text_telethon) > 4096: final_status_text_telethon = final_status_text_telethon[:4092] + "\n..."
        try:
            await safe_edit_message(status_msg_chat_id, status_msg_id, final_status_text_telethon, link_preview=False)
        except Exception as e_final_edit:
            logger.error(f"Failed to update final broadcast status message (Telethon): {e_final_edit}")
            await safe_send_message(status_msg_chat_id, final_status_text_telethon, link_preview=False)
        logger.info(f"Telethon broadcast finished. Sent: {sent_count}, Blocked/Unreachable: {blocked_count_final}, Other Failed: {other_failed_count_final}, Duration: {duration_val:.2f}s")
    except Exception as loop_err_telethon:
         logger.error(f"Error during broadcast loop execution (Telethon): {loop_err_telethon}", exc_info=True)
         try:
             await safe_edit_message(status_msg_chat_id, status_msg_id, f"❌ An error occurred during the broadcast: {html.escape(str(loop_err_telethon))}")
         except:
             await safe_send_message(status_msg_chat_id, "❌ An error occurred during the broadcast process.")

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
    if reply_message.photo and hasattr(reply_message.file, 'id'):
        file_id_to_store = reply_message.file.id
        logger.info(f"Attempting to use reply_message.file.id for PFP: {file_id_to_store} for user {user_id_str}")
    elif reply_message.document and reply_message.document.mime_type.startswith("image/") and hasattr(reply_message.file, 'id'):
        file_id_to_store = reply_message.file.id
        logger.info(f"Attempting to use reply_message.file.id from image document for PFP: {file_id_to_store} for user {user_id_str}")
    else:
        if reply_message.photo:
            logger.warning(f"Could not get message.file.id for photo for user {user_id_str}. PFP might not work reliably.")
            # Attempt to re-download and send to ourself to get a stable ID (Telethon specific workaround)
            try:
                temp_pfp_msg = await client.send_file('me', reply_message.photo)
                file_id_to_store = temp_pfp_msg.media.photo.id if temp_pfp_msg.media and hasattr(temp_pfp_msg.media, 'photo') and hasattr(temp_pfp_msg.media.photo, 'id') else None
                await temp_pfp_msg.delete()
                if not file_id_to_store:
                    return await safe_reply(event, "Could not obtain a stable file reference for this photo. Try a different image.")
                logger.info(f"Obtained PFP file ID via self-send: {file_id_to_store} for user {user_id_str}")
            except Exception as e_resend:
                logger.error(f"Error trying to get stable PFP ID via self-send: {e_resend}")
                return await safe_reply(event, "Failed to process the image for PFP. Try sending it as a new photo directly, not forwarded.")

        else:
             return await safe_reply(event, "Failed to process the image for PFP.")

    if not isinstance(file_id_to_store, (str, int)): # File IDs can sometimes be int64
        logger.error(f"PFP file_id_to_store is not a string or int: {file_id_to_store} (type: {type(file_id_to_store)}) for user {user_id_str}. Aborting PFP set.")
        return await safe_reply(event, "⚠️ Failed to get a valid file reference for the image. PFP not set.")

    try:
        users_collection.update_one(
            {"_id": user_id_str},
            {"$set": {"pfp_file_id": str(file_id_to_store)}} # Ensure it's stored as string
        )
        await safe_reply(event, "✅ Your profile picture has been updated for the bot!")
    except Exception as e:
        logger.error(f"Failed to set PFP in DB for user {user_id_str}: {e}", exc_info=True)
        await safe_reply(event, "⚠️ Failed to update your profile picture due to a database error.")

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
    deduction_result = await add_credits_to_user(user_id, -amount)
    if deduction_result == "insufficient": return await safe_reply(event, f"Insufficient credits confirmed by DB (should have been caught).")
    if not deduction_result: return await safe_reply(event, f"Failed to process your bet (deduction step), {user_mention}. Please try again.")
    user_choice_is_heads = choice_str.startswith("h")
    actual_flip_is_heads = random.choice([True, False])
    actual_flip_display = "Heads" if actual_flip_is_heads else "Tails"
    result_message = f"🪙 Coin Flip for {user_mention}!\nBet Amount: {amount} credits\n"
    result_message += f"You Chose: <b>{'Heads' if user_choice_is_heads else 'Tails'}</b>\n"
    result_message += f"Coin Landed On: <b>{actual_flip_display}</b>!\n\n"
    new_balance_after_deduction = current_credits - amount
    if user_choice_is_heads == actual_flip_is_heads:
        winnings_credited = amount * 2
        payout_result = await add_credits_to_user(user_id, winnings_credited)
        if payout_result:
            new_balance_after_win = new_balance_after_deduction + winnings_credited
            result_message += f"🎉 Congratulations! You won {amount} credits (total {winnings_credited} credited back)!\n"
            result_message += f"Your new balance: {new_balance_after_win} credits."
        else:
            result_message += f"💔 You won, but there was an error crediting your winnings. Please contact an admin. Your balance (after deduction): {new_balance_after_deduction}."
            logger.error(f"Flip win: Failed to credit winnings {winnings_credited} to user {user_id}")
    else:
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
    deduction_result = await add_credits_to_user(user_id, -amount)
    if deduction_result == "insufficient": return await safe_reply(event, f"Insufficient credits confirmed by DB (should have been caught).")
    if not deduction_result: return await safe_reply(event, f"Failed to process your bet (deduction step), {user_mention}. Please try again.")
    user_choice_is_odd = choice_str.startswith("o")
    dice_roll_val = random.randint(1, 6)
    actual_roll_is_odd = (dice_roll_val % 2 != 0)
    result_message = f"🎲 Dice Roll for {user_mention}!\nBet Amount: {amount} credits\n"
    result_message += f"You Chose: <b>{'Odd' if user_choice_is_odd else 'Even'}</b>\n"
    result_message += f"The Die Rolled: <b>{dice_roll_val}</b> ({'Odd' if actual_roll_is_odd else 'Even'})!\n\n"
    new_balance_after_deduction = current_credits - amount
    if user_choice_is_odd == actual_roll_is_odd:
        winnings_credited_dice = amount * 2
        payout_result_dice = await add_credits_to_user(user_id, winnings_credited_dice)
        if payout_result_dice:
            new_balance_after_win_dice = new_balance_after_deduction + winnings_credited_dice
            result_message += f"🎉 Congratulations! You won {amount} credits (total {winnings_credited_dice} credited back)!\n"
            result_message += f"Your new balance: {new_balance_after_win_dice} credits."
        else:
            result_message += f"💔 You won, but there was an error crediting your winnings. Please contact an admin. Your balance (after deduction): {new_balance_after_deduction}."
            logger.error(f"Dice win: Failed to credit winnings {winnings_credited_dice} to user {user_id}")
    else:
        result_message += f"💔 Unlucky! You lost {amount} credits.\n"
        result_message += f"Your new balance: {new_balance_after_deduction} credits."
    await safe_reply(event, result_message)

# --- Periodic Tasks ---
async def periodic_game_cleanup_task():
    global games
    while True:
        await asyncio.sleep(60) # Check every minute
        now = time.monotonic()
        games_to_cleanup = []
        async with games_lock:
            for game_id, g_data in games.items():
                last_active = g_data.get('last_activity_timestamp', g_data.get('monotonic_created_at', 0))
                if (now - last_active) > GAME_INACTIVITY_TIMEOUT:
                    games_to_cleanup.append((game_id, g_data.get('chat_id')))

        for game_id_cleanup, chat_id_cleanup in games_to_cleanup:
            logger.info(f"Game {game_id_cleanup} in chat {chat_id_cleanup} timed out. Cleaning up.")
            await cleanup_game(game_id_cleanup, chat_id_cleanup, reason="timeout due to inactivity")

LEADERBOARD_ACHIEVEMENT_FIELDS = ["runs", "wickets", "matches_played", "credits"]
LEADERBOARD_ACHIEVEMENT_PREFIX = "Leaderboard Rank"
FIELD_NAME_MAP_FOR_ACHIEVEMENTS = {
    "credits": "Richest Players", "runs": "Run Scorers",
    "wickets": "Wicket Takers", "matches_played": "Most Active"
}

async def manage_leaderboard_achievements():
    if users_collection is None:
        logger.warning("Skipping leaderboard achievement update: DB not available.")
        return

    logger.info("Starting leaderboard achievement update cycle...")
    all_possible_lb_achievements_list = []
    for field in LEADERBOARD_ACHIEVEMENT_FIELDS:
        for rank in range(1, 4): # Top 3
            all_possible_lb_achievements_list.append(f"{LEADERBOARD_ACHIEVEMENT_PREFIX} #{rank} - {FIELD_NAME_MAP_FOR_ACHIEVEMENTS[field]}")

    # Step 1: Get all users who currently have any of these specific leaderboard achievements
    users_with_old_lb_achievements = list(users_collection.find(
        {"achievements": {"$in": all_possible_lb_achievements_list}},
        {"_id": 1, "achievements": 1} # Projection
    ))

    bulk_ops_remove = []
    for user_doc in users_with_old_lb_achievements:
        user_id_str = user_doc["_id"]
        # Filter only the specific leaderboard achievements to remove from this user
        achievements_to_strip_for_this_user = [
            ach for ach in user_doc.get("achievements", []) if ach in all_possible_lb_achievements_list
        ]
        if achievements_to_strip_for_this_user:
            bulk_ops_remove.append(
                UpdateOne({"_id": user_id_str}, {"$pullAll": {"achievements": achievements_to_strip_for_this_user}})
            )

    if bulk_ops_remove:
        try:
            result_remove = await asyncio.to_thread(users_collection.bulk_write, bulk_ops_remove, ordered=False)
            logger.info(f"Leaderboard achievements: Stripped old from {result_remove.modified_count} users documents.")
        except Exception as e:
            logger.error(f"Error stripping old leaderboard achievements: {e}", exc_info=True)
            return # Stop if stripping fails, to avoid awarding duplicates if removal failed partially

    # Step 2 & 3: Fetch current top 3 for each category and award new achievements
    bulk_ops_add = []
    for field in LEADERBOARD_ACHIEVEMENT_FIELDS:
        top_3_users_for_field, err_msg = await asyncio.to_thread(_get_leaderboard_text_sync, field, top_n=3)
        if err_msg or not top_3_users_for_field:
            logger.warning(f"Could not fetch top 3 for {field} achievements: {err_msg or 'No users'}")
            continue

        for rank_idx, user_data_top in enumerate(top_3_users_for_field):
            rank = rank_idx + 1
            user_id_to_award_str = str(user_data_top['_id'])
            achievement_name = f"{LEADERBOARD_ACHIEVEMENT_PREFIX} #{rank} - {FIELD_NAME_MAP_FOR_ACHIEVEMENTS[field]}"
            bulk_ops_add.append(
                UpdateOne({"_id": user_id_to_award_str}, {"$addToSet": {"achievements": achievement_name}})
            )

    if bulk_ops_add:
        try:
            result_add = await asyncio.to_thread(users_collection.bulk_write, bulk_ops_add, ordered=False)
            logger.info(f"Leaderboard achievements: Awarded/updated for {result_add.modified_count} users documents.")
        except Exception as e:
            logger.error(f"Error awarding new leaderboard achievements: {e}", exc_info=True)

    logger.info("Leaderboard achievement update cycle finished.")


async def periodic_leaderboard_achievement_task():
    while True:
        await manage_leaderboard_achievements()
        await asyncio.sleep(LEADERBOARD_ACHIEVEMENT_UPDATE_INTERVAL)


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

        asyncio.create_task(periodic_game_cleanup_task())
        logger.info("Periodic game cleanup task started.")

        if users_collection is not None: # Only if DB is connected
            asyncio.create_task(periodic_leaderboard_achievement_task())
            logger.info("Periodic leaderboard achievement update task started.")
        else:
            logger.warning("Skipping leaderboard achievement task: DB not connected.")


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

