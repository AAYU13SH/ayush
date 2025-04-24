# --- START OF FULLY REVISED FILE with MongoDB, Ball Count & DM Leaderboards (v5) ---

import telebot
from telebot import types # For Inline Keyboards
import random
import logging
from uuid import uuid4
import os
import html
import urllib.parse
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, ReplyParameters, LinkPreviewOptions # Added new types
from pymongo import MongoClient, ReturnDocument # Import pymongo
from datetime import datetime # For timestamping user registration

# --- Bot Configuration ---
BOT_TOKEN = "7870704761:AAH-RMKO7chV0nu6-o5wUYFiat7XwBW6OCk" # Replace with your bot token
if BOT_TOKEN == "YOUR_BOT_TOKEN" or not BOT_TOKEN:
    print("ERROR: Please replace 'YOUR_BOT_TOKEN' with your actual bot token.")
    exit()

# --- MongoDB Configuration ---
MONGO_URI = "mongodb+srv://yesvashisht:yash2005@clusterdf.yagj9ok.mongodb.net/?retryWrites=true&w=majority&appName=Clusterdf" # Replace with your MongoDB URI
MONGO_DB_NAME = "tct_cricket_bot_db"
if MONGO_URI == "YOUR_MONGODB_URI" or not MONGO_URI:
     print("ERROR: Please configure MONGO_URI.")
     # exit() # Commented out for testing without DB if needed

bot = telebot.TeleBot(BOT_TOKEN)
bot_username = None # Will be fetched later

# --- Admin Configuration ---
xmods = [6293455550, 6265981509]

# --- Database Setup ---
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) # Added timeout
    client.admin.command('ping')
    db = client[MONGO_DB_NAME]
    users_collection = db.users
    print("Successfully connected to MongoDB and pinged the deployment.")
except Exception as e:
    print(f"ERROR: Could not connect to MongoDB at {MONGO_URI}.")
    print(f"Error details: {e}")
    db = None # Ensure db is None if connection fails
    users_collection = None
    print("Warning: Bot running without database persistence.")


# --- Cricket Game States ---
STATE_WAITING = "WAITING"
STATE_TOSS = "TOSS"
STATE_BAT_BOWL = "BAT_BOWL"
STATE_P1_BAT = "P1_BAT"
STATE_P1_BOWL_WAIT = "P1_BOWL_WAIT"
STATE_P2_BAT = "P2_BAT"
STATE_P2_BOWL_WAIT = "P2_BOWL_WAIT"

# --- In-memory storage for active games ---
games = {}

# --- Logging ---
logger = telebot.logger
telebot.logger.setLevel(logging.INFO)

# --- Helper Functions ---
def get_player_name_telebot(user):
    if user is None: return "Unknown Player"
    name = user.first_name
    if user.last_name: name += f" {user.last_name}"
    if not name and user.username: name = f"@{user.username}"
    if not name: name = f"User_{user.id}"
    return name

def create_standard_keyboard_telebot(game_id, buttons_per_row=3):
    markup = types.InlineKeyboardMarkup(row_width=buttons_per_row)
    buttons = [types.InlineKeyboardButton(str(i), callback_data=f"num:{i}:{game_id}") for i in range(1, 7)]
    markup.add(*buttons)
    return markup

def cleanup_game_telebot(game_id, chat_id, reason="ended", edit_markup=True):
    logger.info(f"Cleaning up game {game_id} in chat {chat_id} (Reason: {reason})")
    game_data = games.pop(game_id, None)
    if game_data and game_data.get('message_id') and edit_markup:
        if reason != "finished normally":
            try:
                bot.edit_message_reply_markup(chat_id=chat_id, message_id=game_data['message_id'], reply_markup=None)
            except Exception as e:
                if "message is not modified" not in str(e) and "message to edit not found" not in str(e):
                    logger.error(f"Could not edit reply markup for game {game_id} on cleanup: {e}")

# --- Database Helper Functions ---
def get_user_data(user_id_str):
    if users_collection is None: return None
    try: return users_collection.find_one({"_id": user_id_str})
    except Exception as e: logger.error(f"DB error fetching user {user_id_str}: {e}"); return None

def register_user(user: types.User):
    if users_collection is None: return False
    user_id_str = str(user.id); now = datetime.utcnow()
    user_doc = {"$set": {"full_name": user.full_name, "username": user.username, "last_seen": now},
                "$setOnInsert": {"_id": user_id_str, "runs": 0, "wickets": 0, "achievements": [], "registered_at": now}}
    try:
        result = users_collection.update_one({"_id": user_id_str}, user_doc, upsert=True)
        return result.upserted_id is not None or result.matched_count > 0
    except Exception as e: logger.error(f"DB error registering user {user_id_str}: {e}"); return False

def add_runs_to_user(user_id_str, runs_to_add):
    if users_collection is None or runs_to_add <= 0: return False
    try:
        result = users_collection.update_one({"_id": user_id_str}, {"$inc": {"runs": runs_to_add}}, upsert=False)
        return result.matched_count > 0
    except Exception as e: logger.error(f"DB error adding runs to user {user_id_str}: {e}"); return False

def add_wicket_to_user(user_id_str):
    if users_collection is None: return False
    try:
        result = users_collection.update_one({"_id": user_id_str}, {"$inc": {"wickets": 1}}, upsert=False)
        return result.matched_count > 0
    except Exception as e: logger.error(f"DB error adding wicket to user {user_id_str}: {e}"); return False

# Helper to get mention from DB doc
def get_user_mention_from_db(user_doc):
    if not user_doc: return "Unknown User"
    uid_str = user_doc.get("_id"); name = user_doc.get("full_name", f"User {uid_str}")
    return f"[{html.escape(name)}](tg://user?id={uid_str})"

# --- Leaderboard Display Logic (Helper Functions) ---
def _display_runs_leaderboard(chat_id):
    """Fetches and sends the Top 10 Runs Leaderboard to the specified chat."""
    if users_collection is None:
        try: bot.send_message(chat_id, "⚠️ Database connection is unavailable. Cannot fetch leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send DB error msg to {chat_id}: {send_err}")
        return
    try:
        top = list(users_collection.find({"runs": {"$gt": 0}}, {"_id": 1, "full_name": 1, "runs": 1}).sort("runs", -1).limit(10)) # Changed to 10
        if not top:
            bot.send_message(chat_id, "🏏 No runs scored by anyone yet.")
            return
        medals = ['🥇', '🥈', '🥉'] # Only top 3 get medals
        rank_markers = ['4️⃣','5️⃣','6️⃣','7️⃣','8️⃣','9️⃣','🔟']
        txt = "🏆 *Top 10 Run Scorers:*\n\n"
        for i, u in enumerate(top):
            rank_prefix = medals[i] if i < len(medals) else (rank_markers[i-len(medals)] if i-len(medals) < len(rank_markers) else f"{i+1}.")
            txt += f"{rank_prefix} {get_user_mention_from_db(u)} - *{u.get('runs', 0)}* runs\n"
        bot.send_message(chat_id, txt, parse_mode='Markdown', link_preview_options=LinkPreviewOptions(is_disabled=True))
    except Exception as e:
        logger.error(f"DB/Send error during runs leaderboard for {chat_id}: {e}")
        try: bot.send_message(chat_id, "⚠️ An error occurred while fetching the leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send leaderboard error msg to {chat_id}: {send_err}")

def _display_wickets_leaderboard(chat_id):
    """Fetches and sends the Top 10 Wickets Leaderboard to the specified chat."""
    if users_collection is None:
        try: bot.send_message(chat_id, "⚠️ Database connection is unavailable. Cannot fetch leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send DB error msg to {chat_id}: {send_err}")
        return
    try:
        top = list(users_collection.find({"wickets": {"$gt": 0}}, {"_id": 1, "full_name": 1, "wickets": 1}).sort("wickets", -1).limit(10)) # Changed to 10
        if not top:
            bot.send_message(chat_id, "🎯 No wickets taken by anyone yet.")
            return
        medals = ['🥇', '🥈', '🥉'] # Only top 3 get medals
        rank_markers = ['4️⃣','5️⃣','6️⃣','7️⃣','8️⃣','9️⃣','🔟']
        txt = "🎯 *Top 10 Wicket Takers:*\n\n"
        for i, u in enumerate(top):
            rank_prefix = medals[i] if i < len(medals) else (rank_markers[i-len(medals)] if i-len(medals) < len(rank_markers) else f"{i+1}.")
            txt += f"{rank_prefix} {get_user_mention_from_db(u)} - *{u.get('wickets', 0)}* wickets\n"
        bot.send_message(chat_id, txt, parse_mode='Markdown', link_preview_options=LinkPreviewOptions(is_disabled=True))
    except Exception as e:
        logger.error(f"DB/Send error during wickets leaderboard for {chat_id}: {e}")
        try: bot.send_message(chat_id, "⚠️ An error occurred while fetching the leaderboard.")
        except Exception as send_err: logger.error(f"Failed to send leaderboard error msg to {chat_id}: {send_err}")


# --- Command Handlers ---

@bot.message_handler(commands=['start'])
def handle_start(message: Message):
    user = message.from_user
    user_id_str = str(user.id)
    chat_id = message.chat.id
    mention = f"[{user.full_name}](tg://user?id={user_id_str})"

    # --- Deep Link Handling ---
    if message.chat.type == 'private':
        args = message.text.split()
        if len(args) > 1:
            payload = args[1]
            logger.info(f"User {user.id} started bot in private with payload: {payload}")
            if payload == 'show_lead_runs':
                _display_runs_leaderboard(chat_id)
                return # Don't proceed to registration message
            elif payload == 'show_lead_wickets':
                _display_wickets_leaderboard(chat_id)
                return # Don't proceed to registration message
            # Add other potential payloads here if needed
            # else: pass through to registration if payload is unknown

    # --- Standard /start in Group ---
    if message.chat.type != 'private':
         bot.reply_to(message, "Welcome! Use /cricket in a group to play. Use /start in my DM to register for stats and view leaderboards.")
         return

    # --- Standard /start in DM (Registration) ---
    if users_collection is None:
         bot.reply_to(message, "⚠️ Database connection is unavailable. Registration and stats are disabled.")
         return

    if get_user_data(user_id_str):
        register_user(user) # Update details like name/username/last_seen
        bot.reply_to(message, f"{mention}, you are already registered! Use /help to see commands.", parse_mode='markdown')
        return

    if register_user(user):
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton('Channel', url='https://t.me/TCTCRICKET'),
                   InlineKeyboardButton('Group', url='https://t.me/+SIzIYQeMsRsyOWM1')) # Corrected group link
        welcome_text = f"Welcome {mention} to the TCT OFFICIAL BOT!\nYou are now registered.\n\nUse /help for commands or check leaderboards:\n/lead_runs\n/lead_wickets"
        bot.send_message(message.chat.id, welcome_text, parse_mode='markdown', reply_markup=markup,
                         link_preview_options=LinkPreviewOptions(is_disabled=True))
        logger.info(f"New user registered: {user.full_name} ({user_id_str})")
        try: # Notify admin
            if xmods: bot.send_message(xmods[0], f"➕ New user: {mention} (`{user_id_str}`)", parse_mode='markdown', link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e: logger.error(f"Could not notify admin: {e}")
    else:
        bot.reply_to(message, "⚠️ An error occurred during registration. Please try again later.")

@bot.message_handler(commands=['help'])
def help_command(message):
    is_admin = message.from_user.id in xmods
    user_commands = """*User Commands:*
  `/start` - Register (in DM) or handle deep links.
  `/help` - This help message.
  `/cricket` - Start a cricket game (in group).
  `/cancel` - Cancel your current game (in group).
  `/my_achievement` - View your stats & achievements (reply or DM).
  `/lead_runs` - View Top 10 Run Scorers (DM recommended).
  `/lead_wickets` - View Top 10 Wicket Takers (DM recommended)."""
    admin_commands = """*Admin Commands:*
  `/achieve <user_id> <title>` - Add achievement (or reply).
  `/remove_achievement <user_id> <title>` - Remove achievement (or reply).
  `/broad <message>` - Broadcast (or reply).
  `/reduce_runs <user_id> <amount>` - Reduce runs (or reply).
  `/reduce_wickets <user_id> <amount>` - Reduce wickets (or reply).
  `/clear_all_stats` - Reset all stats (use with caution!).
  `/user_count` - Show total registered users."""
    help_text = "📜 *Available Commands*\n" + user_commands
    if is_admin: help_text += "\n\n" + admin_commands
    bot.reply_to(message, help_text, parse_mode='Markdown')

@bot.message_handler(commands=['cricket'])
def start_cricket(message):
    user = message.from_user; user_id_str = str(user.id)
    if message.chat.type == 'private':
        bot.reply_to(message, "Cricket games can only be started in group chats.")
        return
    if users_collection is not None and not get_user_data(user_id_str):
         return bot.reply_to(message, f"@{get_player_name_telebot(user)}, please /start me in DM first to register before playing.")
    elif users_collection is None: logger.warning(f"Game start by {user_id_str} while DB is down.")

    chat_id = message.chat.id; player1_name = get_player_name_telebot(user)
    logger.info(f"User {player1_name} ({user.id}) initiated /cricket in chat {chat_id}")

    # Check existing/active games...
    for gid, gdata in list(games.items()):
        if gdata['chat_id'] == chat_id:
            p1_id = gdata.get('player1', {}).get('id'); p2_id = gdata.get('player2', {}).get('id')
            if gdata['state'] == STATE_WAITING and p1_id == user.id:
                return bot.reply_to(message, "You already started a game waiting for players. Use /cancel first if you want to restart.")
            if user.id == p1_id or user.id == p2_id:
                 return bot.reply_to(message, "You are already participating in an active game in this chat! Use /cancel if you wish to stop it.")
            # Maybe add a check for *any* active game in the chat?
            # if gdata['state'] != STATE_WAITING:
            #     return bot.reply_to(message, "Another game is already in progress in this chat.")

    # Create game...
    game_id = str(uuid4())
    game_data = { # Added ball_count
        'chat_id': chat_id, 'message_id': None, 'state': STATE_WAITING,
        'player1': {'id': user.id, 'name': player1_name, 'user_obj': user},
        'player2': None, 'p1_score': 0, 'p2_score': 0, 'innings': 1,
        'current_batter': None, 'current_bowler': None, 'toss_winner': None,
        'p1_toss_choice': None, 'batter_choice': None, 'target': None,
        'ball_count': 0 # Initialize ball count
    }
    games[game_id] = game_data
    logger.info(f"Created game {game_id} for {player1_name} in chat {chat_id}")
    markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("Join Game 🏏", callback_data=f"join:_:{game_id}"))
    try:
        sent_message = bot.send_message(chat_id, f"🏏 New Cricket Game started by {player1_name}!\n\nWaiting for a second player to join...", reply_markup=markup, parse_mode="Markdown")
        games[game_id]["message_id"] = sent_message.message_id
    except Exception as e:
        logger.error(f"Failed to send initial cricket game message {game_id}: {e}");
        games.pop(game_id, None) # Clean up if sending fails

@bot.message_handler(commands=['cancel'])
def cancel_cricket(message):
    user = message.from_user; chat_id = message.chat.id; game_to_cancel_id = None
    if message.chat.type == 'private':
        bot.reply_to(message, "You can only cancel games in the group chat where they were started.")
        return

    logger.info(f"User {get_player_name_telebot(user)} ({user.id}) trying to /cancel in chat {chat_id}")
    for gid, gdata in list(games.items()):
        if gdata['chat_id'] == chat_id:
             p1_id = gdata.get('player1', {}).get('id'); p2_id = gdata.get('player2', {}).get('id')
             if user.id == p1_id or user.id == p2_id:
                 game_to_cancel_id = gid; break

    if game_to_cancel_id:
        logger.info(f"Cancelling game {game_to_cancel_id} by user {user.id}")
        game_data = games.get(game_to_cancel_id)
        # Notify players in the game message if possible
        player_text = ""
        if game_data:
            p1n = game_data.get('player1', {}).get('name', 'P1')
            p2n = game_data.get('player2', {}).get('name')
            player_text = f" ({p1n}{' vs ' + p2n if p2n else ''})"

        cleanup_game_telebot(game_to_cancel_id, chat_id, reason="cancelled by user")
        try:
            # Edit the original message if possible
            if game_data and game_data.get('message_id'):
                bot.edit_message_text(f"❌ Cricket game{player_text} cancelled by {get_player_name_telebot(user)}.",
                                      chat_id=chat_id, message_id=game_data['message_id'], reply_markup=None, parse_mode="Markdown")
            else: # Fallback reply if editing fails or message_id missing
                 bot.reply_to(message, f"❌ Cricket game{player_text} cancelled.")
        except Exception as e:
            logger.warning(f"Could not edit cancel message for game {game_to_cancel_id}: {e}")
            bot.reply_to(message, f"❌ Cricket game{player_text} cancelled.") # Fallback reply

    else:
        bot.reply_to(message, "You aren't currently participating in an active game in this chat.")

# --- Broadcast Command (Admin) --- (Unchanged)
@bot.message_handler(commands=['broad'])
def handle_broadcast(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    try:
        user_ids_to_broadcast = [user["_id"] for user in users_collection.find({}, {"_id": 1})]
    except Exception as e: logger.error(f"DB error fetching users for broadcast: {e}"); return bot.reply_to(message, "⚠️ Error fetching users.")
    if not user_ids_to_broadcast: return bot.reply_to(message, "⚠️ No registered users found.")
    content_to_send = None; is_forward = False
    if message.reply_to_message: content_to_send = message.reply_to_message; is_forward = True; logger.info(f"Admin {message.from_user.id} broadcasting via forward.")
    else:
        args = message.text.split(maxsplit=1)
        if len(args) < 2: return bot.reply_to(message, "⚠️ Usage: `/broadcast <message>` or reply.")
        content_to_send = args[1]; is_forward = False; logger.info(f"Admin {message.from_user.id} broadcasting text.")
    sent_count = 0; failed_count = 0; total_users = len(user_ids_to_broadcast)
    status_message = bot.reply_to(message, f"📢 Broadcasting to {total_users} users... [0/{total_users}]")
    last_edit_time = datetime.now()
    for i, user_id_str in enumerate(user_ids_to_broadcast):
        try:
            if is_forward: bot.forward_message(chat_id=user_id_str, from_chat_id=message.chat.id, message_id=content_to_send.message_id)
            else: bot.send_message(user_id_str, content_to_send, parse_mode="Markdown")
            sent_count += 1
        except Exception as e: failed_count += 1; logger.warning(f"Broadcast failed for {user_id_str}: {e}")
        now = datetime.now()
        if (now - last_edit_time).total_seconds() > 2 or (i + 1) % 20 == 0 or (i + 1) == total_users:
             try: bot.edit_message_text(f"📢 Broadcasting... [{sent_count}/{total_users}] Sent, [{failed_count}] Failed", chat_id=message.chat.id, message_id=status_message.message_id); last_edit_time = now
             except Exception: pass
    final_text = f"📢 Broadcast Finished!\n✅ Sent: {sent_count}\n❌ Failed: {failed_count}"
    try: bot.edit_message_text(final_text, chat_id=message.chat.id, message_id=status_message.message_id)
    except Exception: bot.reply_to(message, final_text)

# --- Achievement Commands --- (Unchanged)
@bot.message_handler(commands=['achieve'])
def add_achievement(message):
    if message.from_user.id not in xmods: return bot.reply_to(message,"❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    args = message.text.split(maxsplit=1); target_user_id_str = None; title = None
    if message.reply_to_message: target_user_id_str = str(message.reply_to_message.from_user.id); title = args[1].strip() if len(args) >= 2 else None
    else:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3: return bot.reply_to(message, "⚠️ Usage: `/achieve <user_id> <title>`.")
        target_user_id_str = parts[1]; title = parts[2].strip()
        if not target_user_id_str.isdigit(): return bot.reply_to(message, "⚠️ Invalid User ID.")
    if not title: return bot.reply_to(message, "⚠️ Title cannot be empty.")
    encoded_title = urllib.parse.quote(title)
    markup = InlineKeyboardMarkup().add(InlineKeyboardButton("✅ Confirm", callback_data=f"ach_confirm_add_{target_user_id_str}_{encoded_title}"), InlineKeyboardButton("❌ Cancel", callback_data="ach_cancel"))
    bot.reply_to(message, f"🏅 Add achievement \"*{html.escape(title)}*\" to user `{target_user_id_str}`?", reply_markup=markup, parse_mode="markdown")

@bot.message_handler(commands=['remove_achievement'])
def remove_achievement(message):
    if message.from_user.id not in xmods: return bot.reply_to(message,"❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    args = message.text.split(maxsplit=1); target_user_id_str = None; title = None
    if message.reply_to_message: target_user_id_str = str(message.reply_to_message.from_user.id); title = args[1].strip() if len(args) >= 2 else None
    else:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3: return bot.reply_to(message, "⚠️ Usage: `/remove_achievement <user_id> <title>`.")
        target_user_id_str = parts[1]; title = parts[2].strip()
        if not target_user_id_str.isdigit(): return bot.reply_to(message, "⚠️ Invalid User ID.")
    if not title: return bot.reply_to(message, "⚠️ Title cannot be empty.")
    encoded_title = urllib.parse.quote(title)
    markup = InlineKeyboardMarkup().add(InlineKeyboardButton("✅ Confirm", callback_data=f"ach_confirm_remove_{target_user_id_str}_{encoded_title}"), InlineKeyboardButton("❌ Cancel", callback_data="ach_cancel"))
    bot.reply_to(message, f"🗑️ Remove achievement \"*{html.escape(title)}*\" from user `{target_user_id_str}`?", reply_markup=markup, parse_mode="markdown")

@bot.message_handler(commands=['my_achievement'])
def view_my_stats_and_achievements(message):
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    target_user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid_str = str(target_user.id); user_data = get_user_data(uid_str)
    if user_data is None:
        reply_text = f"User [{get_player_name_telebot(target_user)}](tg://user?id={uid_str}) is not registered. "
        reply_text += "Please tell them to /start me in DM." if message.reply_to_message else "Please /start me in DM first."
        return bot.reply_to(message, reply_text, parse_mode="markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))

    runs = user_data.get("runs", 0); wickets = user_data.get("wickets", 0); achievements = user_data.get("achievements", [])
    name = user_data.get("full_name") or get_player_name_telebot(target_user)
    mention = f"[{html.escape(name)}](tg://user?id={uid_str})" # Escape name just in case
    stats_text = f"📊 Stats for {mention}:\n  🏏 Runs: *{runs}*\n  🎯 Wickets: *{wickets}*"
    achievement_text = "\n\n🏆 *Achievements*"
    if achievements: achievement_text += f" ({len(achievements)}):\n" + "\n".join([f"  🏅 `{html.escape(str(title))}`" for title in achievements])
    else: achievement_text += ":\n  *None yet.*"
    bot.reply_to(message, stats_text + achievement_text, parse_mode="markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))

# --- Stat Modification Commands (Admin) --- (Unchanged)
@bot.message_handler(commands=['reduce_runs'])
def reduce_runs_cmd(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    target_user = message.reply_to_message.from_user if message.reply_to_message else None
    parts = message.text.split(); uid_str = None; amount = None
    try:
        if target_user: uid_str = str(target_user.id); amount = int(parts[1])
        elif len(parts) >= 3: uid_str = parts[1]; amount = int(parts[2]); assert uid_str.isdigit()
        else: raise ValueError("Invalid usage")
        assert amount > 0
    except (ValueError, IndexError, AssertionError): return bot.reply_to(message, "⚠️ Usage: Reply or `/reduce_runs <user_id> <amount>`.")
    try:
        user_doc = users_collection.find_one_and_update( {"_id": uid_str}, [{"$set": {"runs": {"$max": [0, {"$subtract": ["$runs", amount]}]}}}], projection={"runs": 1, "full_name": 1}, return_document=ReturnDocument.AFTER)
        if user_doc:
            new_runs = user_doc.get("runs", 0); name = user_doc.get("full_name") or f"user {uid_str}"
            mention = f"[{html.escape(name)}](tg://user?id={uid_str})" # Escape name
            bot.reply_to(message, f"✅ Reduced *{amount}* runs from {mention}. New total: *{new_runs}*.", parse_mode="Markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))
        else: bot.reply_to(message, f"⚠️ User `{uid_str}` not found.")
    except Exception as e: logger.error(f"DB error reducing runs for {uid_str}: {e}"); bot.reply_to(message, "⚠️ DB error.")

@bot.message_handler(commands=['reduce_wickets'])
def reduce_wickets_cmd(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    target_user = message.reply_to_message.from_user if message.reply_to_message else None
    parts = message.text.split(); uid_str = None; amount = None
    try:
        if target_user: uid_str = str(target_user.id); amount = int(parts[1])
        elif len(parts) >= 3: uid_str = parts[1]; amount = int(parts[2]); assert uid_str.isdigit()
        else: raise ValueError("Invalid usage")
        assert amount > 0
    except (ValueError, IndexError, AssertionError): return bot.reply_to(message, "⚠️ Usage: Reply or `/reduce_wickets <user_id> <amount>`.")
    try:
         user_doc = users_collection.find_one_and_update({"_id": uid_str}, [{"$set": {"wickets": {"$max": [0, {"$subtract": ["$wickets", amount]}]}}}], projection={"wickets": 1, "full_name": 1}, return_document=ReturnDocument.AFTER)
         if user_doc:
             new_wickets = user_doc.get("wickets", 0); name = user_doc.get("full_name") or f"user {uid_str}"
             mention = f"[{html.escape(name)}](tg://user?id={uid_str})" # Escape name
             bot.reply_to(message, f"✅ Reduced *{amount}* wickets from {mention}. New total: *{new_wickets}*.", parse_mode="Markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))
         else: bot.reply_to(message, f"⚠️ User `{uid_str}` not found.")
    except Exception as e: logger.error(f"DB error reducing wickets for {uid_str}: {e}"); bot.reply_to(message, "⚠️ DB error.")

@bot.message_handler(commands=['clear_all_stats'])
def clear_all_stats(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⚠️ YES, CLEAR ALL STATS ⚠️", callback_data="confirm_clear_stats"), types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_clear_stats"))
    bot.reply_to(message, "🚨 *DANGER ZONE* 🚨\nThis will reset ALL runs and wickets for ALL users to zero. This action CANNOT be undone.\n\nAre you absolutely sure?", reply_markup=markup, parse_mode="Markdown")

@bot.message_handler(commands=['user_count'])
def user_count(message):
     if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
     if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
     try: count = users_collection.count_documents({}); bot.reply_to(message, f"👥 Registered users in database: {count}")
     except Exception as e: logger.error(f"DB error counting users: {e}"); bot.reply_to(message, "⚠️ Error counting users.")

# --- Leaderboard Commands (Modified) ---

@bot.message_handler(commands=['lead_runs'])
def show_runs_leaderboard(message: Message):
    global bot_username
    if bot_username is None: # Fetch if not already fetched
        try:
             bot_username = bot.get_me().username
        except Exception as e:
             logger.error(f"Failed to get bot username: {e}")
             bot.reply_to(message, "⚠️ Error fetching bot info. Cannot generate link.")
             return

    if message.chat.type in ['group', 'supergroup']:
        # Send link to DM
        button_url = f"https://t.me/{bot_username}?start=show_lead_runs"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("📊 View Top 10 Runs (DM)", url=button_url))
        bot.reply_to(message, "Leaderboards are best viewed privately. Click the button below!", reply_markup=markup)
    else:
        # Show directly in DM
        _display_runs_leaderboard(message.chat.id)

@bot.message_handler(commands=['lead_wickets'])
def show_wickets_leaderboard(message: Message):
    global bot_username
    if bot_username is None: # Fetch if not already fetched
        try:
             bot_username = bot.get_me().username
        except Exception as e:
             logger.error(f"Failed to get bot username: {e}")
             bot.reply_to(message, "⚠️ Error fetching bot info. Cannot generate link.")
             return

    if message.chat.type in ['group', 'supergroup']:
        # Send link to DM
        button_url = f"https://t.me/{bot_username}?start=show_lead_wickets"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🎯 View Top 10 Wickets (DM)", url=button_url))
        bot.reply_to(message, "Leaderboards are best viewed privately. Click the button below!", reply_markup=markup)
    else:
        # Show directly in DM
        _display_wickets_leaderboard(message.chat.id)


# --- Central Callback Query Handler ---
@bot.callback_query_handler(func=lambda call: True)
def handle_callback_query(call):
    user = call.from_user; chat_id = call.message.chat.id
    message_id = call.message.message_id; data = call.data
    logger.debug(f"Callback: Data='{data}', User={user.id}, Chat={chat_id}, Msg={message_id}")

    # --- Achievement & Stat Clear Callbacks --- (Unchanged)
    if data.startswith("ach_") or data == "confirm_clear_stats" or data == "cancel_clear_stats":
        if users_collection is None: return bot.answer_callback_query(call.id, "Database unavailable.", show_alert=True)

        if data.startswith("ach_"): # Achievement
            parts = data.split("_", 4)
            if parts[1] == "cancel":
                 try: bot.edit_message_text("❌ Operation cancelled.", chat_id, message_id, reply_markup=None)
                 except Exception as e: logger.warning(f"Failed to edit 'ach_cancel' message: {e}")
                 return bot.answer_callback_query(call.id)
            if len(parts) < 5: return bot.answer_callback_query(call.id, "Invalid callback data.")
            if user.id not in xmods: return bot.answer_callback_query(call.id, "❌ Not authorized for this action.")

            action, mode, user_id_str, encoded_title = parts[1], parts[2], parts[3], parts[4]; title = urllib.parse.unquote(encoded_title)
            try:
                msg = "DB Error."; res = None
                target_user_data = get_user_data(user_id_str) # Check if user exists first
                if not target_user_data:
                    msg = f"⚠️ User `{user_id_str}` not found in the database."
                elif mode == "add":
                    res = users_collection.update_one({"_id": user_id_str}, {"$addToSet": {"achievements": title}})
                elif mode == "remove":
                    res = users_collection.update_one({"_id": user_id_str}, {"$pull": {"achievements": title}})

                if res: # If update attempted (and user exists)
                    if res.matched_count == 0: msg = f"⚠️ User `{user_id_str}` not found (should not happen here)." # Should be caught above
                    elif res.modified_count == 0:
                        if mode == "add": msg = f"⚠️ Achievement \"*{html.escape(title)}*\" might already exist for `{user_id_str}`. No changes made."
                        else: msg = f"⚠️ Achievement \"*{html.escape(title)}*\" not found for `{user_id_str}`. No changes made."
                    elif mode == "add": msg = f"✅ Added \"*{html.escape(title)}*\" achievement to `{user_id_str}`."; logger.info(f"Admin {user.id} added ach '{title}' for {user_id_str}")
                    elif mode == "remove": msg = f"🗑️ Removed \"*~~{html.escape(title)}~~*\" achievement from `{user_id_str}`."; logger.info(f"Admin {user.id} removed ach '{title}' for {user_id_str}")

                bot.edit_message_text(msg, chat_id, message_id, parse_mode="markdown", reply_markup=None)
            except Exception as e:
                logger.error(f"DB error processing achievement callback {data}: {e}")
                try: bot.edit_message_text("⚠️ An unexpected database error occurred.", chat_id, message_id)
                except Exception as edit_e: logger.error(f"Failed to edit message on DB error: {edit_e}")
            return bot.answer_callback_query(call.id)

        elif data == "confirm_clear_stats": # Stat Clear Confirm
            if user.id not in xmods: return bot.answer_callback_query(call.id, "❌ Not authorized.")
            try:
                res = users_collection.update_many({}, {"$set": {"runs": 0, "wickets": 0}})
                bot.edit_message_text(f"🧹 Stats cleared for *{res.modified_count}* users!", chat_id, message_id, reply_markup=None, parse_mode="Markdown")
                logger.warning(f"Admin {user.id} ({user.full_name}) cleared ALL stats ({res.modified_count} users affected).")
                return bot.answer_callback_query(call.id, "✅ All Stats Cleared!")
            except Exception as e:
                logger.error(f"DB error clearing all stats: {e}")
                bot.edit_message_text("⚠️ Error clearing stats in the database.", chat_id, message_id)
                return bot.answer_callback_query(call.id, "DB error.")

        elif data == "cancel_clear_stats": # Stat Clear Cancel
             try: bot.edit_message_text("❌ Stat clearing operation cancelled.", chat_id, message_id, reply_markup=None)
             except Exception as e: logger.warning(f"Failed to edit 'cancel_clear_stats' message: {e}")
             return bot.answer_callback_query(call.id)
    # --- End Achievement/Stat Clear ---


    # --- Cricket Game Callbacks ---
    try: action, value_str, game_id = data.split(":", 2); value = int(value_str) if value_str.isdigit() else value_str
    except ValueError:
        # logger.debug(f"Ignoring callback with invalid format: {data}")
        return bot.answer_callback_query(call.id) # Ignore non-game format silently

    if game_id not in games:
        logger.warning(f"Callback received for non-existent or ended game {game_id}. Data: {data}");
        try: bot.edit_message_text("This game session has ended or was cancelled.", chat_id, message_id, reply_markup=None)
        except Exception as e:
             if "message is not modified" not in str(e): logger.warning(f"Failed to edit ended game message {message_id}: {e}")
        return bot.answer_callback_query(call.id, "Game session ended.")

    game = games[game_id]
    # Ensure the callback is for the *current* game message
    if message_id != game.get("message_id"):
         logger.warning(f"Callback ignored: Stale message ID for game {game_id}. Msg {message_id} vs Game {game.get('message_id')}.");
         return bot.answer_callback_query(call.id, "Please use the buttons on the latest game message.")

    # --- Game State Machine ---
    current_state = game.get('state'); p1 = game['player1']; p2 = game.get('player2')
    p1_name = p1['name']; p2_name = p2['name'] if p2 else "Player 2"
    p1_id = p1['id']; p2_id = p2['id'] if p2 else None
    logger.debug(f"Processing game callback '{action}' for game {game_id}, state '{current_state}', user {user.id}")

    try:
        # --- JOIN ---
        if action == "join" and current_state == STATE_WAITING:
             if user.id == p1_id:
                 return bot.answer_callback_query(call.id, "You cannot join your own game.")
             if p2: # Already have a player 2
                 return bot.answer_callback_query(call.id, f"{p2_name} has already joined.")

             # Check if P2 is registered
             user_id_str = str(user.id)
             if users_collection is not None and not get_user_data(user_id_str):
                 bot.answer_callback_query(call.id) # Answer first
                 bot.send_message(chat_id,
                                  f"@{get_player_name_telebot(user)}, please /start me in DM first to register before joining a game.",
                                  reply_parameters=ReplyParameters(message_id=message_id))
                 return # Stop processing join

             bot.answer_callback_query(call.id) # Answer query *before* editing message
             player2_name = get_player_name_telebot(user)
             game['player2'] = {"id": user.id, "name": player2_name, "user_obj": user}
             p2_name = player2_name # Update local variable too
             game['state'] = STATE_TOSS
             logger.info(f"Player 2 ({player2_name} - {user.id}) joined game {game_id}.")

             markup = types.InlineKeyboardMarkup(row_width=2).add(
                 types.InlineKeyboardButton("Heads", callback_data=f"toss:H:{game_id}"),
                 types.InlineKeyboardButton("Tails", callback_data=f"toss:T:{game_id}")
             )
             try:
                 bot.edit_message_text(f"✅ {p2_name} has joined the game!\n\n"
                                       f"*{p1_name}* vs *{p2_name}*\n\n"
                                       f"*Coin Toss Time!*\n\n"
                                       f"➡️ {p1_name}, call Heads or Tails:",
                                       chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
             except Exception as e:
                 logger.error(f"Failed to edit message after P2 join G{game_id}: {e}")
                 # Consider reverting state or notifying players if edit fails?

        # --- TOSS ---
        elif action == "toss" and current_state == STATE_TOSS:
             if user.id != p1_id:
                 return bot.answer_callback_query(call.id, f"Waiting for {p1_name} to call the toss.")
             if not p2: # Should not happen if state is TOSS, but check anyway
                 logger.error(f"Game {game_id}: Player 2 missing during TOSS state. Cleaning up.")
                 cleanup_game_telebot(game_id, chat_id, reason="internal error - p2 missing")
                 return bot.answer_callback_query(call.id, "Error: Player 2 seems to have left.")

             bot.answer_callback_query(call.id) # Answer first
             choice = value # 'H' or 'T'
             coin_flip = random.choice(['H', 'T'])
             winner = p1 if choice == coin_flip else p2
             game['toss_winner'] = winner['id']
             game['state'] = STATE_BAT_BOWL
             logger.info(f"Game {game_id}: P1 ({p1_name}) chose {choice}, Coin was {'Heads' if coin_flip == 'H' else 'Tails'}. Toss Winner: {winner['name']} ({winner['id']})")

             markup = types.InlineKeyboardMarkup(row_width=2).add(
                 types.InlineKeyboardButton("Bat 🏏", callback_data=f"batorbowl:bat:{game_id}"),
                 types.InlineKeyboardButton("Bowl 🧤", callback_data=f"batorbowl:bowl:{game_id}")
             )
             try:
                 bot.edit_message_text(f"Coin shows: *{'Heads' if coin_flip == 'H' else 'Tails'}*.\n\n"
                                       f"🎉 *{winner['name']}* won the toss!\n\n"
                                       f"➡️ {winner['name']}, choose whether to Bat first or Bowl first:",
                                       chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
             except Exception as e:
                 logger.error(f"Failed to edit message after toss G{game_id}: {e}")

        # --- BAT/BOWL ---
        elif action == "batorbowl" and current_state == STATE_BAT_BOWL:
             toss_winner_id = game.get('toss_winner')
             if not toss_winner_id: # Should not happen
                 logger.error(f"Game {game_id}: toss_winner missing in BAT_BOWL state. Cleaning up.")
                 cleanup_game_telebot(game_id, chat_id, reason="internal error - toss winner missing")
                 return bot.answer_callback_query(call.id, "Error: Toss winner information lost.")
             if user.id != toss_winner_id:
                 winner_player = p1 if toss_winner_id == p1_id else p2
                 return bot.answer_callback_query(call.id, f"Waiting for {winner_player['name']} (toss winner) to choose.")
             if not p2: # Double check P2 still present
                 logger.error(f"Game {game_id}: Player 2 missing during BAT_BOWL state. Cleaning up.")
                 cleanup_game_telebot(game_id, chat_id, reason="internal error - p2 missing")
                 return bot.answer_callback_query(call.id, "Error: Player 2 seems to have left.")

             bot.answer_callback_query(call.id) # Answer first
             choice = value # 'bat' or 'bowl'
             winner = p1 if toss_winner_id == p1_id else p2
             loser = p2 if toss_winner_id == p1_id else p1

             batter = winner if choice == "bat" else loser
             bowler = loser if choice == "bat" else winner

             game.update({
                 'current_batter': batter['id'],
                 'current_bowler': bowler['id'],
                 'innings': 1,
                 'state': STATE_P1_BAT, # P1 always bats first logically in state machine, even if P2 is batting
                 'p1_score': 0,
                 'p2_score': 0,
                 'target': None,
                 'ball_count': 0 # Reset ball count for Innings 1
             })
             logger.info(f"Game {game_id}: {winner['name']} chose to {choice}. {batter['name']} ({batter['id']}) will bat first.")

             markup = create_standard_keyboard_telebot(game_id)
             try:
                 bot.edit_message_text(f"Alright! {winner['name']} chose to *{choice}* first.\n\n"
                                       f"*--- Innings 1 ---*\n"
                                       f"Target: To Be Determined\n\n"
                                       f"🏏 Batting: *{batter['name']}*\n"
                                       f"🧤 Bowling: *{bowler['name']}*\n"
                                       f"Score: 0 (Balls: 0)\n\n"
                                       f"➡️ {batter['name']}, select your shot (1-6):",
                                       chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
             except Exception as e:
                 logger.error(f"Failed to edit message after bat/bowl choice G{game_id}: {e}")

        # --- Number Choice (Game Turn) ---
        elif action == "num":
            # Determine expected states based on current innings
            expected_batter_state = STATE_P1_BAT if game['innings'] == 1 else STATE_P2_BAT
            expected_bowler_state = STATE_P1_BOWL_WAIT if game['innings'] == 1 else STATE_P2_BOWL_WAIT
            number_chosen = value # The number (1-6) chosen by the user

            if not p2: # Critical check: P2 must exist for the game to proceed
                logger.error(f"Game {game_id}: Player 2 missing during number input state ({current_state}). Cleaning up.")
                cleanup_game_telebot(game_id, chat_id, reason="internal error - p2 missing")
                return bot.answer_callback_query(call.id, "Error: Your opponent seems to have left the game.")

            # Identify current batter and bowler
            batter_id = game['current_batter']; bowler_id = game['current_bowler']
            batter_player = p1 if batter_id == p1_id else p2
            bowler_player = p1 if bowler_id == p1_id else p2
            batter_id_str = str(batter_id); bowler_id_str = str(bowler_id) # For DB updates
            batter_name = batter_player['name']; bowler_name = bowler_player['name']
            current_ball_count = game.get('ball_count', 0) # Get current ball count before potential increment

            # --- Batter's Turn ---
            # --- Batter's Turn ---
            if current_state == expected_batter_state:
                if user.id != batter_id:
                    return bot.answer_callback_query(call.id, f"It's {batter_name}'s turn to bat.")
                if game.get('batter_choice') is not None:
                    return bot.answer_callback_query(call.id, "Waiting for the bowler to bowl.")

                bot.answer_callback_query(call.id, f"You played {number_chosen}. Waiting for the bowler...")
                game['batter_choice'] = number_chosen
                game['state'] = expected_bowler_state # Transition state

                current_game_score = game['p1_score'] if batter_id == p1_id else game['p2_score']
                target_text = f" | Target: *{game['target']}*" if game.get('target') else ""
                innings_text = f"*--- Innings {game['innings']} ---*{target_text}\n"
                markup = create_standard_keyboard_telebot(game_id) # Keep keyboard for bowler

                # --- CORRECTED TEXT ---
                # Displays "Played" but NOT the number chosen, hiding it from the bowler
                text = (f"{innings_text}\n"
                        f"🏏 Bat: {batter_name} (Played)\n" # <-- CORRECTED: Hides the number
                        f"🧤 Bowl: {bowler_name}\n\n"
                        f"Score: {current_game_score} (Balls: {current_ball_count})\n\n" # Show balls *before* increment
                        f"➡️ {bowler_name}, select your delivery (1-6):")
                # --- END CORRECTED TEXT ---
                try:
                    bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
                except Exception as e:
                     logger.error(f"Failed to edit message for bowler's turn G{game_id}: {e}")
            # --- Bowler's Turn ---
            elif current_state == expected_bowler_state:
                 if user.id != bowler_id:
                     return bot.answer_callback_query(call.id, f"It's {bowler_name}'s turn to bowl.")

                 bat_number = game.get('batter_choice')
                 # Safety check: Batter's choice should exist
                 if bat_number is None:
                    logger.error(f"Game {game_id}: CRITICAL - batter_choice is None in bowler state. Reverting state.")
                    game['state'] = expected_batter_state # Revert state
                    try:
                        # Inform players of the error
                        bot.edit_message_text(f"⚠️ Error: Batter's choice was lost. {batter_name}, please select your shot again.",
                                              chat_id, message_id, reply_markup=create_standard_keyboard_telebot(game_id))
                    except Exception as edit_err:
                        logger.error(f"Failed to edit message on batter choice error G{game_id}: {edit_err}")
                    return bot.answer_callback_query(call.id, "Error: Batter's choice missing. Please try again.")

                 bot.answer_callback_query(call.id) # Answer bowler's query
                 bowl_number = number_chosen # The bowler's chosen number

                 # --- Process the Ball ---
                 game['ball_count'] += 1 # <<< INCREMENT BALL COUNT HERE >>>
                 current_ball_count = game['ball_count'] # Get updated count for display

                 result_text = f"*{batter_name}* played: `{bat_number}`\n*{bowler_name}* bowled: `{bowl_number}`\n\n"
                 final_message_text = ""; final_markup = None; game_ended = False

                 # -- OUT --
                 if bat_number == bowl_number:
                    result_text += f"💥 *OUT!* {batter_name} is dismissed!\n"
                    logger.info(f"Game {game_id}: OUT! Batter={batter_name}({batter_id}), Bowler={bowler_name}({bowler_id}), Innings={game['innings']}, Ball={current_ball_count}")

                    # Update bowler's wickets stat
                    if add_wicket_to_user(bowler_id_str):
                        logger.info(f"DB: Wicket added successfully for user {bowler_id_str}")
                    else:
                        # Log if DB update fails but continue game
                        if users_collection is not None: logger.warning(f"DB: Failed to add wicket for user {bowler_id_str}")

                    # Check if Innings 1 or 2
                    if game['innings'] == 1:
                        # End of Innings 1
                        current_game_score = game['p1_score'] if batter_id == p1_id else game['p2_score']
                        game['target'] = current_game_score + 1
                        result_text += f"\n*End of Innings 1*. Target for {bowler_name} is *{game['target']}* runs.\n\n"
                        result_text += f"*--- Innings 2 ---*\n"

                        # Swap roles, reset ball count, reset batter choice, change state
                        game.update({
                            'current_batter': bowler_id, # Previous bowler now bats
                            'current_bowler': batter_id, # Previous batter now bowls
                            'innings': 2,
                            'batter_choice': None,       # Clear choice for next innings
                            'state': STATE_P2_BAT,       # Move to P2 batting state
                            'ball_count': 0              # Reset ball count for Innings 2
                        })

                        new_batter_pl = bowler_player; new_bowler_pl = batter_player
                        new_batter_name = new_batter_pl['name']; new_bowler_name = new_bowler_pl['name']
                        # Get score for the *new* batter (which is 0 at start of innings 2)
                        new_batter_game_score = game['p1_score'] if new_batter_pl['id'] == p1_id else game['p2_score']

                        result_text += (f"Target: *{game['target']}*\n\n"
                                       f"🏏 Batting: *{new_batter_name}*\n"
                                       f"🧤 Bowling: *{new_bowler_name}*\n\n"
                                       f"Score: {new_batter_game_score} (Balls: 0)\n\n" # Show 0 balls
                                       f"➡️ {new_batter_name}, select your shot (1-6):")
                        final_message_text = result_text
                        final_markup = create_standard_keyboard_telebot(game_id) # Keyboard for the new batter

                    else: # Out in Innings 2 -> Game Over
                        game_ended = True
                        bat_score = game['p1_score'] if batter_id == p1_id else game['p2_score']
                        target = game['target']
                        p1_final = game['p1_score']; p2_final = game['p2_score'] # Use scores *before* this ball if needed? No, use current.

                        result_text += f"\n*Game Over!*\n\n--- *Final Scores* ---\n"
                        result_text += f"👤 {p1_name}: *{p1_final}*\n"
                        result_text += f"👤 {p2_name}: *{p2_final}*\n\n"

                        # Determine winner (Bowler wins if score is less than target-1, Tie if equal to target-1)
                        if bat_score < target - 1:
                            margin = target - 1 - bat_score
                            result_text += f"🏆 *{bowler_name} wins by {margin} runs!*"
                        elif bat_score == target - 1:
                             result_text += f"🤝 *It's a Tie!* Scores are level."
                        # This case shouldn't happen if out logic is correct (score cannot be >= target if out)
                        # else: result_text += f"Error in win condition calculation."

                        final_message_text = result_text
                        final_markup = None # No more buttons

                 # -- RUNS --
                 else:
                    runs_scored = bat_number # Runs scored = batter's chosen number
                    result_text += f"🏏 Scored *{runs_scored}* runs!\n"
                    logger.info(f"Game {game_id}: Runs! Scored={runs_scored}. Batter={batter_name}({batter_id}), Bowler={bowler_name}({bowler_id}), Innings={game['innings']}, Ball={current_ball_count}")

                    # Update batter's score in the game
                    current_game_score = 0
                    if batter_id == p1_id:
                        game['p1_score'] += runs_scored
                        current_game_score = game['p1_score']
                    else: # batter_id == p2_id
                        game['p2_score'] += runs_scored
                        current_game_score = game['p2_score']

                    # Update batter's total runs stat in DB
                    if add_runs_to_user(batter_id_str, runs_scored):
                        logger.info(f"DB: Added {runs_scored} runs successfully for user {batter_id_str}")
                    else:
                        # Log if DB update fails but continue game
                        if users_collection is not None: logger.warning(f"DB: Failed to add {runs_scored} runs for user {batter_id_str}")

                    # Reset batter's choice for the next ball
                    game['batter_choice'] = None

                    # Check for game end conditions (only in Innings 2)
                    if game['innings'] == 2 and current_game_score >= game['target']:
                        # Target Chased - Game Over
                        game_ended = True
                        p1_final = game['p1_score']; p2_final = game['p2_score']

                        result_text += f"\n*Target Chased! Game Over!*\n\n--- *Final Scores* ---\n"
                        result_text += f"👤 {p1_name}: *{p1_final}*\n"
                        result_text += f"👤 {p2_name}: *{p2_final}*\n\n"
                        result_text += f"🏆 *{batter_name} wins!*" # Batter (chasing team) wins

                        final_message_text = result_text
                        final_markup = None # No more buttons
                    else:
                        # Game continues, prepare for next ball
                        game['state'] = expected_batter_state # Go back to batter's turn state
                        target_text = f" | Target: *{game['target']}*" if game.get('target') else ""
                        innings_text = f"*--- Innings {game['innings']} ---*{target_text}\n"

                        result_text += (f"\n{innings_text}\n"
                                        f"🏏 Batting: *{batter_name}*\n"
                                        f"🧤 Bowling: *{bowler_name}*\n\n"
                                        f"Score: {current_game_score} (Balls: {current_ball_count})\n\n" # Show updated score and ball count
                                        f"➡️ {batter_name}, select your next shot (1-6):")
                        final_message_text = result_text
                        final_markup = create_standard_keyboard_telebot(game_id) # Keyboard for batter again

                 # --- Edit Message with Result ---
                 try:
                     bot.edit_message_text(final_message_text, chat_id, message_id, reply_markup=final_markup, parse_mode="Markdown")
                 except Exception as edit_err:
                      logger.error(f"Failed to edit message {message_id} after ball processing G{game_id}: {edit_err}")
                      # If editing fails, especially on game end, send a new message as fallback
                      if game_ended:
                           try:
                               bot.send_message(chat_id, final_message_text, parse_mode="Markdown")
                               logger.info(f"Sent game end message as fallback for G{game_id}")
                               # Clean up immediately after sending fallback if edit failed
                               cleanup_game_telebot(game_id, chat_id, reason="finished normally", edit_markup=False) # Don't try editing again
                               return # Exit callback processing for this game
                           except Exception as send_err:
                               logger.error(f"Failed to send fallback game end message for G{game_id}: {send_err}")
                      else:
                          # If game continues and edit fails, maybe just log it or try a simple notification?
                          # bot.send_message(chat_id, "Error updating game message. Please use the latest buttons if possible.")
                          pass # For now, just log the error, game state is updated internally

                 # --- Cleanup if Game Ended ---
                 if game_ended:
                     logger.info(f"Game {game_id} finished normally. P1: {game.get('p1_score', 'N/A')}, P2: {game.get('p2_score', 'N/A')}, Target: {game.get('target', 'N/A')}")
                     cleanup_game_telebot(game_id, chat_id, reason="finished normally", edit_markup=False) # Already edited or sent fallback

        # --- Ignore other actions / invalid states ---
        else:
            logger.warning(f"Ignoring game callback action '{action}' in state '{current_state}' for game {game_id}")
            bot.answer_callback_query(call.id) # Acknowledge callback even if ignored

    # --- Catch unexpected errors during game logic ---
    except Exception as e:
        logger.exception(f"!!! CRITICAL Error processing game callback for game {game_id}: Data='{data}', State='{current_state}'")
        try:
            # Inform the user and try to clean up
            bot.answer_callback_query(call.id, "An unexpected error occurred in the game logic.", show_alert=True)
            # Attempt to clean up the game state if an error occurs
            cleanup_game_telebot(game_id, chat_id, reason="critical error", edit_markup=True) # Try to remove buttons
            bot.send_message(chat_id, "🚨 An unexpected error occurred with the cricket game. The game has been stopped. Please start a new one with /cricket.")
        except Exception as inner_e:
            logger.error(f"Error during critical error handling for game {game_id}: {inner_e}")

from datetime import datetime, timezone # Make sure timezone is imported

# Make sure these are imported at the top
from datetime import datetime, timezone

@bot.message_handler(commands=['ping'])
def handle_ping(message: Message):
    """Checks bot latency and optionally DB connection."""
    start_time = datetime.now(timezone.utc)
    ping_msg = None # Initialize ping_msg to None
    try:
        # Send initial message and get its info
        ping_msg = bot.reply_to(message, "⏳ Pinging...")
        send_time = datetime.now(timezone.utc) # Time after sending initial message

        # Check DB status
        db_status = "N/A"
        db_ping_latency_ms = None

        # Simplified check: Do we have a client object from the initial connection attempt?
        if client is not None:
            db_start_time = datetime.now(timezone.utc)
            try:
                # Attempt the ping using the client
                client.admin.command('ping')
                db_end_time = datetime.now(timezone.utc)
                db_ping_latency = db_end_time - db_start_time
                db_ping_latency_ms = round(db_ping_latency.total_seconds() * 1000)
                # If ping succeeds, we assume connection is generally okay
                db_status = "Connected ✅"
            except Exception as db_e:
                logger.warning(f"Ping command DB check failed: {db_e}")
                # If ping fails on an existing client, report connection error
                db_status = f"Error ❌"
        else:
            # If client is None, it means the initial connection likely failed entirely.
            db_status = "Disconnected ⚠️"
            db_ping_latency_ms = None # Ensure this is None if disconnected

        # Calculate bot latency (time from command received to now, excluding DB ping time ideally, but simple diff is okay)
        # Using time difference between start and the send_time gives a better idea of initial processing + send latency
        # Using end_time gives total time including DB check
        end_time = datetime.now(timezone.utc)
        total_latency = end_time - start_time
        total_latency_ms = round(total_latency.total_seconds() * 1000)

        # Format the final message
        ping_text = f"🏓 *Pong!* \n\n" \
                    f"⏱️ Bot Latency: `{total_latency_ms} ms`\n" \
                    f"🗄️ Database: `{db_status}`"
        if db_ping_latency_ms is not None:
             ping_text += f" (Ping: `{db_ping_latency_ms} ms`)"

        # Edit the original message
        if ping_msg: # Ensure ping_msg was successfully created
            bot.edit_message_text(ping_text, chat_id=ping_msg.chat.id, message_id=ping_msg.message_id, parse_mode="Markdown")
        else: # Fallback if initial message failed
             bot.reply_to(message, ping_text, parse_mode="Markdown")


    except Exception as e:
        logger.error(f"Error during /ping command: {e}")
        # Send a fallback message if editing fails or another error occurs
        try:
             # Use the final text if available, otherwise a generic error
             fallback_text = ping_text if 'ping_text' in locals() else "⚠️ An error occurred while checking the ping."
             # Try editing first if ping_msg exists, otherwise reply
             if ping_msg:
                 bot.edit_message_text(fallback_text, chat_id=ping_msg.chat.id, message_id=ping_msg.message_id, parse_mode="Markdown")
             else:
                 bot.reply_to(message, fallback_text, parse_mode="Markdown")
        except Exception as fallback_e:
            logger.error(f"Error sending fallback ping message: {fallback_e}")
            pass # Ignore if even the fallback fails


# --- Start Polling ---
if __name__ == '__main__':
    logger.info("Starting Combined Cricket & Stats Bot (v5 - DM Leaderboards)...")
    if users_collection is None: logger.warning("!!! BOT RUNNING WITHOUT DATABASE CONNECTION - STATS & REGISTRATION DISABLED !!!")
    else: logger.info("Database connection active.")

    # Fetch bot username at startup
    try:
        bot_info = bot.get_me()
        bot_username = bot_info.username
        logger.info(f"Bot username: @{bot_username} (ID: {bot_info.id})")
    except Exception as e:
        logger.critical(f"CRITICAL: Could not fetch bot username on startup: {e}. Leaderboard links will fail.")
        # bot_username remains None, handlers will show an error message

    try:
        logger.info("Starting bot polling...")
        bot.infinity_polling(logger_level=logging.INFO, # Set to DEBUG for more verbose logs if needed
                             long_polling_timeout=5, # How long Telegram server waits before responding if no updates
                             timeout=10) # How long bot waits for response from Telegram server
    except Exception as poll_err:
        logger.critical(f"Bot polling loop crashed: {poll_err}")
    finally:
        # Close MongoDB connection gracefully
        if 'client' in locals() and client:
             try:
                 client.close()
                 logger.info("MongoDB connection closed.")
             except Exception as close_err:
                 logger.error(f"Error closing MongoDB connection: {close_err}")
        logger.info("Bot polling stopped.")

# --- END OF FULLY REVISED FILE with MongoDB, Ball Count & DM Leaderboards (v5) ---
