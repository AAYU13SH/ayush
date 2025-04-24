# --- START OF FULLY REVISED FILE with MongoDB & Ball Count (v4) ---

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
     # exit()

bot = telebot.TeleBot(BOT_TOKEN)

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

# --- Helper Functions --- (Unchanged from previous version)
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

# --- Database Helper Functions --- (Unchanged from previous version)
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

# --- Command Handlers --- (Largely unchanged, only /start welcome text modified)

@bot.message_handler(commands=['start'])
def handle_start(message):
    if message.chat.type != 'private':
         bot.reply_to(message, "Welcome! Use /cricket in a group to play. Use /start in my DM to register for stats.")
         return
    if users_collection is None:
         bot.reply_to(message, "DB connection unavailable. Registration disabled.")
         return
    user = message.from_user; user_id_str = str(user.id)
    mention = f"[{user.full_name}](tg://user?id={user_id_str})"
    if get_user_data(user_id_str):
        register_user(user) # Update details
        bot.reply_to(message, f"{mention}, you are already registered!", parse_mode='markdown')
        return
    if register_user(user):
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton('Channel', url='https://t.me/TCTCRICKET'),
                   InlineKeyboardButton('Group', url='https://t.me/+SIzIYQeMsRsyOWM1'))
        welcome_text = f"Welcome {mention} to the TCT OFFICIAL BOT!\nYou are now registered.\nUse /help for commands."
        bot.send_message(message.chat.id, welcome_text, parse_mode='markdown', reply_markup=markup,
                         link_preview_options=LinkPreviewOptions(is_disabled=True))
        logger.info(f"New user registered: {user.full_name} ({user_id_str})")
        try: # Notify admin
            if xmods: bot.send_message(xmods[0], f"➕ New user: {mention} (`{user_id_str}`)", parse_mode='markdown', link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e: logger.error(f"Could not notify admin: {e}")
    else: bot.reply_to(message, "⚠️ Error during registration.")

@bot.message_handler(commands=['help'])
def help_command(message):
    is_admin = message.from_user.id in xmods
    user_commands = """*User Commands:*
  `/start` - Register (in DM).
  `/help` - This help message.
  `/cricket` - Start game (in group).
  `/cancel` - Cancel your game (in group).
  `/my_achievement` - View stats (reply or DM).
  `/lead_runs` - Top 5 run scorers.
  `/lead_wickets` - Top 5 wicket takers."""
    admin_commands = """*Admin Commands:*
  `/achieve <user_id> <title>` - Add achievement (or reply).
  `/remove_achievement <user_id> <title>` - Remove achievement (or reply).
  `/broad <message>` - Broadcast (or reply).
  `/reduce_runs <user_id> <amount>` - Reduce runs (or reply).
  `/reduce_wickets <user_id> <amount>` - Reduce wickets (or reply).
  `/clear_all_stats` - Reset all stats.
  `/user_count` - Show registered users."""
    help_text = "📜 *Available Commands*\n" + user_commands
    if is_admin: help_text += "\n\n" + admin_commands
    bot.reply_to(message, help_text, parse_mode='Markdown')

@bot.message_handler(commands=['cricket'])
def start_cricket(message):
    user = message.from_user; user_id_str = str(user.id)
    if users_collection is not None and not get_user_data(user_id_str):
         return bot.reply_to(message, f"@{get_player_name_telebot(user)}, please /start me in DM first.")
    elif users_collection is None: logger.warning(f"Game start by {user_id_str} while DB is down.")
    chat_id = message.chat.id; player1_name = get_player_name_telebot(user)
    logger.info(f"User {player1_name} ({user.id}) initiated /cricket in chat {chat_id}")
    # Check existing/active games...
    for gid, gdata in list(games.items()):
        if gdata['chat_id'] == chat_id:
            p1_id = gdata.get('player1', {}).get('id'); p2_id = gdata.get('player2', {}).get('id')
            if gdata['state'] == STATE_WAITING and p1_id == user.id: return bot.reply_to(message, "You already started a game. Use /cancel.")
            if user.id == p1_id or user.id == p2_id: return bot.reply_to(message, "You are already in a game! Use /cancel.")
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
    markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("Join Game", callback_data=f"join:_:{game_id}"))
    try:
        sent_message = bot.send_message(chat_id, f"🏏 Cricket game by {player1_name}!\nWaiting for P2...", reply_markup=markup)
        games[game_id]["message_id"] = sent_message.message_id
    except Exception as e: logger.error(f"Failed send cricket msg game {game_id}: {e}"); games.pop(game_id, None)

@bot.message_handler(commands=['cancel'])
def cancel_cricket(message):
    user = message.from_user; chat_id = message.chat.id; game_to_cancel_id = None
    logger.info(f"User {get_player_name_telebot(user)} ({user.id}) /cancel in chat {chat_id}")
    for gid, gdata in list(games.items()):
        if gdata['chat_id'] == chat_id:
             p1_id = gdata.get('player1', {}).get('id'); p2_id = gdata.get('player2', {}).get('id')
             if user.id == p1_id or user.id == p2_id: game_to_cancel_id = gid; break
    if game_to_cancel_id:
        logger.info(f"Cancelling game {game_to_cancel_id} by user {user.id}")
        cleanup_game_telebot(game_to_cancel_id, chat_id, reason="cancelled by user")
        bot.reply_to(message, "Cricket game cancelled.")
    else: bot.reply_to(message, "You aren't in an active game here.")

# ... (Broadcast, Achievement, Stat modification, Leaderboard commands remain unchanged from previous version) ...
# --- Broadcast Command (Admin) ---
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

# --- Achievement Commands ---
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
    bot.reply_to(message, f"🏅 Add \"*{html.escape(title)}*\" to user `{target_user_id_str}`?", reply_markup=markup, parse_mode="markdown")

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
    bot.reply_to(message, f"🗑️ Remove \"*{html.escape(title)}*\" from user `{target_user_id_str}`?", reply_markup=markup, parse_mode="markdown")

@bot.message_handler(commands=['my_achievement'])
def view_my_stats_and_achievements(message):
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    target_user = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid_str = str(target_user.id); user_data = get_user_data(uid_str)
    if user_data is None: return bot.reply_to(message, "User not registered. /start in DM.")
    runs = user_data.get("runs", 0); wickets = user_data.get("wickets", 0); achievements = user_data.get("achievements", [])
    name = user_data.get("full_name") or get_player_name_telebot(target_user)
    mention = f"[{name}](tg://user?id={uid_str})"; stats_text = f"📊 Stats for {mention}:\n  🏏 Runs: *{runs}*\n  🎯 Wickets: *{wickets}*"
    achievement_text = "\n\n🏆 *Achievements*";
    if achievements: achievement_text += f" ({len(achievements)}):\n" + "\n".join([f"  🏅 `{html.escape(str(title))}`" for title in achievements])
    else: achievement_text += ":\n  *None yet.*"
    bot.reply_to(message, stats_text + achievement_text, parse_mode="markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))

# --- Stat Modification Commands (Admin) ---
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
        if user_doc: new_runs = user_doc.get("runs", 0); name = user_doc.get("full_name") or f"user {uid_str}"; mention = f"[{name}](tg://user?id={uid_str})"; bot.reply_to(message, f"✅ Reduced *{amount}* runs from {mention}. New total: *{new_runs}*.", parse_mode="Markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))
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
         if user_doc: new_wickets = user_doc.get("wickets", 0); name = user_doc.get("full_name") or f"user {uid_str}"; mention = f"[{name}](tg://user?id={uid_str})"; bot.reply_to(message, f"✅ Reduced *{amount}* wickets from {mention}. New total: *{new_wickets}*.", parse_mode="Markdown", link_preview_options=LinkPreviewOptions(is_disabled=True))
         else: bot.reply_to(message, f"⚠️ User `{uid_str}` not found.")
    except Exception as e: logger.error(f"DB error reducing wickets for {uid_str}: {e}"); bot.reply_to(message, "⚠️ DB error.")

@bot.message_handler(commands=['clear_all_stats'])
def clear_all_stats(message):
    if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    markup = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⚠️ YES, CLEAR ALL STATS ⚠️", callback_data="confirm_clear_stats"), types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_clear_stats"))
    bot.reply_to(message, "🚨 *DANGER ZONE* 🚨\nClear ALL runs/wickets? Cannot be undone.", reply_markup=markup, parse_mode="Markdown")

@bot.message_handler(commands=['user_count'])
def user_count(message):
     if message.from_user.id not in xmods: return bot.reply_to(message, "❌ Not authorized.")
     if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
     try: count = users_collection.count_documents({}); bot.reply_to(message, f"👥 Registered users in database: {count}")
     except Exception as e: logger.error(f"DB error counting users: {e}"); bot.reply_to(message, "⚠️ Error counting users.")

# --- Leaderboard Commands ---
def get_user_mention_from_db(user_doc):
    if not user_doc: return "Unknown User"
    uid_str = user_doc.get("_id"); name = user_doc.get("full_name", f"User {uid_str}")
    return f"[{name}](tg://user?id={uid_str})"

@bot.message_handler(commands=['lead_runs'])
def show_runs_leaderboard(message: Message):
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    try:
        top = list(users_collection.find({"runs": {"$gt": 0}}, {"_id": 1, "full_name": 1, "runs": 1}).sort("runs", -1).limit(5))
        if not top: return bot.reply_to(message, "🏏 No runs scored yet.")
        medals = ['🥇', '🥈', '🥉', '🏅', '🏅']; txt = "🏆 *Top 5 Run Scorers:*\n\n"
        for i, u in enumerate(top): txt += f"{medals[i] if i<len(medals) else '🔹'} {get_user_mention_from_db(u)} - *{u.get('runs', 0)}* runs\n"
        bot.reply_to(message, txt, parse_mode='Markdown', link_preview_options=LinkPreviewOptions(is_disabled=True))
    except Exception as e: logger.error(f"DB error runs leaderboard: {e}"); bot.reply_to(message, "⚠️ Error fetching leaderboard.")

@bot.message_handler(commands=['lead_wickets'])
def show_wickets_leaderboard(message: Message):
    if users_collection is None: return bot.reply_to(message, "⚠️ DB unavailable.")
    try:
        top = list(users_collection.find({"wickets": {"$gt": 0}}, {"_id": 1, "full_name": 1, "wickets": 1}).sort("wickets", -1).limit(5))
        if not top: return bot.reply_to(message, "🎯 No wickets taken yet.")
        medals = ['🥇', '🥈', '🥉', '🏅', '🏅']; txt = "🎯 *Top 5 Wicket Takers:*\n\n"
        for i, u in enumerate(top): txt += f"{medals[i] if i<len(medals) else '🔹'} {get_user_mention_from_db(u)} - *{u.get('wickets', 0)}* wickets\n"
        bot.reply_to(message, txt, parse_mode='Markdown', link_preview_options=LinkPreviewOptions(is_disabled=True))
    except Exception as e: logger.error(f"DB error wickets leaderboard: {e}"); bot.reply_to(message, "⚠️ Error fetching leaderboard.")


# --- Central Callback Query Handler ---
@bot.callback_query_handler(func=lambda call: True)
def handle_callback_query(call):
    user = call.from_user; chat_id = call.message.chat.id
    message_id = call.message.message_id; data = call.data
    logger.debug(f"Callback: Data='{data}', User={user.id}, Chat={chat_id}, Msg={message_id}")

    # --- Achievement & Stat Clear Callbacks ---
    if data.startswith("ach_") or data == "confirm_clear_stats" or data == "cancel_clear_stats":
        if users_collection is None: return bot.answer_callback_query(call.id, "Database unavailable.", show_alert=True)
        if data.startswith("ach_"): # Achievement
            parts = data.split("_", 4)
            if parts[1] == "cancel": bot.edit_message_text("❌ Operation cancelled.", chat_id, message_id, reply_markup=None); return bot.answer_callback_query(call.id)
            if len(parts) < 5 or user.id not in xmods: return bot.answer_callback_query(call.id, "Invalid/Unauthorized.")
            action, mode, user_id_str, encoded_title = parts[1], parts[2], parts[3], parts[4]; title = urllib.parse.unquote(encoded_title)
            try:
                msg = "DB Error."; res = None
                if mode == "add": res = users_collection.update_one({"_id": user_id_str}, {"$addToSet": {"achievements": title}}, upsert=False)
                elif mode == "remove": res = users_collection.update_one({"_id": user_id_str}, {"$pull": {"achievements": title}})
                if res:
                    if res.matched_count == 0: msg = f"⚠️ User `{user_id_str}` not found."
                    elif res.modified_count == 0: msg = f"⚠️ No changes made (already exists/doesn't exist)."
                    elif mode == "add": msg = f"✅ Added \"*{html.escape(title)}*\" to `{user_id_str}`."; logger.info(f"Admin {user.id} added ach '{title}' for {user_id_str}")
                    elif mode == "remove": msg = f"🗑️ Removed \"*~~{html.escape(title)}~~*\" from `{user_id_str}`."; logger.info(f"Admin {user.id} removed ach '{title}' for {user_id_str}")
                bot.edit_message_text(msg, chat_id, message_id, parse_mode="markdown", reply_markup=None)
            except Exception as e: logger.error(f"DB error ach callback {data}: {e}"); bot.edit_message_text("⚠️ DB error.", chat_id, message_id)
            return bot.answer_callback_query(call.id)
        elif data == "confirm_clear_stats": # Stat Clear Confirm
            if user.id not in xmods: return bot.answer_callback_query(call.id, "Not authorized.")
            try: res = users_collection.update_many({}, {"$set": {"runs": 0, "wickets": 0}}); bot.edit_message_text(f"🧹 Stats cleared for {res.modified_count} users!", chat_id, message_id, reply_markup=None); logger.warning(f"Admin {user.id} cleared stats ({res.modified_count})."); return bot.answer_callback_query(call.id, "Stats cleared!")
            except Exception as e: logger.error(f"DB error clearing stats: {e}"); bot.edit_message_text("⚠️ Error clearing stats.", chat_id, message_id); return bot.answer_callback_query(call.id, "DB error.")
        elif data == "cancel_clear_stats": # Stat Clear Cancel
            bot.edit_message_text("❌ Stat clearing cancelled.", chat_id, message_id, reply_markup=None); return bot.answer_callback_query(call.id)
    # --- End Achievement/Stat Clear ---


    # --- Cricket Game Callbacks ---
    try: action, value_str, game_id = data.split(":", 2); value = int(value_str) if value_str.isdigit() else value_str
    except ValueError: return bot.answer_callback_query(call.id) # Ignore non-game format

    if game_id not in games:
        logger.warning(f"Callback ignored: Game {game_id} not found.");
        try: bot.edit_message_text("This game session has ended.", chat_id, message_id, reply_markup=None)
        except Exception: pass
        return bot.answer_callback_query(call.id, "Game session ended.")

    game = games[game_id]
    if message_id != game.get("message_id"):
         logger.warning(f"Callback ignored: Stale msg ID game {game_id}.");
         return bot.answer_callback_query(call.id, "Use buttons on the latest message.")

    # --- Game State Machine ---
    current_state = game.get('state'); p1 = game['player1']; p2 = game.get('player2')
    p1_name = p1['name']; p2_name = p2['name'] if p2 else "P2"
    logger.debug(f"Processing game cb '{action}' game {game_id} state '{current_state}' user {user.id}")

    try:
        # --- JOIN ---
        if action == "join" and current_state == STATE_WAITING:
             bot.answer_callback_query(call.id)
             if user.id == p1['id'] or game.get('player2'): return # Prevent self-join or double-join
             user_id_str = str(user.id)
             if users_collection is not None and not get_user_data(user_id_str): return bot.send_message(chat_id, f"@{get_player_name_telebot(user)}, please /start me first.", reply_parameters=ReplyParameters(message_id=message_id))
             player2_name = get_player_name_telebot(user)
             game['player2'] = {"id": user.id, "name": player2_name, "user_obj": user}
             game['state'] = STATE_TOSS; logger.info(f"P2 ({player2_name} - {user.id}) joined game {game_id}.")
             markup = types.InlineKeyboardMarkup(row_width=2).add(types.InlineKeyboardButton("Heads", callback_data=f"toss:H:{game_id}"), types.InlineKeyboardButton("Tails", callback_data=f"toss:T:{game_id}"))
             bot.edit_message_text(f"✅ {player2_name} joined!\n\n*Coin Toss Time!*\n\nPlayer 1 ({p1_name}), call Heads or Tails:", chat_id, message_id, reply_markup=markup, parse_mode="Markdown")

        # --- TOSS ---
        elif action == "toss" and current_state == STATE_TOSS:
             if user.id != p1['id']: return bot.answer_callback_query(call.id, f"Waiting for {p1_name}.")
             if not p2: logger.error(f"G{game_id}: P2 miss TOSS"); cleanup_game_telebot(game_id, chat_id); return bot.answer_callback_query(call.id, "Error: P2 left?")
             bot.answer_callback_query(call.id); choice = value; coin_flip = random.choice(['H', 'T'])
             winner = p1 if choice == coin_flip else p2; game['toss_winner'] = winner['id']; game['state'] = STATE_BAT_BOWL
             logger.info(f"G{game_id}: P1 chose {choice}, Flip={coin_flip}. Winner: {winner['name']}")
             markup = types.InlineKeyboardMarkup(row_width=2).add(types.InlineKeyboardButton("Bat 🏏", callback_data=f"batorbowl:bat:{game_id}"), types.InlineKeyboardButton("Bowl 🧤", callback_data=f"batorbowl:bowl:{game_id}"))
             bot.edit_message_text(f"Coin: *{'Heads' if coin_flip == 'H' else 'Tails'}*.\n🎉 {winner['name']} won toss!\n\nChoose Bat or Bowl:", chat_id, message_id, reply_markup=markup, parse_mode="Markdown")

        # --- BAT/BOWL ---
        elif action == "batorbowl" and current_state == STATE_BAT_BOWL:
             if user.id != game['toss_winner']: winner_pl = p1 if game['toss_winner'] == p1['id'] else p2; return bot.answer_callback_query(call.id, f"Wait {winner_pl['name']}.")
             if not p2: logger.error(f"G{game_id}: P2 miss BAT_BOWL"); cleanup_game_telebot(game_id, chat_id); return bot.answer_callback_query(call.id, "Error: P2 left?")
             bot.answer_callback_query(call.id); choice = value; winner = p1 if game['toss_winner'] == p1['id'] else p2; loser = p2 if game['toss_winner'] == p1['id'] else p1
             batter = winner if choice == "bat" else loser; bowler = loser if choice == "bat" else winner
             game.update({'current_batter': batter['id'], 'current_bowler': bowler['id'], 'innings': 1, 'state': STATE_P1_BAT, 'p1_score': 0, 'p2_score': 0, 'target': None, 'ball_count': 0}) # Reset ball count
             logger.info(f"G{game_id}: {batter['name']} bats first.")
             markup = create_standard_keyboard_telebot(game_id)
             bot.edit_message_text(f"OK! {batter['name']} bats first.\n\n*--- Innings 1 ---*\nTarget: TBD\n\n🏏 Bat: {batter['name']}\n🧤 Bowl: {bowler['name']}\nScore: 0 (Balls: 0)\n\n➡️ {batter['name']}, select shot (1-6):", chat_id, message_id, reply_markup=markup, parse_mode="Markdown") # Show initial ball count

        # --- Number Choice (Game Turn) ---
        elif action == "num":
            expected_batter_state = STATE_P1_BAT if game['innings'] == 1 else STATE_P2_BAT
            expected_bowler_state = STATE_P1_BOWL_WAIT if game['innings'] == 1 else STATE_P2_BOWL_WAIT
            number_chosen = value

            if not p2: logger.error(f"G{game_id}: P2 miss NUM"); cleanup_game_telebot(game_id, chat_id); return bot.answer_callback_query(call.id, "Error: P2 missing.")

            batter_id = game['current_batter']; bowler_id = game['current_bowler']
            batter_player = p1 if batter_id == p1['id'] else p2; bowler_player = p1 if bowler_id == p1['id'] else p2
            batter_id_str = str(batter_id); bowler_id_str = str(bowler_id)
            batter_name = batter_player['name']; bowler_name = bowler_player['name']
            current_ball_count = game.get('ball_count', 0) # Get current ball count

            # --- Batter's Turn ---
            if current_state == expected_batter_state:
                if user.id != batter_id: return bot.answer_callback_query(call.id, f"Wait {batter_name}.")
                if game.get('batter_choice') is not None: return bot.answer_callback_query(call.id, "Waiting for bowler.")
                bot.answer_callback_query(call.id, f"You chose {number_chosen}. Waiting...")
                game['batter_choice'] = number_chosen; game['state'] = expected_bowler_state
                current_game_score = game['p1_score'] if batter_id == p1['id'] else game['p2_score']
                target_text = f" | Target: {game['target']}" if game.get('target') else ""
                innings_text = f"*--- Innings {game['innings']} ---*{target_text}\n"
                markup = create_standard_keyboard_telebot(game_id)
                text = (f"{innings_text}\n🏏 Bat: {batter_name} (Played)\n🧤 Bowl: {bowler_name}\n\n"
                        f"Score: {current_game_score} (Balls: {current_ball_count})\n\n➡️ {bowler_name}, select delivery (1-6):") # Show current balls
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")

            # --- Bowler's Turn ---
            elif current_state == expected_bowler_state:
                 if user.id != bowler_id: return bot.answer_callback_query(call.id, f"Wait {bowler_name}.")
                 bat_number = game.get('batter_choice')
                 if bat_number is None:
                    logger.error(f"G{game_id}: batter_choice missing!"); game['state'] = expected_batter_state # Revert
                    try: bot.edit_message_text("Error: Batter lost choice. Batter, choose again.", chat_id, message_id, reply_markup=create_standard_keyboard_telebot(game_id))
                    except Exception: pass; return bot.answer_callback_query(call.id, "Error: Batter choice missing.")

                 bot.answer_callback_query(call.id); bowl_number = number_chosen
                 game['ball_count'] += 1 # <<< INCREMENT BALL COUNT HERE >>>
                 current_ball_count = game['ball_count'] # Get updated count

                 result_text = f"{batter_name} played: {bat_number}\n{bowler_name} bowled: {bowl_number}\n\n"
                 final_message_text = ""; final_markup = None; game_ended = False

                 # -- OUT --
                 if bat_number == bowl_number:
                    result_text += f"💥 *OUT!* {batter_name} dismissed!\n\n"
                    logger.info(f"G{game_id}: OUT! B={batter_id}, BW={bowler_id}, I{game['innings']}, Ball {current_ball_count}")
                    if add_wicket_to_user(bowler_id_str): logger.info(f"DB: Wicket++ {bowler_id_str}")
                    if game['innings'] == 1: # End Innings 1
                        current_game_score = game['p1_score'] if batter_id == p1['id'] else game['p2_score']
                        game['target'] = current_game_score + 1
                        result_text += f"End Innings 1. Target: *{game['target']}*.\n\n*--- Innings 2 ---*\n"
                        game.update({'current_batter': bowler_id, 'current_bowler': batter_id, 'innings': 2, 'batter_choice': None, 'state': STATE_P2_BAT, 'ball_count': 0}) # Reset ball count for Innings 2
                        new_batter_pl = bowler_player; new_bowler_pl = batter_player
                        new_batter_name = new_batter_pl['name']; new_bowler_name = new_bowler_pl['name']
                        new_batter_game_score = game['p1_score'] if new_batter_pl['id'] == p1['id'] else game['p2_score']
                        result_text += (f"Target: {game['target']}\n\n🏏 Bat: {new_batter_name}\n🧤 Bowl: {new_bowler_name}\n\n"
                                       f"Score: {new_batter_game_score} (Balls: 0)\n\n➡️ {new_batter_name}, select shot (1-6):") # Show 0 balls
                        final_message_text = result_text; final_markup = create_standard_keyboard_telebot(game_id)
                    else: # Out in Innings 2 -> Game Over
                        game_ended = True
                        bat_score = game['p1_score'] if batter_id == p1['id'] else game['p2_score']; target = game['target']
                        p1_final = game['p1_score']; p2_final = game['p2_score']
                        result_text += f"*Game Over!*\n\n--- *Final Scores* ---\n👤 {p1_name}: {p1_final}\n👤 {p2_name}: {p2_final}\n\n"
                        if bat_score < target - 1: margin = target - 1 - bat_score; result_text += f"🏆 *{bowler_name} wins by {margin} runs!*"
                        elif bat_score == target - 1: result_text += f"🤝 *It's a Tie!*"
                        final_message_text = result_text; final_markup = None

                 # -- RUNS --
                 else:
                    runs_scored = bat_number
                    result_text += f"🏏 Scored *{runs_scored}* runs!\n\n"
                    logger.info(f"G{game_id}: Runs! {runs_scored}. B={batter_id}, BW={bowler_id}, I{game['innings']}, Ball {current_ball_count}")
                    current_game_score = 0
                    if batter_id == p1['id']: game['p1_score'] += runs_scored; current_game_score = game['p1_score']
                    else: game['p2_score'] += runs_scored; current_game_score = game['p2_score']
                    if add_runs_to_user(batter_id_str, runs_scored): logger.info(f"DB: Runs+{runs_scored} {batter_id_str}")
                    game['batter_choice'] = None
                    if game['innings'] == 2 and current_game_score >= game['target']: # Target chased
                        game_ended = True; p1_final = game['p1_score']; p2_final = game['p2_score']
                        result_text += f"*Target Chased! Game Over!*\n\n--- *Final Scores* ---\n👤 {p1_name}: {p1_final}\n👤 {p2_name}: {p2_final}\n\n🏆 *{batter_name} wins!*"
                        final_message_text = result_text; final_markup = None
                    else: # Continue batting
                        game['state'] = expected_batter_state
                        target_text = f" | Target: {game['target']}" if game.get('target') else ""
                        innings_text = f"*--- Innings {game['innings']} ---*{target_text}\n"
                        result_text += (f"{innings_text}\n🏏 Bat: {batter_name}\n🧤 Bowl: {bowler_name}\n\n"
                                        f"Score: {current_game_score} (Balls: {current_ball_count})\n\n➡️ {batter_name}, select shot (1-6):") # Show current balls
                        final_message_text = result_text; final_markup = create_standard_keyboard_telebot(game_id)

                 # --- Edit Message ---
                 try: bot.edit_message_text(final_message_text, chat_id, message_id, reply_markup=final_markup, parse_mode="Markdown")
                 except Exception as edit_err:
                      logger.error(f"Failed edit msg {message_id} G{game_id}: {edit_err}")
                      if game_ended: bot.send_message(chat_id, final_message_text, parse_mode="Markdown"); cleanup_game_telebot(game_id, chat_id, reason="finished normally", edit_markup=False)
                      else: bot.send_message(chat_id, "Error updating game. Use latest msg or /cancel.")

                 if game_ended: cleanup_game_telebot(game_id, chat_id, reason="finished normally", edit_markup=False)

        # --- Ignore other actions ---
        else: bot.answer_callback_query(call.id)

    # --- Catch errors ---
    except Exception as e:
        logger.exception(f"!!! Critical Error processing game callback game {game_id}: Data={data}")
        try: bot.answer_callback_query(call.id, "Unexpected game error.")
        except Exception as ie: logger.error(f"Error answering critical game error cb: {ie}")


# --- Start Polling ---
if __name__ == '__main__':
    logger.info("Starting Combined Cricket & Stats Bot with MongoDB...")
    if users_collection is None: logger.warning("!!! BOT RUNNING WITHOUT DATABASE CONNECTION !!!")
    try:
        logger.info(f"Bot username: @{bot.get_me().username}")
        bot.infinity_polling(logger_level=logging.INFO, long_polling_timeout=5, timeout=10)
    except Exception as poll_err: logger.critical(f"Polling loop crashed: {poll_err}")
    finally:
        if 'client' in locals() and client:
             try: client.close(); logger.info("MongoDB connection closed.")
             except Exception as close_err: logger.error(f"Error closing MongoDB connection: {close_err}")
        logger.info("Bot polling stopped.")

# --- END OF FULLY REVISED FILE with MongoDB & Ball Count (v4) ---
