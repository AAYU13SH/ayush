#6882194604

import telebot
from telebot import types
import time

# Replace 'YOUR_TOKEN' with your actual Telegram Bot token
bot = telebot.TeleBot("7167579834:gAAFmpwPdKHT6eE4aELSOB8CC8gi_tPrutPA")

# Dictionary to store whether a user has joined each group
user_groups = {}

started_users = set()

# List to store banned user IDs
banned_users = set()

# Handler for /start command
@bot.message_handler(commands=['start'])
def handle_start(message):
    if str(message.from_user.id) in banned_users:
        bot.reply_to(message, "YOU ARE BANNED")
    else:
        user_id = message.from_user.id
        user_name = message.from_user.first_name
        if user_groups.get(user_id, False):
            bot.reply_to(message, "Do /cmds for commands")
        else:
                if not all(user_groups.get(user_id, {}).values()):
                    bot.send_message(message.chat.id, "Please join all groups first by clicking the buttons below.")
                else:
                    welcome_message = ("🔸Welcome, {} To Auction Bot\n\n"
                            "🔸You Can Submit Your Pokemon Through This Bot For Auction\n\n"
                            "🔻But Before Using You Have To Join Our Auction Group By Clicking Below Two Buttons "
                            "And Then Click 'Joined' Button").format(user_name)
                markup = types.InlineKeyboardMarkup(row_width=1)
                auction_group_button = types.InlineKeyboardButton("Join Auction Group", url="https://t.me/DBA_HEXA_AUCTION")
                trade_group_button = types.InlineKeyboardButton("Join Trade Group", url="https://t.me/DBA_HEXA_TRADE")
                joined_button = types.InlineKeyboardButton("Joined", callback_data="joined")
                markup.add(auction_group_button, trade_group_button, joined_button)
                bot.send_photo(message.chat.id, open('https://telegra.ph//file/33a7bd56c58e085ddbee4.jpg', 'rb'), caption=welcome_message, reply_markup=markup)
                user_id = message.from_user.id
                started_users.add(user_id)
                user_id = message.from_user.id
                user_ids.add(user_id)

# Handler for 'Joined' button callback
@bot.callback_query_handler(func=lambda call: call.data == "joined")
def joined_callback(call):
    user_id = call.from_user.id
    if user_id not in user_groups:
        user_groups[user_id] = {}
    user_groups[user_id][call.message.chat.id] = True
    bot.answer_callback_query(call.id, "Thanks for joining our group")

# Handler for /cmds command
@bot.message_handler(commands=['cmds'])
def handle_cmds(message):
    if str(message.from_user.id) in banned_users:
        bot.reply_to(message, "YOU ARE BANNED")
    else:
        user_id = message.from_user.id
        if user_groups.get(user_id, False):
            bot.reply_to(message, '''
                     USER COMMANDS : - 

/start - Start The Bot
/add - Send Poke / TMs For Auction
/cancel - Cancel All Running Cmds
/item - List Of All Items In Auc
/natures - Get Natures Page 
/cmds - Get This Message
                     
ADMINS COMMANDS : - 
                     
/users - Get All Bot Users List
/list - Get List Of All Items In Auction 
/ban - Ban Any User
/unban - Unban Any User
/msg - Send Message To User
/approve - Make Someone Bot Admin
/broad - Send Message To All Bot 
/next - Send Next Item In Auction
/unapprove - Remove Someone Bot Admin
                     
OWNER COMMANDS :-

/clear - For Bot Owner''')
        else:
            bot.reply_to(message, "Please join our groups first by clicking the buttons in the start message.")

# List to store active user IDs
active_users = []

# Admin IDs
admin_ids = ["6882194604", "6843210459"]

# Handler for /users 
# Handler for /msg command
@bot.message_handler(commands=['msg'])
def handle_msg(message):
    # Check if the user is an admin
    if message.from_user.id not in admin_id:
        bot.reply_to(message, "You are not authorized to use this command.")
        return

    # Extract user ID and message from the command
    try:
        _, user_id, user_message = message.text.split(maxsplit=2)
        user_id = int(user_id)
    except ValueError:
        bot.reply_to(message, "Invalid syntax. Use /msg (user_id) (message)")
        return

    # Send the message if the user exists
    try:
        bot.send_message(user_id, user_message)
        bot.reply_to(message, f"Message sent to user {user_id}")
    except Exception as e:
        bot.reply_to(message, f"Failed to send message to user {user_id}: {e}")

# Set to store user IDs
user_ids = set()

# Handler for /broad command (for admin only)
@bot.message_handler(commands=['broad'])
def handle_broad(message):
    if str(message.from_user.id) not in admin_ids:
        bot.reply_to(message, "You are not authorized to use this command.")
        return

    # Extract message from the command
    try:
        _, broad_message = message.text.split(maxsplit=1)
    except ValueError:
        bot.reply_to(message, "Invalid syntax. Use /broad (message)")
        return

    # Forward the message to all users
    for user_id in user_ids:
        try:
            bot.send_message(user_id, broad_message)
        except Exception as e:
            print(f"Failed to send message to user {user_id}: {e}")

    bot.reply_to(message, "Message forwarded to all users.")

# Handler for /approve command (for admin only)
@bot.message_handler(commands=['approve'])
def handle_approve(message):
    if str(message.from_user.id) not in admin_ids:
        bot.reply_to(message, "You are not authorized to use this command.")
        return
    # Extract user ID from the command
    try:
        _, user_id = message.text.split(maxsplit=1)
        admin_ids.append(user_id)  # Add the new admin ID to the list of admin IDs (in string form)
        admin_id.append(int(user_id))  # Add the new admin ID to the list of admin IDs (in integer form)
        bot.reply_to(message, f"User with ID {user_id} has been successfully added to admin list.")
    except ValueError:
        bot.reply_to(message, "Invalid syntax. Use /approve <user_id>")

# Handler for /ban command (for admin only)
@bot.message_handler(commands=['ban'])
def handle_ban(message):
    if str(message.from_user.id) not in admin_ids:
        bot.reply_to(message, "You are not authorized to use this command.")
        return

    # Extract user ID from the command
    try:
        _, user_id = message.text.split(maxsplit=1)
        banned_users.add(user_id)  # Add the user ID to the set of banned users
        bot.reply_to(message, f"User with ID {user_id} has been banned.")
    except ValueError:
        bot.reply_to(message, "Invalid syntax. Use /ban <user_id>")

# Handler for /unban command (for admin only)
@bot.message_handler(commands=['unban'])
def handle_unban(message):
    if str(message.from_user.id) not in admin_ids:
        bot.reply_to(message, "You are not authorized to use this command.")
        return

    # Extract user ID from the command
    try:
        _, user_id = message.text.split(maxsplit=1)
        if user_id in banned_users:
            banned_users.remove(user_id)  # Remove the user ID from the set of banned users
            bot.reply_to(message, f"User with ID {user_id} has been unbanned.")
        else:
            bot.reply_to(message, f"User with ID {user_id} is not banned.")
    except ValueError:
        bot.reply_to(message, "Invalid syntax. Use /unban <user_id>")

# Handler for /unapprove command (for admin only)
@bot.message_handler(commands=['unapprove'])
def handle_unapprove(message):
    if str(message.from_user.id) not in admin_ids:
        bot.reply_to(message, "You are not authorized to use this command.")
        return

    # Extract user ID from the command
    try:
        _, user_id = message.text.split(maxsplit=1)
        if user_id in admin_ids:
            admin_ids.remove(user_id)  # Remove the user ID from the list of admin IDs (string form)
            admin_id.remove(int(user_id))  # Remove the user ID from the set of admin IDs (int form)
            bot.reply_to(message, f"User with ID {user_id} has been removed from the admin list.")
        else:
            bot.reply_to(message, f"User with ID {user_id} is not an admin.")
    except ValueError:
        bot.reply_to(message, "Invalid syntax. Use /unapprove <user_id>")

# Define a dictionary to store the counts of different item categories
item_counts = {
    "legendary": 0,
    "non_legendary": 0,
    "shiny": 0,
    "tms": 0,
    "teams": 0,
    "total_items": 0
}

# Check if the user is an admin
def is_admin(user_id):
    return user_id in admin_id

# Define the /item command handler
@bot.message_handler(commands=['item'])
def item(message):
    response = "🔹Currently Items In Auction - \n"
    response += "🔺Legendary: " + str(item_counts["legendary"]) + "\n"
    response += "🔺Non-Legendary: " + str(item_counts["non_legendary"]) + "\n"
    response += "🔺Shiny: " + str(item_counts["shiny"]) + "\n"
    response += "🔺TMs: " + str(item_counts["tms"]) + "\n"
    response += "🔺Teams: " + str(item_counts["teams"]) + "\n"
    response += "\n🔹Total Items: " + str(item_counts["total_items"]) + "\n"

    bot.reply_to(message, response)

# Define the command handlers for adding items to different categories
def add_item(message, category):
    user_id = message.from_user.id
    if is_admin(user_id):
        item_counts[category] += 1
        item_counts["total_items"] += 1
        bot.reply_to(message, f"Added 1 item to {category.capitalize()} category.")
    else:
        bot.reply_to(message, "Only admins can perform this action.")

@bot.message_handler(commands=['legendary'])
def add_legendary(message):
    add_item(message, "legendary")

@bot.message_handler(commands=['non_legendary'])
def add_non_legendary(message):
    add_item(message, "non_legendary")

@bot.message_handler(commands=['shiny'])
def add_shiny(message):
    add_item(message, "shiny")

@bot.message_handler(commands=['tms'])
def add_tms(message):
    add_item(message, "tms")

@bot.message_handler(commands=['teams'])
def add_teams(message):
    add_item(message, "teams")

# Define a dictionary to store the items in each category
approved_items = {
    "legendary": [],
    "non_legendary": [],
    "shiny": [],
    "tms": [],
    "teams": []
}

# Check if the user is an admin
def is_admin(user_id):
    return user_id in admin_id

# Define the ID of the group to send messages to
group_id = -1002041160221 # Replace this with the ID of your group

# Check if the user is an admin
def is_admin(user_id):
    return user_id in admin_id

# Define the /send command handler
@bot.message_handler(commands=['send'])
def send_message_prompt(message):
    if is_admin(message.from_user.id):
        bot.reply_to(message, "Type the message to send in the group")
        bot.register_next_step_handler(message, send_message)
    else:
        bot.reply_to(message, "Only admins can perform this action.")

# Define the function to send or forward the message to the group
def send_message(message):
    if message.forward_from or message.forward_from_chat:
        forwarded_message = message
    else:
        forwarded_message = message.text
    try:
        bot.forward_message(group_id, message.chat.id, message.id)
        bot.send_message(message.chat.id, "Message sent successfully.")
    except Exception as e:
        bot.send_message(message.chat.id, f"Failed to send message: {e}")


# Dictionary to store messages and their corresponding chat IDs
stored_messages = {}

# Define the /store command handler
@bot.message_handler(commands=['store'])
def store_message_prompt(message):
    if is_admin(message.from_user.id):
        bot.reply_to(message, "Type the message you want to store:")
        bot.register_next_step_handler(message, store_message)
    else:
        bot.reply_to(message, "Only admins can perform this action.")

def store_message(message):
    stored_messages[message.message_id] = {"message": message.text, "chat_id": message.chat.id}
    bot.reply_to(message, "Message stored successfully.")

# Define the /next command handler
@bot.message_handler(commands=['next'])
def next_message(message):
    if is_admin(message.from_user.id):
        if stored_messages:
            next_message_id = next(iter(stored_messages))
            next_message_data = stored_messages.pop(next_message_id)
            bot.forward_message(message.chat.id, next_message_data["chat_id"], next_message_id)
        else:
            bot.reply_to(message, "No more stored messages.")
    else:
        bot.reply_to(message, "Only admins can perform this action.")

# Define the handle_dot function
@bot.message_handler(func=lambda message: message.text == "." and (message.chat.type == "group" or message.chat.type == "supergroup") and str(message.from_user.id) in admin_ids)
def handle_dot(message):
    msg = bot.send_message(message.chat.id, "•")
    time.sleep(1.5)
    bot.edit_message_text("• •", message.chat.id, msg.message_id)
    time.sleep(1.5)
    bot.edit_message_text("• • •", message.chat.id, msg.message_id)
    time.sleep(1.5)
    keyboard = types.InlineKeyboardMarkup()
    yes_button = types.InlineKeyboardButton(text="Yes", callback_data="sell_pokemon")
    keyboard.add(yes_button)
    bot.edit_message_text("Do you want to sell the Pokemon?", message.chat.id, msg.message_id, reply_markup=keyboard)

# Define the sell_pokemon_callback function
@bot.callback_query_handler(func=lambda call: call.data == "sell_pokemon")
def sell_pokemon_callback(call):
    bot.answer_callback_query(call.id, "Pokemon has been sold!")
    bot.send_message(call.message.chat.id, "🔊 Pokemon Has Been Sold")

# Define the handle_sold function
@bot.message_handler(commands=['sold'])
def handle_sold(message):
    if is_admin(message.from_user.id):
        try:
            command, *args = message.text.split(' ', 1)
            if len(args) != 1:
                raise ValueError
            pokemon_name = args[0]
            username = message.reply_to_message.from_user.username
            amount = message.reply_to_message.text
            reply_message = f"🔊 {pokemon_name} Has Been Sold\n\n🔸Sold to - @{username}\n🔸Sold for - {amount}\n\n❗ Join Trade Group To Get Seller Username After Auction"
            sent_message = bot.reply_to(message, reply_message)
            bot.pin_chat_message(message.chat.id, sent_message.id)  # Pin the message
        except ValueError:
            bot.reply_to(message, "Please provide the command in the format /sold (pokemon name)")
    else:
        bot.reply_to(message, "You are not authorized to use this command.")

# Define the handle_unsold function
@bot.message_handler(commands=['unsold'])
def handle_unsold(message):
    if is_admin(message.from_user.id):
        try:
            pokemon_name = message.text.split(' ', 1)[1]
            reply_message = f"❌ {pokemon_name} Has Been Unsold"
            sent_message = bot.reply_to(message, reply_message)
            bot.pin_chat_message(message.chat.id, sent_message.id)  # Pin the message
        except IndexError:
            bot.reply_to(message, "Please provide the name of the Pokemon to mark as unsold.")
    else:
        bot.reply_to(message, "You are not authorized to use this command.")

# Enable privacy mode
bot.skip_pending = True

bot_owner_id = "6882194604"

# Check if the user is the bot owner
def is_bot_owner(user_id):
    return str(user_id) == bot_owner_id

# Define the /clear command handler
@bot.message_handler(commands=['clear'])
def clear_messages(message):
    if is_bot_owner(message.from_user.id):
        stored_messages.clear()
        bot.reply_to(message, "All stored messages have been cleared.")
    else:
        bot.reply_to(message, "You are not authorized to use this command.")

# Flag variable to indicate cancellation
cancel_requested = False

# Define the /cancel command handler
@bot.message_handler(commands=['cancel'])
def cancel_command(message):
    global cancel_requested
    cancel_requested = True
    bot.reply_to(message, "All Running Commands Have Been Cancelled ✅")

submissions = {}

# Define the /add command handler
@bot.message_handler(commands=['add'])
def add_command(message):
    # Create inline keyboard with options
    keyboard = types.InlineKeyboardMarkup()
    keyboard.row(
        types.InlineKeyboardButton("Legendary", callback_data="legendary"),
        types.InlineKeyboardButton("Non-Legendary", callback_data="non_legendary"),
        types.InlineKeyboardButton("Shiny", callback_data="shiny"),
        types.InlineKeyboardButton("TMs", callback_data="tms"),
        types.InlineKeyboardButton("Teams", callback_data="teams")
    )

    # Send message to select type of item
    bot.reply_to(message, "Select the type of item you want to add:", reply_markup=keyboard)

# Handle inline button callbacks
@bot.callback_query_handler(func=lambda call: call.data in ["legendary", "non_legendary", "shiny", "tms", "teams"])
def callback_handler(call):
    # Store type of item selected
    submissions[call.message.chat.id] = {"type": call.data}

    if call.data in ["legendary", "non_legendary", "shiny"]:
        # Ask for name of the Pokemon
        bot.send_message(call.message.chat.id, "What is the name of the Pokemon you want to sell?")
        bot.register_next_step_handler(call.message, forward_info)
    elif call.data == "tms":
        # Ask for name of the TM
        bot.send_message(call.message.chat.id, "Enter the name of your TM:")
        bot.register_next_step_handler(call.message, forward_tm_info)
    elif call.data == "teams":
        # Ask for name of the team
        bot.send_message(call.message.chat.id, "What is the name of your team?")
        bot.register_next_step_handler(call.message, forward_team_info)

# Handle next step after getting name of pokemon
def forward_info(message):
    submissions[message.chat.id]["name"] = message.text

    # Ask for info of the pokemon
    bot.send_message(message.chat.id, f"Please provide info of {message.text} by copying the text and pasting it here")
    bot.register_next_step_handler(message, forward_iv_ev)

# Handle next step after getting info of pokemon
def forward_iv_ev(message):
    submissions[message.chat.id]["info"] = message.text

    # Ask for IVs/EVs of the pokemon
    bot.send_message(message.chat.id, f"Please provide IVs/EVs of {submissions[message.chat.id]['name']} by copying the text and pasting it here")
    bot.register_next_step_handler(message, forward_moveset)

# Handle next step after getting IVs/EVs of pokemon
def forward_moveset(message):
    submissions[message.chat.id]["iv_ev"] = message.text

    # Ask for moveset of the pokemon
    bot.send_message(message.chat.id, f"Please provide the moveset of {submissions[message.chat.id]['name']} by copying the text and pasting it here")
    bot.register_next_step_handler(message, ask_iv_boosted)

# Handle next step after getting moveset of pokemon
def ask_iv_boosted(message):
    submissions[message.chat.id]["moveset"] = message.text

    # Ask if IVs are boosted
    bot.send_message(message.chat.id, "Is any IV boosted of this Pokemon?")
    bot.register_next_step_handler(message, ask_base)

# Handle next step after asking if IVs are boosted
def ask_base(message):
    # Store if IVs are boosted
    submissions[message.chat.id]["iv_boosted"] = message.text

    # Ask for base for the Pokemon
    bot.send_message(message.chat.id, "Tell me the base for your Pokemon?")
    bot.register_next_step_handler(message, send_submission)

# Handle next step after asking for base
def send_submission(message):
    # Store base for the Pokemon
    submissions[message.chat.id]["base"] = message.text

    # Send confirmation message
    bot.send_message(message.chat.id, f"Your Pokemon {submissions[message.chat.id]['name']} has been sent for submission")

    # Send all data to admin
    send_to_admin(message.chat.id)

# Handle next step after getting TM info
def forward_tm_info(message):
    submissions[message.chat.id]["name"] = message.text

    # Ask for base for the TM
    bot.send_message(message.chat.id, "Tell me the base for your TM:")
    bot.register_next_step_handler(message, send_submission)

# Handle next step after getting team info
def forward_team_info(message):
    submissions[message.chat.id]["name"] = message.text

    # Ask for team members
    bot.send_message(message.chat.id, "Send the list of team members:")
    bot.register_next_step_handler(message, forward_team_members)

# Handle next step after getting team members
def forward_team_members(message):
    submissions[message.chat.id]["members"] = message.text

    # Ask for base for the team
    bot.send_message(message.chat.id, "Tell me the base for your team:")
    bot.register_next_step_handler(message, send_submission)

# Function to send submission to admin
def send_to_admin(chat_id):
    submission_data = submissions[chat_id]
    admin_chat_id = "6882194604"
    
    # Send header indicating new submission
    bot.send_message(admin_chat_id, "New Submission:")

    # Send each piece of submission data individually
    bot.send_message(admin_chat_id, f"Type: {submission_data['type']}")
    if submission_data['type'] in ["legendary", "non_legendary", "shiny"]:
        bot.send_message(admin_chat_id, f"Name: {submission_data['name']}")
        bot.send_message(admin_chat_id, f"Info: {submission_data['info']}")
        bot.send_message(admin_chat_id, f"IVs/EVs: {submission_data['iv_ev']}")
        bot.send_message(admin_chat_id, f"Moveset: {submission_data['moveset']}")
        bot.send_message(admin_chat_id, f"IV Boosted: {submission_data['iv_boosted']}")
        bot.send_message(admin_chat_id, f"Base: {submission_data['base']}")
    elif submission_data['type'] == "tms":
        bot.send_message(admin_chat_id, f"Name: {submission_data['name']}")
        bot.send_message(admin_chat_id, f"Base: {submission_data['base']}")
    elif submission_data['type'] == "teams":
        bot.send_message(admin_chat_id, f"Name: {submission_data['name']}")
        bot.send_message(admin_chat_id, f"Members: {submission_data['members']}")
        bot.send_message(admin_chat_id, f"Base: {submission_data['base']}")
    bot.send_message(admin_chat_id, f"User ID: {chat_id}")

# Start polling
bot.polling()
