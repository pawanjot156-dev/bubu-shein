fromme os
import logging
import asyncio
import random
import string
from datetime import datetime
from typing import Dict, Tuple, Optional, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ChatMember
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ChatMemberHandler
from telegram.constants import ParseMode
import httpx
from supabase import create_client, Client
from aiohttp import web

# ================= CONFIG =================
TOKEN = "8658584483:AAFISnruz9eyC96ciYMoVhQaC1LqA53xcqk"
SUPABASE_URL = "https://fmwgnrarimdbuhnqsqck.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZtd2ducmFyaW1kYnVobnFzcWNrIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mzk5NTQyMCwiZXhwIjoyMDg5NTcxNDIwfQ.5r4atM9MWNW5olT4zkUTxn7Hove78dZ1OqyTh6BM6-o"
WEBHOOK_URL = "https://bubu-shein.onrender.com/webhook"

# Add your admin IDs here
ADMIN_IDS = [8301446634]

VERIFY_SITE_URL = "https://bubu-shein.onrender.com/v"

DEFAULT_WITHDRAW_POINTS = 3

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ================= SUPABASE INIT =================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ================= HELPER FUNCTIONS =================
async def is_user_joined_channels(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is member of all force-join channels."""
    try:
        channels = supabase.table("channels").select("chat_id, channel_link").execute()
    except Exception as e:
        logger.error(f"Supabase query error in is_user_joined_channels: {e}")
        channels = supabase.table("channels").select("channel_link").execute()

    if not channels.data:
        return True

    all_joined = True
    for ch in channels.data:
        chat_id = ch.get("chat_id")
        try:
            if chat_id:
                member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    all_joined = False
                    break
            else:
                # fallback to username from link
                link = ch["channel_link"]
                chat_username = link.split("/")[-1]
                member = await context.bot.get_chat_member(chat_id=f"@{chat_username}", user_id=user_id)
                if member.status not in ["member", "administrator", "creator"]:
                    all_joined = False
                    break
        except Exception as e:
            logger.error(f"Error checking channel {ch}: {e}")
            all_joined = False
            break
    return all_joined

async def is_user_verified(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    user = supabase.table("users").select("verified").eq("user_id", user_id).execute()
    return user.data and user.data[0].get("verified", False)

async def get_referral_link(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    bot_username = (await context.bot.get_me()).username
    return f"https://t.me/{bot_username}?start={user_id}"

def get_withdraw_points() -> int:
    res = supabase.table("admin_settings").select("value").eq("key", "withdraw_points").execute()
    if res.data:
        return int(res.data[0]["value"])
    return DEFAULT_WITHDRAW_POINTS

def set_withdraw_points(points: int):
    supabase.table("admin_settings").upsert({"key": "withdraw_points", "value": str(points)}).execute()

async def require_verified(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is verified AND still in all channels. If not, block and possibly deduct."""
    user_id = update.effective_user.id
    if user_id in ADMIN_IDS:
        return True

    # First check verified status
    user = supabase.table("users").select("verified, referred_by").eq("user_id", user_id).execute()
    if not (user.data and user.data[0].get("verified", False)):
        await update.message.reply_text("❌ You need to verify first. Use /start to begin.")
        return False

    # Then check channel membership
    if not await is_user_joined_channels(user_id, context):
        # If user left, show force join and possibly deduct from referrer
        await show_force_join_message(update, context)

        # Check if this user has a referrer – deduct one point
        referred_by = user.data[0].get("referred_by")
        if referred_by:
            logger.info(f"Verified user {user_id} left channels, deducting point from referrer {referred_by}")
            # await deduct_referral_bonus(referred_by, user_id, context.bot)
        return False
    return True

# ================= FORCE JOIN HANDLERS =================
async def show_force_join_message(update, context):
    user_id = update.effective_user.id

    channels = supabase.table("channels").select("channel_link, chat_id").execute()

    text = "<b>🚨 Force Join Required</b>\n\nJoin remaining channels:\n"
    keyboard = []
    row = []

    all_joined = True

    for ch in channels.data:
        link = ch["channel_link"]
        chat_id = ch.get("chat_id")

        joined = False

        try:
            if chat_id:
                member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                if member.status in ["member", "administrator", "creator"]:
                    joined = True
            else:
                username = link.split("/")[-1]
                member = await context.bot.get_chat_member(chat_id=f"@{username}", user_id=user_id)
                if member.status in ["member", "administrator", "creator"]:
                    joined = True
        except:
            joined = False

        if joined:
            continue

        all_joined = False

        row.append(InlineKeyboardButton("🔗 Join", url=link))

        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    if all_joined:
        keyboard = [[InlineKeyboardButton("✅ I have joined all", callback_data="joined_all")]]
        text = "<b>✅ All channels joined!</b>\n\nClick below to continue."
    else:
        keyboard.append([InlineKeyboardButton("✅ I have joined all", callback_data="joined_all")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def joined_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if await is_user_joined_channels(user_id, context):
        url = f"{VERIFY_SITE_URL.rstrip('/')}?user_id={user_id}"
        keyboard = [[InlineKeyboardButton("🛑 VERIFY NOW", url=url)]]
        await query.edit_message_text(
            "<b>✅ You have joined all channels!</b>\n\n<b>🛑 Verification required:</b> Click below to verify.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    else:
        await query.edit_message_text("❌ You haven't joined all channels yet. Please join and try again.")

# ================= BOT COMMANDS =================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    args = context.args

    # Handle referral
    if args and args[0].isdigit():
        referrer_id = int(args[0])
        if referrer_id != user_id:
            existing = supabase.table("users").select("user_id").eq("user_id", user_id).execute()
            if not existing.data:
                supabase.table("users").insert({
                    "user_id": user_id,
                    "username": username,
                    "points": 0,
                    "referrals": 0,
                    "referred_by": referrer_id,
                    "verified": False
                }).execute()
                logger.info(f"User {user_id} referred by {referrer_id}")

    # Ensure user exists in DB
    existing = supabase.table("users").select("user_id").eq("user_id", user_id).execute()
    if not existing.data:
        supabase.table("users").insert({
            "user_id": user_id,
            "username": username,
            "points": 0,
            "referrals": 0,
            "referred_by": None,
            "verified": False
        }).execute()
        logger.info(f"New user {user_id} created")

    # If already verified and in channels, show menu
    if await is_user_verified(user_id) and await is_user_joined_channels(user_id, context):
        await show_main_menu(update, context)
        return

    # If not verified but in channels, show verify button
    if await is_user_joined_channels(user_id, context):
        url = f"{VERIFY_SITE_URL.rstrip('/')}?user_id={user_id}"
        keyboard = [[InlineKeyboardButton("🛑 VERIFY NOW", url=url)]]
        await update.message.reply_text(
            "<b>✅ You have joined all channels!</b>\n\n<b>🛑 Verification required:</b> Click below to verify.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    else:
        await show_force_join_message(update, context)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [
        [KeyboardButton("💰 BALANCE"), KeyboardButton("🤝 REFER")],
        [KeyboardButton("🎁 WITHDRAW"), KeyboardButton("📜 MY VOUCHERS")],
        [KeyboardButton("📦 STOCK"), KeyboardButton("🏆 LEADERBOARD")]
    ]
    if user_id in ADMIN_IDS:
        keyboard.append([KeyboardButton("👑 ADMIN PANEL")])
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("<b>🏠 Main Menu</b>", reply_markup=reply_markup, parse_mode=ParseMode.HTML)

# ================= BALANCE =================
async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_verified(update, context):
        return
    user_id = update.effective_user.id
    user = supabase.table("users").select("points, referrals").eq("user_id", user_id).execute().data[0]
    points = user["points"]
    referrals = user["referrals"]
    voucher_cost = get_withdraw_points()
    text = f"<b>💰 Your Points</b>\n\n⭐ Points: {points}\n👥 Referrals: {referrals}\n\n🎁 Voucher Cost: {voucher_cost} point(s)"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# ================= REFER =================
async def refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_verified(update, context):
        return
    user_id = update.effective_user.id
    link = await get_referral_link(user_id, context)
    text = f"<b>🤝 Refer & Earn</b>\n\nInvite friends using your link:\n<code>{link}</code>\n\n✅ Each verified user gives you +1 point."
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# ================= WITHDRAW =================
async def withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_verified(update, context):
        return
    user_id = update.effective_user.id
    user = supabase.table("users").select("points").eq("user_id", user_id).execute().data[0]
    points = user["points"]
    cost = get_withdraw_points()
    if points < cost:
        await update.message.reply_text(f"❌ You need {cost} points to withdraw. You have {points}.")
        return
    keyboard = [[InlineKeyboardButton("📜 AGREE AND GET CODE", callback_data="agree_withdraw")]]
    await update.message.reply_text(
        "<b>📜 Terms & Conditions (Shein):</b>\n\n1️⃣ This Coupon Will Apply Only On SheinVerse Products.\n\nDo you agree to spend?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

async def agree_withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    username = query.from_user.username or f"user_{user_id}"

    coupon = supabase.table("coupons").select("code").eq("used", False).limit(1).execute()
    if not coupon.data:
        await query.edit_message_text("❌ No coupons available. Contact admin.")
        return
    code = coupon.data[0]["code"]

    supabase.table("coupons").update({"used": True, "used_by": user_id, "used_at": datetime.utcnow().isoformat()}).eq("code", code).execute()

    cost = get_withdraw_points()
    user = supabase.table("users").select("points").eq("user_id", user_id).execute().data[0]
    new_points = user["points"] - cost
    supabase.table("users").update({"points": new_points}).eq("user_id", user_id).execute()

    text = f"<b>🎉 Shein Code Generated Successfully!</b>\n\n🎫 Code: <code>{code}</code>\n🛍️ <a href='https://www.sheinindia.in/c/sverse-5939-37961?query=%3Arelevance%3Agenderfilter%3AMen&gridColumns=5#main-content'>Order Here</a>\n\n⚠️ Copy the code and use it immediately."
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    # Notify all admins
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"<b>🛍️ Coupon Redeemed</b>\n\nUser: {username} (<code>{user_id}</code>)\nCode: <code>{code}</code>\nTime: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

# ================= MY VOUCHERS =================
async def my_vouchers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_verified(update, context):
        return
    user_id = update.effective_user.id
    vouchers = supabase.table("coupons").select("code, used_at").eq("used_by", user_id).order("used_at", desc=True).execute()
    if not vouchers.data:
        await update.message.reply_text("<b>📜 MY VOUCHERS</b>\n\n━━━━━━━━━━━━━━━━━━━━\nNo vouchers yet.\n━━━━━━━━━━━━━━━━━━━━\n📊 Total: 0", parse_mode=ParseMode.HTML)
        return
    lines = [f"🎫 <code>{v['code']}</code> (used: {v['used_at'][:10]})" for v in vouchers.data]
    total = len(vouchers.data)
    text = "<b>📜 MY VOUCHERS</b>\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines) + "\n━━━━━━━━━━━━━━━━━━━━\n📊 Total: " + str(total)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# ================= STOCK =================
async def stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_verified(update, context):
        return
    count = supabase.table("coupons").select("code", count="exact").eq("used", False).execute().count
    await update.message.reply_text(f"<b>📦 STOCK</b>\n\nSHEIN COUPON - {count}", parse_mode=ParseMode.HTML)

# ================= LEADERBOARD =================
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_verified(update, context):
        return
    user_id = update.effective_user.id
    top = supabase.table("users").select("username, referrals").order("referrals", desc=True).limit(10).execute().data
    lines = []
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    for i, u in enumerate(top):
        name = u["username"] or f"user_{u['user_id']}"
        lines.append(f"{medals[i]} {name}\n     └ {u['referrals']} referrals")
    all_users = supabase.table("users").select("user_id, referrals").order("referrals", desc=True).execute().data
    rank = 1
    for u in all_users:
        if u["user_id"] == user_id:
            break
        rank += 1
    user_ref = supabase.table("users").select("referrals").eq("user_id", user_id).execute().data[0]["referrals"]
    text = "<b>🏆 Top 10 Leaderboard</b>\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines) + f"\n━━━━━━━━━━━━━━━━━━━━\n📍 Your Rank: {rank} | {user_ref} referrals"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# ================= REFERRAL BONUS HANDLER =================
async def grant_referral_bonus(referrer_id: int, referred_id: int, bot):
    logger.info(f"Granting referral bonus to {referrer_id} for {referred_id}")
    referrer = supabase.table("users").select("points, referrals").eq("user_id", referrer_id).execute().data
    if not referrer:
        logger.error(f"Referrer {referrer_id} not found")
        return
    referrer = referrer[0]
    new_points = referrer["points"] + 1
    new_refs = referrer["referrals"] + 1
    supabase.table("users").update({"points": new_points, "referrals": new_refs}).eq("user_id", referrer_id).execute()
    logger.info(f"Granted +1 point to {referrer_id} (now {new_points} points, {new_refs} referrals)")
    try:
        await bot.send_message(
            chat_id=referrer_id,
            text="<b>🎉 Referral Bonus!</b>\n\n💰 Earned +1 pt(s)\n✅ Full reward credited!\n\n⚠️ Note: If this user leaves any channel, your point will be deducted automatically.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Failed to notify referrer {referrer_id}: {e}")

async def deduct_referral_bonus(referrer_id: int, referred_id: int, bot):
    logger.info(f"Deducting referral bonus from {referrer_id} because {referred_id} left")
    referrer = supabase.table("users").select("points, referrals").eq("user_id", referrer_id).execute().data
    if not referrer:
        logger.error(f"Referrer {referrer_id} not found")
        return
    referrer = referrer[0]
    new_points = max(referrer["points"] - 1, 0)
    new_refs = max(referrer["referrals"] - 1, 0)
    supabase.table("users").update({"points": new_points, "referrals": new_refs}).eq("user_id", referrer_id).execute()
    logger.info(f"Deducted 1 point from {referrer_id} (now {new_points} points, {new_refs} referrals)")
    try:
        await bot.send_message(
            chat_id=referrer_id,
            text="<b>⚠️ Referral Left Channels!</b>\n\n💰 Lost -1 pt(s)\n❌ Reward deducted!\n\n⚠️ Note: A referred user has left a required channel.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Failed to notify referrer {referrer_id}: {e}")

# ================= ADMIN PANEL =================
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    keyboard = [
        [KeyboardButton("📢 BROADCAST"), KeyboardButton("➕ ADD COUPON")],
        [KeyboardButton("➖ REMOVE COUPON"), KeyboardButton("➕ ADD CHANNEL")],
        [KeyboardButton("➖ REMOVE CHANNEL"), KeyboardButton("🎟️ GET A FREE CODE")],
        [KeyboardButton("💰 CHANGE WITHDRAW POINTS")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("<b>👑 Admin Panel</b>", reply_markup=reply_markup, parse_mode=ParseMode.HTML)

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("📢 Send the message you want to broadcast to all users:")
    context.user_data["awaiting_broadcast"] = True

async def add_coupon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("📤 Send the coupons line by line (one code per line):")
    context.user_data["awaiting_coupon_add"] = True

async def remove_coupon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("🔢 Send the number of coupons to remove (will delete oldest unused coupons):")
    context.user_data["awaiting_coupon_remove"] = True

async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("🔗 Send the channel link (e.g., https://t.me/username):")
    context.user_data["awaiting_channel_add"] = True

async def handle_add_channel_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting_channel_add"):
        link = update.message.text.strip()
        try:
            if "t.me/" in link:
                username = link.split("t.me/")[-1].split("?")[0].split("/")[0]
                chat = await context.bot.get_chat(chat_id=f"@{username}")
                chat_id = chat.id
                supabase.table("channels").insert({
                    "channel_link": link,
                    "chat_id": chat_id
                }).execute()
                await update.message.reply_text(f"✅ Channel added with ID {chat_id}.")
            else:
                await update.message.reply_text("❌ Invalid link format. Use https://t.me/username")
        except Exception as e:
            logger.error(f"Error adding channel: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
        context.user_data.pop("awaiting_channel_add")
        return True
    return False

async def remove_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("🔗 Send the channel link to remove:")
    context.user_data["awaiting_channel_remove"] = True

async def handle_remove_channel_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting_channel_remove"):
        link = update.message.text.strip()
        try:
            supabase.table("channels").delete().eq("channel_link", link).execute()
            await update.message.reply_text("✅ Channel removed.")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        context.user_data.pop("awaiting_channel_remove")
        return True
    return False

async def get_free_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("🔢 How many coupons do you need?")
    context.user_data["awaiting_free_code"] = True

async def change_withdraw_points(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("💰 Send the new number of points required to withdraw:")
    context.user_data["awaiting_withdraw_points"] = True

async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        return

    if await handle_add_channel_input(update, context):
        return
    if await handle_remove_channel_input(update, context):
        return

    text = update.message.text

    if context.user_data.get("awaiting_broadcast"):
        users = supabase.table("users").select("user_id").execute().data
        success = 0
        failed = 0
        for u in users:
            try:
                await context.bot.send_message(chat_id=u["user_id"], text=text, parse_mode=ParseMode.HTML)
                success += 1
            except:
                failed += 1
        await update.message.reply_text(f"✅ Broadcast sent.\nSuccess: {success}\nFailed: {failed}")
        context.user_data.pop("awaiting_broadcast")
        return

    if context.user_data.get("awaiting_coupon_add"):
        codes = text.strip().split("\n")
        inserted = 0
        for code in codes:
            code = code.strip()
            if code:
                try:
                    supabase.table("coupons").insert({"code": code, "used": False}).execute()
                    inserted += 1
                except:
                    pass
        await update.message.reply_text(f"✅ Added {inserted} coupons.")
        context.user_data.pop("awaiting_coupon_add")
        return

    if context.user_data.get("awaiting_coupon_remove"):
        try:
            num = int(text)
        except:
            await update.message.reply_text("❌ Invalid number.")
            return
        coupons = supabase.table("coupons").select("id").eq("used", False).order("id").limit(num).execute().data
        ids = [c["id"] for c in coupons]
        if ids:
            supabase.table("coupons").delete().in_("id", ids).execute()
            await update.message.reply_text(f"✅ Removed {len(ids)} coupons.")
        else:
            await update.message.reply_text("❌ No unused coupons found.")
        context.user_data.pop("awaiting_coupon_remove")
        return

    if context.user_data.get("awaiting_free_code"):
        try:
            num = int(text)
        except:
            await update.message.reply_text("❌ Invalid number.")
            return
        coupons = supabase.table("coupons").select("code").eq("used", False).limit(num).execute().data
        codes = [c["code"] for c in coupons]
        if not codes:
            await update.message.reply_text("❌ No unused coupons.")
            return
        for code in codes:
            supabase.table("coupons").update({"used": True, "used_by": user_id, "used_at": datetime.utcnow().isoformat()}).eq("code", code).execute()
        await update.message.reply_text(f"✅ Here are your {len(codes)} codes:\n" + "\n".join(codes))
        context.user_data.pop("awaiting_free_code")
        return

    if context.user_data.get("awaiting_withdraw_points"):
        try:
            points = int(text)
        except:
            await update.message.reply_text("❌ Invalid number.")
            return
        set_withdraw_points(points)
        await update.message.reply_text(f"✅ Withdraw points updated to {points}.")
        context.user_data.pop("awaiting_withdraw_points")
        return

# ================= CHAT MEMBER HANDLER (track leaves) =================
async def track_channel_membership(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat_member = update.chat_member
        if not chat_member:
            return

        logger.info(f"=== CHAT MEMBER UPDATE ===")
        logger.info(f"Chat: {chat_member.chat.id} ({chat_member.chat.title})")
        logger.info(f"User: {chat_member.new_chat_member.user.id} ({chat_member.new_chat_member.user.full_name})")
        logger.info(f"Old status: {chat_member.old_chat_member.status}")
        logger.info(f"New status: {chat_member.new_chat_member.status}")

        channels = supabase.table("channels").select("chat_id").execute()
        if not channels.data:
            return

        channel_ids = [ch["chat_id"] for ch in channels.data if ch.get("chat_id")]
        if not channel_ids:
            return

        chat_id = chat_member.chat.id
        if chat_id not in channel_ids:
            return

        user_id = chat_member.new_chat_member.user.id
        old_status = chat_member.old_chat_member.status
        new_status = chat_member.new_chat_member.status

        if old_status in ["member", "administrator", "creator"] and new_status in ["left", "kicked"]:
            logger.info(f"✅ User {user_id} LEFT channel {chat_id}")

            user_data = supabase.table("users").select("referred_by").eq("user_id", user_id).execute().data
            if not user_data:
                logger.info(f"User {user_id} not in database")
                return
            referrer_id = user_data[0].get("referred_by")
            if not referrer_id:
                logger.info(f"User {user_id} has no referrer")
                return

            logger.info(f"User {user_id} referred by {referrer_id}, deducting point")
           # await deduct_referral_bonus(referrer_id, user_id, context.bot)
    except Exception as e:
        logger.error(f"Exception in track_channel_membership: {e}", exc_info=True)

# ================= ADMIN TEST COMMAND =================
async def test_deduct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to manually test deduction: /testdeduct user_id"""
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not context.args:
        await update.message.reply_text("Usage: /testdeduct <user_id>")
        return
    try:
        user_id = int(context.args[0])
        # Find referrer of this user
        user = supabase.table("users").select("referred_by").eq("user_id", user_id).execute().data
        if not user or not user[0].get("referred_by"):
            await update.message.reply_text("User has no referrer.")
            return
        referrer_id = user[0]["referred_by"]
        await deduct_referral_bonus(referrer_id, user_id, context.bot)
        await update.message.reply_text(f"Deducted 1 point from {referrer_id} for referred user {user_id}.")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

# ================= VERIFICATION PAGE (SELF-HOSTED) =================
async def verification_page(request):
    user_id = request.query.get('user_id', '')
    bot = request.app.get('bot')
    bot_username = "YOUR_BOT_USERNAME"
    if bot:
        try:
            me = await bot.get_me()
            bot_username = me.username
        except:
            pass

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
    <meta http-equiv="Pragma" content="no-cache">
    <meta http-equiv="Expires" content="0">
    <title>Verify Your Device</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: Arial, sans-serif; }}
        body {{ background: linear-gradient(135deg, #667eea, #764ba2); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 20px; }}
        .container {{ background: white; border-radius: 20px; padding: 40px; max-width: 500px; width: 100%; box-shadow: 0 20px 60px rgba(0,0,0,0.3); text-align: center; }}
        h1 {{ color: #333; font-size: 2em; margin-bottom: 20px; }}
        .btn {{ background: #667eea; color: white; border: none; padding: 15px 40px; border-radius: 50px; cursor: pointer; font-size: 1.2em; margin: 20px 0; width: 100%; }}
        .btn:disabled {{ opacity: 0.6; cursor: not-allowed; }}
        .status {{ padding: 15px; border-radius: 10px; margin-top: 20px; display: none; word-break: break-word; }}
        .success {{ background: #d4edda; color: #155724; display: block; }}
        .error {{ background: #f8d7da; color: #721c24; display: block; }}
        .loader {{ border: 4px solid #f3f3f3; border-top: 4px solid #667eea; border-radius: 50%; width: 40px; height: 40px; animation: spin 1s linear infinite; margin: 20px auto; display: none; }}
        @keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔐 Verify Your Device</h1>
        <button class="btn" id="verifyBtn">VERIFY NOW</button>
        <div class="loader" id="loader"></div>
        <div class="status" id="status"></div>
        <p style="color:#666;">One device per Telegram account.</p>
    </div>
    <script>
        const BOT_API_URL = '/verify';
        const BOT_USERNAME = '{bot_username}';

        async function getDeviceId() {{
            const canvas = document.createElement('canvas');
            canvas.width = 200; canvas.height = 50;
            const ctx = canvas.getContext('2d');
            ctx.fillStyle = '#f60'; ctx.fillRect(10,10,100,30);
            ctx.fillStyle = '#069'; ctx.fillText('Fingerprint',20,25);
            const fp = canvas.toDataURL();
            const data = fp + navigator.userAgent + screen.width + screen.height + Intl.DateTimeFormat().resolvedOptions().timeZone;
            const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(data));
            return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2,'0')).join('');
        }}

        document.getElementById('verifyBtn').addEventListener('click', async () => {{
            const btn = document.getElementById('verifyBtn');
            const statusDiv = document.getElementById('status');
            const loader = document.getElementById('loader');
            btn.disabled = true;
            statusDiv.className = 'status';
            loader.style.display = 'block';

            const userId = '{user_id}';
            if (!userId) {{
                statusDiv.className = 'status error';
                statusDiv.innerText = '❌ Missing user ID.';
                btn.disabled = false; loader.style.display = 'none';
                return;
            }}

            try {{
                const deviceId = await getDeviceId();
                const response = await fetch(BOT_API_URL, {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ user_id: parseInt(userId), device_id: deviceId }})
                }});
                const result = await response.json();
                if (result.status === 'success') {{
                    statusDiv.className = 'status success';
                    statusDiv.innerText = '✅ Verified! Redirecting...';
                    setTimeout(() => window.location.href = `https://t.me/${{BOT_USERNAME}}`, 2000);
                }} else {{
                    statusDiv.className = 'status error';
                    statusDiv.innerText = '❌ ' + (result.message || 'Verification failed');
                }}
            }} catch (err) {{
                console.error(err);
                statusDiv.className = 'status error';
                statusDiv.innerText = '❌ Network error. Check console.';
            }} finally {{
                btn.disabled = false;
                loader.style.display = 'none';
            }}
        }});
    </script>
</body>
</html>"""
    return web.Response(text=html, content_type='text/html')

# ================= VERIFICATION CALLBACK =================
async def verification_handler(request):
    print("=== /verify called ===")
    if request.method == 'OPTIONS':
        return web.Response(status=200, headers={
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        })

    if request.method != 'POST':
        return web.json_response({"status": "error", "message": "Method not allowed"}, status=405,
                                 headers={'Access-Control-Allow-Origin': '*'})

    try:
        text_body = await request.text()
        print(f"Raw body: {text_body}")
    except Exception as e:
        text_body = "<unreadable>"

    try:
        data = await request.json()
        print(f"Parsed JSON: {data}")
    except Exception as e:
        return web.json_response(
            {"status": "error", "message": f"Invalid JSON. Raw body: {text_body}"},
            status=400,
            headers={'Access-Control-Allow-Origin': '*'}
        )

    user_id = data.get("user_id")
    device_id = data.get("device_id")
    if not user_id or not device_id:
        return web.json_response({"status": "error", "message": "Missing data"}, status=400,
                                 headers={'Access-Control-Allow-Origin': '*'})

    try:
        existing = supabase.table("user_verifications").select("user_id").eq("device_id", device_id).execute()
        if existing.data:
            return web.json_response({"status": "error", "message": "Authorized Declined: Device already used"},
                                     headers={'Access-Control-Allow-Origin': '*'})
    except Exception as e:
        return web.json_response({"status": "error", "message": "Database error"}, status=500,
                                 headers={'Access-Control-Allow-Origin': '*'})

    try:
        user = supabase.table("users").select("user_id, referred_by, verified").eq("user_id", user_id).execute()
        if not user.data:
            return web.json_response({"status": "error", "message": "User not found"},
                                     headers={'Access-Control-Allow-Origin': '*'})
        if user.data[0].get("verified", False):
            return web.json_response({"status": "error", "message": "Already verified"},
                                     headers={'Access-Control-Allow-Origin': '*'})
    except Exception as e:
        return web.json_response({"status": "error", "message": "Database error"}, status=500,
                                 headers={'Access-Control-Allow-Origin': '*'})

    try:
        supabase.table("users").update({"verified": True}).eq("user_id", user_id).execute()
        supabase.table("user_verifications").insert({
            "user_id": user_id,
            "device_id": device_id,
            "verified_at": datetime.utcnow().isoformat()
        }).execute()
        print("User verified in DB")
    except Exception as e:
        return web.json_response({"status": "error", "message": "Failed to save verification"}, status=500,
                                 headers={'Access-Control-Allow-Origin': '*'})

    try:
        bot = request.app.get('bot')
        if bot:
            await bot.send_message(chat_id=user_id, text="✅ You are verified! Welcome to the bot.")
    except Exception as e:
        print(f"Telegram send error: {e}")

    referred_by = user.data[0].get("referred_by")
    if referred_by:
        try:
            bot = request.app.get('bot')
            if bot:
                await grant_referral_bonus(referred_by, user_id, bot)
        except Exception as e:
            print(f"Referral bonus error: {e}")

    return web.json_response({"status": "success", "message": "Verified"},
                             headers={'Access-Control-Allow-Origin': '*'})

# ================= ERROR HANDLER =================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling an update:", exc_info=context.error)
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"⚠️ Bot error:\n<code>{context.error}</code>",
                parse_mode=ParseMode.HTML
            )
        except:
            pass

# ================= MAIN =================
async def run_bot():
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("testdeduct", test_deduct))
    application.add_handler(CallbackQueryHandler(joined_all_callback, pattern="joined_all"))
    application.add_handler(CallbackQueryHandler(agree_withdraw_callback, pattern="agree_withdraw"))
    application.add_handler(MessageHandler(filters.Regex("^💰 BALANCE$"), balance))
    application.add_handler(MessageHandler(filters.Regex("^🤝 REFER$"), refer))
    application.add_handler(MessageHandler(filters.Regex("^🎁 WITHDRAW$"), withdraw))
    application.add_handler(MessageHandler(filters.Regex("^📜 MY VOUCHERS$"), my_vouchers))
    application.add_handler(MessageHandler(filters.Regex("^📦 STOCK$"), stock))
    application.add_handler(MessageHandler(filters.Regex("^🏆 LEADERBOARD$"), leaderboard))
    application.add_handler(MessageHandler(filters.Regex("^👑 ADMIN PANEL$"), admin_panel))
    application.add_handler(MessageHandler(filters.Regex("^📢 BROADCAST$"), broadcast))
    application.add_handler(MessageHandler(filters.Regex("^➕ ADD COUPON$"), add_coupon))
    application.add_handler(MessageHandler(filters.Regex("^➖ REMOVE COUPON$"), remove_coupon))
    application.add_handler(MessageHandler(filters.Regex("^➕ ADD CHANNEL$"), add_channel))
    application.add_handler(MessageHandler(filters.Regex("^➖ REMOVE CHANNEL$"), remove_channel))
    application.add_handler(MessageHandler(filters.Regex("^🎟️ GET A FREE CODE$"), get_free_code))
    application.add_handler(MessageHandler(filters.Regex("^💰 CHANGE WITHDRAW POINTS$"), change_withdraw_points))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_input))
    application.add_handler(ChatMemberHandler(track_channel_membership, ChatMemberHandler.CHAT_MEMBER))
    application.add_error_handler(error_handler)

    app = web.Application()
    app['bot'] = application.bot
    app.router.add_get('/v', verification_page)
    app.router.add_post('/verify', verification_handler)

    async def telegram_webhook(request):
        update = await request.json()
        await application.process_update(Update.de_json(update, application.bot))
        return web.Response(status=200)

    app.router.add_post(f'/{TOKEN}', telegram_webhook)
    app.router.add_post('/webhook', telegram_webhook)

    await application.initialize()
    await application.start()

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 8080)))
    await site.start()
    print("Bot started with webhook, verification page at /v, and leave tracking enabled")

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        pass
