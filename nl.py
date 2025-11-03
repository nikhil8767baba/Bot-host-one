#!/usr/bin/env python3
# NumberBot v10 — Ultra Advanced Edition (Full)
# Requirements: pip install pyTelegramBotAPI Flask qrcode pillow requests
# NOTES: Replace BOT_TOKEN and UPI_ID placeholders before running.

import os
import sys
import time
import json
import csv
import sqlite3
import logging
import threading
import requests
from uuid import uuid4
from html import escape
from datetime import datetime, timedelta

# Optional web dashboard & QR support
try:
    from flask import Flask, request, render_template_string, send_file, abort
    FLASK_AVAILABLE = True
except Exception:
    FLASK_AVAILABLE = False

try:
    import telebot
    from telebot import types
except Exception:
    raise SystemExit("Install dependency: pip install pyTelegramBotAPI")

try:
    import qrcode
    from PIL import Image
    QR_AVAILABLE = True
except Exception:
    QR_AVAILABLE = False

# ---------------- CONFIG ----------------
BOT_TOKEN = "8001522854:AAHGs-tiQW2dc_3yrS5d2sMFWj5BJgfkzDo"      # <<< REPLACE with your bot token
ADMIN_ID = 8158657600                      # your Telegram numeric ID (kept as provided)
OWNER_USERNAME = "ITS_ME_UNKNOW_USER"            # owner handle without @
UPI_ID = "nikhilgawai07@axl"            # <<< REPLACE with your UPI id if you want buy flow
API_URLS = ["https://numapi.anshapi.workers.dev/?num="]  # number lookup APIs
DB_PATH = "numberbot_v10_full.db"
LOG_DIR = "logs"
LOG_BASENAME = "numberbot_v10"
CREDIT_PRICE_RUPEES = 20
CREDITS_PER_PAYMENT = 10
DAILY_FREE_CREDITS = 1
REFERRAL_BONUS = 3
CACHE_TTL_SECONDS = 60 * 60 * 24  # 24h
MAX_LOOKUPS_PER_MINUTE = 60
BAN_THRESHOLD_PER_MINUTE = 200
AUTO_VERIFY_PAYMENT = False
AUTO_UNBAN_HOURS = 24
DASHBOARD_ENABLE = False
DASHBOARD_PORT = 5000
ADMIN_SECRET = "SET_A_STRONG_SECRET"  # required if dashboard enabled
MONTHLY_TOP_BONUS = 50  # credits reward for monthly top referrer (manual / scheduled)
# ----------------------------------------

# ---------- Logging ----------
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR, exist_ok=True)

def get_log_filename():
    date = datetime.utcnow().strftime("%Y%m%d")
    return os.path.join(LOG_DIR, f"{LOG_BASENAME}_{date}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(get_log_filename()), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ---------- DB ----------
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = conn.cursor()
cur.executescript('''
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    credits INTEGER DEFAULT 5,
    banned INTEGER DEFAULT 0,
    created_at INTEGER,
    last_daily_award INTEGER DEFAULT 0,
    referrer INTEGER DEFAULT NULL,
    streak INTEGER DEFAULT 0,
    last_seen INTEGER DEFAULT 0,
    total_lookups INTEGER DEFAULT 0,
    banned_at INTEGER DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_credits ON users(credits);

CREATE TABLE IF NOT EXISTS lookups(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    number TEXT,
    result TEXT,
    timestamp INTEGER
);
CREATE INDEX IF NOT EXISTS idx_lookups_user_ts ON lookups(user_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS recharges(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount INTEGER,
    verified INTEGER DEFAULT 0,
    txn_id TEXT,
    timestamp INTEGER
);

CREATE TABLE IF NOT EXISTS referrals(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer INTEGER,
    referee INTEGER,
    timestamp INTEGER
);

CREATE TABLE IF NOT EXISTS monthly_rewards(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    month TEXT,
    user_id INTEGER,
    bonus_given INTEGER DEFAULT 0,
    timestamp INTEGER
);
''')
conn.commit()
db_lock = threading.Lock()

def safe_commit():
    try:
        conn.commit()
    except Exception as e:
        logger.exception("DB commit failed: %s", e)

def now_ts():
    return int(time.time())

# ---------- Caching ----------
lookup_cache = {}
cache_lock = threading.Lock()
def cache_get(num):
    with cache_lock:
        item = lookup_cache.get(num)
        if not item: return None
        ts, val = item
        if now_ts() - ts > CACHE_TTL_SECONDS:
            del lookup_cache[num]; return None
        return val
def cache_set(num, val):
    with cache_lock:
        lookup_cache[num] = (now_ts(), val)

# ---------- Rate limiting ----------
rate_limits = {}
rate_lock = threading.Lock()
def record_lookup_attempt(uid):
    now = now_ts()
    with rate_lock:
        lst = rate_limits.get(uid, [])
        lst = [t for t in lst if now - t <= 60]
        lst.append(now)
        rate_limits[uid] = lst
        return len(lst)
def clean_rate_limits_loop():
    while True:
        time.sleep(60)
        with rate_lock:
            now = now_ts()
            for k in list(rate_limits.keys()):
                rate_limits[k] = [t for t in rate_limits[k] if now - t <= 60]
                if not rate_limits[k]: del rate_limits[k]
threading.Thread(target=clean_rate_limits_loop, daemon=True).start()

# ---------- Daily award loop ----------
def daily_award_loop():
    while True:
        try:
            now = now_ts()
            with db_lock:
                cur.execute("SELECT user_id, last_daily_award, streak, last_seen FROM users")
                rows = cur.fetchall()
                for uid, last_award, streak, last_seen in rows:
                    if now - (last_award or 0) >= 86400:
                        cur.execute("UPDATE users SET credits = credits + ? WHERE user_id=?", (DAILY_FREE_CREDITS, uid))
                        if last_seen and now - last_seen <= 48*3600:
                            new_streak = max(1, streak or 0) + 1
                        else:
                            new_streak = 1
                        cur.execute("UPDATE users SET last_daily_award=?, streak=?, last_seen=? WHERE user_id=?", (now, new_streak, now, uid))
                safe_commit()
        except Exception as e:
            logger.exception("daily_award error: %s", e)
        time.sleep(3600)
threading.Thread(target=daily_award_loop, daemon=True).start()

# ---------- Formatter ----------
def ai_format_result(raw):
    try:
        if isinstance(raw, dict):
            parts=[]
            priority=['name','carrier','operator','region','state','city','line_type','type','country','valid']
            for k in priority:
                if k in raw and raw[k]:
                    parts.append(f"• <b>{escape(str(k).capitalize())}:</b> {escape(str(raw[k]))}")
            for k,v in raw.items():
                if k in priority: continue
                parts.append(f"• <b>{escape(str(k).capitalize())}:</b> {escape(str(v))}")
            summary=""
            if 'carrier' in raw and raw.get('region'): summary=f"This number appears registered with {raw.get('carrier')} in {raw.get('region')}."
            elif 'operator' in raw: summary=f"Operator: {raw.get('operator')}."
            if summary: return f"<i>{escape(summary)}</i>\n\n" + "\n".join(parts)
            return "\n".join(parts)
        else:
            text = str(raw).strip()
            if len(text)>800: text = text[:800]+"..."
            return escape(text)
    except Exception as e:
        logger.exception("ai_format_result error: %s", e)
        return escape(str(raw))

# ---------- Lookup API ----------
def lookup_number_api(number):
    cached = cache_get(number)
    if cached is not None:
        return True, cached, True
    last_err=None
    for base in API_URLS:
        try:
            r = requests.get(base + number, timeout=10)
            if r.status_code == 200:
                try: data = r.json()
                except: data = r.text
                cache_set(number, data)
                return True, data, False
            else:
                last_err = f"{base} returned {r.status_code}"
        except Exception as e:
            last_err = str(e)
    return False, f"Lookup failed: {last_err}", False

# ---------- User management ----------
def ensure_user(user, refcode=None):
    with db_lock:
        cur.execute("SELECT user_id FROM users WHERE user_id=?", (user.id,))
        if not cur.fetchone():
            cur.execute("INSERT INTO users (user_id, username, credits, banned, created_at, referrer, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (user.id, user.username or '', 5, 0, now_ts(), refcode, now_ts()))
            safe_commit()
            if refcode:
                try:
                    cur.execute("INSERT INTO referrals (referrer, referee, timestamp) VALUES (?, ?, ?)", (refcode, user.id, now_ts()))
                    cur.execute("UPDATE users SET credits = credits + ? WHERE user_id=?", (REFERRAL_BONUS, refcode))
                    cur.execute("UPDATE users SET credits = credits + ? WHERE user_id=?", (REFERRAL_BONUS, user.id))
                    safe_commit()
                except Exception as e:
                    logger.warning("Referral award failed: %s", e)
        else:
            cur.execute("UPDATE users SET username=?, last_seen=? WHERE user_id=?", (user.username or '', now_ts(), user.id))
            safe_commit()

def get_credits(uid):
    with db_lock:
        cur.execute("SELECT credits FROM users WHERE user_id=?", (uid,))
        r = cur.fetchone()
        return r[0] if r else 0

def set_credits(uid, amt):
    with db_lock:
        cur.execute("UPDATE users SET credits=? WHERE user_id=?", (amt, uid)); safe_commit()

def change_credits(uid, amt):
    with db_lock:
        cur.execute("UPDATE users SET credits = credits + ? WHERE user_id=?", (amt, uid)); safe_commit()

def is_banned(uid):
    with db_lock:
        cur.execute("SELECT banned, banned_at FROM users WHERE user_id=?", (uid,))
        r = cur.fetchone()
        if not r: return False
        banned, banned_at = r
        if banned and AUTO_UNBAN_HOURS and banned_at:
            try:
                if now_ts() - banned_at >= AUTO_UNBAN_HOURS * 3600:
                    unban_user(uid); return False
            except: pass
        return banned == 1

def ban_user(uid):
    with db_lock:
        cur.execute("UPDATE users SET banned=1, banned_at=? WHERE user_id=?", (now_ts(), uid)); safe_commit()

def unban_user(uid):
    with db_lock:
        cur.execute("UPDATE users SET banned=0, banned_at=NULL WHERE user_id=?", (uid,)); safe_commit()

# ---------- Logging lookups ----------
def log_lookup(uid, number, result):
    serialized = json.dumps(result, ensure_ascii=False) if not isinstance(result, str) else result
    with db_lock:
        cur.execute("INSERT INTO lookups (user_id, number, result, timestamp) VALUES (?, ?, ?, ?)",
                    (uid, number, serialized, now_ts()))
        cur.execute("UPDATE users SET total_lookups = total_lookups + 1 WHERE user_id=?", (uid,))
        safe_commit()

# ---------- Utilities ----------
def footer():
    return f"\n\n🤖 Bot by @{OWNER_USERNAME}" if OWNER_USERNAME else ""

# ---------- Bot & Keyboards ----------
bot = telebot.TeleBot(BOT_TOKEN, threaded=True)

def main_kb(uid=None):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🔍 Lookup", callback_data="lookup"))
    kb.add(types.InlineKeyboardButton("💳 Buy Credits", callback_data="buy"),
           types.InlineKeyboardButton("💰 My Credits", callback_data="credits"))
    kb.add(types.InlineKeyboardButton("📜 History", callback_data="history"),
           types.InlineKeyboardButton("📤 Export CSV", callback_data="export_csv"))
    kb.add(types.InlineKeyboardButton("🔁 Transfer Credits", callback_data="transfer"),
           types.InlineKeyboardButton("🎁 My Referrals", callback_data="myref"))
    kb.add(types.InlineKeyboardButton("🏆 Leaderboard", callback_data="leader"),
           types.InlineKeyboardButton("🎯 Top Referrers", callback_data="topref"))
    if OWNER_USERNAME:
        kb.add(types.InlineKeyboardButton("👑 Contact Owner", url=f"https://t.me/{OWNER_USERNAME}"))
    return kb

# ---------- Commands ----------
@bot.message_handler(commands=['start'])
def start(msg):
    args = msg.text.split()
    refcode = None
    if len(args) > 1:
        try: refcode = int(args[1])
        except: refcode = None
    ensure_user(msg.from_user, refcode)
    ref_link = f"https://t.me/{bot.get_me().username}?start={msg.from_user.id}"
    owner_text = f"\n👑 Bot Owner: @{OWNER_USERNAME}" if OWNER_USERNAME else ""
    if msg.from_user.id == ADMIN_ID:
        bot.send_message(msg.chat.id, f"👑 Admin mode active. Use /help_admin{owner_text}")
    else:
        bot.send_message(msg.chat.id,
                         f"👋 Hi {msg.from_user.first_name}!\nSend a phone number without +91 or use menu.\n\n"
                         f"🎁 Invite friends & earn {REFERRAL_BONUS} credits each!\nYour referral link:\n{ref_link}{owner_text}"
                         + footer(),
                         reply_markup=main_kb(msg.from_user.id))

@bot.message_handler(commands=['help'])
def help_cmd(msg):
    bot.reply_to(msg,
                 "Commands:\n"
                 "/start [referrer_id]\n/myid\n/profile\n/myref\n/topref\n/leaderboard\n/history\n/transfer <user_id> <credits>\n/exportcsv\n/owner\n\n"
                 f"Admin: /help_admin\n{('👑 Owner: @'+OWNER_USERNAME) if OWNER_USERNAME else ''}"
                 + footer())

@bot.message_handler(commands=['owner'])
def owner_cmd(msg):
    bot.reply_to(msg, f"👑 Bot Owner: @{OWNER_USERNAME}\nMessage the owner for support." + footer())

@bot.message_handler(commands=['myid'])
def myid(msg):
    bot.reply_to(msg, f"🆔 Your Telegram ID: <code>{msg.from_user.id}</code>" + footer())

@bot.message_handler(commands=['profile'])
def profile_cmd(msg):
    uid = msg.from_user.id
    with db_lock:
        cur.execute("SELECT username, credits, total_lookups, streak, referrer, created_at FROM users WHERE user_id=?", (uid,))
        row = cur.fetchone()
    if not row:
        bot.reply_to(msg, "Profile not found. Use /start" + footer()); return
    username, credits, lookups, streak, referrer, created_at = row
    created = datetime.utcfromtimestamp(created_at).strftime("%Y-%m-%d") if created_at else "N/A"
    ref_text = str(referrer) if referrer else "—"
    cur.execute("SELECT COUNT(*) FROM referrals WHERE referrer=?", (uid,))
    invited = cur.fetchone()[0]
    text = (f"👤 Profile — @{username or 'user'}\n"
            f"🆔 ID: {uid}\n"
            f"💳 Credits: {credits}\n"
            f"🔎 Total lookups: {lookups}\n"
            f"🎯 Invited: {invited}\n"
            f"🔥 Streak: {streak}\n"
            f"📅 Joined: {created}\n"
            f"🔗 Referrer: {ref_text}"
            + footer())
    bot.reply_to(msg, text)

@bot.message_handler(commands=['myref'])
def myref_cmd(msg):
    uid = msg.from_user.id
    ensure_user(msg.from_user)
    ref_link = f"https://t.me/{bot.get_me().username}?start={uid}"
    with db_lock:
        cur.execute("SELECT COUNT(*) FROM referrals WHERE referrer=?", (uid,))
        total_refs = cur.fetchone()[0]
    bot.reply_to(msg, f"🎁 Your referral link:\n{ref_link}\n👥 Invited: {total_refs}\nYou earn {REFERRAL_BONUS} credits per friend!" + footer())

@bot.message_handler(commands=['topref'])
def topref_cmd(msg):
    with db_lock:
        cur.execute("SELECT referrer, COUNT(*) as total FROM referrals GROUP BY referrer ORDER BY total DESC LIMIT 20")
        rows = cur.fetchall()
    if not rows:
        bot.reply_to(msg, "No referrals yet." + footer()); return
    lines=[]
    for i,(uid,cnt) in enumerate(rows,1):
        cur.execute("SELECT username FROM users WHERE user_id=?", (uid,))
        r = cur.fetchone()
        uname = r[0] if r and r[0] else str(uid)
        display = f"@{escape(uname)}" if isinstance(uname,str) and not uname.isdigit() else str(uid)
        lines.append(f"{i}. {display} — {cnt} invites")
    bot.reply_to(msg, "🏆 Top Referrers:\n" + "\n".join(lines) + footer())

@bot.message_handler(commands=['leaderboard'])
def leaderboard_cmd(msg):
    with db_lock:
        cur.execute("SELECT user_id, total_lookups FROM users ORDER BY total_lookups DESC LIMIT 20")
        rows = cur.fetchall()
    if not rows:
        bot.reply_to(msg, "No lookups yet." + footer()); return
    lines=[]
    for i,(uid,cnt) in enumerate(rows,1):
        cur.execute("SELECT username FROM users WHERE user_id=?", (uid,))
        r = cur.fetchone()
        uname = r[0] if r and r[0] else str(uid)
        display = f"@{escape(uname)}" if isinstance(uname,str) and not uname.isdigit() else str(uid)
        lines.append(f"{i}. {display} — {cnt} lookups")
    bot.reply_to(msg, "🏆 Leaderboard:\n"+ "\n".join(lines) + footer())

# ---------- Admin ----------
@bot.message_handler(commands=['help_admin'])
def help_admin(msg):
    if msg.from_user.id != ADMIN_ID: return
    bot.send_message(msg.chat.id,
                     "/approve <user_id> <credits>\n/addcredits <user_id> <amount>\n/setcredits <user_id> <amount>\n/ban <user_id>\n/unban <user_id>\n/pending\n/verify <txn_id>\n/stats\n/broadcast <message>\n/exportdb\n/monthly_reward")

@bot.message_handler(commands=['addcredits'])
def addcredits(msg):
    if msg.from_user.id != ADMIN_ID: return
    try:
        _, uid_s, amt_s = msg.text.split()
        uid, amt = int(uid_s), int(amt_s)
        change_credits(uid, amt)
        bot.reply_to(msg, f"✅ Added {amt} credits to {uid}")
        bot.send_message(uid, f"🎁 Admin added {amt} credits to your account." + footer())
    except Exception as e:
        bot.reply_to(msg, f"Usage: /addcredits <user_id> <amount>\nError: {e}")

@bot.message_handler(commands=['setcredits'])
def setcredits_cmd(msg):
    if msg.from_user.id != ADMIN_ID: return
    try:
        _, uid_s, amt_s = msg.text.split()
        uid, amt = int(uid_s), int(amt_s)
        set_credits(uid, amt)
        bot.reply_to(msg, f"✅ Set credits of {uid} to {amt}")
    except Exception as e:
        bot.reply_to(msg, f"Usage: /setcredits <user_id> <amount>\nError: {e}")

@bot.message_handler(commands=['approve'])
def approve(msg):
    if msg.from_user.id != ADMIN_ID: return
    try:
        _, uid_s, cr_s = msg.text.split()
        uid, cr = int(uid_s), int(cr_s)
        change_credits(uid, cr)
        bot.reply_to(msg, f"✅ Approved {cr} credits for {uid}")
        bot.send_message(uid, f"🎉 Admin approved your recharge of {cr} credits." + footer())
    except Exception as e:
        bot.reply_to(msg, f"Usage: /approve <user_id> <credits>\nError: {e}")

@bot.message_handler(commands=['verify'])
def verify_txn(msg):
    if msg.from_user.id != ADMIN_ID: return
    parts = msg.text.split()
    if len(parts)!=2:
        bot.reply_to(msg, "Usage: /verify <txn_id>"); return
    txn = parts[1]
    with db_lock:
        cur.execute("SELECT user_id, amount FROM recharges WHERE txn_id=? AND verified=0", (txn,))
        row = cur.fetchone()
        if not row:
            bot.reply_to(msg, "Transaction not found or already verified."); return
        uid, amount = row
        change_credits(uid, CREDITS_PER_PAYMENT)
        cur.execute("UPDATE recharges SET verified=1 WHERE txn_id=?", (txn,))
        safe_commit()
    bot.reply_to(msg, f"Verified {txn} for user {uid}")
    bot.send_message(uid, f"🎉 Your payment ({txn}) has been verified. {CREDITS_PER_PAYMENT} credits added." + footer())

@bot.message_handler(commands=['pending'])
def pending(msg):
    if msg.from_user.id != ADMIN_ID: return
    with db_lock:
        cur.execute("SELECT id, user_id, amount, txn_id, timestamp FROM recharges WHERE verified=0 ORDER BY id DESC")
        rows = cur.fetchall()
    if not rows:
        bot.reply_to(msg, "No pending payments."); return
    text = "Pending payments:\n" + "\n".join([f"{r[0]} | user:{r[1]} | ₹{r[2]} | txn:{r[3]} | {datetime.utcfromtimestamp(r[4]).isoformat()}" for r in rows])
    bot.reply_to(msg, text)

@bot.message_handler(commands=['stats'])
def stats(msg):
    if msg.from_user.id != ADMIN_ID: return
    with db_lock:
        cur.execute("SELECT COUNT(*) FROM users"); total_users = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM lookups"); total_lookups = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM recharges WHERE verified=1"); total_recharges = cur.fetchone()[0]
    bot.send_message(msg.chat.id, f"📊 Users: {total_users}\n🔎 Lookups: {total_lookups}\n💳 Recharges: {total_recharges}")

@bot.message_handler(commands=['broadcast'])
def broadcast(msg):
    if msg.from_user.id != ADMIN_ID: return
    try:
        _, text = msg.text.split(' ',1)
    except:
        bot.reply_to(msg, "Usage: /broadcast <message>"); return
    with db_lock:
        cur.execute("SELECT user_id FROM users"); rows = cur.fetchall()
    count=0
    for (uid,) in rows:
        try:
            bot.send_message(uid, text + footer()); count+=1
        except Exception as e:
            logger.warning("Broadcast failed: %s", e)
    bot.reply_to(msg, f"Broadcast sent to {count} users")

@bot.message_handler(commands=['exportdb'])
def exportdb(msg):
    if msg.from_user.id != ADMIN_ID: return
    with db_lock:
        cur.execute("SELECT user_id, username, credits, total_lookups, streak FROM users"); rows = cur.fetchall()
    fname = f"export_{int(time.time())}.csv"
    try:
        with open(fname,'w', newline='', encoding='utf-8') as f:
            w=csv.writer(f); w.writerow(['user_id','username','credits','total_lookups','streak']); w.writerows(rows)
        with open(fname,'rb') as f:
            bot.send_document(msg.chat.id, f)
    finally:
        try: os.remove(fname)
        except: pass

@bot.message_handler(commands=['ban'])
def ban_cmd(msg):
    if msg.from_user.id != ADMIN_ID: return
    try:
        _, uid_s = msg.text.split(); uid=int(uid_s)
        ban_user(uid); bot.reply_to(msg, f"🚫 Banned {uid}")
    except Exception as e:
        bot.reply_to(msg, f"Usage: /ban <user_id>\nError: {e}")

@bot.message_handler(commands=['unban'])
def unban_cmd(msg):
    if msg.from_user.id != ADMIN_ID: return
    try:
        _, uid_s = msg.text.split(); uid=int(uid_s)
        unban_user(uid); bot.reply_to(msg, f"✅ Unbanned {uid}")
    except Exception as e:
        bot.reply_to(msg, f"Usage: /unban <user_id>\nError: {e}")

@bot.message_handler(commands=['monthly_reward'])
def monthly_reward(msg):
    # Admin command: give monthly bonus to top referrer of previous month
    if msg.from_user.id != ADMIN_ID: return
    # determine previous month key like "2025-10"
    prev_month = (datetime.utcnow().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    with db_lock:
        cur.execute("SELECT referrer, COUNT(*) as cnt FROM referrals WHERE strftime('%Y-%m', datetime(timestamp, 'unixepoch'))=? GROUP BY referrer ORDER BY cnt DESC LIMIT 1", (prev_month,))
        row = cur.fetchone()
        if not row:
            bot.reply_to(msg, f"No referrals found for {prev_month}."); return
        winner, cnt = row
        # check if already given
        cur.execute("SELECT id FROM monthly_rewards WHERE month=? AND user_id=?", (prev_month, winner))
        if cur.fetchone():
            bot.reply_to(msg, f"Monthly bonus for {prev_month} already given."); return
        # give bonus
        cur.execute("INSERT INTO monthly_rewards (month, user_id, bonus_given, timestamp) VALUES (?, ?, ?, ?)", (prev_month, winner, MONTHLY_TOP_BONUS, now_ts()))
        cur.execute("UPDATE users SET credits = credits + ? WHERE user_id=?", (MONTHLY_TOP_BONUS, winner))
        safe_commit()
    bot.reply_to(msg, f"✅ Given {MONTHLY_TOP_BONUS} credits to {winner} for top referrer in {prev_month}.")
    try:
        bot.send_message(winner, f"🎉 Congratulations! You received {MONTHLY_TOP_BONUS} bonus credits for being top referrer in {prev_month}." + footer())
    except Exception:
        pass

# ---------- Callback query handler ----------
@bot.callback_query_handler(func=lambda call: True)
def handle_query(call):
    uid = call.from_user.id
    ensure_user(call.from_user)
    data = call.data

    if data == "lookup":
        bot.send_message(uid, "📞 Send number with country code (e.g. +919876543210):" + footer())

    elif data == "credits":
        bot.answer_callback_query(call.id)
        bot.send_message(uid, f"💳 Your Credits: {get_credits(uid)}" + footer(), reply_markup=main_kb(uid))

    elif data == "history":
        with db_lock:
            cur.execute("SELECT number, timestamp FROM lookups WHERE user_id=? ORDER BY id DESC LIMIT 20", (uid,))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(uid, "🕓 No history yet." + footer(), reply_markup=main_kb(uid))
        else:
            hist = "\n".join([f"{r[0]} - {time.strftime('%d %b %H:%M', time.localtime(r[1]))}" for r in rows])
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("🧹 Clear History", callback_data="clear_history"))
            kb.add(types.InlineKeyboardButton("📤 Export CSV", callback_data="export_csv"))
            bot.send_message(uid, f"📜 Recent lookups:\n{hist}" + footer(), reply_markup=kb)

    elif data == "export_csv":
        with db_lock:
            cur.execute("SELECT number, result, timestamp FROM lookups WHERE user_id=? ORDER BY id DESC", (uid,))
            rows = cur.fetchall()
        if not rows:
            bot.send_message(uid, "🕓 No history to export."); return
        fname = f"history_{uid}_{int(time.time())}.csv"
        with open(fname,'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f); w.writerow(['timestamp','number','result'])
            for n,res,ts in rows:
                w.writerow([time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts)), n, res])
        with open(fname,'rb') as f: bot.send_document(uid, f)
        try: os.remove(fname)
        except: pass

    elif data == "transfer":
        bot.send_message(uid, "Send transfer as: /transfer <user_id> <credits>\nExample: /transfer 987654321 5" + footer())

    elif data == "buy":
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("✅ I Have Paid", callback_data="paid"))
        buy_text = (f"💰 Send ₹{CREDIT_PRICE_RUPEES} to UPI ID: `{UPI_ID}`\n"
                    f"You will receive {CREDITS_PER_PAYMENT} credits.\n\n"
                    f"👑 Owner: @{OWNER_USERNAME}" + footer())
        bot.send_message(uid, buy_text, parse_mode="Markdown", reply_markup=kb)
        # Optionally send UPI QR if available
        if QR_AVAILABLE and UPI_ID and UPI_ID != "PUT_YOUR_UPI_ID_HERE":
            try:
                upi_uri = f"upi://pay?pa={UPI_ID}&pn={OWNER_USERNAME}&tn=Credit+Purchase&am={CREDIT_PRICE_RUPEES}"
                img = qrcode.make(upi_uri)
                tmp = f"upi_{uid}_{int(time.time())}.png"
                img.save(tmp)
                with open(tmp,'rb') as f: bot.send_photo(uid, f)
                try: os.remove(tmp)
                except: pass
            except Exception as e:
                logger.warning("QR gen failed: %s", e)

    elif data == "paid":
        txn = str(uuid4())
        with db_lock:
            cur.execute("INSERT INTO recharges (user_id, amount, verified, txn_id, timestamp) VALUES (?, ?, ?, ?, ?)",
                        (uid, CREDIT_PRICE_RUPEES, 1 if AUTO_VERIFY_PAYMENT else 0, txn, now_ts()))
            safe_commit()
        if AUTO_VERIFY_PAYMENT:
            change_credits(uid, CREDITS_PER_PAYMENT)
            bot.send_message(uid, f"🎉 Payment auto-verified. {CREDITS_PER_PAYMENT} credits added. Balance: {get_credits(uid)}" + footer())
        else:
            bot.send_message(uid, "✅ Payment recorded. Admin will verify soon." + footer())
        bot.send_message(ADMIN_ID, f"New payment request:\nUser: {uid}\nAmount: ₹{CREDIT_PRICE_RUPEES}\nTxn: {txn}\nAutoVerified: {AUTO_VERIFY_PAYMENT}")

    elif data == "clear_history":
        with db_lock:
            cur.execute("DELETE FROM lookups WHERE user_id=?", (uid,)); safe_commit()
        bot.send_message(uid, "🧹 History cleared." + footer(), reply_markup=main_kb(uid))

    elif data == "leader":
        with db_lock:
            cur.execute("SELECT user_id, total_lookups FROM users ORDER BY total_lookups DESC LIMIT 10"); rows = cur.fetchall()
        lines = [f"{i+1}. {r[0]} — {r[1]} lookups" for i,r in enumerate(rows)]
        bot.send_message(uid, "🏆 Top users:\n" + "\n".join(lines) + footer())

    elif data == "myref":
        with db_lock:
            cur.execute("SELECT COUNT(*) FROM referrals WHERE referrer=?", (uid,)); total = cur.fetchone()[0]
        ref_link = f"https://t.me/{bot.get_me().username}?start={uid}"
        bot.send_message(uid, f"🎁 Your referral link:\n{ref_link}\nInvited: {total}\nYou earn {REFERRAL_BONUS} credits per friend!" + footer())

    elif data == "topref":
        with db_lock:
            cur.execute("SELECT referrer, COUNT(*) as total FROM referrals GROUP BY referrer ORDER BY total DESC LIMIT 20"); rows = cur.fetchall()
        lines=[]
        for i,(u,cnt) in enumerate(rows,1):
            cur.execute("SELECT username FROM users WHERE user_id=?", (u,)); r=cur.fetchone(); uname = r[0] if r and r[0] else str(u)
            display = f"@{escape(uname)}" if isinstance(uname,str) and not uname.isdigit() else str(u)
            lines.append(f"{i}. {display} — {cnt} invites")
        bot.send_message(uid, "🎯 Top Referrers:\n" + "\n".join(lines) + footer())

    else:
        bot.answer_callback_query(call.id, "Unknown action")

# ---------- Message handler ----------
@bot.message_handler(func=lambda m: True)
def handle_text(msg):
    uid = msg.from_user.id
    ensure_user(msg.from_user)
    text = (msg.text or '').strip()

    # myid
    if text.lower().startswith('/myid'):
        bot.reply_to(msg, f"Your ID: {uid}" + footer()); return

    # history
    if text.lower().startswith('/history'):
        with db_lock:
            cur.execute("SELECT number, timestamp FROM lookups WHERE user_id=? ORDER BY id DESC LIMIT 50", (uid,)); rows = cur.fetchall()
        if not rows:
            bot.reply_to(msg, "🕓 No history yet." + footer()); return
        hist = "\n".join([f"{r[0]} - {time.strftime('%d %b %H:%M', time.localtime(r[1]))}" for r in rows])
        bot.reply_to(msg, f"📜 Your recent lookups:\n{hist}" + footer()); return

    # transfer
    if text.lower().startswith('/transfer'):
        parts = text.split()
        if len(parts)!=3:
            bot.reply_to(msg, "Usage: /transfer <user_id> <credits>"); return
        try:
            to_uid = int(parts[1]); amt = int(parts[2])
        except:
            bot.reply_to(msg, "Invalid arguments."); return
        if amt <= 0:
            bot.reply_to(msg, "Amount must be positive."); return
        if get_credits(uid) < amt:
            bot.reply_to(msg, "❌ Not enough credits."); return
        with db_lock:
            cur.execute("SELECT user_id FROM users WHERE user_id=?", (to_uid,)); if_exists = cur.fetchone()
            if not if_exists:
                bot.reply_to(msg, "Recipient not found."); return
            change_credits(uid, -amt); change_credits(to_uid, amt)
        bot.reply_to(msg, f"✅ Transferred {amt} credits to {to_uid}. Your balance: {get_credits(uid)}"); 
        try: bot.send_message(to_uid, f"🎁 You received {amt} credits from {uid}. Balance: {get_credits(to_uid)}")
        except Exception as e: logger.warning("Notify recipient failed: %s", e)
        return

    # lookup numbers
    if text.startswith('+') or text.isdigit():
        if is_banned(uid):
            bot.reply_to(msg, "🚫 You are banned." + footer()); return
        s = text.strip(); digits = s[1:] if s.startswith('+') else s
        if not digits.isdigit() or len(digits)<8 or len(digits)>15 or len(set(digits))==1:
            bot.reply_to(msg, "⚠️ Invalid number format. Use +countrycode and digits. Example: +919876543210"); return
        attempts = record_lookup_attempt(uid)
        if attempts > BAN_THRESHOLD_PER_MINUTE:
            ban_user(uid); bot.reply_to(msg, "🚫 Auto-banned due to excessive requests."); return
        if attempts > MAX_LOOKUPS_PER_MINUTE:
            bot.reply_to(msg, f"⏳ Rate limit: too many requests. (You used {attempts} in last minute)"); return
        if get_credits(uid) <= 0:
            bot.reply_to(msg, "❌ Not enough credits. Buy more to continue." + footer()); return

        sent = bot.reply_to(msg, "🔎 Fetching info..." + footer())
        ok, raw, cached = lookup_number_api(text)
        if ok:
            change_credits(uid, -1)
            log_lookup(uid, text, raw)
            pretty = ai_format_result(raw)
            try:
                bot.edit_message_text(f"📞 Result for {escape(text)}\n\n{pretty}\n\n💳 Credits left: {get_credits(uid)}" + footer(),
                                      msg.chat.id, sent.message_id, parse_mode='HTML')
            except Exception:
                bot.edit_message_text(f"📞 Result for {text}\n\n{str(raw)}\n\n💳 Credits left: {get_credits(uid)}" + footer(),
                                      msg.chat.id, sent.message_id)
        else:
            bot.edit_message_text(f"{raw}" + footer(), msg.chat.id, sent.message_id)
        return

    # small replies
    if text.lower() in ['hi','hello','hey']:
        bot.reply_to(msg, f"Hi {msg.from_user.first_name}! Use menu." + footer(), reply_markup=main_kb(uid)); return

    bot.reply_to(msg, "❓ Send a phone number (without +91) or use the menu." + footer(), reply_markup=main_kb(uid))

# ---------- Inline query support ----------
@bot.inline_handler(lambda q: True)
def inline_query_handler(inline_query):
    qtext = inline_query.query.strip()
    if not qtext: return
    if not (qtext.startswith('+') or qtext.isdigit()): return
    s = qtext; digits = s[1:] if s.startswith('+') else s
    if not digits.isdigit() or len(digits)<8 or len(digits)>15: return
    ok, raw, cached = lookup_number_api(qtext)
    if ok:
        pretty = ai_format_result(raw)
        content = types.InputTextMessageContent(f"📞 Lookup: {qtext}\n\n{pretty}")
        title = f"Lookup {qtext}"
        r = types.InlineQueryResultArticle(id=qtext+"-1", title=title, input_message_content=content, description=str(type(raw)))
        try:
            bot.answer_inline_query(inline_query.id, [r], cache_time=0)
        except Exception as e:
            logger.warning("Inline answer failed: %s", e)

# ---------- Optional Flask dashboard ----------
if DASHBOARD_ENABLE and FLASK_AVAILABLE:
    app = Flask(__name__)
    def check_secret():
        secret = request.args.get('secret','')
        if not ADMIN_SECRET or secret != ADMIN_SECRET:
            abort(403)

    @app.route('/')
    def home():
        return "NumberBot v10 — Dashboard available at /dashboard?secret=YOUR_SECRET"

    @app.route('/dashboard')
    def dashboard():
        check_secret()
        with db_lock:
            cur.execute("SELECT COUNT(*) FROM users"); total_users = cur.fetchone()[0]
            cur.execute("SELECT SUM(credits) FROM users"); total_credits = cur.fetchone()[0] or 0
            cur.execute("SELECT COUNT(*) FROM referrals"); total_refs = cur.fetchone()[0]
            cur.execute("SELECT r.referrer, COUNT(*) as total FROM referrals r GROUP BY r.referrer ORDER BY total DESC LIMIT 25")
            top = cur.fetchall()
            cur.execute("SELECT user_id, number, datetime(timestamp, 'unixepoch') FROM lookups ORDER BY id DESC LIMIT 50")
            recent = cur.fetchall()
        html = "<h2>NumberBot v10 Dashboard</h2>"
        html += f"<p>Total users: {total_users} | Total credits: {total_credits} | Total referrals: {total_refs}</p>"
        html += "<h3>Top referrers</h3><ol>"
        for r in top: html += f"<li>{r[0]} — {r[1]}</li>"
        html += "</ol><h3>Recent lookups</h3><ul>"
        for u,q,d in recent: html += f"<li>{u} — {q} — {d}</li>"
        html += "</ul>"
        return html

    def run_dashboard():
        app.run(host='0.0.0.0', port=DASHBOARD_PORT, debug=False, threaded=True)

    threading.Thread(target=run_dashboard, daemon=True).start()
    logger.info("Dashboard enabled on port %s (use ?secret=...)", DASHBOARD_PORT)
elif DASHBOARD_ENABLE and not FLASK_AVAILABLE:
    logger.warning("DASHBOARD_ENABLE True but Flask not installed. Install Flask or set DASHBOARD_ENABLE=False")

# ---------- Runner ----------
def run_bot():
    while True:
        try:
            logger.info("Starting NumberBot polling...")
            bot.infinity_polling(timeout=60, long_polling_timeout=5)
        except Exception as e:
            logger.exception("Polling error: %s", e)
            time.sleep(5)

if __name__ == "__main__":
    if BOT_TOKEN.startswith("PUT_"):
        logger.error("Set BOT_TOKEN in config before running.")
        sys.exit(1)
    logger.info("NumberBot v10 starting — Owner: @%s", OWNER_USERNAME)
    run_bot()