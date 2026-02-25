"""
╔══════════════════════════════════════════╗
║        ЩОДЕННИК КЛАСУ  •  v5.6           ║
║     FastAPI + Telegram Bot (Unified)     ║
║        PostgreSQL Cloud Edition          ║
╚══════════════════════════════════════════╝
"""

import logging
import os
import secrets
from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager
from typing import List, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from telegram import (
    Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, MenuButtonWebApp
)
from telegram.constants import ChatType
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ==========================================
# ⚙️ НАЛАШТУВАННЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    log = logging.getLogger(__name__)
    log.warning("❌ BOT_TOKEN не задано.")

WEB_APP_URL  = os.getenv("WEB_APP_URL",  "https://tg-0ncg.onrender.com")
WEBHOOK_URL  = os.getenv("WEBHOOK_URL",  "https://tg-0ncg.onrender.com")  # задати на Render = той самий домен
WEBHOOK_PATH = "/webhook/telegram"
DATABASE_URL = os.getenv("DATABASE_URL")

UPLOAD_DIR = "uploads"
START_WEBAPP = ""  # заповнюється в lifespan
MAX_UPLOAD_MB = 60
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

KYIV_TZ = ZoneInfo("Europe/Kyiv")

def today_kyiv() -> date:
    return datetime.now(KYIV_TZ).date()

# ==========================================
# 🗄 БАЗА ДАНИХ ТА ДАНІ РОЗКЛАДУ
# ==========================================
DAYS_UA = ["Понеділок","Вівторок","Середа","Четвер","П'ятниця","Субота","Неділя"]

SCHEDULE = {
    "Понеділок": ["Алгебра","Фізика","Інформатика","Фізкультура","Англ. Мова","Біологія","Технології"],
    "Вівторок":  ["Хімія","Геометрія","Укр. Мова","Укр. Літ","Фізкультура","Фізика"],
    "Середа":    ["Укр. Мова","Мистецтво","Укр. Літ","Фізика","Географія","Мистецтво (0.5)"],
    "Четвер":    ["Історія","Алгебра","Хімія","Історія України","Біологія","Інформ./Технол.","Англ. Мова"],
    "П'ятниця":  ["Історія України","Зар. Літ","Астрономія","Укр. Мова (дод)","Фізкультура"],
}

BELLS = [
    (1,"09:00","09:45"),(2,"09:55","10:40"),(3,"10:50","11:35"),
    (4,"11:45","12:30"),(0,"12:30","13:00"),
    (5,"13:00","13:45"),(6,"13:55","14:40"),
    (7,"14:50","15:35"),(8,"15:45","16:30"),
]

EMOJI = {
    "Алгебра":"📐","Геометрія":"📏","Фізика":"⚛️","Хімія":"🧪","Біологія":"🌿",
    "Географія":"🌍","Астрономія":"🔭","Інформатика":"💻","Інформ./Технол.":"💻",
    "Технології":"🔧","Англ. Мова":"🇬🇧","Укр. Мова":"🇺🇦","Укр. Мова (дод)":"🇺🇦",
    "Укр. Літ":"📖","Зар. Літ":"📚","Історія":"🏛️","Історія України":"🏳️",
    "Мистецтво":"🎨","Мистецтво (0.5)":"🎨","Фізкультура":"⚽",
}

def ei(s): return EMOJI.get(s, "📌")
def day_name(d: date): return DAYS_UA[d.weekday()]

class DBWrapper:
    def __init__(self, url):
        self.conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
        self.conn.autocommit = True

    def execute(self, query, params=None):
        cur = self.conn.cursor()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        return cur

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

def dbc():
    if not DATABASE_URL:
        raise RuntimeError("❌ DATABASE_URL не задано!")
    return DBWrapper(DATABASE_URL)

def init_db():
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    if not DATABASE_URL:
        return
    with dbc() as c:
        try:
            c.execute("ALTER TABLE homework ADD COLUMN is_important INTEGER DEFAULT 0")
        except Exception:
            pass  # колонка вже існує
        c.execute("""
            CREATE TABLE IF NOT EXISTS homework(
                id SERIAL PRIMARY KEY,
                subject TEXT NOT NULL,
                description TEXT NOT NULL,
                due_date TEXT NOT NULL,
                author_id BIGINT,
                author_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_done INTEGER DEFAULT 0,
                is_important INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS subscribers(
                chat_id BIGINT PRIMARY KEY,
                username TEXT,
                mode TEXT DEFAULT 'private',
                title TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS attachments(
                id SERIAL PRIMARY KEY,
                hw_id INTEGER NOT NULL REFERENCES homework(id) ON DELETE CASCADE,
                original_name TEXT NOT NULL,
                stored_name TEXT NOT NULL UNIQUE,
                mime_type TEXT,
                size_bytes INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

def hw_cleanup():
    cutoff = (today_kyiv() - timedelta(days=3)).isoformat()
    with dbc() as c:
        return c.execute("DELETE FROM homework WHERE due_date < %s", (cutoff,)).rowcount

def sub_get(chat_id):
    with dbc() as c:
        return c.execute("SELECT chat_id,username,mode,title FROM subscribers WHERE chat_id=%s", (chat_id,)).fetchone()

def sub_add(chat_id, username, mode="private", title=None):
    with dbc() as c:
        c.execute(
            """
            INSERT INTO subscribers(chat_id,username,mode,title) 
            VALUES(%s,%s,%s,%s) 
            ON CONFLICT (chat_id) 
            DO UPDATE SET username=EXCLUDED.username, mode=EXCLUDED.mode, title=EXCLUDED.title
            """,
            (chat_id, username, mode, title)
        )

def sub_remove(chat_id):
    with dbc() as c:
        c.execute("DELETE FROM subscribers WHERE chat_id=%s", (chat_id,))

def sub_all():
    with dbc() as c:
        return c.execute("SELECT chat_id FROM subscribers").fetchall()

def _attachments_for_hw_ids(ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not ids:
        return {}
    with dbc() as c:
        rows = c.execute(
            """
            SELECT id, hw_id, original_name, stored_name, mime_type, size_bytes
            FROM attachments
            WHERE hw_id = ANY(%s)
            ORDER BY id
            """,
            (ids,)
        ).fetchall()

    out: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows:
        hw_id = int(r["hw_id"])
        out.setdefault(hw_id, []).append({
            "id": int(r["id"]),
            "name": r["original_name"],
            "url": f"/files/{r['stored_name']}",
            "mime": r["mime_type"] or "",
            "size": int(r["size_bytes"] or 0),
        })
    return out

def hw_for_date_formatted(d: str):
    with dbc() as c:
        rows = c.execute("""
            SELECT id, subject, description, due_date, author_name, author_id, is_important
            FROM homework
            WHERE due_date=%s
            ORDER BY is_important DESC, subject
        """, (d,)).fetchall()

    ids = [int(r["id"]) for r in rows]
    att_map = _attachments_for_hw_ids(ids)

    return [{
        "id": int(r["id"]),
        "subject": r["subject"],
        "description": r["description"],
        "author": r["author_name"] or "—",
        "author_id": r["author_id"],
        "is_important": int(r["is_important"] or 0),
        "attachments": att_map.get(int(r["id"]), [])
    } for r in rows]

def _safe_ext(filename: str) -> str:
    _, ext = os.path.splitext(filename or "")
    ext = (ext or "").lower().strip()
    if len(ext) > 12:
        return ""
    return ext

def _delete_file_quiet(stored_name: str):
    try:
        path = os.path.join(UPLOAD_DIR, stored_name)
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass

# ==========================================
# 🤖 ТЕЛЕГРАМ БОТ (Меню)
# ==========================================
DIV = "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔"
HEADER_MAIN  = f"📚 *Щоденник Класу*\n{DIV}\nОбери розділ:"
HEADER_SCHED = f"📆 *Розклад уроків*\n{DIV}\nОбери день:"

def kb(*rows): return InlineKeyboardMarkup(list(rows))
def _back(cb="go_main", label="◀️  Назад"): return InlineKeyboardButton(label, callback_data=cb)

def kb_main(chat_type: str):
    # В приватці можна web_app
    if chat_type == ChatType.PRIVATE:
        open_btn = InlineKeyboardButton(
            "📱 Відкрити Щоденник",
            web_app=WebAppInfo(url=WEB_APP_URL),
        )
    else:
        open_btn = InlineKeyboardButton(
            "🤖 Відкрити в боті",
            url=START_WEBAPP,   # <- відкриває приватний чат з ботом
        )

    return kb(
        [open_btn],
        [InlineKeyboardButton("📆  Розклад",            callback_data="menu_schedule")],
        [InlineKeyboardButton("🔔  Підписка",           callback_data="menu_sub")],
        [InlineKeyboardButton("❓  Допомога",           callback_data="help")],
        [InlineKeyboardButton("✖  Закрити меню",       callback_data="close_menu")],
    )

def kb_schedule_days():
    btns = [InlineKeyboardButton(d, callback_data=f"sched_{d}") for d in SCHEDULE]
    rows = [[btns[i], btns[i+1]] if i+1 < len(btns) else [btns[i]] for i in range(0, len(btns), 2)]
    rows.append([_back()])
    return InlineKeyboardMarkup(rows)

def kb_sub(is_sub: bool):
    rows = [
        [InlineKeyboardButton("👤  Приватно (цей чат)",   callback_data="sub_private")],
        [InlineKeyboardButton("👥  В групу — інструкція", callback_data="sub_group_info")],
    ]
    if is_sub:
        rows.append([InlineKeyboardButton("🚫  Скасувати підписку", callback_data="sub_cancel")])
    rows.append([_back()])
    return InlineKeyboardMarkup(rows)

async def delete_msg(msg):
    try:
        await msg.delete()
    except Exception:
        pass

async def go_main(q, ctx):
    chat_type = q.message.chat.type  # <- важливо
    await q.edit_message_text(
        HEADER_MAIN,
        parse_mode="Markdown",
        reply_markup=kb_main(chat_type),
    )

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    chat = update.effective_chat
    title = chat.title if chat.type != "private" else None

    if not sub_get(chat.id):
        sub_add(chat.id, u.username or u.first_name, "private" if chat.type == "private" else "group", title)

    chat_type = chat.type
    payload = (ctx.args[0].strip().lower() if ctx.args else "")

    # Якщо зайшли з групи по кнопці "Відкрити в боті" (deep-link)
    if chat_type == ChatType.PRIVATE and payload == "webapp":
        # Тут можна або одразу показати головне меню,
        # або одразу кинути "HEADER_MAIN" (як у go_main)
        await update.message.reply_text(
            HEADER_MAIN,
            parse_mode="Markdown",
            reply_markup=kb_main(chat_type),
        )
        # В приватці я НЕ раджу видаляти старт-повідомлення
        return

    greeting = (
        f"👋 Вітаємо, *{u.first_name}*!\n\n📚 *Щоденник Класу* — офіційний бот класу.\n{DIV}\n"
        f"Тут зберігається домашнє завдання,\nрозклад уроків і нагадування.\n\nОбери розділ:"
    ) if chat.type == "private" else (
        f"📚 *Щоденник Класу* підключено!\n{DIV}\nНагадування надходитимуть щодня о *09:00*."
    )

    await update.message.reply_text(
        greeting,
        parse_mode="Markdown",
        reply_markup=kb_main(chat_type),
    )

    # Видаляти повідомлення в групі ок, у приватці — краще не чіпати
    if chat_type != ChatType.PRIVATE:
        await delete_msg(update.message)

async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_type = update.effective_chat.type

    await update.message.reply_text(
        HEADER_MAIN,
        parse_mode="Markdown",
        reply_markup=kb_main(chat_type),
    )
    await delete_msg(update.message)

async def cmd_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HEADER_SCHED, parse_mode="Markdown", reply_markup=kb_schedule_days())
    await delete_msg(update.message)

async def cb_go_main(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await go_main(q, ctx)

async def cb_close_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Меню закрито ✖")
    try:
        await q.message.delete()
    except Exception:
        await q.edit_message_reply_markup(reply_markup=None)

async def cb_menu_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(HEADER_SCHED, parse_mode="Markdown", reply_markup=kb_schedule_days())

async def cb_sched_day(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    day = q.data.replace("sched_", "")
    subjects = SCHEDULE.get(day, [])
    text = f"📆 *{day}*\n{DIV}\n\n"
    lesson_num = 0
    for num, start, end in BELLS:
        if num == 0:
            text += f"\n╭─ 🍽  *Обідня перерва*\n╰─ {start} – {end}\n\n"
        else:
            lesson_num += 1
            if lesson_num - 1 < len(subjects):
                subj = subjects[lesson_num - 1]
                text += f"╭─ *{num}.* {ei(subj)} {subj}\n╰─ {start} – {end}\n"

    await q.edit_message_text(
        text,
        parse_mode="Markdown",
        reply_markup=kb([_back("menu_schedule", "◀️  До розкладу")], [_back()])
    )

async def cb_menu_sub(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    rec = sub_get(update.effective_chat.id)
    status = f"✅ *Активна* — {'в групу 👥' if rec and rec['mode']=='group' else 'приватно 👤'}" if rec else "❌ *Не активна*"
    await q.edit_message_text(
        f"🔔 *Підписка*\n{DIV}\n\nСтатус: {status}\n\nЩодня о *09:00* надходить список Д/З на поточний день.",
        parse_mode="Markdown",
        reply_markup=kb_sub(bool(rec))
    )

async def cb_sub_private(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_chat.type != "private":
        return await q.answer("⚠️ Тільки в приватному чаті!", show_alert=True)
    sub_add(update.effective_chat.id, update.effective_user.first_name, "private")
    await q.edit_message_text(
        f"✅ *Підписку оформлено!*\n{DIV}\n\n👤 Нагадування щодня о *09:00*.",
        parse_mode="Markdown",
        reply_markup=kb([_back()])
    )

async def cb_sub_group_info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        f"👥 *Підписка групи*\n{DIV}\n\n1️⃣  Додай бота до групи\n2️⃣  Напиши в групі /start\n3️⃣  Готово\n\n💡 Група отримуватиме Д/З о *09:00*.",
        parse_mode="Markdown",
        reply_markup=kb([_back("menu_sub")])
    )

async def cb_sub_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    sub_remove(update.effective_chat.id)
    await q.edit_message_text(
        f"🚫 *Підписку скасовано*\n{DIV}\n\nРанкові нагадування вимкнено.",
        parse_mode="Markdown",
        reply_markup=kb([_back()])
    )

async def cb_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        f"❓ *Довідка*\n{DIV}\n\n"
        "📱 *Щоденник* — відкриває міні-додаток, де зберігаються всі завдання.\n\n"
        "📎 *Вкладення* — можна додати pdf/фото/відео до завдання.\n\n"
        "📆 *Розклад* — уроки і час дзвінків по днях тижня.\n"
        "🔔 *Підписка* — щоденне нагадування про Д/З о 09:00.\n"
        f"{DIV}\n"
        "🤖 *Команди:*\n"
        "/menu — головне меню\n"
        "/schedule — розклад\n\n"
        "🧹 Старі завдання автоматично видаляються.",
        parse_mode="Markdown",
        reply_markup=kb([_back()])
    )

async def _broadcast(bot, text: str):
    """Розсилає повідомлення всім підписникам."""
    for rec in sub_all():
        try:
            await bot.send_message(rec["chat_id"], text, parse_mode="Markdown")
        except Exception as ex:
            log.warning("Broadcast failed %s: %s", rec["chat_id"], ex)


# ==========================================
# ⏰ JOBS (ВИПРАВЛЕНО)
# ==========================================

async def job_morning(ctx: ContextTypes.DEFAULT_TYPE):
    """Пн–Пт 09:00 — розклад на сьогодні + список Д/З."""
    today = today_kyiv()
    if today.weekday() >= 5:
        return

    dn = DAYS_UA[today.weekday()]
    subjects = SCHEDULE.get(dn, [])

    sched_lines = ""
    lesson_idx = 0

    for num, start, end in BELLS:
        if num == 0:
            sched_lines += f"   ☕ Перерва {start}–{end}\n"
        else:
            if lesson_idx < len(subjects):
                s = subjects[lesson_idx]
                sched_lines += (
                    f"╭─ *{num}.* {ei(s)} {s}\n"
                    f"╰─ {start}–{end}\n"
                )
                lesson_idx += 1

    text = f"""☀️ *Доброго ранку!*
📅 *{dn}, {today.strftime('%d.%m')}*
{DIV}

📆 *Розклад на сьогодні:*
{sched_lines}
"""

    rows = hw_for_date_formatted(today.isoformat())

    if rows:
        text += "📚 *Д/З на сьогодні:*\n"
        for r in rows:
            imp  = "🔴 " if r.get("is_important") else ""
            clip = " 📎" if r.get("attachments") else ""
            text += (
                f"╭─ {imp}{ei(r['subject'])} *{r['subject']}*{clip}\n"
                f"│  📋 {r['description']}\n"
                f"╰─ 👤 {r['author']}\n\n"
            )
    else:
        text += "📭 Д/З на сьогодні немає 🎉\n"

    await _broadcast(ctx.bot, text)


async def job_evening(ctx: ContextTypes.DEFAULT_TYPE):
    """Пн–Пт 18:00 — тільки важливе Д/З на завтра."""
    today = today_kyiv()
    if today.weekday() >= 5:
        return

    tomorrow = today + timedelta(days=1)
    if tomorrow.weekday() >= 5:
        return

    rows = hw_for_date_formatted(tomorrow.isoformat())
    important = [r for r in rows if r.get("is_important")]
    if not important:
        return

    dn = DAYS_UA[tomorrow.weekday()]

    text = f"""🔴 *Важливе Д/З на завтра — {dn}, {tomorrow.strftime('%d.%m')}*
{DIV}

"""

    for r in important:
        clip = " 📎" if r.get("attachments") else ""
        text += (
            f"╭─ {ei(r['subject'])} *{r['subject']}*{clip}\n"
            f"│  📋 {r['description']}\n"
            f"╰─ 👤 {r['author']}\n\n"
        )

    await _broadcast(ctx.bot, text)


async def job_sunday_evening(ctx: ContextTypes.DEFAULT_TYPE):
    """Нд 18:00 — всі Д/З на понеділок."""
    today = today_kyiv()
    if today.weekday() != 6:
        return

    tomorrow = today + timedelta(days=1)
    rows = hw_for_date_formatted(tomorrow.isoformat())
    dn = DAYS_UA[tomorrow.weekday()]

    if rows:
        has_imp = any(r.get("is_important") for r in rows)

        text = f"""📋 *Д/З на завтра — {dn}, {tomorrow.strftime('%d.%m')}*
{DIV}

"""

        if has_imp:
            text += "⚠️ *Є важливі завдання!*\n\n"

        for r in rows:
            imp  = "🔴 " if r.get("is_important") else ""
            clip = " 📎" if r.get("attachments") else ""
            text += (
                f"╭─ {imp}{ei(r['subject'])} *{r['subject']}*{clip}\n"
                f"│  📋 {r['description']}\n"
                f"╰─ 👤 {r['author']}\n\n"
            )
    else:
        text = f"""📋 *Д/З на завтра — {dn}, {tomorrow.strftime('%d.%m')}*
{DIV}

📭 На понеділок Д/З немає 🎉
Гарного відпочинку!
"""

    await _broadcast(ctx.bot, text)


async def job_cleanup(ctx: ContextTypes.DEFAULT_TYPE):
    n = hw_cleanup()
    if n:
        log.info("🧹 Автоочищення: %d Д/З видалено", n)

# ==========================================
# 🌐 FASTAPI + INTEGRATION
# ==========================================
ptb_app = Application.builder().token(TOKEN).build() if TOKEN else None

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    if ptb_app:
        await ptb_app.initialize()

        # Команди
        await ptb_app.bot.set_my_commands([
            BotCommand("start", "🚀 Запустити бота"),
            BotCommand("menu", "📚 Головне меню"),
            BotCommand("schedule", "📆 Розклад уроків"),
        ])

        await ptb_app.bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text="📱 Щоденник",
                web_app=WebAppInfo(url=WEB_APP_URL)
            )
        )

        # Хендлери
        ptb_app.add_handler(CommandHandler("start", cmd_start))
        ptb_app.add_handler(CommandHandler("menu", cmd_menu))
        ptb_app.add_handler(CommandHandler("schedule", cmd_schedule))
        ptb_app.add_handler(CallbackQueryHandler(cb_close_menu, pattern="^close_menu$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_go_main, pattern="^go_main$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_menu_schedule, pattern="^menu_schedule$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_sched_day, pattern="^sched_"))
        ptb_app.add_handler(CallbackQueryHandler(cb_menu_sub, pattern="^menu_sub$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_sub_private, pattern="^sub_private$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_sub_group_info, pattern="^sub_group_info$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_sub_cancel, pattern="^sub_cancel$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_help, pattern="^help$"))

        # Jobs
        jq = ptb_app.job_queue
        jq.run_daily(job_morning, time=time(hour=9, minute=15, tzinfo=KYIV_TZ))
        jq.run_daily(job_evening, time=time(hour=18, minute=0, tzinfo=KYIV_TZ))
        jq.run_daily(job_sunday_evening, time=time(hour=18, minute=0, tzinfo=KYIV_TZ))
        jq.run_daily(job_cleanup, time=time(hour=0, minute=5, tzinfo=KYIV_TZ))

        await ptb_app.start()

        # 🔥 ТІЛЬКИ WEBHOOK
        if not WEBHOOK_URL:
            raise RuntimeError("WEBHOOK_URL must be set on Render")

        await ptb_app.bot.delete_webhook(drop_pending_updates=True)

        webhook_url = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
        await ptb_app.bot.set_webhook(webhook_url)

        log.info("Webhook set to %s", webhook_url)

    yield

    if ptb_app:
        await ptb_app.bot.delete_webhook()
        await ptb_app.stop()
        await ptb_app.shutdown()

fastapi_app = FastAPI(lifespan=lifespan)

@fastapi_app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if not ptb_app:
        return JSONResponse({"status": "no bot"}, status_code=503)
    update = Update.de_json(await request.json(), ptb_app.bot)
    await ptb_app.process_update(update)
    return JSONResponse({"status": "ok"})

@fastapi_app.get("/files/{stored_name}")
async def get_file(stored_name: str):
    path = os.path.join(UPLOAD_DIR, stored_name)
    if not os.path.exists(path):
        return JSONResponse({"status": "error", "message": "File not found"}, status_code=404)
    return FileResponse(path, filename=stored_name)

@fastapi_app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("templates/index.html", "r", encoding="utf-8") as f:
        return f.read()

@fastapi_app.get("/api/hw")
async def get_hw_api():
    today = today_kyiv()
    data = {}
    for i in range(3):
        target_date = today + timedelta(days=i)
        iso_date = target_date.isoformat()
        label = "Сьогодні" if i == 0 else "Завтра" if i == 1 else target_date.strftime('%d.%m')
        data[iso_date] = {"label": label, "tasks": hw_for_date_formatted(iso_date)}
    return data

@fastapi_app.get("/api/hw_all")
async def get_hw_all_api():
    today = today_kyiv().isoformat()
    if not DATABASE_URL:
        return []
    with dbc() as c:
        rows = c.execute("""
            SELECT id, subject, description, author_name, author_id, due_date, is_important
            FROM homework
            WHERE due_date >= %s
            ORDER BY due_date, is_important DESC, subject
        """, (today,)).fetchall()

    ids = [int(r["id"]) for r in rows]
    att_map = _attachments_for_hw_ids(ids)

    return [{
        "id": int(r["id"]),
        "subject": r["subject"],
        "description": r["description"],
        "author": r["author_name"] or "—",
        "author_id": r["author_id"],
        "date": r["due_date"],
        "is_important": int(r["is_important"] or 0),
        "attachments": att_map.get(int(r["id"]), [])
    } for r in rows]

@fastapi_app.post("/api/upload")
async def api_upload(files: List[UploadFile] = File(...)):
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    uploaded = []
    total = 0

    for f in files:
        data = await f.read()
        size = len(data)
        total += size
        if size == 0:
            continue
        if total > MAX_UPLOAD_BYTES:
            return JSONResponse({"status":"error","message":f"Занадто великий upload (max {MAX_UPLOAD_MB}MB)"}, status_code=413)

        ext = _safe_ext(f.filename)
        token = secrets.token_hex(16)
        stored = f"{token}{ext}"

        path = os.path.join(UPLOAD_DIR, stored)
        with open(path, "wb") as out:
            out.write(data)

        uploaded.append({
            "name": f.filename,
            "stored_name": stored,
            "url": f"/files/{stored}",
            "mime": f.content_type or "",
            "size": size,
        })

    return {"status": "ok", "files": uploaded}

@fastapi_app.post("/api/hw_add")
async def api_add_hw(request: Request):
    data = await request.json()
    subject = data.get("subject")
    desc = data.get("description")
    due = data.get("date")
    author = data.get("author", "Mini App")
    author_id = data.get("author_id")
    attachments = data.get("attachments") or []
    is_important = int(data.get("is_important") or 0)

    if subject and desc and due:
        with dbc() as c:
            cur = c.execute("""
                INSERT INTO homework(subject, description, due_date, author_name, author_id, is_important)
                VALUES(%s,%s,%s,%s,%s,%s) RETURNING id
            """, (subject, desc, due, author, author_id, is_important))
            hw_id = cur.fetchone()["id"]

            for a in attachments:
                stored_name = a.get("stored_name")
                orig = a.get("name") or "file"
                mime = a.get("mime") or ""
                size = int(a.get("size") or 0)

                if not stored_name:
                    continue
                path = os.path.join(UPLOAD_DIR, stored_name)
                if not os.path.exists(path):
                    continue

                c.execute("""
                    INSERT INTO attachments(hw_id, original_name, stored_name, mime_type, size_bytes)
                    VALUES(%s,%s,%s,%s,%s)
                    ON CONFLICT (stored_name) DO NOTHING
                """, (hw_id, orig, stored_name, mime, size))

    return {"status": "ok"}

@fastapi_app.post("/api/hw_delete")
async def api_delete_hw(request: Request):
    data = await request.json()
    hw_id = data.get("id")
    if not hw_id:
        return {"status": "error", "message": "No ID provided"}

    with dbc() as c:
        rows = c.execute("SELECT stored_name FROM attachments WHERE hw_id=%s", (hw_id,)).fetchall()
        for r in rows:
            _delete_file_quiet(r["stored_name"])
        c.execute("DELETE FROM homework WHERE id=%s", (hw_id,))

    return {"status": "ok"}

@fastapi_app.post("/api/hw_update")
async def api_update_hw(request: Request):
    data = await request.json()
    hw_id = data.get("id")
    subject = data.get("subject")
    due = data.get("date")
    desc = data.get("description")
    attachments = data.get("attachments")
    is_important = int(data.get("is_important") or 0)

    if not hw_id:
        return {"status": "error", "message": "No ID provided"}
    if not (subject and due and desc):
        return {"status": "error", "message": "Invalid data"}

    with dbc() as c:
        c.execute("""
            UPDATE homework
            SET subject=%s, due_date=%s, description=%s, is_important=%s
            WHERE id=%s
        """, (subject, due, desc, is_important, hw_id))

        if attachments is not None:
            kept_names = {a.get("stored_name") for a in (attachments or []) if a.get("stored_name")}
            old = c.execute("SELECT stored_name FROM attachments WHERE hw_id=%s", (hw_id,)).fetchall()
            for r in old:
                if r["stored_name"] not in kept_names:
                    _delete_file_quiet(r["stored_name"])

            c.execute("DELETE FROM attachments WHERE hw_id=%s", (hw_id,))

            for a in (attachments or []):
                stored_name = a.get("stored_name")
                orig = a.get("name") or "file"
                mime = a.get("mime") or ""
                size = int(a.get("size") or 0)
                if not stored_name:
                    continue
                path = os.path.join(UPLOAD_DIR, stored_name)
                if not os.path.exists(path):
                    continue
                c.execute("""
                    INSERT INTO attachments(hw_id, original_name, stored_name, mime_type, size_bytes)
                    VALUES(%s,%s,%s,%s,%s)
                    ON CONFLICT (stored_name) DO NOTHING
                """, (hw_id, orig, stored_name, mime, size))

    return {"status": "ok"}

# Спеціальний маршрут для Cron-job (мінімальне навантаження)
@fastapi_app.get("/ping")
async def ping():
    return {"status": "alive", "timestamp": datetime.now(KYIV_TZ).isoformat()}

# Обробка Favicon (щоб прибрати 404 помилки з логів)
@fastapi_app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    from fastapi.responses import Response
    return Response(status_code=204)
# ==========================================
# 🚀 RUN
# ==========================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000)
