"""
╔══════════════════════════════════════════╗
║        ЩОДЕННИК КЛАСУ  •  v5.6+          ║
║  FastAPI + Telegram Bot (Unified, Stable)║
║        PostgreSQL Cloud Edition          ║
╚══════════════════════════════════════════╝
"""

from __future__ import annotations

import logging
import os
import re
import secrets
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time
from typing import Any, Dict, List, Optional, Sequence

from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, Response
from starlette.concurrency import run_in_threadpool

from telegram import (
    Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, MenuButtonWebApp
)
from telegram.constants import ChatType
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)

# ==========================================================
# ⚙️ CONFIG
# ==========================================================
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO
)
log = logging.getLogger("diary")

TOKEN = os.getenv("BOT_TOKEN")
WEB_APP_URL = os.getenv("WEB_APP_URL", "https://tg-0ncg.onrender.com")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://tg-0ncg.onrender.com")
WEBHOOK_PATH = "/webhook/telegram"
DATABASE_URL = os.getenv("DATABASE_URL")

UPLOAD_DIR = "uploads"
MAX_UPLOAD_MB = 60
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

KYIV_TZ = ZoneInfo("Europe/Kyiv")

def today_kyiv() -> date:
    return datetime.now(KYIV_TZ).date()

# ==========================================================
# 🗓 DATA
# ==========================================================
DAYS_UA = ["Понеділок","Вівторок","Середа","Четвер","П'ятниця","Субота","Неділя"]

SCHEDULE = {
    "Понеділок": ["Алгебра","Фізика","Інформатика","Фізкультура","Англ. Мова","Біологія","Технології"],
    "Вівторок":  ["Хімія","Геометрія","Укр. Мова","Укр. Літ","Фізкультура","Фізика"],
    "Середа":    ["Укр. Мова","Укр. Літ","Фізика","Географія"],
    "Четвер":    ["Історія","Алгебра","Хімія","Історія України","Біологія","Технології","Англ. Мова"],
    "П'ятниця":  ["Історія України","Зар. Літ","Астрономія","Укр. Мова","Фізкультура"],
}

BELLS = [
    (1,"09:00","09:45"),(2,"09:55","10:40"),(3,"10:50","11:35"),
    (4,"11:45","12:30"),(0,"12:30","13:00"),
    (5,"13:00","13:45"),(6,"13:55","14:40"),
    (7,"14:50","15:35"),(8,"15:45","16:30"),
]

EMOJI = {
    "Алгебра":"📐","Геометрія":"📏","Фізика":"⚛️","Хімія":"🧪","Біологія":"🌿",
    "Географія":"🌍","Астрономія":"🔭","Інформатика":"💻",
    "Технології":"🔧","Англ. Мова":"🇬🇧","Укр. Мова":"🇺🇦",
    "Укр. Літ":"📖","Зар. Літ":"📚","Історія":"🏛️","Історія України":"🏳️",
    "Мистецтво":"🎨","Фізкультура":"⚽",
}
def ei(s: str) -> str:
    return EMOJI.get(s, "📌")

DIV = "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔"
HEADER_MAIN  = f"📚 *Щоденник Класу*\n{DIV}\nОбери розділ:"
HEADER_SCHED = f"📆 *Розклад уроків*\n{DIV}\nОбери день:"

# ==========================================================
# 🗄 DB (POOL + THREADSAFE EXEC)
# ==========================================================
@dataclass
class DB:
    pool: ThreadedConnectionPool

    def _exec(self, query: str, params: Optional[Sequence[Any]] = None, fetch: str = "none"):
        """
        fetch: "none" | "one" | "all"
        """
        conn = self.pool.getconn()
        try:
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
                return None
        finally:
            self.pool.putconn(conn)

_db: Optional[DB] = None

async def db_exec(query: str, params: Optional[Sequence[Any]] = None, fetch: str = "none"):
    if _db is None:
        raise RuntimeError("Database is not initialized")
    return await run_in_threadpool(_db._exec, query, params, fetch)

async def init_db():
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    if not DATABASE_URL:
        log.warning("DATABASE_URL is not set — DB features disabled.")
        return

    global _db
    if _db is None:
        # conservative pool size for Render
        pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=6,
            dsn=DATABASE_URL,
        )
        _db = DB(pool=pool)

    # migrations / schema
    # Do not crash on already-exists: use IF NOT EXISTS / try-catch only where needed
    try:
        await db_exec("ALTER TABLE homework ADD COLUMN IF NOT EXISTS is_important INTEGER DEFAULT 0")
    except Exception as e:
        log.warning("ALTER homework is_important failed: %s", e)

    await db_exec("""
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
    await db_exec("""
        CREATE TABLE IF NOT EXISTS subscribers(
            chat_id BIGINT PRIMARY KEY,
            username TEXT,
            mode TEXT DEFAULT 'private',
            title TEXT
        )
    """)
    await db_exec("""
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

async def close_db():
    global _db
    if _db is not None:
        try:
            _db.pool.closeall()
        except Exception:
            pass
        _db = None

async def hw_cleanup() -> int:
    if not DATABASE_URL:
        return 0
    cutoff = (today_kyiv() - timedelta(days=3)).isoformat()
    # rowcount isn't directly returned via our helper, so fetch affected via RETURNING
    rows = await db_exec(
        "DELETE FROM homework WHERE due_date < %s RETURNING id",
        (cutoff,),
        fetch="all"
    )
    return len(rows or [])

async def sub_get(chat_id: int) -> Optional[Dict[str, Any]]:
    if not DATABASE_URL:
        return None
    return await db_exec(
        "SELECT chat_id,username,mode,title FROM subscribers WHERE chat_id=%s",
        (chat_id,),
        fetch="one"
    )

async def sub_add(chat_id: int, username: str, mode: str = "private", title: Optional[str] = None):
    if not DATABASE_URL:
        return
    await db_exec(
        """
        INSERT INTO subscribers(chat_id,username,mode,title)
        VALUES(%s,%s,%s,%s)
        ON CONFLICT (chat_id)
        DO UPDATE SET username=EXCLUDED.username, mode=EXCLUDED.mode, title=EXCLUDED.title
        """,
        (chat_id, username, mode, title)
    )

async def sub_remove(chat_id: int):
    if not DATABASE_URL:
        return
    await db_exec("DELETE FROM subscribers WHERE chat_id=%s", (chat_id,))

async def sub_all() -> List[Dict[str, Any]]:
    if not DATABASE_URL:
        return []
    rows = await db_exec("SELECT chat_id FROM subscribers", fetch="all")
    return list(rows or [])

async def _attachments_for_hw_ids(ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not DATABASE_URL or not ids:
        return {}

    rows = await db_exec(
        """
        SELECT id, hw_id, original_name, stored_name, mime_type, size_bytes
        FROM attachments
        WHERE hw_id = ANY(%s)
        ORDER BY id
        """,
        (ids,),
        fetch="all"
    )

    out: Dict[int, List[Dict[str, Any]]] = {}
    for r in rows or []:
        hw_id = int(r["hw_id"])
        out.setdefault(hw_id, []).append({
            "id": int(r["id"]),
            "name": r["original_name"],
            "url": f"/files/{r['stored_name']}",
            "mime": r["mime_type"] or "",
            "size": int(r["size_bytes"] or 0),
        })
    return out

async def hw_for_date_formatted(d: str) -> List[Dict[str, Any]]:
    if not DATABASE_URL:
        return []

    rows = await db_exec(
        """
        SELECT id, subject, description, due_date, author_name, author_id, is_important
        FROM homework
        WHERE due_date=%s
        ORDER BY is_important DESC, subject
        """,
        (d,),
        fetch="all"
    )

    rows = list(rows or [])
    ids = [int(r["id"]) for r in rows]
    att_map = await _attachments_for_hw_ids(ids)

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

# only allow our generated names: hex + optional ext
_STORED_RE = re.compile(r"^[a-f0-9]{32}(\.[a-z0-9]{1,12})?$", re.IGNORECASE)

# ==========================================================
# 🤖 TELEGRAM BOT UI
# ==========================================================
def kb(*rows): 
    return InlineKeyboardMarkup(list(rows))

def _back(cb: str = "go_main", label: str = "◀️  Назад"):
    return InlineKeyboardButton(label, callback_data=cb)

def kb_main(chat_type: str, start_webapp: str):
    if chat_type == ChatType.PRIVATE:
        open_btn = InlineKeyboardButton("📱 Відкрити Щоденник", web_app=WebAppInfo(url=WEB_APP_URL))
    else:
        open_btn = InlineKeyboardButton("🤖 Відкрити в боті", url=start_webapp or WEB_APP_URL)

    return kb(
        [open_btn],
        [InlineKeyboardButton("📆  Розклад", callback_data="menu_schedule")],
        [InlineKeyboardButton("🔔  Підписка", callback_data="menu_sub")],
        [InlineKeyboardButton("❓  Допомога", callback_data="help")],
        [InlineKeyboardButton("✖  Закрити меню", callback_data="close_menu")],
    )

def kb_schedule_days():
    btns = [InlineKeyboardButton(d, callback_data=f"sched_{d}") for d in SCHEDULE]
    rows = [[btns[i], btns[i+1]] if i+1 < len(btns) else [btns[i]] for i in range(0, len(btns), 2)]
    rows.append([_back()])
    return InlineKeyboardMarkup(rows)

def kb_sub(is_sub: bool):
    rows = [
        [InlineKeyboardButton("👤  Приватно (цей чат)", callback_data="sub_private")],
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

# ==========================================================
# 🤖 BOT HANDLERS
# ==========================================================
ptb_app: Optional[Application] = None
START_WEBAPP = ""

async def go_main(q, ctx):
    chat_type = q.message.chat.type
    try:
        await q.edit_message_text(
            HEADER_MAIN,
            parse_mode="Markdown",
            reply_markup=kb_main(chat_type, START_WEBAPP),
        )
    except Exception as e:
        log.debug("go_main edit failed: %s", e)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    chat = update.effective_chat
    title = chat.title if chat.type != "private" else None

    if chat.type != "private":
        rec = await sub_get(chat.id)
        if not rec:
            await sub_add(chat.id, u.username or u.first_name, "group", title)

    chat_type = chat.type
    payload = (ctx.args[0].strip().lower() if ctx.args else "")

    if chat_type == ChatType.PRIVATE and payload == "webapp":
        await update.message.reply_text(
            HEADER_MAIN,
            parse_mode="Markdown",
            reply_markup=kb_main(chat_type, START_WEBAPP),
        )
        return

    greeting = (
        f"👋 Вітаємо, *{u.first_name}*!\n\n📚 *Щоденник Класу* — офіційний бот класу.\n{DIV}\n"
        f"Тут зберігається домашнє завдання,\nрозклад уроків і нагадування.\n\nОбери розділ:"
    ) if chat.type == "private" else (
        f"📚 *Щоденник Класу* підключено!\n{DIV}\nНагадування надходитимуть щодня о *08:00*."
    )

    await update.message.reply_text(
        greeting,
        parse_mode="Markdown",
        reply_markup=kb_main(chat_type, START_WEBAPP),
    )

    if chat_type != ChatType.PRIVATE:
        await delete_msg(update.message)

async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_type = update.effective_chat.type
    await update.message.reply_text(
        HEADER_MAIN, parse_mode="Markdown", reply_markup=kb_main(chat_type, START_WEBAPP)
    )
    if chat_type != ChatType.PRIVATE:
        await delete_msg(update.message)

async def cmd_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HEADER_SCHED, parse_mode="Markdown", reply_markup=kb_schedule_days())
    if update.effective_chat.type != ChatType.PRIVATE:
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
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

async def cb_menu_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    try:
        await q.edit_message_text(HEADER_SCHED, parse_mode="Markdown", reply_markup=kb_schedule_days())
    except Exception:
        pass

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

    try:
        await q.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=kb([_back("menu_schedule", "◀️  До розкладу")], [_back()])
        )
    except Exception:
        pass

async def cb_menu_sub(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    rec = await sub_get(update.effective_chat.id)
    status = f"✅ *Активна* — {'в групу 👥' if rec and rec['mode']=='group' else 'приватно 👤'}" if rec else "❌ *Не активна*"
    try:
        await q.edit_message_text(
            f"🔔 *Підписка*\n{DIV}\n\nСтатус: {status}\n\nЩодня о *08:00* надходить список Д/З на поточний день.",
            parse_mode="Markdown",
            reply_markup=kb_sub(bool(rec))
        )
    except Exception:
        pass

async def cb_sub_private(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_chat.type != "private":
        return await q.answer("⚠️ Тільки в приватному чаті!", show_alert=True)
    await sub_add(update.effective_chat.id, update.effective_user.first_name, "private")
    try:
        await q.edit_message_text(
            f"✅ *Підписку оформлено!*\n{DIV}\n\n👤 Нагадування щодня о *08:00*.",
            parse_mode="Markdown",
            reply_markup=kb([_back()])
        )
    except Exception:
        pass

async def cb_sub_group_info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    try:
        await q.edit_message_text(
            f"👥 *Підписка групи*\n{DIV}\n\n1️⃣  Додай бота до групи\n2️⃣  Напиши в групі /start\n3️⃣  Готово\n\n💡 Група отримуватиме Д/З о *08:00*.",
            parse_mode="Markdown",
            reply_markup=kb([_back("menu_sub")])
        )
    except Exception:
        pass

async def cb_sub_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await sub_remove(update.effective_chat.id)
    try:
        await q.edit_message_text(
            f"🚫 *Підписку скасовано*\n{DIV}\n\nРанкові нагадування вимкнено.",
            parse_mode="Markdown",
            reply_markup=kb([_back()])
        )
    except Exception:
        pass

async def cb_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    try:
        await q.edit_message_text(
            f"❓ *Довідка*\n{DIV}\n\n"
            "📱 *Щоденник* — відкриває міні-додаток, де зберігаються всі завдання.\n\n"
            "📎 *Вкладення* — можна додати pdf/фото/відео до завдання.\n\n"
            "📆 *Розклад* — уроки і час дзвінків по днях тижня.\n"
            "🔔 *Підписка* — щоденне нагадування про Д/З о 08:00.\n"
            f"{DIV}\n"
            "🤖 *Команди:*\n"
            "/menu — головне меню\n"
            "/schedule — розклад\n\n"
            "🧹 Старі завдання автоматично видаляються.",
            parse_mode="Markdown",
            reply_markup=kb([_back()])
        )
    except Exception:
        pass

async def _broadcast(bot, text: str):
    for rec in await sub_all():
        cid = rec["chat_id"]
        try:
            await bot.send_message(cid, text, parse_mode="Markdown")
        except Exception as ex:
            log.warning("Broadcast failed %s: %s", cid, ex)

# ==========================================================
# ⏰ JOBS
# ==========================================================
async def job_morning(ctx: ContextTypes.DEFAULT_TYPE):
    """Пн–Пт 08:00 — розклад на сьогодні + список Д/З."""
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

    rows = await hw_for_date_formatted(today.isoformat())

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

    rows = await hw_for_date_formatted(tomorrow.isoformat())
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
    rows = await hw_for_date_formatted(tomorrow.isoformat())
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
    n = await hw_cleanup()
    if n:
        log.info("🧹 Автоочищення: %d Д/З видалено", n)

# ==========================================================
# 🌐 FASTAPI + LIFESPAN
# ==========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # DB
    await init_db()

    # BOT
    global ptb_app, START_WEBAPP

    if TOKEN:
        ptb_app = Application.builder().token(TOKEN).build()
        await ptb_app.initialize()

        bot_me = await ptb_app.bot.get_me()
        START_WEBAPP = f"https://t.me/{bot_me.username}?start=webapp"
        log.info("START_WEBAPP = %s", START_WEBAPP)

        await ptb_app.bot.set_my_commands([
            BotCommand("start", "🚀 Запустити бота"),
            BotCommand("menu", "📚 Головне меню"),
            BotCommand("schedule", "📆 Розклад уроків"),
        ])
        await ptb_app.bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(text="📱 Щоденник", web_app=WebAppInfo(url=WEB_APP_URL))
        )

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

        async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
            log.error("PTB error: %s", ctx.error, exc_info=ctx.error)

        ptb_app.add_error_handler(error_handler)

        # jobs
        jq = ptb_app.job_queue
        jq.run_daily(job_morning, time=time(hour=8, minute=0, tzinfo=KYIV_TZ))
        jq.run_daily(job_evening, time=time(hour=18, minute=0, tzinfo=KYIV_TZ))
        jq.run_daily(job_sunday_evening, time=time(hour=18, minute=0, tzinfo=KYIV_TZ))
        jq.run_daily(job_cleanup, time=time(hour=0, minute=5, tzinfo=KYIV_TZ))

        await ptb_app.start()

        if not WEBHOOK_URL:
            raise RuntimeError("WEBHOOK_URL must be set")

        await ptb_app.bot.delete_webhook(drop_pending_updates=True)
        webhook_url = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
        await ptb_app.bot.set_webhook(webhook_url)
        log.info("Webhook set to %s", webhook_url)
    else:
        ptb_app = None
        log.warning("BOT_TOKEN is not set — bot features disabled.")

    yield

    # shutdown
    if ptb_app:
        try:
            await ptb_app.bot.delete_webhook()
        except Exception:
            pass
        try:
            await ptb_app.stop()
            await ptb_app.shutdown()
        except Exception:
            pass

    await close_db()

fastapi_app = FastAPI(lifespan=lifespan)

@fastapi_app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if not ptb_app:
        return JSONResponse({"status": "no bot"}, status_code=503)
    try:
        payload = await request.json()
        update = Update.de_json(payload, ptb_app.bot)
        await ptb_app.process_update(update)
    except Exception as e:
        log.error("Webhook processing error: %s", e)
    return JSONResponse({"status": "ok"})

@fastapi_app.head("/")
async def head_root():
    return Response(status_code=200)

@fastapi_app.get("/", response_class=HTMLResponse)
async def read_root():
    try:
        with open("templates/index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>OK</h1><p>templates/index.html not found.</p>"

@fastapi_app.get("/ping")
async def ping():
    return {"status": "alive", "timestamp": datetime.now(KYIV_TZ).isoformat()}

@fastapi_app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@fastapi_app.get("/files/{stored_name}")
async def get_file(stored_name: str):
    if not _STORED_RE.match(stored_name or ""):
        raise HTTPException(status_code=400, detail="Invalid file id")
    path = os.path.join(UPLOAD_DIR, stored_name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=stored_name)

@fastapi_app.get("/api/hw")
async def get_hw_api():
    today = today_kyiv()
    data: Dict[str, Any] = {}
    for i in range(3):
        target_date = today + timedelta(days=i)
        iso_date = target_date.isoformat()
        label = "Сьогодні" if i == 0 else "Завтра" if i == 1 else target_date.strftime("%d.%m")
        data[iso_date] = {"label": label, "tasks": await hw_for_date_formatted(iso_date)}
    return data

@fastapi_app.get("/api/hw_all")
async def get_hw_all_api():
    if not DATABASE_URL:
        return []
    today = today_kyiv().isoformat()
    rows = await db_exec(
        """
        SELECT id, subject, description, author_name, author_id, due_date, is_important
        FROM homework
        WHERE due_date >= %s
        ORDER BY due_date, is_important DESC, subject
        """,
        (today,),
        fetch="all"
    )
    rows = list(rows or [])
    ids = [int(r["id"]) for r in rows]
    att_map = await _attachments_for_hw_ids(ids)

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

    uploaded: List[Dict[str, Any]] = []
    total = 0

    for f in files:
        ext = _safe_ext(f.filename)
        token = secrets.token_hex(16)
        stored = f"{token}{ext}"
        path = os.path.join(UPLOAD_DIR, stored)

        size = 0
        try:
            with open(path, "wb") as out:
                while True:
                    chunk = await f.read(1024 * 1024)  # 1MB chunks
                    if not chunk:
                        break
                    size += len(chunk)
                    total += len(chunk)
                    if total > MAX_UPLOAD_BYTES:
                        # cleanup partial file
                        try:
                            out.close()
                        except Exception:
                            pass
                        _delete_file_quiet(stored)
                        raise HTTPException(status_code=413, detail=f"Занадто великий upload (max {MAX_UPLOAD_MB}MB)")
                    out.write(chunk)
        finally:
            try:
                await f.close()
            except Exception:
                pass

        if size == 0:
            _delete_file_quiet(stored)
            continue

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
    if not DATABASE_URL:
        return {"status": "error", "message": "DB disabled"}

    data = await request.json()
    subject = data.get("subject")
    desc = data.get("description")
    due = data.get("date")
    author = data.get("author", "Mini App")
    author_id = data.get("author_id")
    attachments = data.get("attachments") or []
    is_important = int(data.get("is_important") or 0)

    if not (subject and desc and due):
        raise HTTPException(status_code=400, detail="Invalid data")

    row = await db_exec(
        """
        INSERT INTO homework(subject, description, due_date, author_name, author_id, is_important)
        VALUES(%s,%s,%s,%s,%s,%s) RETURNING id
        """,
        (subject, desc, due, author, author_id, is_important),
        fetch="one"
    )
    hw_id = int(row["id"])

    for a in attachments:
        stored_name = a.get("stored_name")
        orig = a.get("name") or "file"
        mime = a.get("mime") or ""
        size = int(a.get("size") or 0)

        if not stored_name or not _STORED_RE.match(stored_name):
            continue

        path = os.path.join(UPLOAD_DIR, stored_name)
        if not os.path.exists(path):
            continue

        await db_exec(
            """
            INSERT INTO attachments(hw_id, original_name, stored_name, mime_type, size_bytes)
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT (stored_name) DO NOTHING
            """,
            (hw_id, orig, stored_name, mime, size)
        )

    return {"status": "ok"}

@fastapi_app.post("/api/hw_delete")
async def api_delete_hw(request: Request):
    if not DATABASE_URL:
        return {"status": "error", "message": "DB disabled"}

    data = await request.json()
    hw_id = data.get("id")
    if not hw_id:
        raise HTTPException(status_code=400, detail="No ID provided")

    rows = await db_exec("SELECT stored_name FROM attachments WHERE hw_id=%s", (hw_id,), fetch="all")
    for r in rows or []:
        _delete_file_quiet(r["stored_name"])

    await db_exec("DELETE FROM homework WHERE id=%s", (hw_id,))
    return {"status": "ok"}

@fastapi_app.post("/api/hw_update")
async def api_update_hw(request: Request):
    if not DATABASE_URL:
        return {"status": "error", "message": "DB disabled"}

    data = await request.json()
    hw_id = data.get("id")
    subject = data.get("subject")
    due = data.get("date")
    desc = data.get("description")
    attachments = data.get("attachments")
    is_important = int(data.get("is_important") or 0)

    if not hw_id:
        raise HTTPException(status_code=400, detail="No ID provided")
    if not (subject and due and desc):
        raise HTTPException(status_code=400, detail="Invalid data")

    await db_exec(
        """
        UPDATE homework
        SET subject=%s, due_date=%s, description=%s, is_important=%s
        WHERE id=%s
        """,
        (subject, due, desc, is_important, hw_id)
    )

    if attachments is not None:
        kept_names = {a.get("stored_name") for a in (attachments or []) if a.get("stored_name")}
        old = await db_exec("SELECT stored_name FROM attachments WHERE hw_id=%s", (hw_id,), fetch="all")
        for r in old or []:
            if r["stored_name"] not in kept_names:
                _delete_file_quiet(r["stored_name"])

        await db_exec("DELETE FROM attachments WHERE hw_id=%s", (hw_id,))

        for a in (attachments or []):
            stored_name = a.get("stored_name")
            orig = a.get("name") or "file"
            mime = a.get("mime") or ""
            size = int(a.get("size") or 0)

            if not stored_name or not _STORED_RE.match(stored_name):
                continue
            path = os.path.join(UPLOAD_DIR, stored_name)
            if not os.path.exists(path):
                continue

            await db_exec(
                """
                INSERT INTO attachments(hw_id, original_name, stored_name, mime_type, size_bytes)
                VALUES(%s,%s,%s,%s,%s)
                ON CONFLICT (stored_name) DO NOTHING
                """,
                (hw_id, orig, stored_name, mime, size)
            )

    return {"status": "ok"}

# ==========================================================
# 🚀 RUN
# ==========================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000)