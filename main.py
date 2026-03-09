"""
╔══════════════════════════════════════════╗
║        ЩОДЕННИК КЛАСУ  •  v6.0           ║
║     FastAPI + Telegram Bot (Unified)     ║
║     PostgreSQL + Multi-Diary Edition     ║
╚══════════════════════════════════════════╝
"""

import logging
import os
import secrets
from datetime import datetime, date, timedelta, time
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Request, UploadFile, File, Response
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from telegram import (
    Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup,
    WebAppInfo, MenuButtonWebApp
)
from telegram.constants import ChatType
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

# ==========================================
# ⚙️ НАЛАШТУВАННЯ
# ==========================================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    log = logging.getLogger(__name__)
    log.warning("❌ BOT_TOKEN не задано.")

WEB_APP_URL  = os.getenv("WEB_APP_URL",  os.getenv("RENDER_EXTERNAL_URL", "https://tg-0ncg.onrender.com"))
WEBHOOK_URL  = os.getenv("WEBHOOK_URL") or os.getenv("RENDER_EXTERNAL_URL") or "https://tg-0ncg.onrender.com"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
WEBHOOK_PATH = "/webhook/telegram"
WEBHOOK_SECRET_ACTIVE = False
DATABASE_URL = os.getenv("DATABASE_URL")

UPLOAD_DIR = "uploads"
START_WEBAPP = WEB_APP_URL
MAX_UPLOAD_MB = 60
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

KYIV_TZ = ZoneInfo("Europe/Kyiv")

def today_kyiv() -> date:
    return datetime.now(KYIV_TZ).date()

# ==========================================
# 🗄 ДАНІ: РОЗКЛАД ТА ДЗВІНКИ
# ==========================================
DAYS_UA = ["Понеділок","Вівторок","Середа","Четвер","П'ятниця","Субота","Неділя"]

# ── 11 клас (за замовчуванням) ──────────────────────────────────────────────
SCHEDULE_11 = {
    "Понеділок": ["Алгебра","Геометрія","Фізика","Інформатика","Фізкультура","Англ. Мова","Біологія","Технології"],
    "Вівторок":  ["Хімія","Геометрія","Укр. Мова","Укр. Літ","Фізкультура","Фізика"],
    "Середа":    ["Укр. Мова","Мистецтво","Укр. Літ","Фізика","Географія"],
    "Четвер":    ["Історія","Алгебра","Хімія","Історія України","Біологія","Технології","Англ. Мова"],
    "П'ятниця":  ["Історія України","Зар. Літ","Астрономія","Укр. Мова","Фізкультура"],
}

# ── 9 клас ───────────────────────────────────────────────────────────────────
SCHEDULE_9 = {
    "Понеділок": ["Укр. Мова","Алгебра","Геометрія","Англ. Мова","Біологія","Фізкультура"],
    "Вівторок":  ["Хімія","Укр. Мова","Укр. Літ","Фізика","Алгебра","Інформатика"],
    "Середа":    ["Геометрія","Укр. Мова","Географія","Мистецтво","Англ. Мова"],
    "Четвер":    ["Фізика","Алгебра","Хімія","Біологія","Укр. Літ","Технології"],
    "П'ятниця":  ["Зар. Літ","Укр. Мова","Алгебра","Історія","Фізкультура"],
}

# Для зворотної сумісності
SCHEDULE = SCHEDULE_11

SCHEDULES: Dict[str, dict] = {
    "11": SCHEDULE_11,
    "9":  SCHEDULE_9,
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

def ei(s): return EMOJI.get(s, "📌")
def day_name(d: date): return DAYS_UA[d.weekday()]

# ==========================================
# 🗄 БАЗА ДАНИХ
# ==========================================
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
        # ── Основні таблиці ──────────────────────────────────────────────────
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
                is_important INTEGER DEFAULT 0,
                diary_id INTEGER
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
        c.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS enabled INTEGER DEFAULT 1")

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

        # ── Щоденники (multi-diary) ───────────────────────────────────────────
        c.execute("""
            CREATE TABLE IF NOT EXISTS diaries(
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                grade TEXT NOT NULL DEFAULT '9',
                owner_id BIGINT,
                schedule_key TEXT DEFAULT '9',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS diary_members(
                diary_id INTEGER NOT NULL REFERENCES diaries(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                role TEXT DEFAULT 'member',
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (diary_id, user_id)
            )
        """)

        # ── Міграції ─────────────────────────────────────────────────────────
        try:
            c.execute("ALTER TABLE homework ADD COLUMN is_important INTEGER DEFAULT 0")
        except Exception:
            pass
        try:
            c.execute("ALTER TABLE homework ADD COLUMN diary_id INTEGER")
        except Exception:
            pass

    # Seed: створюємо щоденник 9 класу для користувача 5331432346
    _ensure_9th_grade_diary()


# ==========================================
# 📓 ЩОДЕННИКИ — допоміжні функції
# ==========================================
DIARY_9_OWNER = 5331432346
DEFAULT_ADMIN_ID = 5360495885  # адмін 11 класу

def _ensure_9th_grade_diary() -> int:
    """Гарантує існування щоденника 9 класу. Повертає diary_id."""
    with dbc() as c:
        row = c.execute(
            "SELECT id FROM diaries WHERE owner_id=%s LIMIT 1",
            (DIARY_9_OWNER,)
        ).fetchone()
        if row:
            return int(row["id"])
        cur = c.execute(
            """INSERT INTO diaries(name, grade, owner_id, schedule_key)
               VALUES('Щоденник 9 класу','9',%s,'9') RETURNING id""",
            (DIARY_9_OWNER,)
        )
        diary_id = int(cur.fetchone()["id"])
        c.execute(
            """INSERT INTO diary_members(diary_id, user_id, role)
               VALUES(%s,%s,'admin') ON CONFLICT DO NOTHING""",
            (diary_id, DIARY_9_OWNER)
        )
        log.info("✅ Щоденник 9 класу створено (id=%d)", diary_id)
        return diary_id


def get_user_diary_context(user_id: Optional[int]) -> dict:
    """Повертає контекст щоденника для user_id."""
    default_ctx = {
        "diary_id": None,
        "is_diary_admin": user_id == DEFAULT_ADMIN_ID if user_id else False,
        "grade": "11",
        "schedule_key": "11",
        "name": "11 клас",
    }
    if not user_id:
        return default_ctx

    try:
        with dbc() as c:
            row = c.execute(
                """SELECT dm.diary_id, dm.role, d.grade, d.schedule_key, d.name
                   FROM diary_members dm
                   JOIN diaries d ON d.id = dm.diary_id
                   WHERE dm.user_id=%s
                   LIMIT 1""",
                (user_id,)
            ).fetchone()
    except Exception:
        return default_ctx

    if not row:
        return default_ctx

    return {
        "diary_id": int(row["diary_id"]),
        "is_diary_admin": (row["role"] == "admin"),
        "grade": row["grade"],
        "schedule_key": row["schedule_key"] or "9",
        "name": row["name"],
    }


def diary_get_members(diary_id: int) -> list:
    with dbc() as c:
        return c.execute(
            "SELECT user_id, role, added_at FROM diary_members WHERE diary_id=%s ORDER BY added_at",
            (diary_id,)
        ).fetchall()


def diary_add_member(diary_id: int, user_id: int, role: str = "member") -> bool:
    try:
        with dbc() as c:
            c.execute(
                """INSERT INTO diary_members(diary_id, user_id, role)
                   VALUES(%s,%s,%s)
                   ON CONFLICT (diary_id, user_id) DO UPDATE SET role=EXCLUDED.role""",
                (diary_id, user_id, role)
            )
        return True
    except Exception as e:
        log.error("diary_add_member error: %s", e)
        return False


def diary_remove_member(diary_id: int, user_id: int) -> bool:
    try:
        with dbc() as c:
            c.execute(
                "DELETE FROM diary_members WHERE diary_id=%s AND user_id=%s AND role!='admin'",
                (diary_id, user_id)
            )
        return True
    except Exception as e:
        log.error("diary_remove_member error: %s", e)
        return False


# ==========================================
# 🗄 HOMEWORK — основні функції
# ==========================================
def hw_cleanup():
    cutoff = (today_kyiv() - timedelta(days=3)).isoformat()
    with dbc() as c:
        return c.execute("DELETE FROM homework WHERE due_date < %s", (cutoff,)).rowcount

def sub_get(chat_id):
    with dbc() as c:
        return c.execute(
            "SELECT chat_id,username,mode,title,enabled FROM subscribers WHERE chat_id=%s",
            (chat_id,)
        ).fetchone()

def sub_touch(chat_id, username, mode="private", title=None, enabled_default=0):
    with dbc() as c:
        c.execute(
            """
            INSERT INTO subscribers(chat_id,username,mode,title,enabled)
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT (chat_id)
            DO UPDATE SET username=EXCLUDED.username, mode=EXCLUDED.mode, title=EXCLUDED.title
            """,
            (chat_id, username, mode, title, int(enabled_default))
        )

def sub_enable(chat_id, username, mode="private", title=None):
    with dbc() as c:
        c.execute(
            """
            INSERT INTO subscribers(chat_id,username,mode,title,enabled)
            VALUES(%s,%s,%s,%s,1)
            ON CONFLICT (chat_id)
            DO UPDATE SET username=EXCLUDED.username, mode=EXCLUDED.mode, title=EXCLUDED.title, enabled=1
            """,
            (chat_id, username, mode, title)
        )

def sub_disable(chat_id):
    with dbc() as c:
        c.execute(
            """
            INSERT INTO subscribers(chat_id,username,mode,title,enabled)
            VALUES(%s,NULL,'private',NULL,0)
            ON CONFLICT (chat_id)
            DO UPDATE SET enabled=0
            """,
            (chat_id,)
        )

def sub_all():
    with dbc() as c:
        return c.execute("SELECT chat_id FROM subscribers WHERE enabled=1").fetchall()

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


def hw_for_date_formatted(d: str, diary_id=None):
    with dbc() as c:
        if diary_id is None:
            rows = c.execute("""
                SELECT id, subject, description, due_date, author_name, author_id, is_important
                FROM homework
                WHERE due_date=%s AND diary_id IS NULL
                ORDER BY is_important DESC, subject
            """, (d,)).fetchall()
        else:
            rows = c.execute("""
                SELECT id, subject, description, due_date, author_name, author_id, is_important
                FROM homework
                WHERE due_date=%s AND diary_id=%s
                ORDER BY is_important DESC, subject
            """, (d, diary_id)).fetchall()

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
# 🤖 ТЕЛЕГРАМ БОТ
# ==========================================
DIV = "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔"
HEADER_MAIN  = f"📚 *Щоденник Класу*\n{DIV}\nОбери розділ:"
HEADER_SCHED = f"📆 *Розклад уроків*\n{DIV}\nОбери день:"

def kb(*rows): return InlineKeyboardMarkup(list(rows))
def _back(cb="go_main", label="◀️  Назад"): return InlineKeyboardButton(label, callback_data=cb)

def kb_main(chat_type: str):
    if chat_type == ChatType.PRIVATE:
        open_btn = InlineKeyboardButton(
            "📱 Відкрити Щоденник",
            web_app=WebAppInfo(url=WEB_APP_URL),
        )
    else:
        open_btn = InlineKeyboardButton(
            "🤖 Відкрити в боті",
            url=(START_WEBAPP or WEB_APP_URL),
        )
    return kb(
        [open_btn],
        [InlineKeyboardButton("📆  Розклад",            callback_data="menu_schedule")],
        [InlineKeyboardButton("🔔  Підписка",           callback_data="menu_sub")],
        [InlineKeyboardButton("❓  Допомога",           callback_data="help")],
        [InlineKeyboardButton("✖  Закрити меню",       callback_data="close_menu")],
    )

def kb_schedule_days(schedule: dict):
    days = list(schedule.keys())
    btns = [InlineKeyboardButton(d, callback_data=f"sched_{d}") for d in days]
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

async def delete_private_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_chat and update.effective_chat.type == ChatType.PRIVATE and update.message:
            await delete_msg(update.message)
    except Exception:
        pass

async def go_main(q, ctx):
    chat_type = q.message.chat.type
    await q.edit_message_text(
        HEADER_MAIN,
        parse_mode="Markdown",
        reply_markup=kb_main(chat_type),
    )

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    chat = update.effective_chat
    title = chat.title if chat.type != "private" else None
    rec = sub_get(chat.id)
    mode = "private" if chat.type == "private" else "group"
    if not rec:
        sub_enable(chat.id, u.username or u.first_name, mode, title)
    else:
        sub_touch(chat.id, u.username or u.first_name, mode, title)

    chat_type = chat.type
    payload = (ctx.args[0].strip().lower() if ctx.args else "")
    if chat_type == ChatType.PRIVATE and payload == "webapp":
        await update.message.reply_text(
            HEADER_MAIN, parse_mode="Markdown", reply_markup=kb_main(chat_type),
        )
        return

    greeting = (
        f"👋 Вітаємо, *{u.first_name}*!\n\n📚 *Щоденник Класу* — офіційний бот класу.\n{DIV}\n"
        f"Тут зберігається домашнє завдання,\nрозклад уроків і нагадування.\n\nОбери розділ:"
    ) if chat.type == "private" else (
        f"📚 *Щоденник Класу* підключено!\n{DIV}\nНагадування надходитимуть щодня о *08:00*."
    )
    await update.message.reply_text(
        greeting, parse_mode="Markdown", reply_markup=kb_main(chat_type),
    )
    if chat_type != ChatType.PRIVATE:
        await delete_msg(update.message)

async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_type = update.effective_chat.type
    await update.message.reply_text(
        HEADER_MAIN, parse_mode="Markdown", reply_markup=kb_main(chat_type),
    )
    await delete_msg(update.message)

async def cmd_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    diary_ctx = get_user_diary_context(user_id)
    schedule = SCHEDULES.get(diary_ctx["schedule_key"], SCHEDULE_11)
    await update.message.reply_text(
        HEADER_SCHED, parse_mode="Markdown",
        reply_markup=kb_schedule_days(schedule)
    )
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
    user_id = update.effective_user.id if update.effective_user else None
    diary_ctx = get_user_diary_context(user_id)
    schedule = SCHEDULES.get(diary_ctx["schedule_key"], SCHEDULE_11)
    await q.edit_message_text(
        HEADER_SCHED, parse_mode="Markdown",
        reply_markup=kb_schedule_days(schedule)
    )

async def cb_sched_day(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = update.effective_user.id if update.effective_user else None
    diary_ctx = get_user_diary_context(user_id)
    schedule = SCHEDULES.get(diary_ctx["schedule_key"], SCHEDULE_11)

    day = q.data.replace("sched_", "")
    subjects = schedule.get(day, [])
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
        text, parse_mode="Markdown",
        reply_markup=kb([_back("menu_schedule", "◀️  До розкладу")], [_back()])
    )

async def cb_menu_sub(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    rec = sub_get(update.effective_chat.id)
    is_active = bool(rec and int(rec.get("enabled", 1)) == 1)
    status = (
        f"✅ *Активна* — {'в групу 👥' if rec and rec.get('mode')=='group' else 'приватно 👤'}"
        if is_active else "❌ *Не активна*"
    )
    await q.edit_message_text(
        f"🔔 *Підписка*\n{DIV}\n\nСтатус: {status}\n\nЩодня о *08:00* надходить список Д/З на поточний день.",
        parse_mode="Markdown", reply_markup=kb_sub(is_active)
    )

async def cb_sub_private(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_chat.type != "private":
        return await q.answer("⚠️ Тільки в приватному чаті!", show_alert=True)
    sub_enable(update.effective_chat.id, update.effective_user.first_name, "private")
    await q.edit_message_text(
        f"✅ *Підписку оформлено!*\n{DIV}\n\n👤 Нагадування щодня о *08:00*.",
        parse_mode="Markdown", reply_markup=kb([_back()])
    )

async def cb_sub_group_info(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        f"👥 *Підписка групи*\n{DIV}\n\n1️⃣  Додай бота до групи\n2️⃣  Напиши в групі /start\n3️⃣  Готово\n\n💡 Група отримуватиме Д/З о *08:00*.",
        parse_mode="Markdown", reply_markup=kb([_back("menu_sub")])
    )

async def cb_sub_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    sub_disable(update.effective_chat.id)
    await q.edit_message_text(
        f"🚫 *Підписку скасовано*\n{DIV}\n\nРанкові нагадування вимкнено.",
        parse_mode="Markdown", reply_markup=kb([_back()])
    )

async def cb_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        f"❓ *Довідка*\n{DIV}\n\n"
        "📱 *Щоденник* — відкриває міні-додаток.\n\n"
        "📎 *Вкладення* — pdf/фото/відео до завдання.\n\n"
        "📆 *Розклад* — уроки і час дзвінків.\n"
        "🔔 *Підписка* — щоденне нагадування о 08:00.\n"
        f"{DIV}\n"
        "🤖 *Команди:*\n/menu — головне меню\n/schedule — розклад\n\n"
        "🧹 Старі завдання автоматично видаляються.",
        parse_mode="Markdown", reply_markup=kb([_back()])
    )

async def _broadcast(bot, text: str, chat_ids=None):
    if chat_ids is None:
        targets = sub_all()
        chat_ids = [r["chat_id"] for r in targets]
    for cid in chat_ids:
        try:
            await bot.send_message(cid, text, parse_mode="Markdown")
        except Exception as ex:
            log.warning("Broadcast failed %s: %s", cid, ex)


def _get_diary_subscriber_ids(diary_id: Optional[int]) -> List[int]:
    """Повертає chat_id підписників для конкретного щоденника."""
    if diary_id is None:
        # Усі підписники, яких немає в жодному custom diary
        try:
            with dbc() as c:
                members_in_custom = set(
                    r["user_id"] for r in c.execute(
                        "SELECT user_id FROM diary_members"
                    ).fetchall()
                )
                rows = c.execute(
                    "SELECT chat_id FROM subscribers WHERE enabled=1"
                ).fetchall()
            return [r["chat_id"] for r in rows if r["chat_id"] not in members_in_custom]
        except Exception:
            return []
    else:
        try:
            with dbc() as c:
                members = c.execute(
                    "SELECT user_id FROM diary_members WHERE diary_id=%s", (diary_id,)
                ).fetchall()
                member_ids = [r["user_id"] for r in members]
                if not member_ids:
                    return []
                rows = c.execute(
                    "SELECT chat_id FROM subscribers WHERE enabled=1 AND chat_id = ANY(%s)",
                    (member_ids,)
                ).fetchall()
            return [r["chat_id"] for r in rows]
        except Exception:
            return []


def _build_morning_text(today: date, diary_id: Optional[int], schedule_key: str) -> str:
    schedule = SCHEDULES.get(schedule_key, SCHEDULE_11)
    dn = DAYS_UA[today.weekday()]
    subjects = schedule.get(dn, [])
    sched_lines = ""
    lesson_idx = 0
    for num, start, end in BELLS:
        if num == 0:
            sched_lines += f"   ☕ Перерва {start}–{end}\n"
        else:
            if lesson_idx < len(subjects):
                s = subjects[lesson_idx]
                sched_lines += f"╭─ *{num}.* {ei(s)} {s}\n╰─ {start}–{end}\n"
                lesson_idx += 1

    text = f"☀️ *Доброго ранку!*\n📅 *{dn}, {today.strftime('%d.%m')}*\n{DIV}\n\n📆 *Розклад на сьогодні:*\n{sched_lines}\n"
    rows = hw_for_date_formatted(today.isoformat(), diary_id=diary_id)
    if rows:
        text += "📚 *Д/З на сьогодні:*\n"
        for r in rows:
            imp  = "🔴 " if r.get("is_important") else ""
            clip = " 📎" if r.get("attachments") else ""
            text += f"╭─ {imp}{ei(r['subject'])} *{r['subject']}*{clip}\n│  📋 {r['description']}\n╰─ 👤 {r['author']}\n\n"
    else:
        text += "📭 Д/З на сьогодні немає 🎉\n"
    return text


# ==========================================
# ⏰ JOBS
# ==========================================
async def job_morning(ctx: ContextTypes.DEFAULT_TYPE):
    today = today_kyiv()
    if today.weekday() >= 5:
        return

    # Надіслати для щоденника за замовчуванням (11 клас)
    text_11 = _build_morning_text(today, diary_id=None, schedule_key="11")
    ids_11 = _get_diary_subscriber_ids(None)
    await _broadcast(ctx.bot, text_11, ids_11)

    # Надіслати для кожного кастомного щоденника
    try:
        with dbc() as c:
            diaries = c.execute("SELECT id, schedule_key FROM diaries").fetchall()
        for d in diaries:
            text = _build_morning_text(today, diary_id=int(d["id"]), schedule_key=d["schedule_key"] or "9")
            ids = _get_diary_subscriber_ids(int(d["id"]))
            await _broadcast(ctx.bot, text, ids)
    except Exception as e:
        log.error("job_morning diary error: %s", e)


async def job_evening(ctx: ContextTypes.DEFAULT_TYPE):
    today = today_kyiv()
    if today.weekday() >= 5:
        return
    tomorrow = today + timedelta(days=1)
    if tomorrow.weekday() >= 5:
        return

    async def _send_evening(diary_id, schedule_key):
        rows = hw_for_date_formatted(tomorrow.isoformat(), diary_id=diary_id)
        important = [r for r in rows if r.get("is_important")]
        if not important:
            return
        dn = DAYS_UA[tomorrow.weekday()]
        text = f"🔴 *Важливе Д/З на завтра — {dn}, {tomorrow.strftime('%d.%m')}*\n{DIV}\n\n"
        for r in important:
            clip = " 📎" if r.get("attachments") else ""
            text += f"╭─ {ei(r['subject'])} *{r['subject']}*{clip}\n│  📋 {r['description']}\n╰─ 👤 {r['author']}\n\n"
        ids = _get_diary_subscriber_ids(diary_id)
        await _broadcast(ctx.bot, text, ids)

    await _send_evening(None, "11")
    try:
        with dbc() as c:
            diaries = c.execute("SELECT id, schedule_key FROM diaries").fetchall()
        for d in diaries:
            await _send_evening(int(d["id"]), d["schedule_key"] or "9")
    except Exception as e:
        log.error("job_evening diary error: %s", e)


async def job_sunday_evening(ctx: ContextTypes.DEFAULT_TYPE):
    today = today_kyiv()
    if today.weekday() != 6:
        return
    tomorrow = today + timedelta(days=1)
    dn = DAYS_UA[tomorrow.weekday()]

    async def _send_sunday(diary_id, schedule_key):
        rows = hw_for_date_formatted(tomorrow.isoformat(), diary_id=diary_id)
        has_imp = any(r.get("is_important") for r in rows)
        if rows:
            text = f"📋 *Д/З на завтра — {dn}, {tomorrow.strftime('%d.%m')}*\n{DIV}\n\n"
            if has_imp:
                text += "⚠️ *Є важливі завдання!*\n\n"
            for r in rows:
                imp  = "🔴 " if r.get("is_important") else ""
                clip = " 📎" if r.get("attachments") else ""
                text += f"╭─ {imp}{ei(r['subject'])} *{r['subject']}*{clip}\n│  📋 {r['description']}\n╰─ 👤 {r['author']}\n\n"
        else:
            text = f"📋 *Д/З на завтра — {dn}, {tomorrow.strftime('%d.%m')}*\n{DIV}\n\n📭 На понеділок Д/З немає 🎉\nГарного відпочинку!\n"
        ids = _get_diary_subscriber_ids(diary_id)
        await _broadcast(ctx.bot, text, ids)

    await _send_sunday(None, "11")
    try:
        with dbc() as c:
            diaries = c.execute("SELECT id, schedule_key FROM diaries").fetchall()
        for d in diaries:
            await _send_sunday(int(d["id"]), d["schedule_key"] or "9")
    except Exception as e:
        log.error("job_sunday_evening diary error: %s", e)


async def job_cleanup(ctx: ContextTypes.DEFAULT_TYPE):
    n = hw_cleanup()
    if n:
        log.info("🧹 Автоочищення: %d Д/З видалено", n)


# ==========================================
# 🌐 FASTAPI
# ==========================================
ptb_app = Application.builder().token(TOKEN).build() if TOKEN else None

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    if ptb_app:
        await ptb_app.initialize()
        await ptb_app.bot.set_my_commands([
            BotCommand("start", "🚀 Запустити бота"),
            BotCommand("menu", "📚 Головне меню"),
            BotCommand("schedule", "📆 Розклад уроків"),
        ])

        global START_WEBAPP
        try:
            me = await ptb_app.bot.get_me()
            if getattr(me, "username", None):
                START_WEBAPP = f"https://t.me/{me.username}?start=webapp"
        except Exception:
            pass

        await ptb_app.bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text="📱 Щоденник",
                web_app=WebAppInfo(url=WEB_APP_URL)
            )
        )

        ptb_app.add_handler(CommandHandler("start", cmd_start))
        ptb_app.add_handler(CommandHandler("menu", cmd_menu))
        ptb_app.add_handler(CommandHandler("schedule", cmd_schedule))
        ptb_app.add_handler(MessageHandler(filters.COMMAND & filters.ChatType.PRIVATE, delete_private_command), group=1)
        ptb_app.add_handler(CallbackQueryHandler(cb_close_menu, pattern="^close_menu$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_go_main, pattern="^go_main$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_menu_schedule, pattern="^menu_schedule$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_sched_day, pattern="^sched_"))
        ptb_app.add_handler(CallbackQueryHandler(cb_menu_sub, pattern="^menu_sub$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_sub_private, pattern="^sub_private$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_sub_group_info, pattern="^sub_group_info$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_sub_cancel, pattern="^sub_cancel$"))
        ptb_app.add_handler(CallbackQueryHandler(cb_help, pattern="^help$"))

        jq = ptb_app.job_queue
        jq.run_daily(job_morning, time=time(hour=8, minute=0, tzinfo=KYIV_TZ))
        jq.run_daily(job_evening, time=time(hour=18, minute=0, tzinfo=KYIV_TZ))
        jq.run_daily(job_sunday_evening, time=time(hour=18, minute=0, tzinfo=KYIV_TZ))
        jq.run_daily(job_cleanup, time=time(hour=0, minute=5, tzinfo=KYIV_TZ))

        await ptb_app.start()

        if not WEBHOOK_URL:
            raise RuntimeError("WEBHOOK_URL must be set")

        webhook_url = WEBHOOK_URL.rstrip('/') + WEBHOOK_PATH
        global WEBHOOK_SECRET_ACTIVE
        WEBHOOK_SECRET_ACTIVE = False
        try:
            if WEBHOOK_SECRET:
                await ptb_app.bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET, drop_pending_updates=False)
                WEBHOOK_SECRET_ACTIVE = True
            else:
                await ptb_app.bot.set_webhook(url=webhook_url, drop_pending_updates=False)
        except TypeError:
            await ptb_app.bot.set_webhook(webhook_url)
        log.info('Webhook set to %s', webhook_url)

    yield

    if ptb_app:
        await ptb_app.stop()
        await ptb_app.shutdown()


fastapi_app = FastAPI(lifespan=lifespan)


@fastapi_app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if not ptb_app:
        return JSONResponse({"status": "no bot"}, status_code=503)
    if WEBHOOK_SECRET_ACTIVE:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
            return JSONResponse({"status": "forbidden"}, status_code=403)
    try:
        payload = await request.json()
        update = Update.de_json(payload, ptb_app.bot)
    except Exception as e:
        log.warning('Bad webhook payload: %s', e)
        return JSONResponse({"status": "bad_request"}, status_code=400)
    await ptb_app.process_update(update)
    return JSONResponse({"status": "ok"})


@fastapi_app.get("/files/{stored_name}")
async def get_file(stored_name: str):
    path = os.path.join(UPLOAD_DIR, stored_name)
    if not os.path.exists(path):
        return JSONResponse({"status": "error", "message": "File not found"}, status_code=404)
    return FileResponse(path, filename=stored_name)


@fastapi_app.head("/")
async def head_root():
    return Response(status_code=200)


@fastapi_app.get("/", response_class=HTMLResponse)
async def read_root():
    with open("templates/index.html", "r", encoding="utf-8") as f:
        return f.read()


# ─────────────────────────────────────────────────────────────────────────────
# 📡 API — USER CONTEXT
# ─────────────────────────────────────────────────────────────────────────────
@fastapi_app.get("/api/user_context")
async def api_user_context(user_id: Optional[int] = None):
    ctx = get_user_diary_context(user_id)
    schedule_key = ctx["schedule_key"]
    schedule = SCHEDULES.get(schedule_key, SCHEDULE_11)
    return {
        "diary_id": ctx["diary_id"],
        "is_diary_admin": ctx["is_diary_admin"],
        "grade": ctx["grade"],
        "schedule_key": schedule_key,
        "name": ctx["name"],
        "schedule": schedule,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 📡 API — HOMEWORK
# ─────────────────────────────────────────────────────────────────────────────
@fastapi_app.get("/api/hw")
async def get_hw_api(user_id: Optional[int] = None):
    ctx = get_user_diary_context(user_id)
    diary_id = ctx["diary_id"]
    today = today_kyiv()
    data = {}
    for i in range(3):
        target_date = today + timedelta(days=i)
        iso_date = target_date.isoformat()
        label = "Сьогодні" if i == 0 else "Завтра" if i == 1 else target_date.strftime('%d.%m')
        data[iso_date] = {"label": label, "tasks": hw_for_date_formatted(iso_date, diary_id=diary_id)}
    return data


@fastapi_app.get("/api/hw_all")
async def get_hw_all_api(user_id: Optional[int] = None):
    ctx = get_user_diary_context(user_id)
    diary_id = ctx["diary_id"]
    today = today_kyiv().isoformat()
    if not DATABASE_URL:
        return []
    with dbc() as c:
        if diary_id is None:
            rows = c.execute("""
                SELECT id, subject, description, author_name, author_id, due_date, is_important
                FROM homework
                WHERE due_date >= %s AND diary_id IS NULL
                ORDER BY due_date, is_important DESC, subject
            """, (today,)).fetchall()
        else:
            rows = c.execute("""
                SELECT id, subject, description, author_name, author_id, due_date, is_important
                FROM homework
                WHERE due_date >= %s AND diary_id=%s
                ORDER BY due_date, is_important DESC, subject
            """, (today, diary_id)).fetchall()

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
            "name": f.filename, "stored_name": stored,
            "url": f"/files/{stored}", "mime": f.content_type or "", "size": size,
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

    # Визначаємо diary_id з контексту користувача
    user_id = int(author_id) if author_id else None
    ctx = get_user_diary_context(user_id)
    diary_id = ctx["diary_id"]

    if subject and desc and due:
        with dbc() as c:
            cur = c.execute("""
                INSERT INTO homework(subject, description, due_date, author_name, author_id, is_important, diary_id)
                VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (subject, desc, due, author, author_id, is_important, diary_id))
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
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT (stored_name) DO NOTHING
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
            UPDATE homework SET subject=%s, due_date=%s, description=%s, is_important=%s
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
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT (stored_name) DO NOTHING
                """, (hw_id, orig, stored_name, mime, size))
    return {"status": "ok"}


# ─────────────────────────────────────────────────────────────────────────────
# 📡 API — DIARY MEMBERS (адмін щоденника)
# ─────────────────────────────────────────────────────────────────────────────
@fastapi_app.get("/api/diary/members")
async def api_diary_members(user_id: Optional[int] = None):
    if not user_id:
        return JSONResponse({"status": "error", "message": "user_id required"}, status_code=400)
    ctx = get_user_diary_context(user_id)
    if not ctx["is_diary_admin"] or ctx["diary_id"] is None:
        return JSONResponse({"status": "error", "message": "Not a diary admin"}, status_code=403)
    members = diary_get_members(ctx["diary_id"])
    return {
        "diary_id": ctx["diary_id"],
        "diary_name": ctx["name"],
        "members": [
            {"user_id": int(m["user_id"]), "role": m["role"],
             "added_at": str(m["added_at"])[:16] if m["added_at"] else "—"}
            for m in members
        ]
    }


@fastapi_app.post("/api/diary/add_member")
async def api_diary_add_member(request: Request):
    data = await request.json()
    admin_id = data.get("admin_user_id")
    new_uid = data.get("user_id")
    role = data.get("role", "member")

    if not admin_id or not new_uid:
        return JSONResponse({"status": "error", "message": "admin_user_id and user_id required"}, status_code=400)

    ctx = get_user_diary_context(int(admin_id))
    if not ctx["is_diary_admin"] or ctx["diary_id"] is None:
        return JSONResponse({"status": "error", "message": "Not a diary admin"}, status_code=403)

    if role not in ("member", "admin"):
        role = "member"

    ok = diary_add_member(ctx["diary_id"], int(new_uid), role)
    return {"status": "ok" if ok else "error"}


@fastapi_app.post("/api/diary/remove_member")
async def api_diary_remove_member(request: Request):
    data = await request.json()
    admin_id = data.get("admin_user_id")
    target_uid = data.get("user_id")

    if not admin_id or not target_uid:
        return JSONResponse({"status": "error", "message": "admin_user_id and user_id required"}, status_code=400)

    ctx = get_user_diary_context(int(admin_id))
    if not ctx["is_diary_admin"] or ctx["diary_id"] is None:
        return JSONResponse({"status": "error", "message": "Not a diary admin"}, status_code=403)

    if int(target_uid) == int(admin_id):
        return JSONResponse({"status": "error", "message": "Cannot remove yourself"}, status_code=400)

    ok = diary_remove_member(ctx["diary_id"], int(target_uid))
    return {"status": "ok" if ok else "error"}


# ─────────────────────────────────────────────────────────────────────────────
# 📡 Ping / Favicon
# ─────────────────────────────────────────────────────────────────────────────
@fastapi_app.get("/ping")
async def ping():
    return {"status": "alive", "timestamp": datetime.now(KYIV_TZ).isoformat()}

@fastapi_app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)


# ==========================================
# 🚀 RUN
# ==========================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=8000)
