# 962ElStore - Single-file Phase 1
# All application Python code and the initial SQL migration are contained in this file.
from __future__ import annotations
import asyncio, hashlib, logging, os, re, sys, zlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger("962elstore")

# ===== SCHEMA VALIDATION =====
_SCHEMA_RE = re.compile(r"^[a-z][a-z0-9_]{2,40}$")

_RESERVED = {
    "public", "pg_catalog", "information_schema", "auth", "storage",
    "extensions", "realtime", "graphql", "graphql_public", "vault",
    "pgbouncer", "supabase_functions", "net", "cron",
}

def validate_schema_name(name: str) -> str:
    if not isinstance(name, str) or not _SCHEMA_RE.match(name):
        raise ValueError(
            f"اسم المخطط {name!r} غير صالح: حروف إنجليزية صغيرة وأرقام و _ فقط"
        )
    if name in _RESERVED or name.startswith("pg_"):
        raise ValueError(f"اسم المخطط {name!r} محجوز ولا يجوز استخدامه")
    return name

# ===== CONFIGURATION =====
class ConfigError(Exception):
    """خطأ في الإعدادات (رسالته بالعربية وواضحة للمستخدم)."""

from dataclasses import dataclass

@dataclass
class Admin:
    user_id: int
    name: str

class Settings:
    bot_token: str = field(repr=False)
    database_url: str = field(repr=False)
    admins: tuple
    schema_prod: str
    schema_test: str
    store_name: str
    log_level: str
    db_pool_max: int
    port: Optional[int]

def parse_admins(raw: str) -> tuple:
    admins = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ConfigError(
                "متغير ADMINS يجب أن يكون بالشكل: 123456789:محمد,987654321:حسام"
            )
        uid_text, name = part.split(":", 1)
        uid_text, name = uid_text.strip(), name.strip()
        if not uid_text.isdigit() or int(uid_text) <= 0 or not name:
            raise ConfigError(f"قيمة مشرف غير صحيحة في ADMINS: {part!r}")
        admins.append(Admin(int(uid_text), name))
    ids = [a.user_id for a in admins]
    if len(set(ids)) != len(ids):
        raise ConfigError("يوجد معرف مشرف مكرر في ADMINS")
    if not 1 <= len(admins) <= 2:
        raise ConfigError("ADMINS يجب أن يحتوي على مشرف واحد على الأقل وحد أقصى مشرفان")
    return tuple(admins)

def load_settings(env: Optional[Mapping[str, str]] = None) -> Settings:
    env = os.environ if env is None else env

    def required(name: str) -> str:
        value = (env.get(name) or "").strip()
        if not value:
            raise ConfigError(f"المتغير المطلوب {name} غير موجود. أضفه في إعدادات Render.")
        return value

    token = required("BOT_TOKEN")
    db_url = required("DATABASE_URL")
    if not db_url.startswith(("postgresql://", "postgres://")):
        raise ConfigError("DATABASE_URL يجب أن يبدأ بـ postgresql://")

    admins = parse_admins(required("ADMINS"))

    schema_prod = (env.get("DB_SCHEMA_PROD") or "els962").strip()
    schema_test = (env.get("DB_SCHEMA_TEST") or "els962_test").strip()
    try:
        validate_schema_name(schema_prod)
        validate_schema_name(schema_test)
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
    if schema_prod == schema_test:
        raise ConfigError("اسم مخطط الإنتاج والاختبار يجب أن يكونا مختلفين")

    try:
        pool_max = int(env.get("DB_POOL_MAX") or 5)
    except ValueError as exc:
        raise ConfigError("DB_POOL_MAX يجب أن يكون رقماً") from exc
    if not 2 <= pool_max <= 20:
        raise ConfigError("DB_POOL_MAX يجب أن يكون بين 2 و 20")

    port_raw = (env.get("PORT") or "").strip()
    port = int(port_raw) if port_raw.isdigit() else None

    return Settings(
        bot_token=token,
        database_url=db_url,
        admins=admins,
        schema_prod=schema_prod,
        schema_test=schema_test,
        store_name=(env.get("STORE_NAME") or "962ElStore").strip(),
        log_level=(env.get("LOG_LEVEL") or "INFO").strip().upper(),
        db_pool_max=pool_max,
        port=port,
    )

# ===== DATABASE CONNECTION =====
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, url: str, default_schema: str, max_size: int = 5) -> None:
        self._url = url
        self.default_schema = validate_schema_name(default_schema)
        self._max_size = max_size
        self._pool: Optional[AsyncConnectionPool] = None

    @property
    def pool(self) -> AsyncConnectionPool:
        if self._pool is None:
            raise RuntimeError("قاعدة البيانات غير مفتوحة بعد")
        return self._pool

    def _new_pool(self) -> AsyncConnectionPool:
        return AsyncConnectionPool(
            conninfo=self._url,
            min_size=1,
            max_size=self._max_size,
            open=False,
            name="els962",
            check=AsyncConnectionPool.check_connection,
            kwargs={
                "row_factory": dict_row,
                "prepare_threshold": None,  # متوافق مع Supabase pooler
                "connect_timeout": 10,
            },
        )

    async def open(self, timeout: float = 30) -> None:
        pool = self._new_pool()
        try:
            await pool.open(wait=True, timeout=timeout)
        except BaseException:
            await pool.close()
            raise
        self._pool = pool

    async def open_with_retry(self, attempts: int = 8, base_delay: float = 2.0) -> None:
        delay = base_delay
        for attempt in range(1, attempts + 1):
            try:
                await self.open()
                logger.info("تم الاتصال بقاعدة البيانات (المحاولة %s)", attempt)
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "فشل الاتصال بقاعدة البيانات (المحاولة %s/%s): %s",
                    attempt, attempts, type(exc).__name__,
                )
                if attempt == attempts:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def transaction(self, schema: Optional[str] = None) -> AsyncIterator[AsyncConnection]:
        schema = validate_schema_name(schema or self.default_schema)
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                        sql.Identifier(schema)
                    )
                )
                yield conn

    async def fetch_one(self, query: str, params: Optional[Sequence[Any]] = None,
                        schema: Optional[str] = None) -> Optional[dict]:
        async with self.transaction(schema) as conn:
            cur = await conn.execute(query, params)
            return await cur.fetchone()

    async def fetch_all(self, query: str, params: Optional[Sequence[Any]] = None,
                        schema: Optional[str] = None) -> list:
        async with self.transaction(schema) as conn:
            cur = await conn.execute(query, params)
            return await cur.fetchall()

    async def execute(self, query: str, params: Optional[Sequence[Any]] = None,
                      schema: Optional[str] = None) -> None:
        async with self.transaction(schema) as conn:
            await conn.execute(query, params)

    async def health_check(self) -> bool:
        try:
            async with self.pool.connection() as conn:
                cur = await conn.execute("SELECT 1 AS ok")
                row = await cur.fetchone()
                return bool(row and row["ok"] == 1)
        except Exception as exc:  # noqa: BLE001
            logger.error("فحص قاعدة البيانات فشل: %s", type(exc).__name__)
            return False

# ===== MISC UTILITIES =====
_EXACT = {
    "password", "pin", "pin_hash", "token", "secret", "otp", "code",
    "credentials", "login_data", "encryption_key", "subscription_data",
    "bot_token", "database_url",
}

_SUFFIXES = ("_password", "_token", "_secret", "_pin", "_hash", "_credentials")

MASK = "***"

def _is_sensitive(key: str) -> bool:
    k = key.lower()
    return k in _EXACT or k.endswith(_SUFFIXES)

def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: (MASK if isinstance(k, str) and _is_sensitive(k) else sanitize(v))
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [sanitize(v) for v in value]
    return value

# ===== AUDIT LOGGING =====
async def log_action(
    conn,
    *,
    action: str,
    admin: Optional[Admin] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    old_value: Any = None,
    new_value: Any = None,
    reason: Optional[str] = None,
) -> int:
    cur = await conn.execute(
        "INSERT INTO audit_log (admin_id, admin_name, action, entity_type, entity_id,"
        " old_value, new_value, reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        " RETURNING id",
        (
            admin.user_id if admin else None,
            admin.name if admin else None,
            action,
            entity_type,
            entity_id,
            Jsonb(sanitize(old_value)) if old_value is not None else None,
            Jsonb(sanitize(new_value)) if new_value is not None else None,
            reason,
        ),
    )
    row = await cur.fetchone()
    return row["id"]

# ===== STORE SETTINGS =====
async def get_setting(db, key: str, schema: Optional[str] = None) -> Optional[str]:
    row = await db.fetch_one("SELECT value FROM store_settings WHERE key = %s", (key,), schema=schema)
    return row["value"] if row else None

async def set_setting(db, key: str, value: str, admin: Admin, schema: Optional[str] = None) -> None:
    async with db.transaction(schema) as conn:
        cur = await conn.execute("SELECT value FROM store_settings WHERE key = %s FOR UPDATE", (key,))
        old = await cur.fetchone()
        await conn.execute(
            "INSERT INTO store_settings (key, value, updated_at, updated_by_admin_id)"
            " VALUES (%s, %s, now(), %s)"
            " ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value,"
            " updated_at = now(), updated_by_admin_id = EXCLUDED.updated_by_admin_id",
            (key, value, admin.user_id),
        )
        await log_action(
            conn, action="setting_changed", admin=admin, entity_type="store_settings",
            entity_id=key, old_value={"value": old["value"]} if old else None,
            new_value={"value": value},
        )

# ===== ADMIN SECURITY =====
def get_admin(settings: Settings, user_id: int) -> Optional[Admin]:
    for admin in settings.admins:
        if admin.user_id == user_id:
            return admin
    return None

def is_admin(settings: Settings, user_id: int) -> bool:
    return get_admin(settings, user_id) is not None

# ===== LOGGING =====
_PATTERNS = [
    (re.compile(r"\d{6,12}:[A-Za-z0-9_-]{30,}"), "[REDACTED_TOKEN]"),
    (re.compile(r"(postgres(?:ql)?://[^:/\s@]+:)([^@\s]+)(@)"), r"\1[REDACTED]\3"),
    (re.compile(r"(?i)(pin|password|passwd|secret|token)(\s*[=:]\s*)\S+"), r"\1\2[REDACTED]"),
]

def redact(text: str) -> str:
    for pattern, repl in _PATTERNS:
        text = pattern.sub(repl, text)
    return text

class RedactingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))

def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(
        RedactingFormatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level, logging.INFO))
    # مكتبة httpx تطبع روابط فيها التوكن، نخفض مستواها
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

# ===== HEALTH SERVER =====
logger = logging.getLogger(__name__)

_RESPONSE = (
    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
    b"Content-Length: 2\r\nConnection: close\r\n\r\nok"
)

async def start_health_server(port: int) -> asyncio.AbstractServer:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await asyncio.wait_for(reader.read(1024), timeout=5)
            writer.write(_RESPONSE)
            await writer.drain()
        except Exception:  # noqa: BLE001
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", port)
    logger.info("خادم الصحة يعمل على المنفذ %s", port)
    return server

# ===== JORDAN TIME =====
JORDAN_TZ = ZoneInfo("Asia/Amman")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def to_jordan(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("التاريخ يجب أن يحمل معلومات المنطقة الزمنية")
    return dt.astimezone(JORDAN_TZ)

def format_jordan(dt: datetime, fmt: str = "%Y-%m-%d %H:%M") -> str:
    return to_jordan(dt).strftime(fmt)

# ===== DATABASE MIGRATIONS =====
SQL_DIR = None

_FILE_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")

_LOCK_BASE = 962_000_000

class MigrationError(Exception):
    pass

class Migration:
    version: int
    name: str
    sql_text: str
    checksum: str

def discover_migrations(sql_dir=None) -> List[Migration]:
    text = FOUNDATION_SQL
    return [Migration(1, "foundation", text, hashlib.sha256(text.encode("utf-8")).hexdigest())]

def _lock_key(schema: str) -> int:
    return _LOCK_BASE + (zlib.crc32(schema.encode()) % 1_000_000)

async def run_migrations(db, schema: str, migrations: Optional[List[Migration]] = None) -> List[int]:
    """يطبق Migrations الناقصة ويعيد أرقام ما طُبق الآن."""
    from psycopg import sql

    validate_schema_name(schema)
    migrations = discover_migrations() if migrations is None else migrations
    applied_now: List[int] = []

    async with db.pool.connection() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(%s)", (_lock_key(schema),))

            cur = await conn.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (schema,)
            )
            if await cur.fetchone():
                cur = await conn.execute(
                    "SELECT count(*) AS n, "
                    "count(*) FILTER (WHERE table_name = 'schema_migrations') AS tracked "
                    "FROM information_schema.tables WHERE table_schema = %s",
                    (schema,),
                )
                info = await cur.fetchone()
                if info["n"] > 0 and info["tracked"] == 0:
                    raise MigrationError(
                        f"المخطط {schema} موجود وفيه جداول لا تخص المشروع. "
                        "اختر اسماً آخر في DB_SCHEMA_PROD / DB_SCHEMA_TEST."
                    )
            else:
                await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

            await conn.execute(
                sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(sql.Identifier(schema))
            )
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version integer PRIMARY KEY,"
                " name text NOT NULL,"
                " checksum text NOT NULL,"
                " applied_at timestamptz NOT NULL DEFAULT now())"
            )
            cur = await conn.execute("SELECT version, checksum FROM schema_migrations")
            applied = {row["version"]: row["checksum"] for row in await cur.fetchall()}

            for m in migrations:
                if m.version in applied:
                    if applied[m.version] != m.checksum:
                        raise MigrationError(
                            f"تم تعديل Migration رقم {m.version} بعد تطبيقه. أنشئ ملفاً جديداً بدلاً من التعديل."
                        )
                    continue
                await conn.execute(m.sql_text)
                await conn.execute(
                    "INSERT INTO schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
                    (m.version, m.name, m.checksum),
                )
                applied_now.append(m.version)
    return applied_now

# ===== EMBEDDED SQL MIGRATION =====
FOUNDATION_SQL = "-- المرحلة 1: الأساسات (الإعدادات + سجل التدقيق + أحداث النظام)\n-- تعمل داخل مخطط المشروع فقط ولا تلمس أي جدول قديم.\n\nCREATE TABLE store_settings (\n    key text PRIMARY KEY,\n    value text NOT NULL,\n    updated_at timestamptz NOT NULL DEFAULT now(),\n    updated_by_admin_id bigint\n);\n\nINSERT INTO store_settings (key, value) VALUES\n    ('store_open', 'false'),\n    ('launch_activated', 'false'),\n    ('unpaid_timeout_minutes', '30'),\n    ('review_request_days_before', '3'),\n    ('admin_session_minutes', '30'),\n    ('pin_max_attempts', '5'),\n    ('pin_lockout_minutes', '15')\nON CONFLICT (key) DO NOTHING;\n\nCREATE TABLE audit_log (\n    id bigserial PRIMARY KEY,\n    created_at timestamptz NOT NULL DEFAULT now(),\n    admin_id bigint,\n    admin_name text,\n    action text NOT NULL,\n    entity_type text,\n    entity_id text,\n    old_value jsonb,\n    new_value jsonb,\n    reason text\n);\n\nCREATE INDEX idx_audit_entity ON audit_log (entity_type, entity_id, created_at DESC);\nCREATE INDEX idx_audit_created ON audit_log (created_at DESC);\n\nCREATE FUNCTION prevent_audit_log_change() RETURNS trigger\nLANGUAGE plpgsql AS $$\nBEGIN\n    RAISE EXCEPTION 'audit_log is append-only';\nEND;\n$$;\n\nCREATE TRIGGER trg_audit_log_no_update_delete\n    BEFORE UPDATE OR DELETE ON audit_log\n    FOR EACH ROW EXECUTE FUNCTION prevent_audit_log_change();\n\nCREATE TRIGGER trg_audit_log_no_truncate\n    BEFORE TRUNCATE ON audit_log\n    FOR EACH STATEMENT EXECUTE FUNCTION prevent_audit_log_change();\n\nCREATE TABLE system_events (\n    id bigserial PRIMARY KEY,\n    created_at timestamptz NOT NULL DEFAULT now(),\n    level text NOT NULL,\n    component text NOT NULL,\n    message text NOT NULL,\n    details jsonb\n);\n\nCREATE INDEX idx_system_events_created ON system_events (created_at DESC);\n"

# ===== TELEGRAM BOT =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None: return
    settings = context.application.bot_data["settings"]
    await message.reply_text(f"أهلاً بك في {settings.store_name} 👋\nالمتجر قيد التجهيز حالياً، وسنفتح أبوابنا قريباً بإذن الله.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message, user = update.effective_message, update.effective_user
    if message is None or user is None: return
    settings = context.application.bot_data["settings"]
    admin = get_admin(settings, user.id)
    if admin is None: return
    db = context.application.bot_data["db"]
    lines = [f"مرحباً {admin.name} ✅", "حالة النظام (المرحلة 1):"]
    if not await db.health_check():
        lines.append("❌ قاعدة البيانات: لا يوجد اتصال")
        await message.reply_text("\n".join(lines)); return
    lines.append("✅ قاعدة البيانات: متصلة")
    for label, schema in (("الإنتاج", settings.schema_prod), ("الاختبار", settings.schema_test)):
        rows = await db.fetch_all("SELECT version FROM schema_migrations ORDER BY version", schema=schema)
        versions = ", ".join(str(row["version"]) for row in rows) or "لا شيء"
        lines.append(f"• مخطط {label} ({schema}): Migrations = {versions}")
    store_open = await get_setting(db, "store_open", schema=settings.schema_prod)
    lines.append(f"• المتجر مفتوح للطلبات: {'نعم' if store_open == 'true' else 'لا (وضع الاختبار)'}")
    await message.reply_text("\n".join(lines))

async def post_init(app: Application) -> None:
    settings = app.bot_data["settings"]
    db = Database(settings.database_url, settings.schema_prod, max_size=settings.db_pool_max)
    await db.open_with_retry(); app.bot_data["db"] = db
    for schema in (settings.schema_prod, settings.schema_test):
        applied = await run_migrations(db, schema)
        logger.info("المخطط %s جاهز (Migrations جديدة: %s)", schema, applied or "لا شيء")
    if settings.port: app.bot_data["health_server"] = await start_health_server(settings.port)
    for admin in settings.admins:
        try: await app.bot.send_message(admin.user_id, f"✅ {settings.store_name} يعمل الآن (المرحلة 1 - وضع الاختبار).\nأرسل /status لفحص النظام.")
        except Exception as exc: logger.warning("تعذر إرسال إشعار البدء للمشرف %s: %s", admin.name, type(exc).__name__)

async def post_shutdown(app: Application) -> None:
    server = app.bot_data.get("health_server")
    if server is not None: server.close(); await server.wait_closed()
    db = app.bot_data.get("db")
    if db is not None: await db.close()

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("خطأ غير متوقع أثناء معالجة تحديث", exc_info=context.error)

def main() -> None:
    try: settings = load_settings()
    except ConfigError as exc:
        print(f"خطأ في الإعدادات: {exc}", file=sys.stderr); sys.exit(1)
    setup_logging(settings.log_level)
    app = (ApplicationBuilder().token(settings.bot_token).post_init(post_init).post_shutdown(post_shutdown).build())
    app.bot_data["settings"] = settings
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_error_handler(on_error)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__": main()
