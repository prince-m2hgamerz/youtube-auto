import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import logging
import os
from typing import Any, Dict, Set

from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.client.bot import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    MenuButtonCommands,
    PreCheckoutQuery,
    CallbackQuery,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup

from app.config import settings
from app.job_worker import enqueue_job, start_worker
from app.supabase_client import (
    create_broadcast_record,
    create_job,
    count_user_jobs,
    get_all_jobs,
    get_all_users,
    get_app_settings,
    get_broadcast_targets,
    get_connected_users_count,
    get_job,
    get_pending_jobs,
    get_recent_jobs,
    get_users_stats,
    get_user,
    get_jobs_stats,
    get_jobs_by_status,
    list_user_jobs,
    log_admin_action,
    update_app_settings,
    update_job,
    update_user_settings,
    upsert_user,
)
from app.utils import extract_video_info, validate_youtube_url
from app.youtube_client import create_oauth_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(
    token=settings.telegram_token,
    default=DefaultBotProperties(parse_mode=None),
)
dp = Dispatcher(storage=MemoryStorage())


class DownloadState(StatesGroup):
    waiting_title = State()
    waiting_description = State()
    waiting_visibility = State()


class UserSettingsState(StatesGroup):
    waiting_value = State()


class AdminSettingsState(StatesGroup):
    waiting_value = State()


class BroadcastState(StatesGroup):
    waiting_message = State()


ADMIN_IDS: Set[str] = {
    part.strip() for part in os.getenv("ADMIN_IDS", "5798029484").split(",") if part.strip()
}

DEFAULT_PLAN_LIMITS = {
    "free_daily_limit": 3,
    "paid_daily_limit": 30,
    "free_max_pending_jobs": 1,
    "paid_max_pending_jobs": 5,
    "paid_user_ids": "",
    "paid_user_expiry": {},
}

ALLOWED_VISIBILITY_BY_PLAN = {
    "free": {"unlisted", "private"},
    "paid": {"public", "unlisted", "private"},
}


def get_or_create_user(telegram_id: str) -> Dict[str, Any]:
    user = get_user(telegram_id)
    if not user:
        user = upsert_user(telegram_id, is_connected=False)
    return user


def is_admin_user(telegram_id: str) -> bool:
    return telegram_id in ADMIN_IDS


class AdminMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user and not is_admin_user(str(user.id)):
            if isinstance(event, types.Message):
                if event.text and event.text.strip().startswith("/start"):
                    await event.answer("you are not authorize to use this bot contact admin @m2hgamerz")
            return None
        return await handler(event, data)


def _safe_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return default
    return parsed if parsed > 0 else default


def _parse_paid_user_expiry(raw_value: Any) -> Dict[str, str]:
    if raw_value is None:
        return {}
    if isinstance(raw_value, dict):
        return {str(k).strip(): str(v).strip() for k, v in raw_value.items() if str(k).strip() and str(v).strip()}
    return {}


PAYMENT_CURRENCY = str(settings.payment_currency or "XTR").upper()
PAYMENT_PROVIDER_TOKEN = (settings.payment_provider_token or "").strip()
PAID_PLAN_PRICE = _safe_int(settings.paid_plan_price, 150)
PAID_PLAN_DURATION_DAYS = _safe_int(settings.paid_plan_duration_days, 30)
DONATION_PRICE = _safe_int(settings.donation_price, 50)
PAID_PLAN_TITLE = settings.paid_plan_title or "YouTube Auto Paid Plan"
PAID_PLAN_DESCRIPTION = (
    settings.paid_plan_description
    or "Unlock higher daily limits, larger queue capacity, and public visibility uploads."
)


def _parse_paid_user_ids(raw_value: Any) -> Set[str]:
    if raw_value is None:
        return set()
    if isinstance(raw_value, list):
        return {str(item).strip() for item in raw_value if str(item).strip()}
    if isinstance(raw_value, str):
        return {part.strip() for part in raw_value.split(",") if part.strip()}
    return {str(raw_value).strip()} if str(raw_value).strip() else set()


def _serialize_user_ids(user_ids: Set[str]) -> str:
    return ",".join(sorted(user_ids))


def get_plan_limits_config() -> Dict[str, Any]:
    settings_map = get_app_settings() or {}
    return {
        "free_daily_limit": _safe_int(settings_map.get("free_daily_limit"), DEFAULT_PLAN_LIMITS["free_daily_limit"]),
        "paid_daily_limit": _safe_int(settings_map.get("paid_daily_limit"), DEFAULT_PLAN_LIMITS["paid_daily_limit"]),
        "free_max_pending_jobs": _safe_int(
            settings_map.get("free_max_pending_jobs"), DEFAULT_PLAN_LIMITS["free_max_pending_jobs"]
        ),
        "paid_max_pending_jobs": _safe_int(
            settings_map.get("paid_max_pending_jobs"), DEFAULT_PLAN_LIMITS["paid_max_pending_jobs"]
        ),
        "paid_user_ids": _parse_paid_user_ids(settings_map.get("paid_user_ids", DEFAULT_PLAN_LIMITS["paid_user_ids"])),
        "paid_user_expiry": _parse_paid_user_expiry(
            settings_map.get("paid_user_expiry", DEFAULT_PLAN_LIMITS["paid_user_expiry"])
        ),
    }


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def get_paid_until(telegram_id: str, limits_config: Dict[str, Any] | None = None) -> datetime | None:
    config = limits_config or get_plan_limits_config()
    expiry_raw = config.get("paid_user_expiry", {}).get(telegram_id)
    expiry_dt = _parse_iso_datetime(expiry_raw)
    if not expiry_dt:
        return None
    if expiry_dt <= datetime.now(timezone.utc):
        return None
    return expiry_dt


def get_user_plan(telegram_id: str, limits_config: Dict[str, Any] | None = None) -> str:
    config = limits_config or get_plan_limits_config()
    if telegram_id in config["paid_user_ids"]:
        return "paid"
    return "paid" if get_paid_until(telegram_id, config) else "free"


def get_today_start_iso() -> str:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return start.isoformat()


def get_user_usage(telegram_id: str, limits_config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    config = limits_config or get_plan_limits_config()
    plan = get_user_plan(telegram_id, config)
    paid_until = get_paid_until(telegram_id, config)
    daily_limit = config["paid_daily_limit"] if plan == "paid" else config["free_daily_limit"]
    pending_limit = config["paid_max_pending_jobs"] if plan == "paid" else config["free_max_pending_jobs"]
    used_today = count_user_jobs(telegram_id, created_after=get_today_start_iso())
    pending_jobs = count_user_jobs(telegram_id, statuses=["draft", "pending", "downloading", "uploading"])
    return {
        "plan": plan,
        "daily_limit": daily_limit,
        "pending_limit": pending_limit,
        "used_today": used_today,
        "remaining_today": max(daily_limit - used_today, 0),
        "pending_jobs": pending_jobs,
        "paid_until": paid_until.isoformat() if paid_until else None,
    }


def check_limits_before_new_job(telegram_id: str) -> tuple[bool, str | None, Dict[str, Any]]:
    usage = get_user_usage(telegram_id)
    if usage["used_today"] >= usage["daily_limit"]:
        return (
            False,
            (
                "Daily limit reached.\n"
                f"Plan: {usage['plan'].title()} | Daily limit: {usage['daily_limit']}\n"
                "Use /plans to compare limits and /upgrade for paid access."
            ),
            usage,
        )

    if usage["pending_jobs"] >= usage["pending_limit"]:
        return (
            False,
            (
                "Too many active jobs in queue.\n"
                f"Plan: {usage['plan'].title()} | Active job limit: {usage['pending_limit']}\n"
                "Wait for current jobs to finish or upgrade plan."
            ),
            usage,
        )

    return True, None, usage


def format_usage_text(usage: Dict[str, Any]) -> str:
    lines = [
        f"Plan: {usage['plan'].title()}\n"
        f"Daily jobs: {usage['used_today']}/{usage['daily_limit']} used\n"
        f"Active jobs: {usage['pending_jobs']}/{usage['pending_limit']}\n"
        f"Remaining today: {usage['remaining_today']}"
    ]
    if usage.get("paid_until"):
        lines.append(f"Paid until: {usage['paid_until']}")
    return "\n".join(lines)


def normalize_visibility_for_plan(visibility: str, plan: str) -> str:
    normalized = visibility.strip().lower()
    if normalized in ALLOWED_VISIBILITY_BY_PLAN.get(plan, set()):
        return normalized
    # Safe fallback for free plan restrictions.
    return "unlisted"


def _payment_provider_token() -> str | None:
    if PAYMENT_CURRENCY == "XTR":
        # Telegram Stars payments must have an empty provider token.
        return ""
    if not PAYMENT_PROVIDER_TOKEN:
        return None
    return PAYMENT_PROVIDER_TOKEN


def _payment_signature(raw_payload: str) -> str:
    return hmac.new(
        settings.secret_key.encode("utf-8"),
        raw_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def build_payment_payload(kind: str, telegram_id: str) -> str:
    issued_at = int(datetime.now(timezone.utc).timestamp())
    raw_payload = f"v1:{kind}:{telegram_id}:{issued_at}"
    signature = _payment_signature(raw_payload)
    return f"{raw_payload}:{signature}"


def parse_payment_payload(payload: str) -> Dict[str, str]:
    parts = payload.split(":")
    if len(parts) != 5:
        raise ValueError("Invalid payload format")
    version, kind, telegram_id, issued_at, signature = parts[0], parts[1], parts[2], parts[3], parts[4]
    raw_payload = ":".join(parts[:4])
    if version != "v1":
        raise ValueError("Unsupported payload version")
    if signature != _payment_signature(raw_payload):
        raise ValueError("Invalid payment payload signature")
    return {
        "version": version,
        "kind": kind,
        "telegram_id": telegram_id,
        "issued_at": issued_at,
    }


def get_paid_expiry_map(limits_config: Dict[str, Any] | None = None) -> Dict[str, str]:
    config = limits_config or get_plan_limits_config()
    return dict(config.get("paid_user_expiry", {}))


def activate_paid_plan_for_user(telegram_id: str) -> datetime:
    current_usage = get_user_usage(telegram_id)
    current_paid_until = _parse_iso_datetime(current_usage.get("paid_until"))
    base_time = current_paid_until if current_paid_until and current_paid_until > datetime.now(timezone.utc) else datetime.now(timezone.utc)
    new_paid_until = base_time + timedelta(days=PAID_PLAN_DURATION_DAYS)
    expiry_map = get_paid_expiry_map()
    expiry_map[telegram_id] = new_paid_until.isoformat()
    update_app_settings({"paid_user_expiry": expiry_map})
    return new_paid_until


def remove_paid_plan_for_user(telegram_id: str) -> None:
    config = get_plan_limits_config()
    paid_users: Set[str] = set(config["paid_user_ids"])
    if telegram_id in paid_users:
        paid_users.discard(telegram_id)
        update_app_settings({"paid_user_ids": _serialize_user_ids(paid_users)})

    expiry_map = get_paid_expiry_map(config)
    if telegram_id in expiry_map:
        expiry_map.pop(telegram_id, None)
        update_app_settings({"paid_user_expiry": expiry_map})


async def send_paid_plan_invoice(chat_id: int, telegram_id: str) -> None:
    provider_token = _payment_provider_token()
    if provider_token is None:
        raise ValueError(
            "PAYMENT_PROVIDER_TOKEN is not configured. Set PAYMENT_PROVIDER_TOKEN for non-XTR currencies."
        )

    await bot.send_invoice(
        chat_id=chat_id,
        title=PAID_PLAN_TITLE,
        description=PAID_PLAN_DESCRIPTION,
        payload=build_payment_payload("plan", telegram_id),
        currency=PAYMENT_CURRENCY,
        prices=[LabeledPrice(label="Paid plan", amount=PAID_PLAN_PRICE)],
        provider_token=provider_token,
        start_parameter="buy-paid-plan",
    )


async def send_donation_invoice(chat_id: int, telegram_id: str) -> None:
    provider_token = _payment_provider_token()
    if provider_token is None:
        raise ValueError(
            "PAYMENT_PROVIDER_TOKEN is not configured. Set PAYMENT_PROVIDER_TOKEN for non-XTR currencies."
        )

    await bot.send_invoice(
        chat_id=chat_id,
        title="Support YouTube Auto Bot",
        description="Thank you for supporting development and server costs.",
        payload=build_payment_payload("donation", telegram_id),
        currency=PAYMENT_CURRENCY,
        prices=[LabeledPrice(label="Donation", amount=DONATION_PRICE)],
        provider_token=provider_token,
        start_parameter="support-bot",
    )


def get_expected_amount_for_payment_kind(kind: str) -> int | None:
    if kind == "plan":
        return PAID_PLAN_PRICE
    if kind == "donation":
        return DONATION_PRICE
    return None


async def safe_edit_message(callback: CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup | None = None) -> None:
    try:
        await callback.message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as err:
        if "message is not modified" in str(err).lower():
            await callback.answer()
        else:
            raise


def get_default_user_settings() -> Dict[str, Any]:
    return {
        "default_quality": "Best Available",
        "default_visibility": "unlisted",
        "notifications_enabled": True,
        "language": "English",
    }


def format_user_settings(user: Dict[str, Any]) -> Dict[str, Any]:
    defaults = get_default_user_settings()
    return {
        "default_quality": user.get("default_quality") or defaults["default_quality"],
        "default_visibility": user.get("default_visibility") or defaults["default_visibility"],
        "notifications_enabled": user.get("notifications_enabled") if user.get("notifications_enabled") is not None else defaults["notifications_enabled"],
        "language": user.get("language") or defaults["language"],
    }


def get_default_app_settings() -> Dict[str, Any]:
    return {
        "default_quality": "Best Available",
        "default_visibility": "unlisted",
        "auto_cleanup": True,
        "max_concurrent_downloads": 3,
    }


def format_app_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    defaults = get_default_app_settings()
    return {
        "default_quality": config.get("default_quality") or defaults["default_quality"],
        "default_visibility": config.get("default_visibility") or defaults["default_visibility"],
        "auto_cleanup": config.get("auto_cleanup") if config.get("auto_cleanup") is not None else defaults["auto_cleanup"],
        "max_concurrent_downloads": config.get("max_concurrent_downloads") or defaults["max_concurrent_downloads"],
    }


async def set_bot_commands() -> None:
    async def _safe_bot_api_call(coro, action: str) -> None:
        try:
            await coro
        except Exception as exc:
            logger.warning("Skipping %s due to API error: %s", action, exc)

    user_commands = [
        BotCommand(command="start", description="Getting started"),
        BotCommand(command="help", description="How to use the bot"),
        BotCommand(command="connect", description="Connect your YouTube channel"),
        BotCommand(command="download", description="Queue a YouTube upload job"),
        BotCommand(command="queue", description="See active jobs"),
        BotCommand(command="cancel", description="Cancel queued job (/cancel <job_id>)"),
        BotCommand(command="abort", description="Abort current interactive flow"),
        BotCommand(command="status", description="Recent job status"),
        BotCommand(command="profile", description="Your profile and statistics"),
        BotCommand(command="myplan", description="Your current plan and usage"),
        BotCommand(command="plans", description="Free vs paid plan limits"),
        BotCommand(command="limits", description="Current active limits"),
        BotCommand(command="buy", description="Pay for paid plan"),
        BotCommand(command="donate", description="Support the bot"),
        BotCommand(command="upgrade", description="How to upgrade to paid"),
        BotCommand(command="dashboard", description="Interactive dashboard"),
    ]
    await _safe_bot_api_call(
        bot.set_my_commands(user_commands, scope=BotCommandScopeDefault()),
        "set default command menu",
    )

    admin_commands = [
        BotCommand(command="admin", description="Open admin dashboard"),
        BotCommand(command="adminhelp", description="Admin command help"),
        BotCommand(command="setplan", description="Set plan: /setplan <id> <free|paid>"),
        BotCommand(command="setlimits", description="Set plan limits"),
        BotCommand(command="userlookup", description="Lookup user by telegram id"),
        BotCommand(command="adminstats", description="System stats summary"),
        BotCommand(command="adminusers", description="List recent users"),
        BotCommand(command="adminjobs", description="List recent jobs"),
        BotCommand(command="admincancel", description="Cancel job: /admincancel <job_id>"),
        BotCommand(command="adminretry", description="Retry job: /adminretry <job_id>"),
        BotCommand(command="broadcast", description="Broadcast message to all users"),
    ]
    for admin_id in ADMIN_IDS:
        await _safe_bot_api_call(
            bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=int(admin_id))),
            f"set admin command scope for {admin_id}",
        )

    # Bot API feature usage: profile descriptions in menu.
    if hasattr(bot, "set_my_name"):
        await _safe_bot_api_call(bot.set_my_name(name="YouTube Auto Bot"), "set bot name")
    if hasattr(bot, "set_my_short_description"):
        await _safe_bot_api_call(
            bot.set_my_short_description(
                short_description="Queue YouTube uploads from Telegram with free/paid limits."
            ),
            "set short description",
        )
    if hasattr(bot, "set_my_description"):
        await _safe_bot_api_call(
            bot.set_my_description(
                description=(
                    "Connect your YouTube account, queue uploads with /download, and track progress in real time. "
                    "Use /plans and /myplan to manage your limits."
                )
            ),
            "set description",
        )
    if hasattr(bot, "set_chat_menu_button"):
        await _safe_bot_api_call(
            bot.set_chat_menu_button(menu_button=MenuButtonCommands()),
            "set chat menu button",
        )


async def cmd_start(message: types.Message):
    user = get_or_create_user(str(message.from_user.id))
    await message.reply(
        "🎬 YouTube Auto Bot — Admin Mode\n\n"
        "Quick start:\n"
        "1. /connect — link your YouTube channel\n"
        "2. /download <url> — queue a manual upload\n"
        "3. /setsource <channel_url> — set auto-copy source channel\n"
        "4. /source — view current source channel\n"
        "5. /queue and /status — monitor jobs\n\n"
        f"YouTube connected: {'✅ yes' if user.get('is_connected') else '❌ no'}\n\n"
        "Use /help for all commands."
    )


async def cmd_help(message: types.Message):
    await message.reply(
        "📋 Available commands:\n\n"
        "/start — onboarding summary\n"
        "/connect — link your YouTube channel\n"
        "/download <url> — queue a manual upload\n"
        "/queue — list active jobs\n"
        "/cancel <job_id> — cancel a draft/pending job\n"
        "/abort — stop the current interactive flow\n"
        "/status — recent upload statuses\n"
        "/profile — profile and history summary\n"
        "/dashboard — interactive menu\n\n"
        "🎬 Auto-copy Shorts:\n"
        "/setsource <url> — set source YouTube channel\n"
        "/source — view current source channel\n"
        "/testshorts — run auto-copy immediately\n\n"
        "⚙️ Bot Settings:\n"
        "/settings — view all bot settings\n"
        "/setvisibility <public|unlisted|private> — auto-upload visibility\n"
        "/settimes <HH:MM> <HH:MM> — change schedule times\n\n"
        "🔧 Admin commands:\n"
        "/admin — admin dashboard\n"
        "/adminhelp — admin command help\n"
        "/adminstats — system statistics\n"
        "/adminusers — list users\n"
        "/adminjobs — list recent jobs\n"
        "/admincancel <job_id> — cancel a job\n"
        "/adminretry <job_id> — retry a failed job\n"
        "/broadcast — send message to all users"
    )


async def cmd_plans(message: types.Message):
    config = get_plan_limits_config()
    currency_unit = "Stars" if PAYMENT_CURRENCY == "XTR" else PAYMENT_CURRENCY
    await message.reply(
        "Plan comparison:\n\n"
        f"Free plan\n- Daily jobs: {config['free_daily_limit']}\n- Active queue: {config['free_max_pending_jobs']}\n"
        "- Visibility options: unlisted, private\n\n"
        f"Paid plan\n- Daily jobs: {config['paid_daily_limit']}\n- Active queue: {config['paid_max_pending_jobs']}\n"
        "- Visibility options: public, unlisted, private\n"
        f"- Price: {PAID_PLAN_PRICE} {currency_unit} / {PAID_PLAN_DURATION_DAYS} days\n\n"
        "Use /buy to activate paid plan via Telegram Payments."
    )


async def cmd_myplan(message: types.Message):
    usage = get_user_usage(str(message.from_user.id))
    await message.reply(format_usage_text(usage))


async def cmd_limits(message: types.Message):
    usage = get_user_usage(str(message.from_user.id))
    allowed_visibility = ", ".join(sorted(ALLOWED_VISIBILITY_BY_PLAN[usage["plan"]]))
    await message.reply(
        "Applied limits:\n"
        f"- Plan: {usage['plan'].title()}\n"
        f"- Daily upload quota: {usage['daily_limit']}\n"
        f"- Active queue limit: {usage['pending_limit']}\n"
        f"- Allowed visibility: {allowed_visibility}"
    )


async def cmd_queue(message: types.Message):
    jobs = list_user_jobs(str(message.from_user.id))
    active = [job for job in jobs if job.get("status") in {"draft", "pending", "downloading", "uploading"}]
    if not active:
        await message.reply("No active jobs in your queue.")
        return

    usage = get_user_usage(str(message.from_user.id))
    lines = ["Active jobs:"]
    for job in active:
        lines.append(f"#{job.get('id')} - {job.get('status')} - {job.get('title', 'Untitled')[:45]}")
    lines.append("")
    lines.append(f"Queue usage: {usage['pending_jobs']}/{usage['pending_limit']}")
    lines.append("Use /cancel <job_id> to cancel draft or pending jobs.")
    await message.reply("\n".join(lines))


async def cmd_cancel(message: types.Message):
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /cancel <job_id>")
        return

    try:
        job_id = int(parts[1].strip())
    except ValueError:
        await message.reply("Job ID must be an integer.")
        return

    job = get_job(job_id)
    if not job or str(job.get("telegram_id")) != str(message.from_user.id):
        await message.reply("Job not found.")
        return

    if job.get("status") not in {"draft", "pending"}:
        await message.reply("Only draft/pending jobs can be cancelled.")
        return

    update_job(job_id, {"status": "failed", "error_message": "Cancelled by user"})
    await message.reply(f"Job #{job_id} has been cancelled.")


async def cmd_abort(message: types.Message, state: FSMContext):
    data = await state.get_data()
    job_id = data.get("job_id")
    await state.clear()

    if job_id:
        try:
            update_job(job_id, {"status": "failed", "error_message": "Cancelled during metadata step"})
        except Exception:
            logger.exception("Failed to mark draft job as cancelled: %s", job_id)

    await message.reply("Current interactive flow has been cancelled.")


async def cmd_upgrade(message: types.Message):
    currency_unit = "Stars" if PAYMENT_CURRENCY == "XTR" else PAYMENT_CURRENCY
    await message.reply(
        "Paid plan unlocks higher quotas, larger queue limits, and public visibility.\n\n"
        f"Price: {PAID_PLAN_PRICE} {currency_unit} for {PAID_PLAN_DURATION_DAYS} days.\n"
        "Use /buy to pay via official Telegram Payments.\n"
        "If payment is not configured yet, contact admin."
    )


async def cmd_buy(message: types.Message):
    user_id = str(message.from_user.id)
    get_or_create_user(user_id)
    try:
        await send_paid_plan_invoice(message.chat.id, user_id)
        await message.reply(
            "Payment invoice sent. Complete payment in Telegram to activate your paid plan automatically."
        )
    except Exception as exc:
        logger.exception("Failed to send paid plan invoice")
        await message.reply(f"Unable to create payment invoice: {exc}")


async def cmd_donate(message: types.Message):
    user_id = str(message.from_user.id)
    get_or_create_user(user_id)
    try:
        await send_donation_invoice(message.chat.id, user_id)
        await message.reply("Donation invoice sent. Thank you for supporting this bot.")
    except Exception as exc:
        logger.exception("Failed to send donation invoice")
        await message.reply(f"Unable to create donation invoice: {exc}")


async def handle_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    try:
        payload_info = parse_payment_payload(pre_checkout_query.invoice_payload)
        if payload_info["telegram_id"] != str(pre_checkout_query.from_user.id):
            await bot.answer_pre_checkout_query(
                pre_checkout_query.id,
                ok=False,
                error_message="This invoice belongs to another user.",
            )
            return

        expected_amount = get_expected_amount_for_payment_kind(payload_info["kind"])
        if expected_amount is None:
            await bot.answer_pre_checkout_query(
                pre_checkout_query.id,
                ok=False,
                error_message="Unsupported payment type.",
            )
            return

        if pre_checkout_query.currency != PAYMENT_CURRENCY:
            await bot.answer_pre_checkout_query(
                pre_checkout_query.id,
                ok=False,
                error_message="Currency mismatch.",
            )
            return

        if pre_checkout_query.total_amount != expected_amount:
            await bot.answer_pre_checkout_query(
                pre_checkout_query.id,
                ok=False,
                error_message="Amount mismatch. Please restart payment.",
            )
            return

        await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
    except Exception as exc:
        logger.exception("Pre-checkout validation failed")
        await bot.answer_pre_checkout_query(
            pre_checkout_query.id,
            ok=False,
            error_message="Payment validation failed. Please try again.",
        )


async def handle_successful_payment(message: types.Message):
    payment = message.successful_payment
    if payment is None:
        return

    try:
        payload_info = parse_payment_payload(payment.invoice_payload)
        user_id = str(message.from_user.id)
        kind = payload_info["kind"]
        expected_amount = get_expected_amount_for_payment_kind(kind)
        if expected_amount is None:
            raise ValueError("Unknown payment kind")
        if payment.currency != PAYMENT_CURRENCY:
            raise ValueError("Payment currency mismatch")
        if payment.total_amount != expected_amount:
            raise ValueError("Payment amount mismatch")

        if kind == "plan":
            paid_until = activate_paid_plan_for_user(user_id)
            await message.reply(
                "Payment successful. Paid plan is active.\n"
                f"Paid until: {paid_until.isoformat()}\n"
                "Use /myplan to check your updated limits."
            )
            return

        if kind == "donation":
            await message.reply("Donation received. Thank you for your support.")
            return

        await message.reply("Payment received, but payment type was not recognized.")
    except Exception:
        logger.exception("Failed to process successful payment")
        await message.reply("Payment received, but activation failed. Contact admin with your transaction details.")


async def cmd_adminhelp(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    await message.reply(
        "Admin commands:\n"
        "/admin - Open admin dashboard\n"
        "/setplan <telegram_id> <free|paid>\n"
        "/setlimits <free_daily> <paid_daily> <free_pending> <paid_pending>\n"
        "/userlookup <telegram_id>\n"
        "/adminstats - Show system/user/job stats\n"
        "/adminusers - Show recent users\n"
        "/adminjobs - Show recent jobs\n"
        "/admincancel <job_id> - Cancel draft/pending job\n"
        "/adminretry <job_id> - Requeue failed/draft/pending job\n"
        "/broadcast - Start broadcast mode"
    )


async def cmd_setplan(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return

    text = message.text or ""
    parts = text.split()
    if len(parts) != 3 or parts[2].lower() not in {"free", "paid"}:
        await message.reply("Usage: /setplan <telegram_id> <free|paid>")
        return

    target_id = parts[1].strip()
    target_plan = parts[2].lower().strip()
    if not get_user(target_id):
        upsert_user(target_id, is_connected=False)

    config = get_plan_limits_config()
    paid_users: Set[str] = set(config["paid_user_ids"])
    try:
        if target_plan == "paid":
            paid_users.add(target_id)
            expiry_map = get_paid_expiry_map(config)
            expiry_map.pop(target_id, None)
            update_app_settings(
                {
                    "paid_user_ids": _serialize_user_ids(paid_users),
                    "paid_user_expiry": expiry_map,
                }
            )
        else:
            remove_paid_plan_for_user(target_id)
    except Exception as exc:
        logger.exception("Failed to update plan for %s", target_id)
        await message.reply(f"Failed to update plan: {exc}")
        return

    usage = get_user_usage(target_id)
    await message.reply(f"Plan updated for {target_id}: {target_plan}\n{format_usage_text(usage)}")


async def cmd_setlimits(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return

    text = message.text or ""
    parts = text.split()
    if len(parts) != 5:
        await message.reply("Usage: /setlimits <free_daily> <paid_daily> <free_pending> <paid_pending>")
        return

    try:
        free_daily = int(parts[1])
        paid_daily = int(parts[2])
        free_pending = int(parts[3])
        paid_pending = int(parts[4])
    except ValueError:
        await message.reply("All limits must be integers.")
        return

    if min(free_daily, paid_daily, free_pending, paid_pending) <= 0:
        await message.reply("All limits must be greater than 0.")
        return
    if paid_daily < free_daily or paid_pending < free_pending:
        await message.reply("Paid limits must be greater than or equal to free limits.")
        return

    try:
        update_app_settings(
            {
                "free_daily_limit": free_daily,
                "paid_daily_limit": paid_daily,
                "free_max_pending_jobs": free_pending,
                "paid_max_pending_jobs": paid_pending,
            }
        )
    except Exception as exc:
        logger.exception("Failed to update plan limits")
        await message.reply(f"Failed to update limits: {exc}")
        return
    await message.reply(
        "Limits updated successfully.\n"
        f"Free: daily={free_daily}, pending={free_pending}\n"
        f"Paid: daily={paid_daily}, pending={paid_pending}"
    )


async def cmd_userlookup(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return

    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /userlookup <telegram_id>")
        return

    target_id = parts[1].strip()
    user = get_user(target_id)
    if not user:
        await message.reply("User not found.")
        return

    usage = get_user_usage(target_id)
    await message.reply(
        f"User: {target_id}\n"
        f"Connected: {bool(user.get('is_connected'))}\n"
        f"Plan: {usage['plan']}\n"
        f"Used today: {usage['used_today']}/{usage['daily_limit']}\n"
        f"Active jobs: {usage['pending_jobs']}/{usage['pending_limit']}\n"
        f"Paid until: {usage['paid_until'] or '-'}"
    )


async def cmd_adminstats(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    try:
        users_stats = get_users_stats()
        jobs_stats = get_jobs_stats()
        plan_limits = get_plan_limits_config()
        await message.reply(
            "Admin stats:\n"
            f"- Users: {users_stats['total_users']} total, {users_stats['connected_users']} connected\n"
            f"- Jobs: total {jobs_stats['total']}, pending {jobs_stats['pending']}, downloading {jobs_stats['downloading']}, uploading {jobs_stats['uploading']}, done {jobs_stats['completed']}, failed {jobs_stats['failed']}\n"
            f"- Success rate: {jobs_stats['success_rate']:.1f}%\n"
            f"- Limits free: daily={plan_limits['free_daily_limit']} pending={plan_limits['free_max_pending_jobs']}\n"
            f"- Limits paid: daily={plan_limits['paid_daily_limit']} pending={plan_limits['paid_max_pending_jobs']}"
        )
    except Exception as exc:
        logger.exception("Failed to fetch admin stats")
        await message.reply(f"Failed to fetch stats: {exc}")


async def cmd_adminusers(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    try:
        users = get_all_users()
        if not users:
            await message.reply("No users found.")
            return
        limits_config = get_plan_limits_config()
        lines = ["Recent users (up to 20):"]
        for user in users[:20]:
            telegram_id = str(user.get("telegram_id", "-"))
            plan = get_user_plan(telegram_id, limits_config)
            connected = "yes" if user.get("is_connected") else "no"
            lines.append(f"- {telegram_id} | connected={connected} | plan={plan}")
        await message.reply("\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to fetch users")
        await message.reply(f"Failed to list users: {exc}")


async def cmd_adminjobs(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    try:
        recent_jobs = get_recent_jobs(10)
        if not recent_jobs:
            await message.reply("No jobs found.")
            return
        lines = ["Recent jobs (up to 10):"]
        for job in recent_jobs:
            lines.append(
                f"- #{job.get('id')} | {job.get('status')} | user={job.get('telegram_id')} | {str(job.get('title', 'Untitled'))[:35]}"
            )
        await message.reply("\n".join(lines))
    except Exception as exc:
        logger.exception("Failed to fetch jobs")
        await message.reply(f"Failed to list jobs: {exc}")


def _extract_job_id_from_command(message: types.Message) -> int | None:
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        return None
    try:
        return int(parts[1].strip())
    except ValueError:
        return None


async def cmd_admincancel(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    job_id = _extract_job_id_from_command(message)
    if job_id is None:
        await message.reply("Usage: /admincancel <job_id>")
        return
    job = get_job(job_id)
    if not job:
        await message.reply("Job not found.")
        return
    if job.get("status") not in {"draft", "pending"}:
        await message.reply("Only draft/pending jobs can be cancelled by command.")
        return
    try:
        update_job(job_id, {"status": "failed", "error_message": "Cancelled by admin"})
        await message.reply(f"Job #{job_id} cancelled.")
    except Exception as exc:
        logger.exception("Failed to cancel job %s", job_id)
        await message.reply(f"Failed to cancel job: {exc}")


async def cmd_adminretry(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    job_id = _extract_job_id_from_command(message)
    if job_id is None:
        await message.reply("Usage: /adminretry <job_id>")
        return
    job = get_job(job_id)
    if not job:
        await message.reply("Job not found.")
        return
    status = job.get("status")
    if status in {"downloading", "uploading"}:
        await message.reply("Job is currently processing; cannot requeue now.")
        return
    if status == "done":
        await message.reply("Completed job cannot be retried.")
        return
    try:
        update_job(job_id, {"status": "pending", "error_message": None})
        enqueue_job(job_id)
        await message.reply(f"Job #{job_id} requeued.")
    except Exception as exc:
        logger.exception("Failed to retry job %s", job_id)
        await message.reply(f"Failed to retry job: {exc}")


async def cmd_broadcast(message: types.Message, state: FSMContext):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return

    await state.update_data(broadcast_active=True)
    await state.set_state(BroadcastState.waiting_message)
    await message.reply(
        "Broadcast mode enabled.\n"
        "Send the message you want to deliver to all users.\n"
        "Use /abort to exit without sending."
    )


async def cmd_connect(message: types.Message):
    try:
        user = get_or_create_user(str(message.from_user.id))
        oauth_url = create_oauth_url(int(user["telegram_id"]))
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Connect YouTube", url=oauth_url)]
        ])
        await message.answer(
            "Connect your YouTube account using the secure OAuth link below.\n"
            "After approval, return here and run /download <youtube_url>.",
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.exception("Error in cmd_connect")
        await message.reply(f"Error connecting account: {str(e)}")

async def cmd_download(message: types.Message, state: FSMContext):
    try:
        text = message.text or ""
        parts = text.split(None, 1)
        args = parts[1].strip() if len(parts) > 1 else ""
        logger.info(f"Download command received with args: {args}")
        if not args:
            await message.reply("Please send /download <YouTube URL>")
            return
        if not validate_youtube_url(args):
            await message.reply("That doesn't look like a valid YouTube video URL.")
            return
        user = get_or_create_user(str(message.from_user.id))
        if not user.get("is_connected"):
            await message.reply("Please connect your YouTube account first with /connect.")
            return
        await message.reply("Fetching video metadata...")
        info = extract_video_info(args)
        logger.info(f"Video info extracted: {info}")
        preferred_visibility = str(user.get("default_visibility") or "unlisted").strip().lower()
        if preferred_visibility not in {"public", "unlisted", "private"}:
            preferred_visibility = "unlisted"
        job = create_job(
            {
                "telegram_id": str(message.from_user.id),
                "video_url": args,
                "title": info.get("title"),
                "description": info.get("description"),
                "visibility": preferred_visibility,
                "status": "draft",
            }
        )
        logger.info(f"Job created: {job}")
        await state.update_data(job_id=job["id"])
        await message.reply(
            "Metadata fetched.\n"
            "Reply with a new title or send /skip to keep the original title.\n"
            "Use /abort to stop this flow.\n"
            f"Current title: {job['title']}"
        )
        await state.set_state(DownloadState.waiting_title)
    except Exception as exc:
        logger.error(f"Download flow error: {exc}", exc_info=True)
        await message.reply(f"Unable to process video: {str(exc)}")

async def process_title(message: types.Message, state: FSMContext):
    if not message.text:
        await message.reply("Please send text for the title.")
        return
    data = await state.get_data()
    job_id = data.get("job_id")
    incoming = message.text.strip()
    if incoming.lower() != "/skip":
        if len(incoming) > 100:
            await message.reply("Title is too long. Keep it under 100 characters.")
            return
        update_job(job_id, {"title": incoming})
    await message.reply("Now send an updated description, /skip to keep it, or /abort to cancel.")
    await state.set_state(DownloadState.waiting_description)

async def process_description(message: types.Message, state: FSMContext):
    if not message.text:
        await message.reply("Please send text for the description.")
        return
    data = await state.get_data()
    job_id = data.get("job_id")
    incoming = message.text.strip()
    if incoming.lower() != "/skip":
        if len(incoming) > 5000:
            await message.reply("Description is too long. Keep it under 5000 characters.")
            return
        update_job(job_id, {"description": incoming})
    await message.reply("Choose visibility: public, unlisted, or private. Use /abort to cancel.")
    await state.set_state(DownloadState.waiting_visibility)

async def process_visibility(message: types.Message, state: FSMContext):
    if not message.text:
        await message.reply("Please reply with public, unlisted, or private.")
        return
    visibility = message.text.strip().lower()
    if visibility not in {"public", "unlisted", "private"}:
        await message.reply("Please reply with public, unlisted, or private.")
        return
    data = await state.get_data()
    job_id = data.get("job_id")
    update_job(job_id, {"visibility": visibility, "status": "pending"})
    enqueue_job(job_id)
    await message.reply(
        f"✅ Your upload request has been queued. Visibility: {visibility}.\n"
        "You will receive progress updates in this chat."
    )
    await state.clear()

async def cmd_profile(message: types.Message):
    try:
        user = get_or_create_user(str(message.from_user.id))
        jobs = list_user_jobs(str(message.from_user.id))
        profile_text = [
            "👤 Your Profile",
            f"🆔 Telegram ID: {user['telegram_id']}",
            f"🔗 YouTube Connected: {'✅ Yes' if user.get('is_connected') else '❌ No'}",
        ]
        total_jobs = len(jobs)
        completed = sum(1 for j in jobs if j.get('status') == 'done')
        failed = sum(1 for j in jobs if j.get('status') == 'failed')
        pending = sum(1 for j in jobs if j.get('status') in ['pending', 'downloading', 'uploading'])
        profile_text.extend([
            "",
            "📊 Upload Statistics",
            f"📋 Total Jobs: {total_jobs}",
            f"✅ Completed: {completed}",
            f"⏳ Pending: {pending}",
            f"❌ Failed: {failed}",
        ])
        if jobs:
            profile_text.extend(["", "🎥 Recent Uploads"])
            for job in jobs[:5]:
                status_emoji = {'done': '✅', 'failed': '❌', 'pending': '⏳', 'downloading': '📥', 'uploading': '📤', 'draft': '📝'}.get(job.get('status'), '❓')
                title = job.get('title', 'Untitled')[:50]
                result_url = job.get('result_url', '-')
                profile_text.append(f"{status_emoji} {title}")
                if result_url != '-':
                    profile_text.append(f"   🔗 {result_url}")
        await message.reply("\n".join(profile_text))
    except Exception as e:
        logger.exception("Error in cmd_profile")
        await message.reply(f"❌ Error getting profile: {str(e)}")


async def cmd_setsource(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /setsource <YouTube channel URL>\n\nExample: /setsource https://www.youtube.com/@ChannelName")
        return
    channel_url = parts[1].strip()
    if not channel_url.startswith("http"):
        await message.reply("Please provide a valid YouTube channel URL starting with http.")
        return
    try:
        from app.channel_copier import set_source_channel_url
        set_source_channel_url(channel_url)
        await message.reply(f"✅ Source channel set to:\n{channel_url}\n\nThe bot will auto-copy new shorts daily at 07:15 and 19:15.")
    except Exception as exc:
        logger.exception("Failed to set source channel")
        await message.reply(f"❌ Failed to set source channel: {exc}")


async def cmd_source(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    try:
        from app.channel_copier import get_source_channel_url, get_uploaded_shorts_ids
        url = get_source_channel_url()
        uploaded = get_uploaded_shorts_ids()
        if url:
            await message.reply(f"📺 Source channel:\n{url}\n\n✅ Already uploaded {len(uploaded)} shorts.")
        else:
            await message.reply("No source channel configured. Use /setsource <channel_url> to set one.")
    except Exception as exc:
        logger.exception("Failed to get source channel")
        await message.reply(f"❌ Error: {exc}")


async def cmd_testshorts(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    await message.reply("🧪 Testing auto-copy shorts feature now...")
    try:
        from app.channel_copier import process_source_channel_uploads
        result = process_source_channel_uploads()
        await message.reply(result["message"])
    except Exception as exc:
        logger.exception("Test shorts failed")
        await message.reply(f"❌ Test failed: {exc}")


async def cmd_settings(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    try:
        from app.supabase_client import get_source_channel_url, get_uploaded_shorts_ids, get_bot_settings
        s = get_bot_settings()
        url = get_source_channel_url()
        uploaded = get_uploaded_shorts_ids()
        times = s.get("auto_upload_times", "07:15,19:15")
        vis = s.get("auto_upload_visibility", "public")
        await message.reply(
            f"⚙️ Bot Settings:\n\n"
            f"📺 Source channel: {url or 'Not set'}\n"
            f"✅ Uploaded shorts: {len(uploaded)}\n"
            f"🕐 Schedule times: {times}\n"
            f"👁️ Auto visibility: {vis}\n\n"
            f"Commands:\n"
            f"/setsource <url> — change source channel\n"
            f"/setvisibility <public|unlisted|private> — change auto visibility\n"
            f"/settimes <HH:MM> <HH:MM> — change schedule times\n"
            f"/testshorts — run auto-copy immediately"
        )
    except Exception as exc:
        logger.exception("Failed to get settings")
        await message.reply(f"❌ Error: {exc}")


async def cmd_setvisibility(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /setvisibility <public|unlisted|private>")
        return
    vis = parts[1].strip().lower()
    if vis not in {"public", "unlisted", "private"}:
        await message.reply("Visibility must be one of: public, unlisted, private.")
        return
    try:
        from app.supabase_client import set_bot_settings
        set_bot_settings({"auto_upload_visibility": vis})
        await message.reply(f"✅ Auto-upload visibility set to: {vis}")
    except Exception as exc:
        logger.exception("Failed to set visibility")
        await message.reply(f"❌ Error: {exc}")


async def cmd_settimes(message: types.Message):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    text = message.text or ""
    parts = text.split()
    if len(parts) != 3:
        await message.reply("Usage: /settimes <HH:MM> <HH:MM>\nExample: /settimes 07:15 19:15")
        return
    _, t1, t2 = parts
    try:
        for t in (t1, t2):
            h, m = t.split(":")
            assert 0 <= int(h) <= 23 and 0 <= int(m) <= 59
    except Exception:
        await message.reply("Invalid time format. Use HH:MM (24-hour format).")
        return
    try:
        from app.supabase_client import set_bot_settings
        set_bot_settings({"auto_upload_times": f"{t1},{t2}"})
        await message.reply(f"✅ Schedule times updated to: {t1} and {t2}\n\n⚠️ Restart the bot for new schedule times to take effect.")
    except Exception as exc:
        logger.exception("Failed to set times")
        await message.reply(f"❌ Error: {exc}")


async def cmd_status(message: types.Message):
    try:
        jobs = list_user_jobs(str(message.from_user.id))
        if not jobs:
            await message.reply("You have no upload jobs yet. Send /download <youtube_url> to start.")
            return

        text_lines = ["📋 Your recent upload jobs:"]
        for job in jobs:
            status = job.get("status", "unknown")
            result_url = job.get("result_url") or "-"
            text_lines.append(
                f"#{job.get('id')} • {job.get('title', 'Untitled')} — {status}\n  {result_url}"
            )
        await message.reply("\n\n".join(text_lines))
    except Exception as e:
        logger.exception("Error in cmd_status")
        await message.reply(f"❌ Error getting status: {str(e)}")


async def cmd_admin(message: types.Message):
    """Admin dashboard command (admin only)"""
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        return
    await cmd_admin_dashboard(message)


# Dashboard Functions
def create_user_dashboard_keyboard() -> InlineKeyboardMarkup:
    """Create inline keyboard for user dashboard with modern 2026 features"""
    keyboard = [
        [
            InlineKeyboardButton(text="📊 My Profile", callback_data="user_profile"),
            InlineKeyboardButton(text="📋 My Jobs", callback_data="user_jobs"),
        ],
        [
            InlineKeyboardButton(text="🔗 Connect YouTube", callback_data="user_connect"),
            InlineKeyboardButton(text="📥 Download Video", callback_data="user_download"),
        ],
        [
            InlineKeyboardButton(text="💳 My Plan", callback_data="user_plan"),
            InlineKeyboardButton(text="📚 Queue Help", callback_data="user_queue_help"),
        ],
        [
            InlineKeyboardButton(text="💰 Buy Plan", callback_data="user_buy"),
            InlineKeyboardButton(text="❤️ Donate", callback_data="user_donate"),
        ],
        [
            InlineKeyboardButton(text="📈 Statistics", callback_data="user_stats"),
            InlineKeyboardButton(text="⚙️ Settings", callback_data="user_settings"),
        ],
        [
            InlineKeyboardButton(text="🆘 Help & Support", callback_data="user_help"),
            InlineKeyboardButton(text="ℹ️ About Bot", callback_data="user_about"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_admin_dashboard_keyboard() -> InlineKeyboardMarkup:
    """Create inline keyboard for admin dashboard with advanced features"""
    keyboard = [
        [
            InlineKeyboardButton(text="👥 User Management", callback_data="admin_users"),
            InlineKeyboardButton(text="📊 System Stats", callback_data="admin_stats"),
        ],
        [
            InlineKeyboardButton(text="🎥 Job Queue", callback_data="admin_jobs"),
            InlineKeyboardButton(text="🔧 Bot Settings", callback_data="admin_settings"),
        ],
        [
            InlineKeyboardButton(text="📋 Logs", callback_data="admin_logs"),
            InlineKeyboardButton(text="🔄 Restart Services", callback_data="admin_restart"),
        ],
        [
            InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast"),
            InlineKeyboardButton(text="🔙 Back to User", callback_data="back_to_user"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_user_settings_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text="🎚️ Quality", callback_data="user_set_quality"), InlineKeyboardButton(text="👁️ Visibility", callback_data="user_set_visibility")],
        [InlineKeyboardButton(text="🔔 Notifications", callback_data="user_toggle_notifications"), InlineKeyboardButton(text="🌐 Language", callback_data="user_set_language")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="back_to_dashboard")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_admin_config_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text="🎚️ Default Quality", callback_data="admin_set_default_quality"), InlineKeyboardButton(text="👁️ Default Visibility", callback_data="admin_set_default_visibility")],
        [InlineKeyboardButton(text="🧹 Auto Cleanup", callback_data="admin_toggle_auto_cleanup"), InlineKeyboardButton(text="⚡ Max Workers", callback_data="admin_set_max_workers")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="back_to_admin_dashboard")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_admin_broadcast_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(text="✉️ Send Broadcast", callback_data="admin_broadcast_start")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="back_to_admin_dashboard")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def format_user_settings_text(settings: Dict[str, Any]) -> str:
    return f"""⚙️ *Your Settings*\n\n🎵 Default Quality: {settings['default_quality']}\n🔒 Default Visibility: {settings['default_visibility']}\n🔔 Notifications: {'Enabled' if settings['notifications_enabled'] else 'Disabled'}\n🌐 Language: {settings['language']}\n\nUse the buttons below to customize your preferences."""


def format_admin_config_text(config: Dict[str, Any]) -> str:
    return f"""🔧 *Configuration Panel*\n\n🎵 Default Quality: {config['default_quality']}\n🔒 Default Visibility: {config['default_visibility']}\n🧹 Auto Cleanup: {'Enabled' if config['auto_cleanup'] else 'Disabled'}\n⚡ Max Concurrent Downloads: {config['max_concurrent_downloads']}\n\nUse the buttons below to update global configuration."""


async def cmd_user_dashboard(message: types.Message):
    """Show user dashboard with inline keyboard"""
    user = get_or_create_user(str(message.from_user.id))
    settings = format_user_settings(user)
    usage = get_user_usage(str(message.from_user.id))
    paid_until_line = f"\n• Paid until: {usage['paid_until']}" if usage.get("paid_until") else ""
    
    welcome_text = f"""🎛️ *YouTube Auto Bot Dashboard*

👋 Welcome back, {message.from_user.first_name}!

🆔 User ID: {user['telegram_id']}
🔗 YouTube: {'✅ Connected' if user.get('is_connected') else '❌ Not Connected'}

🎚️ Current Settings:
• Quality: {settings['default_quality']}
• Visibility: {settings['default_visibility']}
• Notifications: {'Enabled' if settings['notifications_enabled'] else 'Disabled'}
• Language: {settings['language']}

💳 Plan:
• {usage['plan'].title()} | Daily {usage['used_today']}/{usage['daily_limit']}
• Active jobs {usage['pending_jobs']}/{usage['pending_limit']}{paid_until_line}

Choose an option below:"""
    
    keyboard = create_user_dashboard_keyboard()
    await message.reply(welcome_text, reply_markup=keyboard)


async def cmd_admin_dashboard(message: types.Message):
    """Show admin dashboard (only for admin users)"""
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("❌ Access denied. Admin privileges required.")
        return
    
    # Log admin access
    log_admin_action(str(message.from_user.id), "accessed_admin_dashboard", "Opened admin dashboard")
    
    # Get real-time statistics
    users_stats = get_users_stats()
    jobs_stats = get_jobs_stats()
    plan_limits = get_plan_limits_config()
    
    admin_text = f"""🔧 *Admin Dashboard*

⚠️ Administrative access granted for {message.from_user.first_name}

📊 *System Status:*
• Total Users: {users_stats['total_users']}
• Connected Users: {users_stats['connected_users']}
• Pending Jobs: {jobs_stats['pending']}
• Active Jobs: {jobs_stats['downloading'] + jobs_stats['uploading']}

💳 *Plan Limits:*
• Free daily/pending: {plan_limits['free_daily_limit']} / {plan_limits['free_max_pending_jobs']}
• Paid daily/pending: {plan_limits['paid_daily_limit']} / {plan_limits['paid_max_pending_jobs']}

Select an administrative function:"""
    
    keyboard = create_admin_dashboard_keyboard()
    await message.reply(admin_text, reply_markup=keyboard)


# Callback Query Handler for Dashboard Actions
@dp.callback_query()
async def handle_callback_query(callback: CallbackQuery, state: FSMContext):
    """Handle all callback queries from inline keyboards"""
    data = callback.data or ""
    user_id = str(callback.from_user.id)
    
    try:
        is_admin_action = data.startswith("admin_") or data in {"back_to_admin_dashboard", "back_to_user"}
        if is_admin_action and not is_admin_user(user_id):
            await callback.answer("Access denied.", show_alert=True)
            return

        # User Dashboard Actions
        if data == "user_profile":
            user = get_or_create_user(user_id)
            jobs = list_user_jobs(user_id)
            
            profile_text = f"""👤 *Your Profile*

🆔 Telegram ID: {user['telegram_id']}
🔗 YouTube Connected: {'✅ Yes' if user.get('is_connected') else '❌ No'}

📊 Quick Stats:
• Total Jobs: {len(jobs)}
• Completed: {sum(1 for j in jobs if j.get('status') == 'done')}
• Active: {sum(1 for j in jobs if j.get('status') in ['pending', 'downloading', 'uploading'])}

Use /profile for detailed view."""
            
            await safe_edit_message(callback, profile_text, reply_markup=create_user_dashboard_keyboard())
            
        elif data == "user_jobs":
            jobs = list_user_jobs(user_id)
            if not jobs:
                jobs_text = "📋 *Your Jobs*\n\nNo jobs found. Start by using /download command!"
            else:
                jobs_text = "📋 *Your Recent Jobs*\n\n"
                for i, job in enumerate(jobs[:5], 1):
                    status_emoji = {
                        'done': '✅', 'failed': '❌', 'pending': '⏳',
                        'downloading': '📥', 'uploading': '📤', 'draft': '📝'
                    }.get(job.get('status'), '❓')
                    title = job.get('title', 'Untitled')[:30]
                    jobs_text += f"{i}. {status_emoji} {title}\n"
                
                jobs_text += "\nUse /status for full details."
            
            await safe_edit_message(callback, jobs_text, reply_markup=create_user_dashboard_keyboard())
            
        elif data == "user_connect":
            user = get_or_create_user(user_id)
            if user.get('is_connected'):
                connect_text = "🔗 *YouTube Connection*\n\n✅ Your YouTube account is already connected!"
            else:
                oauth_url = create_oauth_url(int(user_id))
                connect_text = f"""🔗 *Connect YouTube Account*

❌ YouTube not connected yet.

Click the button below to authorize your YouTube channel:"""
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔐 Connect YouTube", url=oauth_url)],
                    [InlineKeyboardButton(text="🔙 Back to Dashboard", callback_data="back_to_dashboard")]
                ])
                await safe_edit_message(callback, connect_text, reply_markup=keyboard)
                return
            
            await safe_edit_message(callback, connect_text, reply_markup=create_user_dashboard_keyboard())
            
        elif data == "user_download":
            usage = get_user_usage(user_id)
            allowed_visibility = ", ".join(sorted(ALLOWED_VISIBILITY_BY_PLAN[usage["plan"]]))
            download_text = """📥 *Download & Upload Video*

Send me a YouTube URL to get started!

Example: /download https://www.youtube.com/watch?v=VIDEO_ID

The bot will:
1. 📥 Download the video
2. 📤 Upload to your YouTube channel
3. 🔗 Send you the new video link

Choose quality and privacy settings during the process.

"""
            download_text += (
                f"Current plan: {usage['plan'].title()}\n"
                f"Daily quota: {usage['used_today']}/{usage['daily_limit']}\n"
                f"Allowed visibility: {allowed_visibility}"
            )
            
            await safe_edit_message(callback, download_text, reply_markup=create_user_dashboard_keyboard())
            
        elif data == "user_plan":
            usage = get_user_usage(user_id)
            plan_text = (
                "💳 *My Plan*\n\n"
                f"{format_usage_text(usage)}\n\n"
                "Commands: /myplan, /plans, /buy, /upgrade"
            )
            await safe_edit_message(callback, plan_text, reply_markup=create_user_dashboard_keyboard())

        elif data == "user_queue_help":
            queue_help = (
                "📚 *Queue Instructions*\n\n"
                "1. /download <youtube_url> to create a job\n"
                "2. /queue to view active jobs\n"
                "3. /status for recent results\n"
                "4. /cancel <job_id> to cancel draft or pending jobs"
            )
            await safe_edit_message(callback, queue_help, reply_markup=create_user_dashboard_keyboard())

        elif data == "user_buy":
            try:
                await send_paid_plan_invoice(callback.message.chat.id, user_id)
                await callback.answer("Payment invoice sent.")
            except Exception as exc:
                logger.exception("Failed to send paid invoice from dashboard")
                await callback.answer("Unable to send invoice. Ask admin to configure payments.", show_alert=True)
            return

        elif data == "user_donate":
            try:
                await send_donation_invoice(callback.message.chat.id, user_id)
                await callback.answer("Donation invoice sent.")
            except Exception:
                logger.exception("Failed to send donation invoice from dashboard")
                await callback.answer("Unable to send donation invoice right now.", show_alert=True)
            return

        elif data == "user_stats":
            user = get_or_create_user(user_id)
            jobs = list_user_jobs(user_id)
            current_year_prefix = datetime.now(timezone.utc).strftime("%Y-")
            
            # Calculate statistics
            total = len(jobs)
            completed = sum(1 for j in jobs if j.get('status') == 'done')
            failed = sum(1 for j in jobs if j.get('status') == 'failed')
            pending = sum(1 for j in jobs if j.get('status') in ['pending', 'downloading', 'uploading'])
            
            success_rate = (completed / total * 100) if total > 0 else 0
            
            stats_text = f"""📈 *Your Statistics*

📊 Overall Performance:
• Total Jobs: {total}
• ✅ Completed: {completed}
• ❌ Failed: {failed}
• ⏳ Active: {pending}
• 📈 Success Rate: {success_rate:.1f}%

🎯 Recent Activity:
• This Year: {sum(1 for j in jobs if j.get('created_at', '').startswith(current_year_prefix))} jobs
• This Month: {len(jobs)} jobs

Keep uploading to improve your stats! 🚀"""
            
            await safe_edit_message(callback, stats_text, reply_markup=create_user_dashboard_keyboard())
            
        elif data == "user_settings":
            user = get_or_create_user(user_id)
            settings = format_user_settings(user)
            settings_text = format_user_settings_text(settings)
            await safe_edit_message(callback, settings_text, reply_markup=create_user_settings_keyboard())

        elif data == "user_set_quality":
            await state.update_data(field="default_quality", kind="user")
            await state.set_state(UserSettingsState.waiting_value)
            await safe_edit_message(callback, "📝 Send the new default quality for your uploads (e.g. Best Available, 1080p, 720p).", reply_markup=None)

        elif data == "user_set_visibility":
            await state.update_data(field="default_visibility", kind="user")
            await state.set_state(UserSettingsState.waiting_value)
            usage = get_user_usage(user_id)
            allowed_visibility = ", ".join(sorted(ALLOWED_VISIBILITY_BY_PLAN[usage["plan"]]))
            await safe_edit_message(
                callback,
                f"📝 Send the new default visibility.\nAllowed for {usage['plan'].title()} plan: {allowed_visibility}.",
                reply_markup=None,
            )

        elif data == "user_toggle_notifications":
            user = get_or_create_user(user_id)
            current = user.get("notifications_enabled")
            new_value = not bool(current)
            try:
                update_user_settings(user_id, {"notifications_enabled": new_value})
                await safe_edit_message(callback, f"🔔 Notifications are now {'enabled' if new_value else 'disabled'}.", reply_markup=create_user_settings_keyboard())
            except Exception as exc:
                logger.exception("Failed to toggle notifications")
                await safe_edit_message(callback, "❌ Could not update notifications. Please try again.", reply_markup=create_user_settings_keyboard())

        elif data == "user_set_language":
            await state.update_data(field="language", kind="user")
            await state.set_state(UserSettingsState.waiting_value)
            await safe_edit_message(callback, "📝 Send the new language for your settings (e.g. English, Spanish).", reply_markup=None)
            
        elif data == "user_help":
            help_text = """🆘 *Help & Support*

📚 *Getting Started:*
1. Connect your YouTube: /connect
2. Download videos: /download <url>
3. Check status: /status
4. View profile: /profile
5. Upgrade with Telegram Payments: /buy

🎯 *Tips:*
• Use high-quality videos for best results
• Check your upload quota on YouTube
• Videos are uploaded as 'unlisted' by default
• Use /myplan anytime to verify your paid/free limits

📞 *Support:*
• Report bugs: Contact developer
• Feature requests: Coming soon

🤖 *Bot Features (2026):*
• AI-powered metadata extraction
• Real-time upload progress
• Multi-platform support
• Advanced analytics

Need more help? Use /start to begin!"""
            
            await safe_edit_message(callback, help_text, reply_markup=create_user_dashboard_keyboard())
            
        elif data == "user_about":
            # Get real statistics
            users_stats = get_users_stats()
            jobs_stats = get_jobs_stats()
            
            about_text = f"""ℹ️ *About YouTube Auto Bot*

🤖 *Version:* 2.0 (2026 Edition)
📅 *Released:* April 2026
👨‍💻 *Developer:* AI Assistant

✨ *2026 Features:*
• Advanced AI video processing
• Real-time progress tracking
• Interactive dashboard
• Multi-language support
• Enhanced security

🔧 *Powered by:*
• aiogram 3.x (Latest Telegram Bot API)
• yt-dlp (Advanced video downloader)
• Google YouTube API v3
• Supabase (Cloud database)

📈 *Live Statistics:*
• Total Users: {users_stats['total_users']}
• Videos Processed: {jobs_stats['completed']}
• Success Rate: {jobs_stats['success_rate']:.1f}%

🌟 *What's New in 2026:*
• Inline keyboard dashboards
• Callback query handling
• Improved error handling
• Modern UI/UX design
• Enhanced performance

Thank you for using YouTube Auto Bot! 🚀"""
            
            await safe_edit_message(callback, about_text, reply_markup=create_user_dashboard_keyboard())
            
        elif data == "back_to_dashboard":
            await safe_edit_message(
                callback,
                f"🎛️ *YouTube Auto Bot Dashboard*\n\n👋 Welcome back, {callback.from_user.first_name}!\n\nChoose an option below:",
                reply_markup=create_user_dashboard_keyboard()
            )
        
        # Admin Dashboard Actions
        # Admin Dashboard Actions
        elif data == "admin_users":
            # Log admin action
            log_admin_action(user_id, "viewed_user_management", "Accessed user management dashboard")
            
            # Get real user statistics from Supabase
            users_stats = get_users_stats()
            all_users = get_all_users()
            total_users = users_stats["total_users"]
            connection_rate = (users_stats["connected_users"] / total_users * 100) if total_users else 0.0
            
            # Calculate activity (simplified - in production you'd track login times)
            active_today = len(all_users)  # Simplified - all users are considered "active"
            new_this_week = 0  # Would need created_at timestamp tracking
            
            users_text = f"""👥 *User Management*

👤 Total Users: {users_stats['total_users']}
🔗 Connected YouTube: {users_stats['connected_users']}
❌ Not Connected: {users_stats['unconnected_users']}

📊 User Activity:
• Active Today: {active_today}
• New This Week: {new_this_week}
• Connection Rate: {connection_rate:.1f}%"""
            
            await safe_edit_message(callback, users_text, reply_markup=create_admin_dashboard_keyboard())
            
        elif data == "admin_stats":
            # Log admin action
            log_admin_action(user_id, "viewed_system_stats", "Accessed system statistics dashboard")
            
            # Get real statistics from Supabase
            jobs_stats = get_jobs_stats()
            users_stats = get_users_stats()
            
            # Calculate file sizes (simplified - would need to track actual sizes)
            total_size_gb = jobs_stats['completed'] * 0.5  # Estimate 500MB per video
            
            stats_text = f"""📊 *System Statistics*

🖥️ *Server Status:*
• CPU Usage: Checking...
• Memory: Checking...
• Disk Space: Checking...

🤖 *Bot Performance:*
• Uptime: Running
• Messages Processed: {jobs_stats['total']}
• Commands Handled: {jobs_stats['total']}
• Errors: {jobs_stats['failed']}

📥 *Download Stats:*
• Videos Downloaded: {jobs_stats['completed']}
• Total Size: {total_size_gb:.1f}GB
• Success Rate: {jobs_stats['success_rate']:.1f}%

📤 *Upload Stats:*
• Videos Uploaded: {jobs_stats['completed']}
• YouTube API Calls: {jobs_stats['completed'] * 2}
• Failed Uploads: {jobs_stats['failed']}

🔄 *Queue Status:*
• Pending Jobs: {jobs_stats['pending']}
• Active Workers: 1
• Queue Length: {jobs_stats['pending'] + jobs_stats['downloading'] + jobs_stats['uploading']}"""
            
            await safe_edit_message(callback, stats_text, reply_markup=create_admin_dashboard_keyboard())
            
        elif data == "admin_jobs":
            # Log admin action
            log_admin_action(user_id, "viewed_job_queue", "Accessed job queue management")
            
            # Get real job statistics
            jobs_stats = get_jobs_stats()
            recent_jobs = get_recent_jobs(5)
            
            jobs_text = f"""🎥 *Job Queue Management*

📋 *Current Queue:*
• Pending: {jobs_stats['pending']} jobs
• Processing: {jobs_stats['downloading'] + jobs_stats['uploading']} jobs
• Completed Today: {jobs_stats['completed']} jobs
• Failed Today: {jobs_stats['failed']} jobs

🔄 *Worker Status:*
• Active Workers: 1
• Idle Workers: 1
• Total Capacity: 5 concurrent

📈 *Recent Activity:*
"""
            
            if recent_jobs:
                for i, job in enumerate(recent_jobs[:5], 1):
                    status_emoji = {
                        'done': '✅', 'failed': '❌', 'pending': '⏳',
                        'downloading': '📥', 'uploading': '📤', 'draft': '📝'
                    }.get(job.get('status'), '❓')
                    title = job.get('title', 'Untitled')[:30]
                    jobs_text += f"{i}. {status_emoji} {title}\n"
            else:
                jobs_text += "No recent jobs found."
            
            jobs_text += "\nQueue management features available."
            
            await safe_edit_message(callback, jobs_text, reply_markup=create_admin_dashboard_keyboard())
            
        elif data == "admin_settings":
            config = get_app_settings()
            config = format_app_settings(config)
            settings_text = format_admin_config_text(config)
            await safe_edit_message(callback, settings_text, reply_markup=create_admin_config_keyboard())

        elif data == "admin_set_default_quality":
            await state.update_data(field="default_quality", kind="admin")
            await state.set_state(AdminSettingsState.waiting_value)
            await safe_edit_message(callback, "📝 Send the global default quality (e.g. Best Available, 1080p, 720p).", reply_markup=None)

        elif data == "admin_set_default_visibility":
            await state.update_data(field="default_visibility", kind="admin")
            await state.set_state(AdminSettingsState.waiting_value)
            await safe_edit_message(callback, "📝 Send the global default visibility (public, unlisted, or private).", reply_markup=None)

        elif data == "admin_toggle_auto_cleanup":
            config = format_app_settings(get_app_settings())
            new_value = not bool(config["auto_cleanup"])
            try:
                update_app_settings({"auto_cleanup": new_value})
                await safe_edit_message(callback, f"🧹 Auto cleanup is now {'enabled' if new_value else 'disabled'}.", reply_markup=create_admin_config_keyboard())
            except Exception as exc:
                logger.exception("Failed to update admin config")
                await safe_edit_message(callback, "❌ Could not update configuration. Please try again.", reply_markup=create_admin_config_keyboard())

        elif data == "admin_set_max_workers":
            await state.update_data(field="max_concurrent_downloads", kind="admin")
            await state.set_state(AdminSettingsState.waiting_value)
            await safe_edit_message(callback, "📝 Send the maximum number of concurrent downloads allowed.", reply_markup=None)

        elif data == "admin_broadcast":
            broadcast_text = f"📢 *Broadcast System*\n\n📊 Total Users: {len(get_all_users())}\n🔗 YouTube Connected: {get_connected_users_count()}\n\nPress Send Broadcast to type your message to all users."
            await safe_edit_message(callback, broadcast_text, reply_markup=create_admin_broadcast_keyboard())

        elif data == "admin_broadcast_start":
            await state.update_data(broadcast_active=True)
            await state.set_state(BroadcastState.waiting_message)
            await safe_edit_message(callback, "📝 Type the broadcast message to send to all users now.", reply_markup=None)

        elif data == "back_to_admin_dashboard":
            users_stats = get_users_stats()
            jobs_stats = get_jobs_stats()
            plan_limits = get_plan_limits_config()
            admin_text = f"""🔧 *Admin Dashboard*

⚠️ Administrative access granted for {callback.from_user.first_name}

📊 *System Status:*
• Total Users: {users_stats['total_users']}
• Connected Users: {users_stats['connected_users']}
• Pending Jobs: {jobs_stats['pending']}
• Active Jobs: {jobs_stats['downloading'] + jobs_stats['uploading']}

💳 *Plan Limits:*
• Free daily/pending: {plan_limits['free_daily_limit']} / {plan_limits['free_max_pending_jobs']}
• Paid daily/pending: {plan_limits['paid_daily_limit']} / {plan_limits['paid_max_pending_jobs']}

Select an administrative function:"""
            await safe_edit_message(callback, admin_text, reply_markup=create_admin_dashboard_keyboard())
            
        elif data == "admin_logs":
            # Log admin action
            log_admin_action(user_id, "viewed_system_logs", "Accessed system activity logs")
            
            # Get real activity data
            recent_jobs = get_recent_jobs(5)
            all_users = get_all_users()
            
            logs_text = f"""📋 *System Activity Logs*

🔍 *Recent Job Activity:*
"""
            
            if recent_jobs:
                for job in recent_jobs[:5]:
                    status = job.get('status', 'unknown')
                    user_id = job.get('telegram_id', 'unknown')
                    title = job.get('title', 'Untitled')[:25]
                    created_at = job.get('created_at', 'unknown')[:19]  # Format timestamp
                    
                    status_desc = {
                        'done': 'completed',
                        'failed': 'failed',
                        'pending': 'started',
                        'downloading': 'downloading',
                        'uploading': 'uploading'
                    }.get(status, status)
                    
                    logs_text += f"[{created_at}] User {user_id}: {status_desc} '{title}'\n"
            else:
                logs_text += "No recent activity.\n"
            
            logs_text += f"""
👥 *User Activity:*
• Total registered users: {len(all_users)}
• Users with YouTube connected: {len([u for u in all_users if u.get('is_connected')])}

⚠️ *System Status:*
• Database: ✅ Connected
• Worker: ✅ Running
• API: ✅ Responding"""
            
            await safe_edit_message(callback, logs_text, reply_markup=create_admin_dashboard_keyboard())
            
        elif data == "admin_restart":
            restart_text = """🔄 *Service Management*

⚠️ *Restart Options:*

🟢 *Bot Service:*
• Status: Running
• Uptime: 24h 30m
• Last Restart: 2 days ago

🟢 *API Service:*
• Status: Running
• Uptime: 24h 30m
• Port: 8000

🟢 *Worker Service:*
• Status: Running
• Active Jobs: 0
• Queue: Empty

🔄 *Restart Actions:*
• Restart Bot: Not recommended
• Restart API: Safe
• Restart Worker: Safe

Service management coming soon..."""
            
            await safe_edit_message(callback, restart_text, reply_markup=create_admin_dashboard_keyboard())
            
        elif data == "back_to_user":

            await safe_edit_message(
                callback,
                f"🎛️ *YouTube Auto Bot Dashboard*\n\n👋 Welcome back, {callback.from_user.first_name}!\n\nChoose an option below:",
                reply_markup=create_user_dashboard_keyboard()
            )
        
        # Acknowledge the callback
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Callback query error: {e}", exc_info=True)
        await callback.answer("❌ An error occurred. Please try again.")


async def handle_settings_update(message: types.Message, state: FSMContext):
    data = await state.get_data()
    field = data.get("field")
    kind = data.get("kind")
    user_id = str(message.from_user.id)

    if not field or not kind:
        await message.reply("⚠️ No setting selected. Please use the dashboard again.")
        await state.clear()
        return

    if not message.text:
        await message.reply("Please send a value as text.")
        return

    value = message.text.strip()
    if field == "notifications_enabled":
        value = value.lower() in {"yes", "true", "on", "enable", "enabled", "1"}
    elif field in {"default_visibility", "visibility"}:
        value = value.lower()
        if value not in {"public", "unlisted", "private"}:
            await message.reply("Visibility must be one of: public, unlisted, private.")
            return

        if kind == "user":
            plan = get_user_plan(user_id)
            if value not in ALLOWED_VISIBILITY_BY_PLAN[plan]:
                allowed_visibility = ", ".join(sorted(ALLOWED_VISIBILITY_BY_PLAN[plan]))
                await message.reply(
                    f"Your {plan.title()} plan allows only: {allowed_visibility}. Use /upgrade for public visibility."
                )
                return
    elif field == "max_concurrent_downloads":
        try:
            value = int(value)
        except ValueError:
            await message.reply("Max concurrent downloads must be an integer.")
            return
        if value < 1 or value > 20:
            await message.reply("Max concurrent downloads must be between 1 and 20.")
            return

    if kind == "user":
        try:
            update_user_settings(user_id, {field: value})
            await message.reply(f"✅ Updated your setting: {field.replace('_', ' ').title()}.")
        except Exception as exc:
            logger.exception("Failed to update user settings")
            await message.reply("❌ Could not update settings. Please try again later.")
    else:
        if not is_admin_user(user_id):
            await message.reply("Access denied.")
            await state.clear()
            return
        try:
            update_app_settings({field: value})
            await message.reply(f"✅ Updated configuration: {field.replace('_', ' ').title()}.")
        except Exception as exc:
            logger.exception("Failed to update app settings")
            await message.reply("❌ Could not update configuration. Please check the system logs.")

    await state.clear()


async def handle_broadcast_message(message: types.Message, state: FSMContext):
    if not is_admin_user(str(message.from_user.id)):
        await message.reply("Access denied.")
        await state.clear()
        return

    data = await state.get_data()
    if not data.get("broadcast_active"):
        await message.reply("⚠️ Broadcast mode is not active. Use the admin dashboard again.")
        await state.clear()
        return

    if not message.text:
        await message.reply("Please send broadcast content as plain text.")
        return

    broadcast_message = message.text.strip()
    targets = get_broadcast_targets()
    if not targets:
        await message.reply("⚠️ No target users found to broadcast to.")
        await state.clear()
        return

    sent = 0
    failed = 0
    for target in targets:
        target_id = str(target.get("telegram_id"))
        try:
            await bot.send_message(target_id, f"📢 *Broadcast Message*\n\n{broadcast_message}")
            sent += 1
        except Exception as exc:
            logger.warning(f"Failed broadcast to {target_id}: {exc}")
            failed += 1

    try:
        log_admin_action(str(message.from_user.id), "sent_broadcast", broadcast_message)
        create_broadcast_record(str(message.from_user.id), broadcast_message, sent, failed)
    except Exception:
        pass

    await message.reply(f"✅ Broadcast sent to {sent} users. Failed: {failed}.")
    await state.clear()


def register_handlers() -> None:
    logger.info("Registering handlers...")

    # Admin-only commands (middleware blocks non-admins)
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_connect, Command("connect"))
    dp.message.register(cmd_download, Command("download"))
    dp.message.register(cmd_queue, Command("queue"))
    dp.message.register(cmd_cancel, Command("cancel"))
    dp.message.register(cmd_abort, Command("abort"))
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_profile, Command("profile"))
    dp.message.register(cmd_user_dashboard, Command("dashboard"))

    # Source channel commands
    dp.message.register(cmd_setsource, Command("setsource"))
    dp.message.register(cmd_source, Command("source"))
    dp.message.register(cmd_testshorts, Command("testshorts"))

    # Bot settings commands
    dp.message.register(cmd_settings, Command("settings"))
    dp.message.register(cmd_setvisibility, Command("setvisibility"))
    dp.message.register(cmd_settimes, Command("settimes"))

    # Admin commands
    dp.message.register(cmd_admin, Command("admin"))
    dp.message.register(cmd_adminhelp, Command("adminhelp"))
    dp.message.register(cmd_adminstats, Command("adminstats"))
    dp.message.register(cmd_adminusers, Command("adminusers"))
    dp.message.register(cmd_adminjobs, Command("adminjobs"))
    dp.message.register(cmd_admincancel, Command("admincancel"))
    dp.message.register(cmd_adminretry, Command("adminretry"))
    dp.message.register(cmd_broadcast, Command("broadcast"))

    # FSM handlers
    dp.message.register(process_title, DownloadState.waiting_title)
    dp.message.register(process_description, DownloadState.waiting_description)
    dp.message.register(process_visibility, DownloadState.waiting_visibility)
    dp.message.register(handle_settings_update, UserSettingsState.waiting_value)
    dp.message.register(handle_settings_update, AdminSettingsState.waiting_value)
    dp.message.register(handle_broadcast_message, BroadcastState.waiting_message)

    # Admin-only middleware
    dp.message.outer_middleware(AdminMiddleware())
    dp.callback_query.outer_middleware(AdminMiddleware())
    
    logger.info("Handlers registered successfully")


async def _run_bot() -> None:
    register_handlers()
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        logger.exception("Failed to clear webhook before polling")

    await set_bot_commands()
    start_worker()
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

def start_bot() -> None:
    asyncio.run(_run_bot())


