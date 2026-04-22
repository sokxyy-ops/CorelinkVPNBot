import os
import uuid
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)
from sqlalchemy import (
    Integer,
    String,
    Boolean,
    DateTime,
    BigInteger,
    Text,
    ForeignKey,
    select,
    func,
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from yookassa import Configuration, Payment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("corelink")

# =========================================================
# CONFIG
# =========================================================
def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value

BOT_TOKEN = env("BOT_TOKEN", required=True)
DATABASE_URL = env("DATABASE_URL", required=True)
BASE_URL = env("BASE_URL", required=True)  # https://your-app.up.railway.app
YOOKASSA_SHOP_ID = env("YOOKASSA_SHOP_ID", "")
YOOKASSA_SECRET_KEY = env("YOOKASSA_SECRET_KEY", "")
YOOKASSA_RETURN_URL = env("YOOKASSA_RETURN_URL", "https://t.me/")
MENU_IMAGE_PATH = env("MENU_IMAGE_PATH", "menu.jpg")

REQUIRED_CHANNEL = env("REQUIRED_CHANNEL", "")  # @channelusername or -100...
CHANNEL_URL = env("CHANNEL_URL", "")  # https://t.me/yourchannel

ADMINS = [int(x) for x in env("ADMINS", "").split(",") if x.strip().isdigit()]

DEFAULT_CORE_PRICE = int(env("DEFAULT_CORE_PRICE", "29900"))  # 299 RUB
DEFAULT_PRO_PRICE = int(env("DEFAULT_PRO_PRICE", "39900"))    # 399 RUB

DEFAULT_CORE_SUB_LINK = env("DEFAULT_CORE_SUB_LINK", "")
DEFAULT_PRO_SUB_LINK = env("DEFAULT_PRO_SUB_LINK", "")

WEBHOOK_PATH = "/telegram/webhook"
YOOKASSA_WEBHOOK_PATH = "/yookassa/webhook"

MSK = timezone(timedelta(hours=3))
LIFETIME_END = datetime(2099, 12, 31, 23, 59, 59)

if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
    Configuration.account_id = YOOKASSA_SHOP_ID
    Configuration.secret_key = YOOKASSA_SECRET_KEY

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
router = Router()
dp.include_router(router)
app = FastAPI(title="CoreLink VPN Bot")

# =========================================================
# DB
# =========================================================
class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    username: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    is_banned: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Tariff(Base):
    __tablename__ = "tariffs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, index=True)  # core, pro
    title: Mapped[str] = mapped_column(String(100))
    price_kopecks: Mapped[int] = mapped_column(Integer)
    max_devices: Mapped[int] = mapped_column(Integer)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    tariff_code: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(50), default="active")
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: LIFETIME_END)
    max_devices_snapshot: Mapped[int] = mapped_column(Integer)

class PaymentRecord(Base):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    tariff_code: Mapped[str] = mapped_column(String(50))
    external_payment_id: Mapped[Optional[str]] = mapped_column(String(255), unique=True, nullable=True)
    amount_kopecks: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(50), default="pending")
    payment_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    idempotence_key: Mapped[str] = mapped_column(String(255), unique=True)
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class AppSetting(Base):
    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

class AdminLog(Base):
    __tablename__ = "admin_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    admin_tg_id: Mapped[int] = mapped_column(BigInteger, index=True)
    action: Mapped[str] = mapped_column(String(255))
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

engine = create_async_engine(DATABASE_URL, future=True, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# =========================================================
# SIMPLE RUNTIME STATE FOR ADMIN FLOWS
# =========================================================
PENDING_BROADCAST = set()
PENDING_SEARCH = set()
PENDING_SENDTO = {}  # admin_tg_id -> target_tg_id

# =========================================================
# HELPERS
# =========================================================
def admin_allowed(tg_id: int) -> bool:
    return tg_id in ADMINS

def rub(kop: int) -> str:
    return f"{kop / 100:.0f} ₽"

def forever_text() -> str:
    return "НАВСЕГДА"

def fmt_dt(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).astimezone(MSK).strftime("%d.%m.%Y %H:%M МСК")

def safe_name(user: User) -> str:
    if user.username:
        return f"@{user.username}"
    if user.first_name:
        return user.first_name[:20]
    return str(user.telegram_id)

async def get_user_by_tg(session: AsyncSession, telegram_id: int) -> Optional[User]:
    res = await session.execute(select(User).where(User.telegram_id == telegram_id))
    return res.scalar_one_or_none()

async def ensure_user(session: AsyncSession, tg_user) -> User:
    user = await get_user_by_tg(session, tg_user.id)
    if not user:
        user = User(
            telegram_id=tg_user.id,
            username=tg_user.username,
            first_name=tg_user.first_name,
            last_name=tg_user.last_name,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
    else:
        user.username = tg_user.username
        user.first_name = tg_user.first_name
        user.last_name = tg_user.last_name
        await session.commit()
    return user

async def get_tariff(session: AsyncSession, code: str) -> Optional[Tariff]:
    res = await session.execute(select(Tariff).where(Tariff.code == code, Tariff.is_active == True))
    return res.scalar_one_or_none()

async def get_active_subscription(session: AsyncSession, user_id: int) -> Optional[Subscription]:
    res = await session.execute(
        select(Subscription).where(
            Subscription.user_id == user_id,
            Subscription.status == "active",
        ).order_by(Subscription.id.desc())
    )
    return res.scalars().first()

async def get_setting(session: AsyncSession, key: str, default: str = "") -> str:
    res = await session.execute(select(AppSetting).where(AppSetting.key == key))
    row = res.scalar_one_or_none()
    return row.value if row and row.value is not None else default

async def set_setting(session: AsyncSession, key: str, value: str):
    res = await session.execute(select(AppSetting).where(AppSetting.key == key))
    row = res.scalar_one_or_none()
    if row:
        row.value = value
    else:
        session.add(AppSetting(key=key, value=value))
    await session.commit()

async def log_admin(session: AsyncSession, admin_tg_id: int, action: str, details: Optional[str] = None):
    session.add(AdminLog(admin_tg_id=admin_tg_id, action=action, details=details))
    await session.commit()

async def is_subscribed_to_required_channel(user_id: int) -> bool:
    if not REQUIRED_CHANNEL:
        return True
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status in {"member", "administrator", "creator"}
    except Exception:
        return False

async def seed_defaults(session: AsyncSession):
    for code, title, price, devices in [
        ("core", "Core", DEFAULT_CORE_PRICE, 3),
        ("pro", "Pro", DEFAULT_PRO_PRICE, 10),
    ]:
        tariff = await get_tariff(session, code)
        if not tariff:
            session.add(Tariff(
                code=code,
                title=title,
                price_kopecks=price,
                max_devices=devices,
                is_active=True,
            ))
    await session.commit()

    if DEFAULT_CORE_SUB_LINK:
        await set_setting(session, "core_sub_link", DEFAULT_CORE_SUB_LINK)
    if DEFAULT_PRO_SUB_LINK:
        await set_setting(session, "pro_sub_link", DEFAULT_PRO_SUB_LINK)

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with SessionLocal() as session:
        await seed_defaults(session)

def main_menu_keyboard(has_sub: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="💳 Купить доступ", callback_data="buy_menu")],
        [InlineKeyboardButton(text="ℹ️ Информация", callback_data="info")],
    ]
    if has_sub:
        rows.append([InlineKeyboardButton(text="📄 Моя подписка", callback_data="my_sub")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def tariff_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💠 Core — 299 ₽", callback_data="buy:core")],
            [InlineKeyboardButton(text="🚀 Pro — 399 ₽", callback_data="buy:pro")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")],
        ]
    )

def payment_keyboard(url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить", url=url)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")],
        ]
    )

def gate_keyboard() -> InlineKeyboardMarkup:
    rows = []
    if CHANNEL_URL:
        rows.append([InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_URL)])
    rows.append([InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
            [InlineKeyboardButton(text="💰 Цены", callback_data="admin:prices")],
            [InlineKeyboardButton(text="🔗 Sub links", callback_data="admin:links")],
            [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin:users:0")],
            [InlineKeyboardButton(text="📣 Рассылка", callback_data="admin:broadcast")],
        ]
    )

def back_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:home")]]
    )

def users_page_keyboard(users: list[User], page: int, total: int) -> InlineKeyboardMarkup:
    rows = []
    for user in users:
        rows.append([InlineKeyboardButton(
            text=f"{safe_name(user)} | {user.telegram_id}",
            callback_data=f"admin:user:{user.telegram_id}"
        )])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin:users:{page-1}"))
    if (page + 1) * 8 < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin:users:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="🔎 Найти пользователя", callback_data="admin:search")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def user_manage_keyboard(target_tg_id: int, banned: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📨 Написать сообщение", callback_data=f"admin:sendto:{target_tg_id}")],
        [InlineKeyboardButton(text="🎁 Выдать Core", callback_data=f"admin:grant:{target_tg_id}:core")],
        [InlineKeyboardButton(text="🎁 Выдать Pro", callback_data=f"admin:grant:{target_tg_id}:pro")],
    ]
    if banned:
        rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin:unban:{target_tg_id}")])
    else:
        rows.append([InlineKeyboardButton(text="⛔ Забанить", callback_data=f"admin:ban:{target_tg_id}")])
    rows.append([InlineKeyboardButton(text="⬅️ К пользователям", callback_data="admin:users:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def send_main_menu(target):
    async with SessionLocal() as session:
        user = await get_user_by_tg(session, target.from_user.id)
        has_sub = False
        if user:
            has_sub = (await get_active_subscription(session, user.id)) is not None

    caption = (
        "🚀 <b>CoreLink VPN</b>\n\n"
        "Надежный доступ без ограничений по времени.\n\n"
        "<b>Тарифы:</b>\n"
        "💠 Core — 3 устройства, безлимитный трафик, доступ навсегда — <b>299 ₽</b>\n"
        "🚀 Pro — 10 устройств, безлимитный трафик, доступ навсегда — <b>399 ₽</b>\n\n"
        "После оплаты бот выдаст <b>sub link</b>.\n"
        "Его нужно вставить в <b>Happ</b>."
    )

    photo = FSInputFile(MENU_IMAGE_PATH)
    if isinstance(target, Message):
        await target.answer_photo(photo=photo, caption=caption, reply_markup=main_menu_keyboard(has_sub))
    else:
        try:
            await target.message.delete()
        except Exception:
            pass
        await bot.send_photo(target.from_user.id, photo=photo, caption=caption, reply_markup=main_menu_keyboard(has_sub))

def build_sub_link_message(link: str, tariff_title: str, max_devices: int) -> str:
    return (
        f"✅ <b>Доступ выдан</b>\n\n"
        f"Тариф: <b>{tariff_title}</b>\n"
        f"Лимит устройств: <b>{max_devices}</b>\n"
        f"Доступ: <b>{forever_text()}</b>\n\n"
        f"🔗 <b>Ваш sub link</b>\n"
        f"<blockquote>{link}</blockquote>\n\n"
        f"<b>Что делать дальше:</b>\n"
        f"1. Скачай <b>Happ</b>\n"
        f"2. Открой приложение\n"
        f"3. Нажми <b>плюсик</b> в правом верхнем углу\n"
        f"4. Вставь ссылку из буфера\n"
        f"5. Сохрани и подключись"
    )

# =========================================================
# PAYMENTS / ACCESS
# =========================================================
async def create_payment(session: AsyncSession, user: User, tariff: Tariff) -> PaymentRecord:
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
        raise RuntimeError("YooKassa env not configured")

    idempotence_key = str(uuid.uuid4())
    amount_rub = f"{tariff.price_kopecks / 100:.2f}"

    payment = Payment.create(
        {
            "amount": {"value": amount_rub, "currency": "RUB"},
            "capture": True,
            "confirmation": {"type": "redirect", "return_url": YOOKASSA_RETURN_URL},
            "description": f"CoreLink VPN | {tariff.title} | tg:{user.telegram_id}",
            "metadata": {
                "telegram_id": str(user.telegram_id),
                "tariff_code": tariff.code,
            },
        },
        idempotence_key,
    )

    record = PaymentRecord(
        user_id=user.id,
        tariff_code=tariff.code,
        external_payment_id=payment.id,
        amount_kopecks=tariff.price_kopecks,
        status=payment.status,
        payment_url=payment.confirmation.confirmation_url,
        idempotence_key=idempotence_key,
    )
    session.add(record)
    await session.commit()
    await session.refresh(record)
    return record

async def grant_access_for_user(session: AsyncSession, user: User, tariff_code: str) -> tuple[Optional[Subscription], Optional[str], Optional[int], Optional[str]]:
    tariff = await get_tariff(session, tariff_code)
    if not tariff:
        return None, None, None, None

    sub_link = await get_setting(session, f"{tariff_code}_sub_link", "")
    if not sub_link:
        return None, None, None, f"Для тарифа {tariff_code.upper()} не задан sub link"

    current = await get_active_subscription(session, user.id)
    if current:
        current.status = "cancelled"
        await session.commit()

    sub = Subscription(
        user_id=user.id,
        tariff_code=tariff.code,
        status="active",
        started_at=datetime.utcnow(),
        expires_at=LIFETIME_END,
        max_devices_snapshot=tariff.max_devices,
    )
    session.add(sub)
    await session.commit()
    await session.refresh(sub)

    return sub, sub_link, tariff.max_devices, None

async def process_successful_payment(payment_id: str):
    async with SessionLocal() as session:
        res = await session.execute(select(PaymentRecord).where(PaymentRecord.external_payment_id == payment_id))
        pay = res.scalar_one_or_none()
        if not pay:
            return

        if pay.status == "succeeded" and pay.paid_at:
            return

        user = await session.get(User, pay.user_id)
        tariff = await get_tariff(session, pay.tariff_code)
        if not user or not tariff:
            return

        pay.status = "succeeded"
        pay.paid_at = datetime.utcnow()
        await session.commit()

        sub, sub_link, max_devices, error = await grant_access_for_user(session, user, tariff.code)

        if error:
            text = (
                "✅ Оплата прошла\n\n"
                "Но admin ещё не задал sub link для этого тарифа.\n"
                "Свяжись с поддержкой."
            )
        else:
            text = build_sub_link_message(sub_link, tariff.title, max_devices)

        try:
            await bot.send_message(user.telegram_id, text)
        except Exception:
            pass

# =========================================================
# USER ROUTES
# =========================================================
@router.message(CommandStart())
async def start_cmd(message: Message):
    async with SessionLocal() as session:
        user = await ensure_user(session, message.from_user)
        if user.is_banned:
            await message.answer("⛔ Доступ к боту ограничен.")
            return

    if not await is_subscribed_to_required_channel(message.from_user.id):
        await message.answer(
            "Чтобы пользоваться ботом, сначала подпишись на наш Telegram-канал.",
            reply_markup=gate_keyboard(),
        )
        return

    await send_main_menu(message)

@router.callback_query(F.data == "check_sub")
async def cb_check_sub(callback: CallbackQuery):
    if not await is_subscribed_to_required_channel(callback.from_user.id):
        await callback.answer("Подписка на канал не найдена", show_alert=True)
        return
    await send_main_menu(callback)
    await callback.answer("Подписка подтверждена")

@router.callback_query(F.data == "back_main")
async def cb_back_main(callback: CallbackQuery):
    if not await is_subscribed_to_required_channel(callback.from_user.id):
        try:
            await callback.message.edit_caption(
                caption="Чтобы пользоваться ботом, сначала подпишись на канал.",
                reply_markup=gate_keyboard(),
            )
        except Exception:
            await callback.message.edit_text(
                "Чтобы пользоваться ботом, сначала подпишись на канал.",
                reply_markup=gate_keyboard(),
            )
        await callback.answer()
        return
    await send_main_menu(callback)
    await callback.answer()

@router.callback_query(F.data == "info")
async def cb_info(callback: CallbackQuery):
    text = (
        "ℹ️ <b>Информация</b>\n\n"
        "• После оплаты вы получаете доступ к CoreLink VPN <b>НАВСЕГДА</b>\n"
        "• Никаких продлений и ежемесячных платежей\n"
        "• Безлимитный трафик\n"
        "• <b>Core</b> — до 3 устройств\n"
        "• <b>Pro</b> — до 10 устройств\n"
        "• После оплаты бот выдаёт <b>sub link</b>\n"
        "• Этот sub link нужно вставить в <b>Happ</b>\n\n"
        "<b>Различия тарифов:</b>\n"
        "💠 <b>Core</b> — 3 устройства, 299 ₽\n"
        "🚀 <b>Pro</b> — 10 устройств, 399 ₽\n\n"
        "Покупая доступ, вы соглашаетесь с правилами использования сервиса."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]]
    )
    try:
        await callback.message.edit_caption(caption=text, reply_markup=kb)
    except Exception:
        await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()

@router.callback_query(F.data == "buy_menu")
async def cb_buy_menu(callback: CallbackQuery):
    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        if user.is_banned:
            await callback.answer("Вы заблокированы", show_alert=True)
            return

    if not await is_subscribed_to_required_channel(callback.from_user.id):
        try:
            await callback.message.edit_caption(
                caption="Чтобы пользоваться ботом, сначала подпишись на канал.",
                reply_markup=gate_keyboard(),
            )
        except Exception:
            await callback.message.edit_text(
                "Чтобы пользоваться ботом, сначала подпишись на канал.",
                reply_markup=gate_keyboard(),
            )
        await callback.answer()
        return

    text = (
        "💳 <b>Выбор тарифа</b>\n\n"
        "💠 <b>Core</b>\n"
        "• 3 устройства\n"
        "• безлимитный трафик\n"
        "• доступ навсегда\n"
        "• цена: <b>299 ₽</b>\n\n"
        "🚀 <b>Pro</b>\n"
        "• 10 устройств\n"
        "• безлимитный трафик\n"
        "• доступ навсегда\n"
        "• цена: <b>399 ₽</b>\n\n"
        "После оплаты бот выдаст готовый sub link."
    )
    try:
        await callback.message.edit_caption(caption=text, reply_markup=tariff_keyboard())
    except Exception:
        await callback.message.edit_text(text, reply_markup=tariff_keyboard())
    await callback.answer()

@router.callback_query(F.data.startswith("buy:"))
async def cb_buy_tariff(callback: CallbackQuery):
    if not await is_subscribed_to_required_channel(callback.from_user.id):
        try:
            await callback.message.edit_caption(
                caption="Чтобы пользоваться ботом, сначала подпишись на канал.",
                reply_markup=gate_keyboard(),
            )
        except Exception:
            await callback.message.edit_text(
                "Чтобы пользоваться ботом, сначала подпишись на канал.",
                reply_markup=gate_keyboard(),
            )
        await callback.answer()
        return

    code = callback.data.split(":", 1)[1]

    async with SessionLocal() as session:
        user = await ensure_user(session, callback.from_user)
        if user.is_banned:
            await callback.answer("Вы заблокированы", show_alert=True)
            return

        tariff = await get_tariff(session, code)
        if not tariff:
            await callback.answer("Тариф недоступен", show_alert=True)
            return

        pay = await create_payment(session, user, tariff)
        text = (
            f"🧾 <b>Оплата тарифа {tariff.title}</b>\n\n"
            f"Цена: <b>{rub(tariff.price_kopecks)}</b>\n"
            f"Лимит устройств: <b>{tariff.max_devices}</b>\n"
            f"Доступ: <b>{forever_text()}</b>\n\n"
            f"После успешной оплаты бот автоматически выдаст sub link."
        )
        try:
            await callback.message.edit_caption(caption=text, reply_markup=payment_keyboard(pay.payment_url))
        except Exception:
            await callback.message.edit_text(text, reply_markup=payment_keyboard(pay.payment_url))
        await callback.answer()

@router.callback_query(F.data == "my_sub")
async def cb_my_sub(callback: CallbackQuery):
    async with SessionLocal() as session:
        user = await get_user_by_tg(session, callback.from_user.id)
        if not user:
            await callback.answer("Нет данных", show_alert=True)
            return

        sub = await get_active_subscription(session, user.id)
        if not sub:
            await callback.answer("Активного доступа нет", show_alert=True)
            return

        tariff = await get_tariff(session, sub.tariff_code)
        sub_link = await get_setting(session, f"{sub.tariff_code}_sub_link", "")

        text = (
            "📄 <b>Моя подписка</b>\n\n"
            f"Тариф: <b>{sub.tariff_code.upper()}</b>\n"
            f"Статус: <b>{sub.status}</b>\n"
            f"Доступ: <b>{forever_text()}</b>\n"
            f"Лимит устройств: <b>{sub.max_devices_snapshot}</b>\n\n"
        )
        if sub_link and tariff:
            text += (
                f"🔗 <b>Ваш sub link</b>\n"
                f"<blockquote>{sub_link}</blockquote>\n\n"
                f"<b>Что делать дальше:</b>\n"
                f"1. Скачай <b>Happ</b>\n"
                f"2. Открой приложение\n"
                f"3. Нажми <b>плюсик</b> в правом верхнем углу\n"
                f"4. Вставь ссылку из буфера\n"
                f"5. Сохрани и подключись"
            )
        else:
            text += "⚠️ Sub link для этого тарифа ещё не задан админом."

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_main")]]
    )
    try:
        await callback.message.edit_caption(caption=text, reply_markup=kb)
    except Exception:
        await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()

# =========================================================
# ADMIN ROUTES
# =========================================================
@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not admin_allowed(message.from_user.id):
        await message.answer("Нет доступа.")
        return
    await message.answer("👑 <b>Админ-меню</b>", reply_markup=admin_home_keyboard())

@router.callback_query(F.data == "admin:home")
async def cb_admin_home(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    await callback.message.edit_text("👑 <b>Админ-меню</b>", reply_markup=admin_home_keyboard())
    await callback.answer()

@router.callback_query(F.data == "admin:stats")
async def cb_admin_stats(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    async with SessionLocal() as session:
        users_count = (await session.execute(select(func.count(User.id)))).scalar() or 0
        active_count = (await session.execute(select(func.count(Subscription.id)).where(Subscription.status == "active"))).scalar() or 0
        turnover = (await session.execute(
            select(func.coalesce(func.sum(PaymentRecord.amount_kopecks), 0)).where(PaymentRecord.status == "succeeded")
        )).scalar() or 0

    text = (
        "📊 <b>Статистика</b>\n\n"
        f"Пользователей: <b>{users_count}</b>\n"
        f"Активных доступов: <b>{active_count}</b>\n"
        f"Оборот: <b>{rub(int(turnover))}</b>"
    )
    await callback.message.edit_text(text, reply_markup=back_admin_keyboard())
    await callback.answer()

@router.callback_query(F.data == "admin:prices")
async def cb_admin_prices(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    async with SessionLocal() as session:
        core = await get_tariff(session, "core")
        pro = await get_tariff(session, "pro")

    text = (
        "💰 <b>Цены и различия</b>\n\n"
        f"💠 Core: <b>{rub(core.price_kopecks)}</b>\n"
        "• 3 устройства\n"
        "• безлимитный трафик\n"
        "• доступ навсегда\n\n"
        f"🚀 Pro: <b>{rub(pro.price_kopecks)}</b>\n"
        "• 10 устройств\n"
        "• безлимитный трафик\n"
        "• доступ навсегда\n\n"
        "Изменить цены:\n"
        "<code>/setprice core 299</code>\n"
        "<code>/setprice pro 399</code>"
    )
    await callback.message.edit_text(text, reply_markup=back_admin_keyboard())
    await callback.answer()

@router.callback_query(F.data == "admin:links")
async def cb_admin_links(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    async with SessionLocal() as session:
        core_link = await get_setting(session, "core_sub_link", "")
        pro_link = await get_setting(session, "pro_sub_link", "")

    text = (
        "🔗 <b>Sub links</b>\n\n"
        f"Core:\n<blockquote>{core_link or 'не задан'}</blockquote>\n\n"
        f"Pro:\n<blockquote>{pro_link or 'не задан'}</blockquote>\n\n"
        "Установить ссылки:\n"
        "<code>/setsub core https://example.com/sub</code>\n"
        "<code>/setsub pro https://example.com/sub</code>\n\n"
        "Все пользователи одного тарифа получают одну и ту же ссылку."
    )
    await callback.message.edit_text(text, reply_markup=back_admin_keyboard())
    await callback.answer()

@router.callback_query(F.data.startswith("admin:users:"))
async def cb_admin_users(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    page = int(callback.data.split(":")[2])
    per_page = 8

    async with SessionLocal() as session:
        total = (await session.execute(select(func.count(User.id)))).scalar() or 0
        res = await session.execute(select(User).order_by(User.id.desc()).offset(page * per_page).limit(per_page))
        users = res.scalars().all()

    text = f"👥 <b>Пользователи</b>\n\nСтраница: <b>{page + 1}</b>\nВсего: <b>{total}</b>"
    await callback.message.edit_text(text, reply_markup=users_page_keyboard(users, page, total))
    await callback.answer()

@router.callback_query(F.data == "admin:search")
async def cb_admin_search(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    PENDING_SEARCH.add(callback.from_user.id)
    await callback.message.edit_text(
        "🔎 Пришли следующим сообщением <b>Telegram ID</b> пользователя или <b>@username</b>.",
        reply_markup=back_admin_keyboard(),
    )
    await callback.answer()

async def render_user_card_text(session: AsyncSession, user: User) -> str:
    sub = await get_active_subscription(session, user.id)
    text = (
        "👤 <b>Пользователь</b>\n\n"
        f"ID: <code>{user.telegram_id}</code>\n"
    )
    if user.username:
        text += f"Username: <b>@{user.username}</b>\n"
    if user.first_name:
        text += f"Имя: <b>{user.first_name}</b>\n"
    text += f"Бан: <b>{'да' if user.is_banned else 'нет'}</b>\n"

    if sub:
        text += (
            f"Тариф: <b>{sub.tariff_code.upper()}</b>\n"
            f"Доступ: <b>{forever_text()}</b>\n"
            f"Лимит устройств: <b>{sub.max_devices_snapshot}</b>\n"
        )
    else:
        text += "Активного доступа нет\n"

    return text

@router.callback_query(F.data.startswith("admin:user:"))
async def cb_admin_user(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    target_tg_id = int(callback.data.split(":")[2])

    async with SessionLocal() as session:
        user = await get_user_by_tg(session, target_tg_id)
        if not user:
            await callback.answer("Пользователь не найден", show_alert=True)
            return
        text = await render_user_card_text(session, user)

    await callback.message.edit_text(text, reply_markup=user_manage_keyboard(target_tg_id, user.is_banned))
    await callback.answer()

@router.callback_query(F.data.startswith("admin:ban:"))
async def cb_admin_ban(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    target_tg_id = int(callback.data.split(":")[2])
    async with SessionLocal() as session:
        user = await get_user_by_tg(session, target_tg_id)
        if not user:
            await callback.answer("Пользователь не найден", show_alert=True)
            return
        user.is_banned = True
        await session.commit()
        await log_admin(session, callback.from_user.id, "ban", str(target_tg_id))
        text = await render_user_card_text(session, user)

    await callback.message.edit_text(text, reply_markup=user_manage_keyboard(target_tg_id, True))
    await callback.answer("Пользователь забанен", show_alert=True)

@router.callback_query(F.data.startswith("admin:unban:"))
async def cb_admin_unban(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    target_tg_id = int(callback.data.split(":")[2])
    async with SessionLocal() as session:
        user = await get_user_by_tg(session, target_tg_id)
        if not user:
            await callback.answer("Пользователь не найден", show_alert=True)
            return
        user.is_banned = False
        await session.commit()
        await log_admin(session, callback.from_user.id, "unban", str(target_tg_id))
        text = await render_user_card_text(session, user)

    await callback.message.edit_text(text, reply_markup=user_manage_keyboard(target_tg_id, False))
    await callback.answer("Пользователь разбанен", show_alert=True)

@router.callback_query(F.data.startswith("admin:sendto:"))
async def cb_admin_sendto(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    target_tg_id = int(callback.data.split(":")[2])
    PENDING_SENDTO[callback.from_user.id] = target_tg_id
    await callback.message.edit_text(
        f"📨 Пришли следующим сообщением текст для пользователя <code>{target_tg_id}</code>.",
        reply_markup=back_admin_keyboard(),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("admin:grant:"))
async def cb_admin_grant(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    _, _, tg_id_str, tariff_code = callback.data.split(":")
    target_tg_id = int(tg_id_str)

    async with SessionLocal() as session:
        user = await get_user_by_tg(session, target_tg_id)
        if not user:
            user = User(telegram_id=target_tg_id)
            session.add(user)
            await session.commit()
            await session.refresh(user)

        sub, sub_link, max_devices, error = await grant_access_for_user(session, user, tariff_code)
        await log_admin(session, callback.from_user.id, "grant", f"{target_tg_id}:{tariff_code}")

        if error:
            text_to_user = f"⚠️ {error}"
        else:
            tariff = await get_tariff(session, tariff_code)
            text_to_user = build_sub_link_message(sub_link, tariff.title, max_devices)

        card_text = await render_user_card_text(session, user)
        banned = user.is_banned

    try:
        await bot.send_message(target_tg_id, text_to_user)
    except Exception:
        pass

    await callback.message.edit_text(card_text, reply_markup=user_manage_keyboard(target_tg_id, banned))
    await callback.answer("Доступ выдан", show_alert=True)

@router.callback_query(F.data == "admin:broadcast")
async def cb_admin_broadcast(callback: CallbackQuery):
    if not admin_allowed(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    PENDING_BROADCAST.add(callback.from_user.id)
    await callback.message.edit_text(
        "📣 Пришли следующим сообщением текст для рассылки всем пользователям.",
        reply_markup=back_admin_keyboard(),
    )
    await callback.answer()

# =========================================================
# ADMIN COMMANDS
# =========================================================
@router.message(Command("setprice"))
async def cmd_setprice(message: Message):
    if not admin_allowed(message.from_user.id):
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) != 3:
        await message.answer("Использование: <code>/setprice core 299</code>")
        return

    _, code, price_rub = parts
    if code not in {"core", "pro"}:
        await message.answer("Только core или pro.")
        return

    try:
        price_kopecks = int(float(price_rub) * 100)
    except ValueError:
        await message.answer("Цена должна быть числом.")
        return

    async with SessionLocal() as session:
        tariff = await get_tariff(session, code)
        if not tariff:
            await message.answer("Тариф не найден.")
            return
        tariff.price_kopecks = price_kopecks
        await session.commit()
        await log_admin(session, message.from_user.id, "set_price", f"{code}:{price_kopecks}")

    await message.answer(f"✅ Цена {code.upper()} обновлена: {rub(price_kopecks)}")

@router.message(Command("setsub"))
async def cmd_setsub(message: Message):
    if not admin_allowed(message.from_user.id):
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) != 3:
        await message.answer("Использование: <code>/setsub core https://example.com/sub</code>")
        return

    _, code, link = parts
    if code not in {"core", "pro"}:
        await message.answer("Только core или pro.")
        return

    async with SessionLocal() as session:
        await set_setting(session, f"{code}_sub_link", link)
        await log_admin(session, message.from_user.id, "set_sub_link", code)

    await message.answer(f"✅ Sub link для {code.upper()} обновлён.")

# =========================================================
# MESSAGE ROUTER FOR ADMIN FLOWS
# =========================================================
@router.message()
async def fallback_message_router(message: Message):
    if not admin_allowed(message.from_user.id):
        return

    # Search user flow
    if message.from_user.id in PENDING_SEARCH:
        PENDING_SEARCH.discard(message.from_user.id)
        query = (message.text or "").strip()

        async with SessionLocal() as session:
            user = None
            if query.startswith("@"):
                res = await session.execute(select(User).where(User.username == query[1:]))
                user = res.scalar_one_or_none()
            elif query.isdigit():
                user = await get_user_by_tg(session, int(query))

            if not user:
                await message.answer("Пользователь не найден.")
                return

            text = await render_user_card_text(session, user)

        await message.answer(text, reply_markup=user_manage_keyboard(user.telegram_id, user.is_banned))
        return

    # Send direct message flow
    if message.from_user.id in PENDING_SENDTO:
        target_tg_id = PENDING_SENDTO.pop(message.from_user.id)
        text = (message.text or "").strip()
        if not text:
            await message.answer("Пустое сообщение не отправлено.")
            return
        try:
            await bot.send_message(target_tg_id, text)
            await message.answer("✅ Сообщение отправлено.")
        except Exception as e:
            await message.answer(f"Не удалось отправить: {e}")
        return

    # Broadcast flow
    if message.from_user.id in PENDING_BROADCAST:
        PENDING_BROADCAST.discard(message.from_user.id)
        text = (message.text or "").strip()
        if not text:
            await message.answer("Пустая рассылка отменена.")
            return

        async with SessionLocal() as session:
            res = await session.execute(select(User.telegram_id).where(User.is_banned == False))
            users = [row[0] for row in res.all()]
            sent = 0
            for tg_id in users:
                try:
                    await bot.send_message(tg_id, f"📣 {text}")
                    sent += 1
                    await asyncio.sleep(0.04)
                except Exception:
                    continue
            await log_admin(session, message.from_user.id, "broadcast", f"sent:{sent}")

        await message.answer(f"✅ Рассылка завершена. Отправлено: {sent}")
        return

# =========================================================
# FASTAPI
# =========================================================
@app.get("/")
async def root():
    return {
        "ok": True,
        "telegram_webhook": f"{BASE_URL}{WEBHOOK_PATH}",
        "yookassa_webhook": f"{BASE_URL}{YOOKASSA_WEBHOOK_PATH}",
    }

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    data = await request.json()
    await dp.feed_raw_update(bot, data)
    return JSONResponse({"ok": True})

@app.post(YOOKASSA_WEBHOOK_PATH)
async def yookassa_webhook(request: Request):
    payload = await request.json()
    event = payload.get("event")
    obj = payload.get("object", {})
    payment_id = obj.get("id")

    if event == "payment.succeeded" and payment_id:
        await process_successful_payment(payment_id)

    return JSONResponse({"ok": True})

# =========================================================
# STARTUP / SHUTDOWN
# =========================================================
async def setup_webhook():
    await bot.set_webhook(
        url=f"{BASE_URL}{WEBHOOK_PATH}",
        allowed_updates=dp.resolve_used_update_types(),
        drop_pending_updates=True,
    )
    logger.info("Webhook set: %s%s", BASE_URL, WEBHOOK_PATH)

@app.on_event("startup")
async def on_startup():
    await create_tables()
    await setup_webhook()
    logger.info("App started")

@app.on_event("shutdown")
async def on_shutdown():
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass
    await bot.session.close()
    await engine.dispose()
