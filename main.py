import asyncio
import logging
import os
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, CommandStart
from config import BOT_TOKEN, FARM_TYPES, NFT_GIFTS, GAME_NAME, ADMIN_IDS, CRYSTAL_SHOP, CRYSTAL_CASES
from database import (
    init_db, get_or_create_user, get_user_stars, 
    buy_farm, get_user_farms, buy_nft, get_user_nfts,
    calculate_total_boost, collect_farm_income,
    register_referral, give_referral_reward, get_referral_count,
    create_auction, get_active_auctions, place_bid, end_auction,
    activate_farms, is_banned, ban_user, unban_user,
    admin_add_stars, admin_add_farm, admin_add_nft,
    get_all_users, get_all_chats, add_chat, spend_stars, add_stars,
    get_user_by_internal_id, get_user_info_by_internal_id,
    get_top_by_balance, get_top_by_income_per_minute, get_top_by_nft_count
)

from database import (
    get_user_crystals, transfer_crystals, collect_farm_income_with_crystals,
    spend_crystals, add_crystals,
    create_farm_trade, get_farm_trade, set_farm_trade_status, transfer_farm_ownership
)
from keyboards import (
    get_main_menu, get_farm_shop_keyboard, 
    get_nft_shop_keyboard, get_casino_menu, 
    get_mines_keyboard, get_mines_bet_keyboard,
    get_dice_choice_keyboard, get_dice_bet_keyboard, get_slots_bet_keyboard
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def weighted_choice(items):
    import random
    total = sum(int(i.get('weight', 0)) for i in items)
    r = random.uniform(0, total)
    upto = 0
    for item in items:
        w = int(item.get('weight', 0))
        if w <= 0:
            continue
        upto += w
        if upto >= r:
            return item
    return items[-1] if items else None

def pick_random_nft_key():
    import random
    keys = list(NFT_GIFTS.keys())
    return random.choice(keys) if keys else None

async def resolve_target_user(target: str):
    target = (target or "").strip()
    if not target:
        return None

    if target.startswith("@"):
        try:
            chat = await bot.get_chat(target)
            user = await get_or_create_user(chat.id)
            return {
                'user_id': user['user_id'],
                'internal_id': user.get('internal_id')
            }
        except Exception:
            return None

    if target.isdigit():
        num = int(target)

        # Heuristic: internal_id is usually small; telegram user_id is usually large.
        if len(target) <= 9:
            user_by_internal = await get_user_by_internal_id(num)
            if user_by_internal:
                return {
                    'user_id': user_by_internal['user_id'],
                    'internal_id': user_by_internal.get('internal_id')
                }

        user = await get_or_create_user(num)
        return {
            'user_id': user['user_id'],
            'internal_id': user.get('internal_id')
        }

    return None

if not BOT_TOKEN or BOT_TOKEN == "":
    logger.error("BOT_TOKEN не установлен! Установите переменную окружения BOT_TOKEN")
    raise ValueError("BOT_TOKEN не установлен")

token_parts = BOT_TOKEN.split(":")
if len(token_parts) != 2:
    logger.error("Неверный формат BOT_TOKEN! Должен быть в формате '123456789:ABCdefGHIjklMNOpqrsTUVwxyz'")
    raise ValueError("Неверный формат BOT_TOKEN")

if not token_parts[0].isdigit():
    logger.error("Первая часть токена должна быть числом!")
    raise ValueError("Неверный формат BOT_TOKEN")

logger.info(f"Токен бота загружен (ID бота: {token_parts[0]}, длина: {len(BOT_TOKEN)})")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

mines_games = {}
pending_bets = {}

async def ban_check_middleware(handler, event, data):
    if isinstance(event, (Message, CallbackQuery)):
        if hasattr(event, 'from_user') and event.from_user:
            user_id = event.from_user.id
            try:
                banned = await is_banned(user_id)
                if banned:
                    if isinstance(event, Message):
                        await event.answer("❌ Вы заблокированы в боте!")
                    elif isinstance(event, CallbackQuery):
                        await event.answer("❌ Вы заблокированы в боте!", show_alert=True)
                    return
            except Exception as db_error:
                logger.error(f"Ошибка проверки бана для user_id {user_id}: {db_error}")
    return await handler(event, data)

dp.message.middleware(ban_check_middleware)
dp.callback_query.middleware(ban_check_middleware)

@dp.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    
    args = message.text.split()[1:] if len(message.text.split()) > 1 else []
    
    is_new_user = False
    if args:
        try:
            referrer_id = int(args[0])
            if referrer_id != user_id:
                is_new_user = await register_referral(referrer_id, user_id)
                if is_new_user:
                    await give_referral_reward(user_id)
                    try:
                        from config import REFERRAL_REWARD
                        referrer_name = message.from_user.full_name or f"@{message.from_user.username}" if message.from_user.username else "Пользователь"
                        referrer_mention = f"@{message.from_user.username}" if message.from_user.username else referrer_name
                        notification = (
                            f"🎉 Новый пользователь {referrer_mention} зарегистрировался по вашей реферальной ссылке!\n"
                            f"💰 Вам зачислено {REFERRAL_REWARD} ⭐"
                        )
                        await bot.send_message(referrer_id, notification)
                    except:
                        pass
        except ValueError:
            pass
    
    user = await get_or_create_user(user_id)
    
    welcome_text = (
        f"🌟 Добро пожаловать в {GAME_NAME}!\n\n"
        "💰 Валюта: Звезды ⭐\n"
        "🌾 Покупайте фермы, которые приносят звезды\n"
        "🎁 Покупайте NFT подарки для буста к доходу\n\n"
    )
    
    if is_new_user:
        from config import REFERRAL_REWARD
        welcome_text += f"🎉 Вы получили {REFERRAL_REWARD} ⭐ за регистрацию по реферальной ссылке!\n\n"
    
    welcome_text += "Используйте меню для навигации или команду /help для списка команд!"
    
    if message.chat.type == "private":
        await message.answer(welcome_text, reply_markup=get_main_menu())
    else:
        await message.reply(welcome_text)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        f"📖 Справка по командам {GAME_NAME}\n\n"
        "🔹 /start - Начать игру или зарегистрироваться\n"
        "🔹 /help - Показать эту справку\n"
        "🔹 /profile - Показать ваш профиль\n"
        "🔹 /farms - Показать ваши фермы\n"
        "🔹 /shop - Открыть магазин ферм\n"
        "🔹 /nft - Открыть магазин NFT\n"
        "🔹 /collect - Собрать доход с ферм\n"
        "🔹 /activate - Активировать фермы (каждые 6 часов)\n"
        "🔹 /cases - Кейсы за кристаллы\n"
        "🔹 /crystals - Баланс кристаллов\n"
        "🔹 /crystal_shop - Обмен кристаллов на звезды\n"
        "🔹 /send_crystals <target> <amount> - Отправить кристаллы\n"
        "    target: internal_id | telegram_id | @username\n"
        "🔹 /sell_farm <farm_id> <target> <price> - Продать ферму\n"
        "    target: internal_id | telegram_id | @username\n"
        "🔹 /referral - Получить реферальную ссылку\n"
        "🔹 /auction - Показать активные аукционы\n"
        "🔹 /top - Показать топ игроков\n\n"
        "💡 Важно:\n"
        "• Фермы нужно активировать каждые 6 часов\n"
        "• Только активированные фермы приносят доход\n"
        "• Используйте NFT для увеличения дохода\n"
        "• Приглашайте друзей по реферальной ссылке!"
    )
    
    if message.chat.type == "private":
        await message.answer(help_text)
    else:
        await message.reply(help_text)

@dp.message(Command("ahelp"))
async def cmd_ahelp(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    ahelp_text = (
        "🛠 Админ-команды\n\n"
        "🔸 /profile_id <internal_id> - Профиль по игровому ID\n"
        "🔸 /add_stars <target> <amount> - Выдать/снять звезды\n"
        "🔸 /add_crystals <target> <amount> - Выдать/снять кристаллы\n"
        "    target: internal_id | telegram_id | @username\n"
    )

    if message.chat.type == "private":
        await message.answer(ahelp_text)
    else:
        await message.reply(ahelp_text)

@dp.message(Command("referral"))
async def cmd_referral(message: Message):
    user_id = message.from_user.id
    user = await get_or_create_user(user_id)
    internal_id = user.get('internal_id')
    referrals = await get_referral_count(user_id)

    try:
        me = await bot.get_me()
        bot_username = me.username
    except Exception:
        bot_username = None

    if bot_username and internal_id is not None:
        link = f"https://t.me/{bot_username}?start={internal_id}"
        try:
            from config import REFERRAL_REWARD
            reward_text = f"\n\n🎁 Награда за приглашение: {REFERRAL_REWARD} ⭐"
        except Exception:
            reward_text = ""

        text = (
            "🔗 Ваша реферальная ссылка:\n"
            f"{link}\n\n"
            f"👥 Приглашено: {referrals}" + reward_text
        )
    else:
        text = (
            "❌ Не удалось сформировать реферальную ссылку.\n"
            "Попробуйте позже или используйте /start."
        )

    if message.chat.type == "private":
        await message.answer(text)
    else:
        await message.reply(text)

@dp.message(F.text == "🔗 Реферальная ссылка")
async def referral_button(message: Message):
    await cmd_referral(message)

@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    await show_profile_handler(message)

@dp.message(F.text == "⭐ Мой профиль")
async def show_profile(message: Message):
    await show_profile_handler(message)

@dp.message(Command("profile_id"))
async def cmd_profile_id(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Использование: /profile_id internal_id\nПример: /profile_id 1")
        return
    
    try:
        internal_id = int(args[1])
        user = await get_user_by_internal_id(internal_id)
        if not user:
            await message.reply(f"❌ Пользователь с ID {internal_id} не найден!")
            return
        
        user_id = user['user_id']
        stars = user['stars']
        farms = await get_user_farms(user_id)
        nfts = await get_user_nfts(user_id)
        boost = await calculate_total_boost(user_id)
        referrals = await get_referral_count(user_id)
        
        from datetime import datetime
        active_farms = 0
        for farm in farms:
            is_active = farm.get('is_active', 0)
            if is_active:
                last_activated = farm.get('last_activated')
                if last_activated:
                    last_activated_dt = datetime.fromisoformat(last_activated)
                    hours_passed = (datetime.now() - last_activated_dt).total_seconds() / 3600
                    if hours_passed < 6:
                        active_farms += 1
        
        try:
            tg_user = await bot.get_chat(user_id)
            username = f"@{tg_user.username}" if tg_user.username else tg_user.full_name or "Неизвестно"
        except:
            username = "Неизвестно"
        
        profile_text = (
            f"👤 Профиль пользователя\n\n"
            f"🆔 ID: {internal_id}\n"
            f"📱 Telegram: {username} ({user_id})\n"
            f"⭐ Звезд: {stars}\n"
            f"🌾 Ферм: {len(farms)} (активных: {active_farms})\n"
            f"🎁 NFT: {len(nfts)}\n"
            f"⚡ Буст к доходу: {int((boost - 1) * 100)}%\n"
            f"🔗 Рефералов: {referrals}\n"
        )
        
        await message.reply(profile_text)
    except ValueError:
        await message.reply("❌ Неверный формат! Используйте: /profile_id internal_id")

async def show_profile_handler(message: Message):
    user_id = message.from_user.id
    user = await get_or_create_user(user_id)
    stars = user['stars']
    crystals = user.get('crystals', 0) or 0
    
    farms = await get_user_farms(user_id)
    nfts = await get_user_nfts(user_id)
    boost = await calculate_total_boost(user_id)
    referrals = await get_referral_count(user_id)
    
    from datetime import datetime
    active_farms = 0
    for farm in farms:
        is_active = farm.get('is_active', 0)
        if is_active:
            last_activated = farm.get('last_activated')
            if last_activated:
                last_activated_dt = datetime.fromisoformat(last_activated)
                hours_passed = (datetime.now() - last_activated_dt).total_seconds() / 3600
                if hours_passed < 6:
                    active_farms += 1
    
    internal_id = user.get('internal_id', 'N/A')
    profile_text = (
        f"👤 Ваш профиль\n\n"
        f"🆔 ID: {internal_id}\n"
        f"⭐ Звезд: {stars}\n"
        f"💎 Кристаллов: {crystals}\n"
        f"🌾 Ферм: {len(farms)} (активных: {active_farms})\n"
        f"🎁 NFT: {len(nfts)}\n"
        f"⚡ Буст к доходу: {int((boost - 1) * 100)}%\n"
        f"🔗 Рефералов: {referrals}\n\n"
    )
    
    if farms:
        profile_text += "Ваши фермы:\n"
        from datetime import datetime
        farm_counts = {}
        for farm in farms:
            farm_type = farm['farm_type']
            if farm_type not in farm_counts:
                farm_counts[farm_type] = {'total': 0, 'active': 0}
            farm_counts[farm_type]['total'] += 1

            is_active = farm.get('is_active', 0)
            if is_active:
                last_activated = farm.get('last_activated')
                if last_activated:
                    last_activated_dt = datetime.fromisoformat(last_activated)
                    hours_passed = (datetime.now() - last_activated_dt).total_seconds() / 3600
                    if hours_passed < 6:
                        farm_counts[farm_type]['active'] += 1

        total_active_base_per_hour = 0
        total_all_base_per_hour = 0

        for farm_type, data in farm_counts.items():
            if farm_type not in FARM_TYPES:
                continue
            farm_data = FARM_TYPES[farm_type]
            total = data['total']
            active = data['active']
            status = "✅" if active > 0 else "❌"

            base_active_per_hour = farm_data['income_per_hour'] * active
            base_all_per_hour = farm_data['income_per_hour'] * total
            total_active_base_per_hour += base_active_per_hour
            total_all_base_per_hour += base_all_per_hour

            boosted_active_per_hour = int(base_active_per_hour * boost)
            boosted_all_per_hour = int(base_all_per_hour * boost)

            profile_text += f"{status} {farm_data['name']}: {total} шт. (активных: {active})\n"
            profile_text += f"  Активные без буста: {round(base_active_per_hour / 60, 2)} ⭐/мин | {base_active_per_hour} ⭐/час\n"
            profile_text += f"  Активные с бустом: {round(boosted_active_per_hour / 60, 2)} ⭐/мин | {boosted_active_per_hour} ⭐/час\n"
            profile_text += f"  Всего без буста: {round(base_all_per_hour / 60, 2)} ⭐/мин | {base_all_per_hour} ⭐/час\n"
            profile_text += f"  Всего с бустом: {round(boosted_all_per_hour / 60, 2)} ⭐/мин | {boosted_all_per_hour} ⭐/час\n\n"

        total_active_boosted_per_hour = int(total_active_base_per_hour * boost)
        total_all_boosted_per_hour = int(total_all_base_per_hour * boost)
        profile_text += (
            f"📊 Итого (активные) без буста: {round(total_active_base_per_hour / 60, 2)} ⭐/мин | {total_active_base_per_hour} ⭐/час\n"
            f"⚡ Итого (активные) с бустом: {round(total_active_boosted_per_hour / 60, 2)} ⭐/мин | {total_active_boosted_per_hour} ⭐/час\n"
            f"📊 Итого (все фермы) без буста: {round(total_all_base_per_hour / 60, 2)} ⭐/мин | {total_all_base_per_hour} ⭐/час\n"
            f"⚡ Итого (все фермы) с бустом: {round(total_all_boosted_per_hour / 60, 2)} ⭐/мин | {total_all_boosted_per_hour} ⭐/час\n"
        )
    
    if nfts:
        profile_text += "\nВаши NFT:\n"
        nft_counts = {}
        for nft in nfts:
            nft_type = nft['nft_type']
            nft_counts[nft_type] = nft_counts.get(nft_type, 0) + 1
        
        for nft_type, count in nft_counts.items():
            if nft_type in NFT_GIFTS:
                profile_text += f"  {NFT_GIFTS[nft_type]['name']}: {count} шт.\n"
    
    if message.chat.type == "private":
        await message.answer(profile_text)
    else:
        await message.reply(profile_text)

@dp.message(Command("farms"))
async def cmd_farms(message: Message):
    await show_farms_handler(message)

@dp.message(F.text == "🌾 Мои фермы")
async def show_farms(message: Message):
    await show_farms_handler(message)

async def show_farms_handler(message: Message):
    user_id = message.from_user.id
    farms = await get_user_farms(user_id)
    
    if not farms:
        response = "У вас пока нет ферм. Купите их в магазине! 🛒"
        if message.chat.type == "private":
            await message.answer(response)
        else:
            await message.reply(response)
        return
    
    from datetime import datetime
    farm_counts = {}
    active_count = 0
    inactive_count = 0
    
    for farm in farms:
        farm_type = farm['farm_type']
        farm_counts[farm_type] = farm_counts.get(farm_type, {'total': 0, 'active': 0})
        farm_counts[farm_type]['total'] += 1
        
        is_active = farm.get('is_active', 0)
        if is_active:
            last_activated = farm.get('last_activated')
            if last_activated:
                last_activated_dt = datetime.fromisoformat(last_activated)
                hours_passed = (datetime.now() - last_activated_dt).total_seconds() / 3600
                if hours_passed < 6:
                    farm_counts[farm_type]['active'] += 1
                    active_count += 1
                else:
                    inactive_count += 1
            else:
                inactive_count += 1
        else:
            inactive_count += 1
    
    farms_text = "🌾 Ваши фермы:\n\n"
    boost = await calculate_total_boost(user_id)
    total_all_base_per_hour = 0
    total_active_base_per_hour = 0

    for farm_type, data in farm_counts.items():
        if farm_type in FARM_TYPES:
            farm_data = FARM_TYPES[farm_type]
            total = data['total']
            active = data['active']
            inactive = total - active

            base_active_per_hour = farm_data['income_per_hour'] * active
            base_all_per_hour = farm_data['income_per_hour'] * total
            total_active_base_per_hour += base_active_per_hour
            total_all_base_per_hour += base_all_per_hour

            boosted_active_per_hour = int(base_active_per_hour * boost)

            income_per_min = round(base_active_per_hour / 60, 2)
            status = "✅" if active > 0 else "❌"
            farms_text += f"{status} {farm_data['name']}: {total} шт. (активных: {active})\n"
            if active > 0:
                farms_text += f"  Доход (активные) без буста: {income_per_min} ⭐/мин | {base_active_per_hour} ⭐/час\n"
                farms_text += f"  ⚡ Доход (активные) с бустом: {round(boosted_active_per_hour / 60, 2)} ⭐/мин | {boosted_active_per_hour} ⭐/час\n\n"
            else:
                farms_text += f"  ⚠️ Требуется активация (/activate)\n\n"

    total_active_boosted_per_hour = int(total_active_base_per_hour * boost)
    total_all_boosted_per_hour = int(total_all_base_per_hour * boost)

    farms_text += f"📊 Итого (активные) без буста: {round(total_active_base_per_hour / 60, 2)} ⭐/мин | {total_active_base_per_hour} ⭐/час\n"
    farms_text += f"⚡ Итого (активные) с бустом: {round(total_active_boosted_per_hour / 60, 2)} ⭐/мин | {total_active_boosted_per_hour} ⭐/час\n"
    farms_text += f"📊 Итого (все фермы) без буста: {round(total_all_base_per_hour / 60, 2)} ⭐/мин | {total_all_base_per_hour} ⭐/час\n"
    farms_text += f"⚡ Итого (все фермы) с бустом: {round(total_all_boosted_per_hour / 60, 2)} ⭐/мин | {total_all_boosted_per_hour} ⭐/час\n"
    
    if inactive_count > 0:
        farms_text += f"\n⚠️ {inactive_count} ферм требуют активации! Используйте /activate"
    
    if message.chat.type == "private":
        await message.answer(farms_text)
    else:
        await message.reply(farms_text)

@dp.message(Command("farm_ids"))
async def cmd_farm_ids(message: Message):
    user_id = message.from_user.id
    farms = await get_user_farms(user_id)
    if not farms:
        if message.chat.type == "private":
            await message.answer("🌾 У вас нет ферм.")
        else:
            await message.reply("🌾 У вас нет ферм.")
        return

    text = "🌾 Ваши фермы (ID для трейдов):\n\n"
    for farm in farms:
        farm_id = farm.get('id')
        farm_type = farm.get('farm_type')
        farm_name = FARM_TYPES.get(farm_type, {}).get('name', str(farm_type))
        text += f"🆔 {farm_id} — {farm_name}\n"

    if message.chat.type == "private":
        await message.answer(text)
    else:
        await message.reply(text)

@dp.message(Command("shop"))
async def cmd_shop(message: Message):
    await show_farm_shop_handler(message)

@dp.message(F.text == "🛒 Магазин ферм")
async def show_farm_shop(message: Message):
    await show_farm_shop_handler(message)

async def show_farm_shop_handler(message: Message):
    user_id = message.from_user.id
    stars = await get_user_stars(user_id)
    
    shop_text = f"🛒 Магазин ферм\n\n⭐ Ваши звезды: {stars}\n\nВыберите ферму:"
    
    if message.chat.type == "private":
        await message.answer(shop_text, reply_markup=get_farm_shop_keyboard())
    else:
        await message.reply(shop_text + "\n💡 В группах используйте команды для покупки")

@dp.message(Command("nft"))
async def cmd_nft(message: Message):
    await show_nft_shop_handler(message)

@dp.message(F.text == "🎁 Магазин NFT")
async def show_nft_shop(message: Message):
    await show_nft_shop_handler(message)

async def show_nft_shop_handler(message: Message):
    user_id = message.from_user.id
    stars = await get_user_stars(user_id)
    
    shop_text = (
        f"🎁 Магазин NFT подарков\n\n"
        f"⭐ Ваши звезды: {stars}\n\n"
        f"NFT дают буст к доходу с ферм!\n\n"
        f"Выберите NFT:"
    )
    
    if message.chat.type == "private":
        await message.answer(shop_text, reply_markup=get_nft_shop_keyboard())
    else:
        await message.reply(shop_text + "\n💡 В группах используйте команды для покупки")

@dp.message(Command("activate"))
async def cmd_activate(message: Message):
    user_id = message.from_user.id
    farms = await get_user_farms(user_id)
    
    if not farms:
        response = "У вас нет ферм для активации! Купите фермы в магазине. 🛒"
        if message.chat.type == "private":
            await message.answer(response)
        else:
            await message.reply(response)
        return
    
    activated, total = await activate_farms(user_id)
    
    if activated > 0:
        response = (
            f"✅ Активировано ферм: {activated} из {total}\n\n"
            f"🌾 Ваши фермы активны на следующие 6 часов!\n"
            f"💡 Не забудьте собрать доход командой /collect"
        )
    else:
        from datetime import datetime
        can_activate_soon = False
        min_hours_left = 6
        for farm in farms:
            last_activated = farm.get('last_activated')
            if last_activated:
                last_activated_dt = datetime.fromisoformat(last_activated)
                hours_passed = (datetime.now() - last_activated_dt).total_seconds() / 3600
                hours_left = 6 - hours_passed
                if hours_left > 0:
                    min_hours_left = min(min_hours_left, hours_left)
                    can_activate_soon = True
        
        if can_activate_soon:
            hours = int(min_hours_left)
            minutes = int((min_hours_left - hours) * 60)
            response = (
                f"⏰ Все фермы уже активированы!\n\n"
                f"🔄 Следующая активация через: {hours}ч {minutes}м"
            )
        else:
            response = (
                f"✅ Все фермы активированы!\n\n"
                f"💡 Фермы активны на 6 часов. Используйте /collect для сбора дохода."
            )
    
    if message.chat.type == "private":
        await message.answer(response)
    else:
        await message.reply(response)

@dp.message(Command("crystals"))
async def cmd_crystals(message: Message):
    user_id = message.from_user.id
    crystals = await get_user_crystals(user_id)
    if message.chat.type == "private":
        await message.answer(f"💎 Ваши кристаллы: {crystals}")
    else:
        await message.reply(f"💎 Ваши кристаллы: {crystals}")

@dp.message(F.text == "🎁 Кейсы")
async def cases_button(message: Message):
    await cmd_cases(message)

@dp.message(Command("add_crystals"))
async def cmd_add_crystals(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /add_crystals <target> <amount>\nTarget: internal_id | telegram_id | @username")
        return

    target = args[1]
    try:
        amount = int(args[2])
    except ValueError:
        await message.reply("❌ amount должен быть числом")
        return

    if amount == 0:
        await message.reply("❌ amount не может быть 0")
        return

    resolved = await resolve_target_user(target)
    if not resolved:
        await message.reply("❌ Пользователь не найден")
        return

    await add_crystals(resolved['user_id'], amount)
    await message.reply(f"✅ Выдано кристаллов: {amount} 💎\nКому: {target} (tg_id={resolved['user_id']}, id={resolved.get('internal_id')})")

@dp.message(Command("add_stars"))
async def cmd_add_stars(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /add_stars <target> <amount>\nTarget: internal_id | telegram_id | @username")
        return

    target = args[1]
    try:
        amount = int(args[2])
    except ValueError:
        await message.reply("❌ amount должен быть числом")
        return

    if amount == 0:
        await message.reply("❌ amount не может быть 0")
        return

    resolved = await resolve_target_user(target)
    if not resolved:
        await message.reply("❌ Пользователь не найден")
        return

    await add_stars(resolved['user_id'], amount)
    await message.reply(f"✅ Выдано звезд: {amount} ⭐\nКому: {target} (tg_id={resolved['user_id']}, id={resolved.get('internal_id')})")

@dp.message(Command("send_crystals"))
async def cmd_send_crystals(message: Message):
    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /send_crystals <target> <amount>\nTarget: internal_id | telegram_id | @username")
        return

    try:
        amount = int(args[2])
    except ValueError:
        await message.reply("❌ Неверный формат. Используйте: /send_crystals <target> <amount>")
        return

    if amount <= 0:
        await message.reply("❌ Сумма должна быть больше 0")
        return

    resolved = await resolve_target_user(args[1])
    if not resolved:
        await message.reply("❌ Пользователь не найден")
        return

    from_user_id = message.from_user.id
    to_user_id = resolved['user_id']
    if to_user_id == from_user_id:
        await message.reply("❌ Нельзя отправить самому себе")
        return

    ok = await transfer_crystals(from_user_id, to_user_id, amount)
    if not ok:
        await message.reply("❌ Недостаточно кристаллов")
        return

    await message.reply(f"✅ Вы отправили {amount} 💎 пользователю {args[1]}")
    try:
        await bot.send_message(to_user_id, f"💎 Вам пришли кристаллы: +{amount}\nОт игрока ID { (await get_or_create_user(from_user_id)).get('internal_id', 'N/A') }")
    except Exception:
        pass

@dp.message(Command("crystal_shop"))
async def cmd_crystal_shop(message: Message):
    user_id = message.from_user.id
    crystals = await get_user_crystals(user_id)
    text = f"💎 Магазин кристаллов\n\n💠 У вас: {crystals} 💎\n\nВыберите товар:"
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for item_id, item in CRYSTAL_SHOP.items():
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{item['name']} — {item['price']} 💎",
                callback_data=f"buy_crystal_{item_id}"
            )
        ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    if message.chat.type == "private":
        await message.answer(text, reply_markup=kb)
    else:
        await message.reply(text)

@dp.callback_query(F.data.startswith("buy_crystal_"))
async def buy_crystal_item(callback: CallbackQuery):
    user_id = callback.from_user.id
    item_id = callback.data.split("_")[2]
    if item_id not in CRYSTAL_SHOP:
        await callback.answer("❌ Товар не найден", show_alert=True)
        return

    item = CRYSTAL_SHOP[item_id]
    price = item['price']
    ok = await spend_crystals(user_id, price)
    if not ok:
        await callback.answer("❌ Недостаточно кристаллов", show_alert=True)
        return

    await add_stars(user_id, item['stars'])
    await callback.answer(f"✅ Покупка успешна: {item['name']}", show_alert=True)
    await cmd_crystal_shop(callback.message)

@dp.message(Command("cases"))
async def cmd_cases(message: Message):
    user_id = message.from_user.id
    crystals = await get_user_crystals(user_id)

    text = f"🎁 Кейсы за кристаллы\n\n💠 У вас: {crystals} 💎\n\nВыберите кейс:"
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for case_id, case in CRYSTAL_CASES.items():
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{case['name']} — {case['price']} 💎",
                callback_data=f"open_case_{case_id}"
            )
        ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])

    if message.chat.type == "private":
        await message.answer(text, reply_markup=kb)
    else:
        await message.reply(text)

@dp.callback_query(F.data.startswith("open_case_"))
async def open_case(callback: CallbackQuery):
    user_id = callback.from_user.id
    case_id = callback.data.split("_")[2]
    if case_id not in CRYSTAL_CASES:
        await callback.answer("❌ Кейс не найден", show_alert=True)
        return

    case = CRYSTAL_CASES[case_id]
    price = int(case.get('price', 0))
    if price <= 0:
        await callback.answer("❌ Ошибка кейса", show_alert=True)
        return

    ok = await spend_crystals(user_id, price)
    if not ok:
        await callback.answer("❌ Недостаточно кристаллов", show_alert=True)
        return

    reward = weighted_choice(case.get('rewards', []))
    if not reward:
        await callback.answer("❌ Ошибка награды", show_alert=True)
        return

    reward_text = ""
    rtype = reward.get('type')
    if rtype == 'stars':
        amount = int(reward.get('amount', 0))
        if amount > 0:
            await add_stars(user_id, amount)
            reward_text = f"⭐ {amount} звезд"
    elif rtype == 'crystals':
        amount = int(reward.get('amount', 0))
        if amount > 0:
            await add_crystals(user_id, amount)
            reward_text = f"💎 {amount} кристаллов"
    elif rtype == 'nft':
        nft_key = pick_random_nft_key()
        if nft_key:
            already = await get_user_nfts(user_id)
            if any(item.get('nft_type') == nft_key for item in already):
                await add_stars(user_id, 500)
                reward_text = f"🎁 NFT-дубликат → ⭐ 500 звезд"
            else:
                await admin_add_nft(user_id, nft_key)
                reward_text = f"🎁 NFT: {NFT_GIFTS[nft_key]['name']}"
        else:
            await add_stars(user_id, 200)
            reward_text = "⭐ 200 звезд"
    else:
        await add_stars(user_id, 100)
        reward_text = "⭐ 100 звезд"

    stars = await get_user_stars(user_id)
    crystals = await get_user_crystals(user_id)

    await callback.message.answer(
        f"🎁 Вы открыли кейс: {case['name']}\n\n"
        f"Получено: {reward_text}\n\n"
        f"Баланс: ⭐ {stars} | 💎 {crystals}"
    )

    await callback.answer("✅ Кейс открыт!", show_alert=True)

@dp.message(Command("sell_farm"))
async def cmd_sell_farm(message: Message):
    args = message.text.split()
    if len(args) < 4:
        await message.reply("Использование: /sell_farm <farm_id> <target> <price>\nTarget: internal_id | telegram_id | @username")
        return

    try:
        farm_id = int(args[1])
        price = int(args[3])
    except ValueError:
        await message.reply("❌ Неверный формат")
        return

    if price <= 0:
        await message.reply("❌ Цена должна быть больше 0")
        return

    seller_id = message.from_user.id

    buyer_resolved = await resolve_target_user(args[2])
    if not buyer_resolved:
        await message.reply("❌ Покупатель не найден")
        return

    buyer_id = buyer_resolved['user_id']
    if buyer_id == seller_id:
        await message.reply("❌ Нельзя продать самому себе")
        return

    trade_id = await create_farm_trade(seller_id, buyer_id, farm_id, price)
    if not trade_id:
        await message.reply("❌ Ферма не найдена или не принадлежит вам")
        return

    await message.reply(f"✅ Оффер создан. ID трейда: {trade_id}")

    try:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_farm_trade_{trade_id}"),
                InlineKeyboardButton(text="❌ Отказ", callback_data=f"decline_farm_trade_{trade_id}")
            ]
        ])
        await bot.send_message(
            buyer_id,
            f"🔁 Вам предлагают купить ферму\n\n🆔 Trade: {trade_id}\n🆔 Farm: {farm_id}\n💰 Цена: {price} ⭐\n\nПринять?",
            reply_markup=kb
        )
    except Exception:
        pass

@dp.callback_query(F.data.startswith("accept_farm_trade_"))
async def accept_farm_trade(callback: CallbackQuery):
    trade_id = int(callback.data.split("_")[3])
    trade = await get_farm_trade(trade_id)
    if not trade or trade.get('status') != 'pending':
        await callback.answer("❌ Трейд не найден", show_alert=True)
        return

    buyer_id = callback.from_user.id
    if trade.get('buyer_id') != buyer_id:
        await callback.answer("❌ Это не ваш трейд", show_alert=True)
        return

    price = int(trade.get('price'))
    seller_id = int(trade.get('seller_id'))
    farm_id = int(trade.get('farm_id'))

    ok = await spend_stars(buyer_id, price)
    if not ok:
        await callback.answer("❌ Недостаточно звезд", show_alert=True)
        return

    await add_stars(seller_id, price)
    transferred = await transfer_farm_ownership(farm_id, seller_id, buyer_id)
    if not transferred:
        await add_stars(buyer_id, price)
        await spend_stars(seller_id, price)
        await callback.answer("❌ Ферма уже недоступна", show_alert=True)
        return

    await set_farm_trade_status(trade_id, 'completed')
    await callback.answer("✅ Трейд завершен", show_alert=True)
    try:
        await bot.send_message(seller_id, f"✅ Вашу ферму купили! Trade {trade_id} (+{price} ⭐)")
    except Exception:
        pass

@dp.callback_query(F.data.startswith("decline_farm_trade_"))
async def decline_farm_trade(callback: CallbackQuery):
    trade_id = int(callback.data.split("_")[3])
    trade = await get_farm_trade(trade_id)
    if not trade or trade.get('status') != 'pending':
        await callback.answer("❌ Трейд не найден", show_alert=True)
        return

    buyer_id = callback.from_user.id
    if trade.get('buyer_id') != buyer_id:
        await callback.answer("❌ Это не ваш трейд", show_alert=True)
        return

    await set_farm_trade_status(trade_id, 'declined')
    await callback.answer("❌ Отклонено", show_alert=True)
    try:
        await bot.send_message(int(trade.get('seller_id')), f"❌ Покупатель отклонил трейд {trade_id}")
    except Exception:
        pass

@dp.message(F.text == "⚡ Активировать фермы")
async def activate_farms_button(message: Message):
    await cmd_activate(message)

@dp.message(Command("collect"))
async def cmd_collect(message: Message):
    await collect_income_handler(message)

@dp.message(F.text == "💰 Собрать доход")
async def collect_income(message: Message):
    await collect_income_handler(message)

@dp.message(Command("casino"))
async def cmd_casino(message: Message):
    if message.chat.type == "private":
        await message.answer("🎰 Казино\n\nВыберите игру:", reply_markup=get_casino_menu())
    else:
        await message.reply("🎰 Казино\n\nВ группах используйте команды.")

@dp.message(F.text == "🎰 Казино")
async def show_casino(message: Message):
    await cmd_casino(message)

@dp.message(Command("auction"))
async def cmd_auction(message: Message):
    from datetime import datetime

    auctions = await get_active_auctions()
    if not auctions:
        if message.chat.type == "private":
            await message.answer("🔨 Аукцион\n\nСейчас нет активных аукционов.")
        else:
            await message.reply("🔨 Аукцион\n\nСейчас нет активных аукционов.")
        return

    text = "🔨 Аукцион\n\nАктивные лоты:\n\n"
    now = datetime.now()
    for auction in auctions:
        farm_type = auction.get("farm_type")
        farm_name = FARM_TYPES.get(farm_type, {}).get("name", str(farm_type))

        end_time_raw = auction.get("end_time")
        time_left_text = ""
        if end_time_raw:
            try:
                end_time = datetime.fromisoformat(end_time_raw)
                delta = end_time - now
                minutes_left = max(0, int(delta.total_seconds() // 60))
                hours = minutes_left // 60
                minutes = minutes_left % 60
                time_left_text = f"⏳ Осталось: {hours}ч {minutes}м\n"
            except Exception:
                time_left_text = ""

        text += (
            f"🆔 ID: {auction.get('id')}\n"
            f"🌾 Лот: {farm_name}\n"
            f"💰 Текущая ставка: {auction.get('current_bid')} ⭐\n"
            f"{time_left_text}"
            "\n"
        )

    text += "Чтобы сделать ставку: /bid <id> <сумма>"

    if message.chat.type == "private":
        await message.answer(text)
    else:
        await message.reply(text)

@dp.message(F.text == "🔨 Аукцион")
async def show_auction(message: Message):
    await cmd_auction(message)

async def collect_income_handler(message: Message):
    user_id = message.from_user.id
    farms = await get_user_farms(user_id)
    
    if not farms:
        response = "У вас нет ферм для сбора дохода! Купите фермы в магазине. 🛒"
        if message.chat.type == "private":
            await message.answer(response)
        else:
            await message.reply(response)
        return
    
    income, crystals_gained = await collect_farm_income_with_crystals(user_id)
    stars = await get_user_stars(user_id)
    crystals = await get_user_crystals(user_id)
    boost = await calculate_total_boost(user_id)
    
    from datetime import datetime
    total_income_per_hour = 0
    active_farms_count = 0
    for farm in farms:
        is_active = farm.get('is_active', 0)
        if is_active:
            last_activated = farm.get('last_activated')
            if last_activated:
                last_activated_dt = datetime.fromisoformat(last_activated)
                hours_passed = (datetime.now() - last_activated_dt).total_seconds() / 3600
                if hours_passed < 6:
                    farm_type = farm['farm_type']
                    if farm_type in FARM_TYPES:
                        total_income_per_hour += FARM_TYPES[farm_type]['income_per_hour']
                        active_farms_count += 1
    
    total_income_per_hour_boosted = int(total_income_per_hour * boost)
    total_income_per_min_boosted = round(total_income_per_hour_boosted / 60, 2)
    total_income_per_min = round(total_income_per_hour / 60, 2)
    
    if income > 0:
        boost_text = ""
        if boost > 1.0:
            boost_text = f"\n⚡ Буст от NFT: {int((boost - 1) * 100)}%"

        crystals_text = ""
        if crystals_gained > 0:
            crystals_text = f"\n💎 Найдено кристаллов: +{crystals_gained}" 
        
        response = (
            f"💰 Вы собрали доход!\n\n"
            f"⭐ Получено: {income} звезд{boost_text}\n"
            f"💎 Всего звезд: {stars}\n"
            f"💠 Кристаллов: {crystals}{crystals_text}\n\n"
            f"📊 Текущий доход ({active_farms_count} активных ферм):\n"
            f"   {total_income_per_min} ⭐/мин | {total_income_per_hour} ⭐/час"
        )
        if boost > 1.0:
            response += f"\n   ⚡ С бустом: {total_income_per_min_boosted} ⭐/мин | {total_income_per_hour_boosted} ⭐/час"
    else:
        if active_farms_count == 0:
            response = (
                f"⚠️ У вас нет активных ферм!\n"
                f"💎 Ваши звезды: {stars}\n\n"
                f"💡 Используйте /activate для активации ферм"
            )
        else:
            response = (
                f"⏰ Доход еще не накоплен.\n"
                f"💎 Ваши звезды: {stars}\n\n"
                f"📊 Текущий доход ({active_farms_count} активных ферм):\n"
                f"   {total_income_per_min} ⭐/мин | {total_income_per_hour} ⭐/час"
            )
            if boost > 1.0:
                response += f"\n   ⚡ С бустом: {total_income_per_min_boosted} ⭐/мин | {total_income_per_hour_boosted} ⭐/час"
            response += "\n\nДоход накапливается каждый час!"
    
    if message.chat.type == "private":
        await message.answer(response)
    else:
        await message.reply(response)

@dp.callback_query(F.data.startswith("buy_farm_"))
async def handle_buy_farm(callback: CallbackQuery):
    farm_id = callback.data.split("_")[2]
    
    if farm_id not in FARM_TYPES:
        await callback.answer("Ошибка: неверный тип фермы", show_alert=True)
        return
    
    user_id = callback.from_user.id
    farm_data = FARM_TYPES[farm_id]
    
    success = await buy_farm(user_id, farm_id)
    
    if success:
        stars = await get_user_stars(user_id)
        await callback.answer(
            f"✅ Вы купили {farm_data['name']}!",
            show_alert=True
        )
        
        shop_text = f"🛒 Магазин ферм\n\n⭐ Ваши звезды: {stars}\n\n"
        shop_text += f"✅ Вы купили {farm_data['name']}!\n\n"
        
        for farm_id_item, farm_data_item in FARM_TYPES.items():
            income_per_min = round(farm_data_item['income_per_hour'] / 60, 2)
            shop_text += (
                f"{farm_data_item['name']}\n"
                f"💰 Цена: {farm_data_item['price']} ⭐\n"
                f"📈 Доход: {income_per_min} ⭐/мин | {farm_data_item['income_per_hour']} ⭐/час\n\n"
            )
        
        await callback.message.edit_text(shop_text, reply_markup=get_farm_shop_keyboard())
    else:
        stars = await get_user_stars(user_id)
        await callback.answer(
            f"❌ Недостаточно звезд! Нужно {farm_data['price']}, у вас {stars}",
            show_alert=True
        )

@dp.callback_query(F.data == "casino_mines")
async def casino_mines_handler(callback: CallbackQuery):
    from keyboards import get_mines_bet_keyboard
    await callback.message.edit_text(
        "💣 Мины\n\n"
        "Выберите сумму ставки:",
        reply_markup=get_mines_bet_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "mines_custom_bet")
async def mines_custom_bet(callback: CallbackQuery):
    pending_bets[callback.from_user.id] = "mines"
    await callback.message.edit_text(
        "💣 Мины\n\nВведите сумму ставки (мин. 10 ⭐):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="casino_mines")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "casino_dice")
async def casino_dice_handler(callback: CallbackQuery):
    await callback.message.edit_text(
        "🎲 Кости\n\nВыберите сумму ставки:",
        reply_markup=get_dice_bet_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "dice_custom_bet")
async def dice_custom_bet(callback: CallbackQuery):
    pending_bets[callback.from_user.id] = "dice"
    await callback.message.edit_text(
        "🎲 Кости\n\nВведите сумму ставки (мин. 10 ⭐):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="casino_dice")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "casino_slots")
async def casino_slots_handler(callback: CallbackQuery):
    await callback.message.edit_text(
        "🎰 Слоты\n\nВыберите сумму ставки:",
        reply_markup=get_slots_bet_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "slots_custom_bet")
async def slots_custom_bet(callback: CallbackQuery):
    pending_bets[callback.from_user.id] = "slots"
    await callback.message.edit_text(
        "🎰 Слоты\n\nВведите сумму ставки (мин. 10 ⭐):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="casino_slots")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("buy_nft_"))
async def handle_buy_nft(callback: CallbackQuery):
    try:
        nft_id = callback.data[len("buy_nft_"):]
        user_id = callback.from_user.id
        
        if nft_id not in NFT_GIFTS:
            await callback.answer("❌ Такого NFT не существует!", show_alert=True)
            return
        
        nft = NFT_GIFTS[nft_id]
        user_stars = await get_user_stars(user_id)
        
        if user_stars < nft['price']:
            await callback.answer(f"❌ Недостаточно звезд! Нужно {nft['price']} ⭐", show_alert=True)
            return
        
        # Check if user already has this NFT
        user_nfts = await get_user_nfts(user_id)
        if any(item.get('nft_type') == nft_id for item in user_nfts):
            await callback.answer("❌ У вас уже есть этот NFT!", show_alert=True)
            return
        
        success = await buy_nft(user_id, nft_id)
        if success:
            await callback.answer(f"🎉 Поздравляем! Вы купили {nft['name']}!", show_alert=True)
            await show_nft_shop_handler(callback.message)
        else:
            await callback.answer("❌ Ошибка при покупке NFT. Попробуйте позже.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in handle_buy_nft: {str(e)}")
        await callback.answer("❌ Произошла ошибка. Пожалуйста, попробуйте позже.", show_alert=True)

@dp.message(F.text.isdigit())
async def handle_mines_bet(message: Message):
    try:
        user_id = message.from_user.id
        pending_game = pending_bets.get(user_id)
        if pending_game not in ("mines", "dice", "slots"):
            return

        bet_amount = int(message.text)
        stars = await get_user_stars(user_id)
        
        if bet_amount < 10:
            await message.reply("❌ Минимальная ставка: 10 ⭐", reply_markup=get_casino_menu())
            return
            
        if bet_amount > stars:
            await message.reply("❌ Недостаточно звезд!", reply_markup=get_casino_menu())
            return
            
        if pending_game == "mines":
            pending_bets.pop(user_id, None)
            await spend_stars(user_id, bet_amount)

            import random
            mines_count = random.randint(3, 5)
            mines_positions = random.sample(range(25), mines_count)

            game_key = f"{message.message_id + 1}_{user_id}"  # Using next message ID
            mines_games[game_key] = {
                'mines': mines_positions,
                'opened': [],
                'multiplier': 1.0,
                'bet': bet_amount
            }

            await message.answer(
                f"💣 Мины\n\n"
                f"Ставка: {bet_amount} ⭐\n"
                f"Мин: {mines_count}\n\n"
                f"Выберите клетку:",
                reply_markup=get_mines_keyboard(bet_amount)
            )
            return

        if pending_game == "dice":
            pending_bets.pop(user_id, None)
            await message.answer(
                f"🎲 Кости\n\nСтавка: {bet_amount} ⭐\n\nВыберите: чёт или нечёт",
                reply_markup=get_dice_choice_keyboard(bet_amount)
            )
            return

        if pending_game == "slots":
            pending_bets.pop(user_id, None)
            await spend_stars(user_id, bet_amount)

            slots_msg = await bot.send_dice(chat_id=message.chat.id, emoji="🎰")
            value = slots_msg.dice.value

            if value == 64:
                win = bet_amount * 5
                await add_stars(user_id, win)
                await message.answer(f"🎰 Джекпот!\n✅ Вы выиграли {win} ⭐!")
            elif value in (1, 22, 43):
                win = bet_amount * 2
                await add_stars(user_id, win)
                await message.answer(f"🎰 Удачно!\n✅ Вы выиграли {win} ⭐!")
            else:
                await message.answer(f"🎰 Не повезло\n❌ Вы проиграли {bet_amount} ⭐")
            return
    except ValueError:
        await message.reply("❌ Пожалуйста, введите корректное число", reply_markup=get_casino_menu())

@dp.callback_query(F.data.startswith("dice_bet_"))
async def dice_bet_selected(callback: CallbackQuery):
    user_id = callback.from_user.id
    bet_amount = int(callback.data.split("_")[2])
    stars = await get_user_stars(user_id)

    if bet_amount > stars:
        await callback.answer("❌ Недостаточно звезд!", show_alert=True)
        return

    if bet_amount < 10:
        await callback.answer("❌ Минимальная ставка: 10 ⭐", show_alert=True)
        return

    await callback.message.edit_text(
        f"🎲 Кости\n\nСтавка: {bet_amount} ⭐\n\nВыберите: чёт или нечёт",
        reply_markup=get_dice_choice_keyboard(bet_amount)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("dice_play_"))
async def dice_play(callback: CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split("_")
    if len(parts) != 4:
        await callback.answer("❌ Ошибка!", show_alert=True)
        return

    choice = parts[2]
    bet_amount = int(parts[3])
    stars = await get_user_stars(user_id)

    if bet_amount > stars:
        await callback.answer("❌ Недостаточно звезд!", show_alert=True)
        return

    if bet_amount < 10:
        await callback.answer("❌ Минимальная ставка: 10 ⭐", show_alert=True)
        return

    await spend_stars(user_id, bet_amount)
    dice_msg = await bot.send_dice(chat_id=callback.message.chat.id, emoji="🎲")
    value = dice_msg.dice.value

    choice_even = choice == "even"
    is_even = (value % 2 == 0)
    if is_even == choice_even:
        win = bet_amount * 2
        await add_stars(user_id, win)
        await callback.message.answer(f"🎲 Выпало: {value}\n✅ Вы выиграли {win} ⭐!")
    else:
        await callback.message.answer(f"🎲 Выпало: {value}\n❌ Вы проиграли {bet_amount} ⭐")

    await callback.answer()

@dp.callback_query(F.data.startswith("slots_bet_"))
async def slots_start(callback: CallbackQuery):
    user_id = callback.from_user.id
    bet_amount = int(callback.data.split("_")[2])
    stars = await get_user_stars(user_id)

    if bet_amount > stars:
        await callback.answer("❌ Недостаточно звезд!", show_alert=True)
        return

    if bet_amount < 10:
        await callback.answer("❌ Минимальная ставка: 10 ⭐", show_alert=True)
        return

    await spend_stars(user_id, bet_amount)
    slots_msg = await bot.send_dice(chat_id=callback.message.chat.id, emoji="🎰")
    value = slots_msg.dice.value

    if value == 64:
        win = bet_amount * 5
        await add_stars(user_id, win)
        await callback.message.answer(f"🎰 Джекпот!\n✅ Вы выиграли {win} ⭐!")
    elif value in (1, 22, 43):
        win = bet_amount * 2
        await add_stars(user_id, win)
        await callback.message.answer(f"🎰 Удачно!\n✅ Вы выиграли {win} ⭐!")
    else:
        await callback.message.answer(f"🎰 Не повезло\n❌ Вы проиграли {bet_amount} ⭐")

    await callback.answer()

@dp.callback_query(F.data.startswith("mines_bet_"))
async def mines_start(callback: CallbackQuery):
    user_id = callback.from_user.id
    bet_amount = int(callback.data.split("_")[2])
    stars = await get_user_stars(user_id)
    
    if bet_amount > stars:
        await callback.answer("❌ Недостаточно звезд!", show_alert=True)
        return
    
    if bet_amount < 10:
        await callback.answer("❌ Минимальная ставка: 10 ⭐", show_alert=True)
        return
    
    await spend_stars(user_id, bet_amount)
    
    import random
    mines_count = random.randint(3, 5)
    mines_positions = random.sample(range(25), mines_count)
    
    game_key = f"{callback.message.message_id}_{user_id}"
    mines_games[game_key] = {
        'mines': mines_positions,
        'opened': [],
        'multiplier': 1.0,
        'bet': bet_amount
    }
    
    await callback.message.edit_text(
        f"💣 Мины\n\n"
        f"Ставка: {bet_amount} ⭐\n"
        f"Мин: {mines_count}\n\n"
        f"Выберите клетку:",
        reply_markup=get_mines_keyboard(bet_amount)
    )

@dp.callback_query(F.data.startswith("mine_"))
async def mines_click(callback: CallbackQuery):
    if callback.data.startswith("mine_opened_"):
        await callback.answer("❌ Эта клетка уже открыта!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    parts = callback.data.split("_")
    cell = int(parts[1])
    bet_amount = int(parts[2])
    game_key = f"{callback.message.message_id}_{user_id}"
    
    if game_key not in mines_games:
        await callback.answer("❌ Игра не найдена!", show_alert=True)
        return
    
    game = mines_games[game_key]
    mines_positions = game['mines']
    opened = game['opened']
    multiplier = game['multiplier']
    
    if cell in opened:
        await callback.answer("❌ Эта клетка уже открыта!", show_alert=True)
        return
    
    if cell in mines_positions:
        opened.append(cell)
        del mines_games[game_key]
        await callback.answer("💣 МИНА! Вы проиграли!", show_alert=True)
        await callback.message.edit_text(
            f"💣 Мины\n\n"
            f"❌ Вы наступили на мину!\n"
            f"Проиграно: {bet_amount} ⭐",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
            ])
        )
        return
    
    opened.append(cell)
    multiplier += 0.1
    game['multiplier'] = multiplier
    game['opened'] = opened
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for i in range(25):
        row = i // 5
        col = i % 5
        if col == 0:
            keyboard.inline_keyboard.append([])
        if i in opened:
            keyboard.inline_keyboard[row].append(
                InlineKeyboardButton(text="✅", callback_data=f"mine_opened_{i}_{bet_amount}")
            )
        else:
            keyboard.inline_keyboard[row].append(
                InlineKeyboardButton(text="❓", callback_data=f"mine_{i}_{bet_amount}")
            )
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text=f"💰 Забрать ({round(multiplier, 1)}x)", callback_data=f"mines_cashout_{bet_amount}"),
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
    ])
    
    await callback.message.edit_text(
        f"💣 Мины\n\n"
        f"Ставка: {bet_amount} ⭐\n"
        f"Множитель: {round(multiplier, 1)}x\n"
        f"Открыто: {len(opened)}/25\n\n"
        f"Выберите клетку:",
        reply_markup=keyboard
    )
    await callback.answer("✅ Безопасно!")

@dp.callback_query(F.data.startswith("mine_opened_"))
async def mines_opened_click(callback: CallbackQuery):
    await callback.answer("❌ Эта клетка уже открыта!", show_alert=True)

@dp.callback_query(F.data.startswith("mines_cashout_"))
async def mines_cashout(callback: CallbackQuery):
    user_id = callback.from_user.id
    bet_amount = int(callback.data.split("_")[2])
    game_key = f"{callback.message.message_id}_{user_id}"
    
    if game_key not in mines_games:
        await callback.answer("❌ Ошибка!", show_alert=True)
        return
    
    game = mines_games[game_key]
    multiplier = game['multiplier']
    win = int(bet_amount * multiplier)
    await add_stars(user_id, win)
    
    del mines_games[game_key]
    
    await callback.message.edit_text(
        f"💣 Мины\n\n"
        f"✅ Вы забрали выигрыш!\n\n"
        f"Ставка: {bet_amount} ⭐\n"
        f"Множитель: {round(multiplier, 1)}x\n"
        f"Выигрыш: {win} ⭐",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ])
    )
    await callback.answer(f"✅ Выигрыш: {win} ⭐!", show_alert=True)

@dp.callback_query(F.data == "casino_roulette")
async def casino_roulette_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "🎯 Рулетка\n\n"
        "Ставка: учетверение\n\n"
        "Отправьте сумму ставки:\n"
        "/roulette amount",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ])
    )

@dp.message(Command("roulette"))
async def cmd_roulette(message: Message):
    user_id = message.from_user.id
    if await is_banned(user_id):
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Использование: /roulette amount")
        return
    
    try:
        bet = int(args[1])
        stars = await get_user_stars(user_id)
        
        if bet < 10:
            await message.reply("❌ Минимальная ставка: 10 ⭐")
            return
        
        if bet > stars:
            await message.reply("❌ Недостаточно звезд!")
            return
        
        await spend_stars(user_id, bet)
        
        import random
        colors = ["🔴", "⚫", "🟢"]
        player_color = random.choice(colors)
        wheel_color = random.choice(colors)
        
        if player_color == wheel_color:
            multiplier = 5 if wheel_color == "🟢" else 4
            win = bet * multiplier
            await add_stars(user_id, win)
            await message.reply(
                f"🎯 Вы выбрали: {player_color}\n"
                f"🎯 Выпало: {wheel_color}\n\n"
                f"✅ Вы выиграли {win} ⭐!"
            )
        else:
            await message.reply(
                f"🎯 Вы выбрали: {player_color}\n"
                f"🎯 Выпало: {wheel_color}\n\n"
                f"❌ Вы проиграли {bet} ⭐"
            )
    except ValueError:
        await message.reply("❌ Неверный формат!")

@dp.message(F.new_chat_members)
async def on_new_member(message: Message):
    for member in message.new_chat_members:
        if member.id == bot.id:
            await add_chat(message.chat.id, message.chat.type, message.chat.title)
            welcome_text = (
                f"🌟 Добро пожаловать в {GAME_NAME}!\n\n"
                f"Я игровой бот с фермами, NFT и казино!\n\n"
                f"Используйте команды:\n"
                f"/start - Начать игру\n"
                f"/help - Справка\n"
                f"/profile - Профиль\n"
                f"/casino - Казино"
            )
            await message.reply(welcome_text)

async def health_check(request):
    return web.Response(text="OK")

async def start_http_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get('PORT', 8000)))
    await site.start()
    logger.info("HTTP сервер запущен на порту %s", os.environ.get('PORT', 8000))
    return runner

async def main():
    import os
    
    await init_db()
    logger.info("База данных инициализирована")
    
    http_runner = await start_http_server()
    
    try:
        logger.info("Бот запущен")
        await dp.start_polling(bot)
    finally:
        await http_runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())

