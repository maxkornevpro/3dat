import asyncio
import logging
import os
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, CommandStart
from config import BOT_TOKEN, FARM_TYPES, NFT_GIFTS, GAME_NAME, ADMIN_IDS
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
from keyboards import (
    get_main_menu, get_farm_shop_keyboard, 
    get_nft_shop_keyboard, get_back_keyboard, get_auction_keyboard,
    get_admin_menu, get_casino_menu, get_farm_select_keyboard, get_nft_select_keyboard,
    get_mines_keyboard
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        f"🌾 Ферм: {len(farms)} (активных: {active_farms})\n"
        f"🎁 NFT: {len(nfts)}\n"
        f"⚡ Буст к доходу: {int((boost - 1) * 100)}%\n"
        f"🔗 Рефералов: {referrals}\n\n"
    )
    
    if farms:
        profile_text += "Ваши фермы:\n"
        farm_counts = {}
        for farm in farms:
            farm_type = farm['farm_type']
            farm_counts[farm_type] = farm_counts.get(farm_type, 0) + 1
        
        for farm_type, count in farm_counts.items():
            if farm_type in FARM_TYPES:
                profile_text += f"  {FARM_TYPES[farm_type]['name']}: {count} шт.\n"
    
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
    total_income = 0
    total_active_income = 0
    
    for farm_type, data in farm_counts.items():
        if farm_type in FARM_TYPES:
            farm_data = FARM_TYPES[farm_type]
            total = data['total']
            active = data['active']
            inactive = total - active
            
            income = farm_data['income_per_hour'] * active
            total_active_income += income
            total_income += farm_data['income_per_hour'] * total
            
            income_per_min = round(income / 60, 2)
            status = "✅" if active > 0 else "❌"
            farms_text += f"{status} {farm_data['name']}: {total} шт. (активных: {active})\n"
            if active > 0:
                farms_text += f"  Доход: {income_per_min} ⭐/мин | {income} ⭐/час\n\n"
            else:
                farms_text += f"  ⚠️ Требуется активация (/activate)\n\n"
    
    boost = await calculate_total_boost(user_id)
    if boost > 1.0:
        total_income_boosted = int(total_active_income * boost)
        total_income_boosted_per_min = round(total_income_boosted / 60, 2)
        farms_text += f"📊 Доход (активные): {round(total_active_income / 60, 2)} ⭐/мин | {total_active_income} ⭐/час\n"
        farms_text += f"⚡ С бустом: {total_income_boosted_per_min} ⭐/мин | {total_income_boosted} ⭐/час\n"
    else:
        total_income_per_min = round(total_active_income / 60, 2)
        farms_text += f"📊 Доход (активные): {total_income_per_min} ⭐/мин | {total_active_income} ⭐/час\n"
    
    if inactive_count > 0:
        farms_text += f"\n⚠️ {inactive_count} ферм требуют активации! Используйте /activate"
    
    if message.chat.type == "private":
        await message.answer(farms_text)
    else:
        await message.reply(farms_text)

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

@dp.message(Command("collect"))
async def cmd_collect(message: Message):
    await collect_income_handler(message)

@dp.message(F.text == "💰 Собрать доход")
async def collect_income(message: Message):
    await collect_income_handler(message)

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
    
    income = await collect_farm_income(user_id)
    stars = await get_user_stars(user_id)
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
        
        response = (
            f"💰 Вы собрали доход!\n\n"
            f"⭐ Получено: {income} звезд{boost_text}\n"
            f"💎 Всего звезд: {stars}\n\n"
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

@dp.callback_query(F.data.startswith("buy_nft_"))
async def handle_buy_nft(callback: CallbackQuery):
    nft_id = callback.data.split("_")[2]
    
    if nft_id not in NFT_GIFTS:
        await callback.answer("Ошибка: неверный тип NFT", show_alert=True)
        return
    
    user_id = callback.from_user.id
    nft_data = NFT_GIFTS[nft_id]
    
    success = await buy_nft(user_id, nft_id)
    
    if success:
        stars = await get_user_stars(user_id)
        boost = await calculate_total_boost(user_id)
        boost_text = f"+{int((nft_data['boost'] - 1) * 100)}%"
        
        await callback.answer(
            f"✅ Вы купили {nft_data['name']}! Буст: {boost_text}",
            show_alert=True
        )
        
        shop_text = (
            f"🎁 Магазин NFT подарков\n\n"
            f"⭐ Ваши звезды: {stars}\n\n"
            f"✅ Вы купили {nft_data['name']}!\n"
            f"⚡ Общий буст: {int((boost - 1) * 100)}%\n\n"
        )
        
        for nft_id_item, nft_data_item in NFT_GIFTS.items():
            boost_item_text = f"+{int((nft_data_item['boost'] - 1) * 100)}%"
            shop_text += (
                f"{nft_data_item['name']}\n"
                f"💰 Цена: {nft_data_item['price']} ⭐\n"
                f"⚡ Буст: {boost_item_text}\n\n"
            )
        
        await callback.message.edit_text(shop_text, reply_markup=get_nft_shop_keyboard())
    else:
        stars = await get_user_stars(user_id)
        await callback.answer(
            f"❌ Недостаточно звезд! Нужно {nft_data['price']}, у вас {stars}",
            show_alert=True
        )

@dp.message(Command("referral"))
async def cmd_referral(message: Message):
    await show_referral_link_handler(message)

@dp.message(F.text == "🔗 Реферальная ссылка")
async def show_referral_link(message: Message):
    await show_referral_link_handler(message)

async def show_referral_link_handler(message: Message):
    user_id = message.from_user.id
    referrals = await get_referral_count(user_id)
    
    from config import REFERRAL_REWARD
    bot_username = (await bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start={user_id}"
    
    referral_text = (
        f"🔗 Ваша реферальная ссылка:\n\n"
        f"{referral_link}\n\n"
        f"💰 За каждого приглашенного друга вы получаете награду!\n"
        f"🎁 Новый пользователь получает {REFERRAL_REWARD} ⭐\n\n"
        f"👥 Приглашено друзей: {referrals}"
    )
    
    if message.chat.type == "private":
        await message.answer(referral_text)
    else:
        await message.reply(referral_text)

@dp.message(Command("top"))
async def cmd_top(message: Message):
    top_balance = await get_top_by_balance(5)
    top_income = await get_top_by_income_per_minute(5)
    top_nft = await get_top_by_nft_count(5)
    
    top_text = "🏆 ТОП ИГРОКОВ\n\n"
    
    top_text += "💰 ТОП ПО БАЛАНСУ:\n"
    for idx, user in enumerate(top_balance, 1):
        try:
            tg_user = await bot.get_chat(user['user_id'])
            username = f"@{tg_user.username}" if tg_user.username else tg_user.full_name or "Неизвестно"
        except:
            username = "Неизвестно"
        medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][idx - 1]
        top_text += f"{medal} {username} - {user['stars']} ⭐\n"
    
    top_text += "\n📈 ТОП ПО ДОХОДУ В МИНУТУ:\n"
    for idx, user in enumerate(top_income, 1):
        try:
            tg_user = await bot.get_chat(user['user_id'])
            username = f"@{tg_user.username}" if tg_user.username else tg_user.full_name or "Неизвестно"
        except:
            username = "Неизвестно"
        medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][idx - 1]
        income = round(user['income_per_minute'], 2)
        top_text += f"{medal} {username} - {income} ⭐/мин\n"
    
    top_text += "\n🎁 ТОП ПО КОЛИЧЕСТВУ NFT:\n"
    for idx, user in enumerate(top_nft, 1):
        try:
            tg_user = await bot.get_chat(user['user_id'])
            username = f"@{tg_user.username}" if tg_user.username else tg_user.full_name or "Неизвестно"
        except:
            username = "Неизвестно"
        medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][idx - 1]
        top_text += f"{medal} {username} - {user['nft_count']} NFT\n"
    
    if message.chat.type == "private":
        await message.answer(top_text)
    else:
        await message.reply(top_text)

@dp.message(Command("auction"))
async def cmd_auction(message: Message):
    await show_auctions_handler(message)

@dp.message(F.text == "🔨 Аукцион")
async def show_auctions(message: Message):
    await show_auctions_handler(message)

async def show_auctions_handler(message: Message):
    user_id = message.from_user.id
    
    from datetime import datetime
    active_auctions = await get_active_auctions()
    for auction in active_auctions:
        end_time = datetime.fromisoformat(auction['end_time'])
        if datetime.now() >= end_time:
            await end_auction(auction['id'])
    
    auctions = await get_active_auctions()
    
    if not auctions:
        from random import choice
        
        farm_types = list(FARM_TYPES.keys())[-4:]
        for i in range(3):
            farm_type = choice(farm_types)
            farm_data = FARM_TYPES[farm_type]
            starting_price = farm_data['price'] // 2
            await create_auction(farm_type, starting_price, 24)
        
        auctions = await get_active_auctions()
    
    if not auctions:
        response = "Сейчас нет активных аукционов. Попробуйте позже!"
        if message.chat.type == "private":
            await message.answer(response)
        else:
            await message.reply(response)
        return
    
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    auctions_text = "🔨 Активные аукционы:\n\n"
    keyboard_buttons = []
    
    for auction in auctions:
        farm_type = auction['farm_type']
        if farm_type in FARM_TYPES:
            farm_data = FARM_TYPES[farm_type]
            end_time = datetime.fromisoformat(auction['end_time'])
            time_left = end_time - datetime.now()
            hours_left = int(time_left.total_seconds() / 3600)
            minutes_left = int((time_left.total_seconds() % 3600) / 60)
            
            auctions_text += (
                f"{farm_data['name']}\n"
                f"💰 Текущая ставка: {auction['current_bid']} ⭐\n"
                f"⏰ Осталось: {hours_left}ч {minutes_left}м\n\n"
            )
            
            keyboard_buttons.append([
                InlineKeyboardButton(
                    text=f"{farm_data['name']} - {auction['current_bid']} ⭐",
                    callback_data=f"auction_{auction['id']}"
                )
            ])
    
    keyboard_buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    if message.chat.type == "private":
        await message.answer(auctions_text, reply_markup=keyboard)
    else:
        await message.reply(auctions_text + "\n💡 В группах используйте команды для участия в аукционах")

@dp.callback_query(F.data.startswith("auction_"))
async def handle_auction_select(callback: CallbackQuery):
    auction_id = int(callback.data.split("_")[1])
    
    auctions = await get_active_auctions()
    auction = next((a for a in auctions if a['id'] == auction_id), None)
    
    if not auction:
        await callback.answer("Аукцион не найден или уже завершен", show_alert=True)
        return
    
    from datetime import datetime
    farm_type = auction['farm_type']
    if farm_type in FARM_TYPES:
        farm_data = FARM_TYPES[farm_type]
        end_time = datetime.fromisoformat(auction['end_time'])
        time_left = end_time - datetime.now()
        hours_left = int(time_left.total_seconds() / 3600)
        minutes_left = int((time_left.total_seconds() % 3600) / 60)
        
        auction_text = (
            f"🔨 Аукцион: {farm_data['name']}\n\n"
            f"💰 Текущая ставка: {auction['current_bid']} ⭐\n"
            f"⏰ Осталось: {hours_left}ч {minutes_left}м\n\n"
            f"Выберите размер ставки:"
        )
        await callback.message.edit_text(auction_text, reply_markup=get_auction_keyboard(auction_id, auction['current_bid']))

@dp.callback_query(F.data.startswith("bid_"))
async def handle_bid(callback: CallbackQuery):
    parts = callback.data.split("_")
    auction_id = int(parts[1])
    bid_amount = int(parts[2])
    
    user_id = callback.from_user.id
    success, message_text = await place_bid(auction_id, user_id, bid_amount)
    
    if success:
        await callback.answer(f"✅ {message_text}", show_alert=True)
        auctions = await get_active_auctions()
        auction = next((a for a in auctions if a['id'] == auction_id), None)
        if auction:
            from datetime import datetime
            farm_type = auction['farm_type']
            if farm_type in FARM_TYPES:
                farm_data = FARM_TYPES[farm_type]
                end_time = datetime.fromisoformat(auction['end_time'])
                time_left = end_time - datetime.now()
                hours_left = int(time_left.total_seconds() / 3600)
                minutes_left = int((time_left.total_seconds() % 3600) / 60)
                
                auction_text = (
                    f"🔨 Аукцион: {farm_data['name']}\n\n"
                    f"💰 Текущая ставка: {auction['current_bid']} ⭐\n"
                    f"⏰ Осталось: {hours_left}ч {minutes_left}м\n\n"
                    f"✅ Ваша ставка принята!\n\n"
                    f"Выберите размер следующей ставки:"
                )
                await callback.message.edit_text(auction_text, reply_markup=get_auction_keyboard(auction_id, auction['current_bid']))
    else:
        await callback.answer(f"❌ {message_text}", show_alert=True)

@dp.callback_query(F.data == "back_to_main")
async def handle_back(callback: CallbackQuery):
    await callback.answer()
    await callback.message.delete()

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к админ панели!")
        return
    
    admin_text = (
        "🔐 Админ панель\n\n"
        "Выберите действие:"
    )
    await message.answer(admin_text, reply_markup=get_admin_menu())

@dp.message(Command("ahelp"))
async def cmd_ahelp(message: Message):
    try:
        user_id = message.from_user.id
        
        if user_id not in ADMIN_IDS:
            await message.answer("❌ У вас нет доступа к админ панели!")
            return
        
        help_text = (
            "🔐 Справка по админским командам\n\n"
            "📋 Основные команды:\n"
            "• /admin - Открыть админ панель с кнопками\n"
            "• /ahelp - Показать эту справку\n\n"
            "💰 Управление ресурсами:\n"
            "• /give_stars internal_id amount - Выдать звезды пользователю\n"
            "  Пример: /give_stars 1 1000\n\n"
            "• /give_farm farm_id internal_id - Выдать ферму пользователю\n"
            "  Пример: /give_farm starter 1\n"
            "  Доступные типы: starter, basic, advanced, premium, elite, legendary, mythic, ultimate, quantum, cosmic, divine, infinity\n\n"
            "• /give_nft nft_id internal_id - Выдать NFT пользователю\n"
            "  Пример: /give_nft snoop_dogg 1\n"
            "  Доступные NFT: snoop_dogg, lunar_snake, crystal_ball, golden_coin, diamond_ring, magic_lamp, fire_dragon, cosmic_star, golden_crown, mystic_orb\n\n"
            "🚫 Управление пользователями:\n"
            "• /ban internal_id [причина] - Забанить пользователя\n"
            "  Пример: /ban 1 Нарушение правил\n"
            "  Пример: /ban 1 (без причины)\n\n"
            "• /unban internal_id - Разбанить пользователя\n"
            "  Пример: /unban 1\n\n"
            "👤 Просмотр профиля:\n"
            "• /profile_id internal_id - Показать профиль пользователя\n"
            "  Пример: /profile_id 1\n\n"
            "📢 Рассылка:\n"
            "• /broadcast - Рассылка всем пользователям и чатам\n"
            "  Использование: Ответьте на сообщение командой /broadcast\n"
            "  Отправит текст сообщения всем пользователям и чатам\n\n"
            "💡 Примечание: Все команды доступны только админам!"
        )
        
        if message.chat.type == "private":
            await message.answer(help_text)
        else:
            await message.reply(help_text)
    except Exception as e:
        logger.error(f"Ошибка в /ahelp: {e}")

@dp.callback_query(F.data == "admin_help")
async def admin_help_callback(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    
    help_text = (
        "🔐 Справка по админским командам\n\n"
        "📋 Основные команды:\n"
        "• /admin - Открыть админ панель с кнопками\n"
        "• /ahelp - Показать эту справку\n\n"
        "💰 Управление ресурсами:\n"
        "• /give_stars user_id amount - Выдать звезды пользователю\n"
        "  Пример: /give_stars 123456789 1000\n\n"
        "• /give_farm farm_id user_id - Выдать ферму пользователю\n"
        "  Пример: /give_farm starter 123456789\n"
        "  Доступные типы: starter, basic, advanced, premium, elite, legendary, mythic, ultimate, quantum, cosmic, divine, infinity\n\n"
        "• /give_nft nft_id user_id - Выдать NFT пользователю\n"
        "  Пример: /give_nft snoop_dogg 123456789\n"
        "  Доступные NFT: snoop_dogg, lunar_snake, crystal_ball, golden_coin, diamond_ring, magic_lamp, fire_dragon, cosmic_star, golden_crown, mystic_orb\n\n"
        "🚫 Управление пользователями:\n"
        "• /ban user_id [причина] - Забанить пользователя\n"
        "  Пример: /ban 123456789 Нарушение правил\n"
        "  Пример: /ban 123456789 (без причины)\n\n"
        "• /unban user_id - Разбанить пользователя\n"
        "  Пример: /unban 123456789\n\n"
        "📢 Рассылка:\n"
        "• /broadcast - Рассылка всем пользователям и чатам\n"
        "  Использование: Ответьте на сообщение командой /broadcast\n"
        "  Отправит текст сообщения всем пользователям и чатам\n\n"
        "💡 Примечание: Все команды доступны только админам!"
    )
    
    await callback.message.edit_text(help_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
    ]))

@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    await callback.message.edit_text("🔐 Админ панель\n\nВыберите действие:", reply_markup=get_admin_menu())

@dp.callback_query(F.data == "admin_give_stars")
async def admin_give_stars_handler(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    await callback.message.edit_text(
        "💰 Выдача звезд\n\n"
        "Отправьте в формате:\n"
        "<code>/give_stars user_id amount</code>\n\n"
        "Пример: /give_stars 123456789 1000",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
        ])
    )

@dp.message(Command("give_stars"))
async def cmd_give_stars(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /give_stars internal_id amount\nПример: /give_stars 1 1000")
        return
    
    try:
        internal_id = int(args[1])
        amount = int(args[2])
        user = await get_user_by_internal_id(internal_id)
        if not user:
            await message.reply(f"❌ Пользователь с ID {internal_id} не найден!")
            return
        user_id = user['user_id']
        await admin_add_stars(user_id, amount)
        await message.reply(f"✅ Пользователю ID {internal_id} (TG: {user_id}) выдано {amount} ⭐")
    except ValueError:
        await message.reply("❌ Неверный формат! Используйте: /give_stars internal_id amount")

@dp.callback_query(F.data == "admin_give_farm")
async def admin_give_farm_handler(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    await callback.message.edit_text(
        "🌾 Выдача фермы\n\n"
        "Выберите тип фермы:",
        reply_markup=get_farm_select_keyboard()
    )

@dp.callback_query(F.data.startswith("admin_farm_"))
async def admin_give_farm_select(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    
    farm_id = callback.data.split("_")[2]
    await callback.message.edit_text(
        f"🌾 Выдача фермы\n\n"
        f"Тип: {FARM_TYPES[farm_id]['name']}\n\n"
        f"Отправьте ID пользователя:\n"
        f"<code>/give_farm {farm_id} user_id</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_give_farm")]
        ])
    )

@dp.message(Command("give_farm"))
async def cmd_give_farm(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /give_farm farm_id internal_id\nПример: /give_farm starter 1")
        return
    
    try:
        farm_id = args[1]
        internal_id = int(args[2])
        user = await get_user_by_internal_id(internal_id)
        if not user:
            await message.reply(f"❌ Пользователь с ID {internal_id} не найден!")
            return
        user_id = user['user_id']
        if farm_id not in FARM_TYPES:
            await message.reply("❌ Неверный тип фермы!")
            return
        await admin_add_farm(user_id, farm_id)
        await message.reply(f"✅ Пользователю ID {internal_id} (TG: {user_id}) выдана {FARM_TYPES[farm_id]['name']}")
    except ValueError:
        await message.reply("❌ Неверный формат! Используйте: /give_farm farm_id internal_id")

@dp.callback_query(F.data == "admin_give_nft")
async def admin_give_nft_handler(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    await callback.message.edit_text(
        "🎁 Выдача NFT\n\n"
        "Выберите NFT:",
        reply_markup=get_nft_select_keyboard()
    )

@dp.callback_query(F.data.startswith("admin_nft_"))
async def admin_give_nft_select(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    
    nft_id = callback.data.split("_")[2]
    await callback.message.edit_text(
        f"🎁 Выдача NFT\n\n"
        f"Тип: {NFT_GIFTS[nft_id]['name']}\n\n"
        f"Отправьте ID пользователя:\n"
        f"<code>/give_nft {nft_id} user_id</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_give_nft")]
        ])
    )

@dp.message(Command("give_nft"))
async def cmd_give_nft(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = message.text.split()
    if len(args) < 3:
        await message.reply("Использование: /give_nft nft_id internal_id\nПример: /give_nft snoop_dogg 1")
        return
    
    try:
        nft_id = args[1]
        internal_id = int(args[2])
        user = await get_user_by_internal_id(internal_id)
        if not user:
            await message.reply(f"❌ Пользователь с ID {internal_id} не найден!")
            return
        user_id = user['user_id']
        if nft_id not in NFT_GIFTS:
            await message.reply("❌ Неверный тип NFT!")
            return
        await admin_add_nft(user_id, nft_id)
        await message.reply(f"✅ Пользователю ID {internal_id} (TG: {user_id}) выдано {NFT_GIFTS[nft_id]['name']}")
    except ValueError:
        await message.reply("❌ Неверный формат! Используйте: /give_nft nft_id internal_id")

@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        await message.reply("Использование: /ban internal_id [причина]\nПример: /ban 1 Нарушение правил")
        return
    
    try:
        internal_id = int(args[1])
        reason = args[2] if len(args) > 2 else "Нарушение правил"
        user = await get_user_by_internal_id(internal_id)
        if not user:
            await message.reply(f"❌ Пользователь с ID {internal_id} не найден!")
            return
        user_id = user['user_id']
        await ban_user(user_id, reason, message.from_user.id)
        await message.reply(f"✅ Пользователь ID {internal_id} (TG: {user_id}) забанен. Причина: {reason}")
    except ValueError:
        await message.reply("❌ Неверный формат! Используйте: /ban internal_id [причина]")

@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Использование: /unban internal_id\nПример: /unban 1")
        return
    
    try:
        internal_id = int(args[1])
        user = await get_user_by_internal_id(internal_id)
        if not user:
            await message.reply(f"❌ Пользователь с ID {internal_id} не найден!")
            return
        user_id = user['user_id']
        await unban_user(user_id)
        await message.reply(f"✅ Пользователь ID {internal_id} (TG: {user_id}) разбанен")
    except ValueError:
        await message.reply("❌ Неверный формат! Используйте: /unban internal_id")

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    if not message.reply_to_message:
        await message.reply("Ответьте на сообщение для рассылки")
        return
    
    text = message.reply_to_message.text or message.reply_to_message.caption
    if not text:
        await message.reply("Сообщение должно содержать текст")
        return
    
    users = await get_all_users()
    chats = await get_all_chats()
    
    sent = 0
    failed = 0
    
    await message.reply(f"📢 Начинаю рассылку...\nПользователей: {len(users)}\nЧатов: {len(chats)}")
    
    for user in users:
        try:
            await bot.send_message(user['user_id'], text)
            sent += 1
        except:
            failed += 1
    
    for chat in chats:
        try:
            await bot.send_message(chat['chat_id'], text)
            sent += 1
        except:
            failed += 1
    
    await message.reply(f"✅ Рассылка завершена!\nОтправлено: {sent}\nОшибок: {failed}")

@dp.message(F.text == "🎰 Казино")
async def show_casino(message: Message):
    user_id = message.from_user.id
    stars = await get_user_stars(user_id)
    casino_text = (
        f"🎰 Казино\n\n"
        f"⭐ Ваши звезды: {stars}\n\n"
        f"Выберите игру:"
    )
    await message.answer(casino_text, reply_markup=get_casino_menu())

@dp.callback_query(F.data == "casino_dice")
async def casino_dice(callback: CallbackQuery):
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "🎲 Кости\n\n"
        "Ставка: удвоение\n\n"
        "Отправьте сумму ставки:\n"
        "/dice amount",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ])
    )

@dp.message(Command("dice"))
async def cmd_dice(message: Message):
    user_id = message.from_user.id
    if await is_banned(user_id):
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.reply("Использование: /dice amount")
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
        player_dice = random.randint(1, 6)
        bot_dice = random.randint(1, 6)
        
        win_chance = random.random()
        if win_chance < 0.45:
            win = bet * 2
            await add_stars(user_id, win)
            await message.reply(
                f"🎲 Вы: {player_dice}\n"
                f"🎲 Бот: {bot_dice}\n\n"
                f"✅ Вы выиграли {win} ⭐!"
            )
        else:
            await message.reply(
                f"🎲 Вы: {player_dice}\n"
                f"🎲 Бот: {bot_dice}\n\n"
                f"❌ Вы проиграли {bet} ⭐"
            )
    except ValueError:
        await message.reply("❌ Неверный формат!")

@dp.callback_query(F.data == "casino_mines")
async def casino_mines_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    stars = await get_user_stars(user_id)
    await callback.message.edit_text(
        "💣 Мины\n\n"
        "Выберите сумму ставки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="10 ⭐", callback_data="mines_bet_10")],
            [InlineKeyboardButton(text="50 ⭐", callback_data="mines_bet_50")],
            [InlineKeyboardButton(text="100 ⭐", callback_data="mines_bet_100")],
            [InlineKeyboardButton(text="500 ⭐", callback_data="mines_bet_500")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ])
    )

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

