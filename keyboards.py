from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from config import FARM_TYPES, NFT_GIFTS

def get_main_menu():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⭐ Мой профиль"), KeyboardButton(text="🌾 Мои фермы")],
            [KeyboardButton(text="🛒 Магазин ферм"), KeyboardButton(text="🎁 Магазин NFT")],
            [KeyboardButton(text="💰 Собрать доход"), KeyboardButton(text="🔗 Реферальная ссылка")],
            [KeyboardButton(text="🔨 Аукцион"), KeyboardButton(text="🎰 Казино")]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_farm_shop_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    for farm_id, farm_data in FARM_TYPES.items():
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{farm_data['name']} - {farm_data['price']}⭐ ({farm_data['income_per_hour']}⭐/час)",
                callback_data=f"buy_farm_{farm_id}"
            )
        ])
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
    ])
    
    return keyboard

def get_nft_shop_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    
    for nft_id, nft_data in NFT_GIFTS.items():
        boost_text = f"+{int((nft_data['boost'] - 1) * 100)}%"
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{nft_data['name']} - {nft_data['price']}⭐ ({boost_text})",
                callback_data=f"buy_nft_{nft_id}"
            )
        ])
    
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
    ])
    
    return keyboard

def get_back_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])
    return keyboard

def get_auction_keyboard(auction_id: int, current_bid: int):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=f"💰 Ставка: {current_bid + 100} ⭐",
                callback_data=f"bid_{auction_id}_{current_bid + 100}"
            )
        ],
        [
            InlineKeyboardButton(
                text=f"💰 Ставка: {current_bid + 500} ⭐",
                callback_data=f"bid_{auction_id}_{current_bid + 500}"
            )
        ],
        [
            InlineKeyboardButton(
                text=f"💰 Ставка: {current_bid + 1000} ⭐",
                callback_data=f"bid_{auction_id}_{current_bid + 1000}"
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])
    return keyboard

def get_admin_menu():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Справка (/ahelp)", callback_data="admin_help")],
        [InlineKeyboardButton(text="👤 Управление пользователями", callback_data="admin_users")],
        [InlineKeyboardButton(text="💰 Выдать звезды", callback_data="admin_give_stars")],
        [InlineKeyboardButton(text="🌾 Выдать ферму", callback_data="admin_give_farm")],
        [InlineKeyboardButton(text="🎁 Выдать NFT", callback_data="admin_give_nft")],
        [InlineKeyboardButton(text="🚫 Забанить", callback_data="admin_ban")],
        [InlineKeyboardButton(text="✅ Разбанить", callback_data="admin_unban")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")]
    ])
    return keyboard

def get_casino_menu():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎲 Кости (x2)", callback_data="casino_dice")],
        [InlineKeyboardButton(text="💣 Мины", callback_data="casino_mines")],
        [InlineKeyboardButton(text="🎯 Рулетка (x5)", callback_data="casino_roulette")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])
    return keyboard

def get_mines_keyboard(bet_amount: int):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for i in range(25):
        row = i // 5
        col = i % 5
        if col == 0:
            keyboard.inline_keyboard.append([])
        keyboard.inline_keyboard[row].append(
            InlineKeyboardButton(text="❓", callback_data=f"mine_{i}_{bet_amount}")
        )
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="💰 Забрать", callback_data=f"mines_cashout_{bet_amount}"),
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
    ])
    return keyboard

def get_farm_select_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for farm_id, farm_data in FARM_TYPES.items():
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=farm_data['name'],
                callback_data=f"admin_farm_{farm_id}"
            )
        ])
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")
    ])
    return keyboard

def get_nft_select_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for nft_id, nft_data in NFT_GIFTS.items():
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=nft_data['name'],
                callback_data=f"admin_nft_{nft_id}"
            )
        ])
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")
    ])
    return keyboard
