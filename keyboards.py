from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from config import FARM_TYPES, NFT_GIFTS

def get_main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⭐ Мой профиль"), KeyboardButton(text="🌾 Мои фермы")],
            [KeyboardButton(text="🛒 Магазин ферм"), KeyboardButton(text="🎁 Магазин NFT")],
            [KeyboardButton(text="💰 Собрать доход"), KeyboardButton(text="⚡ Активировать фермы")],
            [KeyboardButton(text="🎁 Кейсы"), KeyboardButton(text="🔗 Реферальная ссылка")],
            [KeyboardButton(text="🔨 Аукцион"), KeyboardButton(text="🎰 Казино")]
        ],
        resize_keyboard=True
    )

def get_farm_shop_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for farm_id, farm in FARM_TYPES.items():
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{farm['name']} - {farm['price']}⭐ ({farm['income_per_hour']}⭐/час)",
                callback_data=f"buy_farm_{farm_id}"
            )
        ])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return keyboard

def get_nft_shop_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for nft_id, nft in NFT_GIFTS.items():
        boost = int((nft["boost"] - 1) * 100)
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{nft['name']} - {nft['price']}⭐ (+{boost}%)",
                callback_data=f"buy_nft_{nft_id}"
            )
        ])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return keyboard

def get_casino_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💣 Мины", callback_data="casino_mines")],
        [InlineKeyboardButton(text="🎲 Кости", callback_data="casino_dice")],
        [InlineKeyboardButton(text="🎰 Слоты", callback_data="casino_slots")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])

def get_dice_choice_keyboard(bet_amount: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Чёт", callback_data=f"dice_play_even_{bet_amount}"),
            InlineKeyboardButton(text="Нечёт", callback_data=f"dice_play_odd_{bet_amount}")
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="casino_dice")]
    ])

def get_dice_bet_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="10 ⭐", callback_data="dice_bet_10")],
        [InlineKeyboardButton(text="50 ⭐", callback_data="dice_bet_50")],
        [InlineKeyboardButton(text="100 ⭐", callback_data="dice_bet_100")],
        [InlineKeyboardButton(text="500 ⭐", callback_data="dice_bet_500")],
        [InlineKeyboardButton(text="🎲 Своя ставка", callback_data="dice_custom_bet")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])

def get_slots_bet_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="10 ⭐", callback_data="slots_bet_10")],
        [InlineKeyboardButton(text="50 ⭐", callback_data="slots_bet_50")],
        [InlineKeyboardButton(text="100 ⭐", callback_data="slots_bet_100")],
        [InlineKeyboardButton(text="500 ⭐", callback_data="slots_bet_500")],
        [InlineKeyboardButton(text="🎲 Своя ставка", callback_data="slots_custom_bet")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])

def get_mines_bet_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="10 ⭐", callback_data="mines_bet_10")],
        [InlineKeyboardButton(text="50 ⭐", callback_data="mines_bet_50")],
        [InlineKeyboardButton(text="100 ⭐", callback_data="mines_bet_100")],
        [InlineKeyboardButton(text="500 ⭐", callback_data="mines_bet_500")],
        [InlineKeyboardButton(text="🎲 Своя ставка", callback_data="mines_custom_bet")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])

def get_mines_keyboard(bet_amount):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for i in range(25):
        if i % 5 == 0:
            keyboard.inline_keyboard.append([])
        keyboard.inline_keyboard[-1].append(
            InlineKeyboardButton(text="❓", callback_data=f"mine_{i}_{bet_amount}")
        )
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text=f"💰 Забрать", callback_data=f"mines_cashout_{bet_amount}")
    ])
    return keyboard
