BOT_TOKEN = "8255377913:AAFlkYfXZeqi-vxSbOLHAKmZ6qkZTaBDwrw"

GAME_NAME = "0DAY FARM EMPIRE"
INITIAL_STARS = 200
FARM_BASE_PRICE = 50
FARM_BASE_INCOME = 5

ADMIN_IDS = [5538590798, 891015442, 5253753886, 1246190987]

NFT_GIFTS = {
    "snoop_dogg": {
        "name": "🎤 Snoop Dogg",
        "price": 9000,
        "boost": 1.25,        "gift_id": "snoop_dogg"
    },
    "lunar_snake": {
        "name": "🐍 Lunar Snake",
        "price": 6500,
        "boost": 1.15,        "gift_id": "lunar_snake"
    },
    "crystal_ball": {
        "name": "🔮 Crystal Ball",
        "price": 11000,
        "boost": 1.3,        "gift_id": "crystal_ball"
    },
    "golden_coin": {
        "name": "🪙 Golden Coin",
        "price": 5500,
        "boost": 1.12,        "gift_id": "golden_coin"
    },
    "diamond_ring": {
        "name": "💍 Diamond Ring",
        "price": 18000,
        "boost": 1.4,        "gift_id": "diamond_ring"
    },
    "magic_lamp": {
        "name": "🪔 Magic Lamp",
        "price": 14000,
        "boost": 1.32,        "gift_id": "magic_lamp"
    },
    "fire_dragon": {
        "name": "🐉 Fire Dragon",
        "price": 22000,
        "boost": 1.45,        "gift_id": "fire_dragon"
    },
    "cosmic_star": {
        "name": "⭐ Cosmic Star",
        "price": 15000,
        "boost": 1.35,        "gift_id": "cosmic_star"
    },
    "golden_crown": {
        "name": "👑 Golden Crown",
        "price": 28000,
        "boost": 1.6,        "gift_id": "golden_crown"
    },
    "mystic_orb": {
        "name": "🔮 Mystic Orb",
        "price": 17000,
        "boost": 1.38,        "gift_id": "mystic_orb"
    }
}

REFERRAL_REWARD = 100

CRYSTAL_SHOP = {
    "stars_500": {
        "name": "⭐ 500 звезд",
        "price": 5,
        "stars": 500
    },
    "stars_2000": {
        "name": "⭐ 2000 звезд",
        "price": 15,
        "stars": 2000
    },
    "stars_10000": {
        "name": "⭐ 10000 звезд",
        "price": 60,
        "stars": 10000
    }
}

CRYSTAL_CASES = {
    "bronze": {
        "name": "🥉 Бронзовый кейс",
        "price": 3,
        "rewards": [
            {"type": "stars", "amount": 1200, "weight": 60},
            {"type": "stars", "amount": 3000, "weight": 25},
            {"type": "crystals", "amount": 1, "weight": 15},
            {"type": "item", "item_key": "prefix_rookie", "qty": 1, "weight": 3},
            {"type": "item", "item_key": "collectible_chip", "qty": 1, "weight": 2},
            {"type": "nft", "weight": 5}
        ]
    },
    "silver": {
        "name": "🥈 Серебряный кейс",
        "price": 8,
        "rewards": [
            {"type": "stars", "amount": 6000, "weight": 55},
            {"type": "stars", "amount": 15000, "weight": 25},
            {"type": "stars", "amount": 35000, "weight": 10},
            {"type": "crystals", "amount": 3, "weight": 10},
            {"type": "item", "item_key": "prefix_veteran", "qty": 1, "weight": 4},
            {"type": "item", "item_key": "collectible_relic", "qty": 1, "weight": 3},
            {"type": "nft", "weight": 5}
        ]
    },
    "gold": {
        "name": "🥇 Золотой кейс",
        "price": 20,
        "rewards": [
            {"type": "stars", "amount": 30000, "weight": 50},
            {"type": "stars", "amount": 100000, "weight": 25},
            {"type": "stars", "amount": 250000, "weight": 10},
            {"type": "crystals", "amount": 7, "weight": 10},
            {"type": "item", "item_key": "prefix_legend", "qty": 1, "weight": 6},
            {"type": "item", "item_key": "collectible_artifact", "qty": 1, "weight": 4},
            {"type": "nft", "weight": 10}
        ]
    }
}

CASE_ITEMS = {
    "prefix_rookie": {"name": "[ROOKIE]", "type": "prefix"},
    "prefix_veteran": {"name": "[VETERAN]", "type": "prefix"},
    "prefix_legend": {"name": "[LEGEND]", "type": "prefix"},
    "collectible_chip": {"name": "🧩 Чип коллекционера", "type": "collectible"},
    "collectible_relic": {"name": "🗿 Реликвия", "type": "collectible"},
    "collectible_artifact": {"name": "🏺 Артефакт", "type": "collectible"}
}

CONTESTS = [
    {
        "title": "Войди в ТОП-3 по балансу",
        "description": "Займи место в ТОП-3 игроков по ⭐ и получи награду.",
        "reward": "NFT (выдаётся админом) за 300 ⭐ (условно)",
        "how_to": "Проверь /top и напиши в поддержку, если ты в ТОП-3."
    },
    {
        "title": "Собери коллекцию",
        "description": "Собери предметы из кейсов и получи уникальный префикс.",
        "reward": "Префикс [COLLECTOR] (выдаётся админом)",
        "how_to": "Покажи /inventory в поддержку."
    }
]

FARM_TYPES = {
    "starter": {
        "name": "🌱 Стартовая ферма",
        "price": 200,
        "income_per_hour": 60
    },
    "basic": {
        "name": "🌾 Базовая ферма",
        "price": 500,
        "income_per_hour": 240
    },
    "advanced": {
        "name": "🚜 Продвинутая ферма",
        "price": 2000,
        "income_per_hour": 1200
    },
    "premium": {
        "name": "🏭 Премиум ферма",
        "price": 8000,
        "income_per_hour": 5400
    },
    "elite": {
        "name": "💎 Элитная ферма",
        "price": 25000,
        "income_per_hour": 18000
    },
    "legendary": {
        "name": "👑 Легендарная ферма",
        "price": 75000,
        "income_per_hour": 60000
    },
    "mythic": {
        "name": "🌟 Мифическая ферма",
        "price": 200000,
        "income_per_hour": 180000
    },
    "ultimate": {
        "name": "⚡ Ультимативная ферма",
        "price": 500000,
        "income_per_hour": 450000
    },
    "quantum": {
        "name": "⚛️ Квантовая ферма",
        "price": 1000000,
        "income_per_hour": 900000
    },
    "cosmic": {
        "name": "🌌 Космическая ферма",
        "price": 2500000,
        "income_per_hour": 2250000
    },
    "divine": {
        "name": "✨ Божественная ферма",
        "price": 5000000,
        "income_per_hour": 4500000
    },
    "infinity": {
        "name": "♾️ Бесконечная ферма",
        "price": 10000000,
        "income_per_hour": 9000000
    }
}

