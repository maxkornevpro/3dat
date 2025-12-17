BOT_TOKEN = "8255377913:AAHRb_HxFImYUx_kGhN6tmTZkw0zYySZAKc"

GAME_NAME = "0DAY FARM EMPIRE"
INITIAL_STARS = 200
FARM_BASE_PRICE = 50
FARM_BASE_INCOME = 5

ADMIN_IDS = [5538590798, 891015442, 5253753886, 1246190987]

NFT_MARKET_FEE_PCT = 0.07

ACHIEVEMENTS = [
    {"id": "collect_1k", "stat": "stars_collected", "target": 1000, "title": "Сборщик I", "reward_stars": 200},
    {"id": "collect_100k", "stat": "stars_collected", "target": 100000, "title": "Сборщик II", "reward_stars": 2000},
    {"id": "collect_1m", "stat": "stars_collected", "target": 1000000, "title": "Сборщик III", "reward_stars": 15000},

    {"id": "farms_5", "stat": "farms_bought", "target": 5, "title": "Фермер I", "reward_stars": 500},
    {"id": "farms_25", "stat": "farms_bought", "target": 25, "title": "Фермер II", "reward_stars": 3000},
    {"id": "farms_100", "stat": "farms_bought", "target": 100, "title": "Фермер III", "reward_stars": 20000},

    {"id": "cases_10", "stat": "cases_opened", "target": 10, "title": "Охотник за кейсами I", "reward_stars": 800},
    {"id": "cases_50", "stat": "cases_opened", "target": 50, "title": "Охотник за кейсами II", "reward_stars": 5000},
    {"id": "cases_200", "stat": "cases_opened", "target": 200, "title": "Охотник за кейсами III", "reward_stars": 25000},
]

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
        "boost": 1.4,        "gift_id": "diamond_ring", "limit": 100
    },
    "magic_lamp": {
        "name": "🪔 Magic Lamp",
        "price": 14000,
        "boost": 1.32,        "gift_id": "magic_lamp"
    },
    "fire_dragon": {
        "name": "🐉 Fire Dragon",
        "price": 22000,
        "boost": 1.45,        "gift_id": "fire_dragon", "limit": 75
    },
    "cosmic_star": {
        "name": "⭐ Cosmic Star",
        "price": 15000,
        "boost": 1.35,        "gift_id": "cosmic_star"
    },
    "golden_crown": {
        "name": "👑 Golden Crown",
        "price": 28000,
        "boost": 1.6,        "gift_id": "golden_crown", "limit": 50
    },
    "mystic_orb": {
        "name": "🔮 Mystic Orb",
        "price": 17000,
        "boost": 1.38,        "gift_id": "mystic_orb"
    }
}

STAR_FARM_CASES = {
    "basic": {
        "name": "🎁 Basic Case",
        "price_stars": 250,
        "rarity_weights": {"common": 80, "rare": 18, "epic": 2}
    },
    "rare": {
        "name": "🎁 Rare Case",
        "price_stars": 750,
        "rarity_weights": {"common": 65, "rare": 30, "epic": 5}
    },
    "epic": {
        "name": "🎁 Epic Case",
        "price_stars": 2000,
        "rarity_weights": {"rare": 45, "epic": 45, "legendary": 10}
    },
    "legendary": {
        "name": "🎁 Legendary Case",
        "price_stars": 5000,
        "rarity_weights": {"epic": 60, "legendary": 35, "mythic": 5}
    }
}

CASE_FARM_TYPES = {
    "case_common_1": {"name": "🌿 Case Farm (Common)", "rarity": "common", "income_per_hour": 120},
    "case_common_2": {"name": "🍀 Case Farm (Common)", "rarity": "common", "income_per_hour": 160},
    "case_rare_1": {"name": "🟦 Case Farm (Rare)", "rarity": "rare", "income_per_hour": 420},
    "case_rare_2": {"name": "🔷 Case Farm (Rare)", "rarity": "rare", "income_per_hour": 520},
    "case_epic_1": {"name": "🟪 Case Farm (Epic)", "rarity": "epic", "income_per_hour": 1400},
    "case_epic_2": {"name": "💠 Case Farm (Epic)", "rarity": "epic", "income_per_hour": 1800},
    "case_legendary_1": {"name": "🟧 Case Farm (Legendary)", "rarity": "legendary", "income_per_hour": 5200},
    "case_mythic_1": {"name": "🟥 Case Farm (Mythic)", "rarity": "mythic", "income_per_hour": 15000}
}

SATURDAY_FARM_POOL = [
    {"key": "sat_farm_1", "name": "🗓️ Субботняя ферма: Лимонная", "income_per_hour": 900, "price_stars": 2500, "price_crystals": 0},
    {"key": "sat_farm_2", "name": "🗓️ Субботняя ферма: Аркада", "income_per_hour": 1400, "price_stars": 4200, "price_crystals": 0},
    {"key": "sat_farm_3", "name": "🗓️ Субботняя ферма: Лаборатория", "income_per_hour": 2200, "price_stars": 0, "price_crystals": 12},
    {"key": "sat_farm_4", "name": "🗓️ Субботняя ферма: Метеоритная", "income_per_hour": 3200, "price_stars": 7000, "price_crystals": 0},
    {"key": "sat_farm_5", "name": "🗓️ Субботняя ферма: Кристальная", "income_per_hour": 5000, "price_stars": 0, "price_crystals": 20},
    {"key": "sat_farm_6", "name": "🗓️ Субботняя ферма: Пирамида", "income_per_hour": 3800, "price_stars": 8500, "price_crystals": 0}
]

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


