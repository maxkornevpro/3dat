import aiosqlite
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional

DB_NAME = "game_bot.db"

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                stars INTEGER DEFAULT 200,
                crystals INTEGER DEFAULT 0,
                last_collect TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        try:
            await db.execute("ALTER TABLE users ADD COLUMN internal_id INTEGER")
            await db.commit()
        except:
            pass

        try:
            await db.execute("ALTER TABLE users ADD COLUMN crystals INTEGER DEFAULT 0")
            await db.commit()
        except:
            pass
        
        try:
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_internal_id ON users(internal_id)")
        except:
            pass
        
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE internal_id IS NULL")
        null_count = (await cursor.fetchone())[0]
        if null_count > 0:
            cursor = await db.execute("SELECT user_id FROM users WHERE internal_id IS NULL ORDER BY created_at")
            users = await cursor.fetchall()
            for idx, (user_id,) in enumerate(users, start=1):
                cursor = await db.execute("SELECT MAX(internal_id) FROM users WHERE internal_id IS NOT NULL")
                result = await cursor.fetchone()
                max_id = result[0] if result[0] is not None else 0
                new_id = max_id + idx
                await db.execute("UPDATE users SET internal_id = ? WHERE user_id = ?", (new_id, user_id))
            await db.commit()
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS farms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                farm_type TEXT,
                purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activated TIMESTAMP,
                is_active BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS farm_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER,
                buyer_id INTEGER,
                farm_id INTEGER,
                price INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (seller_id) REFERENCES users (user_id),
                FOREIGN KEY (buyer_id) REFERENCES users (user_id),
                FOREIGN KEY (farm_id) REFERENCES farms (id)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS nfts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                nft_type TEXT,
                purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_items (
                user_id INTEGER,
                item_key TEXT,
                qty INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, item_key),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                prefix TEXT,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS item_auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER,
                item_key TEXT,
                qty INTEGER,
                starting_price INTEGER,
                current_bid INTEGER,
                current_bidder_id INTEGER,
                end_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (seller_id) REFERENCES users (user_id),
                FOREIGN KEY (current_bidder_id) REFERENCES users (user_id)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER,
                reward_given BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users (user_id),
                FOREIGN KEY (referred_id) REFERENCES users (user_id),
                UNIQUE(referred_id)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                farm_type TEXT,
                starting_price INTEGER,
                current_bid INTEGER,
                current_bidder_id INTEGER,
                end_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (current_bidder_id) REFERENCES users (user_id)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bans (
                user_id INTEGER PRIMARY KEY,
                reason TEXT,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                banned_by INTEGER
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                chat_type TEXT,
                title TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.commit()


async def get_user_prefix(user_id: int) -> str:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT prefix FROM user_settings WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] else ""


async def set_user_prefix(user_id: int, prefix: Optional[str]) -> bool:
    prefix = (prefix or "").strip()
    if len(prefix) > 24:
        prefix = prefix[:24]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO user_settings (user_id, prefix) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET prefix=excluded.prefix",
            (user_id, prefix or None)
        )
        await db.commit()
    return True


async def get_user_items(user_id: int) -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT item_key, qty FROM user_items WHERE user_id = ? AND qty > 0 ORDER BY item_key ASC",
            (user_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def add_item(user_id: int, item_key: str, qty: int = 1) -> bool:
    if qty <= 0:
        return False
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO user_items (user_id, item_key, qty) VALUES (?, ?, ?) ON CONFLICT(user_id, item_key) DO UPDATE SET qty = qty + excluded.qty",
            (user_id, item_key, qty)
        )
        await db.commit()
    return True


async def remove_item(user_id: int, item_key: str, qty: int = 1) -> bool:
    if qty <= 0:
        return False
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            "SELECT qty FROM user_items WHERE user_id = ? AND item_key = ?",
            (user_id, item_key)
        )
        row = await cursor.fetchone()
        current = row[0] if row else 0
        if current < qty:
            await db.rollback()
            return False

        new_qty = current - qty
        if new_qty == 0:
            await db.execute(
                "DELETE FROM user_items WHERE user_id = ? AND item_key = ?",
                (user_id, item_key)
            )
        else:
            await db.execute(
                "UPDATE user_items SET qty = ? WHERE user_id = ? AND item_key = ?",
                (new_qty, user_id, item_key)
            )
        await db.commit()
        return True


async def transfer_stars(from_user_id: int, to_user_id: int, amount: int) -> bool:
    if amount <= 0:
        return False

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            "SELECT COALESCE(stars, 0) FROM users WHERE user_id = ?",
            (from_user_id,)
        )
        row = await cursor.fetchone()
        current = row[0] if row else 0
        if current < amount:
            await db.rollback()
            return False

        await db.execute(
            "UPDATE users SET stars = COALESCE(stars, 0) - ? WHERE user_id = ?",
            (amount, from_user_id)
        )
        await db.execute(
            "UPDATE users SET stars = COALESCE(stars, 0) + ? WHERE user_id = ?",
            (amount, to_user_id)
        )
        await db.commit()
        return True


async def transfer_item(from_user_id: int, to_user_id: int, item_key: str, qty: int) -> bool:
    if qty <= 0:
        return False
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            "SELECT qty FROM user_items WHERE user_id = ? AND item_key = ?",
            (from_user_id, item_key)
        )
        row = await cursor.fetchone()
        current = row[0] if row else 0
        if current < qty:
            await db.rollback()
            return False

        new_qty = current - qty
        if new_qty == 0:
            await db.execute(
                "DELETE FROM user_items WHERE user_id = ? AND item_key = ?",
                (from_user_id, item_key)
            )
        else:
            await db.execute(
                "UPDATE user_items SET qty = ? WHERE user_id = ? AND item_key = ?",
                (new_qty, from_user_id, item_key)
            )

        await db.execute(
            "INSERT INTO user_items (user_id, item_key, qty) VALUES (?, ?, ?) ON CONFLICT(user_id, item_key) DO UPDATE SET qty = qty + excluded.qty",
            (to_user_id, item_key, qty)
        )
        await db.commit()
        return True


async def create_item_auction(seller_id: int, item_key: str, qty: int, starting_price: int, duration_hours: int = 24) -> Optional[int]:
    if qty <= 0 or starting_price <= 0 or duration_hours <= 0:
        return None

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            "SELECT qty FROM user_items WHERE user_id = ? AND item_key = ?",
            (seller_id, item_key)
        )
        row = await cursor.fetchone()
        current = row[0] if row else 0
        if current < qty:
            await db.rollback()
            return None

        new_qty = current - qty
        if new_qty == 0:
            await db.execute(
                "DELETE FROM user_items WHERE user_id = ? AND item_key = ?",
                (seller_id, item_key)
            )
        else:
            await db.execute(
                "UPDATE user_items SET qty = ? WHERE user_id = ? AND item_key = ?",
                (new_qty, seller_id, item_key)
            )

        end_time = (datetime.now() + timedelta(hours=duration_hours)).isoformat()
        cursor = await db.execute(
            "INSERT INTO item_auctions (seller_id, item_key, qty, starting_price, current_bid, end_time, status) VALUES (?, ?, ?, ?, ?, ?, 'active')",
            (seller_id, item_key, qty, starting_price, starting_price, end_time)
        )
        await db.commit()
        return cursor.lastrowid


async def get_active_item_auctions() -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM item_auctions WHERE status = 'active' AND end_time > datetime('now') ORDER BY end_time ASC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def place_item_bid(auction_id: int, user_id: int, bid_amount: int) -> tuple[bool, str]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM item_auctions WHERE id = ? AND status = 'active'",
            (auction_id,)
        )
        auction = await cursor.fetchone()
        if not auction:
            return False, "Аукцион не найден или уже завершен"

        auction_dict = dict(auction)
        end_time = datetime.fromisoformat(auction_dict['end_time'])
        if datetime.now() >= end_time:
            await db.execute(
                "UPDATE item_auctions SET status = 'ended' WHERE id = ?",
                (auction_id,)
            )
            await db.commit()
            return False, "Аукцион уже завершен"

        current_bid = auction_dict['current_bid']
        if bid_amount <= current_bid:
            return False, f"Ставка должна быть больше {current_bid} ⭐"

        user_stars = await get_user_stars(user_id)
        if user_stars < bid_amount:
            return False, "Недостаточно звезд"

        await db.execute("BEGIN IMMEDIATE")

        if auction_dict.get('current_bidder_id'):
            await add_stars(auction_dict['current_bidder_id'], auction_dict['current_bid'])

        await spend_stars(user_id, bid_amount)
        await db.execute(
            "UPDATE item_auctions SET current_bid = ?, current_bidder_id = ? WHERE id = ?",
            (bid_amount, user_id, auction_id)
        )
        await db.commit()
        return True, f"Ставка принята: {bid_amount} ⭐"


async def end_item_auction(auction_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM item_auctions WHERE id = ?",
            (auction_id,)
        )
        auction = await cursor.fetchone()
        if not auction:
            return None

        auction_dict = dict(auction)
        if auction_dict['status'] != 'active':
            return None

        await db.execute(
            "UPDATE item_auctions SET status = 'ended' WHERE id = ?",
            (auction_id,)
        )
        await db.commit()

        winner_id = auction_dict.get('current_bidder_id')
        if winner_id:
            await add_item(winner_id, auction_dict['item_key'], int(auction_dict.get('qty', 1) or 1))
        else:
            await add_item(auction_dict['seller_id'], auction_dict['item_key'], int(auction_dict.get('qty', 1) or 1))

        return auction_dict

async def get_next_internal_id() -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT MAX(internal_id) FROM users WHERE internal_id IS NOT NULL")
        result = await cursor.fetchone()
        max_id = result[0] if result[0] is not None else 0
        return max_id + 1

async def get_or_create_user(user_id: int) -> Dict:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")

        cursor = await db.execute(
            "SELECT * FROM users WHERE user_id = ?",
            (user_id,)
        )
        user = await cursor.fetchone()

        if not user:
            internal_id = await get_next_internal_id()
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, internal_id, stars, last_collect) VALUES (?, ?, ?, ?)",
                (user_id, internal_id, 200, datetime.now().isoformat())
            )

        cursor = await db.execute(
            "SELECT * FROM users WHERE user_id = ?",
            (user_id,)
        )
        user = await cursor.fetchone()

        if user and user['internal_id'] is None:
            internal_id = await get_next_internal_id()
            await db.execute(
                "UPDATE users SET internal_id = ? WHERE user_id = ?",
                (internal_id, user_id)
            )
            cursor = await db.execute(
                "SELECT * FROM users WHERE user_id = ?",
                (user_id,)
            )
            user = await cursor.fetchone()

        await db.commit()
        return dict(user)

async def get_user_stars(user_id: int) -> int:
    user = await get_or_create_user(user_id)
    return user['stars']

async def get_user_crystals(user_id: int) -> int:
    user = await get_or_create_user(user_id)
    return user.get('crystals', 0) or 0

async def add_stars(user_id: int, amount: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET stars = stars + ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.commit()

async def add_crystals(user_id: int, amount: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET crystals = COALESCE(crystals, 0) + ? WHERE user_id = ?",
            (amount, user_id)
        )
        await db.commit()

async def spend_stars(user_id: int, amount: int) -> bool:
    current_stars = await get_user_stars(user_id)
    if current_stars >= amount:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE users SET stars = stars - ? WHERE user_id = ?",
                (amount, user_id)
            )
            await db.commit()
        return True
    return False

async def spend_crystals(user_id: int, amount: int) -> bool:
    current = await get_user_crystals(user_id)
    if current >= amount:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE users SET crystals = COALESCE(crystals, 0) - ? WHERE user_id = ?",
                (amount, user_id)
            )
            await db.commit()
        return True
    return False

async def transfer_crystals(from_user_id: int, to_user_id: int, amount: int) -> bool:
    if amount <= 0:
        return False

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")

        cursor = await db.execute(
            "SELECT COALESCE(crystals, 0) FROM users WHERE user_id = ?",
            (from_user_id,)
        )
        row = await cursor.fetchone()
        from_balance = row[0] if row else 0
        if from_balance < amount:
            await db.commit()
            return False

        await db.execute(
            "UPDATE users SET crystals = COALESCE(crystals, 0) - ? WHERE user_id = ?",
            (amount, from_user_id)
        )
        await db.execute(
            "UPDATE users SET crystals = COALESCE(crystals, 0) + ? WHERE user_id = ?",
            (amount, to_user_id)
        )
        await db.commit()
        return True

async def buy_farm(user_id: int, farm_type: str) -> bool:
    from config import FARM_TYPES
    
    if farm_type not in FARM_TYPES:
        return False
    
    price = FARM_TYPES[farm_type]["price"]
    
    if await spend_stars(user_id, price):
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO farms (user_id, farm_type, last_activated, is_active) VALUES (?, ?, ?, 0)",
                (user_id, farm_type, datetime.now().isoformat())
            )
            await db.commit()
        return True
    return False

async def activate_farms(user_id: int) -> tuple[int, int]:
    farms = await get_user_farms(user_id)
    if not farms:
        return 0, 0
    
    activated_count = 0
    now = datetime.now()
    
    async with aiosqlite.connect(DB_NAME) as db:
        for farm in farms:
            farm_id = farm['id']
            last_activated = farm.get('last_activated')
            is_active = farm.get('is_active', 0)
            
            can_activate = False
            if not last_activated or not is_active:
                can_activate = True
            else:
                last_activated_dt = datetime.fromisoformat(last_activated)
                hours_passed = (now - last_activated_dt).total_seconds() / 3600
                if hours_passed >= 6:
                    can_activate = True
            
            if can_activate:
                await db.execute(
                    "UPDATE farms SET last_activated = ?, is_active = 1 WHERE id = ?",
                    (now.isoformat(), farm_id)
                )
                activated_count += 1
        
        await db.commit()
    
    return activated_count, len(farms)

async def get_user_farms(user_id: int) -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM farms WHERE user_id = ?",
            (user_id,)
        )
        farms = await cursor.fetchall()
        return [dict(farm) for farm in farms]

async def buy_nft(user_id: int, nft_type: str) -> bool:
    from config import NFT_GIFTS
    
    if nft_type not in NFT_GIFTS:
        return False
    
    price = NFT_GIFTS[nft_type]["price"]
    
    if await spend_stars(user_id, price):
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO nfts (user_id, nft_type) VALUES (?, ?)",
                (user_id, nft_type)
            )
            await db.commit()
        return True
    return False

async def get_user_nfts(user_id: int) -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM nfts WHERE user_id = ?",
            (user_id,)
        )
        nfts = await cursor.fetchall()
        return [dict(nft) for nft in nfts]

async def calculate_total_boost(user_id: int) -> float:
    from config import NFT_GIFTS
    
    nfts = await get_user_nfts(user_id)
    total_boost = 1.0
    
    for nft in nfts:
        nft_type = nft['nft_type']
        if nft_type in NFT_GIFTS:
            b = float(NFT_GIFTS[nft_type].get("boost", 1.0))
            total_boost += max(0.0, b - 1.0)

    return min(total_boost, 2.5)

async def collect_farm_income(user_id: int) -> int:
    from config import FARM_TYPES
    
    user = await get_or_create_user(user_id)
    farms = await get_user_farms(user_id)
    
    if not farms:
        return 0
    
    last_collect = datetime.fromisoformat(user['last_collect']) if user['last_collect'] else datetime.now()
    now = datetime.now()
    hours_passed = (now - last_collect).total_seconds() / 3600
    
    hours_passed = min(hours_passed, 24)
    
    total_income = 0
    for farm in farms:
        is_active = farm.get('is_active', 0)
        if not is_active:
            continue
        
        last_activated = farm.get('last_activated')
        if last_activated:
            last_activated_dt = datetime.fromisoformat(last_activated)
            hours_since_activation = (now - last_activated_dt).total_seconds() / 3600
            if hours_since_activation >= 6:
                async with aiosqlite.connect(DB_NAME) as db:
                    await db.execute(
                        "UPDATE farms SET is_active = 0 WHERE id = ?",
                        (farm['id'],)
                    )
                    await db.commit()
                continue
        
        farm_type = farm['farm_type']
        if farm_type in FARM_TYPES:
            income_per_hour = FARM_TYPES[farm_type]["income_per_hour"]
            if last_activated:
                last_activated_dt = datetime.fromisoformat(last_activated)
                collect_from = max(last_activated_dt, last_collect)
                hours_for_income = (now - collect_from).total_seconds() / 3600
                hours_for_income = min(hours_for_income, hours_passed)
            else:
                hours_for_income = hours_passed
            
            total_income += income_per_hour * hours_for_income
    
    boost = await calculate_total_boost(user_id)
    total_income = int(total_income * boost)
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET last_collect = ? WHERE user_id = ?",
            (now.isoformat(), user_id)
        )
        await db.commit()
    
    if total_income > 0:
        await add_stars(user_id, total_income)
    
    return total_income

async def collect_farm_income_with_crystals(user_id: int) -> tuple[int, int]:
    income = await collect_farm_income(user_id)
    farms = await get_user_farms(user_id)
    if not farms:
        return income, 0

    import random
    crystals_gained = 0
    active_farms = 0
    for farm in farms:
        if not farm.get('is_active', 0):
            continue
        active_farms += 1
        if random.random() < 0.015:
            crystals_gained += 1

    crystals_gained += active_farms // 40

    if crystals_gained > 0:
        await add_crystals(user_id, crystals_gained)

    return income, crystals_gained

async def create_farm_trade(seller_id: int, buyer_id: int, farm_id: int, price: int) -> Optional[int]:
    if price <= 0:
        return None

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT id FROM farms WHERE id = ? AND user_id = ?",
            (farm_id, seller_id)
        )
        farm = await cursor.fetchone()
        if not farm:
            return None

        cursor = await db.execute(
            "INSERT INTO farm_trades (seller_id, buyer_id, farm_id, price, status) VALUES (?, ?, ?, ?, 'pending')",
            (seller_id, buyer_id, farm_id, price)
        )
        await db.commit()
        return cursor.lastrowid

async def get_farm_trade(trade_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM farm_trades WHERE id = ?",
            (trade_id,)
        )
        trade = await cursor.fetchone()
        return dict(trade) if trade else None

async def set_farm_trade_status(trade_id: int, status: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE farm_trades SET status = ? WHERE id = ?",
            (status, trade_id)
        )
        await db.commit()

async def transfer_farm_ownership(farm_id: int, from_user_id: int, to_user_id: int) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            "SELECT id FROM farms WHERE id = ? AND user_id = ?",
            (farm_id, from_user_id)
        )
        farm = await cursor.fetchone()
        if not farm:
            await db.commit()
            return False

        await db.execute(
            "UPDATE farms SET user_id = ? WHERE id = ?",
            (to_user_id, farm_id)
        )
        await db.commit()
        return True

async def register_referral(referrer_id: int, referred_id: int) -> bool:
    if referrer_id == referred_id:
        return False
    
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT * FROM referrals WHERE referred_id = ?",
            (referred_id,)
        )
        existing = await cursor.fetchone()
        
        if existing:
            return False
        
        await db.execute(
            "INSERT INTO referrals (referrer_id, referred_id, reward_given) VALUES (?, ?, 0)",
            (referrer_id, referred_id)
        )
        await db.commit()
        return True

async def give_referral_reward(referred_id: int) -> bool:
    from config import REFERRAL_REWARD
    
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT * FROM referrals WHERE referred_id = ? AND reward_given = 0",
            (referred_id,)
        )
        referral = await cursor.fetchone()
        
        if not referral:
            return False
        
        await add_stars(referred_id, REFERRAL_REWARD)
        
        await db.execute(
            "UPDATE referrals SET reward_given = 1 WHERE referred_id = ?",
            (referred_id,)
        )
        await db.commit()
        return True

async def get_referral_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) as count FROM referrals WHERE referrer_id = ?",
            (user_id,)
        )
        result = await cursor.fetchone()
        return result[0] if result else 0

async def create_auction(farm_type: str, starting_price: int, duration_hours: int = 24) -> int:
    from config import FARM_TYPES
    
    if farm_type not in FARM_TYPES:
        return 0
    
    end_time = datetime.now() + timedelta(hours=duration_hours)
    
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "INSERT INTO auctions (farm_type, starting_price, current_bid, end_time, status) VALUES (?, ?, ?, ?, 'active')",
            (farm_type, starting_price, starting_price, end_time.isoformat())
        )
        await db.commit()
        return cursor.lastrowid

async def get_active_auctions() -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM auctions WHERE status = 'active' AND end_time > datetime('now') ORDER BY end_time ASC"
        )
        auctions = await cursor.fetchall()
        return [dict(auction) for auction in auctions]

async def place_bid(auction_id: int, user_id: int, bid_amount: int) -> tuple[bool, str]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM auctions WHERE id = ? AND status = 'active'",
            (auction_id,)
        )
        auction = await cursor.fetchone()
        
        if not auction:
            return False, "Аукцион не найден или уже завершен"
        
        auction_dict = dict(auction)
        
        end_time = datetime.fromisoformat(auction_dict['end_time'])
        if datetime.now() >= end_time:
            await db.execute(
                "UPDATE auctions SET status = 'ended' WHERE id = ?",
                (auction_id,)
            )
            await db.commit()
            return False, "Аукцион уже завершен"
        
        current_bid = auction_dict['current_bid']
        if bid_amount <= current_bid:
            return False, f"Ставка должна быть больше {current_bid} ⭐"
        
        user_stars = await get_user_stars(user_id)
        if user_stars < bid_amount:
            return False, "Недостаточно звезд"
        
        if auction_dict['current_bidder_id']:
            await add_stars(auction_dict['current_bidder_id'], auction_dict['current_bid'])
        
        await spend_stars(user_id, bid_amount)
        
        await db.execute(
            "UPDATE auctions SET current_bid = ?, current_bidder_id = ? WHERE id = ?",
            (bid_amount, user_id, auction_id)
        )
        await db.commit()
        
        return True, f"Ставка принята: {bid_amount} ⭐"

async def end_auction(auction_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM auctions WHERE id = ?",
            (auction_id,)
        )
        auction = await cursor.fetchone()
        
        if not auction:
            return None
        
        auction_dict = dict(auction)
        
        if auction_dict['status'] != 'active':
            return None
        
        await db.execute(
            "UPDATE auctions SET status = 'ended' WHERE id = ?",
            (auction_id,)
        )
        await db.commit()
        
        if auction_dict['current_bidder_id']:
            winner_id = auction_dict['current_bidder_id']
            farm_type = auction_dict['farm_type']
            
            await db.execute(
                "INSERT INTO farms (user_id, farm_type) VALUES (?, ?)",
                (winner_id, farm_type)
            )
            await db.commit()
        
        return auction_dict

async def is_banned(user_id: int) -> bool:
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            cursor = await db.execute(
                "SELECT 1 FROM bans WHERE user_id = ?",
                (user_id,)
            )
            result = await cursor.fetchone()
            return result is not None
    except Exception as e:
        return False

async def ban_user(user_id: int, reason: str, admin_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR REPLACE INTO bans (user_id, reason, banned_by) VALUES (?, ?, ?)",
            (user_id, reason, admin_id)
        )
        await db.commit()

async def unban_user(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "DELETE FROM bans WHERE user_id = ?",
            (user_id,)
        )
        await db.commit()

async def admin_add_stars(user_id: int, amount: int):
    await add_stars(user_id, amount)

async def admin_add_farm(user_id: int, farm_type: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO farms (user_id, farm_type, last_activated, is_active) VALUES (?, ?, ?, 0)",
            (user_id, farm_type, datetime.now().isoformat())
        )
        await db.commit()

async def admin_add_nft(user_id: int, nft_type: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO nfts (user_id, nft_type) VALUES (?, ?)",
            (user_id, nft_type)
        )
        await db.commit()

async def get_all_users() -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users")
        users = await cursor.fetchall()
        return [dict(user) for user in users]

async def get_all_chats() -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM chats")
        chats = await cursor.fetchall()
        return [dict(chat) for chat in chats]

async def add_chat(chat_id: int, chat_type: str, title: str = None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR IGNORE INTO chats (chat_id, chat_type, title) VALUES (?, ?, ?)",
            (chat_id, chat_type, title)
        )
        await db.commit()

async def get_next_internal_id() -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT MAX(internal_id) FROM users WHERE internal_id IS NOT NULL")
        result = await cursor.fetchone()
        max_id = result[0] if result[0] is not None else 0
        return max_id + 1

async def get_user_by_internal_id(internal_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM users WHERE internal_id = ?",
            (internal_id,)
        )
        user = await cursor.fetchone()
        return dict(user) if user else None

async def get_user_info_by_internal_id(internal_id: int) -> Optional[Dict]:
    user = await get_user_by_internal_id(internal_id)
    if user:
        return {
            'user_id': user['user_id'],
            'internal_id': user['internal_id'],
            'stars': user['stars'],
            'crystals': user.get('crystals', 0) or 0
        }
    return None

async def get_top_by_balance(limit: int = 5) -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT user_id, stars, internal_id FROM users ORDER BY stars DESC LIMIT ?",
            (limit,)
        )
        users = await cursor.fetchall()
        return [dict(user) for user in users]

async def get_top_by_income_per_minute(limit: int = 5) -> List[Dict]:
    from config import FARM_TYPES
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT user_id, internal_id FROM users")
        all_users = await cursor.fetchall()
        
        user_incomes = []
        for user_row in all_users:
            user_id = user_row['user_id']
            internal_id = user_row['internal_id']
            
            farms = await get_user_farms(user_id)
            nfts = await get_user_nfts(user_id)
            boost = await calculate_total_boost(user_id)
            
            total_income_per_hour = 0
            from datetime import datetime
            now = datetime.now()
            
            for farm in farms:
                is_active = farm.get('is_active', 0)
                if is_active:
                    last_activated = farm.get('last_activated')
                    if last_activated:
                        last_activated_dt = datetime.fromisoformat(last_activated)
                        hours_passed = (now - last_activated_dt).total_seconds() / 3600
                        if hours_passed < 6:
                            farm_type = farm['farm_type']
                            if farm_type in FARM_TYPES:
                                total_income_per_hour += FARM_TYPES[farm_type]['income_per_hour']
            
            total_income_per_hour = int(total_income_per_hour * boost)
            total_income_per_minute = total_income_per_hour / 60
            
            user_incomes.append({
                'user_id': user_id,
                'internal_id': internal_id,
                'income_per_minute': total_income_per_minute
            })
        
        user_incomes.sort(key=lambda x: x['income_per_minute'], reverse=True)
        return user_incomes[:limit]

async def get_top_by_nft_count(limit: int = 5) -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT u.user_id, u.internal_id, COUNT(n.id) as nft_count
            FROM users u
            LEFT JOIN nfts n ON u.user_id = n.user_id
            GROUP BY u.user_id, u.internal_id
            ORDER BY nft_count DESC
            LIMIT ?
        """, (limit,))
        users = await cursor.fetchall()
        return [dict(user) for user in users]

