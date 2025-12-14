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
            CREATE TABLE IF NOT EXISTS nfts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                nft_type TEXT,
                purchased_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
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

async def get_next_internal_id() -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT MAX(internal_id) FROM users WHERE internal_id IS NOT NULL")
        result = await cursor.fetchone()
        max_id = result[0] if result[0] is not None else 0
        return max_id + 1

async def get_or_create_user(user_id: int) -> Dict:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM users WHERE user_id = ?",
            (user_id,)
        )
        user = await cursor.fetchone()
        
        if not user:
            internal_id = await get_next_internal_id()
            await db.execute(
                "INSERT INTO users (user_id, internal_id, stars, last_collect) VALUES (?, ?, ?, ?)",
                (user_id, internal_id, 200, datetime.now().isoformat())
            )
            await db.commit()
            cursor = await db.execute(
                "SELECT * FROM users WHERE user_id = ?",
                (user_id,)
            )
            user = await cursor.fetchone()
        elif user['internal_id'] is None:
            internal_id = await get_next_internal_id()
            await db.execute(
                "UPDATE users SET internal_id = ? WHERE user_id = ?",
                (internal_id, user_id)
            )
            await db.commit()
            cursor = await db.execute(
                "SELECT * FROM users WHERE user_id = ?",
                (user_id,)
            )
            user = await cursor.fetchone()
        
        return dict(user)

async def get_user_stars(user_id: int) -> int:
    user = await get_or_create_user(user_id)
    return user['stars']

async def add_stars(user_id: int, amount: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE users SET stars = stars + ? WHERE user_id = ?",
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
            total_boost *= NFT_GIFTS[nft_type]["boost"]
    
    return total_boost

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
            'stars': user['stars']
        }
    return None

