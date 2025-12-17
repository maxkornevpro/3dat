import aiosqlite
import asyncio
from datetime import datetime, timedelta
import random
from typing import List, Dict, Optional, Tuple

DB_NAME = "game_bot.db"

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                stars INTEGER DEFAULT 200,
                crystals INTEGER DEFAULT 0,
                xp INTEGER DEFAULT 0,
                level INTEGER DEFAULT 1,
                last_collect TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS case_drops (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                case_type TEXT,
                reward_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS global_quests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quest_type TEXT,
                title TEXT,
                goal_value INTEGER,
                progress_value INTEGER DEFAULT 0,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT DEFAULT 'active',
                reward_buff_multiplier REAL DEFAULT 1.0,
                reward_buff_seconds INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS global_buffs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                buff_type TEXT,
                multiplier REAL DEFAULT 1.0,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS contests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                description TEXT,
                reward TEXT,
                how_to TEXT,
                created_by INTEGER,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_farm_auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER,
                farm_type TEXT,
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
            CREATE TABLE IF NOT EXISTS user_nft_auctions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER,
                nft_type TEXT,
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
            await db.execute("ALTER TABLE users ADD COLUMN xp INTEGER DEFAULT 0")
            await db.commit()
        except:
            pass

        try:
            await db.execute("ALTER TABLE users ADD COLUMN level INTEGER DEFAULT 1")
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
                speed_level INTEGER DEFAULT 1,
                cap_level INTEGER DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        try:
            await db.execute("ALTER TABLE farms ADD COLUMN speed_level INTEGER DEFAULT 1")
            await db.commit()
        except:
            pass

        try:
            await db.execute("ALTER TABLE farms ADD COLUMN cap_level INTEGER DEFAULT 1")
            await db.commit()
        except:
            pass

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
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id INTEGER PRIMARY KEY,
                stars_collected INTEGER DEFAULT 0,
                farms_bought INTEGER DEFAULT 0,
                cases_opened INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_achievements (
                user_id INTEGER,
                achievement_id TEXT,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, achievement_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS season_snapshots (
                user_id INTEGER,
                season_key TEXT,
                start_stars INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, season_key),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS season_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                season_key TEXT,
                rank INTEGER,
                user_id INTEGER,
                internal_id INTEGER,
                season_score INTEGER,
                reward_stars INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

        await db.execute("""
            CREATE TABLE IF NOT EXISTS saturday_offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                offer_date TEXT,
                farm_key TEXT,
                name TEXT,
                income_per_hour INTEGER,
                price_stars INTEGER DEFAULT 0,
                price_crystals INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Farm upgrades table for income boosts
        await db.execute("""
            CREATE TABLE IF NOT EXISTS farm_upgrades (
                farm_id INTEGER PRIMARY KEY,
                income_boost REAL DEFAULT 1.0,
                upgrade_level INTEGER DEFAULT 0,
                last_upgraded TIMESTAMP,
                FOREIGN KEY (farm_id) REFERENCES farms (id)
            )
        """)
        
        # NFT Listings table for the NFT marketplace
        await db.execute("""
            CREATE TABLE IF NOT EXISTS nft_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER NOT NULL,
                nft_type TEXT NOT NULL,
                price INTEGER NOT NULL,
                fee_pct REAL NOT NULL,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sold_at TIMESTAMP NULL,
                buyer_id INTEGER NULL,
                FOREIGN KEY (seller_id) REFERENCES users (user_id),
                FOREIGN KEY (buyer_id) REFERENCES users (user_id)
            )
        """)

        await db.commit()


async def add_case_drop(user_id: int, case_type: str, reward_text: str) -> None:
    case_type = (case_type or "").strip()[:32]
    reward_text = (reward_text or "").strip()[:256]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO case_drops (user_id, case_type, reward_text) VALUES (?, ?, ?)",
            (user_id, case_type, reward_text)
        )
        await db.commit()


async def get_case_drops(user_id: int, limit: int = 20) -> List[Dict]:
    limit = int(limit or 20)
    if limit <= 0:
        limit = 20
    if limit > 50:
        limit = 50
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT case_type, reward_text, created_at FROM case_drops WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, limit)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_active_global_buff() -> Optional[Dict]:
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM global_buffs WHERE end_time > ? ORDER BY id DESC LIMIT 1",
            (now,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def activate_global_buff(multiplier: float, seconds: int, buff_type: str = 'income') -> None:
    multiplier = float(multiplier or 1.0)
    seconds = int(seconds or 0)
    if seconds <= 0:
        return
    start = datetime.now()
    end = start + timedelta(seconds=seconds)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO global_buffs (buff_type, multiplier, start_time, end_time) VALUES (?, ?, ?, ?)",
            (buff_type, multiplier, start.isoformat(), end.isoformat())
        )
        await db.commit()


async def get_or_create_global_quest() -> Dict:
    """Creates a default global quest if none is active.

    Default quest: collect stars server-wide within 24h.
    Reward: income multiplier buff for 3h.
    """
    now = datetime.now()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")

        cursor = await db.execute(
            "SELECT * FROM global_quests WHERE status = 'active' ORDER BY id DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        if row:
            q = dict(row)
            end_time = q.get('end_time')
            if end_time:
                try:
                    end_dt = datetime.fromisoformat(end_time)
                    if end_dt <= now:
                        await db.execute(
                            "UPDATE global_quests SET status = 'failed' WHERE id = ?",
                            (int(q['id']),)
                        )
                        row = None
                except Exception:
                    pass

        if not row:
            start = now
            end = now + timedelta(hours=24)
            await db.execute(
                """INSERT INTO global_quests (
                    quest_type, title, goal_value, progress_value, start_time, end_time,
                    status, reward_buff_multiplier, reward_buff_seconds
                ) VALUES (?, ?, ?, 0, ?, ?, 'active', ?, ?)""",
                (
                    'collect_stars',
                    'Глобалка: собрать ⭐ всем сервером',
                    1_000_000,
                    start.isoformat(),
                    end.isoformat(),
                    1.25,
                    3 * 60 * 60,
                )
            )
            cursor = await db.execute(
                "SELECT * FROM global_quests WHERE status = 'active' ORDER BY id DESC LIMIT 1"
            )
            row = await cursor.fetchone()

        await db.commit()
        return dict(row)


async def add_global_quest_progress(amount: int) -> Dict:
    amount = int(amount or 0)
    if amount <= 0:
        return await get_or_create_global_quest()

    now = datetime.now()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")

        cursor = await db.execute(
            "SELECT * FROM global_quests WHERE status = 'active' ORDER BY id DESC LIMIT 1"
        )
        row = await cursor.fetchone()
        if not row:
            await db.commit()
            return await get_or_create_global_quest()

        q = dict(row)
        end_time = q.get('end_time')
        if end_time:
            try:
                if datetime.fromisoformat(end_time) <= now:
                    await db.execute(
                        "UPDATE global_quests SET status = 'failed' WHERE id = ?",
                        (int(q['id']),)
                    )
                    await db.commit()
                    return await get_or_create_global_quest()
            except Exception:
                pass

        new_progress = int(q.get('progress_value', 0) or 0) + amount
        goal = int(q.get('goal_value', 0) or 0)

        if goal > 0 and new_progress >= goal:
            await db.execute(
                "UPDATE global_quests SET progress_value = ?, status = 'completed' WHERE id = ?",
                (new_progress, int(q['id']))
            )
            mult = float(q.get('reward_buff_multiplier', 1.0) or 1.0)
            secs = int(q.get('reward_buff_seconds', 0) or 0)
            await db.commit()
            await activate_global_buff(mult, secs, buff_type='income')
            # start a new quest next time user opens /quests or contributes
            q['progress_value'] = new_progress
            q['status'] = 'completed'
            return q

        await db.execute(
            "UPDATE global_quests SET progress_value = ? WHERE id = ?",
            (new_progress, int(q['id']))
        )
        await db.commit()
        q['progress_value'] = new_progress
        return q


def farm_speed_multiplier(speed_level: int) -> float:
    speed_level = int(speed_level or 1)
    if speed_level < 1:
        speed_level = 1
    if speed_level > 10:
        speed_level = 10
    return 1.0 + (speed_level - 1) * 0.05


def farm_cap_hours(cap_level: int) -> float:
    cap_level = int(cap_level or 1)
    if cap_level < 1:
        cap_level = 1
    if cap_level > 10:
        cap_level = 10
    # база: 6 часов, дальше растёт
    return 6.0 * (1.35 ** (cap_level - 1))


def farm_upgrade_cost(base_price: int, current_level: int) -> int:
    base_price = int(base_price or 0)
    current_level = int(current_level or 1)
    if current_level < 1:
        current_level = 1
    if current_level > 10:
        current_level = 10
    base_cost = max(50, base_price // 2)
    return int(round(base_cost * (1.35 ** (current_level - 1))))


async def upgrade_farm(user_id: int, farm_id: int, kind: str) -> Tuple[bool, str]:
    from config import FARM_TYPES
    kind = (kind or "").strip().lower()
    if kind not in ("speed", "cap"):
        return False, "Неверный тип апгрейда"

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        try:
            cursor = await db.execute(
                "SELECT id, user_id, farm_type, speed_level, cap_level FROM farms WHERE id = ?",
                (int(farm_id),)
            )
            row = await cursor.fetchone()
            if not row:
                await db.rollback()
                return False, "Ферма не найдена"
            farm = dict(row)
            if int(farm.get('user_id')) != int(user_id):
                await db.rollback()
                return False, "Это не ваша ферма"

            farm_type = str(farm.get('farm_type') or "")
            if farm_type.startswith("case_"):
                await db.rollback()
                return False, "Case-фермы нельзя улучшать"

            if farm_type not in FARM_TYPES:
                await db.rollback()
                return False, "Нельзя улучшить эту ферму"

            base_price = int(FARM_TYPES[farm_type].get('price', 0) or 0)
            if kind == "speed":
                current = int(farm.get('speed_level', 1) or 1)
                if current >= 10:
                    await db.rollback()
                    return False, "Скорость уже на максимуме"
                cost = farm_upgrade_cost(base_price, current)
                ok = await spend_stars(user_id, cost)
                if not ok:
                    await db.rollback()
                    return False, "Недостаточно звезд"
                await db.execute("UPDATE farms SET speed_level = speed_level + 1 WHERE id = ?", (int(farm_id),))
                await db.commit()
                return True, f"⚡ Скорость улучшена до {current + 1} уровня (-{cost} ⭐)"

            current = int(farm.get('cap_level', 1) or 1)
            if current >= 10:
                await db.rollback()
                return False, "Лимит уже на максимуме"
            cost = farm_upgrade_cost(base_price, current)
            ok = await spend_stars(user_id, cost)
            if not ok:
                await db.rollback()
                return False, "Недостаточно звезд"
            await db.execute("UPDATE farms SET cap_level = cap_level + 1 WHERE id = ?", (int(farm_id),))
            await db.commit()
            return True, f"📦 Лимит улучшен до {current + 1} уровня (-{cost} ⭐)"
        except Exception:
            await db.rollback()
            return False, "Ошибка при улучшении"


async def get_active_contests() -> List[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM contests WHERE status = 'active' ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def add_contest(title: str, description: str, reward: str, how_to: str, created_by: int) -> bool:
    if not (title or description or reward or how_to):
        return False
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO contests (title, description, reward, how_to, created_by, status) VALUES (?, ?, ?, ?, ?, 'active')",
            (title, description, reward, how_to, created_by)
        )
        await db.commit()
        return True


async def clear_contests() -> None:
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE contests SET status = 'ended' WHERE status = 'active'")
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


async def get_farm_dynamic_price(user_id: int, farm_type: str) -> int:
    from config import FARM_TYPES
    if farm_type not in FARM_TYPES:
        return 0
    base_price = int(FARM_TYPES[farm_type]["price"])
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM farms WHERE user_id = ? AND farm_type = ?",
            (user_id, farm_type)
        )
        row = await cursor.fetchone()
        owned = int(row[0]) if row else 0

    price = int(round(base_price * (1.5 ** owned)))
    if price < base_price:
        price = base_price
    return price


async def buy_farm_dynamic(user_id: int, farm_type: str, price: int) -> bool:
    from config import FARM_TYPES
    if farm_type not in FARM_TYPES:
        return False
    if price <= 0:
        return False
    if await spend_stars(user_id, price):
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO farms (user_id, farm_type, last_activated, is_active) VALUES (?, ?, ?, 0)",
                (user_id, farm_type, datetime.now().isoformat())
            )
            await db.commit()
        return True
    return False


async def create_user_farm_auction(seller_id: int, farm_type: str, starting_price: int, duration_hours: int = 24) -> tuple[int, str]:
    from config import FARM_TYPES
    if farm_type not in FARM_TYPES:
        return 0, "Неверный тип фермы"
    if starting_price <= 0:
        return 0, "Цена должна быть > 0"

    real_price = int(FARM_TYPES[farm_type]["price"])
    max_start = int(real_price // 1.5)
    if max_start <= 0:
        max_start = 1
    if starting_price > max_start:
        return 0, f"Максимальная стартовая цена: {max_start} ⭐"

    async with aiosqlite.connect(DB_NAME) as db:
        # забираем у продавца одну ферму данного типа
        cursor = await db.execute(
            "SELECT id FROM farms WHERE user_id = ? AND farm_type = ? ORDER BY id ASC LIMIT 1",
            (seller_id, farm_type)
        )
        row = await cursor.fetchone()
        if not row:
            return 0, "У вас нет такой фермы"
        farm_row_id = row[0]

        await db.execute("BEGIN IMMEDIATE")
        try:
            await db.execute("DELETE FROM farms WHERE id = ? AND user_id = ?", (farm_row_id, seller_id))
            end_time = datetime.now() + timedelta(hours=duration_hours)
            cur = await db.execute(
                "INSERT INTO user_farm_auctions (seller_id, farm_type, starting_price, current_bid, end_time, status) VALUES (?, ?, ?, ?, ?, 'active')",
                (seller_id, farm_type, starting_price, starting_price, end_time.isoformat())
            )
            await db.commit()
            return cur.lastrowid, ""
        except Exception:
            await db.rollback()
            return 0, "Ошибка при создании лота"


async def create_user_nft_auction(seller_id: int, nft_type: str, starting_price: int, duration_hours: int = 24) -> tuple[int, str]:
    from config import NFT_GIFTS
    if nft_type not in NFT_GIFTS:
        return 0, "Неверный NFT"
    if starting_price <= 0:
        return 0, "Цена должна быть > 0"

    real_price = int(NFT_GIFTS[nft_type]["price"])
    max_start = int(real_price // 1.5)
    if max_start <= 0:
        max_start = 1
    if starting_price > max_start:
        return 0, f"Максимальная стартовая цена: {max_start} ⭐"

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT id FROM nfts WHERE user_id = ? AND nft_type = ? ORDER BY id ASC LIMIT 1",
            (seller_id, nft_type)
        )
        row = await cursor.fetchone()
        if not row:
            return 0, "У вас нет такого NFT"
        nft_row_id = row[0]

        await db.execute("BEGIN IMMEDIATE")
        try:
            await db.execute("DELETE FROM nfts WHERE id = ? AND user_id = ?", (nft_row_id, seller_id))
            end_time = datetime.now() + timedelta(hours=duration_hours)
            cur = await db.execute(
                "INSERT INTO user_nft_auctions (seller_id, nft_type, starting_price, current_bid, end_time, status) VALUES (?, ?, ?, ?, ?, 'active')",
                (seller_id, nft_type, starting_price, starting_price, end_time.isoformat())
            )
            await db.commit()
            return cur.lastrowid, ""
        except Exception:
            await db.rollback()
            return 0, "Ошибка при создании лота"


async def get_active_user_auctions() -> Dict[str, List[Dict]]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cur1 = await db.execute(
            "SELECT * FROM user_farm_auctions WHERE status = 'active' AND end_time > datetime('now') ORDER BY end_time ASC"
        )
        farms = [dict(r) for r in await cur1.fetchall()]
        cur2 = await db.execute(
            "SELECT * FROM user_nft_auctions WHERE status = 'active' AND end_time > datetime('now') ORDER BY end_time ASC"
        )
        nfts = [dict(r) for r in await cur2.fetchall()]
        return {"farms": farms, "nfts": nfts}


async def place_user_farm_bid(auction_id: int, user_id: int, bid_amount: int) -> tuple[bool, str]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM user_farm_auctions WHERE id = ? AND status = 'active'",
            (auction_id,)
        )
        auction = await cursor.fetchone()
        if not auction:
            return False, "Аукцион не найден или уже завершен"

        a = dict(auction)
        try:
            end_time = datetime.fromisoformat(a['end_time'])
        except Exception:
            end_time = datetime.now()
        if datetime.now() >= end_time:
            await db.execute("UPDATE user_farm_auctions SET status = 'ended' WHERE id = ?", (auction_id,))
            await db.commit()
            return False, "Аукцион уже завершен"

        current_bid = int(a.get('current_bid') or 0)
        if bid_amount <= current_bid:
            return False, f"Ставка должна быть больше {current_bid} ⭐"

        if int(a.get('seller_id')) == int(user_id):
            return False, "Нельзя ставить на свой лот"

        user_stars = await get_user_stars(user_id)
        if user_stars < bid_amount:
            return False, "Недостаточно звезд"

        await db.execute("BEGIN IMMEDIATE")
        try:
            prev_bidder = a.get('current_bidder_id')
            prev_bid = int(a.get('current_bid') or 0)
            if prev_bidder:
                await add_stars(int(prev_bidder), prev_bid)

            ok = await spend_stars(user_id, bid_amount)
            if not ok:
                await db.rollback()
                return False, "Недостаточно звезд"

            await db.execute(
                "UPDATE user_farm_auctions SET current_bid = ?, current_bidder_id = ? WHERE id = ?",
                (bid_amount, user_id, auction_id)
            )
            await db.commit()
            return True, f"Ставка принята: {bid_amount} ⭐"
        except Exception:
            await db.rollback()
            return False, "Ошибка при ставке"


async def end_user_farm_auction(auction_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM user_farm_auctions WHERE id = ?",
            (auction_id,)
        )
        auction = await cursor.fetchone()
        if not auction:
            return None
        a = dict(auction)
        if a.get('status') != 'active':
            return None

        await db.execute("UPDATE user_farm_auctions SET status = 'ended' WHERE id = ?", (auction_id,))
        await db.commit()

        winner_id = a.get('current_bidder_id')
        seller_id = int(a.get('seller_id'))
        farm_type = a.get('farm_type')
        qty = 1

        if winner_id:
            await add_stars(seller_id, int(a.get('current_bid') or 0))
            # передать ферму победителю
            async with aiosqlite.connect(DB_NAME) as db2:
                await db2.execute(
                    "INSERT INTO farms (user_id, farm_type, last_activated, is_active) VALUES (?, ?, ?, 0)",
                    (int(winner_id), farm_type, datetime.now().isoformat())
                )
                await db2.commit()
        else:
            # ставок нет — вернуть ферму продавцу
            async with aiosqlite.connect(DB_NAME) as db2:
                await db2.execute(
                    "INSERT INTO farms (user_id, farm_type, last_activated, is_active) VALUES (?, ?, ?, 0)",
                    (seller_id, farm_type, datetime.now().isoformat())
                )
                await db2.commit()

        return a


async def place_user_nft_bid(auction_id: int, user_id: int, bid_amount: int) -> tuple[bool, str]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM user_nft_auctions WHERE id = ? AND status = 'active'",
            (auction_id,)
        )
        auction = await cursor.fetchone()
        if not auction:
            return False, "Аукцион не найден или уже завершен"

        a = dict(auction)
        try:
            end_time = datetime.fromisoformat(a['end_time'])
        except Exception:
            end_time = datetime.now()
        if datetime.now() >= end_time:
            await db.execute("UPDATE user_nft_auctions SET status = 'ended' WHERE id = ?", (auction_id,))
            await db.commit()
            return False, "Аукцион уже завершен"

        current_bid = int(a.get('current_bid') or 0)
        if bid_amount <= current_bid:
            return False, f"Ставка должна быть больше {current_bid} ⭐"

        if int(a.get('seller_id')) == int(user_id):
            return False, "Нельзя ставить на свой лот"

        user_stars = await get_user_stars(user_id)
        if user_stars < bid_amount:
            return False, "Недостаточно звезд"

        await db.execute("BEGIN IMMEDIATE")
        try:
            prev_bidder = a.get('current_bidder_id')
            prev_bid = int(a.get('current_bid') or 0)
            if prev_bidder:
                await add_stars(int(prev_bidder), prev_bid)

            ok = await spend_stars(user_id, bid_amount)
            if not ok:
                await db.rollback()
                return False, "Недостаточно звезд"

            await db.execute(
                "UPDATE user_nft_auctions SET current_bid = ?, current_bidder_id = ? WHERE id = ?",
                (bid_amount, user_id, auction_id)
            )
            await db.commit()
            return True, f"Ставка принята: {bid_amount} ⭐"
        except Exception:
            await db.rollback()
            return False, "Ошибка при ставке"


async def end_user_nft_auction(auction_id: int) -> Optional[Dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM user_nft_auctions WHERE id = ?",
            (auction_id,)
        )
        auction = await cursor.fetchone()
        if not auction:
            return None
        a = dict(auction)
        if a.get('status') != 'active':
            return None

        await db.execute("UPDATE user_nft_auctions SET status = 'ended' WHERE id = ?", (auction_id,))
        await db.commit()

        winner_id = a.get('current_bidder_id')
        seller_id = int(a.get('seller_id'))
        nft_type = a.get('nft_type')

        if winner_id:
            await add_stars(seller_id, int(a.get('current_bid') or 0))
            async with aiosqlite.connect(DB_NAME) as db2:
                await db2.execute(
                    "INSERT INTO nfts (user_id, nft_type) VALUES (?, ?)",
                    (int(winner_id), nft_type)
                )
                await db2.commit()
        else:
            async with aiosqlite.connect(DB_NAME) as db2:
                await db2.execute(
                    "INSERT INTO nfts (user_id, nft_type) VALUES (?, ?)",
                    (seller_id, nft_type)
                )
                await db2.commit()

        return a


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
                "INSERT OR IGNORE INTO users (user_id, internal_id, stars, last_collect, xp, level) VALUES (?, ?, ?, ?, 0, 1)",
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

    user = await get_or_create_user(user_id)
    level = int(user.get('level', 1) or 1)
    total_boost *= get_level_bonus_multiplier(level)
    try:
        buff = await get_active_global_buff()
        if buff:
            total_boost *= float(buff.get('multiplier', 1.0) or 1.0)
    except Exception:
        pass
    return min(total_boost, 2.5)


def xp_needed_for_next_level(level: int) -> int:
    level = int(level or 1)
    if level < 1:
        level = 1
    return 100 * level


def get_level_bonus_multiplier(level: int) -> float:
    level = int(level or 1)
    if level < 1:
        level = 1
    return 1.0 + (level - 1) * 0.005


async def add_xp(user_id: int, amount: int) -> Dict:
    amount = int(amount or 0)
    if amount <= 0:
        user = await get_or_create_user(user_id)
        level = int(user.get('level', 1) or 1)
        xp = int(user.get('xp', 0) or 0)
        return {
            'level': level,
            'xp': xp,
            'xp_needed': xp_needed_for_next_level(level),
            'leveled_up': False,
            'levels_gained': 0
        }

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("SELECT level, xp FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if not row:
            await db.rollback()
            user = await get_or_create_user(user_id)
            level = int(user.get('level', 1) or 1)
            xp = int(user.get('xp', 0) or 0)
        else:
            level = int(row['level'] or 1)
            xp = int(row['xp'] or 0)

        xp += amount
        levels_gained = 0
        while xp >= xp_needed_for_next_level(level):
            xp -= xp_needed_for_next_level(level)
            level += 1
            levels_gained += 1

        await db.execute("UPDATE users SET xp = ?, level = ? WHERE user_id = ?", (xp, level, user_id))
        await db.commit()

        return {
            'level': level,
            'xp': xp,
            'xp_needed': xp_needed_for_next_level(level),
            'leveled_up': levels_gained > 0,
            'levels_gained': levels_gained
        }

async def collect_farm_income(user_id: int) -> int:
    from config import FARM_TYPES, CASE_FARM_TYPES
    
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
        farm_defs = {}
        farm_defs.update(FARM_TYPES)
        farm_defs.update(CASE_FARM_TYPES)
        try:
            special = await get_special_farm_types()
            farm_defs.update(special)
        except Exception:
            pass
        if farm_type in farm_defs:
            base_income_per_hour = farm_defs[farm_type]["income_per_hour"]
            speed_level = int(farm.get('speed_level', 1) or 1)
            cap_level = int(farm.get('cap_level', 1) or 1)

            eff_income_per_hour = base_income_per_hour
            if not str(farm_type).startswith("case_"):
                eff_income_per_hour = int(round(base_income_per_hour * farm_speed_multiplier(speed_level)))

            cap_h = farm_cap_hours(cap_level) if not str(farm_type).startswith("case_") else 6.0

            if last_activated:
                last_activated_dt = datetime.fromisoformat(last_activated)
                cap_from = now - timedelta(hours=cap_h)
                collect_from = max(last_activated_dt, last_collect, cap_from)
                hours_for_income = (now - collect_from).total_seconds() / 3600
                hours_for_income = min(hours_for_income, hours_passed)
            else:
                cap_from = now - timedelta(hours=cap_h)
                collect_from = max(last_collect, cap_from)
                hours_for_income = (now - collect_from).total_seconds() / 3600
                hours_for_income = min(hours_for_income, hours_passed)
            
            total_income += eff_income_per_hour * hours_for_income
    
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
            return False, f"Ставка должна быть больше {current_bid} "
        
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
        
        return True, f"Ставка принята: {bid_amount} "

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
                        hours_since_activation = (now - last_activated_dt).total_seconds() / 3600
                        if hours_since_activation < 6:
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


async def get_total_nft_count(nft_type: Optional[str] = None) -> int:
    """Returns total minted NFT count.

    If nft_type is provided, returns count only for that nft_type.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        if nft_type:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM nfts WHERE nft_type = ?",
                (str(nft_type),)
            )
        else:
            cursor = await db.execute("SELECT COUNT(*) FROM nfts")
        row = await cursor.fetchone()
        return int(row[0] or 0)


async def _is_saturday_now() -> bool:
    try:
        return datetime.now().weekday() == 5
    except Exception:
        return False


async def get_special_farm_types() -> Dict[str, Dict]:
    from config import SATURDAY_FARM_POOL

    result: Dict[str, Dict] = {}
    for f in SATURDAY_FARM_POOL:
        key = str(f.get('key'))
        if not key:
            continue
        result[key] = {
            'name': f.get('name', key),
            'income_per_hour': int(f.get('income_per_hour', 0) or 0)
        }
    return result


async def get_or_create_saturday_offers() -> None:
    if not await _is_saturday_now():
        return

    from config import SATURDAY_FARM_POOL

    offer_date = datetime.now().date().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")

        cursor = await db.execute(
            "SELECT 1 FROM saturday_offers WHERE offer_date = ? LIMIT 1",
            (offer_date,)
        )
        if await cursor.fetchone():
            await db.commit()
            return

        pool = list(SATURDAY_FARM_POOL)
        random.shuffle(pool)
        picks = pool[: min(3, len(pool))]

        for p in picks:
            farm_key = str(p.get('key'))
            name = str(p.get('name', farm_key))
            income_per_hour = int(p.get('income_per_hour', 0) or 0)
            price_stars = int(p.get('price_stars', 0) or 0)
            price_crystals = int(p.get('price_crystals', 0) or 0)

            await db.execute(
                """
                INSERT INTO saturday_offers (
                    offer_date, farm_key, name, income_per_hour, price_stars, price_crystals, status
                ) VALUES (?, ?, ?, ?, ?, ?, 'active')
                """,
                (offer_date, farm_key, name, income_per_hour, price_stars, price_crystals),
            )

        await db.commit()


async def get_active_saturday_offers() -> List[Dict]:
    offer_date = datetime.now().date().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT id, offer_date, farm_key, name, income_per_hour, price_stars, price_crystals
            FROM saturday_offers
            WHERE offer_date = ? AND status = 'active'
            ORDER BY id ASC
            """,
            (offer_date,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def buy_saturday_offer(user_id: int, offer_id: int) -> Tuple[bool, str]:
    if not await _is_saturday_now():
        return False, "Сегодня субботнего магазина нет"

    offer_date = datetime.now().date().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT * FROM saturday_offers
            WHERE id = ? AND offer_date = ? AND status = 'active'
            """,
            (int(offer_id), offer_date),
        )
        row = await cursor.fetchone()

    if not row:
        return False, "Оффер не найден"

    offer = dict(row)
    ps = int(offer.get('price_stars', 0) or 0)
    pc = int(offer.get('price_crystals', 0) or 0)
    farm_key = str(offer.get('farm_key'))
    name = str(offer.get('name', farm_key))

    if pc > 0:
        ok = await spend_crystals(user_id, pc)
        if not ok:
            return False, "Недостаточно кристаллов"
    else:
        ok = await spend_stars(user_id, ps)
        if not ok:
            return False, "Недостаточно звезд"

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO farms (user_id, farm_type, last_activated, is_active) VALUES (?, ?, ?, 0)",
            (user_id, farm_key, datetime.now().isoformat()),
        )
        await db.commit()

    return True, f"Куплено: {name}"


def _current_season_key() -> str:
    # ISO week key like 2025-W51
    now = datetime.now()
    iso_year, iso_week, _ = now.isocalendar()
    return f"{iso_year}-W{int(iso_week):02d}"


async def get_top_by_season_score(limit: int = 50) -> List[Dict]:
    limit = int(limit or 50)
    if limit <= 0:
        limit = 50
    if limit > 200:
        limit = 200

    season_key = _current_season_key()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        users_cur = await db.execute("SELECT user_id, internal_id, stars FROM users")
        users = await users_cur.fetchall()

        # ensure snapshot exists for this season for all users
        await db.execute("BEGIN IMMEDIATE")
        for u in users:
            uid = int(u[0])  # user_id is the first column
            stars = int(u[2] or 0)  # stars is the third column
            cursor = await db.execute(
                "SELECT 1 FROM season_snapshots WHERE user_id = ? AND season_key = ? LIMIT 1",
                (uid, season_key),
            )
            if not await cursor.fetchone():
                await db.execute(
                    "INSERT INTO season_snapshots (user_id, season_key, start_stars) VALUES (?, ?, ?)",
                    (uid, season_key, stars),
                )
        await db.commit()

        snap_cur = await db.execute(
            """
            SELECT u.user_id, u.internal_id, u.stars,
                   (u.stars - s.start_stars) AS season_score
            FROM users u
            JOIN season_snapshots s
              ON s.user_id = u.user_id AND s.season_key = ?
            ORDER BY season_score DESC
            LIMIT ?
            """,
            (season_key, limit),
        )
        rows = await snap_cur.fetchall()
        return [
            {
                'user_id': r['user_id'],
                'internal_id': r['internal_id'],
                'stars': r['stars'],
                'season_score': int(r['season_score'] or 0),
            }
            for r in rows
        ]


async def get_season_archive(limit_seasons: int = 5, limit_rows: int = 10) -> List[Dict]:
    limit_seasons = int(limit_seasons or 5)
    limit_rows = int(limit_rows or 10)
    if limit_seasons <= 0:
        limit_seasons = 5
    if limit_rows <= 0:
        limit_rows = 10
    if limit_rows > 50:
        limit_rows = 50

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        seasons_cur = await db.execute(
            "SELECT DISTINCT season_key FROM season_archive ORDER BY season_key DESC LIMIT ?",
            (limit_seasons,),
        )
        seasons = [r[0] for r in await seasons_cur.fetchall()]
        if not seasons:
            return []

        result: List[Dict] = []
        for sk in seasons:
            rows_cur = await db.execute(
                """
                SELECT rank, internal_id, season_score, reward_stars
                FROM season_archive
                WHERE season_key = ?
                ORDER BY rank ASC
                LIMIT ?
                """,
                (sk, limit_rows),
            )
            rows = await rows_cur.fetchall()
            result.append({'season': sk, 'rows': [dict(r) for r in rows]})
        return result


async def increment_user_stat(user_id: int, stat: str, amount: int = 1) -> None:
    stat = (stat or '').strip()
    if not stat:
        return
    amount = int(amount or 0)
    if amount == 0:
        return
    if stat not in ('stars_collected', 'farms_bought', 'cases_opened'):
        return

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)",
            (int(user_id),),
        )
        await db.execute(
            f"UPDATE user_stats SET {stat} = {stat} + ? WHERE user_id = ?",
            (amount, int(user_id)),
        )
        await db.commit()


async def get_user_stats(user_id: int) -> Dict:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT stars_collected, farms_bought, cases_opened FROM user_stats WHERE user_id = ?",
            (int(user_id),),
        )
        row = await cursor.fetchone()
        if not row:
            return {'stars_collected': 0, 'farms_bought': 0, 'cases_opened': 0}
        d = dict(row)
        d['stars_collected'] = int(d.get('stars_collected', 0) or 0)
        d['farms_bought'] = int(d.get('farms_bought', 0) or 0)
        d['cases_opened'] = int(d.get('cases_opened', 0) or 0)
        return d


async def get_user_achievement_ids(user_id: int) -> List[str]:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT achievement_id FROM user_achievements WHERE user_id = ?",
            (int(user_id),),
        )
        rows = await cursor.fetchall()
        return [str(r[0]) for r in rows]


async def create_nft_listing(user_id: int, nft_type: str, price: int, fee_pct: float = 0.0) -> Tuple[bool, str]:
    """
    Создаёт новое объявление на продажу NFT.
    Возвращает (success: bool, message: str).
    """
    if not nft_type or not isinstance(price, int) or price <= 0:
        return False, " Некорректные параметры"
    
    fee_pct = float(fee_pct or 0.0)
    if fee_pct < 0 or fee_pct > 100:
        fee_pct = 0.0

    async with aiosqlite.connect(DB_NAME) as db:
        # Проверяем, есть ли у пользователя NFT такого типа
        cursor = await db.execute(
            "SELECT id FROM nfts WHERE user_id = ? AND nft_type = ? LIMIT 1",
            (user_id, nft_type)
        )
        nft = await cursor.fetchone()
        if not nft:
            return False, f" У вас нет NFT типа {nft_type}"
        
        # Удаляем NFT у пользователя
        await db.execute(
            "DELETE FROM nfts WHERE id = ? AND user_id = ?",
            (nft[0], user_id)
        )
        
        # Создаём объявление
        await db.execute(
            """
            INSERT INTO nft_listings (seller_id, nft_type, price, fee_pct, status)
            VALUES (?, ?, ?, ?, 'active')
            """,
            (user_id, nft_type, price, fee_pct)
        )
        
        await db.commit()
        return True, f" NFT {nft_type} выставлен на продажу за {price}"
            
async def get_active_nft_listings(limit: int = 25, offset: int = 0) -> List[Dict]:
    """
    Возвращает активные объявления о продаже NFT.
    """
    limit = max(1, min(int(limit or 25), 100))
    offset = max(0, int(offset or 0))
    
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT l.*, u.internal_id as seller_internal_id
            FROM nft_listings l
            JOIN users u ON l.seller_id = u.user_id
            WHERE l.status = 'active'
            ORDER BY l.price ASC, l.created_at ASC
            LIMIT ? OFFSET ?
            """,
            (limit, offset)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def buy_nft_listing(buyer_id: int, listing_id: int) -> Tuple[bool, str]:
    """
    Покупка NFT с маркетплейса.
    Возвращает (success: bool, message: str).
    """
    if not listing_id or not buyer_id:
        return False, " Некорректные параметры"
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        
        try:
            # Получаем объявление
            cursor = await db.execute(
                """
                SELECT l.*, u.stars as buyer_stars
                FROM nft_listings l
                JOIN users u ON u.user_id = ?
                WHERE l.id = ? AND l.status = 'active' AND l.seller_id != ?
                """,
                (buyer_id, listing_id, buyer_id)
            )
            listing = await cursor.fetchone()
            
            if not listing:
                return False, " Объявление не найдено или уже куплено"
            
            listing = dict(listing)
            price = int(listing['price'])
            seller_id = int(listing['seller_id'])
            nft_type = listing['nft_type']
            fee_pct = float(listing.get('fee_pct', 0.0))
            
            # Проверяем баланс покупателя
            buyer_stars = int(listing.get('buyer_stars', 0))
            if buyer_stars < price:
                return False, f" Недостаточно звёзд. Нужно: {price} "
            
            # Вычисляем комиссию
            fee_amount = int(price * (fee_pct / 100.0))
            seller_gets = price - fee_amount
            
            # Списание у покупателя
            await db.execute(
                "UPDATE users SET stars = stars - ? WHERE user_id = ?",
                (price, buyer_id)
            )
            
            # Зачисление продавцу (за вычетом комиссии)
            if seller_gets > 0:
                await db.execute(
                    "UPDATE users SET stars = stars + ? WHERE user_id = ?",
                    (seller_gets, seller_id)
                )
            
            # Передаём NFT покупателю
            await db.execute(
                "INSERT INTO nfts (user_id, nft_type) VALUES (?, ?)",
                (buyer_id, nft_type)
            )
            
            # Помечаем объявление как проданное
            await db.execute(
                """
                UPDATE nft_listings 
                SET status = 'sold', 
                    sold_at = CURRENT_TIMESTAMP, 
                    buyer_id = ?
                WHERE id = ?
                """,
                (buyer_id, listing_id)
            )
            
            await db.commit()
            return True, f" Вы купили NFT {nft_type} за {price} "
            
        except Exception as e:
            await db.rollback()
            logger.error(f"Error in buy_nft_listing: {str(e)}", exc_info=True)
            return False, " Ошибка при покупке NFT"


async def upgrade_farm(farm_id: int, user_id: int) -> Tuple[bool, str]:
    """
    Upgrade a farm with a random chance of getting an income boost
    Returns: (success: bool, message: str)
    """
    upgrade_chances = [
        (2.0, 0.05, 100),   # 5% chance for 2x boost (красный)
        (1.5, 0.15, 50),    # 15% chance for 1.5x boost (оранжевый)
        (1.2, 0.30, 20),    # 30% chance for 1.2x boost (желтый)
        (1.0, 0.50, 0)      # 50% chance for no boost (зеленый)
    ]
    
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT f.id, f.user_id, f.farm_type, 
                   COALESCE(fu.income_boost, 1.0) as income_boost,
                   COALESCE(fu.upgrade_level, 0) as upgrade_level,
                   u.stars
            FROM farms f
            LEFT JOIN farm_upgrades fu ON f.id = fu.farm_id
            JOIN users u ON f.user_id = u.user_id
            WHERE f.id = ? AND f.user_id = ?
            """, 
            (farm_id, user_id)
        )
        farm = await cursor.fetchone()
        
        if not farm:
            return False, "Ферма не найдена или у вас нет к ней доступа"
        
        current_boost = farm['income_boost']
        current_level = farm['upgrade_level']
        
        # Calculate upgrade cost (increases with level)
        upgrade_cost = 100 * (current_level + 1)
        
        # Check if user has enough stars
        if farm['stars'] < upgrade_cost:
            return False, f"❌ Недостаточно звезд! Нужно {upgrade_cost}⭐"
        
        # Deduct stars
        await db.execute(
            "UPDATE users SET stars = stars - ? WHERE user_id = ?",
            (upgrade_cost, user_id)
        )
        
        # Roll for upgrade
        roll = random.random()
        cumulative = 0
        new_boost = 1.0
        boost_name = ""
        
        for boost, chance, _ in upgrade_chances:
            cumulative += chance
            if roll < cumulative:
                new_boost = boost
                if boost == 2.0:
                    boost_name = "🔥 КРИТИЧЕСКИЙ УРОВЕНЬ! x2.0"
                elif boost == 1.5:
                    boost_name = "🔶 Отличный результат! x1.5"
                elif boost == 1.2:
                    boost_name = "🔸 Неплохо! x1.2"
                else:
                    boost_name = "✅ Без изменений"
                break
        
        # Update farm upgrade
        new_level = current_level + 1
        
        if current_level > 0:
            await db.execute(
                """
                UPDATE farm_upgrades 
                SET income_boost = ?, upgrade_level = ?, last_upgraded = CURRENT_TIMESTAMP
                WHERE farm_id = ?
                """,
                (new_boost, new_level, farm_id)
            )
        else:
            await db.execute(
                """
                INSERT INTO farm_upgrades (farm_id, income_boost, upgrade_level, last_upgraded)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (farm_id, new_boost, new_level)
            )
        
        await db.commit()
        
        if new_boost > 1.0:
            return True, (
                f"{boost_name}\n"
                f"Уровень улучшения: {new_level} (+{int((new_boost-1)*100)}% к доходу)\n"
                f"Следующее улучшение: {100 * (new_level + 1)}⭐"
            )
        else:
            return True, (
                f"{boost_name}\n"
                f"Уровень: {new_level} (Доход не изменился)\n"
                f"Попробуйте снова!"
            )

async def get_farm_income(
    farm_type: str, 
    speed_level: int = 1, 
    cap_level: int = 1,
    farm_id: int = None
) -> Tuple[float, int]:
    """
    Calculate farm income based on type, levels and upgrades
    Returns: (income_per_hour, capacity)
    """
    from config import FARM_TYPES, CASE_FARM_TYPES
    
    # Get base income and capacity from config
    farm_defs = {**FARM_TYPES, **CASE_FARM_TYPES}
    if farm_type not in farm_defs:
        return 0.0, 0
        
    base_income = farm_defs[farm_type].get('income_per_hour', 10)
    base_capacity = farm_defs[farm_type].get('capacity', 10)
    
    # Apply speed and capacity level bonuses
    income = base_income * (1 + (speed_level - 1) * 0.2)
    capacity = int(base_capacity * (1 + (cap_level - 1) * 0.5))
    
    # Apply upgrade boost if farm_id is provided
    if farm_id:
        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT income_boost FROM farm_upgrades WHERE farm_id = ?", 
                (farm_id,)
            )
            result = await cursor.fetchone()
            if result and result['income_boost']:
                income *= result['income_boost']
    
    return income, capacity
