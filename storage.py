import os
import asyncio
import asyncpg
import logging

logger = logging.getLogger(__name__)

_pool = None
_pool_lock = asyncio.Lock()


def get_database_url():
    url = os.getenv("DATABASE_URL")

    if url:
        return url

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    database = os.getenv("PGDATABASE")

    if not all([host, user, password, database]):
        raise RuntimeError(
            "Database variables missing. Set DATABASE_URL or PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE in Railway worker variables."
        )

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


async def get_pool():
    global _pool

    if _pool is not None and not _pool._closed:
        return _pool

    async with _pool_lock:
        if _pool is not None and not _pool._closed:
            return _pool

        db_url = get_database_url()

        _pool = await asyncpg.create_pool(
            db_url,
            min_size=3,
            max_size=20,
            max_inactive_connection_lifetime=300,
            command_timeout=30,
        )

    return _pool


async def safe_acquire():
    pool = await get_pool()
    try:
        return pool.acquire(timeout=10)
    except Exception as e:
        logger.error(f"DB acquire error: {e}")
        raise

async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bin_cache (
                bin TEXT PRIMARY KEY,
                brand TEXT,
                card_type TEXT,
                level TEXT,
                bank TEXT,
                country TEXT,
                emoji TEXT,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS check_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                card_last4 TEXT NOT NULL,
                bin TEXT NOT NULL,
                status TEXT NOT NULL,
                message TEXT,
                gate TEXT NOT NULL,
                time_taken REAL,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS premium_users (
                user_id BIGINT PRIMARY KEY,
                expires_at TIMESTAMP NOT NULL,
                plan TEXT DEFAULT 'premium',
                activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS redeem_keys (
                key TEXT PRIMARY KEY,
                duration_days INTEGER NOT NULL,
                created_by BIGINT,
                redeemed_by BIGINT,
                redeemed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_check_history_user_time 
            ON check_history (user_id, checked_at)
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_by BIGINT,
                reason TEXT,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_groups (
                group_id BIGINT PRIMARY KEY,
                group_name TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_proxies (
                user_id BIGINT PRIMARY KEY,
                proxy TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                ip TEXT,
                country TEXT,
                is_rotating BOOLEAN DEFAULT FALSE,
                approval_code TEXT UNIQUE,
                approved_at TIMESTAMP,
                last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

async def get_cached_bin(bin_number: str) -> Optional[Dict]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow(
            'SELECT brand, card_type, level, bank, country, emoji FROM bin_cache WHERE bin = $1',
            bin_number[:6]
        )
        if row:
            return {
                'bin_info': f"{row['brand']} - {row['card_type']} - {row['level']}",
                'bank': row['bank'],
                'country': f"{row['country']} {row['emoji'] or ''}"
            }
        return None

async def cache_bin(bin_number: str, data: Dict):
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        try:
            brand = data.get('brand', 'Unknown').upper()
            card_type = data.get('type', 'Unknown').upper()
            level = data.get('level', 'Unknown').upper()
            bank = data.get('bank', 'Unknown')
            if isinstance(bank, dict):
                bank = bank.get('name', 'Unknown')
            bank = str(bank).upper()
            country = data.get('country_name', data.get('country', 'Unknown'))
            if isinstance(country, dict):
                country = country.get('name', 'Unknown')
            country = str(country).upper()
            emoji = data.get('country_flag', '')
            
            await conn.execute('''
                INSERT INTO bin_cache (bin, brand, card_type, level, bank, country, emoji)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (bin) DO UPDATE SET
                    brand = $2, card_type = $3, level = $4, bank = $5, country = $6, emoji = $7, cached_at = CURRENT_TIMESTAMP
            ''', bin_number[:6], brand, card_type, level, bank, country, emoji)
        except Exception as e:
            logger.warning(f"BIN cache error: {e}")

async def add_check_history(user_id: int, card: str, bin_num: str, status: str, message: str, gate: str, time_taken: float):
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        try:
            last4 = card[-4:] if len(card) >= 4 else card
            safe_message = (message[:200] if message else '')
            await conn.execute('''
                INSERT INTO check_history (user_id, card_last4, bin, status, message, gate, time_taken)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', user_id, last4, bin_num[:6], status, safe_message, gate, time_taken)
        except Exception as e:
            logger.warning(f"Check history error: {e}")

async def get_user_stats(user_id: int) -> Dict:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status IN ('approved', 'charged', 'ccn')) as hits,
                COUNT(*) FILTER (WHERE status IN ('dead', 'declined')) as dead,
                COUNT(*) FILTER (WHERE status = 'error') as errors
            FROM check_history
            WHERE user_id = $1 AND checked_at > NOW() - INTERVAL '24 hours'
        ''', user_id)
        return {
            'total_24h': row['total'],
            'hits_24h': row['hits'],
            'dead_24h': row['dead'],
            'errors_24h': row['errors']
        }

async def get_user_check_count_recent(user_id: int, seconds: int = 60) -> int:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow('''
            SELECT COUNT(*) as cnt FROM check_history
            WHERE user_id = $1 AND checked_at > NOW() - make_interval(secs := $2)
        ''', user_id, float(seconds))
        return row['cnt'] if row else 0

import secrets
import string

OWNER_IDS = [int(os.environ.get('BOT_OWNER_ID', '7593550190'))]

def generate_key(length=16):
    chars = string.ascii_uppercase + string.digits
    return 'JACK-' + ''.join(secrets.choice(chars) for _ in range(length))

async def create_redeem_key(created_by: int, duration_days: int) -> Optional[str]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        key = generate_key()
        try:
            await conn.execute('''
                INSERT INTO redeem_keys (key, duration_days, created_by)
                VALUES ($1, $2, $3)
            ''', key, duration_days, created_by)
            return key
        except Exception as e:
            logger.warning(f"Create key error: {e}")
            return None

async def redeem_key(user_id: int, key: str) -> Dict:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                'SELECT duration_days, redeemed_by FROM redeem_keys WHERE key = $1 FOR UPDATE',
                key.strip().upper()
            )
            
            if not row:
                return {'success': False, 'error': 'Invalid key'}
            
            if row['redeemed_by']:
                return {'success': False, 'error': 'Key already redeemed'}
            
            duration_days = row['duration_days']
            
            existing = await conn.fetchrow(
                'SELECT expires_at FROM premium_users WHERE user_id = $1',
                user_id
            )
            
            if existing and existing['expires_at'] > datetime.now():
                new_expires = existing['expires_at'] + timedelta(days=duration_days)
            else:
                new_expires = datetime.now() + timedelta(days=duration_days)
            
            await conn.execute('''
                INSERT INTO premium_users (user_id, expires_at)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET expires_at = $2
            ''', user_id, new_expires)
            
            await conn.execute('''
                UPDATE redeem_keys SET redeemed_by = $1, redeemed_at = CURRENT_TIMESTAMP
                WHERE key = $2
            ''', user_id, key.strip().upper())
            
            return {'success': True, 'expires_at': new_expires, 'days': duration_days}

async def is_premium(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow(
            'SELECT expires_at FROM premium_users WHERE user_id = $1',
            user_id
        )
        if row and row['expires_at'] > datetime.now():
            return True
        return False

async def get_premium_info(user_id: int) -> Optional[Dict]:
    if user_id in OWNER_IDS:
        return {'expires_at': datetime.now() + timedelta(days=9999), 'plan': 'Owner'}
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow(
            'SELECT expires_at, plan FROM premium_users WHERE user_id = $1',
            user_id
        )
        if row:
            return {'expires_at': row['expires_at'], 'plan': row['plan']}
        return None

async def add_premium_user(user_id: int, days: int) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        try:
            existing = await conn.fetchrow(
                'SELECT expires_at FROM premium_users WHERE user_id = $1',
                user_id
            )
            
            if existing and existing['expires_at'] > datetime.now():
                new_expires = existing['expires_at'] + timedelta(days=days)
            else:
                new_expires = datetime.now() + timedelta(days=days)
            
            await conn.execute('''
                INSERT INTO premium_users (user_id, expires_at)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET expires_at = $2
            ''', user_id, new_expires)
            return True
        except Exception as e:
            logger.warning(f"Add premium error: {e}")
            return False

async def get_all_premium_users() -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        rows = await conn.fetch('''
            SELECT user_id, expires_at, plan, activated_at
            FROM premium_users
            ORDER BY expires_at DESC
        ''')
        return [dict(row) for row in rows]

async def remove_premium_user(user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        result = await conn.execute(
            'DELETE FROM premium_users WHERE user_id = $1',
            user_id
        )
        return 'DELETE 1' in result

async def remove_premium_by_key(key: str) -> Dict:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow(
            'SELECT redeemed_by, duration_days FROM redeem_keys WHERE key = $1',
            key.strip().upper()
        )
        if not row:
            return {'success': False, 'error': 'Key not found'}
        if not row['redeemed_by']:
            return {'success': False, 'error': 'Key was never redeemed'}
        user_id = row['redeemed_by']
        await conn.execute('DELETE FROM premium_users WHERE user_id = $1', user_id)
        return {'success': True, 'user_id': user_id, 'days': row['duration_days']}

async def register_user(user_id: int, username: str = None, first_name: str = None):
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        try:
            await conn.execute('''
                INSERT INTO bot_users (user_id, username, first_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE SET username = $2, first_name = $3
            ''', user_id, username, first_name)
        except Exception as e:
            logger.warning(f"Register user error: {e}")

async def get_all_bot_user_ids() -> List[int]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        rows = await conn.fetch('SELECT user_id FROM bot_users')
        return [row['user_id'] for row in rows]

async def ban_user(user_id: int, banned_by: int, reason: str = None) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        try:
            await conn.execute('''
                INSERT INTO banned_users (user_id, banned_by, reason)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE SET banned_by = $2, reason = $3, banned_at = CURRENT_TIMESTAMP
            ''', user_id, banned_by, reason)
            return True
        except Exception as e:
            logger.warning(f"Ban user error: {e}")
            return False

async def unban_user(user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        result = await conn.execute('DELETE FROM banned_users WHERE user_id = $1', user_id)
        return 'DELETE 1' in result

async def is_banned(user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow('SELECT user_id FROM banned_users WHERE user_id = $1', user_id)
        return row is not None

async def add_admin(user_id: int, added_by: int) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        try:
            await conn.execute('''
                INSERT INTO bot_admins (user_id, added_by)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO NOTHING
            ''', user_id, added_by)
            return True
        except Exception as e:
            logger.warning(f"Add admin error: {e}")
            return False

async def remove_admin(user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        result = await conn.execute('DELETE FROM bot_admins WHERE user_id = $1', user_id)
        return 'DELETE 1' in result

async def is_admin(user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow('SELECT user_id FROM bot_admins WHERE user_id = $1', user_id)
        return row is not None

async def get_all_admins() -> list:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        rows = await conn.fetch('SELECT user_id, added_by, added_at FROM bot_admins ORDER BY added_at')
        return [dict(row) for row in rows]

async def is_admin_or_owner(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    return await is_admin(user_id)

async def set_user_proxy(user_id: int, proxy: str, ip: str = None, country: str = None, is_rotating: bool = False) -> str:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))
        expires = datetime.now() + timedelta(days=2)
        await conn.execute('''
            INSERT INTO user_proxies (user_id, proxy, status, ip, country, is_rotating, approval_code, expires_at, last_checked, created_at)
            VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
                proxy = $2, status = 'pending', ip = $3, country = $4, is_rotating = $5,
                approval_code = $6, expires_at = $7, last_checked = CURRENT_TIMESTAMP, created_at = CURRENT_TIMESTAMP, approved_at = NULL
        ''', user_id, proxy, ip, country, is_rotating, code, expires)
        return code

async def approve_user_proxy(code: str) -> Optional[Dict]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow(
            'SELECT user_id, proxy FROM user_proxies WHERE approval_code = $1',
            code.strip().upper()
        )
        if not row:
            return None
        expires = datetime.now() + timedelta(days=2)
        await conn.execute(
            "UPDATE user_proxies SET status = 'approved', approved_at = CURRENT_TIMESTAMP, expires_at = $2 WHERE approval_code = $1",
            code.strip().upper(), expires
        )
        return {'user_id': row['user_id'], 'proxy': row['proxy']}

async def decline_user_proxy(code: str) -> Optional[Dict]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow(
            'SELECT user_id, proxy FROM user_proxies WHERE approval_code = $1',
            code.strip().upper()
        )
        if not row:
            return None
        await conn.execute('DELETE FROM user_proxies WHERE approval_code = $1', code.strip().upper())
        return {'user_id': row['user_id'], 'proxy': row['proxy']}

async def get_user_proxy(user_id: int) -> Optional[Dict]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow(
            'SELECT proxy, status, ip, country, is_rotating, approved_at, expires_at, last_checked FROM user_proxies WHERE user_id = $1',
            user_id
        )
        if row:
            return dict(row)
        return None

async def get_approved_user_proxy(user_id: int) -> Optional[str]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        row = await conn.fetchrow(
            "SELECT proxy FROM user_proxies WHERE user_id = $1 AND status = 'approved' AND expires_at > CURRENT_TIMESTAMP",
            user_id
        )
        return row['proxy'] if row else None

async def remove_user_proxy(user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        result = await conn.execute('DELETE FROM user_proxies WHERE user_id = $1', user_id)
        return 'DELETE 1' in result

async def get_expired_proxies() -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        rows = await conn.fetch(
            "SELECT user_id, proxy FROM user_proxies WHERE status = 'approved' AND approved_at <= CURRENT_TIMESTAMP - INTERVAL '2 days'"
        )
        return [dict(r) for r in rows]

async def get_proxies_needing_check() -> List[Dict]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        rows = await conn.fetch(
            "SELECT user_id, proxy FROM user_proxies WHERE status = 'approved' AND last_checked < CURRENT_TIMESTAMP - INTERVAL '24 hours'"
        )
        return [dict(r) for r in rows]

async def update_proxy_last_checked(user_id: int):
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        await conn.execute(
            'UPDATE user_proxies SET last_checked = CURRENT_TIMESTAMP WHERE user_id = $1',
            user_id
        )

async def mark_proxy_dead(user_id: int):
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        await conn.execute('DELETE FROM user_proxies WHERE user_id = $1', user_id)

async def register_group(group_id: int, group_name: str = None):
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        await conn.execute(
            'INSERT INTO bot_groups (group_id, group_name) VALUES ($1, $2) ON CONFLICT (group_id) DO UPDATE SET group_name = $2',
            group_id, group_name
        )

async def get_all_bot_group_ids() -> List[int]:
    pool = await get_pool()
    async with pool.acquire(timeout=10) as conn:
        rows = await conn.fetch('SELECT group_id FROM bot_groups')
        return [row['group_id'] for row in rows]
