from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import os
import re
import aiohttp
import psutil
import platform
import time
import time as time_module
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import RetryAfter, TimedOut, NetworkError

stop_flags = {}
stop_reasons = {}
active_tasks = {}
active_sessions = {}
session_counter = 0
_session_lock = asyncio.Lock()

def generate_session_code(length=7):
    import random, string
    return ''.join(random.choices(string.ascii_uppercase, k=length))
BOT_START_TIME = datetime.now()

_premium_cache = {}
PREMIUM_CACHE_TTL = 120

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
OWNER_ID = int(os.getenv('BOT_OWNER_ID', '7593550190'))
OWNER_USERNAME = "E9krish"
BOT_USERNAME = "FN_CHECKERR_BOT"

REQUIRED_GROUP_ID = -1003066786395
REQUIRED_GROUP_LINK = "https://t.me/+XkvUejvYBE1hZTll"

MASS_CHECK_LIMIT = 2000
MASS_CHECK_COOLDOWN = 60
mass_check_cooldowns = {}

async def check_user_joined(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if user_id == OWNER_ID:
        return True
    try:
        member = await context.bot.get_chat_member(chat_id=REQUIRED_GROUP_ID, user_id=user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.warning(f"Group membership check failed for {user_id}: {e}")
        return False

async def send_join_required(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    from storage import is_banned as _is_banned
    if await _is_banned(user_id):
        target = update.message if update.message else (update.callback_query.message if update.callback_query else None)
        if target:
            await safe_send(target.reply_text("🚫 You are banned from using this bot."))
        return False
    chat_type = update.effective_chat.type
    chat_id = update.effective_chat.id
    is_other_group = chat_type in ['group', 'supergroup'] and chat_id != REQUIRED_GROUP_ID
    
    if chat_type in ['group', 'supergroup']:
        from storage import register_group
        asyncio.create_task(register_group(chat_id, update.effective_chat.title))
    
    if is_other_group:
        from storage import OWNER_IDS
        if user_id in OWNER_IDS or await cached_is_premium(user_id):
            return True
        target = update.message if update.message else update.callback_query.message
        await safe_send(target.reply_text("⚠️ Only premium users can use this bot in external groups.\n\nUse /start in @FN_CHECKERR_BOT for pricing."))
        return False
    
    joined = await check_user_joined(user_id, context)
    if joined:
        return True
    text = f"""🔒 𝗔𝗖𝗖𝗘𝗦𝗦 𝗗𝗘𝗡𝗜𝗘𝗗
━━━━━━━━━━━━━━━━━
𝚈𝚘𝚞 𝚖𝚞𝚜𝚝 𝚓𝚘𝚒𝚗 𝚘𝚞𝚛 𝚐𝚛𝚘𝚞𝚙 𝚝𝚘 𝚞𝚜𝚎 𝚝𝚑𝚒𝚜 𝚋𝚘𝚝."""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 𝗝𝗢𝗜𝗡 𝗚𝗥𝗢𝗨𝗣", url=REQUIRED_GROUP_LINK)],
        [InlineKeyboardButton("✅ 𝗩𝗘𝗥𝗜𝗙𝗬", callback_data="check_joined")]
    ])
    target = update.message if update.message else update.callback_query.message
    await safe_send(target.reply_text(text, reply_markup=keyboard))
    return False

PREMIUM_REQUIRED_MSG = f"""💎 𝗣𝗥𝗘𝗠𝗜𝗨𝗠 𝗢𝗡𝗟𝗬
━━━━━━━━━━━━━━━━━
𝚃𝚑𝚒𝚜 𝚐𝚊𝚝𝚎 𝚛𝚎𝚚𝚞𝚒𝚛𝚎𝚜 𝚙𝚛𝚎𝚖𝚒𝚞𝚖.

🥉 𝟳𝗗 ━ $𝟱 · 🥈 𝟭𝟱𝗗 ━ $𝟭𝟬 · 🥇 𝟯𝟬𝗗 ━ $𝟮𝟬

📩 @{OWNER_USERNAME} · /buy"""

from bin_lookup import lookup_bin
import bin_lookup as _bin_lookup_module

RATE_LIMIT_FREE = 15
RATE_LIMIT_PREMIUM = 60
RATE_LIMIT_WINDOW = 60

_user_rate_limits = {}
_rate_limit_lock = asyncio.Lock()

SESSION_STALE_TIMEOUT = 25200

async def cached_is_premium(user_id: int) -> bool:
    from storage import OWNER_IDS
    if user_id in OWNER_IDS:
        return True
    now = time.time()
    if user_id in _premium_cache:
        cached_val, cached_at = _premium_cache[user_id]
        if now - cached_at < PREMIUM_CACHE_TTL:
            return cached_val
    from storage import is_premium
    result = await is_premium(user_id)
    _premium_cache[user_id] = (result, now)
    return result

# ============ UTILITIES ============

def to_mono(text):
    normal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.,'!? "
    mono = "𝙰𝙱𝙲𝙳𝙴𝙵𝙶𝙷𝙸𝙹𝙺𝙻𝙼𝙽𝙾𝙿𝚀𝚁𝚂𝚃𝚄𝚅𝚆𝚇𝚈𝚉𝚊𝚋𝚌𝚍𝚎𝚏𝚐𝚑𝚒𝚓𝚔𝚕𝚖𝚗𝚘𝚙𝚚𝚛𝚜𝚝𝚞𝚟𝚠𝚡𝚢𝚣𝟶𝟷𝟸𝟹𝟺𝟻𝟼𝟽𝟾𝟿.,'!? "
    return ''.join(mono[normal.index(c)] if c in normal else c for c in str(text))

def to_bold(text):
    normal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    bold = "𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"
    return ''.join(bold[normal.index(c)] if c in normal else c for c in str(text))

async def send_hit_to_group(context, username: str, user_id: int, status: str, response: str, gateway: str):
    """Send approved/charged hit notification to the group"""
    try:
        user_link = f"tg://user?id={user_id}"
        bot_link  = f"https://t.me/{BOT_USERNAME}"
        pe = _PE
        mb = _MB()

        if status == 'charged':
            (mb
             .e(pe['c_ts']).t(' 𝗛𝗜𝗧 𝗗𝗘𝗧𝗘𝗖𝗧𝗘𝗗 ').e(pe['c_te']).t('\n')
             .t('━━━━━━━━━━━━━━━━━\n')
             .e(pe['hit_usr']).t('𝗨𝘀𝗲𝗿 ➜ ')
            )
            mb.link(to_mono(username), user_link)
            (mb
             .t('\n')
             .e(pe['hit_sts']).t('𝗦𝘁𝗮𝘁𝘂𝘀 ➜ 𝗖𝗵𝗮𝗿𝗴𝗲𝗱 ').e(pe['hit_c_se']).t('\n')
             .e(pe['d_rs']).t(f'𝗥𝗲𝘀𝗽𝗼𝗻𝘀𝗲 ➜ {to_mono(response[:80])}\n')
             .e(pe['gate']).t(f'𝗚𝗮𝘁𝗲 ➜ {to_mono(gateway)}\n')
             .t('━━━━━━━━━━━━━━━━━\n')
             .t('⟐ ')
            )
        else:
            (mb
             .e(pe['a_ts']).t(' 𝗛𝗜𝗧 𝗗𝗘𝗧𝗘𝗖𝗧𝗘𝗗 ').e(pe['hit_a_te']).t('\n')
             .t('━━━━━━━━━━━━━━━━━\n')
             .e(pe['hit_usr']).t('𝗨𝘀𝗲𝗿 ➜ ')
            )
            mb.link(to_mono(username), user_link)
            (mb
             .t('\n')
             .e(pe['hit_sts']).t('𝗦𝘁𝗮𝘁𝘂𝘀 ➜ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱 ').e(pe['a_se']).t('\n')
             .e(pe['d_rs']).t(f'𝗥𝗲𝘀𝗽𝗼𝗻𝘀𝗲 ➜ {to_mono(response[:80])}\n')
             .e(pe['gate']).t(f'𝗚𝗮𝘁𝗲 ➜ {to_mono(gateway)}\n')
             .t('━━━━━━━━━━━━━━━━━\n')
             .t('⟐ ')
            )

        mb.link(f'@{BOT_USERNAME}', bot_link)
        hit_text, hit_entities = mb.build()

        await safe_send(context.bot.send_message(
            chat_id=REQUIRED_GROUP_ID,
            text=hit_text,
            entities=hit_entities,
            disable_web_page_preview=True
        ))
    except Exception as e:
        logger.warning(f"Failed to send hit to group: {e}")

def sanitize_error(error_str: str) -> str:
    sensitive_patterns = ['Traceback', 'File "/', 'asyncpg', 'psycopg', 'DATABASE_URL', 'password', 'token', 'secret', 'Unauthorized', 'Proxy']
    error_str = str(error_str)[:100]
    error_str = re.sub(r'https?://[^\s\'">,]+', '[redacted]', error_str)
    for pattern in sensitive_patterns:
        if pattern.lower() in error_str.lower():
            return "Service temporarily unavailable. Try again."
    return error_str

async def safe_send(coro_or_fn, retries=2):
    for attempt in range(retries + 1):
        try:
            if callable(coro_or_fn):
                return await coro_or_fn()
            else:
                return await coro_or_fn
        except RetryAfter as e:
            if attempt < retries:
                await asyncio.sleep(e.retry_after + 0.5)
                if not callable(coro_or_fn):
                    logger.warning(f"Telegram flood control (no retry): retry_after={e.retry_after}")
                    return None
            else:
                logger.warning(f"Telegram flood control: retry_after={e.retry_after}")
                return None
        except (TimedOut, NetworkError) as e:
            if attempt < retries:
                await asyncio.sleep(1)
                if not callable(coro_or_fn):
                    logger.warning(f"Telegram network error (no retry): {e}")
                    return None
            else:
                logger.warning(f"Telegram network error: {e}")
                return None
        except Exception as e:
            logger.warning(f"Telegram send error: {e}")
            return None

async def check_rate_limit(user_id: int, is_prem: bool) -> bool:
    from storage import OWNER_IDS
    if user_id in OWNER_IDS:
        return True
    
    now = time_module.time()
    limit = RATE_LIMIT_PREMIUM if is_prem else RATE_LIMIT_FREE
    
    async with _rate_limit_lock:
        if user_id not in _user_rate_limits:
            _user_rate_limits[user_id] = []
        
        _user_rate_limits[user_id] = [t for t in _user_rate_limits[user_id] if now - t < RATE_LIMIT_WINDOW]
        
        if len(_user_rate_limits[user_id]) >= limit:
            return False
        
        _user_rate_limits[user_id].append(now)
        return True

async def cleanup_stale_sessions():
    now = time_module.time()
    stale = []
    for sid, info in list(active_sessions.items()):
        started = info.get('started_at', 0)
        if now - started > SESSION_STALE_TIMEOUT:
            stale.append(sid)
    
    for sid in stale:
        info = active_sessions.pop(sid, None)
        if info:
            uid = info.get('user_id')
            if uid:
                stop_flags.pop(uid, None)
                stop_reasons.pop(uid, None)
                task = active_tasks.pop(uid, None)
                if task and not task.done():
                    task.cancel()
            logger.info(f"Cleaned up stale session {sid}")

async def periodic_cleanup():
    while True:
        try:
            await asyncio.sleep(120)
            await cleanup_stale_sessions()
            
            now = time_module.time()
            async with _rate_limit_lock:
                expired_users = [uid for uid, times in _user_rate_limits.items() 
                               if not times or (now - max(times)) > RATE_LIMIT_WINDOW * 2]
                for uid in expired_users:
                    del _user_rate_limits[uid]
            
            expired_prem = [uid for uid, (val, ts) in _premium_cache.items() if now - ts > PREMIUM_CACHE_TTL * 2]
            for uid in expired_prem:
                _premium_cache.pop(uid, None)
            
            expired_cd = [uid for uid, ts in mass_check_cooldowns.items() if now - ts > MASS_CHECK_COOLDOWN * 3]
            for uid in expired_cd:
                mass_check_cooldowns.pop(uid, None)
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

_proxy_check_bot = None

async def periodic_proxy_check():
    await asyncio.sleep(300)
    while True:
        try:
            from storage import get_expired_proxies, get_proxies_needing_check, mark_proxy_dead, update_proxy_last_checked, remove_user_proxy
            from proxy_manager import validate_proxy

            expired = await get_expired_proxies()
            for ep in expired:
                uid = ep['user_id']
                await remove_user_proxy(uid)
                logger.info(f"[Proxy] Auto-removed proxy after 2 days for user {uid}")
                try:
                    if _proxy_check_bot:
                        await safe_send(_proxy_check_bot.send_message(
                            chat_id=uid,
                            text="""⛔ 𝗣𝗿𝗼𝘅𝘆 𝗔𝘂𝘁𝗼-𝗥𝗲𝗺𝗼𝘃𝗲𝗱
━━━━━━━━━━━━━━━━━
𝚈𝚘𝚞𝚛 𝚙𝚛𝚘𝚡𝚢 𝚑𝚊𝚜 𝚋𝚎𝚎𝚗 𝚊𝚞𝚝𝚘-
𝚛𝚎𝚖𝚘𝚟𝚎𝚍 𝚊𝚏𝚝𝚎𝚛 𝟸 𝚍𝚊𝚢𝚜.

𝙿𝚕𝚎𝚊𝚜𝚎 𝚊𝚍𝚍 𝚊 𝚗𝚎𝚠 𝚘𝚗𝚎:
/setproxy ip:port:user:pass
━━━━━━━━━━━━━━━━━"""
                        ))
                except:
                    pass

            to_check = await get_proxies_needing_check()
            for pc in to_check:
                uid = pc['user_id']
                proxy_str = pc['proxy']
                try:
                    is_alive = await validate_proxy(proxy_str, timeout=15)
                    if is_alive:
                        await update_proxy_last_checked(uid)
                        logger.info(f"[Proxy] Revalidated proxy for user {uid} - LIVE")
                    else:
                        await mark_proxy_dead(uid)
                        logger.info(f"[Proxy] Proxy dead for user {uid} - removed")
                        try:
                            if _proxy_check_bot:
                                await safe_send(_proxy_check_bot.send_message(
                                    chat_id=uid,
                                    text="""⛔ 𝗬𝗼𝘂𝗿 𝗣𝗿𝗼𝘅𝗶𝗲𝘀 𝗔𝗿𝗲 𝗗𝗲𝗮𝗱 𝗢𝗿 𝗜𝗻𝘃𝗮𝗹𝗶𝗱 ⛔️
━━━━━━━━━━━━━━━━━
𝙵𝚒𝚛𝚜𝚝 𝚁𝚎𝚖𝚘𝚟𝚎 𝚈𝚘𝚞𝚛 𝙿𝚛𝚘𝚡𝚢
𝚃𝚑𝚛𝚘𝚞𝚐𝚑 /rproxy 𝚊𝚗𝚍 𝙿𝚕𝚎𝚊𝚜𝚎
𝙰𝚍𝚍 𝙽𝚎𝚠 𝙾𝚗𝚎 𝚃𝚑𝚛𝚘𝚞𝚐𝚑
/setproxy ip:port:user:pass
━━━━━━━━━━━━━━━━━"""
                                ))
                        except:
                            pass
                except Exception as e:
                    logger.warning(f"[Proxy] Revalidation error for user {uid}: {e}")
                await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"Proxy check error: {e}")
        
        await asyncio.sleep(3600)

def _utf16_len(s: str) -> int:
    return len(s.encode('utf-16-le')) // 2

class _MB:
    """Message builder that tracks UTF-16 offsets for Telegram entities."""
    __slots__ = ('text', 'entities')

    def __init__(self):
        self.text = ''
        self.entities = []

    def _off(self):
        return _utf16_len(self.text)

    def t(self, s: str) -> '_MB':
        self.text += s
        return self

    def e(self, eid) -> '_MB':
        off = self._off()
        self.text += '🌟'  # SMP placeholder = 2 UTF-16 code units
        self.entities.append(MessageEntity(type='custom_emoji', offset=off, length=2, custom_emoji_id=str(eid)))
        return self

    def code(self, s: str) -> '_MB':
        off = self._off()
        self.text += s
        self.entities.append(MessageEntity(type='code', offset=off, length=_utf16_len(s)))
        return self

    def link(self, s: str, url: str) -> '_MB':
        off = self._off()
        self.text += s
        self.entities.append(MessageEntity(type='text_link', offset=off, length=_utf16_len(s), url=url))
        return self

    def build(self):
        return self.text, self.entities

_PE = {
    'card':    '5445353829304387411',
    'sts':     '6271786398404055377',
    'bin':     '5204242830687494041',
    'bank':    '5264895611517300926',
    'country': '5224450179368767019',
    'gate':    '5801044672658805468',
    'time':    '6275942492227504481',
    'by':      '5798542313043006836',
    'd_ts': '6269316311172518259',
    'd_te': '6271786398404055377',
    'd_se': '6269316311172518259',
    'd_rs': '6152108311522054901',
    'a_ts': '6278555627639801385',
    'a_te': '6334832347896091123',
    'a_se': '6278555627639801385',
    'a_rs': '6296577138615125756',
    'a_rm': '6296577138615125756',
    'c_ts': '5463156928307801722',
    'c_te': '5463046637842608206',
    'c_se': '5231005931550030290',
    'c_rs': '6296577138615125756',
    'hit_a_te':  '5999193233672902329',
    'hit_usr':   '6242490508082418175',
    'hit_sts':   '5801093729775260336',
    'hit_c_se':  '5998905410734529922',
}

def format_response(card, status, result, bin_data, gate, time_taken, username, user_id=None):
    bot_link  = f"https://t.me/{BOT_USERNAME}"
    user_link = f"tg://user?id={user_id}" if user_id else bot_link
    safe_bin  = bin_data or {}
    pe        = _PE

    if status == 'charged':
        ts, te, header  = pe['c_ts'], pe['c_te'], '𝗖𝗛𝗔𝗥𝗚𝗘𝗗'
        sts_txt, sts_e  = '𝙰𝚙𝚙𝚛𝚘𝚟𝚎𝚍!', pe['c_se']
        rs, rmode       = pe['c_rs'], 'plain'
    elif status in ('approved', 'live', 'ccn'):
        ts, te, header  = pe['a_ts'], pe['a_te'], '𝗔𝗣𝗣𝗥𝗢𝗩𝗘𝗗'
        sts_txt, sts_e  = '𝙰𝚙𝚙𝚛𝚘𝚟𝚎𝚍!', pe['a_se']
        rs, rmode       = pe['a_rs'], 'approved'
    elif status == '3ds':
        ts, te, header  = pe['d_ts'], pe['d_te'], '𝗗𝗘𝗖𝗟𝗜𝗡𝗘𝗗'
        sts_txt, sts_e  = '𝟹𝙳𝚂 𝚁𝚎𝚚𝚞𝚒𝚛𝚎𝚍', pe['d_se']
        rs, rmode       = pe['d_rs'], 'plain'
    else:
        ts, te, header  = pe['d_ts'], pe['d_te'], '𝗗𝗘𝗖𝗟𝗜𝗡𝗘𝗗'
        sts_txt, sts_e  = '𝙳𝚎𝚊𝚍!', pe['d_se']
        rs, rmode       = pe['d_rs'], 'plain'

    mb = _MB()
    (mb
     .e(ts).t(f' 𝗙𝗡 𝗖𝗛𝗘𝗖𝗞𝗘𝗥 ➜ {header} ').e(te).t('\n')
     .t('━━━━━━━━━━━━━━━━━\n')
     .e(pe['card']).t('𝗖𝗮𝗿𝗱 ➜ ').code(card).t('\n')
     .e(pe['sts']).t(f'𝗦𝘁𝗮𝘁𝘂𝘀 ➜ {sts_txt} ').e(sts_e).t('\n')
     .e(rs).t('𝗥𝗲𝘀𝘂𝗹𝘁 ➜ ')
    )
    if rmode == 'approved':
        mb.t('𝙰𝚙𝚙𝚛𝚘𝚟𝚎𝚍! ').e(pe['a_rm']).t(f' | {to_mono(result)}')
    else:
        mb.t(to_mono(result))
    (mb
     .t('\n')
     .t('━━━━━━━━━━━━━━━━━\n')
     .e(pe['bin']).t(f"𝗕𝗶𝗻 ➜ {to_mono(safe_bin.get('bin_info', 'Unknown'))}\n")
     .e(pe['bank']).t(f"𝗕𝗮𝗻𝗸 ➜ {to_mono(safe_bin.get('bank', 'Unknown'))}\n")
     .e(pe['country']).t(f"𝗖𝗼𝘂𝗻𝘁𝗿𝘆 ➜ {to_mono(safe_bin.get('country', 'Unknown'))}\n")
     .t('━━━━━━━━━━━━━━━━━\n')
     .e(pe['gate']).t(f'𝗚𝗮𝘁𝗲 ➜ {gate}\n')
     .e(pe['time']).t(f'𝗧𝗶𝗺𝗲 ➜ {to_mono(f"{float(time_taken):.2f}s")}\n')
     .e(pe['by']).t('𝗕𝘆 ➜ ')
    )
    mb.link(to_mono(username), user_link)
    mb.t('\n⟐ ')
    mb.link('𝗙𝗡 𝗖𝗛𝗘𝗖𝗞𝗘𝗥', bot_link)
    return mb.build()

# ============ START COMMAND ============

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    from storage import is_banned, register_user
    if await is_banned(user_id):
        await update.message.reply_text("🚫 You are banned from using this bot.")
        return
    
    if not await send_join_required(update, context):
        return
    
    uname = update.effective_user.username
    fname = update.effective_user.first_name
    await register_user(user_id, uname, fname)
    
    username = fname or "User"
    
    from storage import is_premium, OWNER_IDS
    is_prem = await is_premium(user_id)
    tier_text = "👑 Owner" if user_id in OWNER_IDS else ("💎 Premium" if is_prem else "🆓 Free")
    user_link = f"tg://user?id={user_id}"
    
    text = f"""⚡ 𝗙𝗡 𝗖𝗛𝗘𝗖𝗞𝗘𝗥
━━━━━━━━━━━━━━━━━
𝗨𝘀𝗲𝗿 ➜ <a href="{user_link}">{username}</a>
𝗧𝗶𝗲𝗿 ➜ {tier_text}
𝗕𝗼𝘁 ➜ @{BOT_USERNAME}
━━━━━━━━━━━━━━━━━
𝙵𝚊𝚜𝚝 · 𝚁𝚎𝚕𝚒𝚊𝚋𝚕𝚎 · 𝙿𝚘𝚠𝚎𝚛𝚏𝚞𝚕"""
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ 𝗚𝗔𝗧𝗘𝗦", callback_data="menu_gates"),
         InlineKeyboardButton("🛠 𝗧𝗢𝗢𝗟𝗦", callback_data="menu_tools")],
        [InlineKeyboardButton("👤 𝗢𝗪𝗡𝗘𝗥", callback_data="menu_owner"),
         InlineKeyboardButton("💎 𝗣𝗥𝗜𝗖𝗜𝗡𝗚", callback_data="menu_pricing")],
        [InlineKeyboardButton("💬 𝗝𝗢𝗜𝗡 𝗚𝗥𝗢𝗨𝗣", url="https://t.me/fnchecker_chat")]
    ])
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode='HTML', disable_web_page_preview=True)

# ============ MENU CALLBACKS ============

async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        data = query.data
        
        if data == "menu_gates":
            user_id = update.effective_user.id
            username = update.effective_user.first_name or "User"
            from storage import is_premium, OWNER_IDS
            is_prem = await is_premium(user_id)
            tier_text = "👑 Owner" if user_id in OWNER_IDS else ("💎 Premium" if is_prem else "🆓 Free")
            text = f"""⚡ 𝗚𝗔𝗧𝗘 𝗦𝗘𝗟𝗘𝗖𝗧𝗜𝗢𝗡
━━━━━━━━━━━━━━━━━
𝗨𝘀𝗲𝗿 ➜ {username} · {tier_text}
𝙲𝚑𝚘𝚘𝚜𝚎 𝚢𝚘𝚞𝚛 𝚌𝚑𝚎𝚌𝚔𝚒𝚗𝚐 𝚖𝚘𝚍𝚎"""
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 𝗔𝗨𝗧𝗛", callback_data="gates_auth"),
                 InlineKeyboardButton("💰 𝗖𝗛𝗔𝗥𝗚𝗘𝗗", callback_data="gates_charged")],
                [InlineKeyboardButton("📦 𝗠𝗔𝗦𝗦 𝗖𝗛𝗘𝗖𝗞", callback_data="gates_mass")],
                [InlineKeyboardButton("◀ 𝗕𝗔𝗖𝗞", callback_data="menu_main")]
            ])
            await query.edit_message_text(text, reply_markup=keyboard)
        
        elif data == "gates_auth":
            text = """🔐 𝗔𝗨𝗧𝗛 𝗚𝗔𝗧𝗘𝗦
━━━━━━━━━━━━━━━━━
/b3 ➜ 𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 𝙲𝚟𝚟 · $𝟶 · ✅
  ↳ 𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 𝙲𝚅𝚅 𝚊𝚞𝚝𝚑 𝚌𝚑𝚎𝚌𝚔
━━━━━━━━━━━━━━━━━
𝚃𝚒𝚎𝚛 ➜ 𝙵𝚛𝚎𝚎 + 𝙿𝚛𝚎𝚖𝚒𝚞𝚖"""
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀ 𝗕𝗔𝗖𝗞", callback_data="menu_gates")]])
            await query.edit_message_text(text, reply_markup=keyboard)
        
        elif data == "gates_charged":
            text = """💰 𝗖𝗛𝗔𝗥𝗚𝗘𝗗 𝗚𝗔𝗧𝗘𝗦
━━━━━━━━━━━━━━━━━
/pp ➜ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟻 · ✅
  ↳ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟻 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝚎𝚛𝚒𝚏𝚢

/pp2 ➜ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟸 · ✅
  ↳ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝟸

/sc ➜ 𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷 · ✅
  ↳ 𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷 𝚌𝚑𝚊𝚛𝚐𝚎

/sc2 ➜ 𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚟𝟸 · ✅
  ↳ 𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝟸

/pp3 ➜ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟹 · ✅
  ↳ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝟹

/pp4 ➜ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟺 · ✅
  ↳ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝟺
━━━━━━━━━━━━━━━━━
𝚃𝚒𝚎𝚛 ➜ 𝙿𝚛𝚎𝚖𝚒𝚞𝚖 𝙾𝚗𝚕𝚢"""
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀ 𝗕𝗔𝗖𝗞", callback_data="menu_gates")]])
            await query.edit_message_text(text, reply_markup=keyboard)
        
        elif data == "gates_mass":
            text = """📦 𝗠𝗔𝗦𝗦 𝗖𝗛𝗘𝗖𝗞
━━━━━━━━━━━━━━━━━
𝚁𝚎𝚙𝚕𝚢 𝚝𝚘 𝚊 .𝚝𝚡𝚝 𝚏𝚒𝚕𝚎

/mpp ➜ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟻 · ✅
  ↳ 𝙼𝚊𝚜𝚜 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟻 𝚌𝚑𝚊𝚛𝚐𝚎

/mpp2 ➜ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟸 · ✅
  ↳ 𝙼𝚊𝚜𝚜 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝟸

/mb3 ➜ 𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 · ✅
  ↳ 𝙼𝚊𝚜𝚜 𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 𝙲𝚅𝚅 𝚌𝚑𝚎𝚌𝚔

/msc ➜ 𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷 · ✅
  ↳ 𝙼𝚊𝚜𝚜 𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷 𝚌𝚑𝚊𝚛𝚐𝚎

/msc2 ➜ 𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚟𝟸 · ✅
  ↳ 𝙼𝚊𝚜𝚜 𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝟸

/mpp3 ➜ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟹 · ✅
  ↳ 𝙼𝚊𝚜𝚜 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝟹

/mpp4 ➜ 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟺 · ✅
  ↳ 𝙼𝚊𝚜𝚜 𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚌𝚑𝚊𝚛𝚐𝚎 𝚟𝟺"""
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀ 𝗕𝗔𝗖𝗞", callback_data="menu_gates")]])
            await query.edit_message_text(text, reply_markup=keyboard)
        
        elif data == "menu_tools":
            text = """🛠 𝗧𝗢𝗢𝗟𝗦
━━━━━━━━━━━━━━━━━
/stats ➜ 𝚅𝚒𝚎𝚠 𝟸𝟺𝚑 𝚌𝚑𝚎𝚌𝚔𝚒𝚗𝚐 𝚜𝚝𝚊𝚝𝚜
/stop ➜ 𝚂𝚝𝚘𝚙 𝚊𝚌𝚝𝚒𝚟𝚎 𝚖𝚊𝚜𝚜 𝚌𝚑𝚎𝚌𝚔"""
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀ 𝗕𝗔𝗖𝗞", callback_data="menu_main")]])
            await query.edit_message_text(text, reply_markup=keyboard)
        
        elif data == "menu_owner":
            text = f"""👤 𝗢𝗪𝗡𝗘𝗥
━━━━━━━━━━━━━━━━━
𝗗𝗲𝘃 ➜ @{OWNER_USERNAME}
𝗦𝘂𝗽𝗽𝗼𝗿𝘁 ➜ @{OWNER_USERNAME}"""
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀ 𝗕𝗔𝗖𝗞", callback_data="menu_main")]])
            await query.edit_message_text(text, reply_markup=keyboard)
        
        elif data == "menu_pricing":
            text = f"""💎 𝗣𝗥𝗘𝗠𝗜𝗨𝗠 𝗣𝗟𝗔𝗡𝗦
━━━━━━━━━━━━━━━━━
🥉 𝟳𝗗 ━ $𝟱 · 🥈 𝟭𝟱𝗗 ━ $𝟭𝟬 · 🥇 𝟯𝟬𝗗 ━ $𝟮𝟬
━━━━━━━━━━━━━━━━━
📩 @{OWNER_USERNAME}"""
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("◀ 𝗕𝗔𝗖𝗞", callback_data="menu_main")]])
            await query.edit_message_text(text, reply_markup=keyboard)
        
        elif data == "menu_main":
            user_id = update.effective_user.id
            username = update.effective_user.first_name or "User"
            from storage import is_premium, OWNER_IDS
            is_prem = await is_premium(user_id)
            tier_text = "👑 Owner" if user_id in OWNER_IDS else ("💎 Premium" if is_prem else "🆓 Free")
            user_link = f"tg://user?id={user_id}"
            
            text = f"""⚡ 𝗙𝗡 𝗖𝗛𝗘𝗖𝗞𝗘𝗥
━━━━━━━━━━━━━━━━━
𝗨𝘀𝗲𝗿 ➜ <a href="{user_link}">{username}</a>
𝗧𝗶𝗲𝗿 ➜ {tier_text}
𝗕𝗼𝘁 ➜ @{BOT_USERNAME}
━━━━━━━━━━━━━━━━━
𝙵𝚊𝚜𝚝 · 𝚁𝚎𝚕𝚒𝚊𝚋𝚕𝚎 · 𝙿𝚘𝚠𝚎𝚛𝚏𝚞𝚕"""
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ 𝗚𝗔𝗧𝗘𝗦", callback_data="menu_gates"),
                 InlineKeyboardButton("🛠 𝗧𝗢𝗢𝗟𝗦", callback_data="menu_tools")],
                [InlineKeyboardButton("👤 𝗢𝗪𝗡𝗘𝗥", callback_data="menu_owner"),
                 InlineKeyboardButton("💎 𝗣𝗥𝗜𝗖𝗜𝗡𝗚", callback_data="menu_pricing")],
                [InlineKeyboardButton("💬 𝗝𝗢𝗜𝗡 𝗚𝗥𝗢𝗨𝗣", url="https://t.me/fnchecker_chat")]
            ])
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML', disable_web_page_preview=True)
        
        await query.answer()
    except Exception as e:
        await query.answer("Error occurred", show_alert=True)
        logger.error(f"Menu callback error: {e}")

async def check_joined_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        user_id = update.effective_user.id
        joined = await check_user_joined(user_id, context)
        
        if joined:
            await query.answer("✅ Verified! You can now use the bot.", show_alert=True)
            username = update.effective_user.first_name or "User"
            from storage import is_premium, OWNER_IDS
            is_prem = await is_premium(user_id)
            tier_text = "👑 Owner" if user_id in OWNER_IDS else ("💎 Premium" if is_prem else "🆓 Free")
            user_link = f"tg://user?id={user_id}"
            text = f"""⚡ 𝗙𝗡 𝗖𝗛𝗘𝗖𝗞𝗘𝗥
━━━━━━━━━━━━━━━━━
𝗨𝘀𝗲𝗿 ➜ <a href="{user_link}">{username}</a>
𝗧𝗶𝗲𝗿 ➜ {tier_text}
𝗕𝗼𝘁 ➜ @{BOT_USERNAME}
━━━━━━━━━━━━━━━━━
𝙵𝚊𝚜𝚝 · 𝚁𝚎𝚕𝚒𝚊𝚋𝚕𝚎 · 𝙿𝚘𝚠𝚎𝚛𝚏𝚞𝚕"""
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ 𝗚𝗔𝗧𝗘𝗦", callback_data="menu_gates"),
                 InlineKeyboardButton("🛠 𝗧𝗢𝗢𝗟𝗦", callback_data="menu_tools")],
                [InlineKeyboardButton("👤 𝗢𝗪𝗡𝗘𝗥", callback_data="menu_owner"),
                 InlineKeyboardButton("💎 𝗣𝗥𝗜𝗖𝗜𝗡𝗚", callback_data="menu_pricing")],
                [InlineKeyboardButton("💬 𝗝𝗢𝗜𝗡 𝗚𝗥𝗢𝗨𝗣", url="https://t.me/fnchecker_chat")]
            ])
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML', disable_web_page_preview=True)
        else:
            await query.answer("❌ You haven't joined the group yet. Please join first!", show_alert=True)
    except Exception as e:
        await query.answer("Error occurred", show_alert=True)
        logger.error(f"Check joined callback error: {e}")

async def stop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        if query.data.startswith("stop_"):
            target_user_id = int(query.data.split("_")[1])
            caller_id = update.effective_user.id
            from storage import OWNER_IDS
            if caller_id != target_user_id and caller_id not in OWNER_IDS:
                await query.answer("Only the person who started this check can stop it.", show_alert=True)
                return
            stop_flags[target_user_id] = True
            await query.answer("Stopping...")
    except Exception as e:
        await query.answer("Error occurred", show_alert=True)
        logger.error(f"Stop callback error: {e}")

# ============ PAYPAL $5 GATE (PREMIUM) ============

async def pp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    is_our_group = update.effective_chat.id == REQUIRED_GROUP_ID
    from storage import OWNER_IDS
    is_prem = await cached_is_premium(user_id)
    if user_id not in OWNER_IDS and not is_prem and not is_our_group:
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'pp', is_mass=False)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    if not await check_rate_limit(user_id, is_prem):
        await safe_send(update.message.reply_text(f"Slow down. Max {RATE_LIMIT_PREMIUM} checks per minute."))
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /pp CC|MM|YY|CVV")
        return
    
    card_input = context.args[0].strip()
    parts = card_input.split('|')
    
    if len(parts) < 4:
        await update.message.reply_text("Invalid format. Use: /pp CC|MM|YY|CVV")
        return
    
    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    username = update.effective_user.username or update.effective_user.first_name or "User"
    
    status_msg = await safe_send(update.message.reply_text("𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐..."))
    if not status_msg:
        return
    
    try:
        from gates.gate1 import check_card
        from storage import add_check_history
        
        bin_data, result = await asyncio.gather(lookup_bin(cc), check_card(cc, mm, yy, cvv, user_proxies=user_proxies))
        
        status = result.get('status', 'error')
        message = result.get('message', 'Unknown error')
        time_taken = result.get('time', 0)
        
        logger.info(f"[PayPal $5] User: {username} ({user_id}) | Card: {card_input} | Status: {status} | Raw: {message}")
        
        try:
            await add_check_history(user_id, cc, cc[:6], status, message, 'PayPal Charge $5', time_taken)
        except:
            pass
        
        response, _r_ents = format_response(card_input, status, message, bin_data, '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟻', time_taken, username, user_id)
        await safe_send(status_msg.edit_text(response, entities=_r_ents, disable_web_page_preview=True))
        
        if status in ['approved', 'charged', 'ccn']:
            await send_hit_to_group(context, username, user_id, status, message, 'PayPal $5')
        
    except Exception as e:
        await safe_send(status_msg.edit_text(sanitize_error(str(e))))

async def check_single_card_pp(cc, mm, yy, cvv, username="Unknown", user_proxies=None):
    from gates.gate1 import check_card
    bin_data = await lookup_bin(cc)
    result = await check_card(cc, mm, yy, cvv, user_proxies=user_proxies)
    card = f"{cc}|{mm}|{yy}|{cvv}"
    logger.info(f"[PayPal $5 Mass] Card: {card} | Status: {result.get('status', 'error')} | Raw: {result.get('message', 'Unknown')} CHECKED BY {username}")
    return {
        'card': card,
        'status': result.get('status', 'error'),
        'message': result.get('message', 'Unknown'),
        'bin_data': bin_data,
        'time': result.get('time', 0)
    }


# ============ MASS PAYPAL $5 (BATCHED) ============

async def _run_mass_check_pp(update, context, cards, status_msg, session_id, user_id, username, user_proxies=None):
    chat_id = update.effective_chat.id
    total = len(cards)
    success, approved, dead, checked = 0, 0, 0, 0
    current_response = "𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐..."
    
    def build_buttons():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💎 {current_response[:35]}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {checked}/{total}", callback_data="x")],
            [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
        ])
    
    try:
        from storage import add_check_history
        
        for i in range(0, total, 10):
            if stop_flags.get(user_id):
                break
            
            batch = cards[i:i+10]
            tasks = [asyncio.create_task(check_single_card_pp(cc, mm, yy, cvv, username, user_proxies=user_proxies)) for cc, mm, yy, cvv in batch]
            
            for coro in asyncio.as_completed(tasks):
                if stop_flags.get(user_id):
                    for t in tasks:
                        t.cancel()
                    break
                
                try:
                    res = await coro
                except Exception as card_err:
                    checked += 1
                    dead += 1
                    current_response = sanitize_error(str(card_err))[:40]
                    logger.error(f"Mass PP card processing error: {card_err}")
                    await safe_send(status_msg.edit_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟻 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
                    continue
                
                checked += 1
                current_response = str(res.get('message', 'Unknown'))[:40]
                card_parts = res.get('card', '||||').split('|')
                
                try:
                    await add_check_history(user_id, card_parts[0], card_parts[0][:6], res.get('status', 'error'), res.get('message', ''), 'PayPal Charge $5', res.get('time', 0))
                except:
                    pass
                
                if res.get('status') == 'charged':
                    success += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟻', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'PayPal $5')
                elif res.get('status') in ['approved', 'ccn']:
                    approved += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟻', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'PayPal $5')
                else:
                    dead += 1
                
                await safe_send(status_msg.edit_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟻 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
            
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
            if i + 10 < total and not stop_flags.get(user_id):
                await asyncio.sleep(5)
        
        status_text = "𝚂𝚝𝚘𝚙𝚙𝚎𝚍" if stop_flags.get(user_id) else "𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎"
        await safe_send(status_msg.edit_text(f"⚡ 𝗣𝗮𝘆𝗣𝗮𝗹 $𝟱 𝗠𝗮𝘀𝘀 · {status_text}\n━━━━━━━━━━━━━━━━━", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {total}", callback_data="x")]
        ])))
        
    except asyncio.CancelledError:
        logger.warning(f"Mass PP check cancelled for user {user_id}")
        await safe_send(update.message.reply_text(f"Mass check interrupted. Checked {checked}/{total} cards."))
    except Exception as e:
        logger.error(f"Mass PP check error: {e}")
        await safe_send(update.message.reply_text(sanitize_error(str(e))))
    finally:
        stop_flags.pop(user_id, None)
        stop_reasons.pop(user_id, None)
        active_sessions.pop(session_id, None)
        active_tasks.pop(user_id, None)
        if user_id != OWNER_ID:
            mass_check_cooldowns[user_id] = time.time()

async def mpp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    from storage import is_premium, OWNER_IDS
    if user_id not in OWNER_IDS and not await is_premium(user_id):
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'pp', is_mass=True)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    chat_id = update.effective_chat.id
    if user_id in active_tasks:
        await update.message.reply_text("You already have an active mass check. Use /stop first.")
        return
    
    if user_id != OWNER_ID and user_id in mass_check_cooldowns:
        elapsed = time.time() - mass_check_cooldowns[user_id]
        if elapsed < MASS_CHECK_COOLDOWN:
            remaining = int(MASS_CHECK_COOLDOWN - elapsed)
            await update.message.reply_text(f"⏳ Cooldown active. Wait {remaining}s before starting another mass check.")
            return
    
    reply = update.message.reply_to_message
    if not reply or not reply.document:
        await update.message.reply_text("Reply to a file containing cards in CC|MM|YY|CVV format")
        return
    
    try:
        file = await reply.document.get_file()
        content = (await file.download_as_bytearray()).decode('utf-8')
        
        cards = []
        for line in content.split('\n'):
            parts = line.strip().split('|')
            if len(parts) >= 4 and len(parts[0]) >= 13:
                cards.append((parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()))
        
        if not cards:
            await update.message.reply_text("No valid cards found")
            return
        
        from storage import OWNER_IDS
        if user_id not in OWNER_IDS and len(cards) > MASS_CHECK_LIMIT:
            cards = cards[:MASS_CHECK_LIMIT]
            await update.message.reply_text(f"Limiting to {MASS_CHECK_LIMIT} cards per session")
        
        username = update.effective_user.username or update.effective_user.first_name or "User"
        
        global session_counter
        async with _session_lock:
            session_counter += 1
            session_id = f"session_{user_id}_{session_counter}"
        
        stop_flags[user_id] = False
        scode = generate_session_code()
        active_sessions[session_id] = {'user_id': user_id, 'total': len(cards), 'chat_id': chat_id, 'started_at': time_module.time(), 'username': username, 'gate': 'PayPal $5', 'code': scode}
        
        try:
            status_msg = await update.message.reply_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟻 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {len(cards)}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Starting...", callback_data="x")],
                [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
            ]))
        except Exception:
            stop_flags.pop(user_id, None)
            active_sessions.pop(session_id, None)
            return
        
        task = asyncio.create_task(_run_mass_check_pp(update, context, cards, status_msg, session_id, user_id, username, user_proxies=user_proxies))
        active_tasks[user_id] = task
        
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))


# ============ PAYPAL $2 v2 GATE (PREMIUM) ============

async def pp2_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    is_our_group = update.effective_chat.id == REQUIRED_GROUP_ID
    from storage import OWNER_IDS
    is_prem = await cached_is_premium(user_id)
    if user_id not in OWNER_IDS and not is_prem and not is_our_group:
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'pp2', is_mass=False)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    if not await check_rate_limit(user_id, is_prem):
        await safe_send(update.message.reply_text(f"Slow down. Max {RATE_LIMIT_PREMIUM} checks per minute."))
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /pp2 CC|MM|YY|CVV")
        return
    
    card_input = context.args[0].strip()
    parts = card_input.split('|')
    
    if len(parts) < 4:
        await update.message.reply_text("Invalid format. Use: /pp2 CC|MM|YY|CVV")
        return
    
    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    username = update.effective_user.username or update.effective_user.first_name or "User"
    
    status_msg = await safe_send(update.message.reply_text("𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐..."))
    if not status_msg:
        return
    
    try:
        from gates.gate2 import check_card
        from storage import add_check_history
        
        bin_data, result = await asyncio.gather(lookup_bin(cc), check_card(cc, mm, yy, cvv, user_proxies=user_proxies))
        
        status = result.get('status', 'error')
        message = result.get('message', 'Unknown error')
        time_taken = result.get('time', 0)
        
        logger.info(f"[PayPal $2] User: {username} ({user_id}) | Card: {card_input} | Status: {status} | Raw: {message}")
        
        try:
            await add_check_history(user_id, cc, cc[:6], status, message, 'PayPal Charge $2 v2', time_taken)
        except:
            pass
        
        response, _r_ents = format_response(card_input, status, message, bin_data, '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', time_taken, username, user_id)
        await safe_send(status_msg.edit_text(response, entities=_r_ents, disable_web_page_preview=True))
        
        if status in ['approved', 'charged', 'ccn']:
            await send_hit_to_group(context, username, user_id, status, message, 'PayPal $2 v2')
        
    except Exception as e:
        await safe_send(status_msg.edit_text(sanitize_error(str(e))))

async def check_single_card_pp2(cc, mm, yy, cvv, username="Unknown", user_proxies=None):
    from gates.gate2 import check_card
    bin_data = await lookup_bin(cc)
    result = await check_card(cc, mm, yy, cvv, user_proxies=user_proxies)
    card = f"{cc}|{mm}|{yy}|{cvv}"
    logger.info(f"[PayPal $2 Mass] Card: {card} | Status: {result.get('status', 'error')} | Raw: {result.get('message', 'Unknown')} CHECKED BY {username}")
    return {
        'card': card,
        'status': result.get('status', 'error'),
        'message': result.get('message', 'Unknown'),
        'bin_data': bin_data,
        'time': result.get('time', 0)
    }

async def _run_mass_check_pp2(update, context, cards, status_msg, session_id, user_id, username, user_proxies=None):
    chat_id = update.effective_chat.id
    total = len(cards)
    success, approved, dead, checked = 0, 0, 0, 0
    current_response = "𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐..."
    
    def build_buttons():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💎 {current_response[:35]}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {checked}/{total}", callback_data="x")],
            [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
        ])
    
    try:
        from storage import add_check_history
        
        for i in range(0, total, 10):
            if stop_flags.get(user_id):
                break
            
            batch = cards[i:i+10]
            tasks = [asyncio.create_task(check_single_card_pp2(cc, mm, yy, cvv, username, user_proxies=user_proxies)) for cc, mm, yy, cvv in batch]
            
            for coro in asyncio.as_completed(tasks):
                if stop_flags.get(user_id):
                    for t in tasks:
                        t.cancel()
                    break
                
                try:
                    res = await coro
                except Exception as card_err:
                    checked += 1
                    dead += 1
                    current_response = sanitize_error(str(card_err))[:40]
                    logger.error(f"Mass PP2 card processing error: {card_err}")
                    await safe_send(status_msg.edit_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
                    continue
                
                checked += 1
                current_response = str(res.get('message', 'Unknown'))[:40]
                card_parts = res.get('card', '||||').split('|')
                
                try:
                    await add_check_history(user_id, card_parts[0], card_parts[0][:6], res.get('status', 'error'), res.get('message', ''), 'PayPal Charge $2 v2', res.get('time', 0))
                except:
                    pass
                
                if res.get('status') == 'charged':
                    success += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'PayPal $2 v2')
                elif res.get('status') in ['approved', 'ccn']:
                    approved += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'PayPal $2 v2')
                else:
                    dead += 1
                
                await safe_send(status_msg.edit_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
            
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
            if i + 10 < total and not stop_flags.get(user_id):
                await asyncio.sleep(5)
        
        status_text = "𝚂𝚝𝚘𝚙𝚙𝚎𝚍" if stop_flags.get(user_id) else "𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎"
        await safe_send(status_msg.edit_text(f"⚡ 𝗣𝗮𝘆𝗣𝗮𝗹 $𝟮 𝗠𝗮𝘀𝘀 · {status_text}\n━━━━━━━━━━━━━━━━━", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {total}", callback_data="x")]
        ])))
        
    except asyncio.CancelledError:
        logger.warning(f"Mass PP2 check cancelled for user {user_id}")
        await safe_send(update.message.reply_text(f"Mass check interrupted. Checked {checked}/{total} cards."))
    except Exception as e:
        logger.error(f"Mass PP2 check error: {e}")
        await safe_send(update.message.reply_text(sanitize_error(str(e))))
    finally:
        stop_flags.pop(user_id, None)
        stop_reasons.pop(user_id, None)
        active_sessions.pop(session_id, None)
        active_tasks.pop(user_id, None)
        if user_id != OWNER_ID:
            mass_check_cooldowns[user_id] = time.time()

async def mpp2_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    from storage import is_premium, OWNER_IDS
    if user_id not in OWNER_IDS and not await is_premium(user_id):
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'pp2', is_mass=True)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    chat_id = update.effective_chat.id
    if user_id in active_tasks:
        await update.message.reply_text("You already have an active mass check. Use /stop first.")
        return
    
    if user_id != OWNER_ID and user_id in mass_check_cooldowns:
        elapsed = time.time() - mass_check_cooldowns[user_id]
        if elapsed < MASS_CHECK_COOLDOWN:
            remaining = int(MASS_CHECK_COOLDOWN - elapsed)
            await update.message.reply_text(f"⏳ Cooldown active. Wait {remaining}s before starting another mass check.")
            return
    
    reply = update.message.reply_to_message
    if not reply or not reply.document:
        await update.message.reply_text("Reply to a file containing cards in CC|MM|YY|CVV format")
        return
    
    try:
        file = await reply.document.get_file()
        content = (await file.download_as_bytearray()).decode('utf-8')
        
        cards = []
        for line in content.split('\n'):
            parts = line.strip().split('|')
            if len(parts) >= 4 and len(parts[0]) >= 13:
                cards.append((parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()))
        
        if not cards:
            await update.message.reply_text("No valid cards found")
            return
        
        from storage import OWNER_IDS
        if user_id not in OWNER_IDS and len(cards) > MASS_CHECK_LIMIT:
            cards = cards[:MASS_CHECK_LIMIT]
            await update.message.reply_text(f"Limiting to {MASS_CHECK_LIMIT} cards per session")
        
        username = update.effective_user.username or update.effective_user.first_name or "User"
        
        global session_counter
        async with _session_lock:
            session_counter += 1
            session_id = f"session_{user_id}_{session_counter}"
        
        stop_flags[user_id] = False
        scode = generate_session_code()
        active_sessions[session_id] = {'user_id': user_id, 'total': len(cards), 'chat_id': chat_id, 'started_at': time_module.time(), 'username': username, 'gate': 'PayPal $2', 'code': scode}
        
        try:
            status_msg = await update.message.reply_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {len(cards)}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Starting...", callback_data="x")],
                [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
            ]))
        except Exception:
            stop_flags.pop(user_id, None)
            active_sessions.pop(session_id, None)
            return
        
        task = asyncio.create_task(_run_mass_check_pp2(update, context, cards, status_msg, session_id, user_id, username, user_proxies=user_proxies))
        active_tasks[user_id] = task
        
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))

# ============ STRIPE CHARGE $1 GATE (PREMIUM) ============

async def sc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    is_our_group = update.effective_chat.id == REQUIRED_GROUP_ID
    from storage import OWNER_IDS
    is_prem = await cached_is_premium(user_id)
    if user_id not in OWNER_IDS and not is_prem and not is_our_group:
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'sc', is_mass=False)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    if not await check_rate_limit(user_id, is_prem):
        await safe_send(update.message.reply_text(f"Slow down. Max {RATE_LIMIT_PREMIUM} checks per minute."))
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /sc CC|MM|YY|CVV")
        return
    
    card_input = context.args[0].strip()
    parts = card_input.split('|')
    
    if len(parts) < 4:
        await update.message.reply_text("Invalid format. Use: /sc CC|MM|YY|CVV")
        return
    
    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    username = update.effective_user.username or update.effective_user.first_name or "User"
    
    status_msg = await safe_send(update.message.reply_text("𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐..."))
    if not status_msg:
        return
    
    try:
        from gates.gate5 import check_card
        from storage import add_check_history
        
        bin_data, result = await asyncio.gather(lookup_bin(cc), check_card(cc, mm, yy, cvv, user_proxies=user_proxies))
        
        status = result.get('status', 'error')
        message = result.get('message', 'Unknown error')
        time_taken = result.get('time', 0)
        logger.info(f"[Stripe Charge $1] User: {username} ({user_id}) | Card: {card_input} | Status: {status} | Msg: {message[:100]}")
        
        try:
            await add_check_history(user_id, cc, cc[:6], status, message, 'Stripe Charge $1', time_taken)
        except:
            pass
        
        response, _r_ents = format_response(card_input, status, message, bin_data, '𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷', time_taken, username, user_id)
        await safe_send(status_msg.edit_text(response, entities=_r_ents, disable_web_page_preview=True))
        
        if status in ['approved', 'charged']:
            await send_hit_to_group(context, username, user_id, status, message, 'Stripe Charge $1')
        
    except Exception as e:
        await safe_send(status_msg.edit_text(sanitize_error(str(e))))

async def check_single_card_sc(cc, mm, yy, cvv, username="Unknown", user_proxies=None):
    from gates.gate5 import check_card
    bin_data = await lookup_bin(cc)
    result = await check_card(cc, mm, yy, cvv, user_proxies=user_proxies)
    card = f"{cc}|{mm}|{yy}|{cvv}"
    logger.info(f"[Stripe Charge $1 Mass] Card: {card} | Status: {result.get('status', 'error')} | Msg: {str(result.get('message', 'Unknown'))[:100]} CHECKED BY {username}")
    return {
        'card': card,
        'status': result.get('status', 'error'),
        'message': result.get('message', 'Unknown'),
        'bin_data': bin_data,
        'time': result.get('time', 0)
    }

async def _run_mass_check_sc(update, context, cards, status_msg, session_id, user_id, username, user_proxies=None):
    chat_id = update.effective_chat.id
    total = len(cards)
    success, approved, threeDS, dead, checked = 0, 0, 0, 0, 0
    current_response = "𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐..."
    
    def build_buttons():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗖𝗵𝗮𝗿𝗴𝗲𝗱  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"🔐 𝟯𝗗𝗦  ·  {threeDS}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💎 {current_response[:35]}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {checked}/{total}", callback_data="x")],
            [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
        ])
    
    try:
        from storage import add_check_history
        
        for i in range(0, total, 3):
            if stop_flags.get(user_id):
                break
            
            batch = cards[i:i+3]
            tasks = [asyncio.create_task(check_single_card_sc(cc, mm, yy, cvv, username, user_proxies=user_proxies)) for cc, mm, yy, cvv in batch]
            
            for coro in asyncio.as_completed(tasks):
                if stop_flags.get(user_id):
                    for t in tasks:
                        t.cancel()
                    break
                
                try:
                    res = await coro
                except Exception as card_err:
                    checked += 1
                    dead += 1
                    current_response = sanitize_error(str(card_err))[:40]
                    logger.error(f"Mass SC card processing error: {card_err}")
                    await safe_send(status_msg.edit_text(f"𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
                    continue
                
                checked += 1
                msg = str(res.get('message', 'Unknown'))
                current_response = msg[:40]
                card_parts = res.get('card', '||||').split('|')
                
                try:
                    await add_check_history(user_id, card_parts[0], card_parts[0][:6], res.get('status', 'error'), res.get('message', ''), 'Stripe Charge $1', res.get('time', 0))
                except:
                    pass
                
                if res.get('status') == 'charged':
                    success += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'Stripe Charge $1')
                elif res.get('status') == 'approved':
                    approved += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'Stripe Charge $1')
                elif '3DS' in msg or '3ds' in msg.lower() or 'requires_action' in msg.lower():
                    threeDS += 1
                else:
                    dead += 1
                
                await safe_send(status_msg.edit_text(f"𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
            
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
            if i + 3 < total and not stop_flags.get(user_id):
                await asyncio.sleep(10)
        
        status_text = "𝚂𝚝𝚘𝚙𝚙𝚎𝚍" if stop_flags.get(user_id) else "𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎"
        await safe_send(status_msg.edit_text(f"⚡ 𝗦𝘁𝗿𝗶𝗽𝗲 𝗖𝗵𝗮𝗿𝗴𝗲 $𝟭 𝗠𝗮𝘀𝘀 · {status_text}\n━━━━━━━━━━━━━━━━━", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗖𝗵𝗮𝗿𝗴𝗲𝗱  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"🔐 𝟯𝗗𝗦  ·  {threeDS}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {total}", callback_data="x")]
        ])))
        
    except asyncio.CancelledError:
        logger.warning(f"Mass SC check cancelled for user {user_id}")
        await safe_send(update.message.reply_text(f"Mass check interrupted. Checked {checked}/{total} cards."))
    except Exception as e:
        logger.error(f"Mass SC check error: {e}")
        await safe_send(update.message.reply_text(sanitize_error(str(e))))
    finally:
        stop_flags.pop(user_id, None)
        stop_reasons.pop(user_id, None)
        active_sessions.pop(session_id, None)
        active_tasks.pop(user_id, None)
        if user_id != OWNER_ID:
            mass_check_cooldowns[user_id] = time.time()

async def msc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    from storage import is_premium, OWNER_IDS
    if user_id not in OWNER_IDS and not await is_premium(user_id):
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'sc', is_mass=True)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    chat_id = update.effective_chat.id
    if user_id in active_tasks:
        await update.message.reply_text("You already have an active mass check. Use /stop first.")
        return
    
    if user_id != OWNER_ID and user_id in mass_check_cooldowns:
        elapsed = time.time() - mass_check_cooldowns[user_id]
        if elapsed < MASS_CHECK_COOLDOWN:
            remaining = int(MASS_CHECK_COOLDOWN - elapsed)
            await update.message.reply_text(f"⏳ Cooldown active. Wait {remaining}s before starting another mass check.")
            return
    
    reply = update.message.reply_to_message
    if not reply or not reply.document:
        await update.message.reply_text("Reply to a file containing cards in CC|MM|YY|CVV format")
        return
    
    try:
        file = await reply.document.get_file()
        content = (await file.download_as_bytearray()).decode('utf-8')
        
        cards = []
        for line in content.split('\n'):
            parts = line.strip().split('|')
            if len(parts) >= 4 and len(parts[0]) >= 13:
                cards.append((parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()))
        
        if not cards:
            await update.message.reply_text("No valid cards found")
            return
        
        from storage import OWNER_IDS
        if user_id not in OWNER_IDS and len(cards) > MASS_CHECK_LIMIT:
            cards = cards[:MASS_CHECK_LIMIT]
            await update.message.reply_text(f"Limiting to {MASS_CHECK_LIMIT} cards per session")
        
        username = update.effective_user.username or update.effective_user.first_name or "User"
        
        global session_counter
        async with _session_lock:
            session_counter += 1
            session_id = f"session_{user_id}_{session_counter}"
        
        stop_flags[user_id] = False
        scode = generate_session_code()
        active_sessions[session_id] = {'user_id': user_id, 'total': len(cards), 'chat_id': chat_id, 'started_at': time_module.time(), 'username': username, 'gate': 'Stripe Charge $1', 'code': scode}
        
        try:
            status_msg = await update.message.reply_text(f"𝚂𝚝𝚛𝚒𝚙𝚎 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟷 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {len(cards)}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Starting...", callback_data="x")],
                [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
            ]))
        except Exception:
            stop_flags.pop(user_id, None)
            active_sessions.pop(session_id, None)
            return
        
        task = asyncio.create_task(_run_mass_check_sc(update, context, cards, status_msg, session_id, user_id, username, user_proxies=user_proxies))
        active_tasks[user_id] = task
        
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))

# ============ STRIPE GBP £1 GATE — SC2 (PREMIUM) ============

async def sc2_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    is_our_group = update.effective_chat.id == REQUIRED_GROUP_ID
    from storage import OWNER_IDS
    is_prem = await cached_is_premium(user_id)
    if user_id not in OWNER_IDS and not is_prem and not is_our_group:
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return

    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'sc2', is_mass=False)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return

    if not await check_rate_limit(user_id, is_prem):
        await safe_send(update.message.reply_text(f"Slow down. Max {RATE_LIMIT_PREMIUM} checks per minute."))
        return

    if not context.args:
        await update.message.reply_text("Usage: /sc2 CC|MM|YY|CVV")
        return

    card_input = context.args[0].strip()
    parts = card_input.split('|')

    if len(parts) < 4:
        await update.message.reply_text("Invalid format. Use: /sc2 CC|MM|YY|CVV")
        return

    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    username = update.effective_user.username or update.effective_user.first_name or "User"

    status_msg = await safe_send(update.message.reply_text("𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐..."))
    if not status_msg:
        return

    try:
        from gates.gate6 import check_card
        from storage import add_check_history

        bin_data, result = await asyncio.gather(lookup_bin(cc), check_card(cc, mm, yy, cvv, user_proxies=user_proxies))

        status    = result.get('status', 'error')
        message   = result.get('message', 'Unknown error')
        time_taken = result.get('time', 0)
        logger.info(f"[Stripe $1 v2] User: {username} ({user_id}) | Card: {card_input} | Status: {status} | Msg: {message[:100]}")

        try:
            await add_check_history(user_id, cc, cc[:6], status, message, 'Stripe $1 v2', time_taken)
        except:
            pass

        # Remap status for display:
        # - gate6 'approved' / CCN / 3DS  → show as '3ds'   (no group hit)
        # - insufficient_funds dead        → show as 'approved' (group hit)
        # - charged                        → show as 'charged'  (group hit)
        msg_lower = message.lower()
        if status == 'approved' or 'CCN' in message or '3ds' in msg_lower or 'requires_action' in msg_lower:
            display_status = '3ds'
            send_hit = False
        elif status == 'dead' and ('insufficient_funds' in msg_lower or 'insufficient funds' in msg_lower):
            display_status = 'approved'
            send_hit = True
        elif status == 'charged':
            display_status = 'charged'
            send_hit = True
        else:
            display_status = status
            send_hit = False

        response, _r_ents = format_response(card_input, display_status, message, bin_data, '𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚟𝟸', time_taken, username, user_id)
        await safe_send(status_msg.edit_text(response, entities=_r_ents, disable_web_page_preview=True))

        if send_hit:
            await send_hit_to_group(context, username, user_id, display_status, message, 'Stripe $1 v2')

    except Exception as e:
        await safe_send(status_msg.edit_text(sanitize_error(str(e))))


async def check_single_card_sc2(cc, mm, yy, cvv, username="Unknown", user_proxies=None):
    from gates.gate6 import check_card
    bin_data = await lookup_bin(cc)
    result   = await check_card(cc, mm, yy, cvv, user_proxies=user_proxies)
    card     = f"{cc}|{mm}|{yy}|{cvv}"
    logger.info(f"[Stripe $1 v2 Mass] Card: {card} | Status: {result.get('status','error')} | Msg: {str(result.get('message','Unknown'))[:100]} CHECKED BY {username}")
    return {
        'card':     card,
        'status':   result.get('status', 'error'),
        'message':  result.get('message', 'Unknown'),
        'bin_data': bin_data,
        'time':     result.get('time', 0),
    }


async def _run_mass_check_sc2(update, context, cards, status_msg, session_id, user_id, username, user_proxies=None):
    total = len(cards)
    success, approved, threeDS, dead, checked = 0, 0, 0, 0, 0
    current_response = "𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐..."

    def build_buttons():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗖𝗵𝗮𝗿𝗴𝗲𝗱  ·  {success}",  callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"🔐 𝟯𝗗𝗦  ·  {threeDS}",       callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}",    callback_data="x")],
            [InlineKeyboardButton(f"💎 {current_response[:35]}",   callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {checked}/{total}", callback_data="x")],
            [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
        ])

    try:
        from storage import add_check_history

        for i in range(0, total, 3):
            if stop_flags.get(user_id):
                break

            batch = cards[i:i+3]
            tasks = [asyncio.create_task(check_single_card_sc2(cc, mm, yy, cvv, username, user_proxies=user_proxies)) for cc, mm, yy, cvv in batch]

            for coro in asyncio.as_completed(tasks):
                if stop_flags.get(user_id):
                    for t in tasks:
                        t.cancel()
                    break

                try:
                    res = await coro
                except Exception as card_err:
                    checked += 1
                    dead    += 1
                    current_response = sanitize_error(str(card_err))[:40]
                    logger.error(f"Mass SC2 card processing error: {card_err}")
                    await safe_send(status_msg.edit_text(f"𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚟𝟸 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
                    continue

                checked += 1
                msg = str(res.get('message', 'Unknown'))
                current_response = msg[:40]
                card_parts = res.get('card', '||||').split('|')

                try:
                    await add_check_history(user_id, card_parts[0], card_parts[0][:6], res.get('status', 'error'), res.get('message', ''), 'Stripe $1 v2', res.get('time', 0))
                except:
                    pass

                if res.get('status') == 'charged':
                    # Charged → send hit message + group
                    success += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚟𝟸', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'Stripe $1 v2')
                elif res.get('status') == 'approved' or 'CCN' in msg or '3ds' in msg.lower() or 'requires_action' in msg.lower():
                    # 3DS / CCN → silent, counter only, no message or group
                    threeDS += 1
                elif res.get('status') == 'dead' and ('insufficient_funds' in msg.lower() or 'insufficient funds' in msg.lower()):
                    # Insufficient funds → Approved counter + send hit message + group
                    approved += 1
                    hit_msg, _r_ents = format_response(res['card'], 'approved', res['message'], res.get('bin_data'), '𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚟𝟸', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, 'approved', res['message'], 'Stripe $1 v2')
                else:
                    dead += 1

                await safe_send(status_msg.edit_text(f"𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚟𝟸 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))

            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            if i + 3 < total and not stop_flags.get(user_id):
                await asyncio.sleep(10)

        status_text = "𝚂𝚝𝚘𝚙𝚙𝚎𝚍" if stop_flags.get(user_id) else "𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎"
        await safe_send(status_msg.edit_text(f"⚡ 𝗦𝘁𝗿𝗶𝗽𝗲 $𝟭 𝘃𝟮 𝗠𝗮𝘀𝘀 · {status_text}\n━━━━━━━━━━━━━━━━━", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗖𝗵𝗮𝗿𝗴𝗲𝗱  ·  {success}",  callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"🔐 𝟯𝗗𝗦  ·  {threeDS}",       callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}",    callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {total}",       callback_data="x")],
        ])))

    except asyncio.CancelledError:
        logger.warning(f"Mass SC2 check cancelled for user {user_id}")
        await safe_send(update.message.reply_text(f"Mass check interrupted. Checked {checked}/{total} cards."))
    except Exception as e:
        logger.error(f"Mass SC2 check error: {e}")
        await safe_send(update.message.reply_text(sanitize_error(str(e))))
    finally:
        stop_flags.pop(user_id, None)
        stop_reasons.pop(user_id, None)
        active_sessions.pop(session_id, None)
        active_tasks.pop(user_id, None)
        if user_id != OWNER_ID:
            mass_check_cooldowns[user_id] = time.time()


async def msc2_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    from storage import is_premium, OWNER_IDS
    if user_id not in OWNER_IDS and not await is_premium(user_id):
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return

    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'sc2', is_mass=True)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return

    chat_id = update.effective_chat.id
    if user_id in active_tasks:
        await update.message.reply_text("You already have an active mass check. Use /stop first.")
        return

    if user_id != OWNER_ID and user_id in mass_check_cooldowns:
        elapsed = time.time() - mass_check_cooldowns[user_id]
        if elapsed < MASS_CHECK_COOLDOWN:
            remaining = int(MASS_CHECK_COOLDOWN - elapsed)
            await update.message.reply_text(f"⏳ Cooldown active. Wait {remaining}s before starting another mass check.")
            return

    reply = update.message.reply_to_message
    if not reply or not reply.document:
        await update.message.reply_text("Reply to a file containing cards in CC|MM|YY|CVV format")
        return

    try:
        file    = await reply.document.get_file()
        content = (await file.download_as_bytearray()).decode('utf-8')

        cards = []
        for line in content.split('\n'):
            parts = line.strip().split('|')
            if len(parts) >= 4 and len(parts[0]) >= 13:
                cards.append((parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()))

        if not cards:
            await update.message.reply_text("No valid cards found")
            return

        from storage import OWNER_IDS
        if user_id not in OWNER_IDS and len(cards) > MASS_CHECK_LIMIT:
            cards = cards[:MASS_CHECK_LIMIT]
            await update.message.reply_text(f"Limiting to {MASS_CHECK_LIMIT} cards per session")

        username = update.effective_user.username or update.effective_user.first_name or "User"

        global session_counter
        async with _session_lock:
            session_counter += 1
            session_id = f"session_{user_id}_{session_counter}"

        stop_flags[user_id] = False
        scode = generate_session_code()
        active_sessions[session_id] = {
            'user_id': user_id, 'total': len(cards), 'chat_id': chat_id,
            'started_at': time_module.time(), 'username': username,
            'gate': 'Stripe $1 v2', 'code': scode,
        }

        try:
            status_msg = await update.message.reply_text(
                f"𝚂𝚝𝚛𝚒𝚙𝚎 $𝟷 𝚟𝟸 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {len(cards)}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Starting...", callback_data="x")],
                    [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
                ])
            )
        except Exception:
            stop_flags.pop(user_id, None)
            active_sessions.pop(session_id, None)
            return

        task = asyncio.create_task(
            _run_mass_check_sc2(update, context, cards, status_msg, session_id, user_id, username, user_proxies=user_proxies)
        )
        active_tasks[user_id] = task

    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))

# ============ PAYPAL $2 v3 GATE (PREMIUM) ============

async def pp3_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    is_our_group = update.effective_chat.id == REQUIRED_GROUP_ID
    from storage import OWNER_IDS
    is_prem = await cached_is_premium(user_id)
    if user_id not in OWNER_IDS and not is_prem and not is_our_group:
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'pp3', is_mass=False)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    if not await check_rate_limit(user_id, is_prem):
        await safe_send(update.message.reply_text(f"Slow down. Max {RATE_LIMIT_PREMIUM} checks per minute."))
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /pp3 CC|MM|YY|CVV")
        return
    
    card_input = context.args[0].strip()
    parts = card_input.split('|')
    
    if len(parts) < 4:
        await update.message.reply_text("Invalid format. Use: /pp3 CC|MM|YY|CVV")
        return
    
    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    username = update.effective_user.username or update.effective_user.first_name or "User"
    
    status_msg = await safe_send(update.message.reply_text("𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐..."))
    if not status_msg:
        return
    
    try:
        from gates.gate3 import check_card
        from storage import add_check_history
        
        bin_data, result = await asyncio.gather(lookup_bin(cc), check_card(cc, mm, yy, cvv, user_proxies=user_proxies))
        
        status = result.get('status', 'error')
        message = result.get('message', 'Unknown error')
        time_taken = result.get('time', 0)
        
        logger.info(f"[PayPal $2 v3] User: {username} ({user_id}) | Card: {card_input} | Status: {status} | Raw: {message}")
        
        try:
            await add_check_history(user_id, cc, cc[:6], status, message, 'PayPal Charge $2 v3', time_taken)
        except:
            pass
        
        response, _r_ents = format_response(card_input, status, message, bin_data, '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', time_taken, username, user_id)
        await safe_send(status_msg.edit_text(response, entities=_r_ents, disable_web_page_preview=True))
        
        if status in ['approved', 'charged', 'ccn']:
            await send_hit_to_group(context, username, user_id, status, message, 'PayPal $2 v3')
        
    except Exception as e:
        await safe_send(status_msg.edit_text(sanitize_error(str(e))))

async def check_single_card_hk(cc, mm, yy, cvv, username="Unknown", user_proxies=None):
    from gates.gate3 import check_card
    bin_data = await lookup_bin(cc)
    result = await check_card(cc, mm, yy, cvv, user_proxies=user_proxies)
    card = f"{cc}|{mm}|{yy}|{cvv}"
    logger.info(f"[PayPal $2 v3 Mass] Card: {card} | Status: {result.get('status', 'error')} | Raw: {result.get('message', 'Unknown')} CHECKED BY {username}")
    return {
        'card': card,
        'status': result.get('status', 'error'),
        'message': result.get('message', 'Unknown'),
        'bin_data': bin_data,
        'time': result.get('time', 0)
    }

async def _run_mass_check_hk(update, context, cards, status_msg, session_id, user_id, username, user_proxies=None):
    chat_id = update.effective_chat.id
    total = len(cards)
    success, approved, dead, checked = 0, 0, 0, 0
    current_response = "𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐..."
    
    def build_buttons():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💎 {current_response[:35]}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {checked}/{total}", callback_data="x")],
            [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
        ])
    
    try:
        from storage import add_check_history
        
        for i in range(0, total, 10):
            if stop_flags.get(user_id):
                break
            
            batch = cards[i:i+10]
            tasks = [asyncio.create_task(check_single_card_hk(cc, mm, yy, cvv, username, user_proxies=user_proxies)) for cc, mm, yy, cvv in batch]
            
            for coro in asyncio.as_completed(tasks):
                if stop_flags.get(user_id):
                    for t in tasks:
                        t.cancel()
                    break
                
                try:
                    res = await coro
                except Exception as card_err:
                    checked += 1
                    dead += 1
                    current_response = sanitize_error(str(card_err))[:40]
                    logger.error(f"Mass PP3 card processing error: {card_err}")
                    await safe_send(status_msg.edit_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
                    continue
                
                checked += 1
                current_response = str(res.get('message', 'Unknown'))[:40]
                card_parts = res.get('card', '||||').split('|')
                
                try:
                    await add_check_history(user_id, card_parts[0], card_parts[0][:6], res.get('status', 'error'), res.get('message', ''), 'PayPal Charge $2 v3', res.get('time', 0))
                except:
                    pass
                
                if res.get('status') == 'charged':
                    success += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'PayPal $2 v3')
                elif res.get('status') in ['approved', 'ccn']:
                    approved += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'PayPal $2 v3')
                else:
                    dead += 1
                
                await safe_send(status_msg.edit_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
            
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
            if i + 10 < total and not stop_flags.get(user_id):
                await asyncio.sleep(5)
        
        status_text = "𝚂𝚝𝚘𝚙𝚙𝚎𝚍" if stop_flags.get(user_id) else "𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎"
        await safe_send(status_msg.edit_text(f"⚡ 𝗣𝗮𝘆𝗣𝗮𝗹 $𝟮 𝗠𝗮𝘀𝘀 · {status_text}\n━━━━━━━━━━━━━━━━━", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {total}", callback_data="x")]
        ])))
        
    except asyncio.CancelledError:
        logger.warning(f"Mass HK check cancelled for user {user_id}")
        await safe_send(update.message.reply_text(f"Mass check interrupted. Checked {checked}/{total} cards."))
    except Exception as e:
        logger.error(f"Mass HK check error: {e}")
        await safe_send(update.message.reply_text(sanitize_error(str(e))))
    finally:
        stop_flags.pop(user_id, None)
        stop_reasons.pop(user_id, None)
        active_sessions.pop(session_id, None)
        active_tasks.pop(user_id, None)
        if user_id != OWNER_ID:
            mass_check_cooldowns[user_id] = time.time()

async def mpp3_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    from storage import is_premium, OWNER_IDS
    if user_id not in OWNER_IDS and not await is_premium(user_id):
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'pp3', is_mass=True)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    chat_id = update.effective_chat.id
    if user_id in active_tasks:
        await update.message.reply_text("You already have an active mass check. Use /stop first.")
        return
    
    if user_id != OWNER_ID and user_id in mass_check_cooldowns:
        elapsed = time.time() - mass_check_cooldowns[user_id]
        if elapsed < MASS_CHECK_COOLDOWN:
            remaining = int(MASS_CHECK_COOLDOWN - elapsed)
            await update.message.reply_text(f"⏳ Cooldown active. Wait {remaining}s before starting another mass check.")
            return
    
    reply = update.message.reply_to_message
    if not reply or not reply.document:
        await update.message.reply_text("Reply to a file containing cards in CC|MM|YY|CVV format")
        return
    
    try:
        file = await reply.document.get_file()
        content = (await file.download_as_bytearray()).decode('utf-8')
        
        cards = []
        for line in content.split('\n'):
            parts = line.strip().split('|')
            if len(parts) >= 4 and len(parts[0]) >= 13:
                cards.append((parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()))
        
        if not cards:
            await update.message.reply_text("No valid cards found")
            return
        
        from storage import OWNER_IDS
        if user_id not in OWNER_IDS and len(cards) > MASS_CHECK_LIMIT:
            cards = cards[:MASS_CHECK_LIMIT]
            await update.message.reply_text(f"Limiting to {MASS_CHECK_LIMIT} cards per session")
        
        username = update.effective_user.username or update.effective_user.first_name or "User"
        
        global session_counter
        async with _session_lock:
            session_counter += 1
            session_id = f"session_{user_id}_{session_counter}"
        
        stop_flags[user_id] = False
        scode = generate_session_code()
        active_sessions[session_id] = {'user_id': user_id, 'total': len(cards), 'chat_id': chat_id, 'started_at': time_module.time(), 'username': username, 'gate': 'PayPal $2 v3', 'code': scode}
        
        try:
            status_msg = await update.message.reply_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟹 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {len(cards)}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Starting...", callback_data="x")],
                [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
            ]))
        except Exception:
            stop_flags.pop(user_id, None)
            active_sessions.pop(session_id, None)
            return
        
        task = asyncio.create_task(_run_mass_check_hk(update, context, cards, status_msg, session_id, user_id, username, user_proxies=user_proxies))
        active_tasks[user_id] = task
        
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))

# ============ PAYPAL $2 v4 GATE (PREMIUM) ============

async def pp4_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    is_our_group = update.effective_chat.id == REQUIRED_GROUP_ID
    from storage import OWNER_IDS
    is_prem = await cached_is_premium(user_id)
    if user_id not in OWNER_IDS and not is_prem and not is_our_group:
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'pp4', is_mass=False)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    if not await check_rate_limit(user_id, is_prem):
        await safe_send(update.message.reply_text(f"Slow down. Max {RATE_LIMIT_PREMIUM} checks per minute."))
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /pp4 CC|MM|YY|CVV")
        return
    
    card_input = context.args[0].strip()
    parts = card_input.split('|')
    
    if len(parts) < 4:
        await update.message.reply_text("Invalid format. Use: /pp4 CC|MM|YY|CVV")
        return
    
    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    username = update.effective_user.username or update.effective_user.first_name or "User"
    
    status_msg = await safe_send(update.message.reply_text("𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐..."))
    if not status_msg:
        return
    
    try:
        from gates.gate7 import check_card
        from storage import add_check_history
        
        bin_data, result = await asyncio.gather(lookup_bin(cc), check_card(cc, mm, yy, cvv, user_proxies=user_proxies))
        
        status = result.get('status', 'error')
        message = result.get('message', 'Unknown error')
        time_taken = result.get('time', 0)
        
        logger.info(f"[PayPal $2 v4] User: {username} ({user_id}) | Card: {card_input} | Status: {status} | Raw: {message}")
        
        try:
            await add_check_history(user_id, cc, cc[:6], status, message, 'PayPal Charge $2 v4', time_taken)
        except:
            pass
        
        response, _r_ents = format_response(card_input, status, message, bin_data, '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', time_taken, username, user_id)
        await safe_send(status_msg.edit_text(response, entities=_r_ents, disable_web_page_preview=True))
        
        if status in ['approved', 'charged', 'ccn']:
            await send_hit_to_group(context, username, user_id, status, message, 'PayPal $2 v4')
        
    except Exception as e:
        await safe_send(status_msg.edit_text(sanitize_error(str(e))))

async def check_single_card_pp4(cc, mm, yy, cvv, username="Unknown", user_proxies=None):
    from gates.gate7 import check_card
    bin_data = await lookup_bin(cc)
    result = await check_card(cc, mm, yy, cvv, user_proxies=user_proxies)
    card = f"{cc}|{mm}|{yy}|{cvv}"
    logger.info(f"[PayPal $2 v4 Mass] Card: {card} | Status: {result.get('status', 'error')} | Raw: {result.get('message', 'Unknown')} CHECKED BY {username}")
    return {
        'card': card,
        'status': result.get('status', 'error'),
        'message': result.get('message', 'Unknown'),
        'bin_data': bin_data,
        'time': result.get('time', 0)
    }

async def _run_mass_check_pp4(update, context, cards, status_msg, session_id, user_id, username, user_proxies=None):
    chat_id = update.effective_chat.id
    total = len(cards)
    success, approved, dead, checked = 0, 0, 0, 0
    current_response = "𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐..."
    
    def build_buttons():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💎 {current_response[:35]}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {checked}/{total}", callback_data="x")],
            [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
        ])
    
    try:
        from storage import add_check_history
        
        for i in range(0, total, 10):
            if stop_flags.get(user_id):
                break
            
            batch = cards[i:i+10]
            tasks = [asyncio.create_task(check_single_card_pp4(cc, mm, yy, cvv, username, user_proxies=user_proxies)) for cc, mm, yy, cvv in batch]
            
            for coro in asyncio.as_completed(tasks):
                if stop_flags.get(user_id):
                    for t in tasks:
                        t.cancel()
                    break
                
                try:
                    res = await coro
                except Exception as card_err:
                    checked += 1
                    dead += 1
                    current_response = sanitize_error(str(card_err))[:40]
                    logger.error(f"Mass PP4 card processing error: {card_err}")
                    await safe_send(status_msg.edit_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟺 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
                    continue
                
                checked += 1
                current_response = str(res.get('message', 'Unknown'))[:40]
                card_parts = res.get('card', '||||').split('|')
                
                try:
                    await add_check_history(user_id, card_parts[0], card_parts[0][:6], res.get('status', 'error'), res.get('message', ''), 'PayPal Charge $2 v4', res.get('time', 0))
                except:
                    pass
                
                if res.get('status') == 'charged':
                    success += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'PayPal $2 v4')
                elif res.get('status') in ['approved', 'ccn']:
                    approved += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙿𝚊𝚢𝙿𝚊𝚕 𝙲𝚑𝚊𝚛𝚐𝚎 $𝟸', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'PayPal $2 v4')
                else:
                    dead += 1
                
                await safe_send(status_msg.edit_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟺 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
            
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
            if i + 10 < total and not stop_flags.get(user_id):
                await asyncio.sleep(5)
        
        status_text = "𝚂𝚝𝚘𝚙𝚙𝚎𝚍" if stop_flags.get(user_id) else "𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎"
        await safe_send(status_msg.edit_text(f"⚡ 𝗣𝗮𝘆𝗣𝗮𝗹 $𝟮 𝘃𝟰 𝗠𝗮𝘀𝘀 · {status_text}\n━━━━━━━━━━━━━━━━━", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗦𝘂𝗰𝗰𝗲𝘀𝘀  ·  {success}", callback_data="x")],
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {total}", callback_data="x")]
        ])))
        
    except asyncio.CancelledError:
        logger.warning(f"Mass PP4 check cancelled for user {user_id}")
        await safe_send(update.message.reply_text(f"Mass check interrupted. Checked {checked}/{total} cards."))
    except Exception as e:
        logger.error(f"Mass PP4 check error: {e}")
        await safe_send(update.message.reply_text(sanitize_error(str(e))))
    finally:
        stop_flags.pop(user_id, None)
        stop_reasons.pop(user_id, None)
        active_sessions.pop(session_id, None)
        active_tasks.pop(user_id, None)
        if user_id != OWNER_ID:
            mass_check_cooldowns[user_id] = time.time()

async def mpp4_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    from storage import is_premium, OWNER_IDS
    if user_id not in OWNER_IDS and not await is_premium(user_id):
        await update.message.reply_text(PREMIUM_REQUIRED_MSG)
        return
    
    has_proxy, user_proxies, proxy_msg = await check_user_proxy(user_id, 'pp4', is_mass=True)
    if not has_proxy:
        await update.message.reply_text(proxy_msg)
        return
    
    chat_id = update.effective_chat.id
    if user_id in active_tasks:
        await update.message.reply_text("You already have an active mass check. Use /stop first.")
        return
    
    if user_id != OWNER_ID and user_id in mass_check_cooldowns:
        elapsed = time.time() - mass_check_cooldowns[user_id]
        if elapsed < MASS_CHECK_COOLDOWN:
            remaining = int(MASS_CHECK_COOLDOWN - elapsed)
            await update.message.reply_text(f"⏳ Cooldown active. Wait {remaining}s before starting another mass check.")
            return
    
    reply = update.message.reply_to_message
    if not reply or not reply.document:
        await update.message.reply_text("Reply to a file containing cards in CC|MM|YY|CVV format")
        return
    
    try:
        file = await reply.document.get_file()
        content = (await file.download_as_bytearray()).decode('utf-8')
        
        cards = []
        for line in content.split('\n'):
            parts = line.strip().split('|')
            if len(parts) >= 4 and len(parts[0]) >= 13:
                cards.append((parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()))
        
        if not cards:
            await update.message.reply_text("No valid cards found")
            return
        
        from storage import OWNER_IDS
        if user_id not in OWNER_IDS and len(cards) > MASS_CHECK_LIMIT:
            cards = cards[:MASS_CHECK_LIMIT]
            await update.message.reply_text(f"Limiting to {MASS_CHECK_LIMIT} cards per session")
        
        username = update.effective_user.username or update.effective_user.first_name or "User"
        
        global session_counter
        async with _session_lock:
            session_counter += 1
            session_id = f"session_{user_id}_{session_counter}"
        
        stop_flags[user_id] = False
        scode = generate_session_code()
        active_sessions[session_id] = {'user_id': user_id, 'total': len(cards), 'chat_id': chat_id, 'started_at': time_module.time(), 'username': username, 'gate': 'PayPal $2 v4', 'code': scode}
        
        try:
            status_msg = await update.message.reply_text(f"𝙿𝚊𝚢𝙿𝚊𝚕 $𝟸 𝚟𝟺 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {len(cards)}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Starting...", callback_data="x")],
                [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
            ]))
        except Exception:
            stop_flags.pop(user_id, None)
            active_sessions.pop(session_id, None)
            return
        
        task = asyncio.create_task(_run_mass_check_pp4(update, context, cards, status_msg, session_id, user_id, username, user_proxies=user_proxies))
        active_tasks[user_id] = task
        
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))

# ============ BRAINTREE CVV AUTH (FREE + PREMIUM) ============

async def b3_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⛔ 𝗚𝗮𝘁𝗲 𝗗𝗶𝘀𝗮𝗯𝗹𝗲𝗱\n"
        "━━━━━━━━━━━━━━━━━\n"
        "𝚃𝚑𝚒𝚜 𝙶𝚊𝚝𝚎 𝙷𝚊𝚜 𝙱𝚎𝚎𝚗 𝙳𝚒𝚜𝚊𝚋𝚕𝚎𝚍\n"
        "𝙱𝚢 𝚃𝚑𝚎 𝙾𝚠𝚗𝚎𝚛.\n\n"
        "𝙿𝚕𝚎𝚊𝚜𝚎 𝚄𝚜𝚎 𝙾𝚝𝚑𝚎𝚛 𝙶𝚊𝚝𝚎𝚜.\n"
        "━━━━━━━━━━━━━━━━━"
    )
    return
    user_id = update.effective_user.id  # noqa: F401 — gate disabled, code preserved
    is_prem = await cached_is_premium(user_id)
    
    if not await check_rate_limit(user_id, is_prem):
        limit = RATE_LIMIT_PREMIUM if is_prem else RATE_LIMIT_FREE
        await safe_send(update.message.reply_text(f"Slow down. Max {limit} checks per minute."))
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /b3 CC|MM|YY|CVV")
        return
    
    card_input = context.args[0].strip()
    parts = card_input.split('|')
    
    if len(parts) < 4:
        await update.message.reply_text("Invalid format. Use: /b3 CC|MM|YY|CVV")
        return
    
    cc, mm, yy, cvv = parts[0], parts[1], parts[2], parts[3]
    username = update.effective_user.username or update.effective_user.first_name or "User"
    
    status_msg = await safe_send(update.message.reply_text("𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐..."))
    if not status_msg:
        return
    
    try:
        from gates.gate4 import check_card
        from storage import add_check_history
        
        bin_data, result = await asyncio.gather(lookup_bin(cc), check_card(cc, mm, yy, cvv))
        
        status = result.get('status', 'error')
        message = result.get('message', 'Unknown error')
        time_taken = result.get('time', 0)
        
        logger.info(f"[Braintree Auth] User: {username} ({user_id}) | Card: {card_input} | Status: {status} | Raw: {message}")
        
        try:
            await add_check_history(user_id, cc, cc[:6], status, message, 'Braintree Auth', time_taken)
        except:
            pass
        
        response, _r_ents = format_response(card_input, status, message, bin_data, '𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 𝙲𝚟𝚟 𝙰𝚞𝚝𝚑', time_taken, username, user_id)
        await safe_send(status_msg.edit_text(response, entities=_r_ents, disable_web_page_preview=True))
        
        if status in ['approved', 'live', 'charged', 'ccn']:
            await send_hit_to_group(context, username, user_id, status, message, 'Braintree CVV Auth')
        
    except Exception as e:
        await safe_send(status_msg.edit_text(sanitize_error(str(e))))

# ============ MASS BRAINTREE AUTH (BATCHED) ============

async def check_single_card_b3(cc, mm, yy, cvv, username="Unknown"):
    from gates.gate4 import check_card
    bin_data = await lookup_bin(cc)
    result = await check_card(cc, mm, yy, cvv)
    card = f"{cc}|{mm}|{yy}|{cvv}"
    logger.info(f"[Braintree Mass] Card: {card} | Status: {result.get('status', 'error')} | Raw: {result.get('message', 'Unknown')} CHECKED BY {username}")
    return {
        'card': card,
        'status': result.get('status', 'error'),
        'message': result.get('message', 'Unknown'),
        'bin_data': bin_data,
        'time': result.get('time', 0)
    }

async def _run_mass_check_b3(update, context, cards, status_msg, session_id, user_id, username):
    chat_id = update.effective_chat.id
    total = len(cards)
    approved, dead, checked = 0, 0, 0
    current_response = "𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐..."
    
    def build_buttons():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💎 {current_response[:35]}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {checked}/{total}", callback_data="x")],
            [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
        ])
    
    try:
        from storage import add_check_history
        
        for i in range(0, total, 3):
            if stop_flags.get(user_id):
                break
            
            batch = cards[i:i+3]
            tasks = [asyncio.create_task(check_single_card_b3(cc, mm, yy, cvv, username)) for cc, mm, yy, cvv in batch]
            
            for coro in asyncio.as_completed(tasks):
                if stop_flags.get(user_id):
                    for t in tasks:
                        t.cancel()
                    break
                
                try:
                    res = await coro
                except Exception as card_err:
                    checked += 1
                    dead += 1
                    current_response = sanitize_error(str(card_err))[:40]
                    logger.error(f"Mass B3 card processing error: {card_err}")
                    await safe_send(status_msg.edit_text(f"𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 𝙲𝚟𝚟 𝙰𝚞𝚝𝚑 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
                    continue
                
                checked += 1
                current_response = str(res.get('message', 'Unknown'))[:40]
                card_parts = res.get('card', '||||').split('|')
                
                try:
                    await add_check_history(user_id, card_parts[0], card_parts[0][:6], res.get('status', 'error'), res.get('message', ''), 'Braintree Auth', res.get('time', 0))
                except:
                    pass
                
                if res.get('status') in ['approved', 'charged', 'ccn']:
                    approved += 1
                    hit_msg, _r_ents = format_response(res['card'], res['status'], res['message'], res.get('bin_data'), '𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 𝙲𝚟𝚟 𝙰𝚞𝚝𝚑', res.get('time', 0), username, user_id)
                    await safe_send(update.message.reply_text(hit_msg, entities=_r_ents, disable_web_page_preview=True))
                    await send_hit_to_group(context, username, user_id, res['status'], res['message'], 'Braintree CVV Auth')
                else:
                    dead += 1
                
                await safe_send(status_msg.edit_text(f"𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 𝙲𝚟𝚟 𝙰𝚞𝚝𝚑 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {total}", reply_markup=build_buttons()))
            
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            
            if i + 3 < total and not stop_flags.get(user_id):
                await asyncio.sleep(10)
        
        status_text = "𝚂𝚝𝚘𝚙𝚙𝚎𝚍" if stop_flags.get(user_id) else "𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎"
        await safe_send(status_msg.edit_text(f"⚡ 𝗕𝗿𝗮𝗶𝗻𝘁𝗿𝗲𝗲 𝗠𝗮𝘀𝘀 · {status_text}\n━━━━━━━━━━━━━━━━━", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱  ·  {approved}", callback_data="x")],
            [InlineKeyboardButton(f"❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲𝗱  ·  {dead}", callback_data="x")],
            [InlineKeyboardButton(f"💳 𝗧𝗼𝘁𝗮𝗹  ·  {total}", callback_data="x")]
        ])))
        
    except asyncio.CancelledError:
        logger.warning(f"Mass B3 check cancelled for user {user_id}")
        await safe_send(update.message.reply_text(f"Mass check interrupted. Checked {checked}/{total} cards."))
    except Exception as e:
        logger.error(f"Mass B3 check error: {e}")
        await safe_send(update.message.reply_text(sanitize_error(str(e))))
    finally:
        stop_flags.pop(user_id, None)
        stop_reasons.pop(user_id, None)
        active_sessions.pop(session_id, None)
        active_tasks.pop(user_id, None)
        if user_id != OWNER_ID:
            mass_check_cooldowns[user_id] = time.time()

async def mb3_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⛔ 𝗚𝗮𝘁𝗲 𝗗𝗶𝘀𝗮𝗯𝗹𝗲𝗱\n"
        "━━━━━━━━━━━━━━━━━\n"
        "𝚃𝚑𝚒𝚜 𝙶𝚊𝚝𝚎 𝙷𝚊𝚜 𝙱𝚎𝚎𝚗 𝙳𝚒𝚜𝚊𝚋𝚕𝚎𝚍\n"
        "𝙱𝚢 𝚃𝚑𝚎 𝙾𝚠𝚗𝚎𝚛.\n\n"
        "𝙿𝚕𝚎𝚊𝚜𝚎 𝚄𝚜𝚎 𝙾𝚝𝚑𝚎𝚛 𝙶𝚊𝚝𝚎𝚜.\n"
        "━━━━━━━━━━━━━━━━━"
    )
    return
    user_id = update.effective_user.id  # noqa: F401 — gate disabled, code preserved
    from storage import is_premium, OWNER_IDS
    
    chat_id = update.effective_chat.id
    if user_id in active_tasks:
        await update.message.reply_text("You already have an active mass check. Use /stop first.")
        return
    
    if user_id != OWNER_ID and user_id in mass_check_cooldowns:
        elapsed = time.time() - mass_check_cooldowns[user_id]
        if elapsed < MASS_CHECK_COOLDOWN:
            remaining = int(MASS_CHECK_COOLDOWN - elapsed)
            await update.message.reply_text(f"⏳ Cooldown active. Wait {remaining}s before starting another mass check.")
            return
    
    reply = update.message.reply_to_message
    if not reply or not reply.document:
        await update.message.reply_text("Reply to a file containing cards in CC|MM|YY|CVV format")
        return
    
    try:
        file = await reply.document.get_file()
        content = (await file.download_as_bytearray()).decode('utf-8')
        
        cards = []
        for line in content.split('\n'):
            parts = line.strip().split('|')
            if len(parts) >= 4 and len(parts[0]) >= 13:
                cards.append((parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()))
        
        if not cards:
            await update.message.reply_text("No valid cards found")
            return
        
        from storage import OWNER_IDS
        if user_id not in OWNER_IDS and len(cards) > MASS_CHECK_LIMIT:
            cards = cards[:MASS_CHECK_LIMIT]
            await update.message.reply_text(f"Limiting to {MASS_CHECK_LIMIT} cards per session")
        
        username = update.effective_user.username or update.effective_user.first_name or "User"
        
        global session_counter
        async with _session_lock:
            session_counter += 1
            session_id = f"session_{user_id}_{session_counter}"
        
        stop_flags[user_id] = False
        scode = generate_session_code()
        active_sessions[session_id] = {'user_id': user_id, 'total': len(cards), 'chat_id': chat_id, 'started_at': time_module.time(), 'username': username, 'gate': 'Braintree CVV', 'code': scode}
        
        try:
            status_msg = await update.message.reply_text(f"𝙱𝚛𝚊𝚒𝚗𝚝𝚛𝚎𝚎 𝙲𝚟𝚟 𝙰𝚞𝚝𝚑 𝙼𝚊𝚜𝚜 𝙲𝚑𝚎𝚌𝚔𝚒𝚗𝚐...\n𝗧𝗼𝘁𝗮𝗹: {len(cards)}", reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Starting...", callback_data="x")],
                [InlineKeyboardButton("⏹ 𝗦𝗧𝗢𝗣", callback_data=f"stop_{user_id}")]
            ]))
        except Exception:
            stop_flags.pop(user_id, None)
            active_sessions.pop(session_id, None)
            return
        
        task = asyncio.create_task(_run_mass_check_b3(update, context, cards, status_msg, session_id, user_id, username))
        active_tasks[user_id] = task
        
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))


# ============ UTILITY COMMANDS ============

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    try:
        from storage import get_user_stats
        user_id = update.effective_user.id
        stats = await get_user_stats(user_id)
        username = update.effective_user.username or update.effective_user.first_name or "User"
        
        await update.message.reply_text(f"""📊 𝗦𝘁𝗮𝘁𝘀 · 𝟮𝟰𝗵
━━━━━━━━━━━━━━━━━
𝗨𝘀𝗲𝗿 ➜ {to_mono(username)}
𝗧𝗼𝘁𝗮𝗹 ➜ {stats['total_24h']}
𝗛𝗶𝘁𝘀 ➜ {stats['hits_24h']}
𝗗𝗲𝗮𝗱 ➜ {stats['dead_24h']}
𝗘𝗿𝗿𝗼𝗿𝘀 ➜ {stats['errors_24h']}
━━━━━━━━━━━━━━━━━""")
    except Exception as e:
        await update.message.reply_text("❌ Error fetching stats.")
        logger.error(f"Stats error: {e}")

async def vps_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import is_admin_or_owner
    if not await is_admin_or_owner(update.effective_user.id):
        await update.message.reply_text("Admin only.")
        return
    
    try:
        uptime = datetime.now() - BOT_START_TIME
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)
        
        await update.message.reply_text(f"""🖥 𝗩𝗣𝗦 𝗦𝘁𝗮𝘁𝘂𝘀
━━━━━━━━━━━━━━━━━
𝗨𝗽𝘁𝗶𝗺𝗲 ➜ {uptime.days}d {uptime.seconds//3600}h {(uptime.seconds%3600)//60}m
𝗥𝗔𝗠 ➜ {memory.percent}%
𝗖𝗣𝗨 ➜ {cpu_percent}%
━━━━━━━━━━━━━━━━━
𝗦𝗲𝘀𝘀𝗶𝗼𝗻𝘀 ➜ {len(active_sessions)}
𝗕𝗜𝗡 𝗖𝗮𝗰𝗵𝗲 ➜ {len(_bin_lookup_module._cache)}
𝗗𝗕 𝗣𝗼𝗼𝗹 ➜ 20 max
━━━━━━━━━━━━━━━━━""")
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))

async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    try:
        await update.message.reply_text(f"""💎 𝗣𝗿𝗲𝗺𝗶𝘂𝗺 𝗣𝗹𝗮𝗻𝘀
━━━━━━━━━━━━━━━━━
🥉 𝟳𝗗 ━ $𝟱 · 🥈 𝟭𝟱𝗗 ━ $𝟭𝟬 · 🥇 𝟯𝟬𝗗 ━ $𝟮𝟬
━━━━━━━━━━━━━━━━━
📩 @{OWNER_USERNAME}""")
    except Exception as e:
        await update.message.reply_text("❌ Error.")
        logger.error(f"Buy error: {e}")

async def redeem_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    if not context.args:
        await update.message.reply_text("Usage: /redeem <KEY>")
        return
    
    try:
        from storage import redeem_key
        result = await redeem_key(update.effective_user.id, context.args[0])
        
        if result['success']:
            await update.message.reply_text(f"✅ 𝗣𝗿𝗲𝗺𝗶𝘂𝗺 𝗮𝗰𝘁𝗶𝘃𝗮𝘁𝗲𝗱 for {result['days']} days!\n𝗘𝘅𝗽𝗶𝗿𝗲𝘀: {result['expires_at'].strftime('%Y-%m-%d %H:%M')}")
        else:
            await update.message.reply_text(f"{result['error']}")
    except Exception as e:
        await update.message.reply_text("❌ Error redeeming key.")
        logger.error(f"Redeem error: {e}")

async def checkprem_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    try:
        from storage import get_premium_info
        info = await get_premium_info(update.effective_user.id)
        
        if info:
            await update.message.reply_text(f"💎 𝗣𝗿𝗲𝗺𝗶𝘂𝗺 𝗦𝘁𝗮𝘁𝘂𝘀\n━━━━━━━━━━━━━━━━━\n𝗣𝗹𝗮𝗻 ➜ {info['plan']}\n𝗘𝘅𝗽𝗶𝗿𝗲𝘀 ➜ {info['expires_at'].strftime('%Y-%m-%d %H:%M')}\n━━━━━━━━━━━━━━━━━")
        else:
            await update.message.reply_text("No premium subscription")
    except Exception as e:
        await update.message.reply_text("❌ Error checking premium.")
        logger.error(f"Checkprem error: {e}")

async def genkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import is_admin_or_owner, create_redeem_key
    if not await is_admin_or_owner(update.effective_user.id):
        await update.message.reply_text("Admin only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /genkey <days>")
        return
    
    try:
        days = int(context.args[0])
        key = await create_redeem_key(update.effective_user.id, days)
        await update.message.reply_text(f"🔑 𝗞𝗲𝘆 𝗚𝗲𝗻𝗲𝗿𝗮𝘁𝗲𝗱\n━━━━━━━━━━━━━━━━━\n𝗞𝗲𝘆 ➜ `{key}`\n𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻 ➜ {days} days\n━━━━━━━━━━━━━━━━━", parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))

async def addprem_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import is_admin_or_owner, add_premium_user
    if not await is_admin_or_owner(update.effective_user.id):
        await update.message.reply_text("Admin only.")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addprem <user_id> <days>")
        return
    
    try:
        user_id = int(context.args[0])
        days = int(context.args[1])
        if await add_premium_user(user_id, days):
            await update.message.reply_text(f"✅ Added {days} days premium to user {user_id}")
        else:
            await update.message.reply_text("Failed to add premium")
    except Exception as e:
        await update.message.reply_text(sanitize_error(str(e)))

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    from storage import OWNER_IDS, is_admin_or_owner

    if context.args and await is_admin_or_owner(user_id):
        code = context.args[0].upper()
        reason = ' '.join(context.args[1:]) if len(context.args) > 1 else None

        target_session = None
        for sid, info in active_sessions.items():
            if info.get('code') == code:
                target_session = info
                break

        if not target_session:
            await update.message.reply_text(f"No active session found with code {code}")
            return

        target_uid = target_session['user_id']
        stop_flags[target_uid] = True
        if reason:
            stop_reasons[target_uid] = reason

        reason_text = f"\nReason: {reason}" if reason else ""
        stopped_by = "Owner" if user_id in OWNER_IDS else "Admin"
        await update.message.reply_text(f"Stopping session {code} ({target_session.get('username', 'Unknown')} - {target_session.get('gate', '?')})...{reason_text}")

        try:
            msg = f"🚫 Your mass check has been stopped by {stopped_by}.{reason_text}"
            await context.bot.send_message(chat_id=target_session['chat_id'], text=msg)
        except Exception:
            pass
        return

    if user_id in active_tasks:
        stop_flags[user_id] = True
        await update.message.reply_text("Stopping mass check...")
    else:
        await update.message.reply_text("No active mass check")

async def session_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import is_admin_or_owner
    if not await is_admin_or_owner(update.effective_user.id):
        await update.message.reply_text("Admin only.")
        return

    try:
        if not active_sessions:
            await update.message.reply_text("No active mass check sessions.")
            return

        lines = [f"📋 𝗔𝗰𝘁𝗶𝘃𝗲 𝗦𝗲𝘀𝘀𝗶𝗼𝗻𝘀\n━━━━━━━━━━━━━━━━━"]
        for sid, info in active_sessions.items():
            code = info.get('code', '?')
            uname = info.get('username', 'Unknown')
            gate = info.get('gate', '?')
            total = info.get('total', 0)
            elapsed = int(time_module.time() - info.get('started_at', time_module.time()))
            mins, secs = divmod(elapsed, 60)
            lines.append(f"\n{to_bold(code)}\n𝗨𝘀𝗲𝗿 ➜ {to_mono(uname)}\n𝗚𝗮𝘁𝗲 ➜ {to_mono(gate)}\n𝗖𝗮𝗿𝗱𝘀 ➜ {to_mono(str(total))}\n𝗧𝗶𝗺𝗲 ➜ {to_mono(f'{mins}m {secs}s')}")
        lines.append(f"\n━━━━━━━━━━━━━━━━━━━━━━━\n  𝗧𝗼𝘁𝗮𝗹: {to_mono(str(len(active_sessions)))} {to_mono('session(s)')}\n  𝚃𝚘 𝚜𝚝𝚘𝚙: /stop <{to_mono('CODE')}> [{to_mono('reason')}]")

        await update.message.reply_text('\n'.join(lines))
    except Exception as e:
        await update.message.reply_text("❌ Error fetching sessions.")
        logger.error(f"Session error: {e}")

# ============ ADMIN MANAGEMENT (OWNER ONLY) ============

async def addadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import OWNER_IDS, add_admin, is_admin
    if update.effective_user.id not in OWNER_IDS:
        await update.message.reply_text("Owner only.")
        return

    target_id = None
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    elif context.args:
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid user ID.")
            return

    if not target_id:
        await update.message.reply_text("Usage: /addadmin <user_id> or reply to a user's message")
        return

    if target_id in OWNER_IDS:
        await update.message.reply_text("That user is already an owner.")
        return

    if await is_admin(target_id):
        await update.message.reply_text(f"User {target_id} is already an admin.")
        return

    success = await add_admin(target_id, update.effective_user.id)
    if success:
        await update.message.reply_text(f"✅ User {target_id} is now an admin.")
    else:
        await update.message.reply_text("Failed to add admin.")

async def rmadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import OWNER_IDS, remove_admin
    if update.effective_user.id not in OWNER_IDS:
        await update.message.reply_text("Owner only.")
        return

    target_id = None
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    elif context.args:
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid user ID.")
            return

    if not target_id:
        await update.message.reply_text("Usage: /rmadmin <user_id> or reply to a user's message")
        return

    success = await remove_admin(target_id)
    if success:
        await update.message.reply_text(f"✅ User {target_id} has been removed from admins.")
    else:
        await update.message.reply_text(f"User {target_id} is not an admin.")

# ============ BROADCAST ============

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import OWNER_IDS, get_all_bot_user_ids, get_all_bot_group_ids
    if update.effective_user.id not in OWNER_IDS:
        await update.message.reply_text("Owner only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>\n\nSend a message to all bot users and groups.")
        return
    
    broadcast_text = update.message.text.split(None, 1)[1]
    user_ids = await get_all_bot_user_ids()
    group_ids = await get_all_bot_group_ids()
    
    total = len(user_ids) + len(group_ids)
    if total == 0:
        await update.message.reply_text("No registered users or groups found.")
        return
    
    status_msg = await update.message.reply_text(f"📡 Broadcasting to {len(user_ids)} users & {len(group_ids)} groups...")
    
    sent, failed = 0, 0
    for cid in user_ids + group_ids:
        try:
            await context.bot.send_message(chat_id=cid, text=broadcast_text, parse_mode='HTML', disable_web_page_preview=True)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    
    await safe_send(status_msg.edit_text(f"📡 𝗕𝗿𝗼𝗮𝗱𝗰𝗮𝘀𝘁 𝗖𝗼𝗺𝗽𝗹𝗲𝘁𝗲\n━━━━━━━━━━━━━━━━━\n𝗦𝗲𝗻𝘁 ➜ {sent}\n𝗙𝗮𝗶𝗹𝗲𝗱 ➜ {failed}\n𝗨𝘀𝗲𝗿𝘀 ➜ {len(user_ids)}\n𝗚𝗿𝗼𝘂𝗽𝘀 ➜ {len(group_ids)}\n━━━━━━━━━━━━━━━━━"))

# ============ DELETE USER SUBSCRIPTION ============

async def deluser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import is_admin_or_owner, remove_premium_user, remove_premium_by_key
    if not await is_admin_or_owner(update.effective_user.id):
        await update.message.reply_text("Admin only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /deluser <user_id or redeem_key>\n\nRemoves premium subscription.")
        return
    
    try:
        arg = context.args[0].strip()
        
        if arg.isdigit():
            user_id = int(arg)
            success = await remove_premium_user(user_id)
            if success:
                _premium_cache.pop(user_id, None)
                await update.message.reply_text(f"✅ Premium removed for user {user_id}")
            else:
                await update.message.reply_text(f"❌ No premium subscription found for user {user_id}")
        else:
            result = await remove_premium_by_key(arg)
            if result['success']:
                _premium_cache.pop(result['user_id'], None)
                await update.message.reply_text(f"✅ Premium removed for user {result['user_id']} (key: {arg})")
            else:
                await update.message.reply_text(f"❌ {result['error']}")
    except Exception as e:
        await update.message.reply_text("❌ Error removing user.")
        logger.error(f"Deluser error: {e}")

# ============ BAN / UNBAN ============

async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import OWNER_IDS, is_admin_or_owner, ban_user, is_admin
    if not await is_admin_or_owner(update.effective_user.id):
        await update.message.reply_text("Admin only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id> [reason]")
        return
    
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    
    if target_id in OWNER_IDS:
        await update.message.reply_text("Cannot ban an owner.")
        return
    
    if await is_admin(target_id) and update.effective_user.id not in OWNER_IDS:
        await update.message.reply_text("Only the owner can ban an admin.")
        return
    
    reason = ' '.join(context.args[1:]) if len(context.args) > 1 else None
    success = await ban_user(target_id, update.effective_user.id, reason)
    
    if success:
        await update.message.reply_text(f"🚫 User {target_id} has been banned." + (f"\nReason: {reason}" if reason else ""))
    else:
        await update.message.reply_text("Failed to ban user.")

async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from storage import is_admin_or_owner, unban_user
    if not await is_admin_or_owner(update.effective_user.id):
        await update.message.reply_text("Admin only.")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return
    
    success = await unban_user(target_id)
    if success:
        await update.message.reply_text(f"✅ User {target_id} has been unbanned.")
    else:
        await update.message.reply_text(f"User {target_id} was not banned.")

# ============ PROXY MANAGEMENT ============

PROXY_NEEDED_MSG = """🔴 𝗣𝗿𝗼𝘅𝗶𝗲𝘀 𝗡𝗲𝗲𝗱𝗲𝗱
━━━━━━━━━━━━━━━━━
𝙿𝚕𝚎𝚊𝚜𝚎 𝙰𝚍𝚍 𝚈𝚘𝚞𝚛 𝙿𝚛𝚘𝚡𝚒𝚎𝚜 𝙱𝚎𝚏𝚘𝚛𝚎
𝚄𝚜𝚒𝚗𝚐 𝚃𝚑𝚎 𝙱𝚘𝚝.

𝙰𝚍𝚍 𝙿𝚛𝚘𝚡𝚒𝚎𝚜 𝚃𝚑𝚛𝚘𝚞𝚐𝚑:
/setproxy ip:port:user:pass
━━━━━━━━━━━━━━━━━"""

PROXY_PENDING_MSG = """⏳ 𝗣𝗿𝗼𝘅𝘆 𝗣𝗲𝗻𝗱𝗶𝗻𝗴 𝗔𝗽𝗽𝗿𝗼𝘃𝗮𝗹
━━━━━━━━━━━━━━━━━
𝚈𝚘𝚞𝚛 𝚙𝚛𝚘𝚡𝚢 𝚒𝚜 𝚊𝚠𝚊𝚒𝚝𝚒𝚗𝚐 𝚘𝚠𝚗𝚎𝚛
𝚊𝚙𝚙𝚛𝚘𝚟𝚊𝚕. 𝙿𝚕𝚎𝚊𝚜𝚎 𝚠𝚊𝚒𝚝 𝚔𝚒𝚗𝚍𝚕𝚢.
━━━━━━━━━━━━━━━━━"""

FREE_GATES_NO_PROXY = {'b3'}

async def check_user_proxy(user_id: int, gate_cmd: str, is_mass: bool = False) -> tuple:
    from storage import OWNER_IDS, get_approved_user_proxy, get_user_proxy
    if user_id in OWNER_IDS:
        return True, None, None
    if gate_cmd in FREE_GATES_NO_PROXY and not is_mass:
        return True, None, None
    proxy_str = await get_approved_user_proxy(user_id)
    if proxy_str:
        return True, [proxy_str], None
    proxy_info = await get_user_proxy(user_id)
    if proxy_info and proxy_info['status'] == 'pending':
        return False, None, PROXY_PENDING_MSG
    return False, None, PROXY_NEEDED_MSG

async def setproxy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name or "User"
    full_name = f"{update.effective_user.first_name or ''} {update.effective_user.last_name or ''}".strip() or username

    if not context.args:
        await update.message.reply_text("𝗨𝘀𝗮𝗴𝗲: /setproxy ip:port:user:pass")
        return

    proxy_str = context.args[0].strip()
    parts = proxy_str.split(':')
    if len(parts) < 2:
        await update.message.reply_text("❌ 𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚏𝚘𝚛𝚖𝚊𝚝. 𝚄𝚜𝚎: ip:port:user:pass")
        return

    status_msg = await safe_send(update.message.reply_text("🔄 𝚅𝚊𝚕𝚒𝚍𝚊𝚝𝚒𝚗𝚐 𝚙𝚛𝚘𝚡𝚢..."))
    if not status_msg:
        return

    try:
        from proxy_manager import get_proxy_info
        info = await get_proxy_info(proxy_str, timeout=20)

        if info['status'] != 'Live':
            fail_msg = f"""🔴 𝗣𝗿𝗼𝘅𝘆 𝗩𝗮𝗹𝗶𝗱𝗮𝘁𝗶𝗼𝗻 𝗙𝗮𝗶𝗹𝗲𝗱!
━━━━━━━━━━━━━━━━━
• 𝗦𝘁𝗮𝘁𝘂𝘀 ➜ ⛔ 𝙳𝚎𝚊𝚍
━━━━━━━━━━━━━━━━━
𝙿𝚕𝚎𝚊𝚜𝚎 𝚝𝚛𝚢 𝚊 𝚍𝚒𝚏𝚏𝚎𝚛𝚎𝚗𝚝 𝚙𝚛𝚘𝚡𝚢."""
            await safe_send(status_msg.edit_text(fail_msg))
            return

        ip = info.get('ip', 'Unknown')
        country = info.get('country', 'Unknown')
        is_rotating = info.get('is_rotating', False)
        rotation_text = "🔄 𝚁𝚘𝚝𝚊𝚝𝚒𝚗𝚐" if is_rotating else "📌 𝚂𝚝𝚊𝚝𝚒𝚌"

        from storage import set_user_proxy
        code = await set_user_proxy(user_id, proxy_str, ip=ip, country=country, is_rotating=is_rotating)

        success_msg = f"""🟢 𝗣𝗿𝗼𝘅𝘆 𝗩𝗮𝗹𝗶𝗱𝗮𝘁𝗶𝗼𝗻 𝗦𝘂𝗰𝗰𝗲𝘀𝘀𝗳𝘂𝗹!
━━━━━━━━━━━━━━━━━
• 𝗦𝘁𝗮𝘁𝘂𝘀 ➜ ✅ 𝙻𝙸𝚅𝙴
• 𝗥𝗼𝘁𝗮𝘁𝗶𝗼𝗻 ➜ {rotation_text}
• 𝗖𝗼𝘂𝗻𝘁𝗿𝘆 ➜ {to_mono(country)}
• 𝗜𝗣 ➜ {to_mono(ip)}
━━━━━━━━━━━━━━━━━

⏳ 𝗢𝘄𝗻𝗲𝗿 𝗔𝗽𝗽𝗿𝗼𝘃𝗮𝗹 𝗥𝗲𝗾𝘂𝗶𝗿𝗲𝗱
━━━━━━━━━━━━━━━━━
𝚈𝚘𝚞𝚛 𝙿𝚛𝚘𝚡𝚢 𝚁𝚎𝚚𝚞𝚎𝚜𝚝 𝙷𝚊𝚜 𝙱𝚎𝚎𝚗
𝚂𝚎𝚗𝚝 𝚃𝚘 𝚃𝚑𝚎 𝙾𝚠𝚗𝚎𝚛 𝙵𝚘𝚛 𝙰𝚙𝚙𝚛𝚘𝚟𝚊𝚕.
𝚈𝚘𝚞 𝚆𝚒𝚕𝚕 𝙱𝚎 𝙽𝚘𝚝𝚒𝚏𝚒𝚎𝚍 𝙾𝚗𝚌𝚎 𝙰𝚙𝚙𝚛𝚘𝚟𝚎𝚍.
━━━━━━━━━━━━━━━━━
𝙿𝚕𝚎𝚊𝚜𝚎 𝚆𝚊𝚒𝚝 𝙺𝚒𝚗𝚍𝚕𝚢 🙏"""

        await safe_send(status_msg.edit_text(success_msg))

        owner_msg = f"""📋 𝗣𝗿𝗼𝘅𝘆 𝗔𝗽𝗽𝗿𝗼𝘃𝗮𝗹 𝗥𝗲𝗾𝘂𝗲𝘀𝘁𝗲𝗱
━━━━━━━━━━━━━━━━━
• 𝗦𝘁𝗮𝘁𝘂𝘀 ➜ ✅ 𝙻𝙸𝚅𝙴
• 𝗥𝗼𝘁𝗮𝘁𝗶𝗼𝗻 ➜ {rotation_text}
• 𝗖𝗼𝘂𝗻𝘁𝗿𝘆 ➜ {to_mono(country)}
• 𝗜𝗣 ➜ {to_mono(ip)}
• 𝗣𝗿𝗼𝘅𝘆 ➜ <code>{proxy_str}</code>
• 𝗨𝘀𝗲𝗿 ➜ <a href="tg://user?id={user_id}">{to_mono(full_name)}</a> ({user_id})
━━━━━━━━━━━━━━━━━"""

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ 𝗔𝗽𝗽𝗿𝗼𝘃𝗲", callback_data=f"proxy_approve_{code}"),
                InlineKeyboardButton("❌ 𝗗𝗲𝗰𝗹𝗶𝗻𝗲", callback_data=f"proxy_decline_{code}")
            ]
        ])

        await safe_send(context.bot.send_message(
            chat_id=OWNER_ID,
            text=owner_msg,
            parse_mode='HTML',
            reply_markup=keyboard,
            disable_web_page_preview=True
        ))

    except Exception as e:
        logger.error(f"Set proxy error: {e}")
        await safe_send(status_msg.edit_text("❌ 𝙴𝚛𝚛𝚘𝚛 𝚟𝚊𝚕𝚒𝚍𝚊𝚝𝚒𝚗𝚐 𝚙𝚛𝚘𝚡𝚢. 𝚃𝚛𝚢 𝚊𝚐𝚊𝚒𝚗."))

async def proxy_approval_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    caller_id = update.effective_user.id
    from storage import OWNER_IDS
    if caller_id not in OWNER_IDS:
        await query.answer("Owner only.", show_alert=True)
        return

    data = query.data
    if data.startswith("proxy_approve_"):
        code = data.replace("proxy_approve_", "")
        from storage import approve_user_proxy
        result = await approve_user_proxy(code)
        if result:
            await query.answer("Proxy Approved!")
            await safe_send(query.message.edit_text(
                query.message.text + "\n\n✅ 𝗔𝗣𝗣𝗥𝗢𝗩𝗘𝗗",
                parse_mode='HTML',
                disable_web_page_preview=True
            ))
            try:
                await safe_send(context.bot.send_message(
                    chat_id=result['user_id'],
                    text="""✅ 𝗣𝗿𝗼𝘅𝘆 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱!
▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬

𝖸𝗈𝗎𝗋 𝗉𝗋𝗈𝗑𝗒 𝗋𝖾𝗊𝗎𝖾𝗌𝗍 𝗁𝖺𝗌 𝖻𝖾𝖊𝗇
𝖺𝗉𝗉𝗋𝗈𝗏𝖾𝖽 𝖻𝗒 𝗍𝗁𝖾 𝗈𝗐𝗇𝖾𝗋.
𝖸𝗈𝗎 𝖼𝖆𝗇 𝗇𝗈𝖜 𝗎𝗌𝖾 𝖆𝗅𝗅 𝗀𝖆𝗍𝖊𝗌!

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"""
                ))
            except:
                pass
        else:
            await query.answer("Code not found or already handled.", show_alert=True)

    elif data.startswith("proxy_decline_"):
        code = data.replace("proxy_decline_", "")
        from storage import decline_user_proxy
        result = await decline_user_proxy(code)
        if result:
            await query.answer("Proxy Declined!")
            await safe_send(query.message.edit_text(
                query.message.text + "\n\n❌ 𝗗𝗘𝗖𝗟𝗜𝗡𝗘𝗗",
                parse_mode='HTML',
                disable_web_page_preview=True
            ))
            try:
                await safe_send(context.bot.send_message(
                    chat_id=result['user_id'],
                    text="""❌ 𝗣𝗿𝗼𝘅𝘆 𝗥𝗲𝗷𝗲𝗰𝘁𝗲𝗱
▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬

𝖸𝗈𝗎𝗋 𝗉𝗋𝗈𝗑𝗒 𝗋𝖾𝗊𝗎𝖾𝗌𝗍 𝗁𝖺𝗌 𝖻𝖾𝖊𝗇
𝗋𝖾𝗃𝖊𝖈𝗍𝖾𝖉 𝖻𝗒 𝗍𝗁𝖾 𝗈𝗐𝗇𝖾𝗋.
𝖯𝗅𝖾𝖆𝗌𝖊 𝖼𝗈𝗇𝗍𝖆𝖈𝗍 𝗍𝗁𝖊 𝗈𝗐𝗇𝖊𝗋 𝗈𝗋
𝗍𝗋𝖆 𝗐𝗂𝗍𝗁 𝖆 𝖽𝗂𝖋𝖋𝖊𝗋𝖊𝗇𝗍 𝗉𝗋𝗈𝗑𝖞.

/setproxy ip:port:user:pass

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"""
                ))
            except:
                pass
        else:
            await query.answer("Code not found or already handled.", show_alert=True)

async def approved_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    from storage import OWNER_IDS
    if user_id not in OWNER_IDS:
        return
    if not context.args:
        await update.message.reply_text("Usage: /approved <code>")
        return
    code = context.args[0].strip().upper()
    from storage import approve_user_proxy
    result = await approve_user_proxy(code)
    if result:
        await update.message.reply_text(f"✅ Proxy approved for user {result['user_id']}")
        try:
            await safe_send(context.bot.send_message(
                chat_id=result['user_id'],
                text="""✅ 𝗣𝗿𝗼𝘅𝘆 𝗔𝗽𝗽𝗿𝗼𝘃𝗲𝗱!
▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬

𝖸𝗈𝗎𝗋 𝗉𝗋𝗈𝗑𝗒 𝗋𝖾𝗊𝗎𝖾𝗌𝗍 𝗁𝖺𝗌 𝖻𝖾𝖊𝗇
𝖺𝗉𝗉𝗋𝗈𝗏𝖾𝖽 𝖻𝗒 𝗍𝗁𝖾 𝗈𝗐𝗇𝖾𝗋.
𝖸𝗈𝗎 𝖼𝖆𝗇 𝗇𝗈𝖜 𝗎𝗌𝖾 𝖆𝗅𝗅 𝗀𝖆𝗍𝖊𝗌!

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"""
            ))
        except:
            pass
    else:
        await update.message.reply_text("❌ Invalid or already handled code.")

async def declined_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    from storage import OWNER_IDS
    if user_id not in OWNER_IDS:
        return
    if not context.args:
        await update.message.reply_text("Usage: /declined <code>")
        return
    code = context.args[0].strip().upper()
    from storage import decline_user_proxy
    result = await decline_user_proxy(code)
    if result:
        await update.message.reply_text(f"❌ Proxy declined for user {result['user_id']}")
        try:
            await safe_send(context.bot.send_message(
                chat_id=result['user_id'],
                text="""❌ 𝗣𝗿𝗼𝘅𝘆 𝗥𝗲𝗷𝗲𝗰𝘁𝗲𝗱
▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬

𝖸𝗈𝗎𝗋 𝗉𝗋𝗈𝗑𝗒 𝗋𝖾𝗊𝗎𝖾𝗌𝗍 𝗁𝖺𝗌 𝖻𝖾𝖊𝗇
𝗋𝖾𝗃𝖊𝖈𝗍𝖾𝖉 𝖻𝗒 𝗍𝗁𝖞 𝗈𝗐𝗇𝖾𝗋.
𝖯𝗅𝖾𝖆𝗌𝖊 𝖼𝗈𝗇𝗍𝖆𝖈𝗍 𝗍𝗁𝖾 𝗈𝗐𝗇𝖾𝗋 𝗈𝗋
𝗍𝗋𝖆 𝗐𝗂𝗍𝗁 𝖆 𝖽𝗂𝖋𝖋𝖊𝗋𝖔𝖓𝗍 𝗉𝗋𝗈𝗑𝖞.

/setproxy ip:port:user:pass

▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬"""
            ))
        except:
            pass
    else:
        await update.message.reply_text("❌ Invalid or already handled code.")

async def myproxy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    try:
        user_id = update.effective_user.id
        from storage import get_user_proxy
        proxy_info = await get_user_proxy(user_id)
        if not proxy_info:
            await update.message.reply_text(PROXY_NEEDED_MSG)
            return
        
        status_emoji = "✅" if proxy_info['status'] == 'approved' else "⏳" if proxy_info['status'] == 'pending' else "⛔"
        status_text = "𝙰𝚙𝚙𝚛𝚘𝚟𝚎𝚍" if proxy_info['status'] == 'approved' else "𝙿𝚎𝚗𝚍𝚒𝚗𝚐" if proxy_info['status'] == 'pending' else "𝙳𝚎𝚊𝚍"
        rotation = "🔄 𝚁𝚘𝚝𝚊𝚝𝚒𝚗𝚐" if proxy_info.get('is_rotating') else "📌 𝚂𝚝𝚊𝚝𝚒𝚌"
        expires = proxy_info.get('expires_at')
        exp_text = expires.strftime('%Y-%m-%d %H:%M') if expires else "N/A"

        msg = f"""📋 𝗬𝗼𝘂𝗿 𝗣𝗿𝗼𝘅𝘆 𝗜𝗻𝗳𝗼
━━━━━━━━━━━━━━━━━
• 𝗦𝘁𝗮𝘁𝘂𝘀 ➜ {status_emoji} {status_text}
• 𝗥𝗼𝘁𝗮𝘁𝗶𝗼𝗻 ➜ {rotation}
• 𝗖𝗼𝘂𝗻𝘁𝗿𝘆 ➜ {to_mono(proxy_info.get('country', 'Unknown'))}
• 𝗜𝗣 ➜ {to_mono(proxy_info.get('ip', 'Unknown'))}
• 𝗘𝘅𝗽𝗶𝗿𝗲𝘀 ➜ {to_mono(exp_text)}
━━━━━━━━━━━━━━━━━"""
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text("❌ Error fetching proxy info.")
        logger.error(f"Myproxy error: {e}")

async def rproxy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await send_join_required(update, context):
        return
    try:
        user_id = update.effective_user.id
        from storage import get_user_proxy, remove_user_proxy
        proxy_info = await get_user_proxy(user_id)
        if not proxy_info:
            await update.message.reply_text("""❌ 𝗡𝗼 𝗣𝗿𝗼𝘅𝘆 𝗙𝗼𝘂𝗻𝗱
━━━━━━━━━━━━━━━━━
𝚈𝚘𝚞 𝚍𝚘𝚗'𝚝 𝚑𝚊𝚟𝚎 𝚊𝚗𝚢 𝚙𝚛𝚘𝚡𝚢
𝚊𝚍𝚍𝚎𝚍 𝚒𝚗 𝚝𝚑𝚎 𝚋𝚘𝚝.

𝙰𝚍𝚍 𝚘𝚗𝚎 𝚠𝚒𝚝𝚑:
/setproxy ip:port:user:pass
━━━━━━━━━━━━━━━━━""")
            return
        removed = await remove_user_proxy(user_id)
        if removed:
            await update.message.reply_text("""✅ 𝗣𝗿𝗼𝘅𝘆 𝗥𝗲𝗺𝗼𝘃𝗲𝗱
━━━━━━━━━━━━━━━━━
𝙰𝚕𝚕 𝚢𝚘𝚞𝚛 𝚙𝚛𝚘𝚡𝚒𝚎𝚜 𝚑𝚊𝚟𝚎 𝚋𝚎𝚎𝚗
𝚜𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕𝚕𝚢 𝚛𝚎𝚖𝚘𝚟𝚎𝚍.

𝚃𝚘 𝚊𝚍𝚍 𝚊 𝚗𝚎𝚠 𝚘𝚗𝚎:
/setproxy ip:port:user:pass
━━━━━━━━━━━━━━━━━""")
        else:
            await update.message.reply_text("❌ Error removing proxy. Try again.")
    except Exception as e:
        await update.message.reply_text("❌ Error removing proxy.")
        logger.error(f"Rproxy error: {e}")

# ============ DATABASE INIT ============

async def init_bot_db():
    from storage import init_db
    await init_db()

# ============ ERROR HANDLER ============

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, RetryAfter):
        logger.warning(f"Flood control: retry after {context.error.retry_after}s")
        return
    if isinstance(context.error, (TimedOut, NetworkError)):
        logger.warning(f"Network issue: {context.error}")
        return
    logger.error(f"Update {update} caused error: {context.error}")

# ============ MAIN ============

def main():
    if not BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return
    
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(True).connect_timeout(30).read_timeout(30).write_timeout(30).pool_timeout(30).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("pp", pp_command))
    app.add_handler(CommandHandler("mpp", mpp_command))
    app.add_handler(CommandHandler("pp2", pp2_command))
    app.add_handler(CommandHandler("mpp2", mpp2_command))
    app.add_handler(CommandHandler("sc", sc_command))
    app.add_handler(CommandHandler("msc", msc_command))
    app.add_handler(CommandHandler("sc2", sc2_command))
    app.add_handler(CommandHandler("msc2", msc2_command))
    app.add_handler(CommandHandler("pp3", pp3_command))
    app.add_handler(CommandHandler("mpp3", mpp3_command))
    app.add_handler(CommandHandler("pp4", pp4_command))
    app.add_handler(CommandHandler("mpp4", mpp4_command))
    app.add_handler(CommandHandler("b3", b3_command))
    app.add_handler(CommandHandler("mb3", mb3_command))
    
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("vps", vps_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("session", session_command))
    
    app.add_handler(CommandHandler("buy", buy_command))
    app.add_handler(CommandHandler("redeem", redeem_command))
    app.add_handler(CommandHandler("checkprem", checkprem_command))
    app.add_handler(CommandHandler("premium", checkprem_command))
    app.add_handler(CommandHandler("genkey", genkey_command))
    app.add_handler(CommandHandler("addprem", addprem_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("deluser", deluser_command))
    app.add_handler(CommandHandler("ban", ban_command))
    app.add_handler(CommandHandler("unban", unban_command))
    app.add_handler(CommandHandler("addadmin", addadmin_command))
    app.add_handler(CommandHandler("rmadmin", rmadmin_command))
    app.add_handler(CommandHandler("setproxy", setproxy_command))
    app.add_handler(CommandHandler("myproxy", myproxy_command))
    app.add_handler(CommandHandler("rproxy", rproxy_command))
    app.add_handler(CommandHandler("approved", approved_command))
    app.add_handler(CommandHandler("declined", declined_command))
    
    app.add_handler(CallbackQueryHandler(check_joined_callback, pattern="^check_joined$"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_"))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern="^gates_"))
    app.add_handler(CallbackQueryHandler(stop_callback, pattern="^stop_"))
    app.add_handler(CallbackQueryHandler(proxy_approval_callback, pattern="^proxy_"))
    
    app.add_error_handler(error_handler)
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_bot_db())
    
    cleanup_task = loop.create_task(periodic_cleanup())
    
    global _proxy_check_bot
    _proxy_check_bot = app.bot
    proxy_task = loop.create_task(periodic_proxy_check())
    
    logger.info("Bot starting with production config (pool=20, rate limits, cleanup)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    import fcntl
    import sys
    import time
    
    LOCK_FILE = "/tmp/fn_checker_bot.lock"
    
    try:
        lock_fd = open(LOCK_FILE, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
    except (IOError, BlockingIOError):
        logger.error("Another bot instance is already running. Exiting.")
        sys.exit(1)
    
    time.sleep(2)
    
    try:
        main()
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()
            os.remove(LOCK_FILE)
        except:
            pass
