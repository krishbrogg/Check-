import asyncio
import aiohttp
import logging

logger = logging.getLogger(__name__)

BIN_API = "https://bins.antipublic.cc/bins/{}"

_cache: dict = {}
_locks: dict = {}
_locks_lock = asyncio.Lock()

EMPTY = {'bin_info': 'Unknown', 'bank': 'Unknown', 'country': 'Unknown'}


def _build_result(data: dict) -> dict:
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
    flag = data.get('country_flag', '')
    return {
        'bin_info': f"{brand} - {card_type} - {level}",
        'bank': bank,
        'country': f"{country} {flag}".strip(),
    }


async def lookup_bin(card_number: str) -> dict:
    bin6 = card_number[:6]

    if bin6 in _cache:
        return _cache[bin6]

    try:
        from storage import get_cached_bin, cache_bin
        cached = await get_cached_bin(bin6)
        if cached:
            _cache[bin6] = cached
            return cached
    except Exception:
        pass

    async with _locks_lock:
        if bin6 not in _locks:
            _locks[bin6] = asyncio.Lock()
        lock = _locks[bin6]

    async with lock:
        if bin6 in _cache:
            return _cache[bin6]

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            ) as session:
                async with session.get(BIN_API.format(bin6)) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        result = _build_result(data)
                        _cache[bin6] = result
                        try:
                            from storage import cache_bin
                            await cache_bin(bin6, data)
                        except Exception:
                            pass
                        return result
                    else:
                        logger.warning(f"[BIN] HTTP {resp.status} for {bin6}")
        except Exception as e:
            logger.warning(f"[BIN] Lookup failed for {bin6}: {e}")

    return EMPTY


def clear_cache():
    _cache.clear()
    _locks.clear()
