import asyncio
import random
import aiohttp

class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.lock = asyncio.Lock()
        self.index = 0
        self.user_index = {}
    
    def load_proxies(self, filepath='proxies.txt'):
        try:
            with open(filepath, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            return len(self.proxies)
        except FileNotFoundError:
            self.proxies = []
            return 0
    
    def save_proxies(self, filepath='proxies.txt'):
        with open(filepath, 'w') as f:
            for proxy in self.proxies:
                f.write(proxy + '\n')
    
    def add_global_proxy(self, proxy: str) -> bool:
        proxy = proxy.strip()
        if proxy and proxy not in self.proxies:
            self.proxies.append(proxy)
            self.save_proxies()
            return True
        return False
    
    def remove_global_proxy(self, proxy: str) -> bool:
        proxy = proxy.strip()
        if proxy in self.proxies:
            self.proxies.remove(proxy)
            self.save_proxies()
            return True
        return False
    
    def get_global_proxies(self):
        return self.proxies.copy()
    
    def parse_proxy(self, proxy_str):
        try:
            if '@' in proxy_str:
                user_pass, host_port = proxy_str.split('@')
                user, passwd = user_pass.split(':')
                host, port = host_port.split(':')
            else:
                parts = proxy_str.split(':')
                if len(parts) == 4:
                    host, port, user, passwd = parts
                else:
                    host, port = parts[0], parts[1]
                    user, passwd = None, None
            
            if user and passwd:
                return {
                    'user': user,
                    'password': passwd,
                    'host': host,
                    'port': int(port),
                    'url': f"http://{user}:{passwd}@{host}:{port}"
                }
            else:
                return {
                    'user': None,
                    'password': None,
                    'host': host,
                    'port': int(port),
                    'url': f"http://{host}:{port}"
                }
        except:
            return None
    
    async def get_proxy(self):
        async with self.lock:
            if not self.proxies:
                return None
            proxy_str = self.proxies[self.index % len(self.proxies)]
            self.index += 1
            return self.parse_proxy(proxy_str)
    
    def get_random_proxy(self):
        if not self.proxies:
            return None
        proxy_str = random.choice(self.proxies)
        return self.parse_proxy(proxy_str)
    
    def get_random_proxy_from_list(self, proxy_list):
        if not proxy_list:
            return None
        proxy_str = random.choice(proxy_list)
        return self.parse_proxy(proxy_str)
    
    def get_aiohttp_proxy(self, proxy_list=None):
        if proxy_list:
            proxy = self.get_random_proxy_from_list(proxy_list)
        else:
            proxy = self.get_random_proxy()
        if proxy:
            return proxy['url']
        return None
    
    def get_playwright_proxy(self):
        proxy = self.get_random_proxy()
        if proxy:
            return {
                'server': f"http://{proxy['host']}:{proxy['port']}",
                'username': proxy['user'],
                'password': proxy['password']
            }
        return None

async def validate_proxy(proxy_str: str, timeout: int = 10) -> bool:
    try:
        parsed = proxy_manager.parse_proxy(proxy_str)
        if not parsed:
            return False
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get('https://httpbin.org/ip', proxy=parsed['url']) as resp:
                return resp.status == 200
    except:
        return False

async def get_proxy_info(proxy_str: str, timeout: int = 15) -> dict:
    """Get detailed proxy information"""
    result = {
        'proxy': proxy_str,
        'status': 'Dead',
        'ip': None,
        'country': None,
        'country_code': None,
        'type': 'Unknown',
        'is_rotating': False,
        'response_time': None
    }
    
    try:
        parsed = proxy_manager.parse_proxy(proxy_str)
        if not parsed:
            return result
        
        # Detect proxy type from port
        port = parsed['port']
        if port in [1080, 1081, 1082]:
            result['type'] = 'SOCKS5'
        elif port in [3128, 8080, 8888, 80]:
            result['type'] = 'HTTP'
        elif port == 443:
            result['type'] = 'HTTPS'
        else:
            result['type'] = 'HTTP/HTTPS'
        
        import time
        start_time = time.time()
        
        # First request to get IP
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get('https://httpbin.org/ip', proxy=parsed['url']) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    first_ip = data.get('origin', '').split(',')[0].strip()
                    result['ip'] = first_ip
                    result['status'] = 'Live'
                    result['response_time'] = round((time.time() - start_time) * 1000)
                else:
                    return result
        
        # Get geo info
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(f'http://ip-api.com/json/{result["ip"]}?fields=country,countryCode') as geo_resp:
                    if geo_resp.status == 200:
                        geo_data = await geo_resp.json()
                        result['country'] = geo_data.get('country', 'Unknown')
                        result['country_code'] = geo_data.get('countryCode', 'XX')
        except:
            result['country'] = 'Unknown'
            result['country_code'] = 'XX'
        
        # Check if rotating (make second request)
        try:
            await asyncio.sleep(0.5)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                async with session.get('https://httpbin.org/ip', proxy=parsed['url']) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        second_ip = data.get('origin', '').split(',')[0].strip()
                        if second_ip != first_ip:
                            result['is_rotating'] = True
        except:
            pass
        
        return result
        
    except Exception as e:
        return result

proxy_manager = ProxyManager()
proxy_manager.load_proxies()
