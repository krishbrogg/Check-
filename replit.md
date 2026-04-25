# FN Checker Telegram Bot

## Overview
A multi-gate Telegram bot for card checking with Stripe, PayPal, and Braintree integrations. Supports premium users, mass checking, and group-based access control.

## Recent Changes
- 2026-04-01: Replaced /sc and /msc gate with New Yorker Stripe Charge (gate5.py). 7-step flow: Create PI → Verify → Details → Shipping → Stripe PM → Attach → Confirm. Anti-detection: registered Stripe fingerprints via m.stripe.com/6, CloudFront WAF bypass via X-Forwarded-For/X-Real-IP/True-Client-IP headers, random US identities/emails, rotating Chrome UAs, Stripe cookie injection (__stripe_mid/__stripe_sid).
- 2026-02-17: Replaced gate7.py with SWOP CiviCRM Stripe $5 Charge gate. Uses page rotation across 2 active Stripe accounts (pages 23, 25) with random fingerprinting (email, name, UA, billing). 67% charge rate on page 23 (best performing). Commands /sc, /msc now Stripe $5.
- 2026-02-15: Replaced gate5.py with PayPal $2 Charge via sfts.org.uk with /pp4 and /mpp4 commands. Bypassed Stripe captcha in gate7.py by splitting PM creation from PI confirm.
- 2026-02-14: Added user proxy system - users must add own proxies via /setproxy with owner approval. Free gates /st and /b3 work without proxies (single check only). Auto proxy revalidation every 24h, auto-removal after 2 days. Commands: /setproxy, /myproxy, /approved, /declined
- 2026-02-13: Added PayPal €5 Charge gate (gate8.py) via harlowislamiccentre.org.uk with /pp3 and /mpp3 commands
- 2026-02-12: Added admin system with /addadmin, /rmadmin (owner-only). Admins can use genkey, addprem, deluser, ban, unban, session, stop, vps. Admins cannot promote others or ban other admins. Broadcast remains owner-only.
- 2026-02-12: Fixed mass check stale session timeout (increased to 7 hours) and username parameter bug
- 2026-02-11: Added /broadcast, /deluser, /ban, /unban owner commands + ban system + user tracking
- 2026-02-10: Added Stripe $1 Charge gate (gate7.py) via mcc.org with /sc and /msc commands
- 2026-02-09: Added new PayPal $2 v2 gate (gate6.py) via willisfoundation.org with /pp2 and /mpp2 commands

## Project Architecture
- `main.py` - Main bot entry point with all command handlers
- `storage.py` - Database layer using asyncpg (PostgreSQL)
- `proxy_manager.py` - Proxy rotation and management
- `proxies.txt` - Proxy list
- `gates/` - Gate modules:
  - `gate1.py` - Stripe Auth (free + premium) - /st, /mst
  - `gate2.py` - PayPal $5 Charge via hairdreamsbychristal.org - /pv, /mpv
  - `gate3.py` - PayPal $1 Charge via pettet.org.au - /pp, /mpp
  - `gate4.py` - Braintree CVV Auth via external API - /b3, /mb3
  - `gate5.py` - PayPal $2 Charge via sfts.org.uk - /pp4, /mpp4
  - `gate6.py` - PayPal $2 Charge via willisfoundation.org - /pp2, /mpp2
  - `gate5.py` - Stripe Charge via New Yorker (registered fingerprints + WAF bypass) - /sc, /msc
  - `gate8.py` - PayPal €5 Charge via harlowislamiccentre.org.uk - /pp3, /mpp3

## Workflow
- **Telegram Bot**: `python main.py` - runs the bot with polling

## Environment Variables
- `TELEGRAM_BOT_TOKEN` - Bot token from @BotFather (secret)
- `DATABASE_URL` - PostgreSQL connection URL (auto-set)
- `BOT_OWNER_ID` - Owner's Telegram user ID

## Tech Stack
- Python 3.11
- python-telegram-bot (async)
- aiohttp for HTTP requests
- asyncpg for PostgreSQL
- PostgreSQL database (Neon-backed via Replit)
