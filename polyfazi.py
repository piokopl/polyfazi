#!/usr/bin/env python3
"""
PolyFazi Trading Bot
Trading bot for 15-minute crypto prediction markets on Polymarket
Logic: Buy tokens when price reaches defined thresholds with optional retries and multiplier

Configuration via .env file:
- CHECK_DELAY_SEC: Seconds after window opens to check prices (e.g., 10 = check at 1:10 if window opens at 1:00)
- UP_THRESHOLD: Minimum price for UP token to trigger buy (e.g., 0.45)
- DOWN_THRESHOLD: Minimum price for DOWN token to trigger buy (e.g., 0.45)
- BET_USD: Base bet amount in USD
- RETRIES_ENABLED: YES/NO - enable retry purchases
- MAX_RETRIES: Number of retries (each retry is 1 minute apart)
- RETRY_MULTIPLIER: Multiply bet amount on each retry (e.g., 1.5 = 50% increase)
- MARKETS: Comma-separated list of markets (BTC,ETH,XRP,SOL)
"""

import os
import sys
import time
import json
import sqlite3
import requests
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv, set_key
from eth_account import Account
from web3 import Web3
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, ApiCreds
from py_clob_client.order_builder.constants import BUY

# ========================== CONFIGURATION ==========================

load_dotenv()

# Timing configuration
CHECK_DELAY_SEC = int(os.getenv("CHECK_DELAY_SEC", "10"))  # Seconds after window opens to check
if CHECK_DELAY_SEC < 0:
    CHECK_DELAY_SEC = 0
if CHECK_DELAY_SEC > 840:  # Max 14 minutes into 15-min window
    CHECK_DELAY_SEC = 840

# Price thresholds for buying
UP_THRESHOLD = float(os.getenv("UP_THRESHOLD", "0.45"))
DOWN_THRESHOLD = float(os.getenv("DOWN_THRESHOLD", "0.45"))

# Bet configuration
BET_USD = float(os.getenv("BET_USD", "1.1"))

# Retry configuration
RETRIES_ENABLED = os.getenv("RETRIES_ENABLED", "NO").upper() == "YES"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_MULTIPLIER = float(os.getenv("RETRY_MULTIPLIER", "1.0"))  # 1.0 = no increase, 1.5 = 50% increase

# Markets
MARKETS_ENV = os.getenv("MARKETS", "BTC,ETH,XRP,SOL")
MARKETS = [m.strip().upper() for m in MARKETS_ENV.split(",") if m.strip()]

# Wallet configuration
PROXY_PK = os.getenv("PROXY_PK")
FUNDER_PROXY = os.getenv("FUNDER_PROXY", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")

if not PROXY_PK or not PROXY_PK.startswith("0x"):
    raise SystemExit("Missing PROXY_PK in .env!")

# Paths
BASE_DIR = os.getenv("BASE_DIR", "/home/ubuntu/polyfazi")
LOG_FILE = f"{BASE_DIR}/logs/trades.log"
DB_FILE = f"{BASE_DIR}/trades.db"
REPORTS_DIR = f"{BASE_DIR}/logs/reports"
os.makedirs(f"{BASE_DIR}/logs/reports", exist_ok=True)

# Polymarket configuration
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CHAIN_ID = 137
SIGNATURE_TYPE = 2
POLYGON_RPC = "https://polygon-rpc.com"
USDC_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# ========================== LOGGER ==========================

def log(text: str) -> None:
    """Log message to console and file"""
    line = f"[{datetime.now(tz=ZoneInfo('UTC')).strftime('%Y-%m-%d %H:%M:%S UTC')}] {text}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def send_discord(msg: str) -> None:
    """Send notification to Discord webhook"""
    if not DISCORD_WEBHOOK:
        return
    try:
        requests.post(DISCORD_WEBHOOK, json={"content": msg}, timeout=5)
    except Exception:
        pass

# ========================== WEB3 ==========================

w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))

def get_balance(addr: str) -> float:
    """Get USDC balance for address"""
    try:
        abi = '[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"}]'
        contract = w3.eth.contract(address=USDC_ADDRESS, abi=abi)
        raw = contract.functions.balanceOf(Web3.to_checksum_address(addr)).call()
        return raw / 1e6
    except Exception:
        return 0.0

# ========================== CLOB CLIENT ==========================

client = ClobClient(
    host=CLOB_HOST,
    key=PROXY_PK,
    chain_id=CHAIN_ID,
    signature_type=SIGNATURE_TYPE,
    funder=FUNDER_PROXY or None,
)

if not hasattr(client, "builder_config"):
    client.builder_config = None

def setup_api_creds() -> None:
    """Setup API credentials from .env or generate new ones"""
    api_key = os.getenv("API_KEY")
    api_secret = os.getenv("API_SECRET")
    api_passphrase = os.getenv("API_PASSPHRASE")

    if api_key and api_secret and api_passphrase:
        try:
            creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
            client.set_api_creds(creds)
            log("✓ API credentials loaded from .env")
            return
        except Exception as e:
            log(f"⚠ Error loading API creds from .env: {e}")

    try:
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        set_key(".env", "API_KEY", creds.api_key)
        set_key(".env", "API_SECRET", creds.api_secret)
        set_key(".env", "API_PASSPHRASE", creds.api_passphrase)
        log("✓ API credentials generated and saved")
    except Exception as e:
        log(f"❌ FATAL: API credentials error: {e}")
        raise

# ========================== DATABASE ==========================

def init_database():
    """Initialize SQLite database"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            symbol TEXT,
            window_start TEXT,
            window_end TEXT,
            slug TEXT,
            token_id TEXT,
            condition_id TEXT,
            side TEXT,
            trigger_price REAL,
            threshold REAL,
            entry_price REAL,
            size REAL,
            bet_usd REAL,
            retry_number INTEGER,
            order_status TEXT,
            order_id TEXT,
            final_outcome TEXT,
            exit_price REAL,
            pnl_usd REAL,
            roi_pct REAL,
            settled BOOLEAN DEFAULT 0,
            settled_at TEXT,
            claimed BOOLEAN DEFAULT 0,
            claimed_at TEXT,
            claim_tx TEXT
        )
    ''')
    
    # Add new columns if they don't exist (for existing databases)
    # Must be done before creating indexes on those columns
    try:
        c.execute('ALTER TABLE trades ADD COLUMN condition_id TEXT')
    except:
        pass
    try:
        c.execute('ALTER TABLE trades ADD COLUMN claimed BOOLEAN DEFAULT 0')
    except:
        pass
    try:
        c.execute('ALTER TABLE trades ADD COLUMN claimed_at TEXT')
    except:
        pass
    try:
        c.execute('ALTER TABLE trades ADD COLUMN claim_tx TEXT')
    except:
        pass
    
    # Now create indexes (after columns exist)
    c.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_settled ON trades(settled)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_claimed ON trades(claimed)')
    
    conn.commit()
    conn.close()
    log("✓ Database initialized")

def save_trade(**kwargs):
    """Save trade to database"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO trades (timestamp, symbol, window_start, window_end, slug, token_id,
        condition_id, side, trigger_price, threshold, entry_price, size, bet_usd, retry_number,
        order_status, order_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        datetime.now(tz=ZoneInfo('UTC')).isoformat(),
        kwargs['symbol'], kwargs['window_start'], kwargs['window_end'],
        kwargs['slug'], kwargs['token_id'], kwargs.get('condition_id'),
        kwargs['side'], kwargs['trigger_price'],
        kwargs['threshold'], kwargs['entry_price'], kwargs['size'], kwargs['bet_usd'],
        kwargs['retry_number'], kwargs['order_status'], kwargs['order_id']
    ))
    trade_id = c.lastrowid
    conn.commit()
    conn.close()
    log(f"✓ Trade #{trade_id} saved to database")
    return trade_id

# ========================== MARKET DATA ==========================

def get_current_slug(symbol: str) -> str:
    """Generate slug for current 15-minute window"""
    now_et = datetime.now(tz=ZoneInfo("America/New_York"))
    minute_slot = (now_et.minute // 15) * 15
    window_start_et = now_et.replace(minute=minute_slot, second=0, microsecond=0)
    window_start_utc = window_start_et.astimezone(ZoneInfo("UTC"))
    ts = int(window_start_utc.timestamp())
    slug = f"{symbol.lower()}-updown-15m-{ts}"
    return slug

def get_window_times(symbol: str):
    """Get window start and end times in ET"""
    now_et = datetime.now(tz=ZoneInfo("America/New_York"))
    minute_slot = (now_et.minute // 15) * 15
    window_start_et = now_et.replace(minute=minute_slot, second=0, microsecond=0)
    window_end_et = window_start_et + timedelta(minutes=15)
    return window_start_et, window_end_et

def get_token_ids(symbol: str):
    """Get UP and DOWN token IDs and condition_id from Gamma API"""
    slug = get_current_slug(symbol)
    log(f"[{symbol}] Looking for market: {slug}")
    
    for attempt in range(1, 13):
        try:
            r = requests.get(f"{GAMMA_API_BASE}/markets/slug/{slug}", timeout=5)
            if r.status_code == 200:
                m = r.json()
                clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
                condition_id = m.get("conditionId") or m.get("condition_id")
                
                if isinstance(clob_ids, str):
                    try:
                        clob_ids = json.loads(clob_ids)
                    except:
                        clob_ids = [x.strip().strip('"') for x in clob_ids.strip("[]").split(",")]
                if isinstance(clob_ids, list) and len(clob_ids) >= 2:
                    log(f"[{symbol}] Tokens found: UP {clob_ids[0][:10]}... | DOWN {clob_ids[1][:10]}...")
                    if condition_id:
                        log(f"[{symbol}] Condition ID: {condition_id[:20]}...")
                    return clob_ids[0], clob_ids[1], condition_id
        except Exception as e:
            log(f"[{symbol}] Error fetching tokens: {e}")
        if attempt < 12:
            time.sleep(4)
    return None, None, None

def get_token_prices(up_token: str, down_token: str) -> tuple:
    """
    Get current prices for UP and DOWN tokens from order book.
    Returns (up_price, down_price) as mid prices, or (None, None) on error.
    """
    up_price = None
    down_price = None
    
    # Get UP token price
    try:
        book = client.get_order_book(up_token)
        if isinstance(book, dict):
            bids = book.get("bids", []) or []
            asks = book.get("asks", []) or []
        else:
            bids = getattr(book, "bids", []) or []
            asks = getattr(book, "asks", []) or []
        
        log(f"  [DEBUG] UP order book: {len(bids)} bids, {len(asks)} asks")
        
        if bids and asks:
            # Try to get best bid (highest bid) and best ask (lowest ask)
            # Order books are typically sorted - bids descending, asks ascending
            # But py_clob_client may return them differently, so we find max/min
            best_bid = None
            best_ask = None
            
            for bid in bids:
                price = float(bid.price) if hasattr(bid, 'price') else float(bid.get('price', 0))
                if best_bid is None or price > best_bid:
                    best_bid = price
            
            for ask in asks:
                price = float(ask.price) if hasattr(ask, 'price') else float(ask.get('price', 0))
                if best_ask is None or price < best_ask:
                    best_ask = price
            
            if best_bid and best_ask:
                up_price = (best_bid + best_ask) / 2.0
                log(f"  [DEBUG] UP: best_bid={best_bid:.4f}, best_ask={best_ask:.4f}, mid={up_price:.4f}")
        elif bids:
            # Only bids available - use highest bid as price estimate
            best_bid = max(float(b.price) if hasattr(b, 'price') else float(b.get('price', 0)) for b in bids)
            up_price = best_bid
            log(f"  [DEBUG] UP: only bids, using best_bid={best_bid:.4f}")
        elif asks:
            # Only asks available - use lowest ask as price estimate
            best_ask = min(float(a.price) if hasattr(a, 'price') else float(a.get('price', 0)) for a in asks)
            up_price = best_ask
            log(f"  [DEBUG] UP: only asks, using best_ask={best_ask:.4f}")
        else:
            log(f"  [DEBUG] UP: empty order book")
            
    except Exception as e:
        log(f"Error getting UP price: {e}")
        import traceback
        log(traceback.format_exc())
    
    # Get DOWN token price
    try:
        book = client.get_order_book(down_token)
        if isinstance(book, dict):
            bids = book.get("bids", []) or []
            asks = book.get("asks", []) or []
        else:
            bids = getattr(book, "bids", []) or []
            asks = getattr(book, "asks", []) or []
        
        log(f"  [DEBUG] DOWN order book: {len(bids)} bids, {len(asks)} asks")
        
        if bids and asks:
            best_bid = None
            best_ask = None
            
            for bid in bids:
                price = float(bid.price) if hasattr(bid, 'price') else float(bid.get('price', 0))
                if best_bid is None or price > best_bid:
                    best_bid = price
            
            for ask in asks:
                price = float(ask.price) if hasattr(ask, 'price') else float(ask.get('price', 0))
                if best_ask is None or price < best_ask:
                    best_ask = price
            
            if best_bid and best_ask:
                down_price = (best_bid + best_ask) / 2.0
                log(f"  [DEBUG] DOWN: best_bid={best_bid:.4f}, best_ask={best_ask:.4f}, mid={down_price:.4f}")
        elif bids:
            best_bid = max(float(b.price) if hasattr(b, 'price') else float(b.get('price', 0)) for b in bids)
            down_price = best_bid
            log(f"  [DEBUG] DOWN: only bids, using best_bid={best_bid:.4f}")
        elif asks:
            best_ask = min(float(a.price) if hasattr(a, 'price') else float(a.get('price', 0)) for a in asks)
            down_price = best_ask
            log(f"  [DEBUG] DOWN: only asks, using best_ask={best_ask:.4f}")
        else:
            log(f"  [DEBUG] DOWN: empty order book")
            
    except Exception as e:
        log(f"Error getting DOWN price: {e}")
        import traceback
        log(traceback.format_exc())
    
    return up_price, down_price

def get_best_ask_price(token_id: str) -> float:
    """Get best ask price for a token (price we would pay to buy)"""
    try:
        book = client.get_order_book(token_id)
        if isinstance(book, dict):
            asks = book.get("asks", []) or []
            bids = book.get("bids", []) or []
        else:
            asks = getattr(book, "asks", []) or []
            bids = getattr(book, "bids", []) or []
        
        if asks:
            # Best ask is the lowest ask price
            best_ask = min(float(a.price) if hasattr(a, 'price') else float(a.get('price', 0)) for a in asks)
            return best_ask
        elif bids:
            # Fallback to highest bid if no asks
            best_bid = max(float(b.price) if hasattr(b, 'price') else float(b.get('price', 0)) for b in bids)
            log(f"  [WARNING] No asks available, using best bid {best_bid:.4f} as price")
            return best_bid
    except Exception as e:
        log(f"Error getting ask price: {e}")
    return None

# ========================== ORDER MANAGER ==========================

def place_order(token_id: str, price: float, size: float) -> dict:
    """Place order on CLOB"""
    try:
        log(f"Placing order: {size} shares at ${price:.4f}")

        order_client = client

        if not hasattr(order_client, "builder_config"):
            order_client.builder_config = None

        api_key = os.getenv("API_KEY")
        api_secret = os.getenv("API_SECRET")
        api_passphrase = os.getenv("API_PASSPHRASE")
        if api_key and api_secret and api_passphrase:
            try:
                creds = ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase
                )
                order_client.set_api_creds(creds)
            except Exception as e:
                log(f"⚠ Error setting API creds in place_order: {e}")

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY,
        )

        signed_order = order_client.create_order(order_args)
        resp = order_client.post_order(signed_order, OrderType.GTC)

        status = resp.get("status", "UNKNOWN") if resp else "UNKNOWN"
        order_id = resp.get("orderID") if resp else None

        log(f"✓ Order placed: {status} (ID: {order_id})")
        return {
            'success': True,
            'status': status,
            'order_id': order_id,
            'error': None
        }

    except Exception as e:
        log(f"❌ Order error: {e}")
        import traceback
        log(traceback.format_exc())
        return {
            'success': False,
            'status': 'ERROR',
            'order_id': None,
            'error': str(e)
        }

# ========================== SETTLEMENT ==========================

def check_and_settle_trades():
    """Check and settle completed trades"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    now = datetime.now(tz=ZoneInfo('UTC'))
    c.execute('SELECT id, symbol, slug, token_id, side, entry_price, size, bet_usd FROM trades WHERE settled = 0 AND datetime(window_end) < datetime(?)', (now.isoformat(),))
    unsettled = c.fetchall()

    if not unsettled:
        log("ℹ No trades to settle")
        conn.close()
        return

    log(f"📊 Settling {len(unsettled)} trades...")
    total_pnl = 0
    settled_count = 0
    expired_count = 0

    for trade_id, symbol, slug, token_id, side, entry_price, size, bet_usd in unsettled:
        try:
            book = client.get_order_book(token_id)
            bids = getattr(book, "bids", []) or []
            asks = getattr(book, "asks", []) or []
            if bids and asks:
                final_price = (float(bids[-1].price if hasattr(bids[-1], 'price') else bids[-1].get('price', 0)) +
                              float(asks[-1].price if hasattr(asks[-1], 'price') else asks[-1].get('price', 0))) / 2.0
            else:
                final_price = 0.5

            side_u = (side or "").upper()
            if side_u in ("UP", "YES"):
                exit_value = final_price
            else:
                exit_value = 1.0 - final_price

            pnl_usd = (exit_value * size) - bet_usd
            roi_pct = (pnl_usd / bet_usd) * 100 if bet_usd > 0 else 0

            c.execute('UPDATE trades SET final_outcome=?, exit_price=?, pnl_usd=?, roi_pct=?, settled=1, settled_at=? WHERE id=?',
                     ('SETTLED', final_price, pnl_usd, roi_pct, now.isoformat(), trade_id))

            emoji = "✅" if pnl_usd > 0 else "❌"
            log(f"{emoji} Trade #{trade_id} [{symbol}] {side}: {pnl_usd:+.2f}$ ({roi_pct:+.1f}%)")
            total_pnl += pnl_usd
            settled_count += 1

        except Exception as e:
            error_str = str(e)
            # Check if market has expired (404 = no orderbook exists)
            if "404" in error_str or "No orderbook exists" in error_str:
                # Market expired - mark as settled with EXPIRED status
                # We can't determine PnL without the final price, so mark as unknown
                c.execute('UPDATE trades SET final_outcome=?, exit_price=?, pnl_usd=?, roi_pct=?, settled=1, settled_at=? WHERE id=?',
                         ('EXPIRED', None, None, None, now.isoformat(), trade_id))
                log(f"⏰ Trade #{trade_id} [{symbol}] {side}: Market expired, cannot determine outcome")
                expired_count += 1
            else:
                log(f"⚠️ Error settling trade #{trade_id}: {e}")

    conn.commit()
    conn.close()

    if settled_count > 0 or expired_count > 0:
        msg = f"📊 Settlement: {settled_count} settled"
        if settled_count > 0:
            msg += f" (PnL: ${total_pnl:+.2f})"
        if expired_count > 0:
            msg += f", {expired_count} expired (unknown outcome)"
        send_discord(msg)

# ========================== REPORTS ==========================

def generate_statistics():
    """Generate performance statistics report"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT COUNT(*), SUM(bet_usd), SUM(pnl_usd), AVG(roi_pct) FROM trades WHERE settled = 1')
    result = c.fetchone()
    total_trades = result[0] or 0

    if not total_trades:
        log("ℹ No settled trades for analysis")
        conn.close()
        return

    total_invested, total_pnl, avg_roi = result[1] or 0, result[2] or 0, result[3] or 0
    c.execute('SELECT COUNT(*) FROM trades WHERE settled = 1 AND pnl_usd > 0')
    winning_trades = c.fetchone()[0]
    win_rate = (winning_trades / total_trades) * 100

    report = []
    report.append("=" * 80)
    report.append("📊 POLYFAZI BOT - PERFORMANCE REPORT")
    report.append("=" * 80)
    report.append(f"Total trades:     {total_trades}")
    report.append(f"Win rate:         {win_rate:.1f}%")
    report.append(f"Total PnL:        ${total_pnl:.2f}")
    report.append(f"Total invested:   ${total_invested:.2f}")
    report.append(f"Average ROI:      {avg_roi:.2f}%")
    report.append(f"Total ROI:        {(total_pnl/total_invested)*100:.2f}%")
    report.append("=" * 80)

    report_text = "\n".join(report)
    log(report_text)

    report_file = f"{REPORTS_DIR}/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_file, 'w') as f:
        f.write(report_text)

    send_discord(f"📊 **PERFORMANCE REPORT**\n```\n{report_text}\n```")
    conn.close()

# ========================== CLAIM WINNINGS ==========================

# CTF Contract on Polygon
CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# Minimal ABI for redeemPositions
CTF_ABI = '''[
    {
        "constant": false,
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "id", "type": "uint256"}
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }
]'''

def claim_winnings_for_condition(condition_id: str) -> dict:
    """
    Claim winnings for a specific condition by calling redeemPositions on CTF contract.
    
    Args:
        condition_id: The condition ID of the resolved market
        
    Returns:
        dict with success status, tx_hash, and error if any
    """
    try:
        # Get account from private key
        account = Account.from_key(PROXY_PK)
        
        # Setup contract
        ctf_contract = w3.eth.contract(
            address=Web3.to_checksum_address(CTF_CONTRACT_ADDRESS),
            abi=json.loads(CTF_ABI)
        )
        
        # Parameters for redeemPositions
        # collateralToken: USDC on Polygon
        collateral_token = Web3.to_checksum_address(USDC_ADDRESS)
        # parentCollectionId: always bytes32(0) for Polymarket
        parent_collection_id = bytes(32)
        # conditionId: the condition we want to redeem
        condition_id_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        # indexSets: [1, 2] for binary markets (YES and NO)
        index_sets = [1, 2]
        
        # Build transaction
        nonce = w3.eth.get_transaction_count(account.address)
        
        # Estimate gas
        try:
            gas_estimate = ctf_contract.functions.redeemPositions(
                collateral_token,
                parent_collection_id,
                condition_id_bytes,
                index_sets
            ).estimate_gas({'from': account.address})
            gas_limit = int(gas_estimate * 1.2)  # 20% buffer
        except Exception as e:
            log(f"  Gas estimation failed: {e}")
            # Default gas limit if estimation fails
            gas_limit = 200000
        
        # Get gas price
        gas_price = w3.eth.gas_price
        
        # Build the transaction
        tx = ctf_contract.functions.redeemPositions(
            collateral_token,
            parent_collection_id,
            condition_id_bytes,
            index_sets
        ).build_transaction({
            'from': account.address,
            'nonce': nonce,
            'gas': gas_limit,
            'gasPrice': gas_price,
            'chainId': CHAIN_ID
        })
        
        # Sign and send
        signed_tx = w3.eth.account.sign_transaction(tx, PROXY_PK)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hash_hex = tx_hash.hex()
        
        log(f"  📤 Claim TX sent: {tx_hash_hex}")
        
        # Wait for receipt
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        
        if receipt['status'] == 1:
            log(f"  ✅ Claim successful! TX: {tx_hash_hex}")
            return {
                'success': True,
                'tx_hash': tx_hash_hex,
                'error': None
            }
        else:
            log(f"  ❌ Claim TX failed (reverted)")
            return {
                'success': False,
                'tx_hash': tx_hash_hex,
                'error': 'Transaction reverted'
            }
            
    except Exception as e:
        log(f"  ❌ Claim error: {e}")
        return {
            'success': False,
            'tx_hash': None,
            'error': str(e)
        }

def claim_all_unclaimed_trades():
    """
    Claim winnings for all unclaimed trades where window has ended.
    Should be called ~5 minutes after window closes.
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    now = datetime.now(tz=ZoneInfo('UTC'))
    
    # Find trades that:
    # 1. Are not claimed yet
    # 2. Window has ended (at least 5 minutes ago to ensure resolution)
    # 3. Have a condition_id
    five_min_ago = (now - timedelta(minutes=5)).isoformat()
    
    c.execute('''
        SELECT DISTINCT condition_id, symbol 
        FROM trades 
        WHERE claimed = 0 
        AND condition_id IS NOT NULL 
        AND datetime(window_end) < datetime(?)
    ''', (five_min_ago,))
    
    unclaimed = c.fetchall()
    
    if not unclaimed:
        log("ℹ No unclaimed trades to process")
        conn.close()
        return
    
    log(f"💰 Claiming winnings for {len(unclaimed)} conditions...")
    
    claimed_count = 0
    failed_count = 0
    
    for condition_id, symbol in unclaimed:
        log(f"[{symbol}] Claiming condition: {condition_id[:20]}...")
        
        result = claim_winnings_for_condition(condition_id)
        
        if result['success']:
            # Mark all trades with this condition_id as claimed
            c.execute('''
                UPDATE trades 
                SET claimed = 1, claimed_at = ?, claim_tx = ? 
                WHERE condition_id = ?
            ''', (now.isoformat(), result['tx_hash'], condition_id))
            claimed_count += 1
        else:
            # Check if error indicates "nothing to claim" (no balance)
            error_str = str(result.get('error', '')).lower()
            if 'revert' in error_str or 'execution reverted' in error_str:
                # Likely no tokens to claim - mark as claimed anyway to avoid retrying
                log(f"[{symbol}] No tokens to claim or already claimed")
                c.execute('''
                    UPDATE trades 
                    SET claimed = 1, claimed_at = ?, claim_tx = 'NO_BALANCE' 
                    WHERE condition_id = ?
                ''', (now.isoformat(), condition_id))
                claimed_count += 1
            else:
                failed_count += 1
        
        # Small delay between claims
        time.sleep(2)
    
    conn.commit()
    conn.close()
    
    if claimed_count > 0 or failed_count > 0:
        msg = f"💰 Claim results: {claimed_count} claimed, {failed_count} failed"
        log(msg)
        send_discord(msg)

# ========================== MAIN TRADING LOGIC ==========================

def execute_buy(symbol: str, token_id: str, side: str, trigger_price: float, 
                threshold: float, bet_amount: float, retry_number: int, condition_id: str = None) -> bool:
    """
    Execute a buy order for a token.
    Returns True if order was placed successfully.
    Retries up to 5 times if order fails.
    """
    # Get current ask price (what we would pay)
    ask_price = get_best_ask_price(token_id)
    if ask_price is None:
        log(f"[{symbol}] Cannot get ask price for {side}, skipping")
        return False
    
    # Ensure price is valid
    price = max(0.01, min(0.99, ask_price))
    
    # Calculate size
    size = round(bet_amount / price, 6)
    
    # Minimum size check
    MIN_SIZE = 5.0
    if size < MIN_SIZE:
        old_size = size
        size = MIN_SIZE
        bet_amount = round(size * price, 4)
        log(f"[{symbol}] Size {old_size:.4f} < min {MIN_SIZE}, bumping to {size:.4f}. Effective stake ≈ ${bet_amount:.2f}")
    
    # Log the trade
    retry_str = f" (retry #{retry_number})" if retry_number > 0 else ""
    log(f"[{symbol}] 📈 BUY {side} ${bet_amount:.2f}{retry_str} | Trigger: {trigger_price:.4f} >= {threshold:.4f} | Price: {price:.4f}")
    send_discord(f"**[{symbol}] BUY {side} ${bet_amount:.2f}**{retry_str} | Trigger: {trigger_price:.4f} | Price: {price:.4f}")
    
    # Place order with up to 5 retries
    MAX_ORDER_RETRIES = 5
    result = None
    
    for attempt in range(1, MAX_ORDER_RETRIES + 1):
        result = place_order(token_id, price, size)
        
        if result['success']:
            log(f"[{symbol}] Order status: {result['status']}")
            break
        else:
            log(f"[{symbol}] ❌ Order failed (attempt {attempt}/{MAX_ORDER_RETRIES}): {result['error']}")
            if attempt < MAX_ORDER_RETRIES:
                log(f"[{symbol}] Retrying in 2s...")
                time.sleep(2)
    
    # Only save to database if order was successful
    if result and result['success']:
        try:
            window_start, window_end = get_window_times(symbol)
            save_trade(
                symbol=symbol,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                slug=get_current_slug(symbol),
                token_id=token_id,
                condition_id=condition_id,
                side=side,
                trigger_price=trigger_price,
                threshold=threshold,
                entry_price=price,
                size=size,
                bet_usd=bet_amount,
                retry_number=retry_number,
                order_status=result['status'],
                order_id=result['order_id'],
            )
        except Exception as e:
            log(f"[{symbol}] Database error: {e}")
        return True
    else:
        log(f"[{symbol}] ❌ Order failed after {MAX_ORDER_RETRIES} attempts, not saving to database")
        send_discord(f"**[{symbol}] ❌ ORDER FAILED** {side} ${bet_amount:.2f} after {MAX_ORDER_RETRIES} attempts")
        return False

def check_and_trade_symbol(symbol: str, retry_number: int = 0, bet_multiplier: float = 1.0):
    """
    Check prices for a symbol and execute trades if thresholds are met.
    
    Args:
        symbol: Trading symbol (e.g., 'BTC')
        retry_number: Current retry number (0 = first check)
        bet_multiplier: Multiplier for bet amount (compounds with retries)
    """
    # Get token IDs and condition_id
    up_id, down_id, condition_id = get_token_ids(symbol)
    if not up_id or not down_id:
        log(f"[{symbol}] Market not found, skipping")
        return
    
    # Get current prices
    up_price, down_price = get_token_prices(up_id, down_id)
    
    retry_str = f" (retry #{retry_number})" if retry_number > 0 else ""
    up_price_str = f"{up_price:.4f}" if up_price is not None else "N/A"
    down_price_str = f"{down_price:.4f}" if down_price is not None else "N/A"
    log(f"[{symbol}] Prices{retry_str}: UP={up_price_str} | DOWN={down_price_str}")
    log(f"[{symbol}] Thresholds: UP>={UP_THRESHOLD:.4f} | DOWN>={DOWN_THRESHOLD:.4f}")
    
    # Calculate bet amount with multiplier
    current_bet = BET_USD * bet_multiplier
    
    # Check UP threshold
    if up_price is not None and up_price >= UP_THRESHOLD:
        log(f"[{symbol}] ✓ UP threshold met: {up_price:.4f} >= {UP_THRESHOLD:.4f}")
        execute_buy(symbol, up_id, "UP", up_price, UP_THRESHOLD, current_bet, retry_number, condition_id)
    else:
        log(f"[{symbol}] ✗ UP threshold NOT met: {up_price_str} < {UP_THRESHOLD:.4f}")
    
    # Check DOWN threshold
    if down_price is not None and down_price >= DOWN_THRESHOLD:
        log(f"[{symbol}] ✓ DOWN threshold met: {down_price:.4f} >= {DOWN_THRESHOLD:.4f}")
        execute_buy(symbol, down_id, "DOWN", down_price, DOWN_THRESHOLD, current_bet, retry_number, condition_id)
    else:
        log(f"[{symbol}] ✗ DOWN threshold NOT met: {down_price_str} < {DOWN_THRESHOLD:.4f}")

def run_check_for_symbol(symbol: str, retry_number: int, bet_multiplier: float):
    """Wrapper function to run check_and_trade_symbol in a thread"""
    try:
        check_and_trade_symbol(symbol, retry_number=retry_number, bet_multiplier=bet_multiplier)
    except Exception as e:
        log(f"[{symbol}] ❌ Thread error: {e}")

def schedule_claim_after_window():
    """
    Schedule claim to run 5 minutes after current window closes.
    Runs in a separate thread so it doesn't block main trading loop.
    """
    now = datetime.utcnow()
    
    # Calculate when current window ends
    seconds_into_window = (now.minute % 15) * 60 + now.second
    seconds_to_window_end = 900 - seconds_into_window
    
    # Wait until 5 minutes after window end
    total_wait = seconds_to_window_end + 300  # 300s = 5 minutes
    
    def delayed_claim():
        log(f"💰 Claim scheduled in {total_wait}s (5 min after window closes)")
        time.sleep(total_wait)
        log(f"\n{'='*90}")
        log(f"💰 CLAIM TIME | {datetime.now(tz=ZoneInfo('UTC')).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        log(f"{'='*90}\n")
        claim_all_unclaimed_trades()
    
    # Start claim in background thread
    claim_thread = threading.Thread(target=delayed_claim, daemon=True)
    claim_thread.start()

def process_window():
    """
    Process a single 15-minute window:
    1. Wait for CHECK_DELAY_SEC after window opens
    2. Check all markets IN PARALLEL
    3. If retries enabled, repeat checks every minute up to MAX_RETRIES times
    4. Schedule claim for 5 minutes after window closes
    """
    log(f"\n{'='*90}")
    log(f"🔄 NEW WINDOW | {datetime.now(tz=ZoneInfo('UTC')).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log(f"{'='*90}\n")
    
    # Schedule claim for 5 min after this window closes (runs in background)
    schedule_claim_after_window()
    
    # Initial check (retry_number = 0) - run all markets in parallel
    threads = []
    for sym in MARKETS:
        t = threading.Thread(target=run_check_for_symbol, args=(sym, 0, 1.0))
        t.start()
        threads.append(t)
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Handle retries if enabled
    if RETRIES_ENABLED and MAX_RETRIES > 0:
        log(f"\n📌 Retries enabled: {MAX_RETRIES} retries, multiplier={RETRY_MULTIPLIER}")
        
        current_multiplier = 1.0
        for retry in range(1, MAX_RETRIES + 1):
            # Wait 60 seconds between retries
            log(f"\n⏱️ Waiting 60s for retry #{retry}...")
            time.sleep(60)
            
            # Apply multiplier
            current_multiplier *= RETRY_MULTIPLIER
            
            log(f"\n{'='*90}")
            log(f"🔁 RETRY #{retry}/{MAX_RETRIES} | Bet multiplier: {current_multiplier:.2f}x")
            log(f"{'='*90}\n")
            
            # Run all markets in parallel
            threads = []
            for sym in MARKETS:
                t = threading.Thread(target=run_check_for_symbol, args=(sym, retry, current_multiplier))
                t.start()
                threads.append(t)
            
            # Wait for all threads to complete
            for t in threads:
                t.join()
    
    # Settle any completed trades
    check_and_settle_trades()

# ========================== MAIN ==========================

def main():
    """Main bot loop"""
    log("🚀 Starting PolyFazi Trading Bot...")
    log("📝 Logic: Buy tokens when price >= threshold")
    setup_api_creds()
    init_database()

    addr = Account.from_key(PROXY_PK).address
    log("=" * 90)
    log(f"🤖 POLYFAZI BOT | Markets: {', '.join(MARKETS)}")
    log(f"💼 Wallet: {addr[:10]}...{addr[-8:]} | Balance: {get_balance(addr):.2f} USDC")
    log(f"⚙️  Configuration:")
    log(f"   - CHECK_DELAY_SEC: {CHECK_DELAY_SEC}s after window opens")
    log(f"   - UP_THRESHOLD: {UP_THRESHOLD:.4f}")
    log(f"   - DOWN_THRESHOLD: {DOWN_THRESHOLD:.4f}")
    log(f"   - BET_USD: ${BET_USD}")
    log(f"   - RETRIES_ENABLED: {'YES' if RETRIES_ENABLED else 'NO'}")
    if RETRIES_ENABLED:
        log(f"   - MAX_RETRIES: {MAX_RETRIES}")
        log(f"   - RETRY_MULTIPLIER: {RETRY_MULTIPLIER}x")
    log("=" * 90)

    cycle = 0
    
    while True:
        try:
            now = datetime.utcnow()
            
            # Seconds into current 15-minute window
            seconds_into_window = (now.minute % 15) * 60 + now.second
            
            # Always wait for NEXT window start, then add CHECK_DELAY_SEC
            # Time until next window starts (0, 15, 30, 45 minute marks)
            seconds_to_next_window = 900 - seconds_into_window
            
            # If we're very close to window start (within 2 seconds), treat as new window
            if seconds_to_next_window > 898:
                seconds_to_next_window = 0
            
            total_wait = seconds_to_next_window + CHECK_DELAY_SEC
            
            if seconds_to_next_window > 0:
                next_window_min = ((now.minute // 15) + 1) * 15 % 60
                log(f"⏱️ Waiting {seconds_to_next_window}s for next window (:{next_window_min:02d}) + {CHECK_DELAY_SEC}s delay = {total_wait}s total")
            else:
                log(f"⏱️ Window just started, waiting {CHECK_DELAY_SEC}s delay...")
            
            time.sleep(total_wait)
            
            cycle += 1
            log(f"\n{'='*90}\n🔄 CYCLE #{cycle} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n{'='*90}\n")
            
            process_window()

            if cycle % 16 == 0:
                log("\n📊 Generating performance report...")
                generate_statistics()

        except KeyboardInterrupt:
            log("\n⛔ Bot stopped by user")
            log("📊 Generating final report...")
            generate_statistics()
            break

        except Exception as e:
            log(f"❌ Critical error: {e}")
            import traceback
            log(traceback.format_exc())
            send_discord(f"❌ Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
