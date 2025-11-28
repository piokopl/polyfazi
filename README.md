# PolyFazi Trading Bot

**Fully automated scalping bot for Polymarket 15-minute UP/DOWN crypto prediction markets**

PolyFazi monitors brand-new 15-minute Polymarket windows (BTC, ETH, SOL, XRP, etc.) and instantly buys YES (UP) or NO (DOWN) tokens whenever their price hits your predefined threshold — taking advantage of early mispricings right after a market opens.

### Key Features
- Auto-detects new 15-minute Polymarket markets the moment they appear
- Buys UP or DOWN tokens when price ≥ user-defined threshold (e.g., 0.90 → strong edge)
- Optional retry logic with configurable delay, max retries, and exponential bet sizing (martingale-style)
- Full trade history stored in SQLite + detailed logging
- Automatic settlement calculation (PnL, ROI)
- Automatic claiming of winnings via Polymarket’s CTF contract on Polygon
- Discord webhook notifications for entries, errors, claims, and reports
- Periodic performance reports (win rate, total PnL, ROI)
- Runs 24/7 with precise ET-based window timing

### Strategy in Simple Terms
1. Wait for a new 15-minute window to open (xx:00, xx:15, xx:30, xx:45 ET)
2. After `CHECK_DELAY_SEC` seconds, fetch UP and DOWN token prices
3. If price ≥ threshold → buy instantly for `BET_USD`
4. If retries enabled → re-check every minute (up to `MAX_RETRIES` times), multiplying stake by `RETRY_MULTIPLIER` each time
5. ~5 minutes after window closes → automatically claim any winnings (currently not working)
6. Every 4 hours → generate and send performance report
