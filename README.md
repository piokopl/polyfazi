# PolyFazi Trading Bot

**Fully automated scalping bot for Polymarket 15-minute UP/DOWN crypto prediction markets**

PolyFazi monitors brand-new 15-minute Polymarket windows (BTC, ETH, SOL, XRP, etc.) and instantly buys YES (UP) or NO (DOWN) tokens whenever their price hits your predefined threshold — taking advantage of early mispricings right after a market opens.

### Key Features
- Auto-detects new 15-minute Polymarket markets the moment they appear
- Buys UP or DOWN tokens when price ≥ user-defined threshold (e.g., 0.90 → strong edge)
- Optional retry logic with configurable delay, max retries, and exponential bet sizing (martingale-style)
- Full trade history stored in SQLite + detailed logging
- Automatic settlement calculation (PnL, ROI)
- Automatic claiming of winnings via Polymarket’s CTF contract on Polygon (not yeat working)
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

### Note
This script was created for hobby and academic purposes. The chances of profiting from this strategy are very low, as cryptocurrency prices fluctuate significantly within such a narrow timeframe. The problem is that theoretically, each trade should yield around 10% profit, while in the case of price fluctuations, the loss is 100%. Therefore, there is a requirement of 10 correct trades to compensate for the losses of one bad one, which is difficult to achieve. A solution would be to calculate the deviation of the potential entry price from the starting price and only enter a trade when it is large enough that a potential drop below the entry price at the end of the trade would not result in a loss, or to apply a stop loss at the starting price of the trade to limit losses. If you are interested, I invite you to develop this script and share your feedback.
