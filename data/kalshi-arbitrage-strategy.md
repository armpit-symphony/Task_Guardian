# Kalshi Arbitrage Strategy

## The Strategy

**Cross-Exchange Arbitrage:**
- Find SAME event on Polymarket + Kalshi
- Buy YES on cheaper exchange
- Buy NO on expensive exchange
- Profit = $1 - (YES_price + NO_price)

**Example from bot:**
```
Polymarket: Kevin Warsh YES = 42¢
Kalshi: Kevin Warsh NO = 57¢
Total cost: 99¢
Payout: $1.00
Profit: 1¢
```

## Our Challenge

Polymarket is restricted for Phil. Options:

### Option 1: Wait for Production
- Get production API on Kalshi
- When Polymarket opens up, use cross-exchange arbitrage

### Option 2: Within-Kalshi Hedging
- Find correlated markets on Kalshi
- Buy YES on one, NO on related
- Less efficient but works

### Option 3: Signal Service
- Monitor markets
- Sell signals to others who trade
- Lower risk, recurring revenue

---

## For Now: Build Within-Kalshi

### Steps:
1. Get markets from Kalshi API
2. Use LLM to find related/correlated markets
3. Buy hedged positions
4. Track with our bot

### Implementation Plan:
```
1. Fetch all active markets
2. Use LLM to identify related pairs
3. Calculate hedge ratios
4. Execute paired trades
5. Monitor positions
```

---

## Bot Structure (for Kalshi)

```
kalshi-arbitrage/
├── bot.py           # Main trading loop
├── markets.py       # Fetch markets from API
├── analyzer.py      # LLM analysis for correlations
├── strategy.py      # Calculate positions
├── positions.py     # Track holdings
└── config.py       # API keys
```

---

## Ready to Build?

We have:
- ✅ Kalshi API working (demo)
- ✅ Python bot structure
- ❌ Need production account for real trading

Let me know if you want to build the within-Kalshi hedging system.
