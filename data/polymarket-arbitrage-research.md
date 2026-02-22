# Polymarket Arbitrage Research

## What People Are Doing

### The Method: Sum-to-One Arbitrage
- Polymarket: YES + NO must = $1.00 at settlement
- Buy both YES and NO → guaranteed to have $1 at resolution
- Profit when prices are mispriced

### Real Results (from articles)
| Person | Result | Timeframe |
|--------|--------|-----------|
| Clawdbot user | $100 → $347 | 24 hours |
| Industry total | $40 million extracted | 2024-2025 |

### The Strategy: Split + CLOB
```
1. Split: Convert USDC.e → YES + NO tokens on both sides
2. CLOB: Sell unwanted side on order book to reduce net cost
3. Result: Hedged position, lower cost basis
```

### LLM-Powered Hedge Discovery
- Scan trending markets
- Use LLM to find logically related market pairs
- Coverage tiers: T1 (95%+), T2 (90-95%), T3 (85-90%)

---

## Existing Tools

### PolyClaw (OpenClaw Skill)
**GitHub:** chainstacklabs/polyclaw

**Features:**
- Browse markets (trending, search)
- Execute trades on Polymarket
- Position tracking (local JSON)
- Hedge discovery with LLM

**Commands:**
```
polyclaw markets trending      # Top markets
polyclaw hedge scan           # Find hedging opportunities  
polyclaw buy YES 50          # Buy $50 YES
polyclaw positions            # Show positions
```

**Requirements:**
- Chainstack node (Polygon RPC)
- OpenRouter API key (LLM)
- Private key (for signing)
- USDC.e on Polygon

---

## How to Build This

### 1. Get Access
- Polymarket.com account
- Polygon wallet with USDC.e
- Chainstack (free tier works)
- OpenRouter API key

### 2. Install PolyClaw
```bash
clawhub install polyclaw
cd ~/.openclaw/skills/polyclaw
uv sync
```

### 3. Configure
```json
"polyclaw": {
  "enabled": true,
  "env": {
    "CHAINSTACK_NODE": "https://polygon-mainnet.core.chainstack.com/YOUR_KEY",
    "POLYCLAW_PRIVATE_KEY": "0x...",
    "OPENROUTER_API_KEY": "sk-or-v1-..."
  }
}
```

### 4. Run
```
polyclaw markets trending
polyclaw hedge scan --limit 10
polyclaw buy YES 50 <market_id>
```

---

## Key Insight

The arbitrage window is **narrowing**:
- 2024: 30 second execution window
- 2026: 800 milliseconds

But:
- $15.7 billion cumulative volume
- $12 billion in January 2026 alone
- Still ~$40M extracted by automated traders

---

## For Us

Since we have **Kalshi** (regulated, demo working):
1. Start with Kalshi for practice
2. Graduate to Polymarket when ready
3. Use similar strategy: find related markets, hedge

The LLM can help find the arbitrage opportunities by analyzing market relationships.
