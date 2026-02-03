# Hyperliquid Analytics Dashboard

Korean-language analytics dashboard for Hyperliquid DEX with automated data updates.

## 📊 Features

| Section | Korean | Description |
|---------|--------|-------------|
| Dashboard | 대시보드 | Overview: 24h volume, total OI, active markets |
| Markets & OI | 마켓 & OI | All markets with price, volume, OI, funding |
| Funding Rates | 펀딩비 | Current funding rates with annualized APY |
| Liquidation Risk | 청산 위험 | Whale positions near liquidation price |
| Whale Tracker | 고래 추적기 | Top 200 PnL traders + biggest positions |

## 🚀 Deployment

### GitHub Pages

1. Create a new GitHub repository
2. Upload all files:
   ```
   index.html
   fetch_data.py
   data.json
   .github/workflows/update.yml
   README.md
   ```
3. Go to Settings → Pages → Source: main branch
4. Enable GitHub Actions (Settings → Actions → General → Allow all actions)
5. Access at: `https://username.github.io/repo-name`

### Manual Update

```bash
pip install requests
python fetch_data.py
```

## ⏰ Auto Updates

GitHub Actions runs automatically:
- 07:00 KST (22:00 UTC)
- 13:00 KST (04:00 UTC)
- 19:00 KST (10:00 UTC)
- 01:00 KST (16:00 UTC)

Manual trigger: Actions → Update Hyperliquid Data → Run workflow

## 📡 API Sources

- **Market Data**: `https://api.hyperliquid.xyz/info` (metaAndAssetCtxs)
- **Leaderboard**: `https://stats-data.hyperliquid.xyz/Mainnet/leaderboard`
- **Positions**: `https://api.hyperliquid.xyz/info` (clearinghouseState)

## 📁 File Structure

```
├── index.html           # Dashboard UI (Korean)
├── fetch_data.py        # Python data fetcher
├── data.json            # Cached data (auto-updated)
├── README.md
└── .github/
    └── workflows/
        └── update.yml   # GitHub Actions workflow
```

## 🎨 Design

- Dark theme (#0a0a0f background)
- Cyan accents (#22d3ee)
- Korean UI with Noto Sans KR font
- JetBrains Mono for numbers
- Mobile responsive

## License

MIT
