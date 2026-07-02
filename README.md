# 13F Tracker

追踪机构 13F 持仓的自动化 dashboard。

- `index.html` — 前端（GitHub Pages 托管，读 `data/data.json`）
- `edgar_13f.py` — SEC EDGAR 抓取脚本（全文搜索 CUSIP → 解析 infotable → 季度 diff）
- `.github/workflows/fetch-13f.yml` — 每天 UTC 22:00 自动抓取并 commit 数据

## 一次性设置

1. **Settings → Secrets and variables → Actions → New repository secret**
   - Name: `EDGAR_CONTACT_EMAIL`，Value: 你的邮箱（SEC fair-access 要求）
2. **Settings → Pages** → Source 选 "Deploy from a branch" → `main` / `(root)` → Save
3. **Actions 标签页** → 选 "Fetch 13F data" → "Run workflow" 手动跑第一次

跑完后访问 `https://<用户名>.github.io/<仓库名>/`。

## 加股票

编辑 `edgar_13f.py` 里的 `TICKERS`（ticker + 9 位 CUSIP，任何一份 13F 申报里可查），
`WATCHLIST_FUNDS` 里维护触发信号卡片的机构 CIK。
