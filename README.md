# BetPawa Virtual Football Analyst Bot

Telegram bot that scrapes betPawa live, analyses team strength, and predicts
upcoming virtual football fixtures.

---

## Deploy to Railway (recommended)

### 1 — Get your bot token
1. Open Telegram → search **@BotFather**
2. Send `/newbot` and follow the prompts
3. Copy the token (looks like `123456789:ABCdef…`)

### 2 — Push to GitHub
```bash
git init
git add .
git commit -m "init"
git remote add origin https://github.com/YOUR_USER/vsbot.git
git push -u origin main
```

### 3 — Deploy on Railway
1. Go to [railway.app](https://railway.app) → **New Project**
2. Choose **Deploy from GitHub repo** → select your repo
3. Click **Variables** → **Add Variable**:
   - Name:  `BOT_TOKEN`
   - Value: `123456789:ABCdef…`  (your token from step 1)
4. Railway auto-detects `Procfile` and deploys.  Done ✅

The bot runs as a **worker** (no web server needed) — Railway keeps it alive 24/7.

---

## Local run (for testing)

```bash
pip install -r requirements.txt
export BOT_TOKEN="123456789:ABCdef…"
python bot.py
```

---

## Commands

| Command | Description |
|---------|-------------|
| `/results` | Load last 3 matchdays — pick a league |
| `/nextroundresults` | Next round fixtures + pre-set scores — pick a league |
| `/predict` | Model predictions for next matchday |
| `/compare` | betPawa's fixtures vs our model side-by-side |
| `/teams` | Full team strength table |
| `/team Arsenal` | Deep report: record, form, strengths, weaknesses |
| `/live` | Currently live round scores |
| `/add Arsenal 2-1 Chelsea` | Add a result manually |
| `/clear` | Wipe all stored data |

---

## API Notes

All endpoints confirmed from betPawa's own source JS:

```
GET /api/sportsbook/virtual/v1/seasons/list/past?leagueId=7794
    → past round IDs and names

GET /api/sportsbook/virtual/v1/seasons/list/actual?leagueId=7794
    → current/upcoming round IDs

GET /api/sportsbook/virtual/v2/events/list/by-round/{roundId}?page=matchups
    → fixture pairings + HomeScore/AwayScore once betPawa seeds them

GET /api/sportsbook/virtual/v2/events/list/by-round/{roundId}?page=live
    → live in-progress scores
```

**Score fields in response** (priority order from source):
1. `event.HomeScore` / `event.AwayScore`
2. `event.results.scoreboard.scoreHome` / `scoreAway`
3. `event.results.display.scoreHome` / `scoreAway`

**League IDs:** 7794=England · 7795=Spain · 7796=Italy · 9184=Germany
               9183=France · 13774=Netherlands · 13773=Portugal

> ⚠️ betPawa's API may require an authenticated session cookie for some
> endpoints. If data doesn't load, use `/add Home 2-1 Away` to enter
> results manually — all analysis and prediction features work identically.

---

## Score Seeding — Confirmed from Source

Virtual match results are algorithmically pre-determined **before** each
round starts. The `/nextroundresults` command fetches these via
`?page=matchups`. When scores are already seeded the output shows:

```
🎯 Next Round — Pre-Set Results
🏴󠁧󠁢󠁥󠁮󠁧󠁿 England Virtual League · Matchday 47

🟢 Arsenal  2 – 0  Wolves
🟢 Liverpool  3 – 1  Newcastle
🔴 Chelsea  0 – 2  Man City
🟡 Tottenham  1 – 1  Aston Villa
...

🏆 Matchday Stats
  Total goals: 19
  Biggest win: Liverpool 3–1 Newcastle
  Clean sheets: Arsenal, Man City, Crystal Palace
```

When scores are not yet seeded it shows fixtures only:

```
🔍 Next Round Fixtures
⚽ Arsenal  vs  Wolves
⚽ Liverpool  vs  Newcastle
...
Fixtures locked in. Scores seeded before the round starts.
```
