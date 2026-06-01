"""
BetPawa Virtual Football Bot — Clean API Edition

All endpoints confirmed from betPawa JS bundle (_app-4204efaaaf359a13.js):

  Base v1: /api/sportsbook/virtual/v1
  Base v2: /api/sportsbook/virtual/v2

  GET /api/sportsbook/virtual/v1/seasons/list/actual
      → { items: [ { id, name, matchSeconds, realWorldMatchSeconds,
                     rounds: [ { id, name, tradingTime:{start,end} } ] } ] }

  GET /api/sportsbook/virtual/v1/seasons/list/past
      → same shape as actual

  GET /api/sportsbook/virtual/v2/events/list/by-round/{roundId}?page=upcoming
      → { items: [ { id, homeTeam:{id,name}, awayTeam:{id,name},
                     homeScore, awayScore, homeHTScore, awayHTScore,
                     startTime, markets: [ { marketType:{id,name,priority},
                     row:[{prices:[{name,price}]}] } ] } ] }

  GET /api/sportsbook/virtual/v2/events/list/by-round/{roundId}?page=live
      → same shape, scores updating in real-time

  GET /api/sportsbook/virtual/v2/events/list/by-round/{roundId}?page=matchups
      → same shape, past results with final scores

  GET /api/sportsbook/virtual/v1/standings/by-season/{seasonId}
      → { id, name, competitionStandings: [
            { id (leagueId), participantStandings: [
                { position, id, name,
                  points: {won, draw, lost, total},
                  scoreStanding: {scored, conceded},
                  form: ["W","D","L",...] }
            ] }
          ] }

Market type IDs (confirmed):
  1X2   → 3743
  O/U   → 5000
  BTTS  → 3795
  DC    → 4693
  HT/FT → 4706

League IDs:
  7794 England, 7795 Spain, 7796 Italy, 9183 France,
  9184 Netherlands, 13773 Germany, 13774 Portugal
"""

import asyncio, os, re, logging, time, datetime, json, io, base64, hashlib, random
import httpx
from aiohttp import web as _aiohttp_web
try:
    import firebase_admin
    from firebase_admin import credentials as _fb_credentials, firestore as _fb_firestore
    _FIREBASE_AVAILABLE = True
except ImportError:
    _FIREBASE_AVAILABLE = False
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, PicklePersistence,
)

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("vsbot")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set!")

BETPAWA_COOKIE = os.environ.get("BETPAWA_COOKIE", "")
CHANNEL_ID     = os.environ.get("CHANNEL_ID", "")
APP_API_KEY    = os.environ.get("APP_API_KEY", "")        # Secret key app sends in x-app-key header
APP_HTTP_PORT  = int(os.environ.get("APP_HTTP_PORT", "8080"))
FIREBASE_KEY_PATH = os.environ.get("FIREBASE_KEY_PATH", "")
FIREBASE_KEY_JSON = os.environ.get("FIREBASE_KEY_JSON", "")
_HARDCODED_ADMIN_ID = 0   # ← set your Telegram numeric ID here
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0")) or _HARDCODED_ADMIN_ID

# ─── FIREBASE INIT ────────────────────────────────────────────────────────────
_fb_db = None
if _FIREBASE_AVAILABLE:
    try:
        _key = None
        if FIREBASE_KEY_JSON:
            _key = _fb_credentials.Certificate(json.loads(FIREBASE_KEY_JSON))
        elif FIREBASE_KEY_PATH and os.path.exists(FIREBASE_KEY_PATH):
            _key = _fb_credentials.Certificate(FIREBASE_KEY_PATH)
        if _key:
            firebase_admin.initialize_app(_key)
            _fb_db = _fb_firestore.client()
            log.info("✅ Firebase connected — subscriptions backed up to Firestore")
        else:
            log.info("ℹ️  Firebase not configured — using pickle only")
    except Exception as _fbe:
        log.warning(f"⚠️ Firebase init failed: {_fbe} — using pickle only")

# ─── ACCESS CONTROL ───────────────────────────────────────────────────────────
def _access(bot_data):
    if "access" not in bot_data:
        bot_data["access"] = {"allowed_channels": set(), "users": {}, "pending_user": {}}
    d = bot_data["access"]
    for k, v in [("allowed_channels", set()), ("users", {}), ("pending_user", {})]:
        if k not in d: d[k] = v
    d["allowed_channels"] = set(str(x) for x in d["allowed_channels"])
    return d

def _is_admin(uid): return ADMIN_ID != 0 and uid == ADMIN_ID

def _is_authorized(uid, bot_data):
    acc = _access(bot_data)
    uid = str(uid)
    if uid not in acc["users"]: return False
    return acc["users"][uid]["expire_ts"] > time.time()

def _remaining_days(uid, bot_data):
    acc = _access(bot_data)
    e = acc["users"].get(str(uid))
    if not e: return None
    return max(0.0, (e["expire_ts"] - time.time()) / 86400)

def _can_access(uid, bot_data):
    return _is_admin(uid) or _is_authorized(uid, bot_data)

# ─── LEAGUES ──────────────────────────────────────────────────────────────────
LEAGUES = {
    7794:  {"name": "England",     "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "iso": "england"},
    7795:  {"name": "Spain",       "flag": "🇪🇸", "iso": "spain"},
    7796:  {"name": "Italy",       "flag": "🇮🇹", "iso": "italy"},
    9183:  {"name": "France",      "flag": "🇫🇷", "iso": "france"},
    # JS bundle: {7794:"england",7795:"spain",7796:"italy",9184:"germany",9183:"france",13774:"netherlands",13773:"portugal"}
    9184:  {"name": "Germany",     "flag": "🇩🇪", "iso": "germany"},
    13773: {"name": "Portugal",    "flag": "🇵🇹", "iso": "portugal"},
    13774: {"name": "Netherlands", "flag": "🇳🇱", "iso": "netherlands"},
}

# ISO code → league_id (from JS: {7794:"england",7795:"spain",7796:"italy",9184:"germany",9183:"france",13774:"netherlands",13773:"portugal"})
ISO_TO_LID = {v["iso"]: k for k, v in LEAGUES.items()}

# Known team name fragments per league — used as fallback filter when
# competition.id is absent from the API event payload.
# Team codes exactly as returned by the BetPawa API (3-letter abbreviations).
# Confirmed from live site screenshots — the API uses these short codes, NOT full names.
LEAGUE_TEAMS: dict[int, set[str]] = {
    7794: {  # England — confirmed from screenshots: ARS WHU AST SUN BHA WOL BRE MUN BUR NEW EVE FUL LEE BOU MCI CRY NOT LIV TOT CHE
        "ARS","WHU","AST","SUN","BHA","WOL","BRE","MUN",
        "BUR","NEW","EVE","FUL","LEE","BOU","MCI","CRY",
        "NOT","LIV","TOT","CHE",
    },
    7795: {  # Spain
        "BAR","RMA","ATM","SEV","VAL","VIL","ATH","RSO",
        "BET","CEL","GET","ESP","GIR","OSA","RAY","MLL",
        "ALA","GRA","VLL","LPA",
    },
    7796: {  # Italy
        "JUV","INT","MIL","ROM","LAZ","ATL","NAP","FIO",
        "ATA","TOR","GEN","BOL","UDI","SAS","VER","SAL",
        "LEC","CAG","MON","EMP",
    },
    9183: {  # France
        "PSG","MAR","LYO","MON","LIL","REN","NIC","STR",
        "LEN","MNP","NAN","REI","BRE","AUX","ANG","TOU",
        "STE","HAV",
    },
    9184: {  # Germany
        "BAY","DOR","LEV","EIN","LEI","WOB","GLB","UNB",
        "AUG","BOC","FRE","HOF","MAI","SCF","HER","KOL",
        "STP","HOL","STU","BRE",
    },
    13773: {  # Portugal
        "BEN","POR","SPO","BRA","VIT","GUI","PAC","BOA",
        "RIO","FAR","NAC","ESB","ARC","EBE","MON","VIZ",
    },
    13774: {  # Netherlands
        "PSV","AJA","FEY","AZA","TWE","UTR","GRO","HEE",
        "SPA","WIL","VVV","HER","NEC","RKC","ZWO","EXC",
        "GOA","ALM","NAC","PEC",
    },
}

def ld(lid): return f"{LEAGUES[lid]['flag']} {LEAGUES[lid]['name']}"

# ─── API ──────────────────────────────────────────────────────────────────────
BASE = "https://www.betpawa.ug"
EP_ACTUAL   = "/api/sportsbook/virtual/v1/seasons/list/actual"
EP_PAST     = "/api/sportsbook/virtual/v1/seasons/list/past"
EP_EVENTS   = "/api/sportsbook/virtual/v2/events/list/by-round/{rid}"
EP_STANDINGS= "/api/sportsbook/virtual/v1/standings/by-season/{sid}"

PAGE_LIVE     = "live"
PAGE_UPCOMING = "upcoming"
PAGE_MATCHUPS = "matchups"

# Market type IDs confirmed from JS bundle
MARKET_IDS = {"1X2": "3743", "O/U": "5000", "BTTS": "3795", "DC": "4693", "HT/FT": "4706"}
MARKET_ORDER = ["1X2", "O/U", "BTTS", "DC", "HT/FT"]

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-UG,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.betpawa.ug/virtual-sports",
    "Devicetype":      "desktop",
    "X-Pawa-Brand":    "betpawa-uganda",
    "X-Pawa-Language": "en",
}
if BETPAWA_COOKIE:
    HEADERS["Cookie"] = BETPAWA_COOKIE

# ─── HTTP ─────────────────────────────────────────────────────────────────────
async def _get(client: httpx.AsyncClient, url: str, params: dict = None):
    try:
        r = await client.get(url, headers=HEADERS, params=params, timeout=15, follow_redirects=True)
        log.info(f"GET {r.url} → {r.status_code}")
        if r.status_code == 200:
            try:
                d = r.json()
                return d if d else None
            except Exception as e:
                log.error(f"JSON parse error: {e} — {r.text[:200]}")
        else:
            log.warning(f"HTTP {r.status_code} ← {url}")
            log.warning(f"  Body: {r.text[:300]}")
    except Exception as e:
        log.error(f"Request failed {url}: {e}")
    return None

# ─── FORMAT HELPERS ───────────────────────────────────────────────────────────
SEP = "━" * 28

def _chunks(text: str, limit=4000):
    if len(text) <= limit: return [text]
    parts, buf = [], []
    for line in text.split("\n"):
        if sum(len(l)+1 for l in buf)+len(line) > limit:
            parts.append("\n".join(buf)); buf = []
        buf.append(line)
    if buf: parts.append("\n".join(buf))
    return parts

def _form_icons(form):
    m = {"W": "🟢", "D": "🟡", "L": "🔴"}
    return " ".join(m.get(x, "⚪") for x in form) or "—"

def _iso_to_ms(t):
    if not t: return 0
    if isinstance(t, (int, float)):
        t = int(t)
        return t * 1000 if t < 10_000_000_000 else t
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})', str(t))
    if m:
        import calendar
        dt = datetime.datetime(*map(int, m.groups()), tzinfo=datetime.timezone.utc)
        return int(calendar.timegm(dt.timetuple())) * 1000
    return 0

def _start_ms(r):
    tt = r.get("tradingTime")
    if isinstance(tt, dict):
        v = _iso_to_ms(tt.get("start"))
        if v: return v
    return _iso_to_ms(r.get("startTime") or r.get("bettingClosesTime") or 0)

# ─── SEASONS / ROUNDS ─────────────────────────────────────────────────────────
async def fetch_seasons(client, past=False) -> list[dict]:
    """Returns list of season dicts: {id, name, rounds:[{id,name,tradingTime}]}"""
    ep = EP_PAST if past else EP_ACTUAL
    data = await _get(client, BASE + ep)
    if not data: return []
    items = data.get("items", []) if isinstance(data, dict) else data
    seasons = []
    for item in items:
        if not isinstance(item, dict): continue
        sid = str(item.get("id", "")).strip()
        if not sid: continue
        seasons.append({
            "id":     sid,
            "name":   item.get("name", f"#{sid}"),
            "matchSeconds": item.get("matchSeconds", 0),
            "realWorldMatchSeconds": item.get("realWorldMatchSeconds", 0),
            "rounds": item.get("rounds", []),
        })
    seasons.sort(key=lambda s: int(s["id"]) if s["id"].isdigit() else 0, reverse=True)
    return seasons

# ── seasons cache: refreshed at most once per minute ──────────────────────────
_seasons_cache:     list[dict] = []
_seasons_cache_ts:  float      = 0.0
_seasons_cache_ttl: float      = 60.0   # seconds
_seasons_lock = asyncio.Lock()

async def fetch_all_seasons(client) -> list[dict]:
    """
    Fetch and merge actual + past seasons, newest first.
    Result is cached for 60 s so rapid UI navigation never redundantly
    hits the seasons API (the log showed it being called on every click).
    """
    global _seasons_cache, _seasons_cache_ts
    now = time.time()
    if _seasons_cache and now - _seasons_cache_ts < _seasons_cache_ttl:
        return _seasons_cache

    async with _seasons_lock:
        # Double-check after acquiring lock
        now = time.time()
        if _seasons_cache and now - _seasons_cache_ts < _seasons_cache_ttl:
            return _seasons_cache

        actual = await fetch_seasons(client, past=False)
        past   = await fetch_seasons(client, past=True)
        seen   = {s["id"] for s in actual}
        for s in past:
            if s["id"] not in seen:
                actual.append(s)
                seen.add(s["id"])
        actual.sort(key=lambda s: int(s["id"]) if s["id"].isdigit() else 0, reverse=True)
        log.info(f"fetch_all_seasons: {len(actual)} seasons cached for {_seasons_cache_ttl:.0f}s")
        _seasons_cache    = actual
        _seasons_cache_ts = time.time()

    return _seasons_cache

# ─── EVENTS ───────────────────────────────────────────────────────────────────
# Per-process cache so concurrent H2H lookups never duplicate the same HTTP call.
_round_cache: dict[tuple, list] = {}
_round_locks: dict[tuple, "asyncio.Lock"] = {}

async def fetch_events(client, round_id, page: str) -> list[dict]:
    key = (str(round_id), page)
    if key in _round_cache:
        return _round_cache[key]
    if key not in _round_locks:
        _round_locks[key] = asyncio.Lock()
    async with _round_locks[key]:
        if key in _round_cache:
            return _round_cache[key]
        url  = BASE + EP_EVENTS.format(rid=round_id)
        data = await _get(client, url, params={"page": page})
        items = []
        if data:
            raw = data.get("items", []) if isinstance(data, dict) else data
            items = raw if isinstance(raw, list) else []
        log.info(f"fetch_events round={round_id} page={page}: {len(items)} items")
        # Diagnostic: log ALL keys of the first event on matchups so we can see
        # every score-related field the API actually returns.
        if items and page == PAGE_MATCHUPS:
            ev0 = items[0]
            # Log all top-level keys and their values (truncated for readability)
            all_keys = {k: (str(ev0[k])[:120] if ev0[k] is not None else None) for k in ev0}
            log.info(f"fetch_events matchup ev0 ALL keys: {all_keys}")
            score_keys = {k: ev0[k] for k in ev0 if any(x in k.lower() for x in ("score","result","home","away","ht","ft","period","half")) and k not in ("homeTeam","awayTeam","homeId","awayId")}
            log.info(f"fetch_events matchup ev0 SCORE keys: {score_keys}")
        # Only cache matchups (completed history) — upcoming odds change every round.
        # For matchups: only cache if at least one event has a valid score OR a
        # populated results block (historical rounds store scores in results, not score field).
        if page == PAGE_MATCHUPS:
            def _has_data(ev):
                if _get_score(ev)[0] is not None:
                    return True
                results = ev.get("results")
                if isinstance(results, dict):
                    ppr = results.get("participantPeriodResults") or []
                    if len(ppr) >= 2:
                        return True
                return False
            has_scores = any(_has_data(ev) for ev in items) if items else False
            if has_scores:
                _round_cache[key] = items
            else:
                log.info(f"fetch_events: round={round_id} matchups has no scores yet — NOT caching, will retry.")
    return items

def _filter_by_league(events: list[dict], league_id: int) -> list[dict]:
    """
    Filter events to only those belonging to the given league.
    The events endpoint returns ALL leagues in one round (66 items = 6 leagues × 11 matches).

    Priority:
      1. competition.id / category.id exact match  (most reliable when present)
      2. Exact team-code match via LEAGUE_TEAMS     (fallback — codes are 3-letter like ARS, WHU)
    """
    # Method 1: competition/category id exact match
    out = []
    for ev in events:
        comp = ev.get("competition") or ev.get("category") or {}
        if isinstance(comp, dict):
            try:
                if int(comp.get("id", -1)) == league_id:
                    out.append(ev)
            except (TypeError, ValueError):
                pass
    if out:
        log.info(f"_filter_by_league lid={league_id}: competition.id match → {len(out)} events")
        return out

    # Method 2: exact team-code match (API returns 3-letter codes like ARS, WHU, TOT…)
    known = {t.upper() for t in LEAGUE_TEAMS.get(league_id, set())}
    if known:
        for ev in events:
            h, a = _get_teams(ev)
            if h.strip().upper() in known or a.strip().upper() in known:
                out.append(ev)
        if out:
            log.info(f"_filter_by_league lid={league_id}: team-code match → {len(out)} events")
            return out

    # Log a sample of actual team names to help diagnose missing codes
    sample = [(h, a) for ev in events[:6] for h, a in [_get_teams(ev)]]
    log.warning(f"_filter_by_league lid={league_id}: NO match in {len(events)} events. Sample teams: {sample}")
    return []

def _get_teams(ev: dict) -> tuple[str, str]:
    """Extract home/away team names from an event."""
    home = ev.get("homeTeam") or {}
    away = ev.get("awayTeam") or {}
    if isinstance(home, dict) and home.get("name"):
        return str(home["name"]), str(away.get("name", "?"))
    # participants list fallback
    parts = ev.get("participants") or []
    if len(parts) >= 2:
        h = next((p for p in parts if str(p.get("type","")).upper()=="HOME"), parts[0])
        a = next((p for p in parts if str(p.get("type","")).upper()=="AWAY"), parts[1])
        return str(h.get("name","?")), str(a.get("name","?"))
    return "?", "?"

def _get_score(ev: dict) -> tuple:
    """
    Returns (home_score_FT, away_score_FT, ht_home, ht_away).
    Always returns FULL-TIME scores as first two elements.

    BetPawa matchups API encodes scores in a single 'score' field as a
    decimal string: "H.A" where H = home goals, A = away goals (integer).
    e.g. "2.1" = 2-1, "9.0" = 9-0, "0.0" = 0-0, "10.3" = 10-3.
    The decimal part is the LITERAL away score, NOT a float fraction.

    results is None on the matchups page — all score data is in 'score' only.
    Other fields (HomeScore, homeScore etc.) are used as fallbacks for live pages.
    """
    def _to_int(v):
        if v is None: return None
        s = str(v).strip()
        if s == "": return None
        try: return int(float(s))
        except: return None

    # ── Priority 1: 'score' field — "H.A" decimal encoding (live page only) ──
    # The 'score' field is ONLY reliable for live/upcoming events. For historical
    # matchup rounds the API always returns score=0.0 even after the match finishes —
    # the real FT scores live in results.participantPeriodResults (Priority 3).
    # So: only use the score field when results is absent AND the match has started.
    score_raw = ev.get("score")
    if score_raw is not None and ev.get("results") is None:
        s = str(score_raw).strip()
        if s and "." in s:
            # Only trust if match has already started
            start_ms = 0
            st = ev.get("startTime")
            if st:
                try:
                    import re as _re
                    m = _re.search(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})", str(st))
                    if m:
                        import datetime as _dt, calendar as _cal
                        dt = _dt.datetime(*map(int, m.groups()), tzinfo=_dt.timezone.utc)
                        start_ms = int(_cal.timegm(dt.timetuple())) * 1000
                except Exception:
                    pass
            import time as _time
            now_ms = int(_time.time() * 1000)
            if start_ms == 0 or start_ms < now_ms:
                try:
                    parts = s.split(".")
                    hs  = int(parts[0])
                    as_ = int(parts[1])  # decimal part IS the away score literally
                    hht = _to_int(ev.get("HomeHTScore") or ev.get("homeHTScore"))
                    aht = _to_int(ev.get("AwayHTScore") or ev.get("awayHTScore"))
                    return hs, as_, hht, aht
                except (IndexError, ValueError):
                    pass

    # ── Priority 2: explicit HomeScore/AwayScore fields (live/other pages) ──
    def _pick_score(*keys):
        for k in keys:
            raw = ev.get(k)
            if raw is None: continue
            if isinstance(raw, dict):
                v = _to_int(raw.get("ft") or raw.get("fullTime") or raw.get("full"))
                if v is not None: return v
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                return int(raw)
            v = _to_int(raw)
            if v is not None: return v
        return None

    hs  = _pick_score("HomeScore",   "homeScore")
    as_ = _pick_score("AwayScore",   "awayScore")
    hht = _pick_score("HomeHTScore", "homeHTScore")
    aht = _pick_score("AwayHTScore", "awayHTScore")

    if hs is not None and as_ is None and (hht is not None or aht is not None):
        as_ = 0
    if as_ is not None and hs is None and (hht is not None or aht is not None):
        hs = 0

    if hs is not None and as_ is not None:
        return hs, as_, hht, aht

    # ── Priority 3: results.scoreboard ──
    results = ev.get("results")
    if isinstance(results, dict):
        sb = results.get("scoreboard")
        if isinstance(sb, dict):
            hs2  = _to_int(sb.get("scoreHome"))
            as2_ = _to_int(sb.get("scoreAway"))
            hht2 = _to_int(sb.get("scoreHomeHT"))
            aht2 = _to_int(sb.get("scoreAwayHT"))
            if hs2 is not None and as2_ is not None:
                return hs2, as2_, hht2, aht2

        ppr = results.get("participantPeriodResults") or []
        if len(ppr) >= 2:
            try:
                def _ppr_score(entry, ht=False):
                    slug = "FIRST_HALF" if ht else "FULL_TIME_EXCLUDING_OVERTIME"
                    for pr in entry.get("periodResults") or []:
                        if pr.get("period", {}).get("slug") == slug and pr.get("type") == "SCORE":
                            return _to_int(pr.get("result"))
                    return None
                home_r = next((p for p in ppr if str(p.get("participant",{}).get("type","")).upper()=="HOME"), ppr[0])
                away_r = next((p for p in ppr if str(p.get("participant",{}).get("type","")).upper()=="AWAY"), ppr[1])
                hs3  = _ppr_score(home_r)
                as3_ = _ppr_score(away_r)
                hht3 = _ppr_score(home_r, ht=True)
                aht3 = _ppr_score(away_r, ht=True)
                if hs3 is not None and as3_ is not None:
                    return hs3, as3_, hht3, aht3
            except (StopIteration, IndexError, AttributeError):
                pass

    return None, None, None, None

def _extract_markets(ev: dict) -> dict:
    """
    Extract all 5 market types from an event.
    Returns dict: { "1X2": {...}, "O/U": [...], "BTTS": {...}, "DC": {...}, "HT/FT": {...} }
    Market structure confirmed from JS:
      market.marketType.id  → "3743" etc
      market.row[0].prices  → [{name:"1", price:1.92}, {name:"X",...}, {name:"2",...}]
    """
    markets = ev.get("markets") or []
    result  = {k: None for k in MARKET_ORDER}
    id_to_name = {v: k for k, v in MARKET_IDS.items()}

    for mkt in markets:
        mt    = mkt.get("marketType") or {}
        mid   = str(mt.get("id") or "")
        mname = id_to_name.get(mid)
        if not mname:
            # Try name match
            n = str(mt.get("name") or "").upper()
            if "1X2" in n:     mname = "1X2"
            elif "OVER" in n or "UNDER" in n or "O/U" in n: mname = "O/U"
            elif "BTTS" in n or "BOTH" in n:  mname = "BTTS"
            elif "DOUBLE" in n or mid == "4693": mname = "DC"
            elif "HALF" in n and "TIME" in n:   mname = "HT/FT"
        if not mname: continue

        rows   = mkt.get("row") or []
        prices = []
        for row in rows:
            prices.extend(row.get("prices") or [])

        def _p(name):
            for po in prices:
                if str(po.get("name","")).strip().upper() == name.upper():
                    try:
                        v = float(po["price"])
                        return v if v > 1.0 else None
                    except: pass
            return None

        if mname == "1X2":
            result["1X2"] = {"1": _p("1"), "X": _p("X"), "2": _p("2")}

        elif mname == "O/U":
            # Multiple lines — group by handicap/line
            lines = {}
            for row in rows:
                line = row.get("handicap")
                if line is None: line = row.get("line")
                ps   = row.get("prices") or []
                over = under = None
                for po in ps:
                    n  = str(po.get("name","")).upper()
                    try:
                        v = float(po["price"])
                        if v <= 1.0: continue
                        if "OVER" in n or n == "O":  over  = v
                        elif "UNDER" in n or n == "U": under = v
                    except: pass
                if line is not None and (over or under):
                    lines[str(line)] = {"O": over, "U": under}
            result["O/U"] = lines if lines else None

        elif mname == "BTTS":
            result["BTTS"] = {"Yes": _p("YES") or _p("Yes"), "No": _p("NO") or _p("No")}

        elif mname == "DC":
            result["DC"] = {"1X": _p("1X"), "X2": _p("X2"), "12": _p("12")}

        elif mname == "HT/FT":
            htft = {}
            for po in prices:
                n = str(po.get("name","")).strip()
                try:
                    v = float(po["price"])
                    if v > 1.0: htft[n] = v
                except: pass
            result["HT/FT"] = htft if htft else None

    return result

# ─── STANDINGS ────────────────────────────────────────────────────────────────
async def fetch_standings(client, season_id: str, league_id: int) -> list[dict]:
    """
    Fetch standings for a specific league from a season.
    Confirmed endpoint: GET /api/sportsbook/virtual/v1/standings/by-season/{seasonId}
    Response: { competitionStandings: [ { id (leagueId), participantStandings:[...] } ] }
    """
    url  = BASE + EP_STANDINGS.format(sid=season_id)
    data = await _get(client, url)
    if not data: return []

    comp_standings = data.get("competitionStandings") or []
    # Find our league
    league_entry = None
    for cs in comp_standings:
        try:
            if int(cs.get("id", -1)) == league_id:
                league_entry = cs; break
        except: pass

    if not league_entry:
        log.warning(f"standings: leagueId {league_id} not found. Available: {[c.get('id') for c in comp_standings]}")
        # If only one entry, use it
        if len(comp_standings) == 1:
            league_entry = comp_standings[0]
        else:
            return []

    rows = []
    for p in (league_entry.get("participantStandings") or []):
        pts_obj = p.get("points") or {}
        sc_obj  = p.get("scoreStanding") or {}
        w   = int(pts_obj.get("won",  0)  or 0)
        d   = int(pts_obj.get("draw", 0)  or 0)
        l   = int(pts_obj.get("lost", 0)  or 0)
        pts = int(pts_obj.get("total", 0) or 0) or w*3+d
        gf  = int(sc_obj.get("scored",   0) or 0)
        ga  = int(sc_obj.get("conceded", 0) or 0)
        form_raw = p.get("form") or []
        form = [str(f).upper()[:1] for f in form_raw if str(f).upper()[:1] in ("W","D","L")]
        rows.append({
            "pos":  int(p.get("position") or 0),
            "name": str(p.get("name") or p.get("id") or "?"),
            "pts": pts, "w": w, "d": d, "l": l,
            "gf": gf, "ga": ga, "gd": gf-ga,
            "played": w+d+l, "form": form[-5:],
        })
    rows.sort(key=lambda r: (-r["pts"], -r["gd"], -r["gf"], r["name"]))
    for i, r in enumerate(rows, 1):
        r["pos"] = i
    log.info(f"standings season={season_id} league={league_id}: {len(rows)} teams")
    return rows

# ─── APP DATA STRUCTURE ───────────────────────────────────────────────────────
import random as _random

def _app_data(bot_data: dict) -> dict:
    d = bot_data.setdefault("app_data", {})
    d.setdefault("devices", {})
    d.setdefault("codes",   {})
    d.setdefault("purchase_message", "")
    return d

def _app_gen_code() -> str:
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    part  = lambda n: "".join(_random.choices(chars, k=n))
    return f"SIG-{part(4)}-{part(4)}-{part(4)}"

ONLINE_TIMEOUT_SECS = 90

def _is_device_online(fingerprint: str, bot_data: dict) -> bool:
    last = bot_data.get("app_heartbeats", {}).get(fingerprint, 0)
    return (int(time.time()) - last) <= ONLINE_TIMEOUT_SECS

def _last_seen_str(fingerprint: str, bot_data: dict) -> str:
    last = bot_data.get("app_heartbeats", {}).get(fingerprint, 0)
    if last == 0: return "never"
    diff = int(time.time()) - last
    if diff < 60:    return f"{diff}s ago"
    if diff < 3600:  return f"{diff//60}m ago"
    if diff < 86400: return f"{diff//3600}h ago"
    return f"{diff//86400}d ago"

def bot_data_online_count(bot_data: dict) -> int:
    threshold = int(time.time()) - ONLINE_TIMEOUT_SECS
    activated_fps = set(bot_data.get("app_heartbeats", {}).keys())
    activated = sum(1 for ts in bot_data.get("app_heartbeats", {}).values() if ts >= threshold)
    guests    = sum(1 for fp, ts in bot_data.get("app_guest_heartbeats", {}).items()
                    if ts >= threshold and fp not in activated_fps)
    return activated + guests

def _app_pred_to_card(p: dict) -> dict:
    return {
        "homeTeam":     p.get("homeTeam", ""),
        "awayTeam":     p.get("awayTeam", ""),
        "league":       p.get("league", ""),
        "leagueFlag":   p.get("leagueFlag", ""),
        "season":       p.get("season", ""),
        "matchday":     p.get("matchday", ""),
        "tip":          p.get("tip", ""),
        "confidence":   p.get("confidence", ""),
        "overallLabel": p.get("overallLabel", ""),
        "odds":         p.get("odds", ""),
        "rawText":      p.get("rawText", ""),
        "timestamp":    p.get("timestamp", 0),
        "messageId":    p.get("messageId", 0),
        "result":       p.get("result", "PENDING"),
        "score":        p.get("score", ""),
    }

# ─── FIREBASE HELPERS ─────────────────────────────────────────────────────────
_FB_ENCRYPT_KEY_ENV = os.environ.get("FB_ENCRYPT_KEY", "")

def _fb_derive_fernet():
    try:
        from cryptography.fernet import Fernet
        secret = (_FB_ENCRYPT_KEY_ENV or BOT_TOKEN).encode()
        raw32  = hashlib.sha256(secret).digest()
        key    = base64.urlsafe_b64encode(raw32)
        return Fernet(key)
    except Exception as e:
        log.warning(f"Fernet init failed: {e}")
        return None

def _fb_encrypt_payload(data: dict) -> str | None:
    f = _fb_derive_fernet()
    if not f: return None
    try:
        return f.encrypt(json.dumps(data, separators=(",",":"), default=str).encode()).decode()
    except Exception as e:
        log.warning(f"Encrypt failed: {e}"); return None

def _fb_decrypt_payload(ciphertext: str) -> dict | None:
    f = _fb_derive_fernet()
    if not f: return None
    try:
        return json.loads(f.decrypt(ciphertext.encode()))
    except Exception as e:
        log.warning(f"Decrypt failed: {e}"); return None

def _fb_save_subscription(code: str, fingerprint: str, uid: str, username: str, exp_ts: int, days: int):
    if not _fb_db: return
    try:
        _fb_db.collection("subscriptions").document(code).set({
            "code": code, "fingerprint": fingerprint, "userId": uid,
            "username": username, "expireTs": exp_ts, "days": days,
            "activatedAt": int(time.time()), "updatedAt": int(time.time()),
        })
        _fb_db.collection("devices").document(fingerprint).set({
            "code": code, "userId": uid, "username": username,
            "expireTs": exp_ts, "updatedAt": int(time.time()),
        })
        log.info(f"✅ Firebase: subscription saved code={code} fp={fingerprint[:8]}")
    except Exception as e:
        log.warning(f"Firebase save failed: {e}")

def _fb_get_by_fingerprint(fingerprint: str) -> dict | None:
    if not _fb_db: return None
    try:
        doc = _fb_db.collection("devices").document(fingerprint).get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        log.warning(f"Firebase get_by_fingerprint failed: {e}"); return None

def _fb_get_by_code(code: str) -> dict | None:
    if not _fb_db: return None
    try:
        doc = _fb_db.collection("subscriptions").document(code).get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        log.warning(f"Firebase get_by_code failed: {e}"); return None

def _fb_update_fingerprint(code: str, new_fingerprint: str):
    if not _fb_db: return
    try:
        sub = _fb_get_by_code(code)
        if not sub: return
        _fb_db.collection("subscriptions").document(code).update(
            {"fingerprint": new_fingerprint, "updatedAt": int(time.time())})
        _fb_db.collection("devices").document(new_fingerprint).set({
            "code": code, "userId": sub.get("userId",""),
            "username": sub.get("username",""), "expireTs": sub.get("expireTs",0),
            "updatedAt": int(time.time()),
        })
        log.info(f"Firebase: fingerprint updated code={code} new={new_fingerprint[:8]}")
    except Exception as e:
        log.warning(f"Firebase update_fingerprint failed: {e}")

def _fb_cloud_push(bot_data: dict):
    if not _fb_db: return
    try:
        acc  = bot_data.get("access", {})
        app  = bot_data.get("app_data", {})
        if not prediction_memory:
            log.warning("_fb_cloud_push: prediction_memory is EMPTY — pushing empty brain. If this is after a restore, brain may not have loaded yet.")
        payload = {
            "saved_at":         int(time.time()),
            "users":            {str(k): v for k, v in acc.get("users", {}).items()},
            "allowed_channels": list(acc.get("allowed_channels", set())),
            "auto_chats":       list(bot_data.get("auto_chats", set())),
            "app_devices":      app.get("devices", {}),
            "app_codes":        app.get("codes", {}),
            "purchase_message": app.get("purchase_message", ""),
            "predictions":      prediction_memory,
            "strategy_stats":   strategy_stats,
            "thresholds":       thresholds,
            "posted_rounds":    list(_posted_rounds),
        }
        ciphertext = _fb_encrypt_payload(payload)
        if not ciphertext:
            log.warning("_fb_cloud_push: encryption returned None — is 'cryptography' installed? Brain NOT saved to Firebase.")
            return
        _fb_db.collection("bot_state").document("subscriptions").set(
            {"payload": ciphertext, "saved_at": int(time.time()), "version": 1})
        log.info(f"☁️  Firebase cloud push OK (incl. brain: {len(prediction_memory)} predictions, {len(payload.get('app_devices', {}))} devices)")
    except Exception as e:
        log.warning(f"_fb_cloud_push failed: {e}")

def _fb_cloud_restore(bot_data: dict) -> bool:
    if not _fb_db: return False
    try:
        doc = _fb_db.collection("bot_state").document("subscriptions").get()
        if not doc.exists:
            log.info("☁️  No cloud snapshot found — starting fresh")
            return False
        ciphertext = doc.to_dict().get("payload", "")
        if not ciphertext: return False
        data = _fb_decrypt_payload(ciphertext)
        if not data:
            log.warning("☁️  Cloud restore: decryption failed — wrong key?")
            return False
        acc = bot_data.setdefault("access", {"users":{}, "allowed_channels":set(), "pending_user":{}})
        acc.setdefault("users", {}); acc.setdefault("allowed_channels", set())
        restored = 0
        now_ts = time.time()
        for uid_str, udata in data.get("users", {}).items():
            if uid_str not in acc["users"]:
                acc["users"][str(uid_str)] = udata; restored += 1
        # Re-add all non-expired Telegram users back to auto_chats so predictions
        # resume immediately after a restart without requiring them to /start again.
        existing_chats = bot_data.setdefault("auto_chats", set())
        for uid_str, udata in acc["users"].items():
            if udata.get("expire_ts", 0) > now_ts and not uid_str.startswith("app_"):
                existing_chats.add(str(uid_str))
                log.info(f"☁️  Cloud restore: re-added uid {uid_str} to auto_chats (subscription active)")
        for ch in data.get("allowed_channels", []):
            acc["allowed_channels"].add(str(ch))
        existing_chats = bot_data.setdefault("auto_chats", set())
        for ch in data.get("auto_chats", []):
            existing_chats.add(str(ch))
        app = _app_data(bot_data)
        dev_r = code_r = 0
        for fp, rec in data.get("app_devices", {}).items():
            if fp not in app["devices"]: app["devices"][fp] = rec; dev_r += 1
        for code, rec in data.get("app_codes", {}).items():
            if code not in app["codes"]: app["codes"][code] = rec; code_r += 1
        if data.get("purchase_message") and not app.get("purchase_message"):
            app["purchase_message"] = data["purchase_message"]
        global prediction_memory, strategy_stats, thresholds, _posted_rounds
        brain_r = 0
        if data.get("predictions"):
            prediction_memory = data["predictions"]
            brain_r = len(prediction_memory)
        for k, v in data.get("strategy_stats", {}).items():
            if k in strategy_stats: strategy_stats[k] = v
        for k, v in data.get("thresholds", {}).items():
            if k in thresholds:
                rv = float(v)
                # Safety clamp: corrupted draws can push thresholds to their max,
                # permanently silencing all predictions. Cap on restore.
                if k == "dominance": rv = min(rv, 0.65)
                elif k == "draw":    rv = min(rv, 0.55)
                thresholds[k] = rv
        # Restore posted_rounds but DROP the most recent one so the bot re-sends
        # predictions for the current active round after a restart.
        restored_rounds = set(data.get("posted_rounds", []))
        if restored_rounds:
            most_recent = max(restored_rounds, key=lambda r: int(r) if r.isdigit() else 0)
            restored_rounds.discard(most_recent)
            log.info(f"☁️  Cloud restore: dropped most-recent round {most_recent} from _posted_rounds so it re-posts on next loop.")
        _posted_rounds = restored_rounds
        log.info(f"☁️  Cloud restore: {restored} tg-users, {dev_r} app-devices, {code_r} codes, {brain_r} brain predictions")
        return (restored + dev_r + code_r + brain_r) > 0
    except Exception as e:
        log.warning(f"_fb_cloud_restore failed: {e}"); return False


# ─── KEYBOARDS ────────────────────────────────────────────────────────────────
# ─── H2H (HEAD-TO-HEAD) ACROSS ALL SEASONS ──────────────────────────────────
# Cache: (home_lower, away_lower) -> result dict.  Survives for the process lifetime.
_h2h_cache: dict[tuple, dict] = {}
_h2h_locks: dict[tuple, "asyncio.Lock"] = {}

# ─── PREDICTION ENGINE ────────────────────────────────────────────────────────
# Step 1: Global prediction memory — persists in-process; saved to disk via JSON.
PRED_FILE = "predictions.json"

prediction_memory: list[dict] = []   # {home, away, league_id, pick, confidence, result, round_id, timestamp}

strategy_stats: dict[str, dict] = {
    "HOME WIN": {"wins": 0, "losses": 0},
    "AWAY WIN": {"wins": 0, "losses": 0},
    "DRAW":     {"wins": 0, "losses": 0},
    "1X":       {"wins": 0, "losses": 0},
    "X2":       {"wins": 0, "losses": 0},
}

thresholds: dict[str, float] = {
    "dominance": 0.60,   # minimum win-rate for a "dominant" pick
    "draw":      0.50,   # minimum draw-rate to back DRAW
}

# Track which rounds we have already auto-posted to avoid duplicate blasts.
_posted_rounds: set[str] = set()


def _load_predictions():
    """Load persisted predictions + stats from disk (called once at startup)."""
    global prediction_memory, strategy_stats, thresholds, _posted_rounds
    if not os.path.exists(PRED_FILE):
        return
    try:
        with open(PRED_FILE) as f:
            data = json.load(f)
        prediction_memory = data.get("predictions", [])
        for k, v in data.get("strategy_stats", {}).items():
            if k in strategy_stats:
                strategy_stats[k] = v
        for k, v in data.get("thresholds", {}).items():
            if k in thresholds:
                rv = float(v)
                if k == "dominance": rv = min(rv, 0.65)
                elif k == "draw":    rv = min(rv, 0.55)
                thresholds[k] = rv
        _posted_rounds = set(data.get("posted_rounds", []))
        log.info(f"Loaded {len(prediction_memory)} predictions from {PRED_FILE}")
    except Exception as e:
        log.error(f"Could not load {PRED_FILE}: {e}")


def _save_predictions():
    """Persist predictions + stats to disk, and live-sync to Firebase."""
    try:
        with open(PRED_FILE, "w") as f:
            json.dump({
                "predictions":   prediction_memory,
                "strategy_stats": strategy_stats,
                "thresholds":    thresholds,
                "posted_rounds": list(_posted_rounds),
            }, f, indent=2)
    except Exception as e:
        log.error(f"Could not save {PRED_FILE}: {e}")
    if _fb_db and _tg_app is not None:
        try:
            _fb_cloud_push(_tg_app.bot_data)
        except Exception as e:
            log.warning(f"_save_predictions Firebase sync failed: {e}")


# ── Step 4: win-rate helper ───────────────────────────────────────────────────
def get_strategy_accuracy(pick: str) -> float:
    stats = strategy_stats.get(pick, {})
    total = stats.get("wins", 0) + stats.get("losses", 0)
    return stats["wins"] / total if total else 0.0


# ── Step 5: adaptive threshold adjuster ──────────────────────────────────────
def adjust_thresholds():
    home_acc = get_strategy_accuracy("HOME WIN")
    draw_acc = get_strategy_accuracy("DRAW")

    if home_acc < 0.60:
        thresholds["dominance"] = min(thresholds["dominance"] + 0.05, 0.75)
    elif home_acc > 0.75:
        thresholds["dominance"] = max(thresholds["dominance"] - 0.02, 0.55)

    if draw_acc > 0.70:
        thresholds["draw"] = max(thresholds["draw"] - 0.05, 0.40)
    elif draw_acc < 0.50:
        thresholds["draw"] = min(thresholds["draw"] + 0.05, 0.60)

    log.info(f"adjust_thresholds → dominance={thresholds['dominance']:.2f}  draw={thresholds['draw']:.2f}")


# ── Step 6: adaptive pick engine ─────────────────────────────────────────────
def evaluate_h2h_pick(home: str, away: str, h2h: dict) -> dict | None:
    """Return {pick, confidence} or None if no confident pick available."""
    n = h2h.get("played", 0)
    if n < 3:          # need at least 3 meetings
        return None

    hw = h2h["hw"]
    d  = h2h["d"]
    aw = h2h["aw"]

    home_rate = hw / n
    draw_rate = d  / n
    away_rate = aw / n

    dom = thresholds["dominance"]
    drw = thresholds["draw"]

    if home_rate >= dom and aw == 0:
        return {"pick": "HOME WIN", "confidence": 90}
    if away_rate >= dom and hw == 0:
        return {"pick": "AWAY WIN", "confidence": 90}
    if draw_rate >= drw:
        return {"pick": "DRAW", "confidence": 85}
    if home_rate + draw_rate >= 0.80:
        return {"pick": "1X", "confidence": 88}
    if away_rate + draw_rate >= 0.80:
        return {"pick": "X2", "confidence": 88}

    return None


def _store_prediction(home: str, away: str, league_id: int, round_id: str, pick: str, confidence: int):
    """Step 1 — record a new prediction."""
    prediction_memory.append({
        "home":       home,
        "away":       away,
        "league_id":  league_id,
        "round_id":   round_id,
        "pick":       pick,
        "confidence": confidence,
        "result":     None,
        "timestamp":  time.time(),
    })
    _save_predictions()


# ── Step 2: update results from finished-round matchup data ──────────────────
def update_results(finished_matches: list[dict], bot_data: dict | None = None):
    """Match finished scores against pending predictions and fill in results.
    Also updates app_predictions so the app shows WIN/LOSS in history."""
    changed = False
    for pred in prediction_memory:
        if pred.get("result") is not None:
            continue
        for match in finished_matches:
            if (match["home"].upper() == pred["home"].upper() and
                    match["away"].upper() == pred["away"].upper()):
                hs, as_ = match["hs"], match["as_"]
                if hs > as_:   actual = "HOME"
                elif hs < as_: actual = "AWAY"
                else:           actual = "DRAW"
                pred["result"] = actual
                changed = True
                log.info(f"Prediction resolved: {pred['home']} vs {pred['away']} → {actual}  (pick was {pred['pick']})")

                # ── Sync result into app_predictions ──────────────────────────
                if bot_data is not None:
                    pick = pred.get("pick", "")
                    correct = (
                        (pick == "HOME WIN" and actual == "HOME") or
                        (pick == "AWAY WIN" and actual == "AWAY") or
                        (pick == "DRAW"     and actual == "DRAW") or
                        (pick == "1X"       and actual in ("HOME", "DRAW")) or
                        (pick == "X2"       and actual in ("AWAY", "DRAW"))
                    )
                    app_result = "WIN" if correct else "LOSS"
                    score_str  = f"{hs}-{as_}"
                    pred_rid   = str(pred.get("round_id", ""))
                    app_preds  = bot_data.get("app_predictions", [])
                    for ap in app_preds:
                        # Match on team names + round_id (via _rid) + still PENDING
                        # to avoid updating the wrong entry if the same fixture
                        # repeats across different rounds.
                        ap_rid = str(ap.get("_rid", ""))
                        if (ap.get("homeTeam", "").upper() == pred["home"].upper() and
                                ap.get("awayTeam", "").upper() == pred["away"].upper() and
                                ap.get("result", "") == "PENDING" and
                                (not pred_rid or not ap_rid or ap_rid == pred_rid)):
                            ap["result"] = app_result
                            ap["score"]  = score_str
                            log.info(f"app_predictions updated: {pred['home']} vs {pred['away']} → {app_result} ({score_str}) rid={pred_rid}")
                break  # stop searching matches once this prediction is resolved
    if changed:
        _save_predictions()


# ── Step 3: evaluate performance of resolved predictions ─────────────────────
def evaluate_performance():
    for pred in prediction_memory:
        if pred.get("result") is None:
            continue
        if pred.get("_evaluated"):
            continue                 # already counted
        pick   = pred["pick"]
        actual = pred["result"]
        correct = (
            (pick == "HOME WIN" and actual == "HOME") or
            (pick == "AWAY WIN" and actual == "AWAY") or
            (pick == "DRAW"     and actual == "DRAW") or
            (pick == "1X"       and actual in ("HOME", "DRAW")) or
            (pick == "X2"       and actual in ("AWAY", "DRAW"))
        )
        if pick in strategy_stats:
            if correct:
                strategy_stats[pick]["wins"]   += 1
            else:
                strategy_stats[pick]["losses"] += 1
        pred["_evaluated"] = True

    _save_predictions()

def _team_codes(league_id: int) -> set[str]:
    """Return the known 3-letter codes for a league (lower-cased)."""
    return {t.lower() for t in LEAGUE_TEAMS.get(league_id, set())}

H2H_MAX = 10   # maximum number of meetings to collect and display

async def _scan_round_for_h2h(
    client, rid: str, rname: str, hn: str, an: str, league_codes: set
) -> list[dict]:
    """
    Fetch one finished round and return a list of meeting dicts for the
    given fixture (home_name vs away_name, EXACT orientation only).
    Reversed fixtures (away @ home) are NEVER included.
    Each meeting: {rid, rname, hs, as_, hht, aht}
    Cached via _round_cache so each round URL is fetched at most once.
    """
    events = await fetch_events(client, rid, PAGE_MATCHUPS)
    meetings = []
    hn_u = hn.strip().upper()
    an_u = an.strip().upper()
    for ev in events:
        eh, ea = _get_teams(ev)
        eh_u = eh.strip().upper()
        ea_u = ea.strip().upper()
        # League filter — skip events from other leagues using exact code match
        if league_codes and not (eh_u in league_codes or ea_u in league_codes):
            continue
        # STRICT directional match: home must be home, away must be away.
        # Reversed fixture (e.g. MUN vs ARS when looking for ARS vs MUN) is SKIPPED.
        if eh_u != hn_u or ea_u != an_u:
            continue
        hs, as_, hht, aht = _get_score(ev)
        if hs is None or as_ is None:
            continue
        meetings.append({"rid": rid, "rname": rname, "hs": hs, "as_": as_, "hht": hht, "aht": aht})
    return meetings


async def fetch_h2h_all_seasons(client, home_name: str, away_name: str, league_id: int) -> dict:
    """
    Scan every finished round (newest first) for the exact fixture
    home_name @ home vs away_name @ away.  Stops collecting once
    H2H_MAX (10) meetings are found.

    _round_cache deduplicates HTTP calls across concurrent fixture lookups.

    Returns:
        {
          "hw": int, "d": int, "aw": int, "played": int,   # totals
          "meetings": [                                      # up to 10, newest first
              {"rid", "rname", "hs", "as_", "hht", "aht"}, …
          ]
        }
    """
    hn  = home_name.strip().lower()
    an  = away_name.strip().lower()
    key = (hn, an)

    if key in _h2h_cache:
        return _h2h_cache[key]
    if key not in _h2h_locks:
        _h2h_locks[key] = asyncio.Lock()

    async with _h2h_locks[key]:
        if key in _h2h_cache:
            return _h2h_cache[key]

        all_seasons  = await fetch_all_seasons(client)
        now_ms       = int(time.time() * 1000)
        league_codes = {t.upper() for t in LEAGUE_TEAMS.get(league_id, set())}

        # Build a flat list of (round_id, round_name) for COMPLETED rounds only.
        # A round is only considered finished when tradingTime.end (match kick-off)
        # PLUS the real-world match duration has passed. Using tradingTime.start
        # (betting opens) is too early — it includes the current active round whose
        # matchups page only has score:"0.0" (unplayed), producing fake 0-0 draws.
        round_pairs: list[tuple[str, str]] = []
        for season in all_seasons:
            rws_ms = int((season.get("realWorldMatchSeconds") or season.get("matchSeconds") or 300) * 1000)
            rnds = sorted(
                season.get("rounds", []),
                key=lambda r: int(r.get("id", 0)) if str(r.get("id", "")).isdigit() else 0,
                reverse=True,
            )
            for rnd in rnds:
                rid = str(rnd.get("id", "")).strip()
                if not rid:
                    continue
                tt = rnd.get("tradingTime") or {}
                # tradingTime.end = match kick-off; add match duration for full-time
                end_ms = _iso_to_ms(tt.get("end") or tt.get("start") or 0) or _start_ms(rnd)
                if end_ms + rws_ms <= now_ms:
                    rname = str(rnd.get("name", rid))
                    round_pairs.append((rid, rname))


        # Scan finished rounds sequentially newest-first.
        # STRICT DIRECTIONAL — only home=home_name AND away=away_name.
        # Reversed fixture is NEVER included.
        log.info(
            f"H2H FETCH ▶ STRICT direction: [{home_name.upper()} HOME] vs [{away_name.upper()} AWAY] "
            f"— reversed [{away_name.upper()} vs {home_name.upper()}] is EXCLUDED"
        )
        all_meetings: list[dict] = []
        for rid, rname in round_pairs:
            if len(all_meetings) >= H2H_MAX:
                break
            mtgs = await _scan_round_for_h2h(client, rid, rname, hn, an, league_codes)
            all_meetings.extend(mtgs)
        meetings = all_meetings[:H2H_MAX]

        hw = sum(1 for m in meetings if m["hs"] > m["as_"])
        d  = sum(1 for m in meetings if m["hs"] == m["as_"])
        aw = sum(1 for m in meetings if m["hs"] < m["as_"])

        result = {
            "hw": hw, "d": d, "aw": aw,
            "played": len(meetings),
            "meetings": meetings,
        }
        _h2h_cache[key] = result
        log.info(
            f"H2H RESULT ✓ {home_name.upper()} (HOME) vs {away_name.upper()} (AWAY) "
            f"(lid={league_id}): {len(meetings)} meetings — {hw}W {d}D {aw}L [exact direction only]"
        )

    return _h2h_cache[key]


def _h2h_line(h2h: dict) -> str:
    """Bold highlighted H2H badge for the fixtures list (upcoming view)."""
    if not h2h or h2h["played"] == 0:
        return "⬜ _No H2H history_"
    hw, d, aw, n = h2h["hw"], h2h["d"], h2h["aw"], h2h["played"]
    trend = "🟢" if hw > aw and hw > d else ("🔴" if aw > hw and aw > d else "🟡")
    return f"{trend} *H2H (last {n}):* 🟢`{hw}`  🟡`{d}`  🔴`{aw}`"


def _h2h_block(home_name: str, away_name: str, h2h: dict) -> str:
    """
    Full H2H block for the match-detail view — shows up to H2H_MAX
    individual results, newest first, plus a summary line.
    """
    if not h2h or h2h["played"] == 0:
        return "📜 *Head-to-Head*\n  _No historical data found_\n\n"

    hw, d, aw, n = h2h["hw"], h2h["d"], h2h["aw"], h2h["played"]
    meetings      = h2h.get("meetings", [])

    lines  = [f"📜 *Head-to-Head (last {n} meetings)*"]
    lines += [f"  🟢 {home_name} wins: *{hw}*   🟡 Draws: *{d}*   🔴 {away_name} wins: *{aw}*"]
    lines += [""]

    for m in meetings:
        hs, as_ = m["hs"], m["as_"]
        hht, aht = m.get("hht"), m.get("aht")
        if hs > as_:   icon = "🟢"
        elif hs == as_: icon = "🟡"
        else:           icon = "🔴"
        ht_str = f" _(HT {hht}-{aht})_" if hht is not None else ""
        lines.append(f"  {icon} MD{m['rname']}: *{hs}–{as_}*{ht_str}")

    lines.append("")
    return "\n".join(lines) + "\n"


def _team_stats(team_name: str, meetings: list, as_home: bool) -> dict:
    n = len(meetings)
    if n == 0:
        return {"n": 0, "gf": 0, "ga": 0, "w": 0, "d": 0, "l": 0,
                "avg_gf": 0.0, "avg_ga": 0.0, "btts": 0, "over15": 0, "over25": 0}
    gf = ga = w = d = l = btts = over15 = over25 = 0
    for m in meetings:
        my_g  = m["hs"] if as_home else m["as_"]
        opp_g = m["as_"] if as_home else m["hs"]
        gf += my_g; ga += opp_g
        total = my_g + opp_g
        if my_g > opp_g:    w += 1
        elif my_g == opp_g: d += 1
        else:               l += 1
        if my_g > 0 and opp_g > 0: btts   += 1
        if total > 1:               over15 += 1
        if total > 2:               over25 += 1
    return {
        "n": n, "gf": gf, "ga": ga, "w": w, "d": d, "l": l,
        "avg_gf": round(gf / n, 2), "avg_ga": round(ga / n, 2),
        "btts": btts, "over15": over15, "over25": over25,
    }


def _stats_summary_line(h2h: dict, home: str, away: str) -> str:
    meetings = h2h.get("meetings", [])
    n = len(meetings)
    if n == 0:
        return "  📊 _No historical data_"
    hs  = _team_stats(home, meetings, as_home=True)
    as_ = _team_stats(away, meetings, as_home=False)
    over25_n = sum(1 for m in meetings if m["hs"] + m["as_"] > 2)
    btts_n   = sum(1 for m in meetings if m["hs"] > 0 and m["as_"] > 0)
    pct = lambda x: f"{round(x/n*100)}%"
    return (
        f"  🏠 `{hs['w']}W {hs['d']}D {hs['l']}L` avg scored `{hs['avg_gf']}` conceded `{hs['avg_ga']}`\n"
        f"  ✈️ `{as_['w']}W {as_['d']}D {as_['l']}L` avg scored `{as_['avg_gf']}` conceded `{as_['avg_ga']}`\n"
        f"  O2.5: {pct(over25_n)} · BTTS: {pct(btts_n)} (last {n})"
    )


def _stats_block(home: str, away: str, h2h: dict) -> str:
    meetings = h2h.get("meetings", [])
    n = len(meetings)
    if n == 0:
        return "📊 *Match Stats*\n  _No historical data_\n\n"
    hs  = _team_stats(home, meetings, as_home=True)
    as_ = _team_stats(away, meetings, as_home=False)
    total_goals = sum(m["hs"] + m["as_"] for m in meetings)
    btts_n   = sum(1 for m in meetings if m["hs"] > 0 and m["as_"] > 0)
    over15_n = sum(1 for m in meetings if m["hs"] + m["as_"] > 1)
    over25_n = sum(1 for m in meetings if m["hs"] + m["as_"] > 2)
    pct = lambda x: f"{round(x/n*100)}%"
    lines = [
        f"📊 *Stats across last {n} meetings*", "",
        f"🏠 *{home}* (at home)",
        f"  Record:          `{hs['w']}W  {hs['d']}D  {hs['l']}L`",
        f"  Goals scored:    `{hs['gf']}` · avg `{hs['avg_gf']}` / game",
        f"  Goals conceded:  `{hs['ga']}` · avg `{hs['avg_ga']}` / game", "",
        f"✈️ *{away}* (away)",
        f"  Record:          `{as_['w']}W  {as_['d']}D  {as_['l']}L`",
        f"  Goals scored:    `{as_['gf']}` · avg `{as_['avg_gf']}` / game",
        f"  Goals conceded:  `{as_['ga']}` · avg `{as_['avg_ga']}` / game", "",
        f"📈 *Fixture trends*",
        f"  Avg total goals:  `{round(total_goals/n, 2)}` / game",
        f"  BTTS:             `{btts_n}/{n}` ({pct(btts_n)})",
        f"  Over 1.5:         `{over15_n}/{n}` ({pct(over15_n)})",
        f"  Over 2.5:         `{over25_n}/{n}` ({pct(over25_n)})", "",
    ]
    return "\n".join(lines) + "\n"


async def _find_upcoming_round(cl) -> tuple:
    """
    Find the upcoming round using the EXACT same algorithm as the betPawa site.

    Reverse-engineered from _app-4204efaaaf359a13.js:

      1. GET /seasons/list/actual  (only actual, NOT past)
      2. For every round in every season, compute:
             bettingClosesTime = tradingTime.end  (ISO -> epoch ms)
      3. Flat-sort all rounds by bettingClosesTime ASC
      4. virtualRoundIndex = first index where bettingClosesTime > now
      5. roundId = gameRounds[virtualRoundIndex].id
      6. Fetch ?page=upcoming with that roundId

    If the actual seasons list is empty or no future round is found, we
    fall back to /seasons/list/past using the same logic.
    """
    global _seasons_cache, _seasons_cache_ts
    _seasons_cache_ts = 0.0           # force-refresh on every upcoming tap

    actual_seasons = await fetch_seasons(cl, past=False)

    def _build_game_rounds(seasons: list) -> list:
        """
        Mirror the JS gameRound builder:
          bettingClosesTime = tradingTime.end (ms)
          status: PAST_GAME_ROUND / BETTING_ACTIVE / BETTING_NOT_OPEN
        Returns flat list sorted by bettingClosesTime ASC.
        """
        now_ms = int(time.time() * 1000)
        game_rounds = []
        for s in seasons:
            for r in s.get("rounds", []):
                rid = str(r.get("id", "")).strip()
                if not rid:
                    continue
                tt = r.get("tradingTime") or {}
                betting_closes = _iso_to_ms(tt.get("end") or tt.get("start") or 0)
                betting_opens  = _iso_to_ms(tt.get("start") or 0)
                if betting_closes == 0:
                    betting_closes = _start_ms(r)
                    betting_opens  = betting_closes
                if betting_closes < now_ms:
                    status = "PAST_GAME_ROUND"
                elif betting_opens <= now_ms:
                    status = "BETTING_ACTIVE"
                else:
                    status = "BETTING_NOT_OPEN"
                game_rounds.append({
                    "id":                rid,
                    "name":              str(r.get("name", rid)),
                    "season_id":         s["id"],
                    "bettingClosesTime": betting_closes,
                    "status":            status,
                })
        game_rounds.sort(key=lambda x: x["bettingClosesTime"])
        return game_rounds

    now_ms = int(time.time() * 1000)

    # ── Primary: actual seasons (mirrors site exactly) ────────────────────────
    game_rounds = _build_game_rounds(actual_seasons)
    idx = next((i for i, r in enumerate(game_rounds) if r["bettingClosesTime"] > now_ms), -1)
    if idx != -1:
        gr = game_rounds[idx]
        log.info(f"_find_upcoming_round: [actual idx={idx}] round {gr['id']} ({gr['name']}) status={gr['status']}")
        events = await fetch_events(cl, gr["id"], PAGE_UPCOMING)
        if events:
            return gr["id"], gr["name"], gr.get("season_id", "")

    # ── Fallback: past seasons with same algorithm ────────────────────────────
    past_seasons = await fetch_seasons(cl, past=True)
    game_rounds_past = _build_game_rounds(past_seasons)
    idx = next((i for i, r in enumerate(game_rounds_past) if r["bettingClosesTime"] > now_ms), -1)
    if idx != -1:
        gr = game_rounds_past[idx]
        log.info(f"_find_upcoming_round: [past idx={idx}] round {gr['id']} ({gr['name']}) status={gr['status']}")
        events = await fetch_events(cl, gr["id"], PAGE_UPCOMING)
        if events:
            return gr["id"], gr["name"], gr.get("season_id", "")

    # ── Last resort: highest actual round that returns upcoming events ─────────
    for gr in sorted(game_rounds, key=lambda x: int(x["id"]) if x["id"].isdigit() else 0, reverse=True):
        events = await fetch_events(cl, gr["id"], PAGE_UPCOMING)
        if events:
            log.warning(f"_find_upcoming_round: [last-resort] round {gr['id']} ({gr['name']})")
            return gr["id"], gr["name"], gr.get("season_id", "")

    log.error("_find_upcoming_round: could not find any upcoming round")
    return None, None, None


def league_kb(prefix: str) -> InlineKeyboardMarkup:
    buttons, row = [], []
    for lid, info in LEAGUES.items():
        row.append(InlineKeyboardButton(f"{info['flag']} {info['name']}", callback_data=f"{prefix}:{lid}"))
        if len(row) == 2: buttons.append(row); row = []
    if row: buttons.append(row)
    return InlineKeyboardMarkup(buttons)

def back_kb(cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Back", callback_data=cb)]])

# ─── /start ───────────────────────────────────────────────────────────────────
async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    if _is_admin(uid):
        chats = c.bot_data.setdefault("auto_chats", set())
        chats.add(str(u.effective_chat.id))
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🧠 Brain Status",   callback_data="menu:brainstatus"),
                InlineKeyboardButton("🟢 Online Now",     callback_data="menu:activeonline"),
            ],
            [
                InlineKeyboardButton("📊 Standings",      callback_data="menu:standings"),
                InlineKeyboardButton("👑 Admin Status",   callback_data="menu:status"),
            ],
            [
                InlineKeyboardButton("➕ Add User",    callback_data="menu:adduser_prompt"),
                InlineKeyboardButton("➖ Remove User", callback_data="menu:removeuser_prompt"),
            ],
            [
                InlineKeyboardButton("📢 Add Channel",    callback_data="menu:addchannel_prompt"),
                InlineKeyboardButton("🗑 Remove Channel", callback_data="menu:removechannel_prompt"),
            ],
            [
                InlineKeyboardButton("📱 Gen App Code",   callback_data="menu:appgencode_prompt"),
                InlineKeyboardButton("👥 App Users",      callback_data="menu:appusers"),
            ],
            [
                InlineKeyboardButton("✉️ Set App Msg",    callback_data="menu:appsetmsg_prompt"),
                InlineKeyboardButton("📡 Live",           callback_data="menu:live"),
            ],
        ])
        await u.message.reply_text(
            "👑 *BetPawa Virtual Bot — Admin*\n\nSelect a feature:",
            parse_mode="Markdown", reply_markup=kb,
        )
        return

    if _is_authorized(uid, c.bot_data):
        rem = _remaining_days(uid, c.bot_data)
        chats = c.bot_data.setdefault("auto_chats", set())
        chats.add(str(u.effective_chat.id))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🪪 My Status", callback_data="menu:mystatus")],
        ])
        await u.message.reply_text(
            f"✅ *BetPawa Virtual Bot*\n_{rem:.1f} days remaining_\n\nSelect a feature:",
            parse_mode="Markdown", reply_markup=kb,
        )
        return

    acc = _access(c.bot_data)
    contact = InlineKeyboardMarkup([[InlineKeyboardButton("💬 Contact Admin", url="https://t.me/MrSimTech")]])
    if str(uid) in acc["users"]:
        await u.message.reply_text("⏰ *Access expired.* Contact admin to renew.", parse_mode="Markdown", reply_markup=contact)
    else:
        await u.message.reply_text("🔒 *Access Restricted.* Contact admin to get access.", parse_mode="Markdown", reply_markup=contact)

async def cmd_stop(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    if not _can_access(uid, c.bot_data):
        await u.message.reply_text("🔒 Access restricted."); return
    chats = c.bot_data.get("auto_chats", set())
    chats.discard(str(u.effective_chat.id))
    await u.message.reply_text("⛔ *Stopped.* No longer receiving auto-posts.", parse_mode="Markdown")

async def cmd_mystatus(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    if _is_admin(uid):
        await u.message.reply_text("👑 *You are the admin.*", parse_mode="Markdown"); return
    rem = _remaining_days(uid, c.bot_data)
    if rem is None:
        await u.message.reply_text("❌ No active subscription."); return
    await u.message.reply_text(f"🪪 *Your Subscription*\n\n⏳ *{rem:.1f} days remaining*", parse_mode="Markdown")

# ─── /adduser /removeuser ─────────────────────────────────────────────────────
async def cmd_adduser(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    if not _is_admin(uid):
        await u.message.reply_text("🔒 Admin only."); return
    args = u.message.text.split()[1:] if u.message.text else []
    if len(args) < 2:
        await u.message.reply_text("Usage: /adduser <user_id> <days>"); return
    target, days = str(args[0]), int(args[1])
    acc = _access(c.bot_data)
    acc["users"][target] = {
        "expire_ts": time.time() + days * 86400,
        "days": days, "added_ts": time.time(),
    }
    await u.message.reply_text(f"✅ User {target} added for {days} days.")

async def cmd_removeuser(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    if not _is_admin(uid): await u.message.reply_text("🔒 Admin only."); return
    args = u.message.text.split()[1:] if u.message.text else []
    if not args: await u.message.reply_text("Usage: /removeuser <user_id>"); return
    acc = _access(c.bot_data)
    acc["users"].pop(str(args[0]), None)
    await u.message.reply_text(f"✅ User {args[0]} removed.")

async def cmd_addchannel(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    if not _is_admin(uid): await u.message.reply_text("🔒 Admin only."); return
    args = u.message.text.split()[1:] if u.message.text else []
    if not args: await u.message.reply_text("Usage: /addchannel <channel_id>"); return
    acc = _access(c.bot_data)
    acc["allowed_channels"].add(str(args[0]))
    await u.message.reply_text(f"✅ Channel {args[0]} added.")

async def cmd_removechannel(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    if not _is_admin(uid): await u.message.reply_text("🔒 Admin only."); return
    args = u.message.text.split()[1:] if u.message.text else []
    if not args: await u.message.reply_text("Usage: /removechannel <channel_id>"); return
    acc = _access(c.bot_data)
    acc["allowed_channels"].discard(str(args[0]))
    await u.message.reply_text(f"✅ Channel {args[0]} removed.")

# ─── MENU CALLBACK ────────────────────────────────────────────────────────────
async def cb_menu(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query  = u.callback_query
    await query.answer()
    uid    = query.from_user.id
    action = query.data.split(":", 1)[1]

    if not _can_access(uid, c.bot_data):
        await query.message.reply_text("🔒 Access restricted."); return

    is_admin = _is_admin(uid)

    if action == "activeonline" and is_admin:
        app_d   = _app_data(c.bot_data)
        devices = app_d.get("devices", {})
        now_ts  = int(time.time())
        lines   = ["🟢 *Activated Users Online Right Now*\n"]
        found   = False
        for fp, rec in sorted(devices.items(), key=lambda x: -x[1].get("activated", 0)):
            exp_ts = rec.get("expire_ts", 0)
            if exp_ts > 0 and now_ts > exp_ts: continue
            if not _is_device_online(fp, c.bot_data): continue
            found = True
            username  = rec.get("username", "?")
            days_left = max(0, int((exp_ts - now_ts) / 86400)) if exp_ts > 0 else 0
            fp_short  = fp[:8].upper() + "…"
            lines.append(f"🟢 `{fp_short}` — *{username}* — {days_left}d left")
        if not found:
            lines.append("_(No activated users online right now)_\n")
            lines.append("_Users appear online when the app is open (updates every 60s)_")
        _thr      = now_ts - ONLINE_TIMEOUT_SECS
        _act_fps  = set(c.bot_data.get("app_heartbeats", {}).keys())
        _act_cnt  = sum(1 for ts in c.bot_data.get("app_heartbeats", {}).values() if ts >= _thr)
        _gest_cnt = sum(1 for fp, ts in c.bot_data.get("app_guest_heartbeats", {}).items() if ts >= _thr and fp not in _act_fps)
        lines.append(f"\n_Total online: {_act_cnt + _gest_cnt}  (✅ {_act_cnt} activated  +  👁 {_gest_cnt} on login screen)_")
        await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "live":
        await query.message.reply_text("📡 *Live Scores* — Select league:", parse_mode="Markdown", reply_markup=league_kb("live"))

    elif action == "standings":
        await query.message.reply_text("📊 *Standings* — Select league:", parse_mode="Markdown", reply_markup=league_kb("standings"))

    elif action == "mystatus":
        rem = _remaining_days(uid, c.bot_data)
        if rem is None:
            await query.message.reply_text("❌ No active subscription.")
        else:
            await query.message.reply_text(f"🪪 *Your Subscription*\n\n⏳ *{rem:.1f} days remaining*", parse_mode="Markdown")

    elif action == "brainstatus" and is_admin:
        # Full brain status report inline
        total     = len(prediction_memory)
        resolved  = [p for p in prediction_memory if p.get("result") is not None]
        pending   = [p for p in prediction_memory if p.get("result") is None]
        correct   = [p for p in resolved if _is_correct(p)]
        n_res     = len(resolved)
        n_cor     = len(correct)
        overall   = n_cor / n_res * 100 if n_res else 0.0

        if total < 20:    xp_label = "🌱 Beginner — still learning"
        elif total < 60:  xp_label = "📘 Developing — gaining patterns"
        elif total < 120: xp_label = "🧠 Experienced — model stabilising"
        elif total < 250: xp_label = "🔥 Advanced — high reliability"
        else:             xp_label = "⚡ Expert — fully trained"

        trend_str = "—"
        if n_res >= 20:
            last10 = resolved[-10:]
            prev10 = resolved[-20:-10]
            acc_last = sum(1 for p in last10 if _is_correct(p)) / 10 * 100
            acc_prev = sum(1 for p in prev10 if _is_correct(p)) / 10 * 100
            diff = acc_last - acc_prev
            if diff > 3:    trend_str = f"📈 +{diff:.1f}% (improving)"
            elif diff < -3: trend_str = f"📉 {diff:.1f}% (declining)"
            else:           trend_str = f"➡️ stable ({acc_last:.0f}% last 10)"

        pick_lines = []
        for pick, stats in strategy_stats.items():
            w, l = stats["wins"], stats["losses"]
            t = w + l
            if t == 0: continue
            acc = w / t * 100
            bar = "🟩" * round(acc / 10) + "⬜" * (10 - round(acc / 10))
            pick_lines.append(f"  {_pick_emoji(pick)} *{pick}*: `{w}/{t}` ({acc:.0f}%)\n  {bar}")

        dom = thresholds["dominance"]
        drw = thresholds["draw"]
        dom_bar = "🟩" * round(dom * 10) + "⬜" * (10 - round(dom * 10))
        drw_bar = "🟩" * round(drw * 10) + "⬜" * (10 - round(drw * 10))

        SEP2 = "━" * 26
        lines = [
            f"🧠 *Brain Status Report*", f"{SEP2}", "",
            f"*Experience*",
            f"  {xp_label}",
            f"  Total predictions : `{total}`",
            f"  Resolved          : `{n_res}`",
            f"  Pending           : `{len(pending)}`",
            f"  Correct           : `{n_cor}`", "",
            f"*Overall Accuracy*",
            f"  `{overall:.1f}%`  {'🟩' * round(overall/10)}{'⬜' * (10 - round(overall/10))}",
            f"  Trend: {trend_str}", "",
            f"*Pick Accuracy*",
        ]
        if pick_lines: lines += pick_lines
        else: lines.append("  _No resolved picks yet_")
        lines += [
            "", f"*Adaptive Thresholds*",
            f"  Dominance : `{dom:.2f}`  {dom_bar}",
            f"  Draw      : `{drw:.2f}`  {drw_bar}", "",
            f"_{total} predictions tracked · thresholds auto-adjust every round_",
        ]
        text = "\n".join(lines)
        for chunk in _chunks(text):
            await query.message.reply_text(chunk, parse_mode="Markdown")

    elif action == "status" and is_admin:
        acc   = _access(c.bot_data)
        users = acc.get("users", {})
        chans = acc.get("allowed_channels", set())
        now   = time.time()
        lines = [f"👑 *Admin Status*\n",
                 f"Users: {len(users)}",
                 f"Channels: {len(chans)}",
                 ""]
        for uid_s, info in list(users.items()):
            rem = max(0, (info["expire_ts"] - now)/86400)
            lines.append(f"  `{uid_s}`: {rem:.1f}d left")
        await query.message.reply_text("\n".join(lines), parse_mode="Markdown")

    elif action == "adduser_prompt" and is_admin:
        await query.message.reply_text("👤 *Add User*\n\n`/adduser <user_id> <days>`", parse_mode="Markdown")

    elif action == "appgencode_prompt" and is_admin:
        await query.message.reply_text(
            "📱 *Generate App Code*\n\n"
            "Send: `/appgencode <days>`\n"
            "Example: `/appgencode 30`\n\n"
            "_One-time code locked to device on first use._",
            parse_mode="Markdown")

    elif action == "appusers" and is_admin:
        await _do_appusers(query.message.reply_text, c)

    elif action == "appsetmsg_prompt" and is_admin:
        await query.message.reply_text(
            "✉️ *Set App Purchase Message*\n\n"
            "Send: `/appsetmsg <your message>`\n"
            "Example: `/appsetmsg Contact @MrSimTech on Telegram to get access.`\n\n"
            "_This message is shown to locked/non-activated app users._",
            parse_mode="Markdown")

    elif action == "removeuser_prompt" and is_admin:
        await query.message.reply_text("👤 *Remove User*\n\n`/removeuser <user_id>`", parse_mode="Markdown")

    elif action == "addchannel_prompt" and is_admin:
        await query.message.reply_text("📢 *Add Channel*\n\n`/addchannel <channel_id>`", parse_mode="Markdown")

    elif action == "removechannel_prompt" and is_admin:
        await query.message.reply_text("🗑 *Remove Channel*\n\n`/removechannel <channel_id>`", parse_mode="Markdown")

    elif action == "backup" and is_admin:
        # Brain backup — send predictions.json as a downloadable file
        if not prediction_memory and not any(v["wins"] + v["losses"] for v in strategy_stats.values()):
            await query.message.reply_text("⚠️ No brain data to back up yet.")
            return
        payload = json.dumps({
            "predictions":    prediction_memory,
            "strategy_stats": strategy_stats,
            "thresholds":     thresholds,
            "posted_rounds":  list(_posted_rounds),
            "exported_at":    datetime.datetime.utcnow().isoformat() + "Z",
        }, indent=2)
        fname = f"brain_backup_{int(time.time())}.json"
        await query.message.reply_document(
            document=payload.encode(),
            filename=fname,
            caption=(
                f"🧠 *Brain Backup*\n"
                f"`{len(prediction_memory)}` predictions · "
                f"dom=`{thresholds['dominance']:.2f}` draw=`{thresholds['draw']:.2f}`\n\n"
                f"To restore: send this file back to the bot after `/restorebrain`"
            ),
            parse_mode="Markdown",
        )

# ─── LIVE SCORES ─────────────────────────────────────────────────────────────
# Flow: live:<LID> → fetch current round → page=live → show live scores with scores
# The site refreshes every ~5s; we show current snapshot + refresh button

async def cb_live(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query; await q.answer()
    if not _can_access(q.from_user.id, c.bot_data): return
    parts = q.data.split(":")
    lid   = int(parts[1])
    # Optional: round_id passed as parts[2] for refresh
    forced_rid = parts[2] if len(parts) > 2 else None

    await q.edit_message_text(f"⏳ Fetching live scores for {ld(lid)}…")

    async with httpx.AsyncClient() as cl:
        # Get current season rounds to find live/most-recent round
        seasons = await fetch_seasons(cl, past=False)
        if not seasons or not seasons[0].get("rounds"):
            seasons = await fetch_seasons(cl, past=True)

        round_id = round_name = None
        if forced_rid:
            round_id = forced_rid
        else:
            # Find the currently live or most recent round
            if seasons:
                rounds = seasons[0].get("rounds", [])
                now_ms = int(time.time() * 1000)
                # Sort rounds by start time
                sorted_rounds = sorted(rounds, key=lambda r: _start_ms(r))
                # Find live round (started but not too long ago — matchSeconds is the duration)
                match_ms = (seasons[0].get("matchSeconds") or 300) * 1000
                live_r = None
                for r in sorted_rounds:
                    st = _start_ms(r)
                    if st > 0 and st <= now_ms <= st + match_ms:
                        live_r = r; break
                if not live_r:
                    # Most recently started
                    past_rounds = [r for r in sorted_rounds if _start_ms(r) < now_ms]
                    live_r = past_rounds[-1] if past_rounds else (sorted_rounds[0] if sorted_rounds else None)
                if live_r:
                    round_id   = str(live_r.get("id",""))
                    round_name = str(live_r.get("name",""))

        if not round_id:
            await q.edit_message_text(f"❌ No active round found for {ld(lid)}.",
                                       reply_markup=back_kb("menu:live")); return

        events = await fetch_events(cl, round_id, PAGE_LIVE)
        if not events:
            # Try matchups page (round may have just ended)
            events = await fetch_events(cl, round_id, PAGE_MATCHUPS)

        # Filter to the selected league only
        events = _filter_by_league(events, lid)

    back = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Refresh", callback_data=f"live:{lid}:{round_id}"),
        InlineKeyboardButton("« Leagues", callback_data="menu:live"),
    ]])

    if not events:
        await q.edit_message_text(
            f"📡 *{ld(lid)} — Matchday {round_name or round_id}*\n\n_No live data available right now._",
            parse_mode="Markdown", reply_markup=back,
        ); return

    now_str = datetime.datetime.utcnow().strftime("%H:%M:%S UTC")
    text    = f"📡 *{ld(lid)} — MD {round_name or round_id} · LIVE*\n_{now_str}_\n{SEP}\n\n"
    goals   = finished = 0

    for ev in events:
        home, away = _get_teams(ev)
        hs, as_, hht, aht = _get_score(ev)
        if hs is not None and as_ is not None:
            icon = "🟢" if hs > as_ else "🔴" if hs < as_ else "🟡"
            ht   = f"({hht}-{aht}) " if hht is not None else ""
            text += f"{icon} *{home}*  {ht}{hs}–{as_}  *{away}*\n"
            goals += hs + as_; finished += 1
        else:
            text += f"⚽ *{home}*  vs  *{away}*\n"

    text += f"\n{SEP}\n"
    if finished:
        text += f"⚽ {goals} goals · {finished}/{len(events)} finished\n"
    text += f"_🟢 Home  🔴 Away  🟡 Draw_\n_Updated: {now_str}_\n_Tap 🔄 Refresh for latest scores_"

    for i, chunk in enumerate(_chunks(text)):
        if i == 0: await q.edit_message_text(chunk, parse_mode="Markdown", reply_markup=back)
        else:      await q.message.reply_text(chunk, parse_mode="Markdown")

# ─── STANDINGS (direct from menu) ────────────────────────────────────────────
async def cb_standings(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query; await q.answer()
    if not _can_access(q.from_user.id, c.bot_data): return
    parts = q.data.split(":")
    lid   = int(parts[1])

    if len(parts) == 2:
        await q.edit_message_text(f"⏳ Loading seasons…")
        async with httpx.AsyncClient() as cl:
            seasons = await fetch_all_seasons(cl)
        if not seasons:
            await q.edit_message_text("❌ Could not load seasons.", reply_markup=back_kb("menu:standings")); return
        btns = [[InlineKeyboardButton(f"📅 {s['name']}", callback_data=f"standings:{lid}:{s['id']}")] for s in seasons[:20]]
        btns.append([InlineKeyboardButton("« Back", callback_data="menu:standings")])
        await q.edit_message_text(
            f"📊 *{ld(lid)} Standings*\n\nSelect a season:",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(btns),
        )
        return

    sid = parts[2]
    await q.edit_message_text(f"⏳ Loading standings…")
    async with httpx.AsyncClient() as cl:
        rows = await fetch_standings(cl, sid, lid)

    back = InlineKeyboardMarkup([[
        InlineKeyboardButton("« Seasons",  callback_data=f"standings:{lid}"),
        InlineKeyboardButton("« Leagues",  callback_data="menu:standings"),
    ]])

    if not rows:
        await q.edit_message_text(
            f"⚠️ *{ld(lid)}* — No standings for season #{sid}.\n_Data loads as matches are played._",
            parse_mode="Markdown", reply_markup=back,
        ); return

    text  = f"📊 *{ld(lid)}*\n_Season #{sid}_\n{SEP}\n"
    text += "`#  Club   Pts  W  D  L  Goals  GD`\n"
    text += f"{SEP}\n"
    for r in rows:
        gd  = f"{'+' if r['gd']>0 else ''}{r['gd']}"
        gl  = f"{r['gf']}:{r['ga']}"
        frm = _form_icons(r["form"])
        text += f"`{r['pos']:>2}. {r['name']:<5} {r['pts']:>3} {r['w']:>2} {r['d']:>2} {r['l']:>2} {gl:<6} {gd:>4}`  {frm}\n"
    text += f"\n_{len(rows)} clubs_"

    for i, chunk in enumerate(_chunks(text)):
        if i == 0: await q.edit_message_text(chunk, parse_mode="Markdown", reply_markup=back)
        else:      await q.message.reply_text(chunk, parse_mode="Markdown")

# ─── ADMIN COMMANDS ───────────────────────────────────────────────────────────
async def cmd_showstatus(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id): return
    acc   = _access(c.bot_data)
    users = acc.get("users", {})
    now   = time.time()
    lines = [f"👑 *Admin Status*", f"Users: {len(users)}", f"Channels: {len(acc.get('allowed_channels',set()))}", ""]
    for uid_s, info in users.items():
        rem = max(0,(info["expire_ts"]-now)/86400)
        lines.append(f"  `{uid_s}`: {rem:.1f}d left")
    await u.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ─── BRAIN STATUS ─────────────────────────────────────────────────────────────
def _is_correct(p: dict) -> bool:
    pick, result = p.get("pick"), p.get("result")
    return (
        (pick == "HOME WIN" and result == "HOME") or
        (pick == "AWAY WIN" and result == "AWAY") or
        (pick == "DRAW"     and result == "DRAW") or
        (pick == "1X"       and result in ("HOME", "DRAW")) or
        (pick == "X2"       and result in ("AWAY", "DRAW"))
    )


async def cmd_brainstatus(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /brainstatus — Full learning engine health report.
    Shows experience, overall accuracy, per-league breakdown,
    per-pick breakdown, adaptive threshold state, and trend.
    """
    if not _can_access(u.effective_user.id, c.bot_data):
        await u.message.reply_text("🔒 Access restricted.")
        return

    total     = len(prediction_memory)
    resolved  = [p for p in prediction_memory if p.get("result") is not None]
    pending   = [p for p in prediction_memory if p.get("result") is None]
    correct   = [p for p in resolved if _is_correct(p)]
    n_res     = len(resolved)
    n_cor     = len(correct)
    overall   = n_cor / n_res * 100 if n_res else 0.0

    # ── Experience level ──────────────────────────────────────────────────────
    if total < 20:
        xp_label = "🌱 Beginner   — still learning"
    elif total < 60:
        xp_label = "📘 Developing — gaining patterns"
    elif total < 120:
        xp_label = "🧠 Experienced — model stabilising"
    elif total < 250:
        xp_label = "🔥 Advanced   — high reliability"
    else:
        xp_label = "⚡ Expert     — fully trained"

    # ── Trend: last 10 vs previous 10 ────────────────────────────────────────
    trend_str = "—"
    if n_res >= 20:
        last10 = resolved[-10:]
        prev10 = resolved[-20:-10]
        acc_last = sum(1 for p in last10 if _is_correct(p)) / 10 * 100
        acc_prev = sum(1 for p in prev10 if _is_correct(p)) / 10 * 100
        diff = acc_last - acc_prev
        if diff > 3:
            trend_str = f"📈 +{diff:.1f}% (improving)"
        elif diff < -3:
            trend_str = f"📉 {diff:.1f}% (declining)"
        else:
            trend_str = f"➡️ stable ({acc_last:.0f}% last 10)"

    # ── Per-pick accuracy ─────────────────────────────────────────────────────
    pick_lines = []
    for pick, stats in strategy_stats.items():
        w, l = stats["wins"], stats["losses"]
        t = w + l
        if t == 0:
            continue
        acc = w / t * 100
        bar = "🟩" * round(acc / 10) + "⬜" * (10 - round(acc / 10))
        pick_lines.append(f"  {_pick_emoji(pick)} *{pick}*: `{w}/{t}` ({acc:.0f}%)\n  {bar}")

    # ── Per-league accuracy ───────────────────────────────────────────────────
    league_stats: dict[int, dict] = {}
    for p in resolved:
        lid = p.get("league_id")
        if lid not in LEAGUES:
            continue
        if lid not in league_stats:
            league_stats[lid] = {"w": 0, "t": 0}
        league_stats[lid]["t"] += 1
        if _is_correct(p):
            league_stats[lid]["w"] += 1

    league_lines = []
    sorted_leagues = sorted(
        league_stats.items(),
        key=lambda x: x[1]["w"] / x[1]["t"] if x[1]["t"] else 0,
        reverse=True,
    )
    for lid, ls in sorted_leagues:
        if ls["t"] == 0:
            continue
        linfo = LEAGUES[lid]
        acc   = ls["w"] / ls["t"] * 100
        bar   = "🟩" * round(acc / 10) + "⬜" * (10 - round(acc / 10))
        medal = ""
        if acc >= 75:   medal = " 🥇"
        elif acc >= 60: medal = " 🥈"
        elif acc >= 50: medal = " 🥉"
        league_lines.append(
            f"  {linfo['flag']} *{linfo['name']}*{medal}: `{ls['w']}/{ls['t']}` ({acc:.0f}%)\n  {bar}"
        )

    # ── Adaptive thresholds ───────────────────────────────────────────────────
    dom = thresholds["dominance"]
    drw = thresholds["draw"]
    dom_bar = "🟩" * round(dom * 10) + "⬜" * (10 - round(dom * 10))
    drw_bar = "🟩" * round(drw * 10) + "⬜" * (10 - round(drw * 10))

    # ── Assemble message ──────────────────────────────────────────────────────
    SEP2 = "━" * 26
    lines = [
        f"🧠 *Brain Status Report*",
        f"{SEP2}",
        f"",
        f"*Experience*",
        f"  {xp_label}",
        f"  Total predictions : `{total}`",
        f"  Resolved          : `{n_res}`",
        f"  Pending           : `{len(pending)}`",
        f"  Correct           : `{n_cor}`",
        f"",
        f"*Overall Accuracy*",
        f"  `{overall:.1f}%`  {'🟩' * round(overall/10)}{'⬜' * (10 - round(overall/10))}",
        f"  Trend: {trend_str}",
        f"",
        f"*Pick Accuracy*",
    ]
    if pick_lines:
        lines += pick_lines
    else:
        lines.append("  _No resolved picks yet_")

    lines += ["", f"*League Accuracy*"]
    if league_lines:
        lines += league_lines
    else:
        lines.append("  _No league data yet_")

    lines += [
        "",
        f"*Adaptive Thresholds*",
        f"  Dominance threshold : `{dom:.2f}`",
        f"  {dom_bar}",
        f"  Draw threshold      : `{drw:.2f}`",
        f"  {drw_bar}",
        f"",
        f"_{total} predictions tracked · thresholds auto-adjust every round_",
    ]

    text = "\n".join(lines)
    for chunk in _chunks(text):
        await u.message.reply_text(chunk, parse_mode="Markdown")


# ─── BACKUP & RESTORE ─────────────────────────────────────────────────────────
async def cmd_backupbrain(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /backupbrain — Send predictions.json as a file download.
    Admin only. Safe to download and re-upload on a new deploy.
    """
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.")
        return

    if not prediction_memory and not any(v["wins"] + v["losses"] for v in strategy_stats.values()):
        await u.message.reply_text("⚠️ No brain data to back up yet.")
        return

    payload = json.dumps({
        "predictions":    prediction_memory,
        "strategy_stats": strategy_stats,
        "thresholds":     thresholds,
        "posted_rounds":  list(_posted_rounds),
        "exported_at":    datetime.datetime.utcnow().isoformat() + "Z",
    }, indent=2)

    fname = f"brain_backup_{int(time.time())}.json"
    await u.message.reply_document(
        document=io.BytesIO(payload.encode()),
        filename=fname,
        caption=(
            f"🧠 *Brain Backup*\n"
            f"`{len(prediction_memory)}` predictions · "
            f"thresholds: dom=`{thresholds['dominance']:.2f}` draw=`{thresholds['draw']:.2f}`\n\n"
            f"To restore: send this file back to the bot with `/restorebrain`"
        ),
        parse_mode="Markdown",
    )


async def cmd_restorebrain(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /restorebrain — Tell the bot you're about to send a backup file.
    The next document you send will be processed as a restore.
    """
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.")
        return
    c.user_data["awaiting_brain_restore"] = True
    await u.message.reply_text(
        "📂 *Ready to restore*\n\nSend the `brain_backup_*.json` file now.",
        parse_mode="Markdown",
    )


async def handle_brain_restore(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    Document handler: if admin flagged a restore, accept the next JSON file
    and restore predictions + stats + thresholds from it.
    """
    if not _is_admin(u.effective_user.id):
        return
    if not c.user_data.get("awaiting_brain_restore"):
        return

    doc = u.message.document
    if not doc or not doc.file_name.endswith(".json"):
        await u.message.reply_text("⚠️ Please send a `.json` brain backup file.")
        return

    c.user_data["awaiting_brain_restore"] = False

    try:
        tg_file = await doc.get_file()
        raw = await tg_file.download_as_bytearray()
        data = json.loads(raw.decode())
    except Exception as e:
        await u.message.reply_text(f"❌ Could not read file: {e}")
        return

    global prediction_memory, strategy_stats, thresholds, _posted_rounds

    prediction_memory = data.get("predictions", [])
    for k, v in data.get("strategy_stats", {}).items():
        if k in strategy_stats:
            strategy_stats[k] = v
    for k, v in data.get("thresholds", {}).items():
        if k in thresholds:
            thresholds[k] = float(v)
    _posted_rounds = set(data.get("posted_rounds", []))

    _save_predictions()

    n   = len(prediction_memory)
    res = len([p for p in prediction_memory if p.get("result") is not None])
    cor = len([p for p in prediction_memory if p.get("result") is not None and _is_correct(p)])
    acc = cor / res * 100 if res else 0.0

    await u.message.reply_text(
        f"✅ *Brain restored successfully!*\n\n"
        f"  Predictions loaded : `{n}`\n"
        f"  Resolved           : `{res}`\n"
        f"  Overall accuracy   : `{acc:.1f}%`\n"
        f"  Dominance threshold: `{thresholds['dominance']:.2f}`\n"
        f"  Draw threshold     : `{thresholds['draw']:.2f}`",
        parse_mode="Markdown",
    )


# ─── AUTO-BROADCAST: PICK SUMMARY MESSAGE ────────────────────────────────────
def _confidence_bar(pct: int) -> str:
    filled = round(pct / 10)
    return "🟩" * filled + "⬜" * (10 - filled)


def _pick_emoji(pick: str) -> str:
    return {"HOME WIN": "🏠", "AWAY WIN": "✈️", "DRAW": "🟡", "1X": "🔒", "X2": "🔒"}.get(pick, "🎯")


def _accuracy_report() -> str:
    lines = ["📊 *Strategy Accuracy (all-time)*"]
    total_preds = len([p for p in prediction_memory if p.get("result")])
    total_resolved = len([p for p in prediction_memory if p.get("result") is not None])
    if not total_resolved:
        return ""
    for pick, stats in strategy_stats.items():
        t = stats["wins"] + stats["losses"]
        if t == 0:
            continue
        acc = stats["wins"] / t * 100
        lines.append(f"  {_pick_emoji(pick)} *{pick}*: `{stats['wins']}/{t}` ({acc:.0f}%)")
    lines.append(f"\n_Total resolved: {total_resolved} predictions_")
    return "\n".join(lines)


async def auto_post_upcoming(app) -> bool:
    """
    Check for a new upcoming round. If it hasn't been posted yet:
      • One message per league, only matches with a confident non-draw pick.
      • Shows only 1X2 and DC market odds for the picked outcome.
      • Card format: Season / Matchday header, one row per qualifying match.
    Returns True if a new round was posted.
    """
    async with httpx.AsyncClient() as cl:
        round_id, round_name, season_id = await _find_upcoming_round(cl)

    if not round_id or round_id in _posted_rounds:
        return False

    log.info(f"auto_post_upcoming: new round detected → {round_id} ({round_name}) season={season_id}")

    # ── Collect destination chats ─────────────────────────────────────────────
    destinations: set[str] = set()
    if CHANNEL_ID:
        destinations.add(str(CHANNEL_ID))
    # Always include the admin's private chat so predictions are never silently dropped
    if ADMIN_ID:
        destinations.add(str(ADMIN_ID))
    for cid in app.bot_data.get("auto_chats", set()):
        destinations.add(str(cid))

    if not destinations:
        log.warning("auto_post_upcoming: no destinations configured (set ADMIN_ID or CHANNEL_ID env vars).")
        _posted_rounds.add(round_id)
        _save_predictions()
        return True

    # ── Build and send one message per league ─────────────────────────────────
    any_sent = False
    async with httpx.AsyncClient() as cl:
        for lid, linfo in LEAGUES.items():
            events = await fetch_events(cl, round_id, PAGE_UPCOMING)
            events = _filter_by_league(events, lid)
            if not events:
                continue

            fixture_teams = [_get_teams(ev) for ev in events]
            h2h_list = []
            for h, a in fixture_teams:
                h2h_list.append(await fetch_h2h_all_seasons(cl, h, a, lid))

            # ── Build match rows — skip non-picks and draws ───────────────────
            match_rows: list[str] = []
            for ev, h2h in zip(events, h2h_list):
                home, away = _get_teams(ev)
                mkts = _extract_markets(ev)
                ox   = mkts.get("1X2") or {}
                dc   = mkts.get("DC")  or {}

                pick_result = evaluate_h2h_pick(home, away, h2h)

                # Skip: no pick, or pick is DRAW (we never show draws)
                if not pick_result or pick_result["pick"] == "DRAW":
                    continue

                pick = pick_result["pick"]
                conf = pick_result["confidence"]

                # Resolve which market odds to show for this pick
                # 1X2 side: HOME WIN → "1", AWAY WIN → "2", 1X → "1", X2 → "2"
                # DC side:  1X → "1X", X2 → "X2", HOME WIN → "12", AWAY WIN → "12"
                if pick == "HOME WIN":
                    market_label = "1X2"
                    odds_str = f"1  {ox.get('1', '—')}"
                elif pick == "AWAY WIN":
                    market_label = "1X2"
                    odds_str = f"2  {ox.get('2', '—')}"
                elif pick == "1X":
                    market_label = "DC"
                    odds_str = f"1X  {dc.get('1X', '—')}"
                elif pick == "X2":
                    market_label = "DC"
                    odds_str = f"X2  {dc.get('X2', '—')}"
                else:
                    continue   # safety — should not happen

                bar = _confidence_bar(conf)
                row = (
                    f"┆ ⚽ *{home}* vs *{away}*\n"
                    f"┆ 🏆 {market_label}  `{odds_str}`\n"
                    f"┆ 🎯 *PICK: {pick}* ({conf}% conf)\n"
                    f"┆ {bar}"
                )
                match_rows.append(row)
                _store_prediction(home, away, lid, round_id, pick, conf)

                # ── Store into app_predictions for the app history/preview ────
                _app_preds = app.bot_data.setdefault("app_predictions", [])
                _app_preds.append({
                    "homeTeam":     home,
                    "awayTeam":     away,
                    "league":       LEAGUES[lid]["name"],
                    "leagueFlag":   LEAGUES[lid]["flag"],
                    "season":       str(season_id),
                    "matchday":     str(round_name),
                    "tip":          pick,
                    "confidence":   f"{conf}%",
                    "overallLabel": pick,
                    "odds":         odds_str,
                    "rawText":      row,
                    "timestamp":    int(time.time()),
                    "messageId":    0,
                    "result":       "PENDING",
                    "score":        "",
                    "_lid":         lid,
                    "_rid":         str(round_id),
                })
                if len(_app_preds) > 200:
                    app.bot_data["app_predictions"] = _app_preds[-200:]

            if not match_rows:
                continue   # no qualifying picks for this league — send nothing

            # ── Compose the league card ───────────────────────────────────────
            header = (
                f"{linfo['flag']} *{linfo['name']}*\n"
                f"Season `{season_id}` / Matchday `{round_name}`\n"
                f"{SEP}"
            )
            body = f"\n{SEP}\n".join(match_rows)
            card = f"{header}\n\n{body}"

            # Send league card (split if too long)
            for chunk in _chunks(card):
                for chat_id in destinations:
                    try:
                        await app.bot.send_message(chat_id=chat_id, text=chunk, parse_mode="Markdown")
                    except Exception as e:
                        log.error(f"auto_post send_message to {chat_id} failed: {e}")
            any_sent = True

    _posted_rounds.add(round_id)
    _save_predictions()

    # ── Maybe add one free pick for this round ────────────────────────────────
    # Runs after predictions are stored so _rid fields are available.
    _free_preview_maybe_add_pick(app.bot_data, round_id, round_name)

    log.info(f"auto_post_upcoming: round {round_id} posted to {len(destinations)} destination(s).")
    return any_sent


async def auto_update_results(app):
    """
    After each round finishes, scan PAGE_MATCHUPS for the most recent played rounds,
    update any pending predictions, evaluate performance, and adjust thresholds.

    Waits 90 seconds after the round's tradingTime.end before fetching, so all
    full-time scores are finalised before we read them. If scores are still missing
    (round is still live), we skip and retry next cycle — _round_cache won't cache
    incomplete results so the retry always hits the API fresh.
    """
    pending = [p for p in prediction_memory if p.get("result") is None]
    if not pending:
        return

    log.info(f"auto_update_results: {len(pending)} unresolved predictions — checking matchups…")

    now_s  = time.time()
    now_ms = int(now_s * 1000)
    RESULT_DELAY_MS = 90_000  # wait 90 s after round ends before fetching FT scores

    async with httpx.AsyncClient() as cl:
        all_seasons = await fetch_all_seasons(cl)

        # Build a lookup: round_id -> bettingClosesTime (ms) from season data
        round_end_ms: dict[str, int] = {}
        for season in all_seasons:
            rws = season.get("realWorldMatchSeconds") or season.get("matchSeconds") or 0
            for rnd in season.get("rounds", []):
                rid = str(rnd.get("id", "")).strip()
                if not rid: continue
                tt = rnd.get("tradingTime") or {}
                end_ms = _iso_to_ms(tt.get("end") or tt.get("start") or 0) or _start_ms(rnd)
                # Add the real-world match duration so we know when the last whistle blows
                round_end_ms[rid] = end_ms + int(rws * 1000)

        # Gather all finished round IDs that contain pending predictions
        pending_rids = {p["round_id"] for p in pending}

        for season in all_seasons:
            for rnd in season.get("rounds", []):
                rid = str(rnd.get("id","")).strip()
                if rid not in pending_rids:
                    continue

                # Round hasn't started yet — skip
                if _start_ms(rnd) >= now_ms:
                    continue

                # Round has ended but we haven't waited 90 s after full-time yet — skip
                ft_deadline_ms = round_end_ms.get(rid, 0) + RESULT_DELAY_MS
                if now_ms < ft_deadline_ms:
                    wait_s = (ft_deadline_ms - now_ms) / 1000
                    log.info(f"auto_update_results: round {rid} — waiting {wait_s:.0f}s more for FT scores to settle.")
                    continue

                log.info(f"auto_update_results: fetching FT results for round {rid} (90s grace passed).")
                events = await fetch_events(cl, rid, PAGE_MATCHUPS)
                finished = []
                for ev in events:
                    h, a = _get_teams(ev)
                    hs, as_, hht, aht = _get_score(ev)
                    if hs is not None and as_ is not None:
                        finished.append({"home": h, "away": a, "hs": hs, "as_": as_})
                        log.info(f"  score: {h} {hs}-{as_} {a}  (HT: {hht}-{aht})")
                    else:
                        log.warning(f"  NO SCORE: {h} vs {a} — raw ev keys: { {k:ev[k] for k in ev if any(x in k.lower() for x in ('score','result','home','away','ht','ft'))} }")

                if finished:
                    log.info(f"auto_update_results: round {rid} — {len(finished)} scores found, updating predictions.")
                    update_results(finished, bot_data=app.bot_data)
                else:
                    log.warning(f"auto_update_results: round {rid} — no scores yet after 90s wait. Will retry next cycle.")

    evaluate_performance()
    adjust_thresholds()
    log.info("auto_update_results: done.")


async def _auto_loop(app):
    """
    Background task:
      • Every 30 s: check for a new upcoming round → post picks if new.
      • Every 60 s: resolve pending predictions from finished rounds and adapt thresholds.
        (auto_update_results internally waits 90 s after round FT before fetching scores)
    """
    result_check_interval = 60    # seconds — check frequently; 90s delay is inside auto_update_results
    last_result_check     = 0.0

    while True:
        try:
            await auto_post_upcoming(app)
        except Exception as e:
            log.error(f"_auto_loop post error: {e}")

        try:
            if time.time() - last_result_check >= result_check_interval:
                await auto_update_results(app)
                last_result_check = time.time()
        except Exception as e:
            log.error(f"_auto_loop result-update error: {e}")

        await asyncio.sleep(30)


# ─── APP ADMIN COMMANDS ───────────────────────────────────────────────────────

async def cmd_appgencode(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Admin: /appgencode <days> — generate a one-time activation code."""
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only."); return
    try:
        days = int(c.args[0]) if c.args else 30
        if not 1 <= days <= 3650: raise ValueError
    except (ValueError, IndexError):
        await u.message.reply_text("❌ Usage: `/appgencode <days>`\nExample: `/appgencode 30`", parse_mode="Markdown"); return
    code = _app_gen_code()
    app  = _app_data(c.bot_data)
    app["codes"][code] = {"days": days, "used": False, "created_at": int(time.time()), "fingerprint": None, "user_id": None}
    _fb_cloud_push(c.bot_data)
    await u.message.reply_text(
        f"✅ *Activation Code Generated*\n\n`{code}`\n\n"
        f"⏰ Valid for *{days} day{'s' if days != 1 else ''}* upon first use\n"
        f"🔑 One-time use — locked to device on activation\n\nShare this code with the user.",
        parse_mode="Markdown")
    log.info(f"📱 App code generated: {code} ({days} days)")


async def cmd_appsetmsg(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Admin: /appsetmsg <text> — set the purchase message shown to locked users."""
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only."); return
    text = (u.message.text or "")
    idx  = text.find(" ")
    if idx < 0 or not text[idx:].strip():
        await u.message.reply_text(
            "❌ Usage: `/appsetmsg <your message>`\n\nExample:\n`/appsetmsg Contact @MrSimTech on Telegram to purchase access.`",
            parse_mode="Markdown"); return
    msg = text[idx:].strip()
    _app_data(c.bot_data)["purchase_message"] = msg
    await u.message.reply_text(f"✅ *Purchase message updated.*\n\nLocked users in the app will now see:\n\n_{msg}_", parse_mode="Markdown")


async def _do_appusers(reply_fn, c):
    """Core appusers logic — works from both command and button."""
    app     = _app_data(c.bot_data)
    devices = app.get("devices", {})
    codes   = app.get("codes",   {})
    now_ts  = int(time.time())

    if not devices:
        await reply_fn("📱 *App Users*\n\nNo devices registered yet.\n\nUse `/appgencode <days>` to create an activation code.", parse_mode="Markdown"); return

    activated_online, activated_offline = [], []
    expired_online,   expired_offline   = [], []
    not_activated_online, not_activated_offline = [], []

    for fp, rec in devices.items():
        exp_ts  = rec.get("expire_ts", 0)
        expired = exp_ts > 0 and now_ts > exp_ts
        is_on   = _is_device_online(fp, c.bot_data)
        entry   = (fp, rec)
        if not expired:
            if is_on: activated_online.append(entry)
            else:     activated_offline.append(entry)
        else:
            if is_on: expired_online.append(entry)
            else:     expired_offline.append(entry)

    guest_hb  = c.bot_data.get("app_guest_heartbeats", {})
    threshold = now_ts - ONLINE_TIMEOUT_SECS
    for fp, ts in guest_hb.items():
        if fp in devices: continue
        is_on = ts >= threshold
        entry = (fp, {"username": "Not Activated", "expire_ts": 0, "activated": ts})
        if is_on: not_activated_online.append(entry)
        else:     not_activated_offline.append(entry)

    def _fmt(fp, rec, is_on):
        exp_ts    = rec.get("expire_ts", 0)
        days_left = max(0, int((exp_ts - now_ts) / 86400)) if exp_ts > 0 else 0
        expired   = exp_ts > 0 and now_ts > exp_ts
        fp_short  = fp[:8].upper() + "…"
        sub_status = "❌ EXPIRED" if expired else f"✅ {days_left}d left"
        online_icon = "🟢 Online" if is_on else f"⚫ {_last_seen_str(fp, c.bot_data)}"
        return f"• `{fp_short}` — *{rec.get('username','?')}* — {sub_status} — {online_icon}"

    _threshold      = now_ts - ONLINE_TIMEOUT_SECS
    _act_fps        = set(c.bot_data.get("app_heartbeats", {}).keys())
    _act_online_cnt = sum(1 for ts in c.bot_data.get("app_heartbeats", {}).values() if ts >= _threshold)
    _guest_online_cnt = sum(1 for fp, ts in c.bot_data.get("app_guest_heartbeats", {}).items() if ts >= _threshold and fp not in _act_fps)
    _total_online = _act_online_cnt + _guest_online_cnt

    lines = [
        f"📱 *App Users Overview*\n",
        f"👥 *Total Online Now: {_total_online}*  (✅ {_act_online_cnt} activated  +  👁 {_guest_online_cnt} on login screen)",
        f"",
        f"🟢 Activated Online:     *{len(activated_online)}*  |  ⚫ Activated Offline:     *{len(activated_offline)}*",
        f"⏰ Expired Online:       *{len(expired_online)}*  |  💀 Expired Offline:       *{len(expired_offline)}*",
        f"👁 Not Activated Online: *{len(not_activated_online)}*  |  🔇 Not Activated Offline: *{len(not_activated_offline)}*\n",
    ]
    if activated_online:
        lines.append("─── 🟢 *Activated & Online* ───")
        for fp, rec in sorted(activated_online, key=lambda x: -x[1].get("activated", 0)):
            lines.append(_fmt(fp, rec, True))
        lines.append("")
    if not_activated_online:
        lines.append("─── 👁 *Not Activated & Online (Login Screen)* ───")
        for fp, rec in sorted(not_activated_online, key=lambda x: -(c.bot_data.get("app_guest_heartbeats",{}).get(x[0],0))):
            lines.append(_fmt(fp, rec, True))
        lines.append("")
    if expired_online:
        lines.append("─── ⏰ *Expired & Online* ───")
        for fp, rec in sorted(expired_online, key=lambda x: -x[1].get("activated", 0)):
            lines.append(_fmt(fp, rec, True))
        lines.append("")
    if activated_offline:
        lines.append("─── ⚫ *Activated & Offline* ───")
        for fp, rec in sorted(activated_offline, key=lambda x: -(c.bot_data.get("app_heartbeats",{}).get(x[0],0))):
            lines.append(_fmt(fp, rec, False))
        lines.append("")
    if expired_offline:
        lines.append("─── 💀 *Expired & Offline* ───")
        for fp, rec in sorted(expired_offline, key=lambda x: -(c.bot_data.get("app_heartbeats",{}).get(x[0],0)))[:10]:
            lines.append(_fmt(fp, rec, False))
        lines.append("")
    used   = sum(1 for x in codes.values() if x.get("used"))
    unused = sum(1 for x in codes.values() if not x.get("used"))
    lines.append(f"🔑 Codes: {used} used / {unused} unused")
    await reply_fn("\n".join(lines), parse_mode="Markdown")




async def cmd_apptamperhistory(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Admin: /apptamperhistory <WIN|LOSS> <homeTeam> v <awayTeam> <tip> — inject a result card into public history."""
    if not _is_admin(u.effective_user.id): await u.message.reply_text("🔒 Admin only."); return
    args = c.args or []
    if len(args) < 4:
        await u.message.reply_text(
            "Usage: `/apptamperhistory WIN ManUtd v Chelsea DC 1X`\n\n"
            "Arguments: `<WIN|LOSS> <homeTeam> v <awayTeam> <tip...>`\n"
            "Example: `/apptamperhistory WIN Arsenal v Liverpool Over 2.5`",
            parse_mode="Markdown")
        return
    result_str = args[0].upper()
    if result_str not in ("WIN", "LOSS"):
        await u.message.reply_text("❌ Result must be WIN or LOSS."); return
    # Find 'v' separator between teams
    try:
        v_idx = [a.lower() for a in args[1:]].index("v") + 1
    except ValueError:
        await u.message.reply_text("❌ Missing 'v' separator between teams. E.g. `ManUtd v Chelsea`."); return
    home = " ".join(args[1:v_idx])
    remainder = args[v_idx + 1:]
    # Everything after second team that looks like a tip
    away_and_tip = remainder
    # Heuristic: if last word(s) look like a tip keyword, split there
    away = away_and_tip[0] if away_and_tip else "Unknown"
    tip  = " ".join(away_and_tip[1:]) if len(away_and_tip) > 1 else "Pick"

    import time as _time
    card = {
        "homeTeam":    home,
        "awayTeam":    away,
        "tip":         tip,
        "result":      result_str,
        "timestamp":   int(_time.time()),
        "confidence":  "",
        "odds":        "",
        "league":      "Admin Pick",
        "leagueFlag":  "🛡",
        "overallLabel": "",
        "score":       "",
    }
    bot_data = c.bot_data
    pub_hist = bot_data.setdefault("app_public_history", [])
    pub_hist.insert(0, card)
    # Keep only last 100
    if len(pub_hist) > 100:
        bot_data["app_public_history"] = pub_hist[:100]
    icon = "✅" if result_str == "WIN" else "❌"
    await u.message.reply_text(
        f"{icon} Injected into public history:\n"
        f"*{home} v {away}* — `{tip}` — *{result_str}*",
        parse_mode="Markdown")

async def cmd_appusers(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id): await u.message.reply_text("🔒 Admin only."); return
    await _do_appusers(u.message.reply_text, c)


async def cmd_allusers(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Admin: /allusers — total overview of every user."""
    if not _is_admin(u.effective_user.id): return
    app     = _app_data(c.bot_data)
    devices = app.get("devices", {})
    codes   = app.get("codes",   {})
    now_ts  = int(time.time())
    hb      = c.bot_data.get("app_heartbeats", {})
    total        = len(devices)
    active       = sum(1 for r in devices.values() if r.get("expire_ts", 0) > now_ts)
    expired_cnt  = total - active
    online_cnt   = bot_data_online_count(c.bot_data)
    codes_used   = sum(1 for x in codes.values() if x.get("used"))
    codes_unused = sum(1 for x in codes.values() if not x.get("used"))
    guest_hb     = c.bot_data.get("app_guest_heartbeats", {})
    threshold    = now_ts - ONLINE_TIMEOUT_SECS
    guests_total = sum(1 for fp in guest_hb if fp not in devices)
    guests_online= sum(1 for fp, ts in guest_hb.items() if fp not in devices and ts >= threshold)
    last_seen_fp = max(hb, key=hb.get) if hb else None
    last_seen_info = ""
    if last_seen_fp:
        uname = devices.get(last_seen_fp, {}).get("username", "unknown")
        last_seen_info = f"\n🕒 Last active: *{uname}* ({_last_seen_str(last_seen_fp, c.bot_data)})"
    text = (
        f"📊 *All App Users — Total Overview*\n{'━'*26}\n"
        f"👥 Total activated devices: *{total}*\n"
        f"✅ Active (not expired):    *{active}*\n"
        f"❌ Expired:                 *{expired_cnt}*\n{'━'*26}\n"
        f"🌐 *Total online now:       {online_cnt + guests_online}*\n"
        f"   🟢 Activated online:     *{online_cnt}*\n"
        f"   👁 On login screen:      *{guests_online}*  (of {guests_total} total guests)\n"
        f"🔑 Codes used: {codes_used}  |  Unused: {codes_unused}"
        f"{last_seen_info}\n{'━'*26}\n"
        f"_Use /appusers for full details per device_"
    )
    await u.message.reply_text(text, parse_mode="Markdown")


# ─── APP HTTP SERVER ──────────────────────────────────────────────────────────
_tg_app = None   # set in main() after Application is built

def _http_json(data: dict, status: int = 200) -> _aiohttp_web.Response:
    return _aiohttp_web.Response(text=json.dumps(data), status=status, content_type="application/json")

def _http_check_key(request: _aiohttp_web.Request) -> bool:
    if not APP_API_KEY: return True
    return request.headers.get("x-app-key", "") == APP_API_KEY


async def _http_app_check(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appCheck { "fingerprint": "..." }"""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: return _http_json({"status": "NETWORK_ERROR"}, 400)
    fingerprint = (body.get("fingerprint") or "").strip()
    if not fingerprint: return _http_json({"status": "NOT_FOUND"})
    bot_data = _tg_app.bot_data
    app      = _app_data(bot_data)
    devices  = app["devices"]
    now_ts   = int(time.time())
    rec = devices.get(fingerprint)
    if not rec and _fb_db:
        fb_rec = _fb_get_by_fingerprint(fingerprint)
        if fb_rec:
            uid = fb_rec.get("userId",""); exp_ts = fb_rec.get("expireTs",0)
            devices[fingerprint] = {"user_id": uid, "username": fb_rec.get("username","User"),
                "expire_ts": exp_ts, "days": fb_rec.get("days",30),
                "code_used": fb_rec.get("code",""), "activated": fb_rec.get("activatedAt",now_ts)}
            rec = devices[fingerprint]
            log.info(f"♻️  Reinstall recovery via Firebase: fp={fingerprint[:8]}")
    if not rec: return _http_json({"status": "NOT_FOUND"})
    if rec.get("removed"):
        return _http_json({"status": "REMOVED"})  # admin removed this device
    uid = rec.get("user_id",""); exp_ts = rec.get("expire_ts",0)
    days_left = max(0, int((exp_ts - now_ts) / 86400)) if exp_ts > 0 else 0
    if exp_ts > 0 and now_ts > exp_ts:
        return _http_json({"status": "EXPIRED", "userId": uid, "username": rec.get("username","User")})
    return _http_json({"status": "ACTIVATED_RETURNING", "userId": uid,
        "expireTs": exp_ts, "daysLeft": days_left, "username": rec.get("username","User")})


async def _http_app_activate(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appActivate { "code": "SIG-...", "fingerprint": "...", "firstName": "..." }"""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: return _http_json({"status": "NETWORK_ERROR"}, 400)
    code        = (body.get("code") or "").strip().upper()
    fingerprint = (body.get("fingerprint") or "").strip()
    first_name  = (body.get("firstName") or "User").strip()
    if not code or not fingerprint: return _http_json({"status": "CODE_INVALID"})
    bot_data = _tg_app.bot_data
    app = _app_data(bot_data); codes = app["codes"]; devices = app["devices"]
    now_ts = int(time.time())
    if code not in codes: return _http_json({"status": "CODE_INVALID"})
    rec = codes[code]
    if rec.get("used"):
        old_fp = rec.get("fingerprint","")
        if old_fp == fingerprint:
            dev = devices.get(fingerprint,{}); uid = dev.get("user_id",rec.get("user_id",""))
            exp_ts = dev.get("expire_ts",0)
            days_left = max(0, int((exp_ts - now_ts) / 86400)) if exp_ts > 0 else 0
            return _http_json({"status":"ACTIVATED_RETURNING","userId":uid,"expireTs":exp_ts,
                "daysLeft":days_left,"username":dev.get("username",first_name),
                "purchaseMessage":app.get("purchase_message","")})
        if _fb_db:
            fb_sub = _fb_get_by_code(code)
            if fb_sub:
                uid = fb_sub.get("userId",f"app_{fingerprint[:8]}"); exp_ts = fb_sub.get("expireTs",0)
                days_left = max(0, int((exp_ts - now_ts) / 86400)) if exp_ts > 0 else 0
                if exp_ts > 0 and now_ts > exp_ts: return _http_json({"status":"EXPIRED"})
                _fb_update_fingerprint(code, fingerprint)
                devices[fingerprint] = {"user_id":uid,"username":fb_sub.get("username",first_name),
                    "expire_ts":exp_ts,"days":fb_sub.get("days",30),"code_used":code,"activated":fb_sub.get("activatedAt",now_ts)}
                rec["fingerprint"] = fingerprint
                return _http_json({"status":"ACTIVATED_RETURNING","userId":uid,"expireTs":exp_ts,
                    "daysLeft":days_left,"username":fb_sub.get("username",first_name),
                    "purchaseMessage":app.get("purchase_message","")})
        return _http_json({"status": "CODE_USED"})
    days = rec.get("days",30); exp_ts = now_ts + days * 86400
    uid  = f"app_{fingerprint[:8]}"
    exp_date = datetime.datetime.fromtimestamp(exp_ts, datetime.timezone.utc).strftime("%d %b %Y")
    codes[code].update({"used":True,"fingerprint":fingerprint,"user_id":uid,"activated_at":now_ts})
    devices[fingerprint] = {"user_id":uid,"username":first_name,"expire_ts":exp_ts,
        "days":days,"code_used":code,"activated":now_ts}
    acc = _access(bot_data)
    acc["users"][uid] = {"expire_ts":exp_ts,"days":days,"added_ts":now_ts,"notified_expire":False,"source":"app"}
    _fb_save_subscription(code, fingerprint, uid, first_name, exp_ts, days)
    _fb_cloud_push(bot_data)
    try:
        if ADMIN_ID and _tg_app:
            fb_status = "☁️ backed up" if _fb_db else "📦 local only"
            await _tg_app.bot.send_message(chat_id=ADMIN_ID,
                text=f"📱 *New App Activation*\n\n👤 User: {first_name} (`{uid}`)\n🔑 Code: `{code}`\n⏰ Days: {days}\n📆 Expires: {exp_date}\n🔒 Device: `{fingerprint[:8].upper()}…`\n💾 Storage: {fb_status}",
                parse_mode="Markdown")
    except Exception as _e: log.warning(f"App activation notify failed: {_e}")
    log.info(f"📱 App activated: uid={uid} code={code} days={days}")
    return _http_json({"status":"ACTIVATED_NEW","userId":uid,"expireTs":exp_ts,"daysLeft":days,
        "username":first_name,"purchaseMessage":app.get("purchase_message","")})


async def _http_app_purchase(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appPurchase — returns purchase message for locked users."""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    msg = _app_data(_tg_app.bot_data).get("purchase_message","")
    return _http_json({"status":"OK","message": msg or "Contact the admin on Telegram to purchase premium signals access."})


async def _http_app_history(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appHistory { "fingerprint": "...", "limit": 50 } — predictions history."""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: return _http_json({"status": "NETWORK_ERROR"}, 400)
    fingerprint = (body.get("fingerprint") or "").strip()
    limit       = int(body.get("limit", 50))
    bot_data    = _tg_app.bot_data
    app         = _app_data(bot_data)
    devices     = app["devices"]
    now_ts      = int(time.time())
    if fingerprint and fingerprint not in devices:
        return _http_json({"status": "NOT_FOUND", "cards": []})
    if fingerprint:
        rec = devices[fingerprint]; exp_ts = rec.get("expire_ts",0)
        if exp_ts > 0 and now_ts > exp_ts:
            return _http_json({"status": "EXPIRED", "cards": []})
    preds = bot_data.get("app_predictions", [])
    cards = [_app_pred_to_card(p) for p in reversed(preds)][:limit]
    return _http_json({"status": "OK", "cards": cards})


async def _http_app_heartbeat(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appHeartbeat { "fingerprint": "..." } — keep device online, return sub status."""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: return _http_json({"status": "NETWORK_ERROR"}, 400)
    fingerprint = (body.get("fingerprint") or "").strip()
    if not fingerprint: return _http_json({"status": "OK"})
    bot_data = _tg_app.bot_data; now_ts = int(time.time())
    hb = bot_data.setdefault("app_heartbeats", {})
    hb[fingerprint] = now_ts
    cutoff = now_ts - 7 * 86400
    bot_data["app_heartbeats"] = {fp: ts for fp, ts in hb.items() if ts > cutoff}
    app = _app_data(bot_data); devices = app.get("devices",{})
    rec = devices.get(fingerprint)
    if not rec: return _http_json({"status": "NOT_FOUND"})
    exp_ts    = rec.get("expire_ts",0)
    days_left = max(0, int((exp_ts - now_ts) / 86400)) if exp_ts > 0 else 0
    is_expired = exp_ts > 0 and now_ts > exp_ts
    return _http_json({"status": "EXPIRED" if is_expired else "ACTIVE",
        "daysLeft": days_left, "expireTs": exp_ts, "username": rec.get("username",""), "serverTs": now_ts})


async def _http_app_guest_heartbeat(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appGuestHeartbeat { "fingerprint": "..." } — track non-activated devices."""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: return _http_json({"status": "NETWORK_ERROR"}, 400)
    fingerprint = (body.get("fingerprint") or "").strip()
    if not fingerprint: return _http_json({"status": "OK"})
    bot_data = _tg_app.bot_data; now_ts = int(time.time())
    guests = bot_data.setdefault("app_guest_heartbeats", {})
    guests[fingerprint] = now_ts
    cutoff = now_ts - 7 * 86400
    bot_data["app_guest_heartbeats"] = {fp: ts for fp, ts in guests.items() if ts > cutoff}
    return _http_json({"status": "OK"})


async def _http_app_online_users(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appOnlineUsers — admin device counts."""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    bot_data  = _tg_app.bot_data; app = _app_data(bot_data)
    devices   = app.get("devices",{}); now_ts = int(time.time())
    threshold = now_ts - ONLINE_TIMEOUT_SECS
    heartbeats = bot_data.get("app_heartbeats",{})
    ao = ao2 = eo = eo2 = 0
    for fp, rec in devices.items():
        exp_ts  = rec.get("expire_ts",0); expired = exp_ts > 0 and now_ts > exp_ts
        is_on   = heartbeats.get(fp,0) >= threshold
        if not expired:
            if is_on: ao += 1
            else:     ao2 += 1
        else:
            if is_on: eo += 1
            else:     eo2 += 1
    return _http_json({"status":"OK","activatedOnline":ao,"activatedOffline":ao2,
        "expiredOnline":eo,"expiredOffline":eo2,"totalDevices":len(devices),"serverTs":now_ts})


def _free_preview_maybe_add_pick(bot_data: dict, round_id: str, round_name: str):
    """
    Called by auto_post_upcoming each time a NEW round is posted.
    Adds one free pick for this round if it is time to do so:
      - Max 5 picks per UTC day.
      - After each pick, skip 2-4 rounds before picking again.
      - Pick 1 random match from 1 random league not already in today's list.
      - The pick is stored by identity key so the endpoint serves it stably.
      - Refreshing never adds picks — only the auto-loop does.
      - Resets at UTC midnight.
    """
    utc_day = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    # Daily reset
    if bot_data.get("fp_day", "") != utc_day:
        bot_data["fp_day"]         = utc_day
        bot_data["fp_keys"]        = []   # "home|away|league" strings, in order added
        bot_data["fp_leagues"]     = []   # league names already picked today
        bot_data["fp_rounds_seen"] = 0    # rounds posted so far today
        bot_data["fp_next_at"]     = 0    # fp_rounds_seen value when next pick is due
        log.info(f"free_preview: daily reset for {utc_day}")

    # Already have 5 picks today
    if len(bot_data.get("fp_keys", [])) >= 5:
        return

    # Count this round
    bot_data["fp_rounds_seen"] = bot_data.get("fp_rounds_seen", 0) + 1
    rounds_seen = bot_data["fp_rounds_seen"]
    next_at     = bot_data.get("fp_next_at", 0)

    # Not yet time for the next pick
    if rounds_seen < next_at:
        log.info(f"free_preview: round {round_name} ({rounds_seen}), next pick at {next_at} — skipping")
        return

    # Find candidates from this round, excluding already-used leagues
    preds        = bot_data.get("app_predictions", [])
    used_leagues = set(bot_data.get("fp_leagues", []))
    candidates   = [
        p for p in preds
        if str(p.get("_rid", "")) == str(round_id)
        and p.get("league", "") not in used_leagues
    ]

    if not candidates:
        # No usable candidate — retry next round
        bot_data["fp_next_at"] = rounds_seen + 1
        log.info(f"free_preview: round {round_name} — no candidates (used: {used_leagues}), retry next round")
        return

    # Pick 1 random league → 1 random match from it
    league_map: dict[str, list] = {}
    for p in candidates:
        league_map.setdefault(p.get("league", ""), []).append(p)

    chosen_league = random.choice(list(league_map.keys()))
    chosen_match  = random.choice(league_map[chosen_league])
    key = (
        f"{chosen_match.get('homeTeam','')}|"
        f"{chosen_match.get('awayTeam','')}|"
        f"{chosen_match.get('league','')}"
    )

    bot_data["fp_keys"].append(key)
    bot_data["fp_leagues"].append(chosen_league)

    # Schedule next pick after skipping 2-4 rounds
    skip = random.randint(2, 4) if len(bot_data["fp_keys"]) < 5 else 0
    bot_data["fp_next_at"] = rounds_seen + 1 + skip

    log.info(
        f"free_preview: pick #{len(bot_data['fp_keys'])} added — {key} "
        f"(round {round_name}, skipping {skip} rounds before next)"
    )


async def _http_app_free_preview(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appFreePreview — returns picks unlocked so far today (0-5).

    Picks are added one at a time by _free_preview_maybe_add_pick via the
    auto-loop as new rounds are posted. Refreshing NEVER adds a new pick —
    it only updates WIN/LOSS/score on already-shown picks.
    """
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    bot_data = _tg_app.bot_data
    preds    = bot_data.get("app_predictions", [])
    today_ts = int(time.time()) - 86400

    # Build lookup for live result sync: identity key -> latest pred dict
    pred_lookup: dict[str, dict] = {}
    for p in preds:
        k = f"{p.get('homeTeam','')}|{p.get('awayTeam','')}|{p.get('league','')}"
        pred_lookup[k] = p

    cards = []
    for key in bot_data.get("fp_keys", []):
        p = pred_lookup.get(key)
        if p:
            cards.append(_app_pred_to_card(p))

    total_today = sum(
        1 for p in preds
        if p.get("result", "PENDING") == "PENDING" and p.get("timestamp", 0) > today_ts
    )

    return _http_json({
        "status":      "OK",
        "cards":       cards,
        "total_today": total_today,
        "pool_size":   len(bot_data.get("fp_keys", [])),
    })


async def _http_app_public_history(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appPublicHistory — WIN picks visible to all users (track record)."""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: body = {}
    limit    = min(int(body.get("limit", 30)), 100)
    bot_data = _tg_app.bot_data
    preds    = bot_data.get("app_predictions", [])
    completed = [p for p in reversed(preds) if p.get("result","PENDING") == "WIN"]
    return _http_json({"status":"OK","cards":[_app_pred_to_card(p) for p in completed[:limit]]})


async def _http_app_get_referral_code(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appGetReferralCode { "fingerprint": "..." } — get or create SGO-XXXX code."""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: return _http_json({"status": "NETWORK_ERROR"}, 400)
    fingerprint = (body.get("fingerprint") or "").strip()
    if not fingerprint: return _http_json({"status": "NOT_FOUND"})
    bot_data = _tg_app.bot_data; app = _app_data(bot_data)
    devices  = app.get("devices",{}); now_ts = int(time.time())
    rec = devices.get(fingerprint)
    if not rec: return _http_json({"status": "NOT_FOUND"})
    if rec.get("expire_ts",0) > 0 and now_ts > rec["expire_ts"]:
        return _http_json({"status": "EXPIRED"})
    referrals = bot_data.setdefault("app_referrals", {})
    entry = referrals.get(fingerprint)
    if not entry:
        import random, string
        chars = string.ascii_uppercase + string.digits
        unique_code = "SGO-" + "".join(random.choices(chars, k=4))
        existing_codes = {v.get("code") for v in referrals.values()}
        while unique_code in existing_codes:
            unique_code = "SGO-" + "".join(random.choices(chars, k=4))
        entry = {"code": unique_code, "referred": [], "rewards": 0}
        referrals[fingerprint] = entry
    return _http_json({"status": "OK", "referralCode": entry["code"]})


async def _http_app_submit_referral(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appSubmitReferral { "fingerprint": "...", "referralCode": "SGO-XXXX" }"""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: return _http_json({"status": "NETWORK_ERROR"}, 400)
    fingerprint   = (body.get("fingerprint")  or "").strip()
    referral_code = (body.get("referralCode") or "").strip().upper()
    if not fingerprint or not referral_code: return _http_json({"status": "CODE_NOT_FOUND"})
    bot_data = _tg_app.bot_data; referrals = bot_data.setdefault("app_referrals",{})
    owner_fp = owner_entry = None
    for fp, entry in referrals.items():
        if entry.get("code") == referral_code:
            owner_fp = fp; owner_entry = entry; break
    if not owner_fp: return _http_json({"status":"CODE_NOT_FOUND","message":"Referral code not found. Ask your friend to open their Status tab first."})
    if owner_fp == fingerprint: return _http_json({"status":"SELF_REFERRAL","message":"You cannot use your own referral code."})
    b_entry = referrals.get(fingerprint,{})
    for ref in b_entry.get("referred",[]):
        if ref.get("fp") == owner_fp: return _http_json({"status":"CIRCULAR_REFERRAL","message":"You cannot refer someone who has referred you."})
    for ref in owner_entry.get("referred",[]):
        if ref.get("fp") == fingerprint: return _http_json({"status":"CIRCULAR_REFERRAL","message":"You cannot refer back the person who referred you."})
    for fp_check, entry_check in referrals.items():
        if fp_check == fingerprint: continue
        for ref in entry_check.get("referred",[]):
            if ref.get("fp") == fingerprint: return _http_json({"status":"ALREADY_REFERRED","message":"You have already been referred by someone else."})
    already = [r for r in owner_entry.get("referred",[]) if r.get("fp") == fingerprint]
    if already: return _http_json({"status":"ALREADY_CONFIRMED","message":"This referral was already confirmed. 1 day was previously added."})
    import random, string
    secret = "REF-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    app = _app_data(bot_data); devices = app.get("devices",{}); now_ts = int(time.time())
    acc = _access(bot_data)
    owner_new_exp = referree_new_exp = 0
    for fp_to_add in [owner_fp, fingerprint]:
        dev_rec = devices.get(fp_to_add, {})
        exp_ts  = dev_rec.get("expire_ts", 0)
        # If never set or already expired, start from now; otherwise extend from current expiry
        base_ts = exp_ts if (exp_ts and exp_ts > now_ts) else now_ts
        new_exp = base_ts + 86400
        dev_rec["expire_ts"] = new_exp
        devices[fp_to_add] = dev_rec          # ensure it's written back
        uid_add = dev_rec.get("user_id", "")
        if uid_add and uid_add in acc["users"]:
            acc["users"][uid_add]["expire_ts"] = new_exp
        log.info(f"Referral +1 day: fp={fp_to_add[:8]} old_exp={exp_ts} new_exp={new_exp} uid={uid_add}")
        if fp_to_add == owner_fp:
            owner_new_exp = new_exp
        else:
            referree_new_exp = new_exp
    owner_entry.setdefault("referred",[]).append({"fp":fingerprint,"secret":secret,"ts":now_ts})
    _fb_cloud_push(bot_data)
    try:
        if ADMIN_ID and _tg_app:
            owner_rec = devices.get(owner_fp,{}); sub_rec = devices.get(fingerprint,{})
            owner_days = max(0, int((owner_new_exp - now_ts) / 86400)) if owner_new_exp else 0
            sub_days   = max(0, int((referree_new_exp - now_ts) / 86400)) if referree_new_exp else 0
            await _tg_app.bot.send_message(chat_id=ADMIN_ID,
                text=(
                    f"🎁 *Referral Confirmed — 1 Day Auto-Added*\n\n"
                    f"👤 Referrer: *{owner_rec.get('username','?')}* (`{owner_fp[:8].upper()}…`) → {owner_days}d left\n"
                    f"👤 Referred: *{sub_rec.get('username','?')}* (`{fingerprint[:8].upper()}…`) → {sub_days}d left\n"
                    f"✅ *+1 day added to both subscriptions automatically.*"
                ),
                parse_mode="Markdown")
    except Exception as _e: log.warning(f"Referral admin notify failed: {_e}")
    # Return the referree's updated subscription info so the app can refresh its view
    referree_days_left = max(0, int((referree_new_exp - now_ts) / 86400)) if referree_new_exp else 0
    return _http_json({
        "status":   "OK",
        "message":  "🎉 Referral confirmed! 1 day has been added to your subscription.",
        "daysLeft": referree_days_left,
        "expireTs": referree_new_exp,
    })


async def _http_app_redeem_referral(request: _aiohttp_web.Request) -> _aiohttp_web.Response:
    """POST /appRedeemReferral { "fingerprint": "...", "activationCode": "R1D-..." }"""
    if not _http_check_key(request): return _http_json({"status": "UNAUTHORIZED"}, 401)
    try:    body = await request.json()
    except: return _http_json({"status": "NETWORK_ERROR"}, 400)
    activation_code = (body.get("activationCode") or "").strip().upper()
    fingerprint     = (body.get("fingerprint")    or "").strip()
    if not activation_code or not fingerprint: return _http_json({"status": "CODE_INVALID"})
    base_code = activation_code[4:] if activation_code.startswith("R1D-") else activation_code
    bot_data = _tg_app.bot_data; app = _app_data(bot_data)
    codes = app["codes"]; devices = app["devices"]; now_ts = int(time.time())
    if base_code not in codes: return _http_json({"status": "CODE_INVALID"})
    rec = codes[base_code]
    if rec.get("used"):
        if rec.get("fingerprint","") == fingerprint:
            dev = devices.get(fingerprint,{}); uid = dev.get("user_id",rec.get("user_id",""))
            exp_ts = dev.get("expire_ts",0)
            days_left = max(0, int((exp_ts - now_ts) / 86400)) if exp_ts > 0 else 0
            return _http_json({"status":"ACTIVATED_RETURNING","userId":uid,"expireTs":exp_ts,"daysLeft":days_left,"username":dev.get("username","User")})
        return _http_json({"status": "CODE_USED"})
    days = rec.get("days",30); exp_ts = now_ts + days * 86400; uid = f"app_{fingerprint[:8]}"
    codes[base_code].update({"used":True,"fingerprint":fingerprint,"user_id":uid,"activated_at":now_ts})
    devices[fingerprint] = {"user_id":uid,"username":"ReferralUser","expire_ts":exp_ts,"days":days,"code_used":base_code,"activated":now_ts}
    acc = _access(bot_data)
    acc["users"][uid] = {"expire_ts":exp_ts,"days":days,"added_ts":now_ts,"notified_expire":False,"source":"referral"}
    _fb_save_subscription(base_code, fingerprint, uid, "ReferralUser", exp_ts, days)
    _fb_cloud_push(bot_data)
    try:
        if ADMIN_ID and _tg_app:
            exp_date = datetime.datetime.fromtimestamp(exp_ts, datetime.timezone.utc).strftime("%d %b %Y")
            await _tg_app.bot.send_message(chat_id=ADMIN_ID,
                text=f"🎁 *Referral Reward Activated*\n\n🔑 Code: `{base_code}` (R1D prefix)\n⏰ Days: {days}\n📆 Expires: {exp_date}\n🔒 Device: `{fingerprint[:8].upper()}…`",
                parse_mode="Markdown")
    except Exception as _e: log.warning(f"Referral redeem notify failed: {_e}")
    log.info(f"🎁 Referral reward redeemed: fp={fingerprint[:8]} code={base_code} days={days}")
    return _http_json({"status":"ACTIVATED_NEW","userId":uid,"expireTs":exp_ts,"daysLeft":days,"username":"ReferralUser"})


async def _run_http_server():
    """Run aiohttp HTTP server on APP_HTTP_PORT alongside Telegram polling."""
    app_web = _aiohttp_web.Application()
    app_web.router.add_post("/appCheck",           _http_app_check)
    app_web.router.add_post("/appActivate",        _http_app_activate)
    app_web.router.add_post("/appPurchase",        _http_app_purchase)
    app_web.router.add_post("/appHistory",         _http_app_history)
    app_web.router.add_post("/appHeartbeat",       _http_app_heartbeat)
    app_web.router.add_post("/appGuestHeartbeat",  _http_app_guest_heartbeat)
    app_web.router.add_post("/appOnlineUsers",     _http_app_online_users)
    app_web.router.add_post("/appFreePreview",     _http_app_free_preview)
    app_web.router.add_post("/appPublicHistory",   _http_app_public_history)
    app_web.router.add_post("/appGetReferralCode", _http_app_get_referral_code)
    app_web.router.add_post("/appSubmitReferral",  _http_app_submit_referral)
    app_web.router.add_post("/appRedeemReferral",  _http_app_redeem_referral)
    runner = _aiohttp_web.AppRunner(app_web)
    await runner.setup()
    site = _aiohttp_web.TCPSite(runner, "0.0.0.0", APP_HTTP_PORT)
    await site.start()
    log.info(f"📱 App HTTP server listening on port {APP_HTTP_PORT}")


# ─── /resetbrain ──────────────────────────────────────────────────────────────
async def cmd_resetbrain(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    /resetbrain Confirm — admin-only hard reset.

    Clears:
      • prediction_memory, strategy_stats, thresholds  (in-process)
      • predictions.json                                (disk)
      • Firebase bot_state/subscriptions payload        (cloud brain only —
        subscriptions/devices/codes are NOT touched)
      • _posted_rounds                                  (so next round re-posts)
      • _h2h_cache / _round_cache                       (stale history gone)

    Requires the literal word "Confirm" as the first argument to prevent
    accidental wipes.
    """
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.")
        return

    args = (u.message.text or "").split()
    if len(args) < 2 or args[1] != "Confirm":
        await u.message.reply_text(
            "⚠️ *Reset Brain — Confirmation Required*\n\n"
            "This will permanently delete all stored predictions, strategy stats, "
            "thresholds, and the Firebase brain snapshot.\n\n"
            "Subscriptions, devices, and activation codes are *not* affected.\n\n"
            "To proceed, send:\n`/resetbrain Confirm`",
            parse_mode="Markdown",
        )
        return

    global prediction_memory, strategy_stats, thresholds, _posted_rounds

    # ── 1. Reset in-process state ─────────────────────────────────────────────
    prediction_memory = []
    strategy_stats = {
        "HOME WIN": {"wins": 0, "losses": 0},
        "AWAY WIN": {"wins": 0, "losses": 0},
        "DRAW":     {"wins": 0, "losses": 0},
        "1X":       {"wins": 0, "losses": 0},
        "X2":       {"wins": 0, "losses": 0},
    }
    thresholds = {"dominance": 0.60, "draw": 0.50}
    _posted_rounds = set()

    # ── 2. Clear H2H and round caches so stale fake data is gone ─────────────
    _h2h_cache.clear()
    _round_cache.clear()
    _round_locks.clear()
    _h2h_locks.clear()

    # ── 3. Wipe predictions.json on disk ─────────────────────────────────────
    try:
        with open(PRED_FILE, "w") as f:
            json.dump({
                "predictions": [], "strategy_stats": strategy_stats,
                "thresholds": thresholds, "posted_rounds": [],
            }, f, indent=2)
        log.info("resetbrain: predictions.json wiped.")
    except Exception as e:
        log.warning(f"resetbrain: could not wipe {PRED_FILE}: {e}")

    # ── 4. Wipe Firebase brain (bot_state doc) — subscriptions untouched ─────
    fb_status = "ℹ️ Firebase not configured"
    if _fb_db:
        try:
            # Re-push a clean payload that preserves subscriptions/devices/codes
            # but has an empty brain section.
            bot_data = c.bot_data
            acc  = bot_data.get("access", {})
            app  = bot_data.get("app_data", {})
            clean_payload = {
                "saved_at":         int(time.time()),
                "users":            {str(k): v for k, v in acc.get("users", {}).items()},
                "allowed_channels": list(acc.get("allowed_channels", set())),
                "auto_chats":       list(bot_data.get("auto_chats", set())),
                "app_devices":      app.get("devices", {}),
                "app_codes":        app.get("codes", {}),
                "purchase_message": app.get("purchase_message", ""),
                "predictions":      [],
                "strategy_stats":   strategy_stats,
                "thresholds":       thresholds,
                "posted_rounds":    [],
            }
            ciphertext = _fb_encrypt_payload(clean_payload)
            if ciphertext:
                _fb_db.collection("bot_state").document("subscriptions").set(
                    {"payload": ciphertext, "saved_at": int(time.time()), "version": 1})
                fb_status = "☁️ Firebase brain wiped"
                log.info("resetbrain: Firebase bot_state cleared (subscriptions intact).")
            else:
                fb_status = "⚠️ Firebase encryption failed — disk reset only"
        except Exception as e:
            fb_status = f"⚠️ Firebase error: {e}"
            log.warning(f"resetbrain: Firebase wipe failed: {e}")

    log.info(f"🔴 Brain hard-reset by admin {u.effective_user.id}")
    await u.message.reply_text(
        f"✅ *Brain Reset Complete*\n\n"
        f"  Predictions cleared  : `0`\n"
        f"  Strategy stats reset : default\n"
        f"  Dominance threshold  : `0.60`\n"
        f"  Draw threshold       : `0.50`\n"
        f"  Posted rounds        : cleared\n"
        f"  H2H / round cache    : cleared\n"
        f"  Disk ({PRED_FILE})   : wiped\n"
        f"  {fb_status}\n\n"
        f"_Subscriptions, devices and codes are untouched._\n"
        f"_The bot will start learning fresh from the next round._",
        parse_mode="Markdown",
    )


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    persistence = PicklePersistence(filepath="vsbot_data.pkl")
    app = (Application.builder()
           .token(BOT_TOKEN)
           .persistence(persistence)
           .build())

    # Commands
    app.add_handler(CommandHandler("start",         cmd_start))
    app.add_handler(CommandHandler("help",          cmd_start))
    app.add_handler(CommandHandler("stop",          cmd_stop))
    app.add_handler(CommandHandler("mystatus",      cmd_mystatus))
    app.add_handler(CommandHandler("adduser",       cmd_adduser))
    app.add_handler(CommandHandler("removeuser",    cmd_removeuser))
    app.add_handler(CommandHandler("addchannel",    cmd_addchannel))
    app.add_handler(CommandHandler("removechannel", cmd_removechannel))
    app.add_handler(CommandHandler("showstatus",    cmd_showstatus))
    app.add_handler(CommandHandler("brainstatus",   cmd_brainstatus))
    app.add_handler(CommandHandler("backupbrain",   cmd_backupbrain))
    app.add_handler(CommandHandler("restorebrain",  cmd_restorebrain))
    app.add_handler(CommandHandler("resetbrain",    cmd_resetbrain))
    app.add_handler(MessageHandler(filters.Document.MimeType("application/json"), handle_brain_restore))
    # App management commands
    app.add_handler(CommandHandler("appgencode",    cmd_appgencode))
    app.add_handler(CommandHandler("appsetmsg",     cmd_appsetmsg))
    app.add_handler(CommandHandler("appusers",      cmd_appusers))
    app.add_handler(CommandHandler("apptamperhistory", cmd_apptamperhistory))
    app.add_handler(CommandHandler("allusers",      cmd_allusers))

    # Menu
    app.add_handler(CallbackQueryHandler(cb_menu,            pattern=r"^menu:"))

    # Live
    app.add_handler(CallbackQueryHandler(cb_live,            pattern=r"^live:"))

    # Standings (direct)
    app.add_handler(CallbackQueryHandler(cb_standings,       pattern=r"^standings:"))

    # Load persisted prediction data
    _load_predictions()

    # ── Wire HTTP server global ref ──────────────────────────────────────────────
    global _tg_app
    _tg_app = app

    # Register the background auto-loop (new-round detector + result updater)
    async def _on_startup(application):
        # Auto-register admin's private chat as a broadcast destination
        if ADMIN_ID:
            chats = application.bot_data.setdefault("auto_chats", set())
            chats.add(str(ADMIN_ID))
            log.info(f"_on_startup: admin {ADMIN_ID} auto-registered as broadcast destination.")

        bd = application.bot_data
        # Always attempt Firebase restore on startup — it merges (never overwrites)
        # existing pickle data, and fixes the case where pickle is stale or missing.
        restored = _fb_cloud_restore(bd)
        if restored:
            log.info("☁️  Data restored from Firebase cloud (subscriptions + brain).")
        else:
            log.info("ℹ️  No Firebase data to restore — using local pickle only.")

        # Start HTTP app server
        asyncio.ensure_future(_run_http_server())

        # Start prediction auto-loop
        asyncio.create_task(_auto_loop(application))

        # Subscription expiry checker — notify users when their sub expires
        async def _expiry_checker():
            while True:
                try:
                    acc2   = _access(application.bot_data)
                    now_ts = time.time()
                    for uid_s, entry in list(acc2["users"].items()):
                        if entry["expire_ts"] <= now_ts and not entry.get("notified_expire"):
                            entry["notified_expire"] = True
                            try:
                                await application.bot.send_message(
                                    chat_id=int(uid_s),
                                    text="⏰ *Your BetPawa Bot access has expired.*\n\nContact the admin to renew your subscription.",
                                    parse_mode="Markdown")
                            except Exception: pass
                            chats2 = application.bot_data.get("auto_chats", set())
                            chats2.discard(uid_s)
                    _fb_cloud_push(application.bot_data)
                except Exception as e:
                    log.error(f"expiry_checker error: {e}")
                await asyncio.sleep(3600)

        asyncio.create_task(_expiry_checker())

    app.post_init = _on_startup

    log.info(f"🤖 BetPawa Bot starting — admin={ADMIN_ID}")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
