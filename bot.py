"""
BetPawa Virtual Football Analyst Bot — v4
Fixed for real betPawa API structure (confirmed from network captures):

actual/past endpoint shape:
  { items: [ { id: "137297", name: "#137297", rounds: [
      { id: "1179973", name: "01", tradingTime: { start: "...", end: "..." } },
      ...
  ] } ] }

Events endpoint:
  GET /api/sportsbook/virtual/v2/events/list/by-round/{roundId}?page=upcoming|live|matchups
  { items: [ { id, participants: [{name, type:"HOME"|"AWAY"}], markets:[...], results:{...} } ] }

Standings endpoint:
  GET /api/sportsbook/virtual/v1/seasons/{seasonId}/standing?leagueId={id}

Key fixes vs v3:
  - tradingTime.start used for round timing (not startTime)
  - Round IDs come from rounds[].id (not gameRoundId)
  - Season ID from items[].id
  - _round_start_ms reads tradingTime correctly
  - /live fetches current round properly
  - /standings uses correct season_id
  - Team name abbreviations mapped for display
"""

import asyncio
import os, re, logging, time, calendar, datetime, math, json, io
from collections import defaultdict, Counter

import httpx
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
    MessageHandler, filters,
    PicklePersistence,
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
CHANNEL_ID     = os.environ.get("CHANNEL_ID", "")   # Telegram channel/group ID
# Storage chat for /backup auto-upload and /fetchdata restore = ADMIN_ID (your own Telegram chat)

# ── ADMIN CONFIG ──────────────────────────────────────────────────────────────
# Set your Telegram numeric user ID here (get it from @userinfobot on Telegram)
# OR set the ADMIN_ID environment variable in Railway — whichever you prefer.
_HARDCODED_ADMIN_ID = 0   # ← REPLACE 0 WITH YOUR NUMERIC TELEGRAM ID e.g. 123456789

ADMIN_ID = int(os.environ.get("ADMIN_ID", "0")) or _HARDCODED_ADMIN_ID

# ─── ACCESS CONTROL STORAGE ───────────────────────────────────────────────────
# Stored in bot_data["access"]:
#   {
#     "allowed_channels": set of str channel/group IDs,
#     "users": { str(user_id): {"expire_ts": float, "days": int, "added_ts": float} },
#     "pending_user": { str(admin_chat_id): str(user_id_being_added) }
#   }

def _access(bot_data: dict) -> dict:
    if "access" not in bot_data:
        bot_data["access"] = {
            "allowed_channels": set(),
            "users": {},
            "pending_user": {},
        }
    d = bot_data["access"]
    if "allowed_channels" not in d: d["allowed_channels"] = set()
    if "users"            not in d: d["users"]            = {}
    if "pending_user"     not in d: d["pending_user"]     = {}
    # Always normalize channel IDs to str to prevent int/str mismatch from pickle
    d["allowed_channels"] = set(str(x) for x in d["allowed_channels"])
    return d

async def _fetch_utc_now() -> datetime.datetime:
    """Fetch current UTC time from worldtimeapi.org; fall back to system time."""
    try:
        async with httpx.AsyncClient() as cl:
            r = await cl.get("https://worldtimeapi.org/api/timezone/UTC", timeout=5)
            if r.status_code == 200:
                data = r.json()
                iso  = data.get("datetime", "")
                m    = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})", iso)
                if m:
                    return datetime.datetime(*map(int, m.groups()), tzinfo=datetime.timezone.utc)
    except Exception as e:
        log.warning(f"worldtimeapi fetch failed: {e}")
    return datetime.datetime.now(datetime.timezone.utc)

def _is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID

def _is_authorized_user(user_id: int, bot_data: dict) -> bool:
    """Returns True if user has a valid non-expired subscription."""
    acc = _access(bot_data)
    uid = str(user_id)
    if uid not in acc["users"]:
        return False
    entry = acc["users"][uid]
    now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
    return entry["expire_ts"] > now_ts

def _remaining_days(user_id: int, bot_data: dict) -> float | None:
    acc = _access(bot_data)
    uid = str(user_id)
    if uid not in acc["users"]:
        return None
    entry  = acc["users"][uid]
    now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
    secs   = entry["expire_ts"] - now_ts
    return max(0.0, secs / 86400)

# ─── LEAGUES ──────────────────────────────────────────────────────────────────
LEAGUES: dict[int, dict] = {
    7794:  {"name": "England",     "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿"},
    7795:  {"name": "Spain",       "flag": "🇪🇸"},
    7796:  {"name": "Italy",       "flag": "🇮🇹"},
    9183:  {"name": "France",      "flag": "🇫🇷"},
    9184:  {"name": "Netherlands", "flag": "🇳🇱"},
    13773: {"name": "Germany",     "flag": "🇩🇪"},
    13774: {"name": "Portugal",    "flag": "🇵🇹"},
}


# ── ACTIVE LEAGUES ───────────────────────────────────────────────────────────
# Only leagues in this set are predicted, stored, and learned from.
# All others are completely dormant — no predictions, no storing, no learning.
# To re-enable a league, add its ID back to this set.
ACTIVE_LEAGUES: set = {7794}  # England only

def ld(league_id: int) -> str:
    l = LEAGUES[league_id]
    return f"{l['flag']} {l['name']}"

# Full team name rosters (used for league filtering)
# Exact 3-letter codes as used by the betPawa API (confirmed from live screenshots)
# Seed whitelists — used only to bootstrap filtering on first run.
# The bot auto-learns the real team list from live API data each season.
# New seasons with promotions/relegations are handled automatically —
# no manual updates needed. Tier calculations always use the actual
# observed team count, never a hardcoded number.
LEAGUE_TEAMS: dict[int, set[str]] = {
    7794: {  # England (seed) — 20 teams
        "AST","ARS","BOU","BRE","BUR","CHE","CRY","EVE","FUL","LIV",
        "MCI","MUN","NEW","NOT","TOT","WHU","WOL","BHA","SUN","LEE",
    },
    7795: {  # Spain (seed) — 20 teams
        "ALA","ATH","ATM","BAR","BET","CEL","ELC","ESP","GET","GIR",
        "LEV","MAL","OSA","OVI","RAY","RMA","RSO","SEV","VAL","VIL",
    },
    7796: {  # Italy (seed) — 20 teams
        "ATA","BOL","CAG","COM","CRE","FIO","GEN","INT","JUV","LAZ",
        "LEC","MIL","NAP","PAR","PIS","ROM","SAS","TOR","UDI","VER",
    },
    9183: {  # France (seed)
        "ASM","AUX","BRE","HAV","LEN","LIL","LOR","LYO",
        "MAR","MET","NAN","NIC","PSG","REN","STR","TOU","ANG","PAR",
    },
    9184: {  # Netherlands (seed) — 18 teams
        "AJA","AZA","EXC","FEY","FOR","GAE","HEE","HER",
        "NAC","NEC","PEC","PSV","SPA","TEL","TWE","UTR","VOL","GRO",
    },
    13773: {  # Germany (seed) — 18 teams
        "AUG","COL","DOR","EIN","FCB","FRE","HEI","HOF",
        "HSV","LEV","MAI","MON","RBL","STP","STU","UNI","WER","WOL",
    },
    13774: {  # Portugal (seed) — 18 teams
        "ALV","ARO","AVS","BEN","BRA","CAS","EST","ETA","FAM",
        "GIL","GUI","MOR","NAC","POR","RIO","SAN","SPO","TON",
    },
}



# ─── API ──────────────────────────────────────────────────────────────────────
BASE = "https://www.betpawa.ug"
EP_PAST_ROUNDS   = "/api/sportsbook/virtual/v1/seasons/list/past"
EP_ACTUAL_ROUNDS = "/api/sportsbook/virtual/v1/seasons/list/actual"
EP_EVENTS        = "/api/sportsbook/virtual/v2/events/list/by-round/{round_id}"
EP_STANDINGS     = "/api/sportsbook/virtual/v1/seasons/{season_id}/standing"

PAGE_LIVE     = "live"
PAGE_UPCOMING = "upcoming"
PAGE_MATCHUPS = "matchups"

HEADERS = {
    "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-UG,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.betpawa.ug/virtual-sports",
    "Content-Type":    "application/json",
    "Devicetype":      "desktop",
    "X-Pawa-Brand":    "betpawa-uganda",
    "X-Pawa-Language": "en",
    "Sec-Fetch-Site":  "same-origin",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Dest":  "empty",
}
if BETPAWA_COOKIE:
    HEADERS["Cookie"] = BETPAWA_COOKIE
    log.info("🍪 BETPAWA_COOKIE loaded")
else:
    log.warning("⚠️  No BETPAWA_COOKIE — some endpoints may return 401/403")

# ─── HTTP HELPER ──────────────────────────────────────────────────────────────
async def _get(client: httpx.AsyncClient, url: str, params: dict | None = None):
    full_url = str(url) + (("?" + "&".join(f"{k}={v}" for k,v in params.items())) if params else "")
    log.info(f"🌐 GET {full_url}")
    try:
        r = await client.get(url, headers=HEADERS, params=params,
                             timeout=15, follow_redirects=True)
        log.info(f"↩️  HTTP {r.status_code} ← {r.url}")
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception as je:
                log.error(f"❌ JSON parse failed — {url} — {je}")
                log.error(f"   Raw body: {r.text[:400]}")
                return None
            if data:
                # Log shape of response to help debug structure issues
                if isinstance(data, dict):
                    log.info(f"✅ JSON keys={list(data.keys())[:8]}")
                elif isinstance(data, list):
                    log.info(f"✅ JSON list len={len(data)}")
                return data
            log.warning(f"⚠️  HTTP 200 but EMPTY body — {url}")
        elif r.status_code == 401:
            log.error(f"🔐 401 UNAUTHORIZED — cookie missing or expired — {url}")
            log.error(f"   Set BETPAWA_COOKIE env var in Railway")
        elif r.status_code == 403:
            log.error(f"🚫 403 FORBIDDEN — IP blocked or session invalid — {url}")
        elif r.status_code == 404:
            log.error(f"🔍 404 NOT FOUND — wrong endpoint or round ID — {url}")
        else:
            log.warning(f"⚠️  HTTP {r.status_code} — {url}")
        try:
            log.warning(f"   Response body: {r.text[:500]}")
        except Exception:
            pass
    except httpx.TimeoutException:
        log.error(f"⏱️  TIMEOUT after 15s — {url}")
    except httpx.ConnectError as e:
        log.error(f"🔌 CONNECTION ERROR — {url} — {e}")
    except Exception as e:
        log.error(f"💥 REQUEST FAILED — {url} — {type(e).__name__}: {e}")
    return None

# ─── TIME HELPERS ─────────────────────────────────────────────────────────────
def _iso_to_ms(t) -> int:
    """Convert ISO string or numeric timestamp to milliseconds."""
    if not t:
        return 0
    if isinstance(t, (int, float)):
        t = int(t)
        return t * 1000 if t < 10_000_000_000 else t
    if isinstance(t, str):
        m = re.search(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})', t)
        if m:
            dt = datetime.datetime(*map(int, m.groups()), tzinfo=datetime.timezone.utc)
            return int(calendar.timegm(dt.timetuple())) * 1000
    return 0

def _round_start_ms(r: dict) -> int:
    """
    Get round start time in ms. betPawa uses tradingTime.start, not startTime.
    Falls back to startTime for legacy shapes.
    """
    # Primary: tradingTime.start (confirmed from API capture)
    tt = r.get("tradingTime")
    if isinstance(tt, dict):
        v = _iso_to_ms(tt.get("start"))
        if v:
            return v
    # Fallback
    return _iso_to_ms(r.get("startTime") or r.get("start_time") or r.get("startDate") or 0)

def _round_id_int(r: dict) -> int:
    v = r.get("id") or r.get("gameRoundId") or r.get("roundId") or 0
    try:    return int(v)
    except: return 0

# ─── SCORE EXTRACTION ─────────────────────────────────────────────────────────
def _int(v) -> int | None:
    if v is None: return None
    try:
        s = str(v).strip()
        return int(float(s)) if s not in ("", "null") else None
    except (ValueError, TypeError):
        return None

def _extract_score(event: dict) -> tuple[int | None, int | None]:
    """Extract home/away full-time score from event dict."""

    # 0. injected scores (from two-stage prediction flow)
    _ih = event.get("_injected_score_h")
    _ia = event.get("_injected_score_a")
    if _ih is not None and _ia is not None:
        return int(_ih), int(_ia)
    sc = event.get("score")
    if isinstance(sc, dict):
        h = _int(sc.get("home") or sc.get("homeScore") or sc.get("scoreHome"))
        a = _int(sc.get("away") or sc.get("awayScore") or sc.get("scoreAway"))
        if h is not None and a is not None:
            return h, a

    # 2. top-level HomeScore / AwayScore
    h = _int(event.get("HomeScore") or event.get("homeScore"))
    a = _int(event.get("AwayScore") or event.get("awayScore"))
    if h is not None and a is not None:
        return h, a

    # 3. results sub-dict
    res = event.get("results")
    if isinstance(res, dict):

        for sub in ("scoreboard", "display", "fullTime", "score"):
            s2 = res.get(sub)
            if isinstance(s2, dict):
                h = _int(s2.get("scoreHome") or s2.get("home") or s2.get("homeScore"))
                a = _int(s2.get("scoreAway") or s2.get("away") or s2.get("awayScore"))
                if h is not None and a is not None:
                    return h, a

        h = _int(res.get("HomeScore") or res.get("homeScore") or res.get("scoreHome"))
        a = _int(res.get("AwayScore") or res.get("awayScore") or res.get("scoreAway"))
        if h is not None and a is not None:
            return h, a

        # participantPeriodResults — betPawa confirmed structure
        ppr = res.get("participantPeriodResults")
        if isinstance(ppr, list) and ppr:
            home_sc = away_sc = None
            for entry in ppr:
                if not isinstance(entry, dict):
                    continue
                p_info = entry.get("participant") or {}
                ptype = str(
                    p_info.get("type") or entry.get("type") or
                    entry.get("participantType") or entry.get("side") or ""
                ).upper()

                for pr in (entry.get("periodResults") or []):
                    if not isinstance(pr, dict):
                        continue
                    period_obj  = pr.get("period") or {}
                    period_slug = str(period_obj.get("slug") or "").upper()
                    pr_type     = str(pr.get("type") or "").upper()
                    is_fulltime = (
                        "FULL_TIME" in period_slug or
                        period_slug == "" or
                        pr_type == "SCORE"
                    )
                    if "HALF" not in period_slug and is_fulltime:
                        v = _int(pr.get("result") or pr.get("score") or pr.get("value"))
                        if v is not None:
                            if "HOME" in ptype:
                                home_sc = v
                            elif "AWAY" in ptype:
                                away_sc = v

                if home_sc is None or away_sc is None:
                    pd_num = entry.get("period")
                    try:    pd_int = int(pd_num) if pd_num is not None else -1
                    except: pd_int = -1
                    if pd_int == 0:
                        v = _int(entry.get("score") or entry.get("value"))
                        if v is not None:
                            pt = ptype.lower()
                            if "home" in pt:   home_sc = v
                            elif "away" in pt: away_sc = v

            if home_sc is not None and away_sc is not None:
                return home_sc, away_sc

    return None, None

def _extract_ht_score(event: dict) -> tuple[int | None, int | None]:
    """Extract half-time score."""
    res = event.get("results")
    if not isinstance(res, dict):
        return None, None
    ppr = res.get("participantPeriodResults")
    if not isinstance(ppr, list):
        return None, None
    home_ht = away_ht = None
    for entry in ppr:
        if not isinstance(entry, dict):
            continue
        p_info = entry.get("participant") or {}
        ptype  = str(p_info.get("type") or entry.get("type") or "").upper()
        for pr in (entry.get("periodResults") or []):
            if not isinstance(pr, dict):
                continue
            period_obj  = pr.get("period") or {}
            period_slug = str(period_obj.get("slug") or "").upper()
            if "FIRST_HALF" in period_slug or period_slug == "HALF_TIME":
                v = _int(pr.get("result") or pr.get("score") or pr.get("value"))
                if v is not None:
                    if "HOME" in ptype:  home_ht = v
                    elif "AWAY" in ptype: away_ht = v
    return home_ht, away_ht

# ─── EVENT NORMALISATION ──────────────────────────────────────────────────────
def _team_name(p) -> str:
    if isinstance(p, dict):
        return str(p.get("name") or p.get("teamName") or "?")
    return str(p) if p else "?"

def _norm_event(raw: dict) -> dict:
    parts = raw.get("participants", [])
    if len(parts) >= 2:
        home_p = next((p for p in parts if str(p.get("type","")).upper() == "HOME"), parts[0])
        away_p = next((p for p in parts if str(p.get("type","")).upper() == "AWAY"), parts[1])
        home = _team_name(home_p)
        away = _team_name(away_p)
    else:
        home = _team_name(raw.get("homeTeam") or raw.get("home_team")) or str(raw.get("homeName","?"))
        away = _team_name(raw.get("awayTeam") or raw.get("away_team")) or str(raw.get("awayName","?"))
    hs, as_ = _extract_score(raw)
    return {"home": home, "away": away, "hs": hs, "as_": as_, "_raw": raw}

def _flatten(raw) -> list[dict]:
    if not raw: return []
    if isinstance(raw, dict):
        items = raw.get("items")
        if isinstance(items, list) and items:
            first = items[0]
            if isinstance(first, dict) and "rounds" not in first:
                return items
        for k in ("matches","events","data","games"):
            if k in raw and isinstance(raw[k], list):
                return _flatten(raw[k])
        if any(k in raw for k in ("participants","HomeScore","score")):
            return [raw]
    if isinstance(raw, list):
        if raw and isinstance(raw[0], dict):
            if any(k in raw[0] for k in ("participants","HomeScore","score","id")):
                return raw
        out = []
        for item in raw:
            out.extend(_flatten(item))
        return out
    return []

def _filter_league(events: list[dict], league_id: int) -> list[dict]:
    """Keep only events where BOTH teams belong to this league's known code set.
    Also auto-learns new team codes seen in actual rounds — handles promoted/relegated
    teams across seasons without manual whitelist updates."""
    if not events:
        return []

    codes = LEAGUE_TEAMS.get(league_id, set()).copy()  # copy — never mutate the shared whitelist
    if not codes:
        return events

    # Auto-learn: if most events pass the whitelist, the few that don't are likely
    # promoted teams in a new season — add them to the live set.
    # Strategy: score each event, then accept any team that appears alongside known teams.
    # A full round has exactly N/2 matches (e.g. 10 for 20 teams). If we get ≥ half
    # the expected matches cleanly, trust the round and absorb unknowns.
    clean = []
    unknown_pairs = []
    for raw in events:
        m = _norm_event(raw)
        h = m["home"].upper().strip()
        a = m["away"].upper().strip()
        if h in codes and a in codes:
            clean.append(raw)
        else:
            unknown_pairs.append((h, a, raw))

    # If we have a substantial clean match set, absorb unknowns into the league.
    # Guard: unknowns must be a small minority (<=20%) of the total event pool AND
    # we need at least half the expected round size clean before trusting unknowns.
    # This prevents the shared 66-event pool (same round_id across all leagues) from
    # flooding every league's whitelist with foreign teams — the root cause of all
    # leagues posting identical fixtures (cross-league data bleed).
    _expected_round = max(9, len(codes) // 2)
    _min_clean = max(_expected_round // 2, 4)   # need >= half a round of clean matches
    _max_unknown_ratio = 0.20                    # unknowns must be <= 20% of total events
    _unknown_ratio = len(unknown_pairs) / max(len(events), 1)
    if len(clean) >= _min_clean and unknown_pairs and _unknown_ratio <= _max_unknown_ratio:
        for h, a, raw in unknown_pairs:
            if h not in codes:
                codes.add(h)
                LEAGUE_TEAMS.setdefault(league_id, set()).add(h)
                log.info(f"🆕 filter_league [{league_id}]: auto-learned team '{h}' (now in whitelist)")
            if a not in codes:
                codes.add(a)
                LEAGUE_TEAMS.setdefault(league_id, set()).add(a)
                log.info(f"🆕 filter_league [{league_id}]: auto-learned team '{a}' (now in whitelist)")
            clean.append(raw)

    out = clean
    if out:
        log.info(f"🏟️ filter_league lid={league_id}: {len(events)}→{len(out)} events")
        return out

    # Fallback: try matching just the first 3 chars of team name
    out = []
    for raw in events:
        m = _norm_event(raw)
        h3 = m["home"].upper()[:3]
        a3 = m["away"].upper()[:3]
        if h3 in codes and a3 in codes:
            out.append(raw)

    if out:
        log.info(f"🏟️ filter_league lid={league_id} (3-char): {len(events)}→{len(out)} events")
        return out

    names = [_norm_event(e)["home"] + " vs " + _norm_event(e)["away"] for e in events[:5]]
    log.warning(f"⚠️ filter_league lid={league_id}: NO MATCH. Sample: {names}")
    return []

# ─── ODDS EXTRACTION ──────────────────────────────────────────────────────────
def _extract_odds(event: dict) -> dict:
    """
    Extract odds from betPawa's confirmed API structure:
      market = {
        'marketType': {'id': '3743', 'name': '1X2 - FT', ...},
        'row': [{'prices': [
            {'name': '1', 'price': 1.97},
            {'name': 'X', 'price': 3.40},
            {'name': '2', 'price': 4.20},
        ]}]
      }
    Prices live in market['row'][0]['prices'], NOT in 'outcomes'.
    """
    markets = event.get("markets", [])
    result: dict = {"1x2": {}, "ou": [], "btts": {}, "dc": {}, "htft": {}}
    if not markets:
        return result

    for mkt in markets:
        # Market type info
        mt    = mkt.get("marketType") or {}
        mname = str(mt.get("name") or mt.get("displayName") or "").upper()
        mid   = str(mt.get("id") or "")

        # Prices live in row[0]['prices']
        rows  = mkt.get("row") or []
        if not rows:
            continue
        prices = rows[0].get("prices") or [] if rows else []
        if not prices:
            # try flattening all rows
            prices = [p for row in rows for p in (row.get("prices") or [])]
        if not prices:
            continue

        def _p(price_obj):
            v = price_obj.get("price")
            try:
                f = float(v)
                return round(f, 2) if f > 1.0 else None
            except (TypeError, ValueError):
                return None

        def _name(price_obj):
            return str(price_obj.get("name") or price_obj.get("displayName") or "").strip().upper()

        # ── 1X2 — marketType id 3743 or name contains '1X2'
        if mid == "3743" or "1X2" in mname:
            if not result["1x2"]:
                for po in prices:
                    n = _name(po); p = _p(po)
                    if p is None: continue
                    if n == "1":   result["1x2"]["1"] = p
                    elif n == "X": result["1x2"]["X"] = p
                    elif n == "2": result["1x2"]["2"] = p

        # ── BTTS — marketType id 3795
        elif mid == "3795" or "BOTH TEAMS" in mname or "BTTS" in mname:
            if not result["btts"]:
                for po in prices:
                    n = _name(po); p = _p(po)
                    if p is None: continue
                    if n == "YES":  result["btts"]["Yes"] = p
                    elif n == "NO": result["btts"]["No"]  = p

        # ── Over/Under — marketType id 5000
        # Each line (1.5, 2.5, 3.5 ...) is a SEPARATE row — must iterate all rows
        elif mid == "5000" or "OVER" in mname or "UNDER" in mname or "TOTAL" in mname:
            import re as _re
            seen_ou = set()
            for row in rows:
                row_prices = row.get("prices") or []
                if not row_prices:
                    continue
                # Get the line from handicap field on this row
                line = row.get("handicap")
                for po in row_prices:
                    n = _name(po); p = _p(po)
                    if p is None: continue
                    # If line not on row, try to parse from name e.g. "OVER 2.5"
                    row_line = line
                    if row_line is None:
                        m2 = _re.search(r"[\d]+\.?[\d]*", n)
                        row_line = m2.group() if m2 else None
                    side = "O" if n.startswith("O") else ("U" if n.startswith("U") else None)
                    if side and row_line is not None:
                        key = (side, str(row_line))
                        if key not in seen_ou:
                            seen_ou.add(key)
                            result["ou"].append((side, str(row_line), p))

        # ── Double Chance — marketType id 4693
        elif mid == "4693" or "DOUBLE" in mname:
            if not result["dc"]:
                # DC can also be spread across rows
                for row in rows:
                    for po in (row.get("prices") or []):
                        n = _name(po); p = _p(po)
                        if p is None: continue
                        if n in ("1X","1 X"):   result["dc"]["1X"] = p
                        elif n in ("X2","X 2"): result["dc"]["X2"] = p
                        elif n in ("12","1 2"): result["dc"]["12"] = p

        # ── HT/FT — marketType id 4706
        # Format from betPawa: name = "1/1", "1/X", "1/2", "X/1", "X/X", "X/2",
        #                              "2/1", "2/X", "2/2"
        # Meaning: HT_result / FT_result  (1=Home, X=Draw, 2=Away)
        elif mid == "4706" or "HT/FT" in mname or ("HT" in mname and "FT" in mname):
            if not result["htft"]:
                for row in rows:
                    for po in (row.get("prices") or []):
                        n = _name(po); p = _p(po)
                        if p is not None:
                            # normalise: "1/1" stays as "1/1", strip spaces
                            clean = n.replace(" ", "")
                            result["htft"][clean] = p

    return result

def _fmt_odds_full(odds: dict) -> str:
    sections = []

    o1x2 = odds.get("1x2", {})
    if o1x2:
        h = o1x2.get("1", "—")
        x = o1x2.get("X", "—")
        a = o1x2.get("2", "—")
        sections.append(f"*1X2*\n`  1: {h:<6}  X: {x:<6}  2: {a}`")

    ou = odds.get("ou", [])
    if ou:
        by_line: dict[str, dict] = {}
        for side, line, price in ou:
            by_line.setdefault(line, {})[side] = price
        ou_lines = []
        for line in sorted(by_line.keys(), key=lambda x: float(x)):
            o  = by_line[line].get("O", "—")
            uu = by_line[line].get("U", "—")
            ou_lines.append(f"`  Over {line}: {o:<6}  Under {line}: {uu}`")
        sections.append("*O/U*\n" + "\n".join(ou_lines))

    btts = odds.get("btts", {})
    if btts:
        y = btts.get("Yes", "—")
        n = btts.get("No", "—")
        sections.append(f"*BTTS*\n`  Yes: {y:<6}  No: {n}`")

    dc = odds.get("dc", {})
    if dc:
        parts = []
        for k in ("1X", "X2", "12"):
            if k in dc:
                parts.append(f"{k}: {dc[k]}")
        sections.append("*DC*\n`  " + "   ".join(parts) + "`")

    htft = odds.get("htft", {})
    if htft:
        # Display in the 3×3 grid betPawa uses:
        # Row 1: 1/1  1/X  1/2
        # Row 2: X/1  X/X  X/2
        # Row 3: 2/1  2/X  2/2
        grid_order = ["1/1","1/X","1/2","X/1","X/X","X/2","2/1","2/X","2/2"]
        grid_rows = []
        for i in range(0, 9, 3):
            cells = []
            for k in grid_order[i:i+3]:
                v = htft.get(k, "—")
                cells.append(f"{k}:{v:<7}")
            grid_rows.append("`  " + "  ".join(cells) + "`")
        sections.append("*HT/FT*\n" + "\n".join(grid_rows))

    return "\n".join(sections) if sections else "_No odds available_"

def _fmt_prediction_result(home: str, away: str,
                             p: dict, fp_match: dict | None,
                             odds: dict) -> str:
    """
    Build the full Telegram message block for one predicted match.
    Shows:
      - Teams, tip, confidence
      - Form audit: win% from last 6 matches both sides + reasons
      - Tier verdict (strong confirmed / weak threat level)
      - Matched markets from fingerprint (all confirmed markets)
      - HT/FT prediction if fingerprint has one
    """
    fr   = p.get("form_report", {})
    tip  = p.get("tip", "?")
    conf = p.get("conf", 0.0)
    icon = p.get("icon", "⚽")

    lines = []

    # ── Header ────────────────────────────────────────────────────────────────
    lines.append(f"*{home} vs {away}*")
    lines.append(f"{icon} *{tip}*  •  Confidence: *{conf:.1f}%*")

    # ── Tier info ──────────────────────────────────────────────────────────────
    if fr:
        st = fr.get("strong_tier", ""); wt = fr.get("weak_tier", "")
        lines.append(
            f"📊 {fr['strong_team'].upper()} [{st}] vs {fr['weak_team'].upper()} [{wt}]"
        )

    # ── Win% from last 6 (both sides) ─────────────────────────────────────────
    if fr and fr.get("strong_games_n", 0) > 0:
        sw = fr.get("strong_wpct", 0.0)
        ww = fr.get("weak_wpct",   0.0)
        sw_bar = "█" * int(sw / 10) + "░" * (10 - int(sw / 10))
        ww_bar = "█" * int(ww / 10) + "░" * (10 - int(ww / 10))
        lines.append(
            f"📈 Last 6 — *{fr['strong_team']}*: {sw_bar} {sw:.0f}%"
        )
        lines.append(
            f"📉 Last 6 — *{fr['weak_team']}*:   {ww_bar} {ww:.0f}%"
        )

        # ── Form verdict ──────────────────────────────────────────────────────
        verdict = fr.get("verdict", "")
        if verdict:
            lines.append(f"{verdict}")

        # ── Loss reasons (strong team) ────────────────────────────────────────
        if fr.get("loss_reasons"):
            lines.append("  ⚠️ Strong team recent: " + " | ".join(fr["loss_reasons"]))

        # ── Threat reasons (weak/moderate team) ───────────────────────────────
        if fr.get("win_reasons"):
            lines.append("  🔺 Opponent recent: " + " | ".join(fr["win_reasons"]))

        # ── Recovery engine verdict ───────────────────────────────────────────
        rec = fr.get("recovery", {})
        if rec and rec.get("data_confirmed"):
            sig = rec.get("recovery_signal", "UNCERTAIN")
            strength = rec.get("recovery_strength", 0.0)
            if sig == "STRONG_RECOVERS":
                lines.append(
                    f"  🔄 *Recovery confirmed* — strong team expected to bounce back "
                    f"(confidence {strength:.0%})"
                )
            elif sig == "WEAK_REPEATS":
                lines.append(
                    f"  ⚡ *Upset pattern confirmed* — weak/moderate side "
                    f"may repeat the win (confidence {strength:.0%})"
                )
            # Show top 3 reasons
            reasons = rec.get("reasoning", [])
            for r in reasons[:3]:
                lines.append(f"    • {r}")
        elif rec and not rec.get("data_confirmed"):
            reasons = rec.get("reasoning", [])
            if reasons:
                lines.append(f"  🔍 Recovery check: {reasons[0]}")

    # ── Fingerprint / H2H matched markets ────────────────────────────────────
    if fp_match and fp_match.get("n_samples", 0) > 0:
        n      = fp_match["n_samples"]
        sim    = fp_match.get("confidence", 0.0)
        source = fp_match.get("source", "odds_match")
        flip   = "🔄 (reversed fixture)" if fp_match.get("flipped") else ""

        if source == "odds_match":
            lines.append(f"\n🔮 *Odds pattern match* — {n} similar {'game' if n==1 else 'games'} {flip}")
            lines.append(f"   Odds similarity: {sim*100:.0f}%")
        else:
            # H2H fallback path
            h2h_cv = fp_match.get("h2h_cv", {})
            agr_icon = {"strong":"✅","partial":"⚡","conflict":"⚠️","none":"❓"}.get(
                h2h_cv.get("agreement","none"), "❓")
            lines.append(f"\n📂 *H2H history* — {n} total meetings (odds didn't match)")
            hv_n = fp_match.get("h2h_cv", {}).get("h2h_n", 0)
            av_n = n - hv_n
            lines.append(f"   Same venue: {hv_n} games  |  Reversed venue: {av_n} games")
            lines.append(f"   {agr_icon} Cross-validation: {h2h_cv.get('reason','')}")
            if h2h_cv.get("h2h_home_pct"):
                lines.append(
                    f"   Home-venue outcomes — "
                    f"🏠{h2h_cv['h2h_home_pct']:.0f}%  "
                    f"🤝{100-h2h_cv['h2h_home_pct']-h2h_cv.get('h2h_away_pct',0):.0f}%  "
                    f"✈️{h2h_cv.get('h2h_away_pct',0):.0f}%"
                )

        dom_out  = fp_match.get("dominant_outcome", "?")
        dom_htft = fp_match.get("dominant_htft")
        out_icon = {"HOME":"🏠","AWAY":"✈️","DRAW":"🤝"}.get(dom_out, "⚽")
        lines.append(f"   {out_icon} Historical outcome: *{dom_out}*")

        if dom_htft:
            _t = {"1":"Home","X":"Draw","2":"Away"}
            parts = dom_htft.split("/")
            if len(parts) == 2:
                ht_str = _t.get(parts[0], parts[0])
                ft_str = _t.get(parts[1], parts[1])
                lines.append(f"   ⏱ HT/FT pattern: *{ht_str} / {ft_str}*")

        # ── All confirmed matching markets ─────────────────────────────────────
        matched = fp_match.get("matched_markets", {})
        if matched:
            lines.append("   📋 Confirmed markets:")
            for mkt, info in matched.items():
                lines.append(f"      • {mkt}: {info}")

    # ── 1X2 odds ──────────────────────────────────────────────────────────────
    o1x2 = odds.get("1x2", {})
    if o1x2:
        h = o1x2.get("1","—"); x = o1x2.get("X","—"); a = o1x2.get("2","—")
        lines.append(f"\n`1:{h}  X:{x}  2:{a}`")

    # ── HT/FT odds ────────────────────────────────────────────────────────────
    htft = odds.get("htft", {})
    if htft:
        grid_order = ["1/1","1/X","1/2","X/1","X/X","X/2","2/1","2/X","2/2"]
        rows = []
        for i in range(0, 9, 3):
            cells = []
            for k in grid_order[i:i+3]:
                v = htft.get(k)
                if v: cells.append(f"{k}:{v}")
            if cells:
                rows.append("  " + "  ".join(cells))
        if rows:
            lines.append("*HT/FT odds*\n`" + "\n".join(rows) + "`")

    return "\n".join(lines)


def _build_matched_markets(fp_db: dict, home: str, away: str,
                             query_odds: dict) -> dict:
    """
    For each market in the current odds, check how often stored fingerprint
    records with similar odds produced each outcome.
    Returns dict of market descriptions with win rates.
    """
    fk_fwd = f"{home}|{away}".lower()
    fk_rev = f"{away}|{home}".lower()

    records = fp_db.get(fk_fwd, []) or fp_db.get(fk_rev, [])
    if not records:
        return {}

    n = len(records)
    matched = {}

    # 1X2
    outcomes = [r.get("outcome") for r in records if r.get("outcome")]
    if outcomes:
        for out in ("HOME", "DRAW", "AWAY"):
            pct = round(outcomes.count(out) / len(outcomes) * 100)
            if pct > 0:
                icon = {"HOME":"🏠","DRAW":"🤝","AWAY":"✈️"}[out]
                matched[f"1X2 {icon} {out}"] = f"{pct}% ({outcomes.count(out)}/{n})"

    # HT/FT
    htfts = [r.get("htft_result") for r in records if r.get("htft_result") and "?" not in r.get("htft_result","")]
    if htfts:
        from collections import Counter
        top3 = Counter(htfts).most_common(3)
        _t = {"1":"H","X":"D","2":"A"}
        for htft_k, cnt in top3:
            pct = round(cnt / len(htfts) * 100)
            parts = htft_k.split("/")
            if len(parts) == 2:
                label = f"{_t.get(parts[0],parts[0])}/{_t.get(parts[1],parts[1])}"
                matched[f"HT/FT {label}"] = f"{pct}% ({cnt}/{len(htfts)})"

    # BTTS
    btts_res = [r.get("btts_result") for r in records if r.get("btts_result") is not None]
    if btts_res:
        yes_pct = round(sum(btts_res) / len(btts_res) * 100)
        matched["BTTS"] = f"Yes {yes_pct}% / No {100-yes_pct}%"

    # O2.5
    ou25_res = [r.get("ou25_result") for r in records if r.get("ou25_result") is not None]
    if ou25_res:
        over_pct = round(sum(ou25_res) / len(ou25_res) * 100)
        matched["O/U 2.5"] = f"Over {over_pct}% / Under {100-over_pct}%"

    return matched


# ─── ROUND LIST FETCHER ───────────────────────────────────────────────────────
async def fetch_round_list(client, league_id: int, past: bool = False) -> list[dict]:
    """
    Fetch rounds from betPawa.
    Real API shape (confirmed):
      { items: [ { id: "137297", name: "#137297", rounds: [
          { id: "1179973", name: "01", tradingTime: { start: "...", end: "..." } },
          ...
      ] } ] }
    Returns flat list of round dicts each with: id, name, tradingTime, _seasonId, _seasonName
    """
    ep   = EP_PAST_ROUNDS if past else EP_ACTUAL_ROUNDS
    data = await _get(client, BASE + ep, params={"leagueId": league_id})
    if not data:
        return []

    items = data.get("items", data) if isinstance(data, dict) else data
    if not isinstance(items, list):
        return []

    rounds: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        sub = item.get("rounds")
        if isinstance(sub, list) and sub:
            # Nested: item is a season, sub is list of rounds
            season_id   = item.get("id")
            season_name = item.get("name", "")
            for r in sub:
                if not isinstance(r, dict): continue
                r["_seasonId"]   = season_id
                r["_seasonName"] = season_name
                rounds.append(r)
        else:
            # Flat: item is already a round
            rounds.append(item)

    # Sort: past = most-recent first (by id desc); actual = soonest first (by tradingTime)
    if past:
        rounds.sort(key=_round_id_int, reverse=True)
    else:
        rounds.sort(key=lambda r: (_round_start_ms(r), _round_id_int(r)))

    log.info(f"🗓️ fetch_round_list: {len(rounds)} rounds (past={past}, league={league_id})")
    if rounds:
        log.info(f"   First round: id={rounds[0].get('id')} name={rounds[0].get('name')} "
                 f"tradingTime={rounds[0].get('tradingTime')}")
    return rounds

def _pick_current_round(rounds: list[dict]) -> dict | None:
    """Pick live/soonest/most-recent round from actual list."""
    now_ms = int(time.time() * 1000)
    WINDOW = 5 * 60 * 1000  # 5 min per virtual match

    live = future = recent = None
    for r in rounds:
        st = _round_start_ms(r)
        if st == 0:
            continue
        if st <= now_ms < st + WINDOW:
            if live is None or st > _round_start_ms(live):
                live = r
        elif st > now_ms:
            if future is None or st < _round_start_ms(future):
                future = r
        else:
            if recent is None or st > _round_start_ms(recent):
                recent = r

    return live or future or recent or (rounds[0] if rounds else None)

# ─── EVENTS FETCHER ───────────────────────────────────────────────────────────
async def fetch_round_events(client, round_id, page: str) -> list[dict]:
    """Fetch events for a single round. round_id is the numeric round id e.g. 1179973."""
    url  = BASE + EP_EVENTS.format(round_id=round_id)
    data = await _get(client, url, params={"page": page})
    if data is None:
        log.error(f"❌ fetch_round_events: round={round_id} page={page} — no data returned")
        return []

    # Log raw structure to diagnose shape mismatches
    if isinstance(data, dict):
        items = data.get("items")
        log.info(f"📦 round={round_id} page={page} — raw keys={list(data.keys())[:10]}, "
                 f"items={'list['+str(len(items))+']' if isinstance(items,list) else type(items).__name__}")
        if isinstance(items, list) and items and page == PAGE_UPCOMING:
            # Only log detailed structure for upcoming page (the one that should have odds)
            first = items[0]
            if isinstance(first, dict):
                log.info(f"   first item keys={list(first.keys())[:12]}")
                mkts = first.get("markets", [])
                tmc  = first.get("totalMarketCount", "N/A")
                log.info(f"   first item: markets={len(mkts)}, totalMarketCount={tmc}, "
                         f"participants={len(first.get('participants',[]))}, "
                         f"hasScore={'HomeScore' in first or 'results' in first or 'score' in first}")
                if tmc == 0 or tmc == "N/A":
                    log.warning(f"   ⚠️  totalMarketCount={tmc} — betPawa not serving odds for this round/page")
    elif isinstance(data, list):
        log.info(f"📦 round={round_id} page={page} — raw list len={len(data)}")

    events = _flatten(data)
    if events:
        has_odds   = sum(1 for e in events if e.get("markets"))
        has_scores = sum(1 for e in events if _extract_score(e)[0] is not None)
        log.info(f"✅ round={round_id} page={page}: {len(events)} events, "
                 f"with_markets={has_odds}/{len(events)}, with_scores={has_scores}/{len(events)}")
        # Only deep-log odds when markets are actually present
        if has_odds > 0:
            _log_odds_debug(events[0], round_id)
        elif page == PAGE_UPCOMING:
            # Expected to have odds but doesn't — log first event keys as clue
            ev0 = events[0]
            tmc = ev0.get("totalMarketCount", "?")
            log.warning(f"⚠️  page=upcoming but NO markets — totalMarketCount={tmc} "
                        f"event_keys={list(ev0.keys())[:10]}")
    else:
        log.warning(f"⚠️  round={round_id} page={page}: NO EVENTS after flatten")
        if isinstance(data, dict):
            log.warning(f"   Full raw response: {str(data)[:600]}")
    return events


def _log_odds_debug(event: dict, round_id):
    """Spot-check odds extraction on one event."""
    parts = event.get("participants", [])
    names = [p.get("name","?") for p in parts]
    mkts  = event.get("markets", [])
    if not mkts:
        log.warning(f"🔍 round={round_id} {' v '.join(names)}: NO markets")
        return
    extracted = _extract_odds(event)
    x2 = extracted.get("1x2", {})
    if x2:
        log.info(f"✅ odds {' v '.join(names)}: 1X2={x2} btts={extracted.get('btts')} ou={len(extracted.get('ou',[]))}")
    else:
        # Only dump raw structure when extraction fails
        mkt0 = mkts[0]
        rows = mkt0.get("row") or []
        prices = rows[0].get("prices") if rows else []
        log.warning(f"❌ odds extraction FAILED {' v '.join(names)} — mkt0_keys={list(mkt0.keys())} prices={prices[:2]}")

# ─── PAST RESULTS FETCHER ─────────────────────────────────────────────────────
async def fetch_past_results(client, league_id: int, n: int = 3) -> list[tuple[str, list[dict]]]:
    """Fetch n completed rounds with scores."""
    rounds = await fetch_round_list(client, league_id, past=True)
    now_ms = int(time.time() * 1000)
    out    = []
    attempts = 0
    MAX_ATTEMPTS = 60

    for r in rounds:
        if len(out) >= n or attempts >= MAX_ATTEMPTS:
            break
        attempts += 1
        rid  = r.get("id") or r.get("gameRoundId")
        name = r.get("name") or str(rid)
        if not rid:
            continue

        events = await fetch_round_events(client, rid, PAGE_MATCHUPS)

        # Skip rounds whose events haven't started yet
        if events:
            first_ev = events[0]
            ev_st = _round_start_ms({"tradingTime": first_ev.get("tradingTime"),
                                     "startTime": first_ev.get("startTime")})
            if ev_st > 0 and ev_st > now_ms:
                log.info(f"⏩ skip future round {rid}")
                continue

        filtered = _filter_league(events, league_id)
        scored   = [e for e in filtered if _extract_score(e)[0] is not None]
        log.info(f"round={rid}: {len(filtered)} filtered, {len(scored)} scored")
        if scored:
            out.append((str(name), filtered))
    return out

# ─── NEXT ROUND FETCHER ───────────────────────────────────────────────────────
async def fetch_next_round(client, league_id: int):
    """
    Returns (round_name, round_id, season_id, events, has_scores) for the next round.

    CONFIRMED BEHAVIOUR:
    - page=upcoming  → has ODDS (markets) only for rounds NOT YET PLAYED
    - page=matchups  → has SCORES only for completed rounds, markets always empty
    - page=live      → active round, markets may be present

    Strategy:
    1. From the actual (non-past) round list, find the FIRST round whose
       tradingTime.end is still in the future — that's the one with odds.
    2. Fetch it on page=upcoming to get odds-rich events.
    3. Done. No merging needed — we only need odds for predictions.
       Scores come from past rounds via the learning job separately.
    """
    rounds = await fetch_round_list(client, league_id, past=False)
    if not rounds:
        return "", "", "", [], False

    now_ms = int(time.time() * 1000)

    # Walk sorted (soonest-first) list — find first round that hasn't ended
    nxt = None
    for r in rounds:
        te      = r.get("tradingTime", {})
        end_str = te.get("end") if isinstance(te, dict) else None
        et      = _iso_to_ms(end_str) if end_str else (_round_start_ms(r) + 5 * 60 * 1000)
        if et > now_ms:   # still in the future → this round has odds
            nxt = r
            log.info(f"🎯 [{league_id}] Next round with odds: "
                     f"id={r.get('id')} start={te.get('start','?')} end={end_str or '?'}")
            break

    if not nxt:
        log.warning(f"⚠️  [{league_id}] No future round found in actual list — all {len(rounds)} rounds are past")
        return "", "", "", [], False   # nothing to predict yet — wait for next season

    round_id    = str(nxt.get("id") or nxt.get("gameRoundId") or "")
    round_name  = str(nxt.get("name") or round_id)
    season_id   = str(nxt.get("_seasonId") or "")
    if not round_id:
        return round_name, "", "", [], False

    # Fetch upcoming page ONLY — this is the only page with odds
    log.info(f"📡 [{league_id}] Fetching page=upcoming for round {round_id}")
    events = await fetch_round_events(client, round_id, PAGE_UPCOMING)
    events = _filter_league(events, league_id)

    with_odds  = sum(1 for e in events if _extract_odds(e).get("1x2"))
    has_scores = any(_extract_score(e)[0] is not None for e in events)
    log.info(f"✅ [{league_id}] fetch_next_round: {len(events)} events, "
             f"with_1x2_odds={with_odds}/{len(events)}, has_scores={has_scores}")

    return round_name, round_id, season_id, events, has_scores

async def fetch_completed_round(client, league_id: int):
    """
    Returns (round_name, round_id, season_id, events, has_scores).

    Strategy — predict-then-confirm:
    - Fetch the UPCOMING round (has odds) for predictions
    - Also check the PREVIOUS round for scores
    - Return upcoming events with odds so predictions can be made
    - has_scores=True only when the PREVIOUS round has all scores confirmed

    This allows the bot to:
    1. Build predictions from upcoming odds (correct)
    2. Only POST once the previous round's scores are confirmed (no ⏳ pending)
    3. Show the just-confirmed round's results alongside its pre-match predictions

    The trick: we cache predictions for upcoming round_id, then when that
    round_id appears in past rounds with scores, we post it with results.
    """
    # Get the upcoming round (has odds for prediction)
    rounds_actual = await fetch_round_list(client, league_id, past=False)
    if not rounds_actual:
        return "", "", "", [], False

    now_ms = int(time.time() * 1000)

    # Find upcoming round (not yet ended)
    upcoming = None
    just_ended = None
    for i, r in enumerate(rounds_actual):
        te      = r.get("tradingTime", {})
        end_str = te.get("end") if isinstance(te, dict) else None
        et      = _iso_to_ms(end_str) if end_str else (_round_start_ms(r) + 5 * 60 * 1000)
        if et > now_ms:
            upcoming = r
            # The round just before this one just ended
            if i > 0:
                just_ended = rounds_actual[i - 1]
            break

    if not upcoming:
        return "", "", "", [], False

    upcoming_id   = str(upcoming.get("id") or upcoming.get("gameRoundId") or "")
    upcoming_name = str(upcoming.get("name") or upcoming_id)
    season_id     = str(upcoming.get("_seasonId") or "")

    # Fetch upcoming events WITH ODDS
    events = await fetch_round_events(client, upcoming_id, PAGE_UPCOMING)
    events = _filter_league(events, league_id)

    if not events:
        return upcoming_name, upcoming_id, season_id, [], False

    with_odds = sum(1 for e in events if _extract_odds(e).get("1x2"))

    # Check if the just-ended round has confirmed scores
    has_confirmed_scores = False
    prev_round_id        = ""
    prev_scores          = {}  # fixture_key → (score_h, score_a)

    # First try: just_ended from actual rounds list (normal case)
    # Fallback: most recent past round (fresh start / bot was offline)
    _check_round = None
    if just_ended:
        _check_round = just_ended
    else:
        past_rounds = await fetch_round_list(client, league_id, past=True)
        if past_rounds:
            _check_round = past_rounds[0]

    if _check_round:
        _check_id    = str(_check_round.get("id") or _check_round.get("gameRoundId") or "")
        score_events = await fetch_round_events(client, _check_id, PAGE_MATCHUPS)
        score_events = _filter_league(score_events, league_id)
        if score_events:
            has_confirmed_scores = all(
                _extract_score(e)[0] is not None for e in score_events
            )
            if has_confirmed_scores:
                prev_round_id = _check_id
                # Build score map for previous round — used to update old cards
                # Do NOT inject into upcoming events (those are not yet played)
                for sc in score_events:
                    nm = _norm_event(sc)
                    sh, sa = _extract_score(sc)
                    if sh is not None and sa is not None and nm["home"] and nm["away"]:
                        fk = _fixture_key(nm["home"], nm["away"])
                        prev_scores[fk]                          = (sh, sa)
                        prev_scores[f"{nm['home']}|{nm['away']}"] = (sh, sa)
                        prev_scores[f"{nm['away']}|{nm['home']}"] = (sa, sh)

    log.info(f"✅ [{league_id}] fetch_completed_round {upcoming_id}: "
             f"{len(events)} events, with_odds={with_odds}/{len(events)}, "
             f"confirmed={has_confirmed_scores}, prev_rid={prev_round_id}, "
             f"prev_scores={len(prev_scores)}")

    return upcoming_name, upcoming_id, season_id, events, has_confirmed_scores, prev_round_id, prev_scores


async def fetch_live_round(client, league_id: int):
    """
    Returns (round_name, season_name, events) for the currently-live round.
    Uses tradingTime window to find the live round.
    """
    rounds = await fetch_round_list(client, league_id, past=False)
    if not rounds:
        # Also try past rounds in case we're between seasons
        rounds = await fetch_round_list(client, league_id, past=True)
        if rounds:
            # Past rounds sorted most-recent first
            rounds = rounds[:20]  # only check recent ones

    nxt = _pick_current_round(rounds)
    if not nxt:
        return "", "", []

    round_id   = nxt.get("id") or nxt.get("gameRoundId")
    round_name = nxt.get("name") or str(round_id)
    season_name = nxt.get("_seasonName", "")

    log.info(f"🔴 fetch_live_round: id={round_id} name={round_name}")

    # Try upcoming first (has odds), then live (has live scores but may lack odds)
    # page=matchups strips markets from all rounds — never use it for odds
    events = await fetch_round_events(client, round_id, PAGE_UPCOMING)
    events = _filter_league(events, league_id)
    if not events:
        events = await fetch_round_events(client, round_id, PAGE_LIVE)
        events = _filter_league(events, league_id)

    with_odds = sum(1 for e in events if e.get("markets")) if events else 0
    log.info(f"🔴 [{league_id}] live round {round_id}: {len(events)} events, with_odds={with_odds}/{len(events)}")

    return str(round_name), str(season_name), events

# ─── STANDINGS FETCHER ────────────────────────────────────────────────────────
async def fetch_standings(client, league_id: int) -> tuple[str, str, list[dict]]:
    """
    Fetch league standings. Uses the season_id from the actual (current) season first,
    then falls back to past season.
    """
    # Try current season first (shows live standings)
    for past in (False, True):
        rounds = await fetch_round_list(client, league_id, past=past)
        if not rounds:
            continue

        season_id   = rounds[0].get("_seasonId")
        season_name = rounds[0].get("_seasonName", "")
        round_name  = rounds[0].get("name", "")

        if not season_id:
            log.warning(f"fetch_standings: no _seasonId in rounds (past={past})")
            continue

        # Try season_id from rounds, then +1 and +2 — betPawa sometimes serves
        # season N rounds while season N+1 is already active in the UI
        try:
            base_id    = int(season_id)
            candidates = [base_id, base_id + 1, base_id + 2]
        except (TypeError, ValueError):
            candidates = [season_id]

        data = None
        # Try multiple endpoint variants — betPawa changes these between deployments
        ep_variants = [
            "/api/sportsbook/virtual/v1/seasons/{sid}/standing",
            "/api/sportsbook/virtual/v1/seasons/{sid}/standings",
            "/api/sportsbook/virtual/v1/seasons/{sid}/leaderboard",
            "/api/sportsbook/virtual/v2/seasons/{sid}/standing",
        ]
        for sid in candidates:
            for ep_var in ep_variants:
                url  = BASE + ep_var.format(sid=sid)
                data = await _get(client, url, params={"leagueId": league_id})
                if data:
                    season_id = str(sid)
                    log.info(f"fetch_standings: found standings for season_id={sid} via {ep_var}")
                    break
            if data:
                break
            log.warning(f"fetch_standings: no data for season_id={sid}")

        if not data:
            continue

        # Normalise response
        items = data.get("items") or data.get("standings") or data.get("table") or []
        if isinstance(data, list):
            items = data

        rows = []
        for item in items:
            if not isinstance(item, dict):
                continue

            team_info = item.get("team") or item.get("participant") or item
            name = (team_info.get("name") or team_info.get("teamName") or
                    item.get("teamName") or item.get("name") or "?")
            if isinstance(name, dict):
                name = name.get("name") or "?"

            def _i(k, *alts):
                for key in (k, *alts):
                    v = item.get(key)
                    if v is not None:
                        try: return int(v)
                        except: pass
                return 0

            pts = _i("points", "pts", "Points")
            w   = _i("won", "wins", "w", "W")
            d   = _i("drawn", "draws", "d", "D")
            l   = _i("lost", "losses", "l", "L")
            gf  = _i("goalsFor", "scored", "goalsScored", "gf")
            ga  = _i("goalsAgainst", "conceded", "goalsConceded", "ga")
            pos = _i("position", "rank", "pos")

            form_raw = item.get("form") or item.get("recentForm") or []
            if isinstance(form_raw, str):
                form_raw = list(form_raw.upper())
            form = [str(f).upper()[:1] for f in form_raw if str(f).upper()[:1] in ("W","D","L")]

            rows.append(dict(pos=pos, name=name, pts=pts, w=w, d=d, l=l,
                             gf=gf, ga=ga, form=form[-5:]))

        if rows:
            if any(r["pos"] for r in rows):
                rows.sort(key=lambda r: r["pos"])
            else:
                rows.sort(key=lambda r: (-r["pts"], -(r["gf"] - r["ga"])))
                for i, r in enumerate(rows, 1):
                    r["pos"] = i
            log.info(f"✅ fetch_standings: {len(rows)} teams for season {season_id}")
            return season_name, round_name, rows

    return "", "", []

# ─── ALL RESULTS FETCHER ──────────────────────────────────────────────────────
async def fetch_all_results(client, league_id: int) -> list[tuple[str, list[dict]]]:
    rounds = await fetch_round_list(client, league_id, past=True)
    out    = []
    for r in rounds:
        rid  = r.get("id") or r.get("gameRoundId")
        name = r.get("name") or str(rid)
        if not rid: continue
        events = await fetch_round_events(client, rid, PAGE_MATCHUPS)
        if not events:
            events = await fetch_round_events(client, rid, PAGE_UPCOMING)
        filtered = _filter_league(events, league_id)
        scored   = [e for e in filtered if _extract_score(e)[0] is not None]
        if scored:
            out.append((str(name), filtered))
    return out

# ─── ANALYSIS ENGINE ──────────────────────────────────────────────────────────
# Weights for recency — most recent match counts most, but older games still matter
# Extended to 30 entries to cover deeper stat builds. Decay is gentler (0.92 per step).
RECENCY_WEIGHTS = [
    1.00, 0.92, 0.85, 0.78, 0.72, 0.66, 0.61, 0.56, 0.52, 0.48,
    0.44, 0.41, 0.38, 0.35, 0.32, 0.30, 0.28, 0.26, 0.24, 0.22,
    0.20, 0.19, 0.18, 0.17, 0.16, 0.15, 0.14, 0.13, 0.12, 0.11,
]

def _recency_w(idx: int, total: int) -> float:
    """idx=0 is oldest, idx=total-1 is newest."""
    rev = total - 1 - idx   # 0=newest
    return RECENCY_WEIGHTS[rev] if rev < len(RECENCY_WEIGHTS) else 0.10

def build_stats(events: list[dict]) -> dict[str, dict]:
    """
    Extended stats per team using plain dicts (pickle-safe for bot_data).
    """
    # First pass: collect ordered events per team to apply recency
    team_events: dict[str, list[dict]] = defaultdict(list)
    for raw in events:
        m = _norm_event(raw)
        if m["hs"] is None or m["as_"] is None:
            continue
        team_events[m["home"]].append({"ih": True,  "gf": m["hs"], "ga": m["as_"], "opp": m["away"]})
        team_events[m["away"]].append({"ih": False, "gf": m["as_"], "ga": m["hs"], "opp": m["home"]})

    # Second pass: collect H2H records using plain dicts
    h2h: dict[str, dict[str, dict]] = {}
    for raw in events:
        m = _norm_event(raw)
        if m["hs"] is None or m["as_"] is None:
            continue
        h = m["home"]; a = m["away"]
        if h not in h2h:
            h2h[h] = {}
        if a not in h2h[h]:
            h2h[h][a] = dict(hw=0, aw=0, d=0, hgf=0, hga=0, n=0, scorelines={})
        rec = h2h[h][a]
        rec["n"]   += 1
        rec["hgf"] += m["hs"]
        rec["hga"] += m["as_"]
        sl = f"{m['hs']}-{m['as_']}"
        rec["scorelines"][sl] = rec["scorelines"].get(sl, 0) + 1
        if m["hs"] > m["as_"]:    rec["hw"] += 1
        elif m["hs"] == m["as_"]: rec["d"]  += 1
        else:                      rec["aw"] += 1

    def _blank():
        return dict(
            p=0, w=0, d=0, l=0, gf=0, ga=0,
            hp=0, hw=0, hd=0, hl=0, hgf=0, hga=0,
            ap=0, aw=0, ad=0, al=0, agf=0, aga=0,
            cs=0, fts=0, form=[],
            wgf=0.0, wga=0.0, ww=0.0, wd=0.0, wl=0.0,
            scored_both=0, over25=0, goal_times=[], h2h={},
            # Exact scoreline frequency: {(hg,ag): count}
            scorelines={},
            # Home/away specific scorelines
            home_scorelines={}, away_scorelines={},
        )

    s: dict[str, dict] = {}

    for team, evs in team_events.items():
        if team not in s:
            s[team] = _blank()
        t = s[team]
        n = len(evs)
        for idx, e in enumerate(evs):
            gf, ga, ih = e["gf"], e["ga"], e["ih"]
            w_r = _recency_w(idx, n)

            t["p"] += 1; t["gf"] += gf; t["ga"] += ga
            t["goal_times"].append(gf + ga)

            if ga == 0: t["cs"]  += 1
            if gf == 0: t["fts"] += 1
            if gf > 0 and ga > 0: t["scored_both"] += 1
            if gf + ga >= 3:      t["over25"]      += 1

            if ih:
                t["hp"] += 1; t["hgf"] += gf; t["hga"] += ga
                if gf > ga:   t["hw"] += 1
                elif gf==ga:  t["hd"] += 1
                else:         t["hl"] += 1
            else:
                t["ap"] += 1; t["agf"] += gf; t["aga"] += ga
                if gf > ga:   t["aw"] += 1
                elif gf==ga:  t["ad"] += 1
                else:         t["al"] += 1

            t["wgf"] += gf * w_r
            t["wga"] += ga * w_r
            if gf > ga:   t["ww"] += w_r; t["form"].append("W")
            elif gf==ga:  t["wd"] += w_r; t["form"].append("D")
            else:         t["wl"] += w_r; t["form"].append("L")

            # Track exact scorelines (from team's perspective: gf-ga)
            sl_key = f"{gf}-{ga}"
            t["scorelines"][sl_key] = t["scorelines"].get(sl_key, 0) + 1
            if ih:
                t["home_scorelines"][sl_key] = t["home_scorelines"].get(sl_key, 0) + 1
            else:
                t["away_scorelines"][sl_key] = t["away_scorelines"].get(sl_key, 0) + 1

        t["form"] = t["form"][-5:]
        t["h2h"]  = h2h.get(team, {})

    return s


def _poisson_prob(lam: float, k: int) -> float:
    """P(X=k) for Poisson distribution."""
    if lam <= 0: return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)

def _match_probs_poisson(exp_h: float, exp_a: float) -> tuple[float, float, float]:
    """
    Use Poisson model to compute P(home win), P(draw), P(away win).
    Sum over scorelines up to 8 goals each side.
    """
    ph = pd_val = pa = 0.0
    for i in range(9):
        for j in range(9):
            p = _poisson_prob(exp_h, i) * _poisson_prob(exp_a, j)
            if i > j:   ph    += p
            elif i == j: pd_val += p
            else:        pa    += p
    total = ph + pd_val + pa or 1.0
    return ph/total, pd_val/total, pa/total


def _form_streak(form: list) -> float:
    """
    Returns a momentum multiplier based on current streak.
    All-W last 5 → +0.12 boost. All-L last 5 → -0.12 penalty.
    Mixed → proportional. Used to amplify recency signal.
    """
    if not form:
        return 0.0
    recent = form[-5:]
    pts    = sum({"W": 1, "D": 0.3, "L": -1}.get(r, 0) for r in recent)
    # Normalise to -0.12 … +0.12 range
    return round(max(-0.12, min(0.12, pts / len(recent) * 0.12)), 4)


def strength_score(st: dict) -> float:
    """
    Composite strength 0–100 using:
    - Recency-weighted win rate   (35%)
    - Recency-weighted attack     (20%)
    - Recency-weighted defence    (20%)
    - Raw win rate                (15%)
    - Form points last-5          (10%)
    """
    p    = st["p"] or 1
    wtot = st["ww"] + st["wd"] + st["wl"] or 1.0

    w_wr  = st["ww"] / wtot                                  # weighted win rate
    w_atk = min(st["wgf"] / wtot / 3.0, 1.0)                # weighted attack
    w_def = max(0.0, 1.0 - st["wga"] / wtot / 3.0)          # weighted defence

    raw_wr = st["w"] / p
    frm    = st["form"]
    fp     = sum({"W":3,"D":1,"L":0}.get(r,0) for r in frm) / (len(frm)*3 or 1)

    return round((w_wr*0.35 + w_atk*0.20 + w_def*0.20 + raw_wr*0.15 + fp*0.10)*100, 1)


def _home_advantage_factor(hst: dict) -> float:
    """How much better this team is at home vs overall. Range ~0.9–1.3."""
    p  = hst["p"]  or 1
    hp = hst["hp"] or 1
    overall_wr = hst["w"] / p
    home_wr    = hst["hw"] / hp
    if overall_wr < 0.05:
        return 1.0
    ratio = home_wr / overall_wr
    return max(0.8, min(1.4, ratio))


def _away_penalty_factor(ast: dict) -> float:
    """How much worse this team is away vs overall. Range ~0.7–1.1."""
    p  = ast["p"]  or 1
    ap = ast["ap"] or 1
    overall_wr = ast["w"] / p
    away_wr    = ast["aw"] / ap
    if overall_wr < 0.05:
        return 1.0
    ratio = away_wr / overall_wr
    return max(0.6, min(1.1, ratio))


def _h2h_adjustment(hst: dict, away: str) -> tuple[float, float]:
    """
    Returns (home_boost, away_boost) based on head-to-head record.
    Boost is additive to win probability, capped at ±0.08.
    """
    h2h = hst.get("h2h", {}).get(away)
    if not h2h or h2h["n"] < 2:
        return 0.0, 0.0
    n = h2h["n"]
    h_rate = h2h["hw"] / n
    a_rate = h2h["aw"] / n
    # Boost proportional to dominance, scaled by sample size confidence
    conf   = min(1.0, n / 6)
    h_boost = (h_rate - 0.40) * 0.20 * conf   # 0.40 = neutral baseline
    a_boost = (a_rate - 0.30) * 0.20 * conf   # 0.30 = neutral away baseline
    return (max(-0.08, min(0.08, h_boost)),
            max(-0.08, min(0.08, a_boost)))


def _expected_goals(hst: dict, ast: dict,
                     league_avg_goals: float = 2.5) -> tuple[float, float]:
    """
    Dixon-Coles-inspired xG, calibrated to learned league goal average.
    Uses attack/defence rates relative to the league mean so the model
    self-corrects as it discovers this league scores more/less than 2.5.
    """
    hp  = hst["hp"] or 1
    ap  = ast["ap"] or 1
    p_h = hst["p"]  or 1
    p_a = ast["p"]  or 1

    # Per-team rates
    h_att   = (hst["hgf"] / hp + hst["gf"] / p_h) / 2   # home attack
    a_def_w = (ast["aga"] / ap + ast["ga"] / p_a)  / 2   # away defence weakness
    a_att   = (ast["agf"] / ap + ast["gf"] / p_a)  / 2   # away attack
    h_def_w = (hst["hga"] / hp + hst["ga"] / p_h)  / 2   # home defence weakness

    # Raw xG
    raw_h = (h_att + a_def_w) / 2
    raw_a = (a_att + h_def_w) / 2

    # Calibrate to learned league average: scale so combined xG ≈ league_avg_goals
    combined = raw_h + raw_a or 1.0
    scale    = league_avg_goals / combined
    # Only apply scale if we have a meaningful learned average (not default 2.5)
    scale    = max(0.7, min(1.4, scale))   # never distort more than 40%
    exp_h = round(raw_h * scale, 2)
    exp_a = round(raw_a * scale, 2)

    # Floor/cap to realistic virtual football range
    exp_h = max(0.3, min(4.5, exp_h))
    exp_a = max(0.2, min(4.0, exp_a))
    return exp_h, exp_a


def _odds_to_prob(odds: float) -> float:
    """Convert decimal odds to implied probability, removing overround."""
    if not odds or odds <= 1.0:
        return 0.0
    return 1.0 / odds


def _remove_overround(raw_probs: dict) -> dict:
    """
    Normalise implied probabilities to remove bookmaker overround (margin).
    Returns dict with same keys, probabilities summing to 1.0.
    """
    total = sum(raw_probs.values())
    if total <= 0:
        return raw_probs
    return {k: v / total for k, v in raw_probs.items()}


def _odds_implied_probs(odds: dict) -> dict | None:
    """
    Extract fair win/draw/loss probabilities from betPawa's 1X2 odds.
    This is the single most reliable signal — betPawa's own pricing
    directly reflects the pre-seeded result algorithm.
    Returns None if odds not available.
    """
    o1x2 = odds.get("1x2", {})
    h_odd = o1x2.get("1")
    d_odd = o1x2.get("X")
    a_odd = o1x2.get("2")
    if not (h_odd and d_odd and a_odd):
        return None
    raw = {
        "hw": _odds_to_prob(h_odd),
        "dw": _odds_to_prob(d_odd),
        "aw": _odds_to_prob(a_odd),
    }
    return _remove_overround(raw)


def _ou_implied(odds: dict) -> dict:
    """Extract over/under 2.5 implied probability from betPawa odds."""
    result = {"over25": None, "under25": None}
    for side, line, price in odds.get("ou", []):
        if str(line) in ("2.5", "2"):
            if side == "O":
                result["over25"] = _odds_to_prob(price)
            elif side == "U":
                result["under25"] = _odds_to_prob(price)
    if result["over25"] and result["under25"]:
        total = result["over25"] + result["under25"]
        result["over25"] /= total
        result["under25"] /= total
    return result


def _btts_implied(odds: dict) -> float | None:
    """Extract BTTS Yes implied probability from betPawa odds."""
    btts = odds.get("btts", {})
    y = btts.get("Yes")
    n = btts.get("No")
    if not (y and n):
        return None
    yes_p = _odds_to_prob(y)
    no_p  = _odds_to_prob(n)
    total = yes_p + no_p
    return yes_p / total if total > 0 else None


def _validation_score(our_tip: str, o_hw: float, o_dw: float, o_aw: float,
                      s_hw: float, s_dw: float, s_aw: float,
                      p_hw: float, p_dw: float, p_aw: float,
                      odds_probs: dict | None) -> tuple[float, list[str]]:
    """
    Multi-pass validation: count how many independent models agree with our tip.
    Returns (agreement_score 0.0-1.0, list of agreeing signals).

    Signals checked:
      1. Odds-implied (betPawa's own pricing)     — weight 0.40
      2. Poisson xG model                         — weight 0.25
      3. Strength-ratio model                     — weight 0.20
      4. Ensemble (combined)                      — weight 0.15
    """
    tip_map = {"HOME WIN": "hw", "AWAY WIN": "aw", "DRAW / CLOSE": "dw"}
    key = tip_map.get(our_tip, "hw")

    signals   = []
    score     = 0.0

    # 1. betPawa odds signal (strongest — 40%)
    if odds_probs:
        op = odds_probs[key]
        if op == max(odds_probs.values()):
            signals.append("📊 betPawa odds agree")
            score += 0.40
        elif op > 0.38:
            signals.append("📊 betPawa odds lean same")
            score += 0.20

    # 2. Poisson model (25%)
    p_best = {"hw": p_hw, "dw": p_dw, "aw": p_aw}
    if p_best[key] == max(p_best.values()):
        signals.append("🔢 Poisson model agrees")
        score += 0.25

    # 3. Strength model (20%)
    s_best = {"hw": s_hw, "dw": s_dw, "aw": s_aw}
    if s_best[key] == max(s_best.values()):
        signals.append("💪 Strength model agrees")
        score += 0.20

    # 4. Ensemble (15%)
    e_best = {"hw": o_hw, "dw": o_dw, "aw": o_aw}
    if e_best[key] == max(e_best.values()):
        signals.append("🎯 Ensemble agrees")
        score += 0.15

    return round(score, 3), signals


def _compute_standings_from_fp_db(fp_db: dict, season_id: str = "",
                                   match_log: list | None = None) -> dict:
    """
    Build a live standings table by replaying ALL stored match results.

    PRIMARY source: match_log — a plain list of every result ever seen,
    stored in model["match_log"]. One entry per real match, never deduplicated
    by odds similarity. This gives exact standings matching BetPawa.

    FALLBACK: fp_db scan (legacy path, used if match_log is empty/None).
    fp_db is deduplicated by odds profile so it can undercount repeated results —
    this is why standings diverged from BetPawa before the match_log fix.

    season_id: if provided, only results from that season are counted.

    Returns dict: {team_name: {name, pos, pts, w, d, l, gf, ga, gd, form}}
    """
    table: dict[str, dict] = {}

    def _team(name: str) -> dict:
        if name not in table:
            table[name] = {
                "name": name, "pts": 0, "w": 0, "d": 0, "l": 0,
                "gf": 0, "ga": 0, "gd": 0, "form": [], "pos": 0,
                "played": 0,
            }
        return table[name]

    records_source = match_log if match_log else []

    if records_source:
        # ── PRIMARY PATH: match_log ──────────────────────────────────────────
        # Each entry: {home, away, score_h, score_a, round_id, season_id}
        # home/away are the ACTUAL home/away teams (not canonical).
        # score_h/score_a are goals for actual home/away.
        seen_rids: set[tuple] = set()
        for rec in records_source:
            if season_id:
                rec_season = str(rec.get("season_id", ""))
                # Only skip if BOTH sides are non-empty AND they differ.
                # Empty rec_season means "season unknown" — include in all views.
                if rec_season and rec_season != str(season_id):
                    continue
            rid = rec.get("round_id", 0)
            home = rec.get("home", "")
            away = rec.get("away", "")
            sh   = rec.get("score_h")
            sa   = rec.get("score_a")
            if sh is None or sa is None or not home or not away:
                continue
            dedup = (home, away, rid, sh, sa)
            if dedup in seen_rids:
                continue
            seen_rids.add(dedup)

            gh, ga = int(sh), int(sa)
            th = _team(home)
            ta = _team(away)
            th["gf"] += gh;  th["ga"] += ga
            ta["gf"] += ga;  ta["ga"] += gh
            th["played"] += 1
            ta["played"] += 1

            if gh > ga:
                th["pts"] += 3;  th["w"] += 1;  th["form"].append("W")
                ta["l"] += 1;                    ta["form"].append("L")
            elif gh == ga:
                th["pts"] += 1;  th["d"] += 1;  th["form"].append("D")
                ta["pts"] += 1;  ta["d"] += 1;  ta["form"].append("D")
            else:
                ta["pts"] += 3;  ta["w"] += 1;  ta["form"].append("W")
                th["l"] += 1;                    th["form"].append("L")

    else:
        # ── FALLBACK PATH: fp_db scan (legacy) ──────────────────────────────
        # WARNING: fp_db deduplicates by odds similarity — may undercount.
        # Only used when match_log is not yet populated (cold start / old data).
        seen: set[tuple] = set()
        for fk, records in fp_db.items():
            parts = fk.split("|")
            if len(parts) != 2:
                continue
            canon_home, canon_away = parts[0], parts[1]
            for rec in records:
                if season_id:
                    rec_season = str(rec.get("season_id", ""))
                    if rec_season and rec_season != str(season_id):
                        continue
                rid = rec.get("round_id", 0)
                sh  = rec.get("score_h")
                sa  = rec.get("score_a")
                if sh is None or sa is None:
                    continue
                dedup = (fk, rid, sh, sa)
                if dedup in seen:
                    continue
                seen.add(dedup)
                gh, ga = int(sh), int(sa)
                th = _team(canon_home)
                ta = _team(canon_away)
                th["gf"] += gh;  th["ga"] += ga
                ta["gf"] += ga;  ta["ga"] += gh
                th["played"] += 1;  ta["played"] += 1
                if gh > ga:
                    th["pts"] += 3;  th["w"] += 1;  th["form"].append("W")
                    ta["l"] += 1;                    ta["form"].append("L")
                elif gh == ga:
                    th["pts"] += 1;  th["d"] += 1;  th["form"].append("D")
                    ta["pts"] += 1;  ta["d"] += 1;  ta["form"].append("D")
                else:
                    ta["pts"] += 3;  ta["w"] += 1;  ta["form"].append("W")
                    th["l"] += 1;                    th["form"].append("L")

    if not table:
        return {}

    # Compute GD, trim form to last 5
    for t in table.values():
        t["gd"]   = t["gf"] - t["ga"]
        t["form"] = t["form"][-5:]

    # Sort: pts desc → gd desc → gf desc → ga asc → name desc
    # BetPawa confirmed tiebreaker order — when pts/gd/gf all equal,
    # fewer goals conceded ranks higher; if still tied, reverse alphabetical
    # (verified: LIV before EVE, both 2pts 0GD 2GF 2GA)
    ranked = sorted(
        table.values(),
        key=lambda r: (-r["pts"], -r["gd"], -r["gf"], r["ga"], [-ord(c) for c in r["name"]])
    )

    result = {}
    for i, r in enumerate(ranked):
        r["pos"] = i + 1
        result[r["name"]] = r

    n = len(result)
    if n > 0:
        log.debug(f"📊 Standings: {n} teams, source={'match_log' if match_log else 'fp_db'} (season={season_id!r})")

    return result


def _get_all_tiers(standings: dict) -> dict:
    """
    Classify teams by standings position:

    STRONG   = positions 1–5
    MODERATE = positions 11–15
    WEAK     = positions 16 to last (leagues vary: 18, 19, or 20 teams)

    Positions 6–10 are mid-table — not used for S-W matchup filtering.

    Returns dict: {team_name_lower: tier_str}
    """
    if not standings:
        return {}
    sorted_teams = sorted(standings.values(), key=lambda r: r.get("pos", 999))
    result = {}
    for i, r in enumerate(sorted_teams):
        pos = i + 1  # 1-indexed
        name = r["name"]
        if pos <= 5:
            tier = "STRONG"
        elif 11 <= pos <= 15:
            tier = "MODERATE"
        elif pos >= 16:
            tier = "WEAK"
        else:
            tier = "MID"  # positions 6–10 — not used

        # Index by full name (lowercase), 3-letter abbrev, first 3 chars, and raw name
        result[name.lower()] = tier
        result[name] = tier
        # First 3 chars (BetPawa short code: "WER" for Werder Bremen, "FCB" for FC Bayern)
        result[name[:3].lower()] = tier
        result[name[:3].upper()] = tier
        # Initials abbrev (e.g. "RBL" from "RB Leipzig")
        words = name.split()
        if len(words) >= 2:
            abbrev = "".join(w[0] for w in words if w)[:3].upper()
            result[abbrev.lower()] = tier
            result[abbrev] = tier
    return result


def _find_tier(team: str, tier_map: dict) -> str:
    """Fuzzy-match team name to tier. Returns STRONG/WEAK/MODERATE/MID/UNKNOWN."""
    if not tier_map:
        return "UNKNOWN"
    tl = team.lower().strip()
    # Exact match first
    if tl in tier_map:
        return tier_map[tl]
    # 3-letter code: check if tl matches start of any key
    for k, v in tier_map.items():
        if k.lower() == tl:
            return v
    # Short code (3 letters) vs full name: "wer" in "werder bremen" ✓
    # But require it matches the START of a word to avoid false matches
    if len(tl) <= 4:
        for k, v in tier_map.items():
            kl = k.lower()
            # Must start with the short code
            if kl.startswith(tl):
                return v
            # Or short code matches first letters of words (e.g. "rbl" → "rb leipzig")
            words = kl.split()
            abbrev = "".join(w[0] for w in words if w)[:len(tl)]
            if abbrev == tl:
                return v
    # Full name substring (longer names)
    if len(tl) > 4:
        for k, v in tier_map.items():
            if tl in k.lower() or k.lower() in tl:
                return v
    return "UNKNOWN"


def _get_standings_tiers(standings: dict, top_n: int = 5, bottom_n: int = 5
                          ) -> tuple[set[str], set[str]]:
    """Legacy helper — returns (strong_names, weak_names) sets."""
    tier_map = _get_all_tiers(standings)
    strong = {k for k, v in tier_map.items() if v == "STRONG"}
    weak   = {k for k, v in tier_map.items() if v == "WEAK"}
    return strong, weak


def _find_in_standings(name: str, standings: dict) -> dict | None:
    """Fuzzy-match a team name against standings dict keys."""
    if not standings:
        return None
    if name in standings:
        return standings[name]
    nl = name.lower()
    for k, v in standings.items():
        if nl == k.lower():
            return v
    for k, v in standings.items():
        if nl in k.lower() or k.lower() in nl:
            return v
    return None


def _is_strong_vs_weak(home: str, away: str, standings: dict,
                        top_n: int = 5, bottom_n: int = 5) -> bool:
    """
    Returns True when:
    - One team is STRONG and the other is WEAK, OR
    - One team is STRONG and the other is MODERATE
    (MODERATE = the 5 teams just above the WEAK bottom 5)

    NEAR_STRONG vs anyone = False (not posted).
    Both STRONG = False (not posted — too close to call).
    """
    if not standings:
        return False  # no standings → can't confirm S-W, don't tag or filter

    tier_map  = _get_all_tiers(standings)
    home_tier = _find_tier(home, tier_map)
    away_tier = _find_tier(away, tier_map)

    valid_pairs = {
        ("STRONG", "WEAK"),
        ("STRONG", "MODERATE"),
        ("WEAK",   "STRONG"),
        ("MODERATE", "STRONG"),
    }
    return (home_tier, away_tier) in valid_pairs


# ═══════════════════════════════════════════════════════════════════════════════
# ── STRATEGY ENGINE ────────────────────────────────────────────────────────────
# Implements the user's betting strategy:
#   Strong team that LOST last game → expected to recover → tip WIN
#   Weak team that WON last game    → expected to lose next → tip OPPONENT WIN
#
# Each prediction is cross-confirmed with historical learnt data.
# Only shown when strategy direction matches history direction.
# ═══════════════════════════════════════════════════════════════════════════════

def _strategy_get_last_game(team: str, model: dict) -> dict | None:
    """Get the most recent completed game for a team from match_log."""
    tl = team.lower()
    ml = model.get("match_log", [])
    games = []
    for entry in ml:
        eh = (entry.get("home") or "").lower()
        ea = (entry.get("away") or "").lower()
        sh = entry.get("score_h")
        sa = entry.get("score_a")
        if sh is None or sa is None:
            continue
        if tl in eh or eh.startswith(tl[:3]):
            games.append({
                "gf": sh, "ga": sa, "role": "HOME",
                "opponent": entry.get("away", ""),
                "opp_pos": None,
                "round_id": entry.get("round_id", 0),
            })
        elif tl in ea or ea.startswith(tl[:3]):
            games.append({
                "gf": sa, "ga": sh, "role": "AWAY",
                "opponent": entry.get("home", ""),
                "opp_pos": None,
                "round_id": entry.get("round_id", 0),
            })
    if not games:
        return None
    games.sort(key=lambda g: g["round_id"])
    g = games[-1]
    if g["gf"] > g["ga"]:   g["result"] = "WIN"
    elif g["gf"] == g["ga"]: g["result"] = "DRAW"
    else:                    g["result"] = "LOSS"
    return g


def _strategy_analyze_match(home: str, away: str,
                              standings: dict, model: dict,
                              hist_home_win_pct: float,
                              hist_away_win_pct: float,
                              hist_draw_pct: float,
                              hist_n: int,
                              hist_btts_pct: float,
                              ) -> dict | None:
    """
    Apply the user's strategy to a match.

    Returns a dict with:
      strategy_tip:   "HOME" | "AWAY" | "SKIP" | None
      market:         "1X2" | "DC" | "BTTS" | "SKIP"
      strategy_pct:   float 0-100
      history_pct:    float 0-100
      history_agrees: bool
      reason:         str explanation
      card_line:      str formatted card line

    Returns None if match doesn't qualify for strategy filter.
    """
    if not standings or not model:
        return None

    tier_map   = _get_all_tiers(standings)
    home_tier  = _find_tier(home, tier_map)
    away_tier  = _find_tier(away, tier_map)
    home_pos   = (standings.get(home) or {}).get("pos", 99)
    away_pos   = (standings.get(away) or {}).get("pos", 99)

    # Must involve at least one strong (1-5) or weak (16+) team
    if home_tier not in ("STRONG","WEAK") and away_tier not in ("STRONG","WEAK"):
        return None

    home_last = _strategy_get_last_game(home, model)
    away_last = _strategy_get_last_game(away, model)

    if not home_last or not away_last:
        return None

    # Skip draws — no direction
    if home_last["result"] == "DRAW" or away_last["result"] == "DRAW":
        return None

    home_lost = home_last["result"] == "LOSS"
    home_won  = home_last["result"] == "WIN"
    away_lost = away_last["result"] == "LOSS"
    away_won  = away_last["result"] == "WIN"

    # ── Get opponent positions from standings ─────────────────────────────────
    def _opp_pos(opp_name: str) -> int:
        for k, v in standings.items():
            if k.lower() == opp_name.lower() or k.lower().startswith(opp_name.lower()[:3]):
                return v.get("pos", 99)
        return 99

    home_last_opp_pos = _opp_pos(home_last.get("opponent",""))
    away_last_opp_pos = _opp_pos(away_last.get("opponent",""))

    # ── Strategy rules ────────────────────────────────────────────────────────
    strategy_tip = None
    market       = "1X2"
    strategy_pct = 0.0
    reason       = ""

    # STRONG team LOST → expect recovery
    if home_tier == "STRONG" and home_lost:
        if away_tier in ("WEAK", "MODERATE"):
            # Strong lost, plays weak/moderate → strong WIN
            # Check if same strength range
            if away_tier == "STRONG":
                market = "DC"; strategy_tip = "HOME"
                strategy_pct = 70.0
                reason = f"{home} pos{home_pos} STRONG lost, plays same-range {away} pos{away_pos}"
            else:
                strategy_tip = "HOME"
                # Goals context: how many goals in last game
                gap = abs(home_pos - away_pos)
                if home_last["gf"] == 0:
                    strategy_pct = 62.0  # scored nothing — slightly less confident
                else:
                    strategy_pct = 72.0
                if away_tier == "WEAK" and gap >= 10:
                    strategy_pct = min(strategy_pct + 8, 88.0)
                reason = (f"{home} pos{home_pos} STRONG lost {home_last['gf']}-{home_last['ga']} "
                          f"vs pos{home_last_opp_pos}, plays {away} pos{away_pos} {away_tier}")

    elif away_tier == "STRONG" and away_lost:
        if home_tier in ("WEAK", "MODERATE"):
            strategy_tip = "AWAY"
            gap = abs(away_pos - home_pos)
            if away_last["gf"] == 0:
                strategy_pct = 62.0
            else:
                strategy_pct = 72.0
            if home_tier == "WEAK" and gap >= 10:
                strategy_pct = min(strategy_pct + 8, 88.0)
            reason = (f"{away} pos{away_pos} STRONG lost {away_last['gf']}-{away_last['ga']} "
                      f"vs pos{away_last_opp_pos}, plays {home} pos{home_pos} {home_tier}")

    # WEAK team WON → check if opponent confirms it will lose
    elif home_tier == "WEAK" and home_won:
        if away_tier == "STRONG" and away_lost:
            # Weak won, strong also lost → strong recovers → AWAY WIN
            strategy_tip = "AWAY"
            strategy_pct = 68.0
            reason = (f"{home} pos{home_pos} WEAK won vs pos{home_last_opp_pos}, "
                      f"{away} pos{away_pos} STRONG lost — strong recovers")
        elif away_tier == "STRONG" and away_won:
            # Weak won, strong also won → strong confirmed → AWAY WIN straight
            strategy_tip = "AWAY"
            strategy_pct = 75.0
            reason = (f"{home} pos{home_pos} WEAK won vs pos{home_last_opp_pos}, "
                      f"{away} pos{away_pos} STRONG won — strong dominant")
        elif away_tier in ("MODERATE","WEAK"):
            # Both weak/moderate range → X2 or skip
            if abs(home_pos - away_pos) <= 2:
                return {"strategy_tip": "SKIP", "market": "SKIP",
                        "strategy_pct": 0, "history_pct": 0,
                        "history_agrees": False,
                        "reason": "Neighboring weak positions — too close",
                        "card_line": ""}
            strategy_tip = "AWAY"
            market = "DC"
            strategy_pct = 60.0
            reason = f"Weak {home} pos{home_pos} won, plays {away} pos{away_pos} — same range"

    elif away_tier == "WEAK" and away_won:
        if home_tier == "STRONG" and home_lost:
            strategy_tip = "HOME"
            strategy_pct = 68.0
            reason = (f"{away} pos{away_pos} WEAK won vs pos{away_last_opp_pos}, "
                      f"{home} pos{home_pos} STRONG lost — strong recovers")
        elif home_tier == "STRONG" and home_won:
            strategy_tip = "HOME"
            strategy_pct = 75.0
            reason = (f"{away} pos{away_pos} WEAK won, "
                      f"{home} pos{home_pos} STRONG won — strong dominant")
        elif home_tier in ("MODERATE","WEAK"):
            if abs(home_pos - away_pos) <= 2:
                return {"strategy_tip": "SKIP", "market": "SKIP",
                        "strategy_pct": 0, "history_pct": 0,
                        "history_agrees": False,
                        "reason": "Neighboring weak positions — too close",
                        "card_line": ""}
            strategy_tip = "HOME"
            market = "DC"
            strategy_pct = 60.0
            reason = f"Weak {away} pos{away_pos} won, plays {home} pos{home_pos} — same range"

    # BTTS check — when both teams showed goals in last game
    # and game context suggests both will score
    if strategy_tip in ("HOME","AWAY"):
        winner     = home if strategy_tip=="HOME" else away
        loser      = away if strategy_tip=="HOME" else home
        winner_last = home_last if strategy_tip=="HOME" else away_last
        loser_last  = away_last if strategy_tip=="HOME" else home_last

        # Both teams scored in last game + same strong range = BTTS
        if (winner_last["gf"] >= 1 and loser_last["gf"] >= 1 and
                home_tier == away_tier == "STRONG"):
            market = "BTTS"
            strategy_pct = max(strategy_pct - 5, 55.0)
            reason += " — both scored last game"

        # Weak team won with 3+ goals against strong → BTTS with strong opponent
        if (home_tier == "WEAK" and home_won and home_last["gf"] >= 3 and
                home_last_opp_pos <= 5):
            market = "BTTS"
            strategy_pct = 65.0
            reason += f" — weak team scored {home_last['gf']} vs strong"
        elif (away_tier == "WEAK" and away_won and away_last["gf"] >= 3 and
                away_last_opp_pos <= 5):
            market = "BTTS"
            strategy_pct = 65.0
            reason += f" — weak team scored {away_last['gf']} vs strong"

    if not strategy_tip or strategy_tip == "SKIP":
        return None

    # ── Cross-confirm with history ────────────────────────────────────────────
    history_pct = 0.0
    history_agrees = False

    if strategy_tip == "HOME":
        history_pct = hist_home_win_pct
        history_agrees = hist_home_win_pct > hist_away_win_pct and hist_home_win_pct > hist_draw_pct
    elif strategy_tip == "AWAY":
        history_pct = hist_away_win_pct
        history_agrees = hist_away_win_pct > hist_home_win_pct and hist_away_win_pct > hist_draw_pct

    if market == "BTTS":
        history_pct = hist_btts_pct
        history_agrees = hist_btts_pct >= 50.0
    elif market == "DC":
        if strategy_tip == "HOME":
            history_pct = hist_home_win_pct + hist_draw_pct
        else:
            history_pct = hist_away_win_pct + hist_draw_pct
        history_agrees = history_pct >= 50.0

    # Only show if history agrees
    if not history_agrees:
        return {
            "strategy_tip": strategy_tip,
            "market": market,
            "strategy_pct": round(strategy_pct, 1),
            "history_pct": round(history_pct, 1),
            "history_agrees": False,
            "reason": reason,
            "card_line": "",
        }

    # ── Format card line ──────────────────────────────────────────────────────
    mkt_label = {
        "1X2": f"{'1' if strategy_tip=='HOME' else '2'}",
        "DC":  f"{'1X' if strategy_tip=='HOME' else 'X2'}",
        "BTTS": "Yes",
    }.get(market, "?")

    card_line = (
        f"┆ 🎯 *Strategy*: ({market}) {mkt_label} "
        f"· {strategy_pct:.0f}% + {history_pct:.0f}%\n"
    )

    return {
        "strategy_tip":   strategy_tip,
        "market":         market,
        "mkt_label":      mkt_label,
        "strategy_pct":   round(strategy_pct, 1),
        "history_pct":    round(history_pct, 1),
        "history_agrees": True,
        "reason":         reason,
        "card_line":      card_line,
        "home_last":      home_last,
        "away_last":      away_last,
        "home_pos":       home_pos,
        "away_pos":       away_pos,
        "home_tier":      home_tier,
        "away_tier":      away_tier,
    }


def _strategy_brain_summary(bot_data: dict) -> str:
    """
    Generate strategy performance summary for Brain Status report.
    Tracks strategy predictions vs actual results.
    """
    stats = bot_data.get("strategy_stats", {})
    if not stats:
        return "📋 *Strategy Stats*: No data yet\n"

    total    = stats.get("total", 0)
    correct  = stats.get("correct", 0)
    skipped  = stats.get("skipped", 0)
    btts_tot = stats.get("btts_total", 0)
    btts_cor = stats.get("btts_correct", 0)
    dc_tot   = stats.get("dc_total", 0)
    dc_cor   = stats.get("dc_correct", 0)
    win_tot  = stats.get("win_total", 0)
    win_cor  = stats.get("win_correct", 0)

    acc     = round(correct/total*100) if total else 0
    b_acc   = round(btts_cor/btts_tot*100) if btts_tot else 0
    dc_acc  = round(dc_cor/dc_tot*100) if dc_tot else 0
    win_acc = round(win_cor/win_tot*100) if win_tot else 0

    lines = [
        f"📋 *Strategy Performance*",
        f"   Overall: {correct}/{total} = {acc}%",
        f"   1X2 WIN: {win_cor}/{win_tot} = {win_acc}%",
        f"   DC:      {dc_cor}/{dc_tot} = {dc_acc}%",
        f"   BTTS:    {btts_cor}/{btts_tot} = {b_acc}%",
        f"   Skipped (no history agree): {skipped}",
    ]
    return "\n".join(lines) + "\n"


def _strategy_update_stats(bot_data: dict, market: str,
                             strategy_tip: str, actual: str):
    """Update strategy stats after result is known."""
    stats = bot_data.setdefault("strategy_stats", {
        "total":0,"correct":0,"skipped":0,
        "btts_total":0,"btts_correct":0,
        "dc_total":0,"dc_correct":0,
        "win_total":0,"win_correct":0,
    })
    stats["total"] = stats.get("total",0) + 1

    if market == "1X2":
        stats["win_total"] = stats.get("win_total",0) + 1
        if strategy_tip == actual:
            stats["correct"]   = stats.get("correct",0) + 1
            stats["win_correct"] = stats.get("win_correct",0) + 1

    elif market == "DC":
        stats["dc_total"] = stats.get("dc_total",0) + 1
        # DC: HOME = 1X, AWAY = X2
        dc_ok = (strategy_tip=="HOME" and actual in ("HOME","DRAW")) or \
                (strategy_tip=="AWAY" and actual in ("AWAY","DRAW"))
        if dc_ok:
            stats["correct"]  = stats.get("correct",0) + 1
            stats["dc_correct"] = stats.get("dc_correct",0) + 1

    elif market == "BTTS":
        stats["btts_total"] = stats.get("btts_total",0) + 1
        if actual == "BTTS_YES":
            stats["correct"]     = stats.get("correct",0) + 1
            stats["btts_correct"] = stats.get("btts_correct",0) + 1


def _standings_signal(home: str, away: str,
                       standings: dict) -> tuple[float, float, float, float]:
    """
    Derive win probability adjustments from league standings data.
    Uses every available dimension: position, points, GD, form, W/D/L record.

    Returns (hw_adj, dw_adj, aw_adj, conf_bonus).
    Adjustments are ADDITIVE deltas to raw probabilities (e.g. +0.05 for home).
    conf_bonus is a confidence boost in percentage points (0–8).

    Dimensions used:
    1. Position gap       — table position difference between teams
    2. Points gap         — actual points difference (more reliable than position)
    3. Goal difference gap — net GD reflects attack/defence dominance
    4. Form gap           — last 5 results comparison (W=3, D=1, L=0 pts)
    5. Win rate gap       — total W/played ratio
    6. Head-to-head dominance from standings (if both in table)
    """
    if not standings:
        return 0.0, 0.0, 0.0, 0.0

    # Fuzzy match: try exact, then case-insensitive, then prefix
    def _find(name: str) -> dict | None:
        if name in standings:
            return standings[name]
        nl = name.lower()
        for k, v in standings.items():
            if k.lower() == nl:
                return v
        for k, v in standings.items():
            if k.lower().startswith(nl[:4]) or nl.startswith(k.lower()[:4]):
                return v
        return None

    h_row = _find(home)
    a_row = _find(away)

    if not h_row or not a_row:
        return 0.0, 0.0, 0.0, 0.0

    n_teams = len(standings)

    # ── 1. Position gap ────────────────────────────────────────────────────────
    # Normalise: position 1 = best. Invert so higher = better.
    h_pos_norm = (n_teams - h_row.get("pos", n_teams)) / max(n_teams - 1, 1)  # 0–1
    a_pos_norm = (n_teams - a_row.get("pos", n_teams)) / max(n_teams - 1, 1)
    pos_gap    = h_pos_norm - a_pos_norm   # +ve = home higher in table

    # ── 2. Points gap ─────────────────────────────────────────────────────────
    h_pts = h_row.get("pts", 0)
    a_pts = a_row.get("pts", 0)
    max_pts = max(h_pts, a_pts, 1)
    pts_gap = (h_pts - a_pts) / max_pts   # normalised -1 to +1

    # ── 3. Goal difference gap ────────────────────────────────────────────────
    h_gd = h_row.get("gf", 0) - h_row.get("ga", 0)
    a_gd = a_row.get("gf", 0) - a_row.get("ga", 0)
    max_gd = max(abs(h_gd), abs(a_gd), 1)
    gd_gap = (h_gd - a_gd) / (max_gd * 2)   # normalised -0.5 to +0.5

    # ── 4. Form gap from standings (last 5) ───────────────────────────────────
    def _form_pts(form_list: list) -> float:
        pts = sum({"W": 3, "D": 1, "L": 0}.get(str(r).upper()[:1], 0) for r in form_list)
        return pts / (len(form_list) * 3) if form_list else 0.5
    h_form_score = _form_pts(h_row.get("form", []))
    a_form_score = _form_pts(a_row.get("form", []))
    form_gap = h_form_score - a_form_score   # -1 to +1

    # ── 5. Win rate gap ───────────────────────────────────────────────────────
    h_played = (h_row.get("w", 0) + h_row.get("d", 0) + h_row.get("l", 0)) or 1
    a_played = (a_row.get("w", 0) + a_row.get("d", 0) + a_row.get("l", 0)) or 1
    h_wr = h_row.get("w", 0) / h_played
    a_wr = a_row.get("w", 0) / a_played
    wr_gap = h_wr - a_wr   # -1 to +1

    # ── 6. Goals scored gap (attack dominance) ────────────────────────────────
    h_gpg = h_row.get("gf", 0) / h_played   # goals per game
    a_gpg = a_row.get("gf", 0) / a_played
    max_gpg = max(h_gpg, a_gpg, 1.0)
    gpg_gap = (h_gpg - a_gpg) / max_gpg   # normalised -1 to +1

    # ── 7. Goals conceded gap (defence dominance) ─────────────────────────────
    h_gcpg = h_row.get("ga", 0) / h_played
    a_gcpg = a_row.get("ga", 0) / a_played
    max_gcpg = max(h_gcpg, a_gcpg, 1.0)
    # Home conceding less than away → home has better defence → positive for home
    gcpg_gap = (a_gcpg - h_gcpg) / max_gcpg   # +ve = home defence better

    # ── Composite standings edge ───────────────────────────────────────────────
    # Weighted combination of all 7 dimensions
    # Weights reflect reliability: pts_gap and pos_gap are most reliable
    edge = (
        pos_gap  * 0.20 +   # table position (20%)
        pts_gap  * 0.25 +   # points accumulated (25%)
        gd_gap   * 0.15 +   # goal difference (15%)
        form_gap * 0.15 +   # recent form last 5 (15%)
        wr_gap   * 0.10 +   # win rate overall (10%)
        gpg_gap  * 0.08 +   # goals scored rate (8%)
        gcpg_gap * 0.07     # goals conceded rate (7%)
    )  # edge range: roughly -1 to +1

    # ── Convert edge to probability adjustments ────────────────────────────────
    # Scale so maximum edge (~0.8+) gives ~12% probability shift
    # This keeps standings as a modifier, not a full override
    max_adj  = 0.12
    hw_adj   = edge * max_adj           # positive edge → home boost
    aw_adj   = -edge * max_adj          # negative edge → away boost
    dw_adj   = -abs(edge) * 0.04        # clear favourite → less draw chance

    # ── Confidence bonus: how decisive is the standings gap? ──────────────────
    # Only bonus when there's a meaningful clear gap (edge > 0.15)
    abs_edge   = abs(edge)
    conf_bonus = 0.0
    if abs_edge > 0.15:
        conf_bonus = min(8.0, (abs_edge - 0.15) * 25)   # up to +8 pts at edge=0.47

    return round(hw_adj, 4), round(dw_adj, 4), round(aw_adj, 4), round(conf_bonus, 2)


    def _consec_streak(form: list) -> float:
        """Positive = win streak, negative = loss streak. Scaled -0.10 to +0.10."""
        if not form: return 0.0
        last = form[-1]
        count = 0
        for r in reversed(form):
            if r == last: count += 1
            else: break
        if last == "W":   return min(0.10, count * 0.025)
        elif last == "L": return max(-0.10, -count * 0.025)
        return 0.0

    def _outcome_margin_boost(outcome_type: str) -> float:
        rec = ota.get(outcome_type, {})
        t   = rec.get("total", 0)
        c   = rec.get("correct", 0)
        if t < 10: return 0.0
        type_acc = c / t
        # If this outcome type accuracy < overall, raise its margin (be more careful)
        return max(0.0, (overall_acc - type_acc) * 15)  # up to +7.5 pts extra

    def _tip_of(h, d, a):
        if h >= a + 0.10 and h >= d + 0.07: return "HOME"
        if a >= h + 0.10 and a >= d + 0.07: return "AWAY"
        return "DRAW"


def predict_match(home: str, away: str, stats: dict,
                  odds: dict | None = None,
                  model: dict | None = None) -> dict:
    """
    Full multi-factor prediction with odds-first validation:
    1. betPawa's own odds → implied probabilities  (weight: 40%)
    2. Poisson xG model                            (weight: 25%)
    3. Strength-ratio model (recency-weighted)     (weight: 20%)
    4. H2H adjustment                              (weight: 15% via boost)
    5. Multi-pass validation — counts signal agreement
    6. Confidence = ensemble probability × validation agreement
    """
    blank = dict(p=0,w=0,d=0,l=0,gf=0,ga=0,
                 hp=0,hw=0,hd=0,hl=0,hgf=0,hga=0,
                 ap=0,aw=0,ad=0,al=0,agf=0,aga=0,
                 cs=0,fts=0,form=[],
                 wgf=0.0,wga=0.0,ww=0.0,wd=0.0,wl=0.0,
                 scored_both=0,over25=0,goal_times=[],h2h={})
    hst = stats.get(home, blank)
    ast = stats.get(away, blank)
    if odds is None:
        odds = {}
    if model is None:
        model = {}

    # ── Learned model params ─────────────────────────────────────────────────
    rounds_done   = model.get("rounds_learned", 0)
    outcome_acc   = model.get("outcome_acc",  0.0)
    league_avg_g  = model.get("avg_goals",    2.5)
    cum           = model.get("cumulative", {})
    cum_total     = cum.get("outcome_total", 0)
    cum_correct   = cum.get("outcome_correct", 0)
    cum_acc       = (cum_correct / cum_total) if cum_total > 0 else outcome_acc

    # ── VIRTUAL FOOTBALL: odds IS the server's probability model ──────────────
    # BetPawa virtual football generates outcomes from odds-implied probabilities.
    # The server draws results using exactly these probability weights.
    # → Odds must dominate (92%+). Poisson/Strength are real-football models
    #   that add noise, not signal, for computer-generated games.
    #
    # Learning role: track odds CALIBRATION (are implied probabilities honored?)
    # If odds say 60% and actual is 63% → small upward nudge for that band.
    sig_acc = model.get("signal_acc", {})
    live_acc = {}
    for sig in ("odds", "poisson", "strength"):
        sa = sig_acc.get(sig, {})
        t  = sa.get("total", 0)
        c  = sa.get("correct", 0)
        if t >= 10:
            live_acc[sig] = c / t
        else:
            live_acc[sig] = {"odds": 0.45, "poisson": 0.38, "strength": 0.36}.get(sig, 0.38)

    # Odds-dominant weights for virtual football
    # Base: odds=92%, residual=8% split between poisson+strength (tiny calibration)
    # After enough rounds: if odds_acc > 58%, further reduce residual → up to 97%
    odds_acc = live_acc["odds"]
    if cum_total >= 50 and odds_acc > 0.57:
        base_odds_w = min(0.97, 0.92 + (odds_acc - 0.57) * 1.25)
    else:
        base_odds_w = 0.92
    residual = 1.0 - base_odds_w
    w_odds     = base_odds_w
    w_poisson  = residual * 0.35   # tiny calibration residual
    w_strength = residual * 0.65   # tiny calibration residual

    # ── 1. betPawa odds → implied probabilities (primary signal) ─────────────
    odds_probs = _odds_implied_probs(odds)
    ou_imp     = _ou_implied(odds)
    btts_imp   = _btts_implied(odds)

    # ── 2. Strength scores + form streak ─────────────────────────────────────
    hsc = strength_score(hst)
    asc = strength_score(ast)
    h_streak = _form_streak(hst.get("form", []))
    a_streak = _form_streak(ast.get("form", []))

    # ── NEW: Recent-3 form (last 3 games) vs overall — momentum signal ────────
    h_form = hst.get("form", [])
    a_form = ast.get("form", [])
    h_recent3 = h_form[-3:] if len(h_form) >= 3 else h_form
    a_recent3 = a_form[-3:] if len(a_form) >= 3 else a_form
    h_hot = sum(1 for x in h_recent3 if x == "W") / max(len(h_recent3), 1)  # 0-1
    a_hot = sum(1 for x in a_recent3 if x == "W") / max(len(a_recent3), 1)  # 0-1

    # Consecutive streak: how many W/L in a row (for both teams)
    def _consec_streak(form: list) -> float:
        """Positive = win streak, negative = loss streak. Scaled -0.10 to +0.10."""
        if not form: return 0.0
        last = form[-1]
        count = 0
        for r in reversed(form):
            if r == last: count += 1
            else: break
        if last == "W":   return min(0.10, count * 0.025)
        elif last == "L": return max(-0.10, -count * 0.025)
        return 0.0

    h_consec = _consec_streak(h_form)
    a_consec = _consec_streak(a_form)

    # ── 3. Strength-ratio model (with form momentum + recent-3 + consec streak) ─
    ha_factor  = _home_advantage_factor(hst)
    ap_factor  = _away_penalty_factor(ast)
    # Combine streak signals: EMA streak + consecutive + recent-3 hot streak
    h_momentum = h_streak + h_consec + (h_hot - 0.5) * 0.06
    a_momentum = a_streak + a_consec + (a_hot - 0.5) * 0.06
    hsc_adj    = (hsc + h_momentum * 100) * ha_factor
    asc_adj    = (asc + a_momentum * 100) * ap_factor
    base_total = hsc_adj + 8 + asc_adj or 1
    s_hw = (hsc_adj + 8) / base_total
    s_aw = asc_adj / base_total
    s_dw = max(0.0, 1.0 - s_hw - s_aw)
    _st  = s_hw + s_dw + s_aw or 1
    s_hw /= _st; s_dw /= _st; s_aw /= _st

    # ── 4. Poisson model (calibrated to learned league avg goals) ─────────────
    exp_h, exp_a = _expected_goals(hst, ast, league_avg_g)
    p_hw, p_dw, p_aw = _match_probs_poisson(exp_h, exp_a)

    # Per-signal tips (for learning feedback)
    def _tip_of(h, d, a):
        if h >= a + 0.10 and h >= d + 0.07: return "HOME"
        if a >= h + 0.10 and a >= d + 0.07: return "AWAY"
        return "DRAW"
    odds_tip     = _tip_of(*odds_probs.values()) if odds_probs else None
    poisson_tip  = _tip_of(p_hw, p_dw, p_aw)
    strength_tip = _tip_of(s_hw, s_dw, s_aw)

    # ── 5. Weighted ensemble (odds weight grows with confirmed experience) ─────
    if odds_probs:
        w_rem = max(0.0, 1.0 - w_odds - w_poisson - w_strength)
        hw = (w_odds     * odds_probs["hw"] +
              w_poisson  * p_hw +
              w_strength * s_hw +
              w_rem      * (odds_probs["hw"] * 0.5 + p_hw * 0.3 + s_hw * 0.2))
        dw = (w_odds     * odds_probs["dw"] +
              w_poisson  * p_dw +
              w_strength * s_dw +
              w_rem      * (odds_probs["dw"] * 0.5 + p_dw * 0.3 + s_dw * 0.2))
        aw = (w_odds     * odds_probs["aw"] +
              w_poisson  * p_aw +
              w_strength * s_aw +
              w_rem      * (odds_probs["aw"] * 0.5 + p_aw * 0.3 + s_aw * 0.2))
    else:
        r = w_poisson + w_strength or 1.0
        hw = (w_poisson/r) * p_hw + (w_strength/r) * s_hw
        dw = (w_poisson/r) * p_dw + (w_strength/r) * s_dw
        aw = (w_poisson/r) * p_aw + (w_strength/r) * s_aw

    # ── 6. H2H adjustment (suppressed for virtual football) ──────────────────
    # In virtual football, team pairings reset each round - H2H history from
    # computer-generated games doesn't reflect real team quality differences.
    # Apply only 5% of H2H signal to avoid adding noise.
    h_boost, a_boost = _h2h_adjustment(hst, away)
    hw = max(0.01, hw + h_boost * 0.05)
    aw = max(0.01, aw + a_boost * 0.05)
    dw = max(0.01, dw - (h_boost + a_boost) * 0.05 * 0.5)

    # Re-normalise
    tot = hw + dw + aw or 1.0
    hw /= tot; dw /= tot; aw /= tot

    # ── 6b. Apply self-learned pattern adjustments ────────────────────────────
    hw, dw, aw = _apply_learned_model(home, away, hw, dw, aw, model)

    # ── 6c. ALGORITHM REVERSE-ENGINEERING: apply PRNG pattern signals ─────────
    # Three engines: rebalancing detector, fixture cycle, round-ID modulo
    round_id_int   = model.get("_current_round_id", 0)
    match_position = model.get("_match_position", 0)  # position within round
    hw, dw, aw, _algo_bonus = _apply_algo_signals(hw, dw, aw, model, round_id_int, home, away)

    # ── Engine 4: per-slot pattern signal ────────────────────────────────────
    slot_pred, slot_conf = _get_slot_signal(model, match_position)
    if slot_pred and slot_conf >= 0.78:
        slot_weight = (slot_conf - 0.75) * 2   # 0.78 → 0.06, 0.90 → 0.30
        if slot_pred == "HOME":
            hw = hw * (1 - slot_weight) + 1.0 * slot_weight
            dw = dw * (1 - slot_weight)
            aw = aw * (1 - slot_weight)
        elif slot_pred == "DRAW":
            dw = dw * (1 - slot_weight) + 1.0 * slot_weight
            hw = hw * (1 - slot_weight)
            aw = aw * (1 - slot_weight)
        else:
            aw = aw * (1 - slot_weight) + 1.0 * slot_weight
            hw = hw * (1 - slot_weight)
            dw = dw * (1 - slot_weight)
        _t = hw + dw + aw or 1.0
        hw /= _t; dw /= _t; aw /= _t
        _algo_bonus += (slot_conf - 0.75) * 20   # up to +3 pts extra

    # ── Engine 5: odds trap correction ───────────────────────────────────────
    # When a specific odds band has historically been wrong more than expected,
    # reduce that outcome's probability. Built up from _update_odds_trap calls.
    if model and odds_probs:
        _imp_h = odds_probs.get("hw", 0.45)
        _trap_h = _get_odds_trap_penalty(model, _imp_h, "HOME")
        _trap_d = _get_odds_trap_penalty(model, _imp_h, "DRAW")
        _trap_a = _get_odds_trap_penalty(model, _imp_h, "AWAY")
        if _trap_h > 0:
            hw = max(0.05, hw - _trap_h)
            dw += _trap_h * 0.5; aw += _trap_h * 0.5
        if _trap_d > 0:
            dw = max(0.05, dw - _trap_d)
            hw += _trap_d * 0.5; aw += _trap_d * 0.5
        if _trap_a > 0:
            aw = max(0.05, aw - _trap_a)
            hw += _trap_a * 0.5; dw += _trap_a * 0.5
        _t = hw + dw + aw or 1.0
        hw /= _t; dw /= _t; aw /= _t

    hw_pct = round(hw * 100, 1)
    dw_pct = round(dw * 100, 1)
    aw_pct = round(aw * 100, 1)

    # ── 7. Tip selection — margins adapt from learned experience ─────────────
    # Base margins tighten as accuracy improves (more selective = more accurate)
    # and widen when accuracy is low (be more conservative)
    # Target: converge toward picking only the clearest cases → 100% accuracy
    recent_acc  = model.get("recent_10_acc", 0.0) if model else 0.0
    overall_acc = cum_acc if cum_total >= 10 else 0.50

    # How well are we doing? Above 60% = tighten margins (be more selective)
    # Below 50% = widen margins (only pick when really clear)
    acc_for_margin = recent_acc if model.get("recent_10_total", 0) >= 20 else overall_acc
    margin_adjust  = (acc_for_margin - 0.50) * 20   # -10 to +10 points

    # Per-outcome type confidence: if DRAW picks have been wrong a lot, raise their bar
    ota = model.get("outcome_type_acc", {}) if model else {}
    def _outcome_margin_boost(outcome_type: str) -> float:
        rec = ota.get(outcome_type, {})
        t   = rec.get("total", 0)
        c   = rec.get("correct", 0)
        if t < 10: return 0.0
        type_acc = c / t
        # If this outcome type accuracy < overall, raise its margin (be more careful)
        return max(0.0, (overall_acc - type_acc) * 15)  # up to +7.5 pts extra

    # Dynamic thresholds
    win_margin  = max(8.0,  12.0 + margin_adjust)   # 7→15 range
    draw_margin = max(5.0,   8.0 + margin_adjust)   # 4→12 range
    draw_thresh = max(28.0, 32.0 - margin_adjust)   # 22→38 range

    home_bar = win_margin  + _outcome_margin_boost("HOME")
    away_bar = win_margin  + _outcome_margin_boost("AWAY")
    draw_bar = draw_margin + _outcome_margin_boost("DRAW")

    if hw_pct >= aw_pct + home_bar and hw_pct >= dw_pct + draw_bar:
        tip, icon = "HOME WIN",     "🏠"
    elif aw_pct >= hw_pct + away_bar and aw_pct >= dw_pct + draw_bar:
        tip, icon = "AWAY WIN",     "✈️"
    elif dw_pct >= draw_thresh and abs(hw_pct - aw_pct) <= 10:
        tip, icon = "DRAW / CLOSE", "🤝"
    elif hw_pct > aw_pct:
        tip, icon = "HOME WIN",     "🏠"
    else:
        tip, icon = "AWAY WIN",     "✈️"

    # ── 8. Multi-pass validation ──────────────────────────────────────────────
    val_score, val_signals = _validation_score(
        tip,
        hw, dw, aw,
        s_hw, s_dw, s_aw,
        p_hw, p_dw, p_aw,
        odds_probs,
    )

    # ── Confidence: anchored to REAL learned accuracy of the winning signal ─────
    # ── Confidence for virtual football: anchor to odds-implied probability ──
    # Since odds IS the server's probability, the implied probability of our
    # chosen outcome is the most honest confidence measure.
    # We then nudge it slightly up/down based on learned calibration data.
    raw_conf  = max(hw, dw, aw) * 100

    # For virtual football, signal agreement is less meaningful since poisson/strength
    # are noisy signals. Use only odds agreement as the primary check.
    tip_outcome = tip.split()[0]   # "HOME", "AWAY", or "DRAW"
    agreeing_signals = []
    agreeing_acc     = []
    for sig, sig_tip in [("odds", odds_tip), ("poisson", poisson_tip), ("strength", strength_tip)]:
        if sig_tip == tip_outcome:
            agreeing_signals.append(sig)
            agreeing_acc.append(live_acc.get(sig, 0.38))

    n_agree = len(agreeing_signals)
    agree_acc = (sum(agreeing_acc) / len(agreeing_acc)) if agreeing_acc else cum_acc

    # Confidence = odds-implied probability (raw_conf), lightly adjusted by calibration
    # Don't blend heavily with historical signal accuracy — that adds noise for virtual
    data_maturity = min(1.0, cum_total / 300)
    blended_conf = (
        raw_conf * (1.0 - data_maturity * 0.15) +   # odds prob dominates (85%+)
        agree_acc * 100 * (data_maturity * 0.15)     # tiny calibration anchor (max 15%)
    )

    # Agreement multiplier: only odds agreement truly matters for virtual football
    # Poisson/strength agreement is coincidental, not meaningful
    odds_agrees = (odds_tip == tip_outcome)
    agree_mult = 1.05 if odds_agrees else 0.95  # small nudge only

    all_agree_bonus = 0.0  # removed for virtual football

    # ── Pattern lock bonus ────────────────────────────────────────────────────
    prior = _pattern_prior(model, home, away)
    lock_bonus = 0.0
    if prior and prior["n"] >= 5:
        dominant = max(prior["hw"], prior["dw"], prior["aw"])
        if dominant >= 0.75:
            lock_bonus = (dominant - 0.75) * 20

    conf = round(min(97.0, blended_conf * agree_mult + all_agree_bonus + lock_bonus + _algo_bonus), 1)

    # ── Margin band calibration: what % of picks at THIS confidence level were correct? ──
    margin_acc  = model.get("margin_acc", {}) if model else {}
    conf_bucket = str(int(conf // 5) * 5)
    band_rec    = margin_acc.get(conf_bucket, [0, 0])
    if band_rec[1] >= 10:
        band_acc    = band_rec[0] / band_rec[1]
        # If this band has been right 70%+, small boost. If below 45%, pull down.
        band_nudge  = (band_acc - 0.55) * 15   # -1.5 to +6.75 pts
        conf        = round(min(97.0, max(50.0, conf + band_nudge)), 1)

    # ── Learning velocity bonus: recent 10 rounds doing better than overall? ──
    recent_10_acc = model.get("recent_10_acc", 0.0) if model else 0.0
    r10_total     = model.get("recent_10_total", 0) if model else 0
    if r10_total >= 20 and recent_10_acc > cum_acc + 0.03:
        # Bot is improving — reward slightly higher confidence
        conf = round(min(97.0, conf + (recent_10_acc - cum_acc) * 10), 1)

    # ── High-confidence mistake penalty: if bot keeps being wrong when sure ──
    hcm = model.get("high_conf_mistakes", 0) if model else 0
    if hcm >= 5 and conf >= 75:
        # Too many surprise mistakes at high confidence — trim the ceiling
        hcm_penalty = min(8.0, (hcm - 4) * 0.8)
        conf = round(max(60.0, conf - hcm_penalty), 1)

    # ── Calibration correction: learned from own over/underconfidence history ──
    calib = model.get("conf_calibration", 0.0) if model else 0.0
    if calib > 3:
        conf = round(max(50.0, conf - calib * 0.30), 1)
    elif calib < -3:
        conf = round(min(97.0, conf - calib * 0.15), 1)

    # ── 9. Side markets — use odds if available, else blend stats + learned rates ─
    p  = hst["p"] or 1
    pa = ast["p"] or 1
    learned_btts_rate   = model.get("btts_rate",   None)
    learned_over25_rate = model.get("over25_rate", None)

    # Confidence in learned rates grows with matches seen.
    # At 50 matches: 40% learned, 60% per-team stats.
    # At 500+ matches: 70% learned league rate, 30% per-team stats.
    _mt          = model.get("cumulative", {}).get("matches_total", 0)
    rate_conf    = min(0.70, 0.40 + (_mt / 1500) * 0.30)
    stat_conf    = 1.0 - rate_conf

    # ── O2.5 ─────────────────────────────────────────────────────────────────
    # Learned O2.5 accuracy directly adjusts the final probability.
    # If the bot has been right on O2.5 65% of the time, predictions shift toward that.
    learned_o25_acc = None
    cum_o25_t = cum.get("over25_total", 0)
    cum_o25_c = cum.get("over25_correct", 0)
    if cum_o25_t >= 20:
        learned_o25_acc = cum_o25_c / cum_o25_t   # e.g. 0.63

    if ou_imp.get("over25") is not None:
        # Odds-based: blend with learned rate when data is mature
        odds_o25 = ou_imp["over25"]
        if learned_over25_rate is not None and _mt >= 50:
            # Weight shifts to learned rate as data grows
            lr_w = min(0.55, 0.30 + (_mt / 1500) * 0.25)
            over25_prob = round(((1 - lr_w) * odds_o25 + lr_w * learned_over25_rate) * 100, 1)
        else:
            over25_prob = round(odds_o25 * 100, 1)
        # Calibrate: if our O2.5 accuracy is known, nudge toward what actually happens
        if learned_o25_acc is not None:
            actual_lean = learned_over25_rate if learned_over25_rate else 0.50
            calib_nudge = (actual_lean - 0.50) * 10  # ±5 pts max
            over25_prob = round(min(95.0, max(20.0, over25_prob + calib_nudge)), 1)
    else:
        stat_over = ((hst["over25"] / p) + (ast["over25"] / pa)) / 2
        if learned_over25_rate is not None and rounds_done >= 3:
            over25_prob = round((stat_conf * stat_over + rate_conf * learned_over25_rate) * 100, 1)
        else:
            over25_prob = round(stat_over * 100, 1)

    # ── BTTS ─────────────────────────────────────────────────────────────────
    learned_btts_acc = None
    cum_btts_t = cum.get("btts_total", 0)
    cum_btts_c = cum.get("btts_correct", 0)
    if cum_btts_t >= 20:
        learned_btts_acc = cum_btts_c / cum_btts_t

    if btts_imp is not None:
        odds_btts = btts_imp
        if learned_btts_rate is not None and _mt >= 50:
            lr_w = min(0.55, 0.30 + (_mt / 1500) * 0.25)
            btts_prob = round(((1 - lr_w) * odds_btts + lr_w * learned_btts_rate) * 100, 1)
        else:
            btts_prob = round(odds_btts * 100, 1)
        if learned_btts_acc is not None:
            actual_lean = learned_btts_rate if learned_btts_rate else 0.50
            calib_nudge = (actual_lean - 0.50) * 10
            btts_prob = round(min(95.0, max(20.0, btts_prob + calib_nudge)), 1)
    else:
        stat_btts = ((hst["scored_both"] / p) + (ast["scored_both"] / pa)) / 2
        if learned_btts_rate is not None and rounds_done >= 3:
            btts_prob = round((stat_conf * stat_btts + rate_conf * learned_btts_rate) * 100, 1)
        else:
            btts_prob = round(stat_btts * 100, 1)

    # Odds display
    o1x2 = odds.get("1x2", {})

    # ── Enrich with fixture case, momentum, fingerprint ──────────────────────
    # These power the card format — history rates, verdict, momentum, fire badge
    _fixture_case  = {}
    _home_momentum = {}
    _away_momentum = {}
    _fp_result     = {}
    _dom_htft      = None
    _confirm_checks = []
    _confirm_rate   = 0.0

    if model:
        fp_db = model.get("fingerprint_db", {})

        # Build tier_map from standings cache if available,
        # otherwise compute from fp_db match history as fallback
        _st = model.get("_standings_cache", {})
        if _st:
            tier_map = _get_all_tiers(_st)
        else:
            tier_map = {}

        # Fixture investigation (history, verdict, markets)
        try:
            _fixture_case = _investigate_fixture(home, away, model, fp_db) or {}
        except Exception as _e:
            log.debug(f"_investigate_fixture error: {_e}")
            _fixture_case = {}

        # Team momentum — reads from match_log directly in model
        try:
            _home_momentum = _compute_team_momentum(home, model, tier_map) or {}
        except Exception as _e:
            log.debug(f"momentum home error: {_e}")
            _home_momentum = {}
        try:
            _away_momentum = _compute_team_momentum(away, model, tier_map) or {}
        except Exception as _e:
            log.debug(f"momentum away error: {_e}")
            _away_momentum = {}

        # Best fingerprint match (fire badge, dominant HT/FT)
        try:
            _fp_result = _find_best_fingerprint(fp_db, home, away, odds) or {}
            _dom_htft  = _fp_result.get("dominant_htft")
        except Exception as _e:
            log.debug(f"fingerprint error: {_e}")
            _fp_result = {}

        # Confirmation signals
        _checks = []
        _tip_out = tip.split()[0]
        if odds_probs:
            _odds_tip_out = ("HOME" if odds_probs["hw"] >= odds_probs["aw"] + 0.08
                             else "AWAY" if odds_probs["aw"] >= odds_probs["hw"] + 0.08
                             else "DRAW")
            _checks.append(("odds", _odds_tip_out == _tip_out))
        if _fp_result.get("n_samples", 0) >= 3:
            _checks.append(("fp", _fp_result.get("dominant_outcome") == _tip_out))
        if _home_momentum.get("games_used", 0) >= 3 and _away_momentum.get("games_used", 0) >= 3:
            _hwin = _home_momentum.get("win_pct", 50)
            _awin = _away_momentum.get("win_pct", 50)
            if _tip_out == "HOME":
                _checks.append(("mom", _hwin > _awin + 10))
            elif _tip_out == "AWAY":
                _checks.append(("mom", _awin > _hwin + 10))
        if _fixture_case.get("n_meetings", 0) >= 3:
            _fc_tip = _fixture_case.get("verdict_tip")
            if _fc_tip:
                _checks.append(("history", _fc_tip == _tip_out))
        _confirm_checks = _checks
        _confirm_rate   = (sum(1 for _, ok in _checks if ok) / len(_checks)
                           if _checks else 0.0)

    return dict(
        hsc=hsc, asc=asc,
        hw=hw_pct, dw=dw_pct, aw=aw_pct,
        exp_h=exp_h, exp_a=exp_a,
        tip=tip, icon=icon, conf=conf,
        btts=btts_prob, over25=over25_prob,
        h2h=hst.get("h2h", {}).get(away),
        val_score=val_score,
        val_signals=val_signals,
        odds_available=odds_probs is not None,
        bp_odds=o1x2,
        odds_tip=odds_tip,
        poisson_tip=poisson_tip,
        strength_tip=strength_tip,
        # Enriched fields for card display
        fixture_case=_fixture_case,
        home_momentum=_home_momentum,
        away_momentum=_away_momentum,
        fp_result=_fp_result,
        dominant_htft=_dom_htft,
        confirm_checks=_confirm_checks,
        confirm_rate=_confirm_rate,
        # Algorithm signals: store implied probs for rebalancing engine
        prob_H=round(odds_probs["hw"] if odds_probs else hw, 4),
        prob_D=round(odds_probs["dw"] if odds_probs else dw, 4),
        prob_A=round(odds_probs["aw"] if odds_probs else aw, 4),
    )


def _odds_home_band(imp_h: float) -> str:
    """Map implied home win probability to a band label for trap detection."""
    if   imp_h >= 0.70: return "high"      # heavy favourite  (odds ~1.1–1.43)
    elif imp_h >= 0.55: return "mid_high"  # moderate fav     (odds ~1.43–1.82)
    elif imp_h >= 0.40: return "mid"       # slight fav       (odds ~1.82–2.5)
    elif imp_h >= 0.25: return "mid_low"   # slight underdog  (odds ~2.5–4.0)
    else:               return "low"       # heavy underdog   (odds ~4.0+)


def _get_odds_trap_penalty(model: dict, imp_h: float, tip_out: str) -> float:
    """
    Returns a probability penalty (0.0–0.15) for the predicted outcome
    when that odds band has historically been wrong more than expected.

    Tracks per-band failure rates in model["odds_trap"][band][outcome].
    When a band shows > 40% failure rate for a specific outcome, apply
    a damping penalty proportional to how much it exceeds 40%.

    Example: high-odds HOME band has 55% failure → penalty = 0.09
    """
    if not model:
        return 0.0
    band      = _odds_home_band(imp_h)
    trap_data = model.get("odds_trap", {})
    band_data = trap_data.get(band, {})
    out_data  = band_data.get(tip_out, {"correct": 0, "total": 0})
    total     = out_data.get("total", 0)
    if total < 15:          # need min 15 samples to trust the trap signal
        return 0.0
    correct     = out_data.get("correct", 0)
    failure_rate = 1.0 - (correct / total)
    # Baseline expected failure = ~55% (winning ~45% is average)
    # Only penalise when failure rate notably exceeds baseline
    excess = failure_rate - 0.55
    if excess <= 0:
        return 0.0
    return min(0.15, excess * 0.6)   # cap at 0.15 probability shift


def _update_odds_trap(model: dict, imp_h: float, tip_out: str, actual: str) -> None:
    """Update the odds trap tracker after a result is known."""
    if not model:
        return
    band      = _odds_home_band(imp_h)
    trap_data = model.setdefault("odds_trap", {})
    band_data = trap_data.setdefault(band, {})
    out_data  = band_data.setdefault(tip_out, {"correct": 0, "total": 0})
    out_data["total"] += 1
    if tip_out == actual:
        out_data["correct"] += 1


def _odds_fp_key(odds: dict) -> tuple:
    """
    Full multi-market fingerprint key from all available odds.
    Structure (14 values):
      [0]  1X2 home
      [1]  1X2 draw
      [2]  1X2 away
      [3]  O/U 1.5 over
      [4]  O/U 2.5 over
      [5]  O/U 3.5 over
      [6]  BTTS yes
      [7]  DC 1X
      [8]  DC X2
      [9]  DC 12
      [10] HT/FT 1/1  (home leads at HT, home wins FT)
      [11] HT/FT X/X  (draw at HT, draw at FT)
      [12] HT/FT 2/2  (away leads at HT, away wins FT)
      [13] HT/FT 1/1 lowest-odds marker (most likely HTFT)

    All values rounded to nearest 0.05 bucket.
    Missing values → 0.0 (handled in similarity by treating as unknown).
    """
    def _b(v, step=0.05):
        try:
            f = float(v)
            return round(round(f / step) * step, 2) if f > 1.0 else 0.0
        except (TypeError, ValueError):
            return 0.0

    o = odds.get("1x2", {})
    h1 = _b(o.get("1")); hx = _b(o.get("X")); h2 = _b(o.get("2"))

    # O/U — build lookup by line
    ou_map = {}
    for side, line, price in odds.get("ou", []):
        ou_map.setdefault(line, {})[side] = price
    ou15o = _b(ou_map.get("1.5", {}).get("O"))
    ou25o = _b(ou_map.get("2.5", {}).get("O"))
    ou35o = _b(ou_map.get("3.5", {}).get("O"))

    # BTTS
    bt = odds.get("btts", {})
    btts_y = _b(bt.get("Yes"))

    # DC
    dc = odds.get("dc", {})
    dc1x = _b(dc.get("1X")); dcx2 = _b(dc.get("X2")); dc12 = _b(dc.get("12"))

    # HT/FT — key outcomes
    htft = odds.get("htft", {})
    htft_11 = _b(htft.get("1/1"))
    htft_xx = _b(htft.get("X/X"))
    htft_22 = _b(htft.get("2/2"))
    # Most likely HTFT = the one with lowest odds (closest to 1.0)
    htft_vals = [(v, k) for k, v in htft.items() if v and v > 1.0]
    htft_min  = _b(min(htft_vals)[0]) if htft_vals else 0.0

    return (h1, hx, h2, ou15o, ou25o, ou35o, btts_y,
            dc1x, dcx2, dc12, htft_11, htft_xx, htft_22, htft_min)


def _fp_similarity(fp_key: tuple, query_key: tuple) -> float:
    """
    Weighted similarity across all 14 fingerprint dimensions.
    1X2 carries most weight (primary identifier).
    HT/FT carries second most (very specific signal).
    Missing values (0.0) in either key are skipped — partial match is ok.

    Returns 0.0 - 1.0.
    """
    if not fp_key or not query_key:
        return 0.0

    # (index, tolerance, weight)
    dims = [
        (0,  0.10, 3.0),   # 1X2 home
        (1,  0.10, 3.0),   # 1X2 draw
        (2,  0.10, 3.0),   # 1X2 away
        (3,  0.08, 1.5),   # O/U 1.5
        (4,  0.10, 2.0),   # O/U 2.5
        (5,  0.12, 1.0),   # O/U 3.5
        (6,  0.10, 1.5),   # BTTS yes
        (7,  0.08, 1.0),   # DC 1X
        (8,  0.10, 1.0),   # DC X2
        (9,  0.08, 1.0),   # DC 12
        (10, 0.15, 2.0),   # HT/FT 1/1
        (11, 0.15, 2.0),   # HT/FT X/X
        (12, 0.15, 2.0),   # HT/FT 2/2
        (13, 0.12, 1.5),   # HT/FT min (most likely outcome)
    ]

    total_w = 0.0; score_w = 0.0
    for idx, tol, w in dims:
        a = fp_key[idx]   if idx < len(fp_key)   else 0.0
        b = query_key[idx] if idx < len(query_key) else 0.0
        if a == 0.0 or b == 0.0:
            continue   # skip missing — don't penalise unknown markets
        diff = abs(a - b)
        total_w += w
        score_w += w * max(0.0, 1.0 - diff / tol)

    return (score_w / total_w) if total_w > 0 else 0.0


def _detect_odds_repeat(fp_db: dict, home: str, away: str,
                         current_odds: dict,
                         league_id: int = 0,
                         bot_data: dict = None) -> dict:
    """
    Triple cross-check odds repeat detection.
    STRICT: only this exact fixture in this exact league.

    PRIMARY SOURCE: odds_store — server-fetched odds saved before each round.
    FALLBACK: fp_db odds_snapshot — for rounds learned while bot was running.

    CHECK 1 — EXACT FIXTURE + LEAGUE:
      Canonical key (home|away sorted alphabetically uppercase).
      Only records for this league. No cross-league comparison ever.
      Records without real 1x2 odds are excluded.

    CHECK 2 — RAW ODDS EXACT MATCH (±5% per value):
      Compares raw server odds directly. Each market checked independently.
      ALL values in a market must match within ±5%.
      Minimum 4 markets must independently pass.

    CHECK 3 — RESULT CONSISTENCY ≥67%:
      Dominant result across all matched records must be ≥67%.
      Wrong-result entries from odds_store are excluded before consistency check.
    """
    if not current_odds:
        return {"matched": False, "repeat_count": 0, "fail_reason": "no current odds"}

    # ── CHECK 1: Exact fixture + league isolation ─────────────────────────────
    canon = sorted([home.strip().upper(), away.strip().upper()])
    fk    = "|".join(canon)

    # Build records from TWO sources — odds_store (primary) + fp_db (fallback)
    recs = []

    # SOURCE 1: odds_store — server-fetched, most reliable
    if bot_data:
        _os_lid = bot_data.get("odds_store", {}).get(str(league_id), {})
        for rid_key, rid_entries in _os_lid.items():
            entry = rid_entries.get(fk)
            if not entry:
                continue
            snap = entry.get("odds_snapshot", {})
            if not snap or not snap.get("1x2"):
                continue
            outcome = entry.get("outcome")  # None = pending, str = result known
            # Skip entries where outcome is known but WRONG
            # (bad odds pattern — don't want to repeat bad predictions)
            # We keep pending (outcome=None) and correct ones
            recs.append({
                "odds_snapshot": snap,
                "outcome":       outcome,
                "score_h":       entry.get("score_h"),
                "score_a":       entry.get("score_a"),
                "ht_h":          None,
                "ht_a":          None,
                "league_id":     league_id,
                "source":        "odds_store",
                "round_id":      rid_key,
                "home":          entry.get("home", ""),
                "away":          entry.get("away", ""),
            })

    # SOURCE 2: fp_db — fallback for older rounds
    if fp_db:
        canon2 = sorted([home.strip(), away.strip()])
        fk2    = "|".join(canon2)
        recs_raw = fp_db.get(fk) or fp_db.get(fk2) or []
        for r in recs_raw:
            rec_lid = r.get("league_id")
            if rec_lid and league_id and int(rec_lid) != int(league_id):
                continue
            snap = r.get("odds_snapshot") or {}
            if not snap or not snap.get("1x2"):
                continue
            recs.append({
                "odds_snapshot": snap,
                "outcome":       r.get("outcome"),
                "score_h":       r.get("score_h"),
                "score_a":       r.get("score_a"),
                "ht_h":          r.get("ht_h"),
                "ht_a":          r.get("ht_a"),
                "league_id":     league_id,
                "source":        "fp_db",
                "round_id":      str(r.get("round_id", "")),
            })

    if len(recs) < 2:
        return {"matched": False, "repeat_count": len(recs),
                "fail_reason": f"only {len(recs)} records with real odds (need 2+)"}

    # ── Extract current odds values ────────────────────────────────────────────
    def _get_1x2(snap):
        o = snap.get("1x2") or {}
        h = o.get("1") or o.get("home")
        d = o.get("X") or o.get("draw")
        a = o.get("2") or o.get("away")
        return h, d, a

    def _get_dc(snap):
        dc = snap.get("dc") or {}
        return dc.get("1X"), dc.get("X2"), dc.get("12")

    def _get_btts(snap):
        bt = snap.get("btts") or {}
        return bt.get("Yes") or bt.get("yes")

    def _get_ou(snap, line):
        for side, ln, price in snap.get("ou", []):
            if str(ln) == str(line) and side == "O":
                return price
        return None

    def _get_ou_u(snap, line):
        for side, ln, price in snap.get("ou", []):
            if str(ln) == str(line) and side == "U":
                return price
        return None

    def _get_htft_main(snap):
        htft = snap.get("htft") or {}
        vals = {}
        for k in ["1/1", "X/X", "2/2"]:
            if htft.get(k):
                vals[k] = htft[k]
        return vals

    cur_1x2   = _get_1x2(current_odds)
    cur_dc    = _get_dc(current_odds)
    cur_btts  = _get_btts(current_odds)
    cur_htft  = _get_htft_main(current_odds)

    # Build O/U avail dynamically from ALL lines present in current odds
    # Covers both Over and Under for every line (6, 10, 14 or 1.5, 2.5, 3.5)
    cur_ou_map = {}  # key="O/U{line}_{side}" value=price
    for _side, _line, _price in current_odds.get("ou", []):
        cur_ou_map[f"O/U{_line}_{_side}"] = _price

    # Available markets in current odds
    avail = {}
    if all(v is not None for v in cur_1x2):
        avail["1X2"] = cur_1x2
    if all(v is not None for v in cur_dc):
        avail["DC"]  = cur_dc
    if cur_btts is not None:
        avail["BTTS"] = (cur_btts,)
    # Add every O/U line+side present in current odds
    for _k, _pv in cur_ou_map.items():
        avail[_k] = (_pv,)
    if len(cur_htft) >= 2:
        avail["HT/FT"] = cur_htft

    n_available = len(avail)
    if n_available < 4:
        return {"matched": False, "repeat_count": 0,
                "fail_reason": f"only {n_available} markets in current odds (need 4+)"}

    # ── CHECK 2: Raw odds exact match per record ──────────────────────────────
    TOL = 0.05  # ±5% tolerance on raw odds value

    def _vals_match(cur_vals, rec_vals):
        """All values must be within ±5% of each other."""
        if len(cur_vals) != len(rec_vals):
            return False
        for cv, rv in zip(cur_vals, rec_vals):
            if cv is None or rv is None:
                return False
            try:
                diff = abs(float(cv) - float(rv))
                if diff > TOL:
                    return False
            except (TypeError, ValueError):
                return False
        return True

    def _check_record(snap):
        """Returns list of markets that strictly match."""
        matched = []
        rec_1x2  = _get_1x2(snap)
        rec_dc   = _get_dc(snap)
        rec_btts = _get_btts(snap)
        rec_htft = _get_htft_main(snap)

        # Build rec O/U map dynamically from ALL lines in stored snap
        rec_ou_map = {}
        for _side, _line, _price in snap.get("ou", []):
            rec_ou_map[f"O/U{_line}_{_side}"] = _price

        if "1X2" in avail and _vals_match(cur_1x2, rec_1x2):   matched.append("1X2")
        if "DC"  in avail and _vals_match(cur_dc,  rec_dc):     matched.append("DC")
        if "BTTS" in avail and rec_btts is not None and abs(float(cur_btts)-float(rec_btts)) <= TOL:
            matched.append("BTTS")
        # Match every O/U line+side that exists in both current and stored
        for _k, _cur_pv in cur_ou_map.items():
            _rec_pv = rec_ou_map.get(_k)
            if _rec_pv is not None and abs(float(_cur_pv) - float(_rec_pv)) <= TOL:
                matched.append(_k)
        if "HT/FT" in avail and rec_htft:
            htft_match = sum(
                1 for k, cv in cur_htft.items()
                if k in rec_htft and abs(float(cv)-float(rec_htft[k])) <= TOL
            )
            if htft_match >= 2:
                matched.append("HT/FT")

        return matched

    # ── CROSS-CHECK 1: market matching ────────────────────────────────────────
    qualified = []
    for r in recs:
        snap = r.get("odds_snapshot") or {}
        mkts = _check_record(snap)
        if len(mkts) >= 4:
            qualified.append({"record": r, "markets": mkts})

    if not qualified:
        return {"matched": False, "repeat_count": 0,
                "fail_reason": "CROSS-CHECK 1 FAILED: no records match 4+ markets at ±5%"}

    # ── CROSS-CHECK 2: verify each qualified record belongs to this exact fixture
    # Re-confirm canonical key matches — catches any key collision edge cases
    verified = []
    for q in qualified:
        rec = q["record"]
        rec_home = rec.get("home", "").strip().upper()
        rec_away = rec.get("away", "").strip().upper()
        rec_canon = "|".join(sorted([rec_home, rec_away]))
        # Must match our canonical key exactly
        if rec_canon != fk:
            continue  # wrong fixture — skip
        # Must be same league
        rec_lid = rec.get("league_id")
        if rec_lid and league_id and int(rec_lid) != int(league_id):
            continue  # wrong league — skip
        verified.append(q)

    if not verified:
        return {"matched": False, "repeat_count": 0,
                "fail_reason": "CROSS-CHECK 2 FAILED: records don't match this exact fixture+league"}

    # Use best verified record (most markets matched)
    verified.sort(key=lambda x: -len(x["markets"]))
    best_record     = verified[0]["record"]
    matched_markets = verified[0]["markets"]
    n_matched       = len(matched_markets)

    # ── CROSS-CHECK 3: result consistency across ALL verified records ──────────
    # ── CROSS-CHECK 3: result consistency + cycle sequence ────────────────
    # Sort ALL verified records chronologically (oldest→newest)
    # to build the true sequence of outcomes for this fixture.
    _all_with_outcome = sorted(
        [q for q in verified if q["record"].get("outcome")],
        key=lambda q: int(q["record"].get("round_id", 0) or 0)
    )

    outcome_counts = {}
    score_lines    = []   # chronological: oldest→newest with direction label

    for q in _all_with_outcome:
        rec = q["record"]
        out = rec.get("outcome", "")
        if not out:
            continue
        outcome_counts[out] = outcome_counts.get(out, 0) + 1
        sh   = rec.get("score_h")
        sa   = rec.get("score_a")
        ht_h = rec.get("ht_h")
        ht_a = rec.get("ht_a")
        if sh is not None and sa is not None:
            ht_str   = f"{ht_h}-{ht_a}/" if ht_h is not None and ht_a is not None else ""
            dir_icon = "🏠" if out == "HOME" else ("✈️" if out == "AWAY" else "🤝")
            score_lines.append(f"{dir_icon}{ht_str}{sh}-{sa}")

    repeat_count = len(_all_with_outcome)
    known_count  = len(outcome_counts)

    if not outcome_counts:
        return {"matched": False, "repeat_count": repeat_count,
                "fail_reason": "CROSS-CHECK 3 FAILED: no confirmed results in matched records"}

    dominant_out      = max(outcome_counts, key=outcome_counts.get)
    consistency_count = outcome_counts[dominant_out]
    total_known       = sum(outcome_counts.values())
    consistency_pct   = round(consistency_count / total_known * 100)

    # Detect cycle rotation pattern from the full sequence
    # ── FULL SCORE CYCLE DETECTION ──────────────────────────────────────────
    # ── FULL SCORE CYCLE DETECTION (England only — 7794) ──────────────────
    # For England: use score_history_7794 keyed by fixture+odds fingerprint
    # This gives the complete cross-round history for the EXACT same odds.
    # Other leagues: direction-only from verified records.
    _full_seq = []

    if league_id == 7794 and bot_data:
        # Build odds fingerprint from current odds (1x2)
        _cur_1x2_fp = current_odds.get("1x2", {})
        _ofp = f"{_cur_1x2_fp.get('1',0)}-{_cur_1x2_fp.get('X',0)}-{_cur_1x2_fp.get('2',0)}"
        _sh_key = f"{fk}|{_ofp}"  # fk = canonical fixture key (already computed above)
        _sh_store = bot_data.get("score_history_7794", {})
        _fh = _sh_store.get(_sh_key, [])
        # Build full_seq from score_history — complete picture of this odds pattern
        for _r in _fh:
            _out = _r.get("outcome", "")
            _sh  = _r.get("score_h")
            _sa  = _r.get("score_a")
            if _out and _sh is not None and _sa is not None:
                _full_seq.append((_out, _sh, _sa))

    # Fallback: build from verified records (all leagues, or England if no history yet)
    if not _full_seq:
        for q in _all_with_outcome:
            rec = q["record"]
            out = rec.get("outcome", "")
            sh  = rec.get("score_h")
            sa  = rec.get("score_a")
            if out and sh is not None and sa is not None:
                _full_seq.append((out, sh, sa))

    _cycle_next       = None
    _cycle_next_score = None
    _cycle_period     = None
    _cycle_pos        = None

    if len(_full_seq) >= 2:
        if league_id == 7794:  # England only — full score cycle tracking
            _n = len(_full_seq)
            for _period in range(1, _n // 2 + 1):
                _pattern = _full_seq[:_period]
                _matches = all(_full_seq[_i] == _pattern[_i % _period] for _i in range(_n))
                if _matches:
                    _cycle_period     = _period
                    _cycle_pos        = _n % _period
                    _next_in_pat      = _pattern[_cycle_pos]
                    _cycle_next       = _next_in_pat[0]
                    _cycle_next_score = f"{_next_in_pat[1]}-{_next_in_pat[2]}"
                    break

        # Direction-only fallback (all leagues including England if no score cycle)
        if not _cycle_period:
            _dir_seq     = [s[0] for s in _full_seq]
            _alternating = all(_dir_seq[i] != _dir_seq[i+1] for i in range(len(_dir_seq)-1))
            _cycle_next  = ("HOME" if _dir_seq[-1] == "AWAY" else "AWAY") if _alternating else dominant_out

    # Annotate score_lines with 🔁 at cycle restart points (England only)
    _annotated_lines = []
    for _i, (_sl, _q) in enumerate(zip(score_lines, _all_with_outcome)):
        _entry = _sl
        if _cycle_period and _i > 0 and _i % _cycle_period == 0:
            _entry = "🔁 " + _entry
        _annotated_lines.append(_entry)
    score_lines = _annotated_lines

    _seq = [s[0] for s in _full_seq]  # direction sequence for maintenance check
    if consistency_pct < 67:
        return {"matched": False, "repeat_count": repeat_count,
                "fail_reason": f"CROSS-CHECK 3 FAILED: {consistency_pct}% consistent (need 67%+)"}
    # ── RECENCY CHECK: detect if pattern has cycled ──────────────────────────
    # ── MAINTENANCE CHECK ──────────────────────────────────────────────────
    # ── MAINTENANCE CHECK ──────────────────────────────────────────────────
    # Uses cycle_next as the expected direction:
    # - If alternating cycle: expects opposite of last result
    # - If consistent cycle: expects same as dominant
    # The last 2 results must BOTH match the expected direction to fire.
    _expected_dir = _cycle_next if _cycle_next else dominant_out
    _sorted_verified = sorted(
        [q for q in verified if q["record"].get("outcome")],
        key=lambda q: int(q["record"].get("round_id", 0) or 0),
        reverse=True
    )
    if len(_sorted_verified) >= 2:
        _r1 = _sorted_verified[0]["record"].get("outcome")
        _r2 = _sorted_verified[1]["record"].get("outcome")
        if not (_r1 == _expected_dir and _r2 == _expected_dir):
            return {"matched": False, "repeat_count": repeat_count,
                    "fail_reason": f"MAINTENANCE: last 2 [{_r1},{_r2}] — need both {_expected_dir} to restore"}

    # ── All checks passed ─────────────────────────────────────────────────────
    match_pct = round(n_matched / n_available * 100)
    # Compute confidence from raw odds closeness across all matched markets
    diffs = []
    snap_b = best_record.get("odds_snapshot") or {}
    for mk in matched_markets:
        if mk == "1X2":
            for cv, rv in zip(cur_1x2, _get_1x2(snap_b)):
                if cv and rv: diffs.append(abs(float(cv)-float(rv)))
        elif mk == "DC":
            for cv, rv in zip(cur_dc, _get_dc(snap_b)):
                if cv and rv: diffs.append(abs(float(cv)-float(rv)))
        elif mk == "BTTS":
            if cur_btts and _get_btts(snap_b):
                diffs.append(abs(float(cur_btts) - float(_get_btts(snap_b))))
        elif mk.startswith("O/U"):
            # Dynamic O/U key e.g. "O/U6_O", "O/U10_U"
            _cur_pv = cur_ou_map.get(mk)
            _rec_pv = {f"O/U{s}{l}_{sd}": p for sd,l,p in snap_b.get("ou",[]) for s in [""]}.get(mk)
            # simpler: rebuild rec_ou_map for snap_b
            _snap_b_ou = {f"O/U{_sl}_{_ss}": _sp for _ss,_sl,_sp in snap_b.get("ou",[])}
            _rec_pv = _snap_b_ou.get(mk)
            if _cur_pv and _rec_pv:
                diffs.append(abs(float(_cur_pv) - float(_rec_pv)))
    avg_diff   = sum(diffs)/len(diffs) if diffs else 0
    confidence = round(max(0, 100 - avg_diff * 1000))

    # Use cycle_next as the authoritative direction when cycle is detected
    _effective_out = _cycle_next if _cycle_next else dominant_out
    out_label = {"HOME": "Home WIN", "AWAY": "Away WIN", "DRAW": "Draw"}.get(_effective_out, _effective_out)

    if consistency_pct == 100 and repeat_count >= 2:
        tier     = "💎 ELITE LOCK"
        tier_msg = f"all 5 markets identical — 100% same result every time"
    elif consistency_pct >= 67:
        tier     = "🏆 ELITE"
        tier_msg = f"all 5 markets confirmed — {consistency_pct}% same result"

    scores_str = "  ·  ".join(score_lines[:3])
    # Show current in-progress cycle run only
    # cycle_pos_now = how many steps into the current (incomplete) loop
    # If pos=0 (loop just completed): show the last full period so pattern is visible
    if _cycle_period:
        _cycle_pos_now = len(_full_seq) % _cycle_period
        if _cycle_pos_now == 0:
            _cur_run = score_lines[-_cycle_period:]  # last completed loop
        else:
            _cur_run = score_lines[-_cycle_pos_now:]  # current partial loop
        scores_str = "  ·  ".join(_cur_run)
    else:
        scores_str = "  ·  ".join(score_lines[-3:])  # last 3 if no cycle yet
    # Cycle next — show predicted direction AND score if full cycle detected
    _cycle_icon = "🏠" if _cycle_next == "HOME" else ("✈️" if _cycle_next == "AWAY" else "")
    if _cycle_period and _cycle_next_score:
        _period_label = f"period {_cycle_period}" if _cycle_period > 1 else ""
        _cycle_str = (f"{_cycle_icon} *{_cycle_next}*  `{_cycle_next_score}` "
                      f"— score cycle ({_period_label})")
    elif _cycle_next:
        _cycle_str = f"{_cycle_icon} *{_cycle_next}* next in cycle"
    else:
        _cycle_str = ""

    _period_info = f" · {_cycle_period}-step cycle" if _cycle_period else ""

    # Map matched markets to 5 core slots for display
    _core5 = {"1X2": False, "DC": False, "BTTS": False, "O/U": False, "HT/FT": False}
    for _mk in matched_markets:
        if _mk == "1X2":            _core5["1X2"]  = True
        elif _mk == "DC":           _core5["DC"]   = True
        elif _mk == "BTTS":         _core5["BTTS"] = True
        elif _mk.startswith("O/U"): _core5["O/U"]  = True
        elif _mk == "HT/FT":        _core5["HT/FT"]= True
    _verified_count = sum(_core5.values())

    # Count occurrences of the effective direction (cycle_next or dominant)
    _effective_count = outcome_counts.get(_effective_out, 0)
    # If cycle overrides dominant, note it clearly
    _cycle_override  = (_cycle_next and _cycle_next != dominant_out)
    _override_note   = f" *(cycle override — dominant was {dominant_out})*" if _cycle_override else ""

    star_label = (
        f"{tier} — {tier_msg}{_period_info}\n"
        f"┆    📌 Result: *{out_label}*  ({_effective_count}/{repeat_count}x){_override_note}\n"
        + (f"┆    🔄 Cycle: {scores_str}\n" if scores_str else "")
        + (f"┆    ➡️  {_cycle_str}\n" if _cycle_str else "")
        + f"┆    🔑 Markets verified: {_verified_count}/5\n"
        f"┆    🎯 Odds precision: {confidence}%  ·  ±5% tolerance"
    )

    return {
        "matched":           True,
        "match_pct":         match_pct,
        "markets_matched":   matched_markets,
        "markets_total":     n_available,
        "repeat_count":      repeat_count,
        "best_record":       best_record,
        "confidence":        confidence,
        "outcome":           _effective_out,
        "consistency_pct":   consistency_pct,
        "consistency_count": consistency_count,
        "tier":              tier,
        "cycle_next":        _cycle_next,
        "score_history":     score_lines,
        "star_label":        star_label,
    }
    """
    Full H2H analysis between two teams — used when odds don't match well.

    Checks ALL historical meetings and splits by home venue:
    - home_as_home: games where current home team was HOME vs this opponent
    - home_as_away: games where current home team was AWAY vs this opponent (reversed)

    Returns a dict with outcome percentages, HTFT breakdown, BTTS/O25 rates,
    and a derived signal + confidence.
    """
    fk_fwd = f"{home}|{away}".lower()   # home was HOME
    fk_rev = f"{away}|{home}".lower()   # home was AWAY (reversed)

    recs_home_venue = fp_db.get(fk_fwd, [])   # home team was home
    recs_away_venue = fp_db.get(fk_rev, [])   # home team was away

    def _outcome_stats(records: list, flip: bool = False) -> dict:
        """Aggregate outcome stats from a list of records. flip=True inverts H/A."""
        if not records:
            return {}
        flip_map = {"HOME": "AWAY", "AWAY": "HOME", "DRAW": "DRAW"}
        outcomes = []
        htfts    = []
        btts     = []
        ou25     = []
        scores   = []
        for r in records:
            out = r.get("outcome")
            if out:
                outcomes.append(flip_map[out] if flip else out)
            h = r.get("htft_result")
            if h and "?" not in h:
                if flip:
                    parts = h.split("/")
                    f = {"1":"2","2":"1","X":"X"}
                    h = f.get(parts[0],parts[0]) + "/" + f.get(parts[1],parts[1]) if len(parts)==2 else h
                htfts.append(h)
            if r.get("btts_result") is not None:
                btts.append(r["btts_result"])
            if r.get("ou25_result") is not None:
                ou25.append(r["ou25_result"])
            sh = r.get("score_h"); sa = r.get("score_a")
            if sh is not None and sa is not None:
                if flip:
                    scores.append((sa, sh))   # flip home/away goals
                else:
                    scores.append((sh, sa))

        n = len(outcomes) if outcomes else 1
        result = {"n": len(records)}
        for out in ("HOME", "DRAW", "AWAY"):
            cnt = outcomes.count(out)
            result[f"{out}_pct"] = round(cnt / n * 100, 1)
            result[f"{out}_n"]   = cnt
        # Dominant outcome
        if outcomes:
            dom = max(set(outcomes), key=outcomes.count)
            result["dominant"] = dom
            result["dominant_pct"] = round(outcomes.count(dom)/n*100, 1)
        # HTFT top 3
        if htfts:
            from collections import Counter
            result["htft_top3"] = Counter(htfts).most_common(3)
        # BTTS
        if btts:
            result["btts_yes_pct"] = round(sum(btts)/len(btts)*100, 1)
        # O/U 2.5
        if ou25:
            result["over25_pct"] = round(sum(ou25)/len(ou25)*100, 1)
        # Avg score
        if scores:
            result["avg_gf"] = round(sum(s[0] for s in scores)/len(scores), 2)
            result["avg_ga"] = round(sum(s[1] for s in scores)/len(scores), 2)
        return result

    home_venue_stats = _outcome_stats(recs_home_venue, flip=False)  # same venue
    away_venue_stats = _outcome_stats(recs_away_venue, flip=True)   # reversed venue

    # All meetings combined
    all_records      = recs_home_venue + recs_away_venue
    all_stats        = _outcome_stats(
        recs_home_venue,  # same-venue meetings weighted more
        flip=False
    )

    return {
        "home_venue":   home_venue_stats,   # when home team was home
        "away_venue":   away_venue_stats,   # when home team was away (flipped)
        "total_meetings": len(all_records),
        "home_venue_n": len(recs_home_venue),
        "away_venue_n": len(recs_away_venue),
    }


def _cross_validate_h2h_vs_form(h2h: dict, home_form: list, away_form: list,
                                  home_form_wpct: float, away_form_wpct: float) -> dict:
    """
    Cross-validate H2H home-venue outcomes against current 6-match form.

    Logic:
    - H2H home venue says HOME wins X% of the time
    - Current home team's last 6 form says they win Y% of the time
    - If X and Y both >= 55% → strong agreement → high confidence
    - If X >= 55% but Y < 45% → conflict → reduce confidence
    - If both weak → no signal

    Returns {signal, confidence, agreement, reason}
    """
    hv = h2h.get("home_venue", {})
    home_h2h_pct  = hv.get("HOME_pct", 0.0)
    away_h2h_pct  = hv.get("AWAY_pct", 0.0)
    draw_h2h_pct  = hv.get("DRAW_pct", 0.0)
    h2h_dom       = hv.get("dominant", None)
    h2h_dom_pct   = hv.get("dominant_pct", 0.0)
    h2h_n         = hv.get("n", 0)

    # Also check all-meetings (includes reversed fixture)
    av = h2h.get("away_venue", {})
    # away_venue already has outcomes flipped so HOME means home team won there too
    all_home_pct  = (home_h2h_pct * h2h_n + av.get("HOME_pct", 0.0) * av.get("n", 0))
    all_n         = h2h_n + av.get("n", 0)
    all_home_pct  = round(all_home_pct / all_n, 1) if all_n > 0 else 0.0
    all_away_pct  = 100.0 - all_home_pct - (draw_h2h_pct if h2h_n > 0 else 0.0)

    signal     = None
    confidence = 0.0
    agreement  = "none"
    reason     = ""

    # Determine H2H dominant direction
    if h2h_dom == "HOME" and h2h_dom_pct >= 50:
        h2h_says = "HOME"
        h2h_str  = h2h_dom_pct
    elif h2h_dom == "AWAY" and h2h_dom_pct >= 50:
        h2h_says = "AWAY"
        h2h_str  = h2h_dom_pct
    else:
        h2h_says = None
        h2h_str  = 0.0

    # Compare with form
    if h2h_says == "HOME":
        if home_form_wpct >= 50:
            # H2H and form both say HOME wins
            agreement  = "strong"
            confidence = round((h2h_str * 0.6 + home_form_wpct * 0.4), 1)
            signal     = "HOME"
            reason     = (f"H2H home venue {h2h_str:.0f}% + "
                          f"current form {home_form_wpct:.0f}% both confirm HOME")
        elif home_form_wpct >= 35:
            # H2H says home, form is borderline
            agreement  = "partial"
            confidence = round(h2h_str * 0.7, 1)
            signal     = "HOME"
            reason     = (f"H2H home venue {h2h_str:.0f}% but form only {home_form_wpct:.0f}%")
        else:
            # H2H says home but current form is poor — conflict
            agreement  = "conflict"
            confidence = round(h2h_str * 0.4, 1)
            signal     = "HOME"
            reason     = (f"H2H home {h2h_str:.0f}% conflicts with poor form {home_form_wpct:.0f}%")

    elif h2h_says == "AWAY":
        if away_form_wpct >= 50:
            agreement  = "strong"
            confidence = round((h2h_str * 0.6 + away_form_wpct * 0.4), 1)
            signal     = "AWAY"
            reason     = (f"H2H {h2h_str:.0f}% AWAY + away form {away_form_wpct:.0f}% confirm")
        elif away_form_wpct >= 35:
            agreement  = "partial"
            confidence = round(h2h_str * 0.7, 1)
            signal     = "AWAY"
            reason     = (f"H2H {h2h_str:.0f}% AWAY, away form {away_form_wpct:.0f}%")
        else:
            agreement  = "conflict"
            confidence = round(h2h_str * 0.4, 1)
            signal     = "AWAY"
            reason     = (f"H2H {h2h_str:.0f}% AWAY conflicts with poor away form {away_form_wpct:.0f}%")
    else:
        # No dominant H2H direction — no signal
        reason = "H2H outcomes too mixed — no signal"

    return {
        "signal":       signal,
        "confidence":   confidence,
        "agreement":    agreement,
        "reason":       reason,
        "h2h_home_pct": home_h2h_pct,
        "h2h_away_pct": away_h2h_pct,
        "all_home_pct": all_home_pct,
        "h2h_n":        h2h.get("total_meetings", 0),
        "h2h_dom":      h2h_says,
        "h2h_dom_pct":  h2h_str,
    }


def _investigate_fixture(home: str, away: str, model: dict,
                          tier_map: dict) -> dict:
    """
    FULL FIXTURE INVESTIGATION ENGINE — the detective.

    Goes through ALL historical data for this exact fixture across every
    season and round stored in match_log (not capped at 15 like fp_db).

    Builds a complete case file:

    1. DOMINANCE PATTERN — who wins this matchup most?
       Across ALL meetings: HOME win%, DRAW%, AWAY win%
       Recent trend: has the dominant team been winning MORE or LESS lately?

    2. UPSET CONDITIONS — when did the expected loser actually win?
       For each upset: what was the winner's form at the time?
       Was the favourite in bad form when they lost?
       This tells us: "upsets happen when X is in poor form"

    3. SCORE PATTERN — typical margin and goal profile
       Average goals scored/conceded per team in this fixture
       Most common scoreline range (e.g. "usually decided by 1 goal")
       BTTS rate, Over 2.5 rate specifically for THIS fixture

    4. STREAK — is there a current win streak for either team?
       Last 5 meetings in order — who has been winning recently?

    5. VERDICT — cross-reference all findings with TODAY's form
       If dominant team is also in good current form → STRONG CONFIRM
       If dominant team is in poor form → WARN (upset possible)
       If weaker team's form has improved significantly → CAUTION

    Returns a dict with the full case file and a verdict.
    """
    home_l = home.lower()
    away_l = away.lower()
    match_log = model.get("match_log", [])

    # ── Gather ALL meetings between these two teams ───────────────────────────
    meetings = []
    for entry in match_log:
        eh = (entry.get("home") or "").lower()
        ea = (entry.get("away") or "").lower()
        sh = entry.get("score_h")
        sa = entry.get("score_a")
        if sh is None or sa is None:
            continue
        # Match regardless of which was home/away
        if (home_l in eh or eh.startswith(home_l[:3])) and \
           (away_l in ea or ea.startswith(away_l[:3])):
            # home was actually home
            meetings.append({
                "round_id": entry.get("round_id", 0),
                "season_id": entry.get("season_id", ""),
                "home_goals": sh,
                "away_goals": sa,
                "perspective": "normal",   # home team is our 'home'
            })
        elif (away_l in eh or eh.startswith(away_l[:3])) and \
             (home_l in ea or ea.startswith(home_l[:3])):
            # away was actually home — flip perspective
            meetings.append({
                "round_id": entry.get("round_id", 0),
                "season_id": entry.get("season_id", ""),
                "home_goals": sa,   # flipped: our home team's goals
                "away_goals": sh,
                "perspective": "reversed",
            })

    if not meetings:
        return {"n_meetings": 0, "verdict": "no_history", "case_strength": 0.0}

    meetings.sort(key=lambda m: m["round_id"])
    n = len(meetings)

    # ── 1. DOMINANCE PATTERN ─────────────────────────────────────────────────
    home_wins = sum(1 for m in meetings if m["home_goals"] > m["away_goals"])
    draws     = sum(1 for m in meetings if m["home_goals"] == m["away_goals"])
    away_wins = sum(1 for m in meetings if m["home_goals"] < m["away_goals"])

    home_win_pct  = round(home_wins / n * 100, 1)
    draw_pct      = round(draws     / n * 100, 1)
    away_win_pct  = round(away_wins / n * 100, 1)

    dominant      = "HOME" if home_wins > away_wins else ("AWAY" if away_wins > home_wins else "DRAW")
    dominant_pct  = home_win_pct if dominant == "HOME" else (away_win_pct if dominant == "AWAY" else draw_pct)

    # Recent trend: last 6 meetings vs overall
    recent6 = meetings[-6:]
    r6_hw   = sum(1 for m in recent6 if m["home_goals"] > m["away_goals"])
    r6_aw   = sum(1 for m in recent6 if m["home_goals"] < m["away_goals"])
    r6_d    = len(recent6) - r6_hw - r6_aw
    r6_dom  = "HOME" if r6_hw > r6_aw else ("AWAY" if r6_aw > r6_hw else "DRAW")
    r6_dom_pct = round((r6_hw if r6_dom=="HOME" else r6_aw if r6_dom=="AWAY" else r6_d) / len(recent6) * 100, 1)

    trend_consistent = (r6_dom == dominant)

    # ── 2. UPSET CONDITIONS ───────────────────────────────────────────────────
    # An upset = the non-dominant team won
    upset_conditions = []
    for m in meetings:
        actual = "HOME" if m["home_goals"] > m["away_goals"] else \
                 ("AWAY" if m["home_goals"] < m["away_goals"] else "DRAW")
        if actual != dominant and actual != "DRAW":
            # This was an upset — investigate what conditions allowed it
            r_id = m["round_id"]
            upset_team  = home if actual == "HOME" else away
            losing_team = away  if actual == "HOME" else home
            # What was the upset team's form before this match?
            upset_form  = _get_opponent_form_at_round(upset_team,  r_id, match_log, n=5)
            losing_form = _get_opponent_form_at_round(losing_team, r_id, match_log, n=5)
            margin = abs(m["home_goals"] - m["away_goals"])
            upset_conditions.append({
                "round_id":    r_id,
                "winner":      actual,
                "upset_form_wpct":  upset_form["win_pct"],
                "upset_trend":      upset_form["trend"],
                "losing_form_wpct": losing_form["win_pct"],
                "losing_trend":     losing_form["trend"],
                "margin":           margin,
                "score":            f"{m['home_goals']}-{m['away_goals']}",
            })

    # Summarise upset pattern
    n_upsets = len(upset_conditions)
    upset_rate = round(n_upsets / n * 100, 1)
    # When upsets happened, was the dominant team in bad form?
    dominant_bad_form_upsets = sum(
        1 for u in upset_conditions
        if u["losing_form_wpct"] <= 35 or u["losing_trend"] == "FALLING"
    )
    # When upsets happened, was the underdog in good form?
    underdog_good_form_upsets = sum(
        1 for u in upset_conditions
        if u["upset_form_wpct"] >= 55 or u["upset_trend"] == "RISING"
    )

    # ── 3. SCORE PATTERN ─────────────────────────────────────────────────────
    all_goals  = [m["home_goals"] + m["away_goals"] for m in meetings]
    home_goals = [m["home_goals"] for m in meetings]
    away_goals = [m["away_goals"] for m in meetings]
    avg_total  = round(sum(all_goals) / n, 2)
    avg_hg     = round(sum(home_goals) / n, 2)
    avg_ag     = round(sum(away_goals) / n, 2)
    btts_rate  = round(sum(1 for m in meetings if m["home_goals"] > 0 and m["away_goals"] > 0) / n * 100, 1)
    o25_rate   = round(sum(1 for m in meetings if m["home_goals"] + m["away_goals"] > 2) / n * 100, 1)
    margins    = [abs(m["home_goals"] - m["away_goals"]) for m in meetings]
    avg_margin = round(sum(margins) / n, 2)
    close_games= round(sum(1 for mg in margins if mg <= 1) / n * 100, 1)  # % decided by 1 goal

    # ── 4. STREAK ─────────────────────────────────────────────────────────────
    last5_results = []
    for m in meetings[-5:]:
        if   m["home_goals"] > m["away_goals"]: last5_results.append("H")
        elif m["home_goals"] < m["away_goals"]: last5_results.append("A")
        else:                                   last5_results.append("D")

    # Current streak for dominant team
    streak_team = "HOME" if dominant == "HOME" else "AWAY"
    streak = 0
    for r in reversed(last5_results):
        if r == ("H" if streak_team == "HOME" else "A"):
            streak += 1
        else:
            break

    # ── 5. VERDICT — cross-reference with current form ────────────────────────
    home_curr_mom = _compute_team_momentum(home, model, tier_map)
    away_curr_mom = _compute_team_momentum(away, model, tier_map)
    home_mom_score = home_curr_mom.get("score", 50.0)
    away_mom_score = away_curr_mom.get("score", 50.0)

    case_strength  = 0.0   # 0.0–1.0: how strong is the historical case
    verdict        = dominant
    verdict_reason = []

    # Base: how dominant is the historical pattern?
    if dominant_pct >= 70:
        case_strength += 0.40
        verdict_reason.append(f"{dominant} wins {dominant_pct:.0f}% of {n} meetings")
    elif dominant_pct >= 55:
        case_strength += 0.25
        verdict_reason.append(f"{dominant} leads {dominant_pct:.0f}% of {n} meetings")
    else:
        case_strength += 0.10
        verdict_reason.append(f"contested — {dominant} only {dominant_pct:.0f}%")

    # Recent trend confirms?
    if trend_consistent and r6_dom_pct >= 60:
        case_strength += 0.20
        verdict_reason.append(f"recent 6: {r6_dom} {r6_dom_pct:.0f}% (trend holds)")
    elif not trend_consistent:
        case_strength -= 0.10
        verdict_reason.append(f"⚠️ recent trend shifted to {r6_dom} ({r6_dom_pct:.0f}%)")

    # Streak confirms?
    if streak >= 3:
        case_strength += 0.15
        verdict_reason.append(f"{streak}-game winning streak for {dominant}")
    elif streak >= 2:
        case_strength += 0.08

    # Current form of dominant team confirms?
    dom_curr_mom = home_mom_score if dominant == "HOME" else away_mom_score
    opp_curr_mom = away_mom_score if dominant == "HOME" else home_mom_score

    if dom_curr_mom >= 60 and opp_curr_mom <= 45:
        case_strength += 0.20
        verdict_reason.append(f"today: {dominant} in form ({dom_curr_mom:.0f}) vs opponent weak ({opp_curr_mom:.0f})")
    elif dom_curr_mom >= 55:
        case_strength += 0.10
        verdict_reason.append(f"today: {dominant} momentum ok ({dom_curr_mom:.0f})")
    elif dom_curr_mom <= 40:
        case_strength -= 0.15
        verdict_reason.append(f"⚠️ {dominant} poor form today ({dom_curr_mom:.0f}) — upset risk")

    # Upset risk check
    if n_upsets > 0:
        dom_team_curr_form = home_curr_mom if dominant == "HOME" else away_curr_mom
        is_in_bad_form = dom_team_curr_form.get("trend") == "FALLING" or dom_curr_mom <= 40
        if dominant_bad_form_upsets > 0 and is_in_bad_form:
            case_strength -= 0.20
            verdict_reason.append(
                f"⚠️ upset pattern: {dominant_bad_form_upsets}/{n_upsets} upsets when "
                f"dominant was in bad form — current form matches upset condition"
            )
        elif underdog_good_form_upsets > 0:
            underdog_team_curr = away_curr_mom if dominant == "HOME" else home_curr_mom
            if underdog_team_curr.get("trend") == "RISING" or opp_curr_mom >= 58:
                case_strength -= 0.12
                verdict_reason.append(
                    f"⚠️ underdog rising — {underdog_good_form_upsets} past upsets "
                    f"happened when underdog was in good form (current: {opp_curr_mom:.0f})"
                )

    case_strength = max(0.0, min(1.0, case_strength))

    # ── SUSPICION FILTER — catch false STRONG CASE picks ─────────────────────
    # Even when history strongly favours one team, certain current conditions
    # suggest the pattern may not hold THIS time. Check for:
    #
    # 1. HOLLOW DOMINANCE — dominant team's recent wins are all vs weak/crisis
    #    opponents (all ⚠️ tags). If they haven't beaten anyone meaningful
    #    recently, their historical dominance may be inflated.
    #
    # 2. UNDERDOG QUALITY — the underdog has been getting results vs strong
    #    opponents (✅ tags). They may be genuinely improving despite history.
    #
    # 3. FP DISAGREEMENT + CLOSE GAMES — if the fingerprint disagrees AND
    #    most historical meetings were decided by 1 goal, the pattern is fragile.
    #
    # 4. DRAW TRAP — if both teams have 3+ draws in last 6, the match is likely
    #    to draw regardless of who history says should win.

    suspicion_flags = []

    # Check dominant team's recent form quality
    dom_team     = home if dominant == "HOME" else away
    dom_mom_data = home_curr_mom if dominant == "HOME" else away_curr_mom
    und_mom_data = away_curr_mom if dominant == "HOME" else home_curr_mom

    dom_trend_str = dom_mom_data.get("trend_str", "")
    und_trend_str = und_mom_data.get("trend_str", "")

    # Count quality tags in last 6 results
    dom_hollow = dom_trend_str.count("⚠️")   # hollow/weak wins
    dom_quality = dom_trend_str.count("✅")  # quality wins vs strong opponents
    und_hollow = und_trend_str.count("⚠️")
    und_quality = und_trend_str.count("✅")

    # ── Quality-adjusted form score ───────────────────────────────────────────
    # Win% alone is misleading — 67% wins vs neutral opponents beats
    # 50% wins vs weak opponents in real terms.
    # Compare: (win_pct × avg_quality_of_opponents_faced) for each team
    dom_raw_wins   = dom_mom_data.get("score", 50.0)
    und_raw_wins   = und_mom_data.get("score", 50.0)
    dom_opp_qual   = dom_mom_data.get("opp_quality", 0.5)   # avg strength of opponents faced
    und_opp_qual   = und_mom_data.get("opp_quality", 0.5)
    dom_avg_qual   = dom_mom_data.get("avg_quality", 0.5)   # quality of dominant team's results
    und_avg_qual   = und_mom_data.get("avg_quality", 0.5)

    # Quality-adjusted score: momentum score weighted by opponent quality faced
    # Higher opp_quality means they beat better teams — more meaningful wins
    dom_adj_score = dom_raw_wins * (1.0 + dom_opp_qual * 0.3)
    und_adj_score = und_raw_wins * (1.0 + und_opp_qual * 0.3)

    # 5. FORM REVERSAL — underdog's quality-adjusted score exceeds dominant team's
    if und_adj_score > dom_adj_score * 1.15 and und_raw_wins >= 35:
        case_strength -= 0.20
        suspicion_flags.append(
            f"form reversal — underdog adj.score {und_adj_score:.0f} > "
            f"dominant {dom_adj_score:.0f} (wins vs stronger opponents)"
        )

    # 1. Hollow dominance: dominant team has 5+ hollow results, 0 quality wins
    if dom_hollow >= 5 and dom_quality == 0:
        case_strength -= 0.15
        suspicion_flags.append("dominant team hollow wins only — inflated history")

    # 2. Underdog quality: underdog has 2+ quality results recently
    if und_quality >= 2 and dom_quality == 0:
        case_strength -= 0.15
        suspicion_flags.append(f"underdog showing quality ({und_quality}✅) — genuinely improving")

    # 3. Fragile pattern: close games history + fp disagrees
    # close_games already computed above (% of meetings decided by 1 goal)
    if close_games >= 70 and n_upsets >= 1:
        case_strength -= 0.10
        suspicion_flags.append(f"fragile pattern — {close_games:.0f}% decided by 1 goal, {n_upsets} upsets")

    # 4. Draw trap: both teams have 3+ draws in recent 6
    dom_draws = dom_trend_str.count("D")
    und_draws = und_trend_str.count("D")
    if dom_draws >= 3 and und_draws >= 3:
        case_strength -= 0.15
        suspicion_flags.append(f"draw trap — both teams draw-heavy ({dom_draws}D vs {und_draws}D recently)")

    # If suspicion flags reduce case_strength below STRONG CASE threshold,
    # downgrade the verdict label accordingly
    case_strength = max(0.0, min(1.0, case_strength))

    # Final verdict label
    if case_strength >= 0.70:
        verdict_label = f"✅ STRONG CASE — {dominant}"
    elif case_strength >= 0.50:
        verdict_label = f"📋 CONFIRMED — {dominant}"
    elif case_strength >= 0.30:
        verdict_label = f"⚠️ CAUTION — {dominant} likely but upset risk"
    else:
        verdict_label = f"❓ UNCERTAIN — mixed history"

    return {
        "n_meetings":      n,
        "dominant":        dominant,
        "dominant_pct":    dominant_pct,
        "home_win_pct":    home_win_pct,
        "draw_pct":        draw_pct,
        "away_win_pct":    away_win_pct,
        "recent6":         last5_results,
        "r6_dom":          r6_dom,
        "r6_dom_pct":      r6_dom_pct,
        "trend_consistent": trend_consistent,
        "streak":          streak,
        "streak_team":     streak_team,
        "n_upsets":        n_upsets,
        "upset_rate":      upset_rate,
        "dominant_bad_form_upsets":   dominant_bad_form_upsets,
        "underdog_good_form_upsets":  underdog_good_form_upsets,
        "avg_total_goals": avg_total,
        "avg_home_goals":  avg_hg,
        "avg_away_goals":  avg_ag,
        "btts_rate":       btts_rate,
        "o25_rate":        o25_rate,
        "avg_margin":      avg_margin,
        "close_games_pct": close_games,
        "case_strength":   round(case_strength, 3),
        "verdict":         verdict,
        "verdict_label":   verdict_label,
        "verdict_reason":  verdict_reason,
        "suspicion_flags": suspicion_flags,
        "home_momentum":   home_mom_score,
        "away_momentum":   away_mom_score,
    }


def _find_best_fingerprint(fp_db: dict, home: str, away: str,
                             odds: dict, model: dict | None = None) -> dict | None:
    """
    Three-stage fingerprint search:

    STAGE 1 — Own-league odds matching (similarity >= 0.55):
      Find stored records where all-market odds profile closely matches today's.
      Same fixture gets +0.20 bonus. Reverse fixture flips outcomes.

    STAGE 2 — H2H fallback (when odds don't match):
      If no good odds match found, look at ALL previous meetings between these
      two specific teams regardless of odds.
    """
    if not fp_db:
        return None

    query_key    = _odds_fp_key(odds)
    # Canonical key = alphabetical sort (matches how records are stored)
    canon_parts  = sorted([home, away])
    fixture_key  = "|".join(canon_parts)
    # Is the current match "flipped" vs canonical? (home team is NOT canon first)
    current_flipped = (home != canon_parts[0])

    # ── STAGE 1: Own-league odds similarity search (indexed) ─────────────────
    ODDS_THRESH = 0.55

    best_score       = 0.0
    best_record      = None
    best_is_same_fix = False

    # Build or reuse the odds bucket index — invalidate when fp_db size changes
    _oidx_size = model.get("_odds_idx_size", -1) if model else -1
    if _oidx_size != len(fp_db):
        _odds_idx = _rebuild_odds_index(fp_db)
        if model is not None:
            model["_odds_idx"]      = _odds_idx
            model["_odds_idx_size"] = len(fp_db)
    else:
        _odds_idx = model.get("_odds_idx", {}) if model else {}
        if not _odds_idx:
            _odds_idx = _rebuild_odds_index(fp_db)

    # Always include the exact fixture records regardless of odds bucket
    _same_fix_records = [(fixture_key, r) for r in fp_db.get(fixture_key, [])]

    # Candidate set: same-fixture records + records in neighbouring odds buckets
    try:
        _qh = float(query_key[0]) if query_key else 0.0
    except (IndexError, TypeError, ValueError):
        _qh = 0.0
    _candidates = list(_same_fix_records)
    if _qh > 0:
        for _b_offset in (-0.2, 0.0, 0.2):
            _bucket = round(round((_qh + _b_offset) / 0.2) * 0.2, 1)
            for _fk, _rec in _odds_idx.get(_bucket, []):
                if _fk != fixture_key:   # same-fixture already included above
                    _candidates.append((_fk, _rec))
    else:
        # No odds available — fall back to full scan (rare cold-start case)
        _candidates = [(fk, r) for fk, recs in fp_db.items()
                       if isinstance(recs, list) for r in recs]

    for fk, rec in _candidates:
        is_same    = (fk == fixture_key)
        stored_key = tuple(rec.get("fp_key", []))
        sim        = _fp_similarity(stored_key, query_key)
        sim_boosted = min(1.0, sim + 0.20) if is_same else sim
        if sim_boosted > best_score:
            best_score       = sim_boosted
            best_record      = rec
            best_is_same_fix = is_same
            best_raw_odds_sim = sim

    # No cross-league fallback — own league only

    def _flip_htft(h):
        if not h: return h
        parts = h.split("/")
        if len(parts) == 2:
            f = {"1":"2","2":"1","X":"X","?":"?"}
            return f.get(parts[0],parts[0]) + "/" + f.get(parts[1],parts[1])
        return h

    matched_markets = _build_matched_markets(fp_db, home, away, odds)

    if best_record and best_score >= ODDS_THRESH:
        # Good match found — tag source precisely:
        #   same_fixture_odds_match  → same teams + odds align
        #   same_fixture_only        → same teams but odds differ
        #   odds_match_only          → different teams, pure odds similarity
        if best_is_same_fix and best_raw_odds_sim >= ODDS_THRESH:
            src = "same_fixture_odds_match"
        elif best_is_same_fix:
            src = "same_fixture_only"
        else:
            src = "odds_match_only"

        # ── Step 1: build today's context (odds key + current positions) ────────
        all_recs    = fp_db.get(fixture_key, [])
        today_odds  = tuple(_odds_fp_key(odds))   # compute from odds param
        # Current league positions (canonical: canon_parts[0]=home, [1]=away)
        _standings_raw = (model or {}).get("_cached_standings", {})
        _n_teams       = len(_standings_raw) or 18
        today_pos_h    = (_standings_raw.get(canon_parts[0], {}).get("position") or None)
        today_pos_a    = (_standings_raw.get(canon_parts[1], {}).get("position") or None)

        def _pos_tier(pos):
            """Map position to tier bucket for similarity comparison."""
            if pos is None:              return None
            if pos <= 5:                 return "TOP"
            if pos <= _n_teams // 2:     return "MID_TOP"
            if pos <= _n_teams - 5:      return "MID_BOT"
            return "BOTTOM"

        today_tier_h = _pos_tier(today_pos_h)
        today_tier_a = _pos_tier(today_pos_a)

        # ── Step 2: score every stored record by odds+strength match ─────────
        # A record scores higher when:
        #   - odds profile closely matches today (primary)
        #   - strength tier of both teams at time of that match matches today (secondary)
        # Records with BOTH matching are the gold standard — use their outcome.
        def _record_match_score(r):
            _os = _fp_similarity(tuple(r.get("fp_key", [])), today_odds)
            # Strength tier match bonus
            _tier_bonus = 0.0
            r_tier_h = _pos_tier(r.get("pos_h"))
            r_tier_a = _pos_tier(r.get("pos_a"))
            if r_tier_h is not None and today_tier_h is not None:
                if r_tier_h == today_tier_h:   _tier_bonus += 0.08
            if r_tier_a is not None and today_tier_a is not None:
                if r_tier_a == today_tier_a:   _tier_bonus += 0.08
            return _os + _tier_bonus, _os

        # Bucket records by how well they match
        # Tier A: odds match (>=0.97) + strength tier match  → highest trust
        # Tier B: odds match (>=0.97) only                   → good
        # Tier C: odds in range (>=0.80) + strength match    → useful
        # Tier D: closest available                          → fallback (never blank)
        tier_a, tier_b, tier_c = [], [], []
        for r in all_recs:
            total_score, odds_sim = _record_match_score(r)
            r_tier_h = _pos_tier(r.get("pos_h"))
            r_tier_a = _pos_tier(r.get("pos_a"))
            strength_match = (
                (r_tier_h == today_tier_h or today_tier_h is None) and
                (r_tier_a == today_tier_a or today_tier_a is None)
            )
            if odds_sim >= 0.97 and strength_match:
                tier_a.append(r)
            elif odds_sim >= 0.97:
                tier_b.append(r)
            elif odds_sim >= 0.80 and strength_match:
                tier_c.append(r)

        # Pick the best available bucket — never blank
        if tier_a:
            signal_pool = tier_a
            match_quality = "odds+strength"
        elif tier_b:
            signal_pool = tier_b
            match_quality = "odds_only"
        elif tier_c:
            signal_pool = tier_c
            match_quality = "strength_range"
        else:
            # Fallback: use single best-odds record (already found above)
            signal_pool = [best_record]
            match_quality = "closest_fallback"

        # ── Step 3: quality-weighted vote across signal pool ─────────────────
        # Each record votes for its outcome, but its vote weight reflects HOW
        # convincingly that outcome happened — based on the 6-match form context
        # stored with that record at learning time.
        #
        # Vote weight logic (combines winner form + loser form):
        #   winner was strong (win_pct >= 55%) AND improving   → HIGH weight (0.90–1.0)
        #   winner was strong but loser also showed form        → MEDIUM weight (0.70–0.85)
        #   winner form unavailable but odds strongly matched   → base weight (0.65)
        #   winner was weak (win_pct < 40%) — lucky win         → LOW weight (0.40–0.55)
        #   winner beat a strong opponent (goals showed fight)  → bonus weight (+0.10)
        #
        # This means:
        #   ARS beats LIV when ARS had 5/6 wins + 2.1 goals/game → weight 0.95
        #   ARS beats LIV when ARS had 1/6 wins (lucky/fluke)    → weight 0.42
        # Result: all seasons contribute but quality seasons dominate the vote.

        def _record_vote_weight(r: dict) -> float:
            """Compute vote quality weight 0.30–1.0 for a single stored record."""
            outcome_r = r.get("outcome", "")
            score_h_r = r.get("score_h", 0) or 0
            score_a_r = r.get("score_a", 0) or 0

            # Identify winner and loser form snapshots (stored at learning time)
            # Records are stored in CANONICAL orientation (canon_parts[0] = H).
            # outcome is also canonical. _form_h = canonical home team at that time.
            fh = r.get("_form_h") or {}
            fa = r.get("_form_a") or {}

            if outcome_r == "HOME":
                w_form = fh   # winner was canonical home
                l_form = fa
                w_goals = score_h_r
                l_goals = score_a_r
            elif outcome_r == "AWAY":
                w_form = fa   # winner was canonical away
                l_form = fh
                w_goals = score_a_r
                l_goals = score_h_r
            else:
                # DRAW — both teams share weight equally; use combined quality
                w_form = fh; l_form = fa
                w_goals = l_goals = (score_h_r + score_a_r) / 2

            # No form data stored (old records before this feature) → neutral weight
            if not w_form.get("n") and not l_form.get("n"):
                return 0.65

            w_win_pct   = w_form.get("win_pct", 50.0)
            w_goal_avg  = w_form.get("goal_avg", 1.5)
            l_win_pct   = l_form.get("win_pct", 50.0)
            l_goal_avg  = l_form.get("goal_avg", 1.5)

            # Base weight from winner's form strength
            if   w_win_pct >= 70: base = 0.92
            elif w_win_pct >= 55: base = 0.80
            elif w_win_pct >= 40: base = 0.65
            else:                 base = 0.42   # winner was struggling — fluke territory

            # Bonus: winner was scoring goals (offensive strength confirmed)
            if w_goal_avg >= 2.0: base += 0.05
            elif w_goal_avg < 0.8: base -= 0.05

            # Penalty: loser was also in strong form → less predictable outcome
            if l_win_pct >= 55: base -= 0.08

            # Bonus: match had goals even in a loss (both teams competitive, not walkover)
            total_goals = w_goals + l_goals
            if total_goals >= 3: base += 0.04  # high-scoring = real game, not fluke
            elif total_goals == 0: base -= 0.06  # 0-0 or 0-N — may have been anomalous

            # Bonus: winner scored AND loser also scored (fought but still lost)
            # This confirms the losing team's weakness was genuine on that day
            if w_goals > 0 and l_goals > 0: base += 0.03

            return max(0.30, min(1.0, base))

        # Compute weighted vote totals per outcome
        _outcome_weights: dict[str, float] = {"HOME": 0.0, "DRAW": 0.0, "AWAY": 0.0}
        _outcome_counts:  dict[str, int]   = {"HOME": 0,   "DRAW": 0,   "AWAY": 0}
        _total_weight = 0.0

        for r in signal_pool:
            out = r.get("outcome")
            if not out:
                continue
            w = _record_vote_weight(r)
            _outcome_weights[out] = _outcome_weights.get(out, 0.0) + w
            _outcome_counts[out]  = _outcome_counts.get(out, 0) + 1
            _total_weight        += w

        if _total_weight > 0:
            outcome         = max(_outcome_weights, key=_outcome_weights.get)
            _pool_vote_rate = _outcome_weights[outcome] / _total_weight
        elif signal_pool:
            # Fallback: raw majority if all weights are zero
            _pool_outcomes  = [r.get("outcome") for r in signal_pool if r.get("outcome")]
            outcome         = max(set(_pool_outcomes), key=_pool_outcomes.count) if _pool_outcomes else "HOME"
            _pool_vote_rate = _pool_outcomes.count(outcome) / len(_pool_outcomes) if _pool_outcomes else 0.0
        else:
            outcome         = best_record.get("outcome", "HOME")
            _pool_vote_rate = 0.0

        dom_outcome = best_record.get("dominant_outcome", outcome)
        dom_htft    = best_record.get("dominant_htft")

        # Confidence boost from match quality and vote unanimity
        if match_quality == "odds+strength":
            best_score = min(1.0, best_score + 0.10 * _pool_vote_rate)
        elif match_quality == "odds_only":
            best_score = min(1.0, best_score + 0.05 * _pool_vote_rate)
        elif match_quality == "strength_range":
            best_score = min(1.0, best_score + 0.03 * _pool_vote_rate)

        # ── Step 4: last-6 cross-check (all meetings, any odds/strength) ──────
        # Final sanity check — if last 6 meetings of this exact fixture
        # consistently say something different, temper the confidence.
        last6      = all_recs[-6:] if len(all_recs) >= 6 else all_recs
        _l6_conf   = 0.0
        _l6_agrees = False
        if last6:
            last6_outcomes = [r.get("outcome") for r in last6 if r.get("outcome")]
            if last6_outcomes:
                _l6_majority      = max(set(last6_outcomes), key=last6_outcomes.count)
                _l6_majority_rate = last6_outcomes.count(_l6_majority) / len(last6_outcomes)
                _l6_agrees = (_l6_majority == outcome)
                _l6_conf   = _l6_majority_rate
                if _l6_agrees and _l6_majority_rate >= 0.60:
                    best_score = min(1.0, best_score + 0.05)
                elif not _l6_agrees and _l6_majority_rate >= 0.60:
                    best_score = max(0.0, best_score - 0.08)

        # ── Flip back to actual match perspective ─────────────────────────────
        if current_flipped:
            flip_map    = {"HOME":"AWAY","AWAY":"HOME","DRAW":"DRAW"}
            outcome     = flip_map.get(outcome, outcome)
            dom_outcome = flip_map.get(dom_outcome, dom_outcome)
            dom_htft    = _flip_htft(dom_htft)

        return {
            "confidence":       best_score,
            "dominant_outcome": dom_outcome,
            "dominant_htft":    dom_htft,
            "n_samples":        len(all_recs),
            "signal_pool_size": len(signal_pool),
            "match_quality":    match_quality,
            "pool_vote_rate":   round(_pool_vote_rate, 3),
            "flipped":          current_flipped,
            "matched_markets":  matched_markets,
            "source":           src,
            "raw_odds_sim":     best_raw_odds_sim,
            "last6_agrees":     _l6_agrees,
            "last6_conf":       round(_l6_conf, 3),
        }

    # best_record exists but score < ODDS_THRESH — still track same-fixture for Stage 2
    # We expose this so Stage 2 (H2H) can be skipped in favour of form-only for same teams
    _same_fixture_low_odds = best_is_same_fix and best_record is not None

    # ── STAGE 2: H2H fallback — odds didn't match, use all meetings ──────────
    # All records now stored under canonical key (alphabetical)
    all_meetings = fp_db.get(fixture_key, [])
    total_meetings = len(all_meetings)

    if total_meetings == 0:
        return None   # Never met before — no fallback possible

    h2h = _h2h_home_analysis(fp_db, home, away)

    # Get current 6-match form for cross-validation
    if model:
        home_games = _team_last6(home, model)
        away_games = _team_last6(away, model)
        _, _, home_wpct = _form_win_pct(home_games)
        _, _, away_wpct = _form_win_pct(away_games)
    else:
        home_wpct = 50.0
        away_wpct = 50.0

    cv = _cross_validate_h2h_vs_form(h2h, [], [], home_wpct, away_wpct)

    if not cv.get("signal"):
        return None   # H2H too mixed — no reliable signal

    # Build HTFT from H2H home venue records
    hv_recs  = fp_db.get(fixture_key_fwd, [])
    dom_htft = None
    if hv_recs:
        htfts = [r.get("htft_result") for r in hv_recs
                 if r.get("htft_result") and "?" not in r.get("htft_result","")]
        if htfts:
            from collections import Counter
            dom_htft = Counter(htfts).most_common(1)[0][0]

    # Build matched markets from H2H records
    h2h_markets = {}
    hv = h2h.get("home_venue", {})
    if hv.get("n", 0) > 0:
        n = hv["n"]
        for out in ("HOME","DRAW","AWAY"):
            pct = hv.get(f"{out}_pct", 0.0)
            cnt = hv.get(f"{out}_n", 0)
            icon = {"HOME":"🏠","DRAW":"🤝","AWAY":"✈️"}[out]
            if cnt > 0:
                h2h_markets[f"1X2 {icon} {out} (same venue)"] = f"{pct}% ({cnt}/{n})"
        if hv.get("htft_top3"):
            _t = {"1":"H","X":"D","2":"A"}
            for htft_k, cnt in hv["htft_top3"]:
                pct = round(cnt/n*100, 1)
                parts = htft_k.split("/")
                label = f"{_t.get(parts[0],parts[0])}/{_t.get(parts[1],parts[1])}" if len(parts)==2 else htft_k
                h2h_markets[f"HT/FT {label} (H2H)"] = f"{pct}% ({cnt}/{n})"
        if hv.get("btts_yes_pct") is not None:
            y = hv["btts_yes_pct"]
            h2h_markets["BTTS (H2H)"] = f"Yes {y:.0f}% / No {100-y:.0f}%"
        if hv.get("over25_pct") is not None:
            o = hv["over25_pct"]
            h2h_markets["O/U 2.5 (H2H)"] = f"Over {o:.0f}% / Under {100-o:.0f}%"
    if hv.get("avg_gf") is not None:
        h2h_markets["Avg score (H2H home)"] = f"{hv['avg_gf']:.1f} – {hv['avg_ga']:.1f}"

    # Also add all-meetings stats
    av = h2h.get("away_venue", {})
    if av.get("n", 0) > 0:
        an = av["n"]
        for out in ("HOME","DRAW","AWAY"):
            pct = av.get(f"{out}_pct", 0.0)
            cnt = av.get(f"{out}_n", 0)
            if cnt > 0:
                icon = {"HOME":"🏠","DRAW":"🤝","AWAY":"✈️"}[out]
                h2h_markets[f"1X2 {icon} {out} (reversed venue)"] = f"{pct}% ({cnt}/{an})"

    return {
        "confidence":       cv["confidence"] / 100,   # normalise to 0-1
        "dominant_outcome": cv["signal"],
        "dominant_htft":    dom_htft,
        "n_samples":        total_meetings,
        "flipped":          False,
        "matched_markets":  h2h_markets,
        "source":           "h2h_fallback",
        "h2h_cv":           cv,                       # cross-validation details
    }


def _get_position(fp_db: dict, team: str, model: dict | None) -> int | None:
    """Return current standings position for a team, or None if unknown."""
    if not model:
        return None
    standings = model.get("_cached_standings", {})
    entry = standings.get(team, {})
    return entry.get("position") or None


def _rebuild_team_index(fp_db: dict) -> dict:
    """Build team_name → [fixture_key, ...] index from fp_db for O(1) team lookups."""
    idx: dict[str, list[str]] = {}
    for fk in fp_db:
        parts = fk.split("|")
        if len(parts) != 2:
            continue
        for name in parts:
            idx.setdefault(name.lower(), []).append(fk)
    return idx


def _rebuild_odds_index(fp_db: dict) -> dict:
    """
    Build odds-bucket → [(fixture_key, record), ...] index.
    Bucketed on 1X2 home odds rounded to 0.2 steps — narrows the odds
    similarity search from all 4600 fixtures to ~200 candidates (same odds band).
    Allows _fp_fingerprint_match to skip 95% of records without checking them.
    """
    idx: dict[float, list[tuple]] = {}
    for fk, records in fp_db.items():
        if not isinstance(records, list):
            continue
        for rec in records:
            fp_key = rec.get("fp_key", [])
            if not fp_key:
                continue
            try:
                h_odds = float(fp_key[0])
            except (IndexError, TypeError, ValueError):
                continue
            # Use 0.2-wide buckets so a query at 1.95 hits both 1.8 and 2.0 buckets
            bucket = round(round(h_odds / 0.2) * 0.2, 1)
            idx.setdefault(bucket, []).append((fk, rec))
    return idx


def _team_last6(team: str, model: dict, n: int = 6) -> list[dict]:
    """
    Get last N match results for a team.

    PRIMARY source: match_log — the full chronological record of every match the
    bot has ever seen for this league (up to 2000 entries). This is much richer
    than fingerprint_db which caps at 15 records per fixture pair.

    FALLBACK: fingerprint_db scan — used only when match_log is absent or empty.

    Using match_log means form is derived from the bot's COMPLETE experience:
    a team that has played 80 rounds will have all 80 rounds available for form,
    not just the 6-15 records that fit in fp_db for each individual fixture.
    """
    tl        = team.lower()
    match_log = model.get("match_log", [])

    if match_log:
        # Scan match_log for this team's appearances (chronological, oldest first)
        games = []
        for entry in match_log:
            eh = (entry.get("home") or "").lower()
            ea = (entry.get("away") or "").lower()
            sh = entry.get("score_h"); sa = entry.get("score_a")
            if sh is None or sa is None:
                continue
            if tl in eh or eh.startswith(tl[:3]):
                games.append({
                    "gf": sh, "ga": sa, "role": "HOME",
                    "opponent": entry.get("away", ""),
                    "round_id": entry.get("round_id", 0),
                })
            elif tl in ea or ea.startswith(tl[:3]):
                games.append({
                    "gf": sa, "ga": sh, "role": "AWAY",
                    "opponent": entry.get("home", ""),
                    "round_id": entry.get("round_id", 0),
                })
        if games:
            games.sort(key=lambda g: g["round_id"])
            return games[-n:]

    # ── Fallback: derive from fingerprint_db (old path) ──────────────────────
    fp_db = model.get("fingerprint_db", {})
    if not fp_db:
        return []

    idx_size = model.get("_team_idx_size", -1)
    if idx_size != len(fp_db):
        model["_team_idx"]      = _rebuild_team_index(fp_db)
        model["_team_idx_size"] = len(fp_db)

    team_idx = model["_team_idx"]
    fk_list  = team_idx.get(tl, [])

    games = []
    for fk in fk_list:
        records = fp_db.get(fk, [])
        parts   = fk.split("|")
        if len(parts) != 2:
            continue
        ht, at = parts[0].lower(), parts[1].lower()
        for rec in records:
            sh = rec.get("score_h"); sa = rec.get("score_a")
            if sh is None or sa is None:
                continue
            if tl in ht:
                games.append({
                    "gf": sh, "ga": sa, "role": "HOME",
                    "opponent": parts[1],
                    "round_id": rec.get("round_id", 0),
                })
            else:
                games.append({
                    "gf": sa, "ga": sh, "role": "AWAY",
                    "opponent": parts[0],
                    "round_id": rec.get("round_id", 0),
                })
    games.sort(key=lambda g: g["round_id"])
    return games[-n:]


def _form_win_pct(games: list) -> tuple[int, int, float]:
    """Returns (wins, total, win_pct) from a list of {gf, ga} games."""
    if not games:
        return 0, 0, 0.0
    wins  = sum(1 for g in games if g["gf"] > g["ga"])
    total = len(games)
    return wins, total, round(wins / total * 100, 1)


def _get_strong_side(home: str, away: str, standings: dict) -> str | None:
    """
    Returns "HOME" if home team is STRONG and away is WEAK/MODERATE,
            "AWAY" if away team is STRONG and home is WEAK/MODERATE,
            None  if neither is a valid STRONG vs WEAK/MODERATE matchup.
    """
    if not standings:
        return None
    tier_map  = _get_all_tiers(standings)
    h_tier    = _find_tier(home, tier_map)
    a_tier    = _find_tier(away, tier_map)
    if h_tier == "STRONG" and a_tier in ("WEAK", "MODERATE"):
        return "HOME"
    if a_tier == "STRONG" and h_tier in ("WEAK", "MODERATE"):
        return "AWAY"
    return None


def _get_opponent_form_at_round(opponent: str, round_id: int,
                                  match_log: list, n: int = 5) -> dict:
    """
    Get an opponent's form BEFORE a specific round — i.e. what state they
    were in at the moment they played against our team.

    This answers: "when ARS lost to LIV in round 142, was LIV actually
    strong at that time, or were they on a bad run too?"

    Returns:
      win_pct:       float 0–100 (wins in last n games before this round)
      goal_avg:      float (goals scored avg in those games)
      concede_avg:   float (goals conceded avg)
      trend:         "RISING" / "FALLING" / "STABLE"
      games_found:   int (how many games were found)
      tier_context:  str description e.g. "LIV was on 4W/5 before this match"
    """
    opp_lower = opponent.lower()
    # Get all games for this opponent strictly BEFORE this round
    prior_games = []
    for entry in match_log:
        r_id = entry.get("round_id", 0)
        try:
            r_id = int(r_id)
        except (TypeError, ValueError):
            r_id = 0
        if r_id >= int(round_id):
            continue   # only games BEFORE this round
        eh = (entry.get("home") or "").lower()
        ea = (entry.get("away") or "").lower()
        sh = entry.get("score_h"); sa = entry.get("score_a")
        if sh is None or sa is None:
            continue
        if opp_lower in eh or eh.startswith(opp_lower[:3]):
            prior_games.append({
                "gf": sh, "ga": sa,
                "round_id": r_id,
            })
        elif opp_lower in ea or ea.startswith(opp_lower[:3]):
            prior_games.append({
                "gf": sa, "ga": sh,
                "round_id": r_id,
            })

    if not prior_games:
        return {"win_pct": 50.0, "goal_avg": 1.5, "concede_avg": 1.5,
                "trend": "STABLE", "games_found": 0,
                "tier_context": f"{opponent}: no prior data"}

    prior_games.sort(key=lambda g: g["round_id"])
    recent = prior_games[-n:]
    n_g    = len(recent)

    wins   = sum(1 for g in recent if g["gf"] > g["ga"])
    draws  = sum(1 for g in recent if g["gf"] == g["ga"])
    gf_avg = sum(g["gf"] for g in recent) / n_g
    ga_avg = sum(g["ga"] for g in recent) / n_g
    wpct   = round(wins / n_g * 100, 1)

    # Trend: compare last 2 vs first 2
    if n_g >= 4:
        first2_w = sum(1 for g in recent[:2] if g["gf"] > g["ga"])
        last2_w  = sum(1 for g in recent[-2:] if g["gf"] > g["ga"])
        if   last2_w > first2_w: trend = "RISING"
        elif last2_w < first2_w: trend = "FALLING"
        else:                    trend = "STABLE"
    else:
        trend = "STABLE"

    tier_context = (
        f"{opponent}: {wins}W/{draws}D/{n_g-wins-draws}L "
        f"in {n_g} before this match ({trend.lower()})"
    )

    return {
        "win_pct":      wpct,
        "goal_avg":     round(gf_avg, 2),
        "concede_avg":  round(ga_avg, 2),
        "trend":        trend,
        "games_found":  n_g,
        "tier_context": tier_context,
    }


def _score_result_quality(game: dict, model: dict, tier_map: dict) -> dict:
    """
    RESULT QUALITY ANALYSER — scores a single past result for how meaningful it was.

    For each game in a team's last 6, answers:
      "Was this win/loss/draw genuinely meaningful, or was it hollow?"

    Checks THREE things about the OPPONENT at the time of that match:

    1. OPPONENT TIER — were they strong, moderate, or weak at that time?
       (from current tier_map — best approximation we have)

    2. OPPONENT'S OWN FORM BEFORE THAT MATCH — were they on a good or bad run?
       A win against an opponent who was 4W/5 = high quality win.
       A loss to an opponent who was 0W/5 = alarming loss.
       A win against an opponent who was 0W/5 = hollow win (they were in crisis).

    3. SCORE MARGIN AND GOAL CONTEXT — was it convincing or a fluke?
       Won 4-0 = dominant.  Won 1-0 = narrow.
       Lost 0-1 = competitive.  Lost 0-4 = collapse.

    Returns:
      result_quality:  float  0.0–1.0
        1.0 = maximum quality (beat a strong team in form convincingly)
        0.7 = solid result  (beat moderate team in good form, or narrow win vs strong)
        0.5 = neutral / hollow  (beat a weak crisis team, or expected result)
        0.3 = poor result  (drew with weak team, or lost narrowly to moderate)
        0.0 = alarming  (lost to weak team in bad form)

      meaning:  str — human-readable explanation
      result:   "W" / "D" / "L"
      opp_form: dict — opponent's form snapshot at time of match
    """
    gf   = game.get("gf", 0)
    ga   = game.get("ga", 0)
    opp  = game.get("opponent", "")
    r_id = game.get("round_id", 0)

    if gf > ga:    result = "W"
    elif gf == ga: result = "D"
    else:          result = "L"

    margin = abs(gf - ga)
    goals_total = gf + ga

    # Opponent tier (from current standings — best we have)
    opp_tier = _find_tier(opp, tier_map) if opp else "UNKNOWN"

    # Opponent's form at the time of this match
    match_log = model.get("match_log", [])
    opp_form  = _get_opponent_form_at_round(opp, r_id, match_log, n=5)
    opp_wpct  = opp_form["win_pct"]    # 0–100
    opp_trend = opp_form["trend"]      # RISING / FALLING / STABLE

    # ── Quality scoring ────────────────────────────────────────────────────────
    quality = 0.5   # start neutral
    meaning_parts = []

    if result == "W":
        # Base quality from opponent tier
        if   opp_tier == "STRONG":   base = 0.80; meaning_parts.append(f"beat STRONG {opp}")
        elif opp_tier == "MODERATE": base = 0.60; meaning_parts.append(f"beat MODERATE {opp}")
        else:                        base = 0.40; meaning_parts.append(f"beat WEAK {opp}")

        # Bonus/penalty from opponent's own form at the time
        if opp_wpct >= 60:
            base += 0.12   # beat a team that was in good form → real win
            meaning_parts.append(f"opp was hot ({opp_wpct:.0f}% wins)")
        elif opp_wpct <= 25:
            base -= 0.15   # beat a team in crisis → hollow win
            meaning_parts.append(f"opp was in crisis ({opp_wpct:.0f}% wins)")

        if opp_trend == "RISING":
            base += 0.05   # beat a team that was improving = even better
            meaning_parts.append("opp was rising")
        elif opp_trend == "FALLING":
            base -= 0.05   # beat a falling team = less credit
            meaning_parts.append("opp was falling")

        # Score margin
        if   margin >= 3: base += 0.08; meaning_parts.append(f"{gf}-{ga} dominant")
        elif margin == 2: base += 0.04; meaning_parts.append(f"{gf}-{ga} convincing")
        elif margin == 1: base -= 0.02; meaning_parts.append(f"{gf}-{ga} narrow")

        quality = base

    elif result == "L":
        # Base acceptability of loss from opponent tier
        if   opp_tier == "STRONG":   base = 0.45; meaning_parts.append(f"lost to STRONG {opp}")
        elif opp_tier == "MODERATE": base = 0.30; meaning_parts.append(f"lost to MODERATE {opp}")
        else:                        base = 0.10; meaning_parts.append(f"lost to WEAK {opp} ⚠️")

        # Opponent's form at the time
        if opp_wpct >= 60:
            base += 0.10   # lost to a team in excellent form → acceptable
            meaning_parts.append(f"opp was dominant ({opp_wpct:.0f}%)")
        elif opp_wpct <= 25:
            base -= 0.15   # lost to a team in crisis → alarming
            meaning_parts.append(f"opp was in crisis ({opp_wpct:.0f}%) ⚠️")

        if opp_trend == "RISING":
            base += 0.05   # lost to a rising team → somewhat expected
        elif opp_trend == "FALLING":
            base -= 0.08   # lost to a falling team → alarm
            meaning_parts.append("opp was falling ⚠️")

        # Score margin (did they fight back?)
        if goals_total >= 3:
            base += 0.05   # e.g. 1-2, 2-3 — competitive game
            meaning_parts.append("competitive game")
        elif margin >= 3:
            base -= 0.10   # collapsed
            meaning_parts.append(f"{gf}-{ga} collapse")

        quality = base

    else:  # DRAW
        if   opp_tier == "STRONG":   base = 0.65; meaning_parts.append(f"drew with STRONG {opp}")
        elif opp_tier == "MODERATE": base = 0.45; meaning_parts.append(f"drew with MODERATE {opp}")
        else:                        base = 0.25; meaning_parts.append(f"drew with WEAK {opp} (missed win)")

        if opp_wpct >= 60:
            base += 0.08   # held a hot team = good point
            meaning_parts.append(f"opp in form ({opp_wpct:.0f}%)")
        elif opp_wpct <= 25:
            base -= 0.12   # only drew with crisis team = poor
            meaning_parts.append(f"opp in crisis ({opp_wpct:.0f}%)")

        quality = base

    quality = max(0.0, min(1.0, quality))
    meaning = " | ".join(meaning_parts) if meaning_parts else "neutral result"

    return {
        "result_quality": round(quality, 3),
        "meaning":        meaning,
        "result":         result,
        "opp_tier":       opp_tier,
        "opp_form":       opp_form,
        "goals":          f"{gf}-{ga}",
        "margin":         margin,
    }


def _compute_team_momentum(team: str, model: dict,
                            tier_map: dict, n: int = 6) -> dict:
    """
    MOMENTUM INTELLIGENCE ENGINE — computes a real momentum score 0–100 for a team.

    Now uses RESULT QUALITY SCORING for each of the last 6 games:
    Each game's contribution to the momentum score depends on:
      - Was the opponent worth beating? (tier + their own form at that time)
      - Was the win/loss/draw convincing or hollow?
      - Is the team trending up or down across those 6 games?

    Three dimensions:

    1. QUALITY-WEIGHTED TREND (up to 40 pts)
       Not just W/L counts but quality-weighted points.
       Win quality 0.9 vs STRONG in form = 3 × 0.9 = 2.7 pts
       Win quality 0.35 vs WEAK in crisis = 3 × 0.35 = 1.05 pts
       Trend = second-half quality-pts vs first-half quality-pts.

    2. RESULT QUALITY AVERAGE (up to 35 pts)
       Average quality score across all 6 games → scaled to 0–35.
       High avg quality = team is consistently doing the right things.
       Low avg quality = results are hollow or alarming.

    3. GOAL MOMENTUM (up to 25 pts)
       Scoring tells you about attacking confidence.
       Conceding tells you about defensive fragility.

    Returns:
      score:           float 0–100
      trend:           "RISING" / "FALLING" / "STABLE"
      trend_str:       "W(✅)L(⚠️)W(✅)..." showing quality label per game
      result_qualities: list of per-game quality dicts
      avg_quality:     float 0–1 average result quality
      attack_avg:      float
      defend_avg:      float
      games_used:      int
      summary:         str
    """
    games = _team_last6(team, model, n=n)
    if not games:
        return {
            "score": 50.0, "trend": "STABLE", "trend_str": "no data",
            "result_qualities": [], "avg_quality": 0.5,
            "attack_avg": 1.5, "defend_avg": 1.5,
            "games_used": 0, "summary": "no form data",
            "opp_quality": 0.0, "goal_momentum": 0.0,
        }

    n_g = len(games)

    # ── Score each of the last 6 results for quality ─────────────────────────
    result_qualities = []
    for g in games:
        rq = _score_result_quality(g, model, tier_map)
        result_qualities.append(rq)

    avg_quality = sum(r["result_quality"] for r in result_qualities) / n_g

    # ── 1. QUALITY-WEIGHTED TREND ─────────────────────────────────────────────
    def _quality_pts(g, rq):
        raw = 3 if g["gf"] > g["ga"] else (1 if g["gf"] == g["ga"] else 0)
        return raw * rq["result_quality"]

    mid        = n_g // 2
    first_half = list(zip(games[:mid], result_qualities[:mid]))   if mid > 0 else []
    second_half= list(zip(games[mid:], result_qualities[mid:]))   if mid > 0 else list(zip(games, result_qualities))

    first_qpts  = sum(_quality_pts(g, rq) for g, rq in first_half)  / max(1, len(first_half))
    second_qpts = sum(_quality_pts(g, rq) for g, rq in second_half) / max(1, len(second_half))

    trend_delta = second_qpts - first_qpts
    if   trend_delta >= 0.8:  trend = "RISING";  trend_score = 35.0
    elif trend_delta >= 0.2:  trend = "RISING";  trend_score = 25.0
    elif trend_delta >= -0.2: trend = "STABLE";  trend_score = 20.0
    elif trend_delta >= -0.7: trend = "FALLING"; trend_score = 12.0
    else:                     trend = "FALLING"; trend_score = 5.0

    # Build trend string with quality indicator per game
    trend_parts = []
    for g, rq in zip(games, result_qualities):
        r   = rq["result"]
        q   = rq["result_quality"]
        tag = "✅" if q >= 0.65 else ("➡️" if q >= 0.45 else "⚠️")
        trend_parts.append(f"{r}{tag}")
    trend_str = ",".join(trend_parts) + f" → {trend.lower()}"

    # ── 2. RESULT QUALITY AVERAGE → scaled to 0–35 ───────────────────────────
    # avg_quality 0.0 = all alarming → 0 pts
    # avg_quality 0.5 = neutral      → 17.5 pts
    # avg_quality 1.0 = exceptional  → 35 pts
    quality_score = avg_quality * 35.0
    opp_raw = avg_quality   # keep for return

    # ── 3. GOAL MOMENTUM ──────────────────────────────────────────────────────
    attack_avg = sum(g["gf"] for g in games) / n_g
    defend_avg = sum(g["ga"] for g in games) / n_g

    goal_score = 0.0
    if   attack_avg >= 2.5: goal_score += 12.0
    elif attack_avg >= 2.0: goal_score += 10.0
    elif attack_avg >= 1.5: goal_score += 5.0
    elif attack_avg < 0.8:  goal_score -= 10.0
    elif attack_avg < 1.2:  goal_score -= 5.0

    if   defend_avg <= 0.8: goal_score += 8.0
    elif defend_avg <= 1.3: goal_score += 4.0
    elif defend_avg >= 2.5: goal_score -= 8.0
    elif defend_avg >= 2.0: goal_score -= 4.0

    if n_g >= 4:
        recent3_gf = sum(g["gf"] for g in games[-3:]) / 3
        older3_gf  = sum(g["gf"] for g in games[:-3]) / max(1, n_g - 3)
        if recent3_gf - older3_gf >= 0.7:
            goal_score += 5.0
        elif older3_gf - recent3_gf >= 0.7:
            goal_score -= 5.0

    goal_normed = max(0.0, min(25.0, 12.5 + goal_score))

    # ── Final score ────────────────────────────────────────────────────────────
    raw_score   = trend_score + quality_score + goal_normed
    final_score = min(100.0, max(0.0, raw_score))

    wins    = sum(1 for g in games if g["gf"] > g["ga"])
    win_pct = round(wins / n_g * 100) if n_g > 0 else 0

    if final_score >= 75:
        summary = f"🔥 On fire — {trend_str} | atk {attack_avg:.1f} def {defend_avg:.1f}"
    elif final_score >= 58:
        summary = f"📈 Good form — {trend_str} | atk {attack_avg:.1f} def {defend_avg:.1f}"
    elif final_score >= 42:
        summary = f"➡️ Mixed form — {trend_str} | atk {attack_avg:.1f} def {defend_avg:.1f}"
    elif final_score >= 25:
        summary = f"📉 Weak form — {trend_str} | atk {attack_avg:.1f} def {defend_avg:.1f}"
    else:
        summary = f"💀 Crisis — {trend_str} | atk {attack_avg:.1f} def {defend_avg:.1f}"

    return {
        "score":            round(final_score, 1),
        "trend":            trend,
        "trend_str":        trend_str,
        "result_qualities": result_qualities,
        "avg_quality":      round(avg_quality, 3),
        "opp_quality":      round(opp_raw, 3),
        "goal_momentum":    round(goal_score, 1),
        "attack_avg":       round(attack_avg, 2),
        "defend_avg":       round(defend_avg, 2),
        "games_used":       n_g,
        "win_pct":          win_pct,
        "summary":          summary,
    }




def _six_match_form_audit(home: str, away: str,
                           model: dict,
                           standings: dict) -> tuple[str | None, float, dict]:
    """
    Full 6-match form audit — upgraded with Momentum Intelligence.

    Now computes full momentum scores for both teams (trend direction +
    opponent quality + goal progression) before issuing any signal.
    The confidence adjustment is driven by the MOMENTUM GAP between teams,
    not just raw win% — so a rising weak team vs a falling strong team
    correctly reduces confidence in the strong team.

    Returns:
      signal:      "HOME" / "AWAY" / None
      conf_adjust: float — positive = boost, negative = reduce
      report:      dict with full audit + momentum details
    """
    tier_map  = _get_all_tiers(standings)
    home_tier = _find_tier(home, tier_map)
    away_tier = _find_tier(away, tier_map)

    home_games = _team_last6(home, model)
    away_games = _team_last6(away, model)

    home_wins, home_total, home_wpct = _form_win_pct(home_games)
    away_wins, away_total, away_wpct = _form_win_pct(away_games)

    # ── Momentum scores for both teams ────────────────────────────────────────
    home_momentum = _compute_team_momentum(home, model, tier_map)
    away_momentum = _compute_team_momentum(away, model, tier_map)
    mom_gap = home_momentum["score"] - away_momentum["score"]  # +ve = home stronger

    # ── Identify strong / weak side ────────────────────────────────────────────
    if home_tier == "STRONG" and away_tier in ("WEAK", "MODERATE"):
        strong_team, strong_games, strong_wpct = home, home_games, home_wpct
        strong_momentum = home_momentum
        weak_team,   weak_games,   weak_wpct   = away, away_games, away_wpct
        weak_momentum = away_momentum
        expected_winner = "HOME"
    elif away_tier == "STRONG" and home_tier in ("WEAK", "MODERATE"):
        strong_team, strong_games, strong_wpct = away, away_games, away_wpct
        strong_momentum = away_momentum
        weak_team,   weak_games,   weak_wpct   = home, home_games, home_wpct
        weak_momentum = home_momentum
        expected_winner = "AWAY"
    else:
        # Not a classic tier matchup — still emit a signal if momentum gap is large
        if abs(mom_gap) >= 20 and home_games and away_games:
            _signal  = "HOME" if mom_gap > 0 else "AWAY"
            _adj     = min(8.0, abs(mom_gap) * 0.2)
            _report  = {
                "strong_team": home if mom_gap > 0 else away,
                "weak_team":   away if mom_gap > 0 else home,
                "strong_tier": home_tier if mom_gap > 0 else away_tier,
                "weak_tier":   away_tier if mom_gap > 0 else home_tier,
                "home_momentum": home_momentum,
                "away_momentum": away_momentum,
                "mom_gap":       round(mom_gap, 1),
                "verdict":       f"Momentum gap {abs(mom_gap):.0f} pts → {_signal}",
                "conf_adjust":   round(_adj, 1),
                "source":        "momentum_gap_only",
            }
            return _signal, _adj, _report
        return None, 0.0, {}

    # ── Audit strong team's recent losses ─────────────────────────────────────
    recent_strong = strong_games[-3:]
    losses = [g for g in recent_strong if g["gf"] < g["ga"]]
    strong_loss_penalty = 0.0
    loss_reasons = []

    for loss in losses:
        gf, ga   = loss["gf"], loss["ga"]
        total_g  = gf + ga
        margin   = ga - gf
        opp_tier = _find_tier(loss["opponent"], tier_map)

        if opp_tier == "STRONG":
            penalty = 1.0
            reason  = f"lost {gf}-{ga} vs strong (fair)"
        elif total_g >= 4:
            penalty = 2.0
            reason  = f"lost {gf}-{ga} high-scoring (competitive)"
        elif total_g >= 2 and margin == 1:
            if opp_tier == "MODERATE":
                penalty = 6.0
                reason  = f"lost {gf}-{ga} vs moderate (borderline)"
            else:
                penalty = 4.0
                reason  = f"lost {gf}-{ga} narrow"
        elif total_g <= 1:
            if opp_tier in ("MODERATE", "WEAK"):
                penalty = 14.0
                reason  = f"lost {gf}-{ga} vs {opp_tier.lower()} (warning)"
            else:
                penalty = 8.0
                reason  = f"lost {gf}-{ga} low-scoring"
        else:
            penalty = 5.0
            reason  = f"lost {gf}-{ga}"

        strong_loss_penalty += penalty
        loss_reasons.append(reason)

    # ── Momentum penalty on strong team: if it's FALLING, add extra penalty ──
    if strong_momentum["trend"] == "FALLING":
        strong_loss_penalty += min(10.0, (50.0 - strong_momentum["score"]) * 0.25)
        loss_reasons.append(f"strong team momentum FALLING ({strong_momentum['score']:.0f}/100)")

    # ── Audit weak/moderate team's recent wins ────────────────────────────────
    recent_weak = weak_games[-3:]
    wins_weak   = [g for g in recent_weak if g["gf"] > g["ga"]]
    weak_threat_score = 0.0
    win_reasons = []

    for win in wins_weak:
        gf, ga   = win["gf"], win["ga"]
        total_g  = gf + ga
        margin   = gf - ga
        opp_tier = _find_tier(win["opponent"], tier_map)

        if opp_tier in ("WEAK", "MODERATE"):
            if margin >= 3:
                score = 8.0
                reason = f"won {gf}-{ga} vs {opp_tier.lower()} (dominant)"
            elif total_g >= 4:
                score = 6.0
                reason = f"won {gf}-{ga} vs {opp_tier.lower()} (competitive)"
            else:
                score = 3.0
                reason = f"won {gf}-{ga} vs {opp_tier.lower()} (narrow)"
        elif opp_tier == "STRONG":
            score  = 15.0
            reason = f"won {gf}-{ga} vs strong (major threat)"
        else:
            score  = 4.0
            reason = f"won {gf}-{ga}"

        weak_threat_score += score
        win_reasons.append(reason)

    # ── Momentum bonus on weak team: if it's RISING, add threat ─────────────
    if weak_momentum["trend"] == "RISING":
        rising_bonus = min(12.0, (weak_momentum["score"] - 50.0) * 0.3)
        if rising_bonus > 0:
            weak_threat_score += rising_bonus
            win_reasons.append(f"weak team momentum RISING ({weak_momentum['score']:.0f}/100)")

    # ── Determine signal and confidence adjustment ─────────────────────────────
    loss_fair      = strong_loss_penalty < 10.0
    genuine_threat = weak_threat_score >= 12.0

    # ── Base verdict from existing penalty/threat scoring ────────────────────
    if loss_fair and not genuine_threat:
        conf_adjust = max(0.0, 12.0 - strong_loss_penalty * 0.5)
        verdict     = "✅ Strong team confirmed"
    elif loss_fair and genuine_threat:
        conf_adjust = max(-5.0, 5.0 - weak_threat_score * 0.3)
        verdict     = "⚠️ Strong team ok but weak side showing form"
    else:
        conf_adjust = -strong_loss_penalty
        verdict     = "❌ Strong team showing weakness"

    signal = expected_winner

    # ── Momentum gap adjustment ────────────────────────────────────────────────
    # After all penalty/threat scoring, if the strong team's momentum score
    # is MUCH higher than the weak team's, add extra confirmation boost.
    # If the weak team is surging past the strong team in momentum, dampen.
    _strong_mom = strong_momentum["score"]
    _weak_mom   = weak_momentum["score"]
    _mom_gap    = _strong_mom - _weak_mom   # +ve = strong team has better momentum
    if _mom_gap >= 20:
        _mom_boost  = min(8.0, _mom_gap * 0.15)
        conf_adjust += _mom_boost
        verdict     += f" | momentum edge +{_mom_boost:.1f} ({_strong_mom:.0f} vs {_weak_mom:.0f})"
    elif _mom_gap <= -15:
        _mom_damp   = min(10.0, abs(_mom_gap) * 0.18)
        conf_adjust -= _mom_damp
        verdict     += f" | weak team surging momentum -{_mom_damp:.1f} ({_weak_mom:.0f} vs {_strong_mom:.0f})"

    # ── Recovery Pattern Analysis ──────────────────────────────────────────────
    recovery = {}
    has_recent_loss = any(g["gf"] < g["ga"] for g in strong_games[-3:])
    has_weak_recent_win = any(g["gf"] > g["ga"] for g in weak_games[-3:])
    weak_tier = away_tier if expected_winner == "HOME" else home_tier

    if (has_recent_loss or has_weak_recent_win) and model:
        recovery = _recovery_pattern_analysis(
            strong_team  = strong_team,
            opponent     = weak_team,
            opponent_tier = weak_tier,
            model        = model,
            standings    = standings,
        )
        if recovery.get("data_confirmed"):
            rec_signal = recovery["recovery_signal"]
            if rec_signal == "STRONG_RECOVERS":
                bonus = recovery["strong_conf_bonus"]
                conf_adjust += bonus
                if verdict.startswith("❌"):
                    verdict = f"🔄 Strong team recovering (confirmed by AI, +{bonus:.0f})"
                elif verdict.startswith("⚠️"):
                    verdict = f"🔄 Strong team recovering despite weak-side form (+{bonus:.0f})"
                else:
                    verdict += f" + recovery confirmed (+{bonus:.0f})"
            elif rec_signal == "WEAK_REPEATS":
                penalty = min(recovery["weak_conf_bonus"], 8.0)
                conf_adjust -= penalty
                verdict = f"⚠️ Upset pattern noted — weak side showed genuine form ({'-'}{penalty:.0f})"

    # ── Build full report for display ─────────────────────────────────────────
    report = {
        "strong_team":      strong_team,
        "weak_team":        weak_team,
        "strong_tier":      home_tier if expected_winner == "HOME" else away_tier,
        "weak_tier":        weak_tier,
        "strong_wpct":      strong_wpct,
        "strong_wins":      f"{int(strong_wpct*len(strong_games)/100) if strong_games else 0}/{len(strong_games)}",
        "weak_wpct":        weak_wpct,
        "weak_wins":        f"{int(weak_wpct*len(weak_games)/100) if weak_games else 0}/{len(weak_games)}",
        "loss_reasons":     loss_reasons,
        "win_reasons":      win_reasons,
        "loss_penalty":     round(strong_loss_penalty, 1),
        "threat_score":     round(weak_threat_score, 1),
        "verdict":          verdict,
        "conf_adjust":      round(conf_adjust, 1),
        "strong_games_n":   len(strong_games),
        "weak_games_n":     len(weak_games),
        # Momentum Intelligence data
        "home_momentum":    home_momentum,
        "away_momentum":    away_momentum,
        "strong_momentum":  strong_momentum,
        "weak_momentum":    weak_momentum,
        "mom_gap":          round(_mom_gap, 1),
        # Recovery engine output (if it ran)
        "recovery":         recovery,
        "recovery_signal":  recovery.get("recovery_signal", "N/A"),
        "recovery_reasons": recovery.get("reasoning", []),
    }

    return signal, conf_adjust, report





def _recovery_pattern_analysis(
    strong_team: str,
    opponent: str,
    opponent_tier: str,
    model: dict,
    standings: dict,
) -> dict:
    """
    RECOVERY PATTERN ENGINE.

    Called when a STRONG team recently lost and is now playing a WEAK or
    MODERATE team. Determines — with confirming data — whether:

    A) The strong team is likely to RECOVER (bounce back and win), or
    B) The weak/moderate team's win was GENUINE and they may do it again.

    Uses TWO-SIDED confirmation:

    STRONG RECOVERY SIDE (confirming the strong team bounces back):
      — How did they lose? (to a strong opponent = fair; to weak = alarming)
      — Was the loss margin large or narrow? (narrow = more likely to recover)
      — In their last 6 games overall, how many did they win before the loss?
      — Do they have a historical pattern of bouncing back after losses?
        (checks last 10 fixture_mem records: after a loss, how often do they win next?)

    WEAK/MODERATE GENUINE WIN SIDE (confirming the upset was real):
      — Who did the weak/moderate team beat? (beat a strong = major signal)
      — Was the win convincing (margin ≥ 2) or a scrape (margin = 1)?
      — How many of their last 6 were wins? (3+ = on form, not a fluke)
      — Do they have a pattern of following wins with more wins?
        (momentum check from fixture_mem)

    Returns dict:
    {
      "recovery_signal":    "STRONG_RECOVERS" / "WEAK_REPEATS" / "UNCERTAIN",
      "recovery_strength":  float  0.0–1.0   (how confident the signal is)
      "strong_conf_bonus":  float  (positive = boost strong, negative = doubt)
      "weak_conf_bonus":    float  (positive = boost weak/moderate side)
      "reasoning":          list[str]  — every reason for the verdict
      "data_confirmed":     bool   — True only if ≥ 3 confirming data points
      "strong_loss_detail": dict   — about the strong team's recent loss
      "weak_win_detail":    dict   — about the weak team's recent win
    }
    """
    tier_map = _get_all_tiers(standings)
    fp_db    = model.get("fingerprint_db", {})
    ai_mem   = model.get("ai_brain", {}).get("fixture_mem", {})

    result = {
        "recovery_signal":   "UNCERTAIN",
        "recovery_strength": 0.0,
        "strong_conf_bonus": 0.0,
        "weak_conf_bonus":   0.0,
        "reasoning":         [],
        "data_confirmed":    False,
        "strong_loss_detail": {},
        "weak_win_detail":    {},
    }
    reasons      = []
    strong_score = 0.0   # positive = STRONG_RECOVERS
    weak_score   = 0.0   # positive = WEAK_REPEATS

    # ── Pull last 6 games for both teams ──────────────────────────────────────
    strong_games = _team_last6(strong_team, model)
    weak_games   = _team_last6(opponent,    model)

    if not strong_games or not weak_games:
        result["reasoning"] = ["insufficient form data"]
        return result

    # ── STRONG SIDE: analyse their most recent loss ───────────────────────────
    # Find the most recent loss
    recent_strong_loss = None
    for g in reversed(strong_games):
        if g["gf"] < g["ga"]:
            recent_strong_loss = g
            break

    strong_loss_detail = {}
    if recent_strong_loss:
        loss_gf   = recent_strong_loss["gf"]
        loss_ga   = recent_strong_loss["ga"]
        loss_margin = loss_ga - loss_gf
        loss_opp    = recent_strong_loss.get("opponent", "")
        loss_opp_tier = _find_tier(loss_opp, tier_map)
        total_goals   = loss_gf + loss_ga

        strong_loss_detail = {
            "score":     f"{loss_gf}-{loss_ga}",
            "margin":    loss_margin,
            "opponent":  loss_opp,
            "opp_tier":  loss_opp_tier,
            "total_goals": total_goals,
        }

        # Who did they lose to?
        if loss_opp_tier == "STRONG":
            # Lost to a peer — completely acceptable, doesn't affect recovery odds
            strong_score += 6.0
            reasons.append(f"strong lost {loss_gf}-{loss_ga} to another STRONG ({loss_opp}) — fair loss, recovery expected")
        elif loss_opp_tier == "MODERATE":
            if loss_margin == 1:
                # Narrow loss to moderate — borderline, but can recover
                strong_score += 2.0
                reasons.append(f"strong lost narrowly {loss_gf}-{loss_ga} to MODERATE — slight concern, some recovery likely")
            elif total_goals >= 4:
                # High-scoring vs moderate — competitive, less alarming
                strong_score += 1.0
                reasons.append(f"strong lost {loss_gf}-{loss_ga} to MODERATE (high-scoring) — competitive loss")
            else:
                # Clear loss to moderate — genuine weakness
                strong_score -= 2.0
                weak_score   += 3.0
                reasons.append(f"strong lost clearly {loss_gf}-{loss_ga} to MODERATE — weakness confirmed")
        elif loss_opp_tier == "WEAK":
            # Lost to a weak team — alarming signal, might repeat
            strong_score -= 5.0
            weak_score   += 6.0
            reasons.append(f"strong lost {loss_gf}-{loss_ga} to WEAK ({loss_opp}) — serious alarm, upset likely again")
        else:
            # Unknown opponent tier
            strong_score += 0.5
            reasons.append(f"strong lost {loss_gf}-{loss_ga} to unknown-tier opponent")

        # Was the loss narrow or heavy?
        if loss_margin == 1:
            strong_score += 2.0
            reasons.append("narrow 1-goal loss → recovery more likely")
        elif loss_margin >= 3:
            strong_score -= 3.0
            weak_score   += 2.0
            reasons.append(f"heavy {loss_margin}-goal loss → recovery less certain")

        # How was their form BEFORE the loss?
        games_before_loss = [g for g in strong_games if g["round_id"] < recent_strong_loss["round_id"]]
        wins_before = sum(1 for g in games_before_loss if g["gf"] > g["ga"])
        if games_before_loss:
            pre_loss_wpct = wins_before / len(games_before_loss)
            if pre_loss_wpct >= 0.6:
                strong_score += 4.0
                reasons.append(f"strong was winning {wins_before}/{len(games_before_loss)} before this loss — single blip, recovery expected")
            elif pre_loss_wpct <= 0.3:
                strong_score -= 2.0
                reasons.append(f"strong was already struggling before this loss ({wins_before}/{len(games_before_loss)} wins) — pattern of weakness")
    else:
        # No recent loss — strong team has NOT lost recently
        strong_score += 5.0
        reasons.append("strong team has no recent loss — form is solid")

    # ── STRONG SIDE: historical bounce-back pattern from AI memory ────────────
    # Look at fixture_mem for strong team's fixtures — after a loss, how often
    # do they win their very next recorded match?
    bounce_wins = bounce_total = 0
    for fk, mem_records in ai_mem.items():
        parts = fk.split("|")
        if len(parts) != 2:
            continue
        sl = strong_team.lower()
        if sl not in parts[0].lower() and sl not in parts[1].lower():
            continue
        # Look for loss→win pairs in sequence
        for i in range(1, len(mem_records)):
            prev = mem_records[i-1]
            curr = mem_records[i]
            # Determine outcome for strong_team in each record
            def _outcome_for_team(rec, team):
                tl = team.lower()
                h = rec.get("home","").lower(); a = rec.get("away","").lower()
                if tl in h:
                    return "HOME" if rec["score_h"] > rec["score_a"] else ("DRAW" if rec["score_h"] == rec["score_a"] else "LOSS")
                elif tl in a:
                    return "HOME" if rec["score_a"] > rec["score_h"] else ("DRAW" if rec["score_h"] == rec["score_a"] else "LOSS")
                return None
            prev_out = _outcome_for_team(prev, strong_team)
            curr_out = _outcome_for_team(curr, strong_team)
            if prev_out == "LOSS":
                bounce_total += 1
                if curr_out == "HOME":   # won next match
                    bounce_wins += 1

    if bounce_total >= 4:
        bounce_rate = bounce_wins / bounce_total
        if bounce_rate >= 0.65:
            strong_score += 5.0
            reasons.append(f"AI memory: strong team bounces back {bounce_wins}/{bounce_total} times after a loss ({bounce_rate:.0%}) — reliable recovery pattern")
        elif bounce_rate >= 0.45:
            strong_score += 2.0
            reasons.append(f"AI memory: strong team recovers moderately after losses ({bounce_rate:.0%}, {bounce_total} samples)")
        else:
            strong_score -= 1.0
            reasons.append(f"AI memory: strong team recovers poorly after losses ({bounce_rate:.0%}, {bounce_total} samples)")
    elif bounce_total >= 2:
        bounce_rate = bounce_wins / bounce_total
        reasons.append(f"AI memory: limited bounce-back data ({bounce_wins}/{bounce_total}) — not conclusive yet")
        if bounce_rate >= 0.5:
            strong_score += 1.0

    # ── WEAK/MODERATE SIDE: analyse their most recent win ─────────────────────
    recent_weak_win = None
    for g in reversed(weak_games):
        if g["gf"] > g["ga"]:
            recent_weak_win = g
            break

    weak_win_detail = {}
    if recent_weak_win:
        win_gf    = recent_weak_win["gf"]
        win_ga    = recent_weak_win["ga"]
        win_margin = win_gf - win_ga
        win_opp    = recent_weak_win.get("opponent", "")
        win_opp_tier = _find_tier(win_opp, tier_map)
        total_win_goals = win_gf + win_ga

        weak_win_detail = {
            "score":       f"{win_gf}-{win_ga}",
            "margin":      win_margin,
            "opponent":    win_opp,
            "opp_tier":    win_opp_tier,
            "total_goals": total_win_goals,
        }

        # Who did they beat?
        if win_opp_tier == "STRONG":
            weak_score   += 8.0
            strong_score -= 2.0
            reasons.append(f"weak/moderate beat a STRONG team ({win_opp}) {win_gf}-{win_ga} — genuine capability confirmed")
        elif win_opp_tier in ("MODERATE", "WEAK"):
            if win_margin >= 2:
                weak_score += 3.0
                reasons.append(f"weak/moderate won {win_gf}-{win_ga} vs {win_opp_tier.lower()} (convincing) — on form")
            else:
                weak_score += 1.0
                reasons.append(f"weak/moderate scraped {win_gf}-{win_ga} vs {win_opp_tier.lower()} — narrow win, less convincing")
        else:
            weak_score += 1.5
            reasons.append(f"weak/moderate won {win_gf}-{win_ga} — unknown opponent tier")

        # How many of last 6 did they win?
        weak_total_wins = sum(1 for g in weak_games if g["gf"] > g["ga"])
        if weak_total_wins >= 3:
            weak_score   += 4.0
            strong_score -= 1.0
            reasons.append(f"weak/moderate has {weak_total_wins}/6 wins recently — genuine form, not a fluke")
        elif weak_total_wins == 2:
            weak_score += 1.5
            reasons.append(f"weak/moderate has {weak_total_wins}/6 wins — modest form")
        else:
            strong_score += 2.0
            reasons.append(f"weak/moderate has only {weak_total_wins}/6 wins — limited form, likely a one-off")
    else:
        # No recent win for weak/moderate
        strong_score += 4.0
        reasons.append(f"weak/moderate has no recent win — strong team recovery very likely")

    # ── WEAK/MODERATE SIDE: momentum pattern from AI memory ───────────────────
    # After a win, how often does this team win their next match?
    momentum_wins = momentum_total = 0
    for fk, mem_records in ai_mem.items():
        parts = fk.split("|")
        if len(parts) != 2:
            continue
        ol = opponent.lower()
        if ol not in parts[0].lower() and ol not in parts[1].lower():
            continue
        for i in range(1, len(mem_records)):
            prev = mem_records[i-1]
            curr = mem_records[i]
            def _out_for(rec, team):
                tl = team.lower()
                h = rec.get("home","").lower(); a = rec.get("away","").lower()
                if tl in h:
                    return "WIN" if rec["score_h"] > rec["score_a"] else ("DRAW" if rec["score_h"] == rec["score_a"] else "LOSS")
                elif tl in a:
                    return "WIN" if rec["score_a"] > rec["score_h"] else ("DRAW" if rec["score_h"] == rec["score_a"] else "LOSS")
                return None
            prev_out = _out_for(prev, opponent)
            curr_out = _out_for(curr, opponent)
            if prev_out == "WIN":
                momentum_total += 1
                if curr_out == "WIN":
                    momentum_wins += 1

    if momentum_total >= 4:
        momentum_rate = momentum_wins / momentum_total
        if momentum_rate >= 0.55:
            weak_score   += 4.0
            reasons.append(f"AI memory: weak/moderate follows wins with more wins {momentum_wins}/{momentum_total} ({momentum_rate:.0%}) — momentum confirmed")
        elif momentum_rate <= 0.30:
            strong_score += 2.0
            reasons.append(f"AI memory: weak/moderate rarely follows win with another win ({momentum_rate:.0%}) — likely one-off")
        else:
            reasons.append(f"AI memory: weak/moderate momentum neutral ({momentum_rate:.0%}, {momentum_total} samples)")

    # ── VERDICT ───────────────────────────────────────────────────────────────
    # Net score: positive = STRONG_RECOVERS, negative = WEAK_REPEATS
    net = strong_score - weak_score
    data_points = (
        (1 if recent_strong_loss else 0) +
        (1 if recent_weak_win else 0) +
        (1 if bounce_total >= 3 else 0) +
        (1 if momentum_total >= 3 else 0) +
        (1 if len(strong_games) >= 4 else 0) +
        (1 if len(weak_games) >= 4 else 0)
    )
    data_confirmed = data_points >= 3

    if not data_confirmed:
        result["reasoning"]  = reasons + [f"only {data_points} data points — verdict not confirmed"]
        result["data_confirmed"] = False
        result["strong_loss_detail"] = strong_loss_detail
        result["weak_win_detail"]    = weak_win_detail
        return result

    # Compute strength of signal (0.0–1.0)
    total_evidence = strong_score + weak_score or 1.0
    if net > 2.0:
        signal   = "STRONG_RECOVERS"
        strength = min(1.0, net / (total_evidence * 0.5))
        strong_conf_bonus = strength * 10.0
        weak_conf_bonus   = 0.0
    elif net < -2.0:
        signal   = "WEAK_REPEATS"
        strength = min(1.0, abs(net) / (total_evidence * 0.5))
        strong_conf_bonus = 0.0
        weak_conf_bonus   = strength * 10.0
    else:
        signal   = "UNCERTAIN"
        strength = 0.0
        strong_conf_bonus = 0.0
        weak_conf_bonus   = 0.0
        reasons.append(f"net score too close ({net:+.1f}) — no confident verdict")

    result.update({
        "recovery_signal":    signal,
        "recovery_strength":  round(strength, 3),
        "strong_conf_bonus":  round(strong_conf_bonus, 1),
        "weak_conf_bonus":    round(weak_conf_bonus, 1),
        "reasoning":          reasons,
        "data_confirmed":     data_confirmed,
        "strong_loss_detail": strong_loss_detail,
        "weak_win_detail":    weak_win_detail,
    })
    return result

# ─── CORRECT SCORE ENGINE ─────────────────────────────────────────────────────

def _parse_sl(sl: str) -> tuple[int, int]:
    """Parse '2-1' → (2, 1)."""
    try:
        h, a = sl.split("-")
        return int(h), int(a)
    except Exception:
        return (0, 0)


def _cs_from_odds(odds: dict) -> dict[str, float]:
    """
    Extract correct score implied probabilities from betPawa's CS market if available.
    Returns {scoreline_str: prob} normalised.
    """
    result = {}
    for mkt in odds.get("_raw_markets", []):
        mtype = str(mkt.get("marketType") or mkt.get("name") or "").upper()
        if "CORRECT" not in mtype and "SCORE" not in mtype and "CS" not in mtype:
            continue
        outcomes = mkt.get("outcomes") or mkt.get("selections") or []
        for o in outcomes:
            name = str(o.get("name") or o.get("label") or "").strip()
            # Match patterns like "2:1", "2-1", "Home 2-1", etc.
            m = re.search(r'(\d+)[\s:\-](\d+)', name)
            if m:
                sl = f"{m.group(1)}-{m.group(2)}"
                price = None
                for f in ("price", "odds", "decimalOdds", "value"):
                    v = o.get(f)
                    if v is not None:
                        try:
                            price = float(v)
                            break
                        except Exception:
                            pass
                if price and price > 1.0:
                    result[sl] = 1.0 / price
    if result:
        total = sum(result.values())
        if total > 0:
            result = {k: v/total for k, v in result.items()}
    return result


def _poisson_scoreline_probs(exp_h: float, exp_a: float,
                              max_goals: int = 7) -> dict[str, float]:
    """
    Generate all scoreline probabilities using Poisson distribution.
    Returns {scoreline: probability} for all scores up to max_goals each.
    """
    probs = {}
    for h in range(max_goals + 1):
        for a in range(max_goals + 1):
            p = _poisson_prob(exp_h, h) * _poisson_prob(exp_a, a)
            probs[f"{h}-{a}"] = p
    # Normalise
    total = sum(probs.values()) or 1.0
    return {k: v/total for k, v in probs.items()}


def _h2h_scoreline_probs(h2h_rec: dict | None, n_min: int = 3) -> dict[str, float]:
    """
    Convert H2H scoreline history into a probability distribution.
    Only used when there are at least n_min meetings.
    """
    if not h2h_rec or h2h_rec.get("n", 0) < n_min:
        return {}
    sl_counts = h2h_rec.get("scorelines", {})
    total = sum(sl_counts.values()) or 1
    # Apply recency-like smoothing: repeated scores get boosted
    probs = {}
    for sl, cnt in sl_counts.items():
        # Boost repeated patterns: score that appeared 3+ times gets ×1.5
        boost = 1.5 if cnt >= 3 else (1.2 if cnt >= 2 else 1.0)
        probs[sl] = (cnt / total) * boost
    # Re-normalise
    total2 = sum(probs.values()) or 1
    return {k: v/total2 for k, v in probs.items()}


def _team_scoreline_probs(hst: dict, ast: dict) -> dict[str, float]:
    """
    Build scoreline probabilities from each team's historical scoreline patterns.
    Cross-matches home team's home scorelines vs away team's away scorelines.
    """
    home_sls = hst.get("home_scorelines", {})  # home team's scores when playing at home
    away_sls = ast.get("away_scorelines", {})   # away team's scores when playing away (from away's POV: gf=away, ga=home)

    if not home_sls and not away_sls:
        return {}

    # Normalise each set
    def _norm(d):
        t = sum(d.values()) or 1
        return {k: v/t for k, v in d.items()}

    h_norm = _norm(home_sls) if home_sls else {}
    # Away scorelines are stored as gf-ga FROM AWAY TEAM'S PERSPECTIVE
    # So "2-1" in away_sls means away scored 2, home scored 1 → actual: home 1-2 away
    # We need to flip them
    a_flipped = {}
    for sl, prob in (_norm(away_sls) if away_sls else {}).items():
        h_g, a_g = _parse_sl(sl)
        a_flipped[f"{a_g}-{h_g}"] = prob   # flip: now from home perspective

    # Combine: average of both distributions
    all_keys = set(h_norm.keys()) | set(a_flipped.keys())
    combined = {}
    for sl in all_keys:
        hp = h_norm.get(sl, 0)
        ap = a_flipped.get(sl, 0)
        combined[sl] = (hp + ap) / 2

    total = sum(combined.values()) or 1
    return {k: v/total for k, v in combined.items()}


def _ou_filter(probs: dict[str, float], ou_imp: dict) -> dict[str, float]:
    """
    Re-weight scoreline probabilities using betPawa's O/U implied probs.
    If betPawa says 70% over 2.5, scorelines with 3+ goals get boosted.
    """
    over_p = ou_imp.get("over25")
    if not over_p:
        return probs

    under_p = 1.0 - over_p
    result = {}
    for sl, p in probs.items():
        h, a = _parse_sl(sl)
        total_goals = h + a
        if total_goals >= 3:
            result[sl] = p * (over_p * 1.8)    # boost high-scoring scorelines
        else:
            result[sl] = p * (under_p * 1.8)   # boost low-scoring scorelines

    total = sum(result.values()) or 1
    return {k: v/total for k, v in result.items()}


def _btts_filter(probs: dict[str, float], btts_imp: float | None) -> dict[str, float]:
    """Re-weight using BTTS implied probability."""
    if btts_imp is None:
        return probs
    no_btts = 1.0 - btts_imp
    result = {}
    for sl, p in probs.items():
        h, a = _parse_sl(sl)
        if h > 0 and a > 0:
            result[sl] = p * (btts_imp * 1.6)
        else:
            result[sl] = p * (no_btts * 1.6)
    total = sum(result.values()) or 1
    return {k: v/total for k, v in result.items()}


def _outcome_filter(probs: dict[str, float], tip: str,
                    hw_p: float, dw_p: float, aw_p: float) -> dict[str, float]:
    """
    Re-weight scorelines to match the predicted outcome.
    If tip is HOME WIN, massively boost home-win scorelines proportionally.
    """
    result = {}
    for sl, p in probs.items():
        h, a = _parse_sl(sl)
        if h > a:        outcome_p = hw_p
        elif h == a:     outcome_p = dw_p
        else:            outcome_p = aw_p
        result[sl] = p * (outcome_p * 3.0)
    total = sum(result.values()) or 1
    return {k: v/total for k, v in result.items()}


def predict_correct_score(home: str, away: str, stats: dict,
                           match_pred: dict, odds: dict | None = None,
                           top_n: int = 5) -> list[dict]:
    """
    Predict the top N most likely correct scores using a 4-layer ensemble:

    Layer 1 — Poisson distribution (exp goals from stats)          weight: 30%
    Layer 2 — H2H historical scoreline patterns                     weight: 35%
    Layer 3 — Team home/away scoreline patterns (cross-matched)    weight: 20%
    Layer 4 — CS market odds (if available)                        weight: 15%

    Then applies 3 filters:
      - O/U 2.5 re-weighting  (betPawa odds)
      - BTTS re-weighting     (betPawa odds)
      - Outcome filter        (only scorelines matching predicted winner)

    Returns list of {score, prob, conf_label} sorted by probability desc.
    """
    blank = dict(p=0, home_scorelines={}, away_scorelines={}, h2h={}, goal_times=[])
    hst = stats.get(home, blank)
    ast = stats.get(away, blank)
    if odds is None:
        odds = {}

    exp_h = match_pred.get("exp_h", 1.5)
    exp_a = match_pred.get("exp_a", 1.2)
    tip   = match_pred.get("tip", "HOME WIN")
    hw_p  = match_pred.get("hw", 50) / 100
    dw_p  = match_pred.get("dw", 25) / 100
    aw_p  = match_pred.get("aw", 25) / 100

    ou_imp   = _ou_implied(odds)
    btts_imp = _btts_implied(odds)

    # ── Layer 1: Poisson ──────────────────────────────────────────────────────
    poisson_p = _poisson_scoreline_probs(exp_h, exp_a)

    # ── Layer 2: H2H patterns ─────────────────────────────────────────────────
    h2h_rec   = hst.get("h2h", {}).get(away)
    h2h_p     = _h2h_scoreline_probs(h2h_rec)

    # ── Layer 3: Team pattern cross-match ─────────────────────────────────────
    team_p    = _team_scoreline_probs(hst, ast)

    # ── Layer 4: CS market odds ───────────────────────────────────────────────
    cs_odds_p = _cs_from_odds(odds)

    # ── Combine layers ────────────────────────────────────────────────────────
    all_scores = set(poisson_p.keys())
    if h2h_p:    all_scores |= set(h2h_p.keys())
    if team_p:   all_scores |= set(team_p.keys())
    if cs_odds_p: all_scores |= set(cs_odds_p.keys())

    combined = {}
    for sl in all_scores:
        h, a = _parse_sl(sl)
        # Only include realistic scorelines (max 7 goals per team)
        if h > 7 or a > 7:
            continue
        p1 = poisson_p.get(sl, 0)
        p2 = h2h_p.get(sl, 0)    if h2h_p    else p1 * 0.8
        p3 = team_p.get(sl, 0)   if team_p   else p1 * 0.8
        p4 = cs_odds_p.get(sl, 0) if cs_odds_p else 0

        if cs_odds_p:
            combined[sl] = 0.30*p1 + 0.35*p2 + 0.20*p3 + 0.15*p4
        else:
            combined[sl] = 0.35*p1 + 0.40*p2 + 0.25*p3

    # ── Apply filters ─────────────────────────────────────────────────────────
    combined = _ou_filter(combined, ou_imp)
    combined = _btts_filter(combined, btts_imp)
    combined = _outcome_filter(combined, tip, hw_p, dw_p, aw_p)

    # ── Sort and pick top N ───────────────────────────────────────────────────
    sorted_scores = sorted(combined.items(), key=lambda x: x[1], reverse=True)

    results = []
    for sl, raw_prob in sorted_scores[:top_n]:
        pct = round(raw_prob * 100, 1)
        # Confidence label
        if pct >= 25:   lbl = "🔴 HIGH"
        elif pct >= 15: lbl = "🟡 MEDIUM"
        else:           lbl = "⚪ LOW"
        results.append({"score": sl, "prob": pct, "label": lbl})

    return results


def strengths_weaknesses(st: dict) -> tuple[list[str], list[str]]:
    p = st["p"] or 1
    hp = st["hp"] or 1
    ap = st["ap"] or 1
    good, bad = [], []
    if st["w"]/p >= 0.50:           good.append("🏆 Strong winning record")
    if st["gf"]/p >= 2.2:           good.append("⚽ Lethal attack")
    if st["cs"]/p >= 0.30:          good.append("🛡️ Solid defence")
    if st["hw"]/hp >= 0.55:         good.append("🏠 Dominant at home")
    if st["aw"]/ap >= 0.40:         good.append("✈️ Strong away form")
    if st["over25"]/p >= 0.60:      good.append("🔥 High-scoring games")
    if st["scored_both"]/p >= 0.55: good.append("⚡ Both teams score often")
    if not good: good = ["📊 Average — no standout strength"]
    if st["l"]/p >= 0.50:           bad.append("📉 Too many defeats")
    if st["ga"]/p >= 2.5:           bad.append("🚨 Leaky defence")
    if st["fts"]/p >= 0.35:         bad.append("🚫 Struggles to score")
    if st["d"]/p >= 0.40:           bad.append("😐 Too many draws")
    if st["al"]/ap >= 0.55:         bad.append("✈️ Poor away record")
    if not bad: bad = ["✅ No major weaknesses"]
    return good, bad

async def _ensure_stats(c, league_id: int, client=None) -> dict | None:
    stats = c.bot_data.get(f"stats_{league_id}")
    # Refresh cache every 60 minutes so patterns stay current
    last_fetch = c.bot_data.get(f"stats_ts_{league_id}", 0)
    age_mins   = (time.time() - last_fetch) / 60
    if stats and age_mins < 60:
        return stats
    async def _do_fetch(cl):
        rounds = await fetch_past_results(cl, league_id, n=30)
        if not rounds:
            return stats   # return stale cache rather than None
        all_events = [e for _, evs in rounds for e in evs]
        s = build_stats(all_events)
        c.bot_data[f"matches_{league_id}"] = all_events
        c.bot_data[f"stats_{league_id}"]   = s
        c.bot_data[f"stats_ts_{league_id}"] = time.time()
        c.bot_data["active_league"]         = league_id

        # ── Fetch and cache standings for prediction use ───────────────────────
        try:
            _sn, _rn, std_rows = await fetch_standings(cl if client else cl, league_id)
            if std_rows:
                # Build lookup dict: team_name → standings row
                std_map = {r["name"]: r for r in std_rows}
                c.bot_data[f"standings_{league_id}"] = std_map
                log.info(f"📊 Standings cached for league {league_id}: {len(std_map)} teams")
        except Exception as _se:
            log.warning(f"Standings cache failed for {league_id}: {_se}")

        # ── Update stats but PRESERVE existing learned model weights ──────────
        # Only bootstrap if this league has never been learned before.
        # If a model already exists with real rounds_learned, keep it — don't wipe.
        existing_model = c.bot_data.get(f"model_{league_id}")
        already_learned = (
            isinstance(existing_model, dict) and
            existing_model.get("rounds_learned", 0) > 0
        )
        if already_learned:
            log.info(f"🧠 [{league_id}] Keeping existing model "
                     f"({existing_model['rounds_learned']} rounds learned) — skipping bootstrap")
        else:
            c.bot_data[f"model_{league_id}"] = None   # force fresh model
            _bootstrap_learning(c.bot_data, league_id, rounds, s)

        return s
    if client:
        return await _do_fetch(client)
    async with httpx.AsyncClient() as cl:
        return await _do_fetch(cl)


def _bootstrap_learning(bot_data: dict, league_id: int,
                         rounds: list[tuple], stats: dict):
    """
    Learn from all historical rounds that were just fetched.
    Replays each round: builds a synthetic prediction from stats available
    BEFORE that round, then compares to the actual result.
    This gives the model instant training data without waiting for live rounds.
    """
    model = _get_model(bot_data, league_id)

    # Process rounds oldest-first so learning builds up correctly
    ordered = list(reversed(rounds))   # fetch_past_results returns newest first

    # We'll build cumulative stats round by round for realistic replay
    cumulative_events: list[dict] = []

    for round_idx, (round_name, events) in enumerate(ordered):
        scored = [e for e in events if _norm_event(e)["hs"] is not None]
        if not scored:
            continue

        # Stats available BEFORE this round = all previous rounds
        if round_idx == 0:
            cumulative_events += scored
            continue   # need at least 1 prior round to predict

        prior_stats = build_stats(cumulative_events)

        # Build synthetic predictions + results for this round
        predictions = []
        results     = []

        for raw in scored:
            m  = _norm_event(raw)
            ev_odds = _extract_odds(raw)

            # Predict using only prior data
            p = predict_match(m["home"], m["away"], prior_stats, ev_odds, model)

            cs_preds = predict_correct_score(
                m["home"], m["away"], prior_stats, p, ev_odds, top_n=1
            )

            predictions.append({
                "home":         m["home"],
                "away":         m["away"],
                "tip":          p["tip"],
                "conf":         p.get("conf", 50.0),       # must store real conf for band calibration
                "odds_tip":     p.get("odds_tip"),
                "poisson_tip":  p.get("poisson_tip"),
                "strength_tip": p.get("strength_tip"),
                "pred_h":       p["exp_h"],
                "pred_a":       p["exp_a"],
                "cs_top":       cs_preds[0]["score"] if cs_preds else None,
                # Side markets — required for btts/o25 band calibration
                "btts_pred":    p.get("btts", 50.0) >= 50,
                "btts_prob":    p.get("btts", 50.0),
                "over25_pred":  p.get("over25", 50.0) >= 50,
                "over25_prob":  p.get("over25", 50.0),
                "prob_H":       p.get("prob_H", 0.45),
                "prob_D":       p.get("prob_D", 0.27),
                "prob_A":       p.get("prob_A", 0.28),
                "round_id":     0,
            })
            results.append({
                "home":     m["home"],
                "away":     m["away"],
                "actual_h": m["hs"],
                "actual_a": m["as_"],
            })

        # Learn from this round — mark as bootstrap so cold-start gate
        # can distinguish synthetic data from real live-round data
        _learn_from_round(bot_data, league_id, predictions, results,
                          season_id="", round_id=0, is_bootstrap=True)
        _get_model(bot_data, league_id)["_bootstrap_rounds"] = \
            _get_model(bot_data, league_id).get("_bootstrap_rounds", 0) + 1
        _learn_algo_signals(_get_model(bot_data, league_id), predictions, results, round_id_int=0)
        _ai_postmatch_analysis(_get_model(bot_data, league_id), predictions, results,
                               standings=None, round_id=0)

        # Add this round to cumulative pool for next iteration
        cumulative_events += scored

    log.info(f"🎓 Bootstrap complete for league {league_id}: "
             f"{model['rounds_learned']} rounds learned, "
             f"outcome_acc={model['outcome_acc']:.1%}, "
             f"weights={model['weights']}")


# ─── SELF-LEARNING ENGINE ─────────────────────────────────────────────────────
# The bot tracks every prediction it makes and every result that comes in.
# After each round it:
#   1. Scores itself — which signals were right/wrong
#   2. Adjusts model weights toward whichever signals performed best (using full cumulative history)
#   3. Learns per-league biases (some leagues favour home more than others)
#   4. Tracks exact scoreline hit rate to improve CS predictions
#   5. Detects betPawa algorithm patterns (home bias, high-scoring rounds, etc.)

# Default model weights — virtual football: odds IS the server algorithm
# Poisson/strength are real-football models that add noise for virtual games
DEFAULT_WEIGHTS = {
    "odds":     0.92,   # betPawa implied odds → server's probability model
    "poisson":  0.05,   # tiny residual (calibration only)
    "strength": 0.03,   # tiny residual (calibration only)
    "h2h":      0.00,   # suppressed — H2H is noise for virtual football
}

LEARNING_RATE   = 0.04   # how fast weights shift per round (smaller = more stable)
MIN_WEIGHT      = 0.05   # no signal ever drops below this
MAX_WEIGHT      = 0.70   # no single signal dominates above this


def _get_model(bot_data: dict, league_id: int) -> dict:
    """Get the current learned model for a league, or defaults.
    Also patches any missing keys on old cached models (forward-compat).
    """
    key = f"model_{league_id}"
    defaults = {
        "weights":          dict(DEFAULT_WEIGHTS),
        "home_bias":        1.0,
        "avg_goals":        2.5,
        "cs_hit_rate":      0.0,
        "outcome_acc":      0.0,
        "btts_acc":         0.0,
        "over25_acc":       0.0,
        "btts_rate":        0.50,
        "over25_rate":      0.50,
        "conf_calibration": 0.0,
        "rounds_learned":   0,
        "signal_acc": {
            "odds":     {"correct": 0, "total": 0},
            "poisson":  {"correct": 0, "total": 0},
            "strength": {"correct": 0, "total": 0},
        },
        "pattern_memory": {},
        # ── TRUE CUMULATIVE COUNTERS (never fade) ─────────────────────────
        "cumulative": {
            "outcome_correct": 0,
            "outcome_total":   0,
            "btts_correct":    0,
            "btts_total":      0,
            "over25_correct":  0,
            "over25_total":    0,
            "goals_total":     0,
            "matches_total":   0,
            "home_wins":       0,
            "draws":           0,
            "away_wins":       0,
            "btts_count":      0,
            "over25_count":    0,
            "conf_sum_correct": 0.0,
            "conf_sum_wrong":   0.0,
            "n_correct":        0,
            "n_wrong":          0,
        },
        # ── IMPROVEMENT ENGINE ────────────────────────────────────────────
        # 1X2 confidence bands: margin_acc[bucket] = [correct, total]
        "margin_acc": {},
        # BTTS confidence bands: btts_band_acc["yes"|"no"][bucket] = [correct, total]
        "btts_band_acc": {"yes": {}, "no": {}},
        # O2.5 confidence bands: o25_band_acc["yes"|"no"][bucket] = [correct, total]
        "o25_band_acc":  {"yes": {}, "no": {}},
        # HT/FT accuracy: htft_acc[outcome] = {"correct": N, "total": N}
        # outcome = "1/1", "1/X", "1/2", "X/1", "X/X", "X/2", "2/1", "2/X", "2/2"
        "htft_acc": {},
        # Per-outcome accuracy: how often HOME/DRAW/AWAY tips were correct
        "outcome_type_acc": {
            "HOME": {"correct": 0, "total": 0},
            "DRAW": {"correct": 0, "total": 0},
            "AWAY": {"correct": 0, "total": 0},
        },
        # High-confidence mistake counter: wrong when conf >= 75
        "high_conf_mistakes": 0,
        # Learning velocity: accuracy per last 10 rounds (rolling window)
        "recent_10_correct": 0,
        "recent_10_total":   0,
        "recent_10_acc":     0.0,
    }
    if key not in bot_data or bot_data[key] is None:
        bot_data[key] = defaults
    else:
        # Patch any keys that are missing from old cached model
        m = bot_data[key]
        for k, v in defaults.items():
            if k not in m:
                m[k] = v if not isinstance(v, dict) else dict(v)
        # Ensure signal_acc sub-keys exist
        sa = m.setdefault("signal_acc", {})
        for sig in ("odds", "poisson", "strength"):
            sa.setdefault(sig, {"correct": 0, "total": 0})
        # Ensure cumulative sub-keys exist
        cum = m.setdefault("cumulative", {})
        for ck, cv in defaults["cumulative"].items():
            cum.setdefault(ck, cv)
        # Ensure new cumulative keys exist on old models
        cum.setdefault("btts_count",  0)
        cum.setdefault("over25_count", 0)
        # Forward-compat: improvement engine fields
        m.setdefault("margin_acc", {})
        m.setdefault("btts_band_acc", {"yes": {}, "no": {}})
        m.setdefault("o25_band_acc",  {"yes": {}, "no": {}})
        m.setdefault("htft_acc", {})
        m.setdefault("outcome_type_acc", {
            "HOME": {"correct": 0, "total": 0},
            "DRAW": {"correct": 0, "total": 0},
            "AWAY": {"correct": 0, "total": 0},
        })
        m.setdefault("high_conf_mistakes", 0)
        m.setdefault("recent_10_correct", 0)
        m.setdefault("recent_10_total",   0)
        m.setdefault("recent_10_acc",     0.0)
        # ── SELF-CONSTRUCTING INTELLIGENCE ENGINE ─────────────────────────────
        # mistake_dna: per fixture, record every wrong prediction with full context
        # {fixture_key: [{predicted, actual, conf, odds_h, odds_d, odds_a,
        #                 form_h, form_a, round_id, conditions_hash, resolved_by}]}
        m.setdefault("mistake_dna", {})
        # pattern_rules: self-discovered rules from repeated mistake patterns
        # {rule_id: {condition_hash, correct_outcome, confirmations, rejections,
        #            confidence, born_round, last_seen_round, active}}
        m.setdefault("pattern_rules", {})
        # condition_outcomes: for each unique condition fingerprint, track outcomes
        # {conditions_hash: {HOME:n, DRAW:n, AWAY:n, total:n}}
        m.setdefault("condition_outcomes", {})
        # rule_id counter
        m.setdefault("_rule_counter", 0)
        # form_sequence: per fixture, track form state at time of each result
        # helps discover "when team X is on 3+ win streak vs team Y on loss streak → HOME"
        m.setdefault("form_state_memory", {})
        # ── AI BRAIN forward-compatibility patch ──────────────────────────────
        # If an old brain file is loaded with old ai_brain keys, migrate smoothly.
        # Old keys: fixture_trust, signal_blame, signal_trust, tier_outcomes,
        #           odds_band_trust, market_corr, fixture_history
        # New keys: fixture_mem, signal_acc, tier_acc, band_acc, market_acc
        old_ai = m.get("ai_brain", {})
        if old_ai:
            # Migrate fixture_history → fixture_mem if old key exists and new is missing
            if "fixture_history" in old_ai and "fixture_mem" not in old_ai:
                old_fh = old_ai.pop("fixture_history", {})
                new_fm = {}
                for fk, hist in old_fh.items():
                    new_fm[fk] = []
                    for h in (hist or []):
                        new_fm[fk].append({
                            "round":         h.get("round", 0),
                            "home":          fk.split("|")[0] if "|" in fk else "",
                            "away":          fk.split("|")[1] if "|" in fk else "",
                            "predicted":     h.get("predicted", ""),
                            "actual":        h.get("actual", ""),
                            "score_h":       0,
                            "score_a":       0,
                            "correct":       h.get("correct", False),
                            "odds_tip":      h.get("odds_tip"),
                            "fp_tip":        h.get("fp_tip"),
                            "form_tip":      h.get("form_tip"),
                            "h_tier":        h.get("tier_pair","").split("_vs_")[0] if "_vs_" in h.get("tier_pair","") else "UNKNOWN",
                            "a_tier":        h.get("tier_pair","").split("_vs_")[1] if "_vs_" in h.get("tier_pair","") else "UNKNOWN",
                            "tier_pair":     h.get("tier_pair", ""),
                            "strong_side":   None,
                            "blame":         h.get("blame", []),
                            "primary_blame": h.get("primary_blame", ""),
                            "btts":          h.get("btts", False),
                            "over25":        h.get("over25", False),
                            "btts_prob":     50.0,
                            "over25_prob":   50.0,
                            "odds_h":        None,
                            "odds_d":        None,
                            "odds_a":        None,
                        })
                old_ai["fixture_mem"] = new_fm

            # Migrate signal_trust/signal_blame → signal_acc if needed
            if "signal_trust" in old_ai and "signal_acc" not in old_ai:
                st = old_ai.pop("signal_trust", {})
                sb = old_ai.pop("signal_blame", {})
                old_ai["signal_acc"] = {
                    sig: {"correct": 0, "total": 0, "recent": []}
                    for sig in ("odds", "fingerprint", "tier", "form")
                }

            # Migrate tier_outcomes → tier_acc
            if "tier_outcomes" in old_ai and "tier_acc" not in old_ai:
                old_ai["tier_acc"] = old_ai.pop("tier_outcomes", {})

            # Migrate odds_band_trust → band_acc
            if "odds_band_trust" in old_ai and "band_acc" not in old_ai:
                old_ai["band_acc"] = old_ai.pop("odds_band_trust", {})

            # Migrate market_corr → market_acc
            if "market_corr" in old_ai and "market_acc" not in old_ai:
                old_ai["market_acc"] = {
                    "btts_yes": {"correct": 0, "total": 0},
                    "btts_no":  {"correct": 0, "total": 0},
                    "over25":   {"correct": 0, "total": 0},
                    "under25":  {"correct": 0, "total": 0},
                }
                old_ai.pop("market_corr", None)

            # Remove any leftover old keys
            for stale in ("fixture_trust",):
                old_ai.pop(stale, None)

    return bot_data[key]


def _fixture_key(home: str, away: str) -> str:
    return f"{home}|{away}"


# ─── ALGORITHM REVERSE-ENGINEERING ENGINES ────────────────────────────────────
# Virtual football uses predictable PRNG-based algorithms.
# These three engines exploit structure in the server's generation method.

def _detect_cycle(seq: list, min_len: int = 2, max_len: int = 8) -> tuple[int, float]:
    """
    Detect a repeating cycle in an outcome sequence.
    Returns (cycle_length, confidence) or (0, 0.0) if no cycle found.
    confidence = fraction of last 3*cycle_len entries that match the pattern.
    """
    if len(seq) < min_len * 3:
        return 0, 0.0
    best_len, best_conf = 0, 0.0
    for clen in range(min_len, min(max_len + 1, len(seq) // 2 + 1)):
        # Take the most recent 'clen' as the candidate pattern
        pattern = seq[-clen:]
        # How far back can we verify it?
        check_start = len(seq) - clen * 3
        if check_start < 0:
            check_start = 0
        matches = 0
        total   = 0
        for i in range(check_start, len(seq)):
            expected = pattern[i % clen]
            if seq[i] == expected:
                matches += 1
            total += 1
        if total == 0:
            continue
        conf = matches / total
        # Require high confidence: at least 75% of the last 3 cycles match
        if conf > best_conf and conf >= 0.75:
            best_conf = conf
            best_len  = clen
    return best_len, best_conf


def _apply_algo_signals(hw: float, dw: float, aw: float,
                         model: dict, round_id_int: int,
                         home: str, away: str) -> tuple[float, float, float, float]:
    """
    Apply algorithm reverse-engineering signals to adjust win probabilities.
    Returns (hw, dw, aw, algo_bonus) where algo_bonus is confidence boost.

    Three engines:
    1. Rebalancing detector — server enforces long-run distribution = implied prob
    2. Fixture cycle detector — same matchup may cycle through outcomes predictably
    3. Round-ID modulo pattern — PRNG seed from round_id creates detectable correlations
    """
    algo_bonus = 0.0

    # ── Engine 1: Rebalancing Detector ──────────────────────────────────────
    # The server must honor implied probabilities over time (house edge depends on it).
    # If HOME has won 15% more than expected in last 30 rounds → overdue for correction.
    reb = model.get("rebalance", {})
    window = reb.get("window", [])  # list of {H_exp, D_exp, A_exp, H_act, D_act, A_act}
    if len(window) >= 15:
        # Use last 30 rounds (or all available)
        w = window[-30:]
        exp_H = sum(r["H_exp"] for r in w) / len(w)
        exp_D = sum(r["D_exp"] for r in w) / len(w)
        exp_A = sum(r["A_exp"] for r in w) / len(w)
        act_H = sum(r["H_act"] for r in w) / len(w)
        act_D = sum(r["D_act"] for r in w) / len(w)
        act_A = sum(r["A_act"] for r in w) / len(w)

        dev_H = act_H - exp_H   # positive = HOME over-represented → expect less
        dev_D = act_D - exp_D
        dev_A = act_A - exp_A

        # Apply mean-reversion correction: deviations pull back toward zero
        # Scale: 10% over-representation → -5% probability adjustment
        strength = min(0.08, len(w) / 400)  # grows to max 0.08 as data matures
        hw = max(0.01, hw - dev_H * strength * 5)
        dw = max(0.01, dw - dev_D * strength * 5)
        aw = max(0.01, aw - dev_A * strength * 5)

        # Algo bonus: if strong rebalancing signal (>8% deviation), boost confidence
        max_dev = max(abs(dev_H), abs(dev_D), abs(dev_A))
        if max_dev > 0.08 and len(w) >= 25:
            algo_bonus += min(5.0, max_dev * 40)

    # ── Engine 2: Fixture Cycle Detector ─────────────────────────────────────
    # Same teams meeting repeatedly may cycle: HOME→DRAW→AWAY→HOME→...
    fk = _fixture_key(home, away)
    pm = model.get("pattern_memory", {}).get(fk, {})
    outcome_seq = pm.get("outcome_seq", [])
    cycle_len, cycle_conf = _detect_cycle(outcome_seq)
    if cycle_len > 0 and cycle_conf >= 0.80:
        # Next in cycle
        next_pos = len(outcome_seq) % cycle_len
        pattern  = outcome_seq[-cycle_len:]
        predicted = pattern[next_pos]
        # Weight the cycle prediction by confidence
        cycle_weight = (cycle_conf - 0.75) * 4   # 0.80 → 0.20, 0.95 → 0.80
        if predicted == "HOME":
            hw = hw * (1 - cycle_weight) + 1.0 * cycle_weight
            dw = dw * (1 - cycle_weight)
            aw = aw * (1 - cycle_weight)
        elif predicted == "DRAW":
            dw = dw * (1 - cycle_weight) + 1.0 * cycle_weight
            hw = hw * (1 - cycle_weight)
            aw = aw * (1 - cycle_weight)
        else:  # AWAY
            aw = aw * (1 - cycle_weight) + 1.0 * cycle_weight
            hw = hw * (1 - cycle_weight)
            dw = dw * (1 - cycle_weight)
        algo_bonus += cycle_conf * 10   # up to +9.5 pts when cycle_conf=0.95

    # ── Engine 3: Round-ID Modulo Pattern ────────────────────────────────────
    # If PRNG is seeded from round_id, then (round_id % N) may correlate with outcomes.
    rmp = model.get("round_mod_patterns", {})
    best_mod_signal = None
    best_mod_conf   = 0.60   # minimum threshold to use
    if round_id_int > 0:
        for mod_s, mod_data in rmp.items():
            mod = int(mod_s)
            remainder_s = str(round_id_int % mod)
            rec = mod_data.get(remainder_s, {})
            total = rec.get("H", 0) + rec.get("D", 0) + rec.get("A", 0)
            if total < 8:
                continue   # need enough samples
            best_outcome  = max(rec, key=lambda k: rec.get(k, 0))
            best_out_conf = rec.get(best_outcome, 0) / total
            # Require dominant: at least 65% of rounds with this modulo go same way
            if best_out_conf > best_mod_conf:
                best_mod_conf   = best_out_conf
                best_mod_signal = best_outcome

        if best_mod_signal:
            mod_weight = (best_mod_conf - 0.60) * 2.5   # 0.65 → 0.125, 0.80 → 0.50
            if best_mod_signal == "HOME":
                hw = hw * (1 - mod_weight) + 1.0 * mod_weight
                dw = dw * (1 - mod_weight)
                aw = aw * (1 - mod_weight)
            elif best_mod_signal == "DRAW":
                dw = dw * (1 - mod_weight) + 1.0 * mod_weight
                hw = hw * (1 - mod_weight)
                aw = aw * (1 - mod_weight)
            else:
                aw = aw * (1 - mod_weight) + 1.0 * mod_weight
                hw = hw * (1 - mod_weight)
                dw = dw * (1 - mod_weight)
            algo_bonus += (best_mod_conf - 0.60) * 30   # up to +6 pts

    # Re-normalise
    tot = hw + dw + aw or 1.0
    hw /= tot; dw /= tot; aw /= tot
    return hw, dw, aw, algo_bonus



# ─── AI SELF-ANALYSIS ENGINE ──────────────────────────────────────────────────
# After every round: diagnoses every wrong prediction, identifies which signal
# failed (odds / fingerprint / standings-tier / form), updates all accuracy
# structures, and every 10 rounds fully re-evaluates and rebalances blend weights.
#
# STORED IN model["ai_brain"]:
#   fixture_mem   — per-fixture full memory: list of match records with every
#                   signal, outcome, blame, tier, score. Up to 30 per fixture.
#                   AI needs 6-10+ records before forming any lesson.
#   signal_acc    — {"odds":{correct,total,recent[]}, "fingerprint":..., ...}
#                   Rolling accuracy per signal. recent[] = last 50 outcomes.
#   tier_acc      — {"STRONG_vs_WEAK":{correct,total}, ...}
#                   Per tier-pair accuracy for S-W/S-M matchups.
#   band_acc      — {"<1.30":{correct,total}, "1.30-1.60":..., ...}
#                   Accuracy by odds band of the tipped selection.
#   market_acc    — {"btts_yes":{correct,total}, "btts_no":..., ...}
#                   BTTS and O2.5 market prediction accuracy.
#   intelligence  — summary dict updated every 10 rounds: weights, accs, counts.
#   ai_log        — last 50 wrong-pick diagnoses (for /stats display)

def _ai_postmatch_analysis(
    model: dict,
    predictions: list[dict],
    results: list[dict],
    standings: dict | None,
    round_id: int = 0,
) -> None:
    """
    AI BRAIN — POST-MATCH LEARNING ENGINE.

    Called after every round. For every match:
    1. Records full match data: odds, signals, actual outcome, score, tiers, markets.
    2. Diagnoses each wrong prediction — which signal failed and why.
    3. Updates per-fixture memory (up to 30 records per fixture — the AI needs
       6-10+ data points before forming any strong opinion).
    4. Updates per-signal rolling accuracy (odds / fingerprint / tier / form).
    5. Updates per-tier-pair accuracy (STRONG_vs_WEAK, STRONG_vs_MODERATE etc).
    6. Updates odds-band accuracy (what price ranges are reliable).
    7. Every 10 rounds: calls full AI re-evaluation and weight rebalance.

    Nothing is discarded. The AI gets smarter every round it sees.
    """
    if not results or not predictions:
        return

    ai = model.setdefault("ai_brain", {})

    # ── Core data stores ──────────────────────────────────────────────────────
    fixture_mem    = ai.setdefault("fixture_mem", {})      # per-fixture full memory
    signal_acc     = ai.setdefault("signal_acc", {         # rolling per-signal accuracy
        "odds":        {"correct": 0, "total": 0, "recent": []},
        "fingerprint": {"correct": 0, "total": 0, "recent": []},
        "tier":        {"correct": 0, "total": 0, "recent": []},
        "form":        {"correct": 0, "total": 0, "recent": []},
    })
    tier_acc       = ai.setdefault("tier_acc", {})         # per tier-pair accuracy
    band_acc       = ai.setdefault("band_acc", {})         # per odds-band accuracy
    market_acc     = ai.setdefault("market_acc", {         # BTTS / O2.5 market accuracy
        "btts_yes": {"correct": 0, "total": 0},
        "btts_no":  {"correct": 0, "total": 0},
        "over25":   {"correct": 0, "total": 0},
        "under25":  {"correct": 0, "total": 0},
    })
    ai_log         = ai.setdefault("ai_log", [])           # recent wrong-pick diagnoses

    result_map = {_fixture_key(r["home"], r["away"]): r for r in results}
    tier_map   = _get_all_tiers(standings) if standings else {}

    for pred in predictions:
        fk   = _fixture_key(pred["home"], pred["away"])
        res  = result_map.get(fk)
        if not res:
            continue

        home, away = pred["home"], pred["away"]
        ah = res.get("actual_h", 0) or 0
        aa = res.get("actual_a", 0) or 0

        if ah > aa:    actual = "HOME"
        elif ah == aa: actual = "DRAW"
        else:          actual = "AWAY"

        our_tip  = (pred.get("tip", "") or "").split()[0]
        correct  = (our_tip == actual)

        odds_tip = pred.get("odds_tip")
        fp_tip   = pred.get("poisson_tip")
        form_tip = pred.get("strength_tip")
        bp_odds  = pred.get("bp_odds", {})

        h_tier = _find_tier(home, tier_map) if tier_map else "UNKNOWN"
        a_tier = _find_tier(away, tier_map) if tier_map else "UNKNOWN"
        tier_pair = f"{h_tier}_vs_{a_tier}"

        strong_side = None
        if h_tier == "STRONG" and a_tier in ("WEAK", "MODERATE"):
            strong_side = "HOME"
        elif a_tier == "STRONG" and h_tier in ("WEAK", "MODERATE"):
            strong_side = "AWAY"

        actual_btts   = (ah >= 1 and aa >= 1)
        actual_over25 = (ah + aa) > 2
        btts_prob     = pred.get("btts_prob", 50.0)
        over25_prob   = pred.get("over25_prob", 50.0)

        # ── 1. Per-signal accuracy update ────────────────────────────────────
        for sig, tip in [("odds", odds_tip), ("fingerprint", fp_tip), ("form", form_tip)]:
            if tip:
                sa = signal_acc.setdefault(sig, {"correct": 0, "total": 0, "recent": []})
                sa["total"] += 1
                hit = (tip == actual)
                if hit: sa["correct"] += 1
                sa.setdefault("recent", []).append(1 if hit else 0)
                if len(sa["recent"]) > 50: sa["recent"].pop(0)

        if strong_side is not None:
            sa = signal_acc.setdefault("tier", {"correct": 0, "total": 0, "recent": []})
            sa["total"] += 1
            hit = (strong_side == actual)
            if hit: sa["correct"] += 1
            sa.setdefault("recent", []).append(1 if hit else 0)
            if len(sa["recent"]) > 50: sa["recent"].pop(0)

        # ── 2. Tier-pair accuracy ─────────────────────────────────────────────
        if strong_side is not None:
            tp = tier_acc.setdefault(tier_pair, {"correct": 0, "total": 0})
            tp["total"] += 1
            if strong_side == actual: tp["correct"] += 1

        # ── 3. Odds-band accuracy ─────────────────────────────────────────────
        tipped_odds_str = (
            bp_odds.get("1") if our_tip == "HOME" else
            bp_odds.get("2") if our_tip == "AWAY" else
            bp_odds.get("X")
        )
        if tipped_odds_str:
            try:
                o = float(tipped_odds_str)
                if   o < 1.30: band = "<1.30"
                elif o < 1.60: band = "1.30-1.60"
                elif o < 1.90: band = "1.60-1.90"
                elif o < 2.30: band = "1.90-2.30"
                else:          band = "2.30+"
                b = band_acc.setdefault(band, {"correct": 0, "total": 0})
                b["total"] += 1
                if correct: b["correct"] += 1
            except (ValueError, TypeError):
                pass

        # ── 4. Market accuracy ────────────────────────────────────────────────
        if btts_prob >= 60:
            market_acc["btts_yes"]["total"] += 1
            if actual_btts: market_acc["btts_yes"]["correct"] += 1
        elif btts_prob <= 40:
            market_acc["btts_no"]["total"] += 1
            if not actual_btts: market_acc["btts_no"]["correct"] += 1
        if over25_prob >= 60:
            market_acc["over25"]["total"] += 1
            if actual_over25: market_acc["over25"]["correct"] += 1
        elif over25_prob <= 40:
            market_acc["under25"]["total"] += 1
            if not actual_over25: market_acc["under25"]["correct"] += 1

        # ── 5. Per-fixture memory record ──────────────────────────────────────
        # Every piece of data about this match is saved. The AI uses 6-10+ records
        # before forming an opinion about this fixture.
        blame = []
        primary_blame = ""
        diagnosis = ""

        if not correct:
            if odds_tip  and odds_tip  != actual: blame.append("odds")
            if fp_tip    and fp_tip    != actual: blame.append("fingerprint")
            if form_tip  and form_tip  != actual: blame.append("form")
            if strong_side and strong_side != actual: blame.append("tier")
            if not blame: blame.append("odds")
            primary_blame = blame[0]

            # Detailed diagnosis — context of why it went wrong
            parts = []
            if "odds" in blame:
                parts.append(
                    f"odds said {odds_tip} "
                    f"(H={bp_odds.get('1','?')} D={bp_odds.get('X','?')} A={bp_odds.get('2','?')})"
                    f" but actual={actual}"
                )
            if "fingerprint" in blame:
                parts.append(f"fingerprint said {fp_tip} but actual={actual}")
            if "tier" in blame:
                parts.append(f"tier said {strong_side} wins ({h_tier} vs {a_tier}) but actual={actual}")
            if "form" in blame:
                parts.append(f"form said {form_tip} but actual={actual}")
            diagnosis = "; ".join(parts)

            ai_log.append({
                "round":         round_id,
                "match":         f"{home} vs {away}",
                "predicted":     our_tip,
                "actual":        actual,
                "score":         f"{ah}-{aa}",
                "primary_blame": primary_blame,
                "blame":         blame,
                "diagnosis":     diagnosis,
                "tier_pair":     tier_pair,
            })

        # ── Full record saved to fixture memory ──────────────────────────────
        # Every data point is stored so the AI can compare past conditions
        # to today's conditions and recognise "same situation happening again".
        # Standings positions at time of match are encoded as a tier snapshot
        # so we can later ask: "was the team rated STRONG then, and did they lose?"

        # Standings position at time of this match (for future lesson context)
        h_pos_now = a_pos_now = h_pts_now = a_pts_now = None
        if standings:
            _h_row = _find_in_standings(home, standings)
            _a_row = _find_in_standings(away, standings)
            if _h_row: h_pos_now = _h_row.get("pos"); h_pts_now = _h_row.get("pts")
            if _a_row: a_pos_now = _a_row.get("pos"); a_pts_now = _a_row.get("pts")

        # ── Run recovery pattern analysis for this match ─────────────────────
        # Only runs when a strong team is involved vs weak/moderate.
        # Result stored in fixture_mem so the AI accumulates whether
        # "STRONG_RECOVERS" and "WEAK_REPEATS" predictions were correct.
        recovery_sig    = None
        recovery_correct = None
        recovery_strength_stored = 0.0
        if standings and tier_pair in ("STRONG_vs_WEAK","STRONG_vs_MODERATE",
                                        "WEAK_vs_STRONG","MODERATE_vs_STRONG"):
            _rec = _recovery_pattern_analysis(
                strong_team   = home if strong_side == "HOME" else away,
                opponent      = away if strong_side == "HOME" else home,
                opponent_tier = a_tier if strong_side == "HOME" else h_tier,
                model         = model,
                standings     = standings,
            )
            if _rec.get("data_confirmed"):
                recovery_sig = _rec["recovery_signal"]
                recovery_strength_stored = _rec.get("recovery_strength", 0.0)
                # Was the recovery prediction correct?
                if recovery_sig == "STRONG_RECOVERS":
                    # Correct if STRONG side actually won
                    expected_strong_outcome = "HOME" if strong_side == "HOME" else "AWAY"
                    recovery_correct = (actual == expected_strong_outcome)
                elif recovery_sig == "WEAK_REPEATS":
                    # Correct if WEAK/MODERATE side actually won
                    expected_weak_outcome = "AWAY" if strong_side == "HOME" else "HOME"
                    recovery_correct = (actual == expected_weak_outcome)

                # Update recovery accuracy tracker in ai_brain
                rec_acc = ai.setdefault("recovery_acc", {
                    "STRONG_RECOVERS": {"correct": 0, "total": 0, "recent": []},
                    "WEAK_REPEATS":    {"correct": 0, "total": 0, "recent": []},
                })
                if recovery_sig in rec_acc and recovery_correct is not None:
                    rec_acc[recovery_sig]["total"]  += 1
                    if recovery_correct:
                        rec_acc[recovery_sig]["correct"] += 1
                    rec_acc[recovery_sig].setdefault("recent", []).append(
                        1 if recovery_correct else 0
                    )
                    # Keep last 30
                    if len(rec_acc[recovery_sig]["recent"]) > 30:
                        rec_acc[recovery_sig]["recent"].pop(0)

                    c_ = rec_acc[recovery_sig]["correct"]
                    t_ = rec_acc[recovery_sig]["total"]
                    log.info(
                        f"🔄 Recovery acc [{recovery_sig}]: "
                        f"{'✅' if recovery_correct else '❌'} "
                        f"→ {c_}/{t_} ({c_/t_:.0%}) | "
                        f"{home} vs {away} actual={actual}"
                    )

        mem = fixture_mem.setdefault(fk, [])
        mem.append({
            "round":         round_id,
            "home":          home,
            "away":          away,
            "predicted":     our_tip,
            "actual":        actual,
            "score_h":       ah,
            "score_a":       aa,
            "correct":       correct,
            "odds_tip":      odds_tip,
            "fp_tip":        fp_tip,
            "form_tip":      form_tip,
            "h_tier":        h_tier,
            "a_tier":        a_tier,
            "tier_pair":     tier_pair,
            "strong_side":   strong_side,
            "blame":         blame,
            "primary_blame": primary_blame,
            "btts":          actual_btts,
            "over25":        actual_over25,
            "btts_prob":     btts_prob,
            "over25_prob":   over25_prob,
            "odds_h":        bp_odds.get("1"),
            "odds_d":        bp_odds.get("X"),
            "odds_a":        bp_odds.get("2"),
            # Standings context at time of match — for condition replay
            "h_pos":         h_pos_now,
            "a_pos":         a_pos_now,
            "h_pts":         h_pts_now,
            "a_pts":         a_pts_now,
            # Recovery pattern result — did the engine call it right?
            "recovery_signal":   recovery_sig,
            "recovery_correct":  recovery_correct,
            "recovery_strength": recovery_strength_stored,
            # Confirmation engine data — how aligned were signals when we predicted?
            # AI uses this to learn: "when confirm_rate < 0.5 we were wrong 70% of the time"
            "confirm_rate":      pred.get("confirm_rate", 0.6),
            "confirm_summary":   pred.get("confirm_summary", ""),
        })
        # ── Smart fixture_mem eviction ────────────────────────────────────────
        # At 30 records: if ≥70% wrong in last 10, this fixture is chronically
        # unpredictable — wipe it entirely so fresh active data can replace it.
        # This frees memory for genuinely progressing fixtures.
        # Otherwise just drop the oldest record (standard rolling window).
        if len(mem) > 30:
            _recent10   = mem[-10:]
            _wrong_rate = sum(1 for r in _recent10 if not r["correct"]) / len(_recent10)
            if _wrong_rate >= 0.70:
                # Chronically wrong — clear this fixture's memory entirely.
                # The next appearance will start fresh with no prior bias.
                log.debug(
                    f"🧹 AI fixture_mem wiped [{fk}]: "
                    f"{_wrong_rate:.0%} wrong in last 10 — clearing for fresh start"
                )
                fixture_mem[fk] = []
            else:
                mem.pop(0)   # rolling window: drop oldest

    # ── Trim ai_log ───────────────────────────────────────────────────────────
    if len(ai_log) > 50:
        ai["ai_log"] = ai_log[-50:]

    # ── Full AI re-evaluation every 10 rounds ─────────────────────────────────
    rounds_done = model.get("rounds_learned", 0)
    if rounds_done > 0 and rounds_done % 10 == 0:
        _ai_full_reeval(model, ai, standings)


def _ai_full_reeval(model: dict, ai: dict, standings: dict | None) -> None:
    """
    Full AI re-evaluation: run every 10 rounds.
    Analyses all signals and fixtures comprehensively, then adjusts blend weights.
    Logs a full AI thinking report.
    """
    signal_acc = ai.get("signal_acc", {})
    tier_acc   = ai.get("tier_acc", {})
    band_acc   = ai.get("band_acc", {})
    fixture_mem = ai.get("fixture_mem", {})

    # ── Compute rolling accuracy (last 20 samples) per signal ─────────────────
    def _recent_acc(sig: str) -> float | None:
        rec = signal_acc.get(sig, {}).get("recent", [])
        if len(rec) < 10: return None
        last20 = rec[-20:]
        return sum(last20) / len(last20)

    def _cumul_acc(sig: str) -> float | None:
        r = signal_acc.get(sig, {})
        t = r.get("total", 0)
        if t < 20: return None
        return r["correct"] / t

    odds_acc = _recent_acc("odds") or _cumul_acc("odds")
    fp_acc   = _recent_acc("fingerprint") or _cumul_acc("fingerprint")
    tier_acc_val = _recent_acc("tier") or _cumul_acc("tier")
    form_acc = _recent_acc("form") or _cumul_acc("form")

    # ── Tier effectiveness check ──────────────────────────────────────────────
    # Only count valid S-W and S-M matchups
    sw_correct = sw_total = 0
    for pair, rec in tier_acc.items():
        if "STRONG" in pair and ("WEAK" in pair or "MODERATE" in pair):
            sw_correct += rec["correct"]
            sw_total   += rec["total"]
    sw_rate = (sw_correct / sw_total) if sw_total >= 10 else None

    # ── Fixture-level intelligence: how many fixtures have 6+ records? ────────
    mature_fixtures   = sum(1 for m in fixture_mem.values() if len(m) >= 6)
    unstable_fixtures = []  # fixtures where the AI is consistently wrong

    for fk, mem in fixture_mem.items():
        if len(mem) < 6:
            continue
        recent10 = mem[-10:]
        wrong = [r for r in recent10 if not r["correct"]]
        # If wrong ≥ 60% of recent predictions → this fixture is unstable
        if len(wrong) / len(recent10) >= 0.60:
            most_actual = Counter(r["actual"] for r in wrong).most_common(1)
            if most_actual:
                unstable_fixtures.append((fk, most_actual[0][0], len(wrong), len(recent10)))

    # ── Best performing odds band ─────────────────────────────────────────────
    best_band = best_band_acc_val = None
    for band, rec in band_acc.items():
        if rec["total"] >= 15:
            a = rec["correct"] / rec["total"]
            if best_band_acc_val is None or a > best_band_acc_val:
                best_band_acc_val = a
                best_band = band

    # ── Rebalance weights ─────────────────────────────────────────────────────
    _ai_rebalance_weights_v2(model, odds_acc, fp_acc, tier_acc_val, form_acc, sw_rate)

    # ── Store intelligence summary ────────────────────────────────────────────
    ai["intelligence"] = {
        "rounds_evaluated":  model.get("rounds_learned", 0),
        "odds_acc_recent":   round(odds_acc, 3) if odds_acc else None,
        "fp_acc_recent":     round(fp_acc, 3) if fp_acc else None,
        "tier_acc_sw":       round(sw_rate, 3) if sw_rate else None,
        "form_acc_recent":   round(form_acc, 3) if form_acc else None,
        "sw_total":          sw_total,
        "mature_fixtures":   mature_fixtures,
        "unstable_fixtures": len(unstable_fixtures),
        "best_band":         best_band,
        "best_band_acc":     round(best_band_acc_val, 3) if best_band_acc_val else None,
        "weights":           dict(model.get("weights", {})),
    }

    log.info(
        f"🤖 AI full re-eval (R#{model.get('rounds_learned',0)}): "
        f"odds={f'{odds_acc:.0%}' if odds_acc else 'n/a'} "
        f"fp={f'{fp_acc:.0%}' if fp_acc else 'n/a'} "
        f"tier={f'{sw_rate:.0%}({sw_total})' if sw_rate else 'n/a'} "
        f"form={f'{form_acc:.0%}' if form_acc else 'n/a'} "
        f"| mature_fixtures={mature_fixtures} unstable={len(unstable_fixtures)} "
        f"| weights={model.get('weights', {})}"
    )


def _ai_rebalance_weights_v2(
    model: dict,
    odds_acc: float | None,
    fp_acc: float | None,
    tier_acc: float | None,
    form_acc: float | None,
    sw_rate: float | None,
) -> None:
    """
    Intelligent weight rebalancing. Rules:

    ODDS (anchor — betPawa's own implied probability):
      Always at least 55%. Rises toward 80% if it's been very accurate (≥72%).
      Falls toward 55% if accuracy is poor (<50%).

    FINGERPRINT (stored match pattern memory):
      Grows from 10% up to 28% as more data accumulates AND accuracy is high.
      Shrinks to 5% minimum if consistently wrong.

    TIER (STRONG vs WEAK/MODERATE — your strict rule):
      Grows toward 18% when S-W rule has been right ≥68% of the time.
      Shrinks to 2% when tier is misleading (<45% on S-W matchups).

    FORM (recent 6-match momentum):
      Small signal — 3% to 10%. Grows only when correlated with correct calls.

    Weights always normalise to 1.0.
    """
    MIN_ODDS, MAX_ODDS   = 0.55, 0.82
    MIN_FP,   MAX_FP     = 0.05, 0.28
    MIN_TIER, MAX_TIER   = 0.02, 0.18
    MIN_FORM, MAX_FORM   = 0.02, 0.10

    w = dict(model.get("weights", {
        "odds": 0.70, "fingerprint": 0.12, "tier": 0.12, "form": 0.06
    }))

    # ── Odds weight ───────────────────────────────────────────────────────────
    if odds_acc is not None:
        if odds_acc >= 0.72:
            target = MIN_ODDS + (MAX_ODDS - MIN_ODDS) * min(1.0, (odds_acc - 0.50) / 0.30)
        elif odds_acc < 0.50:
            target = MIN_ODDS
        else:
            target = MIN_ODDS + (MAX_ODDS - MIN_ODDS) * (odds_acc - 0.50) / 0.22
        # Smooth: move 15% toward target each evaluation
        w["odds"] = w.get("odds", 0.70) * 0.85 + target * 0.15

    w["odds"] = max(MIN_ODDS, min(MAX_ODDS, w["odds"]))

    # ── Fingerprint weight ────────────────────────────────────────────────────
    if fp_acc is not None:
        if fp_acc >= 0.65:
            target = MIN_FP + (MAX_FP - MIN_FP) * min(1.0, (fp_acc - 0.50) / 0.25)
        elif fp_acc < 0.45:
            target = MIN_FP
        else:
            target = MIN_FP + (MAX_FP - MIN_FP) * (fp_acc - 0.45) / 0.20
        w["fingerprint"] = w.get("fingerprint", 0.12) * 0.85 + target * 0.15

    w["fingerprint"] = max(MIN_FP, min(MAX_FP, w.get("fingerprint", 0.12)))

    # ── Tier weight ───────────────────────────────────────────────────────────
    if sw_rate is not None:
        if sw_rate >= 0.68:
            target = MIN_TIER + (MAX_TIER - MIN_TIER) * min(1.0, (sw_rate - 0.50) / 0.30)
        elif sw_rate < 0.45:
            target = MIN_TIER
        else:
            target = MIN_TIER + (MAX_TIER - MIN_TIER) * (sw_rate - 0.45) / 0.23
        w["tier"] = w.get("tier", 0.12) * 0.85 + target * 0.15

    w["tier"] = max(MIN_TIER, min(MAX_TIER, w.get("tier", 0.12)))

    # ── Form weight ───────────────────────────────────────────────────────────
    if form_acc is not None:
        if form_acc >= 0.60:
            target = MIN_FORM + (MAX_FORM - MIN_FORM) * min(1.0, (form_acc - 0.50) / 0.20)
        elif form_acc < 0.45:
            target = MIN_FORM
        else:
            target = MIN_FORM + (MAX_FORM - MIN_FORM) * (form_acc - 0.45) / 0.15
        w["form"] = w.get("form", 0.06) * 0.85 + target * 0.15

    w["form"] = max(MIN_FORM, min(MAX_FORM, w.get("form", 0.06)))

    # ── Normalise to 1.0 ─────────────────────────────────────────────────────
    total = sum(w.get(k, 0) for k in ("odds", "fingerprint", "tier", "form"))
    if total > 0:
        for k in ("odds", "fingerprint", "tier", "form"):
            w[k] = round(w[k] / total, 4)

    model["weights"] = w

    log.info(
        f"⚖️ AI weights rebalanced: "
        f"Odds={w.get('odds',0):.0%}  "
        f"FP={w.get('fingerprint',0):.0%}  "
        f"Tier={w.get('tier',0):.0%}  "
        f"Form={w.get('form',0):.0%}"
    )


def _ai_get_fixture_trust(model: dict, home: str, away: str) -> float:
    """
    Per-fixture trust score from AI brain.
    0.10 = historically very unreliable for this fixture.
    0.95 = historically very reliable.
    0.50 = no history yet.
    """
    fk  = _fixture_key(home, away)
    mem = model.get("ai_brain", {}).get("fixture_mem", {}).get(fk, [])
    if not mem:
        return 0.50
    recent = mem[-10:]
    correct_count = sum(1 for r in recent if r["correct"])
    raw = correct_count / len(recent)
    # Scale from 0.10–0.95
    return round(0.10 + raw * 0.85, 4)


def _ai_get_fixture_lesson(
    model: dict,
    home: str,
    away: str,
    standings: dict | None,
    odds: dict | None,
) -> dict:
    """
    THE AI THINKING ENGINE — called before every prediction.

    This function reads the full memory for this fixture, thinks about what
    has been happening, and decides whether to correct the base prediction.

    It reasons in 5 stages:

    STAGE 1 — HOW MUCH DATA DO WE HAVE?
      < 6 records  → not enough to form an opinion. No correction.
      6-9 records  → forming. Needs strong confirmation to act.
      10+ records  → mature. Can act on well-confirmed patterns.
      20+ records  → experienced. AI trusts its own judgment strongly.

    STAGE 2 — WHAT IS THE PATTERN?
      What have we been predicting? What has actually been happening?
      Is there a consistent failure direction?
      e.g. "We keep predicting HOME, but AWAY wins 7 out of 10 times"
      Or:  "We used to be right but something changed 4 rounds ago"

    STAGE 3 — WHY HAS IT BEEN HAPPENING?
      Analyse the blame history:
      — "tier" blame: strong team keeps losing → they're not really STRONG right now
      — "odds" blame: odds keep pointing wrong → server may be misleading here
      — "fingerprint" blame: stored history is outdated or wrong match-type
      — "form" blame: form signal keeps missing → team form is volatile here

    STAGE 4 — DOES TODAY CONFIRM THE PATTERN?
      Re-examine TODAY's data:
      — Current 6-match form of both teams (are they still trending that way?)
      — Current tier positions (are standings confirming the pattern?)
      — Today's odds direction (are odds also pointing the corrected way?)
      — Score patterns from stored memory (do they score/concede similarly?)

    STAGE 5 — HOW STRONGLY SHOULD WE ACT?
      Score: proportion of checks confirmed × data maturity × consistency strength
      Correction weight: 0.05 (soft nudge) to 0.45 (strong override)
      The weight only reaches 0.35+ when ALL of these are true:
        — 10+ records in memory
        — Pattern consistent for 6+ of last 10
        — At least 3 confirmation checks pass today
        — Primary blame is clear (not contradictory)

    Returns:
      {
        "has_lesson":       bool,
        "corrected_tip":    str | None,   "HOME" / "DRAW" / "AWAY"
        "correction_w":     float,        0.0–0.45
        "confidence":       float,        0.0–1.0  AI internal confidence score
        "data_points":      int,          how many records the decision is based on
        "reason":           str,          full reasoning summary
        "confirmed_by":     list[str],    what confirmed the lesson today
        "stage_reached":    int,          which reasoning stage was reached
      }
    """
    fk  = _fixture_key(home, away)
    ai  = model.get("ai_brain", {})
    mem = ai.get("fixture_mem", {}).get(fk, [])

    NULL = {
        "has_lesson": False, "corrected_tip": None, "correction_w": 0.0,
        "confidence": 0.0, "data_points": len(mem), "reason": "",
        "confirmed_by": [], "stage_reached": 0,
    }

    # ── STAGE 1: Data sufficiency ──────────────────────────────────────────────
    n = len(mem)
    if n < 6:
        NULL["reason"] = f"only {n} records — need 6+ to form opinion"
        NULL["stage_reached"] = 1
        return NULL

    # Data maturity multiplier: 6→0.40, 10→0.70, 20→0.95, 30→1.0
    if n < 10:
        maturity = 0.40 + (n - 6) * 0.075
    elif n < 20:
        maturity = 0.70 + (n - 10) * 0.025
    else:
        maturity = min(1.0, 0.95 + (n - 20) * 0.005)

    # ── STAGE 2: Pattern analysis ──────────────────────────────────────────────
    # Use last 10 for pattern (more recent = more relevant)
    recent = mem[-10:]
    all_n  = len(recent)
    wrongs = [r for r in recent if not r["correct"]]
    rights = [r for r in recent if r["correct"]]

    error_rate = len(wrongs) / all_n

    # Need meaningful failure rate to learn from
    if error_rate < 0.30:
        NULL["reason"] = (
            f"good accuracy on last {all_n} ({len(rights)}/{all_n} correct) — "
            f"no correction needed"
        )
        NULL["stage_reached"] = 2
        return NULL

    # What outcome has actually been happening when we're wrong?
    actual_counter  = Counter(r["actual"]    for r in wrongs)
    predict_counter = Counter(r["predicted"] for r in wrongs)

    if not actual_counter:
        NULL["reason"] = "no wrong predictions to learn from"
        NULL["stage_reached"] = 2
        return NULL

    # The lesson direction: what SHOULD we have predicted?
    lesson_actual = actual_counter.most_common(1)[0][0]
    lesson_pred   = predict_counter.most_common(1)[0][0]

    # Consistency: how many of the wrong picks point the same direction?
    n_consistent = actual_counter[lesson_actual]
    consistency  = n_consistent / len(wrongs)  # 0.0–1.0

    # Not consistent enough → contradictory failures, no clear lesson
    if consistency < 0.55 and n < 15:
        NULL["reason"] = (
            f"inconsistent failures: {dict(actual_counter)} — "
            f"no clear pattern yet ({n} records)"
        )
        NULL["stage_reached"] = 2
        return NULL

    # Also look at ALL records (not just last 10) for the overall outcome distribution
    all_actuals  = Counter(r["actual"] for r in mem)
    long_term_winner = all_actuals.most_common(1)[0][0]
    long_term_rate   = all_actuals[long_term_winner] / len(mem)

    # Detect regime change: has something changed recently?
    # Compare last 5 vs first 5 of available memory
    if n >= 10:
        first5 = mem[:5]
        last5  = mem[-5:]
        first5_actual = Counter(r["actual"] for r in first5).most_common(1)
        last5_actual  = Counter(r["actual"] for r in last5).most_common(1)
        regime_changed = (
            first5_actual and last5_actual and
            first5_actual[0][0] != last5_actual[0][0]
        )
    else:
        regime_changed = False

    # If regime changed, trust the recent pattern more than the long-term one
    if regime_changed:
        # Recent pattern dominates
        effective_lesson = lesson_actual
        regime_note = "regime_changed_recently"
    else:
        # If recent and long-term agree → stronger signal
        effective_lesson = lesson_actual
        regime_note = "consistent_long_term" if lesson_actual == long_term_winner else "recent_vs_longterm_split"

    # ── STAGE 3: Why has this been happening? ─────────────────────────────────
    blame_counter = Counter()
    for r in wrongs:
        for b in r.get("blame", []):
            blame_counter[b] += 1

    primary_blame = blame_counter.most_common(1)[0][0] if blame_counter else "odds"
    blame_concentrated = (
        blame_counter[primary_blame] / sum(blame_counter.values()) >= 0.50
        if blame_counter else False
    )

    # Tier-specific analysis: if tier is blamed, check HOW the tier is failing
    tier_analysis = ""
    if primary_blame == "tier":
        # Are we predicting the strong team to win but they keep losing?
        strong_sided_wrongs = [r for r in wrongs if r.get("strong_side") == lesson_pred]
        if strong_sided_wrongs:
            tier_analysis = f"strong_team_({lesson_pred})_consistently_losing"
        else:
            tier_analysis = "tier_mismatch_unclear"

    # Odds-specific analysis: are the odds consistently pointing the wrong way?
    odds_analysis = ""
    if primary_blame == "odds":
        # Check if the actual winner had consistently higher odds (was the 'underdog')
        underdog_wins = 0
        for r in wrongs:
            h_odds = r.get("odds_h"); a_odds = r.get("odds_a")
            if h_odds and a_odds:
                try:
                    fh_o = float(h_odds); fa_o = float(a_odds)
                    # Lower odds = favourite. If actual winner had higher odds → upset
                    if r["actual"] == "HOME" and fh_o > fa_o: underdog_wins += 1
                    if r["actual"] == "AWAY" and fa_o > fh_o: underdog_wins += 1
                except (ValueError, TypeError):
                    pass
        if underdog_wins >= 2:
            odds_analysis = f"underdog_wins_{underdog_wins}x_in_last_{len(wrongs)}"

    # ── STAGE 4: Confirm using TODAY's data ───────────────────────────────────
    confirmed_by     = []
    conf_score       = 0.0   # 0.0–1.0 sum of confirmation weights

    home_games = _team_last6(home, model)
    away_games = _team_last6(away, model)

    def _fs(games):
        """Form summary: wins, losses, wpct, trend, gpg, gcpg."""
        if not games:
            return {"w":0,"l":0,"wpct":0.0,"trend":"unknown","gpg":0.0,"gcpg":0.0,"n":0}
        n   = len(games)
        w   = sum(1 for g in games if g["gf"] > g["ga"])
        l   = sum(1 for g in games if g["gf"] < g["ga"])
        gpg  = sum(g["gf"] for g in games) / n
        gcpg = sum(g["ga"] for g in games) / n
        # Trend: last 3 vs first 3
        if n >= 4:
            wl3 = sum(1 for g in games[-3:] if g["gf"] > g["ga"])
            wf3 = sum(1 for g in games[:3]  if g["gf"] > g["ga"])
            trend = "rising" if wl3 > wf3 else ("falling" if wl3 < wf3 else "stable")
        else:
            trend = "stable"
        return {"w":w,"l":l,"wpct":w/n,"trend":trend,"gpg":gpg,"gcpg":gcpg,"n":n}

    hf = _fs(home_games)
    af = _fs(away_games)

    # Form checks for each lesson direction
    if effective_lesson == "AWAY":
        if af["trend"] == "rising":
            confirmed_by.append("away_rising"); conf_score += 0.20
        if af["wpct"] >= 0.55:
            confirmed_by.append(f"away_wpct={af['wpct']:.0%}"); conf_score += 0.15
        if hf["trend"] == "falling":
            confirmed_by.append("home_falling"); conf_score += 0.15
        if hf["wpct"] <= 0.35:
            confirmed_by.append(f"home_wpct={hf['wpct']:.0%}"); conf_score += 0.10
        if af["gpg"] > hf["gpg"]:
            confirmed_by.append("away_scores_more"); conf_score += 0.08
        if af["gcpg"] < hf["gcpg"]:
            confirmed_by.append("away_concedes_less"); conf_score += 0.07

    elif effective_lesson == "HOME":
        if hf["trend"] == "rising":
            confirmed_by.append("home_rising"); conf_score += 0.20
        if hf["wpct"] >= 0.55:
            confirmed_by.append(f"home_wpct={hf['wpct']:.0%}"); conf_score += 0.15
        if af["trend"] == "falling":
            confirmed_by.append("away_falling"); conf_score += 0.15
        if af["wpct"] <= 0.35:
            confirmed_by.append(f"away_wpct={af['wpct']:.0%}"); conf_score += 0.10
        if hf["gpg"] > af["gpg"]:
            confirmed_by.append("home_scores_more"); conf_score += 0.08
        if hf["gcpg"] < af["gcpg"]:
            confirmed_by.append("home_concedes_less"); conf_score += 0.07

    elif effective_lesson == "DRAW":
        if abs(hf["wpct"] - af["wpct"]) < 0.15:
            confirmed_by.append("balanced_form"); conf_score += 0.25
        if hf["trend"] == "stable" and af["trend"] == "stable":
            confirmed_by.append("both_stable"); conf_score += 0.15
        if abs(hf["gpg"] - af["gpg"]) < 0.3:
            confirmed_by.append("similar_attack"); conf_score += 0.10

    # Standings confirmation — compare today's positions to historical context
    if standings:
        tier_map_now = _get_all_tiers(standings)
        h_tier_now   = _find_tier(home, tier_map_now)
        a_tier_now   = _find_tier(away, tier_map_now)
        h_row = _find_in_standings(home, standings)
        a_row = _find_in_standings(away, standings)

        if h_row and a_row:
            h_pts = h_row.get("pts", 0)
            a_pts = a_row.get("pts", 0)
            h_pos = h_row.get("pos", 99)
            a_pos = a_row.get("pos", 99)

            # ── Historical standings context comparison ───────────────────────
            # Look at what the standings were in past wrong predictions.
            # If home was at pos 3 and still lost, and today they're ALSO around pos 3,
            # that's a stronger confirmation than just "they're STRONG tier today".
            wrong_h_positions = [r["h_pos"] for r in wrongs if r.get("h_pos") is not None]
            wrong_a_positions = [r["a_pos"] for r in wrongs if r.get("a_pos") is not None]
            wrong_h_pts_list  = [r["h_pts"] for r in wrongs if r.get("h_pts") is not None]
            wrong_a_pts_list  = [r["a_pts"] for r in wrongs if r.get("a_pts") is not None]

            if wrong_h_positions:
                avg_h_pos_when_wrong = sum(wrong_h_positions) / len(wrong_h_positions)
                avg_a_pos_when_wrong = sum(wrong_a_positions) / len(wrong_a_positions) if wrong_a_positions else 99
                # "Same situation" = current positions are within 3 spots of avg when wrong
                h_pos_similar = abs(h_pos - avg_h_pos_when_wrong) <= 3
                a_pos_similar = abs(a_pos - avg_a_pos_when_wrong) <= 3
                if h_pos_similar and a_pos_similar:
                    confirmed_by.append(
                        f"standings_same_as_when_wrong(H:{h_pos}≈{avg_h_pos_when_wrong:.0f}"
                        f" A:{a_pos}≈{avg_a_pos_when_wrong:.0f})"
                    )
                    conf_score += 0.20  # Strong signal: same position context = same pattern

            if effective_lesson == "AWAY":
                if a_pts > h_pts:
                    confirmed_by.append(f"away_more_pts({a_pts}v{h_pts})")
                    conf_score += 0.12
                if a_pos < h_pos:
                    confirmed_by.append(f"away_higher_table({a_pos}v{h_pos})")
                    conf_score += 0.10
                # Special: if tier blamed AND strong home is actually struggling in form
                if primary_blame == "tier" and h_tier_now == "STRONG":
                    if hf["wpct"] <= 0.40:
                        confirmed_by.append("strong_home_underperforming_tier")
                        conf_score += 0.18
                if primary_blame == "tier" and a_tier_now in ("WEAK", "MODERATE"):
                    if af["wpct"] >= 0.55:
                        confirmed_by.append("weak_away_outperforming_tier")
                        conf_score += 0.18
                # Check if away team has been climbing the table vs when we were wrong
                if wrong_a_positions and a_pos < min(wrong_a_positions):
                    confirmed_by.append(f"away_climbed_table_since_wrong(was_{min(wrong_a_positions)}_now_{a_pos})")
                    conf_score += 0.12

            elif effective_lesson == "HOME":
                if h_pts > a_pts:
                    confirmed_by.append(f"home_more_pts({h_pts}v{a_pts})")
                    conf_score += 0.12
                if h_pos < a_pos:
                    confirmed_by.append(f"home_higher_table({h_pos}v{a_pos})")
                    conf_score += 0.10
                if primary_blame == "tier" and a_tier_now == "STRONG":
                    if af["wpct"] <= 0.40:
                        confirmed_by.append("strong_away_underperforming_tier")
                        conf_score += 0.18
                if primary_blame == "tier" and h_tier_now in ("WEAK", "MODERATE"):
                    if hf["wpct"] >= 0.55:
                        confirmed_by.append("weak_home_outperforming_tier")
                        conf_score += 0.18
                if wrong_h_positions and h_pos < min(wrong_h_positions):
                    confirmed_by.append(f"home_climbed_table_since_wrong(was_{min(wrong_h_positions)}_now_{h_pos})")
                    conf_score += 0.12

    # Odds direction confirmation
    if odds:
        o1x2  = odds.get("1x2", {})
        raw_h = o1x2.get("1"); raw_a = o1x2.get("2")
        def _imp(o):
            try: return (1/float(o)) if float(o)>1.0 else None
            except: return None
        imp_h = _imp(raw_h); imp_a = _imp(raw_a)
        if imp_h and imp_a:
            if effective_lesson == "AWAY" and imp_a > imp_h:
                confirmed_by.append("odds_favour_away_too"); conf_score += 0.08
            elif effective_lesson == "HOME" and imp_h > imp_a:
                confirmed_by.append("odds_favour_home_too"); conf_score += 0.08
            elif effective_lesson in ("HOME", "AWAY") and (
                (effective_lesson == "AWAY" and imp_h > imp_a) or
                (effective_lesson == "HOME" and imp_a > imp_h)
            ):
                # Odds disagree with our lesson — slight penalty
                conf_score -= 0.05
                confirmed_by.append("odds_disagree(-)")

    # ── Recovery pattern as Stage 4 confirmation check ────────────────────────
    # The recovery engine checks the specific scenario of: strong team lost recently,
    # or weak/moderate won recently. Its findings are used HERE as one more
    # confirmation check for the lesson engine — not to drive predictions independently.
    # The lesson engine still decides whether to act, based on ALL checks combined.
    if standings:
        try:
            _rec_check = _recovery_pattern_analysis(
                strong_team   = home if _get_strong_side(home, away, standings) == "HOME" else away,
                opponent      = away if _get_strong_side(home, away, standings) == "HOME" else home,
                opponent_tier = _find_tier(away if _get_strong_side(home, away, standings) == "HOME" else home,
                                           _get_all_tiers(standings)),
                model         = model,
                standings     = standings,
            )
            if _rec_check.get("data_confirmed"):
                rec_sig = _rec_check["recovery_signal"]
                rec_str = _rec_check["recovery_strength"]
                # Check if recovery signal agrees with the lesson direction
                _strong_side = _get_strong_side(home, away, standings)
                strong_outcome = "HOME" if _strong_side == "HOME" else ("AWAY" if _strong_side == "AWAY" else None)
                weak_outcome   = "AWAY" if _strong_side == "HOME" else ("HOME" if _strong_side == "AWAY" else None)

                if rec_sig == "STRONG_RECOVERS" and effective_lesson == strong_outcome:
                    # Recovery confirms: strong will bounce back AND lesson says the same
                    confirmed_by.append(f"recovery_confirms_strong(str={rec_str:.0%})")
                    conf_score += 0.12 * rec_str
                elif rec_sig == "WEAK_REPEATS" and effective_lesson == weak_outcome:
                    # Recovery confirms: weak repeated the upset AND lesson says the same
                    confirmed_by.append(f"recovery_confirms_weak_repeat(str={rec_str:.0%})")
                    conf_score += 0.12 * rec_str
                elif rec_sig == "STRONG_RECOVERS" and effective_lesson == weak_outcome:
                    # Recovery disagrees with lesson direction — slight penalty
                    conf_score -= 0.05
                    confirmed_by.append("recovery_contradicts_lesson(-)")
                elif rec_sig == "WEAK_REPEATS" and effective_lesson == strong_outcome:
                    conf_score -= 0.05
                    confirmed_by.append("recovery_contradicts_lesson(-)")
        except Exception:
            pass  # recovery check is optional — never block the lesson engine

    # Memory consistency bonus: if the pattern has held for 6+ of last 10
    mem_consistency_bonus = 0.0
    if n_consistent >= 6:
        mem_consistency_bonus = min(0.20, (n_consistent - 5) * 0.05)
        confirmed_by.append(f"memory_consistent({n_consistent}/{len(wrongs)})")
        conf_score += mem_consistency_bonus

    # ── STAGE 5: Decision ─────────────────────────────────────────────────────
    # Minimum requirements to act:
    # — At least 3 confirmation checks (not counting memory consistency)
    #   OR total conf_score ≥ 0.35 after all checks
    # — With only 6-9 records: need conf_score ≥ 0.45 (cautious)
    # — With 10-19 records:    need conf_score ≥ 0.35 (confident)
    # — With 20+ records:      need conf_score ≥ 0.28 (experienced)

    non_memory_checks = [c for c in confirmed_by if not c.startswith("memory_") and not c.endswith("(-)")]
    n_checks = len(non_memory_checks)

    if n < 10:
        threshold = 0.45
    elif n < 20:
        threshold = 0.35
    else:
        threshold = 0.28

    if conf_score < threshold or n_checks < 2:
        NULL["reason"] = (
            f"lesson({lesson_pred}→{effective_lesson}) exists "
            f"but confirmation too weak "
            f"(score={conf_score:.2f} need={threshold:.2f}, checks={n_checks})"
        )
        NULL["data_points"]   = n
        NULL["stage_reached"] = 4
        return NULL

    # ── Correction weight ─────────────────────────────────────────────────────
    # Base: conf_score × maturity × consistency
    # Scale: 0.05 minimum nudge → 0.45 maximum override
    raw_w = conf_score * maturity * consistency
    correction_w = max(0.05, min(0.45, raw_w))

    # Confidence score (0–1): how sure the AI is of this lesson
    ai_confidence = min(1.0, conf_score * maturity)

    reason = (
        f"[{n} records, {n_consistent}/{len(wrongs)} consistent wrongs] "
        f"kept predicting {lesson_pred}→actual={effective_lesson} "
        f"| primary blame: {primary_blame}"
        + (f" ({tier_analysis})" if tier_analysis else "")
        + (f" ({odds_analysis})" if odds_analysis else "")
        + f" | regime: {regime_note}"
        + f" | confirmed by: {', '.join(confirmed_by[:5])}"
        + f" | confidence={ai_confidence:.2f} weight={correction_w:.2f}"
    )

    return {
        "has_lesson":    True,
        "corrected_tip": effective_lesson,
        "correction_w":  round(correction_w, 3),
        "confidence":    round(ai_confidence, 3),
        "data_points":   n,
        "reason":        reason,
        "confirmed_by":  confirmed_by,
        "stage_reached": 5,
    }


def _ai_get_blend_weights(model: dict) -> dict:
    """Return the current AI-managed blend weights."""
    return model.get("weights", {
        "odds": 0.70, "fingerprint": 0.12, "tier": 0.12, "form": 0.06
    })



def _learn_algo_signals(model: dict, predictions: list, results: list,
                          round_id_int: int):
    """
    Update all three algorithm reverse-engineering trackers after a round.
    """
    result_map = {_fixture_key(r["home"], r["away"]): r for r in results}

    # ── Engine 2 update: extend outcome_seq for each fixture ─────────────────
    for pred in predictions:
        fk  = _fixture_key(pred["home"], pred["away"])
        res = result_map.get(fk)
        if not res:
            continue
        ah, aa = res["actual_h"], res["actual_a"]
        if ah > aa:    actual_out = "HOME"
        elif ah == aa: actual_out = "DRAW"
        else:          actual_out = "AWAY"

        pm = model["pattern_memory"].setdefault(fk, {})
        seq = pm.setdefault("outcome_seq", [])
        seq.append(actual_out)
        # Keep last 40 outcomes per fixture
        if len(seq) > 40:
            seq.pop(0)

    # ── Engine 1 update: rebalancing tracker ─────────────────────────────────
    reb = model.setdefault("rebalance", {"window": []})
    window = reb["window"]

    # Compute expected rates from prediction confidence/odds
    n_preds = len([p for p in predictions if _fixture_key(p["home"], p["away"]) in result_map])
    if n_preds == 0:
        return

    # Actual outcomes this round
    H_act = D_act = A_act = 0
    for r in results:
        ah, aa = r["actual_h"], r["actual_a"]
        if ah > aa:    H_act += 1
        elif ah == aa: D_act += 1
        else:          A_act += 1
    total = H_act + D_act + A_act or 1

    # Expected from predictions (what the odds implied)
    H_exp = D_exp = A_exp = 0.0
    n_exp = 0
    for pred in predictions:
        if _fixture_key(pred["home"], pred["away"]) not in result_map:
            continue
        # Use stored odds probabilities if available, else use tip distribution
        H_exp += pred.get("prob_H", 0.45)
        D_exp += pred.get("prob_D", 0.27)
        A_exp += pred.get("prob_A", 0.28)
        n_exp += 1
    if n_exp > 0:
        H_exp /= n_exp; D_exp /= n_exp; A_exp /= n_exp

    window.append({
        "H_exp": H_exp, "D_exp": D_exp, "A_exp": A_exp,
        "H_act": H_act / total,
        "D_act": D_act / total,
        "A_act": A_act / total,
        "round_id": round_id_int,
    })
    # Keep last 100 rounds
    if len(window) > 100:
        window.pop(0)

    # ── Engine 3 update: round-ID modulo patterns ─────────────────────────────
    # dom_out defined here so Engine 4 (league_outcome_seq) can always use it
    dom_out = "HOME" if H_act >= D_act and H_act >= A_act else (
              "DRAW" if D_act >= A_act else "AWAY")
    if round_id_int > 0:
        rmp = model.setdefault("round_mod_patterns", {})
        for mod in range(2, 16):
            mod_s       = str(mod)
            remainder_s = str(round_id_int % mod)
            rmp.setdefault(mod_s, {})
            rmp[mod_s].setdefault(remainder_s, {"H": 0, "D": 0, "A": 0})
            key = {"HOME": "H", "DRAW": "D", "AWAY": "A"}[dom_out]
            rmp[mod_s][remainder_s][key] += 1

    # ── Engine 4: League-level outcome sequence ────────────────────────────────
    # Track the full outcome sequence across ALL rounds for this league.
    # e.g. [HOME_DOMINANT, AWAY_DOMINANT, HOME_DOMINANT, ...] — look for
    # macro-cycles in the server's generation pattern across rounds.
    # Also track per-position within round (match 1, match 2, ... match N)
    # to detect position-based seeding.
    league_seq = model.setdefault("league_outcome_seq", [])  # list of dominant outcome per round
    # Per-match-slot tracking: match slot 0,1,2... within round
    slot_tracker = model.setdefault("slot_outcomes", {})   # {slot_idx: [outcomes...]}

    for i, r in enumerate(results):
        ah, aa = r["actual_h"], r["actual_a"]
        if ah > aa:    out = "H"
        elif ah == aa: out = "D"
        else:          out = "A"
        slot_key = str(i)
        slot_tracker.setdefault(slot_key, [])
        slot_tracker[slot_key].append(out)
        # Keep last 60 per slot
        if len(slot_tracker[slot_key]) > 60:
            slot_tracker[slot_key].pop(0)

    # Dominant round outcome (used for league sequence)
    dom_short = {"HOME": "H", "DRAW": "D", "AWAY": "A"}.get(dom_out, "H")
    league_seq.append(dom_short)
    if len(league_seq) > 100:
        league_seq.pop(0)


def _get_slot_signal(model: dict, match_position: int) -> tuple[str | None, float]:
    """
    Check if match at this position in the round has a predictable outcome pattern.
    Returns (predicted_outcome, confidence) or (None, 0.0).
    """
    slot_tracker = model.get("slot_outcomes", {})
    key = str(match_position)
    seq = slot_tracker.get(key, [])
    if len(seq) < 10:
        return None, 0.0
    cycle_len, cycle_conf = _detect_cycle(seq)
    if cycle_len > 0 and cycle_conf >= 0.78:
        next_pos  = len(seq) % cycle_len
        pattern   = seq[-cycle_len:]
        predicted = {"H": "HOME", "D": "DRAW", "A": "AWAY"}.get(pattern[next_pos])
        return predicted, cycle_conf
    # Even without cycle, check simple dominance in last 20
    recent = seq[-20:]
    counts = {"H": recent.count("H"), "D": recent.count("D"), "A": recent.count("A")}
    best   = max(counts, key=counts.get)
    best_c = counts[best] / len(recent)
    if best_c >= 0.72:   # 72%+ dominance in last 20 = meaningful signal
        return {"H": "HOME", "D": "DRAW", "A": "AWAY"}[best], best_c
    return None, 0.0


def _update_pattern_memory(model: dict, home: str, away: str,
                            actual_h: int, actual_a: int):
    """
    Store ALL results keyed by fixture — true cumulative learning.
    No cap, no decay. Uses simple running win-rate counts.
    """
    fk = _fixture_key(home, away)
    pm = model["pattern_memory"]
    if fk not in pm:
        pm[fk] = {"results": [], "home_wins": 0, "draws": 0, "away_wins": 0,
                   "total_h": 0, "total_a": 0, "n": 0,
                   "hg_sum": 0, "ag_sum": 0}
    r = pm[fk]

    # Migrate old EMA-based entries that lack hg_sum (from previous bot version)
    if "hg_sum" not in r:
        # Reconstruct sums from stored results
        r["hg_sum"] = sum(rh for rh, ra in r.get("results", []))
        r["ag_sum"] = sum(ra for rh, ra in r.get("results", []))
        # Fix home_wins/draws/away_wins to be integer counts not EMA floats
        r["home_wins"] = sum(1 for rh, ra in r.get("results", []) if rh > ra)
        r["draws"]     = sum(1 for rh, ra in r.get("results", []) if rh == ra)
        r["away_wins"] = sum(1 for rh, ra in r.get("results", []) if rh < ra)

    r["results"].append((actual_h, actual_a))
    # Keep last 50 meetings for memory, but counts are always exact
    if len(r["results"]) > 50:
        # When we trim, adjust raw counters by removing the oldest entry
        oldest_h, oldest_a = r["results"].pop(0)
        r["hg_sum"] -= oldest_h
        r["ag_sum"] -= oldest_a
        if oldest_h > oldest_a:    r["home_wins"] -= 1
        elif oldest_h == oldest_a: r["draws"]     -= 1
        else:                      r["away_wins"]  -= 1
        r["n"] -= 1
        r["total_h"] = r["hg_sum"] / max(r["n"], 1)
        r["total_a"] = r["ag_sum"] / max(r["n"], 1)

    # Simple cumulative increment
    r["hg_sum"] = r.get("hg_sum", 0) + actual_h
    r["ag_sum"] = r.get("ag_sum", 0) + actual_a
    r["n"] += 1
    if actual_h > actual_a:    r["home_wins"] += 1
    elif actual_h == actual_a: r["draws"]     += 1
    else:                      r["away_wins"]  += 1
    r["total_h"] = r["hg_sum"] / r["n"]
    r["total_a"] = r["ag_sum"] / r["n"]


def _pattern_prior(model: dict, home: str, away: str) -> dict | None:
    """
    Return a probability prior from fixture pattern memory.
    Only used when we have 3+ confirmed historical meetings.
    Uses true cumulative counts — no decay.
    Handles old EMA-based backups gracefully.
    """
    fk = _fixture_key(home, away)
    r  = model["pattern_memory"].get(fk)
    if not r or r["n"] < 3:
        return None
    hw = r["home_wins"]
    dw = r["draws"]
    aw = r["away_wins"]
    # Old EMA format stored floats; new format stores ints.
    # Convert to raw counts if needed by using n as reference.
    if isinstance(hw, float) and hw <= 1.0 and hw > 0:
        # EMA-weighted fracs — multiply by n to get approximate counts
        hw = hw * r["n"]
        dw = dw * r["n"]
        aw = aw * r["n"]
    tot = hw + dw + aw or 1.0
    return {
        "hw":    hw / tot,
        "dw":    dw / tot,
        "aw":    aw / tot,
        "avg_h": r["total_h"],
        "avg_a": r["total_a"],
        "n":     r["n"],
    }


def _form_snapshot_for_record(team: str, model: dict) -> dict:
    """
    Capture a compact form snapshot for a team at the current moment.
    Stored with each fp_db record so future predictions can weight votes
    by how strong/weak this team actually was when this outcome occurred.

    Returns dict with:
      win_pct       — % of last 6 games won           (0–100)
      goal_avg      — average goals scored per game   (float)
      concede_avg   — average goals conceded per game (float)
      scored_both   — % of games where team scored    (0–100)
      n             — number of games available (0 if no data)
    """
    games = _team_last6(team, model, n=6)
    if not games:
        return {"win_pct": 0.0, "goal_avg": 0.0, "concede_avg": 0.0,
                "scored_both": 0.0, "n": 0}
    n = len(games)
    wins      = sum(1 for g in games if g["gf"] > g["ga"])
    goals_f   = sum(g["gf"] for g in games)
    goals_a   = sum(g["ga"] for g in games)
    scored    = sum(1 for g in games if g["gf"] > 0)
    return {
        "win_pct":     round(wins / n * 100, 1),
        "goal_avg":    round(goals_f / n, 2),
        "concede_avg": round(goals_a / n, 2),
        "scored_both": round(scored / n * 100, 1),
        "n":           n,
    }


def _learn_from_round(bot_data: dict, league_id: int,
                       predictions: list[dict], results: list[dict],
                       round_id: int = 0, season_id: str = "",
                       is_bootstrap: bool = False):
    """
    Called after every round completes.
    Saves FULL match data — all market odds + all outcomes — into the
    fingerprint database and pattern memory. Nothing is discarded.

    is_bootstrap: if True, skip writing to fingerprint_db and match_log.
    """
    if not results:
        return

    # ── Smart odds_store management ───────────────────────────────────────────
    # Rules:
    #   1. Mark outcome + score on this round's entries when results come in
    #   2. DELETE only entries that turned BAD (outcome recorded + was wrong prediction)
    #      Bad = the odds pattern produced the WRONG result → not valuable → fully removed
    #   3. KEEP forever: entries with correct/consistent outcome regardless of age
    #      Valuable repeating patterns must never be deleted just because they're old
    #   4. DELETE stale entries: saved but result never came after 3+ rounds
    #      (means bot was offline when round played — no real result recorded)
    _os      = bot_data.setdefault("odds_store", {})
    _lid_os  = _os.setdefault(str(league_id), {})
    _rid_str = str(round_id)

    # Step 1: Mark outcome + score on each entry for this round
    if _rid_str in _lid_os:
        for res in results:
            ah = res.get("actual_h", 0) or 0
            aa = res.get("actual_a", 0) or 0
            ft = "HOME" if ah > aa else ("AWAY" if aa > ah else "DRAW")
            _canon = "|".join(sorted([
                res.get("home", "").strip().upper(),
                res.get("away", "").strip().upper()
            ]))
            entry = _lid_os[_rid_str].get(_canon)
            if entry:
                entry["outcome"] = ft
                entry["score_h"] = ah
                entry["score_a"] = aa
                if res.get("ht_h") is not None:
                    entry["ht_h"] = res.get("ht_h")
                    entry["ht_a"] = res.get("ht_a")
                # England only: build cross-round score history per fixture
                # England only: score history keyed by fixture+odds fingerprint
                # Same odds = same cycle track. Different odds = new track.
                # Records both new scores AND repeats (cycle confirmations).
                if league_id == 7794:
                    _sh_store = bot_data.setdefault("score_history_7794", {})
                    _snap = entry.get("odds_snapshot", {})
                    _o1x2 = _snap.get("1x2", {})
                    _ofp  = f"{_o1x2.get('1',0)}-{_o1x2.get('X',0)}-{_o1x2.get('2',0)}"
                    _sh_key = f"{_canon}|{_ofp}"  # fixture+odds fingerprint
                    _fh = _sh_store.setdefault(_sh_key, [])
                    # Skip if this exact round already recorded
                    if not any(r["round_id"] == (int(_rid_str) if _rid_str.isdigit() else 0) for r in _fh):
                        _existing_scores = {(r["score_h"], r["score_a"]) for r in _fh}
                        _fh.append({
                            "outcome":      ft,
                            "score_h":      ah,
                            "score_a":      aa,
                            "round_id":     int(_rid_str) if _rid_str.isdigit() else 0,
                            "is_new_score": (ah, aa) not in _existing_scores,
                            "is_repeat":    (ah, aa) in _existing_scores,
                        })
                        _fh.sort(key=lambda x: x["round_id"])
    # Step 2+3+4: Scan ALL rounds — delete only bad/stale, keep valuable forever
    sorted_rids = sorted(
        _lid_os.keys(),
        key=lambda x: int(x) if x.isdigit() else 0
    )

    # ── Cross-round pattern analysis ──────────────────────────────────────────
    # For each fixture across ALL rounds: collect all confirmed results.
    # If 3+ confirmed results AND ≥67% are inconsistent (no dominant result)
    # → the odds pattern is unreliable for this fixture → delete ALL its entries.
    fixture_results = {}  # fk → list of outcomes across all rounds
    for rid_key in _lid_os:
        for fk, entry in _lid_os[rid_key].items():
            out = entry.get("outcome")
            if out:
                fixture_results.setdefault(fk, []).append(out)

    # Identify fixtures whose odds pattern proved bad
    bad_fixtures = set()
    for fk, outcomes in fixture_results.items():
        if len(outcomes) < 3:
            continue  # not enough data yet — give it more rounds
        # Find dominant result
        from collections import Counter as _Ctr
        counts   = _Ctr(outcomes)
        dominant = counts.most_common(1)[0]
        dominant_pct = round(dominant[1] / len(outcomes) * 100)
        if dominant_pct < 67:
            # Odds repeat for this fixture is inconsistent — pattern proved unreliable
            bad_fixtures.add(fk)
            log.info(f"🗑️ odds_store [{league_id}]: deleting bad pattern {fk} "
                     f"({dominant_pct}% consistent over {len(outcomes)} results)")

    # Now per-round cleanup
    for rid_key in list(_lid_os.keys()):
        rid_entries = _lid_os[rid_key]
        to_delete   = []

        for fk, entry in rid_entries.items():
            outcome = entry.get("outcome")

            # Delete if fixture proved to be a bad pattern
            if fk in bad_fixtures:
                to_delete.append(fk)
                continue

            if outcome is None:
                # No result recorded — stale if 3+ newer rounds passed
                age = sum(
                    1 for r in sorted_rids
                    if rid_key.isdigit() and r.isdigit() and int(r) > int(rid_key)
                )
                if age >= 3:
                    to_delete.append(fk)  # stale — never got result → remove

        # Complete removal — no residues
        for fk in to_delete:
            del rid_entries[fk]

        # If round has no entries left → delete round dict entirely
        if not rid_entries:
            del _lid_os[rid_key]

    model = _get_model(bot_data, league_id)
    cum   = model["cumulative"]

    # Build lookup: fixture_key → prediction (for odds snapshot)
    pred_map = {_fixture_key(p["home"], p["away"]): p for p in (predictions or [])}

    fp_db = model.setdefault("fingerprint_db", {})

    outcome_correct = btts_correct = over25_correct = 0
    total_goals     = 0
    home_wins = draws = away_wins = n_matches = 0

    # Canonical team codes for this league — only save records for teams that belong here
    _league_codes = LEAGUE_TEAMS.get(league_id, set())

    for res in results:
        # Guard: skip results for teams not in this league
        if _league_codes:
            _rh = res.get("home", "").upper().strip()
            _ra = res.get("away", "").upper().strip()
            if not ((_rh in _league_codes or _rh[:3] in _league_codes) and
                    (_ra in _league_codes or _ra[:3] in _league_codes)):
                log.debug(f"_learn_from_round [{league_id}]: skipping {_rh} vs {_ra} — not in league")
                continue

        ah  = res.get("actual_h", 0) or 0
        aa  = res.get("actual_a", 0) or 0
        ht_h = res.get("ht_h")
        ht_a = res.get("ht_a")

        n_matches   += 1
        total_goals += ah + aa

        # FT outcome
        if ah > aa:    ft_out = "HOME"; home_wins += 1
        elif ah == aa: ft_out = "DRAW"; draws     += 1
        else:          ft_out = "AWAY"; away_wins += 1

        # HT/FT combination — stored using betPawa's own notation: 1=Home, X=Draw, 2=Away
        # So "1/1" means Home winning at HT and Home wins FT, matching the odds grid exactly
        if ht_h is not None and ht_a is not None:
            _s = lambda gh, ga: "1" if gh > ga else ("2" if gh < ga else "X")
            htft_str = f"{_s(ht_h, ht_a)}/{_s(ah, aa)}"
        else:
            # Derive from FT only when HT not available
            _s = lambda gh, ga: "1" if gh > ga else ("2" if gh < ga else "X")
            htft_str = f"?/{_s(ah, aa)}"   # partial — HT unknown

        actual_btts = (ah > 0 and aa > 0)
        actual_over = (ah + aa) > 2

        # DC result
        if ft_out == "HOME":   dc_res = "1X"
        elif ft_out == "DRAW": dc_res = "1X" if ah >= aa else "X2"
        else:                   dc_res = "X2"
        if ah > 0 and aa > 0 and ft_out != "DRAW":
            dc_res = "12"

        # Use alphabetical canonical key — first team alphabetically is always "home"
        canon_parts = sorted([res["home"], res["away"]])
        fk          = "|".join(canon_parts)
        canon_home  = canon_parts[0]   # alphabetically first = canonical home
        is_flipped  = (res["home"] != canon_home)  # actual home was the "away" team

        pred = pred_map.get(fk) or pred_map.get(_fixture_key(res["home"], res["away"]), {})

        # Saved odds snapshot
        odds_snap = pred.get("_odds_snapshot", {})
        # Fallback: look up from odds_store (server-fetched odds saved before round played)
        if not odds_snap or not odds_snap.get("1x2"):
            _os_entry = (bot_data.get("odds_store", {})
                         .get(str(league_id), {})
                         .get(str(round_id), {})
                         .get("|".join(sorted([
                             res.get("home","").strip().upper(),
                             res.get("away","").strip().upper()
                         ])), {}))
            if _os_entry.get("odds_snapshot", {}).get("1x2"):
                odds_snap = _os_entry["odds_snapshot"]
        fp_key    = list(_odds_fp_key(odds_snap)) if odds_snap else [0.0, 0.0, 0.0]

        # ── Flip scores/outcome/odds if actual home != canonical home ─────────
        # All records stored from canonical home team's perspective so they
        # are directly comparable regardless of who hosted on match day.
        if is_flipped:
            rec_score_h, rec_score_a = aa, ah          # swap goals
            rec_ht_h,    rec_ht_a    = ht_a, ht_h      # swap HT goals
            # Flip outcome
            _flip_out = {"HOME": "AWAY", "AWAY": "HOME", "DRAW": "DRAW"}
            rec_outcome = _flip_out.get(ft_out, ft_out)
            # Flip HT/FT notation: "1" means actual home, flip 1↔2
            def _flip_htft(s):
                return s.replace("1","@").replace("2","1").replace("@","2") if s and "/" in s else s
            rec_htft = _flip_htft(htft_str)
            # Flip DC result
            _flip_dc = {"1X": "X2", "X2": "1X", "12": "12"}
            rec_dc = _flip_dc.get(dc_res, dc_res)
            # Flip odds snapshot (swap imp_h/imp_a)
            if odds_snap and "1x2" in odds_snap:
                flipped_odds = dict(odds_snap)
                ox = dict(odds_snap.get("1x2", {}))
                ox["1"], ox["2"] = ox.get("2", ox.get("1")), ox.get("1", ox.get("2"))
                flipped_odds["1x2"] = ox
                fp_key = list(_odds_fp_key(flipped_odds)) if flipped_odds else fp_key
            rec_btts = actual_btts   # BTTS is symmetric
        else:
            rec_score_h, rec_score_a = ah, aa
            rec_ht_h,    rec_ht_a    = ht_h, ht_a
            rec_outcome = ft_out
            rec_htft    = htft_str
            rec_dc      = dc_res
            rec_btts    = actual_btts

        # ── Save full record to fingerprint_db + match_log ───────────────────
        # SKIP during bootstrap — synthetic records (round_id=0, no real odds)
        # must not pollute fp_db or match_log. Bootstrap only updates cumulative
        # stats below. Real live rounds always write here.
        if not is_bootstrap:
            fp_db.setdefault(fk, [])
            record = {
                "fp_key":        fp_key,
                "odds_snapshot": odds_snap,
                "outcome":       rec_outcome,
                "htft_result":   rec_htft,
                "btts_result":   rec_btts,
                "ou15_result":   (ah + aa) > 1,
                "ou25_result":   actual_over,
                "ou35_result":   (ah + aa) > 3,
                "score_h":       rec_score_h,
                "score_a":       rec_score_a,
                "ht_h":          rec_ht_h,
                "ht_a":          rec_ht_a,
                "dc_result":     rec_dc,
                "round_id":      round_id,
                "season_id":     season_id,
                "league_id":     league_id,
                "was_flipped":   is_flipped,
                "pos_h":         _get_position(fp_db, canon_parts[0], model),
                "pos_a":         _get_position(fp_db, canon_parts[1], model),
                "_form_h":       _form_snapshot_for_record(res["home"], model),
                "_form_a":       _form_snapshot_for_record(res["away"], model),
            }
            _new_key = tuple(fp_key)
            _should_save = True
            for _er in fp_db[fk]:
                _s = _fp_similarity(tuple(_er.get("fp_key", [])), _new_key)
                if _s >= 0.97:
                    if _er.get("outcome") == rec_outcome:
                        _should_save = False
                    break
            if _should_save:
                fp_db[fk].append(record)
                if len(fp_db[fk]) > 15:
                    _all_recs  = fp_db[fk]
                    _n_all     = len(_all_recs)
                    _dom       = max(set(r.get("outcome","") for r in _all_recs),
                                     key=lambda o: sum(1 for r in _all_recs if r.get("outcome")==o))
                    _worst_idx = 0
                    _worst_score = 2.0
                    for _i, _r in enumerate(_all_recs):
                        _recency    = _i / (_n_all - 1) if _n_all > 1 else 1.0
                        _consistent = 1.0 if _r.get("outcome") == _dom else 0.0
                        _score      = 0.60 * _recency + 0.40 * _consistent
                        if _score < _worst_score:
                            _worst_score = _score
                            _worst_idx   = _i
                    fp_db[fk].pop(_worst_idx)
                model.pop("_team_idx_size", None)
                model.pop("_odds_idx_size", None)

            # ── Append to match_log ───────────────────────────────────────────
            _match_log = model.setdefault("match_log", [])
            _ml_dedup_key = (res["home"], res["away"], round_id, ah, aa)
            _ml_seen = model.setdefault("_ml_seen", {})
            if _ml_dedup_key not in _ml_seen:
                _ml_seen[_ml_dedup_key] = 1
                _match_log.append({
                    "home":      res["home"],
                    "away":      res["away"],
                    "score_h":   ah,
                    "score_a":   aa,
                    "round_id":  round_id,
                    "season_id": season_id,
                })
            # ── Smart match_log eviction when cap is reached ─────────────────
            # Instead of slicing off the oldest 1000 entries blindly,
            # identify which fixtures are "inactive" — teams that haven't
            # appeared in the last 300 entries — and purge those first.
            # Active fixtures (both teams seen recently) are never evicted early.
            # NOTE: store team names as-is (not lowercased) to match lookup exactly.
            if len(_match_log) > 2000:
                _recent_300  = _match_log[-300:]
                _active_teams: set[str] = set()
                for _e in _recent_300:
                    _active_teams.add((_e.get("home") or "").upper())
                    _active_teams.add((_e.get("away") or "").upper())

                # Also protect the current season's entries from being evicted —
                # identify current season as the season_id that appears most in
                # the last 300 entries (most-recent season dominates tail).
                from collections import Counter as _Counter
                _recent_sids = [_e.get("season_id", "") for _e in _recent_300 if _e.get("season_id")]
                _cur_sid_ev  = _Counter(_recent_sids).most_common(1)[0][0] if _recent_sids else ""

                # Partition: inactive = both teams absent from recent 300
                # Also protect entries belonging to the most-recent season.
                _inactive_idx = []
                for _i, _e in enumerate(_match_log):
                    _eh = (_e.get("home") or "").upper()
                    _ea = (_e.get("away") or "").upper()
                    _es = _e.get("season_id", "")
                    if _cur_sid_ev and _es == _cur_sid_ev:
                        continue   # always protect current-season entries
                    if _eh not in _active_teams and _ea not in _active_teams:
                        _inactive_idx.append(_i)

                if len(_inactive_idx) >= 500:
                    # Enough inactive entries — remove oldest inactive first
                    _to_remove = set(_inactive_idx[:len(_match_log) - 2000 + 200])
                    _match_log[:] = [_e for _i, _e in enumerate(_match_log)
                                     if _i not in _to_remove]
                else:
                    # No inactive entries or not enough — fall back to oldest-first trim
                    # but protect current-season entries regardless
                    if _cur_sid_ev:
                        _cur_entries  = [_e for _e in _match_log if _e.get("season_id") == _cur_sid_ev]
                        _old_entries  = [_e for _e in _match_log if _e.get("season_id") != _cur_sid_ev]
                        _trim_old     = _old_entries[max(0, len(_old_entries) - (2000 - len(_cur_entries))):]
                        _match_log[:] = _trim_old + _cur_entries
                    else:
                        _match_log[:] = _match_log[-2000:]

                # Rebuild _ml_seen from trimmed log to stay consistent
                model["_ml_seen"] = {
                    (r["home"], r["away"], r["round_id"], r["score_h"], r["score_a"]): 1
                    for r in _match_log
                }

        # ── Derive dominant outcome / HTFT / pattern memory ──────────────────
        # These all reference fp_db[fk] — only run when we actually wrote to it
        if not is_bootstrap and fp_db.get(fk):
            outcomes_all = [r["outcome"] for r in fp_db[fk]]
            dom = max(set(outcomes_all), key=outcomes_all.count)
            dom_conf = outcomes_all.count(dom) / len(outcomes_all)
            for r in fp_db[fk]:
                r["dominant_outcome"] = dom
                r["dominant_outcome_conf"] = round(dom_conf, 3)

            htfts = [r["htft_result"] for r in fp_db[fk] if r.get("htft_result")]
            if htfts:
                dom_htft = max(set(htfts), key=htfts.count)
                dom_htft_conf = htfts.count(dom_htft) / len(htfts)
                for r in fp_db[fk]:
                    r["dominant_htft"]      = dom_htft
                    r["dominant_htft_conf"] = round(dom_htft_conf, 3)
                for r in fp_db[fk]:
                    r["n_samples"] = len(fp_db[fk])

        # ── Pattern memory (for form audit) ───────────────────────────────────
        _update_pattern_memory(model, res["home"], res["away"], ah, aa)

        # ── Cumulative stats ───────────────────────────────────────────────────
        our_out = pred.get("tip", "").split()[0] if pred.get("tip") else None
        if our_out == ft_out:
            outcome_correct += 1
            cum["outcome_correct"] += 1
            cum["conf_sum_correct"] += pred.get("conf", 50.0)
            cum["n_correct"]        += 1
        elif our_out:
            cum["conf_sum_wrong"]   += pred.get("conf", 50.0)
            cum["n_wrong"]          += 1

        # ── Odds trap tracker ─────────────────────────────────────────────────
        # Track per-odds-band failure rates so the trap detector builds up
        # signal over time. Uses the implied home win prob stored at pred time.
        if our_out and not is_bootstrap:
            _trap_imp_h = pred.get("prob_H", imp_h if "imp_h" in dir() else 0.45)
            _update_odds_trap(model, _trap_imp_h, our_out, ft_out)

        if actual_btts  and pred.get("btts_pred", False) == actual_btts:  btts_correct  += 1
        if actual_over  and pred.get("over25_pred", False) == actual_over: over25_correct += 1
        if actual_btts:  cum["btts_count"]  += 1
        if actual_over:  cum["over25_count"] += 1

        # Band accuracy
        if our_out and pred.get("conf"):
            conf_val = pred["conf"]
            bucket   = str(int(conf_val // 5) * 5)
            ma = model.setdefault("margin_acc", {})
            ma.setdefault(bucket, [0, 0])
            ma[bucket][1] += 1
            if our_out == ft_out:
                ma[bucket][0] += 1

        # ── HT/FT accuracy learning ───────────────────────────────────────────
        # Track which HT/FT outcomes the fingerprint predicted vs actual.
        # dominant_htft from the fp_match is our prediction.
        # htft_str is the actual result.
        # This gives the bot IQ over HT/FT patterns just like 1X2.
        if not is_bootstrap and "?" not in htft_str:
            _htft_pred = pred.get("dominant_htft")
            _htft_actual = htft_str  # e.g. "1/1", "X/2", "2/2"
            if _htft_pred and _htft_actual:
                _htft_acc = model.setdefault("htft_acc", {})
                _hrec = _htft_acc.setdefault(_htft_pred, {"correct": 0, "total": 0})
                _hrec["total"] += 1
                if _htft_pred == _htft_actual:
                    _hrec["correct"] += 1
                # Also track per-actual-outcome distribution (how often each result occurs)
                _hdist = model.setdefault("htft_dist", {})
                _hdist[_htft_actual] = _hdist.get(_htft_actual, 0) + 1

            # ── Strategy stats learning ───────────────────────────────────────
            _strat_tip = pred.get("strategy_tip")
            _strat_mkt = pred.get("strategy_market")
            if _strat_tip and _strat_mkt:
                _actual_out = ft_out
                _btts_actual = "BTTS_YES" if (ah > 0 and aa > 0) else "BTTS_NO"
                _learn_actual = _btts_actual if _strat_mkt == "BTTS" else _actual_out
                _strategy_update_stats(bot_data, _strat_mkt, _strat_tip, _learn_actual)

            # ── Odds repeat learning ──────────────────────────────────────────
            if pred.get("odds_repeat"):
                _or_stats = bot_data.setdefault("odds_repeat_stats", {
                    "total":0,"correct":0,"skipped":0,
                    "full_match_total":0,"full_match_correct":0,
                    "partial_match_total":0,"partial_match_correct":0,
                    "elite_lock_total":0,"elite_lock_correct":0,
                    "elite_total":0,"elite_correct":0,
                    "premium_total":0,"premium_correct":0,
                })
                _or_pct      = pred.get("odds_repeat_pct", 0)
                _or_outcome  = pred.get("odds_repeat_outcome", "")
                _or_consist  = pred.get("odds_repeat_consistency", 0)
                _or_tier     = pred.get("odds_repeat_tier", "")
                _or_correct  = (_or_outcome == ft_out)

                _or_stats["total"]   = _or_stats.get("total", 0) + 1
                if _or_correct:
                    _or_stats["correct"] = _or_stats.get("correct", 0) + 1

                if _or_pct == 100 and _or_consist == 100:
                    _or_stats["elite_lock_total"]   = _or_stats.get("elite_lock_total", 0) + 1
                    if _or_correct:
                        _or_stats["elite_lock_correct"] = _or_stats.get("elite_lock_correct", 0) + 1
                elif _or_pct == 100:
                    _or_stats["elite_total"]   = _or_stats.get("elite_total", 0) + 1
                    if _or_correct:
                        _or_stats["elite_correct"] = _or_stats.get("elite_correct", 0) + 1
                elif _or_pct >= 75:
                    _or_stats["premium_total"]   = _or_stats.get("premium_total", 0) + 1
                    if _or_correct:
                        _or_stats["premium_correct"] = _or_stats.get("premium_correct", 0) + 1

                if _or_pct == 100:
                    _or_stats["full_match_total"]   = _or_stats.get("full_match_total", 0) + 1
                    if _or_correct:
                        _or_stats["full_match_correct"] = _or_stats.get("full_match_correct", 0) + 1
                else:
                    _or_stats["partial_match_total"]   = _or_stats.get("partial_match_total", 0) + 1
                    if _or_correct:
                        _or_stats["partial_match_correct"] = _or_stats.get("partial_match_correct", 0) + 1
            ota = model.setdefault("outcome_type_acc", {
                "HOME": {"correct":0,"total":0},
                "DRAW": {"correct":0,"total":0},
                "AWAY": {"correct":0,"total":0},
            })
            ota.setdefault(our_out, {"correct":0,"total":0})
            ota[our_out]["total"] += 1
            if our_out == ft_out:
                ota[our_out]["correct"] += 1

        # Skip-outcome learning
        if pred.get("_skipped"):
            skip_stats = model.setdefault("skip_outcomes", {})
            reason_key = pred.get("_skip_reason","unknown").split("=")[0]
            rec2 = skip_stats.setdefault(reason_key, {"would_have_won":0,"would_have_lost":0})
            if our_out == ft_out: rec2["would_have_won"]  += 1
            else:                  rec2["would_have_lost"] += 1

        # ── Signal accuracy tracking ──────────────────────────────────────────
        sa = model.setdefault("signal_acc", {
            "odds":     {"correct": 0, "total": 0},
            "poisson":  {"correct": 0, "total": 0},
            "strength": {"correct": 0, "total": 0},
        })
        for sig_name in ("odds", "poisson", "strength"):
            tip = pred.get(f"{sig_name}_tip")
            if tip:
                sa.setdefault(sig_name, {"correct": 0, "total": 0})
                sa[sig_name]["total"] += 1
                if tip == ft_out:
                    sa[sig_name]["correct"] += 1

    if n_matches == 0:
        return

    # ── Update cumulative totals ───────────────────────────────────────────────
    cum["outcome_total"]   += n_matches
    cum["btts_total"]      += n_matches
    cum["over25_total"]    += n_matches
    cum["btts_correct"]    += btts_correct
    cum["over25_correct"]  += over25_correct
    cum["goals_total"]     += total_goals
    cum["matches_total"]   += n_matches
    cum["home_wins"]       += home_wins
    cum["draws"]           += draws
    cum["away_wins"]       += away_wins

    model["rounds_learned"] = model.get("rounds_learned", 0) + 1

    ot = cum["outcome_total"]
    model["outcome_acc"]   = round(cum["outcome_correct"] / ot, 4) if ot > 0 else 0.0
    bt = cum["btts_total"]
    model["btts_acc"]      = round(cum["btts_correct"] / bt, 4) if bt > 0 else 0.0
    o25t = cum["over25_total"]
    model["over25_acc"]    = round(cum["over25_correct"] / o25t, 4) if o25t > 0 else 0.0
    mt = cum["matches_total"]
    model["avg_goals"]     = round(cum["goals_total"] / mt, 3) if mt > 0 else 2.5
    model["btts_rate"]     = round(cum["btts_count"] / mt, 3) if mt > 0 else 0.50
    model["over25_rate"]   = round(cum["over25_count"] / mt, 3) if mt > 0 else 0.50

    # Rolling last-10 accuracy
    recent_q = model.setdefault("recent_q", [])
    recent_q.append((outcome_correct, n_matches))
    if len(recent_q) > 10: recent_q.pop(0)
    r10c = sum(c for c,_ in recent_q)
    r10t = sum(t for _,t in recent_q)
    model["recent_10_correct"] = r10c
    model["recent_10_total"]   = r10t
    model["recent_10_acc"]     = round(r10c/r10t, 4) if r10t > 0 else 0.0

    # ── Rolling phase tracker — last 10 rounds outcome distribution ───────────
    # Tracks HOME/DRAW/AWAY dominance so the bot knows which direction the
    # league is currently in. Self-adjusts every round automatically.
    _phase_q = model.setdefault("phase_q", [])
    _phase_q.append({"home": home_wins, "draw": draws, "away": away_wins, "n": n_matches})
    if len(_phase_q) > 10: _phase_q.pop(0)
    _ph_home  = sum(r["home"] for r in _phase_q)
    _ph_draw  = sum(r["draw"] for r in _phase_q)
    _ph_away  = sum(r["away"] for r in _phase_q)
    _ph_total = max(_ph_home + _ph_draw + _ph_away, 1)
    model["phase_home_pct"] = round(_ph_home  / _ph_total * 100)
    model["phase_draw_pct"] = round(_ph_draw  / _ph_total * 100)
    model["phase_away_pct"] = round(_ph_away  / _ph_total * 100)
    # Phase label: what direction is dominant RIGHT NOW in this league
    # Phase thresholds calibrated for virtual football distributions:
    # typical: HOME~45% DRAW~24% AWAY~31%
    # HOME phase  = ≥50% home wins  (clearly above normal)
    # AWAY phase  = ≥35% away wins  (clearly above normal for virtual)
    # DRAW phase  = ≥30% draws      (above normal)
    # NEUTRAL     = none dominant   → both HOME and AWAY allowed
    if   model["phase_home_pct"] >= 50:  model["phase_label"] = "HOME"
    elif model["phase_away_pct"] >= 35:  model["phase_label"] = "AWAY"
    elif model["phase_draw_pct"] >= 30:  model["phase_label"] = "DRAW"
    else:                                 model["phase_label"] = "NEUTRAL"


    fp_count = sum(len(v) for v in fp_db.values())

    # ── Update signal weights from accumulated signal_acc data ───────────────
    # Once any signal has ≥50 samples its accuracy drives weight adjustment.
    # More accurate signal gets more weight; less accurate gets trimmed.
    # Weights are bounded: MIN_WEIGHT(5%) to MAX_WEIGHT(70%).
    sa = model.get("signal_acc", {})
    _sa_accs = {}
    for _sig in ("odds", "poisson", "strength"):
        _rec = sa.get(_sig, {"correct": 0, "total": 0})
        if _rec["total"] >= 50:
            _sa_accs[_sig] = _rec["correct"] / _rec["total"]

    if len(_sa_accs) >= 2:
        weights = model.setdefault("weights", dict(DEFAULT_WEIGHTS))
        # Compute relative accuracy scores
        _max_acc = max(_sa_accs.values()) or 1.0
        _min_acc = min(_sa_accs.values()) or 0.0
        _spread  = _max_acc - _min_acc

        if _spread >= 0.02:   # only adjust if signals are meaningfully different
            for _sig, _acc in _sa_accs.items():
                _current = weights.get(_sig, DEFAULT_WEIGHTS.get(_sig, 0.05))
                # Shift weight toward how much better/worse than average
                _avg_acc = sum(_sa_accs.values()) / len(_sa_accs)
                _delta   = (_acc - _avg_acc) * LEARNING_RATE
                _new_w   = max(MIN_WEIGHT, min(MAX_WEIGHT, _current + _delta))
                weights[_sig] = round(_new_w, 4)
            # Re-normalise weights to sum to 1.0
            _wsum = sum(weights.get(s, 0) for s in ("odds", "poisson", "strength"))
            if _wsum > 0:
                for _sig in ("odds", "poisson", "strength"):
                    weights[_sig] = round(weights.get(_sig, 0) / _wsum, 4)
            log.info(
                f"⚖️  [{league_id}] Weights updated: "
                f"Odds={weights.get('odds',0):.0%} "
                f"Pois={weights.get('poisson',0):.0%} "
                f"Str={weights.get('strength',0):.0%}"
            )

    log.info(
        f"🧠 League {league_id} R#{model['rounds_learned']}: "
        f"1X2={model['outcome_acc']:.1%} ({cum['outcome_correct']}/{ot}) "
        f"last10={model['recent_10_acc']:.1%} "
        f"BTTS={model['btts_acc']:.1%} O25={model['over25_acc']:.1%} "
        f"| 📂 FP-DB: {len(fp_db)} fixtures / {fp_count} records"
    )

    # ── Smart fp_db dict-level eviction ───────────────────────────────────────
    # When fp_db grows beyond 600 fixture keys, evict the least useful ones.
    # "Least useful" = fixtures with the most chaotic/unstable prediction record:
    #   - Score each fixture 0.0 (worst) → 1.0 (best) on 3 criteria:
    #     1. Consistency: dominant_outcome_conf  (high = outcomes agree = stable)
    #     2. Recency: how recently the last record was added (older = lower)
    #     3. Depth: number of records (more = more experienced = higher value)
    #   - Fixtures with full records (≥10) but low consistency (<40%) go first.
    #   - Empty fixtures and very sparse fixtures (1-2 records) are evicted before
    #     fixtures with 3+ records regardless of consistency.
    # The 400 lowest-scoring fixtures are evicted until back under the cap.
    _FP_DB_CAP = 600
    if len(fp_db) > _FP_DB_CAP:
        _max_rid = max(
            (r.get("round_id", 0) for recs in fp_db.values()
             if isinstance(recs, list) for r in recs),
            default=1
        ) or 1

        def _fp_fixture_score(recs):
            if not recs:
                return 0.0
            n = len(recs)
            # Consistency: dominant outcome confidence (0.33 = random, 1.0 = always same)
            _cons = recs[-1].get("dominant_outcome_conf", 0.33) if recs else 0.33
            # Recency: 0.0 (very old) → 1.0 (most recent round seen)
            _last_rid  = max(r.get("round_id", 0) for r in recs)
            _recency   = _last_rid / _max_rid
            # Depth bonus: more records = more experience = harder to evict
            _depth     = min(1.0, n / 15)
            # Weighted composite — consistency is most important
            return 0.50 * _cons + 0.30 * _recency + 0.20 * _depth

        _scored = [(fk, _fp_fixture_score(recs)) for fk, recs in fp_db.items()]
        _scored.sort(key=lambda x: x[1])   # lowest score first = evict first
        _n_evict = len(fp_db) - _FP_DB_CAP + 100   # evict 100 extra as headroom
        _evicted = 0
        for _fk, _score in _scored[:_n_evict]:
            del fp_db[_fk]
            _evicted += 1
        model.pop("_team_idx_size", None)   # invalidate indexes after bulk delete
        model.pop("_odds_idx_size", None)
        log.info(
            f"🧹 fp_db eviction [{league_id}]: removed {_evicted} unstable/stale fixtures "
            f"({len(fp_db)} remaining) — kept highest-consistency active fixtures"
        )

    # ── Smart fixture_mem dict-level eviction ──────────────────────────────────
    # fixture_mem in ai_brain also grows unbounded. When it exceeds 600 keys,
    # evict the same way: fixtures with full memory but chaotic wrong-rate first,
    # then sparse fixtures, keeping well-performing and recently-active ones.
    _ai    = model.get("ai_brain", {})
    _fmem  = _ai.get("fixture_mem", {})
    _FM_CAP = 600
    if len(_fmem) > _FM_CAP:
        def _fm_fixture_score(mem):
            if not mem:
                return 0.0
            n = len(mem)
            recent10  = mem[-10:]
            _acc      = sum(1 for r in recent10 if r.get("correct")) / len(recent10)
            _last_rid = max(r.get("round", 0) for r in mem)
            _recency  = _last_rid / _max_rid if _max_rid else 0.0
            _depth    = min(1.0, n / 30)
            return 0.55 * _acc + 0.25 * _recency + 0.20 * _depth

        _fm_scored = [(fk, _fm_fixture_score(mem)) for fk, mem in _fmem.items()]
        _fm_scored.sort(key=lambda x: x[1])
        _fm_evict = len(_fmem) - _FM_CAP + 100
        _fm_evicted = 0
        for _fk, _score in _fm_scored[:_fm_evict]:
            del _fmem[_fk]
            _fm_evicted += 1
        log.info(
            f"🧹 fixture_mem eviction [{league_id}]: removed {_fm_evicted} low-accuracy fixtures "
            f"({len(_fmem)} remaining)"
        )


def apply_learned_model(home: str, away: str,
                          hw: float, dw: float, aw: float,
                          model: dict) -> tuple[float, float, float]:
    """Alias kept for any external calls."""
    return _apply_learned_model(home, away, hw, dw, aw, model)


def _apply_learned_model(home: str, away: str,
                          hw: float, dw: float, aw: float,
                          model: dict) -> tuple[float, float, float]:
    """
    Apply all learned adjustments to ensemble probabilities:
    1. League H/D/A base rates — Bayesian prior from all confirmed matches
    2. Fixture pattern prior (if 3+ historical meetings confirmed)
       — weight grows with sample size, caps at 45% for 10+ meetings
    3. Final normalise
    """
    cum = model.get("cumulative", {})
    mt  = cum.get("matches_total", 0)

    # 1. League H/D/A base rates — direct Bayesian prior from learned data
    # Replaces the weak home_bias multiplier with exact learned outcome frequencies.
    # Blend grows with data: 50 matches=12%, 200=22%, 500+=32%
    if mt >= 30:
        league_hw = cum.get("home_wins", 0) / mt
        league_dw = cum.get("draws",     0) / mt
        league_aw = cum.get("away_wins", 0) / mt
        # Blend ramps aggressively: 30 matches=20%, 200=35%, 500+=50%
        # After 85 rounds (~680 matches), blend ≈ 48% — the league pattern
        # is a strong anchor that the server itself is built around.
        base_blend = min(0.50, 0.20 + (mt - 30) / 1400)
        hw = (1 - base_blend) * hw + base_blend * league_hw
        dw = (1 - base_blend) * dw + base_blend * league_dw
        aw = (1 - base_blend) * aw + base_blend * league_aw
        tot = hw + dw + aw or 1.0
        hw /= tot; dw /= tot; aw /= tot

    # 2. Fixture pattern prior — more meetings = more weight
    prior = _pattern_prior(model, home, away)
    if prior:
        # Confidence ramps up: 3 meetings=15%, 5=25%, 10+=45%
        n_conf = min(1.0, (prior["n"] - 2) / 8)
        blend  = 0.15 + 0.30 * n_conf             # 15% → 45%
        hw = (1 - blend) * hw + blend * prior["hw"]
        dw = (1 - blend) * dw + blend * prior["dw"]
        aw = (1 - blend) * aw + blend * prior["aw"]
        tot = hw + dw + aw or 1.0
        hw /= tot; dw /= tot; aw /= tot

    return hw, dw, aw


async def _learning_job(context):
    """
    Runs every 6 minutes — checks if any pending round predictions have results
    available now, and if so triggers learning from those results.
    """
    bot_data = context.bot_data

    # ── One-time migration: clear band calibration data that was poisoned by
    # the bootstrap bug (all bootstrap predictions had conf=50.0, so margin_acc["50"]
    # accumulated ~1700 fake entries at various leagues). This runs once and then
    # sets a flag so it never clears again. The bands will rebuild from live rounds.
    if not bot_data.get("_band_data_v2_cleared"):
        log.info("⚙️  One-time migration: clearing poisoned band calibration data "
                 "(margin_acc / btts_band_acc / o25_band_acc) — will rebuild from live rounds")
        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            m = bot_data.get(f"model_{lid}")
            if isinstance(m, dict):
                m.pop("margin_acc",     None)
                m.pop("btts_band_acc",  None)
                m.pop("o25_band_acc",   None)
                m.pop("high_conf_mistakes", None)
                m.pop("conf_calibration",   None)
                log.info(f"   Cleared band data for league {lid}")
        bot_data["_band_data_v2_cleared"] = True

    # ── One-time migration: trim fp_db records to 15 per fixture ───────────────
    # Old cap was 50 records per fixture — far more than needed and the main cause
    # of prediction lag (full scans of 230k+ objects every 30 seconds).
    # Trim to the 15 most recent records per fixture.  Runs once per deploy.
    if not bot_data.get("_fpdb_trimmed_v1"):
        log.info("⚙️  One-time migration: trimming fp_db to 15 records per fixture")
        total_removed = 0
        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            m = bot_data.get(f"model_{lid}")
            if not isinstance(m, dict): continue
            fp_db = m.get("fingerprint_db", {})
            removed = 0
            for fk in fp_db:
                recs = fp_db[fk]
                if isinstance(recs, list) and len(recs) > 15:
                    removed      += len(recs) - 15
                    fp_db[fk]     = recs[-15:]   # keep the 15 most recent
            total_removed += removed
            if removed:
                log.info(f"   League {lid}: trimmed {removed} excess records")
        bot_data["_fpdb_trimmed_v1"] = True
        log.info(f"✅  fp_db trim complete — {total_removed} records removed")

    # ── One-time migration: purge cross-contaminated fp_db records ──────────────
    # Before the LEAGUE_TEAMS.copy() fix, _filter_league mutated the shared set,
    # causing foreign teams (e.g. Portuguese PAR/TON/BEN) to accumulate in every
    # league's fingerprint_db. This purge removes any record whose teams are not
    # in that league's LEAGUE_TEAMS whitelist. Runs once per deploy.
    if not bot_data.get("_fpdb_decontaminated_v1"):
        log.info("⚙️  One-time migration: purging cross-contaminated fp_db records")
        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            m = bot_data.get(f"model_{lid}")
            if not isinstance(m, dict):
                continue
            fp_db = m.get("fingerprint_db", {})
            if not fp_db:
                continue
            codes = LEAGUE_TEAMS.get(lid, set())
            if not codes:
                continue
            purged_keys = 0
            purged_recs = 0
            keys_to_delete = []
            for fk, records in fp_db.items():
                parts = fk.split("|")
                if len(parts) != 2:
                    continue
                h3, a3 = parts[0].upper()[:3], parts[1].upper()[:3]
                h_full, a_full = parts[0].upper(), parts[1].upper()
                home_ok = (h_full in codes or h3 in codes)
                away_ok = (a_full in codes or a3 in codes)
                if not (home_ok and away_ok):
                    purged_keys += 1
                    purged_recs += len(records) if isinstance(records, list) else 0
                    keys_to_delete.append(fk)
            for fk in keys_to_delete:
                del fp_db[fk]
            if purged_keys:
                log.info(f"   League {lid}: removed {purged_keys} foreign fixtures ({purged_recs} records)")
            else:
                log.info(f"   League {lid}: fp_db clean — no foreign fixtures found")
        bot_data["_fpdb_decontaminated_v1"] = True
        log.info("✅  fp_db decontamination complete")

    # ── One-time migration v2: reset LEAGUE_TEAMS to seed whitelists ─────────────
    # The auto-learn threshold bug (too-low _min_clean, no unknown-ratio cap) allowed
    # the shared 66-event round pool to contaminate every league's LEAGUE_TEAMS set
    # with foreign teams. Reset each league's live whitelist back to its seed set so
    # _filter_league starts clean. The improved threshold in the filter prevents
    # re-contamination going forward.
    if not bot_data.get("_league_teams_reset_v2"):
        # Reset the module-level LEAGUE_TEAMS dict (it is the live global)
        # Seed whitelists — same as module-level LEAGUE_TEAMS definition
        _SEED: dict[int, set[str]] = {
            7794: {"AST","ARS","BOU","BRE","BUR","CHE","CRY","EVE","FUL","LIV","MCI","MUN","NEW","NOT","TOT","WHU","WOL","BHA","SUN","LEE"},
            7795: {"ALA","ATH","ATM","BAR","BET","CEL","ELC","ESP","GET","GIR","LEV","MAL","OSA","OVI","RAY","RMA","RSO","SEV","VAL","VIL"},
            7796: {"ATA","BOL","CAG","COM","CRE","FIO","GEN","INT","JUV","LAZ","LEC","MIL","NAP","PAR","PIS","ROM","SAS","TOR","UDI","VER"},
            9183: {"ANG","ASM","AUX","BRE","HAV","LEN","LIL","LOR","LYO","MAR","MET","NAN","NIC","PAR","PSG","REN","STR","TOU"},
            9184: {"AJA","AZA","EXC","FEY","FOR","GAE","HEE","HER","NAC","NEC","PEC","PSV","SPA","TEL","TWE","UTR","VOL","GRO"},
            13773: {"AUG","COL","DOR","EIN","FCB","FRE","HEI","HOF","HSV","LEV","MAI","MON","RBL","STP","STU","UNI","WER","WOL"},
            13774: {"ALV","ARO","AVS","BEN","BRA","CAS","EST","ETA","FAM","GIL","GUI","MOR","NAC","POR","RIO","SAN","SPO","TON"},
        }
        for _lid, _seed in _SEED.items():
            _before = len(LEAGUE_TEAMS.get(_lid, set()))
            LEAGUE_TEAMS[_lid] = set(_seed)  # reset to clean seed
            _after = len(LEAGUE_TEAMS[_lid])
            if _before != _after:
                log.info(f"🧹 LEAGUE_TEAMS reset [{_lid}]: {_before} → {_after} teams (removed {_before-_after} foreign)")
            else:
                log.info(f"✅ LEAGUE_TEAMS [{_lid}]: already clean ({_after} teams)")
        bot_data["_league_teams_reset_v2"] = True
        log.info("✅  LEAGUE_TEAMS reset to seed whitelists complete")

    # ── One-time migration v3/v4: wipe corrupted data, force clean API rebuild ──
    # The old fp_db accumulated cross-league contamination (all 7 leagues shared
    # the same round_id, so England's fp_db received LIV results 7× per round).
    # The v3 bootstrap would inherit this corruption. Instead: wipe fp_db and
    # match_log for ALL leagues and let the data collector rebuild cleanly from
    # the API. The /standings command will show "building..." until enough rounds
    # have been collected. This runs once per deploy of this fix.
    if not bot_data.get("_clean_rebuild_v4"):
        log.info("⚙️  Migration v4: wiping corrupted fp_db + match_log, forcing clean rebuild")
        wiped_leagues = 0
        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            m = bot_data.get(f"model_{lid}")
            if not isinstance(m, dict):
                continue
            old_fp_size = len(m.get("fingerprint_db", {}))
            old_ml_size = len(m.get("match_log", []))
            # Wipe the contaminated data
            m["fingerprint_db"] = {}
            m["match_log"]      = []
            m["_ml_seen"]       = {}
            m.pop("_standings_cache_key", None)
            m.pop("_cached_standings", None)
            m.pop("_team_idx_size", None)
            m.pop("_odds_idx_size", None)
            # Reset cumulative stats too — they were overcounted
            m["cumulative"] = {
                "outcome_correct": 0, "outcome_total": 0,
                "btts_correct": 0,   "btts_total": 0,
                "over25_correct": 0, "over25_total": 0,
                "goals_total": 0,    "matches_total": 0,
                "home_wins": 0,      "draws": 0, "away_wins": 0,
                "conf_sum_correct": 0, "conf_sum_wrong": 0,
                "n_correct": 0, "n_wrong": 0,
                "btts_count": 0, "over25_count": 0,
            }
            m["rounds_learned"] = 0
            # Also wipe cached standings in bot_data
            bot_data.pop(f"standings_{lid}", None)
            wiped_leagues += 1
            log.info(f"   [{lid}] wiped fp_db({old_fp_size} fixtures) + match_log({old_ml_size} entries)")
        # Force backfill to run again from scratch
        bot_data["_collected_map"] = {}
        bot_data.pop("_fpdb_decontaminated_v1", None)
        bot_data.pop("_match_log_built_v3", None)
        bot_data["_clean_rebuild_v4"] = True
        log.info(f"✅  Migration v4 complete — {wiped_leagues} leagues wiped, backfill will rebuild from API")

    # ── One-time migration v5: (legacy — already ran, preserved as no-op) ────────
    # Previously stripped season_id from match_log. That caused per-season standings
    # to show "No data" for any specific season. Migration v6 below rebuilds properly.
    if not bot_data.get("_ml_season_fixed_v5"):
        bot_data["_ml_season_fixed_v5"] = True
        log.info("✅  Migration v5 skipped (superseded by v6)")

    # ── One-time migration v6: rebuild season_id on match_log from fp_db records ──
    # v5 stripped all season_ids, breaking per-season standings views (showed
    # "No data for England — Season #XXXXX"). v6 attempts to restore season_id
    # on each match_log entry by cross-referencing the round_id with fp_db records
    # that DO have a season_id stored. Entries that can't be matched stay as ""
    # (which means "include in all views" — safe fallback).
    if not bot_data.get("_ml_season_fixed_v6"):
        restored_leagues = 0
        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            m = bot_data.get(f"model_{lid}")
            if not isinstance(m, dict):
                continue
            ml = m.get("match_log", [])
            fp_db_v6 = m.get("fingerprint_db", {})
            if not ml:
                continue
            # Build round_id → season_id mapping from fp_db
            rid_to_season: dict[int, str] = {}
            for recs in fp_db_v6.values():
                if not isinstance(recs, list):
                    continue
                for rec in recs:
                    rid = rec.get("round_id")
                    sid = str(rec.get("season_id") or "")
                    if rid and sid:
                        rid_to_season[int(rid)] = sid
            restored = 0
            for entry in ml:
                if not entry.get("season_id"):
                    rid = entry.get("round_id")
                    if rid and int(rid) in rid_to_season:
                        entry["season_id"] = rid_to_season[int(rid)]
                        restored += 1
            restored_leagues += 1
            log.info(f"   [{lid}] v6: restored season_id on {restored}/{len(ml)} match_log entries")
        bot_data["_ml_season_fixed_v6"] = True
        log.info(f"✅  Migration v6 complete — season_ids restored in {restored_leagues} leagues")

    # ── One-time migration v7: purge bootstrap-polluted fp_db and match_log ───
    # Bootstrap was saving synthetic records (round_id=0, season_id="") into
    # fingerprint_db and match_log. These pollute the signal pool with fake
    # data that has no real odds, causing:
    #   - fp_db records with empty odds_snapshot → votes carry no quality weight
    #   - match_log entries with round_id=0 → form calculations show wrong history
    #   - "0 fixtures with 3+ meetings" in brain report (all 1x per fixture)
    # Fix: remove all fp_db records AND match_log entries where round_id=0
    if not bot_data.get("_ml_bootstrap_purge_v7"):
        purged_leagues = 0
        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            m = bot_data.get(f"model_{lid}")
            if not isinstance(m, dict):
                continue
            # Purge fp_db records with round_id=0
            fp_db_v7 = m.get("fingerprint_db", {})
            fp_purged = 0
            for fk in list(fp_db_v7.keys()):
                recs = fp_db_v7[fk]
                if not isinstance(recs, list):
                    continue
                clean = [r for r in recs if r.get("round_id", 0) != 0]
                fp_purged += len(recs) - len(clean)
                if clean:
                    fp_db_v7[fk] = clean
                else:
                    del fp_db_v7[fk]
            # Purge match_log entries with round_id=0
            ml_v7 = m.get("match_log", [])
            ml_clean = [e for e in ml_v7 if e.get("round_id", 0) != 0]
            ml_purged = len(ml_v7) - len(ml_clean)
            if ml_purged > 0:
                m["match_log"] = ml_clean
                # Rebuild _ml_seen
                m["_ml_seen"] = {
                    (r["home"], r["away"], r["round_id"], r["score_h"], r["score_a"]): 1
                    for r in ml_clean
                }
            # Reset _bootstrap_rounds counter so cold-start gate recalculates
            m["_bootstrap_rounds"] = m.get("rounds_learned", 0)
            if fp_purged > 0 or ml_purged > 0:
                purged_leagues += 1
                log.info(f"   [{lid}] v7: purged {fp_purged} fp_db + {ml_purged} match_log bootstrap records")
        bot_data["_ml_bootstrap_purge_v7"] = True
        log.info(f"✅  Migration v7 complete — bootstrap records purged from {purged_leagues} leagues")

    pending  = bot_data.get("pending_predictions", {})
    if not pending:
        return

    # Build a round_id → season_id lookup from recent API data so the learning
    # job can stamp correct season_ids even on pending entries that were queued
    # before the season_id was known (e.g. queued as "" during startup).
    _season_lookup: dict[str, str] = {}   # str(round_id) → season_id
    try:
        async with httpx.AsyncClient() as _sl_client:
            for _sl_lid in LEAGUES:
                try:
                    for _sl_past in (False, True):
                        _sl_rounds = await fetch_round_list(_sl_client, _sl_lid, past=_sl_past)
                        for _sl_r in _sl_rounds:
                            _sl_rid = str(_sl_r.get("id") or _sl_r.get("gameRoundId") or "")
                            _sl_sid = str(_sl_r.get("_seasonId") or "")
                            if _sl_rid and _sl_sid:
                                _season_lookup[_sl_rid] = _sl_sid
                except Exception:
                    pass
    except Exception:
        pass

    async with httpx.AsyncClient() as client:
        for league_id_str, rounds in list(pending.items()):
            league_id = int(league_id_str)
            for round_id, pred_entry in list(rounds.items()):
                try:
                    # Support both old format (list) and new format (dict with preds+season_id)
                    if isinstance(pred_entry, dict) and "preds" in pred_entry:
                        preds     = pred_entry["preds"]
                        _p_season = pred_entry.get("season_id", "")
                    else:
                        preds     = pred_entry   # legacy: plain list
                        _p_season = ""
                    # If season_id was not stored, recover it from the API lookup
                    if not _p_season:
                        _p_season = _season_lookup.get(str(round_id), "")
                        if _p_season and isinstance(pred_entry, dict):
                            pred_entry["season_id"] = _p_season   # persist for next time
                    # Fetch results for this round
                    events = await fetch_round_events(client, round_id, PAGE_MATCHUPS)
                    events = _filter_league(events, league_id)
                    scored = [e for e in events if _extract_score(e)[0] is not None]
                    if len(scored) < len(preds) * 0.5:
                        continue   # results not in yet

                    results = []
                    for e in scored:
                        m    = _norm_event(e)
                        ht_h, ht_a = _extract_ht_score(e)
                        if m["hs"] is not None:
                            results.append({
                                "home": m["home"], "away": m["away"],
                                "actual_h": m["hs"], "actual_a": m["as_"],
                                "ht_h": ht_h, "ht_a": ht_a,
                            })

                    _learn_from_round(bot_data, league_id, preds, results,
                                       round_id=int(round_id) if round_id else 0,
                                       season_id=_p_season)
                    # Always keep current season ID up to date in model
                    if _p_season:
                        _get_model(bot_data, league_id)["_current_season_id"] = str(_p_season)
                    _learn_algo_signals(_get_model(bot_data, league_id), preds, results,
                                        round_id_int=int(round_id) if round_id else 0)
                    _ai_postmatch_analysis(
                        _get_model(bot_data, league_id), preds, results,
                        standings=bot_data.get(f"standings_{league_id}"),
                        round_id=int(round_id) if round_id else 0,
                    )

                    # Remove from pending after learning
                    del rounds[round_id]
                    log.info(f"🎓 Learned from round {round_id} (league {league_id})")

                    # Flush persistence immediately so a redeploy won't lose this round
                    if context.application.persistence:
                        await context.application.persistence.flush()

                except Exception as e:
                    log.warning(f"Learning job error lid={league_id} rid={round_id}: {e}")

            if not rounds:
                del pending[league_id_str]

    # ── Stale pending cleanup — remove rounds that are impossibly old ─────────
    # If a round has been in pending for more than 48 hours with no results,
    # it will never resolve (season ended, API no longer serves it). Drop it
    # so pending doesn't accumulate indefinitely and block new-season learning.
    # We detect "old" by comparing round_id integers — rounds advance monotonically.
    # Find the highest round_id across all leagues from the current API lists,
    # then drop any pending entry whose round_id is more than 500 behind.
    try:
        _all_pending_rids = []
        for _league_rounds in bot_data.get("pending_predictions", {}).values():
            for _pr in _league_rounds:
                try:
                    _all_pending_rids.append(int(_pr))
                except Exception:
                    pass
        # Also get latest known round_id from match_log across all leagues
        _latest_rid = 0
        for _cl_lid in LEAGUES:
            _cl_m  = bot_data.get(f"model_{_cl_lid}", {})
            _cl_ml = _cl_m.get("match_log", [])
            for _cl_e in _cl_ml[-50:]:
                try:
                    _latest_rid = max(_latest_rid, int(_cl_e.get("round_id", 0)))
                except Exception:
                    pass
        if _latest_rid > 0:
            _stale_threshold = _latest_rid - 500
            _stale_dropped   = 0
            for _sp_lid_str, _sp_rounds in list(bot_data.get("pending_predictions", {}).items()):
                for _sp_rid in list(_sp_rounds.keys()):
                    try:
                        if int(_sp_rid) < _stale_threshold:
                            del _sp_rounds[_sp_rid]
                            _stale_dropped += 1
                    except Exception:
                        pass
                if not _sp_rounds:
                    bot_data["pending_predictions"].pop(_sp_lid_str, None)
            if _stale_dropped:
                log.info(f"🧹 Stale pending cleanup: dropped {_stale_dropped} unresolvable rounds "
                         f"(threshold rid < {_stale_threshold})")
    except Exception as _sp_ex:
        log.warning(f"Stale pending cleanup error: {_sp_ex}")

# ─── FORMAT HELPERS ───────────────────────────────────────────────────────────
SEP = "━" * 26

def _form_str(r: list) -> str:
    return " ".join({"W":"🟢","D":"🟡","L":"🔴"}.get(x,"⚪") for x in r) or "—"

def _bar(v: float, w: int = 5) -> str:
    n = max(0, min(w, int(round(v/100*w))))
    return "█"*n + "░"*(w-n)

def _chunks(text: str, limit: int = 4000) -> list[str]:
    if len(text) <= limit:
        return [text]
    parts, buf = [], []
    for line in text.split("\n"):
        if sum(len(l)+1 for l in buf)+len(line) > limit:
            parts.append("\n".join(buf)); buf = []
        buf.append(line)
    if buf:
        parts.append("\n".join(buf))
    return parts

async def _send(target, text: str, **kw):
    for chunk in _chunks(text):
        await target.reply_text(chunk, **kw)

# ─── LEAGUE KEYBOARD ──────────────────────────────────────────────────────────
def league_keyboard(prefix: str) -> InlineKeyboardMarkup:
    buttons, row = [], []
    for lid, info in LEAGUES.items():
        row.append(InlineKeyboardButton(
            text=f"{info['flag']} {info['name']}",
            callback_data=f"{prefix}:{lid}",
        ))
        if len(row) == 2:
            buttons.append(row); row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)

# ─── /start ───────────────────────────────────────────────────────────────────
async def cmd_start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid     = u.effective_user.id
    chat_id = u.effective_chat.id

    # ── Admin menu ────────────────────────────────────────────────────────────
    if _is_admin(uid):
        chats = c.bot_data.setdefault("auto_chats", set())
        chats.add(str(chat_id))
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📡 Raw Status",     callback_data="menu:rawstatus"),
                InlineKeyboardButton("🧠 Brain Stats",    callback_data="menu:brainstat"),
            ],
            [
                InlineKeyboardButton("➕ Add User",       callback_data="menu:adduser_prompt"),
                InlineKeyboardButton("➖ Remove User",    callback_data="menu:removeuser_prompt"),
            ],
            [
                InlineKeyboardButton("📢 Add Channel",    callback_data="menu:addchannel_prompt"),
                InlineKeyboardButton("🗑 Remove Channel", callback_data="menu:removechannel_prompt"),
            ],
            [
                InlineKeyboardButton("💾 Backup Data",    callback_data="menu:backup"),
                InlineKeyboardButton("☁️ Fetch Brain",    callback_data="menu:fetchbrain"),
            ],
            [
                InlineKeyboardButton("📊 Standings",      callback_data="menu:standings"),
            ],
        ])
        await u.message.reply_text(
            "👑 *Admin — BetPawa Bot Active*\n\nTap a button below to take action:",
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        await _run_auto_post(c.bot, c.bot_data)
        return

    # ── Authorized user menu ──────────────────────────────────────────────────
    if _is_authorized_user(uid, c.bot_data):
        rem   = _remaining_days(uid, c.bot_data)
        chats = c.bot_data.setdefault("auto_chats", set())
        chats.add(str(chat_id))
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🪪 My Status", callback_data="menu:mystatus")],
        ])
        await u.message.reply_text(
            f"✅ *BetPawa Auto Picks Active*\n_{rem:.1f} days remaining_\n\nPicks are sent here automatically every new matchday.",
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        await _run_auto_post(c.bot, c.bot_data)
        return

    # ── Expired ───────────────────────────────────────────────────────────────
    contact_btn = InlineKeyboardMarkup([[
        InlineKeyboardButton("💬 Contact Admin", url="https://t.me/MrSimTech")
    ]])
    acc = _access(c.bot_data)
    if str(uid) in acc["users"]:
        await u.message.reply_text(
            "⏰ *Your access has expired.*\n\nContact the admin to renew your subscription.",
            parse_mode="Markdown",
            reply_markup=contact_btn,
        )
        return

    await u.message.reply_text(
        "🔒 *Access Restricted*\n\nThis bot is private. Contact the admin to get access.",
        parse_mode="Markdown",
        reply_markup=contact_btn,
    )


async def cb_menu(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Handle all inline menu button taps."""
    query  = u.callback_query
    await query.answer()
    uid    = query.from_user.id
    action = query.data.split(":", 1)[1]

    is_admin = _is_admin(uid)
    is_user  = _is_authorized_user(uid, c.bot_data)

    if not (is_admin or is_user):
        await query.message.reply_text("🔒 Access restricted.")
        return

    # Admin-only actions
    if action == "showstatus":
        if not is_admin: return
        await _do_showstatus(query.message, c)

    elif action == "rawstatus":
        if not is_admin: return
        await _do_rawstatus(query.message, c)

    elif action == "brainstat":
        if not is_admin: return
        await _do_brainstat(query.message, c)

    elif action == "adduser_prompt":
        if not is_admin: return
        await query.message.reply_text(
            "👤 *Add User*\n\nSend: `/adduser <user_id>`\nExample: `/adduser 123456789`",
            parse_mode="Markdown",
        )

    elif action == "removeuser_prompt":
        if not is_admin: return
        await query.message.reply_text(
            "👤 *Remove User*\n\nSend: `/removeuser <user_id>`\nExample: `/removeuser 123456789`",
            parse_mode="Markdown",
        )

    elif action == "addchannel_prompt":
        if not is_admin: return
        await query.message.reply_text(
            "📢 *Add Channel*\n\nSend: `/addchannel <channel_id>`\nExample: `/addchannel -1001234567890`",
            parse_mode="Markdown",
        )

    elif action == "removechannel_prompt":
        if not is_admin: return
        await query.message.reply_text(
            "🗑 *Remove Channel*\n\nSend: `/removechannel <channel_id>`\nExample: `/removechannel -1001234567890`",
            parse_mode="Markdown",
        )

    elif action == "backup":
        if not is_admin: return
        await query.answer()   # acknowledge immediately — backup can take several seconds
        await cmd_backup_from_message(query.message, c)

    elif action == "fetchbrain":
        if not is_admin: return
        await query.answer()
        file_id = c.bot_data.get("storage_file_id")
        if file_id:
            brain_fids = c.bot_data.get("storage_brain_file_ids", [])
            if not brain_fids:
                legacy = c.bot_data.get("storage_brain_file_id")
                if legacy: brain_fids = [legacy]
            n_chunks = len(brain_fids)
            wait = await query.message.reply_text(
                f"⏳ Restoring from backup ({n_chunks} brain chunk{'s' if n_chunks>1 else ''})…"
            )
            data = await _load_backup_from_file_id(file_id, c.bot, c.bot_data)
            if data:
                chunks_loaded = 0
                for fid in brain_fids:
                    chunk_brain = await _load_brain_from_file_id(fid, c.bot)
                    if chunk_brain:
                        _merge_brain_into_data(data, chunk_brain)
                        chunks_loaded += 1
                await wait.delete()
                if chunks_loaded:
                    await query.message.reply_text(
                        f"🧠 Merged {chunks_loaded}/{n_chunks} brain chunk(s) into restore.",
                        parse_mode="Markdown"
                    )
                await _apply_backup_to_bot(data, query.message, c.bot_data)
            else:
                await wait.edit_text("⚠️ Stored file expired. Send or forward the backup .txt file here and I will load it automatically.")
        else:
            await query.message.reply_text(
                "📂 *No backup in memory.*\n\nJust *send or forward* the `vsbot_backup_*.txt` file here — I will detect and load it automatically.",
                parse_mode="Markdown"
            )

    elif action == "standings":
        if not is_admin: return
        await query.edit_message_text(
            "📊 *Standings — Select League*",
            parse_mode="Markdown",
            reply_markup=league_keyboard("standings"),
        )

    elif action == "stop":
        chats = c.bot_data.get("auto_chats", set())
        chats.discard(str(query.message.chat_id))
        await query.message.reply_text("⛔ *Stopped.* You will no longer receive picks here.", parse_mode="Markdown")

    elif action == "mystatus":
        await _do_mystatus(query.message, uid, c)

    # Shared actions — open league picker
    elif action in ("predict", "results", "live", "upcoming", "standings", "compare"):
        label_map = {
            "predict":   "🔮 Predictions",
            "results":   "📋 Results",
            "live":      "📡 Live Scores",
            "upcoming":  "📅 Upcoming",
            "standings": "🏆 Standings",
            "compare":   "🆚 Compare",
        }
        # Map to the callback prefix used by existing handlers
        prefix_map = {
            "predict":   "predict_cb",
            "results":   "results",
            "live":      "live",
            "upcoming":  "upcoming",
            "standings": "standings",
            "compare":   "compare_cb",
        }
        kb = league_keyboard(prefix_map[action])
        await query.message.reply_text(
            f"{label_map[action]} — choose a league:",
            reply_markup=kb,
        )

# ─── /stop ────────────────────────────────────────────────────────────────────
async def cmd_stop(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid     = u.effective_user.id
    chat_id = str(u.effective_chat.id)
    if not (_is_admin(uid) or _is_authorized_user(uid, c.bot_data)):
        await u.message.reply_text("🔒 Access restricted.", parse_mode="Markdown")
        return
    chats = c.bot_data.get("auto_chats", set())
    chats.discard(chat_id)
    await u.message.reply_text("⛔ *Stopped.* You will no longer receive picks here.", parse_mode="Markdown")

# ─── /mystatus ────────────────────────────────────────────────────────────────
async def _do_mystatus(message, uid: int, c):
    """Core mystatus logic — works from both command and button tap."""
    if _is_admin(uid):
        await message.reply_text("👑 You are the *admin* — unlimited access.", parse_mode="Markdown")
        return
    rem = _remaining_days(uid, c.bot_data)
    if rem is None:
        await message.reply_text("❌ You have no active subscription.", parse_mode="Markdown")
        return
    acc   = _access(c.bot_data)
    entry = acc["users"][str(uid)]
    exp_dt  = datetime.datetime.fromtimestamp(entry["expire_ts"], datetime.timezone.utc)
    exp_str = exp_dt.strftime("%Y-%m-%d %H:%M UTC")
    if rem <= 0:
        await message.reply_text(
            "⏰ *Your access has expired.*\n\nContact the admin to renew.",
            parse_mode="Markdown",
        )
    else:
        bar = _bar(rem / entry["days"] * 100, 10)
        await message.reply_text(
            f"📅 *Your Subscription Status*\n\n"
            f"⏳ Days remaining: *{rem:.1f}*\n"
            f"📆 Expires: `{exp_str}`\n"
            f"Progress: `{bar}`",
            parse_mode="Markdown",
        )


async def cmd_mystatus(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _do_mystatus(u.message, u.effective_user.id, c)

# ─── /addchannel ──────────────────────────────────────────────────────────────
async def cmd_addchannel(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.", parse_mode="Markdown")
        return
    if not c.args:
        await u.message.reply_text("Usage: `/addchannel <channel_id>`\n\nE.g. `/addchannel -1001234567890`", parse_mode="Markdown")
        return
    cid = c.args[0].strip()
    acc = _access(c.bot_data)
    acc["allowed_channels"].add(cid)
    # Also register it as an auto-post target
    chats = c.bot_data.setdefault("auto_chats", set())
    chats.add(cid)
    await u.message.reply_text(f"✅ Channel `{cid}` added. Picks will be posted there.", parse_mode="Markdown")

# ─── /removechannel ───────────────────────────────────────────────────────────
async def cmd_removechannel(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.", parse_mode="Markdown")
        return
    if not c.args:
        await u.message.reply_text("Usage: `/removechannel <channel_id>`", parse_mode="Markdown")
        return
    cid = str(c.args[0].strip())
    acc = _access(c.bot_data)
    was_in_channels = cid in acc["allowed_channels"]
    acc["allowed_channels"].discard(cid)
    chats = c.bot_data.get("auto_chats", set())
    chats.discard(cid)
    if was_in_channels:
        await u.message.reply_text(f"🗑 Channel `{cid}` removed.", parse_mode="Markdown")
    else:
        registered = sorted(acc["allowed_channels"]) or ["none"]
        msg = f"\u26a0\ufe0f `{cid}` was not in Registered Channels.\nCurrently registered: {registered}"
        await u.message.reply_text(msg, parse_mode="Markdown")

# ─── /adduser ─────────────────────────────────────────────────────────────────

# ─── /resetdata ───────────────────────────────────────────────────────────────
async def cmd_resetdata(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Admin-only: wipe all fp_db + match_log data and force clean API rebuild.
    Use when standings are corrupted (e.g. after cross-league contamination)."""
    if not _is_admin(u.effective_user.id):
        return
    args = (u.message.text or "").split()
    # Require confirmation: /resetdata CONFIRM
    if len(args) < 2 or args[1] != "CONFIRM":
        await u.message.reply_text(
            "⚠️ *This will wipe ALL learned data and rebuild from scratch.*\n\n"
            "Standings, fingerprint DB, and match history will be cleared.\n"
            "Data will rebuild over the next few minutes via backfill.\n\n"
            "To confirm: `/resetdata CONFIRM`",
            parse_mode="Markdown"
        )
        return

    wiped = []
    for lid in LEAGUES:
        if lid not in ACTIVE_LEAGUES: continue
        m = c.bot_data.get(f"model_{lid}")
        if not isinstance(m, dict):
            continue
        m["fingerprint_db"] = {}
        m["match_log"]      = []
        m["_ml_seen"]       = {}
        m.pop("_standings_cache_key", None)
        m.pop("_cached_standings", None)
        m.pop("_team_idx_size", None)
        m.pop("_odds_idx_size", None)
        m["cumulative"] = {
            "outcome_correct": 0, "outcome_total": 0,
            "btts_correct": 0,   "btts_total": 0,
            "over25_correct": 0, "over25_total": 0,
            "goals_total": 0,    "matches_total": 0,
            "home_wins": 0,      "draws": 0, "away_wins": 0,
            "conf_sum_correct": 0, "conf_sum_wrong": 0,
            "n_correct": 0, "n_wrong": 0,
            "btts_count": 0, "over25_count": 0,
        }
        m["rounds_learned"] = 0
        c.bot_data.pop(f"standings_{lid}", None)
        wiped.append(LEAGUES[lid]["name"])

    c.bot_data["_collected_map"] = {}
    c.bot_data.pop("_fpdb_decontaminated_v1", None)
    c.bot_data.pop("_match_log_built_v3", None)
    c.bot_data.pop("_clean_rebuild_v4", None)   # allow v4 to re-run
    c.bot_data["pending_predictions"] = {}

    log.info(f"🗑️ /resetdata: wiped {len(wiped)} leagues by admin {u.effective_user.id}")
    await u.message.reply_text(
        f"✅ *Data reset complete.*\n\n"
        f"Wiped leagues: {', '.join(wiped)}\n\n"
        f"The bot will now backfill the last 50 rounds from the API automatically. "
        f"Standings will update within ~2 minutes.",
        parse_mode="Markdown"
    )


async def cmd_adduser(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.", parse_mode="Markdown")
        return
    if not c.args:
        await u.message.reply_text("Usage: `/adduser <user_id>`\n\nThen choose how many days of access.", parse_mode="Markdown")
        return
    target_uid = c.args[0].strip()
    # Store pending and ask for days
    acc = _access(c.bot_data)
    admin_chat  = str(u.effective_chat.id)
    acc["pending_user"][admin_chat] = target_uid

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1 day",  callback_data=f"adduser_days:{target_uid}:1"),
            InlineKeyboardButton("3 days", callback_data=f"adduser_days:{target_uid}:3"),
            InlineKeyboardButton("7 days", callback_data=f"adduser_days:{target_uid}:7"),
        ],
        [
            InlineKeyboardButton("14 days", callback_data=f"adduser_days:{target_uid}:14"),
            InlineKeyboardButton("30 days", callback_data=f"adduser_days:{target_uid}:30"),
            InlineKeyboardButton("60 days", callback_data=f"adduser_days:{target_uid}:60"),
        ],
        [
            InlineKeyboardButton("90 days",  callback_data=f"adduser_days:{target_uid}:90"),
            InlineKeyboardButton("180 days", callback_data=f"adduser_days:{target_uid}:180"),
            InlineKeyboardButton("365 days", callback_data=f"adduser_days:{target_uid}:365"),
        ],
    ])
    await u.message.reply_text(
        f"👤 Adding user `{target_uid}`\n\n"
        f"Select the number of access days, then press *Continue*:",
        parse_mode="Markdown",
        reply_markup=keyboard,
    )

async def cb_adduser_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not _is_admin(query.from_user.id):
        await query.edit_message_text("🔒 Admin only.")
        return
    # data format: adduser_days:<uid>:<days>
    parts = query.data.split(":")
    if len(parts) != 3:
        return
    _, target_uid, days_str = parts
    days = int(days_str)

    # Fetch time from server
    now_dt = await _fetch_utc_now()
    now_ts = now_dt.timestamp()
    expire_ts = now_ts + days * 86400
    expire_dt = datetime.datetime.fromtimestamp(expire_ts, datetime.timezone.utc)
    exp_str   = expire_dt.strftime("%Y-%m-%d %H:%M UTC")

    acc = _access(context.bot_data)
    acc["users"][target_uid] = {
        "expire_ts": expire_ts,
        "days":      days,
        "added_ts":  now_ts,
    }

    # Clean pending
    for k, v in list(acc["pending_user"].items()):
        if v == target_uid:
            del acc["pending_user"][k]

    await query.edit_message_text(
        f"✅ *User `{target_uid}` added!*\n\n"
        f"📅 Access: *{days} days*\n"
        f"⏰ Starts now (server time: `{now_dt.strftime('%Y-%m-%d %H:%M UTC')}`)\n"
        f"📆 Expires: `{exp_str}`\n\n"
        f"Tell the user to send `/start` to the bot.",
        parse_mode="Markdown",
    )

# ─── /removeuser ──────────────────────────────────────────────────────────────
async def cmd_removeuser(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.", parse_mode="Markdown")
        return
    if not c.args:
        await u.message.reply_text("Usage: `/removeuser <user_id>`", parse_mode="Markdown")
        return
    target_uid = c.args[0].strip()
    acc = _access(c.bot_data)
    if target_uid in acc["users"]:
        del acc["users"][target_uid]
        await u.message.reply_text(f"🗑️ User `{target_uid}` removed.", parse_mode="Markdown")
    else:
        await u.message.reply_text(f"❌ User `{target_uid}` not found.", parse_mode="Markdown")

# ─── /showstatus ──────────────────────────────────────────────────────────────
async def _do_showstatus(message, c):
    acc    = _access(c.bot_data)
    now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
    ch_lines = [f"  📢 `{cid}`" for cid in sorted(acc["allowed_channels"])]
    ch_block = "\n".join(ch_lines) if ch_lines else "  _None added_"
    user_lines = []
    for uid, entry in sorted(acc["users"].items()):
        secs_left = entry["expire_ts"] - now_ts
        if secs_left <= 0:
            status = "❌ EXPIRED"; days_left = "0"
        else:
            status = "✅ Active"; days_left = f"{secs_left/86400:.1f}d left"
        exp_str = datetime.datetime.fromtimestamp(
            entry["expire_ts"], datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        user_lines.append(f"  👤 `{uid}` — {status} — {days_left}\n     Expires: `{exp_str}`")
    user_block  = "\n".join(user_lines) if user_lines else "  _No users_"
    auto_chats  = c.bot_data.get("auto_chats", set())
    chat_block  = "\n".join(f"  💬 `{cid}`" for cid in sorted(auto_chats)) or "  _None_"
    text = (f"📊 *Bot Status Overview*\n{SEP}\n\n"
            f"📢 *Registered Channels/Groups:*\n{ch_block}\n\n"
            f"👤 *Authorized Users:*\n{user_block}\n\n"
            f"💬 *Active Auto-Post Chats:*\n{chat_block}")
    await message.reply_text(text, parse_mode="Markdown")

async def cmd_showstatus(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.", parse_mode="Markdown")
        return
    await _do_showstatus(u.message, c)

# ─── /live ────────────────────────────────────────────────────────────────────
async def cmd_live(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "🔴 *Live Scores* — Select league:",
        parse_mode="Markdown",
        reply_markup=league_keyboard("live"),
    )

async def cb_live(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query      = u.callback_query
    await query.answer()
    league_id  = int(query.data.split(":")[1])
    league_str = ld(league_id)
    await query.edit_message_text(f"⏳ Checking live round for {league_str}…")

    async with httpx.AsyncClient() as client:
        round_name, season_name, events = await fetch_live_round(client, league_id)

    if not events:
        await query.edit_message_text(
            f"❌ No live events right now for {league_str}.\n\n"
            f"_Try /upcoming to see the next round fixtures._",
            parse_mode="Markdown"
        )
        return

    season_str = f" · {season_name}" if season_name else ""
    text = f"🔴 *LIVE — {league_str}{season_str}*\n*Matchday {round_name}*\n{SEP}\n\n"
    any_score = False
    for raw in events:
        m = _norm_event(raw)
        hth, ath = _extract_ht_score(raw)
        ht_str = f"({hth}-{ath}) " if hth is not None and ath is not None else ""
        if m["hs"] is None:
            text += f"  ⏳ *{m['home']}*  vs  *{m['away']}*\n"
        else:
            any_score = True
            icon = "🟢" if m["hs"] > m["as_"] else "🔴" if m["hs"] < m["as_"] else "🟡"
            text += f"  {icon} *{m['home']}*  {ht_str}*{m['hs']}–{m['as_']}*  *{m['away']}*\n"

    if not any_score:
        text += f"\n{SEP}\n_Round in progress — scores loading soon._\n_Refresh with /live_"
    else:
        text += f"\n{SEP}\n_🟢 Home win  🔴 Away win  🟡 Draw_\n_(HT score) FT score_"

    await query.edit_message_text(text, parse_mode="Markdown")

# ─── /upcoming ────────────────────────────────────────────────────────────────
async def cmd_upcoming(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "📡 *Upcoming Fixtures with Odds*\n\nSelect a league:",
        parse_mode="Markdown",
        reply_markup=league_keyboard("upcoming"),
    )

async def cb_upcoming(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query      = u.callback_query
    await query.answer()
    league_id  = int(query.data.split(":")[1])
    league_str = ld(league_id)
    await query.edit_message_text(f"⏳ Fetching upcoming odds for {league_str}…")

    async with httpx.AsyncClient() as client:
        round_name, round_id, _cur_season_id, events, has_scores = await fetch_next_round(client, league_id)

    if not events:
        await query.edit_message_text(f"❌ No upcoming fixtures for {league_str}.", parse_mode="Markdown")
        return

    c.bot_data["active_league"] = league_id

    text = f"📡 *{league_str} — Upcoming*\n_Matchday {round_name}_\n{SEP}\n\n"
    for raw in events:
        m    = _norm_event(raw)
        odds = _extract_odds(raw)
        text += f"⚽ *{m['home']} vs {m['away']}*\n"
        text += _fmt_odds_full(odds)
        text += f"\n{SEP}\n"

    for i, chunk in enumerate(_chunks(text)):
        if i == 0: await query.edit_message_text(chunk, parse_mode="Markdown")
        else:      await query.message.reply_text(chunk, parse_mode="Markdown")

# ─── /standings ───────────────────────────────────────────────────────────────
async def cmd_standings(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "📊 *League Standings*\n\nSelect a league:",
        parse_mode="Markdown",
        reply_markup=league_keyboard("standings"),
    )

def _get_known_seasons(fp_db: dict) -> list[str]:
    """Return all unique season_ids found in fp_db, sorted newest-first."""
    seasons: set[str] = set()
    for records in fp_db.values():
        for rec in records:
            sid = str(rec.get("season_id", "")).strip()
            if sid:
                seasons.add(sid)
    try:
        return sorted(seasons, key=lambda s: int(s), reverse=True)
    except (ValueError, TypeError):
        return sorted(seasons, reverse=True)


async def cb_standings(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query  = u.callback_query
    await query.answer()
    parts  = query.data.split(":")   # standings:LID  or  standings:LID:SEASON_ID

    league_id  = int(parts[1])
    league_str = ld(league_id)

    # ── Step 1: league selected, no season yet → show season picker ──────────
    if len(parts) == 2:
        league_model = _get_model(c.bot_data, league_id)
        # Always filter fp_db to own-league teams only — prevents cross-league
        # contamination showing up in season list or standings table
        _raw_fp_db   = league_model.get("fingerprint_db", {})
        _lcodes      = LEAGUE_TEAMS.get(league_id, set())
        fp_db = (
            {fk: recs for fk, recs in _raw_fp_db.items()
             if all(p in _lcodes for p in fk.split("|"))}
            if _lcodes else _raw_fp_db
        )
        known        = _get_known_seasons(fp_db)
        cur_season   = str(c.bot_data.get(f"cur_season_{league_id}", "")).strip()

        if not known and not cur_season:
            await _show_standings_for_season(query, c, league_id, season_id="", label="All data")
            return

        buttons = []
        seen_in_buttons: set[str] = set()

        if cur_season and cur_season not in seen_in_buttons:
            buttons.append([InlineKeyboardButton(
                f"📅 Current  #{cur_season}",
                callback_data=f"standings:{league_id}:{cur_season}"
            )])
            seen_in_buttons.add(cur_season)

        past = [s for s in known if s != cur_season]
        row: list = []
        for s in past[:20]:
            if s in seen_in_buttons:
                continue
            row.append(InlineKeyboardButton(
                f"#{s}", callback_data=f"standings:{league_id}:{s}"
            ))
            seen_in_buttons.add(s)
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        buttons.append([InlineKeyboardButton(
            "📚 All seasons combined", callback_data=f"standings:{league_id}:ALL"
        )])
        buttons.append([InlineKeyboardButton("« Back", callback_data="menu:main")])

        await query.edit_message_text(
            f"📊 *{league_str} — Choose Season*\n\n"
            f"_{len(seen_in_buttons)} season(s) in history_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    # ── Step 2: season chosen → show standings ────────────────────────────────
    chosen_season = parts[2]
    if chosen_season == "ALL":
        await _show_standings_for_season(query, c, league_id, season_id="", label="All seasons")
    else:
        await _show_standings_for_season(query, c, league_id,
                                          season_id=chosen_season,
                                          label=f"Season #{chosen_season}")


async def _show_standings_for_season(query, c, league_id: int,
                                      season_id: str, label: str):
    """Compute and display standings for one season (or all if season_id is empty)."""
    league_str   = ld(league_id)
    league_model = _get_model(c.bot_data, league_id)
    fp_db        = league_model.get("fingerprint_db", {})

    # Filter fp_db to only this league's teams before computing standings.
    # Prevents cross-league contamination showing in standings display.
    league_codes = LEAGUE_TEAMS.get(league_id, set())
    if league_codes:
        fp_db = {
            fk: recs for fk, recs in fp_db.items()
            if all(p in league_codes for p in fk.split("|"))
        }

    _ml = league_model.get("match_log", [])
    computed = _compute_standings_from_fp_db(fp_db, season_id=season_id, match_log=_ml)
    rows     = list(computed.values()) if computed else []

    if not rows and not season_id:
        stats = c.bot_data.get(f"stats_{league_id}")
        if stats:
            # Filter stats to only this league's known teams — the stats dict is
            # built from all live events and contains teams from every league.
            _lcodes_fb = LEAGUE_TEAMS.get(league_id, set())
            rows = []
            for i, (name, st) in enumerate(
                sorted(
                    ((n, s) for n, s in stats.items()
                     if not _lcodes_fb or n.upper() in _lcodes_fb),
                    key=lambda x: (-(x[1]["w"]*3 + x[1]["d"]),
                                   -(x[1]["gf"] - x[1]["ga"]))
                ), 1
            ):
                rows.append(dict(pos=i, name=name,
                                 pts=st["w"]*3 + st["d"],
                                 w=st["w"], d=st["d"], l=st["l"],
                                 gf=st["gf"], ga=st["ga"],
                                 form=st.get("form", [])))
            label = "Local cache"

    if not rows:
        await query.edit_message_text(
            f"❌ No data for {league_str} — {label}.\n\n"
            f"_Results are collected as matches are played._",
            parse_mode="Markdown"
        )
        return

    played_mds: set = set()
    for records in fp_db.values():
        for rec in records:
            if season_id and str(rec.get("season_id", "")) != season_id:
                continue
            rid = rec.get("round_id")
            if rid:
                played_mds.add(rid)

    md_str = f"{len(played_mds)} matchday{'s' if len(played_mds) != 1 else ''}" if played_mds else ""

    text  = f"📊 *{league_str}*\n"
    text += f"_{label}"
    if md_str:
        text += f" · {md_str}"
    text += f"_\n{SEP}\n"
    # Header — column widths must exactly match row format below
    # Format: pos(3) name(5) pts(4) w(3) d(3) l(3) goals(7) gd(5)
    text += "`# `  `Club ` `Pts` ` W` ` D` ` L` `Goals ` ` GD`\n"
    text += f"{SEP}\n"

    for r in sorted(rows, key=lambda x: x["pos"]):
        gd_val     = r.get("gd", r["gf"] - r["ga"])
        gd_str     = f"{'+' if gd_val > 0 else ''}{gd_val}"
        goals      = f"{r['gf']}:{r['ga']}"
        form_icons = _form_str(r.get("form", []))
        # Keep each column in its own backtick span so Telegram
        # monospace renders each cell independently — avoids drift
        pos_str    = f"{r['pos']:>2}."
        name_str   = f"{r['name']:<5}"
        pts_str    = f"{r['pts']:>3}"
        w_str      = f"{r['w']:>2}"
        d_str      = f"{r['d']:>2}"
        l_str      = f"{r['l']:>2}"
        goals_str  = f"{goals:<6}"
        gd_col     = f"{gd_str:>4}"
        text += (
            f"`{pos_str}` `{name_str}` `{pts_str}`"
            f" `{w_str}` `{d_str}` `{l_str}`"
            f" `{goals_str}` `{gd_col}`"
            f"  {form_icons}\n"
        )

    text += f"\n_{len(rows)} clubs · {league_str}_"

    back_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("« Seasons", callback_data=f"standings:{league_id}"),
        InlineKeyboardButton("« Menu",    callback_data="menu:main"),
    ]])

    for i, chunk in enumerate(_chunks(text)):
        if i == 0:
            await query.edit_message_text(chunk, parse_mode="Markdown", reply_markup=back_kb)
        else:
            await query.message.reply_text(chunk, parse_mode="Markdown")

# ─── /results ─────────────────────────────────────────────────────────────────
async def cmd_results(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "📋 *Load Recent Results*\n\nSelect a league:",
        parse_mode="Markdown",
        reply_markup=league_keyboard("results"),
    )

async def cb_results(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query      = u.callback_query
    await query.answer()
    league_id  = int(query.data.split(":")[1])
    league_str = ld(league_id)
    await query.edit_message_text(f"⏳ Fetching last 3 matchdays for {league_str}…")

    async with httpx.AsyncClient() as client:
        rounds = await fetch_past_results(client, league_id, 3)

    if not rounds:
        await query.edit_message_text(
            f"⚠️ *{league_str}* — No scored results found.\n\n"
            "Try /upcoming for fixtures with odds.",
            parse_mode="Markdown",
        )
        c.bot_data["active_league"] = league_id
        return

    all_events: list[dict] = []
    text = f"📋 *{league_str} — Recent Results*\n"

    for round_name, events in rounds:
        text += f"\n📅 *Matchday {round_name}*\n{SEP}\n"
        for raw in events:
            m = _norm_event(raw)
            if m["hs"] is None or m["as_"] is None:
                text += f"  ⚽ {m['home']}  vs  {m['away']}\n"
                continue
            icon  = "🟢" if m["hs"] > m["as_"] else "🔴" if m["hs"] < m["as_"] else "🟡"
            hth, ath = _extract_ht_score(raw)
            ht_str = f"({hth}-{ath}) " if hth is not None and ath is not None else ""
            text += f"  {icon} {m['home']}  {ht_str}*{m['hs']}–{m['as_']}*  {m['away']}\n"
            all_events.append(raw)

    stats = build_stats(all_events)
    c.bot_data[f"matches_{league_id}"] = all_events
    c.bot_data[f"stats_{league_id}"]   = stats
    c.bot_data["active_league"]         = league_id

    scored_count = len(all_events)
    status = f"✅ *{scored_count} scored matches*" if scored_count else "⚠️ *Fixtures — scores pending*"
    text += f"\n{SEP}\n{status}\n_Use /upcoming for odds · /predict for predictions_"

    for i, chunk in enumerate(_chunks(text)):
        if i == 0: await query.edit_message_text(chunk, parse_mode="Markdown")
        else:      await query.message.reply_text(chunk, parse_mode="Markdown")

# ─── /nextroundresults ────────────────────────────────────────────────────────
async def cmd_nextroundresults(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "🔍 *Next Round Fixtures & Scores*\n\nSelect a league:",
        parse_mode="Markdown",
        reply_markup=league_keyboard("nrr"),
    )

async def cb_nextroundresults(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query      = u.callback_query
    await query.answer()
    league_id  = int(query.data.split(":")[1])
    league_str = ld(league_id)
    await query.edit_message_text(f"⏳ Fetching next round for {league_str}…")

    async with httpx.AsyncClient() as client:
        round_name, round_id, _cur_season_id, events, has_scores = await fetch_next_round(client, league_id)

    if not events:
        await query.edit_message_text(f"❌ No fixtures for {league_str}.", parse_mode="Markdown")
        return

    if has_scores:
        header = f"🎯 *Next Round — Pre-Set Results*\n_{league_str} · Matchday {round_name}_\n{SEP}\n\n"
    else:
        header = f"🔍 *Next Round Fixtures*\n_{league_str} · Matchday {round_name}_\n{SEP}\n\n"

    body = ""
    total_goals, clean_sheets, biggest_win, biggest_diff = [], [], None, -1

    for raw in events:
        m = _norm_event(raw)
        hs, as_ = m["hs"], m["as_"]
        if hs is not None and as_ is not None:
            icon = "🟢" if hs > as_ else "🔴" if hs < as_ else "🟡"
            body += f"{icon} *{m['home']}*  {hs} – {as_}  *{m['away']}*\n"
            total_goals.append(hs+as_)
            if as_ == 0: clean_sheets.append(m["home"])
            if hs == 0:  clean_sheets.append(m["away"])
            diff = abs(hs-as_)
            if diff > biggest_diff:
                biggest_diff = diff
                biggest_win  = f"{m['home']} {hs}–{as_} {m['away']}"
        else:
            body += f"⚽ *{m['home']}*  vs  *{m['away']}*\n"

    footer = f"\n{SEP}\n"
    if has_scores and total_goals:
        footer += f"🏆 *Matchday Stats*\n  Total goals: {sum(total_goals)}\n"
        if clean_sheets:
            footer += f"  Clean sheets: {', '.join(clean_sheets[:5])}\n"
        if biggest_win:
            footer += f"  Biggest win: {biggest_win}\n"
        footer += "\n_🟢 Home  🔴 Away  🟡 Draw_"
    else:
        footer += "_Fixtures confirmed. Scores seeded before round starts._\n_Use /upcoming for odds._"

    c.bot_data["next_fixtures"] = {
        "league_id": league_id, "league_str": league_str,
        "round": round_name, "events": events, "has_scores": has_scores,
    }

    full = header + body + footer
    for i, chunk in enumerate(_chunks(full)):
        if i == 0: await query.edit_message_text(chunk, parse_mode="Markdown")
        else:      await query.message.reply_text(chunk, parse_mode="Markdown")

# ─── /predict ─────────────────────────────────────────────────────────────────
async def cmd_predict(u: Update, c: ContextTypes.DEFAULT_TYPE):
    lid = c.bot_data.get("active_league", 7794)
    await u.message.reply_text("⏳ Building predictions…")
    async with httpx.AsyncClient() as client:
        stats = await _ensure_stats(c, lid, client)
        round_name, round_id, _cur_season_id, events, _ = await fetch_next_round(client, lid)

    if not stats:
        await u.message.reply_text("❌ No historical stats. Try /results first.", parse_mode="Markdown")
        return
    if not events:
        await u.message.reply_text("❌ No upcoming fixtures found.", parse_mode="Markdown")
        return

    league_str = ld(lid)
    text = f"🔮 *Predictions — {league_str}*\n_Matchday {round_name}_\n{SEP}\n\n"

    for raw in events:
        m       = _norm_event(raw)
        ev_odds = _extract_odds(raw)
        p       = predict_match(m["home"], m["away"], stats, ev_odds, league_model)
        h2h     = p.get("h2h")
        h2h_ln  = ""
        if h2h and h2h["n"] >= 2:
            h2h_ln = (f"  📋 H2H({h2h['n']}): "
                      f"H{h2h['hw']} D{h2h['d']} A{h2h['aw']}\n")
        bp_odds = p.get("bp_odds", {})
        odds_ln = ""
        if bp_odds.get("1") and bp_odds.get("X") and bp_odds.get("2"):
            odds_ln = f"  💰 {bp_odds['1']} / {bp_odds['X']} / {bp_odds['2']}\n"
        text += (
            f"{p['icon']} *{m['home']} vs {m['away']}*\n"
            f"  Str: {p['hsc']:.0f} vs {p['asc']:.0f}\n"
            f"  🏠{p['hw']:.0f}%  🤝{p['dw']:.0f}%  ✈️{p['aw']:.0f}%\n"
            f"  Tip: *{p['tip']}* ({p['conf']:.0f}% conf)\n"
            f"  xG: {p['exp_h']:.2f}–{p['exp_a']:.2f}  🔢O2.5 {p['over25']:.0f}%\n"
            f"  ⚽ BTTS: *{'Yes ✅' if p['btts'] >= 50 else 'No ❌'}*  ({p['btts']:.0f}%)\n"
            + odds_ln + h2h_ln + "\n"
        )

    await _send(u.message, text, parse_mode="Markdown")

# ─── predict_cb / compare_cb — triggered from menu league picker ──────────────
async def cb_predict_cb(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query = u.callback_query
    await query.answer()
    lid = int(query.data.split(":")[-1])
    c.bot_data["active_league"] = lid
    await query.message.reply_text("⏳ Building predictions…")
    async with httpx.AsyncClient() as client:
        stats = await _ensure_stats(c, lid, client)
        round_name, round_id, _cur_season_id, events, _ = await fetch_next_round(client, lid)
    if not stats:
        await query.message.reply_text("❌ No historical stats yet.", parse_mode="Markdown"); return
    if not events:
        await query.message.reply_text("❌ No upcoming fixtures found.", parse_mode="Markdown"); return
    league_model = _get_model(c.bot_data, lid)
    league_str   = ld(lid)
    text = f"🔮 *Predictions — {league_str}*\n_Matchday {round_name}_\n{SEP}\n\n"
    for match_pos, raw in enumerate(events):
        m       = _norm_event(raw)
        ev_odds = _extract_odds(raw)
        try:
            league_model["_current_round_id"] = int(round_id)
        except (ValueError, TypeError):
            league_model["_current_round_id"] = 0
        league_model["_current_season_id"] = str(_cur_season_id or "")
        league_model["_match_position"] = match_pos
        p       = predict_match(m["home"], m["away"], stats, ev_odds, league_model)
        h2h_ln  = (f"  📋 H2H({h2h['n']}): H{h2h['hw']} D{h2h['d']} A{h2h['aw']}\n"
                   if h2h and h2h["n"] >= 2 else "")
        cs_preds = predict_correct_score(m["home"], m["away"], stats, p, ev_odds, top_n=3)
        btts_tip = "Yes ✅" if p['btts'] >= 50 else "No ❌"
        btts_ln  = f"  ⚽ BTTS: *{btts_tip}*  ({p['btts']:.0f}%)\n"
        text += (
            f"*{m['home']}  v  {m['away']}*\n"
            f"{p['icon']} *{p['tip']}*  {p['conf']:.0f}%\n"
            f"🏠{p['hw']:.0f}%  🤝{p['dw']:.0f}%  ✈️{p['aw']:.0f}%\n"
            + btts_ln + h2h_ln + "\n"
        )
    await _send(query.message, text, parse_mode="Markdown")

async def cb_compare_cb(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query = u.callback_query
    await query.answer()
    lid = int(query.data.split(":")[-1])
    c.bot_data["active_league"] = lid
    await query.message.reply_text("⏳ Loading comparison…")
    async with httpx.AsyncClient() as client:
        stats = await _ensure_stats(c, lid, client)
        round_name, round_id, _cur_season_id, events, has_scores = await fetch_next_round(client, lid)
    if not stats:
        await query.message.reply_text("❌ No stats yet.", parse_mode="Markdown"); return
    if not events:
        await query.message.reply_text("❌ No upcoming fixtures.", parse_mode="Markdown"); return
    league_model = _get_model(c.bot_data, lid)
    league_str   = ld(lid)
    text = f"🆚 *Compare — {league_str}*\n_Matchday {round_name}_\n{SEP}\n\n"
    agreed = disagreed = total = 0
    for match_pos, raw in enumerate(events):
        m       = _norm_event(raw)
        ev_odds = _extract_odds(raw)
        try:
            league_model["_current_round_id"] = int(round_id)
        except (ValueError, TypeError):
            league_model["_current_round_id"] = 0
        league_model["_current_season_id"] = str(_cur_season_id or "")
        league_model["_match_position"] = match_pos
        p       = predict_match(m["home"], m["away"], stats, ev_odds, league_model)
        total  += 1
        our_t   = p["tip"].split()[0]
        if hs is not None and as_ is not None:
            bp_t = "HOME" if hs > as_ else "AWAY" if hs < as_ else "DRAW"
            ok   = (our_t == bp_t)
            if ok: agreed += 1
            else:  disagreed += 1
            match_ln = f"  {'✅' if ok else '❌'} {bp_t}  {hs}–{as_}\n"
        else:
            match_ln = "  ⏳ Scores not seeded yet\n"
        text += (f"*{m['home']} vs {m['away']}*\n"
                 f"  {p['icon']} {p['tip']}  ({p['conf']:.0f}%)\n" + match_ln + "\n")
    text += f"{SEP}\n"
    if has_scores and total:
        acc   = round(agreed/total*100)
        text += f"📊 *Accuracy: {agreed}/{total} ({acc}%)*"
    else:
        text += f"_{total} predictions · scores pending_"
    await _send(query.message, text, parse_mode="Markdown")

# ─── /compare ─────────────────────────────────────────────────────────────────
async def cmd_compare(u: Update, c: ContextTypes.DEFAULT_TYPE):
    lid = c.bot_data.get("active_league", 7794)
    await u.message.reply_text("⏳ Loading comparison…")
    async with httpx.AsyncClient() as client:
        stats = await _ensure_stats(c, lid, client)
        round_name, round_id, _cur_season_id, events, has_scores = await fetch_next_round(client, lid)

    if not stats:
        await u.message.reply_text("❌ No stats. Try /results first.", parse_mode="Markdown")
        return
    if not events:
        await u.message.reply_text("❌ No upcoming fixtures.", parse_mode="Markdown")
        return

    league_str = ld(lid)
    text = f"🆚 *Compare — {league_str}*\n_Matchday {round_name}_\n{SEP}\n\n"
    agreed = disagreed = total = 0

    for raw in events:
        m       = _norm_event(raw)
        ev_odds = _extract_odds(raw)
        p       = predict_match(m["home"], m["away"], stats, ev_odds, league_model)
        hs, as_ = m["hs"], m["as_"]
        total  += 1
        our_t = p["tip"].split()[0]
        if hs is not None and as_ is not None:
            bp_t  = "HOME" if hs > as_ else "AWAY" if hs < as_ else "DRAW"
            ok    = (our_t == bp_t)
            if ok: agreed += 1
            else:  disagreed += 1
            match_ln = f"  {'✅' if ok else '❌'} betPawa: {bp_t} {hs}–{as_}\n"
        else:
            match_ln = f"  ⏳ Scores not seeded yet\n"

        text += (
            f"*{m['home']} vs {m['away']}*\n"
            f"  {p['icon']} Model: {p['tip']}  {p['exp_h']:.0f}–{p['exp_a']:.0f} ({p['conf']:.0f}%)\n"
            + match_ln + "\n"
        )

    text += f"{SEP}\n"
    if has_scores and total:
        acc   = round(agreed/total*100)
        text += f"📊 *Accuracy: {agreed}/{total} ({acc}%)*\n✅ {agreed} matched   ❌ {disagreed} differed"
    else:
        text += f"_{total} predictions ready. Scores not yet seeded._"

    await _send(u.message, text, parse_mode="Markdown")

# ─── /overallgoals ────────────────────────────────────────────────────────────
async def cmd_overallgoals(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "⚽ *Overall Season Goal Stats*\n\nSelect a league:",
        parse_mode="Markdown",
        reply_markup=league_keyboard("og"),
    )

async def cb_overallgoals(u: Update, c: ContextTypes.DEFAULT_TYPE):
    query      = u.callback_query
    await query.answer()
    league_id  = int(query.data.split(":")[1])
    league_str = ld(league_id)
    await query.edit_message_text(f"⏳ Loading all rounds for {league_str}… (may take a moment)")

    async with httpx.AsyncClient() as client:
        rounds = await fetch_all_results(client, league_id)

    if not rounds:
        await query.edit_message_text(f"❌ No data for {league_str}.", parse_mode="Markdown")
        return

    all_events = [e for _, evs in rounds for e in evs]
    stats      = build_stats(all_events)
    matchdays  = len(rounds)
    total_goals = sum(
        (m["hs"] or 0) + (m["as_"] or 0)
        for e in all_events
        for m in [_norm_event(e)]
        if m["hs"] is not None
    )

    ranked = sorted(stats.items(), key=lambda x: x[1]["gf"], reverse=True)
    text   = (
        f"⚽ *{league_str} — Overall Goal Stats*\n"
        f"_{matchdays} matchdays · {len(all_events)} matches · {total_goals} total goals_\n{SEP}\n\n"
    )

    for team, st in ranked:
        p   = st["p"] or 1
        avg = st["gf"]/p
        text += (
            f"*{team}*\n"
            f"  P{st['p']} | Scored: {st['gf']} ({avg:.1f}/g) | "
            f"Conceded: {st['ga']} ({st['ga']/p:.1f}/g)\n"
            f"  CS: {st['cs']} | W{st['w']} D{st['d']} L{st['l']}\n\n"
        )

    for i, chunk in enumerate(_chunks(text)):
        if i == 0: await query.edit_message_text(chunk, parse_mode="Markdown")
        else:      await query.message.reply_text(chunk, parse_mode="Markdown")

# ─── /teams (alias for /standings) ───────────────────────────────────────────
async def cmd_teams(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "📊 *League Standings*\n\nSelect a league:",
        parse_mode="Markdown",
        reply_markup=league_keyboard("standings"),
    )

# ─── /team ────────────────────────────────────────────────────────────────────
async def cmd_team_detail(u: Update, c: ContextTypes.DEFAULT_TYPE):
    lid = c.bot_data.get("active_league", 7794)
    if not c.args:
        await u.message.reply_text("Usage: `/team Arsenal`", parse_mode="Markdown")
        return
    q = " ".join(c.args).lower()
    await u.message.reply_text(f"⏳ Loading report for *{q}*…", parse_mode="Markdown")
    async with httpx.AsyncClient() as client:
        stats = await _ensure_stats(c, lid, client)
    if not stats:
        await u.message.reply_text("❌ No stats. Try /results first.", parse_mode="Markdown")
        return
    name = next((t for t in stats if q in t.lower()), None)
    if not name:
        await u.message.reply_text(
            f"❌ No team matching *{q}*\n\n" + "\n".join(f"• {t}" for t in sorted(stats.keys())),
            parse_mode="Markdown")
        return
    st = stats[name]; sc = strength_score(st); p = st["p"] or 1
    good, bad = strengths_weaknesses(st)
    await u.message.reply_text(
        f"🏟️ *{name}* — {ld(lid)}\n{SEP}\n"
        f"⚡ *Strength: {sc:.1f}/100*  {_bar(sc, 8)}\n\n"
        f"P{st['p']} W{st['w']} D{st['d']} L{st['l']}\n"
        f"Scored: {st['gf']} ({st['gf']/p:.2f}/g)  Conceded: {st['ga']} ({st['ga']/p:.2f}/g)\n"
        f"Clean sheets: {st['cs']} ({st['cs']/p*100:.0f}%)  "
        f"Failed to score: {st['fts']} ({st['fts']/p*100:.0f}%)\n\n"
        f"🏠 Home  P{st['hp']} W{st['hw']} ({st['hw']/max(st['hp'],1)*100:.0f}%)\n"
        f"✈️  Away  P{st['ap']} W{st['aw']} ({st['aw']/max(st['ap'],1)*100:.0f}%)\n\n"
        f"Last 5: {_form_str(st['form'])}\n\n"
        f"💪 *Strengths*\n" + "".join(f"  {x}\n" for x in good) +
        f"\n⚠️ *Weaknesses*\n" + "".join(f"  {x}\n" for x in bad),
        parse_mode="Markdown",
    )

# ─── /debug ───────────────────────────────────────────────────────────────────
async def cmd_debug(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Dump real API structure to diagnose issues."""
    await u.message.reply_text("🔬 Fetching raw API structure…")

    async with httpx.AsyncClient() as client:
        # Fetch actual rounds
        data = await _get(client, BASE + EP_ACTUAL_ROUNDS, params={"leagueId": 7794})

    if not data:
        await u.message.reply_text("❌ No data — check BOT_TOKEN and BETPAWA_COOKIE.")
        return

    items = data.get("items", []) if isinstance(data, dict) else data
    top_keys = list(data.keys()) if isinstance(data, dict) else "list"
    n_items = len(items) if isinstance(items, list) else "?"

    first = items[0] if isinstance(items, list) and items else {}
    first_keys = list(first.keys()) if isinstance(first, dict) else []
    has_rounds = "rounds" in first
    n_rounds = len(first.get("rounds", [])) if isinstance(first, dict) else 0

    first_round = first.get("rounds", [{}])[0] if has_rounds and isinstance(first.get("rounds"), list) else first
    rkeys = list(first_round.keys()) if isinstance(first_round, dict) else []
    rtt   = first_round.get("tradingTime") if isinstance(first_round, dict) else None
    rid   = first_round.get("id") if isinstance(first_round, dict) else None

    text = (
        f"🔬 *Debug — actual endpoint*\n\n"
        f"Top keys: `{top_keys}`\n"
        f"Items count: `{n_items}`\n\n"
        f"First item keys: `{first_keys}`\n"
        f"Has nested rounds: `{has_rounds}`\n"
        f"Rounds in first item: `{n_rounds}`\n\n"
        f"First round keys: `{rkeys}`\n"
        f"First round id: `{rid}`\n"
        f"tradingTime: `{rtt}`\n"
    )
    await u.message.reply_text(text, parse_mode="Markdown")


def _brain_summary_inline(bd: dict) -> str:
    """Compact one-liner learning status used in skip notices."""
    lines = []
    for _lid in LEAGUES:
        m = bd.get(f"model_{_lid}")
        if not isinstance(m, dict) or m.get("rounds_learned", 0) == 0:
            continue
        cum  = m.get("cumulative", {})
        ot   = cum.get("outcome_total", 0)
        oc   = cum.get("outcome_correct", 0)
        acc  = (oc / ot * 100) if ot > 0 else m.get("outcome_acc", 0) * 100
        rds  = m.get("rounds_learned", 0)
        dot  = "🟢" if acc >= 65 else "🟡" if acc >= 50 else "🔴"
        fdb_  = m.get("fingerprint_db", {})
        fp_f  = len(fdb_)
        fp_r  = sum(len(v) for v in fdb_.values() if isinstance(v, list))
        fp_c  = min(100, round(fp_f / 9 * 100))
        lines.append(f"{dot} {LEAGUES[_lid]['name']}: {acc:.0f}% | {fp_f}fix/{fp_r}rec({fp_c}%)")
    if not lines:
        return ""
    return "📈 *Learning Status*\n" + "  ".join(lines[:4]) + (
        ("\n" + "  ".join(lines[4:])) if len(lines) > 4 else ""
    ) + "\n━━━━━━━━━━━━━━━━━━━━━━"


# ─── AUTO POST ENGINE ─────────────────────────────────────────────────────────
CONF_MIN = 60   # minimum confidence % to include a pick in auto-posts

async def _run_auto_post(bot, bot_data: dict):
    """Fetch picks for ALL leagues and send/edit to all registered targets.
    Uses ONLY cached stats — never blocks on a fresh fetch."""

    async with httpx.AsyncClient() as client:

        class _FakeCtx:
            def __init__(self, bd):
                self.bot_data = bd

        ctx = _FakeCtx(bot_data)

        sections      = []
        round_ids     = []
        matchday_nums = []
        _had_events   = False  # tracks if any league had new events to evaluate

        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            try:
                # Only use already-cached stats — skip if not loaded yet
                stats = bot_data.get(f"stats_{lid}")
                if not stats:
                    log.info(f"auto_post: stats not cached yet for {lid}, skipping")
                    continue

                # Fetch next round — wait for previous round to finish first
                # fetch_completed_round only confirms when previous round scores are final
                # This ensures standings + last game results are updated before filtering
                round_name, round_id, _cur_season_id, events, _prev_confirmed, \
                    _prev_rid, _prev_scores = \
                    await fetch_completed_round(client, lid)

                # ── Update previously sent cards with results ──────────────
                # When previous round just finished, edit those sent cards
                # to show ✅/❌ results instead of ⏳ pending
                if _prev_rid and _prev_scores:
                    _prev_sent_key = f"results_updated_{lid}_{_prev_rid}"
                    if not bot_data.get(_prev_sent_key):
                        bot_data[_prev_sent_key] = True
                        # Store prev scores so send loop can update old messages
                        bot_data.setdefault("pending_result_updates", {})
                        bot_data["pending_result_updates"][f"{lid}:{_prev_rid}"] = {
                            "lid":        lid,
                            "round_id":   _prev_rid,
                            "scores":     _prev_scores,
                        }
                        log.info(f"📊 Queued result update for lid={lid} prev_rid={_prev_rid} "
                                 f"({len(_prev_scores)} scores)")
                if not events or not round_id:
                    continue

                # Only predict once previous round is confirmed complete
                if not _prev_confirmed:
                    # Cold start grace: if match_log is empty (fresh deploy with
                    # no brain data), allow through — data_collector will backfill
                    # past rounds and standings will build up within minutes.
                    _ml_size = len(_get_model(bot_data, lid).get("match_log", []))
                    if _ml_size < 18:
                        log.info(f"auto_post [{lid}]: cold start — match_log has {_ml_size} entries, allowing through")
                    else:
                        log.debug(f"auto_post [{lid}]: previous round not yet confirmed — waiting")
                        continue

                # If this league+round was already sent — silent, move on
                _sent_map_check = bot_data.get("auto_sent_per_league", {})
                if _sent_map_check.get(str(lid)) == str(round_id):
                    continue

                round_ids.append(f"{lid}:{round_id}")
                matchday_nums.append(str(round_name))
                agreed = total = 0
                body           = ""
                match_cards    = []
                round_preds    = []

                # Get learned model for this league
                league_model = _get_model(bot_data, lid)

                # Always save current season ID so standings job uses correct season
                if _cur_season_id:
                    league_model["_current_season_id"] = str(_cur_season_id)

                # Cache standings + tier_map into model so predict_match
                # can access them for momentum and fixture investigation
                _st_now = bot_data.get(f"standings_{lid}", {})
                league_model["_standings_cache"] = _st_now
                league_model["_tier_map_cache"]  = _get_all_tiers(_st_now) if _st_now else {}

                for match_pos, raw in enumerate(events):
                    m       = _norm_event(raw)
                    ev_odds = _extract_odds(raw)
                    # Tell the model which round and position we're predicting (for algo engines)
                    try:
                        league_model["_current_round_id"] = int(round_id)
                    except (ValueError, TypeError):
                        league_model["_current_round_id"] = 0
                    league_model["_match_position"] = match_pos
                    p       = predict_match(m["home"], m["away"], stats, ev_odds, league_model)

                    # ── Always store ALL matches for learning — no filter here ──
                    # Learning must see every match so band data matures across all
                    # confidence levels and markets. Display filtering happens below.
                    cs_preds_raw = predict_correct_score(
                        m["home"], m["away"], stats, p, ev_odds, top_n=3
                    )
                    round_preds.append({
                        "home":          m["home"],
                        "away":          m["away"],
                        "tip":           p["tip"],
                        "conf":          p.get("conf", 50.0),
                        "odds_tip":      p.get("odds_tip"),
                        "poisson_tip":   p.get("poisson_tip"),
                        "strength_tip":  p.get("strength_tip"),
                        "pred_h":        p["exp_h"],
                        "pred_a":        p["exp_a"],
                        "btts_pred":     p.get("btts", 50.0) >= 50,
                        "btts_prob":     p.get("btts", 50.0),
                        "over25_pred":   p.get("over25", 50.0) >= 50,
                        "over25_prob":   p.get("over25", 50.0),
                        "dominant_htft": p.get("dominant_htft"),  # HT/FT learning
                        # Algorithm engines
                        "prob_H":        p.get("prob_H", 0.45),
                        "prob_D":        p.get("prob_D", 0.27),
                        "prob_A":        p.get("prob_A", 0.28),
                    })

                    # ══════════════════════════════════════════════════════════
                    # TWO HARD FILTERS — both must pass to show prediction
                    # Filter 1: Strong lost OR Weak won (last game)
                    # ══════════════════════════════════════════════════════════
                    # SOLE GATE: Odds Repeat Detection
                    # Only matches where current odds match historical records
                    # are sent as predictions. Everything else is info on card.
                    # ══════════════════════════════════════════════════════════

                    _tip_g = p["tip"].split()[0]

                    # Never show draws
                    if _tip_g == "DRAW":
                        continue

                    # ── ODDS REPEAT — HARD GATE ───────────────────────────────
                    _fp_db_gate = league_model.get("fingerprint_db", {})
                    _repeat_chk = _detect_odds_repeat(
                        _fp_db_gate, m["home"], m["away"], ev_odds,
                        league_id=lid, bot_data=bot_data
                    )
                    if not _repeat_chk.get("matched"):
                        continue  # ❌ No odds repeat — skip this match

                    # ── INFO: Strategy (view only, never blocks) ──────────────
                    _strat_standings = bot_data.get(f"standings_{lid}", {})
                    _g1_score = 0
                    _g1_label = "⚠️ no standings yet"
                    if _strat_standings:
                        _strat_tiers = _get_all_tiers(_strat_standings)
                        _h_tier = _find_tier(m["home"], _strat_tiers)
                        _a_tier = _find_tier(m["away"], _strat_tiers)
                        _h_last = _strategy_get_last_game(m["home"], league_model)
                        _a_last = _strategy_get_last_game(m["away"], league_model)
                        _h_result = (_h_last or {}).get("result", "")
                        _a_result = (_a_last or {}).get("result", "")
                        _strong_lost = (
                            (_h_tier == "STRONG" and _h_result == "LOSS") or
                            (_a_tier == "STRONG" and _a_result == "LOSS")
                        )
                        _weak_won = (
                            (_h_tier == "WEAK" and _h_result == "WIN") or
                            (_a_tier == "WEAK" and _a_result == "WIN")
                        )
                        if _strong_lost:
                            _sl = m["home"] if (_h_tier=="STRONG" and _h_result=="LOSS") else m["away"]
                            _g1_score = 100
                            _g1_label = f"✅ {_sl}(Strong) lost → recovery"
                        elif _weak_won:
                            _ww = m["home"] if (_h_tier=="WEAK" and _h_result=="WIN") else m["away"]
                            _g1_score = 100
                            _g1_label = f"✅ {_ww}(Weak) won → opp WIN"
                        else:
                            _g1_score = 40
                            _g1_label = f"🟡 no strategy pattern"

                    # ── INFO: History (view only) ─────────────────────────────
                    _fc_g      = p.get("fixture_case", {}) or {}
                    _fc_n_g    = _fc_g.get("n_meetings", 0)
                    _h_hist    = _fc_g.get("home_win_pct", 0)
                    _a_hist    = _fc_g.get("away_win_pct", 0)
                    _draw_hist = _fc_g.get("draw_pct", 0)
                    if _tip_g == "HOME":
                        _winner_hist = _h_hist; _loser_hist = _a_hist
                    else:
                        _winner_hist = _a_hist; _loser_hist = _h_hist

                    if _fc_n_g >= 3:
                        if _winner_hist >= 60:
                            _g2_score = round(_winner_hist)
                            _g2_label = f"✅ {_g2_score}% in {_fc_n_g} meetings"
                        elif _winner_hist >= _loser_hist:
                            _g2_score = round(_winner_hist)
                            _g2_label = f"🟡 {_g2_score}% in {_fc_n_g} meetings"
                        else:
                            _g2_score = round(_winner_hist)
                            _g2_label = f"🔴 {_g2_score}% history (loser leads)"
                    else:
                        _g2_score = 50
                        _g2_label = f"🔵 {_fc_n_g} meetings (building)"

                    # ── INFO: Band accuracy (view only) ───────────────────────
                    _ma      = league_model.get("margin_acc", {})
                    _bba     = league_model.get("btts_band_acc", {"yes":{}, "no":{}})
                    _oba     = league_model.get("o25_band_acc",  {"yes":{}, "no":{}})
                    _1x2_b   = str(int(p["conf"]          // 5) * 5)
                    _bt_b    = str(int(p.get("btts",  50) // 5) * 5)
                    _o25_b   = str(int(p.get("over25",50) // 5) * 5)
                    _bt_side = "yes" if p.get("btts",  50) >= 50 else "no"
                    _o25_side= "yes" if p.get("over25",50) >= 50 else "no"

                    def _band_acc(src, bkt, side=None):
                        d = src[side] if side else src
                        rec = d.get(bkt, [0, 0])
                        return rec[0] / rec[1] if rec[1] >= 10 else None

                    _s1x2 = _band_acc(_ma,  _1x2_b)
                    _sbt  = _band_acc(_bba,  _bt_b,  _bt_side)
                    _so25 = _band_acc(_oba,  _o25_b, _o25_side)
                    _trusted      = [s for s in [_s1x2,_sbt,_so25] if s is not None and s >= 0.60]
                    _no_band_data = all(s is None for s in [_s1x2,_sbt,_so25])
                    if _trusted:
                        _g3_score = round(max(_trusted) * 100)
                        _g3_label = f"✅ {_g3_score}% bot accuracy"
                    elif _no_band_data:
                        _g3_score = round(p["conf"])
                        _g3_label = f"🔵 {_g3_score}% conf (building)"
                    else:
                        _best = max((s for s in [_s1x2,_sbt,_so25] if s is not None), default=0)
                        _g3_score = round(_best * 100)
                        _g3_label = f"🟡 {_g3_score}% bot accuracy"

                    # ── INFO: Momentum (view only) ────────────────────────────
                    _hm_g  = p.get("home_momentum") or {}
                    _am_g  = p.get("away_momentum") or {}
                    _has_mom = (_hm_g.get("games_used", 0) >= 3 and
                                _am_g.get("games_used", 0) >= 3)
                    _hw_g  = _hm_g.get("win_pct", 0)
                    _aw_g  = _am_g.get("win_pct", 0)
                    _ht_g  = _hm_g.get("trend", "STABLE")
                    _at_g  = _am_g.get("trend", "STABLE")
                    if _tip_g == "HOME":
                        _winner_mom = _hw_g; _loser_mom = _aw_g
                        _winner_trend = _ht_g; _loser_trend = _at_g
                    else:
                        _winner_mom = _aw_g; _loser_mom = _hw_g
                        _winner_trend = _at_g; _loser_trend = _ht_g
                    if _has_mom:
                        _mom_gap = _winner_mom - _loser_mom
                        # RISING=60+  FALLING=30-  STABLE=31-59
                        if _winner_mom >= 60 and _mom_gap >= 25:
                            _g4_score = round(min(65 + _mom_gap * 0.3, 92))
                            _g4_label = f"✅ {_winner_mom:.0f}% vs {_loser_mom:.0f}% (+{_mom_gap:.0f}%)"
                        elif _winner_mom >= 60 and _mom_gap >= 10:
                            _g4_score = round(min(55 + _mom_gap * 0.3, 80))
                            _g4_label = f"🟡 {_winner_mom:.0f}% vs {_loser_mom:.0f}%"
                        elif _winner_mom > _loser_mom:
                            _g4_score = round(min(45 + _mom_gap * 0.3, 65))
                            _g4_label = f"🟡 {_winner_mom:.0f}% vs {_loser_mom:.0f}%"
                        else:
                            _g4_score = round(max(20, 35 + _mom_gap * 0.3))
                            _g4_label = f"🔴 {_winner_mom:.0f}% vs {_loser_mom:.0f}%"
                    else:
                        _g4_score = 50
                        _g4_label = "🔵 building"

                    # ── OVERALL: based on info scores + repeat tier ────────────
                    _overall = round(
                        _g1_score * 0.20 +
                        _g2_score * 0.30 +
                        _g3_score * 0.20 +
                        _g4_score * 0.30
                    )
                    if   _overall >= 80: _overall_label = "🔥 STRONG PICK"
                    elif _overall >= 67: _overall_label = "✅ GOOD PICK"
                    elif _overall >= 50: _overall_label = "🟡 MODERATE"
                    else:                _overall_label = "🔴 WEAK SIGNAL"

                    hs, as_ = m["hs"], m["as_"]
                    total  += 1
                    our_t   = p["tip"].split()[0]

                    if hs is not None and as_ is not None:
                        bp_t = "HOME" if hs > as_ else "AWAY" if hs < as_ else "DRAW"
                        ok   = (our_t == bp_t)
                        if ok: agreed += 1
                        result_str = f"{'✅' if ok else '❌'} {hs}–{as_}"
                    else:
                        result_str = "⏳ pending"

                    # ── S-W and fire ──────────────────────────────────────────
                    _standings_now = _strat_standings
                    _tier_map_now  = _get_all_tiers(_standings_now) if _standings_now else {}
                    _svw_card = _is_strong_vs_weak(
                        m["home"], m["away"], _standings_now, _tier_map_now
                    ) if _standings_now else False
                    _svw_tag = "  S-W" if _svw_card else ""
                    _fp_res  = p.get("fp_result", {}) or {}
                    _fire    = " 🔥" if (_fp_res.get("n_samples",0) >= 3
                                         and _fp_res.get("pool_vote_rate",0) >= 0.65) else ""

                    # History rate on tip
                    _fc      = p.get("fixture_case", {}) or {}
                    _fc_n    = _fc.get("n_meetings", 0)
                    _tip_out = p["tip"].split()[0]
                    _hpct    = (_fc.get("home_win_pct") if _tip_out=="HOME" else
                                _fc.get("away_win_pct") if _tip_out=="AWAY" else
                                _fc.get("draw_pct"))
                    _hist_tag = (f"  · {_hpct:.0f}% in {_fc_n} meetings"
                                 if _hpct and _fc_n >= 3 else "")

                    # ── Best 2 markets from fp_db records ─────────────────────
                    _fp_db_now = league_model.get("fingerprint_db", {})
                    _fk        = "|".join(sorted([m["home"], m["away"]]))
                    _recs      = _fp_db_now.get(_fk, [])
                    _nr        = len(_recs)

                    def _rate(key):
                        v = [r.get(key) for r in _recs if r.get(key) is not None]
                        return round(sum(v)/len(v)*100, 1) if v else None

                    # All markets with their rates
                    _mkts = []
                    for _k, _lbl, _ico in [
                        ("ou15_result", "Over 1.5",  "📊"),
                        ("ou25_result", "Over 2.5",  "📈"),
                        ("ou35_result", "Over 3.5",  "📊"),
                        ("btts_result", "BTTS Yes",  "⚽"),
                    ]:
                        _r = _rate(_k)
                        if _r is not None:
                            # Also consider Under/No side
                            _inv = round(100 - _r, 1)
                            if _r >= 60:
                                _mkts.append((_r, _lbl, _ico))
                            if _inv >= 60:
                                _inv_lbl = _lbl.replace("Over","Under").replace("BTTS Yes","BTTS No")
                                _inv_ico = _ico
                                _mkts.append((_inv, _inv_lbl, _inv_ico))

                    # HT/FT as a market candidate
                    _dom_htft = p.get("dominant_htft") or ""
                    if _dom_htft and "?" not in _dom_htft:
                        _htft_acc_data = league_model.get("htft_acc", {})
                        _hrec = _htft_acc_data.get(_dom_htft, {})
                        _htot = _hrec.get("total", 0)
                        _hcor = _hrec.get("correct", 0)
                        _ht_map = {"1": "Home", "X": "Draw", "2": "Away"}
                        _hp = _dom_htft.split("/")
                        _hlbl = (f"HT/FT {_ht_map.get(_hp[0],_hp[0])}"
                                 f"/{_ht_map.get(_hp[1],_hp[1])}"
                                 if len(_hp) == 2 else f"HT/FT {_dom_htft}")

                        if _htot >= 5:
                            # Learned accuracy path — use real tracked accuracy
                            _hpct2 = round(_hcor / _htot * 100)
                            if _hpct2 >= 60:
                                _mkts.append((_hpct2, _hlbl, "⏱"))
                        else:
                            # Fallback: use fp_db dominance confidence directly
                            # dom_htft_conf = how often this pattern appeared across records
                            _fp_db_now2 = league_model.get("fingerprint_db", {})
                            _fk2 = "|".join(sorted([m["home"], m["away"]]))
                            _recs2 = _fp_db_now2.get(_fk2, [])
                            if _recs2:
                                _dom_conf = _recs2[-1].get("dominant_htft_conf", 0)
                                if _dom_conf >= 0.60:
                                    _hpct2 = round(_dom_conf * 100)
                                    _mkts.append((_hpct2, _hlbl, "⏱"))

                    # Sort by rate, take best 2
                    _mkts.sort(key=lambda x: -x[0])
                    market_lines = ""
                    for _mrate, _mname, _mico in _mkts[:2]:
                        _mfire = " 🔥" if _mrate >= 75 else ""
                        _msuf  = f"  {_nr} meetings" if _nr >= 3 else ""
                        market_lines += f"┆ {_mico} {_mname}{_mfire}  {_mrate:.0f}%{_msuf}\n"

                    # ── History verdict (1 line only) ─────────────────────────
                    _fc_line = ""
                    if _fc_n >= 3:
                        _lbl = _fc.get("verdict_label", "")
                        # Only show ✅ STRONG CASE and 📋 CONFIRMED — hide UNCERTAIN and CAUTION
                        _positive = "STRONG CASE" in _lbl or "CONFIRMED" in _lbl
                        _fc_line = f"┆ {_lbl}\n" if (_lbl and _positive) else ""

                    # ── Momentum (simple: team → direction wins%) ─────────────
                    _hmom = p.get("home_momentum") or {}
                    _amom = p.get("away_momentum") or {}
                    _mom_line = ""
                    if _hmom.get("games_used",0) >= 3 and _amom.get("games_used",0) >= 3:
                        _hwp = _hmom.get("win_pct", 0)
                        _awp = _amom.get("win_pct", 0)
                        _htr = _hmom.get("trend", "STABLE")
                        _atr = _amom.get("trend", "STABLE")

                        # Strict trend thresholds:
                        # RISING  = 60%+  FALLING = 30%-  STABLE = 31-59%
                        if _hwp >= 60:
                            _hti = "📈 rising"
                        elif _hwp <= 30:
                            _hti = "📉 falling"
                        else:
                            _hti = "➡️ stable"

                        if _awp >= 60:
                            _ati = "📈 rising"
                        elif _awp <= 30:
                            _ati = "📉 falling"
                        else:
                            _ati = "➡️ stable"

                        _mom_line = (
                            f"┆ {m['home']} → {_hti} {_hwp:.0f}% wins\n"
                            f"┆ {m['away']} → {_ati} {_awp:.0f}% wins\n"
                        )

                    # ── Strategy analysis ─────────────────────────────────────
                    _strategy_line = ""
                    _strategy_result = _strategy_analyze_match(
                        home         = m["home"],
                        away         = m["away"],
                        standings    = _standings_now,
                        model        = league_model,
                        hist_home_win_pct = _fc.get("home_win_pct", 0) if _fc_n >= 3 else 0,
                        hist_away_win_pct = _fc.get("away_win_pct", 0) if _fc_n >= 3 else 0,
                        hist_draw_pct     = _fc.get("draw_pct", 0)     if _fc_n >= 3 else 0,
                        hist_n            = _fc_n,
                        hist_btts_pct     = _rate("btts_result") or 0,
                    )
                    if _strategy_result and _strategy_result.get("history_agrees"):
                        _strategy_line = _strategy_result.get("card_line", "")
                        # Store for learning
                        round_preds[-1]["strategy_tip"]    = _strategy_result.get("strategy_tip")
                        round_preds[-1]["strategy_market"] = _strategy_result.get("market")
                        round_preds[-1]["strategy_pct"]    = _strategy_result.get("strategy_pct")
                        round_preds[-1]["history_pct"]     = _strategy_result.get("history_pct")

                    # ── Odds repeat — already detected at gate, reuse result ──
                    _repeat_line = ""
                    _repeat      = _repeat_chk  # already computed above as the gate
                    if _repeat.get("matched"):
                        _repeat_line = f"┆ {_repeat['star_label']}\n"
                        round_preds[-1]["odds_repeat"]             = True
                        round_preds[-1]["odds_repeat_pct"]         = _repeat.get("match_pct", 0)
                        round_preds[-1]["odds_repeat_outcome"]     = _repeat.get("outcome", "")
                        round_preds[-1]["odds_repeat_count"]       = _repeat.get("repeat_count", 0)
                        round_preds[-1]["odds_repeat_consistency"] = _repeat.get("consistency_pct", 0)
                        round_preds[-1]["odds_repeat_tier"]        = _repeat.get("tier", "")

                        # Boost overall score based on repeat tier
                        _consistency = _repeat.get("consistency_pct", 0)
                        _rep_pct     = _repeat.get("match_pct", 0)
                        if _rep_pct == 100 and _consistency == 100:
                            _overall = min(_overall + 15, 99)
                        elif _rep_pct == 100 and _consistency >= 67:
                            _overall = min(_overall + 10, 99)
                        elif _rep_pct >= 75 and _consistency >= 67:
                            _overall = min(_overall + 6, 99)
                        if   _overall >= 80: _overall_label = "🔥 STRONG PICK"
                        elif _overall >= 67: _overall_label = "✅ GOOD PICK"
                        elif _overall >= 50: _overall_label = "🟡 MODERATE"
                        else:                _overall_label = "🔴 WEAK SIGNAL"

                    # ── TRIPLE-CONFIRM FILTER (direction-agnostic) ───────────
                    # Only post picks where ALL confirm the tip direction:
                    #   1. Top-line has 🔥
                    #   2. Repeat dominant outcome matches tip (HOME or AWAY)
                    #   3. HT/FT verified in repeat AND outcome matches tip
                    #   4. Momentum: predicted winner win% >= 60 (stable/rising)
                    #               predicted loser  win% <= 30 (stable/falling)
                    _repeat_outcome   = _repeat.get("outcome", "")   # "HOME" / "AWAY"
                    _htft_in_repeat   = "HT/FT" in _repeat.get("markets_matched", [])
                    _top_fire         = bool(_fire)
                    _overall_confirms = (_tip_g in ("HOME", "AWAY")) and (_repeat_outcome == _tip_g)
                    _htft_agrees      = _htft_in_repeat and (_repeat_outcome == _tip_g)
                    # Momentum gate — based on each team's own win%, not tip direction
                    if _has_mom:
                        if _tip_g == "HOME":
                            _winner_wp = _hm_g.get("win_pct", 0)
                            _loser_wp  = _am_g.get("win_pct", 0)
                        else:
                            _winner_wp = _am_g.get("win_pct", 0)
                            _loser_wp  = _hm_g.get("win_pct", 0)
                        _mom_ok = (_winner_wp >= 60) and (_loser_wp <= 30)
                    else:
                        _mom_ok = True  # no momentum data yet — allow through

                    if not (_top_fire and _overall_confirms and _htft_agrees and _mom_ok):
                        continue  # ❌ Direction-confirm filter — skip this match

                    # ── LEAGUE PHASE GATE ─────────────────────────────────
                    # ── LEAGUE PHASE GATE ─────────────────────────────────
                    # The tip must align with the current league cycle phase.
                    # Phase is built from actual last-10-round win distribution.
                    # Only confirmed phase directions fire — NEUTRAL allows both.
                    _phase_label = league_model.get("phase_label", "NEUTRAL")
                    _phase_home  = league_model.get("phase_home_pct", 45)
                    _phase_away  = league_model.get("phase_away_pct", 30)
                    # Phase is confirmed HOME: only HOME tips fire
                    if _phase_label == "HOME" and _tip_g != "HOME":
                        continue  # ❌ League in HOME phase — tip must be HOME
                    # Phase is confirmed AWAY: only AWAY tips fire
                    if _phase_label == "AWAY" and _tip_g != "AWAY":
                        continue  # ❌ League in AWAY phase — tip must be AWAY
                    # Phase is DRAW or NEUTRAL: both HOME and AWAY allowed
                    # (DRAW phase = no strong direction, let fixture decide)

                    # ── Gate scores summary line ───────────────────────────────
                    _gate_line = (
                        f"┆ ━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"┆ G1 S-W:   {_g1_label}\n"
                        f"┆ G2 Band:  {_g2_label}\n"
                        f"┆ G3 Hist:  {_g3_label}\n"
                        f"┆ G4 Form:  {_g4_label}\n"
                        f"┆ 🎯 Overall: *{_overall}%* — {_overall_label}\n"
                        f"┆ ━━━━━━━━━━━━━━━━━━━━━━━\n"
                    )

                    # ── Final card ────────────────────────────────────────────
                    card = (
                        f"*{m['home']}  v  {m['away']}*\n"
                        f"{p['icon']} *{p['tip']}{_fire}*  {p['conf']:.0f}%{_hist_tag}{_svw_tag}\n"
                        f"🏠{p['hw']:.0f}%  🤝{p['dw']:.0f}%  ✈️{p['aw']:.0f}%\n"
                        + market_lines
                        + _fc_line
                        + _mom_line
                        + _strategy_line
                        + _repeat_line
                        + _gate_line
                        + f"{result_str}\n"
                    )
                    body += card + "─────────────────\n"
                    match_cards.append(card)

                if not body:
                    _had_events = True  # league had events but none passed filters
                    continue

                _had_events = True  # league had events AND predictions passed

                # Upcoming round never has scores yet — always awaiting results
                has_scores = False
                if has_scores and total:
                    acc      = round(agreed / total * 100)
                    acc_line = f"📊 *{agreed}/{total} correct ({acc}%)*\n"
                else:
                    acc_line = f"_{total} pick{'s' if total>1 else ''} — awaiting results_\n"

                flag = LEAGUES[lid]["flag"]
                name = LEAGUES[lid]["name"]

                # Build league header tag for each match card
                _league_header = f"{flag} *{name}*  MD{round_name}\n"

                sections.append({
                    "lid":        lid,
                    "round_id":   str(round_id),
                    "match_cards": match_cards,   # list of per-match card strings
                    "league_header": _league_header,
                    "text": (                     # kept for change-detection
                        _league_header
                        + body + acc_line
                    ),
                })

                # Queue predictions for learning once results come in
                if round_preds and round_id:
                    pending = bot_data.setdefault("pending_predictions", {})
                    lid_key = str(lid)
                    pending.setdefault(lid_key, {})[str(round_id)] = {
                        "preds":     round_preds,
                        "season_id": _cur_season_id or "",
                    }

            except Exception as exc:
                import traceback
                log.warning(f"auto_post lid={lid}: {exc}\n{traceback.format_exc()}")

    if not sections:
        if not _had_events:
            # Nothing evaluated at all this tick — completely silent
            return

        # Before sending skip — check if ANY league already sent predictions
        # for this same round_id. All leagues share the same round_id.
        # If some leagues sent predictions, other leagues with no picks
        # should stay silent — not fire a skip message.
        _sent_map_now = bot_data.get("auto_sent_per_league", {})
        _shared_rid   = round_ids[0].split(":")[-1] if round_ids else ""
        if not _shared_rid and round_ids:
            _shared_rid = round_ids[0].split(":")[-1]

        # Get the round_id from any processed league this tick
        _any_league_sent = any(
            v == _shared_rid
            for v in _sent_map_now.values()
        ) if _shared_rid else False

        if _any_league_sent:
            # Some leagues already sent predictions this round — stay silent
            return

        # Truly zero predictions sent anywhere — send one skip per round_id
        _skip_md  = Counter(matchday_nums).most_common(1)[0][0] if matchday_nums else "?"
        _skip_rid = _shared_rid or _skip_md
        _skip_key = f"auto_skip_notified_rid_{_skip_rid}"
        if not bot_data.get(_skip_key):
            bot_data[_skip_key] = True
            _skip_ts  = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M UTC")
            _skip_msg = (f"🤖 *MD{_skip_md}*  |  _{_skip_ts}_\n"
                         f"⏭️ No picks this round — no matches passed all filters")
            acc_s = _access(bot_data)
            _st_s = set()
            if ADMIN_ID: _st_s.add(str(ADMIN_ID))
            if CHANNEL_ID: _st_s.add(CHANNEL_ID)
            _st_s.update(acc_s.get("allowed_channels", set()))
            for _chat_s in _st_s:
                try:
                    await bot.send_message(chat_id=_chat_s, text=_skip_msg,
                                           parse_mode="Markdown")
                except Exception:
                    pass
            log.info(f"auto_post: skip notice sent for MD{_skip_md} rid={_skip_rid}")
        return

    # One string per league for change detection
    body_text      = "\n".join(s["text"] for s in sections)
    matchday_label = Counter(matchday_nums).most_common(1)[0][0] if matchday_nums else "?"

    round_key  = tuple(sorted(round_ids))
    prev_key   = bot_data.get("auto_round_key")
    # per-chat, per-league last message IDs: {"chat_id": {"lid": msg_id}}
    msg_ids    = bot_data.setdefault("auto_msg_ids", {})
    prev_body  = bot_data.get("auto_last_body", "")

    # ── Build send targets ─────────────────────────────────────────────────────
    acc = _access(bot_data)
    send_targets = set()

    if ADMIN_ID:
        send_targets.add(str(ADMIN_ID))
    if CHANNEL_ID:
        send_targets.add(CHANNEL_ID)
    send_targets.update(acc.get("allowed_channels", set()))

    chats = bot_data.get("auto_chats", set())
    for cid in chats:
        try:
            uid = int(cid)
        except (ValueError, TypeError):
            uid = 0
        if uid < 0:
            send_targets.add(cid)
        elif _is_authorized_user(uid, bot_data) or (ADMIN_ID and uid == ADMIN_ID):
            send_targets.add(cid)

    send_targets = list(send_targets)

    if not send_targets:
        log.info("auto_post: no send targets configured yet")
        return

    # ── Brain summary (sent once as first message) ─────────────────────────────
    def _brain_summary(bd: dict) -> str:
        lines = []
        for _lid in LEAGUES:
            m = bd.get(f"model_{_lid}")
            if not isinstance(m, dict) or m.get("rounds_learned", 0) == 0:
                continue
            cum  = m.get("cumulative", {})
            ot   = cum.get("outcome_total", 0)
            oc   = cum.get("outcome_correct", 0)
            acc  = (oc / ot * 100) if ot > 0 else m.get("outcome_acc", 0) * 100
            rds  = m.get("rounds_learned", 0)
            dot  = "🟢" if acc >= 65 else "🟡" if acc >= 50 else "🔴"
            _name = LEAGUES[_lid]["name"]
            lines.append(f"{dot} {_name}: {acc:.0f}% ({rds}r)")
        if not lines:
            return ""
        return "📈 *Learning Status*\n" + "  ".join(lines[:4]) + (
            ("\n" + "  ".join(lines[4:])) if len(lines) > 4 else ""
        ) + "\n━━━━━━━━━━━━━━━━━━━━━━\n"

    brain_block = _brain_summary(bot_data)
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M UTC")

    # ── NEW ROUND — send one message per match card ───────────────────────────
    if round_key != prev_key:
        any_sent = False
        # Track which league+round combos were already sent this session
        _sent_map = bot_data.setdefault("auto_sent_per_league", {})
        for chat in send_targets:
            chat_msgs = msg_ids.setdefault(str(chat), {})
            try:
                for sec in sections:
                    _lid_str  = str(sec["lid"])
                    _rid_str  = str(sec["round_id"])
                    # Skip if this exact league+round was already sent
                    if _sent_map.get(_lid_str) == _rid_str:
                        continue
                    _hdr   = sec["league_header"]
                    _cards = sec.get("match_cards", [])
                    _mid_list = []
                    for idx, card in enumerate(_cards):
                        msg_text = _hdr + card
                        sent = await bot.send_message(
                            chat_id=chat, text=msg_text, parse_mode="Markdown"
                        )
                        _mid_list.append(sent.message_id)
                        await asyncio.sleep(0.3)
                    chat_msgs[f"{sec['lid']}_cards"] = _mid_list
                    _sent_map[_lid_str] = _rid_str  # mark as sent

                    # Store card info + message IDs for result update later
                    # When previous round finishes, we edit these messages with ✅/❌
                    _card_infos = []
                    for idx, card in enumerate(_mid_list):
                        # Extract home/away/tip from round_preds if available
                        # We stored round_preds in pending_predictions
                        _card_infos.append({
                            "msg_id":    card,
                            "card_idx":  idx,
                        })
                    # Store round_preds per section for result lookup
                    _sec_preds = sec.get("round_preds_ref", [])
                    bot_data[f"sent_cards_{_lid_str}_{_rid_str}"] = [
                        {
                            "home":      pred.get("home", ""),
                            "away":      pred.get("away", ""),
                            "tip":       pred.get("tip", ""),
                            "card_text": _hdr + _cards[i] if i < len(_cards) else "",
                        }
                        for i, pred in enumerate(
                            (bot_data.get("pending_predictions", {})
                             .get(_lid_str, {})
                             .get(_rid_str, {})
                             .get("preds", []))
                        )
                    ]
                    # Store message IDs per chat for editing
                    for chat2 in send_targets:
                        _c2msgs = msg_ids.setdefault(str(chat2), {})
                        _c2msgs[f"{_lid_str}_prev_cards_{_rid_str}"] = \
                            msg_ids.get(str(chat), {}).get(f"{_lid_str}_cards", [])

                any_sent = True
                log.info(f"📨 New picks posted → {chat} ({sum(len(s.get('match_cards',[])) for s in sections)} matches, round_key={round_key})")
            except Exception as exc:
                log.warning(f"send failed → {chat}: {exc}")

        if any_sent:
            bot_data["auto_round_key"] = round_key
            bot_data["auto_last_body"] = body_text
        return

    # ── SAME ROUND — edit each match card in place when scores arrive ─────────
    if body_text != prev_body:
        ts_upd = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M UTC")
        for chat in send_targets:
            chat_msgs = msg_ids.get(str(chat), {})
            for sec in sections:
                _hdr      = sec["league_header"]
                _cards    = sec.get("match_cards", [])
                _mid_list = chat_msgs.get(f"{sec['lid']}_cards", [])
                for idx, card in enumerate(_cards):
                    msg_text = _hdr + card
                    if idx < len(_mid_list):
                        try:
                            await bot.edit_message_text(
                                chat_id=chat, message_id=_mid_list[idx],
                                text=msg_text,
                                parse_mode="Markdown"
                            )
                            await asyncio.sleep(0.3)
                        except Exception as exc:
                            if "Message is not modified" not in str(exc):
                                log.warning(f"edit failed → {chat} lid={sec['lid']} idx={idx}: {exc}")
                    else:
                        # New card that wasn't sent before — send fresh
                        try:
                            sent = await bot.send_message(
                                chat_id=chat, text=msg_text, parse_mode="Markdown"
                            )
                            _mid_list.append(sent.message_id)
                            chat_msgs[f"{sec['lid']}_cards"] = _mid_list
                        except Exception as exc:
                            log.warning(f"fresh send failed → {chat}: {exc}")
            log.info(f"✏️  Match cards updated with scores → {chat}")
        bot_data["auto_last_body"] = body_text

    # ── Update previously sent cards with results ──────────────────────────────
    # After previous round finishes, edit those cards to show ✅/❌ + score
    _pending_updates = bot_data.get("pending_result_updates", {})
    if _pending_updates:
        _processed_keys = []
        for _upd_key, _upd in list(_pending_updates.items()):
            _upd_lid     = _upd.get("lid")
            _upd_rid     = _upd.get("round_id")
            _upd_scores  = _upd.get("scores", {})
            if not _upd_scores:
                _processed_keys.append(_upd_key)
                continue

            # Find stored cards for this league+round
            _stored_cards = bot_data.get(f"sent_cards_{_upd_lid}_{_upd_rid}", [])
            if not _stored_cards:
                _processed_keys.append(_upd_key)
                continue

            for chat in send_targets:
                chat_msgs = msg_ids.get(str(chat), {})
                _mid_list = chat_msgs.get(f"{_upd_lid}_prev_cards_{_upd_rid}", [])
                for idx, card_info in enumerate(_stored_cards):
                    if idx >= len(_mid_list):
                        break
                    _h = card_info.get("home", "")
                    _a = card_info.get("away", "")
                    _tip = card_info.get("tip", "")
                    # Look up score
                    fk = _fixture_key(_h, _a)
                    score = (_upd_scores.get(fk) or
                             _upd_scores.get(f"{_h}|{_a}") or
                             _upd_scores.get(f"{_a}|{_h}"))
                    if not score:
                        continue
                    sh, sa = score
                    ft_out = "HOME" if sh > sa else "AWAY" if sh < sa else "DRAW"
                    tip_out = _tip.split()[0] if _tip else ""
                    ok = (tip_out == ft_out)
                    result_str = f"{'✅' if ok else '❌'} {sh}–{sa}"
                    # Rebuild card with result replacing ⏳ pending
                    new_card = card_info.get("card_text", "").replace(
                        "⏳ pending", result_str
                    )
                    if new_card and new_card != card_info.get("card_text", ""):
                        try:
                            await bot.edit_message_text(
                                chat_id=chat,
                                message_id=_mid_list[idx],
                                text=new_card,
                                parse_mode="Markdown"
                            )
                            await asyncio.sleep(0.2)
                        except Exception as exc:
                            if "Message is not modified" not in str(exc):
                                log.warning(f"result edit failed → {chat} lid={_upd_lid}: {exc}")

            _processed_keys.append(_upd_key)
            log.info(f"✅ Results updated for lid={_upd_lid} rid={_upd_rid}")

        # Clean up processed updates
        for k in _processed_keys:
            _pending_updates.pop(k, None)


async def _standings_job(context):
    """
    Dedicated standings refresh job — runs every 5 minutes independently.
    Computes standings from match_log (self-calculated, no API needed).
    Uses CURRENT SEASON ONLY so positions are always accurate.
    Refreshes every round (not every 10) so goals update immediately.
    """
    bot_data = context.bot_data

    for lid in LEAGUES:
        if lid not in ACTIVE_LEAGUES: continue
        try:
            league_model = _get_model(bot_data, lid)
            fp_db        = league_model.get("fingerprint_db", {})
            _ml          = league_model.get("match_log", [])

            # Get current season ID from model
            _cur_season  = str(league_model.get("_current_season_id", "") or "")

            # Filter to this league's teams only
            league_codes = LEAGUE_TEAMS.get(lid, set())
            if league_codes:
                fp_db = {
                    fk: recs for fk, recs in fp_db.items()
                    if all(p in league_codes for p in fk.split("|"))
                }

            # Compute standings from current season only
            # This ensures MD1 starts fresh with 0 goals for all teams
            computed = _compute_standings_from_fp_db(
                fp_db,
                season_id=_cur_season,
                match_log=_ml
            )
            rows = list(computed.values()) if computed else []

            # Fallback: if no current season data yet (very start of season)
            # use all-time standings until first round is learned
            if not rows and _ml:
                computed = _compute_standings_from_fp_db(fp_db, match_log=_ml)
                rows = list(computed.values()) if computed else []

            # Final fallback: stats cache
            if not rows:
                stats = bot_data.get(f"stats_{lid}", {})
                if stats:
                    _lcodes = LEAGUE_TEAMS.get(lid, set())
                    rows = []
                    for i, (name, st) in enumerate(
                        sorted(
                            ((n, s) for n, s in stats.items()
                             if not _lcodes or n.upper() in _lcodes),
                            key=lambda x: (-(x[1]["w"]*3 + x[1]["d"]),
                                           -(x[1]["gf"] - x[1]["ga"]))
                        ), 1
                    ):
                        rows.append(dict(pos=i, name=name,
                                         pts=st["w"]*3+st["d"],
                                         w=st["w"], d=st["d"],
                                         l=st.get("l",0),
                                         gf=st["gf"], ga=st["ga"],
                                         form=[]))

            if rows:
                bot_data[f"standings_{lid}"]        = {r["name"]: r for r in rows}
                bot_data[f"standings_rounds_{lid}"] = league_model.get("rounds_learned", 0)
                log.info(f"📊 Standings updated [{lid}]: {len(rows)} teams "
                         f"season={_cur_season or 'all'} "
                         f"(source={'match_log' if computed else 'stats'})")
        except Exception as _e:
            log.debug(f"standings_job [{lid}]: {_e}")


async def _auto_send_job(context):
    await _run_auto_post(context.bot, context.bot_data)

async def _data_collector_job(context):
    """
    Runs every 4 minutes — the brain's dedicated data intake pipe.

    Saves EVERY match from EVERY league unconditionally:
      • Current round (with live odds)
      • Last 5 completed past rounds (backfill)

    NO filtering. NO confidence gates. NO strong-vs-weak check.
    Filtering only exists in the display layer (_run_auto_post).

    Uses plain dicts/lists in bot_data — no sets (sets break PicklePersistence).
    Past rounds with scores are learned from IMMEDIATELY here rather than going
    through pending, so results are never lost between restarts.
    """
    import traceback
    bot_data = context.bot_data
    pending  = bot_data.setdefault("pending_predictions", {})

    # Use a plain dict as the "already collected" registry  {f"{lid}:{rid}": True}
    # Plain dict survives PicklePersistence round-trips correctly (sets do not).
    collected = bot_data.setdefault("_collected_map", {})

    log.info("📡 COLLECTOR: starting data collection tick")

    async with httpx.AsyncClient() as client:
        for lid, linfo in LEAGUES.items():
            name = linfo["name"]
            try:
                league_model     = _get_model(bot_data, lid)
                league_standings = bot_data.get(f"standings_{lid}", {})
                stats            = bot_data.get(f"stats_{lid}", {})

                # ── A) Current / upcoming round with odds ─────────────────────
                # Try upcoming first (has odds). If nothing, try live page.
                round_name = round_id = None
                _cur_season_id = ""
                events = []

                try:
                    round_name, round_id, _cur_season_id, events, _ = await fetch_next_round(client, lid)
                except Exception as e:
                    log.warning(f"COLLECTOR [{name}] fetch_next_round failed: {e}")

                if not events or not round_id:
                    # Upcoming returned nothing — try the currently live round
                    try:
                        rn, _, events = await fetch_live_round(client, lid)
                        if events:
                            # extract round_id from the first event
                            first_ev = events[0] if events else {}
                            round_id = str(first_ev.get("gameRoundId") or
                                          first_ev.get("roundId") or "")
                            round_name = rn
                    except Exception as e:
                        log.warning(f"COLLECTOR [{name}] fetch_live_round failed: {e}")

                if events and round_id:
                    ck = f"{lid}:{round_id}"

                    # ── ALWAYS save odds first — even if round already collected ──
                    # On fresh start the odds_store is empty. We must save all 5
                    # markets for every upcoming round immediately so they are ready
                    # when that round plays and outcomes come in.
                    # Odds are only saved if not already present for this round+fixture.
                    _os     = bot_data.setdefault("odds_store", {})
                    _lid_os = _os.setdefault(str(lid), {})
                    _rid_os = _lid_os.setdefault(str(round_id), {})
                    _odds_saved = 0

                    for raw in events:
                        try:
                            m       = _norm_event(raw)
                            ev_odds = _extract_odds(raw)
                            if not m["home"] or not m["away"]:
                                continue
                            if not ev_odds or not ev_odds.get("1x2"):
                                continue
                            _canon = "|".join(sorted([
                                m["home"].strip().upper(),
                                m["away"].strip().upper()
                            ]))
                            # Only save if not already stored for this round
                            # (never overwrite outcome/score that was already written)
                            if _canon not in _rid_os:
                                _rid_os[_canon] = {
                                    "odds_snapshot": ev_odds,
                                    "home":          m["home"],
                                    "away":          m["away"],
                                    "round_id":      str(round_id),
                                    "league_id":     lid,
                                    "season_id":     _cur_season_id or "",
                                    "saved_ts":      time.time(),
                                    "outcome":       None,
                                    "score_h":       None,
                                    "score_a":       None,
                                    "correct":       None,
                                }
                                _odds_saved += 1
                        except Exception as _oe:
                            log.warning(f"COLLECTOR [{name}] odds save error: {_oe}")

                    if _odds_saved:
                        log.info(f"💾 COLLECTOR [{name}] rnd={round_id}: saved {_odds_saved} odds entries")

                    # ── Queue predictions for learning — only once per round ──
                    if ck not in collected:
                        match_preds = []
                        for raw in events:
                            try:
                                m       = _norm_event(raw)
                                ev_odds = _extract_odds(raw)
                                if not m["home"] or not m["away"]:
                                    continue
                                p = predict_match(
                                    m["home"], m["away"], stats,
                                    ev_odds, league_model
                                )
                                match_preds.append({
                                    "home":           m["home"],
                                    "away":           m["away"],
                                    "tip":            p["tip"],
                                    "conf":           p.get("conf", 50.0),
                                    "odds_tip":       p.get("odds_tip"),
                                    "poisson_tip":    p.get("poisson_tip"),
                                    "strength_tip":   p.get("strength_tip"),
                                    "pred_h":         p["exp_h"],
                                    "pred_a":         p["exp_a"],
                                    "btts_pred":      p.get("btts", 50.0) >= 50,
                                    "btts_prob":      p.get("btts", 50.0),
                                    "over25_pred":    p.get("over25", 50.0) >= 50,
                                    "over25_prob":    p.get("over25", 50.0),
                                    "prob_H":         p.get("prob_H", 0.45),
                                    "prob_D":         p.get("prob_D", 0.27),
                                    "prob_A":         p.get("prob_A", 0.28),
                                    "round_id":       int(round_id) if str(round_id).isdigit() else 0,
                                    "_odds_snapshot": ev_odds,
                                    "_skipped":       False,
                                    "_skip_reason":   "",
                                    "_lead_market":   "1x2",
                                })
                            except Exception as em:
                                log.warning(f"COLLECTOR [{name}] match error: {em}")

                        if match_preds:
                            pending.setdefault(str(lid), {})[str(round_id)] = {
                                "preds":     match_preds,
                                "season_id": _cur_season_id or "",
                            }
                            collected[ck] = 1
                            log.info(
                                f"📥 COLLECTOR [{name}] rnd={round_id} "
                                f"queued {len(match_preds)} matches for learning"
                            )
                    else:
                        log.info(f"📡 COLLECTOR [{name}] rnd={round_id} already queued, skipping")


                # ── B) Backfill past rounds — fill every gap in match_log ─────
                # The old approach skipped backfill once fp_db had >= 10 rounds,
                # meaning the bot permanently fell behind after any restart.
                # New approach: compare the API's list of past round_ids against
                # what's already in match_log.  Only rounds that are MISSING from
                # match_log are fetched.  Cap at 5 per tick to avoid rate-limit
                # bursts, but the cap resets every tick so over several minutes
                # the bot catches up to the full season history.
                try:
                    past_rounds = await fetch_round_list(client, lid, past=True)

                    # What round_ids do we already have in match_log (authoritative)?
                    _ml = league_model.get("match_log", [])
                    ml_rids = {str(r.get("round_id", "")) for r in _ml if r.get("round_id")}

                    # Also accept rounds already processed this session via _collected_map
                    api_rids  = [str(r.get("id") or r.get("gameRoundId") or "") for r in past_rounds]
                    api_rids  = [x for x in api_rids if x]
                    missing   = [rid for rid in api_rids if rid not in ml_rids
                                 and f"{lid}:bf:{rid}" not in collected]

                    log.info(
                        f"📚 COLLECTOR [{name}]: API={len(api_rids)} past rounds, "
                        f"match_log={len(ml_rids)}, missing={len(missing)}"
                    )

                    # Process at most 5 missing rounds per tick (oldest-first so
                    # standings build up chronologically)
                    backfilled   = 0
                    _max_per_tick = 5
                    # Build a quick lookup: rid → round dict (for seasonId)
                    rid_to_round = {
                        str(r.get("id") or r.get("gameRoundId") or ""): r
                        for r in past_rounds
                    }
                    for rid in sorted(missing, key=lambda x: int(x) if x.isdigit() else 0):
                        if backfilled >= _max_per_tick:
                            break
                        r  = rid_to_round.get(rid, {})
                        bk = f"{lid}:bf:{rid}"

                        # Fetch scores (matchups page)
                        evs_sc = await fetch_round_events(client, rid, PAGE_MATCHUPS)
                        evs_sc = _filter_league(evs_sc, lid)
                        scored = [e for e in evs_sc if _norm_event(e)["hs"] is not None]
                        if not scored:
                            # No scores yet (round still pending) — mark done so we skip next tick
                            collected[bk] = 1
                            continue

                        # Fetch odds snapshot for this past round (upcoming page)
                        evs_up  = await fetch_round_events(client, rid, PAGE_UPCOMING)
                        evs_up  = _filter_league(evs_up, lid)
                        odds_map = {
                            _fixture_key(_norm_event(e)["home"], _norm_event(e)["away"]): _extract_odds(e)
                            for e in evs_up
                        }

                        # Build predictions + results and learn immediately
                        preds   = []
                        results = []
                        for e in scored:
                            nm      = _norm_event(e)
                            fk      = _fixture_key(nm["home"], nm["away"])
                            ev_odds = odds_map.get(fk, {})
                            p = predict_match(
                                nm["home"], nm["away"], stats,
                                ev_odds, league_model
                            )
                            preds.append({
                                "home":           nm["home"],
                                "away":           nm["away"],
                                "tip":            p["tip"],
                                "conf":           p.get("conf", 50.0),
                                "btts_pred":      p.get("btts", 50.0) >= 50,
                                "btts_prob":      p.get("btts", 50.0),
                                "over25_pred":    p.get("over25", 50.0) >= 50,
                                "over25_prob":    p.get("over25", 50.0),
                                "_odds_snapshot": ev_odds,
                            })
                            ht_h, ht_a = _extract_ht_score(e)
                            results.append({
                                "home":     nm["home"],
                                "away":     nm["away"],
                                "actual_h": nm["hs"],
                                "actual_a": nm["as_"],
                                "ht_h":     ht_h,
                                "ht_a":     ht_a,
                            })

                        if preds and results:
                            # Use this past round's own season_id from the API.
                            # Do NOT fall back to _cur_season_id — past rounds may
                            # belong to a previous season.
                            _past_season_id = str(r.get("_seasonId") or r.get("seasonId") or "")
                            _learn_from_round(
                                bot_data, lid, preds, results,
                                round_id=int(rid) if rid.isdigit() else 0,
                                season_id=_past_season_id
                            )
                            _learn_algo_signals(
                                _get_model(bot_data, lid), preds, results,
                                round_id_int=int(rid) if rid.isdigit() else 0
                            )
                            _ai_postmatch_analysis(
                                _get_model(bot_data, lid), preds, results,
                                standings=bot_data.get(f"standings_{lid}"),
                                round_id=int(rid) if rid.isdigit() else 0,
                            )
                            collected[bk] = 1
                            backfilled += 1
                            log.info(
                                f"📚 COLLECTOR BACKFILL [{name}] rnd={rid} "
                                f"learned {len(results)} matches (gap-fill)"
                            )

                except Exception as e:
                    log.warning(
                        f"COLLECTOR backfill [{name}]: {e}\n{traceback.format_exc()}"
                    )
            except Exception as outer:
                log.warning(
                    f"COLLECTOR outer error [{name}]: {outer}\n{traceback.format_exc()}"
                )

    # Trim collected map so it doesn't grow forever (keep last 600 entries by round_id)
    # Sort by the numeric round_id embedded in the key so oldest rounds are dropped first.
    # Keys have format "{lid}:{round_id}" or "{lid}:bf:{round_id}".
    if len(collected) > 600:
        def _cmap_sort_key(k: str) -> int:
            try:
                parts = k.split(":")
                return int(parts[-1])   # last segment is always the round_id (or bf round_id)
            except Exception:
                return 0
        _sorted_keys = sorted(collected.keys(), key=_cmap_sort_key)
        # Keep the 600 most recent (highest round_ids)
        bot_data["_collected_map"] = {k: 1 for k in _sorted_keys[-600:]}

    log.info(
        f"📡 COLLECTOR tick done — "
        f"pending_rounds={sum(len(v) for v in pending.values())} "
        f"collected_map={len(collected)}"
    )


async def _stats_loader_job(context):
    """Background job: refreshes stats AND standings for all leagues.
    Runs every 55 minutes. Never blocks commands."""
    class _FakeCtx:
        def __init__(self, bd): self.bot_data = bd
    ctx = _FakeCtx(context.bot_data)
    async with httpx.AsyncClient() as client:
        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            try:
                await _ensure_stats(ctx, lid, client)
                log.info(f"📦 Stats refreshed for league {lid}")
            except Exception as e:
                log.warning(f"Stats loader failed for {lid}: {e}")
            # ── Refresh standings for tier filter (once per 55 min is enough) ──
            try:
                _sn, _rn, std_rows = await fetch_standings(client, lid)
                if std_rows:
                    std_map = {r["name"]: r for r in std_rows}
                    context.bot_data[f"standings_{lid}"] = std_map
                    log.info(f"📊 Standings cached for league {lid}: {len(std_map)} teams")
            except Exception as _se:
                pass  # keep existing if fetch fails

def _ai_brain_status_line(model: dict) -> str:
    """
    AI brain diagnostics for the brainstat display.
    Uses the new fixture_mem / signal_acc / tier_acc / band_acc structures.
    """
    ai = model.get("ai_brain", {})
    if not ai:
        return "│ 🤖 AI Brain: learning (no data yet)\n"

    signal_acc  = ai.get("signal_acc",  {})
    tier_acc    = ai.get("tier_acc",    {})
    band_acc    = ai.get("band_acc",    {})
    fixture_mem = ai.get("fixture_mem", {})
    ai_log      = ai.get("ai_log",      [])
    intelligence = ai.get("intelligence", {})
    ai_weights  = model.get("weights",  {})

    def _sacc(sig):
        r = signal_acc.get(sig, {})
        t = r.get("total", 0)
        c = r.get("correct", 0)
        rec = r.get("recent", [])
        if t < 5: return "—"
        cumul = f"{c/t:.0%}({c}/{t})"
        if len(rec) >= 10:
            r10 = sum(rec[-10:]) / 10
            return f"{cumul} r10={r10:.0%}"
        return cumul

    # Tier S-W accuracy (only valid matchups)
    sw_correct = sw_total = 0
    for pair, rec in tier_acc.items():
        if "STRONG" in pair and ("WEAK" in pair or "MODERATE" in pair):
            sw_correct += rec["correct"]
            sw_total   += rec["total"]
    tier_str = (f"{sw_correct/sw_total:.0%} ({sw_correct}/{sw_total})"
                if sw_total >= 5 else "building…")

    # Best odds band
    best_band = best_band_a = None
    for band, rec in band_acc.items():
        if rec["total"] >= 12:
            a = rec["correct"] / rec["total"]
            if best_band_a is None or a > best_band_a:
                best_band_a = a; best_band = band

    # Fixture memory stats
    total_fixtures = len(fixture_mem)
    mature  = sum(1 for m in fixture_mem.values() if len(m) >= 6)
    veteran = sum(1 for m in fixture_mem.values() if len(m) >= 10)

    # Active lessons: fixtures where AI has a confirmed lesson ready
    active_lessons = 0
    for fk, mem in fixture_mem.items():
        if len(mem) >= 6:
            recent10 = mem[-10:]
            wrongs   = [r for r in recent10 if not r["correct"]]
            if len(wrongs) / len(recent10) >= 0.40:
                active_lessons += 1

    # Most recent wrong pick diagnosis
    last_diag = ""
    if ai_log:
        last = ai_log[-1]
        last_diag = (
            f"│ 🔍 Last mistake: *{last.get('match','?')}*  "
            f"{last.get('predicted','?')}→actual {last.get('actual','?')}  "
            f"blamed: *{last.get('primary_blame','?')}*\n"
        )

    w = ai_weights
    wstr = (f"Odds {w.get('odds',0):.0%}  "
            f"FP {w.get('fingerprint',0):.0%}  "
            f"Tier {w.get('tier',0):.0%}  "
            f"Form {w.get('form',0):.0%}")

    # Rounds evaluated by full AI re-eval
    ai_evals = intelligence.get("rounds_evaluated", 0)

    # All odds bands summary
    band_lines = []
    for band in ("<1.30", "1.30-1.60", "1.60-1.90", "1.90-2.30", "2.30+"):
        rec = band_acc.get(band, {})
        t = rec.get("total", 0)
        if t >= 8:
            a = rec["correct"] / t
            bar = "█" * int(a * 10) + "░" * (10 - int(a * 10))
            band_lines.append(f"│    {band:12s} {bar} {a:.0%} ({t})")

    # Recent wrong picks — last 3
    recent_wrong_lines = []
    for entry in (ai_log[-3:] if ai_log else []):
        recent_wrong_lines.append(
            f"│    R{entry.get('round','?')} {entry.get('match','?')[:18]:18s} "
            f"pred={entry.get('predicted','?')} actual={entry.get('actual','?')} "
            f"← {entry.get('primary_blame','?')}"
        )

    # Per-tier pair breakdown
    tier_pair_lines = []
    for pair in sorted(tier_acc.keys()):
        rec = tier_acc[pair]
        t = rec.get("total", 0)
        if t >= 5:
            a = rec["correct"] / t
            tier_pair_lines.append(f"│    {pair:30s} {a:.0%} ({rec['correct']}/{t})")

    # Recovery engine accuracy
    rec_acc   = ai.get("recovery_acc", {})
    def _racc(sig):
        r = rec_acc.get(sig, {}); t = r.get("total",0); c = r.get("correct",0)
        if t < 5: return f"—({t} samples)"
        recent = r.get("recent", [])
        r10 = f" r10={sum(recent[-10:])/10:.0%}" if len(recent) >= 10 else ""
        return f"{c/t:.0%}({c}/{t}){r10}"

    line = (
        f"│ 🤖 *AI BRAIN — What it has learned*\n"
        f"│{'─'*38}\n"
        f"│ 📚 Fixture memory: {total_fixtures} fixtures tracked\n"
        f"│    • {mature} mature (6+ records) — AI can form opinions\n"
        f"│    • {veteran} veteran (10+ records) — AI acts confidently\n"
        f"│    • {active_lessons} active correction lessons ready\n"
        f"│\n"
        f"│ 🔄 *Recovery engine accuracy*\n"
        f"│    STRONG_RECOVERS: {_racc('STRONG_RECOVERS')}\n"
        f"│    WEAK_REPEATS:    {_racc('WEAK_REPEATS')}\n"
        f"│\n"
        f"│ ⚡ *Signal accuracy (what it learned works)*\n"
        f"│    Odds:        {_sacc('odds')}\n"
        f"│    Fingerprint: {_sacc('fingerprint')}\n"
        f"│    Tier (S-W):  {_sacc('tier')} — S-W matchups: {tier_str}\n"
        f"│    Form:        {_sacc('form')}\n"
        f"│\n"
        f"│ ⚖️  *Current AI weights (auto-adjusted every 10 rounds)*\n"
        f"│    {wstr}\n"
        + (f"│    (last AI re-evaluation: round #{ai_evals})\n" if ai_evals else "")
        + f"│\n"
        + (f"│ 🎯 *Odds-band accuracy*\n" + "\n".join(band_lines) + "\n│\n" if band_lines else "")
        + (f"│ 🏆 *Tier-pair accuracy*\n" + "\n".join(tier_pair_lines) + "\n│\n" if tier_pair_lines else "")
        + (f"│ 🔴 *Recent mistakes (AI is learning from these)*\n" + "\n".join(recent_wrong_lines) + "\n" if recent_wrong_lines else "")
    )
    return line

def _algo_status_line(model: dict) -> str:
    """Generate a status line showing algorithm reverse-engineering engine health."""
    parts = []

    # Engine 1: Rebalancing
    reb_len = len(model.get("rebalance", {}).get("window", []))
    if reb_len >= 15:
        window = model["rebalance"]["window"][-30:]
        dev_max = max(
            abs(sum(r["H_act"] - r["H_exp"] for r in window) / len(window)),
            abs(sum(r["D_act"] - r["D_exp"] for r in window) / len(window)),
            abs(sum(r["A_act"] - r["A_exp"] for r in window) / len(window)),
        )
        parts.append(f"Reb:{reb_len}r/{dev_max:.0%}dev")
    else:
        parts.append(f"Reb:building({reb_len}/15)")

    # Engine 2: Fixture cycles
    n_cycles = sum(
        1 for fk, pm in model.get("pattern_memory", {}).items()
        if _detect_cycle(pm.get("outcome_seq", []))[1] >= 0.80
    )
    if n_cycles:
        parts.append(f"Cycles:{n_cycles}🔁")

    # Engine 3: Round-ID modulo patterns
    best_mod_conf = 0.0
    best_mod_desc = None
    rmp = model.get("round_mod_patterns", {})
    for mod_s, mod_data in rmp.items():
        for rem_s, counts in mod_data.items():
            total = counts.get("H", 0) + counts.get("D", 0) + counts.get("A", 0)
            if total < 8:
                continue
            best_out  = max(counts, key=lambda k: counts.get(k, 0))
            best_conf = counts[best_out] / total
            if best_conf > best_mod_conf:
                best_mod_conf = best_conf
                best_mod_desc = f"mod{mod_s}={best_conf:.0%}"
    if best_mod_desc:
        parts.append(f"ModPat:{best_mod_desc}")

    # Engine 4: Slot patterns
    slot_tracker = model.get("slot_outcomes", {})
    n_slot_signals = sum(
        1 for slot_key, seq in slot_tracker.items()
        if _get_slot_signal(model, int(slot_key))[1] >= 0.78
    )
    if n_slot_signals:
        parts.append(f"Slots:{n_slot_signals}🎯")

    # Engine 5: Odds trap zones
    trap_data = model.get("odds_trap", {})
    n_traps = 0
    for band, band_data in trap_data.items():
        for out, od in band_data.items():
            total = od.get("total", 0)
            if total >= 15:
                failure = 1.0 - od.get("correct", 0) / total
                if failure > 0.60:   # 60%+ failure = confirmed trap
                    n_traps += 1
    if n_traps:
        parts.append(f"Traps:{n_traps}🚨")

    if not parts:
        return "│ 🔬 Algo engines: warming up...\n"
    return f"│ 🔬 *Algo:* {' │ '.join(parts)}\n"


async def _do_rawstatus(message, c):
    """
    Cold-start visibility dashboard — shows what the bot actually sees and stores
    right now, even when it has learned zero rounds.

    Sections:
      1. Cold/Warm/Hot banner — instant read on brain state
      2. Live matches RIGHT NOW across all leagues with current scores + goals
      3. Pending learning queue — what's been stored but not yet learned from
      4. Per-league storage health — fingerprint count, confirmed patterns, failure rules
    """
    bd      = c.bot_data
    pending = bd.get("pending_predictions", {})
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # ── 1. Banner ─────────────────────────────────────────────────────────────
    total_rounds = sum(
        bd.get(f"model_{lid}", {}).get("rounds_learned", 0)
        for lid in LEAGUES
    )
    total_fp = sum(
        sum(len(v) for v in bd.get(f"model_{lid}", {}).get("fingerprint_db", {}).values())
        for lid in LEAGUES
    )
    total_pending_rounds = sum(len(rounds) for rounds in pending.values())
    total_pending_preds  = sum(
        len(preds)
        for rounds in pending.values()
        for preds in rounds.values()
    )

    if total_rounds == 0 and total_fp == 0 and total_pending_rounds == 0:
        banner = (
            "🔴 *COLD START — Brain completely empty*\n"
            "No data stored yet. The bot is watching matches now.\n"
            "➡️ First predictions go into *Pending* after the next round starts.\n"
            "➡️ Learning happens ~6 min after a round's results are available.\n"
        )
    elif total_rounds == 0 and total_pending_rounds > 0:
        banner = (
            f"🟡 *COLLECTING — {total_pending_preds} predictions stored, waiting for results*\n"
            f"Rounds are being recorded. Learning starts once results arrive.\n"
        )
    elif total_rounds < 5:
        banner = (
            f"🟡 *WARMING UP — {total_rounds} rounds learned · {total_fp} fingerprints*\n"
            f"Brain is building. Accuracy improves with every round.\n"
        )
    else:
        mature_fixtures = sum(
            sum(1 for v in bd.get(f"model_{lid}", {}).get("fingerprint_db", {}).values()
                if isinstance(v, list) and len(v) >= 3)
            for lid in LEAGUES
        )
        banner = (
            f"🟢 *BRAIN ACTIVE — {total_rounds} rounds · {total_fp} fingerprints · {mature_fixtures} mature fixtures*\n"
        )

    # ── 2. Live matches across all leagues ────────────────────────────────────
    live_lines = []
    wait = await message.reply_text("⏳ Fetching live matches across all 7 leagues…")
    any_live = False

    try:
        async with httpx.AsyncClient() as client:
            for lid, linfo in LEAGUES.items():
                try:
                    rname, _, events = await fetch_live_round(client, lid)
                    if not events:
                        live_lines.append(
                            f"{linfo['flag']} *{linfo['name']}*: _(no active round)_"
                        )
                        continue

                    scored   = [(e, _norm_event(e)) for e in events
                                if _norm_event(e)["hs"] is not None]
                    pending_ = [e for e in events
                                if _norm_event(e)["hs"] is None]

                    total_goals = sum(m["hs"] + m["as_"] for _, m in scored)
                    avg_goals   = total_goals / len(scored) if scored else 0.0
                    has_results = len(scored)

                    header_line = (
                        f"{linfo['flag']} *{linfo['name']}* · Rnd {rname}  "
                        f"_{has_results}/{len(events)} scored · "
                        f"{total_goals}⚽ · avg {avg_goals:.1f}_"
                    )
                    live_lines.append(header_line)

                    # Scored matches with result
                    for _, m in scored:
                        hs = m["hs"]; as_ = m["as_"]
                        if hs > as_:    outcome = "🏠"
                        elif hs < as_:  outcome = "✈️"
                        else:           outcome = "🤝"
                        total = hs + as_
                        over  = "⬆️" if total > 2 else ("⬇️" if total < 2 else "")
                        live_lines.append(
                            f"  {outcome} {m['home']} *{hs}–{as_}* {m['away']}  "
                            f"({total}⚽{over})"
                        )

                    # Pending matches (no score yet)
                    for e in pending_:
                        m = _norm_event(e)
                        live_lines.append(
                            f"  ⏳ {m['home']} vs {m['away']} _(waiting)_"
                        )

                    any_live = any_live or bool(scored)

                except Exception as ex:
                    live_lines.append(
                        f"{linfo['flag']} *{linfo['name']}*: ❌ fetch error"
                    )
                    log.warning(f"rawstatus live fetch error lid={lid}: {ex}")
    except Exception as ex:
        live_lines.append(f"❌ Live fetch completely failed: {ex}")

    await wait.delete()

    # ── 3. Pending learning queue ─────────────────────────────────────────────
    pend_lines = []
    if total_pending_rounds == 0:
        pend_lines.append(
            "📭 Nothing pending\n"
            "_(Either no round has finished yet, or all learned already)_"
        )
    else:
        pend_lines.append(
            f"⏳ *{total_pending_rounds} rounds · {total_pending_preds} predictions* "
            f"stored — waiting for results to arrive"
        )
        for lid_str, rounds in pending.items():
            try:   linfo2 = LEAGUES[int(lid_str)]
            except Exception: linfo2 = {"flag":"", "name": lid_str}
            for rid, preds in rounds.items():
                pend_lines.append(
                    f"  {linfo2['flag']} {linfo2['name']} · "
                    f"Round {rid} · {len(preds)} matches stored, not yet scored"
                )

    # ── 4. Per-league storage health ─────────────────────────────────────────
    store_lines = []
    for lid, linfo in LEAGUES.items():
        model  = bd.get(f"model_{lid}", {})
        rds    = model.get("rounds_learned", 0)
        fp_db  = model.get("fingerprint_db", {})
        n_fix  = len(fp_db)
        n_rec  = sum(len(v) for v in fp_db.values())
        n_mature = sum(1 for v in fp_db.values() if isinstance(v, list) and len(v) >= 3)
        n_pend = sum(
            len(preds)
            for preds in pending.get(str(lid), {}).values()
        )
        acc = model.get("outcome_acc", 0.0)

        if rds == 0 and n_rec == 0 and n_pend == 0:
            dot    = "⚫"
            detail = "nothing stored yet"
        elif rds == 0:
            dot    = "🟡"
            detail = (
                f"{n_rec} records in DB · {n_pend} pending · "
                f"not yet learned from"
            )
        else:
            dot    = "🟢" if acc >= 0.60 else "🟡"
            detail = (
                f"{rds}r · {n_fix} fixtures · {n_rec} records · "
                f"🔒{n_mature} mature · {acc:.0%}acc"
            )
        store_lines.append(f"{dot} {linfo['flag']} *{linfo['name']}*: {detail}")

    # ── Assemble ──────────────────────────────────────────────────────────────
    ts = now_utc.strftime("%H:%M UTC")
    text = (
        f"📡 *Raw Status*  _{ts}_\n"
        f"{'━'*26}\n"
        f"{banner}"
        f"{'━'*26}\n\n"
        f"🔴 *Live Matches Right Now*\n"
        + "\n".join(live_lines) + "\n\n"
        f"{'━'*26}\n"
        f"⏳ *Pending Learning Queue*\n"
        + "\n".join(pend_lines) + "\n\n"
        f"{'━'*26}\n"
        f"🗄 *Storage Health*\n"
        + "\n".join(store_lines) + "\n"
        f"{'━'*26}\n"
        f"_Tap_ 📡 _Raw Status again to refresh_"
    )
    await _send(message, text, parse_mode="Markdown")


async def cmd_rawstatus(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.")
        return
    await _do_rawstatus(u.message, c)


async def _do_brainstat(message, c):

    def _training_score(model: dict) -> float:
        """
        Overall training score (0-100%):
          30% - 1X2 accuracy (cumulative)
          15% - BTTS accuracy
          10% - O2.5 accuracy
          15% - fixture pattern depth (3+ meetings)
          10% - rounds of experience (caps at 100)
          10% - learning velocity (recent 10 rounds vs overall)
          10% - signal convergence (best signal vs random)
        """
        cum   = model.get("cumulative", {})
        ot    = cum.get("outcome_total", 0)
        acc   = (cum["outcome_correct"] / ot) if ot > 0 else model.get("outcome_acc", 0.0)
        bt    = cum.get("btts_total", 0)
        btts_a = (cum["btts_correct"] / bt) if bt > 0 else model.get("btts_acc", 0.0)
        o25t  = cum.get("over25_total", 0)
        o25_a = (cum["over25_correct"] / o25t) if o25t > 0 else model.get("over25_acc", 0.0)
        deep_p = sum(1 for v in model.get("pattern_memory", {}).values() if v.get("n", 0) >= 3)
        patterns  = min(deep_p, 100) / 100
        rounds    = min(model.get("rounds_learned", 0), 100) / 100
        r10_acc   = model.get("recent_10_acc", 0.0)
        r10_total = model.get("recent_10_total", 0)
        velocity  = max(0.0, min(1.0, (r10_acc - acc + 0.10) / 0.20)) if r10_total >= 20 else 0.5
        sig = model.get("signal_acc", {})
        sig_accs = [sig[s]["correct"] / sig[s]["total"]
                    for s in ("odds","poisson","strength") if sig.get(s,{}).get("total",0) >= 3]
        convergence = max(0.0, min(1.0, (max(sig_accs) - 0.33) / 0.50)) if sig_accs else 0
        raw = (0.30*acc) + (0.15*btts_a) + (0.10*o25_a) + (0.15*patterns) +               (0.10*rounds) + (0.10*velocity) + (0.10*convergence)
        return round(min(raw, 1.0), 4)

    def _bar(value: float, width: int = 10) -> str:
        filled = round(value * width)
        return "█" * filled + "░" * (width - filled)

    def _status_icon(score: float) -> str:
        if score >= 0.80: return "🟢"
        if score >= 0.55: return "🟡"
        if score >= 0.20: return "🔴"
        return "⚫"

    def _readiness(score: float) -> str:
        if score >= 0.80: return "MASTERY ✅"
        if score >= 0.65: return "HIGH — Strong predictions"
        if score >= 0.50: return "GOOD — Learning well"
        if score >= 0.35: return "BUILDING — Needs more rounds"
        if score >= 0.20: return "EARLY — Just started"
        return                   "NOT STARTED"

    def _trend(model: dict) -> str:
        r10  = model.get("recent_10_acc", 0.0)
        r10t = model.get("recent_10_total", 0)
        overall = model.get("outcome_acc", 0.0)
        if r10t < 20: return "➡️ building"
        diff = r10 - overall
        if diff >  0.04: return f"📈 +{diff:.0%} improving"
        if diff < -0.04: return f"📉 {diff:.0%} dipping"
        return f"➡️ stable ({r10:.0%})"

    def sp(sig, s):
        r = sig.get(s, {}); t = r.get("total", 0)
        c_val = r.get("correct", 0)
        return f"{c_val/t:.0%}({c_val}/{t})" if t else "—"

    league_scores, league_lines = [], []

    for lid, linfo in LEAGUES.items():
        model = c.bot_data.get(f"model_{lid}")
        if not model or model.get("rounds_learned", 0) == 0:
            league_lines.append(f"┌ {linfo['flag']} *{linfo['name']}*\n└ ⚫ No data yet\n")
            continue

        cum   = model.get("cumulative", {})
        ot    = cum.get("outcome_total", 0)
        oc    = cum.get("outcome_correct", 0)
        bt    = cum.get("btts_total", 0)
        bc    = cum.get("btts_correct", 0)
        o25t  = cum.get("over25_total", 0)
        o25c  = cum.get("over25_correct", 0)
        mt    = cum.get("matches_total", 0) or 1
        hw_n  = cum.get("home_wins", 0)
        dw_n  = cum.get("draws", 0)
        aw_n  = cum.get("away_wins", 0)
        acc   = (oc / ot)   if ot   > 0 else model.get("outcome_acc", 0)
        btts_a= (bc / bt)   if bt   > 0 else model.get("btts_acc", 0)
        o25_a = (o25c/o25t) if o25t > 0 else model.get("over25_acc", 0)

        rounds    = model.get("rounds_learned", 0)
        avg_g     = model.get("avg_goals", 2.5)
        sig       = model.get("signal_acc", {})
        b_rate    = model.get("btts_rate", 0.5)
        o_rate    = model.get("over25_rate", 0.5)
        calib     = model.get("conf_calibration", 0.0)
        hcm       = model.get("high_conf_mistakes", 0)
        r10_acc   = model.get("recent_10_acc", 0.0)
        r10_total = model.get("recent_10_total", 0)
        deep_p    = sum(1 for v in model.get("pattern_memory", {}).values() if v.get("n",0) >= 3)
        w         = model.get("weights", {})
        ts        = _training_score(model)
        league_scores.append(ts)

        # Best and worst signal
        best_sig = worst_sig = "—"
        best_acc = 0.0; worst_acc = 1.0
        for s in ("odds", "poisson", "strength"):
            r = sig.get(s, {}); t = r.get("total", 0)
            if t >= 10:
                a = r["correct"] / t
                if a > best_acc:  best_acc  = a; best_sig  = s.capitalize()
                if a < worst_acc: worst_acc = a; worst_sig = s.capitalize()

        # Per-outcome type accuracy
        ota = model.get("outcome_type_acc", {})
        def _ota_str(k):
            r = ota.get(k, {}); t = r.get("total", 0); c2 = r.get("correct", 0)
            return f"{c2/t:.0%}" if t >= 5 else "?"

        # Margin band — find the best performing confidence band (20+ real samples)
        margin_acc = model.get("margin_acc", {})
        best_band = best_band_acc = None
        for band, (bc2, bt2) in margin_acc.items():
            if bt2 >= 20:
                ba = bc2 / bt2
                if best_band_acc is None or ba > best_band_acc:
                    best_band_acc = ba; best_band = band

        calib_lbl = ("🔵 calibrated" if abs(calib) <= 2
                     else ("🔺 overconfident" if calib > 0 else "🔻 underconfident"))

        league_lines.append(
            f"┌ {linfo['flag']} *{linfo['name']}*  {_status_icon(ts)} *{ts*100:.0f}%*\n"
            f"│ {_bar(ts)} → 100%\n"
            f"│\n"
            f"│ 📚 *Experience:* {rounds} rounds  |  {mt} games\n"
            f"│ 🎯 *1X2:*  {acc:.1%} ({oc}/{ot})  {_bar(acc)}\n"
            f"│ 🎯 *BTTS:* {btts_a:.1%} ({bc}/{bt})  *O2.5:* {o25_a:.1%} ({o25c}/{o25t})\n"
            f"│ 📊 *Trend:* {_trend(model)}\n"
            f"│ 🏠H:{_ota_str('HOME')} 🤝D:{_ota_str('DRAW')} ✈️A:{_ota_str('AWAY')}  (tip accuracy by type)\n"
            f"│ ⚽ avg {avg_g:.1f}g  dist: 🏠{hw_n/mt:.0%} 🤝{dw_n/mt:.0%} ✈️{aw_n/mt:.0%}\n"
            f"│ 📡 Best signal: *{best_sig}* {best_acc:.0%}  |  {calib_lbl}\n"
            f"│ 🧩 Signals — Odds:{sp(sig,'odds')} Poisson:{sp(sig,'poisson')} Str:{sp(sig,'strength')}\n"
            f"│ 🗄 Patterns: {deep_p} fixtures with 3+ meetings\n"
            + (f"│ 🔥 Sweet spot: ~{best_band}% confidence → {best_band_acc:.0%} correct\n" if best_band else "")
            + (f"│ ⚠️ High-conf mistakes: {hcm}\n" if hcm >= 3 else "")
            + f"│ ⚖️ Wts: Odds {w.get('odds',0):.0%}  Pois {w.get('poisson',0):.0%}  Str {w.get('strength',0):.0%}\n"
            # AI brain diagnostics
            + _ai_brain_status_line(model)
            # Algorithm reverse-engineering status
            + _algo_status_line(model)
            + f"└─────────────────────────────\n"
        )

    active  = len(league_scores)

    # Overall = total correct 1X2 predictions across all leagues
    _bd = c.bot_data
    total_correct = sum(
        _bd.get(f"model_{lid}", {}).get("cumulative", {}).get("outcome_correct", 0)
        for lid in LEAGUES
    )
    total_preds = sum(
        _bd.get(f"model_{lid}", {}).get("cumulative", {}).get("outcome_total", 0)
        for lid in LEAGUES
    )
    overall_pct = total_correct / total_preds if total_preds > 0 else 0.0
    icon        = _status_icon(sum(league_scores) / active if active else 0)

    # Strategy performance summary
    _strat_summary = _strategy_brain_summary(_bd)

    # Odds repeat performance summary
    _or_stats  = _bd.get("odds_repeat_stats", {})
    _or_total  = _or_stats.get("total", 0)
    _or_correct= _or_stats.get("correct", 0)
    _or_el_t   = _or_stats.get("elite_lock_total", 0)
    _or_el_c   = _or_stats.get("elite_lock_correct", 0)
    _or_e_t    = _or_stats.get("elite_total", 0)
    _or_e_c    = _or_stats.get("elite_correct", 0)
    _or_p_t    = _or_stats.get("premium_total", 0)
    _or_p_c    = _or_stats.get("premium_correct", 0)
    if _or_total > 0:
        _or_acc  = round(_or_correct/_or_total*100)
        _el_acc  = round(_or_el_c/_or_el_t*100) if _or_el_t else 0
        _e_acc   = round(_or_e_c/_or_e_t*100)   if _or_e_t  else 0
        _p_acc   = round(_or_p_c/_or_p_t*100)   if _or_p_t  else 0
        _or_summary = (
            f"⭐ *Premium Signal Performance*\n"
            f"   Overall: {_or_correct}/{_or_total} = {_or_acc}%\n"
            f"   💎 Elite Lock (100% mkts+100% consistent): {_or_el_c}/{_or_el_t} = {_el_acc}%\n"
            f"   🏆 Elite (100% mkts): {_or_e_c}/{_or_e_t} = {_e_acc}%\n"
            f"   ⭐ Premium (75%+ mkts): {_or_p_c}/{_or_p_t} = {_p_acc}%\n"
        )
    else:
        _or_summary = "⭐ *Premium Signal Performance*: No data yet\n"

    header = (
        f"🧠 *Brain Training Report*\n"
        f"{'━'*26}\n"
        f"{icon} *Overall: {total_correct}/{total_preds} correct ({overall_pct:.1%})*\n"
        f"📚 {active}/{len(LEAGUES)} leagues active\n"
        f"{'━'*26}\n\n"
        f"{_strat_summary}"
        f"{'━'*26}\n\n"
        f"{_or_summary}"
        f"{'━'*26}\n\n"
    )

    await _send(message, header + "\n".join(league_lines), parse_mode="Markdown")


async def cmd_brainstat(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not _is_admin(u.effective_user.id):
        return
    await _do_brainstat(u.message, c)


# ─── BACKUP / RESTORE ─────────────────────────────────────────────────────────
# Brain chunk size limit — 35 MB leaves comfortable headroom under Telegram's 50 MB cap.
# If the brain exceeds this the backup splits into multiple vsbot_brain_NofM_*.txt files.
# On restore, send ALL chunk files (plus the core file) in any order — the bot merges them.
BRAIN_CHUNK_BYTES = 10 * 1024 * 1024   # 10 MB per chunk — small files = fast upload/download, no lag


def _split_brain_into_chunks(brain_data: dict, ts_str: str, date_str: str) -> list[tuple]:
    """
    Split brain_data into as many chunks as needed, each under BRAIN_CHUNK_BYTES.
    Each chunk is a self-contained brain file so any single chunk can be loaded alone.

    Strategy: pack league data one league at a time. If adding the next league would
    push the chunk over the limit, seal the current chunk and start a new one.
    A single league that is itself > limit gets its own chunk (no split within a league).

    Returns list of (bio, filename, size_kb) tuples ready to send.
    """
    chunks_raw   = []   # list of dicts: {league_id_str: {...}}
    current      = {}
    current_size = 0

    for lid_str, ldata in brain_data.items():
        entry_txt  = json.dumps({lid_str: ldata}, separators=(",",":"))
        entry_size = len(entry_txt.encode())

        if current and (current_size + entry_size) > BRAIN_CHUNK_BYTES:
            chunks_raw.append(current)
            current      = {}
            current_size = 0

        current[lid_str] = ldata
        current_size    += entry_size

    if current:
        chunks_raw.append(current)

    total = len(chunks_raw)
    bios  = []
    for i, chunk in enumerate(chunks_raw, 1):
        payload = {
            "backup_ts":    time.time(),
            "backup_date":  date_str,
            "brain":        chunk,
            "chunk_index":  i,
            "chunk_total":  total,
        }
        txt  = json.dumps(payload, separators=(",",":"))
        bio  = io.BytesIO(txt.encode())
        name = (f"vsbot_brain_{i}of{total}_{ts_str}.txt"
                if total > 1 else f"vsbot_brain_{ts_str}.txt")
        bio.name = name
        bio.seek(0)
        bios.append((bio, name, len(txt) / 1024))

    return bios


async def _do_backup(message, c):
    """
    Core backup logic — callable from both /backup command and button.

    Sends:
      1. vsbot_backup_*.txt          — core data (users, models without fp_db)
      2. vsbot_brain_[NofM]_*.txt    — brain data, auto-split into chunks if > 35 MB
         If the brain fits in one file → single vsbot_brain_*.txt (no chunk suffix).
         If it needs splitting → vsbot_brain_1of3_*.txt, vsbot_brain_2of3_*.txt, etc.

    On restore: send ALL files (core + all brain chunks) in any order.
    The bot auto-detects and merges them — no commands needed.
    """
    try:
        await _do_backup_inner(message, c)
    except Exception as e:
        log.error(f"❌ _do_backup failed: {type(e).__name__}: {e}", exc_info=True)
        try:
            err_msg = f"❌ *Backup failed:* `{type(e).__name__}: {e}`\n\nCheck bot logs for details."
            await message.reply_text(err_msg, parse_mode="Markdown")
        except Exception:
            pass


async def _do_backup_inner(message, c):
    """Inner backup implementation — called by _do_backup with error handling."""
    bd  = c.bot_data
    acc = bd.get("access", {})

    models     = {}
    brain_data = {}

    for lid in LEAGUES:
        if lid not in ACTIVE_LEAGUES: continue
        m = bd.get(f"model_{lid}")
        if not isinstance(m, dict):
            continue
        has_data = (
            m.get("rounds_learned", 0) > 0
            or len(m.get("fingerprint_db", {})) > 0
            or len(m.get("pattern_memory", {})) > 0
            or len(m.get("match_log", [])) > 0
        )
        if not has_data:
            continue

        # Pop _ml_seen BEFORE json serialization — tuple keys crash json.dumps
        _ml_seen_raw = m.pop("_ml_seen", {}) if isinstance(m, dict) else {}
        m_copy = json.loads(json.dumps(m, default=str))
        if _ml_seen_raw:
            m["_ml_seen"] = _ml_seen_raw   # restore on live model (we only removed from copy)
        m_copy.pop("_cached_standings", None)
        m_copy.pop("_ml_seen", None)   # ensure not in copy regardless

        brain_data[str(lid)] = {
            "fingerprint_db": m_copy.pop("fingerprint_db", {}),
            "pattern_memory": m_copy.pop("pattern_memory", {}),
            "ai_brain":       m_copy.pop("ai_brain",       {}),
            "match_log":      m_copy.pop("match_log",      []),
            # _ml_seen has tuple keys (tuples not JSON-serializable as dict keys).
            # Drop from backup — rebuilt from match_log on restore.
        }
        models[str(lid)] = m_copy

    users_raw = acc.get("users", {})
    users     = {str(k): v for k, v in users_raw.items()}
    chats     = list(bd.get("auto_chats", set()))
    channels  = list(acc.get("allowed_channels", set()))
    ts_str    = time.strftime("%Y%m%d_%H%M")
    date_str  = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())

    payload = {
        "backup_ts":           time.time(),
        "backup_date":         date_str,
        "users":               users,
        "auto_chats":          chats,
        "allowed_channels":    channels,
        "pending_predictions": bd.get("pending_predictions", {}),
        "strategy_stats":      bd.get("strategy_stats", {}),
        "odds_repeat_stats":   bd.get("odds_repeat_stats", {}),
        "odds_store":          bd.get("odds_store", {}),
        "score_history_7794":  bd.get("score_history_7794", {}),
        "models":              models,
        # ── Migration flags: carry forward so a restore never re-runs
        # destructive one-time migrations on already-clean data.
        "migration_flags": {
            "_band_data_v2_cleared":    bd.get("_band_data_v2_cleared",    False),
            "_fpdb_trimmed_v1":         bd.get("_fpdb_trimmed_v1",         False),
            "_fpdb_decontaminated_v1":  bd.get("_fpdb_decontaminated_v1",  False),
            "_league_teams_reset_v2":   bd.get("_league_teams_reset_v2",   False),
            "_match_log_built_v3":      bd.get("_match_log_built_v3",      False),
            "_clean_rebuild_v4":        bd.get("_clean_rebuild_v4",        False),
            "_ml_season_fixed_v5":      bd.get("_ml_season_fixed_v5",      False),
        },
    }

    core_txt = json.dumps(payload, separators=(",",":"))
    core_bio = io.BytesIO(core_txt.encode())
    core_bio.name = f"vsbot_backup_{ts_str}.txt"
    core_bio.seek(0)

    # ── Split brain into chunks ───────────────────────────────────────────────
    brain_chunks = _split_brain_into_chunks(brain_data, ts_str, date_str)
    n_chunks     = len(brain_chunks)

    league_summary_parts = []
    for lid in models:
        m_s   = models[lid]
        rds   = m_s.get("rounds_learned", 0)
        lacc  = m_s.get("outcome_acc", 0)
        ai_fm = len(brain_data.get(lid, {}).get("ai_brain", {}).get("fixture_mem", {}))
        lname = LEAGUES[int(lid)]["name"]
        league_summary_parts.append(f"{lname}={rds}r/{lacc:.0%}" + (f"/AI:{ai_fm}fx" if ai_fm else ""))
    league_summary = ", ".join(league_summary_parts) or "none"

    core_kb   = len(core_txt) / 1024
    brain_kb  = sum(sz for _, _, sz in brain_chunks)
    chunk_txt = f"{n_chunks} file{'s' if n_chunks > 1 else ''}" if n_chunks > 1 else "1 file"

    caption = (
        f"📦 *BrainBot Backup*\n"
        f"🗓 {date_str}\n"
        f"👥 Users: {len(users)}\n"
        f"📢 Channels: {len(chats)}\n"
        f"🧠 Leagues: {league_summary}\n"
        f"📄 Core: {core_kb:.0f} KB  |  🧠 Brain: {brain_kb:.0f} KB ({chunk_txt})\n"
        f"_To restore: send this file + all brain chunk files._"
    )

    sent_to_user = await message.reply_document(
        document=core_bio, filename=core_bio.name,
        caption=caption, parse_mode="Markdown"
    )

    # ── Send all brain chunks ─────────────────────────────────────────────────
    sent_brain_ids = []
    for i, (bio, fname, sz_kb) in enumerate(brain_chunks, 1):
        chunk_caption = (
            f"🧠 *Brain Data* — chunk {i}/{n_chunks}\n"
            f"🗓 {date_str}  |  {sz_kb:.0f} KB\n"
            + (f"_Send all {n_chunks} brain files + core file to restore._"
               if n_chunks > 1 else "_Restore alongside the core backup file._")
        )
        bio.seek(0)
        sent_b = await message.reply_document(
            document=bio, filename=fname,
            caption=chunk_caption, parse_mode="Markdown"
        )
        sent_brain_ids.append(sent_b.document.file_id)

    # ── Auto-save file_ids for /fetchdata ─────────────────────────────────────
    if ADMIN_ID:
        requester_chat = message.chat_id if hasattr(message, "chat_id") else 0
        if str(requester_chat) == str(ADMIN_ID):
            c.bot_data["storage_file_id"]        = sent_to_user.document.file_id
            c.bot_data["storage_brain_file_ids"] = sent_brain_ids   # list of all chunks
            # Legacy single-key compat
            c.bot_data["storage_brain_file_id"]  = sent_brain_ids[0] if sent_brain_ids else None
            tip = f"☁️ _Backup saved ({n_chunks} brain chunk{'s' if n_chunks>1 else ''}) — use /fetchdata to restore._"
            await message.reply_text(tip, parse_mode="Markdown")
            log.info(f"✅ Backup stored: core + {n_chunks} brain chunks")
        else:
            try:
                core_bio.seek(0)
                sent = await c.bot.send_document(
                    chat_id=ADMIN_ID,
                    document=core_bio, filename=core_bio.name,
                    caption=f"🔄 *Auto-backup (core)*  |  {date_str}\n🧠 {league_summary}",
                    parse_mode="Markdown",
                )
                c.bot_data["storage_file_id"] = sent.document.file_id

                auto_brain_ids = []
                for i, (bio, fname, sz_kb) in enumerate(brain_chunks, 1):
                    bio.seek(0)
                    sb = await c.bot.send_document(
                        chat_id=ADMIN_ID,
                        document=bio, filename=fname,
                        caption=f"🔄 *Auto-backup (brain {i}/{n_chunks})*  |  {date_str}",
                        parse_mode="Markdown",
                    )
                    auto_brain_ids.append(sb.document.file_id)

                c.bot_data["storage_brain_file_ids"] = auto_brain_ids
                c.bot_data["storage_brain_file_id"]  = auto_brain_ids[0] if auto_brain_ids else None
                tip = f"☁️ _Brain saved ({n_chunks} chunk{'s' if n_chunks>1 else ''}) to admin cloud — use /fetchdata to restore._"
                await message.reply_text(tip, parse_mode="Markdown")
                log.info(f"✅ Auto-backup to ADMIN_ID={ADMIN_ID}: core + {n_chunks} brain chunks")
            except Exception as e:
                log.warning(f"Auto-upload to admin chat failed: {e}")
                await message.reply_text(f"⚠️ _Could not auto-save to cloud: {e}_", parse_mode="Markdown")
    else:
        await message.reply_text(
            "💡 _Tip: Set `ADMIN_ID` in Railway env vars to enable one-tap cloud restore with /fetchdata._",
            parse_mode="Markdown"
        )


async def cmd_backup(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Admin only — /backup command."""
    if not _is_admin(u.effective_user.id):
        return
    await _do_backup(u.message, c)


async def cmd_backup_from_message(message, c):
    """Called from the inline button."""
    await _do_backup(message, c)


# ─── /fetchdata + document auto-load ──────────────────────────────────────────
async def _load_backup_from_file_id(file_id: str, bot, bot_data: dict) -> dict | None:
    """Download a Telegram file by file_id and parse as backup JSON. Returns data or None."""
    try:
        tg_file = await bot.get_file(file_id)
        buf = io.BytesIO()
        await tg_file.download_to_memory(buf)
        buf.seek(0)
        raw = buf.read().decode("utf-8", errors="replace")

        # Strip any leading comment lines (lines starting with #)
        json_lines = [l for l in raw.splitlines() if not l.startswith("#")]
        clean = "\n".join(json_lines).strip()

        data = json.loads(clean)

        # Must have backup_date at minimum — models may be empty on cold backups
        if "backup_date" not in data:
            log.warning(f"_load_backup_from_file_id: missing backup_date key — keys={list(data.keys())[:8]}")
            return None

        # Migrate: if "models" key is missing entirely, initialise it
        if "models" not in data:
            data["models"] = {}

        log.info(
            f"✅ Backup loaded: date={data['backup_date']} "
            f"models={len(data['models'])} users={len(data.get('users',{}))} "
            f"pending_rounds={sum(len(v) for v in data.get('pending_predictions',{}).values())}"
        )
        return data

    except json.JSONDecodeError as e:
        log.warning(f"_load_backup_from_file_id: JSON parse error — {e}")
    except Exception as e:
        log.warning(f"_load_backup_from_file_id failed: {type(e).__name__}: {e}")
    return None


async def _load_brain_from_file_id(file_id: str, bot) -> dict | None:
    """Download a vsbot_brain_*.txt file and return parsed brain dict, or None."""
    try:
        tg_file = await bot.get_file(file_id)
        buf = io.BytesIO()
        await tg_file.download_to_memory(buf)
        buf.seek(0)
        raw = buf.read().decode("utf-8", errors="replace")
        json_lines = [l for l in raw.splitlines() if not l.startswith("#")]
        clean = "\n".join(json_lines).strip()
        data = json.loads(clean)
        if "brain" not in data:
            log.warning("_load_brain_from_file_id: missing 'brain' key")
            return None
        log.info(f"✅ Brain file loaded: date={data.get('backup_date')} leagues={len(data['brain'])}")
        return data["brain"]
    except Exception as e:
        log.warning(f"_load_brain_from_file_id failed: {type(e).__name__}: {e}")
    return None


def _merge_brain_into_data(data: dict, brain: dict):
    """
    Merge fingerprint_db and pattern_memory from a brain dict back into
    the core backup data dict (in-place), keyed by league id string.
    Also consolidates reverse fixture keys (e.g. ALA|RSO + RSO|ALA → one canonical key).
    """
    for lid_str, bdata in brain.items():
        if lid_str not in data["models"]:
            data["models"][lid_str] = {}
        raw_fp = bdata.get("fingerprint_db", {})

        # ── Consolidate reverse keys into canonical (alphabetical) key ──────
        # Old brain had both ALA|RSO and RSO|ALA as separate entries.
        # Merge them into one key so no history is lost.
        consolidated = {}
        for fk, records in raw_fp.items():
            parts = fk.split("|")
            if len(parts) == 2:
                canon = "|".join(sorted(parts))  # alphabetical = canonical
            else:
                canon = fk
            if canon not in consolidated:
                consolidated[canon] = []
            consolidated[canon].extend(records)
        # Sort each fixture's records by round_id and deduplicate
        for fk in consolidated:
            seen = set()
            deduped = []
            for r in sorted(consolidated[fk], key=lambda x: x.get("round_id", 0)):
                rid = r.get("round_id", 0)
                score = (r.get("score_h"), r.get("score_a"), rid)
                if score not in seen:
                    seen.add(score)
                    deduped.append(r)
            consolidated[fk] = deduped[-15:]  # keep last 15 — matches live fp_db cap

        data["models"][lid_str]["fingerprint_db"] = consolidated
        data["models"][lid_str]["pattern_memory"]  = bdata.get("pattern_memory",  {})

        # ── Restore match_log + _ml_seen (standings source) ──────────────────
        match_log = bdata.get("match_log", [])
        if match_log:
            data["models"][lid_str]["match_log"] = match_log
            # Rebuild _ml_seen dedup set from restored match_log
            ml_seen = {}
            for entry in match_log:
                dk = (entry.get("home",""), entry.get("away",""),
                      entry.get("round_id",0), entry.get("score_h",0), entry.get("score_a",0))
                ml_seen[dk] = 1
            data["models"][lid_str]["_ml_seen"] = ml_seen
            log.info(f"   [{lid_str}] match_log restored: {len(match_log)} entries")

        # ── Restore ai_brain (all AI learning data) from brain file ──────────
        ai_brain = bdata.get("ai_brain", {})
        if ai_brain:
            data["models"][lid_str]["ai_brain"] = ai_brain
            fm_count  = len(ai_brain.get("fixture_mem",  {}))
            log_entry = ai_brain.get("ai_log", [])
            intel     = ai_brain.get("intelligence", {})
            log.info(
                f"🤖 AI brain restored for league {lid_str}: "
                f"fixture_mem={fm_count} fixtures  "
                f"rounds_evaluated={intel.get('rounds_evaluated',0)}  "
                f"mature={intel.get('mature_fixtures',0)}"
            )


async def _apply_backup_to_bot(data: dict, message, bot_data: dict):
    """Load parsed backup dict into bot memory and reply with summary."""
    _load_baked_data(bot_data, data)

    # Also restore pending predictions if present (survived redeploy)
    if "pending_predictions" in data and data["pending_predictions"]:
        existing = bot_data.setdefault("pending_predictions", {})
        for lid_str, rounds in data["pending_predictions"].items():
            existing.setdefault(lid_str, {}).update(rounds)
        total_pending = sum(
            len(preds)
            for rounds in bot_data["pending_predictions"].values()
            for preds in rounds.values()
        )
        log.info(f"✅ Restored {total_pending} pending predictions from backup")

    # Restore strategy stats
    if "strategy_stats" in data and data["strategy_stats"]:
        bot_data["strategy_stats"] = data["strategy_stats"]
        log.info(f"✅ Restored strategy_stats from backup")

    if "odds_repeat_stats" in data and data["odds_repeat_stats"]:
        bot_data["odds_repeat_stats"] = data["odds_repeat_stats"]
        log.info(f"✅ Restored odds_repeat_stats from backup")

    if "odds_store" in data and data["odds_store"]:
        bot_data["odds_store"] = data["odds_store"]
        log.info(f"✅ Restored odds_store from backup")

    if "score_history_7794" in data and data["score_history_7794"]:
        bot_data["score_history_7794"] = data["score_history_7794"]
        total_sh = sum(len(v) for v in data["score_history_7794"].values())
        log.info(f"✅ Restored score_history_7794: {len(data["score_history_7794"])} fixtures, {total_sh} records")

    models = data.get("models", {})
    users  = data.get("users", {})
    chans  = data.get("allowed_channels", [])
    chats  = data.get("auto_chats", [])
    pend   = data.get("pending_predictions", {})
    pend_rounds = sum(len(v) for v in pend.values())
    pend_preds  = sum(len(p) for v in pend.values() for p in v.values())

    league_lines = []
    for lid_str, m in models.items():
        try:
            lid  = int(lid_str)
            flag = LEAGUES[lid]["flag"]
            name = LEAGUES[lid]["name"]
            rds  = m.get("rounds_learned", 0)
            acc  = m.get("outcome_acc", 0)
            fp_fixtures = len(m.get("fingerprint_db", {}))
            fp_records  = sum(len(v) for v in m.get("fingerprint_db", {}).values())

            # AI brain stats
            ai      = m.get("ai_brain", {})
            fm      = ai.get("fixture_mem", {})
            ai_fxs  = len(fm)
            ai_mat  = sum(1 for v in fm.values() if len(v) >= 6)
            ai_vet  = sum(1 for v in fm.values() if len(v) >= 10)
            intel   = ai.get("intelligence", {})
            ai_eval = intel.get("rounds_evaluated", 0)
            ai_lessons = sum(
                1 for v in fm.values()
                if len(v) >= 6 and
                   sum(1 for r in v[-10:] if not r.get("correct",True)) / max(len(v[-10:]),1) >= 0.40
            )
            w       = m.get("weights", {})
            w_str   = (f"O{w.get('odds',0):.0%} FP{w.get('fingerprint',0):.0%} "
                       f"T{w.get('tier',0):.0%} F{w.get('form',0):.0%}") if w else ""
            sa      = ai.get("signal_acc", {})
            def _sacc_str(sig):
                r = sa.get(sig, {}); t = r.get("total",0); c = r.get("correct",0)
                return f"{c/t:.0%}" if t >= 10 else "—"

            if rds > 0:
                ln = (
                    f"  {flag} *{name}*\n"
                    f"    📊 {rds} rounds · 1X2 {acc:.0%} · "
                    f"FP-DB {fp_fixtures}fx/{fp_records}rec\n"
                    f"    🤖 AI mem {ai_fxs}fx ({ai_mat} mature, {ai_vet} veteran) "
                    f"· {ai_lessons} active lessons\n"
                    f"    ⚡ Sig — Odds:{_sacc_str('odds')} "
                    f"FP:{_sacc_str('fingerprint')} Tier:{_sacc_str('tier')} Form:{_sacc_str('form')}\n"
                    f"    ⚖️ Weights: {w_str}"
                )
                if ai_eval:
                    ln += f" · {ai_eval} AI evals"
                league_lines.append(ln)
            else:
                league_lines.append(
                    f"  {flag} *{name}*: building… ({fp_fixtures} fixtures stored)"
                )
        except Exception:
            pass

    if not league_lines:
        league_lines.append("  _(no league data in this backup)_")

    text = (
        f"✅ *Brain Restored!*\n"
        f"📅 `{data.get('backup_date', 'unknown')}`\n"
        f"{'━'*26}\n"
        f"👥 *Users:* {len(users)}   📢 *Channels:* {len(chans)}   💬 *Chats:* {len(chats)}\n"
        + (f"⏳ *Pending:* {pend_rounds} rounds / {pend_preds} predictions restored\n" if pend_rounds else "")
        + f"{'━'*26}\n"
        f"🧠 *Leagues ({len(models)}):*\n"
        + "\n".join(league_lines) + "\n"
        f"{'━'*26}\n"
        f"_Bot is ready_ 🚀"
    )
    await message.reply_text(text, parse_mode="Markdown")
    log.info(
        f"✅ Brain restored: date={data.get('backup_date')} "
        f"leagues={len(models)} users={len(users)} channels={len(chans)}"
    )


async def cmd_fetchdata(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    Admin only — /fetchdata
    Tries to restore from the last known file_id (stored when /backup ran).
    Also loads the brain file (fingerprint_db + pattern_memory) if available.
    If not available, prompts admin to forward/send the backup files directly.
    """
    if not _is_admin(u.effective_user.id):
        await u.message.reply_text("🔒 Admin only.")
        return

    file_id = c.bot_data.get("storage_file_id")
    if file_id:
        brain_fids = c.bot_data.get("storage_brain_file_ids", [])
        if not brain_fids:
            legacy = c.bot_data.get("storage_brain_file_id")
            if legacy: brain_fids = [legacy]
        n_chunks = len(brain_fids)
        wait = await u.message.reply_text(
            f"⏳ Fetching backup + {n_chunks} brain chunk{'s' if n_chunks>1 else ''}…"
        )
        data = await _load_backup_from_file_id(file_id, c.bot, c.bot_data)
        if data:
            chunks_loaded = 0
            for fid in brain_fids:
                chunk_brain = await _load_brain_from_file_id(fid, c.bot)
                if chunk_brain:
                    _merge_brain_into_data(data, chunk_brain)
                    chunks_loaded += 1
                    log.info(f"✅ Brain chunk {chunks_loaded}/{n_chunks} merged")
            await wait.delete()
            await _apply_backup_to_bot(data, u.message, c.bot_data)
            return
        else:
            await wait.edit_text("⚠️ Stored file expired. Please send or forward the backup .txt file here and I will load it automatically.")
    else:
        await u.message.reply_text(
            "📂 *No backup in memory.*\n\n"
            "Just *send or forward* the `vsbot_backup_*.txt` file here — I will detect and load it automatically.",
            parse_mode="Markdown"
        )


async def handle_admin_document(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """
    When admin sends/forwards any .txt document to the bot,
    auto-detect if it's a backup or brain file and load it immediately.
    - vsbot_backup_*.txt  → core backup (users, pending, model stats)
    - vsbot_brain_*.txt   → fingerprint_db + pattern_memory
    Send both files (in any order) and both will be merged automatically.
    No commands needed — just send the files.
    """
    if not _is_admin(u.effective_user.id):
        return

    doc = u.message.document
    if not doc:
        return

    fname = doc.file_name or ""
    if not fname.endswith(".txt"):
        return

    wait = await u.message.reply_text(f"⏳ Reading `{fname}`…", parse_mode="Markdown")

    try:
        tg_file = await c.bot.get_file(doc.file_id)
        buf = io.BytesIO()
        await tg_file.download_to_memory(buf)
        buf.seek(0)
        raw = buf.read().decode("utf-8", errors="replace")
    except Exception as e:
        await wait.edit_text(f"❌ Could not download the file: `{e}`", parse_mode="Markdown")
        return

    json_lines = [l for l in raw.splitlines() if not l.startswith("#")]
    clean = "\n".join(json_lines).strip()

    if not clean:
        await wait.edit_text("❌ File is empty.")
        return

    try:
        data = json.loads(clean)
    except json.JSONDecodeError as e:
        preview = clean[:200].replace("`", "'")
        await wait.edit_text(
            f"❌ *File is not valid JSON.*\nParse error: `{e}`\n\nFile starts with:\n`{preview}`",
            parse_mode="Markdown"
        )
        return

    # ── Detect if this is a brain file (single or chunk) ────────────────────
    if "brain" in data and "backup_date" in data:
        brain        = data["brain"]
        chunk_index  = data.get("chunk_index", 1)
        chunk_total  = data.get("chunk_total", 1)

        # Accumulate brain chunk file_ids — list persists across multiple sends
        stored_ids = c.bot_data.setdefault("storage_brain_file_ids", [])
        if doc.file_id not in stored_ids:
            stored_ids.append(doc.file_id)
        # Legacy single-key compat
        c.bot_data["storage_brain_file_id"] = stored_ids[0] if stored_ids else doc.file_id

        chunks_received = len(stored_ids)
        log.info(f"Brain chunk {chunk_index}/{chunk_total} received — have {chunks_received} chunk(s) stored")

        # If we still need more chunks, tell the user and wait
        if chunks_received < chunk_total:
            await wait.edit_text(
                f"🧠 *Brain chunk {chunk_index}/{chunk_total} stored!*\n"
                f"Date: `{data.get('backup_date', '?')}`\n"
                f"Received {chunks_received}/{chunk_total} brain files.\n\n"
                f"Send the remaining {chunk_total - chunks_received} brain chunk file(s) "
                f"and the `vsbot_backup_*.txt` core file to complete the restore.",
                parse_mode="Markdown"
            )
            return

        # All chunks received — try to merge with core if we have it
        pending_core_fid = c.bot_data.get("storage_file_id")
        if pending_core_fid:
            core_data = await _load_backup_from_file_id(pending_core_fid, c.bot, c.bot_data)
            if core_data:
                # Merge ALL brain chunks into core
                for fid in stored_ids:
                    chunk_brain = await _load_brain_from_file_id(fid, c.bot)
                    if chunk_brain:
                        _merge_brain_into_data(core_data, chunk_brain)
                await wait.delete()
                await u.message.reply_text(
                    f"🧠 *All {chunk_total} brain chunk(s) received!* Merging and restoring…",
                    parse_mode="Markdown"
                )
                await _apply_backup_to_bot(core_data, u.message, c.bot_data)
                return

        # No core backup yet — store and wait
        await wait.edit_text(
            f"🧠 *Brain chunk {chunk_index}/{chunk_total} stored!*\n"
            f"Date: `{data.get('backup_date', '?')}`\n"
            f"All {chunk_total} brain file(s) received.\n\n"
            "Now send the `vsbot_backup_*.txt` core file to complete the restore.",
            parse_mode="Markdown"
        )
        return

    # ── Otherwise treat as a core backup file ─────────────────────────────────
    if "backup_date" not in data:
        keys = list(data.keys())[:8]
        await wait.edit_text(
            f"❌ *Not a valid backup file.*\n"
            f"Missing `backup_date` key. Found keys: `{keys}`\n\n"
            f"Make sure you're sending the `vsbot_backup_*.txt` file.",
            parse_mode="Markdown"
        )
        return

    if "models" not in data:
        data["models"] = {}

    # Remember this file_id for future /fetchdata calls
    c.bot_data["storage_file_id"] = doc.file_id

    # If we already have brain chunks stored, merge them all in now
    brain_fids   = c.bot_data.get("storage_brain_file_ids", [])
    # Fallback: legacy single file_id
    if not brain_fids:
        legacy = c.bot_data.get("storage_brain_file_id")
        if legacy:
            brain_fids = [legacy]
    brain_merged   = False
    chunks_merged  = 0
    for fid in brain_fids:
        chunk_brain = await _load_brain_from_file_id(fid, c.bot)
        if chunk_brain:
            _merge_brain_into_data(data, chunk_brain)
            brain_merged  = True
            chunks_merged += 1
    if brain_merged:
        log.info(f"✅ {chunks_merged} brain chunk(s) merged into core backup during document restore")

    await wait.delete()
    if brain_merged:
        await u.message.reply_text(
            "✅ *Core backup + Brain file merged!* Restoring full brain…",
            parse_mode="Markdown"
        )
    await _apply_backup_to_bot(data, u.message, c.bot_data)



def _load_baked_data(bot_data: dict, baked: dict):
    """
    Called at startup if BAKED_DATA is defined below.
    Always overwrites users and auto_chats — the backup is the source of truth
    and must never be blocked by a stale or empty PicklePersistence file.
    Models are overwritten unless the live pickle has strictly more rounds.
    """
    if not baked:
        return

    # ── Restore migration flags first — this MUST happen before the learning_job
    # migration checks run. If we restored a backup that was already decontaminated,
    # we must carry those flags forward so the migration never re-runs and re-purges
    # valid data that it mistakenly classifies as foreign due to incomplete whitelists.
    mflags = baked.get("migration_flags", {})
    if mflags:
        for flag_key, flag_val in mflags.items():
            if flag_val:  # only set True flags — never clear a flag that's already set
                bot_data[flag_key] = True
        log.info(f"💾 Restored migration flags: {[k for k,v in mflags.items() if v]}")

    # Users — always overwrite so a stale pickle can never hide them
    if baked.get("users"):
        acc = bot_data.setdefault("access", {
            "users": {}, "allowed_channels": set(), "pending_user": {}
        })
        acc.setdefault("users", {})
        acc.setdefault("allowed_channels", set())
        acc.setdefault("pending_user", {})
        for uid_str, udata in baked["users"].items():
            acc["users"][str(uid_str)] = udata   # always str keys
        log.info(f"💾 Restored {len(baked['users'])} users from baked data")

    # Auto-chats — always overwrite
    if baked.get("auto_chats"):
        bot_data["auto_chats"] = set(str(c) for c in baked["auto_chats"])
        log.info(f"💾 Restored {len(bot_data['auto_chats'])} auto-chats from baked data")

    # Allowed channels — restore from backup, OR auto-promote negative IDs from auto_chats
    acc = bot_data.setdefault("access", {
        "users": {}, "allowed_channels": set(), "pending_user": {}
    })
    acc.setdefault("allowed_channels", set())
    if baked.get("allowed_channels"):
        acc["allowed_channels"] = set(str(c) for c in baked["allowed_channels"])
        log.info(f"💾 Restored {len(acc['allowed_channels'])} allowed channels from baked data")
    else:
        # Older backup without allowed_channels — promote any channel/group IDs from auto_chats
        promoted = set()
        for cid in baked.get("auto_chats", []):
            try:
                if int(cid) < 0:
                    promoted.add(str(cid))
            except (ValueError, TypeError):
                pass
        if promoted:
            acc["allowed_channels"] = promoted
            log.info(f"💾 Auto-promoted {len(promoted)} channels from auto_chats to allowed_channels")

    # Learned models — overwrite unless live pickle has strictly more rounds
    if baked.get("models"):
        restored_models = 0
        for lid_str, m in baked["models"].items():
            if not isinstance(m, dict):
                continue
            # Allow restore even if rounds_learned=0 as long as match_log has data
            if m.get("rounds_learned", 0) == 0 and not m.get("match_log"):
                continue
            key      = f"model_{lid_str}"
            existing = bot_data.get(key)
            if isinstance(existing, dict) and existing.get("rounds_learned", 0) > m.get("rounds_learned", 0):
                lid   = int(lid_str)
                lname = LEAGUES.get(lid, {}).get("name", lid_str)
                log.info(f"💾 Kept live model {lname}: {existing['rounds_learned']} rounds (baked={m['rounds_learned']})")
                continue
            m.setdefault("pattern_memory", {})
            m.setdefault("signal_acc", {
                "odds":     {"correct": 0, "total": 0},
                "poisson":  {"correct": 0, "total": 0},
                "strength": {"correct": 0, "total": 0},
            })
            # ── Patch cumulative field — reconstruct from EMA-era data if absent ──
            if "cumulative" not in m or not m["cumulative"].get("outcome_total"):
                rds     = m.get("rounds_learned", 0)
                # Estimate from signal_acc totals (most reliable counter we have)
                sig_total = max(
                    m["signal_acc"].get("poisson",  {}).get("total", 0),
                    m["signal_acc"].get("strength", {}).get("total", 0),
                )
                est_matches = sig_total if sig_total > 0 else rds * 9
                oc_acc   = m.get("outcome_acc",  0.0)
                bt_acc   = m.get("btts_acc",     0.0)
                o25_acc  = m.get("over25_acc",   0.0)
                avg_g    = m.get("avg_goals",    2.5)
                hb       = m.get("home_bias",    1.0)
                home_est = int(est_matches * 0.45 * hb)
                away_est = int(est_matches * 0.30)
                draw_est = max(0, est_matches - home_est - away_est)
                m["cumulative"] = {
                    "outcome_correct":  int(oc_acc  * est_matches),
                    "outcome_total":    est_matches,
                    "btts_correct":     int(bt_acc  * est_matches),
                    "btts_total":       est_matches,
                    "over25_correct":   int(o25_acc * est_matches),
                    "over25_total":     est_matches,
                    "goals_total":      int(avg_g * est_matches),
                    "matches_total":    est_matches,
                    "home_wins":        home_est,
                    "draws":            draw_est,
                    "away_wins":        away_est,
                    "conf_sum_correct": 0.0,
                    "conf_sum_wrong":   0.0,
                    "n_correct":        0,
                    "n_wrong":          0,
                }
                log.info(f"💾 Patched cumulative field for {lid_str} from EMA data "
                         f"(est_matches={est_matches}, 1X2={oc_acc:.1%})")
            # ── Seed band tracking from cumulative data if bands are empty ──
            # This means on first load after upgrade, trust tags work immediately
            # using the already-learned accuracy — no need to re-earn the data.
            cum_s = m.get("cumulative", {})
            mt_s  = cum_s.get("matches_total", 0) or cum_s.get("outcome_total", 0)

            def _seed_band(band_dict, correct_total, grand_total, side):
                """Spread cumulative accuracy evenly across the most likely buckets."""
                if grand_total < 10: return
                if any(v[1] >= 10 for v in band_dict[side].values()): return  # already seeded
                acc = correct_total / grand_total
                # Spread across the 55-75% range as representative buckets
                per_bucket = grand_total // 4
                for b in ("55", "60", "65", "70"):
                    band_dict[side][b] = [int(acc * per_bucket), per_bucket]

            bba = m.setdefault("btts_band_acc", {"yes": {}, "no": {}})
            oba = m.setdefault("o25_band_acc",  {"yes": {}, "no": {}})

            bt_correct = cum_s.get("btts_correct", 0)
            bt_total   = cum_s.get("btts_total", mt_s)
            o25_correct= cum_s.get("over25_correct", 0)
            o25_total  = cum_s.get("over25_total", mt_s)
            btts_rate  = m.get("btts_rate", 0.50)
            o25_rate   = m.get("over25_rate", 0.50)

            # Seed BTTS: "yes" side uses btts accuracy, "no" side inverse
            if bt_total >= 10:
                _seed_band(bba, bt_correct, bt_total, "yes")
                _seed_band(bba, bt_total - bt_correct, bt_total, "no")
            # Seed O2.5
            if o25_total >= 10:
                _seed_band(oba, o25_correct, o25_total, "yes")
                _seed_band(oba, o25_total - o25_correct, o25_total, "no")

            # Seed 1X2 margin_acc from cumulative if empty
            ma = m.setdefault("margin_acc", {})
            oc_s = cum_s.get("outcome_correct", 0)
            ot_s = cum_s.get("outcome_total", 0)
            if ot_s >= 10 and not any(v[1] >= 10 for v in ma.values()):
                acc_s = oc_s / ot_s
                per_b = ot_s // 4
                for b in ("55", "60", "65", "70"):
                    ma[b] = [int(acc_s * per_b), per_b]

            # ── CRITICAL: fingerprint_db priority order ──────────────────────
            # Priority: brain-merged data in m > live in-memory > nothing
            # m["fingerprint_db"] already contains the brain data IF the brain
            # file was merged before this call (via _merge_brain_into_data).
            # Only fall back to the live in-memory copy if m has nothing.
            if not m.get("fingerprint_db") and isinstance(existing, dict):
                live_fp = existing.get("fingerprint_db")
                if live_fp:
                    m["fingerprint_db"] = live_fp
            if not m.get("pattern_memory") and isinstance(existing, dict):
                live_pm = existing.get("pattern_memory")
                if live_pm:
                    m["pattern_memory"] = live_pm
            # ── match_log: brain-merged data takes priority over live ─────────
            if not m.get("match_log") and isinstance(existing, dict):
                live_ml = existing.get("match_log")
                if live_ml:
                    m["match_log"] = live_ml
                    # Rebuild _ml_seen from restored match_log
                    m["_ml_seen"] = {
                        (e.get("home",""), e.get("away",""),
                         e.get("round_id",0), e.get("score_h",0), e.get("score_a",0)): 1
                        for e in live_ml
                    }
            # Never carry _cached_standings into the saved model — it's ephemeral
            m.pop("_cached_standings", None)

            # ── Apply ai_brain migration: if old keys present, convert them ──
            # This ensures brain files saved before the new AI engine work perfectly.
            old_ai = m.get("ai_brain", {})
            if old_ai:
                if "fixture_history" in old_ai and "fixture_mem" not in old_ai:
                    old_fh = old_ai.pop("fixture_history", {})
                    new_fm = {}
                    for fk_k, hist in old_fh.items():
                        new_fm[fk_k] = []
                        for h in (hist or []):
                            new_fm[fk_k].append({
                                "round":         h.get("round", 0),
                                "home":          fk_k.split("|")[0] if "|" in fk_k else "",
                                "away":          fk_k.split("|")[1] if "|" in fk_k else "",
                                "predicted":     h.get("predicted", ""),
                                "actual":        h.get("actual", ""),
                                "score_h":       0, "score_a": 0,
                                "correct":       h.get("correct", False),
                                "odds_tip":      h.get("odds_tip"),
                                "fp_tip":        h.get("fp_tip"),
                                "form_tip":      h.get("form_tip"),
                                "h_tier":        h.get("tier_pair","").split("_vs_")[0] if "_vs_" in h.get("tier_pair","") else "UNKNOWN",
                                "a_tier":        h.get("tier_pair","").split("_vs_")[1] if "_vs_" in h.get("tier_pair","") else "UNKNOWN",
                                "tier_pair":     h.get("tier_pair", ""),
                                "strong_side":   None,
                                "blame":         h.get("blame", []),
                                "primary_blame": h.get("primary_blame", ""),
                                "btts":          h.get("btts", False),
                                "over25":        h.get("over25", False),
                                "btts_prob":     50.0, "over25_prob": 50.0,
                                "odds_h": None, "odds_d": None, "odds_a": None,
                                "h_pos": None,  "a_pos": None,
                                "h_pts": None,  "a_pts": None,
                            })
                    old_ai["fixture_mem"] = new_fm
                if "signal_trust" in old_ai and "signal_acc" not in old_ai:
                    old_ai.pop("signal_trust", None)
                    old_ai.pop("signal_blame", None)
                    old_ai["signal_acc"] = {
                        sig: {"correct": 0, "total": 0, "recent": []}
                        for sig in ("odds", "fingerprint", "tier", "form")
                    }
                if "tier_outcomes" in old_ai and "tier_acc" not in old_ai:
                    old_ai["tier_acc"] = old_ai.pop("tier_outcomes", {})
                if "odds_band_trust" in old_ai and "band_acc" not in old_ai:
                    old_ai["band_acc"] = old_ai.pop("odds_band_trust", {})
                if "market_corr" in old_ai and "market_acc" not in old_ai:
                    old_ai["market_acc"] = {
                        "btts_yes": {"correct": 0, "total": 0},
                        "btts_no":  {"correct": 0, "total": 0},
                        "over25":   {"correct": 0, "total": 0},
                        "under25":  {"correct": 0, "total": 0},
                    }
                    old_ai.pop("market_corr", None)
                old_ai.pop("fixture_trust", None)

            # ── Deep-copy fingerprint_db to break any shared reference ──────────
            # If the backup was generated from a bot where multiple leagues shared
            # the same fingerprint_db dict object in memory (due to the old
            # contamination bug), all leagues would get the same reference here.
            # Mutations in one league's learning would silently corrupt all others.
            # json.loads(json.dumps(...)) gives us a guaranteed independent copy.
            if "fingerprint_db" in m:
                m["fingerprint_db"] = json.loads(json.dumps(m["fingerprint_db"], default=str))

            bot_data[key] = m
            restored_models += 1
            lid   = int(lid_str)
            lname = LEAGUES.get(lid, {}).get("name", lid_str)
            fp_count  = len(m.get("fingerprint_db", {}))
            ai_fm     = len(m.get("ai_brain", {}).get("fixture_mem", {}))
            ai_intel  = m.get("ai_brain", {}).get("intelligence", {})
            ai_r_eval = ai_intel.get("rounds_evaluated", 0)
            log.info(
                f"💾 Restored model {lname}: {m['rounds_learned']} rounds "
                f"1X2={m.get('outcome_acc',0):.1%} "
                f"fp={fp_count} fixtures "
                f"ai_mem={ai_fm} fixtures "
                f"ai_evals={ai_r_eval}"
            )
        log.info(f"💾 Restored {restored_models} league models from baked data")


# ─────────────────────────────────────────────────────────────────────────────
# BAKED_DATA — paste the contents of your backup .txt file here as a dict.
# Leave as {} if starting fresh. When you get a new bot.py from the developer,
# paste your backup JSON here so the bot starts with all your data intact.
# ─────────────────────────────────────────────────────────────────────────────
BAKED_DATA: dict = {}

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    # ── Persistence — survives redeploys ───────────────────────────────────────
    # Railway: mount a volume at /data and set DATA_DIR=/data env var.
    # Locally or without a volume: falls back to /tmp (lost on redeploy but harmless).
    data_dir  = os.environ.get("DATA_DIR", "/data")
    os.makedirs(data_dir, exist_ok=True)
    persist_path = os.path.join(data_dir, "bot_persistence.pkl")

    # ── If BAKED_DATA is set, wipe the pickle so it can never block restore ──
    if BAKED_DATA and os.path.exists(persist_path):
        try:
            os.remove(persist_path)
            log.info("🗑️  Deleted stale pickle — BAKED_DATA will be authoritative source")
        except Exception as _e:
            log.warning(f"Could not delete pickle: {_e}")

    persistence  = PicklePersistence(filepath=persist_path)
    log.info(f"💾 Persistence file: {persist_path}")

    # post_init runs after PicklePersistence loads — so baked data wins over stale pickle
    async def _post_init(application):
        if BAKED_DATA:
            _load_baked_data(application.bot_data, BAKED_DATA)
            log.info("✅ BAKED_DATA loaded into bot memory (post_init)")
        else:
            log.info("ℹ️  No BAKED_DATA — starting fresh")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .persistence(persistence)
        .post_init(_post_init)
        .build()
    )

    # ── Public / User commands ──────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("help",     cmd_start))
    app.add_handler(CommandHandler("stop",     cmd_stop))
    app.add_handler(CommandHandler("mystatus", cmd_mystatus))

    # ── Admin-only commands ─────────────────────────────────────────────────────
    app.add_handler(CommandHandler("addchannel",    cmd_addchannel))
    app.add_handler(CommandHandler("removechannel", cmd_removechannel))
    app.add_handler(CommandHandler("resetdata",     cmd_resetdata))
    app.add_handler(CommandHandler("adduser",       cmd_adduser))
    app.add_handler(CommandHandler("removeuser",    cmd_removeuser))
    app.add_handler(CommandHandler("showstatus",    cmd_showstatus))
    app.add_handler(CommandHandler("brainstat",     cmd_brainstat))
    app.add_handler(CommandHandler("rawstatus",     cmd_rawstatus))
    app.add_handler(CommandHandler("backup",        cmd_backup))
    app.add_handler(CommandHandler("fetchdata",     cmd_fetchdata))
    app.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.PRIVATE, handle_admin_document))

    # ── Callback for adduser day-picker ─────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_adduser_days, pattern=r"^adduser_days:"))

    # ── Menu button callbacks ────────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_menu,        pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(cb_predict_cb,  pattern=r"^predict_cb:"))
    app.add_handler(CallbackQueryHandler(cb_compare_cb,  pattern=r"^compare_cb:"))

    # ── Existing league callback handlers ───────────────────────────────────────
    app.add_handler(CallbackQueryHandler(cb_results,          pattern=r"^results:"))
    app.add_handler(CallbackQueryHandler(cb_nextroundresults, pattern=r"^nextround:"))
    app.add_handler(CallbackQueryHandler(cb_standings,        pattern=r"^standings:"))
    app.add_handler(CallbackQueryHandler(cb_live,             pattern=r"^live:"))
    app.add_handler(CallbackQueryHandler(cb_upcoming,         pattern=r"^upcoming:"))
    app.add_handler(CallbackQueryHandler(cb_overallgoals,     pattern=r"^og:"))

    # ── Startup diagnostic — fires 3 s after boot ─────────────────────────────
    async def _startup_diagnostic(context):
        log.info("=" * 60)
        log.info("STARTUP DIAGNOSTIC — API + odds pipeline check")
        log.info("=" * 60)
        log.info(f"  BOT_TOKEN : {'SET' if BOT_TOKEN else 'MISSING'}")
        log.info(f"  ADMIN_ID  : {ADMIN_ID or 'NOT SET'}")
        log.info(f"  CHANNEL_ID: {CHANNEL_ID or 'NOT SET (direct chat mode)'}")
        log.info(f"  COOKIE    : {'SET' if BETPAWA_COOKIE else 'NOT SET — endpoints may reject'}")
        log.info(f"  BASE URL  : {BASE}")
        log.info("-" * 60)

        # ── Persistence restore summary ────────────────────────────────────────
        bd = context.bot_data
        acc = bd.get("access", {})
        users    = acc.get("users", {})
        channels = bd.get("auto_chats", set())
        log.info("💾 RESTORED FROM PERSISTENCE:")
        log.info(f"   Users    : {len(users)} ({list(users.keys())})")
        log.info(f"   Channels : {len(channels)} ({list(channels)})")
        for lid in LEAGUES:
            if lid not in ACTIVE_LEAGUES: continue
            m = bd.get(f"model_{lid}")
            if isinstance(m, dict) and m.get("rounds_learned", 0) > 0:
                log.info(f"   League {lid}: {m['rounds_learned']} rounds learned, "
                         f"1X2={m.get('outcome_acc',0):.1%} "
                         f"weights={m.get('weights',{})}")
            else:
                log.info(f"   League {lid}: no prior learning — will bootstrap")
        log.info("-" * 60)
        async with httpx.AsyncClient() as client:
            for test_lid, test_name in [(lid, LEAGUES[lid]["name"]) for lid in ACTIVE_LEAGUES if lid in LEAGUES]:
                log.info(f"--- League {test_lid} {test_name} ---")
                past   = await fetch_round_list(client, test_lid, past=True)
                actual = await fetch_round_list(client, test_lid, past=False)
                if not past:
                    log.error(f"FAIL [{test_name}]: no past rounds")
                else:
                    log.info(f"OK   [{test_name}]: {len(past)} past rounds, latest id={past[0].get('id')}")
                if not actual:
                    log.error(f"FAIL [{test_name}]: no upcoming rounds")
                else:
                    log.info(f"OK   [{test_name}]: {len(actual)} upcoming rounds, next id={actual[0].get('id')}")

                # Test UPCOMING page for odds (matchups=scores only, upcoming=odds)
                if actual:
                    rid = actual[0].get("id")
                    log.info(f"ODDS TEST [{test_name}]: fetching page=upcoming for round {rid}")
                    evs_up = await fetch_round_events(client, rid, PAGE_UPCOMING)
                    evs_up = _filter_league(evs_up, test_lid)
                    if evs_up:
                        with_odds = [e for e in evs_up if _extract_odds(e).get("1x2")]
                        log.info(f"OK   [{test_name}]: upcoming round {rid} — "
                                 f"{len(evs_up)} events, {len(with_odds)}/{len(evs_up)} have 1X2 odds")
                        if with_odds:
                            log.info(f"     ODDS WORKING — predictions will use betPawa odds")
                        else:
                            log.warning(f"WARN [{test_name}]: upcoming events have no 1X2 odds — "
                                        f"check market label matching in _extract_odds")
                            # Log raw market structure of first event to diagnose
                            _log_odds_debug(evs_up[0], rid)
                    else:
                        log.warning(f"WARN [{test_name}]: no events on upcoming page for round {rid}")

                # Also confirm past matchups have scores (needed for learning)
                if past:
                    rid2 = past[0].get("id")
                    evs_past = await fetch_round_events(client, rid2, PAGE_MATCHUPS)
                    evs_past = _filter_league(evs_past, test_lid)
                    scored = sum(1 for e in evs_past if _extract_score(e)[0] is not None)
                    log.info(f"HIST [{test_name}]: past round {rid2} — "
                             f"{len(evs_past)} events, {scored} with scores (expected: markets=0, scores>0)")
        log.info("=" * 60)
        log.info("DIAGNOSTIC COMPLETE — look for FAIL/WARN above")
        log.info("=" * 60)

    app.job_queue.run_once(_startup_diagnostic, when=3)

    # (baked data loaded in post_init above, after PicklePersistence initializes)

    # ── Stats loader — runs at startup then every 55 min ───────────────────────
    app.job_queue.run_repeating(_stats_loader_job, interval=3300, first=10)
    # standings are loaded inside _stats_loader_job — runs at first=10s so
    # tier filter works immediately after every deploy without manual command

    # ── Data collector — saves every match/odds/result, runs every 4 min ───────
    # Completely independent of the display engine. No filtering. Every match saved.
    app.job_queue.run_repeating(_data_collector_job, interval=240, first=300)  # 5min delay — gives time to restore brain before backfill runs

    # ── Standings refresh job — runs every 5 minutes independently ──────────────
    app.job_queue.run_repeating(_standings_job, interval=60, first=10)

    # ── Self-learning job — checks for new results every 6 min ─────────────────
    app.job_queue.run_repeating(_learning_job,      interval=120, first=90)

    # ── Auto-post every 30 seconds (uses cached stats only) ────────────────────
    app.job_queue.run_repeating(_auto_send_job, interval=30, first=70)

    # ── Expiry checker — runs every hour ────────────────────────────────────────
    async def _check_expiry_job(context):
        acc    = _access(context.bot_data)
        now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
        for uid, entry in list(acc["users"].items()):
            if entry["expire_ts"] <= now_ts and not entry.get("notified_expire"):
                # Mark as notified so we only ping once
                entry["notified_expire"] = True
                try:
                    await context.bot.send_message(
                        chat_id=int(uid),
                        text=(
                            "⏰ *Your BetPawa Bot access has expired.*\n\n"
                            "You will no longer receive picks.\n"
                            "Contact the admin to renew your subscription."
                        ),
                        parse_mode="Markdown",
                    )
                except Exception as e:
                    log.warning(f"Could not notify expired user {uid}: {e}")
                # Also remove from auto_chats
                chats = context.bot_data.get("auto_chats", set())
                chats.discard(uid)

    app.job_queue.run_repeating(_check_expiry_job, interval=3600, first=60)

    log.info("🤖  BetPawa Bot running (admin=%d) — posting to: %s", ADMIN_ID, CHANNEL_ID or "direct chats")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()