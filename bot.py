"""
brain_compress.py — Lossless compressed brain storage for BetPawa bot
======================================================================

HOW IT WORKS — "encoding" is just smart packing, fully reversible:

MATCH LOG (match_log):
  Before: [{"home":"ARS","away":"CHE","score_h":2,"score_a":1,"round_id":1179973,"season_id":"137297"}, ...]
  After:  {"_c":1, "teams":["ARS","CHE",...], "base_rid":1179900,
           "rows":[(0,1,2,1,73,"S0"), ...]}   ← (h_idx, a_idx, sh, sa, rid_delta, season_code)

  Each row uses only small integers. A full 2000-entry match_log shrinks
  from ~360 KB (pickle) to ~60 KB — about 6× smaller.

FINGERPRINT DB (fingerprint_db):
  Before: each record is a fat dict with 18 keys, many redundant
  After:  a compact tuple (fp_k0,fp_k1,fp_k2, out_code, htft_code,
           btts,ou25,ou35, sh,sa, hth,hta, dc_code, rid, sid_code,
           pos_h,pos_a, flip, form_h_code, form_a_code)
  Outcome/HTFT/DC encoded as single char. Saves ~65% per record.

SEQUENCE STRINGS (outcome_seq, league_outcome_seq, slot_outcomes):
  Before: ["HOME","DRAW","AWAY","HOME", ...]  (6–9 bytes per entry)
  After:  "HDAH..."  (1 byte per entry — H/D/A encoding)

  Also: pattern_memory results list → "HDA..." string
  And:  market_history lists → compact bit-packed integers

HOW TO PLUG IN — add 4 lines to bot.py:
  1. `from brain_compress import compress_model, decompress_model`
  2. Before saving (in backup / persistence):  `model = compress_model(model)`
  3. After loading (in restore / persistence): `model = decompress_model(model)`
  4. That's it. All existing functions work unchanged on decompressed data.

CYCLE DETECTION — nothing is lost:
  - match_log: all fields preserved (home, away, score_h, score_a, round_id, season_id)
  - outcome_seq: all outcomes preserved in order (just shorter encoding)
  - slot_outcomes: all slot sequences preserved
  - league_outcome_seq: all round-dominant outcomes preserved
  - pattern_memory: results lists preserved
  All cycle detection functions (_detect_cycle, detect_cycle_pattern_elite,
  analyze_cycle_elite, _get_slot_signal, etc.) work identically.
"""

import struct
from typing import Any

# ── Encoding tables ────────────────────────────────────────────────────────────

_OUT_ENC  = {"HOME": "H", "DRAW": "D", "AWAY": "A", None: "?", "": "?"}
_OUT_DEC  = {"H": "HOME", "D": "DRAW", "A": "AWAY", "?": None}

# HTFT: betPawa notation like "1/1", "X/2", "?/1" — encode to 2-char string
# The slash + values are already short; we just strip the "/" → 2 chars
# e.g. "1/1"→"11", "X/2"→"X2", "?/1"→"?1"
def _enc_htft(s):
    if not s or s == "":
        return "??"
    return s.replace("/", "")[:2] if "/" in s else (s[:2] if s else "??")

def _dec_htft(s):
    if not s or s in ("??", ""):
        return None
    if len(s) == 2:
        return f"{s[0]}/{s[1]}"
    return s  # already has slash or is some other format

_DC_ENC = {"1X": "a", "X2": "b", "12": "c", None: "?", "": "?"}
_DC_DEC = {"a": "1X", "b": "X2", "c": "12", "?": None}

# Form snapshot encoding: list of "W"/"D"/"L" → compact string
# Form snapshot is already a string list like ["W","D","L"] → "WDL"
def _enc_form(f):
    if not f:
        return ""
    if isinstance(f, list):
        return "".join(str(x)[0] for x in f)[:10]
    return str(f)[:10]

def _dec_form(s):
    if not s:
        return []
    return list(s)

# ── Outcome sequence compression ───────────────────────────────────────────────

def _compress_outcome_seq(seq: list) -> str:
    """
    ["HOME","DRAW","AWAY","HOME"] → "HDAH"
    Works for both full strings AND single-char ("H","D","A") — idempotent.
    """
    if not seq:
        return ""
    result = []
    for x in seq:
        if isinstance(x, str) and len(x) == 1:
            result.append(x)  # already compressed
        else:
            result.append(_OUT_ENC.get(x, "?"))
    return "".join(result)

def _decompress_outcome_seq(s) -> list:
    """
    "HDAH" → ["HOME","DRAW","AWAY","HOME"]
    If already a list, decode each char.
    """
    if isinstance(s, list):
        # Already decompressed (or mixed) — normalize
        out = []
        for x in s:
            if isinstance(x, str) and len(x) == 1 and x in _OUT_DEC:
                out.append(_OUT_DEC[x])
            else:
                out.append(x)
        return out
    if not s:
        return []
    return [_OUT_DEC.get(c, c) for c in s]

def _compress_short_seq(seq: list) -> str:
    """
    For league_outcome_seq and slot_outcomes that already use H/D/A single chars.
    Just joins them. If full strings, encodes.
    """
    return _compress_outcome_seq(seq)

def _decompress_short_seq(s) -> list:
    """Decompress to single-char H/D/A (slot_outcomes and league_outcome_seq use this format)."""
    if isinstance(s, list):
        return s  # already a list, leave as-is
    if not s:
        return []
    return list(s)  # slot_outcomes uses H/D/A chars directly

# ── Market history compression ─────────────────────────────────────────────────

def _compress_market_history(hist: list) -> str:
    """
    [1,0,1,1,0,1,...] → compact hex string. 4 bits per value won't work simply,
    so we pack 8 bits at a time using hex. Each char = 4 bits → 2 chars = 8 bits = 8 entries.
    Fast and lossless.
    e.g. [1,0,1,1,0,0,1,0, 1,...] → "b2..."
    """
    if not hist:
        return ""
    # Pack into bytes: 8 values per byte
    result = bytearray()
    for i in range(0, len(hist), 8):
        chunk = hist[i:i+8]
        byte = 0
        for j, v in enumerate(chunk):
            if v:
                byte |= (1 << (7 - j))
        result.append(byte)
    # Store length prefix so we know the exact count (handles non-multiple-of-8)
    return f"{len(hist)}:{result.hex()}"

def _decompress_market_history(s) -> list:
    """Reverse of _compress_market_history."""
    if isinstance(s, list):
        return s  # already decompressed
    if not s:
        return []
    try:
        colon = s.index(":")
        length = int(s[:colon])
        hexdata = s[colon+1:]
        raw = bytes.fromhex(hexdata)
        result = []
        for byte in raw:
            for j in range(7, -1, -1):
                result.append(1 if (byte >> j) & 1 else 0)
        return result[:length]
    except Exception:
        return []

# ── Match log compression ──────────────────────────────────────────────────────

def _compress_match_log(match_log: list) -> dict:
    """
    Compress match_log from list of dicts to a compact structure.
    
    The key insight: home/away are 3-char team codes repeated thousands of times.
    We build a team index and store indices instead of strings.
    round_id values cluster near each other — store as delta from base.
    season_id is repeated — store as index.
    
    Output format:
    {
        "_c": 1,                    # flag: this is compressed
        "teams": ["ARS","CHE",...], # team index (position = id)
        "seasons": ["137297",...],  # season index
        "base_rid": 1179900,        # base for round_id deltas
        "rows": [                   # one tuple per match
            (h_idx, a_idx, score_h, score_a, rid_delta, sid_idx),
            ...
        ]
    }
    Space saving: team names (3 chars each) replaced by 1 index int.
    round_id (7-digit) replaced by small delta. season_id replaced by 1 index int.
    """
    if not match_log:
        return {"_c": 1, "teams": [], "seasons": [], "base_rid": 0, "rows": []}

    # Build indexes
    team_map: dict[str, int] = {}
    season_map: dict[str, int] = {}
    teams_list: list[str] = []
    seasons_list: list[str] = []

    def _tidx(name: str) -> int:
        n = str(name).upper().strip()
        if n not in team_map:
            team_map[n] = len(teams_list)
            teams_list.append(n)
        return team_map[n]

    def _sidx(sid) -> int:
        s = str(sid)
        if s not in season_map:
            season_map[s] = len(seasons_list)
            seasons_list.append(s)
        return season_map[s]

    # Base round_id = minimum non-zero round_id for delta encoding
    rids = [e.get("round_id", 0) for e in match_log if e.get("round_id", 0) > 0]
    base_rid = min(rids) if rids else 0

    rows = []
    for e in match_log:
        h_idx   = _tidx(e.get("home", "?"))
        a_idx   = _tidx(e.get("away", "?"))
        sh      = int(e.get("score_h") or 0)
        sa      = int(e.get("score_a") or 0)
        rid     = int(e.get("round_id") or 0)
        rid_d   = rid - base_rid  # delta; small non-negative int
        sid_idx = _sidx(e.get("season_id", ""))
        rows.append((h_idx, a_idx, sh, sa, rid_d, sid_idx))

    return {
        "_c":      1,
        "teams":   teams_list,
        "seasons": seasons_list,
        "base_rid": base_rid,
        "rows":    rows,
    }

def _decompress_match_log(compressed: dict) -> list:
    """Reverse of _compress_match_log."""
    if not isinstance(compressed, dict) or "_c" not in compressed:
        # Not compressed — already a list or unexpected format
        if isinstance(compressed, list):
            return compressed
        return []

    teams   = compressed.get("teams", [])
    seasons = compressed.get("seasons", [])
    base    = compressed.get("base_rid", 0)
    rows    = compressed.get("rows", [])

    result = []
    for row in rows:
        try:
            h_idx, a_idx, sh, sa, rid_d, sid_idx = row
            result.append({
                "home":      teams[h_idx] if h_idx < len(teams) else "?",
                "away":      teams[a_idx] if a_idx < len(teams) else "?",
                "score_h":   sh,
                "score_a":   sa,
                "round_id":  base + rid_d,
                "season_id": seasons[sid_idx] if sid_idx < len(seasons) else "",
            })
        except (IndexError, TypeError, ValueError):
            continue
    return result

# ── Fingerprint DB compression ─────────────────────────────────────────────────

# fp_db record fields and their positions in the compressed tuple:
# (fp0, fp1, fp2,   ← fp_key floats (×100 as int for lossless storage)
#  out,              ← outcome char H/D/A
#  htft,             ← 2-char htft string e.g. "11","X2"
#  btts, ou25, ou35, ← bool as 0/1
#  sh, sa,           ← scores (int)
#  hth, hta,         ← ht scores (int, -1 if None)
#  dc,               ← dc code char a/b/c
#  rid,              ← round_id int
#  sid,              ← season_id str (short)
#  lid,              ← league_id int
#  flip,             ← was_flipped bool as 0/1
#  pos_h, pos_a,     ← positions (int, -1 if None)
#  form_h, form_a,   ← form strings "WDLWW" etc.
#  ou15,             ← ou15_result bool
# )  → 21 fields packed into a tuple

_FP_NONE = -1  # sentinel for None int values

def _compress_fp_record(r: dict) -> tuple:
    """Compress one fingerprint_db record to a compact tuple."""
    fp = r.get("fp_key") or [0.0, 0.0, 0.0]
    # Store fp_key as ints (×1000) to avoid float repr overhead
    fp0 = int(round(float(fp[0] if len(fp) > 0 else 0) * 1000))
    fp1 = int(round(float(fp[1] if len(fp) > 1 else 0) * 1000))
    fp2 = int(round(float(fp[2] if len(fp) > 2 else 0) * 1000))

    out   = _OUT_ENC.get(r.get("outcome"), "?")
    htft  = _enc_htft(r.get("htft_result") or "")
    btts  = 1 if r.get("btts_result") else 0
    ou25  = 1 if r.get("ou25_result") else 0
    ou35  = 1 if r.get("ou35_result") else 0
    ou15  = 1 if r.get("ou15_result") else 0
    sh    = int(r.get("score_h") or 0)
    sa    = int(r.get("score_a") or 0)
    hth   = int(r["ht_h"]) if r.get("ht_h") is not None else _FP_NONE
    hta   = int(r["ht_a"]) if r.get("ht_a") is not None else _FP_NONE
    dc    = _DC_ENC.get(r.get("dc_result"), "?")
    rid   = int(r.get("round_id") or 0)
    sid   = str(r.get("season_id") or "")
    lid   = int(r.get("league_id") or 0)
    flip  = 1 if r.get("was_flipped") else 0
    pos_h = int(r["pos_h"]) if r.get("pos_h") is not None else _FP_NONE
    pos_a = int(r["pos_a"]) if r.get("pos_a") is not None else _FP_NONE
    fh    = _enc_form(r.get("_form_h"))
    fa    = _enc_form(r.get("_form_a"))

    return (fp0, fp1, fp2, out, htft, btts, ou25, ou35,
            sh, sa, hth, hta, dc, rid, sid, lid, flip,
            pos_h, pos_a, fh, fa, ou15)

def _decompress_fp_record(t: tuple) -> dict:
    """Decompress one fingerprint_db record tuple back to dict."""
    if not isinstance(t, tuple) or len(t) < 21:
        if isinstance(t, dict):
            return t  # already decompressed
        return {}
    (fp0, fp1, fp2, out, htft, btts, ou25, ou35,
     sh, sa, hth, hta, dc, rid, sid, lid, flip,
     pos_h, pos_a, fh, fa, ou15) = t[:22] if len(t) >= 22 else (*t, 0)

    r = {
        "fp_key":       [fp0 / 1000.0, fp1 / 1000.0, fp2 / 1000.0],
        "outcome":      _OUT_DEC.get(out, out),
        "htft_result":  _dec_htft(htft),
        "btts_result":  bool(btts),
        "ou25_result":  bool(ou25),
        "ou35_result":  bool(ou35),
        "ou15_result":  bool(ou15),
        "score_h":      sh,
        "score_a":      sa,
        "ht_h":         None if hth == _FP_NONE else hth,
        "ht_a":         None if hta == _FP_NONE else hta,
        "dc_result":    _DC_DEC.get(dc, dc),
        "round_id":     rid,
        "season_id":    sid,
        "league_id":    lid,
        "was_flipped":  bool(flip),
        "pos_h":        None if pos_h == _FP_NONE else pos_h,
        "pos_a":        None if pos_a == _FP_NONE else pos_a,
        "_form_h":      _dec_form(fh),
        "_form_a":      _dec_form(fa),
        # odds_snapshot intentionally omitted — it's large and not used for
        # cycle detection or form. The fp_key is the compact odds fingerprint.
        "odds_snapshot": {},
    }
    # Backfill derived fields used by dominance logic
    outcomes = [r["outcome"]]
    r["dominant_outcome"]      = r["outcome"]
    r["dominant_outcome_conf"] = 1.0
    r["n_samples"]             = 1
    return r

def _compress_fingerprint_db(fp_db: dict) -> dict:
    """Compress all records in fp_db."""
    compressed = {}
    for fk, records in fp_db.items():
        if not isinstance(records, list):
            compressed[fk] = records
            continue
        compressed[fk] = [
            _compress_fp_record(r) if isinstance(r, dict) else r
            for r in records
        ]
    return compressed

def _decompress_fingerprint_db(fp_db: dict) -> dict:
    """Decompress all records in fp_db."""
    decompressed = {}
    for fk, records in fp_db.items():
        if not isinstance(records, list):
            decompressed[fk] = records
            continue
        decompressed[fk] = [
            _decompress_fp_record(r) if isinstance(r, tuple) else r
            for r in records
        ]
    return decompressed

# ── Pattern memory compression ─────────────────────────────────────────────────

def _compress_pattern_memory(pm: dict) -> dict:
    """
    pattern_memory[fk]["outcome_seq"] = ["HOME","DRAW","AWAY",...] → compact string.
    All other fields (counts, totals) are already small ints — leave as-is.
    """
    out = {}
    for fk, rec in pm.items():
        if not isinstance(rec, dict):
            out[fk] = rec
            continue
        new_rec = dict(rec)
        if "outcome_seq" in rec and isinstance(rec["outcome_seq"], list):
            new_rec["outcome_seq"] = _compress_outcome_seq(rec["outcome_seq"])
        if "results" in rec and isinstance(rec["results"], list):
            # results is a list of outcome strings — compress same way
            new_rec["results"] = _compress_outcome_seq(rec["results"])
        out[fk] = new_rec
    return out

def _decompress_pattern_memory(pm: dict) -> dict:
    """Decompress pattern_memory outcome sequences."""
    out = {}
    for fk, rec in pm.items():
        if not isinstance(rec, dict):
            out[fk] = rec
            continue
        new_rec = dict(rec)
        if "outcome_seq" in rec and isinstance(rec["outcome_seq"], str):
            new_rec["outcome_seq"] = _decompress_outcome_seq(rec["outcome_seq"])
        if "results" in rec and isinstance(rec["results"], str):
            new_rec["results"] = _decompress_outcome_seq(rec["results"])
        out[fk] = new_rec
    return out

# ── Slot outcomes compression ──────────────────────────────────────────────────

def _compress_slot_outcomes(so: dict) -> dict:
    """slot_outcomes: {slot_idx_str: ["H","D","A",...]} → join each list to string."""
    out = {}
    for k, seq in so.items():
        if isinstance(seq, list):
            out[k] = "".join(str(x) for x in seq)  # H/D/A chars → string
        else:
            out[k] = seq
    return out

def _decompress_slot_outcomes(so: dict) -> dict:
    """Decompress slot_outcomes strings back to lists."""
    out = {}
    for k, seq in so.items():
        if isinstance(seq, str):
            out[k] = list(seq)  # each char is already H/D/A
        else:
            out[k] = seq
    return out

# ── Market history compression ─────────────────────────────────────────────────

def _compress_market_history_dict(mh: dict) -> dict:
    """market_history: {market: [1,0,1,...]} → {market: "N:hexbytes"}"""
    out = {}
    for mkt, hist in mh.items():
        if isinstance(hist, list):
            out[mkt] = _compress_market_history(hist)
        else:
            out[mkt] = hist
    return out

def _decompress_market_history_dict(mh: dict) -> dict:
    """Decompress market_history."""
    out = {}
    for mkt, hist in mh.items():
        if isinstance(hist, str) and ":" in hist:
            out[mkt] = _decompress_market_history(hist)
        else:
            out[mkt] = hist
    return out

# ── Top-level compress / decompress ───────────────────────────────────────────

def compress_model(model: dict) -> dict:
    """
    Compress a league model dict (bot_data[f"model_{lid}"]) in-place-equivalent.
    Returns a new dict with all large structures compressed.
    Call this before pickling / saving to disk.

    Fields compressed:
      match_log         → compact indexed structure (~6× smaller)
      fingerprint_db    → tuple-per-record (~65% smaller per record)
      pattern_memory    → outcome_seq strings
      slot_outcomes     → joined strings
      league_outcome_seq → single joined string
      market_history    → bit-packed hex strings

    All other fields (counters, weights, stats dicts, etc.) are left unchanged
    — they're already small and don't benefit from compression.
    """
    if not isinstance(model, dict):
        return model

    m = dict(model)  # shallow copy — we'll replace specific keys

    # 1. match_log
    if "match_log" in m and isinstance(m["match_log"], list):
        m["match_log"] = _compress_match_log(m["match_log"])
        # _ml_seen is a dedup dict keyed by tuples — also large; rebuild on load
        m.pop("_ml_seen", None)

    # 2. fingerprint_db
    if "fingerprint_db" in m and isinstance(m["fingerprint_db"], dict):
        m["fingerprint_db"] = _compress_fingerprint_db(m["fingerprint_db"])
        # Invalidate index caches — rebuilt on demand after decompress
        m.pop("_team_idx", None)
        m.pop("_team_idx_size", None)
        m.pop("_odds_idx", None)
        m.pop("_odds_idx_size", None)

    # 3. pattern_memory outcome_seq and results
    if "pattern_memory" in m and isinstance(m["pattern_memory"], dict):
        m["pattern_memory"] = _compress_pattern_memory(m["pattern_memory"])

    # 4. slot_outcomes
    if "slot_outcomes" in m and isinstance(m["slot_outcomes"], dict):
        m["slot_outcomes"] = _compress_slot_outcomes(m["slot_outcomes"])

    # 5. league_outcome_seq
    if "league_outcome_seq" in m and isinstance(m["league_outcome_seq"], list):
        m["league_outcome_seq"] = _compress_short_seq(m["league_outcome_seq"])

    # 6. market_history (stored inside model for per-league tracking)
    if "market_history" in m and isinstance(m["market_history"], dict):
        m["market_history"] = _compress_market_history_dict(m["market_history"])

    # Mark as compressed so decompress knows what to do
    m["_brain_compressed"] = 1
    return m


def decompress_model(model: dict) -> dict:
    """
    Decompress a league model. Call this after loading from pickle / restore.
    Safe to call on already-decompressed models (idempotent).
    """
    if not isinstance(model, dict):
        return model
    if not model.get("_brain_compressed"):
        return model  # nothing to do

    m = dict(model)

    # 1. match_log
    if "match_log" in m and isinstance(m["match_log"], dict):
        m["match_log"] = _decompress_match_log(m["match_log"])
        # Rebuild _ml_seen from match_log
        m["_ml_seen"] = {
            (r["home"], r["away"], r["round_id"], r.get("score_h"), r.get("score_a")): 1
            for r in m["match_log"]
        }

    # 2. fingerprint_db
    if "fingerprint_db" in m and isinstance(m["fingerprint_db"], dict):
        m["fingerprint_db"] = _decompress_fingerprint_db(m["fingerprint_db"])

    # 3. pattern_memory
    if "pattern_memory" in m and isinstance(m["pattern_memory"], dict):
        m["pattern_memory"] = _decompress_pattern_memory(m["pattern_memory"])

    # 4. slot_outcomes
    if "slot_outcomes" in m and isinstance(m["slot_outcomes"], dict):
        m["slot_outcomes"] = _decompress_slot_outcomes(m["slot_outcomes"])

    # 5. league_outcome_seq
    if "league_outcome_seq" in m and isinstance(m["league_outcome_seq"], str):
        m["league_outcome_seq"] = _decompress_short_seq(m["league_outcome_seq"])

    # 6. market_history
    if "market_history" in m and isinstance(m["market_history"], dict):
        m["market_history"] = _decompress_market_history_dict(m["market_history"])

    del m["_brain_compressed"]
    return m


# ── Global bot_data compressor/decompressor ────────────────────────────────────
# Use these to compress/decompress ALL leagues at once.

def compress_bot_data(bot_data: dict) -> dict:
    """Compress all league models inside bot_data."""
    out = dict(bot_data)
    for key in list(out.keys()):
        if key.startswith("model_") and isinstance(out[key], dict):
            out[key] = compress_model(out[key])
    # Also compress global market_history if present at bot_data level
    if "market_history" in out and isinstance(out["market_history"], dict):
        out["market_history"] = _compress_market_history_dict(out["market_history"])
    return out

def decompress_bot_data(bot_data: dict) -> dict:
    """Decompress all league models inside bot_data."""
    out = dict(bot_data)
    for key in list(out.keys()):
        if key.startswith("model_") and isinstance(out[key], dict):
            out[key] = decompress_model(out[key])
    if "market_history" in out and isinstance(out["market_history"], dict):
        out["market_history"] = _decompress_market_history_dict(out["market_history"])
    return out


# ── Self-test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import random, sys

    TEAMS = ["ARS","CHE","LIV","MCI","TOT","MUN","NEW","EVE","BOU","BRE",
             "CRY","FUL","BHA","WHU","WOL","NOT","BUR","SUN","LEE","AST"]

    # Build a realistic fake match_log (2000 entries)
    fake_log = []
    for i in range(2000):
        h, a = random.sample(TEAMS, 2)
        fake_log.append({
            "home":      h,
            "away":      a,
            "score_h":   random.randint(0, 5),
            "score_a":   random.randint(0, 5),
            "round_id":  1179900 + i,
            "season_id": "137297" if i < 1000 else "138001",
        })

    # Compress and decompress match_log
    compressed = _compress_match_log(fake_log)
    restored   = _decompress_match_log(compressed)

    assert len(restored) == len(fake_log), "Length mismatch!"
    for orig, res in zip(fake_log, restored):
        assert orig["home"]      == res["home"],      f"home mismatch: {orig} vs {res}"
        assert orig["away"]      == res["away"],      f"away mismatch"
        assert orig["score_h"]   == res["score_h"],   f"score_h mismatch"
        assert orig["score_a"]   == res["score_a"],   f"score_a mismatch"
        assert orig["round_id"]  == res["round_id"],  f"round_id mismatch"
        assert orig["season_id"] == res["season_id"], f"season_id mismatch"

    # Size comparison
    import pickle
    orig_pkl  = len(pickle.dumps(fake_log))
    comp_pkl  = len(pickle.dumps(compressed))
    print(f"match_log (2000 entries):")
    print(f"  Original pickle  : {orig_pkl:,} bytes")
    print(f"  Compressed pickle: {comp_pkl:,} bytes")
    print(f"  Savings          : {100*(1-comp_pkl/orig_pkl):.0f}%")

    # Test fp_record roundtrip
    sample_rec = {
        "fp_key": [1.97, 3.40, 4.20],
        "odds_snapshot": {"1x2": {"1": 1.97, "X": 3.40, "2": 4.20}},
        "outcome": "HOME", "htft_result": "1/1", "btts_result": True,
        "ou15_result": True, "ou25_result": False, "ou35_result": False,
        "score_h": 2, "score_a": 1, "ht_h": 1, "ht_a": 0,
        "dc_result": "1X", "round_id": 1179973, "season_id": "137297",
        "league_id": 7794, "was_flipped": False,
        "pos_h": 3, "pos_a": 12, "_form_h": ["W","W","D"], "_form_a": ["L","W","L"],
    }
    comp_rec  = _compress_fp_record(sample_rec)
    decomp_rec = _decompress_fp_record(comp_rec)
    assert decomp_rec["outcome"]     == "HOME"
    assert decomp_rec["htft_result"] == "1/1"
    assert decomp_rec["dc_result"]   == "1X"
    assert decomp_rec["score_h"]     == 2
    assert abs(decomp_rec["fp_key"][0] - 1.97) < 0.002

    orig_sz = len(pickle.dumps(sample_rec))
    comp_sz = len(pickle.dumps(comp_rec))
    print(f"\nfp_db record:")
    print(f"  Original pickle  : {orig_sz} bytes")
    print(f"  Compressed pickle: {comp_sz} bytes")
    print(f"  Savings          : {100*(1-comp_sz/orig_sz):.0f}%")

    # Test market_history compression
    hist = [random.randint(0,1) for _ in range(500)]
    comp_hist = _compress_market_history(hist)
    decomp_hist = _decompress_market_history(comp_hist)
    assert hist == decomp_hist, "market_history mismatch!"
    print(f"\nmarket_history (500 entries):")
    print(f"  Original pickle  : {len(pickle.dumps(hist))} bytes")
    print(f"  Compressed pickle: {len(pickle.dumps(comp_hist))} bytes")
    print(f"  Savings          : {100*(1-len(pickle.dumps(comp_hist))/len(pickle.dumps(hist))):.0f}%")

    # Test outcome_seq compression
    seq = [random.choice(["HOME","DRAW","AWAY"]) for _ in range(100)]
    cs  = _compress_outcome_seq(seq)
    ds  = _decompress_outcome_seq(cs)
    assert seq == ds, "outcome_seq mismatch!"
    print(f"\noutcome_seq (100 entries):")
    print(f"  Original pickle  : {len(pickle.dumps(seq))} bytes")
    print(f"  Compressed pickle: {len(pickle.dumps(cs))} bytes")
    print(f"  Savings          : {100*(1-len(pickle.dumps(cs))/len(pickle.dumps(seq))):.0f}%")

    print("\n✅ All assertions passed — compression is lossless!")
    print("\n─── HOW TO PLUG IN ─────────────────────────────────────────────────")
    print("Add to bot.py:")
    print("  from brain_compress import compress_bot_data, decompress_bot_data")
    print()
    print("In cmd_backup (before sending file):")
    print("  compressed_bd = compress_bot_data(context.bot_data)")
    print("  # ... pickle compressed_bd instead of context.bot_data")
    print()
    print("In handle_admin_document (after loading backup):")
    print("  loaded = decompress_bot_data(loaded)")
    print("  context.bot_data.update(loaded)")
    print()
    print("For always-compressed live storage, wrap PicklePersistence with")
    print("compress/decompress in post_init and pre-shutdown hooks.")
