#!/usr/bin/env python3
"""
Baseball Roster & Stats Scraper
Scrapes NCAA college baseball team data and exports to Google Sheets.

Supports:
  - Sidearm Sports (uses internal bio+stats API — covers most NCAA schools)
  - Generic HTML table fallback for other platforms

Usage:
    python3 roster_scraper.py <team_url> [options]
"""

import re
import json
import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Constants ──────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

CURRENT_YEAR = datetime.now().year
CLASS_TO_GRAD = {
    "fr": CURRENT_YEAR + 3, "so": CURRENT_YEAR + 2,
    "jr": CURRENT_YEAR + 1, "sr": CURRENT_YEAR,
}

# ── URL helpers ────────────────────────────────────────────────────────────────

def get_roster_url(url: str) -> str:
    url = url.rstrip("/")
    if "/roster" in url:
        return url
    if "/sports/baseball" in url:
        base = url.split("/sports/baseball")[0]
        return f"{base}/sports/baseball/roster"
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/sports/baseball/roster"


def get_stats_url(roster_url: str) -> str:
    return roster_url.replace("/roster", "/stats")


def get_base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def get_team_label(url: str) -> str:
    """Return a clean school name from og:site_name, falling back to domain parsing."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        og = soup.find("meta", property="og:site_name")
        if og and og.get("content", "").strip():
            name = og["content"].strip()
            # Strip "Athletics" / "Athletic Department"
            name = re.sub(r"\s*\bAthletics?\b.*", "", name, flags=re.IGNORECASE).strip()
            # If it starts with an all-caps abbreviation (UNO, UTRGV, SIUE, LSU …)
            # use just that — drop the mascot word that follows
            words = name.split()
            if words and re.match(r"^[A-Z]{2,8}$", words[0]):
                return words[0]
            return name
    except Exception:
        pass
    host = urlparse(url).netloc.replace("www.", "")
    return host.split(".")[0].title()

# ── Stat calculators ──────────────────────────────────────────────────────────

def _int(val) -> int:
    try:
        return int(val or 0)
    except (ValueError, TypeError):
        return 0


def _fmt(val: float, decimals: int = 3) -> str:
    """Format a rate stat, e.g. 0.321 → '.321'"""
    if val is None:
        return ""
    s = f"{val:.{decimals}f}"
    return s.lstrip("0") or "0"  # .321 not 0.321


def calc_avg(h, ab) -> str:
    ab = _int(ab)
    return _fmt(_int(h) / ab) if ab > 0 else ""


def calc_obp(h, bb, hbp, ab, sf) -> str:
    num = _int(h) + _int(bb) + _int(hbp)
    den = _int(ab) + _int(bb) + _int(hbp) + _int(sf)
    return _fmt(num / den) if den > 0 else ""


def calc_slg(h, doubles, triples, hr, ab) -> str:
    ab = _int(ab)
    if ab == 0:
        return ""
    singles = _int(h) - _int(doubles) - _int(triples) - _int(hr)
    tb = singles + 2 * _int(doubles) + 3 * _int(triples) + 4 * _int(hr)
    return _fmt(tb / ab)


def class_to_grad_year(class_str: str) -> str:
    s = class_str.strip().lower()
    # Strip "redshirt" prefix so "Redshirt Junior" → "junior"
    s = re.sub(r"^redshirt\s*", "", s).strip()
    mapping = {
        "fr": CURRENT_YEAR + 3, "freshman": CURRENT_YEAR + 3,
        "so": CURRENT_YEAR + 2, "sophomore": CURRENT_YEAR + 2,
        "jr": CURRENT_YEAR + 1, "junior": CURRENT_YEAR + 1,
        "sr": CURRENT_YEAR,     "senior": CURRENT_YEAR,
        "graduate": CURRENT_YEAR, "graduate student": CURRENT_YEAR, "gr": CURRENT_YEAR,
    }
    year = mapping.get(s) or mapping.get(s[:2])
    return str(year) if year else class_str

# ── Sidearm Sports scraper ────────────────────────────────────────────────────

def _get_sidearm_rp_ids(roster_url: str) -> list[tuple[str, str]]:
    """
    Parse the roster page JSON-LD to get (name, rp_id) pairs.
    Returns list of (name, rp_id).
    """
    resp = requests.get(roster_url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    players = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue

        if isinstance(data, list):
            data = {"item": data}
        items = data.get("item", [])
        for item in items:
            if item.get("@type") == "Person":
                name = item.get("name", "")
                url  = item.get("url", "")
                m = re.search(r"rp_id=(\d+)", url)
                if name and m:
                    players.append((name, m.group(1)))

    return players


def _parse_ip(ip_str) -> float:
    """Convert Sidearm innings-pitched string '7.2' (7 and 2/3 IP) to a float."""
    try:
        parts = str(ip_str or "0").split(".")
        full = int(parts[0])
        outs = int(parts[1]) if len(parts) > 1 else 0
        return full + outs / 3
    except Exception:
        return 0.0


def calc_era(er, ip_str) -> str:
    ip = _parse_ip(ip_str)
    return f"{_int(er) / ip * 9:.2f}" if ip > 0 else ""


def calc_whip(bb, h, ip_str) -> str:
    ip = _parse_ip(ip_str)
    return f"{(_int(bb) + _int(h)) / ip:.2f}" if ip > 0 else ""


# Positions considered pitching vs hitting
_PITCHER_POSITIONS = {"rhp", "lhp", "p", "sp", "rp", "cp", "hp",
                      "lhsp", "rhsp", "lhrp", "rhrp"}
_HITTER_POSITIONS  = {"c", "1b", "2b", "3b", "ss", "of", "lf", "cf", "rf",
                      "dh", "inf", "utl", "util", "ph", "pr", "if", "mid"}


def _is_pitcher_pos(pos: str) -> bool:
    return any(p in _PITCHER_POSITIONS for p in re.split(r"[/,\s]+", pos.lower()) if p)


def _is_hitter_pos(pos: str) -> bool:
    return any(p in _HITTER_POSITIONS for p in re.split(r"[/,\s]+", pos.lower()) if p)


def _fetch_sidearm_player(base_url: str, rp_id: str, sport: str, year: str):
    """Call the Sidearm internal API for one player's bio + hitting + pitching stats."""
    try:
        resp = requests.get(
            f"{base_url}/services/roster_bio_stats.ashx",
            params={"rp_id": rp_id, "sport": sport, "year": year},
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None

    bio        = data.get("bio") or {}
    stats      = data.get("stats") or {}
    game_stats = stats.get("game_by_game_stats") or []

    # Pull season-total hitting and pitching rows (footer stats)
    hitting  = {}
    pitching = {}
    for row in game_stats:
        if not row.get("is_a_footer_stat"):
            continue
        if row.get("hitting")  and not hitting:
            hitting  = row["hitting"]
        if row.get("pitching") and not pitching:
            pitching = row["pitching"]

    class_raw = bio.get("academic_class", "")

    # ── Hitting fields ────────────────────────────────────────────────────────
    h   = hitting.get("hits", "0")
    ab  = hitting.get("at_bats", "0")
    bb  = hitting.get("walks", "0")
    hbp = hitting.get("hit_by_pitch", "0")
    sf  = hitting.get("sacrifice_flies", "0")
    dbl = hitting.get("doubles", "0")
    tri = hitting.get("triples", "0")
    hr  = hitting.get("home_runs", "0")
    has_hitting = hitting and _int(ab) > 0

    # ── Pitching fields ───────────────────────────────────────────────────────
    ip      = pitching.get("innings_pitched", "0")
    p_h     = pitching.get("hits_allowed", "0")
    p_bb    = pitching.get("walks_allowed", "0")
    er      = pitching.get("earned_runs_allowed", "0")
    has_pitching = pitching and _parse_ip(ip) > 0

    bio_fields = {
        "Name":        f"{bio.get('first_name', '')} {bio.get('last_name', '')}".strip(),
        "#":           bio.get("jersey_number", ""),
        "Position":    bio.get("position", ""),
        "Height":      bio.get("height", ""),
        "Weight":      bio.get("weight", ""),
        "Class":       class_raw,
        "Grad Year":   class_to_grad_year(class_raw),
        "Hometown":    bio.get("hometown", ""),
        "High School": bio.get("highschool", ""),
    }

    return {
        **bio_fields,
        # Hitting
        "AVG":  calc_avg(h, ab)              if has_hitting else "",
        "OBP":  calc_obp(h, bb, hbp, ab, sf) if has_hitting else "",
        "SLG":  calc_slg(h, dbl, tri, hr, ab) if has_hitting else "",
        "H":    h   if has_hitting else "",
        "2B":   dbl if has_hitting else "",
        "HR":   hr  if has_hitting else "",
        "RBI":  hitting.get("runs_batted_in", "") if has_hitting else "",
        "AB":   ab  if has_hitting else "",
        "R":    hitting.get("runs_scored", "")   if has_hitting else "",
        "BB":   bb  if has_hitting else "",
        "H_SO": hitting.get("strikeouts", "")    if has_hitting else "",
        "SB":   hitting.get("stolen_bases", "")  if has_hitting else "",
        # Pitching
        "ERA":  calc_era(er, ip)            if has_pitching else "",
        "WHIP": calc_whip(p_bb, p_h, ip)   if has_pitching else "",
        "W":    pitching.get("wins", "")    if has_pitching else "",
        "L":    pitching.get("losses", "")  if has_pitching else "",
        "SV":   pitching.get("saves", "")   if has_pitching else "",
        "IP":   ip                          if has_pitching else "",
        "P_H":  p_h                         if has_pitching else "",
        "ER":   er                          if has_pitching else "",
        "P_BB": p_bb                        if has_pitching else "",
        "P_SO": pitching.get("strikeouts", "") if has_pitching else "",
        # Flags for classification
        "_has_hitting":  has_hitting,
        "_has_pitching": has_pitching,
        "_pos":          bio.get("position", ""),
    }


def scrape_sidearm(roster_url: str) -> list[dict]:
    """
    Full Sidearm Sports scrape: roster page → per-player bio+stats API.
    Fetches all players concurrently.
    """
    base  = get_base_url(roster_url)
    year  = str(CURRENT_YEAR)
    sport = "baseball"

    id_pairs = _get_sidearm_rp_ids(roster_url)
    if not id_pairs:
        return []

    results = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(_fetch_sidearm_player, base, rp_id, sport, year): (name, rp_id)
            for name, rp_id in id_pairs
        }
        for future in as_completed(futures):
            name, rp_id = futures[future]
            player = future.result()
            if player:
                results[rp_id] = player

    # Return in original roster order
    ordered = []
    for name, rp_id in id_pairs:
        p = results.get(rp_id)
        if p:
            ordered.append(p)
        else:
            ordered.append({"Name": name, "#": "", "Position": "", "Height": "",
                            "Weight": "", "Class": "", "Grad Year": "", "Hometown": "",
                            "High School": "", "AVG": "", "OBP": "", "SLG": "",
                            "H": "", "2B": "", "HR": "", "RBI": "",
                            "AB": "", "R": "", "BB": "", "SO": "", "SB": ""})
    return ordered

# ── Generic HTML fallback ─────────────────────────────────────────────────────

def _scrape_html_roster(url: str) -> list[dict]:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    players = []

    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead:
            continue
        cols = [th.get_text(strip=True).lower() for th in thead.find_all("th")]
        if not any(h in cols for h in ["name", "player", "last name"]):
            continue

        for row in table.select("tbody tr"):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            d = dict(zip(cols, cells))
            name_key = next((k for k in d if "name" in k or "player" in k), cols[0])
            name = d.get(name_key, "").strip()
            if not name:
                continue
            pos_key  = next((k for k in d if "pos" in k), "")
            yr_key   = next((k for k in d if any(w in k for w in ["year","class","yr","eligibility"])), "")
            num_key  = next((k for k in d if any(w in k for w in ["no","num","#","jersey"])), "")
            class_raw = d.get(yr_key, "")
            players.append({
                "Name": name, "#": d.get(num_key, ""), "Position": d.get(pos_key, ""),
                "Height": "", "Weight": "", "Class": class_raw,
                "Grad Year": class_to_grad_year(class_raw),
                "Hometown": "", "High School": "",
                "AVG": "", "OBP": "", "SLG": "", "H": "", "2B": "", "HR": "",
                "RBI": "", "AB": "", "R": "", "BB": "", "SO": "", "SB": "",
            })
        if players:
            break

    return players

# ── Public API ────────────────────────────────────────────────────────────────

def scrape_roster(url: str) -> list[dict]:
    """
    Scrape roster. Detects Sidearm Sports and uses its internal API for
    full bio + stats. Falls back to HTML table parsing for other platforms.
    Returns fully-populated player dicts (stats already included).
    """
    # Try Sidearm first (most NCAA schools)
    players = scrape_sidearm(url)
    if players:
        return players

    # Generic HTML fallback
    return _scrape_html_roster(url)


def scrape_stats(url: str) -> dict:
    """
    For Sidearm sites, stats are already embedded in scrape_roster() results.
    This returns empty for Sidearm; merge() handles that gracefully.
    Kept for non-Sidearm HTML table sites.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return {}

    for table in soup.find_all("table"):
        text = table.get_text().lower()
        if "avg" in text and ("rbi" in text or "ab" in text):
            thead = table.find("thead")
            if not thead:
                continue
            cols = [th.get_text(strip=True).lower() for th in thead.find_all("th")]
            stats = {}
            for row in table.select("tbody tr"):
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) < 3:
                    continue
                d = dict(zip(cols, cells))
                name_key = next((k for k in d if "name" in k or "player" in k), cols[0] if cols else None)
                if not name_key:
                    continue
                name = d.get(name_key, "").strip()
                if name and name.lower() not in ("totals", "total", "team"):
                    stats[name] = d
            if stats:
                return stats
    return {}


def merge(roster: list[dict], stats: dict) -> list[dict]:
    """
    For Sidearm rosters, stats are already populated — pass through as-is.
    For HTML rosters, attempt to merge from the stats dict.
    """
    if not stats:
        # Sidearm path: roster already has stats
        return roster

    # HTML fallback path: try to inject stats by name match
    def _norm(s):
        return re.sub(r"\s+", " ", s.lower().strip())

    ALIASES = {
        "AVG": ["avg", "ba"], "OBP": ["obp", "ob%"], "SLG": ["slg", "slg%"],
        "H": ["h", "hits"], "2B": ["2b"], "HR": ["hr"], "RBI": ["rbi"],
        "AB": ["ab"], "R": ["r", "runs"], "BB": ["bb"], "SO": ["so"], "SB": ["sb"],
    }

    def _find(d, aliases):
        for k in d:
            if k in aliases:
                return d[k]
        return ""

    merged = []
    for p in roster:
        norm = _norm(p["Name"])
        s = {}
        for sname, sdata in stats.items():
            if _norm(sname) == norm:
                s = sdata
                break
        if not s:
            last = norm.split()[-1] if norm.split() else ""
            for sname, sdata in stats.items():
                if last and last in _norm(sname).split():
                    s = sdata
                    break
        row = dict(p)
        for col, aliases in ALIASES.items():
            if not row.get(col):
                row[col] = _find(s, aliases)
        merged.append(row)

    return merged


# ── Hitter / Pitcher split ────────────────────────────────────────────────────

# Columns to include in each sheet (bio fields shared, stats differ)
HITTER_COLS = [
    "Name", "#", "Position", "Height", "Weight", "Class",
    "AVG", "OBP", "SLG", "AB", "H", "2B", "HR", "RBI", "R", "BB", "H_SO", "SB",
]
PITCHER_COLS = [
    "Name", "#", "Position", "Height", "Weight", "Class",
    "ERA", "WHIP", "W", "L", "SV", "IP", "P_H", "ER", "P_BB", "P_SO",
]
# Friendly display names for the header row
HITTER_HEADERS  = HITTER_COLS[:6]  + ["AVG","OBP","SLG","AB","H","2B","HR","RBI","R","BB","SO","SB"]
PITCHER_HEADERS = PITCHER_COLS[:6] + ["ERA","WHIP","W","L","SV","IP","H","ER","BB","SO"]


def split_roster(players: list[dict]) -> tuple:
    """
    Split a roster into (hitters, pitchers) lists.

    Rules:
    - Pitcher position (RHP/LHP/P/etc.) → pitchers sheet
    - Hitter position (C/INF/OF/etc.)   → hitters sheet
    - Has pitching stats (IP > 0)       → pitchers sheet (regardless of position)
    - Has hitting stats (AB > 0)        → hitters sheet (regardless of position)
    - Two-way / ambiguous               → both sheets
    """
    hitters  = []
    pitchers = []

    for p in players:
        pos          = p.get("_pos", p.get("Position", ""))
        has_hitting  = p.get("_has_hitting",  bool(p.get("AVG") or p.get("H")))
        has_pitching = p.get("_has_pitching", bool(p.get("ERA") or p.get("IP")))
        is_p_pos     = _is_pitcher_pos(pos)
        is_h_pos     = _is_hitter_pos(pos)

        # A position player with pitching stats → both
        # A pitcher with batting stats → both
        # No position info → decide by stats; if neither, use hitters as default
        goes_hitting  = is_h_pos or has_hitting or (not is_p_pos and not has_pitching)
        goes_pitching = is_p_pos or has_pitching

        # Build clean output rows (drop internal flags)
        def _row(cols):
            return {h: p.get(k, "") for k, h in zip(cols, cols)}

        if goes_hitting:
            row = {h: p.get(k, "") for k, h in zip(HITTER_COLS, HITTER_HEADERS)}
            hitters.append(row)
        if goes_pitching:
            row = {h: p.get(k, "") for k, h in zip(PITCHER_COLS, PITCHER_HEADERS)}
            pitchers.append(row)

    hitters.sort(key=lambda p: _int(p.get("AB") or 0), reverse=True)
    pitchers.sort(key=lambda p: _parse_ip(p.get("IP") or 0), reverse=True)
    return hitters, pitchers
