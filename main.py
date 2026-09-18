#!/usr/bin/env python3
"""
Live Sports Auto-Update & Data Transformation Pipeline
Converts raw sports JSON from axsports repository into:
1. `events.json` conforming to PostgreSQL `public.events` schema.
2. `channels.json` matching the application's channels schema.
3. `playlist.m3u` IPTV playlist with VLC and Kodi headers.
4. Optional direct sync to Supabase / PostgreSQL database.
"""

import os
import sys
import re
import json
import hashlib
import logging
import argparse
import urllib.parse
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple

import requests
from dotenv import load_dotenv

# Optional dependencies for DB sync & rich logging
try:
    from rich.console import Console
    from rich.logging import RichHandler
    console = Console()
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger("live_sports")

# Load environment variables
load_dotenv()

DEFAULT_SPORTS_URL = "https://raw.githubusercontent.com/srhady/axsports/main/live_sports.json"
DEFAULT_CATEGORY_ID = os.getenv("CATEGORY_ID", "events")
HASH_FILE = "last_hash_sports_events.txt"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
DEFAULT_REFERER = "https://iframe.rumsport8.live/"

# Known sport keyword & league mapping dictionary
LEAGUE_SPORT_MAP = {
    # Football / Soccer
    "fa cup": "Football",
    "premier league": "Football",
    "championship": "Football",
    "j1 league": "Football",
    "bundesliga": "Football",
    "2. bundesliga": "Football",
    "la liga": "Football",
    "segunda división": "Football",
    "segunda division": "Football",
    "ligue 1": "Football",
    "ligue 2": "Football",
    "serie a": "Football",
    "serie b": "Football",
    "liga profesional argentina": "Football",
    "liga 1": "Football",
    "süper lig": "Football",
    "super lig": "Football",
    "thai league 1": "Football",
    "fa wsl": "Football",
    "premiership": "Football",
    "liga mx": "Football",
    "liga de expansión mx": "Football",
    "liga de expansion mx": "Football",
    "k league 1": "Football",
    "hnl": "Football",
    "taça de portugal": "Football",
    "taca de portugal": "Football",
    "major league soccer": "Football",
    "mls": "Football",
    "nwsl women": "Football",
    "liga mx femenil": "Football",
    "victoria npl": "Football",
    "campionato primavera - 1": "Football",
    "friendlies clubs": "Football",
    "primeira liga": "Football",
    "copa libertadores": "Football",
    "copa sudamericana": "Football",
    "uefa champions league": "Football",
    "uefa europa league": "Football",
    "uefa conference league": "Football",

    # Baseball
    "mlb": "Baseball",
    "npb": "Baseball",
    "kbo": "Baseball",

    # Basketball
    "wnba": "Basketball",
    "nba": "Basketball",
    "euroleague": "Basketball",
    "fiba": "Basketball",

    # Cricket
    "cpl": "Cricket",
    "caribbean premier league": "Cricket",
    "ipl": "Cricket",
    "sri lanka tour of england": "Cricket",
    "t20": "Cricket",
    "odi": "Cricket",
    "test": "Cricket",
    "bbl": "Cricket",
    "psl": "Cricket",

    # Motorsport / Racing
    "italian formula 4 championship": "Motorsport",
    "motogp": "Motorsport",
    "ferrari challenge australasia": "Motorsport",
    "formula 1": "Motorsport",
    "formula 4": "Motorsport",
    "f1": "Motorsport",
    "nascar": "Motorsport",
    "indycar": "Motorsport",

    # Golf
    "pga tour": "Golf",
    "biltmore championship": "Golf",
    "liv golf": "Golf",
    "dp world tour": "Golf",

    # Tennis
    "wta guadalajara": "Tennis",
    "wta": "Tennis",
    "atp": "Tennis",
    "us open": "Tennis",
    "wimbledon": "Tennis",
    "roland garros": "Tennis",
    "australian open": "Tennis",

    # Rugby / Aussie Rules
    "wxv": "Rugby",
    "npc": "Rugby",
    "afl womens premiership": "AFL",
    "afl premiership": "AFL",
    "nrl women's premiership": "Rugby",
    "super rugby aus": "Rugby",
    "super rugby": "Rugby",
    "cfl": "CFL",

    # Darts
    "world series of darts finals": "Darts",
    "pdc darts": "Darts",

    # Cycling / Mountain Biking
    "uci pro series": "Cycling",
    "uci mountain bike": "Cycling",
    "mountain bike": "Cycling",
    "tour of luxembourg": "Cycling",
    "tour de france": "Cycling",

    # Table Tennis
    "etpl": "Table Tennis",
    "table tennis": "Table Tennis",

    # Boxing / Combat Sports
    "matchroom boxing": "Boxing",
    "boxing": "Boxing",
    "ufc": "MMA",
    "mma": "MMA",

    # Sailing
    "sail grand prix": "Sailing",
    "rolex switzerland sail grand prix": "Sailing",

    # Horse Racing
    "horse racing": "Horse Racing"
}


def calculate_sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def get_last_hash(filepath: str) -> str:
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception as e:
            logger.warning(f"Failed to read hash file '{filepath}': {e}")
    return ""


def save_last_hash(filepath: str, content_hash: str) -> None:
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content_hash)
        logger.info(f"Saved payload hash to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save hash file '{filepath}': {e}")


def fetch_raw_sports_data(url: str) -> Tuple[str, Dict[str, Any]]:
    logger.info(f"Fetching live sports data from: {url}...")
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    raw_text = resp.text
    data = resp.json()
    logger.info(f"Successfully downloaded {len(raw_text.encode('utf-8'))} bytes.")
    return raw_text, data


def detect_sport(league_name: str, match_name: str) -> str:
    """Intelligently detects sport from league and match titles."""
    l_lower = (league_name or "").lower().strip()
    m_lower = (match_name or "").lower().strip()

    # Exact or substring lookup in league map
    for key, sport in LEAGUE_SPORT_MAP.items():
        if key in l_lower or key in m_lower:
            return sport

    # General heuristic keyword checks
    if any(k in l_lower or k in m_lower for k in ["fc", "united", "city", "real", "cup", "league", "athletic", "dynamo"]):
        return "Football"
    if any(k in l_lower or k in m_lower for k in ["cricket", "tour of", "t20", "odi", "innings"]):
        return "Cricket"
    if any(k in l_lower or k in m_lower for k in ["basketball", "hoops"]):
        return "Basketball"
    if any(k in l_lower or k in m_lower for k in ["grand prix", "racing", "speedway", "circuit", "sprint race"]):
        return "Motorsport"
    if any(k in l_lower or k in m_lower for k in ["open", "classic", "invitational"]) and "golf" in (l_lower + m_lower):
        return "Golf"

    return "Sports"


def clean_league_name(league_raw: str) -> str:
    if not league_raw:
        return "Live Sports"
    # Fix common mojibake characters if any
    cleaned = (
        league_raw.replace("Sper", "Süper")
        .replace("Segunda Divisin", "Segunda División")
        .replace("Liga de Expansin", "Liga de Expansión")
        .replace("Taa", "Taça")
        .strip()
    )
    return cleaned


def parse_event_time(start_at: Any) -> str:
    """Converts epoch seconds or date string to ISO 8601 UTC string."""
    if not start_at:
        return datetime.now(timezone.utc).isoformat()
    try:
        if isinstance(start_at, (int, float)) or (isinstance(start_at, str) and start_at.isdigit()):
            dt = datetime.fromtimestamp(int(start_at), tz=timezone.utc)
            return dt.isoformat()
        # String date parsing fallback
        from dateutil import parser as date_parser
        dt = date_parser.parse(str(start_at))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception as e:
        logger.debug(f"Could not parse start time '{start_at}': {e}")
        return datetime.now(timezone.utc).isoformat()


def build_banner_url(match_name: str, fallback_local: str, fallback_league: str) -> str:
    """Generates standard Bing poster URL with fallbacks."""
    if match_name:
        encoded = urllib.parse.quote(match_name.strip())
        return f"https://raw.githubusercontent.com/srhady/axsports/main/bing_posters/{encoded}.jpg"
    return fallback_local or fallback_league or ""


def transform_sports_data(matches: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Transforms axsports matches into:
    - events conforming strictly to public.events schema
    - channels matching channel records
    """
    events: List[Dict[str, Any]] = []
    channels: List[Dict[str, Any]] = []

    # Partition matches into live, hot upcoming, and other upcoming
    live_items: List[Dict[str, Any]] = []
    upcoming_items: List[Dict[str, Any]] = []
    ended_items: List[Dict[str, Any]] = []

    for m in matches:
        raw_status = str(m.get("status") or "").upper().strip()
        is_playing = bool(m.get("is_playing"))
        has_ended = bool(m.get("has_ended"))

        if is_playing or raw_status in ["1H", "2H", "HT", "LIVE", "ET", "P", "INNINGS", "BREAK"]:
            live_items.append((m, "live"))
        elif has_ended or raw_status in ["FT", "AET", "ENDED", "FINISHED", "POST", "CANC"]:
            ended_items.append((m, "ended"))
        else:
            upcoming_items.append((m, "upcoming"))

    # Sort live items by start_at, then hot upcoming, then upcoming chronologically
    live_items.sort(key=lambda x: x[0].get("start_at", 0))
    upcoming_items.sort(key=lambda x: (not x[0].get("ishot", False), x[0].get("start_at", 0)))
    ended_items.sort(key=lambda x: x[0].get("start_at", 0))

    all_sorted = live_items + upcoming_items + ended_items

    for sort_idx, (m, norm_status) in enumerate(all_sorted):
        raw_id = m.get("id")
        if not raw_id:
            continue

        event_id = f"axsports-{raw_id}"
        name = (m.get("name") or "").strip()
        league_raw = m.get("league_name") or ""
        league = clean_league_name(league_raw)
        sport = detect_sport(league, name)

        local_name = (m.get("localteam_name") or "").strip()
        local_logo = (m.get("localteam_logo") or "").strip()
        visitor_name = (m.get("visitorteam_name") or "").strip()
        visitor_logo = (m.get("visitorteam_logo") or "").strip()
        league_logo = (m.get("league_logo") or "").strip()

        # Handle individual sports where local/visitor might not be set
        if not local_name and "vs" in name.lower():
            parts = re.split(r"\s+vs\.?\s+", name, flags=re.IGNORECASE)
            if len(parts) == 2:
                local_name, visitor_name = parts[0].strip(), parts[1].strip()

        banner = build_banner_url(name, local_logo, league_logo)
        start_time_iso = parse_event_time(m.get("start_at") or m.get("timestamp"))

        # Process stream links
        raw_links = m.get("link_live") or []
        channel_ids_for_event: List[str] = []
        seen_stream_urls = set()

        server_counter = 1
        referer = m.get("referer") or DEFAULT_REFERER

        for link in raw_links:
            # Priority: direct chunk URL with token > raw stream link
            stream_url = link.get("videoURL") or link.get("stream_link") or ""
            stream_url = stream_url.strip()

            if not stream_url or stream_url in seen_stream_urls:
                continue
            seen_stream_urls.add(stream_url)

            quality = link.get("display_name") or "HD"
            channel_id = f"{event_id}-server{server_counter}"
            channel_name = f"Server {server_counter}"

            ch_dict = {
                "id": channel_id,
                "name": channel_name,
                "logo": banner or local_logo or league_logo,
                "category": DEFAULT_CATEGORY_ID,
                "country": "Global",
                "language": "English",
                "stream_url": stream_url,
                "is_live": (norm_status == "live"),
                "is_trending": False,
                "quality": quality,
                "headers": {
                    "User-Agent": USER_AGENT,
                    "Referer": referer
                },
                "sort_order": server_counter - 1,
                "proxy": False,
                "drm": None
            }

            channels.append(ch_dict)
            channel_ids_for_event.append(channel_id)
            server_counter += 1

        is_featured = False

        # Construct event matching public.events table
        event_dict = {
            "id": event_id,
            "sport": sport,
            "league": league,
            "home_team": {
                "name": local_name or (name if not visitor_name else "Home"),
                "logo": local_logo or banner,
                "flag": local_logo or banner
            },
            "away_team": {
                "name": visitor_name or ("" if not visitor_name else "Away"),
                "logo": visitor_logo or banner,
                "flag": visitor_logo or banner
            },
            "start_time": start_time_iso,
            "status": norm_status,
            "channels": channel_ids_for_event,
            "banner": banner,
            "is_featured": False,
            "hero_type": 1,
            "custom_title": name,
            "sort_order": sort_idx
        }

        events.append(event_dict)

    return events, channels


def generate_m3u_content(events: List[Dict[str, Any]], channels: List[Dict[str, Any]]) -> str:
    """Generates standard M3U playlist with VLC and Kodi directives."""
    now_utc = datetime.now(timezone.utc).isoformat()
    lines = [
        "#EXTM3U",
        "#PLAYLIST: Live Sports Auto-Sync Playlist",
        f"#EXT-X-UPDATED: {now_utc}",
        ""
    ]

    channel_lookup = {ch["id"]: ch for ch in channels}

    for ev in events:
        sport = ev.get("sport", "Sports")
        event_id = ev.get("id", "")

        for ch_id in ev.get("channels", []):
            ch = channel_lookup.get(ch_id)
            if not ch:
                continue

            ch_name = ch.get("name", "")
            ch_logo = ch.get("logo", "")
            stream_url = ch.get("stream_url", "")
            headers = ch.get("headers") or {}
            ua = headers.get("User-Agent", USER_AGENT)
            ref = headers.get("Referer", DEFAULT_REFERER)

            title = ev.get("custom_title") or ev.get("id", "")
            league = ev.get("league", "")
            m3u_title = f"{title} - {league} [{ch_name}]" if league else f"{title} [{ch_name}]"

            lines.append(
                f'#EXTINF:-1 tvg-id="{event_id}" tvg-name="{m3u_title}" tvg-logo="{ch_logo}" group-title="{sport}", {m3u_title}'
            )
            lines.append(f"#EXTVLCOPT:http-user-agent={ua}")
            lines.append(f"#EXTVLCOPT:http-referrer={ref}")
            lines.append("#KODIPROP:inputstream.adaptive.manifest_type=hls")
            lines.append(stream_url)
            lines.append("")

    return "\n".join(lines)


def sync_to_supabase(events: List[Dict[str, Any]], channels: List[Dict[str, Any]]) -> None:
    """Syncs events and channels to Supabase."""
    try:
        from supabase import create_client, Client, ClientOptions
    except ImportError:
        logger.error("supabase package is not installed. Run: pip install supabase")
        return

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    admin_token = os.getenv("ADMIN_SECRET_TOKEN")

    if not supabase_url or not supabase_key:
        logger.warning("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing. Skipping DB sync.")
        return

    headers = {}
    if admin_token:
        headers["x-admin-token"] = admin_token

    options = ClientOptions(headers=headers) if headers else None
    logger.info(f"Connecting to Supabase at {supabase_url}...")
    supabase: Client = create_client(supabase_url, supabase_key, options=options)

    # 1. Verify / Create Category
    category_id = DEFAULT_CATEGORY_ID
    logger.info(f"Verifying category '{category_id}' in categories table...")
    try:
        cat_res = supabase.table("categories").select("id").eq("id", category_id).execute()
        if not cat_res.data:
            logger.info(f"Creating category '{category_id}'...")
            supabase.table("categories").upsert({
                "id": category_id,
                "name": "Live Sports",
                "icon": "sports",
                "sort_order": 10,
                "active": True
            }).execute()
    except Exception as e:
        logger.warning(f"Category verification check note: {e}")

    # 2. Upsert Channels in batches
    if channels:
        logger.info(f"Upserting {len(channels)} channels to 'channels' table...")
        batch_size = 50
        for i in range(0, len(channels), batch_size):
            batch = channels[i : i + batch_size]
            supabase.table("channels").upsert(batch).execute()

    # 3. Upsert Events in batches
    if events:
        logger.info(f"Upserting {len(events)} events to 'events' table...")
        batch_size = 50
        for i in range(0, len(events), batch_size):
            batch = events[i : i + batch_size]
            supabase.table("events").upsert(batch).execute()

    # 4. Clean up stale axsports channels & events
    try:
        logger.info("Cleaning up stale axsports records...")
        existing_ev_res = supabase.table("events").select("id").like("id", "axsports-%").execute()
        existing_ev_ids = {row["id"] for row in (existing_ev_res.data or [])}
        new_ev_ids = {ev["id"] for ev in events}
        stale_ev_ids = list(existing_ev_ids - new_ev_ids)
        if stale_ev_ids:
            logger.info(f"Deleting {len(stale_ev_ids)} stale events...")
            for i in range(0, len(stale_ev_ids), 50):
                supabase.table("events").delete().in_("id", stale_ev_ids[i:i+50]).execute()

        existing_ch_res = supabase.table("channels").select("id").like("id", "axsports-%").execute()
        existing_ch_ids = {row["id"] for row in (existing_ch_res.data or [])}
        new_ch_ids = {ch["id"] for ch in channels}
        stale_ch_ids = list(existing_ch_ids - new_ch_ids)
        if stale_ch_ids:
            logger.info(f"Deleting {len(stale_ch_ids)} stale channels...")
            for i in range(0, len(stale_ch_ids), 50):
                supabase.table("channels").delete().in_("id", stale_ch_ids[i:i+50]).execute()
    except Exception as e:
        logger.warning(f"Stale cleanup note: {e}")

    # 5. Check app_settings
    try:
        settings_res = supabase.table("app_settings").select("events_version, channels_version").eq("id", 1).execute()
        if settings_res.data:
            s = settings_res.data[0]
            logger.info(f"Database version state: events_version={s.get('events_version')}, channels_version={s.get('channels_version')}")
    except Exception as e:
        logger.debug(f"app_settings query note: {e}")

    logger.info("Supabase sync completed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Live Sports Sync & Transformation Pipeline")
    parser.add_argument("--url", default=os.getenv("SPORTS_JSON_URL", DEFAULT_SPORTS_URL), help="Source JSON URL")
    parser.add_argument("--output-dir", default=".", help="Directory to save output files")
    parser.add_argument("--force", action="store_true", help="Force processing regardless of payload hash")
    parser.add_argument("--sync-db", action="store_true", help="Sync transformed records to Supabase / PostgreSQL")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    hash_path = os.path.join(args.output_dir, HASH_FILE)

    # 1. Fetch raw data
    try:
        raw_json, json_data = fetch_raw_sports_data(args.url)
    except Exception as e:
        logger.error(f"Failed to fetch sports JSON: {e}")
        sys.exit(1)

    # 2. Check hash
    curr_hash = calculate_sha256(raw_json)
    last_hash = get_last_hash(hash_path)

    if curr_hash == last_hash and not args.force and not args.sync_db:
        logger.info("Payload hash unchanged and --force not set. Files are already up to date.")
        sys.exit(0)

    matches = json_data.get("matches", [])
    logger.info(f"Found {len(matches)} matches in dataset.")

    # 3. Transform data
    events, channels = transform_sports_data(matches)
    live_count = sum(1 for e in events if e["status"] == "live")
    upcoming_count = sum(1 for e in events if e["status"] == "upcoming")
    logger.info(f"Transformed {len(events)} events (Live: {live_count}, Upcoming: {upcoming_count}) and {len(channels)} channel streams.")

    # 4. Save events.json
    events_path = os.path.join(args.output_dir, "events.json")
    with open(events_path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(events)} events to {events_path}")

    # 5. Save channels.json
    channels_path = os.path.join(args.output_dir, "channels.json")
    with open(channels_path, "w", encoding="utf-8") as f:
        json.dump(channels, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(channels)} channels to {channels_path}")

    # 6. Save playlist.m3u
    m3u_content = generate_m3u_content(events, channels)
    m3u_path = os.path.join(args.output_dir, "playlist.m3u")
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write(m3u_content)
    logger.info(f"Saved M3U playlist to {m3u_path}")

    # 7. Save hash
    save_last_hash(hash_path, curr_hash)

    # 8. Optional DB Sync
    if args.sync_db:
        sync_to_supabase(events, channels)

    logger.info("All pipeline tasks completed successfully!")


if __name__ == "__main__":
    main()
