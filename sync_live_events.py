#!/usr/bin/env python3
"""
PlayZ TV -> Supabase Live Events & Channels Sync Pipeline
Fetches decrypted live events, resolves multi-stream feeds with ClearKey DRM,
updates Supabase database, and exports events.json, channels.json, and playlist.m3u.
"""

import os
import re
import sys
import time
import base64
import json
import logging
import hashlib
import urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qsl

try:
    import requests
except ImportError:
    print("Error: 'requests' is required. Run 'pip install requests'.", file=sys.stderr)
    sys.exit(1)

try:
    from Crypto.Cipher import AES
except ImportError:
    try:
        from Cryptodome.Cipher import AES
    except ImportError:
        print("Error: 'pycryptodome' is required. Run 'pip install pycryptodome'.", file=sys.stderr)
        sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SyncLiveEvents")

# ---------------------------------------------------------
# Configuration & Environment Variables
# ---------------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://hqmhuvsjlykrdusfkmeg.supabase.co").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_ANON_KEY = os.environ.get(
    "SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhxbWh1dnNqbHlrcmR1c2ZrbWVnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODI4MjI3OTEsImV4cCI6MjA5ODM5ODc5MX0.sfNEW7QG0idyVVRHlDtYjITDfIbsFQQm2u9a3ffoNoA"
)
SUPABASE_ADMIN_TOKEN = (os.environ.get("ADMIN_SECRET_TOKEN") or os.environ.get("SUPABASE_ADMIN_TOKEN") or "GoLiveAdminSecret2026").strip()

EVENTS_FEED_URL = os.environ.get("EVENTS_FEED_URL", "https://pabbdyzone.shop/events.txt")
BASE_PRO_URL = os.environ.get("BASE_PRO_URL", "https://pabbdyzone.shop/")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "16"))
PURGE_HOURS = int(os.environ.get("PURGE_HOURS", "3"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", ".")

# Tier-1 AES-128-CBC Keys for PlayZ TV (Px.app engine)
KEY_PX = b"cz14RStkN01PVE5w"
IV_PX  = b"WTlEvckd2UR41sdk"


# ---------------------------------------------------------
# PlayZ TV AES Decryptor
# ---------------------------------------------------------
def decrypt_px(raw_b64_str: str) -> str:
    """
    Decrypts Tier-1 PlayZ TV encrypted payload:
    1. Base64 decode
    2. Swap adjacent bytes [i] <-> [i+1]
    3. Reverse entire bytearray
    4. Base64 decode second layer
    5. AES-128-CBC Decrypt + PKCS7 unpad
    """
    raw_clean = raw_b64_str.strip().replace("\n", "").replace("\r", "")
    step1 = bytearray(base64.b64decode(raw_clean))
    
    for i in range(0, len(step1) - 1, 2):
        step1[i], step1[i + 1] = step1[i + 1], step1[i]
        
    step1.reverse()
    step4 = base64.b64decode(bytes(step1))
    
    cipher = AES.new(KEY_PX, AES.MODE_CBC, IV_PX)
    dec = cipher.decrypt(step4)
    
    pad_len = dec[-1]
    if 1 <= pad_len <= 16 and dec[-pad_len:] == bytes([pad_len]) * pad_len:
        dec = dec[:-pad_len]
        
    return dec.decode("utf-8")


# ---------------------------------------------------------
# Helpers & Utilities
# ---------------------------------------------------------
def slugify(text: str) -> str:
    """Converts a title or name into a safe identifier."""
    if not text:
        return "event"
    s = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[-\s]+", "-", s)


def parse_stream_url_and_headers(raw_url: str):
    """
    Extracts headers and query params if encoded via pipe (e.g. url|User-Agent=...&Referer=...)
    Returns (clean_url, headers_dict)
    """
    if not raw_url:
        return "", {}
        
    if "|" in raw_url:
        parts = raw_url.split("|", 1)
        clean_url = parts[0].strip()
        headers_str = parts[1].strip()
        headers = {}
        for k, v in parse_qsl(headers_str, keep_blank_values=True):
            headers[k] = v
        return clean_url, headers
        
    return raw_url.strip(), {}


def parse_drm_clearkey(clearkey_str: str):
    """
    Converts 'kid:key' into Supabase DRM JSON:
    {'type': 'clearkey', 'kid': '...', 'key': '...'}
    """
    if not clearkey_str or ":" not in clearkey_str:
        return None
    parts = clearkey_str.split(":", 1)
    return {
        "type": "clearkey",
        "kid": parts[0].strip(),
        "key": parts[1].strip()
    }


def parse_event_datetime(date_str: str, time_str: str):
    """
    Parses DD/MM/YYYY and HH:MM:SS (UTC) to datetime in timezone.utc.
    """
    if not date_str or not time_str:
        return None
        
    clean_date = date_str.replace("\\", "").strip()
    clean_time = time_str.replace("\\", "").strip()
    
    try:
        dt = datetime.strptime(f"{clean_date} {clean_time}", "%d/%m/%Y %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        try:
            dt = datetime.strptime(f"{clean_date} {clean_time}", "%d/%m/%Y %H:%M")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None


def generate_m3u_playlist(events: list, channels: list) -> str:
    """Generates an IPTV M3U playlist with Kodi ClearKey DRM & VLC header directives."""
    now_utc = datetime.now(timezone.utc).isoformat()
    lines = [
        "#EXTM3U",
        "#PLAYLIST: PlayZ TV Live Sports Playlist",
        f"#EXT-X-UPDATED: {now_utc}",
        ""
    ]
    
    ch_map = {c["id"]: c for c in channels}
    for ev in events:
        sport = ev.get("sport", "Sports")
        event_id = ev.get("id", "")
        title = ev.get("custom_title") or ev.get("league", "Live Event")
        league = ev.get("league", "")
        
        for ch_id in ev.get("channels", []):
            ch = ch_map.get(ch_id)
            if not ch:
                continue
            
            stream_url = ch.get("stream_url", "")
            if not stream_url:
                continue
                
            stream_name = ch.get("name", title)
            m3u_title = f"{title} [{stream_name.split(' - ')[-1]}]" if " - " in stream_name else stream_name
            if league and league not in m3u_title:
                m3u_title = f"{m3u_title} ({league})"
                
            logo = ch.get("logo", ev.get("banner", ""))
            
            lines.append(
                f'#EXTINF:-1 tvg-id="{event_id}" tvg-name="{m3u_title}" tvg-logo="{logo}" group-title="{sport}", {m3u_title}'
            )
            
            headers = ch.get("headers") or {}
            if headers.get("User-Agent"):
                lines.append(f"#EXTVLCOPT:http-user-agent={headers['User-Agent']}")
            if headers.get("Referer"):
                lines.append(f"#EXTVLCOPT:http-referrer={headers['Referer']}")
                
            drm = ch.get("drm")
            if drm and drm.get("type") == "clearkey":
                lines.append("#KODIPROP:inputstream.adaptive.manifest_type=mpd")
                lines.append("#KODIPROP:inputstream.adaptive.license_type=org.w3.clearkey")
                lines.append(f'#KODIPROP:inputstream.adaptive.license_key={drm.get("kid")}:{drm.get("key")}')
            elif stream_url.endswith(".mpd"):
                lines.append("#KODIPROP:inputstream.adaptive.manifest_type=mpd")
            else:
                lines.append("#KODIPROP:inputstream.adaptive.manifest_type=hls")
                
            lines.append(stream_url)
            lines.append("")
            
    return "\n".join(lines)


# ---------------------------------------------------------
# Supabase REST Client
# ---------------------------------------------------------
class SupabaseSyncClient:
    def __init__(self, base_url: str, service_key: str, anon_key: str, admin_token: str):
        self.base_url = base_url
        self.headers = {
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }
        
        if service_key:
            logger.info("Using SUPABASE_SERVICE_ROLE_KEY for authentication.")
            self.headers["apikey"] = service_key
            self.headers["Authorization"] = f"Bearer {service_key}"
        else:
            logger.info("Using SUPABASE_ANON_KEY for authentication.")
            self.headers["apikey"] = anon_key
            self.headers["Authorization"] = f"Bearer {anon_key}"

        if admin_token:
            self.headers["x-admin-token"] = admin_token


    def upsert_rows(self, table: str, rows: list, batch_size: int = 100) -> int:
        if not rows:
            return 0
        endpoint = f"{self.base_url}/rest/v1/{table}"
        total_upserted = 0
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            resp = requests.post(endpoint, headers=self.headers, json=batch, timeout=30)
            if resp.status_code not in (200, 201):
                logger.error(f"Failed upserting batch into '{table}' (status {resp.status_code}): {resp.text}")
                resp.raise_for_status()
            total_upserted += len(batch)
            
        return total_upserted

    def get_existing_event_ids(self) -> dict:
        endpoint = f"{self.base_url}/rest/v1/events?select=id,start_time,status"
        resp = requests.get(endpoint, headers=self.headers, timeout=20)
        if resp.status_code == 200:
            return {row["id"]: row for row in resp.json()}
        return {}

    def delete_events(self, event_ids: list) -> int:
        if not event_ids:
            return 0
        endpoint = f"{self.base_url}/rest/v1/events"
        deleted = 0
        batch_size = 50
        for i in range(0, len(event_ids), batch_size):
            chunk = event_ids[i:i + batch_size]
            ids_str = ",".join(chunk)
            resp = requests.delete(f"{endpoint}?id=in.({ids_str})", headers=self.headers, timeout=30)
            if resp.status_code in (200, 204):
                deleted += len(chunk)
            else:
                logger.warning(f"Failed deleting events chunk: {resp.status_code} {resp.text}")
        return deleted

    def delete_stale_event_channels(self, active_channel_ids: set) -> int:
        endpoint = f"{self.base_url}/rest/v1/channels?category=eq.events&select=id"
        resp = requests.get(endpoint, headers=self.headers, timeout=20)
        if resp.status_code != 200:
            return 0
            
        existing_event_channels = [r["id"] for r in resp.json()]
        stale_channels = [cid for cid in existing_event_channels if cid not in active_channel_ids]
        
        if not stale_channels:
            return 0
            
        logger.info(f"Purging {len(stale_channels)} stale event channel streams from public.channels...")
        del_endpoint = f"{self.base_url}/rest/v1/channels"
        batch_size = 50
        deleted = 0
        for i in range(0, len(stale_channels), batch_size):
            chunk = stale_channels[i:i + batch_size]
            ids_str = ",".join(chunk)
            r = requests.delete(f"{del_endpoint}?id=in.({ids_str})", headers=self.headers, timeout=30)
            if r.status_code in (200, 204):
                deleted += len(chunk)
        return deleted


# ---------------------------------------------------------
# Stream Fetching Worker
# ---------------------------------------------------------
def fetch_streams_for_event(event_dict: dict) -> list:
    """Fetches and decrypts the stream list for an individual event."""
    links_rel = event_dict.get("links", "")
    if not links_rel:
        return []
        
    full_url = links_rel if links_rel.startswith("http") else f"{BASE_PRO_URL.rstrip('/')}/{links_rel.lstrip('/')}"
    try:
        req = urllib.request.Request(full_url, headers={"User-Agent": "Dalvik/2.1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            raw_text = resp.read().decode("ascii", errors="ignore").strip()
            
        decrypted_json = decrypt_px(raw_text)
        streams_data = json.loads(decrypted_json)
        
        results = []
        for s in streams_data:
            results.append({
                "name": s.get("name", "Stream"),
                "raw_link": s.get("link", ""),
                "drm_clearkey": s.get("api") or s.get("drm_clearkey"),
                "hasHeader": s.get("hasHeader", False),
                "hasVpnDetection": s.get("hasVpnDetection", False)
            })
        return results
    except Exception as e:
        logger.debug(f"Could not load streams for {event_dict.get('match')}: {e}")
        return []


# ---------------------------------------------------------
# Main Execution Pipeline
# ---------------------------------------------------------
def main():
    t_start = time.time()
    logger.info("==================================================")
    logger.info("Starting PlayZ TV Live Events Sync Pipeline...")
    logger.info("==================================================")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Fetch raw events feed
    logger.info(f"Downloading events catalog from {EVENTS_FEED_URL}...")
    try:
        req = urllib.request.Request(EVENTS_FEED_URL, headers={"User-Agent": "Dalvik/2.1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw_feed = resp.read().decode("ascii", errors="ignore").strip()
    except Exception as e:
        logger.error(f"Failed to fetch live events feed: {e}")
        sys.exit(1)

    # 2. Decrypt events catalog
    logger.info("Decrypting Tier-1 AES-128-CBC catalog payload...")
    try:
        decrypted_feed = decrypt_px(raw_feed)
        catalog_items = json.loads(decrypted_feed)
    except Exception as e:
        logger.error(f"Failed to decrypt events feed: {e}")
        sys.exit(1)

    logger.info(f"Found {len(catalog_items)} raw event records in feed.")

    # 3. Parse event items
    parsed_events = []
    for item in catalog_items:
        ev_data = item.get("event")
        if isinstance(ev_data, str):
            try:
                ev = json.loads(ev_data)
            except Exception:
                continue
        elif isinstance(ev_data, dict):
            ev = ev_data
        else:
            ev = item

        team_a = ev.get("teamAName", ev.get("teamA", "")).strip()
        team_b = ev.get("teamBName", ev.get("teamB", "")).strip()
        match_title = ev.get("match", f"{team_a} vs {team_b}").strip()
        category = ev.get("category", "Sports").strip()
        league = ev.get("eventName", ev.get("tournament", "Championship")).strip()
        
        start_dt = parse_event_datetime(ev.get("date"), ev.get("time"))
        end_dt = parse_event_datetime(ev.get("end_date", ev.get("date")), ev.get("end_time", ev.get("time")))
        
        if not start_dt:
            continue
        if not end_dt or end_dt <= start_dt:
            end_dt = start_dt + timedelta(hours=3)

        ev["_clean_match"] = match_title
        ev["_clean_team_a"] = team_a
        ev["_clean_team_b"] = team_b
        ev["_clean_category"] = category
        ev["_clean_league"] = league
        ev["_start_dt"] = start_dt
        ev["_end_dt"] = end_dt
        
        parsed_events.append(ev)

    logger.info(f"Parsed {len(parsed_events)} valid scheduled events.")

    # 4. Concurrently fetch streams
    logger.info(f"Resolving stream links across {MAX_WORKERS} concurrent worker threads...")
    streams_map = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_to_ev = {pool.submit(fetch_streams_for_event, ev): ev for ev in parsed_events}
        for future in as_completed(future_to_ev):
            ev = future_to_ev[future]
            try:
                streams = future.result()
            except Exception:
                streams = []
            streams_map[id(ev)] = streams

    total_stream_count = sum(len(s) for s in streams_map.values())
    logger.info(f"Extracted {total_stream_count} active live stream feeds.")

    # 5. Transform for Supabase schema and files
    now_utc = datetime.now(timezone.utc)
    purge_cutoff = now_utc - timedelta(hours=PURGE_HOURS)

    channels_to_upsert = []
    events_to_upsert = []
    active_channel_ids = set()
    active_event_ids = set()

    for ev in parsed_events:
        start_dt = ev["_start_dt"]
        end_dt = ev["_end_dt"]

        if end_dt < purge_cutoff:
            continue

        if now_utc < start_dt:
            status = "upcoming"
        elif start_dt <= now_utc <= end_dt:
            status = "live"
        else:
            status = "finished"

        team_a = ev["_clean_team_a"]
        team_b = ev["_clean_team_b"]
        match_title = ev["_clean_match"]
        category = ev["_clean_category"]
        league = ev["_clean_league"]
        
        start_ts = int(start_dt.timestamp())
        event_id = f"event-{slugify(category)}-{slugify(team_a)}-vs-{slugify(team_b)}-{start_ts}"
        active_event_ids.add(event_id)

        event_logo = ev.get("eventLogo") or ev.get("tournamentLogo")
        team_a_flag = ev.get("teamAFlag", "")
        team_b_flag = ev.get("teamBFlag", "")
        is_hot = bool(ev.get("isHot", False))

        streams = streams_map.get(id(ev), [])
        channel_ids_for_event = []

        for idx, s in enumerate(streams):
            clean_url, headers = parse_stream_url_and_headers(s["raw_link"])
            if not clean_url:
                continue

            channel_id = f"{event_id}-s{idx + 1}"
            channel_ids_for_event.append(channel_id)
            active_channel_ids.add(channel_id)

            drm_json = parse_drm_clearkey(s.get("drm_clearkey"))

            channels_to_upsert.append({
                "id": channel_id,
                "name": f"{match_title} ({s['name']})",
                "logo": event_logo or team_a_flag,
                "category": "events",
                "country": "",
                "language": "",
                "is_live": (status == "live"),
                "is_trending": is_hot,
                "quality": "HD",
                "stream_url": clean_url,
                "headers": headers,
                "drm": drm_json,
                "proxy": False,
                "sort_order": idx
            })

        events_to_upsert.append({
            "id": event_id,
            "sport": category,
            "league": league,
            "home_team": {"name": team_a, "flag": team_a_flag},
            "away_team": {"name": team_b, "flag": team_b_flag},
            "start_time": start_dt.isoformat(),
            "status": status,
            "channels": channel_ids_for_event,
            "banner": event_logo,
            "is_featured": is_hot,
            "hero_type": 1,
            "custom_title": match_title,
            "sort_order": 0
        })

    logger.info(f"Prepared {len(events_to_upsert)} events and {len(channels_to_upsert)} channel streams.")

    # 6. Save Local Output Files (events.json, channels.json, playlist.m3u)
    events_json_path = os.path.join(OUTPUT_DIR, "events.json")
    with open(events_json_path, "w", encoding="utf-8") as f:
        json.dump(events_to_upsert, f, indent=2, ensure_ascii=False)
    logger.info(f"Exported {len(events_to_upsert)} events to {events_json_path}")

    channels_json_path = os.path.join(OUTPUT_DIR, "channels.json")
    with open(channels_json_path, "w", encoding="utf-8") as f:
        json.dump(channels_to_upsert, f, indent=2, ensure_ascii=False)
    logger.info(f"Exported {len(channels_to_upsert)} channels to {channels_json_path}")

    m3u_content = generate_m3u_playlist(events_to_upsert, channels_to_upsert)
    m3u_path = os.path.join(OUTPUT_DIR, "playlist.m3u")
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write(m3u_content)
    logger.info(f"Exported IPTV playlist to {m3u_path}")

    hash_path = os.path.join(OUTPUT_DIR, "last_hash_sports_events.txt")
    with open(hash_path, "w", encoding="utf-8") as f:
        f.write(hashlib.sha256(raw_feed.encode("utf-8")).hexdigest())

    # 7. Synchronize with Supabase
    if SUPABASE_URL:
        db = SupabaseSyncClient(
            base_url=SUPABASE_URL,
            service_key=SUPABASE_SERVICE_ROLE_KEY,
            anon_key=SUPABASE_ANON_KEY,
            admin_token=SUPABASE_ADMIN_TOKEN
        )

        if channels_to_upsert:
            logger.info(f"Upserting {len(channels_to_upsert)} stream records into 'public.channels'...")
            up_channels = db.upsert_rows("channels", channels_to_upsert)
            logger.info(f"Successfully upserted {up_channels} channels.")

        if events_to_upsert:
            logger.info(f"Upserting {len(events_to_upsert)} records into 'public.events'...")
            up_events = db.upsert_rows("events", events_to_upsert)
            logger.info(f"Successfully upserted {up_events} events.")

        existing_events = db.get_existing_event_ids()
        stale_event_ids = []
        for ev_id, ev_info in existing_events.items():
            if ev_id not in active_event_ids:
                start_str = ev_info.get("start_time")
                if start_str:
                    try:
                        s_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                        if s_dt < purge_cutoff:
                            stale_event_ids.append(ev_id)
                    except Exception:
                        stale_event_ids.append(ev_id)

        if stale_event_ids:
            logger.info(f"Deleting {len(stale_event_ids)} finished/stale events from 'public.events'...")
            del_cnt = db.delete_events(stale_event_ids)
            logger.info(f"Deleted {del_cnt} expired events.")

        purged_ch = db.delete_stale_event_channels(active_channel_ids)
        if purged_ch:
            logger.info(f"Purged {purged_ch} orphaned event channels.")

    duration = time.time() - t_start
    logger.info("==================================================")
    logger.info(f"Sync pipeline completed successfully in {duration:.2f} seconds!")
    logger.info(f"Events: {len(events_to_upsert)} synced | Channels: {len(channels_to_upsert)} synced")
    logger.info("==================================================")


if __name__ == "__main__":
    main()
