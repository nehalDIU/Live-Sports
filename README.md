# Live Sports Auto-Sync & Database Transformation

Automated pipeline for converting live and upcoming sports stream data from [axsports/live_sports.json](https://raw.githubusercontent.com/srhady/axsports/main/live_sports.json) into:
1. **`events.json`**: Formatted strictly for the PostgreSQL / Supabase `public.events` table schema.
2. **`channels.json`**: Individual server stream records with required HTTP headers (`Referer`, `User-Agent`).
3. **`playlist.m3u`**: Standard IPTV playlist with VLC/Kodi directives (`#EXTVLCOPT`, `#KODIPROP`).
4. **`main.py`**: Automated updater script with payload hash caching and optional direct Supabase synchronization.

---

## Database Table Schema (`public.events`)

```sql
create table public.events (
  id text not null,
  sport text not null,
  league text not null,
  home_team jsonb not null default '{}'::jsonb,
  away_team jsonb not null default '{}'::jsonb,
  start_time timestamp with time zone not null,
  status text not null default 'upcoming'::text,
  channels text[] not null default '{}'::text[],
  banner text null,
  is_featured boolean not null default false,
  hero_type integer null default 1,
  custom_title text null,
  sort_order integer null default 0,
  constraint events_pkey primary key (id)
) TABLESPACE pg_default;

create index if not exists idx_events_is_featured on public.events using btree (is_featured) TABLESPACE pg_default;
create index if not exists idx_events_status_start_time on public.events using btree (status, start_time) TABLESPACE pg_default;
create index if not exists events_start_idx on public.events using btree (start_time) TABLESPACE pg_default;
create index if not exists events_status_idx on public.events using btree (status) TABLESPACE pg_default;
create index if not exists idx_events_status on public.events using btree (status) TABLESPACE pg_default;
create index if not exists idx_events_start_time on public.events using btree (start_time) TABLESPACE pg_default;

create trigger tr_increment_events_version
after insert or delete or update or truncate on events for each statement
execute function increment_events_version ();
```

---

## Data Transformation & Mapping

| Field | Type | Description / Mapping Logic |
| :--- | :--- | :--- |
| `id` | `text` | Unique ID prefixed with source namespace: `axsports-{id}` (e.g. `axsports-43314`) |
| `sport` | `text` | Inferred sport category (Football, Baseball, Basketball, Cricket, Motorsport, Cycling, Tennis, Rugby, Darts, Table Tennis, Boxing, etc.) |
| `league` | `text` | Cleaned league title (e.g. `Premier League`, `La Liga`, `Serie A`, `MLB`) |
| `home_team` | `jsonb` | JSON object containing `{"name": "...", "logo": "...", "flag": "..."}` |
| `away_team` | `jsonb` | JSON object containing `{"name": "...", "logo": "...", "flag": "..."}` |
| `start_time` | `timestamptz` | ISO 8601 UTC timestamp converted from epoch `start_at` / `timestamp` |
| `status` | `text` | Normalized to `'live'`, `'upcoming'`, or `'ended'` |
| `channels` | `text[]` | Array of channel IDs generated for stream servers (`axsports-43314-server1`, etc.) |
| `banner` | `text` | Bing match poster (`https://raw.githubusercontent.com/srhady/axsports/main/bing_posters/{name}.jpg`) with logo fallback |
| `is_featured` | `boolean` | `true` if `ishot` or live in top tier leagues |
| `hero_type` | `integer` | `2` for featured live matches, `1` standard |
| `custom_title` | `text` | Match title (e.g. `"Brentford vs Chelsea"`) |
| `sort_order` | `integer` | Prioritizes live matches, then hot upcoming, then chronological upcoming |

---

## Setup & Execution

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment (Optional for DB sync)
Copy `.env.example` to `.env` and fill in credentials if syncing directly to Supabase:
```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
```

### 3. Run Pipeline
```bash
# Generate events.json, channels.json, and playlist.m3u locally
python main.py

# Force re-generation (bypass hash check)
python main.py --force

# Sync directly to Supabase/PostgreSQL
python main.py --sync-db
```

---

## GitHub Actions Automation (Every 20 Minutes)

A continuous auto-sync workflow is provided in [`.github/workflows/sports_sync.yml`](.github/workflows/sports_sync.yml).

### Setting up Repository Secrets:
Go to your GitHub repository **Settings** -> **Secrets and variables** -> **Actions** -> **New repository secret**:
1. `SUPABASE_URL`: Your Supabase Project URL (`https://hqmhuvsjlykrdusfkmeg.supabase.co`)
2. `SUPABASE_SERVICE_ROLE_KEY`: Your Supabase Key
3. `ADMIN_SECRET_TOKEN`: `GoLiveAdminSecret2026`

The workflow runs on a `cron: '*/20 * * * *'` schedule and can also be manually triggered via `workflow_dispatch`. It fetches updates, syncs the DB, and commits fresh `events.json`, `channels.json`, and `playlist.m3u` files to your repository.

---

## Direct Database Import

### Method 1: Using Python with Supabase
```bash
python main.py --sync-db
```

### Method 2: Importing via PostgreSQL JSON functions
```sql
insert into public.events (
  id, sport, league, home_team, away_team, start_time, 
  status, channels, banner, is_featured, hero_type, custom_title, sort_order
)
select 
  j->>'id',
  j->>'sport',
  j->>'league',
  (j->'home_team')::jsonb,
  (j->'away_team')::jsonb,
  (j->>'start_time')::timestamptz,
  j->>'status',
  array(select jsonb_array_elements_text(j->'channels')),
  j->>'banner',
  (j->>'is_featured')::boolean,
  (j->>'hero_type')::integer,
  j->>'custom_title',
  (j->>'sort_order')::integer
from jsonb_array_elements('[ ... JSON CONTENT OF events.json ... ]'::jsonb) as j
on conflict (id) do update set
  sport = excluded.sport,
  league = excluded.league,
  home_team = excluded.home_team,
  away_team = excluded.away_team,
  start_time = excluded.start_time,
  status = excluded.status,
  channels = excluded.channels,
  banner = excluded.banner,
  is_featured = excluded.is_featured,
  hero_type = excluded.hero_type,
  custom_title = excluded.custom_title,
  sort_order = excluded.sort_order;
```
