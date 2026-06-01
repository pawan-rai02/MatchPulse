# 01_ingestion - Bronze Layer Data Ingestion

## Overview
This folder contains notebooks that ingest raw StatsBomb soccer data from S3 into Bronze layer tables/files. The bronze layer preserves the raw data structure with minimal transformation, adding only metadata fields for tracking and processing.

---

## 📊 Data Summary

### Total Data Volume
- **8,365,579** events
- **87,199** lineup records
- **2,334** unique matches
- **7,197** unique players
- **187** teams
- **131** countries represented

---

## 📁 Notebooks & Data Details

### 1️⃣ `01_ingest_matches_bronze.ipynb`

**Purpose:** Ingests match-level metadata from StatsBomb JSON files

**Source:** `s3a://matchpulse-pawan/raw/statsbomb/matches/`  
**Destination:** `s3a://matchpulse-pawan/bronze/matches/`  
**Format:** Delta Lake

#### Data Statistics
- **Total Matches:** 2,205
- **Competitions:** 7
- **Seasons:** 27
- **Match Date Range:** 2017-08-20 to 2018-05-13

#### Schema
| Field | Type | Description |
|-------|------|-------------|
| `match_id` | BIGINT | Unique match identifier |
| `match_date` | STRING | Date of match |
| `kick_off` | STRING | Match kick-off time |
| `home_team` | STRUCT | Home team details (id, name, gender, managers, country) |
| `away_team` | STRUCT | Away team details (id, name, gender, managers, country) |
| `home_score` | LONG | Home team final score |
| `away_score` | LONG | Away team final score |
| `competition` | STRUCT | Competition details (id, name, country) |
| `competition_stage` | STRUCT | Stage details (id, name) |
| `competition_id` | INT | Competition identifier (extracted from file path) |
| `season_id` | INT | Season identifier (extracted from file path) |
| `match_status` | STRING | Match status |
| `match_week` | LONG | Match week number |
| `stadium` | STRUCT | Stadium details (id, name, country) |
| `referee` | STRUCT | Referee details (id, name, country) |
| `last_updated` | STRING | Last data update timestamp |

#### Sample Teams
- Barcelona, Real Madrid, Valencia, Athletic Club, Eibar, Las Palmas, RC Deportivo La Coruña, Real Betis, Villarreal

---

### 2️⃣ `02_ingest_events_bronze.ipynb`

**Purpose:** Ingests detailed event-level data (passes, shots, tackles, etc.) for each match

**Source:** `s3a://matchpulse-pawan/raw/statsbomb/events/`  
**Destination:** `s3a://matchpulse-pawan/bronze/events/`  
**Format:** Parquet (Snappy compression)

#### Data Statistics
- **Total Events:** 8,365,579
- **Unique Matches:** 2,334
- **Event Types:** 35 distinct types
- **Ingestion Date:** 2026-05-27

#### Top 10 Event Types by Volume
| Event Type | Count |
|------------|-------|
| Pass | 2,335,079 |
| Ball Receipt* | 2,189,531 |
| Carry | 1,815,598 |
| Pressure | 750,872 |
| Ball Recovery | 242,642 |
| Duel | 177,316 |
| Clearance | 106,031 |
| Block | 87,406 |
| Dribble | 85,605 |
| Foul Committed | 71,312 |

#### Schema
| Field | Type | Description |
|-------|------|-------------|
| `match_id` | BIGINT | Match identifier (extracted from filename) |
| `event_id` | STRING | Unique event UUID |
| `index` | INT | Event sequence number within match |
| `period` | INT | Match period (1=1st half, 2=2nd half, etc.) |
| `minute` | INT | Minute of the match |
| `second` | INT | Second within the minute |
| `timestamp` | STRING | Original event timestamp |
| `event_type_id` | INT | Event type identifier |
| `event_type_name` | STRING | Event type name (Pass, Shot, Tackle, etc.) |
| `team_id` | INT | Team identifier |
| `team_name` | STRING | Team name |
| `player_id` | INT | Player identifier (nullable for team events) |
| `player_name` | STRING | Player name (nullable) |
| `location_x` | DOUBLE | X coordinate on pitch (nullable) |
| `location_y` | DOUBLE | Y coordinate on pitch (nullable) |
| `raw_json` | STRING | Full original event JSON for schema flexibility |
| `ingestion_ts` | TIMESTAMP | Ingestion timestamp |

#### Event Coverage
- **Player Events:** 8,331,704 (events with player_id)
- **Average Events per Match:** ~3,583 events

---

### 3️⃣ `03_ingest_lineups_bronze.ipynb`

**Purpose:** Ingests player lineup data for each match (starting XI and substitutes)

**Source:** `s3a://matchpulse-pawan/raw/statsbomb/lineups/`  
**Destination:** `s3a://matchpulse-pawan/bronze/lineups/`  
**Format:** Parquet (Snappy compression)

#### Data Statistics
- **Total Lineup Records:** 87,199
- **Unique Matches:** 2,334
- **Unique Teams:** 187
- **Unique Players:** 7,197
- **Unique Countries:** 131
- **Players with Cards:** 10,379

#### Top 10 Teams by Match Coverage
| Team | Matches |
|------|--------|
| Barcelona | 528 |
| Paris Saint-Germain | 95 |
| Arsenal | 76 |
| Real Madrid | 70 |
| Atlético Madrid | 68 |
| Bayer Leverkusen | 68 |
| Sevilla | 66 |
| Valencia | 65 |
| Espanyol | 63 |
| Athletic Club | 63 |

#### Top 10 Countries by Player Count
| Country | Player Count |
|---------|-------------|
| Spain | 18,445 |
| France | 10,447 |
| Italy | 8,605 |
| Brazil | 5,009 |
| England | 4,876 |
| Argentina | 4,226 |
| Germany | 2,169 |
| Portugal | 1,913 |
| Netherlands | 1,554 |
| Senegal | 1,461 |

#### Schema
| Field | Type | Description |
|-------|------|-------------|
| `match_id` | BIGINT | Match identifier (extracted from filename) |
| `team_id` | INT | Team identifier |
| `team_name` | STRING | Team name |
| `player_id` | INT | Player identifier |
| `player_name` | STRING | Full player name |
| `player_nickname` | STRING | Player nickname (nullable) |
| `jersey_number` | INT | Jersey number |
| `country_id` | INT | Player's country identifier |
| `country_name` | STRING | Player's country name |
| `cards_json` | STRING | JSON array of cards received (card_type, period, reason, time) |
| `positions_json` | STRING | JSON array of position changes during match |
| `raw_json` | STRING | Full original player JSON for schema flexibility |
| `ingestion_ts` | TIMESTAMP | Ingestion timestamp |

---

## 🔄 Data Flow

```
S3 Raw Data (StatsBomb JSON)
    ↓
01_ingest_matches_bronze.ipynb → Delta Lake (Matches metadata)
02_ingest_events_bronze.ipynb → Parquet (Event-level data)
03_ingest_lineups_bronze.ipynb → Parquet (Player lineups)
    ↓
Bronze Layer (s3a://matchpulse-pawan/bronze/)
```

---

## 🏃 Execution Order

1. **First:** `01_ingest_matches_bronze.ipynb` - Loads match metadata
2. **Second:** `02_ingest_events_bronze.ipynb` - Loads event data (requires match_ids)
3. **Third:** `03_ingest_lineups_bronze.ipynb` - Loads lineup data (requires match_ids)

**Note:** All notebooks can be run independently, but running them in order ensures referential integrity.

---

## 📦 Storage Locations

- **Raw Data:** `s3a://matchpulse-pawan/raw/statsbomb/`
  - `matches/{competition_id}/{season_id}.json`
  - `events/{match_id}.json`
  - `lineups/{match_id}.json`

- **Bronze Layer:** `s3a://matchpulse-pawan/bronze/`
  - `matches/` (Delta format)
  - `events/` (Parquet format)
  - `lineups/` (Parquet format)

---

## ⚙️ Configuration

All paths are configured in `/Workspace/Users/pawanvirat32@gmail.com/MatchPulse/config/paths.py`

```python
MATCHES_RAW = "s3a://matchpulse-pawan/raw/statsbomb/matches/"
EVENTS_RAW = "s3a://matchpulse-pawan/raw/statsbomb/events/"
LINEUPS_RAW = "s3a://matchpulse-pawan/raw/statsbomb/lineups/"

MATCHES_BRONZE = "s3a://matchpulse-pawan/bronze/matches/"
EVENTS_BRONZE = "s3a://matchpulse-pawan/bronze/events/"
LINEUPS_BRONZE = "s3a://matchpulse-pawan/bronze/lineups/"
```

---

## 🎯 Key Insights

- **Data Richness:** ~3,583 events per match on average
- **Barcelona Dominance:** 528 matches, most covered team
- **Global Coverage:** 131 countries represented in player base
- **Spanish Focus:** 18,445 Spanish player records (highest)
- **Disciplinary Data:** 10,379 player records include card information
- **Position Tracking:** Position changes tracked throughout matches

---

## 📝 Notes

- All timestamps use ISO 8601 format
- Coordinates use StatsBomb's 120x80 pitch coordinate system
- `raw_json` fields preserve original data for future schema evolution
- `ingestion_ts` tracks when data was loaded into bronze layer
- File paths extracted using `_metadata.file_path` for Unity Catalog compatibility

---

**Last Updated:** 2026-05-29  
**Data Source:** StatsBomb Open Data  
**Maintained by:** MatchPulse Team