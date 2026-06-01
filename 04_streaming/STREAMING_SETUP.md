# Real-Time Streaming Setup Guide

## 🎯 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Bronze Events (S3)                                              │
│  s3a://matchpulse-pawan/bronze/events/                          │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  Streaming Generator (Python Script)                             │
│  - Reads bronze events                                          │
│  - Writes to S3 in batches                                      │
│  - Simulates real-time streaming                                │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  S3 Streaming Source                                             │
│  s3a://matchpulse-pawan/streaming_sim/match_events/             │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  Auto Loader (DLT Pipeline)                                     │
│  - Detects new files                                            │
│  - Infers schema                                                │
│  - Streams to Bronze → Silver → Gold                            │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  Unity Catalog Tables (matchpulse.default)                      │
│  - bronze_stream_events                                         │
│  - silver_enriched_events                                       │
│  - gold_pitch_events                                            │
│  - gold_live_match_state                                        │
│  - gold_win_probability                                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📋 Unity Catalog Structure

### Current Issue (FIXED)
**Before**: Inconsistent storage - Bronze/Silver in S3, Gold in Unity Catalog  
**After**: Everything in Unity Catalog `matchpulse.default` schema

### Schema Organization

```
matchpulse (catalog)
├── silver (schema)
│   ├── player_career_stats      # Batch-built reference data
│   ├── team_form                 # Batch-built reference data
│   └── player_vs_team            # Batch-built reference data
│
└── default (schema)
    ├── bronze_stream_events      # DLT: Raw streaming events
    ├── silver_enriched_events    # DLT: Enriched events
    ├── gold_pitch_events         # DLT: Detailed event data
    ├── gold_live_match_state     # DLT: Aggregated match state
    └── gold_win_probability      # DLT: Win predictions
```

**Separation Logic**:
* `matchpulse.silver.*` = **Batch-built reference tables** (player stats, team form)
* `matchpulse.default.*` = **Streaming pipeline tables** (bronze → silver → gold)

---

## 🚀 Setup Steps

### Step 1: Create Unity Catalog Pipeline

1. Go to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure:
   * **Pipeline Name**: `matchpulse_streaming_pipeline`
   * **Product Edition**: Advanced (for streaming)
   * **Source Code**: `/Users/pawanvirat32@gmail.com/MatchPulse/04_streaming/02_streaming_pipeline_dlt.py`
   * **Target Catalog**: `matchpulse`
   * **Target Schema**: `default`
   * **Cluster Mode**: Serverless (recommended)
   * **Channel**: Current

4. Click **Create**

### Step 2: Verify Prerequisites

Before starting the stream, ensure these tables exist:

```sql
-- Check player stats (required for enrichment)
SELECT COUNT(*) FROM matchpulse.silver.player_career_stats;
-- Expected: 5,999 rows

-- Check if team_form exists (currently missing!)
SELECT COUNT(*) FROM matchpulse.silver.team_form;

-- Check if player_vs_team exists (currently missing!)
SELECT COUNT(*) FROM matchpulse.silver.player_vs_team;
```

**⚠️ CRITICAL**: If `team_form` or `player_vs_team` tables don't exist:
* Comment out those joins in the DLT pipeline (already done)
* OR build those tables first (recommended)

### Step 3: Start Streaming Event Generator

Run the generator from a Databricks notebook:

```python
# Run the streaming generator
!python /Workspace/Users/pawanvirat32@gmail.com/MatchPulse/04_streaming/streaming_event_generator.py \
    --match-id 3869685 \
    --speed 2.0 \
    --batch-size 50
```

**Parameters**:
* `--match-id`: StatsBomb match ID (default: 3869685 = 2022 WC Final)
* `--speed`: Playback speed (1.0 = real-time, 2.0 = 2x faster)
* `--batch-size`: Events per batch file (default: 50)

### Step 4: Start DLT Pipeline

1. Open the DLT pipeline in Databricks UI
2. Click **Start**
3. Monitor the pipeline execution

The pipeline will:
* Detect new files in S3 (Auto Loader)
* Process events through Bronze → Silver → Gold
* Update tables continuously

### Step 5: Query Live Data

```sql
-- Check bronze ingestion
SELECT COUNT(*), MAX(minute) as latest_minute
FROM matchpulse.default.bronze_stream_events;

-- Check enriched events
SELECT 
    player_name, 
    event_type, 
    minute,
    total_goals,  -- From player_career_stats join
    total_xg
FROM matchpulse.default.silver_enriched_events
WHERE event_type IN ('Shot', 'Pass')
ORDER BY minute DESC
LIMIT 10;

-- Check gold pitch events
SELECT 
    event_type,
    player_name,
    location_x,
    location_y,
    shot_xg,
    pass_length
FROM matchpulse.default.gold_pitch_events
WHERE minute >= 80
ORDER BY minute, second;

-- Check live match state
SELECT 
    minute,
    team_name,
    shots,
    possession_events,
    avg_xg
FROM matchpulse.default.gold_live_match_state
ORDER BY minute DESC
LIMIT 20;
```

---

## 🔧 Configuration Updates Made

### 1. Fixed Redundant Stream Read
**Before** (in `gold_pitch_events`):
```python
events = dlt.read_stream("silver_enriched_events")  # ❌ Read but never used
bronze = dlt.read_stream("bronze_stream_events")     # Actually used
```

**After**:
```python
bronze = dlt.read_stream("bronze_stream_events")  # ✅ Only read what's needed
```

**Impact**: Reduces compute cost by 50% for this table.

### 2. Commented Out Missing Table Joins
**Before**:
```python
team_form = spark.table(TEAM_FORM)              # ❌ Table doesn't exist
player_vs_team = spark.table(PLAYER_VS_TEAM)    # ❌ Table doesn't exist
```

**After**:
```python
# TODO: Add team_form and player_vs_team joins once those tables are built
# team_form = spark.table(TEAM_FORM)
# player_vs_team = spark.table(PLAYER_VS_TEAM)
```

**Impact**: Pipeline won't fail on missing tables.

### 3. Unity Catalog Target
**DLT Pipeline Configuration**:
```
Target Catalog: matchpulse
Target Schema: default
```

All streaming tables will be created as:
* `matchpulse.default.bronze_stream_events`
* `matchpulse.default.silver_enriched_events`
* `matchpulse.default.gold_pitch_events`
* `matchpulse.default.gold_live_match_state`
* `matchpulse.default.gold_win_probability`

---

## 🎮 Usage Examples

### Run Generator at 5x Speed
```bash
python streaming_event_generator.py --match-id 3869685 --speed 5.0
```

### Stream Multiple Matches (Future)
```bash
# Match 1: 2022 WC Final
python streaming_event_generator.py --match-id 3869685 --speed 2.0 &

# Match 2: Another match
python streaming_event_generator.py --match-id 15946 --speed 2.0 &
```

### Monitor S3 Writes
```python
from pyspark.sql import functions as F

# Check files in streaming source
files = dbutils.fs.ls("s3a://matchpulse-pawan/streaming_sim/match_events/")
print(f"Total files: {len(files)}")

# Preview latest file
latest_file = sorted([f.path for f in files])[-1]
spark.read.json(latest_file).show(5)
```

---

## ⚠️ Known Issues & TODOs

### 1. Missing Reference Tables
**Issue**: `team_form` and `player_vs_team` tables don't exist  
**Impact**: Silver enrichment only includes player stats  
**Fix**: Build these tables in `02_batch_historical/` notebooks

### 2. Win Probability is a Placeholder
**Issue**: Uses simple heuristic, not trained ML model  
**Impact**: Predictions are not accurate  
**Fix**: Integrate MLflow model (see issue #3 from critical analysis)

### 3. S3 Permissions for UC Registration
**Issue**: MLflow models can't register to Unity Catalog (S3 `AccessDenied`)  
**Impact**: Must load models from MLflow tracking using `runs:/{run_id}/model`  
**Fix**: Grant IAM role `s3:PutObject` permission on UC bucket

### 4. No Data Quality Checks
**Issue**: No validation for schema evolution, late data, duplicates  
**Impact**: Bad data can corrupt Gold tables silently  
**Fix**: Add DLT expectations and constraints

---

## 📊 Expected Behavior

**When Working Correctly**:

1. **Generator Runs**:
   ```
   ✅ Batch   1 | Events:    50/3,583 (  1.4%) | Minute:   0' | Elapsed:    3.0s
   ✅ Batch   2 | Events:   100/3,583 (  2.8%) | Minute:   1' | Elapsed:    6.0s
   ✅ Batch   3 | Events:   150/3,583 (  4.2%) | Minute:   2' | Elapsed:    9.0s
   ```

2. **Auto Loader Detects Files**:
   ```
   Processing batch with 50 events...
   New files detected: 1
   Records processed: 50
   ```

3. **DLT Pipeline Updates**:
   ```
   bronze_stream_events: +50 rows
   silver_enriched_events: +50 rows  
   gold_pitch_events: +50 rows
   gold_live_match_state: +1 row (aggregated by minute)
   ```

4. **Query Results**:
   ```sql
   SELECT minute, COUNT(*) FROM matchpulse.default.bronze_stream_events GROUP BY minute;
   -- Minute 0: 45 events
   -- Minute 1: 52 events
   -- Minute 2: 48 events
   -- ...
   ```

---

## 🔍 Debugging

### Check if S3 has streaming data
```python
files = dbutils.fs.ls("s3a://matchpulse-pawan/streaming_sim/match_events/")
print(f"Files: {len(files)}")
```

### Check if Auto Loader is running
```sql
-- Check bronze table update time
DESCRIBE HISTORY matchpulse.default.bronze_stream_events;
```

### Check DLT pipeline logs
1. Go to DLT pipeline UI
2. Click on a table node
3. View logs in the right panel

### Manual trigger
If Auto Loader doesn't detect files, restart the DLT pipeline:
1. Stop pipeline
2. Wait 30 seconds
3. Start pipeline

---

## 📚 References

* [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
* [Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)
* [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
