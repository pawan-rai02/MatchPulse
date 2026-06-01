# 04_Streaming — Real-Time Match Event Processing

## 📋 Overview

This module implements a **real-time streaming pipeline** that simulates live soccer match events and processes them through a multi-layer data pipeline using **Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables) and **Auto Loader**.

### 🎯 Business Goal

Enable real-time match analytics by:
- Ingesting events as they occur during a live match
- Enriching events with historical player/team statistics
- Computing live match state (score, possession, shots)
- Predicting win probability in real-time
- **NEW:** Professional live dashboard with notebook-optimized display

### 🏗️ Architecture

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────────┐
│   Event         │      │  Lakeflow SDP    │      │   Gold Tables       │
│   Simulator     │ ───► │  Auto Loader     │ ───► │   (Live State +     │
│  (Notebook)     │      │  + Enrichment    │      │   Win Probability)  │
│   + Dashboard   │      └──────────────────┘      └─────────────────────┘
└─────────────────┘              │                           │
     │                           │ stream-static            │ powers
     │ writes JSON               │ joins with               │ dashboards
     ↓                           ↓ Silver tables            ↓
s3://...streaming_sim/    Bronze → Silver → Gold     Real-time insights
```

### 📊 Pipeline Layers

| Layer  | Table Name                  | Purpose                                    | Type        |
|--------|-----------------------------|--------------------------------------------|-------------|
| Bronze | `bronze_stream_events`      | Raw events from S3 via Auto Loader        | Streaming   |
| Silver | `silver_enriched_events`    | Events enriched with player/team stats    | Streaming   |
| Gold   | `gold_live_match_state`     | Aggregated match state by minute          | Streaming   |
| Gold   | `gold_win_probability`      | Win probability predictions               | Streaming   |

---

## 📁 Files in This Directory

### 1. `01_event_replay_simulator.py` (Notebook) ⭐ NEW DASHBOARD

**Purpose:** Simulates a live match by replaying historical StatsBomb events into S3 with a **professional live dashboard** optimized for Databricks notebooks.

---

### 🎨 Live Dashboard Features

#### ⚽ **1. Live Scoreboard**
Persistent header showing:
- Real-time score (auto-updates on goals)
- Team names
- Current match minute
- Elapsed replay time
- Estimated remaining time

```
================================================================================
⚽ MATCHPULSE LIVE REPLAY
================================================================================

Barcelona 2 - 1 Real Madrid
Match Minute: 67' | Elapsed: 6m 42s | Remaining: 2m 18s
```

#### 🎯 **2. Event Detection & Classification**
Automatically detects and displays 11+ event types:

| Event Type      | Icon | Detection Logic                          |
|-----------------|------|------------------------------------------|
| Goal           | ⚽   | Shot outcome = "Goal"                    |
| Own Goal       | 😬   | Shot type contains "Own"                 |
| Yellow Card    | 🟨   | Foul with card type "Yellow"             |
| Red Card       | 🟥   | Foul with card type "Red"                |
| Second Yellow  | 🟨🟨 | Foul with card type "Second Yellow"      |
| Substitution   | 🔄   | Substitution event                       |
| Shot           | 🎯   | Shot event (non-goal)                    |
| Corner         | 🚩   | Pass type = "Corner"                     |
| VAR Review     | 📺   | VAR event                                |
| Penalty        | 🎯   | Penalty events                           |
| Big Chance     | 🔥   | High xG shots (future enhancement)       |

**Example Event Display:**
```
⚽ GOAL! 54'
Phil Foden (Manchester City)

🟨 YELLOW CARD 62'
Declan Rice (Arsenal)

🔄 SUBSTITUTION 70'
Haaland OFF → Alvarez ON
```

#### 📺 **3. Live Commentary Feed**
Scrolling feed showing the last 5 significant events:

```
Recent Events
  [67'] 🔄 Sub: Alvarez ↔ Haaland
  [65'] 🚩 Corner - Arsenal
  [62'] 🟨 Yellow Card - Rice
  [58'] 🎯 Shot - Haaland
  [54'] ⚽ GOAL! Foden
```

**Implementation:** Uses `collections.deque(maxlen=15)` for efficient rolling buffer.

#### ⏱️ **4. Visual Match Timeline**
Horizontal timeline from 0' to 90' with event markers:

```
0'                                              90'
|──⚽──🟨──⚽──🔄──🟥──⚽──────────────────────────|
                        ▲ 67'
```

**Features:**
- 90-character width (1 char = 1 minute)
- Event icons placed at occurrence minute
- Current minute indicator (▲)
- Updates dynamically throughout replay

#### 📊 **5. Live Match Statistics**
Real-time aggregated stats in compact 2-column table:

```
Live Match Stats
┌──────────────────────────────────┬──────────────────────────────────┐
│ Possession  61% - 39%            │ Shots        12 - 7              │
│ xG         1.82 - 0.91           │ Corners       8 - 4              │
│ Cards        2 - 1               │ Avg Batch  2.03s                 │
└──────────────────────────────────┴──────────────────────────────────┘
```

**Derivation Methods:**
- **Possession:** Calculated from event count per team (# home events / total events)
- **Shots:** Count of Shot events per team
- **xG:** Sum of `shot.statsbomb_xg` per team
- **Corners:** Count of Pass events with type = "Corner"
- **Cards:** Count of Foul events with card data

#### 📈 **6. Replay Progress**
Visual progress bar with metrics:

```
Replay Progress
██████████████████████████████░░░░░░░░░░░░░░░░░░░░ 82.4%
Batch 25/30 | Events: 1,236/1,500 | Events/sec: 7.8
```

#### 🏁 **7. End-of-Match Summary**
Comprehensive match report displayed on completion:

```
================================================================================
🏁 MATCH COMPLETE
================================================================================

Manchester City 3 - 1 Arsenal

Goals
⚽ Foden 54'
⚽ Haaland 73'
⚽ Alvarez 88'

Cards
🟨 Rice
🟨 Rodri

Final Statistics
Possession : 61% - 39%
Shots      : 17 - 9
xG         : 2.41 - 1.12

Replay Time : 9m 12s
Events      : 1,500
================================================================================
```

---

### 🏗️ Dashboard Architecture

#### Component Breakdown

**1. EventDetector** (`ParsedEvent` class)
- Parses StatsBomb JSON structure into typed events
- Classifies events into 11+ categories
- Extracts player names, team names, minute data
- Returns structured `ParsedEvent` objects

**2. Manager Classes**

| Manager              | Responsibility                              |
|----------------------|---------------------------------------------|
| `ScoreboardManager`  | Track score, minute, time calculations      |
| `TimelineManager`    | Build 0'-90' visual timeline with markers   |
| `CommentaryFeed`     | Maintain scrolling deque of recent events   |
| `MatchStatsTracker`  | Aggregate possession, shots, xG, cards      |

**3. Display Implementation**
- Uses `IPython.display.clear_output()` for in-place updates
- ANSI color codes for professional styling
- ASCII box drawing characters for tables
- Refreshes display after every batch

**4. Main Replay Loop**
- Integrates all managers
- Processes each batch atomically
- Updates dashboard after every batch
- Shows final summary on completion

#### Design Principles

✅ **Separation of Concerns:** Each manager handles one domain  
✅ **Event-Driven:** Batch processing triggers atomic dashboard updates  
✅ **No Shared State:** Clean interfaces between components  
✅ **Type Safety:** Uses `@dataclass` and type hints throughout  
✅ **Error Handling:** Graceful handling of malformed events  
✅ **Notebook-Optimized:** Uses `clear_output()` instead of terminal widgets  
✅ **Portfolio-Ready:** Clean OOP, docstrings, professional styling  

#### Implementation Choice

**Why `clear_output()` + ANSI colors?**
- **vs Rich.Live():** Rich terminal widgets don't render properly in Databricks notebooks
- **vs basic print:** ANSI colors provide professional styling without dependencies
- **vs static output:** `clear_output(wait=True)` provides live updates without scroll spam
- **Result:** Perfect balance of visual appeal and notebook compatibility

---

### ⚙️ Configuration Options

```python
# Match Selection
MATCH_ID = 15946              # StatsBomb match ID to replay

# Demo Mode
DEMO_MODE = True              # True = fast demo, False = full match
DEMO_EVENT_LIMIT = 1500       # Events to process in demo mode

# Replay Speed
EVENTS_PER_BATCH = 50         # Events per JSON file
BATCH_INTERVAL_SEC = 0.5      # Seconds between batches
```

### 📊 Timing Modes

| Mode       | Events | Batches | Duration | Use Case             |
|------------|--------|---------|----------|----------------------|
| Demo       | 500    | 10      | ~5 sec   | Quick test           |
| Short      | 1,500  | 30      | ~15 sec  | Testing full flow    |
| Full Match | 3,762  | 76      | ~38 sec  | Production demo      |

---

### 🎬 Expected Output (LIVE DASHBOARD)

When you run the notebook, you'll see:

**1. Initialization:**
```
🔄 Initializing Live Dashboard...

✅ Dashboard initialized
   Barcelona vs Real Madrid
   1,500 events in 30 batches
   Estimated duration: ~0.2 minutes

🔴 Starting live replay...
```

**2. Live Dashboard (updates in place):**
```
================================================================================
⚽ MATCHPULSE LIVE REPLAY
================================================================================

Barcelona 2 - 1 Real Madrid
Match Minute: 67' | Elapsed: 6m 42s | Remaining: 2m 18s

Replay Progress
██████████████████████████████░░░░░░░░░░░░░░░░░░░░ 82.4%
Batch 25/30 | Events: 1,236/1,500 | Events/sec: 7.8

Match Timeline
0'                                              90'
|──⚽──🟨──⚽──🔄──🟥──⚽──────────────────────────|
                        ▲ 67'

Live Match Stats
┌──────────────────────────────────┬──────────────────────────────────┐
│ Possession  61% - 39%            │ Shots        12 - 7              │
│ xG         1.82 - 0.91           │ Corners       8 - 4              │
│ Cards        2 - 1               │ Avg Batch  2.03s                 │
└──────────────────────────────────┴──────────────────────────────────┘

Recent Events
  [67'] 🔄 Sub: Alvarez ↔ Haaland
  [65'] 🚩 Corner - Arsenal
  [62'] 🟨 Yellow Card - Rice
  [58'] 🎯 Shot - Haaland
  [54'] ⚽ GOAL! Foden

────────────────────────────────────────────────────────────────────────────────
```

**3. Final Summary:**
```
🏁 MATCH COMPLETE

Barcelona 3 - 1 Real Madrid

Goals
⚽ Messi 23'
⚽ Suarez 54'
⚽ Neymar 78'

Final Statistics
Possession : 58% - 42%
Shots      : 15 - 9
xG         : 2.12 - 1.04

Replay Time : 0.2 minutes
Events      : 1,500
```

---

### 🚀 How to Run the Dashboard

**Step 1:** Run cells 1-5 (configuration and data loading)
```python
# These cells load the match data and prepare batches
```

**Step 2:** Install Rich library (optional - only for DashboardRenderer class)
```python
# Cell 6
%pip install rich --quiet
dbutils.library.restartPython()
```

**Step 3:** Load dashboard components
```python
# Cells 7-8 (run after Python restart)
# - Event Detector
# - Manager Classes
# - (Optional) Dashboard Renderer class
```

**Step 4:** Start the live replay
```python
# Cell 9
# The dashboard will appear and update in real-time
# Press Ctrl+C to stop early (gracefully handled)
```

---

### 🎨 Color Scheme

| Element         | Color        | Style       |
|-----------------|--------------|-------------|
| Goals           | Bright Green | Bold        |
| Yellow Cards    | Yellow       | Normal      |
| Red Cards       | Red          | Bold        |
| Substitutions   | Cyan         | Normal      |
| Timeline        | Blue/Cyan    | Normal      |
| Headers         | White        | Bold        |
| Team Names      | Cyan         | Bold        |
| Scores          | Green        | Bold        |
| Progress Bar    | Green        | Unicode █░  |

---

### 🔧 Customization Guide

#### Change Replay Speed
```python
# Faster (10x speed)
EVENTS_PER_BATCH = 100
BATCH_INTERVAL_SEC = 0.25

# Slower (more realistic)
EVENTS_PER_BATCH = 25
BATCH_INTERVAL_SEC = 1.0
```

#### Adjust Commentary Feed Length
```python
# In Cell 9, modify CommentaryFeed initialization
commentary = CommentaryFeed(max_items=20)  # Show last 20 events

# In print_dashboard function, adjust display
recent = list(commentary.feed)[-10:]  # Show last 10 instead of 5
```

#### Change Timeline Width
```python
# In Cell 9, modify TimelineManager initialization
timeline = TimelineManager(width=60)  # Narrower timeline
```

#### Add Custom Event Types
```python
# In Cell 7, add to EventDetector._classify_event()
elif event_type == 'Duel':
    return ('🤼', 'duel', f"Duel - {player}")
```

---

### 2. `02_streaming_pipeline_dlt.py` (Python File)

**Purpose:** Lakeflow Spark Declarative Pipeline definition that processes streaming events through bronze → silver → gold layers.

#### 🟤 Layer 1: Bronze - Auto Loader Ingestion
```python
@dlt.table(name="bronze_stream_events")
def bronze_stream_events():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(STREAMING_SOURCE)
            .withColumn("ingested_at", F.current_timestamp())
    )
```

**Auto Loader Benefits:**
- No manual file tracking needed
- Scales to millions of files
- Incremental processing
- Exactly-once semantics

#### ⚪ Layer 2: Silver - Stream-Static Enrichment
```python
@dlt.table(name="silver_enriched_events")
def silver_enriched_events():
    events = dlt.read_stream("bronze_stream_events")
    
    # Flatten nested JSON & join with reference tables
    enriched = (
        events_flat
        .join(player_stats, on="player_id", how="left")
        .join(team_form, on="team_id", how="left")
    )
```

#### 🟡 Layer 3: Gold - Live Match State
```python
@dlt.table(name="gold_live_match_state")
def gold_live_match_state():
    events = dlt.read_stream("silver_enriched_events")
    
    return (
        events
        .groupBy("minute", "team_id", "team_name")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type_name") == "Shot", 1).otherwise(0)).alias("shots"),
            F.avg(F.when(F.col("event_type_name") == "Shot", F.col("shot.statsbomb_xg")).otherwise(None)).alias("avg_xg")
        )
    )
```

---

## 🎯 Portfolio Highlights

### What Makes This Project Stand Out

✨ **Professional UX**
- Live dashboard with in-place updates
- ANSI colors and ASCII art for visual appeal
- Notebook-optimized display (works perfectly in Databricks)

🏗️ **Clean Architecture**
- Separation of concerns (4 manager classes)
- Event-driven design
- Type-safe with dataclasses
- Comprehensive docstrings

⚡ **Technical Depth**
- StatsBomb JSON parsing
- Stream processing with Spark
- Delta Lake for ACID transactions
- IPython display integration

📊 **Data Engineering Best Practices**
- Auto Loader for scalable ingestion
- Stream-static joins for enrichment
- Medallion architecture (Bronze-Silver-Gold)
- Unity Catalog for governance

---

## 🐛 Troubleshooting

### Issue: Dashboard not updating
**Solution:** Check that `IPython.display` is available and cell output is not disabled.

### Issue: Events not detected
**Solution:** Verify StatsBomb JSON structure matches expected format. Check `EventDetector._classify_event()` logic.

### Issue: Team names showing as "Unknown Team"
**Solution:** StatsBomb JSON has nested `team.name` structure. Verify event data includes team information.

### Issue: xG showing as 0.00
**Solution:** Not all shots have xG in StatsBomb data. This is expected for older matches.

### Issue: Colors not showing
**Solution:** Some terminals don't support ANSI colors. The dashboard will still work but without colors.

---

## 📚 Additional Resources

- [IPython Display Documentation](https://ipython.readthedocs.io/en/stable/api/generated/IPython.display.html)
- [StatsBomb Open Data](https://github.com/statsbomb/open-data)
- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/)
- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/workflows/delta-live-tables/)

---

## 🎓 Learning Outcomes

By building this module, you demonstrate:
- Notebook-optimized UI development
- Event-driven architecture
- Stream processing with Spark Structured Streaming
- Real-time data pipeline design
- Object-oriented Python
- Type safety and documentation
- Portfolio-quality code presentation

---

**Next Steps:** Deploy the streaming pipeline to production and connect to a live dashboard for real-time match analysis! 🚀