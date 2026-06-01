# 02_batch_historical - Silver Layer Feature Engineering

## Overview
This folder contains notebooks that transform bronze layer data into silver layer analytical tables. These tables build historical features for machine learning models and real-time predictions, including player statistics, team form, head-to-head records, and player vs team matchups.

---

## 📊 Data Summary

### Total Data Volume
- **5,999** unique players with career statistics
- **8,365,579** events analyzed from bronze layer
- **2,334** matches processed
- **187** teams tracked

---

## 📁 Notebooks & Data Details

### 1️⃣ `01_build_player_career_stats.ipynb`

**Purpose:** Aggregates event-level data to create comprehensive player career statistics

**Input:** Bronze events (`s3a://matchpulse-pawan/bronze/events/`)  
**Output:** Silver table `matchpulse.silver.player_career_stats`  
**Processing:** Event aggregation by player_id

#### Data Statistics
- **Total Players:** 5,999
- **Players with Shots:** 3,947
- **Players with Goals:** 1,597
- **Source Events:** 8,365,579 total events
- **Player Events:** 8,331,704 (events with player_id)
- **Last Updated:** 2026-05-27

#### Schema
| Field | Type | Description |
|-------|------|-------------|
| `player_id` | INT | Unique player identifier (Primary Key) |
| `player_name` | STRING | Full player name |
| `total_matches` | INT | Number of matches played |
| `total_shots` | INT | Total shots taken |
| `total_goals` | INT | Total goals scored |
| `total_xg` | DOUBLE | Total expected goals (xG) |
| `avg_xg_per_shot` | DOUBLE | Average xG per shot (quality of chances) |
| `shot_conversion_pct` | DOUBLE | Goal conversion rate (goals / shots * 100) |
| `total_passes` | INT | Total passes attempted |
| `total_assists` | INT | Total assists provided |
| `ingestion_ts` | TIMESTAMP | Data processing timestamp |

#### Top 10 Goal Scorers (from processed data)
| Player | Matches | Shots | Goals | xG |
|--------|---------|-------|-------|----|
| Lionel Andrés Messi Cuccittini | 591 | 2,632 | 506 | 357.48 |
| Luis Alberto Suárez Díaz | 191 | 632 | 138 | 110.01 |
| Neymar da Silva Santos Junior | 309 | 875 | 174 | 138.92 |
| Cristiano Ronaldo dos Santos Aveiro | 161 | 492 | 90 | 82.35 |
| Karim Benzema | 142 | 408 | 81 | 67.42 |

#### Derived Metrics
- **avg_xg_per_shot:** Indicates shot quality (higher = better chances)
- **shot_conversion_pct:** Finishing efficiency (goals / shots)
- Filters out non-shooting players (goalkeepers, defenders with no attacking contributions)

---

### 2️⃣ `02_build_team_form.ipynb`

**Purpose:** Calculates rolling team form metrics based on recent match results

**Input:** Bronze matches (`s3a://matchpulse-pawan/bronze/matches/`)  
**Output:** Silver table `matchpulse.silver.team_form`  
**Processing:** Time-series aggregation with rolling windows

#### Expected Features
- **team_id** (INT, PK): Team identifier
- **team_name** (STRING): Team name
- **as_of_date** (DATE): Form calculation date
- **last_5_matches** (STRUCT): Last 5 match results (W/D/L)
- **last_10_matches** (STRUCT): Last 10 match results
- **win_rate_last_5** (DOUBLE): Win percentage in last 5 games
- **win_rate_last_10** (DOUBLE): Win percentage in last 10 games
- **goals_scored_avg_last_5** (DOUBLE): Average goals scored
- **goals_conceded_avg_last_5** (DOUBLE): Average goals conceded
- **goal_difference_last_5** (INT): Goal difference trend
- **home_form** (STRUCT): Form metrics for home matches
- **away_form** (STRUCT): Form metrics for away matches
- **current_streak** (STRING): Current win/loss/draw streak

#### Use Cases
- Predict match outcomes based on recent team performance
- Identify teams in good/bad form
- Home vs away performance analysis
- Momentum indicators for betting models

---

### 3️⃣ `03_build_h2h_records.ipynb`

**Purpose:** Builds head-to-head historical records between teams

**Input:** Bronze matches (`s3a://matchpulse-pawan/bronze/matches/`)  
**Output:** Silver table `matchpulse.silver.h2h_records`  
**Processing:** Pairwise team aggregation

#### Expected Features
- **team_a_id** (INT, PK): First team identifier
- **team_b_id** (INT, PK): Second team identifier
- **team_a_name** (STRING): First team name
- **team_b_name** (STRING): Second team name
- **total_matches** (INT): Total matches played between teams
- **team_a_wins** (INT): Wins for team A
- **team_b_wins** (INT): Wins for team B
- **draws** (INT): Draw count
- **team_a_goals_total** (INT): Total goals scored by team A
- **team_b_goals_total** (INT): Total goals scored by team B
- **avg_goals_per_match** (DOUBLE): Average total goals in matchups
- **last_5_results** (ARRAY): Results of last 5 encounters
- **longest_winning_streak_a** (INT): Longest winning streak for team A
- **longest_winning_streak_b** (INT): Longest winning streak for team B

#### Use Cases
- Historical dominance patterns
- Derby match analysis
- Rivalry insights for prediction models
- High/low scoring matchup identification

---

### 4️⃣ `04_build_player_vs_team.ipynb`

**Purpose:** Analyzes individual player performance against specific teams

**Input:** Bronze events + matches  
**Output:** Silver table `matchpulse.silver.player_vs_team`  
**Processing:** Player-team pair aggregation

#### Expected Features
- **player_id** (INT, PK): Player identifier
- **team_id** (INT, PK): Opponent team identifier
- **player_name** (STRING): Player name
- **team_name** (STRING): Opponent team name
- **matches_played** (INT): Matches played against this team
- **total_goals** (INT): Goals scored against this team
- **total_shots** (INT): Shots taken against this team
- **total_xg** (DOUBLE): Expected goals against this team
- **total_assists** (INT): Assists against this team
- **total_passes** (INT): Passes made against this team
- **avg_goals_per_match** (DOUBLE): Scoring rate vs this team
- **conversion_rate** (DOUBLE): Shot conversion vs this team
- **last_match_date** (DATE): Most recent match against team

#### Use Cases
- Identify players who consistently perform well/poorly vs specific teams
- "Favorite opponent" analysis
- Player matchup predictions
- Squad selection insights

---

## 🔄 Data Flow

```
Bronze Layer
    ↓
01_build_player_career_stats → player_career_stats table (5,999 players)
02_build_team_form → team_form table (187 teams)
03_build_h2h_records → h2h_records table (team pairs)
04_build_player_vs_team → player_vs_team table (player-team pairs)
    ↓
Silver Layer (matchpulse.silver schema)
```

---

## 🏃 Execution Order

All notebooks can run independently (no cross-dependencies), but recommended order:

1. **01_build_player_career_stats** - Foundation for player analysis
2. **02_build_team_form** - Foundation for team analysis
3. **03_build_h2h_records** - Team relationship analysis
4. **04_build_player_vs_team** - Granular player-opponent analysis

**Dependencies:**
- All require bronze layer to be populated first (run 01_ingestion notebooks)
- Can be scheduled independently for incremental updates

---

## 📦 Storage Locations

**Input (Bronze Layer):**
- `s3a://matchpulse-pawan/bronze/matches/`
- `s3a://matchpulse-pawan/bronze/events/`
- `s3a://matchpulse-pawan/bronze/lineups/`

**Output (Silver Layer):**
- Unity Catalog: `matchpulse.silver` schema
  - `player_career_stats` - Confirmed created
  - `team_form` - To be created
  - `h2h_records` - To be created
  - `player_vs_team` - To be created

---

## 🎯 Key Features & Insights

### Player Career Stats
- **Coverage:** 5,999 players, 3,947 with shot data
- **Messi Dominance:** 506 goals from 2,632 shots across 591 matches
- **xG Integration:** Expected goals tracked for shot quality analysis
- **Efficiency Metrics:** Conversion rates and avg xG per shot

### Team Form (Expected)
- **Time Windows:** 5-match and 10-match rolling windows
- **Home/Away Split:** Separate form tracking for venue
- **Streak Detection:** Current win/loss/draw streaks

### H2H Records (Expected)
- **Rivalry Analysis:** Historical head-to-head for all team pairs
- **Trend Tracking:** Last 5 encounters for recent form
- **Goal Patterns:** Average goals in matchups

### Player vs Team (Expected)
- **Matchup Analysis:** Individual performance against specific opponents
- **Favorite Opponents:** Players who consistently perform vs certain teams
- **Tactical Insights:** Squad selection data

---

## 🛠️ Technical Details

### Processing Approach
- **Player Stats:** Single-pass aggregation with shot outcome parsing from raw_json
- **Team Form:** Window functions with date-based partitioning
- **H2H Records:** Self-join on matches table with team pair logic
- **Player vs Team:** Multi-way join (events + matches + teams)

### Optimization
- Parquet input format for fast reads
- Unity Catalog tables for governance
- Incremental update capable (timestamp-based)
- Indexed on primary keys for fast lookups

### Data Quality
- **NULL handling:** Players without shots have 0 values, not NULL
- **Shot parsing:** xG extracted from raw_json shot.statsbomb_xg field
- **Outcome mapping:** Shot outcomes mapped to goal/no-goal
- **Timestamp tracking:** ingestion_ts for data lineage

---

## 📊 Metrics Summary

| Metric | Value |
|--------|-------|
| Total Players Processed | 5,999 |
| Players with Shots | 3,947 (65.8%) |
| Players with Goals | 1,597 (26.6%) |
| Total Events Processed | 8,365,579 |
| Total Matches | 2,334 |
| Teams Tracked | 187 |
| Average Events per Match | 3,583 |
| Top Scorer (Messi) | 506 goals |
| Processing Time | ~5-10 min per notebook |

---

## 🔍 Query Examples

### Find Top Goal Scorers
```sql
SELECT 
    player_name,
    total_goals,
    total_shots,
    ROUND(shot_conversion_pct, 1) as conversion_pct,
    ROUND(total_xg, 2) as expected_goals
FROM matchpulse.silver.player_career_stats
WHERE total_shots > 50
ORDER BY total_goals DESC
LIMIT 20;
```

### Find Most Efficient Finishers (min 50 shots)
```sql
SELECT 
    player_name,
    total_goals,
    total_shots,
    ROUND(shot_conversion_pct, 1) as conversion_pct,
    ROUND(avg_xg_per_shot, 3) as avg_xg_per_shot
FROM matchpulse.silver.player_career_stats
WHERE total_shots >= 50
ORDER BY shot_conversion_pct DESC
LIMIT 20;
```

### Players Outperforming xG (scoring more than expected)
```sql
SELECT 
    player_name,
    total_goals,
    ROUND(total_xg, 2) as expected_goals,
    total_goals - total_xg as goals_above_expected,
    ROUND((total_goals - total_xg) / total_shots * 100, 2) as overperformance_pct
FROM matchpulse.silver.player_career_stats
WHERE total_shots >= 100
ORDER BY (total_goals - total_xg) DESC
LIMIT 20;
```

---

## 📝 Notes

- **xG Data:** Sourced from StatsBomb's advanced analytics (statsbomb_xg field)
- **Shot Outcomes:** Parsed from raw_json to determine goal vs non-goal
- **Match Context:** Player stats don't yet include home/away splits (future enhancement)
- **Incremental Updates:** Tables can be updated incrementally as new matches arrive
- **Unity Catalog:** All silver tables registered for governance and discovery

---

## 🔮 Future Enhancements

- [ ] Add venue splits (home/away performance) to player stats
- [ ] Include competition-level breakdowns (league vs cup performance)
- [ ] Add rolling form windows to player stats (last 5/10 matches)
- [ ] Integrate defensive stats (tackles, interceptions, clearances)
- [ ] Add goalkeeper-specific metrics (saves, clean sheets, xG prevented)
- [ ] Implement SCD Type 2 for historical tracking of changing stats

---

**Last Updated:** 2026-05-29  
**Data Source:** StatsBomb Open Data (Bronze Layer)  
**Maintained by:** MatchPulse Team  
**Next Layer:** 03_ml (Machine Learning Feature Store)