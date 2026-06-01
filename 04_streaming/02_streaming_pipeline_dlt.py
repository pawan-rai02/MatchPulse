"""
MatchPulse Streaming Pipeline - Lakeflow Spark Declarative Pipeline
====================================================================

Architecture:
  S3 Auto Loader → Bronze → Silver (enriched) → Gold (match state + win prob + pitch events)

Layers:
  1. Bronze  - Raw streaming events from S3 (Auto Loader)
  2. Silver  - Enriched events (stream-static joins with player/team stats)
  3. Gold    - Live match state (stateful aggregation)
  4. Gold    - Win Probability (ML inference - placeholder)
  5. Gold    - Pitch Events (detailed event-level data with all attributes)

Unity Catalog Structure:
  - Bronze/Silver/Gold tables → matchpulse.default schema
  - Player stats → matchpulse.silver schema (batch-built reference data)
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *


# ═══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

# Source path for streaming events (simulator writes here)
STREAMING_SOURCE = "s3a://matchpulse-pawan/streaming_sim/match_events/"

# Unity Catalog tables for static enrichment (batch-built reference data)
PLAYER_CAREER_STATS = "matchpulse.silver.player_career_stats"
TEAM_FORM = "matchpulse.silver.team_form"
PLAYER_VS_TEAM = "matchpulse.silver.player_vs_team"


# ═══════════════════════════════════════════════════════════════════════
# LAYER 1: BRONZE — Auto Loader Stream Source
# ═══════════════════════════════════════════════════════════════════════

@dlt.table(
    name="bronze_stream_events",
    comment="Raw streaming events ingested via Auto Loader from S3",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "minute,timestamp"
    }
)
def bronze_stream_events():
    """
    Ingest streaming JSON events from S3 using Auto Loader.
    
    Auto Loader automatically:
    - Detects new files in S3
    - Infers schema from JSON
    - Handles schema evolution
    - Tracks processed files (checkpoint)
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", f"{STREAMING_SOURCE}_schema")
            .option("cloudFiles.maxFilesPerTrigger", 10)  # Process 10 files per batch
            .load(STREAMING_SOURCE)
            .withColumn("ingested_at", F.current_timestamp())
    )


# ═══════════════════════════════════════════════════════════════════════
# LAYER 2: SILVER — Enrichment via Stream-Static Join
# ═══════════════════════════════════════════════════════════════════════

@dlt.table(
    name="silver_enriched_events",
    comment="Streaming events enriched with player career stats and team form",
    table_properties={
        "quality": "silver"
    }
)
def silver_enriched_events():
    """
    Enrich streaming events with static reference data.
    
    Stream-Static Joins:
    - Player career stats (total goals, xG, shots)
    - Team form (recent performance)
    - Player vs opponent stats
    
    This enables real-time commentary like:
    "Messi shoots - he has 45 career goals vs Real Madrid"
    """
    # Read bronze stream (already flat from generator)
    events = dlt.read_stream("bronze_stream_events")
    
    # Select relevant fields (data is already flat)
    events_flat = (
        events
        .select(
            F.col("event_id"),
            F.col("match_id"),
            F.col("minute"),
            F.col("second"),
            F.col("timestamp"),
            F.col("period"),
            F.col("event_type_name"),
            F.col("player_id"),
            F.col("player_name"),
            F.col("team_id"),
            F.col("team_name"),
            F.col("location_x"),
            F.col("location_y"),
            F.col("ingested_at")
        )
    )
    
    # Load static reference tables (broadcast joins for small tables)
    player_stats = spark.table(PLAYER_CAREER_STATS)
    
    # Join with player career stats
    enriched = (
        events_flat
        .join(
            player_stats.select(
                F.col("player_id"),
                F.col("total_goals"),
                F.col("total_shots"),
                F.col("total_xg"),
                F.col("avg_xg_per_shot"),
                F.col("shot_conversion_pct")
            ),
            on="player_id",
            how="left"
        )
    )
    
    # TODO: Add team_form and player_vs_team joins once those tables are built
    # team_form = spark.table(TEAM_FORM)
    # player_vs_team = spark.table(PLAYER_VS_TEAM)
    
    return enriched

# ═══════════════════════════════════════════════════════════════════════
# LAYER 3: GOLD — Detailed Pitch Events (Event-Level Data)
# ═══════════════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_pitch_events",
    comment="Detailed event-level data with full unpacking of pass, shot, carry, and dribble attributes",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "minute,event_type"
    }
)
def gold_pitch_events():
    """
    Comprehensive event-level table with all event attributes unpacked.
    
    This table provides granular access to:
    - Every event with its location on the pitch
    - Event metadata and timing
    
    Note: Streaming generator provides flat structure.
    For detailed pass/shot analytics, you'll need to parse raw_json field
    or enhance the generator to include those nested fields.
    
    Use cases:
    - Pitch visualization
    - Event timeline analysis
    - Heat maps
    - Player tracking
    """
    # Read from bronze (already flat from generator)
    bronze = dlt.read_stream("bronze_stream_events")
    
    # Select and cast fields (data is already flat)
    pitch_events = (
        bronze
        .select(
            # Basic event identifiers
            F.col("event_id"),
            F.col("match_id").cast("bigint"),
            F.col("period").cast("int"),
            F.col("minute").cast("int"),
            F.col("second").cast("int"),
            F.col("event_type_name").alias("event_type"),
            F.col("index").cast("int"),
            
            # Team and player
            F.col("team_id").cast("int"),
            F.col("team_name"),
            F.col("player_id").cast("int"),
            F.col("player_name"),
            
            # Event location (already flat: location_x, location_y)
            F.col("location_x").cast("double"),
            F.col("location_y").cast("double"),
            
            # Metadata
            F.col("timestamp"),
            F.col("ingestion_ts"),
            F.col("ingested_at"),
            
            # Raw JSON for detailed analysis if needed
            F.col("raw_json")
        )
    )
    
    return pitch_events
# ═══════════════════════════════════════════════════════════════════════
# LAYER 4: GOLD — Stateful Running Match State
# ═══════════════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_live_match_state",
    comment="Real-time match state: score, possession, shots by minute",
    table_properties={
        "quality": "gold"
    }
)
def gold_live_match_state():
    """
    Aggregate streaming events into live match state.
    
    Metrics by minute:
    - Goals scored (both teams)
    - Shots on target
    - Possession percentage
    - xG (expected goals)
    
    This powers the live dashboard.
    """
    events = dlt.read_stream("silver_enriched_events")
    
    # Aggregate by minute and team
    match_state = (
        events
        .groupBy("minute", "team_id", "team_name")
        .agg(
            F.count("*").alias("total_events"),
            
            # Goals (count Pass events with goal_assist flag OR shot outcomes)
            F.sum(
                F.when(F.col("event_type_name") == "Shot", 1).otherwise(0)
            ).alias("shots"),
            
            # Possession events
            F.sum(
                F.when(F.col("possession_team_name") == F.col("team_name"), 1).otherwise(0)
            ).alias("possession_events"),
            
            # Average xG (from shots)
            F.avg(
                F.when(F.col("event_type_name") == "Shot", F.col("shot.statsbomb_xg")).otherwise(None)
            ).alias("avg_xg"),
            
            # Latest timestamp
            F.max("timestamp").alias("last_event_time")
        )
        .withColumn("current_minute", F.col("minute"))
        .withColumn("computed_at", F.current_timestamp())
    )
    
    return match_state


# ═══════════════════════════════════════════════════════════════════════
# LAYER 5: GOLD — Win Probability (Placeholder for ML Inference)
# ═══════════════════════════════════════════════════════════════════════

@dlt.table(
    name="gold_win_probability",
    comment="Real-time win probability updates (placeholder - integrate ML model here)",
    table_properties={
        "quality": "gold"
    }
)
def gold_win_probability():
    """
    Calculate win probability at each match minute.
    
    PLACEHOLDER IMPLEMENTATION:
    - Currently uses simple heuristic (possession + shots)
    - TODO: Replace with actual XGBoost model inference
    - Model should be loaded from MLflow registry
    
    Features for ML model:
    - Current score
    - Shots on target
    - Possession %
    - xG difference
    - Team form
    - Home advantage
    """
    match_state = dlt.read_stream("gold_live_match_state")
    
    # Simple heuristic (replace with actual model)
    win_prob = (
        match_state
        .withColumn(
            "win_probability",
            # Placeholder: normalize shots and possession
            (F.col("shots") * 0.4 + F.col("possession_events") * 0.6) / 100
        )
        .select(
            "current_minute",
            "team_id",
            "team_name",
            "shots",
            "possession_events",
            "win_probability",
            "computed_at"
        )
    )
    
    return win_prob
