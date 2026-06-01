"""
MatchPulse S3 setup
Configure paths + test S3 access
"""

# =====================================================
# BUCKET
# =====================================================

BUCKET = "matchpulse-pawan"

BASE_S3 = f"s3a://{BUCKET}"


# =====================================================
# RAW
# =====================================================

MATCHES_RAW = (
    f"{BASE_S3}/raw/statsbomb/matches/"
)

EVENTS_RAW = (
    f"{BASE_S3}/raw/statsbomb/events/"
)

LINEUPS_RAW = (
    f"{BASE_S3}/raw/statsbomb/lineups/"
)


# =====================================================
# STREAMING
# =====================================================

STREAM_MATCH_EVENTS = (
    f"{BASE_S3}/streaming_sim/match_events/"
)


# =====================================================
# BRONZE
# =====================================================

MATCHES_BRONZE = (
    f"{BASE_S3}/bronze/matches/"
)

EVENTS_BRONZE = (
    f"{BASE_S3}/bronze/events/"
)

LINEUPS_BRONZE = (
    f"{BASE_S3}/bronze/lineups/"
)


# =====================================================
# SILVER
# =====================================================

PLAYER_CAREER_STATS = (
    f"{BASE_S3}/silver/player_career_stats/"
)

TEAM_FORM = (
    f"{BASE_S3}/silver/team_form/"
)

H2H_RECORDS = (
    f"{BASE_S3}/silver/h2h_records/"
)

PLAYER_VS_TEAM = (
    f"{BASE_S3}/silver/player_vs_team/"
)


# =====================================================
# GOLD
# =====================================================

LIVE_MATCH_STATE = (
    f"{BASE_S3}/gold/live_match_state/"
)

WIN_PROBABILITY = (
    f"{BASE_S3}/gold/win_probability/"
)

EVENT_LOG = (
    f"{BASE_S3}/gold/event_log/"
)


# =====================================================
# CHECKPOINTS
# =====================================================

CHECKPOINT_STREAMING = (
    f"{BASE_S3}/checkpoints/streaming_ingest/"
)

CHECKPOINT_ENRICH = (
    f"{BASE_S3}/checkpoints/enrichment/"
)

CHECKPOINT_MATCH = (
    f"{BASE_S3}/checkpoints/match_state/"
)


# =====================================================
# MODELS
# =====================================================

WIN_PROB_MODEL = (
    f"{BASE_S3}/models/win_probability/xgboost_v1/"
)


print("Paths loaded")
print(EVENTS_RAW)