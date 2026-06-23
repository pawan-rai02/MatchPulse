# Databricks notebook source
# DBTITLE 1,Title & Overview
# MAGIC %md
# MAGIC # 2022 FIFA World Cup Final: Pitch Analysis
# MAGIC ## Argentina vs France - December 18, 2022
# MAGIC
# MAGIC **Match ID:** 3869685
# MAGIC
# MAGIC This notebook provides a comprehensive pitch-level analysis of one of the greatest World Cup finals in history using **mplsoccer** library. We'll visualize:
# MAGIC
# MAGIC * 🎯 **Player Heatmap** - Lionel Messi's positioning and movement patterns
# MAGIC * 🔄 **Pass Network** - Argentina's passing connections and team structure
# MAGIC * ⚽ **Shot Map** - All shots from both teams with Expected Goals (xG) analysis
# MAGIC
# MAGIC Data source: StatsBomb open data processed via streaming pipeline → [matchpulse.default.gold_pitch_events](#table)

# COMMAND ----------

# DBTITLE 1,Import Libraries
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F
from mplsoccer import Pitch, VerticalPitch, FontManager
import numpy as np

# Databricks display settings
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 100

# COMMAND ----------

# DBTITLE 1,Data Loading
# MAGIC %md
# MAGIC ## 📊 Load World Cup Final Event Data
# MAGIC
# MAGIC Loading match events from [matchpulse.default.gold_pitch_events](#table), the streaming gold table with flattened pitch coordinates

# COMMAND ----------

# DBTITLE 1,Load Events from Gold
# Load gold events from streaming table (location already flattened)
events_df = (
    spark.table("matchpulse.default.gold_pitch_events")
    .filter(F.col("match_id") == 3869685)
    .select(
        "match_id",
        "team_name",
        "player_name",
        "event_type",
        "location_x",
        "location_y",
        "minute",
        "period",
        "raw_json"  # For pass and shot details
    )
)

# Parse raw_json for pass and shot details
from pyspark.sql.types import DoubleType

events_df = (
    events_df
    .withColumn("pass_end_location_x", 
                F.get_json_object(F.col("raw_json"), "$.pass.end_location[0]").cast(DoubleType()))
    .withColumn("pass_end_location_y", 
                F.get_json_object(F.col("raw_json"), "$.pass.end_location[1]").cast(DoubleType()))
    .withColumn("shot_statsbomb_xg", 
                F.get_json_object(F.col("raw_json"), "$.shot.statsbomb_xg").cast(DoubleType()))
    .withColumn("shot_outcome_name", 
                F.get_json_object(F.col("raw_json"), "$.shot.outcome.name"))
)

# Convert to pandas for mplsoccer
events_pd = events_df.toPandas()

print(f"Total events: {len(events_pd):,}")
print(f"\nTeams: {events_pd['team_name'].unique()}")
print(f"Event types: {events_pd['event_type'].nunique()}")
print(f"\nPass events: {events_pd['event_type'].value_counts().get('Pass', 0)}")
print(f"Shot events: {events_pd['event_type'].value_counts().get('Shot', 0)}")

# COMMAND ----------

# DBTITLE 1,Player Heatmap Section
# MAGIC %md
# MAGIC ## 🔥 Player Heatmap: Lionel Messi
# MAGIC
# MAGIC Visualizing Messi's positioning and movement throughout the final. This shows where he received the ball and operated during the match.

# COMMAND ----------

# DBTITLE 1,Create Messi Heatmap
# Filter for Messi's actions
messi_df = events_pd[
    (events_pd['player_name'] == 'Lionel Andrés Messi Cuccittini') &
    (events_pd['location_x'].notna()) &
    (events_pd['location_y'].notna())
].copy()

print(f"Messi touch events: {len(messi_df)}")

# Create vertical pitch
pitch = VerticalPitch(
    pitch_type='statsbomb',
    pitch_color='#22312b',
    line_color='#c7d5cc',
    half=False
)

fig, ax = pitch.draw(figsize=(10, 14))

# Create heatmap using hexbin
hexmap = pitch.hexbin(
    messi_df['location_x'],
    messi_df['location_y'],
    ax=ax,
    edgecolors='#22312b',
    gridsize=15,
    cmap='hot',
    alpha=0.8
)

# Add colorbar
cbar = plt.colorbar(hexmap, ax=ax, fraction=0.035, pad=0.04)
cbar.set_label('Touch Frequency', rotation=270, labelpad=20, color='white', size=12)
cbar.ax.yaxis.set_tick_params(color='white')
plt.setp(plt.getp(cbar.ax.axes, 'yticklabels'), color='white')

# Title
ax.text(
    60, 122,
    "Lionel Messi - Touch Heatmap",
    size=18,
    color='white',
    fontweight='bold',
    ha='center'
)
ax.text(
    60, 118,
    "2022 World Cup Final | Argentina vs France",
    size=12,
    color='#c7d5cc',
    ha='center'
)

plt.tight_layout()
display(fig)
plt.close()

# COMMAND ----------

# DBTITLE 1,Pass Network Section
# MAGIC %md
# MAGIC ## 🔄 Team Pass Network: Argentina
# MAGIC
# MAGIC Showing passing connections between Argentina players. Line thickness represents the number of passes between players, and node size shows total passes made by each player.

# COMMAND ----------

# DBTITLE 1,Create Argentina Pass Network
# Filter for Argentina passes (excluding throw-ins and set pieces for clarity)
arg_passes = events_pd[
    (events_pd['team_name'] == 'Argentina') &
    (events_pd['event_type'] == 'Pass') &
    (events_pd['location_x'].notna()) &
    (events_pd['pass_end_location_x'].notna()) &
    (events_pd['period'] <= 2)  # Regular time only for cleaner network
].copy()

print(f"Argentina passes in regular time: {len(arg_passes)}")

# Calculate average position for each player
player_positions = (
    arg_passes.groupby('player_name')
    .agg({
        'location_x': 'mean',
        'location_y': 'mean'
    })
    .reset_index()
)

# Filter to players with significant involvement (at least 10 passes)
player_pass_counts = arg_passes['player_name'].value_counts()
key_players = player_pass_counts[player_pass_counts >= 10].index
player_positions = player_positions[player_positions['player_name'].isin(key_players)]

print(f"Key players (10+ passes): {len(player_positions)}")

# Calculate pass connections between players
# We'll use pass start and end locations to infer connections
pass_connections = []

for idx, passer_row in player_positions.iterrows():
    passer_name = passer_row['player_name']
    passer_x = passer_row['location_x']
    passer_y = passer_row['location_y']
    
    # Get all passes from this player
    passer_passes = arg_passes[arg_passes['player_name'] == passer_name]
    
    # For each pass end location, find the closest player position (likely receiver)
    for _, pass_row in passer_passes.iterrows():
        pass_end_x = pass_row['pass_end_location_x']
        pass_end_y = pass_row['pass_end_location_y']
        
        # Find closest player to pass end location (excluding passer)
        min_dist = float('inf')
        receiver = None
        
        for _, receiver_row in player_positions.iterrows():
            if receiver_row['player_name'] != passer_name:
                dist = np.sqrt(
                    (receiver_row['location_x'] - pass_end_x)**2 + 
                    (receiver_row['location_y'] - pass_end_y)**2
                )
                if dist < min_dist and dist < 15:  # Within 15 units
                    min_dist = dist
                    receiver = receiver_row['player_name']
        
        if receiver:
            pass_connections.append((passer_name, receiver))

# Count connections
from collections import Counter
connection_counts = Counter(pass_connections)

print(f"Pass connections identified: {len(connection_counts)}")

# Create pitch
pitch = Pitch(
    pitch_type='statsbomb',
    pitch_color='#22312b',
    line_color='#c7d5cc'
)

fig, ax = pitch.draw(figsize=(14, 10))

# Draw pass connections (lines)
for (passer, receiver), count in connection_counts.items():
    if count >= 3:  # Only show connections with 3+ passes
        passer_pos = player_positions[player_positions['player_name'] == passer].iloc[0]
        receiver_pos = player_positions[player_positions['player_name'] == receiver].iloc[0]
        
        # Draw arrow
        pitch.lines(
            passer_pos['location_x'],
            passer_pos['location_y'],
            receiver_pos['location_x'],
            receiver_pos['location_y'],
            lw=count / 2,  # Line width proportional to pass count
            color='#75AADB',
            alpha=0.4,
            zorder=1,
            ax=ax
        )

# Plot player positions
for _, player in player_positions.iterrows():
    # Circle size based on number of passes
    pass_count = player_pass_counts[player['player_name']]
    size = pass_count * 3
    
    pitch.scatter(
        player['location_x'],
        player['location_y'],
        s=size,
        color='#75AADB',
        edgecolors='white',
        linewidth=2,
        alpha=0.9,
        ax=ax,
        zorder=3
    )
    
    # Player name (last name only for clarity)
    last_name = player['player_name'].split()[-1]
    ax.text(
        player['location_x'],
        player['location_y'] - 3,
        last_name,
        size=8,
        color='white',
        ha='center',
        va='top',
        fontweight='bold',
        zorder=4
    )

# Title
ax.text(
    60, 83,
    "Argentina Pass Network - Regular Time",
    size=18,
    color='white',
    fontweight='bold',
    ha='center'
)
ax.text(
    60, 80,
    "Line width = pass frequency | Node size = total passes | 2022 World Cup Final",
    size=11,
    color='#c7d5cc',
    ha='center'
)

plt.tight_layout()
display(fig)
plt.close()

# COMMAND ----------

# DBTITLE 1,Shot Map Section
# MAGIC %md
# MAGIC ## ⚽ Shot Map: Both Teams
# MAGIC
# MAGIC All shots from both teams sized by Expected Goals (xG). Goals are marked with a star marker.

# COMMAND ----------

# DBTITLE 1,Create Shot Map
import matplotlib.patheffects as pe

# Filter for shots
shots = events_pd[
    (events_pd['event_type'] == 'Shot') &
    (events_pd['location_x'].notna()) &
    (events_pd['shot_statsbomb_xg'].notna())
].copy()

print(f"Total shots: {len(shots)}")
print(f"Argentina shots: {len(shots[shots['team_name'] == 'Argentina'])}")
print(f"France shots: {len(shots[shots['team_name'] == 'France'])}")

# Separate by team
arg_shots = shots[shots['team_name'] == 'Argentina'].copy()
fra_shots = shots[shots['team_name'] == 'France'].copy()

# Identify goals
arg_shots['is_goal'] = arg_shots['shot_outcome_name'] == 'Goal'
fra_shots['is_goal'] = fra_shots['shot_outcome_name'] == 'Goal'

# Count goals for the scoreline
arg_goals_count = int(arg_shots['is_goal'].sum())
fra_goals_count = int(fra_shots['is_goal'].sum())

# Colors
PITCH = '#22312b'
LINE = '#c7d5cc'
ARG = '#7DD3FC'  # Light blue for Argentina
FRA = '#2563EB'  # Darker blue for France

# Create full pitch
fig, ax = plt.subplots(figsize=(14, 9), facecolor=PITCH)

pitch = Pitch(
    pitch_type='statsbomb',
    pitch_color=PITCH,
    line_color=LINE,
    linewidth=2
)

pitch.draw(ax=ax)

# Plot Argentina shots (attacking left to right)
for _, shot in arg_shots.iterrows():
    color = ARG if shot['is_goal'] else 'white'
    alpha = 1 if shot['is_goal'] else 0.35
    
    sc = pitch.scatter(
        shot['location_x'],
        shot['location_y'],
        s=1400 * shot['shot_statsbomb_xg'],
        c=color,
        ec=ARG,
        lw=2,
        alpha=alpha,
        ax=ax,
        zorder=3
    )
    
    # Add glow effect for goals
    if shot['is_goal']:
        sc.set_path_effects([
            pe.withStroke(linewidth=8, foreground=ARG)
        ])

# Plot France shots (flip coordinates - attacking right to left)
for _, shot in fra_shots.iterrows():
    # Mirror the coordinates for France
    x = 120 - shot['location_x']
    y = 80 - shot['location_y']
    
    color = FRA if shot['is_goal'] else 'white'
    alpha = 1 if shot['is_goal'] else 0.35
    
    sc = pitch.scatter(
        x=x,
        y=y,
        s=1400 * shot['shot_statsbomb_xg'],
        c=color,
        ec=FRA,
        lw=2,
        alpha=alpha,
        ax=ax,
        zorder=3
    )
    
    # Add glow effect for goals
    if shot['is_goal']:
        sc.set_path_effects([
            pe.withStroke(linewidth=8, foreground=FRA)
        ])

# -----------------------------
# Header
# -----------------------------
fig.text(
    0.5,
    0.95,
    f"Argentina  🇦🇷 (4) 3 - 3 (2) 🇫🇷  France",
    ha='center',
    color='white',
    fontsize=26,
    fontweight='bold'
)

fig.text(
    0.5,
    0.91,
    "2022 FIFA World Cup Final",
    ha='center',
    color='lightgray',
    fontsize=14
)

# Team Labels on pitch
ax.text(
    90,
    86,
    "ARGENTINA",
    color=ARG,
    fontsize=18,
    fontweight='bold',
    ha='center'
)

ax.text(
    30,
    86,
    "FRANCE",
    color=FRA,
    fontsize=18,
    fontweight='bold',
    ha='center'
)

# Legend
ax.scatter([], [], s=300, c='white', alpha=0.35, ec=ARG, lw=2, label='Shot')
ax.scatter([], [], s=300, c=ARG, ec=ARG, lw=2, label='Goal')

leg = ax.legend(
    loc='lower center',
    bbox_to_anchor=(0.5, -0.03),
    ncol=2,
    frameon=False,
    fontsize=12
)

for text in leg.get_texts():
    text.set_color('white')

ax.set_axis_off()

plt.tight_layout()
display(fig)
plt.close()

# COMMAND ----------

# DBTITLE 1,Summary Statistics
# MAGIC %md
# MAGIC ## 📈 Key Insights & Summary Statistics

# COMMAND ----------

# DBTITLE 1,Calculate Summary Stats
# Calculate key statistics
arg_total_xg = arg_shots['shot_statsbomb_xg'].sum()
fra_total_xg = fra_shots['shot_statsbomb_xg'].sum()

arg_goals = arg_shots['is_goal'].sum()
fra_goals = fra_shots['is_goal'].sum()

arg_shot_count = len(arg_shots)
fra_shot_count = len(fra_shots)

messi_shots = arg_shots[arg_shots['player_name'] == 'Lionel Andrés Messi Cuccittini']
messi_xg = messi_shots['shot_statsbomb_xg'].sum()
messi_goals = messi_shots['is_goal'].sum()

# Display summary
print("=" * 60)
print("          2022 WORLD CUP FINAL - MATCH STATISTICS")
print("=" * 60)
print(f"\n🇦🇷 ARGENTINA")
print(f"   Total Shots: {arg_shot_count}")
print(f"   Goals: {arg_goals}")
print(f"   Total xG: {arg_total_xg:.2f}")
print(f"   xG per Shot: {arg_total_xg/arg_shot_count:.2f}")

print(f"\n🇫🇷 FRANCE")
print(f"   Total Shots: {fra_shot_count}")
print(f"   Goals: {fra_goals}")
print(f"   Total xG: {fra_total_xg:.2f}")
print(f"   xG per Shot: {fra_total_xg/fra_shot_count:.2f}")

print(f"\n⭐ LIONEL MESSI")
print(f"   Shots: {len(messi_shots)}")
print(f"   Goals: {int(messi_goals)}")
print(f"   Total xG: {messi_xg:.2f}")
print(f"   Touch Events: {len(messi_df)}")

print(f"\n📊 MATCH INSIGHTS")
print(f"   Total Events Analyzed: {len(events_pd):,}")
print(f"   xG Difference: {abs(arg_total_xg - fra_total_xg):.2f}")
print(f"   Higher xG Team: {'Argentina' if arg_total_xg > fra_total_xg else 'France'}")
print("=" * 60)