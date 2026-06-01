# MatchPulse ML Pipeline - Win Probability Model

A complete machine learning pipeline for predicting live football match outcomes using XGBoost. The model generates real-time win probabilities (home win, draw, away win) based on in-game match states.

## 📋 Overview

This pipeline processes historical match data to create a predictive model that can estimate match outcomes at any point during a live game. The model considers multiple factors including current score, expected goals (xG), shots, red cards, and team form.

**Model Performance:**
* **Test Accuracy**: 72.64%
* **Training Samples**: 167,961
* **Test Samples**: 41,991
* **Features**: 10 numeric features
* **Target Classes**: 3 (home_win, draw, away_win)
* **Algorithm**: XGBoost Multi-class Classifier

---

## 🏗️ Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Data Sources                                │
│  • Matches Bronze: s3a://matchpulse-pawan/bronze/matches/       │
│  • Events Bronze: s3a://matchpulse-pawan/bronze/events/         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  01_create_training_data.ipynb                                   │
│  • Extracts match states at each minute (0-100)                 │
│  • Calculates cumulative xG, shots, score progression           │
│  • Generates 209,952 training samples from 2,187 matches        │
│  Output: matchpulse.ml.training_match_states                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  02_train_win_probability.ipynb                                  │
│  • Trains XGBoost classifier with 10 features                   │
│  • Logs model to MLflow with metrics and artifacts              │
│  • Attempts Unity Catalog registration                          │
│  Output: MLflow Run 16ea861d4c4d4b97975059f4adaf23bc            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  03_validate_model.ipynb                                         │
│  • Confusion Matrix - Classification performance breakdown      │
│  • Feature Importance - One-line XGBoost visualization          │
│  • Win Probability Trajectory - Liverpool 4-0 Barcelona (2019)  │
│     Famous comeback showing real-time probability shifts        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📓 Notebooks

### 1. `01_create_training_data.ipynb` (ID: 644447621029057)

**Purpose:** Generates minute-by-minute match states from historical matches for model training.

**Process:**
1. Reads matches from Delta table (2,187 matches)
2. Reads events from Parquet files (8,365,579 events)
3. Creates match-minute pairs (0-100 minutes for injury time)
4. Extracts shot events and calculates cumulative xG from raw_json
5. Identifies goal events and red card events
6. Calculates score progression at each minute
7. Aggregates cumulative statistics (shots, xG, red cards)
8. Joins all features into final training dataset
9. Writes to Unity Catalog: `matchpulse.ml.training_match_states`

**Key Features Generated:**
* `minute` - Current match minute (0-100)
* `current_score_diff` - Home score minus away score
* `home_xg_so_far` / `away_xg_so_far` - Cumulative expected goals
* `home_shots` / `away_shots` - Cumulative shots
* `home_red_cards` / `away_red_cards` - Cumulative red cards
* `home_form_pts` / `away_form_pts` - Team form (placeholder: 0)
* `final_outcome` - Target variable (home_win, draw, away_win)

**Output Statistics:**
* Total samples: 209,952
* Average samples per match: ~96 (one per minute)
* Class distribution:
  - Home wins: 95,232 samples (45.4%)
  - Away wins: 64,704 samples (30.8%)
  - Draws: 50,016 samples (23.8%)

**Known Issues Fixed:**
* Events are stored as Parquet (not Delta) - code updated to use `spark.read.format("parquet")`
* Schema is flattened with `event_type_name` and `team_id` at root level
* xG and goal/card data extracted from `raw_json` field using `F.get_json_object()`

---

### 2. `02_train_win_probability.ipynb` (ID: 644447621029058)

**Purpose:** Trains XGBoost multi-class classifier and logs model to MLflow.

**Process:**
1. Loads training data from Unity Catalog table
2. Converts to Pandas DataFrame for sklearn compatibility
3. Encodes target variable (home_win→0, draw→1, away_win→2)
4. Splits data 80/20 (stratified by outcome)
5. Trains XGBoost with optimized hyperparameters
6. Evaluates model on test set
7. Logs model, metrics, and artifacts to MLflow
8. Registers model in Unity Catalog (with error handling)

**XGBoost Hyperparameters:**
```python
params = {
    'objective': 'multi:softprob',
    'num_class': 3,
    'max_depth': 6,
    'learning_rate': 0.1,
    'n_estimators': 200,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'eval_metric': 'mlogloss',
    'random_state': 42
}
```

**Model Performance:**
* **Test Accuracy**: 72.64%
* **Training samples**: 167,961
* **Test samples**: 41,991
* **MLflow Run ID**: 16ea861d4c4d4b97975059f4adaf23bc
* **MLflow Experiment**: `/Users/pawanvirat32@gmail.com/MatchPulse/win_probability_experiments`

**Artifacts Logged:**
* XGBoost model (with signature)
* Confusion matrix plot
* Feature importance plot
* Classification report
* Hyperparameters
* Test/train metrics

**Feature Importance (Top 5):**
1. `current_score_diff` - Most predictive feature (72%)
2. `minute` - Match progression matters (7.5%)
3. `away_xg_so_far` / `home_xg_so_far` - Quality of chances (~6% each)
4. `away_shots` / `home_shots` - Attacking pressure (~5% each)

**Known Issues Fixed:**
* MLflow UC registration requires `mlflow.set_registry_uri("databricks-uc")` **before** starting run
* Registration may fail due to S3 permissions (AccessDenied on UC storage bucket)
* Fallback: Load model from MLflow run using `runs:/{run_id}/model`

**Current Limitation:**
* **Unity Catalog registration fails** with S3 `AccessDenied` error
* **Root cause**: IAM role lacks `s3:PutObject` permission on UC bucket
* **Workaround**: Model logged successfully to MLflow tracking; load with `mlflow.xgboost.load_model(f"runs:/{run_id}/model")`

---

### 3. `03_validate_model.ipynb` (ID: 644447621029059)

**Purpose:** Comprehensive model validation with confusion matrix, feature importance, and famous match replay.

**Process:**
1. Loads test data and trained model from MLflow
2. Makes predictions on test set
3. Creates confusion matrix visualization
4. Generates feature importance bar chart (one line: `model.feature_importances_`)
5. Simulates Liverpool 4-0 Barcelona (2019) minute-by-minute
6. Plots win probability trajectory showing dramatic probability shifts

**Validation Components:**

**1. Confusion Matrix**
* **Overall Accuracy**: 72.64%
* **Home Win Performance**: 89.6% recall (best - model catches most home wins)
* **Draw Performance**: 45.8% recall (hardest to predict, which is realistic in football)
* **Away Win Performance**: 68.4% recall (solid performance)
* **Key Insight**: Model struggles most with draws, often confusing them with wins

**2. Feature Importance**
* **Score Difference dominates** at 72% importance - validates that current score is the strongest predictor
* **Minute** contributes 7.5% - time matters for outcome certainty
* **xG metrics** contribute ~6% each - quality of chances adds value
* **Shot counts** contribute ~5% each - volume matters but less than quality
* **Form points** have zero importance - currently placeholders (all 0)

**Interpretation:**
The feature importance validates the model is learning sensible football patterns - score matters most, but the quality and volume of chances (xG, shots) provide additional context.
sues Fixed:**
* Model loads from MLflow run if UC registration unavailable
* Simulation uses realistic match statistics (xG, shots) for each phase

---

## 📊 Data Requirements

### Input Data

**1. Matches Bronze Table** (Delta format)
* **Location**: `s3a://matchpulse-pawan/bronze/matches/`
* **Format**: Delta
* **Schema**:
  ```
  match_id: long
  match_date: string
  home_team: struct<home_team_id: long, home_team_name: string>
  away_team: struct<away_team_id: long, away_team_name: string>
  home_score: int
  away_score: int
  ```
* **Count**: 2,187 matches

**2. Events Bronze Table** (Parquet format)
* **Location**: `s3a://matchpulse-pawan/bronze/events/`
* **Format**: Parquet (NOT Delta)
* **Schema** (flattened):
  ```
  match_id: long
  event_id: string
  index: long
  period: int
  timestamp: string
  minute: int
  second: int
  event_type_name: string
  team_id: long
  player_id: long
  player_name: string
  raw_json: string  -- Contains nested data (xG, cards, etc.)
  ```
* **Count**: 8,365,579 events
* **Important**: xG values, card types, and other nested fields stored in `raw_json`

### Output Data

**Training Table** (Unity Catalog)
* **Location**: `matchpulse.ml.training_match_states`
* **Format**: Delta
* **Samples**: 209,952
* **Schema**:
  ```
  minute: int
  current_score_diff: int
  home_xg_so_far: double
  away_xg_so_far: double
  home_shots: long
  away_shots: long
  home_red_cards: long
  away_red_cards: long
  home_form_pts: int
  away_form_pts: int
  final_outcome: string  -- Target: home_win, draw, away_win
  ```

**Model Artifacts**
* **MLflow Experiment**: `/Users/pawanvirat32@gmail.com/MatchPulse/win_probability_experiments`
* **Latest Run ID**: 16ea861d4c4d4b97975059f4adaf23bc
* **Model URI**: `runs:/16ea861d4c4d4b97975059f4adaf23bc/model`
* **Intended UC Location**: `matchpulse.ml.win_probability_model` (registration currently fails)

---

## 🚀 How to Run

### Prerequisites

1. **Databricks Workspace** with Serverless compute (or cluster with ML Runtime)
2. **Unity Catalog** enabled with `matchpulse` catalog and `ml` schema
3. **S3 Access** to bronze layer data:
   * `s3a://matchpulse-pawan/bronze/matches/`
   * `s3a://matchpulse-pawan/bronze/events/`
4. **Python Libraries**: Pre-installed on ML Runtime
   * `xgboost`
   * `mlflow`
   * `scikit-learn`
   * `pandas`
   * `matplotlib`
   * `seaborn`

### Execution Order

Run notebooks in sequence:

```bash
# Step 1: Create training data
# Open 01_create_training_data.ipynb
# Run all cells (expected time: 2-3 minutes)
# Output: matchpulse.ml.training_match_states table created

# Step 2: Train model
# Open 02_train_win_probability.ipynb
# Run all cells (expected time: 1-2 minutes)
# Output: Model logged to MLflow

# Step 3: Validate model
# Open 03_validate_model.ipynb
# Run all cells (expected time: 30-60 seconds)
# Output: Confusion matrix, feature importance
```

### Quick Start Commands

```python
# Load trained model
import mlflow
mlflow.set_registry_uri("databricks-uc")

# Option 1: Load from MLflow run (recommended)
run_id = "16ea861d4c4d4b97975059f4adaf23bc"
model = mlflow.xgboost.load_model(f"runs:/{run_id}/model")

# Option 2: Load from UC (when registration succeeds)
# model = mlflow.xgboost.load_model("models:/matchpulse.ml.win_probability_model/latest")

# Make predictions
import pandas as pd
match_state = pd.DataFrame([{
    'minute': 45,
    'current_score_diff': 1,  # Home leading 1-0
    'home_xg_so_far': 1.2,
    'away_xg_so_far': 0.5,
    'home_shots': 8,
    'away_shots': 3,
    'home_red_cards': 0,
    'away_red_cards': 0,
    'home_form_pts': 0,
    'away_form_pts': 0
}])

probabilities = model.predict_proba(match_state)[0]
print(f"Home Win: {probabilities[0]:.1%}")
print(f"Draw: {probabilities[1]:.1%}")
print(f"Away Win: {probabilities[2]:.1%}")
```

---

## 🔧 Technical Details

### Model Architecture

**Algorithm:** XGBoost Multi-class Classifier
* **Type**: Gradient Boosted Trees
* **Objective**: `multi:softprob` (outputs probabilities)
* **Number of classes**: 3
* **Number of estimators**: 200 trees
* **Max depth**: 6
* **Learning rate**: 0.1

### Feature Engineering

**Temporal Features:**
* `minute` - Captures match progression (early/mid/late game dynamics)

**Score Features:**
* `current_score_diff` - Most important feature (home_score - away_score)

**Quality Metrics:**
* `home_xg_so_far`, `away_xg_so_far` - Cumulative expected goals (chance quality)

**Volume Metrics:**
* `home_shots`, `away_shots` - Cumulative shots (attacking pressure)

**Game-Changing Events:**
* `home_red_cards`, `away_red_cards` - Player dismissals (significant impact)

**Team Context:**
* `home_form_pts`, `away_form_pts` - Placeholder for team form (currently 0)

### Target Encoding

```python
target_mapping = {
    'home_win': 0,  # Home team wins
    'draw': 1,      # Match ends in draw
    'away_win': 2   # Away team wins
}
```

### Model Output

For each match state, the model outputs 3 probabilities:
* P(Home Win)
* P(Draw)
* P(Away Win)

Sum = 1.0

---


---

## 📈 Results & Insights

### Model Performance Summary

| Metric | Value | Interpretation |
|--------|-------|----------------|
| Test Accuracy | 72.64% | Good baseline for 3-class problem |
| Home Win Recall | 89.6% | Excellent - catches most home wins |
| Draw Recall | 45.8% | Hardest class (draws are unpredictable) |
| Away Win Recall | 68.4% | Solid performance |
| Top Feature Importance | 72% (score_diff) | Model learns sensible patterns |

### Key Insights

**1. Score Difference is King**
* `current_score_diff` accounts for **72% of feature importance**
* Model heavily weights current score when predicting outcome
* Validates football intuition - score is the best predictor

**2. Draws Are Hardest to Predict**
* Draw class has lowest recall (45.8%)
* Model often confuses draws with wins
* This is realistic - draws are inherently unpredictable in football

**3. xG Adds Value Beyond Shots**
* xG features contribute ~6% importance each
* Shot counts contribute ~5% each
* Quality of chances (xG) slightly more valuable than quantity (shots)

**5. Form Features Need Implementation**
* `home_form_pts` and `away_form_pts` have zero importance
* Currently placeholders (all 0)
* High-priority improvement for production

### Production Readiness

✅ **Ready for pilot deployment:**
* Accuracy > 70% for 3-class problem
* High recall for home wins (89.6%)
* Feature importance validates sensible learning
* Model behavior on famous match shows good intuition

⚠️ **Considerations before production:**
* Resolve UC registration permissions for model versioning
* Implement real team form features (`home_form_pts`, `away_form_pts`)
* Test on recent season data (2024-2025)
* Set up model monitoring and drift detection
* Add confidence thresholds for low-certainty predictions

---

## 🔮 Future Improvements

### Feature Engineering

**High Priority:**
1. **Team Form Metrics** (currently placeholder 0s)
   * Last 5 matches: wins, goals scored/conceded
   * Home/away form split
   * Head-to-head history

2. **Match Context**
   * Competition type (league, cup, derby)
   * Match importance (title race, relegation)
   * Venue (home advantage)

3. **Player-Level Features**
   * Key player absences
   * Starting XI quality ratings
   * Formation/tactics

**Medium Priority:**
4. **Advanced Metrics**
   * Possession percentage
   * Pass completion rate
   * Defensive actions
   * Set piece opportunities

5. **Momentum Indicators**
   * Recent 5-minute shot trends
   * xG trend (increasing/decreasing)
   * Pressure intensity

### Model Architecture

**Experiment with:**
1. **Ensemble Methods**
   * Combine XGBoost with LightGBM/CatBoost
   * Stack multiple model types

2. **Neural Networks**
   * LSTM for sequence modeling (minute-by-minute progression)
   * Attention mechanisms for feature importance

3. **Time-Series Specific**
   * State-space models for match flow
   * Recurrent architectures

### Validation Improvements

**Add More Famous Matches:**
* **Manchester United 3-3 Bayern Munich (1999)** - 2 injury time goals
* **Istanbul 2005** - Liverpool 3-3 AC Milan (3-0 down at halftime)
* **Barcelona 6-1 PSG (2017)** - Remontada comeback
* Show model behavior on dramatic finishes and collapses

### MLOps & Deployment

1. **Model Versioning**
   * Resolve UC permissions
   * Implement model registry with aliases (champion/challenger)

2. **Real-Time Serving**
   * Deploy to Databricks Model Serving endpoint
   * Set up REST API for live predictions
   * Optimize latency (<100ms response time)

3. **Monitoring**
   * Track prediction distribution drift
   * Monitor feature distributions
   * Alert on performance degradation

4. **Retraining Pipeline**
   * Automated weekly retraining on new data
   * A/B testing framework
   * Champion/challenger model comparison

### Data Quality

1. **Handle Missing Data**
   * Some matches may have incomplete event data
   * Imputation strategies for missing xG

2. **Validate Data Freshness**
   * Ensure events are ingested in real-time
   * Handle delayed data scenarios

3. **Expand Dataset**
   * Include more leagues/competitions
   * Add multiple seasons (2015-2025)
   * Increase sample size to 500K+

---

## 📁 File Structure

```
MatchPulse/03_ml/
├── README.md                          # This file
├── 01_create_training_data.ipynb      # Data preparation (ID: 644447621029057)
├── 02_train_win_probability.ipynb     # Model training (ID: 644447621029058)
└── 03_validate_model.ipynb            # Model validation (ID: 644447621029059)

Data Locations:
├── s3a://matchpulse-pawan/bronze/matches/     # Source: matches (Delta)
├── s3a://matchpulse-pawan/bronze/events/      # Source: events (Parquet)
└── matchpulse.ml.training_match_states        # Output: training table (Delta)

MLflow:
└── /Users/pawanvirat32@gmail.com/MatchPulse/win_probability_experiments
    └── Run: 16ea861d4c4d4b97975059f4adaf23bc
```

---

## 🤝 Contributing

When modifying these notebooks:

1. **Test on sample data first** - Don't run full pipeline on 8M events during development
2. **Document schema assumptions** - Note data formats (Delta vs Parquet)
3. **Handle errors gracefully** - Add try/except for UC operations
4. **Log everything** - Use MLflow for experiment tracking
5. **Validate results** - Check class distributions, feature ranges

---

## 📞 Support

**Issues?**
* Check "Known Issues" section above
* Verify S3 access to bronze layer
* Ensure Unity Catalog schema `matchpulse.ml` exists
* Review MLflow experiment logs

**Questions?**
* Model architecture: See XGBoost parameters in notebook 02
* Feature engineering: See notebook 01 for data transformations
* Validation approach: See notebook 03 for confusion matrix, feature importance, and trajectory plots

---

## 📄 License

Part of the MatchPulse project.

---

**Last Updated:** May 30, 2026  
**Model Version:** v1.0  
**MLflow Run:** 16ea861d4c4d4b97975059f4adaf23bc