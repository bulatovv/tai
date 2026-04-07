# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "numpy",
#   "pandas",
#   "scikit-learn",
#   "catboost",
#   "scipy",
#   "optuna",
#   "optuna-integration[catboost]",
# ]
# ///

import duckdb
import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn.model_selection import StratifiedGroupKFold
from catboost import CatBoostClassifier, Pool, EFeaturesSelectionAlgorithm, EShapCalcType
from sklearn.metrics import f1_score, precision_score, recall_score
from scipy.stats import pearsonr
import sys
import os
from pathlib import Path
import warnings
from collections import Counter, defaultdict
import optuna
from optuna_integration import CatBoostPruningCallback

# Suppress warnings
warnings.filterwarnings("ignore")

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.tai.world_tagging.solver import InferenceEngine
from src.tai.world_tagging.rules import rules_def

# Constants
DB_SESSIONS = 'data/world_sessions.db'
DB_ONLINE = 'data/worlds_online.db'
DB_GLOBAL = 'data/online.db'
TARGET_PLAYERS_THRESHOLD = 10
RANDOM_SEED = 42

ALL_TAGS = [
    'rp', 'default', 'by_invite', 'open', 'in_dev', 'dm', 'tdm', 'duel', 'ctf', 'nsfw', 
    'zombie', 'copchase', 'race', 'derby', 'minigames', 'stunt', 'spleef', 'clicker', 
    'warsim', 'tanks', 'ww2', 'svo', 'middle_east', 'cops_robbers', 'cops_vs_crime', 
    'larp', 'chicago', 'ghetto', 'city_rp', 'new_jersey', 'gangwars', 'sa_like', 
    'kartel', 'robbing_sam', 'drugs_bombs', 'county_rp', 'prison', 'post_apo', 'fnaf', 
    'russia', 'save_president', 'air', 'murder_mystery', 'college_rp', 'beta', 'camp_rp', 
    'farm', 'horror', 'thematic_rp', 'rpg', 'static_ads', 'xviwar', 'movie', 'anarchy', 
    'bum', 'party', 'personal_world'
]

def load_and_prep_data():
    print("Loading data...")
    con = duckdb.connect(DB_SESSIONS)
    con.execute(f"ATTACH '{DB_ONLINE}' AS wo")
    con.execute(f"ATTACH '{DB_GLOBAL}' AS gl")

    # 1. Load Sessions (Target sessions)
    sessions_query = """
    SELECT
        name,
        session_start,
        session_end,
        (session_end - session_start) as duration
    FROM world_sessions
    WHERE duration > INTERVAL 1 minute
    ORDER BY session_start
    """
    sessions_df = con.execute(sessions_query).df()
    
    # 2. Load Snapshots (Time series for all worlds)
    snapshots_query = """
    SELECT
        name,
        saved_at,
        players
    FROM wo.worlds_online
    ORDER BY saved_at
    """
    snapshots_df = con.execute(snapshots_query).df()
    
    # 3. Load Global Stats (Total online)
    global_query = """
    SELECT
        queried_at as saved_at,
        online_count as global_players
    FROM gl.online
    ORDER BY saved_at
    """
    global_df = con.execute(global_query).df()
    
    con.close()
    
    # Pre-calculate Tags
    print("Generating tags...")
    engine = InferenceEngine(rules_def)
    unique_names = snapshots_df['name'].unique()
    name_to_tags = {name: list(engine.solve(name)) for name in unique_names}
    
    # --- Prepare Global Time Series (Aligned) ---
    print("Aligning global time series...")
    # 1. Aggregate world players by minute
    snapshots_df['saved_at_min'] = snapshots_df['saved_at'].dt.floor('min')
    total_track_df = snapshots_df.groupby('saved_at_min')['players'].sum().reset_index()
    total_track_df = total_track_df.rename(columns={'saved_at_min': 'time', 'players': 'total_tracked'})
    
    # 2. Prepare global df
    global_df['time'] = global_df['saved_at'].dt.floor('min')
    global_ts = global_df.groupby('time')['global_players'].mean().reset_index() # handle dups if any
    
    # 3. Merge and calculate Shadow
    merged_ts = pd.merge(global_ts, total_track_df, on='time', how='outer').sort_values('time')
    merged_ts = merged_ts.set_index('time').resample('1min').interpolate(method='time').reset_index()
    merged_ts['shadow_players'] = (merged_ts['global_players'] - merged_ts['total_tracked']).clip(lower=0)
    
    # Index snapshots by name for fast lookup
    snapshots_by_name = {name: df.sort_values('saved_at') for name, df in snapshots_df.groupby('name')}
    
    return sessions_df, snapshots_by_name, merged_ts, name_to_tags

def calculate_slope(times, values):
    if len(times) < 2:
        val = values.iloc[0] if len(values) > 0 else 0.0
        return 0.0, val
    try:
        t_norm = (times - times.iloc[0]).dt.total_seconds() / 60.0
        m, c = np.polyfit(t_norm, values, 1)
        return m, c
    except Exception:
        return 0.0, 0.0

def calculate_accel(times, values):
    if len(times) < 3:
        return 0.0
    try:
        t_norm = (times - times.iloc[0]).dt.total_seconds() / 60.0
        a, b, c = np.polyfit(t_norm, values, 2)
        return 2 * a
    except Exception:
        return 0.0

def extract_features_for_session(
    target_row, 
    snapshots_by_name, 
    global_ts, 
    name_to_tags, 
    sessions_df, 
    window_minutes,
    model_name
):
    start_time = target_row['session_start']
    end_time = target_row['session_end']
    name = target_row['name']
    
    obs_end = start_time + timedelta(minutes=window_minutes)
    
    # --- Target Label (Y) ---
    target_snaps = snapshots_by_name.get(name, pd.DataFrame(columns=['saved_at', 'players']))
    if target_snaps.empty:
        return None, None

    future_mask = (target_snaps['saved_at'] >= obs_end) & (target_snaps['saved_at'] <= end_time)
    future_snaps = target_snaps[future_mask]
    y = 1 if not future_snaps.empty and future_snaps['players'].max() >= TARGET_PLAYERS_THRESHOLD else 0

    # --- Features (X) ---
    features = {}
    
    # I. Global Data Features
    # Slice global_ts for the window [start_time, obs_end]
    # We need enough history for trends, let's take window_minutes or at least 15 mins lookback if window is 0
    lookback = max(window_minutes, 15)
    hist_start = obs_end - timedelta(minutes=lookback)
    
    g_hist = global_ts[(global_ts['time'] >= hist_start) & (global_ts['time'] <= obs_end)]
    
    if not g_hist.empty:
        # Global Players
        features['global_players_last'] = g_hist['global_players'].iloc[-1]
        features['global_players_max'] = g_hist['global_players'].max()
        features['global_players_mean'] = g_hist['global_players'].mean()
        features['global_players_std'] = g_hist['global_players'].std()
        gm, gc = calculate_slope(g_hist['time'], g_hist['global_players'])
        features['global_players_trend_slope'] = gm
        features['global_players_trend_intercept'] = gc
        features['global_players_accel'] = calculate_accel(g_hist['time'], g_hist['global_players'])
        
        # Shadow Players
        features['shadow_players_last'] = g_hist['shadow_players'].iloc[-1]
        features['shadow_players_max'] = g_hist['shadow_players'].max()
        features['shadow_players_mean'] = g_hist['shadow_players'].mean()
        features['shadow_players_std'] = g_hist['shadow_players'].std()
        sm, _ = calculate_slope(g_hist['time'], g_hist['shadow_players'])
        features['shadow_players_trend_slope'] = sm
    else:
        # Defaults if missing global data
        for k in ['global_players_last', 'global_players_max', 'global_players_mean', 'global_players_std', 
                  'global_players_trend_slope', 'global_players_trend_intercept', 'global_players_accel',
                  'shadow_players_last', 'shadow_players_max', 'shadow_players_mean', 'shadow_players_std', 
                  'shadow_players_trend_slope']:
            features[k] = 0.0

    # Time context
    features['hour_sin'] = np.sin(2 * np.pi * obs_end.hour / 24)
    features['hour_cos'] = np.cos(2 * np.pi * obs_end.hour / 24)
    dow = obs_end.strftime('%A').lower()
    features['day_of_week_cat'] = dow if dow in ['friday', 'saturday', 'sunday'] else 'other'

    # II. Target Session Features
    obs_mask = (target_snaps['saved_at'] >= start_time) & (target_snaps['saved_at'] <= obs_end)
    obs_data = target_snaps[obs_mask]
    
    target_players_last = 0
    if model_name not in ["none_model", "0m_model"]:
        if not obs_data.empty:
            target_players_last = obs_data['players'].iloc[-1]
            features['target_players_last'] = target_players_last
            features['target_players_max'] = obs_data['players'].max()
            features['target_players_mean'] = obs_data['players'].mean()
            features['target_players_std'] = obs_data['players'].std()
            tm, tc = calculate_slope(obs_data['saved_at'], obs_data['players'])
            features['target_players_trend_slope'] = tm
            features['target_players_trend_intercept'] = tc
            features['target_players_accel'] = calculate_accel(obs_data['saved_at'], obs_data['players'])
        else:
            features.update({'target_players_last': 0, 'target_players_max': 0, 'target_players_mean': 0, 
                             'target_players_std': 0, 'target_players_trend_slope': 0, 
                             'target_players_trend_intercept': 0, 'target_players_accel': 0})
    
    my_tags = name_to_tags.get(name, [])
    if model_name != "none_model":
        features['target_session_tags_text'] = " ".join(my_tags)

    # III. Concurrent Session Features
    cand_mask = (sessions_df['session_start'] < obs_end) & (sessions_df['session_end'] > start_time) & (sessions_df['name'] != name)
    concurrent_cands = sessions_df[cand_mask]
    
    conc_stats = {
        'count': 0, 'players_last_sum': 0, 
        'bleeding_count': 0, 'bleeding_sum': 0, 'bleeding_slope_sum': 0,
        'growing_count': 0, 'growing_sum': 0, 'growing_slope_sum': 0,
        'stagnating_count': 0,
        'niche_comp_count': 0, 'niche_comp_sum': 0,
        'tags': [], 'tag_player_sums': defaultdict(float)
    }
    
    for _, c_row in concurrent_cands.iterrows():
        c_name = c_row['name']
        c_snaps = snapshots_by_name.get(c_name, pd.DataFrame())
        # History up to obs_end
        c_hist = c_snaps[(c_snaps['saved_at'] <= obs_end) & (c_snaps['saved_at'] >= c_row['session_start'])]
        if c_hist.empty: continue
        
        last_p = c_hist['players'].iloc[-1]
        conc_stats['count'] += 1
        conc_stats['players_last_sum'] += last_p
        
        c_tags = name_to_tags.get(c_name, [])
        conc_stats['tags'].extend(c_tags)
        for t in c_tags:
            conc_stats['tag_player_sums'][t] += last_p
            
        if set(c_tags) & set(my_tags):
            conc_stats['niche_comp_count'] += 1
            conc_stats['niche_comp_sum'] += last_p
        
        # Slope calc for category
        if len(c_hist) >= 2:
            m, _ = calculate_slope(c_hist['saved_at'], c_hist['players'])
            if m < -0.1:
                conc_stats['bleeding_count'] += 1
                conc_stats['bleeding_sum'] += last_p
                conc_stats['bleeding_slope_sum'] += m
            elif m > 0.1:
                conc_stats['growing_count'] += 1
                conc_stats['growing_sum'] += last_p
                conc_stats['growing_slope_sum'] += m
            else:
                conc_stats['stagnating_count'] += 1
        else:
            conc_stats['stagnating_count'] += 1

    features['concurrent_sessions_count'] = conc_stats['count']
    features['concurrent_players_last_sum'] = conc_stats['players_last_sum']
    features['concurrent_tags_union_text'] = " ".join(conc_stats['tags'])
    
    features['concurrent_bleeding_player_sum'] = conc_stats['bleeding_sum']
    features['concurrent_bleeding_count'] = conc_stats['bleeding_count']
    features['concurrent_bleeding_slope_sum'] = conc_stats['bleeding_slope_sum']
    
    features['concurrent_growing_player_sum'] = conc_stats['growing_sum']
    features['concurrent_growing_count'] = conc_stats['growing_count']
    features['concurrent_growing_slope_sum'] = conc_stats['growing_slope_sum']
    
    features['concurrent_stagnating_count'] = conc_stats['stagnating_count']
    
    features['niche_competitor_count'] = conc_stats['niche_comp_count']
    features['niche_competitor_player_last_sum'] = conc_stats['niche_comp_sum']
    features['niche_dominance_ratio'] = (target_players_last / conc_stats['niche_comp_sum']) if conc_stats['niche_comp_sum'] > 0 else 0
    
    for tag in ALL_TAGS:
        features[f'concurrent_tag_{tag}_player_sum'] = conc_stats['tag_player_sums'].get(tag, 0)

    # IV. Recently Closed Session Features
    lookback_start = start_time - timedelta(hours=3)
    closed_cands = sessions_df[(sessions_df['session_end'] >= lookback_start) & (sessions_df['session_end'] <= obs_end) & (sessions_df['name'] != name)]
    
    closed_stats = {
        'count': 0, 'last_sum': 0, 'niche_sum': 0,
        'tags': [], 'tag_last_sums': defaultdict(float)
    }
    
    for _, cl_row in closed_cands.iterrows():
        cl_snaps = snapshots_by_name.get(cl_row['name'], pd.DataFrame())
        cl_hist = cl_snaps[cl_snaps['saved_at'] <= obs_end]
        if cl_hist.empty: continue
        
        last_p = cl_hist['players'].iloc[-1]
        closed_stats['count'] += 1
        closed_stats['last_sum'] += last_p
        
        cl_tags = name_to_tags.get(cl_row['name'], [])
        closed_stats['tags'].extend(cl_tags)
        for t in cl_tags:
            closed_stats['tag_last_sums'][t] += last_p
            
        if set(cl_tags) & set(my_tags):
            closed_stats['niche_sum'] += last_p

    features['closed_sessions_count'] = closed_stats['count']
    features['closed_players_last_sum'] = closed_stats['last_sum']
    features['niche_closed_player_last_sum'] = closed_stats['niche_sum']
    features['closed_tags_union_text'] = " ".join(closed_stats['tags'])
    
    for tag in ALL_TAGS:
        features[f'closed_tag_{tag}_last_sum'] = closed_stats['tag_last_sums'].get(tag, 0)

    return features, y

def build_dataset(sessions_df, snapshots_by_name, global_ts, name_to_tags, window_minutes, model_name):
    print(f"Building dataset for {model_name}...")
    X_list, y_list, groups_list = [], [], []
    for i, row in sessions_df.iterrows():
        feats, y = extract_features_for_session(
            row, snapshots_by_name, global_ts, name_to_tags, sessions_df, window_minutes, model_name
        )
        if feats is None: continue
        X_list.append(feats)
        y_list.append(y)
        # Logical Day Grouping: 5:00 to 5:00
        logical_day = (row['session_start'] - timedelta(hours=5)).date()
        groups_list.append(str(logical_day))
    
    if not X_list:
        return pd.DataFrame(), np.array([]), np.array([])
        
    return pd.DataFrame(X_list).fillna(0), np.array(y_list), np.array(groups_list)

def detect_direction(feature_values, shap_values):
    # If std is 0, correlation is NaN/undefined.
    if np.std(feature_values) == 0 or np.std(shap_values) == 0:
        return 0, '0'
    
    r, _ = pearsonr(feature_values, shap_values)
    if r > 0.1:
        return r, '+'
    elif r < -0.1:
        return r, '-'
    else:
        # Check if importance is high (complex) or low (neutral)
        # We need context for "High". For now, if correlation is low, we check magnitude.
        # But per instruction: "Complex: Correlation between -0.1 and 0.1, but High Importance."
        # We'll define "High Importance" later relative to other features. 
        # For the raw function, just return '0' or '~' based on some heuristic or just return '0' for now and refine in aggregation.
        return r, '0' 

# Empirically, CatBoost defaults are strong on this dataset -- tuning gave +0.023 F1 for 10m_model
# but was neutral or slightly negative for all others (15m default F1=0.529 remained best overall).
# l2_leaf_reg pattern: weak models (none/0m) prefer high regularization (15+), strong models (10m/15m)
# prefer low (3-5), suggesting less regularization is needed when features are more informative.
def tune_hyperparams(model_name, X, y, groups, surviving_features, baseline_f1, baseline_prec, baseline_rec):
    print(f"Tuning hyperparameters for {model_name} on {len(surviving_features)} features...")

    X_sel = X[surviving_features]
    sel_cat = [c for c in X_sel.columns if '_cat' in c]
    sel_text = [c for c in X_sel.columns if 'text' in c]

    def make_pool(X_, y_):
        return Pool(X_, y_, cat_features=sel_cat, text_features=sel_text)

    # Fixed validation fold for Optuna objective (enables per-iteration pruning)
    sgkf = StratifiedGroupKFold(n_splits=5)
    tr_idx, val_idx = next(iter(sgkf.split(X_sel, y, groups=groups)))
    train_pool = make_pool(X_sel.iloc[tr_idx], y[tr_idx])
    val_pool   = make_pool(X_sel.iloc[val_idx], y[val_idx])
    y_val = y[val_idx]

    base_params = dict(
        loss_function='Focal:focal_alpha=0.98;focal_gamma=2.0',
        eval_metric='F1',
        random_seed=RANDOM_SEED,
        allow_writing_files=False,
        early_stopping_rounds=50,
        iterations=1000,
        verbose=False,
        cat_features=sel_cat,
        text_features=sel_text,
        tokenizers=[{"tokenizer_id": "Space", "separator_type": "ByDelimiter", "delimiter": " "}],
        dictionaries=[{
            "dictionary_id": "Unigrams",
            "max_dictionary_size": str(len(ALL_TAGS)),
            "occurrence_lower_bound": "1",
            "gram_order": "1"
        }],
        feature_calcers=["BoW"],
    )

    def objective(trial):
        params = {
            **base_params,
            'depth':         trial.suggest_int('depth', 4, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
            'l2_leaf_reg':   trial.suggest_float('l2_leaf_reg', 1, 20, log=True),
        }
        pruning_callback = CatBoostPruningCallback(trial, 'F1')
        model = CatBoostClassifier(**params)
        model.fit(train_pool, eval_set=val_pool, callbacks=[pruning_callback])
        pruning_callback.check_pruned()
        preds = model.predict(val_pool)
        return f1_score(y_val, preds)

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        pruner=optuna.pruners.MedianPruner(n_warmup_steps=5),
        direction='maximize',
    )
    study.optimize(objective, n_trials=50)

    best_params = study.best_params
    print(f"  Best params: {best_params}")

    # Full CV with best params to get an honest estimate
    print(f"  Running full CV with best params...")
    full_sgkf = StratifiedGroupKFold(n_splits=8)
    f1_scores, prec_scores, rec_scores = [], [], []
    for tr_idx, val_idx in full_sgkf.split(X_sel, y, groups=groups):
        m = CatBoostClassifier(**{**base_params, **best_params})
        m.fit(make_pool(X_sel.iloc[tr_idx], y[tr_idx]), eval_set=make_pool(X_sel.iloc[val_idx], y[val_idx]))
        preds = m.predict(make_pool(X_sel.iloc[val_idx], y[val_idx]))
        f1_scores.append(f1_score(y[val_idx], preds))
        prec_scores.append(precision_score(y[val_idx], preds, zero_division=0))
        rec_scores.append(recall_score(y[val_idx], preds, zero_division=0))

    tuned_f1  = np.mean(f1_scores)
    tuned_prec = np.mean(prec_scores)
    tuned_rec  = np.mean(rec_scores)
    delta = tuned_f1 - baseline_f1
    delta_str = f"+{delta:.4f}" if delta >= 0 else f"{delta:.4f}"
    print(f"  Tuned F1: {tuned_f1:.4f} ({delta_str} vs default)")

    n_pruned = len([t for t in study.trials if t.state == optuna.trial.TrialState.PRUNED])
    report_lines = [
        f"Hyperparameter search ({len(study.trials)} trials, {n_pruned} pruned)  |  "
        f"F1: {tuned_f1:.4f} ({delta_str})  Precision: {tuned_prec:.4f}  Recall: {tuned_rec:.4f}",
        f"Best params: {best_params}",
        "",
        "=" * 60,
        "",
    ]
    with open("optimization_results.txt", "a") as f:
        f.write("\n".join(report_lines) + "\n")

    return best_params


def run_optimization(model_name, X, y, groups):
    print(f"Running optimization loop for {model_name}...")
    
    text_features = [c for c in X.columns if 'text' in c]
    cat_features = [c for c in X.columns if '_cat' in c]
    
    n_folds = 8
    sgkf = StratifiedGroupKFold(n_splits=n_folds)
    
    feature_survival_count = Counter()
    feature_shap_sum = defaultdict(float)
    feature_directions = defaultdict(list)
    
    f1_scores = []
    precisions = []
    recalls = []
    
    fold = 0
    for tr_idx, val_idx in sgkf.split(X, y, groups=groups):
        fold += 1
        print(f"  Fold {fold}/{n_folds}...")
        
        # Fresh Pools
        X_train, y_train = X.iloc[tr_idx], y[tr_idx]
        X_val, y_val = X.iloc[val_idx], y[val_idx]
        
        train_pool = Pool(X_train, y_train, cat_features=cat_features, text_features=text_features)
        val_pool = Pool(X_val, y_val, cat_features=cat_features, text_features=text_features)
        
        # 1. Train Initial Model (Default Params)
        model = CatBoostClassifier(
            iterations=1000, 
            verbose=False, 
            random_seed=RANDOM_SEED,
            allow_writing_files=False,
            early_stopping_rounds=50,
            eval_metric='F1',
            loss_function='Focal:focal_alpha=0.98;focal_gamma=2.0',
            cat_features=cat_features,
            text_features=text_features,
            tokenizers = [
                {"tokenizer_id": "Space", "separator_type": "ByDelimiter", "delimiter": " "}
            ],
            dictionaries = [
                {
                    "dictionary_id": "Unigrams",
                    "max_dictionary_size": str(len(ALL_TAGS)),
                    "occurrence_lower_bound": "1",
                    "gram_order": "1"
                }
            ],
            feature_calcers = [
                "BoW",
                #"NaiveBayes"
            ]
        )
        
        # 2. Feature Selection (Shapley Recursive)
        # Eliminate bottom 20% recursively. 
        # We will trust CatBoost's select_features to find a good subset.
        # We set num_features_to_select to 50% to force elimination, but then we look at the summary?
        # Actually, simpler manual approach per fold:
        # Fit -> Get Feature Importance -> Drop bottom 20% -> Refit -> Check F1.
        # Just do one pass of elimination for simplicity and speed as "Recursive" implies RFE.
        # Using built-in select_features.
        
        summary = model.select_features(
            train_pool,
            eval_set=val_pool,
            features_for_select=list(range(X.shape[1])),
            num_features_to_select=int(X.shape[1] * 0.5), # Try to reduce by half
            algorithm=EFeaturesSelectionAlgorithm.RecursiveByShapValues,
            steps=3,
            train_final_model=True,
            plot=False,
            verbose=False
        )
        
        selected_features = summary['selected_features_names']
        
        # Update survival count
        for f in selected_features:
            feature_survival_count[f] += 1
            
        # 3. SHAP Analysis on Validation Set (using the trained model on selected features)
        # Note: model is now trained on selected features?
        # CatBoost's select_features returns a summary. It doesn't replace the 'model' object in-place with the reduced one usually,
        # but `train_final_model=True` should return a model trained on selected features.
        # Wait, `select_features` returns a dict. It doesn't return the model.
        # We need to retrain on selected features.
        
        final_model = CatBoostClassifier(
            iterations=1000, 
            verbose=False, 
            random_seed=RANDOM_SEED,
            allow_writing_files=False,
            early_stopping_rounds=50,
            eval_metric='F1',
            loss_function='Focal:focal_alpha=0.98;focal_gamma=2.0',
            cat_features=[c for c in cat_features if c in selected_features],
            text_features=[c for c in text_features if c in selected_features],
            tokenizers = [
                {"tokenizer_id": "Space", "separator_type": "ByDelimiter", "delimiter": " "}
            ],
            dictionaries = [
                {
                    "dictionary_id": "Unigrams",
                    "max_dictionary_size": str(len(ALL_TAGS)),
                    "occurrence_lower_bound": "1",
                    "gram_order": "1"
                }
            ],
            feature_calcers = [
                "BoW",
                #"NaiveBayes"
            ]
        )
        
        # Filter columns
        train_pool_sel = Pool(X_train[selected_features], y_train, 
                              cat_features=[c for c in cat_features if c in selected_features],
                              text_features=[c for c in text_features if c in selected_features])
        val_pool_sel = Pool(X_val[selected_features], y_val,
                            cat_features=[c for c in cat_features if c in selected_features],
                            text_features=[c for c in text_features if c in selected_features])
        
        final_model.fit(train_pool_sel, eval_set=val_pool_sel)
        
        # Score
        preds = final_model.predict(val_pool_sel)
        score = f1_score(y_val, preds)
        prec = precision_score(y_val, preds, zero_division=0)
        rec = recall_score(y_val, preds, zero_division=0)
        
        f1_scores.append(score)
        precisions.append(prec)
        recalls.append(rec)
        
        # SHAP
        shap_values = final_model.get_feature_importance(val_pool_sel, type="ShapValues")
        # shap_values shape: (n_samples, n_features + 1). The last column is bias.
        shap_values = shap_values[:, :-1]
        
        for idx, col_name in enumerate(selected_features):
            vals = X_val[col_name].values
            # If text/cat feature, we might not get simple numeric correlations easily without encoding.
            # CatBoost SHAP for text/cat is complex. For simplicity, we skip direction calc for text/cat features 
            # or try to cast if possible. Text features are not numeric.
            if col_name in text_features or col_name in cat_features:
                 # Just record importance magnitude
                 mean_abs_shap = np.mean(np.abs(shap_values[:, idx]))
                 feature_shap_sum[col_name] += mean_abs_shap
                 continue

            s_vals = shap_values[:, idx]
            mean_abs_shap = np.mean(np.abs(s_vals))
            feature_shap_sum[col_name] += mean_abs_shap
            
            r, direct = detect_direction(vals, s_vals)
            
            # Post-hoc "Complex" check: if '0' but mean_abs_shap is high (we can't know global high yet).
            # Store raw r for now.
            feature_directions[col_name].append(r)

    # Aggregation
    avg_f1 = np.mean(f1_scores)
    avg_prec = np.mean(precisions)
    avg_rec = np.mean(recalls)
    
    # Voting Logic: Survived in >= 5 folds
    survival_threshold = 5
    final_survivors = [f for f, count in feature_survival_count.items() if count >= survival_threshold]
    
    # Sort survivors by importance
    survivor_stats = []
    for f in final_survivors:
        count = feature_survival_count[f]
        importance = feature_shap_sum[f] / n_folds

        dirs = feature_directions[f]
        if not dirs:
            direction_str = "N/A"
        else:
            avg_r = np.mean(dirs)
            if avg_r > 0.1: direction_str = "+"
            elif avg_r < -0.1: direction_str = "-"
            else:
                direction_str = "~" if importance > 0.1 else "0"

        survivor_stats.append((f, count, importance, direction_str))

    survivor_stats.sort(key=lambda x: x[2], reverse=True)
    meaningful = [(f, c, imp, d) for f, c, imp, d in survivor_stats if imp > 0.0001]
    noise_count = len(survivor_stats) - len(meaningful)

    report_lines = [
        f"## {model_name}",
        f"Default params  |  F1: {avg_f1:.4f}  Precision: {avg_prec:.4f}  Recall: {avg_rec:.4f}",
        f"",
        f"Top features (importance > 0.0001, survived >= {survival_threshold}/{n_folds} folds):",
        f"{'Feature':<45} {'Surv':>4}  {'Importance':>10}  Dir",
        f"{'-'*45} {'-'*4}  {'-'*10}  ---",
    ]
    for f, c, imp, d in meaningful:
        report_lines.append(f"{f:<45} {c:>4}  {imp:>10.4f}  {d}")
    if noise_count:
        report_lines.append(f"  ... +{noise_count} near-zero features omitted")

    report_lines.append("")

    with open("optimization_results.txt", "a") as f:
        f.write("\n".join(report_lines) + "\n")

    return final_survivors, avg_f1, avg_prec, avg_rec


def main():
    if os.path.exists("optimization_results.txt"): os.remove("optimization_results.txt")
    
    sessions, snapshots, global_ts, tags = load_and_prep_data()
    
    scenarios = [
        (0, "none_model"),
        (0, "0m_model"),
        (5, "5m_model"),
        (10, "10m_model"),
        (15, "15m_model")
    ]
    
    for win, name in scenarios:
        X, y, groups = build_dataset(sessions, snapshots, global_ts, tags, win, name)
        if X.empty:
            print(f"Skipping {name} (Empty dataset)")
            continue
        model_name = f"{name}_optimized"
        survivors, avg_f1, avg_prec, avg_rec = run_optimization(model_name, X, y, groups)
        if survivors:
            tune_hyperparams(model_name, X, y, groups, survivors, avg_f1, avg_prec, avg_rec)

if __name__ == "__main__":
    main()
