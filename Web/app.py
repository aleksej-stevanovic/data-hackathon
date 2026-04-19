from flask import Flask, jsonify, request, render_template
import duckdb
import os
import re
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=BASE_DIR)
app.config['TEMPLATES_AUTO_RELOAD'] = True

PARQUET = os.path.abspath(os.path.join(BASE_DIR, '..', 'all_foursquare_locations.parquet'))
SAFE_PATTERN = re.compile(r'^[\w\s&,\-]+$')

SYNERGY_MAP = {
    'Office': ['Cafe', 'Dining and Drinking', 'Travel and Transportation', 'Financial'],
    'Coworking': ['Cafe', 'Dining and Drinking', 'Technology', 'Retail'],
    'Cafe': ['Office', 'Education', 'Retail', 'Health and Medicine'],
    'Dining and Drinking': ['Arts and Entertainment', 'Retail', 'Travel and Transportation', 'Office'],
    'Travel and Transportation': ['Retail', 'Dining and Drinking', 'Financial'],
    'Education': ['Cafe', 'Arts and Entertainment', 'Retail', 'Travel and Transportation'],
    'Financial': ['Office', 'Retail', 'Dining and Drinking'],
    'Health and Medicine': ['Retail', 'Office', 'Travel and Transportation', 'Pharmacy'],
    'Retail': ['Dining and Drinking', 'Arts and Entertainment', 'Travel and Transportation'],
    'Arts and Entertainment': ['Dining and Drinking', 'Travel and Transportation', 'Retail'],
    'Technology': ['Office', 'Coworking', 'Education', 'Cafe']
}

# Global Configuration for Result Limits
# Change these numbers here, and both the API and frontend will automatically update!
DEFAULT_LIMIT = 1000000
MAX_LIMIT = 1000000


def run_query(sql, params=None):
    con = duckdb.connect()
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@app.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    return response


@app.route('/')
def index():
    return render_template('index.html', default_limit=DEFAULT_LIMIT, max_limit=MAX_LIMIT)


@app.route('/api/countries')
def countries():
    rows = run_query(f"""
        SELECT country, COUNT(*) AS cnt, AVG(latitude) AS lat, AVG(longitude) AS lng
        FROM '{PARQUET}'
        WHERE country IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY country
        ORDER BY cnt DESC
        LIMIT 250
    """)
    return jsonify([{'country': r[0], 'count': r[1], 'lat': r[2], 'lng': r[3]} for r in rows])


@app.route('/api/states')
def states():
    country = request.args.get('country', '').strip().upper()
    if not country:
        return jsonify([])

    rows = run_query(f"""
        SELECT UPPER(region) AS reg, COUNT(*) AS cnt, AVG(latitude) AS lat, AVG(longitude) AS lng
        FROM '{PARQUET}'
        WHERE region IS NOT NULL
          AND UPPER(country) = ?
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY UPPER(region)
        ORDER BY cnt DESC
        LIMIT 100
    """, [country])
    return jsonify([{'state': r[0], 'count': r[1], 'lat': r[2], 'lng': r[3]} for r in rows])


@app.route('/api/locations')
def locations():
    country = request.args.get('country', '').strip().upper()
    state = request.args.get('state', '').strip().upper()
    category = request.args.get('category', '').strip()
    open_only = request.args.get('open_only', 'false') == 'true'
    limit = min(request.args.get('limit', DEFAULT_LIMIT, type=int), MAX_LIMIT)

    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)

    if category and not SAFE_PATTERN.match(category):
        return jsonify({'error': 'Invalid category'}), 400

    conds = ['latitude IS NOT NULL', 'longitude IS NOT NULL', 'name IS NOT NULL']
    params = []

    if country:
        conds.append('UPPER(country) = ?')
        params.append(country)
    if state:
        conds.append('UPPER(region) = ?')
        params.append(state)
    if category:
        conds.append("array_to_string(fsq_category_labels, '|') ILIKE ?")
        params.append(f'%{category}%')
    if min_lat is not None:
        conds.append('latitude >= ?'); params.append(min_lat)
    if max_lat is not None:
        conds.append('latitude <= ?'); params.append(max_lat)
    if min_lng is not None:
        conds.append('longitude >= ?'); params.append(min_lng)
    if max_lng is not None:
        conds.append('longitude <= ?'); params.append(max_lng)
    if open_only:
        conds.append('date_closed IS NULL')

    where = ' AND '.join(conds)

    rows = run_query(f"""
        SELECT name, latitude, longitude, address, locality, region, postcode,
               tel, website, email,
               array_to_string(fsq_category_labels, ' | ') AS categories,
               date_closed
        FROM '{PARQUET}'
        WHERE {where}
        LIMIT {limit}
    """, params)

    cols = ['name', 'lat', 'lng', 'address', 'locality', 'region', 'postcode',
            'tel', 'website', 'email', 'categories', 'date_closed']

    return jsonify([
        {k: v for k, v in zip(cols, r) if v is not None}
        for r in rows
    ])



@app.route('/api/category-breakdown')
def category_breakdown():
    country = request.args.get('country', '').strip().upper()
    state = request.args.get('state', '').strip().upper()
    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)

    conds = ['latitude IS NOT NULL', 'longitude IS NOT NULL', 'fsq_category_labels IS NOT NULL']
    params = []

    if country:
        conds.append('UPPER(country) = ?')
        params.append(country)
    if state:
        conds.append('UPPER(region) = ?'); params.append(state)
    if min_lat is not None:
        conds.append('latitude >= ?'); params.append(min_lat)
    if max_lat is not None:
        conds.append('latitude <= ?'); params.append(max_lat)
    if min_lng is not None:
        conds.append('longitude >= ?'); params.append(min_lng)
    if max_lng is not None:
        conds.append('longitude <= ?'); params.append(max_lng)

    where = ' AND '.join(conds)

    rows = run_query(f"""
        SELECT split_part(label, ' > ', 1) AS top_cat, COUNT(*) AS cnt
        FROM (
            SELECT unnest(fsq_category_labels) AS label
            FROM '{PARQUET}'
            WHERE {where}
            LIMIT 100000
        ) sub
        WHERE label IS NOT NULL
        GROUP BY top_cat
        ORDER BY cnt DESC
        LIMIT 12
    """, params)

    return jsonify([{'category': r[0], 'count': r[1]} for r in rows])


@app.route('/api/recommend')
def recommend():
    category = request.args.get('category', '').strip()
    mode = request.args.get('mode', 'smart').strip().lower()

    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)

    if not category or category not in SYNERGY_MAP:
        return jsonify({'error': 'Please select a specific category for recommendations.'}), 400

    synergies = SYNERGY_MAP[category]
    
    conds = ['latitude IS NOT NULL', 'longitude IS NOT NULL', 'fsq_category_labels IS NOT NULL']
    params = []

    if min_lat is not None:
        conds.append('latitude >= ?'); params.append(min_lat)
    if max_lat is not None:
        conds.append('latitude <= ?'); params.append(max_lat)
    if min_lng is not None:
        conds.append('longitude >= ?'); params.append(min_lng)
    if max_lng is not None:
        conds.append('longitude <= ?'); params.append(max_lng)

    where = ' AND '.join(conds)

    syn_conds = " OR ".join(["array_to_string(fsq_category_labels, '|') ILIKE ?" for _ in synergies])
    syn_params = [f"%{s}%" for s in synergies]

    target_cond = "array_to_string(fsq_category_labels, '|') ILIKE ?"
    target_param = f"%{category}%"

    # Parameters must exactly match the order of '?' placeholders in the SQL string
    all_params = syn_params + [target_param] + params

    # Mode Scoring logic
    if mode == 'opportunity':
        # Strong negative competition, ratio helps find highly underserved pockets
        score_expr = "((synergy::FLOAT / activity) * 100.0) - (target_density * 20.0)"
    elif mode == 'competitive':
        # Light negative competition, very high demand, high synergy
        score_expr = "(activity * 1.0) + (synergy * 1.5) - (target_density * 1.0)"
    else:
        # Smart (default): Medium negative competition, high demand, medium-high synergy
        score_expr = "(activity * 0.5) + (synergy * 1.2) - (target_density * 4.0)"

    # DuckDB CTE for splitting the map into 1km (0.01 deg) grid cells
    sql = f"""
        WITH cell_stats AS (
            SELECT 
                FLOOR(latitude / 0.01) * 0.01 + 0.005 AS center_lat,
                FLOOR(longitude / 0.01) * 0.01 + 0.005 AS center_lng,
                COUNT(*) AS activity,
                SUM(CASE WHEN {syn_conds} THEN 1 ELSE 0 END) AS synergy,
                SUM(CASE WHEN {target_cond} THEN 1 ELSE 0 END) AS target_density,
                MODE(locality) AS suburb
            FROM '{PARQUET}'
            WHERE {where}
            GROUP BY 1, 2
        )
        SELECT center_lat, center_lng, activity, synergy, target_density, {score_expr} AS score, suburb
        FROM cell_stats
        WHERE activity > 5
        ORDER BY score DESC
        LIMIT 50
    """

    rows = run_query(sql, all_params)
    
    num_zones = request.args.get('zones', 5, type=int)
    num_zones = max(1, min(50, num_zones))
    selected_rows = rows[:num_zones]

    return jsonify([{'lat': r[0], 'lng': r[1], 'activity': r[2], 'synergy': r[3], 'target_density': r[4], 'score': float(r[5] or 0), 'suburb': r[6]} for r in selected_rows])


@app.route('/api/ml-recommend')
def ml_recommend():
    category = request.args.get('category', '').strip()
    mode = request.args.get('mode', 'smart').strip().lower()

    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)

    if not category or category not in SYNERGY_MAP:
        return jsonify({'error': 'Please select a specific category for recommendations.'}), 400

    synergies = SYNERGY_MAP[category]

    conds = ['latitude IS NOT NULL', 'longitude IS NOT NULL', 'fsq_category_labels IS NOT NULL']
    params = []

    if min_lat is not None:
        conds.append('latitude >= ?'); params.append(min_lat)
    if max_lat is not None:
        conds.append('latitude <= ?'); params.append(max_lat)
    if min_lng is not None:
        conds.append('longitude >= ?'); params.append(min_lng)
    if max_lng is not None:
        conds.append('longitude <= ?'); params.append(max_lng)

    where = ' AND '.join(conds)
    syn_conds = " OR ".join(["array_to_string(fsq_category_labels, '|') ILIKE ?" for _ in synergies])
    syn_params = [f"%{s}%" for s in synergies]
    target_cond = "array_to_string(fsq_category_labels, '|') ILIKE ?"
    target_param = f"%{category}%"

    all_params = syn_params + [target_param] + params

    # Broader fetch than heuristic (activity > 2) to give ML more training signal
    sql = f"""
        WITH cell_stats AS (
            SELECT
                FLOOR(latitude / 0.01) * 0.01 + 0.005 AS center_lat,
                FLOOR(longitude / 0.01) * 0.01 + 0.005 AS center_lng,
                COUNT(*) AS activity,
                SUM(CASE WHEN {syn_conds} THEN 1 ELSE 0 END) AS synergy,
                SUM(CASE WHEN {target_cond} THEN 1 ELSE 0 END) AS target_density,
                MODE(locality) AS suburb
            FROM '{PARQUET}'
            WHERE {where}
            GROUP BY 1, 2
        )
        SELECT center_lat, center_lng, activity, synergy, target_density, suburb
        FROM cell_stats
        WHERE activity > 2
        LIMIT 2000
    """

    rows = run_query(sql, all_params)
    if len(rows) < 10:
        return jsonify({'error': 'Not enough data in this area for ML analysis.'}), 400

    # Engineer features: ratios and log transform capture non-linear patterns
    raw = np.array([[r[0], r[1], r[2], r[3], r[4]] for r in rows], dtype=float)
    suburbs = [r[5] for r in rows]
    activity      = raw[:, 2]
    synergy       = raw[:, 3]
    target_density = raw[:, 4]
    synergy_ratio     = synergy / np.maximum(activity, 1)
    competition_ratio = target_density / np.maximum(activity, 1)
    log_activity      = np.log1p(activity)

    X = np.column_stack([activity, synergy, target_density, synergy_ratio, competition_ratio, log_activity])

    # Use heuristic formula as training labels so the GBR learns non-linear generalisations of it
    if mode == 'opportunity':
        y = (synergy_ratio * 100.0) - (target_density * 20.0)
    elif mode == 'competitive':
        y = (activity * 1.0) + (synergy * 1.5) - (target_density * 1.0)
    else:
        y = (activity * 0.5) + (synergy * 1.2) - (target_density * 4.0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = GradientBoostingRegressor(n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42)
    model.fit(X_scaled, y)

    ml_scores = model.predict(X_scaled)

    feature_names = ['activity', 'synergy', 'target_density', 'synergy_ratio', 'competition_ratio', 'log_activity']
    feature_importance = {k: round(float(v), 4) for k, v in zip(feature_names, model.feature_importances_)}

    num_zones = request.args.get('zones', 5, type=int)
    num_zones = max(1, min(50, num_zones))
    
    top_idx = np.argsort(ml_scores)[-num_zones:][::-1]
    recommendations = [
        {
            'lat': float(raw[i, 0]),
            'lng': float(raw[i, 1]),
            'activity': int(raw[i, 2]),
            'synergy': int(raw[i, 3]),
            'target_density': int(raw[i, 4]),
            'synergy_ratio': round(float(synergy_ratio[i]), 3),
            'competition_ratio': round(float(competition_ratio[i]), 3),
            'ml_score': round(float(ml_scores[i]), 4),
            'suburb': suburbs[i] if suburbs[i] else 'Unknown Area'
        }
        for i in top_idx
    ]

    return jsonify({
        'recommendations': recommendations,
        'feature_importance': feature_importance,
        'model': 'GradientBoostingRegressor',
        'n_cells_analyzed': len(rows),
    })


if __name__ == '__main__':
    print(f'StartupSpot AU — serving from http://localhost:8080')
    print(f'Data: {PARQUET}')
    app.run(debug=True, port=8080)
