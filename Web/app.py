from flask import Flask, jsonify, request, render_template
import duckdb
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=BASE_DIR)

PARQUET = 'https://github.com/SpaghettiFun9/Geographical-Website/releases/download/v1.0/au_locations.parquet'
VALID_STATES = {'ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA'}
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
DEFAULT_LIMIT = 800
MAX_LIMIT = 10000000000000


def run_query(sql, params=None):
    con = duckdb.connect()
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


@app.after_request
def add_cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route('/')
def index():
    return render_template('index.html', default_limit=DEFAULT_LIMIT, max_limit=MAX_LIMIT)


@app.route('/api/states')
def states():
    valid_list = list(VALID_STATES)
    placeholders = ', '.join(['?'] * len(valid_list))
    rows = run_query(f"""
        SELECT UPPER(region) AS reg, COUNT(*) AS cnt
        FROM '{PARQUET}'
        WHERE UPPER(region) IN ({placeholders})
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY UPPER(region)
        ORDER BY cnt DESC
    """, valid_list)
    return jsonify([{'state': r[0], 'count': r[1]} for r in rows])


@app.route('/api/locations')
def locations():
    state = request.args.get('state', '').strip().upper()
    category = request.args.get('category', '').strip()
    open_only = request.args.get('open_only', 'false') == 'true'
    limit = min(request.args.get('limit', DEFAULT_LIMIT, type=int), MAX_LIMIT)

    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)

    if state and state not in VALID_STATES:
        return jsonify({'error': 'Invalid state'}), 400
    if category and not SAFE_PATTERN.match(category):
        return jsonify({'error': 'Invalid category'}), 400

    conds = ['latitude IS NOT NULL', 'longitude IS NOT NULL', 'name IS NOT NULL']
    params = []

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


@app.route('/api/suburb-stats')
def suburb_stats():
    state = request.args.get('state', '').strip().upper()
    category = request.args.get('category', '').strip()

    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)

    if state and state not in VALID_STATES:
        return jsonify({'error': 'Invalid state'}), 400
    if category and not SAFE_PATTERN.match(category):
        return jsonify({'error': 'Invalid category'}), 400

    conds = ['latitude IS NOT NULL', 'longitude IS NOT NULL', 'locality IS NOT NULL']
    params = []

    if state:
        conds.append('UPPER(region) = ?'); params.append(state)
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

    where = ' AND '.join(conds)

    rows = run_query(f"""
        SELECT locality, region, COUNT(*) AS cnt, AVG(latitude) AS lat, AVG(longitude) AS lng
        FROM '{PARQUET}'
        WHERE {where}
        GROUP BY locality, region
        ORDER BY cnt DESC
        LIMIT 50
    """, params)

    return jsonify([
        {'suburb': r[0], 'state': r[1], 'count': r[2], 'lat': r[3], 'lng': r[4]}
        for r in rows
    ])


@app.route('/api/category-breakdown')
def category_breakdown():
    state = request.args.get('state', '').strip().upper()
    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lng = request.args.get('min_lng', type=float)
    max_lng = request.args.get('max_lng', type=float)

    if state and state not in VALID_STATES:
        return jsonify({'error': 'Invalid state'}), 400

    conds = ['latitude IS NOT NULL', 'longitude IS NOT NULL', 'fsq_category_labels IS NOT NULL']
    params = []

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
    mode = request.args.get('mode', 'balanced').strip().lower()

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
    if mode == 'low_comp':
        # Strong negative competition, ratio helps find highly underserved pockets
        score_expr = "((synergy::FLOAT / activity) * 100.0) - (target_density * 20.0)"
    elif mode == 'hub':
        # Light negative competition, very high demand, high synergy
        score_expr = "(activity * 1.0) + (synergy * 1.5) - (target_density * 1.0)"
    else:
        # Balanced (default): Medium negative competition, high demand, medium-high synergy
        score_expr = "(activity * 0.5) + (synergy * 1.2) - (target_density * 4.0)"

    # DuckDB CTE for splitting the map into 500m (0.005 deg) grid cells
    sql = f"""
        WITH cell_stats AS (
            SELECT 
                FLOOR(latitude / 0.005) * 0.005 + 0.0025 AS center_lat,
                FLOOR(longitude / 0.005) * 0.005 + 0.0025 AS center_lng,
                COUNT(*) AS activity,
                SUM(CASE WHEN {syn_conds} THEN 1 ELSE 0 END) AS synergy,
                SUM(CASE WHEN {target_cond} THEN 1 ELSE 0 END) AS target_density
            FROM '{PARQUET}'
            WHERE {where}
            GROUP BY 1, 2
        )
        SELECT center_lat, center_lng, activity, synergy, target_density, {score_expr} AS score
        FROM cell_stats
        WHERE activity > 5
        ORDER BY score DESC
        LIMIT 5
    """

    rows = run_query(sql, all_params)
    return jsonify([{'lat': r[0], 'lng': r[1], 'activity': r[2], 'synergy': r[3], 'target_density': r[4], 'score': float(r[5] or 0)} for r in rows])


if __name__ == '__main__':
    print(f'StartupSpot AU — serving from http://localhost:8080')
    print(f'Data: {PARQUET}')
    app.run(debug=True, port=8080)
