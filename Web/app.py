from flask import Flask, jsonify, request, send_from_directory
import duckdb
import os
import re

app = Flask(__name__)

PARQUET = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'au_locations.parquet'))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VALID_STATES = {'ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA'}
SAFE_PATTERN = re.compile(r'^[\w\s&,\-]+$')


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
    return send_from_directory(BASE_DIR, 'index.html')


@app.route('/api/states')
def states():
    rows = run_query(f"""
        SELECT region, COUNT(*) AS cnt
        FROM '{PARQUET}'
        WHERE region IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY region
        ORDER BY cnt DESC
    """)
    return jsonify([{'state': r[0], 'count': r[1]} for r in rows])


@app.route('/api/locations')
def locations():
    state = request.args.get('state', '').strip().upper()
    category = request.args.get('category', '').strip()
    open_only = request.args.get('open_only', 'false') == 'true'
    limit = min(request.args.get('limit', 800, type=int), 2000)

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


if __name__ == '__main__':
    print(f'StartupSpot AU — serving from http://localhost:8080')
    print(f'Data: {PARQUET}')
    app.run(debug=True, port=8080)
