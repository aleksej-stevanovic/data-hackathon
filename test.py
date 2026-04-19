import duckdb

con = duckdb.connect()

con.execute("""
INSTALL httpfs;
LOAD httpfs;
""")

# Set the correct S3 region and enforce anonymous access.
# This prevents a '403 Forbidden' error if you have local AWS 
# credentials that lack explicit permissions for this public bucket.
con.execute("""
CREATE OR REPLACE SECRET fsq_public_s3 (
    TYPE S3,
    PROVIDER config,
    REGION 'us-east-1'
);
""")

# Read the remote FSQ OS places parquet files
# Make sure dt=2025-07-08 is a valid published partition date!
con.execute("""
CREATE OR REPLACE VIEW fsq_places AS
SELECT *
FROM read_parquet('s3://fsq-os-places-us-east-1/release/dt=2025-07-08/places/parquet/*.parquet');
""")

# Save the entire world's data
con.execute("""
COPY (
    SELECT *
    FROM fsq_places
) TO 'all_foursquare_locations.parquet' (FORMAT PARQUET);
""")

print("Done: wrote all_foursquare_locations.parquet")
con.close()