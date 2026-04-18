import pandas
from pyarrow.parquet import read_parquet


places = read_parquet("hf://datasets/foursquare/fsq-os-places/release/dt=2026-04-14/places/parquet/*.parquet")


