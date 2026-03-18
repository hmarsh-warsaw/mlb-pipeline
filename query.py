import duckdb
import sys

sql_file = sys.argv[1]
conn = duckdb.connect("data/mlb.db")

with open(sql_file) as f:
    queries = f.read().split(";")

for query in queries:
    query = query.strip()
    if query:
        print(f"\n-- {query[:60]}...")
        result = conn.execute(query).df()
        print(result.to_string())
