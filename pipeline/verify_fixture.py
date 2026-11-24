"""Check expected past-only velocity on the redistributable fixture."""
import duckdb

database = duckdb.connect('output/ci/warehouse.duckdb', read_only=True)
actual = dict((row[0], row[1:]) for row in database.execute(
    'select transaction_id, prior_txn_count, prior_1h_count, prior_24h_count from fact_transactions').fetchall())
expected = {
    'a1': (0, 0, 0), 'a2': (1, 1, 1), 'a3': (2, 0, 2),
    'b1': (0, 0, 0), 'b2': (1, 0, 1),
}
assert actual == expected, (actual, expected)
print('fixture velocity: 5/5 expected histories passed')
