import duckdb
from tabulate import tabulate
from collections import defaultdict

# Constants
DATA_SEASON = "2024-25"  # Format used in data directory
PRICE_BUCKET_SIZE = 0.5  # 0.5 million price buckets

# Initialize DuckDB connection
con = duckdb.connect(':memory:')

def load_player_data():
    print("Loading player data...")
    con.execute(f"""
        CREATE TABLE players AS
        SELECT
            id,
            first_name || ' ' || second_name AS full_name,
            element_type,
            now_cost / 10.0 AS price,
            total_points,
            team,
            selected_by_percent::FLOAT AS ownership
        FROM read_csv_auto('data/{DATA_SEASON}/players_raw.csv')
    """)

def load_team_data():
    print("Loading team data...")
    con.execute(f"""
        CREATE TABLE teams AS
        SELECT id, name
        FROM read_csv_auto('data/{DATA_SEASON}/teams.csv')
    """)

def get_player_rankings():
    print("Calculating top 10 player rankings by price buckets...")
    return con.execute("""
        WITH player_buckets AS (
            SELECT
                *,
                FLOOR(price) AS price_bucket
            FROM players
        ),
        ranked_players AS (
            SELECT
                price_bucket,
                CASE
                    WHEN element_type = 1 THEN 'GK'
                    WHEN element_type = 2 THEN 'DEF'
                    WHEN element_type = 3 THEN 'MID'
                    WHEN element_type = 4 THEN 'FWD'
                END AS position,
                full_name,
                t.name AS team_name,
                price,
                total_points,
                ownership,
                ROW_NUMBER() OVER (PARTITION BY price_bucket ORDER BY total_points DESC) AS rank_in_bucket
            FROM player_buckets p
            JOIN teams t ON p.team = t.id
        )
        SELECT *
        FROM ranked_players
        WHERE rank_in_bucket <= 10
        ORDER BY price_bucket, total_points DESC
    """).fetchall()

def display_rankings(rankings):
    buckets = defaultdict(list)
    for row in rankings:
        price_bucket, position, name, team, price, points, ownership, rank = row
        buckets[price_bucket].append([rank, name, position, team, f"£{price}m", points, f"{ownership:.1f}%"])

    for price_bucket, players in sorted(buckets.items()):
        print(f"\nPrice Bucket: £{price_bucket}m - £{price_bucket + PRICE_BUCKET_SIZE}m")
        print(tabulate(players, headers=["Rank", "Name", "Position", "Team", "Price", "Points", "Ownership %"], tablefmt="grid"))

def main():
    print("Starting FPL Player Price Analysis - Top 10 per Bucket")

    load_player_data()
    load_team_data()

    rankings = get_player_rankings()
    display_rankings(rankings)

    print("\nAnalysis completed.")

if __name__ == "__main__":
    main()
