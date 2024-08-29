import duckdb
from tabulate import tabulate
from fuzzywuzzy import process
import pandas as pd

# Constants
DATA_SEASON = "2024-25"
HISTORICAL_DATA = "data/cleaned_merged_seasons.csv"

# Initialize DuckDB connection
con = duckdb.connect(':memory:')

def load_data():
    print("Loading data...")
    con.execute(f"""
        CREATE TABLE players AS
        SELECT
            id,
            first_name || ' ' || second_name AS full_name,
            team
        FROM read_csv_auto('data/{DATA_SEASON}/players_raw.csv')
    """)

    con.execute(f"""
        CREATE TABLE fixtures AS
        SELECT *
        FROM read_csv_auto('data/{DATA_SEASON}/fixtures.csv')
    """)

    con.execute(f"""
        CREATE TABLE teams AS
        SELECT id, name
        FROM read_csv_auto('data/{DATA_SEASON}/teams.csv')
    """)

    con.execute(f"""
        CREATE TABLE historical_data AS
        SELECT *
        FROM read_csv_auto('{HISTORICAL_DATA}')
    """)

def fuzzy_search_player(query):
    player_names = con.execute("SELECT id, full_name FROM players").fetchall()
    results = process.extract(query, [name for _, name in player_names], limit=None)

    # Filter results to only include matches with a score of 90 or higher
    high_score_results = [r for r in results if r[1] >= 90]

    if not high_score_results:
        print("No close matches found. Please try a different search term.")
        return None

    if len(high_score_results) == 1:
        player_id = next(id for id, name in player_names if name == high_score_results[0][0])
        return player_id

    print("Multiple potential matches found:")
    table_data = [
        [id, name, score]
        for (name, score), (id, _) in zip(high_score_results, player_names)
        if name in [r[0] for r in high_score_results]
    ]
    print(tabulate(table_data, headers=["ID", "Name", "Match Score"], tablefmt="grid"))

    # Ask user to select a player if multiple matches are found
    while True:
        choice = input("Enter the ID of the player you want to analyze (or 'cancel' to search again): ")
        if choice.lower() == 'cancel':
            return None
        try:
            chosen_id = int(choice)
            if chosen_id in [row[0] for row in table_data]:
                return chosen_id
            else:
                print("Invalid ID. Please choose from the list above.")
        except ValueError:
            print("Please enter a valid numeric ID or 'cancel'.")


def get_next_fixture(player_id):
    return con.execute(f"""
        WITH player_team AS (
            SELECT team FROM players WHERE id = {player_id}
        )
        SELECT
            CASE
                WHEN f.team_h = p.team THEN f.team_a
                ELSE f.team_h
            END AS opponent_team,
            f.event AS gameweek,
            CASE
                WHEN f.team_h = p.team THEN 'Home'
                ELSE 'Away'
            END AS venue
        FROM fixtures f, player_team p
        WHERE (f.team_h = p.team OR f.team_a = p.team)
          AND f.finished = 'False'
        ORDER BY f.event
        LIMIT 1
    """).fetchone()

def get_historical_results(player_id, opponent_team):
    player_name = con.execute(f"SELECT full_name FROM players WHERE id = {player_id}").fetchone()[0]
    opponent_name = con.execute(f"SELECT name FROM teams WHERE id = {opponent_team}").fetchone()[0]

    results = con.execute(f"""
        SELECT
            season_x AS season,
            kickoff_time,
            CASE WHEN was_home THEN team_h_score ELSE team_a_score END AS player_team_score,
            CASE WHEN was_home THEN team_a_score ELSE team_h_score END AS opponent_score,
            goals_scored,
            assists,
            total_points
        FROM historical_data
        WHERE name = '{player_name}' AND opp_team_name = '{opponent_name}'
        ORDER BY kickoff_time DESC
    """).fetchall()

    return results, player_name, opponent_name

def main():
    load_data()

    while True:
        query = input("Enter player name (or 'quit' to exit): ")
        if query.lower() == 'quit':
            break

        player_id = fuzzy_search_player(query)
        if player_id is None:
            continue

        next_fixture = get_next_fixture(player_id)
        if next_fixture is None:
            print("No upcoming fixtures found for this player.")
            continue

        opponent_team, gameweek, venue = next_fixture
        print(f"\nNext fixture: Gameweek {gameweek}, {venue} against team ID {opponent_team}")

        historical_results, player_name, opponent_name = get_historical_results(player_id, opponent_team)

        print(f"\nHistorical results for {player_name} against {opponent_name}:")
        if not historical_results:
            print("No historical data found.")
        else:
            table_data = [
                [
                    result[0],  # season
                    pd.to_datetime(result[1]).strftime('%Y-%m-%d'),  # date
                    f"{result[2]}-{result[3]}",  # score
                    result[4],  # goals
                    result[5],  # assists
                    result[6]   # points
                ]
                for result in historical_results
            ]
            print(tabulate(table_data, headers=["Season", "Date", "Score", "Goals", "Assists", "Points"], tablefmt="grid"))

        print("\n")

if __name__ == "__main__":
    main()
