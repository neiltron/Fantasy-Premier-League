import duckdb
from tabulate import tabulate
from colorama import Fore, Back, Style, init

# Initialize colorama
init(autoreset=True)

# Constants
DATA_SEASON = "2024-25"
TEAM_SEASON = "24_25"

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
        SELECT id, name, short_name
        FROM read_csv_auto('data/{DATA_SEASON}/teams.csv')
    """)

def get_team_roster(team_id, gameweek):
    return con.execute(f"""
        SELECT p.element, pl.full_name, pl.team
        FROM read_csv_auto('teams/{team_id}/{TEAM_SEASON}/picks_{gameweek}.csv') p
        JOIN players pl ON p.element = pl.id
        ORDER BY p.position
    """).fetchall()

def get_next_fixtures(team_id, current_gameweek):
    return con.execute(f"""
        SELECT
            event AS gameweek,
            CASE
                WHEN team_h = {team_id} THEN team_a
                ELSE team_h
            END AS opponent_id,
            CASE
                WHEN team_h = {team_id} THEN team_h_difficulty
                ELSE team_a_difficulty
            END AS difficulty
        FROM fixtures
        WHERE (team_h = {team_id} OR team_a = {team_id})
          AND event >= {current_gameweek}
        ORDER BY event
        LIMIT 5
    """).fetchall()

def color_difficulty(difficulty):
    if difficulty <= 2:
        return Fore.GREEN + str(difficulty) + Style.RESET_ALL
    elif difficulty == 3:
        return Fore.YELLOW + str(difficulty) + Style.RESET_ALL
    else:
        return Fore.RED + str(difficulty) + Style.RESET_ALL

def main():
    load_data()

    team_id = input("Enter FPL team ID: ")
    current_gameweek = int(input("Enter current gameweek: "))

    roster = get_team_roster(team_id, current_gameweek)

    table_data = []
    headers = ["Player"]

    for player in roster:
        player_id, player_name, player_team = player
        fixtures = get_next_fixtures(player_team, current_gameweek)

        row = [player_name]
        if not table_data:
            headers.extend([f"GW{fixture[0]}" for fixture in fixtures])

        for fixture in fixtures:
            gameweek, opponent_id, difficulty = fixture
            opponent_short_name = con.execute(f"SELECT short_name FROM teams WHERE id = {opponent_id}").fetchone()[0]
            row.append(f"{opponent_short_name[:3].upper()} {color_difficulty(difficulty)}")

        table_data.append(row)

    print("\nFixture Difficulty for Next 5 Gameweeks:")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

if __name__ == "__main__":
    main()
