import duckdb
import pandas as pd
from collections import defaultdict
from pathlib import Path
from tabulate import tabulate

# Constants
DATA_SEASON = "2024-25"
TEAM_SEASON = "24_25"
GAMEWEEK = 11

# Initialize DuckDB connection
con = duckdb.connect(':memory:')

def load_base_data():
    """Load the base player and team data into DuckDB."""
    print("Loading base data...")
    con.execute(f"""
        CREATE TABLE players AS
        SELECT
            id,
            first_name || ' ' || second_name AS full_name,
            team
        FROM read_csv_auto('data/{DATA_SEASON}/players_raw.csv')
    """)

    con.execute(f"""
        CREATE TABLE teams AS
        SELECT id, name, short_name
        FROM read_csv_auto('data/{DATA_SEASON}/teams.csv')
    """)

def get_team_directories():
    """Get all team directories in the teams folder."""
    teams_path = Path("teams")
    return [d for d in teams_path.iterdir() if d.is_dir()]

def analyze_captains(matchweek):
    """Analyze captain and vice-captain picks for a specific matchweek."""
    captain_picks = defaultdict(lambda: {"captain": 0, "vice": 0})
    total_teams = 0
    teams_with_data = 0

    for team_dir in get_team_directories():
        team_id = team_dir.name
        picks_file = team_dir / TEAM_SEASON / f"picks_{matchweek}.csv"
        total_teams += 1

        if not picks_file.exists():
            continue

        teams_with_data += 1

        try:
            picks_data = con.execute(f"""
                SELECT
                    pl.full_name || ' (' || t.short_name || ')' as player_info,
                    p.is_captain,
                    p.is_vice_captain
                FROM read_csv_auto('{picks_file}') p
                JOIN players pl ON p.element = pl.id
                JOIN teams t ON pl.team = t.id
                WHERE p.is_captain = true OR p.is_vice_captain = true
            """).fetchall()

            for player_info, is_captain, is_vice in picks_data:
                if is_captain:
                    captain_picks[player_info]["captain"] += 1
                if is_vice:
                    captain_picks[player_info]["vice"] += 1

        except Exception as e:
            print(f"Error processing team {team_id} for week {matchweek}: {e}")
            continue

    # Find most popular picks
    if not captain_picks:
        return None, 0, 0

    top_captain = max(captain_picks.items(), key=lambda x: x[1]["captain"])
    top_vice = max(captain_picks.items(), key=lambda x: x[1]["vice"])

    return {
        "top_captain": (top_captain[0], top_captain[1]["captain"]),
        "top_vice": (top_vice[0], top_vice[1]["vice"]),
        "total_teams": total_teams,
        "teams_with_data": teams_with_data
    }

def main():
    load_base_data()

    print("Analyzing captain picks for matchweeks 1-" + str(GAMEWEEK) + "...")

    # Prepare data for table
    table_data = []

    for week in range(1, GAMEWEEK + 1):
        result = analyze_captains(week)

        if result is None:
            row = [
                week,
                "No data",
                "0",
                "0%",
                "No data",
                "0",
                "0%",
                "0/0"
            ]
        else:
            coverage = f"{result['teams_with_data']}/{result['total_teams']}"
            captain_pct = f"{(result['top_captain'][1] / result['teams_with_data'] * 100):.1f}%"
            vice_pct = f"{(result['top_vice'][1] / result['teams_with_data'] * 100):.1f}%"

            row = [
                week,
                result['top_captain'][0],
                result['top_captain'][1],
                captain_pct,
                result['top_vice'][0],
                result['top_vice'][1],
                vice_pct,
                coverage
            ]

        table_data.append(row)

    # Display table
    headers = [
        "GW",
        "Most Captained",
        "Count",
        "Owned%",
        "Most Vice",
        "Count",
        "Owned%",
        "Coverage"
    ]

    print("\nCaptaincy Analysis by Gameweek:")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

if __name__ == "__main__":
    main()
