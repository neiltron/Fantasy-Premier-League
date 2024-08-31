import requests
import duckdb
from tabulate import tabulate
import time

# Constants
BASE_URL = "https://fantasy.premierleague.com/api/"
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

def get_league_teams(league_id):
    print(f"Fetching teams for league ID: {league_id}...")
    url = f"{BASE_URL}leagues-classic/{league_id}/standings/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching league data. Status code: {response.status_code}")
        return []
    data = response.json()
    return [
        {
            'entry': team['entry'],
            'team_name': team['entry_name'],
            'player_name': team['player_name'],
            'total_points': team['total'],
            'overall_rank': team['rank']
        }
        for team in data['standings']['results']
    ]

def get_team_players(team_id, gameweek):
    return con.execute(f"""
        SELECT p.element, p.multiplier
        FROM read_csv_auto('teams/{team_id}/{TEAM_SEASON}/picks_{gameweek}.csv') p
    """).fetchall()

def get_live_data(gameweek):
    url = f"{BASE_URL}event/{gameweek}/live/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching live data. Status code: {response.status_code}")
        return {}
    data = response.json()['elements']
    return {str(element['id']): element['stats'] for element in data}

def calculate_gw_points(team_players, live_data):
    total_points = 0
    for player_id, multiplier in team_players:
        player_data = live_data.get(str(player_id), {})
        total_points += player_data.get('total_points', 0) * multiplier
    return total_points

def get_gw_transfers(team_id, gameweek):
    transfers_data = con.execute(f"""
        SELECT event_transfers
        FROM read_csv_auto('teams/{team_id}/{TEAM_SEASON}/gws.csv')
        WHERE event = {gameweek}
    """).fetchone()
    return transfers_data[0] if transfers_data else 0

def main():
    load_data()

    league_id = input("Enter FPL league ID: ")
    gameweek = int(input("Enter current gameweek: "))

    while True:
        league_teams = get_league_teams(league_id)
        live_data = get_live_data(gameweek)

        standings = []
        for team in league_teams:
            team_players = get_team_players(team['entry'], gameweek)
            gw_points = calculate_gw_points(team_players, live_data)
            gw_transfers = get_gw_transfers(team['entry'], gameweek)

            standings.append([
                team['team_name'],
                team['player_name'],
                gw_points,
                gw_transfers,
                team['total_points'] + gw_points,  # Updating total points with live GW points
                team['overall_rank']
            ])

        # Sort standings by total points (descending)
        standings.sort(key=lambda x: x[2], reverse=True)

        # Add rank to each row
        standings = [[i+1] + row for i, row in enumerate(standings)]

        print("\nLive FPL League Standings:")
        print(tabulate(standings, headers=[
            "Rank", "Team Name", "Manager", "GW Points", "GW Transfers", "Total Points", "Overall Rank"
        ], tablefmt="grid"))

        print(f"\nLast updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("Updating in 60 seconds...")
        time.sleep(60)  # Update every 60 seconds

if __name__ == "__main__":
    main()
