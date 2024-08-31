import requests
import csv
import os
from tabulate import tabulate

# Constants
BASE_URL = "https://fantasy.premierleague.com/api/"
LEAGUE_ID = 820322  # Replace with your league ID
SEASON = "24_25"  # Update this for the current season
CURRENT_GW = 3  # Update this to the current gameweek

def get_league_standings(league_id):
    print(f"Fetching standings for league ID: {league_id}...")
    url = f"{BASE_URL}leagues-classic/{league_id}/standings/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching league data. Status code: {response.status_code}")
        return None
    return response.json()

def get_gw_data(team_id, gameweek):
    csv_path = f"teams/{team_id}/{SEASON}/gws.csv"
    if not os.path.exists(csv_path):
        print(f"CSV file not found for team {team_id}")
        return None

    with open(csv_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if int(row['event']) == gameweek:
                return row

    print(f"Data for gameweek {gameweek} not found for team {team_id}")
    return None

def main():
    print("Starting FPL League Weekly Table Script")

    league_data = get_league_standings(LEAGUE_ID)
    if not league_data:
        print("Failed to fetch league data. Exiting.")
        return

    print(f"Generating table for Gameweek {CURRENT_GW}")

    table_data = []
    for team in league_data['standings']['results']:
        entry_id = team['entry']
        team_name = team['entry_name']
        player_name = team['player_name']

        gw_data = get_gw_data(entry_id, CURRENT_GW)

        if gw_data:
            weekly_points = int(gw_data['points'])
            total_points = int(gw_data['total_points'])
            overall_rank = int(gw_data['overall_rank'])
            transfers = int(gw_data['event_transfers'])

            table_data.append([
                team_name,
                player_name,
                weekly_points,
                transfers,
                total_points,
                overall_rank
            ])

    # Sort the table data by weekly points (descending)
    table_data.sort(key=lambda x: x[2], reverse=True)

    # Add rank to each row
    table_data = [[i+1] + row for i, row in enumerate(table_data)]

    print("\nCurrent Weekly Table:")
    print(tabulate(table_data, headers=[
        "Rank", "Team Name", "Manager", "GW Points", "GW Transfers", "Total Points", "Overall Rank"
    ], tablefmt="grid"))

    print("\nScript execution completed.")

if __name__ == "__main__":
    main()
