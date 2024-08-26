import requests
import subprocess
import sys

# Constants
BASE_URL = "https://fantasy.premierleague.com/api/"
LEAGUE_ID = 820322
SEASON = "24_25"  # Update this for the correct season
GAMEWEEK = 2  # Update this for the desired gameweek

def get_league_teams(league_id):
    print(f"Fetching teams for league ID: {league_id}...")
    url = f"{BASE_URL}leagues-classic/{league_id}/standings/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching league data. Status code: {response.status_code}")
        return None
    data = response.json()
    return [(team['entry'], team['entry_name']) for team in data['standings']['results']]

def main():
    print(f"Starting FPL League Team Scraper for League ID: {LEAGUE_ID}")

    league_teams = get_league_teams(LEAGUE_ID)
    if not league_teams:
        print("Failed to fetch league teams. Exiting.")
        return

    print(f"Found {len(league_teams)} teams in the league.")

    for team_id, team_name in league_teams:
        print(f"\nProcessing team: {team_name} (ID: {team_id})")
        try:
            command = f"python teams_scraper.py {team_id} {SEASON} {GAMEWEEK}"
            print(f"Executing command: {command}")
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error executing command for team {team_name}:")
            print(e.stderr)

    print("\nScript execution completed.")

if __name__ == "__main__":
    main()
