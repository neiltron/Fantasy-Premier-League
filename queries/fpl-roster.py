import requests
from datetime import datetime
import pytz
from tabulate import tabulate

# Constants
BASE_URL = "https://fantasy.premierleague.com/api/"
LEAGUE_ID = 820322
GAMEWEEK = 2

def get_league_teams(league_id):
    print(f"Fetching teams for league ID: {league_id}...")
    url = f"{BASE_URL}leagues-classic/{league_id}/standings/"
    response = requests.get(url)
    data = response.json()
    teams = [(team['entry'], team['entry_name']) for team in data['standings']['results']]
    print(f"Found {len(teams)} teams in the league.")
    return teams

def get_team_players(team_id, players_data):
    print(f"Fetching players for team ID: {team_id}...")
    url = f"{BASE_URL}entry/{team_id}/event/{GAMEWEEK}/picks/"
    response = requests.get(url)
    data = response.json()
    players = [next(p for p in players_data if p['id'] == pick['element']) for pick in data['picks']]
    print(f"Team {team_id} has {len(players)} players.")
    return players

def get_fixtures(gameweek):
    print(f"Fetching fixtures for gameweek {gameweek}...")
    url = f"{BASE_URL}fixtures/?event={gameweek}"
    response = requests.get(url)
    fixtures = response.json()
    print(f"Found {len(fixtures)} fixtures for gameweek {gameweek}.")
    return fixtures

def main():
    print("Starting FPL League Parser...")

    # Get league teams
    league_teams = get_league_teams(LEAGUE_ID)

    # Get all players and teams data
    print("Fetching all players and teams data...")
    bootstrap_static = requests.get(f"{BASE_URL}bootstrap-static/").json()
    players_data = bootstrap_static['elements']
    teams_data = {team['id']: team['name'] for team in bootstrap_static['teams']}
    print(f"Loaded data for {len(players_data)} players and {len(teams_data)} teams.")

    # Get fixtures for the current gameweek
    fixtures = get_fixtures(GAMEWEEK)

    # Create a timeline of matches
    print("Creating timeline of matches...")
    timeline = []
    for fixture in fixtures:
        home_team = teams_data[fixture['team_h']]
        away_team = teams_data[fixture['team_a']]
        kickoff_time = datetime.strptime(fixture['kickoff_time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
        timeline.append({
            'time': kickoff_time,
            'home_team': home_team,
            'away_team': away_team,
            'home_difficulty': fixture['team_h_difficulty'],
            'away_difficulty': fixture['team_a_difficulty'],
            'affected_fpl_teams': []
        })
    print(f"Created timeline with {len(timeline)} matches.")

    # For each league team, get their players and add to affected matches
    print("Processing league teams and their players...")
    for team_id, team_name in league_teams:
        print(f"Processing team: {team_name} (ID: {team_id})")
        team_players = get_team_players(team_id, players_data)
        for player in team_players:
            player_team = teams_data[player['team']]
            for match in timeline:
                if player_team in [match['home_team'], match['away_team']]:
                    match['affected_fpl_teams'].append({
                        'team_name': team_name,
                        'player_name': player['web_name'],
                        'player_position': player['element_type']
                    })
        print(f"Finished processing team: {team_name}")

    # Sort timeline by match time
    print("Sorting timeline by match time...")
    timeline.sort(key=lambda x: x['time'])

    # Print the timeline
    print("\nFinal Timeline:")
    for match in timeline:
        print(f"\nMatch time: {match['time'].strftime('%Y-%m-%d %H:%M')} UTC")
        print(f"{match['home_team']} (Difficulty: {match['home_difficulty']}) vs {match['away_team']} (Difficulty: {match['away_difficulty']})")
        
        # Prepare data for the table
        table_data = []
        for affected_team in match['affected_fpl_teams']:
            table_data.append([
                affected_team['team_name'],
                affected_team['player_name'],
                'GK' if affected_team['player_position'] == 1 else
                'DEF' if affected_team['player_position'] == 2 else
                'MID' if affected_team['player_position'] == 3 else
                'FWD'
            ])
        
        # Sort the table data by team name and then by player position
        table_data.sort(key=lambda x: (x[0], ['GK', 'DEF', 'MID', 'FWD'].index(x[2])))
        
        # Print the table
        print(tabulate(table_data, headers=['FPL Team', 'Player', 'Position'], tablefmt='grid'))

    print("\nScript execution completed.")

if __name__ == "__main__":
    main()
