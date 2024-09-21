import requests
import duckdb
from tabulate import tabulate
import time
from collections import defaultdict

FPL_API_URL = "https://fantasy.premierleague.com/api"
POCKETBASE_URL = "https://pb.growcup.lol"
LEAGUE_ID = 820322
CURRENT_GAMEWEEK = 5
DATA_SEASON = "2024-25"
TEAM_SEASON = "24_25"

con = duckdb.connect(':memory:')

def load_data():
    print("Loading data...")
    con.execute(f"""
        CREATE TABLE players AS
        SELECT
            id,
            first_name || ' ' || second_name AS full_name,
            team,
            element_type
        FROM read_csv_auto('data/{DATA_SEASON}/players_raw.csv')
    """)
    con.execute(f"""
        CREATE TABLE teams AS
        SELECT id, name, short_name
        FROM read_csv_auto('data/{DATA_SEASON}/teams.csv')
    """)

def get_player_teams():
    return {row[0]: row[1] for row in con.execute("SELECT id, team FROM players").fetchall()}

def get_league_teams(league_id):
    print(f"Fetching teams for league ID: {league_id}...")
    url = f"{FPL_API_URL}/leagues-classic/{league_id}/standings/"
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

def get_live_data(gameweek):
    url = f"{FPL_API_URL}/event/{gameweek}/live/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching live data. Status code: {response.status_code}")
        return {}
    data = response.json()['elements']
    return {str(element['id']): element['stats'] for element in data}

def get_fixtures_for_gameweek(gameweek):
    url = f"{FPL_API_URL}/fixtures/?event={gameweek}"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching fixtures. Status code: {response.status_code}")
        return {}
    fixtures = {fixture['id']: (fixture['team_h'], fixture['team_a']) for fixture in response.json()}
    print(f"Fetched {len(fixtures)} fixtures for gameweek {gameweek}")
    return fixtures

def calculate_gw_points(team_players, live_data, bonus_points):
    total_points = 0
    captain_id = next((pid for pid, data in team_players.items() if data['is_captain']), None)
    vice_captain_id = next((pid for pid, data in team_players.items() if data['is_vice_captain']), None)

    captain_played = live_data.get(captain_id, {}).get('minutes', 0) > 0 if captain_id else False

    for player_id, player_info in team_players.items():
        # Skip bench players (positions 12-15)
        if player_info['position'] > 11:
            continue

        player_data = live_data.get(player_id, {})

        official_bonus = player_data.get('bonus', 0)
        player_bonus = bonus_points.get(player_id, 0) if official_bonus == 0 else official_bonus

        player_points = player_data.get('total_points', 0)

        if official_bonus == 0:
            player_points += player_bonus

        multiplier = player_info['multiplier']
        if player_info['is_captain'] and captain_played:
            multiplier = 2
        elif player_info['is_vice_captain'] and not captain_played:
            multiplier = 2

        total_points += player_points * multiplier

    return total_points


def calculate_bonus_points(live_data, fixtures, player_teams):
    bonus_points = defaultdict(int)
    print(f"Number of fixtures: {len(fixtures)}")
    print(f"Number of players in live data: {len(live_data)}")
    print(f"Number of players in player_teams: {len(player_teams)}")

    for fixture_id, (team_h, team_a) in fixtures.items():
        print(f"Processing fixture {fixture_id}: {team_h} vs {team_a}")
        fixture_players = defaultdict(list)
        for player_id, stats in live_data.items():
            # Skip players who already have official bonus points
            if stats.get('bonus', 0) > 0:
                continue

            print('bps', stats.get('bps'))
            player_team = player_teams.get(int(player_id))
            if player_team is None:
                print(f"Warning: Player {player_id} not found in player_teams")
                continue
            if player_team in [team_h, team_a]:
                bps = stats.get('bps')
                fixture_players[fixture_id].append((player_id, bps))
                print(f"Player {player_id} (Team {player_team}) added to fixture {fixture_id} with BPS {bps}")

        print(f"Number of players in fixture {fixture_id}: {len(fixture_players[fixture_id])}")

        # Sort players by BPS and assign bonus points
        sorted_players = sorted(fixture_players[fixture_id], key=lambda x: x[1], reverse=True)
        print(sorted_players[:3])
        for i, (player_id, bps) in enumerate(sorted_players[:3]):
            bonus_points[player_id] = 3 - i
            print(f"Assigning {3-i} bonus points to player {player_id} (BPS: {bps})")

    print(f"Total bonus points assigned: {len(bonus_points)}")
    return bonus_points


def get_team_players(team_id, gameweek):
    picks = con.execute(f"""
        SELECT element, multiplier, is_captain, is_vice_captain, position
        FROM read_csv_auto('teams/{team_id}/{TEAM_SEASON}/picks_{gameweek}.csv')
        ORDER BY position
    """).fetchall()
    return {str(pick[0]): {
        'multiplier': pick[1],
        'is_captain': pick[2],
        'is_vice_captain': pick[3],
        'position': pick[4]  # Add position to the returned data
    } for pick in picks}


def get_or_update_record_bak(collection, data, unique_field):
    url = f"{POCKETBASE_URL}/api/collections/{collection}/records"

    # Handle composite keys
    if ',' in unique_field:
        filter_conditions = []
        for field in unique_field.split(','):
            filter_conditions.append(f"({field}='{data[field]}')")
        filter_string = " && ".join(filter_conditions)
    else:
        filter_string = f"({unique_field}='{data[unique_field]}')"

    response = requests.get(f"{url}?filter={filter_string}")

    if response.status_code == 200 and response.json()['items']:
        record_id = response.json()['items'][0]['id']
        update_url = f"{url}/{record_id}"
        response = requests.patch(update_url, json=data)
    else:
        response = requests.post(url, json=data)

    if response.status_code not in [200, 201]:
        print(f"Error updating {collection}: {response.text}")
    else:
        print(f"Successfully updated {collection}")
    return response.json()


def get_or_update_record(collection, data, unique_field):
    url = f"{POCKETBASE_URL}/api/collections/{collection}/records"

    # Handle composite keys
    if ',' in unique_field:
        filter_conditions = [f"({field}='{data[field]}')" for field in unique_field.split(',')]
        filter_string = " && ".join(filter_conditions)
    else:
        filter_string = f"({unique_field}='{data[unique_field]}')"

    # Try to get the existing record
    response = requests.get(f"{url}?filter={filter_string}")
    response.raise_for_status()  # Raise an exception for bad responses

    if response.json()['items']:
        # Update existing record
        record_id = response.json()['items'][0]['id']
        update_url = f"{url}/{record_id}"
        response = requests.patch(update_url, json=data)
        action = "updated"
    else:
        # Create new record
        response = requests.post(url, json=data)
        action = "created"

    response.raise_for_status()  # Raise an exception for bad responses

    print(f"Successfully {action} record in {collection}")
    return response.json()


def update_live_standings(standings, fpl_teams_mapping):

    # print(fpl_teams_mapping)

    for standing in standings:
        team_id = int(standing[1])  # Convert to string to ensure matching with mapping keys
        if team_id not in fpl_teams_mapping:
            print(f"Warning: Team ID {team_id} not found in PocketBase fpl_teams")
            continue

        live_standing_data = {
            "fpl_team": fpl_teams_mapping[team_id],  # Use the mapped PocketBase ID
            "gameweek": int(CURRENT_GAMEWEEK),
            "rank": standing[0],
            "gw_points": standing[4],
            "gw_transfers": standing[5],
            "total_points": standing[10],
            "overall_rank": standing[11]
        }

        # print(live_standing_data)

        try:
            result = get_or_update_record("live_standings", live_standing_data, "fpl_team,gameweek")
            # print(result)
        except requests.RequestException as e:
            print(f"Error occurred: {e}")

def get_gw_transfers(team_id, gameweek):
    try:
        transfers = con.execute(f"""
            SELECT event_transfers
            FROM read_csv_auto('teams/{team_id}/{TEAM_SEASON}/gws.csv')
            WHERE event = {gameweek}
        """).fetchone()
        return transfers[0] if transfers else 0
    except Exception as e:
        print(f"Error fetching transfers for team {team_id}: {str(e)}")
        return 0

def calculate_team_stats(team_players, live_data):
    total_goals = 0
    total_assists = 0
    total_clean_sheets = 0
    total_yellow_cards = 0

    for player_id, player_info in team_players.items():
        # Skip bench players (positions 12-15)
        if player_info['position'] > 11:
            continue

        player_data = live_data.get(player_id, {})
        total_goals += player_data.get('goals_scored', 0)
        total_assists += player_data.get('assists', 0)
        total_clean_sheets += player_data.get('clean_sheets', 0)
        total_yellow_cards += player_data.get('yellow_cards', 0)

    return total_goals, total_assists, total_clean_sheets, total_yellow_cards

def fetch_fpl_teams():
    url = f"{POCKETBASE_URL}/api/collections/fpl_teams/records"
    response = requests.get(url)
    response.raise_for_status()
    teams = response.json()['items']
    return {team['team_id']: team['id'] for team in teams}

def main():
    load_data()
    player_teams = get_player_teams()
    fpl_teams_mapping = fetch_fpl_teams()

    while True:
        league_teams = get_league_teams(LEAGUE_ID)
        print(f"Fetched {len(league_teams)} teams for league {LEAGUE_ID}")

        live_data = get_live_data(CURRENT_GAMEWEEK)
        print(f"Fetched live data for {len(live_data)} players")

        fixtures = get_fixtures_for_gameweek(CURRENT_GAMEWEEK)
        print(f"Fetched {len(fixtures)} fixtures for gameweek {CURRENT_GAMEWEEK}")

        bonus_points = calculate_bonus_points(live_data, fixtures, player_teams)
        print(f"Calculated bonus points for {len(bonus_points)} players")

        standings = []
        for team in league_teams:
            team_players = get_team_players(team['entry'], CURRENT_GAMEWEEK)
            gw_points = calculate_gw_points(team_players, live_data, bonus_points)
            gw_transfers = get_gw_transfers(team['entry'], CURRENT_GAMEWEEK)
            goals, assists, clean_sheets, yellow_cards = calculate_team_stats(team_players, live_data)

            standings.append([
                0,  # Placeholder for rank
                team['entry'],
                team['team_name'],
                team['player_name'],
                gw_points,
                gw_transfers,
                goals,
                assists,
                clean_sheets,
                yellow_cards,
                team['total_points'] + gw_points,  # Updating total points with live GW points
                team['overall_rank']
            ])

        # Sort standings and update ranks
        standings.sort(key=lambda x: x[4], reverse=True)  # Sort by total points
        for i, standing in enumerate(standings, 1):
            standing[0] = i

        print("\nLive FPL League Standings:")
        print(tabulate(standings, headers=[
            "Rank", "Team ID", "Team Name", "Manager", "GW Points", "GW Transfers",
            "Goals", "Assists", "Clean Sheets", "Yellow Cards", "Total Points", "Overall Rank"
        ], tablefmt="grid"))

        update_live_standings(standings, fpl_teams_mapping)

        print(f"\nLast updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("Updating in 60 seconds...")
        time.sleep(60)  # Update every 60 seconds

if __name__ == "__main__":
    main()
