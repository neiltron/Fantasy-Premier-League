import requests
import duckdb
from tabulate import tabulate
from urllib.parse import urlencode
import time
from collections import defaultdict

FPL_API_URL = "https://fantasy.premierleague.com/api"
POCKETBASE_URL = "https://pb.growcup.lol"
LEAGUE_ID = 820322
CURRENT_GAMEWEEK = 6
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

        print(f"Player name: {player_info}, ID: {player_id}")

        player_data = live_data.get(player_id, {})

        # Use official bonus points if available, otherwise use estimated bonus points
        player_bonus = player_data.get('bonus', 0) or bonus_points.get(player_id, 0)

        player_points = player_data.get('total_points', 0)

        # Only add estimated bonus points if there are no official bonus points
        if player_data.get('bonus', 0) == 0:
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
        team_home = con.execute(f"SELECT name FROM teams WHERE id='{team_h}'").fetchall()[0][0]
        team_away = con.execute(f"SELECT name FROM teams WHERE id='{team_a}'").fetchall()[0][0]
        print(f"Processing fixture {fixture_id}: {team_home} ({team_h}) vs {team_away} ({team_a})")

        fixture_players = defaultdict(list)
        skip_fixture = False

        # First, check if any player in this fixture has official bonus points
        for player_id, stats in live_data.items():
            player_team = player_teams.get(int(player_id))
            if player_team in [team_h, team_a]:
                if stats.get('bonus', 0) > 0:
                    skip_fixture = True
                    print(f"Fixture {fixture_id} has official bonus points assigned. Skipping estimation.")
                    break

        if skip_fixture:
            # Assign official bonus points for this fixture
            for player_id, stats in live_data.items():
                player_team = player_teams.get(int(player_id))
                if player_team in [team_h, team_a]:
                    official_bonus = stats.get('bonus', 0)
                    if official_bonus > 0:
                        player_name = con.execute(f"SELECT full_name FROM players WHERE id='{player_id}'").fetchall()[0][0]
                        bonus_points[player_id] = official_bonus
                        print(f"Player {player_name} ({player_id}) has {official_bonus} official bonus points")
            continue

        # If no official bonus points, proceed with estimation
        for player_id, stats in live_data.items():
            player_name = con.execute(f"SELECT full_name FROM players WHERE id='{player_id}'").fetchall()
            if not player_name:
                print(f"Warning: Player {player_id} not found in players table")
                continue
            player_name = player_name[0][0]

            player_team = player_teams.get(int(player_id))
            if player_team is None:
                print(f"Warning: Player {player_name} ({player_id}) not found in player_teams")
                continue
            if player_team in [team_h, team_a]:
                bps = stats.get('bps', 0)
                fixture_players[fixture_id].append((player_id, bps))
                print(f"Player {player_name} ({player_id}) (Team {player_team}) added to fixture {fixture_id} with BPS {bps}")

        print(f"Number of players in fixture {fixture_id}: {len(fixture_players[fixture_id])}")

        # Sort players by BPS and assign estimated bonus points
        sorted_players = sorted(fixture_players[fixture_id], key=lambda x: x[1], reverse=True)
        print(f"Top 3 players for fixture {fixture_id}: {sorted_players[:3]}")
        for i, (player_id, bps) in enumerate(sorted_players[:3]):
            player_name = con.execute(f"SELECT full_name FROM players WHERE id='{player_id}'").fetchall()[0][0]
            bonus_points[player_id] = 3 - i
            print(f"Assigning {3-i} estimated bonus points to player {player_name} ({player_id}) (BPS: {bps})")

    print(f"Total bonus points assigned (official + estimated): {len(bonus_points)}")
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

def update_player_stats(player_id, gameweek, stats):
    player_stats_data = {
        "player": player_id,
        "gameweek": gameweek,
        "points": stats.get('total_points', 0),
        "bps": stats.get('bps', 0),
        "goals": stats.get('goals_scored', 0),
        "assists": stats.get('assists', 0),
        "clean_sheets": stats.get('clean_sheets', 0),
        "minutes": stats.get('minutes', 0)
    }

    try:
        result = get_or_update_record("player_stats", player_stats_data, "player,gameweek")
    except requests.RequestException as e:
        print(f"Error occurred: {e}")


def get_or_update_record(collection, data, unique_field):
    url = f"{POCKETBASE_URL}/api/collections/{collection}/records"

    # Handle composite keys
    if ',' in unique_field:
        filter_conditions = []
        for field in unique_field.split(','):
            if field == 'gameweek':
                condition = f"({field}={data[field]})"
            elif field == 'player':
                condition = f"(player.id='{data[field]}')"
            else:
                condition = f"({field}='{data[field]}')"
            filter_conditions.append(condition)

        filter_string = " && ".join(filter_conditions)
    else:
        filter_string = f"({unique_field}='{data[unique_field]}')"

    params = { 'filter': filter_string }

    # Try to get the existing record
    response = requests.get(f"{url}?{urlencode(params)}")
    response.raise_for_status()  # Raise an exception for bad responses

    if response.json()['items']:
        # Update existing record
        record_id = response.json()['items'][0]['id']
        update_url = f"{url}/{record_id}"
        # print(f"Updating record in {collection}: {record_id}")
        response = requests.patch(update_url, json=data)

        action = "updated"
    else:
        # Create new record
        print(f"Creating new record in {collection}")
        response = requests.post(url, json=data)
        action = "created"

    response.raise_for_status()  # Raise an exception for bad responses

    print(f"Successfully {action} record in {collection} {data} {data.get('gameweek', '')}")
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

def fetch_players():
    players = []
    for page in range(1, 3):
        url = f"{POCKETBASE_URL}/api/collections/players/records?page={page}&perPage=500"
        response = requests.get(url)
        response.raise_for_status()
        players.extend(response.json()['items'])
    return {player['player_id']: player['id'] for player in players}


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

        players_mapping = fetch_players()


        for player_id, stats in live_data.items():
            if int(player_id) in players_mapping:
                update_player_stats(players_mapping[int(player_id)], CURRENT_GAMEWEEK, stats)


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
