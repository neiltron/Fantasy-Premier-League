import requests
import duckdb
import json

POCKETBASE_URL = "https://pb.growcup.lol/"
FPL_API_URL = "https://fantasy.premierleague.com/api"
LEAGUE_ID = 820322
CURRENT_GAMEWEEK = 9
PREVIOUS_GAMEWEEK = CURRENT_GAMEWEEK - 1
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

def get_or_create_record(collection, data, unique_field):
    url = f"{POCKETBASE_URL}api/collections/{collection}/records"

    print(data)

    # Try to create the record
    create_response = requests.post(url, json=data)

    print(create_response.status_code)
    if create_response.status_code == 200 or create_response.status_code == 201:
        print(f"Successfully created new record in {collection}")
        return create_response.json()

    # If creation failed, try to update
    filter_conditions = []
    for field in unique_field.split(','):
        if isinstance(data[field], str):
            filter_conditions.append(f"({field}='{data[field]}')")
        else:
            filter_conditions.append(f"{field}={data[field]}")

    filter_string = " && ".join(filter_conditions)

    get_response = requests.get(f"{url}?filter={filter_string}")

    print(filter_string)

    if get_response.status_code == 200 and get_response.json()['items']:
        record_id = get_response.json()['items'][0]['id']
        update_url = f"{url}/{record_id}"
        update_response = requests.patch(update_url, json=data)

        if update_response.status_code == 200:
            print(f"Successfully updated record in {collection}")
            return update_response.json()
        else:
            print(f"Error updating {collection}: {update_response.text}")
            return None
    else:
        print(f"Error finding existing record in {collection}: {get_response.text}")
        return None

def get_or_create_roster_record(collection, data, unique_field):
    url = f"{POCKETBASE_URL}api/collections/{collection}/records"

    print(data)

    # Try to create the record
    create_response = requests.post(url, json=data)

    print(create_response.status_code)
    if create_response.status_code == 200 or create_response.status_code == 201:
        print(f"Successfully created new record in {collection}")
        return create_response.json()

    # If creation failed, try to update
    filter_conditions = []
    for field in unique_field.split(','):
        print(field)
        if field == 'fpl_team':
            filter_conditions.append(f"(fpl_team.id='{data[field]}')")
        elif field == 'player':
            filter_conditions.append(f"(player.id='{data[field]}')")
        elif isinstance(data[field], str):
            filter_conditions.append(f"({field}='{data[field]}')")
        else:
            filter_conditions.append(f"{field}={data[field]}")

    filter_string = " && ".join(filter_conditions)

    get_response = requests.get(f"{url}?filter={filter_string}")

    print(filter_string)

    if get_response.status_code == 200 and get_response.json()['items']:
        record_id = get_response.json()['items'][0]['id']
        update_url = f"{url}/{record_id}"
        update_response = requests.patch(update_url, json=data)

        if update_response.status_code == 200:
            print(f"Successfully updated record in {collection}")
            return update_response.json()
        else:
            print(f"Error updating {collection}: {update_response.text}")
            return None
    else:
        print(f"Error finding existing record in {collection}: {get_response.text}")
        return None

def get_or_create_player_stats(data):
    collection = 'player_stats'
    url = f"{POCKETBASE_URL}api/collections/{collection}/records"

    print(data)

    # Try to create the record
    create_response = requests.post(url, json=data)

    print(create_response.status_code)
    if create_response.status_code == 200 or create_response.status_code == 201:
        print(f"Successfully created new record in {collection}")
        return create_response.json()

    # If creation failed, try to update
    filter_conditions = []

    filter_conditions.append(f"player.id='{data['player']}'")
    filter_conditions.append(f"gameweek={data['gameweek']}")

    filter_string = " && ".join(filter_conditions)

    get_response = requests.get(f"{url}?filter={filter_string}")

    if get_response.status_code == 200 and get_response.json()['items']:
        record_id = get_response.json()['items'][0]['id']
        update_url = f"{url}/{record_id}"
        update_response = requests.patch(update_url, json=data)

        if update_response.status_code == 200:
            print(f"Successfully updated record in {collection}")
            return update_response.json()
        else:
            print(f"Error updating {collection}: {update_response.text}")
            return None
    else:
        print(f"Error finding existing record in {collection}: {get_response.text}")
        return None


def get_league_data():
    url = f"{FPL_API_URL}/leagues-classic/{LEAGUE_ID}/standings/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching league data: {response.text}")
        return None
    return response.json()

def update_league(league_data):
    league_record = {
        "league_id": LEAGUE_ID,
        "name": league_data['league']['name']
    }
    return get_or_create_record("leagues", league_record, "league_id")

def update_fpl_teams(league_data, league_record):
    for team in league_data['standings']['results']:
        team_data = {
            "team_id": team['entry'],
            "name": team['entry_name'],
            "manager_name": team['player_name'],
            "league": league_record['id']
        }
        get_or_create_record("fpl_teams", team_data, "team_id")

def update_players():
    players_data = con.execute("""
        SELECT p.id as player_id, full_name as name,
               CASE
                   WHEN element_type = 1 THEN 'GK'
                   WHEN element_type = 2 THEN 'DEF'
                   WHEN element_type = 3 THEN 'MID'
                   WHEN element_type = 4 THEN 'FWD'
               END as position,
               t.name as team
        FROM players p
        JOIN teams t ON p.team = t.id
    """).fetchall()

    for player in players_data:
        player_data = {
            "player_id": player[0],
            "name": player[1],
            "position": player[2],
            "team": player[3]
        }
        get_or_create_record("players", player_data, "player_id")

def update_rosters(league_data):
    # Get all FPL team IDs from our league data
    fpl_teams = [team['entry'] for team in league_data['standings']['results']]

    for team_id in fpl_teams:
        try:
            roster_data = con.execute(f"""
                SELECT
                    element as player,
                    position,
                    multiplier,
                    is_captain,
                    is_vice_captain
                FROM read_csv_auto('teams/{team_id}/{TEAM_SEASON}/picks_{CURRENT_GAMEWEEK}.csv')
            """).fetchall()

            # Fetch the Pocketbase record ID for this FPL team
            fpl_team_response = requests.get(f"{POCKETBASE_URL}api/collections/fpl_teams/records?filter=team_id={team_id}")
            if fpl_team_response.status_code != 200 or not fpl_team_response.json()['items']:
                print(f"FPL team with ID {team_id} not found in Pocketbase. Skipping roster update.")
                continue
            fpl_team_record_id = fpl_team_response.json()['items'][0]['id']

            for roster in roster_data:
                player_id, position, multiplier, is_captain, is_vice_captain = roster

                # Fetch the Pocketbase record ID for this player
                player_response = requests.get(f"{POCKETBASE_URL}api/collections/players/records?filter=player_id={player_id}")
                if player_response.status_code != 200 or not player_response.json()['items']:
                    print(f"Player with ID {player_id} not found in Pocketbase. Skipping this player in roster update.")
                    continue
                player_record_id = player_response.json()['items'][0]['id']

                roster_record = {
                    "fpl_team": fpl_team_record_id,
                    "player": player_record_id,
                    "gameweek": CURRENT_GAMEWEEK,
                    "position": position,
                    "multiplier": multiplier,
                    "is_captain": int(is_captain),  # Ensure boolean value
                    "is_vice_captain": int(is_vice_captain)  # Ensure boolean value
                }
                get_or_create_roster_record("rosters", roster_record, "fpl_team,player,gameweek")
        except Exception as e:
            print(f"Error processing roster for team {team_id}: {str(e)}")


def update_player_stats():
    stats_data = con.execute(f"""
        SELECT
            element as player,
            total_points as points,
            bps,
            goals_scored as goals,
            assists,
            clean_sheets,
            minutes
        FROM read_csv_auto('data/{DATA_SEASON}/gws/gw{PREVIOUS_GAMEWEEK}.csv')
    """).fetchall()

    for stat in stats_data:
        player_id, points, bps, goals, assists, clean_sheets, minutes = stat

        # Check if player exists in the players collection
        player_check = requests.get(f"{POCKETBASE_URL}api/collections/players/records?filter=player_id={player_id}")
        if player_check.status_code != 200 or not player_check.json()['items']:
            print(f"Player with ID {player_id} not found in players collection. Skipping stats update.")
            continue

        player_record_id = player_check.json()['items'][0]['id']

        stat_record = {
            "player": player_record_id,
            "gameweek": PREVIOUS_GAMEWEEK,
            "points": points if points is not None else 0,
            "bps": bps if bps is not None else 0,
            "goals": goals if goals is not None else 0,
            "assists": assists if assists is not None else 0,
            "clean_sheets": clean_sheets if clean_sheets is not None else 0,
            "minutes": minutes if minutes is not None else 0
        }

        # Use player_id and gameweek as unique identifier
        try:
            response = get_or_create_player_stats(stat_record)
        except Exception as e:
            print(f"Error updating stats for player {player_id}: {str(e)}")


def main():
    load_data()

    league_data = get_league_data()
    if league_data is None:
        print("Failed to fetch league data. Exiting.")
        return

    league_record = update_league(league_data)
    # update_fpl_teams(league_data, league_record)
    # update_players()
    update_rosters(league_data)
    update_player_stats()

    print("Pocketbase update completed.")

if __name__ == "__main__":
    main()
