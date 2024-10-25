import duckdb
import requests
from tabulate import tabulate
import time
from datetime import datetime, timezone


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

def get_league_teams(league_id):
    print(f"Fetching teams for league ID: {league_id}...")
    url = f"{BASE_URL}leagues-classic/{league_id}/standings/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching league data. Status code: {response.status_code}")
        return []
    data = response.json()
    return [(team['entry'], team['entry_name']) for team in data['standings']['results']]

def get_fixtures_for_gameweek(gameweek):
    return con.execute(f"""
        SELECT
            id AS fixture_id,
            team_h AS home_team_id,
            team_a AS away_team_id,
            team_h_score,
            team_a_score,
            kickoff_time
        FROM fixtures
        WHERE event = {gameweek}
    """).fetchall()

def get_live_data(gameweek):
    url = f"{BASE_URL}event/{gameweek}/live/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching live data. Status code: {response.status_code}")
        return {}
    data = response.json()['elements']
    return {str(element['id']): element['stats'] for element in data}


def get_team_players(team_id, team_name, gameweek):
    players = con.execute(f"""
        SELECT p.element, pl.full_name, t.short_name AS team_name, p.is_captain
        FROM read_csv_auto('teams/{team_id}/{TEAM_SEASON}/picks_{gameweek}.csv') p
        JOIN players pl ON p.element = pl.id
        JOIN teams t ON pl.team = t.id
    """).fetchall()
    return [(player[0], player[1], player[2], team_name, player[3]) for player in players]

def create_match_tables(league_players, fixtures, live_data, gameweek):
    match_tables = {}
    for fixture in fixtures:
        fixture_id, home_team_id, away_team_id, home_score, away_score, kickoff_time = fixture
        home_team_name = con.execute(f"SELECT name FROM teams WHERE id = {home_team_id}").fetchone()[0]
        away_team_name = con.execute(f"SELECT name FROM teams WHERE id = {away_team_id}").fetchone()[0]

        match_tables[fixture_id] = {
            'home_team': home_team_name,
            'away_team': away_team_name,
            'home_score': home_score or 0,
            'away_score': away_score or 0,
            'kickoff_time': kickoff_time,
            'players': {}
        }

    for player in league_players:
        player_id, player_name, team_name, fpl_team_name, is_captain = player
        player_fixtures = con.execute(f"""
            SELECT id
            FROM fixtures
            WHERE event = {gameweek} AND (team_h = (SELECT team FROM players WHERE id = {player_id}) OR team_a = (SELECT team FROM players WHERE id = {player_id}))
        """).fetchone()

        if player_fixtures:
            fixture_id = player_fixtures[0]
            player_live_data = live_data.get(str(player_id), {})

            if player_id not in match_tables[fixture_id]['players']:
                match_tables[fixture_id]['players'][player_id] = {
                    'name': player_name,
                    'team': team_name,
                    'points': player_live_data.get('total_points', 0),
                    'bps': player_live_data.get('bps', 0),
                    'minutes': player_live_data.get('minutes', 0),
                    'fpl_teams': set()
                }
            fpl_team_entry = fpl_team_name[:5] + ('*' if is_captain else '')
            match_tables[fixture_id]['players'][player_id]['fpl_teams'].add(fpl_team_entry)

    # Convert players dict back to list for each fixture and join FPL team names
    for fixture in match_tables.values():
        fixture['players'] = [
            {**player, 'fpl_teams': ', '.join(sorted(player['fpl_teams']))}
            for player in fixture['players'].values()
        ]

    return match_tables

def display_match_tables(match_tables):
    for fixture_id, match_data in match_tables.items():
        home_team = match_data['home_team']
        away_team = match_data['away_team']
        score = f"{match_data['home_score']} - {match_data['away_score']}"
        kickoff_time = match_data['kickoff_time']

        if isinstance(kickoff_time, str):
            kickoff_time = datetime.strptime(kickoff_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        elif isinstance(kickoff_time, datetime):
            if kickoff_time.tzinfo is None:
                kickoff_time = kickoff_time.replace(tzinfo=timezone.utc)

        current_time = datetime.now(timezone.utc)
        minutes_played = max(0, int((current_time - kickoff_time).total_seconds() / 60))

        print(f"\n{home_team} vs {away_team} ({minutes_played} mins)")
        # print(f"Score: {score} ({minutes_played} mins)")

        table_data = [
            [player['name'], player['team'], player['points'], player['bps'], player['minutes'], player['fpl_teams']]
            for player in match_data['players']
        ]
        print(tabulate(table_data, headers=["Player", "Team", "Points", "BPS", "Minutes", "FPL Teams"], tablefmt="grid"))

def main():
    load_data()

    league_id = input("Enter FPL league ID: ")
    gameweek = int(input("Enter current gameweek: "))

    league_teams = get_league_teams(league_id)
    league_players = []
    for team_id, team_name in league_teams:
        league_players.extend(get_team_players(team_id, team_name, gameweek))

    fixtures = get_fixtures_for_gameweek(gameweek)

    while True:
        live_data = get_live_data(gameweek)
        match_tables = create_match_tables(league_players, fixtures, live_data, gameweek)
        display_match_tables(match_tables)

        time.sleep(60)  # Update every 60 seconds



if __name__ == "__main__":
    main()
