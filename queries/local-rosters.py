import duckdb
import requests
from tabulate import tabulate

# Constants
BASE_URL = "https://fantasy.premierleague.com/api/"
LEAGUE_ID = 820322
TEAM_SEASON = "24_25"  # Format used in team directories
DATA_SEASON = "2024-25"  # Format used in data directory
GAMEWEEK = 2  # Update this for the desired gameweek

# Initialize DuckDB connection
con = duckdb.connect(':memory:')

def get_league_teams(league_id):
    print(f"Fetching teams for league ID: {league_id} from FPL API...")
    url = f"{BASE_URL}leagues-classic/{league_id}/standings/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching league data. Status code: {response.status_code}")
        return []
    data = response.json()
    return [(team['entry'], team['entry_name']) for team in data['standings']['results']]
    #return [team['entry'] for team in data['standings']['results']]

def load_csv_files():
    print("Loading CSV files...")
    # Load team data
    con.execute(f"CREATE TABLE teams AS SELECT * FROM read_csv_auto('data/{DATA_SEASON}/teams.csv')")
    
    # Load player data
    con.execute(f"CREATE TABLE players AS SELECT * FROM read_csv_auto('data/{DATA_SEASON}/players_raw.csv')")
    
    # Load fixtures data
    con.execute(f"CREATE TABLE fixtures AS SELECT * FROM read_csv_auto('data/{DATA_SEASON}/fixtures.csv')")

def get_team_roster(team_id):
    print(f"Fetching roster for team ID: {team_id}...")
    return con.execute(f"""
        SELECT p.element, p.position, p.multiplier, p.is_captain, p.is_vice_captain,
               pl.first_name, pl.second_name, pl.element_type, pl.now_cost, pl.total_points,
               t.name AS team_name
        FROM read_csv_auto('teams/{team_id}/{TEAM_SEASON}/picks_{GAMEWEEK}.csv') p
        JOIN players pl ON p.element = pl.id
        JOIN teams t ON pl.team = t.id
        ORDER BY p.position
    """).fetchall()

def get_fixtures_for_gameweek():
    print(f"Fetching fixtures for gameweek {GAMEWEEK}...")
    return con.execute(f"""
        SELECT team_h, team_a, team_h_difficulty, team_a_difficulty
        FROM fixtures
        WHERE event = {GAMEWEEK}
    """).fetchall()

def get_player_name(team_id):
    print(f"Fetching player name for team ID: {team_id}...")
    url = f"{BASE_URL}entry/{team_id}/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching player data. Status code: {response.status_code}")
        return "Unknown Manager"
    data = response.json()
    return f"{data['player_first_name']} {data['player_last_name']}"

def main():
    print(f"Starting FPL Roster Analysis for Gameweek {GAMEWEEK}")

    # Fetch league teams from API
    league_teams = get_league_teams(LEAGUE_ID)
    if not league_teams:
        print("Failed to fetch league teams. Exiting.")
        return

    print(league_teams)
    load_csv_files()
    fixtures = get_fixtures_for_gameweek()

    for team_id, team_name in league_teams:
        try:
            roster = get_team_roster(team_id)
            player_name = get_player_name(team_id)
            
            print(f"\nRoster for Team ID: {team_name} (ID: {team_id}")
            print(f"Manager: {player_name}")
            
            table_data = []
            for player in roster:
                element, position, multiplier, is_captain, is_vice_captain, first_name, second_name, element_type, now_cost, total_points, team_name = player
                
                # Find fixture difficulty for the player's team
                difficulty = next((f[2] if f[0] == team_name else f[3] for f in fixtures if f[0] == team_name or f[1] == team_name), "N/A")
                
                role = "Captain" if is_captain else "Vice Captain" if is_vice_captain else ""
                position_name = ["GK", "DEF", "MID", "FWD"][element_type - 1]
                
                table_data.append([
                    position,
                    f"{first_name} {second_name}",
                    position_name,
                    team_name,
                    difficulty,
                    now_cost / 10,  # Convert to actual cost
                    total_points,
                    multiplier,
                    role
                ])
            
            print(tabulate(table_data, headers=[
                "Pick", "Name", "Position", "Team", "Difficulty", "Cost", "Total Points", "Multiplier", "Role"
            ], tablefmt="grid"))
        except Exception as e:
            print(f"Error processing team {team_id}: {str(e)}")

    print("\nScript execution completed.")

if __name__ == "__main__":
    main()
