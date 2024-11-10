import { Database } from "duckdb-async";

// Constants
const DATA_SEASON = "2024-25";
const TEAM_SEASON = "24_25";
const GAMEWEEK = 11;
const FPL_API_ENDPOINT = `https://fantasy.premierleague.com/api/event/${GAMEWEEK}/live/`;

// Types for FPL API response
interface PlayerStats {
  minutes: number;
  goals_scored: number;
  assists: number;
  penalties_saved: number;
  penalties_missed: number;
  yellow_cards: number;
  red_cards: number;
  total_points: number;
}

interface ExplainStat {
  identifier: string;
  points: number;
  value: number;
}

interface ExplainItem {
  fixture: number;
  stats: ExplainStat[];
}

interface PlayerData {
  id: number;
  stats: PlayerStats;
  explain: ExplainItem[];
}

interface PlayerInfo {
  full_name: string;
  team_id: number;
  team_name: string;
  team_short_name: string;
}

interface FixtureInfo {
  id: number;
  team_h: number;
  team_a: number;
}

class FPLEventMonitor {
  private previousState: Map<number, PlayerStats> = new Map();
  private playerInfo: Map<number, PlayerInfo> = new Map();
  private fixtures: Map<number, FixtureInfo> = new Map();
  private db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  public async initialize(): Promise<void> {
    // Create tables from CSV files
    await this.db.all(`
      CREATE TABLE players AS
      SELECT
        id,
        first_name || ' ' || second_name AS full_name,
        team,
        element_type
      FROM read_csv_auto('data/${DATA_SEASON}/players_raw.csv')
    `);

    await this.db.all(`
      CREATE TABLE teams AS
      SELECT id, name, short_name
      FROM read_csv_auto('data/${DATA_SEASON}/teams.csv')
    `);

    await this.db.all(`
      CREATE TABLE fixtures AS
      SELECT *
      FROM read_csv_auto('data/${DATA_SEASON}/fixtures.csv')
    `);

    // Load player information with team details
    const players = await this.db.all(`
      SELECT
        p.id,
        p.full_name,
        p.team as team_id,
        t.name as team_name,
        t.short_name as team_short_name
      FROM players p
      JOIN teams t ON p.team = t.id
    `);

    // Load fixtures
    const fixtures = await this.db.all(`
      SELECT id, team_h, team_a
      FROM fixtures
    `);

    // Store in memory for quick access
    for (const player of players) {
      this.playerInfo.set(parseInt(player.id), {
        full_name: player.full_name,
        team_id: parseInt(player.team_id),
        team_name: player.team_name,
        team_short_name: player.team_short_name,
      });
    }

    for (const fixture of fixtures) {
      const _fixture: FixtureInfo = {
        id: parseInt(fixture.id),
        team_h: parseInt(fixture.team_h),
        team_a: parseInt(fixture.team_a),
      };

      this.fixtures.set(parseInt(fixture.id), _fixture);
    }
  }

  private getFixtureString(fixtureId: number): string {
    const fixture = this.fixtures.get(fixtureId);
    if (!fixture) return "Unknown Fixture";

    const homeTeam =
      [...this.playerInfo.values()].find((p) => p.team_id === fixture.team_h)
        ?.team_short_name || "Unknown";
    const awayTeam =
      [...this.playerInfo.values()].find((p) => p.team_id === fixture.team_a)
        ?.team_short_name || "Unknown";

    return `${homeTeam} vs ${awayTeam}`;
  }

  private getRelevantEvents(
    playerId: number,
    currentStats: PlayerStats,
    previousStats: PlayerStats | undefined,
    currentMinute: number,
    fixtureId: number,
  ): string[] {
    const events: string[] = [];
    const player = this.playerInfo.get(playerId);
    if (!player) return events;

    const playerDisplay = `${player.full_name} (${player.team_short_name})`;
    const fixtureDisplay = this.getFixtureString(fixtureId);

    if (previousStats) {
      // Goals
      if (currentStats.goals_scored > previousStats.goals_scored) {
        events.push(
          `⚽ ${playerDisplay} - goal at ${currentMinute}' [${fixtureDisplay}]`,
        );
      }

      // Assists
      if (currentStats.assists > previousStats.assists) {
        events.push(
          `👟 ${playerDisplay} - assist at ${currentMinute}' [${fixtureDisplay}]`,
        );
      }

      // Red cards
      if (currentStats.red_cards > previousStats.red_cards) {
        events.push(
          `🟥 ${playerDisplay} - red card at ${currentMinute}' [${fixtureDisplay}]`,
        );
      }

      // Yellow cards
      if (currentStats.yellow_cards > previousStats.yellow_cards) {
        events.push(
          `🟨 ${playerDisplay} - yellow card at ${currentMinute}' [${fixtureDisplay}]`,
        );
      }

      // Penalty saves
      if (currentStats.penalties_saved > previousStats.penalties_saved) {
        events.push(
          `🧤 ${playerDisplay} - penalty save at ${currentMinute}' [${fixtureDisplay}]`,
        );
      }

      // Penalty misses
      if (currentStats.penalties_missed > previousStats.penalties_missed) {
        events.push(
          `❌ ${playerDisplay} - penalty miss at ${currentMinute}' [${fixtureDisplay}]`,
        );
      }
    }

    return events;
  }

  public processUpdate(players: PlayerData[]): string[] {
    const allEvents: string[] = [];

    for (const player of players) {
      const previousStats = this.previousState.get(player.id);
      const currentMinute = player.stats.minutes;

      // Get fixture ID from the explain field
      const fixtureId = player.explain[0]?.fixture;

      if (fixtureId) {
        const events = this.getRelevantEvents(
          player.id,
          player.stats,
          previousStats,
          currentMinute,
          fixtureId,
        );

        allEvents.push(...events);
      }

      // Update state
      this.previousState.set(player.id, { ...player.stats });
    }

    return allEvents;
  }
}

// Example usage:
async function monitorFPLEvents() {
  const db = await Database.create(":memory:");
  const monitor = new FPLEventMonitor(db);

  try {
    // Initialize the monitor with data from CSV files
    await monitor.initialize();

    console.log("FPL Event Monitor initialized. Starting event detection...");

    while (true) {
      try {
        // Fetch data from FPL API
        const response = await fetch(FPL_API_ENDPOINT);
        const data = await response.json();
        const players: PlayerData[] = data.elements;

        // Process updates and get new events
        const newEvents = monitor.processUpdate(players);

        // Log any new events
        for (const event of newEvents) {
          console.log(new Date().toISOString(), event);
        }

        // Wait for 60 seconds before next update
        await new Promise((resolve) => setTimeout(resolve, 6000));
      } catch (error) {
        console.error("Error fetching or processing FPL data:", error);
        // Wait for 30 seconds before retrying after an error
        await new Promise((resolve) => setTimeout(resolve, 30000));
      }
    }
  } finally {
    await db.close();
  }
}

monitorFPLEvents();
