import { Database } from "duckdb-async";
import { createCanvas } from "canvas";
import { writeFileSync } from "fs";

const START_HOUR = 6;
const END_HOUR = 18;
const BLOCKS_PER_HOUR = 4; // 15-minute blocks
const BLOCK_WIDTH = 20; // Width per 15-minute block
const ROW_HEIGHT = 30;
const MATCHWEEK_SEPARATOR = 20;
const TOP_MARGIN = 60;
const LEFT_MARGIN = 160;
const width = 1200;

interface Fixture {
  id: number;
  start_time: string;
  end_time: string;
  team_h: string;
  team_a: string;
  team_h_score: number;
  team_a_score: number;
  event: number;
}

interface DayGroup {
  matchweek: number;
  date: Date;
  windows: TimeWindow[];
}

interface TimeWindow {
  start: Date;
  end: Date;
  activeMatches: Fixture[];
}

// Helper functions for date/time calculations
function formatMatchDayLabel(date: Date, matchweek: number): string {
  const dayName = date
    .toLocaleDateString("en-US", { weekday: "short" })
    .toUpperCase();
  const monthName = date
    .toLocaleDateString("en-US", { month: "short" })
    .toUpperCase();
  const day = date.getDate();
  return `${dayName} ${monthName} ${day}, MW ${matchweek}`;
}

async function main() {
  const db = await Database.create(":memory:");

  // create fixtures table
  await db.all(`
    CREATE TABLE fixtures (
      code INTEGER,
      event INTEGER,
      finished BOOLEAN,
      finished_provisional BOOLEAN,
      id INTEGER,
      kickoff_time TIMESTAMP,
      minutes INTEGER,
      provisional_start_time BOOLEAN,
      started BOOLEAN,
      team_a INTEGER,
      team_a_score FLOAT,
      team_h INTEGER,
      team_h_score FLOAT,
      stats VARCHAR,
      team_h_difficulty INTEGER,
      team_a_difficulty INTEGER,
      pulse_id INTEGER
    );
  `);

  // load fixtures data
  await db.all(`
    COPY fixtures FROM 'data/2024-25/fixtures.csv' (HEADER true, DELIMITER ',');
  `);

  // query to find all match windows. assume 2-hour window for each match
  // 90+ min match time plus post-match sports-talking
  const matchWindows = (await db.all(`
    WITH match_times AS (
      SELECT
        id,
        event,
        kickoff_time as start_time,
        kickoff_time + INTERVAL 120 MINUTE as end_time,
        team_h,
        team_a,
        team_h_score,
        team_a_score
      FROM fixtures
      ORDER BY kickoff_time
    )
    SELECT *
    FROM match_times
    ORDER BY start_time;
  `)) as Fixture[];

  // find overlapping windows
  const timeWindows: TimeWindow[] = [];
  let currentWindow: TimeWindow | null = null;

  for (const match of matchWindows) {
    const matchStart = new Date(match.start_time);
    const matchEnd = new Date(match.end_time);

    if (!currentWindow) {
      currentWindow = {
        start: matchStart,
        end: matchEnd,
        activeMatches: [match],
      };
      timeWindows.push(currentWindow);
      continue;
    }

    // if it starts after current window ends, create new window
    if (matchStart > currentWindow.end) {
      currentWindow = {
        start: matchStart,
        end: matchEnd,
        activeMatches: [match],
      };
      timeWindows.push(currentWindow);
    } else {
      // extend current window if needed.
      // this probably won't happen but AI suggested it, so why not
      currentWindow.end = new Date(
        Math.max(currentWindow.end.getTime(), matchEnd.getTime()),
      );
      currentWindow.activeMatches.push(match);
    }
  }

  // group matches by day and matchweek.
  // we want to draw each day separately but include matchweek label
  // and visual grouping between matchweeks
  const dayGroups: DayGroup[] = [];
  timeWindows.sort((a, b) => a.start.getTime() - b.start.getTime());

  timeWindows.forEach((window) => {
    const matchweek = window.activeMatches[0].event;
    const startDate = new Date(window.start);
    startDate.setHours(0, 0, 0, 0);

    let dayGroup = dayGroups.find(
      (group) =>
        group.matchweek === matchweek &&
        group.date.getTime() === startDate.getTime(),
    );

    if (!dayGroup) {
      dayGroup = {
        matchweek,
        date: startDate,
        windows: [],
      };
      dayGroups.push(dayGroup);
    }

    dayGroup.windows.push(window);
  });

  // sort day groups
  dayGroups.sort((a, b) => {
    if (a.matchweek !== b.matchweek) {
      return a.matchweek - b.matchweek;
    }
    return a.date.getTime() - b.date.getTime();
  });

  // calculate required height
  let totalHeight = TOP_MARGIN;
  let previousMatchweek = -1;

  dayGroups.forEach((dayGroup) => {
    if (dayGroup.matchweek !== previousMatchweek) {
      if (previousMatchweek !== -1) {
        totalHeight += MATCHWEEK_SEPARATOR;
      }
      previousMatchweek = dayGroup.matchweek;
    }
    totalHeight += ROW_HEIGHT;
  });

  totalHeight += TOP_MARGIN;

  // create canvas with calculated height
  const canvas = createCanvas(width, totalHeight);
  const ctx = canvas.getContext("2d");

  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, width, totalHeight);

  // draw hour labels and grid
  ctx.fillStyle = "#000000";
  ctx.font = "12px Arial";
  ctx.textAlign = "center";

  for (let hour = START_HOUR; hour <= END_HOUR; hour++) {
    const hourX =
      LEFT_MARGIN + (hour - START_HOUR) * (BLOCK_WIDTH * BLOCKS_PER_HOUR);

    // draw vertical gridline for hour
    ctx.strokeStyle = "#eaeaea";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(hourX, TOP_MARGIN);
    ctx.lineTo(hourX, totalHeight);
    ctx.stroke();

    // draw 15-minute gridlines
    for (let block = 1; block < BLOCKS_PER_HOUR; block++) {
      const blockX = hourX + block * BLOCK_WIDTH;
      ctx.strokeStyle = "#f5f5f5";
      ctx.beginPath();
      ctx.moveTo(blockX, TOP_MARGIN);
      ctx.lineTo(blockX, totalHeight);
      ctx.stroke();
    }

    // draw hour label
    const displayHour = hour > 12 ? hour - 12 : hour;
    const period = hour >= 12 ? "PM" : "AM";
    const hourStr = `${displayHour}${period}`;
    ctx.fillText(hourStr, hourX, TOP_MARGIN - 20);
  }

  // draw days and their windows
  let currentY = TOP_MARGIN;
  previousMatchweek = -1;

  dayGroups.forEach((dayGroup) => {
    // add extra spacing between matchweeks to visually separate them
    if (dayGroup.matchweek !== previousMatchweek) {
      if (previousMatchweek !== -1) {
        currentY += MATCHWEEK_SEPARATOR;
      }
      previousMatchweek = dayGroup.matchweek;
    }

    // draw matchweek and date labels
    ctx.fillStyle = "#000000";
    ctx.textAlign = "right";
    ctx.font = "12px Arial";
    const dateLabel = formatMatchDayLabel(dayGroup.date, dayGroup.matchweek);
    ctx.fillText(dateLabel, LEFT_MARGIN - 10, currentY + ROW_HEIGHT / 2 + 4);

    // draw horizontal gridline
    ctx.strokeStyle = "#eaeaea";
    ctx.beginPath();
    ctx.moveTo(LEFT_MARGIN, currentY);
    ctx.lineTo(width, currentY);
    ctx.stroke();

    // draw time windows for this day
    dayGroup.windows.forEach((window) => {
      const startHour = window.start.getHours();
      if (startHour < START_HOUR || startHour > END_HOUR) return;

      const endHour = window.end.getHours();

      const startX =
        LEFT_MARGIN +
        ((Math.max(startHour, START_HOUR) - START_HOUR) * BLOCKS_PER_HOUR +
          Math.floor(window.start.getMinutes() / 15)) *
          BLOCK_WIDTH;

      const endX =
        LEFT_MARGIN +
        ((Math.min(endHour, END_HOUR) - START_HOUR) * BLOCKS_PER_HOUR +
          Math.ceil(window.end.getMinutes() / 15)) *
          BLOCK_WIDTH;

      const windowWidth = endX - startX;

      // color based on number of simultaneous matches
      const intensity = Math.min(255, 50 + window.activeMatches.length * 50);
      ctx.fillStyle = `rgb(0, ${intensity}, ${255 - intensity})`;

      // draw rounded rectangle for the time window
      const barHeight = ROW_HEIGHT * 0.6;
      const barY = currentY + (ROW_HEIGHT - barHeight) / 2;
      const radius = 4;

      ctx.beginPath();
      ctx.moveTo(startX + radius, barY);
      ctx.lineTo(endX - radius, barY);
      ctx.quadraticCurveTo(endX, barY, endX, barY + radius);
      ctx.lineTo(endX, barY + barHeight - radius);
      ctx.quadraticCurveTo(
        endX,
        barY + barHeight,
        endX - radius,
        barY + barHeight,
      );
      ctx.lineTo(startX + radius, barY + barHeight);
      ctx.quadraticCurveTo(
        startX,
        barY + barHeight,
        startX,
        barY + barHeight - radius,
      );
      ctx.lineTo(startX, barY + radius);
      ctx.quadraticCurveTo(startX, barY, startX + radius, barY);
      ctx.fill();

      // add match count if there's enough space
      if (windowWidth > 30) {
        ctx.fillStyle = "#000000";
        ctx.font = "10px Arial";
        ctx.textAlign = "center";
        ctx.fillText(
          `${window.activeMatches.length}`,
          startX + windowWidth / 2,
          barY + barHeight / 2 + 3,
        );
      }
    });

    currentY += ROW_HEIGHT;
  });

  const buffer = canvas.toBuffer("image/png");
  writeFileSync("match_windows.png", buffer);

  console.log(`Found ${timeWindows.length} distinct match windows`);
  timeWindows.forEach((window, i) => {
    console.log(`\nWindow ${i + 1}:`);
    console.log(`Start: ${window.start.toISOString()}`);
    console.log(`End: ${window.end.toISOString()}`);
    console.log(`Active matches: ${window.activeMatches.length}`);
  });

  await db.close();
}

main().catch(console.error);
