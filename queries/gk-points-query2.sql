-- Ensure the table is created (if not already done)
CREATE TABLE IF NOT EXISTS cleaned_merged_seasons AS 
SELECT * FROM read_csv_auto('./data/cleaned_merged_seasons.csv');

-- Goalkeeper points and bonus points per season
WITH goalkeeper_season_stats AS (
    SELECT 
        name,
        season_x,
        SUM(total_points) as season_points,
        SUM(bonus) as season_bonus_points,
        COUNT(*) as matches_played
    FROM cleaned_merged_seasons
    WHERE "position" = 'GK'
    GROUP BY name, season_x
),
ranked_goalkeepers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY season_x ORDER BY season_points DESC) as points_rank,
        ROW_NUMBER() OVER (PARTITION BY season_x ORDER BY season_bonus_points DESC) as bonus_points_rank
    FROM goalkeeper_season_stats
),
final_results AS (
    SELECT 
        season_x,
        name,
        season_points,
        points_rank,
        season_bonus_points,
        bonus_points_rank,
        matches_played,
        ROUND(CAST(season_points AS FLOAT) / matches_played, 2) as points_per_match,
        ROUND(CAST(season_bonus_points AS FLOAT) / matches_played, 2) as bonus_points_per_match
    FROM ranked_goalkeepers
    WHERE points_rank <= 5 OR bonus_points_rank <= 5
    UNION ALL
    SELECT 
        season_x,
        '---',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL
    FROM (SELECT DISTINCT season_x FROM ranked_goalkeepers) d
)
SELECT * FROM final_results
ORDER BY season_x desc, CASE WHEN name = '---' THEN 1 ELSE 0 END, points_rank, bonus_points_rank;
