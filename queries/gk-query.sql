CREATE TABLE cleaned_merged_seasons AS 
SELECT * FROM read_csv_auto('./data/cleaned_merged_seasons.csv');

-- Now, let's perform our analysis
WITH goalkeeper_stats AS (
    SELECT 
        name,
        COUNT(*) as appearance_count
    FROM cleaned_merged_seasons
    WHERE "position" = 'GK' AND bonus = 3
    GROUP BY name
)
SELECT 
    gs.name,
    gs.appearance_count,
    cms.season_x,
    cms.team_x,
    cms.opp_team_name,
    cms.was_home,
    cms.saves,
    cms.penalties_saved,
    cms.goals_conceded,
    cms.clean_sheets,
    cms.bps,
    cms.total_points,
    cms.kickoff_time
FROM goalkeeper_stats gs
JOIN cleaned_merged_seasons cms ON gs.name = cms.name
WHERE cms."position" = 'GK' AND cms.bonus = 3
AND gs.appearance_count >= 5
ORDER BY gs.appearance_count DESC, gs.name desc, cms.season_x desc, cms.opp_team_name asc, cms.kickoff_time desc
