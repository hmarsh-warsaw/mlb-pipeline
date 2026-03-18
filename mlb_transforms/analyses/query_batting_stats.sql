-- Query stg_batting_stats for data exploration

-- All players (2024 season)
select *
from stg_batting_stats
where season = 2024
order by ops desc;


