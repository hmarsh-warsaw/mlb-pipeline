-- Staging model: clean and standardize raw batting stats
select
    season,
    player_name,
    team,
    g,
    pa,
    ab,
    h,
    doubles,
    triples,
    hr,
    bb,
    so,
    sb,
    avg,
    obp,
    slg,
    ops,
    woba,
    wrc_plus,
    war,
    playerid
from {{ source('mlb_raw', 'batting_stats') }}
where pa >= 50  -- filter out small sample players
