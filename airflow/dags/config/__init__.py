sql_create_match_table = """CREATE TABLE IF NOT EXISTS `fourth-way-398009.summoner_match.match_data` (
  matchId STRING,
  tier STRING,			
  gameDuration INTEGER,
  gameVersion	STRING,
  date DATE,
  BLUE_WIN BOOLEAN,		
  BLUE_TOP INTEGER,
  BLUE_JUG INTEGER,
  BLUE_MID INTEGER,
  BLUE_ADC INTEGER,
  BLUE_SUP INTEGER,
  RED_TOP	INTEGER,			
  RED_JUG	INTEGER,				
  RED_MID	INTEGER,				
  RED_ADC	INTEGER,				
  RED_SUP	INTEGER,
  BANS STRUCT<list STRUCT<element INTEGER>>,			
  BLUE_BARON INTEGER,				
  BLUE_DRAGON INTEGER,				
  BLUE_RIFT_HERALD INTEGER,				
  RED_BARON	INTEGER,
  RED_DRAGON INTEGER,				
  RED_RIFT_HERALD INTEGER,				
  execution_date DATE
) PARTITION BY
  date
  OPTIONS (
    partition_expiration_days = 60,
    require_partition_filter = TRUE);"""