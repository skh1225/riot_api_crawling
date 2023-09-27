import argparse
import datetime
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("--execution_date", type=datetime.date.fromisoformat, required=True, help="airflow execution date")

args = parser.parse_args()

today = args.execution_date

spark = SparkSession \
    .builder \
    .appName('extract_statistic') \
    .getOrCreate()

match_df = spark.read.parquet(f"gs://summoner-match/processed/match/*.parquet")
match_df.withColumn('tier', F.expr("""
CASE WHEN tier in ('MASTER', 'GRANDMASTER', 'CHALLENGER') THEN 'MASTER+' ELSE tier END
""")).createOrReplaceTempView('match_data')

champ_df = spark.read.parquet(f"gs://summoner-match/processed/champion/*.parquet").filter(F.col("teamPosition") != "")
champ_df.withColumn('tier', F.expr("""
CASE WHEN tier in ('MASTER', 'GRANDMASTER', 'CHALLENGER') THEN 'MASTER+' ELSE tier END
""")).createOrReplaceTempView('champ_data')

spark.sql("""
SELECT * FROM match_data WHERE EXISTS (
SELECT * FROM (SELECT DISTINCT gameVersion_ FROM match_data
ORDER BY cast(split(gameVersion_,'[.]',2)[0] as int) DESC, cast(split(gameVersion_,'[.]',2)[1] as int) DESC
LIMIT 3) a WHERE a.gameVersion_=match_data.gameVersion_)
""").cache().createOrReplaceTempView('match_info')

spark.sql("""
SELECT * FROM champ_data WHERE EXISTS (
SELECT * FROM (SELECT DISTINCT gameVersion_ FROM match_data
ORDER BY cast(split(gameVersion_,'[.]',2)[0] as int) DESC, cast(split(gameVersion_,'[.]',2)[1] as int) DESC
LIMIT 3) a WHERE a.gameVersion_=champ_data.gameVersion_)
""").cache().createOrReplaceTempView('champ_info')

spark.sql("""
SELECT championId, tier, date, count(*) as ban_cnt FROM
(SELECT tier, date, explode(BANS) as championId FROM match_info)
WHERE championId!=-1
GROUP BY tier, date, championId;
""").createOrReplaceTempView('ban_info')


spark.sql("""
SELECT tier, date, gameVersion_, COUNT(*) as total FROM match_info GROUP BY tier, date, gameVersion_;
""").createOrReplaceTempView('total_info')

spark.sql("""
SELECT championId, teamPosition, tier, date, count(CASE WHEN win THEN 1 END) as win_cnt, count(*) as pick_cnt
FROM champ_info
GROUP BY championId, teamPosition, tier, date;
""").createOrReplaceTempView('pick_info')

### 날짜별 밴/픽/승 률
spark.sql("""
SELECT coalesce(a.championId, b.championId) as championId, a.teamPosition, coalesce(a.tier, b.tier) as tier, coalesce(a.date, b.date) as date, c.gameVersion_, a.win_cnt, a.pick_cnt, b.ban_cnt, c.total
FROM pick_info a
FULL OUTER JOIN ban_info b ON a.tier=b.tier and a.date=b.date and a.championId = b.championId
FULL OUTER JOIN total_info c ON coalesce(a.tier,b.tier)=c.tier and coalesce(a.date, b.date)=c.date;
""").cache().createOrReplaceTempView('wpb')

### 1D
df_1D = spark.sql(f"""
SELECT a.championId, a.teamPosition, a.tier, a.win_cnt, a.pick_cnt, a.ban_cnt, b.total FROM
(SELECT championId, teamPosition, tier, SUM(win_cnt) as win_cnt, SUM(pick_cnt) as pick_cnt, SUM(ban_cnt) as ban_cnt
FROM wpb WHERE date>='{today}'
GROUP BY championId, teamPosition, tier) a
JOIN
(SELECT tier, SUM(total) as total FROM (SELECT DISTINCT tier, date, total FROM wpb WHERE date>date_sub('{today}',7)) GROUP BY tier) b
ON a.tier=b.tier;
""").repartition(1)

df_1D.write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/{today}_1D.json")
df_1D.write.format("mongodb").mode('overwrite')\
.option("database","statistics").option("collection", f"{today}_1D").save()

### 1W
df_1W=spark.sql(f"""
SELECT a.championId, a.teamPosition, a.tier, a.win_cnt, a.pick_cnt, a.ban_cnt, b.total FROM
(SELECT championId, teamPosition, tier, SUM(win_cnt) as win_cnt, SUM(pick_cnt) as pick_cnt, SUM(ban_cnt) as ban_cnt
FROM wpb WHERE date>=date_sub('{today}',7)
GROUP BY championId, teamPosition, tier) a
JOIN
(SELECT tier, SUM(total) as total FROM (SELECT DISTINCT tier, date, total FROM wpb WHERE date>date_sub('{today}',7)) GROUP BY tier) b
ON a.tier=b.tier;
""").repartition(1)

df_1W.write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/{today}_1W.json")
df_1W.write.format("mongodb").mode('overwrite')\
.option("database","statistics").option("collection", f"{today}_1W").save()

### 1M
df_1M = spark.sql(f"""
SELECT a.championId, a.teamPosition, a.tier, a.win_cnt, a.pick_cnt, a.ban_cnt, b.total FROM
(SELECT championId, teamPosition, tier, SUM(win_cnt) as win_cnt, SUM(pick_cnt) as pick_cnt, SUM(ban_cnt) as ban_cnt
FROM wpb WHERE date>=date_sub('{today}',30)
GROUP BY championId, teamPosition, tier) a
JOIN
(SELECT tier, SUM(total) as total FROM (SELECT DISTINCT tier, date, total FROM wpb WHERE date>date_sub('{today}',30)) GROUP BY tier) b
ON a.tier=b.tier;
""").repartition(1)

df_1M.write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/{today}_1M.json")
df_1M.write.format("mongodb").mode('overwrite')\
.option("database","statistics").option("collection", f"{today}_1M").save()

#------------------------champion statistic------------------------#
spark.sql("""
SELECT
a.championId,
a.teamPosition,
a.gameVersion_,
a.tier,
ROUND(a.win_cnt/a.pick_cnt,3) as winRate,
ROUND(a.pick_cnt/b.total,3) as pickRate,
ROUND(a.ban_cnt/b.total,3) as banRate,
a.pick_cnt as total
FROM
(SELECT championId, teamPosition, gameVersion_, tier, SUM(win_cnt) as win_cnt, SUM(pick_cnt) as pick_cnt, SUM(ban_cnt) as ban_cnt FROM wpb GROUP BY championId, teamPosition, gameVersion_, tier) a
JOIN
(SELECT gameVersion_, tier, SUM(total) as total FROM (SELECT DISTINCT tier, gameVersion_, date, total FROM wpb) GROUP BY gameVersion_, tier) b
ON a.gameVersion_=b.gameVersion_ and a.tier=b.tier
WHERE a.pick_cnt/b.total>0.005;
""").createOrReplaceTempView('wpb_version')

spark.sql("""
SELECT * FROM (
SELECT
championId,
gameVersion_,
teamPosition,
tier,
statPerks,
ROUND(COUNT(CASE WHEN win THEN 1 END)/COUNT(*),3) as win_rate,
ROUND(COUNT(*)/CAST((SUM(COUNT(*)) OVER(PARTITION BY championId, gameVersion_, teamPosition, tier)) as float),3) as select_ratio,
COUNT(*) as total
FROM champ_info
GROUP BY championId, gameVersion_, teamPosition, statPerks, tier
ORDER BY championId, gameVersion_, teamPosition, tier, total DESC)
WHERE select_ratio>0.005;
""")\
.groupBy(F.col('championId'),F.col('gameVersion_'),F.col('teamPosition'),F.col('tier'))\
.agg(F.collect_list(F.struct(F.col('statPerks'),F.col('win_rate'),F.col('select_ratio'),F.col('total'))).alias('statPerks'))\
.createOrReplaceTempView('stat_perks')

spark.sql("""
SELECT * FROM (
SELECT
championId,
gameVersion_,
teamPosition,
tier,
primaryStyles,
subStyles,
ROUND(COUNT(CASE WHEN win THEN 1 END)/COUNT(*),3) as win_rate,
ROUND(COUNT(*)/CAST((SUM(COUNT(*)) OVER(PARTITION BY championId, gameVersion_, teamPosition, tier)) as float),3) as select_ratio,
COUNT(*) as total
FROM champ_info
GROUP BY championId, gameVersion_, teamPosition, tier, primaryStyles, subStyles
ORDER BY championId, gameVersion_, teamPosition, tier, total DESC)
WHERE select_ratio>0.005;
""")\
.groupBy(F.col('championId'),F.col('gameVersion_'),F.col('teamPosition'),F.col('tier'))\
.agg(F.collect_list(F.struct(F.col('primaryStyles'),F.col('subStyles'),F.col('win_rate'),F.col('select_ratio'),F.col('total'))).alias('perks'))\
.createOrReplaceTempView('perks')

spark.sql("""
SELECT * FROM (
SELECT
championId,
gameVersion_,
teamPosition,
tier,
summonerSpell,
ROUND(COUNT(CASE WHEN win THEN 1 END)/COUNT(*),3) as win_rate,
ROUND(COUNT(*)/CAST((SUM(COUNT(*)) OVER(PARTITION BY championId, gameVersion_, teamPosition, tier)) as float),3) as select_ratio,
COUNT(*) as total
FROM champ_info
GROUP BY championId, gameVersion_, teamPosition, tier, summonerSpell
ORDER BY championId , gameVersion_, teamPosition, tier, total DESC)
WHERE select_ratio>0.005;
""")\
.groupBy(F.col('championId'),F.col('gameVersion_'),F.col('teamPosition'),F.col('tier'))\
.agg(F.collect_list(F.struct(F.col('summonerSpell'),F.col('win_rate'),F.col('select_ratio'),F.col('total'))).alias('summonerSpell'))\
.createOrReplaceTempView('summoner_spell')

spark.sql("""
SELECT * FROM (
SELECT
championId,
gameVersion_,
teamPosition,
tier,
itemStart,
ROUND(COUNT(CASE WHEN win THEN 1 END)/COUNT(*),3) as win_rate,
ROUND(COUNT(*)/CAST((SUM(COUNT(*)) OVER(PARTITION BY championId, gameVersion_, teamPosition, tier)) as float),3) as select_ratio,
COUNT(*) as total
FROM champ_info
GROUP BY championId, gameVersion_, teamPosition, tier, itemStart
ORDER BY championId, gameVersion_, teamPosition, tier, total DESC)
WHERE select_ratio>0.005;
""")\
.groupBy(F.col('championId'),F.col('gameVersion_'),F.col('teamPosition'),F.col('tier'))\
.agg(F.collect_list(F.struct(F.col('itemStart'),F.col('win_rate'),F.col('select_ratio'),F.col('total'))).alias('itemStart'))\
.createOrReplaceTempView('item_start')

spark.sql("""
SELECT * FROM (
SELECT
championId,
gameVersion_,
teamPosition,
tier,
itemBoots,
ROUND(COUNT(CASE WHEN win THEN 1 END)/COUNT(*),3) as win_rate,
ROUND(COUNT(*)/CAST((SUM(COUNT(*)) OVER(PARTITION BY championId, gameVersion_, teamPosition, tier)) as float),3) as select_ratio,
COUNT(*) as total
FROM champ_info
WHERE itemBoots is not NULL
GROUP BY championId, gameVersion_, teamPosition, tier, itemBoots
ORDER BY championId, gameVersion_, teamPosition, tier, total DESC)
WHERE select_ratio>0.005;
""")\
.groupBy(F.col('championId'),F.col('gameVersion_'),F.col('teamPosition'),F.col('tier'))\
.agg(F.collect_list(F.struct(F.col('itemBoots'),F.col('win_rate'),F.col('select_ratio'),F.col('total'))).alias('itemBoots'))\
.createOrReplaceTempView('item_boots')

spark.sql("""
SELECT * FROM (
SELECT
championId,
gameVersion_,
teamPosition,
tier,
itemMyth,
ROUND(COUNT(CASE WHEN win THEN 1 END)/COUNT(*),3) as win_rate,
ROUND(COUNT(*)/CAST((SUM(COUNT(*)) OVER(PARTITION BY championId, gameVersion_, teamPosition, tier)) as float),3) as select_ratio,
COUNT(*) as total
FROM champ_info
WHERE itemMyth is not NULL
GROUP BY championId, gameVersion_, teamPosition, tier, itemMyth
ORDER BY championId, gameVersion_, teamPosition, tier, total DESC)
WHERE select_ratio>0.005;
""")\
.groupBy(F.col('championId'),F.col('gameVersion_'),F.col('teamPosition'),F.col('tier'))\
.agg(F.collect_list(F.struct(F.col('itemMyth'),F.col('win_rate'),F.col('select_ratio'),F.col('total'))).alias('itemMyth'))\
.createOrReplaceTempView('item_myth')

spark.sql("""
SELECT * FROM (
SELECT
championId,
gameVersion_,
teamPosition,
tier,
slice(itemRarity,1,3) as itemRarity,
ROUND(COUNT(CASE WHEN win THEN 1 END)/COUNT(*),3) as win_rate,
ROUND(COUNT(*)/CAST((SUM(COUNT(*)) OVER(PARTITION BY championId, gameVersion_, teamPosition, tier)) as float),3) as select_ratio,
COUNT(*) as total
FROM champ_info
WHERE size(itemRarity) >= 3
GROUP BY championId, gameVersion_, teamPosition, tier, itemRarity
ORDER BY championId , gameVersion_, teamPosition, tier, total DESC)
WHERE select_ratio>0.005;
""")\
.groupBy(F.col('championId'),F.col('gameVersion_'),F.col('teamPosition'),F.col('tier'))\
.agg(F.collect_list(F.struct(F.col('itemRarity'),F.col('win_rate'),F.col('select_ratio'),F.col('total'))).alias('itemRarity'))\
.createOrReplaceTempView('item_rarity')

spark.sql("""
SELECT championId, gameVersion_, teamPosition, tier
, ROUND(AVG(teamDamagePercentage),3) as avgDmgDealtPercent
, ROUND(AVG(damageTakenOnTeamPercentage),3) as avgDmgTakenPercent
, ROUND(AVG(magicDamageDealtToChampions)/(AVG(magicDamageDealtToChampions)+AVG(physicalDamageDealtToChampions)+AVG(trueDamageDealtToChampions)),3) as magicDmg
, ROUND(AVG(physicalDamageDealtToChampions)/(AVG(magicDamageDealtToChampions)+AVG(physicalDamageDealtToChampions)+AVG(trueDamageDealtToChampions)),3) as physicalDmg
, ROUND(AVG(trueDamageDealtToChampions)/(AVG(magicDamageDealtToChampions)+AVG(physicalDamageDealtToChampions)+AVG(trueDamageDealtToChampions)),3) as trueDmg
, ROUND(AVG(timeCCingOthers/gameDuration*60),3) as avgCCingPerMin
FROM champ_info
GROUP BY championId, gameVersion_, teamPosition, tier;
""").createOrReplaceTempView('game_detail')

spark.sql("""
SELECT
championId,
tier,
gameVersion_,
teamPosition,
slice(skillTree,1,13) as skillTree,
ROUND(COUNT(CASE WHEN win THEN 1 END)/COUNT(*),3) as win_rate,
ROUND(COUNT(*)/CAST((SUM(COUNT(*)) OVER(PARTITION BY championId, gameVersion_, teamPosition, tier)) as float),3) as select_ratio,
COUNT(*) as total
FROM champ_info
WHERE size(skillTree) >= 13
GROUP BY championId, tier, gameVersion_, teamPosition, skillTree
ORDER BY championId, tier, gameVersion_, teamPosition, total DESC;
""")\
.groupBy(F.col('championId'),F.col('gameVersion_'),F.col('teamPosition'),F.col('tier'))\
.agg(F.collect_list(F.struct(F.col('skillTree'),F.col('win_rate'),F.col('select_ratio'),F.col('total'))).alias('skillTree'))\
.createOrReplaceTempView('skill_tree')

df_detail = spark.sql("""
SELECT
a.championId, a.teamPosition, a.gameVersion_, a.tier, a.winRate, a.pickRate, a.banRate, a.total,
b.statPerks, c.perks, d.summonerSpell, j.skillTree, e.itemStart, f.itemBoots, g.itemMyth, h.itemRarity,
i.avgDmgDealtPercent, i.avgDmgTakenPercent, i.magicDmg, i.physicalDmg, i.trueDmg,i.avgCCingPerMin
FROM wpb_version a
LEFT JOIN stat_perks b ON a.championId=b.championId and a.teamPosition=b.teamPosition and a.gameVersion_=b.gameVersion_ and a.tier=b.tier
LEFT JOIN perks c ON a.championId=c.championId and a.teamPosition=c.teamPosition and a.gameVersion_=c.gameVersion_ and a.tier=c.tier
LEFT JOIN summoner_spell d ON a.championId=d.championId and a.teamPosition=d.teamPosition and a.gameVersion_=d.gameVersion_ and a.tier=d.tier
LEFT JOIN item_start e ON a.championId=e.championId and a.teamPosition=e.teamPosition and a.gameVersion_=e.gameVersion_ and a.tier=e.tier
LEFT JOIN item_boots f ON a.championId=f.championId and a.teamPosition=f.teamPosition and a.gameVersion_=f.gameVersion_ and a.tier=f.tier
LEFT JOIN item_myth g ON a.championId=g.championId and a.teamPosition=g.teamPosition and a.gameVersion_=g.gameVersion_ and a.tier=g.tier
LEFT JOIN item_rarity h ON a.championId=h.championId and a.teamPosition=h.teamPosition and a.gameVersion_=h.gameVersion_ and a.tier=h.tier
LEFT JOIN game_detail i ON a.championId=i.championId and a.teamPosition=i.teamPosition and a.gameVersion_=i.gameVersion_ and a.tier=i.tier
LEFT JOIN skill_tree j ON a.championId=j.championId and a.teamPosition=j.teamPosition and a.gameVersion_=j.gameVersion_ and a.tier=j.tier;
""").repartition(1)

df_detail.write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/{today}_detail.json")
df_detail.write.format("mongodb").mode('overwrite')\
.option("database","statistics").option("collection", f"champion_statistics_detail").save()