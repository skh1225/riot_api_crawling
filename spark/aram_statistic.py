import datetime
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from google.cloud import storage

spark = SparkSession \
    .builder \
    .appName('aram_statistic') \
    .getOrCreate()


storage_client = storage.Client()
blobs = storage_client.list_blobs('gnimty_bucket')

# gcs file list
date_set = set()
for blob in blobs:
    blob_split = blob.name.split('/')
    if blob_split[0]=='bigquery' and blob_split[1]=='aram':
        date_set.add(blob_split[2].split('.')[0])

# gcs에 존재하는 최근 14일간의 파일 list
file_list = []
for i in range(1,15):
    d = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
    if d in date_set:
        file_list.append(f"gs://gnimty_bucket/bigquery/aram/{d}.parquet")


match_df = spark.read.parquet(*file_list)

match_df.withColumn('gameVersion',F.array_join(F.slice(F.split('gameVersion','[.]',3),1,2),"."))\
.createOrReplaceTempView('match_data')

gameVersion = spark.sql("""SELECT DISTINCT gameVersion FROM match_data
ORDER BY cast(split(gameVersion,'[.]',2)[0] as int) DESC, cast(split(gameVersion,'[.]',2)[1] as int) DESC
LIMIT 1""").first()['gameVersion']

spark.sql(f"""
SELECT * FROM match_data
WHERE match_data.gameVersion='{gameVersion}'
and p_gameEndedInEarlySurrender=false
""").createOrReplaceTempView('match_info')

before_14days = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime("%Y-%m-%d")

spark.sql(f"""
SELECT * FROM match_data
WHERE match_data.date>='{before_14days}'
and p_gameEndedInEarlySurrender=false
""").createOrReplaceTempView('match_info_')

total_cnt = spark.sql("SELECT DISTINCT matchId FROM match_info").count()

#pick, win, total, 픽률 0.5%이상만
spark.sql(f"""
SELECT p_championId as championId,
COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as pick_cnt ,{total_cnt} as total_cnt
FROM match_info
GROUP BY p_championId
HAVING pick_cnt/total_cnt>0.005
""").createOrReplaceTempView('pick_')

spark.sql("""
SELECT p_championId as championId, p_statPerks as statPerks
, COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as select_cnt
, SUM(COUNT(1)) OVER(PARTITION BY p_championId) as total_cnt
FROM match_info
GROUP BY p_championId, p_statPerks
ORDER BY select_cnt DESC
""").filter(F.col('select_cnt')/F.col('total_cnt')>=0.01)\
    .groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('statPerks'),(F.col('win_cnt')/F.col('select_cnt')).alias('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('statPerks'))\
.createOrReplaceTempView('statPerks_')

spark.sql("""
SELECT p_championId as championId, p_primaryStyles as primaryStyles, p_subStyles as subStyles
, COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as select_cnt
, SUM(COUNT(1)) OVER(PARTITION BY p_championId) as total_cnt
FROM match_info
GROUP BY p_championId, p_primaryStyles, p_subStyles
ORDER BY select_cnt DESC
""").filter(F.col('select_cnt')/F.col('total_cnt')>=0.01)\
    .groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('primaryStyles'), F.col('subStyles'),(F.col('win_cnt')/F.col('select_cnt')).alias('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('perks'))\
.createOrReplaceTempView('perks_')

spark.sql("""
SELECT p_championId as championId,  p_summoner1Id as summoner1Id, p_summoner2Id as summoner2Id
, COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as select_cnt
, SUM(COUNT(1)) OVER(PARTITION BY p_championId) as total_cnt
FROM match_info
GROUP BY p_championId, p_summoner1Id, p_summoner2Id
ORDER BY select_cnt DESC
""").filter(F.col('select_cnt')/F.col('total_cnt')>=0.01)\
    .groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('summoner1Id'), F.col('summoner2Id'),(F.col('win_cnt')/F.col('select_cnt')).alias('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('summonerSpell'))\
.createOrReplaceTempView('summonerSpell_')

spark.sql("""
SELECT p_championId as championId,
avg(p_physicalDamageDealtToChampions) as physicalDamage,
avg(p_magicDamageDealtToChampions) as magicDamage,
avg(p_trueDamageDealtToChampions) as trueDamage,
avg(p_magicDamageDealtToChampions)+avg(p_physicalDamageDealtToChampions)+avg(p_trueDamageDealtToChampions) as totalDamage,
avg(p_teamDamagePercentage) as teamDamagePercentage,
avg(p_damageTakenOnTeamPercentage) as damageTakenOnTeamPercentage
FROM match_info
GROUP BY p_championId
""").withColumn('damage',F.struct(F.col('physicalDamage'),F.col('magicDamage'),F.col('trueDamage'),F.col('totalDamage'),F.col('teamDamagePercentage'),F.col('damageTakenOnTeamPercentage')))\
.createOrReplaceTempView('damage_')

spark.sql("""
SELECT p_championId as championId, array_sort(p_itemStart) as itemStart
, COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as select_cnt
, SUM(COUNT(1)) OVER(PARTITION BY p_championId) as total_cnt
FROM match_info
GROUP BY p_championId, p_itemStart
ORDER BY select_cnt DESC
""").filter(F.col('select_cnt')/F.col('total_cnt')>=0.01)\
    .groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('itemStart'),(F.col('win_cnt')/F.col('select_cnt')).alias('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('itemStart'))\
.createOrReplaceTempView('itemStart_')

spark.sql("""
SELECT p_championId as championId, p_itemMiddle as itemMiddle
, COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as select_cnt
, SUM(COUNT(1)) OVER(PARTITION BY p_championId) as total_cnt
FROM match_info
GROUP BY p_championId, p_itemMiddle
ORDER BY select_cnt DESC
""").filter(F.col('select_cnt')/F.col('total_cnt')>=0.01)\
    .groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('itemMiddle'),(F.col('win_cnt')/F.col('select_cnt')).alias('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('itemMiddle'))\
.createOrReplaceTempView('itemMiddle_')

spark.sql("""
SELECT p_championId as championId, slice(p_itemBuild,1,3) as itemBuild
, COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as select_cnt
, SUM(COUNT(1)) OVER(PARTITION BY p_championId) as total_cnt
FROM match_info
WHERE size(p_itemBuild)>=3
GROUP BY p_championId, itemBuild
ORDER BY select_cnt DESC
""").filter(F.col('select_cnt')/F.col('total_cnt')>=0.01)\
    .groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('itemBuild'),(F.col('win_cnt')/F.col('select_cnt')).alias('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('itemBuild'))\
.createOrReplaceTempView('itemBuild_')

spark.sql("""
SELECT p_championId as championId, slice(p_skillTree,1,13) as skillTree
, COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as select_cnt
, SUM(COUNT(1)) OVER(PARTITION BY p_championId) as total_cnt
FROM match_info
WHERE size(p_skillTree)>=13
GROUP BY p_championId, skillTree
ORDER BY select_cnt DESC
""").filter(F.col('select_cnt')/F.col('total_cnt')>=0.01)\
    .groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('skillTree'),(F.col('win_cnt')/F.col('select_cnt')).alias('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('skillTree'))\
.createOrReplaceTempView('skillTree_')

spark.sql("""
SELECT p_championId as championId
        , CASE WHEN ceil(gameDuration/300)<=3 THEN '15'
        WHEN ceil(gameDuration/300)>7 THEN '35+'
        ELSE cast(ceil(gameDuration/300)*5 as string) END  as minute
, COUNT(CASE WHEN p_win THEN 1 END) as win_cnt, COUNT(1) as select_cnt
, SUM(COUNT(1)) OVER(PARTITION BY p_championId) as total_cnt
FROM match_info
GROUP BY p_championId, minute
ORDER BY minute
""").groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('minute'),(F.col('win_cnt')/F.col('select_cnt')).alias('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('powerCurve'))\
.createOrReplaceTempView('powerCurve_')

spark.sql("""
SELECT a.p_championId, a.p_win, a.total_cnt, b.p_championId as s_championId
FROM (SELECT *, COUNT(1) OVER(PARTITION BY p_championId) as total_cnt FROM match_info_) a JOIN match_info_ b
ON a.matchId=b.matchId and a.p_teamId=b.p_teamId and a.p_championId!=b.p_championId
""").createOrReplaceTempView('synergy__')

spark.sql("""
SELECT p_championId as championId, s_championId, win_cnt/select_cnt as win_rate, select_cnt, total_cnt,
ROW_NUMBER() OVER(PARTITION BY p_championId ORDER BY win_cnt/select_cnt DESC) as row_num
FROM (SELECT p_championId, s_championId, total_cnt,
COUNT(CASE WHEN p_win THEN 1 END) as win_cnt,
COUNT(1) as select_cnt
FROM synergy__
GROUP BY p_championId, s_championId, total_cnt)
WHERE select_cnt/total_cnt>=0.01
""").filter(F.col('row_num')<=10)\
.groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('s_championId'),F.col('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('synergy'))\
.createOrReplaceTempView('synergy_')

spark.sql("""
SELECT a.p_championId, a.p_win, a.total_cnt, b.p_championId as c_championId
FROM (SELECT *, COUNT(1) OVER(PARTITION BY p_championId) as total_cnt FROM match_info_) a JOIN match_info_ b
ON a.matchId=b.matchId and a.p_teamId!=b.p_teamId
""").createOrReplaceTempView('counter__')

spark.sql("""
SELECT p_championId as championId, c_championId, win_cnt/select_cnt as win_rate, select_cnt, total_cnt,
ROW_NUMBER() OVER(PARTITION BY p_championId ORDER BY win_cnt/select_cnt DESC) as row_num
FROM (SELECT p_championId, c_championId, total_cnt,
COUNT(CASE WHEN p_win THEN 1 END) as win_cnt,
COUNT(1) as select_cnt
FROM counter__
GROUP BY p_championId, c_championId, total_cnt)
WHERE select_cnt/total_cnt>=0.01
""").filter(F.col('row_num')<=10)\
.groupBy(F.col('championId'))\
.agg(F.collect_list(F.struct(F.col('c_championId'),F.col('win_rate'),(F.col('select_cnt')/F.col('total_cnt')).alias('pick_rate'),F.col('select_cnt'))).alias('counter'))\
.createOrReplaceTempView('counter_')

spark.sql("""
SELECT a.championId, a.win_cnt/a.pick_cnt as win_rate, a.pick_cnt/a.total_cnt as pick_rate, a.pick_cnt,
c.statPerks,
e.summonerSpell,
f.damage,
g.itemStart,
h.itemMiddle,
i.itemBuild,
j.skillTree,
k.powerCurve,
l.counter,
m.synergy,
COUNT(1) OVER(PARTITION BY a.championId) as position_cnt
FROM pick_ a
LEFT JOIN statPerks_ c ON a.championId=c.championId
LEFT JOIN perks_ d ON a.championId=d.championId
LEFT JOIN summonerSpell_ e ON a.championId=e.championId
LEFT JOIN damage_ f ON a.championId=f.championId
LEFT JOIN itemStart_ g ON a.championId=g.championId
LEFT JOIN itemMiddle_ h ON a.championId=h.championId
LEFT JOIN itemBuild_ i ON a.championId=i.championId
LEFT JOIN skillTree_ j ON a.championId=j.championId
LEFT JOIN powerCurve_ k ON a.championId=k.championId
LEFT JOIN counter_ l ON a.championId=l.championId
LEFT JOIN synergy_ m ON a.championId=m.championId
""").withColumn('score', \
            (F.lit(50)+F.lit(1.6)*(F.col('win_rate')*F.lit(100)-F.lit(50)))+\
            F.lit(3.5)*(F.log(F.col('pick_rate'))-F.log(F.lit(2)/F.col('position_cnt')))/F.log(F.lit(200)/F.col('position_cnt'))\
        ).orderBy(F.desc("score"))\
.withColumn('tier', F.when(F.col('score')>= 56,"OP")
        .when(F.col('score')>= 54.5,"1")
            .when(F.col('score')>= 51.5,"2")
            .when(F.col('score')>= 48.5,"3")
            .when(F.col('score')>= 45.5,"4")
        .otherwise("5")).write.format("mongodb").mode('overwrite')\
    .option("database","statistics").option("collection", f"champion_statistics_aram").save()