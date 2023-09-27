import argparse
import numpy as np
import pandas as pd
import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import when

from pyspark.sql import SparkSession
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("--game_version", type=str, help="gameVersion_ ex) 13.16")

args = parser.parse_args()

game_version = args.game_version

spark = SparkSession \
    .builder \
    .appName('champion_tier') \
    .getOrCreate()

# champ_meta = spark.read.json("gs://summoner-match/metadata/champion.json")\
# .select(F.array("data.*").alias("champName")).select(F.explode("champName"))\
# .select(F.col("col.key").alias("championId"),F.col("col.id").alias("name"))

# F.broadcast(champ_meta).createOrReplaceTempView("champ_meta")

df = spark.read.parquet(f"gs://summoner-match/processed/match/*.parquet")

if game_version:
  df.where(f"gameVersion_=='{game_version}'").cache().createOrReplaceTempView('match_info')
else:
  df.cache().createOrReplaceTempView('match_info')

total_num = spark.sql("SELECT * FROM match_info;").count()

F.broadcast(spark.sql("""
SELECT championId, count(*) as ban_cnt FROM
(SELECT tier, date, explode(BANS) as championId FROM match_info)
WHERE championId!=-1
GROUP BY championId;
""")).createOrReplaceTempView('ban_info')


for Position in ['TOP', 'MID', 'JUG', 'ADC', 'SUP']:
  spark.sql(f"""
  (SELECT BLUE_{Position} as {Position}, RED_{Position} as OPP_{Position}, BLUE_WIN as win FROM match_info)
  UNION ALL
  (SELECT RED_{Position} as {Position}, BLUE_{Position} as OPP_{Position}, NOT BLUE_WIN as win FROM match_info)
  """).createOrReplaceTempView(f'{Position}_')

  spark.sql(f"""
  SELECT {Position}, OPP_{Position}, COUNT(CASE WHEN win THEN 1 END)/COUNT(*) as win_rate, COUNT(*) as total
  FROM {Position}_
  GROUP BY {Position}, OPP_{Position};
  """).createOrReplaceTempView(f'{Position}_win')

  spark.sql(f"""
  SELECT {Position}, COUNT(*) as pick
  FROM {Position}_
  GROUP BY {Position}
  HAVING COUNT(*)/{total_num}>0.005;
  """).createOrReplaceTempView(f'{Position}_pick')

  spark.sql(f"""
  SELECT a.{Position} as {Position}, b.{Position} as OPP_{Position}
  FROM {Position}_pick a JOIN {Position}_pick b ON a.{Position}!=b.{Position};
  """).createOrReplaceTempView(f'{Position}_combination')


  spark.sql(f"""
  SELECT a.{Position}, a.OPP_{Position}, c.pick/(SUM(c.pick) OVER(PARTITION BY a.{Position})) as pick_rate
  FROM {Position}_combination a
  LEFT JOIN {Position}_pick b ON a.{Position}=b.{Position}
  LEFT JOIN {Position}_pick c ON a.OPP_{Position}=c.{Position}
  """).createOrReplaceTempView(f'{Position}_adjust')

  spark.sql(f"""
  SELECT a.{Position}, SUM(a.pick_rate*COALESCE(b.win_rate,0.5)) as win_rate
  FROM {Position}_adjust a LEFT JOIN {Position}_win b ON a.{Position}=b.{Position} and a.OPP_{Position}=b.OPP_{Position}
  GROUP BY a.{Position}
  ORDER BY win_rate DESC;
  """).createOrReplaceTempView(f'{Position}_adjust_win_rate')

  #--------------------별필요 없음--------------------#
  spark.sql(f"""
  SELECT {Position}, COUNT(CASE WHEN win THEN 1 END)/COUNT(*) as win_rate
  FROM {Position}_
  GROUP BY {Position}
  """).createOrReplaceTempView(f'{Position}_win_rate')

  position_cnt = spark.sql(f"SELECT * FROM {Position}_pick").count()

  df_tier = spark.sql(f"""
  SELECT a.{Position}, a.win_rate as adjust_win_rate, b.win_rate, d.pick/{total_num} as pick_rate, e.ban_cnt/{total_num} as ban_rate, d.pick
  FROM {Position}_adjust_win_rate a
  LEFT JOIN {Position}_win_rate b ON a.{Position}=b.{Position}
  LEFT JOIN {Position}_pick d ON a.{Position}=d.{Position}
  LEFT JOIN ban_info e ON a.{Position}=e.championId
  ORDER BY adjust_win_rate DESC;
  """)\
  .withColumn('score', \
            (F.lit(50)+F.lit(1.6)*(F.col('adjust_win_rate')*F.lit(100)-F.lit(50)))+\
            F.lit(3.5)*(F.log(F.col('pick_rate'))-F.log(F.lit(1)-F.col('ban_rate'))-F.log(F.lit(2)/F.lit(position_cnt)))/F.log(F.lit(200)/F.lit(position_cnt))\
          ).orderBy(F.desc("score"))\
  .withColumn('tier', when(F.col('score')>= 56,"OP")
          .when(F.col('score')>= 54.5,"1 tier")
            .when(F.col('score')>= 51.5,"2 tier")
            .when(F.col('score')>= 48.5,"3 tier")
            .when(F.col('score')>= 45.5,"4 tier")
          .otherwise("5 tier")).repartition(1)

  df_tier.write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/{game_version}_{Position}_tier.json")
  df_tier.write.format("mongodb").mode('overwrite')\
  .option("database","statistics").option("collection", f"tier_{Position}").save()