import argparse
import numpy as np
import pandas as pd
import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import when

from pyspark.sql.window import Window

from pyspark.sql import SparkSession
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("--start_date", type=datetime.date.fromisoformat, required=True, help="airflow start date")

args = parser.parse_args()

start_date= args.start_date

spark = SparkSession \
    .builder \
    .appName('champion_counter') \
    .getOrCreate()

# champ_meta = spark.read.json("gs://summoner-match/metadata/champion.json")\
# .select(F.array("data.*").alias("champName")).select(F.explode("champName"))\
# .select(F.col("col.key").alias("championId"),F.col("col.id").alias("name"))

# F.broadcast(champ_meta).createOrReplaceTempView("champ_meta")

df = spark.read.parquet(f"gs://summoner-match/processed/match/*.parquet")

df.filter(F.col("date")>=start_date).cache().createOrReplaceTempView('match_info')


total_num = spark.sql("SELECT * FROM match_info;").count()

for Position, Synergy in [('TOP','JUG'),('TOP','MID'),('MID','JUG'),('ADC','SUP'),('JUG','SUP'),('MID','ADC')]:
  ### SYNERGY
  spark.sql(f"""
  SELECT {Position}, {Synergy}, COUNT(*) as pick_cnt, COUNT(CASE WHEN win THEN 1 END) as win_cnt, SUM(COUNT(*)) OVER(PARTITION BY {Position}) as total FROM
  ((SELECT BLUE_{Position} as {Position}, BLUE_{Synergy} as {Synergy}, BLUE_WIN as win FROM match_info)
  UNION ALL
  (SELECT RED_{Position} as {Position}, RED_{Synergy} as {Synergy}, NOT BLUE_WIN as win FROM match_info))
  GROUP BY {Position},{Synergy}
  """).filter((F.col(Position) != "")&(F.col(Synergy) != "")).filter(f"total/{total_num}>0.005").filter("pick_cnt/total > 0.01")\
  .withColumn('win_rate', F.col('win_cnt')/F.col('pick_cnt'))\
  .withColumn('pick_rate', F.col('pick_cnt')/F.col('total'))\
  .withColumn('position_cnt',F.count('*').over(Window.partitionBy(f"{Position}")))\
  .withColumn('score', \
            (F.lit(50)+F.lit(1.6)*(F.col('win_rate')*F.lit(100)-F.lit(50)))+\
            F.lit(3.5)*F.log(F.col('pick_rate')*F.col('position_cnt'))/F.log(F.lit(100)/F.col('position_cnt'))\
          ).orderBy(F.desc("score"))\
  .createOrReplaceTempView('test_synergy')

  df_synergy = spark.sql(f"""
  SELECT
  {Position},
  {Synergy},
  win_rate,
  pick_rate,
  score,
  pick_cnt
  FROM test_synergy
  ORDER BY {Position}, score DESC
  """).repartition(1)

  df_synergy.write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/synergy_{Position}_{Synergy}.json")
  df_synergy.write.format("mongodb").mode('overwrite')\
  .option("database","statistics").option("collection", f"synergy_{Position}_{Synergy}").save()

  # spark.sql(f"""
  # SELECT
  # b.name as {Position},
  # c.name as {Synergy},
  # a.win_rate,
  # a.pick_rate,
  # a.score,
  # a.pick_cnt
  # FROM test_synergy a
  # LEFT JOIN champ_meta b ON a.{Position}=b.championId
  # LEFT JOIN champ_meta c ON a.{Synergy}=c.championId
  # ORDER BY {Position}, score DESC
  # """).repartition(1).write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/{Position}_{Synergy}_synergy.json")

for Synergy, Position in [('TOP','JUG'),('TOP','MID'),('MID','JUG'),('ADC','SUP'),('JUG','SUP'),('MID','ADC')]:
  ### SYNERGY
  spark.sql(f"""
  SELECT {Position}, {Synergy}, COUNT(*) as pick_cnt, COUNT(CASE WHEN win THEN 1 END) as win_cnt, SUM(COUNT(*)) OVER(PARTITION BY {Position}) as total FROM
  ((SELECT BLUE_{Position} as {Position}, BLUE_{Synergy} as {Synergy}, BLUE_WIN as win FROM match_info)
  UNION ALL
  (SELECT RED_{Position} as {Position}, RED_{Synergy} as {Synergy}, NOT BLUE_WIN as win FROM match_info))
  GROUP BY {Position},{Synergy}
  """).filter((F.col(Position) != "")&(F.col(Synergy) != "")).filter(f"total/{total_num}>0.005").filter("pick_cnt/total > 0.01")\
  .withColumn('win_rate', F.col('win_cnt')/F.col('pick_cnt'))\
  .withColumn('pick_rate', F.col('pick_cnt')/F.col('total'))\
  .withColumn('position_cnt',F.count('*').over(Window.partitionBy(f"{Position}")))\
  .withColumn('score', \
            (F.lit(50)+F.lit(1.6)*(F.col('win_rate')*F.lit(100)-F.lit(50)))+\
            F.lit(3.5)*F.log(F.col('pick_rate')*F.col('position_cnt'))/F.log(F.lit(100)/F.col('position_cnt'))\
          ).orderBy(F.desc("score"))\
  .createOrReplaceTempView('test_synergy')

  df_synergy = spark.sql(f"""
  SELECT
  {Position},
  {Synergy},
  win_rate,
  pick_rate,
  score,
  pick_cnt
  FROM test_synergy
  ORDER BY {Position}, score DESC
  """).repartition(1)

  df_synergy.write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/synergy_{Position}_{Synergy}.json")
  df_synergy.write.format("mongodb").mode('overwrite')\
  .option("database","statistics").option("collection", f"synergy_{Position}_{Synergy}").save()
