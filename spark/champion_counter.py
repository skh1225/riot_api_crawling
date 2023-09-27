import argparse
import numpy as np
import pandas as pd
import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import when

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

for Position, Counter in [('TOP','TOP'),('MID','MID'),('JUG','JUG'),('ADC','ADC'),('SUP','SUP'),('ADC','SUP'),('SUP','ADC')]:
  ### COUNTER
  spark.sql(f"""
  (SELECT BLUE_{Position} as {Position}, RED_{Counter} as OPP_{Counter}, BLUE_WIN as win FROM match_info WHERE BLUE_{Position} is not NULL and RED_{Counter} is not NULL)
  UNION ALL
  (SELECT RED_{Position} as {Position}, BLUE_{Counter} as OPP_{Counter}, NOT BLUE_WIN as win FROM match_info WHERE BLUE_{Counter} is not NULL and RED_{Position} is not NULL)
  """).filter((F.col(Position) != "")&(F.col(f'OPP_{Counter}') != "")).createOrReplaceTempView(f'{Position}_')

  spark.sql(f"""
  SELECT
  {Position},
  OPP_{Counter},
  COUNT(CASE WHEN win THEN 1 END)/COUNT(*) as win_rate,
  COUNT(*) as pick,
  SUM(COUNT(*)) OVER(PARTITION BY {Position}) as total_{Position},
  SUM(COUNT(*)) OVER(PARTITION BY OPP_{Counter}) as total_OPP_{Counter}
  FROM {Position}_
  GROUP BY {Position}, OPP_{Counter};
  """).filter((F.col("pick")/F.col(f"total_{Position}") > 0.01) & (F.col(f"total_{Position}")/total_num > 0.005))\
  .createOrReplaceTempView(f'{Position}_win_')

  spark.sql(f"""
  SELECT
  a.{Position},
  a.OPP_{Counter},
  a.win_rate,
  a.pick,
  a.total_{Position},
  b.total_OPP_{Counter},
  a.pick/a.total_{Position}/b.total_OPP_{Counter}*{total_num}/SUM(a.pick/a.total_{Position}/b.total_OPP_{Counter}*{total_num}) OVER(PARTITION BY a.{Position}) as pick_rate,
  COUNT(*) OVER(PARTITION BY a.{Position}) as position_cnt
  FROM {Position}_win_ a
  JOIN (SELECT DISTINCT OPP_{Counter}, total_OPP_{Counter} FROM {Position}_win_) b ON a.OPP_{Counter} = b.OPP_{Counter}
  """).filter(F.col("win_rate") < 0.5).createOrReplaceTempView(f'{Position}_win')

  spark.sql(f"""
  SELECT
  {Position},
  OPP_{Counter},
  win_rate,
  pick_rate,
  pick,
  total_{Position},
  position_cnt
  FROM {Position}_win
  ORDER BY {Position}
  """)\
  .withColumn('score', \
            F.lit(50)+F.lit(1.6)*((F.lit(1)-F.col('win_rate'))*F.lit(100)-F.lit(50))+\
          F.lit(3.5)*F.log(F.col('pick_rate')*F.col('position_cnt'))/F.log(F.lit(100)/F.col('position_cnt'))\
          )\
  .filter(F.col("score") >= 50)\
  .createOrReplaceTempView(f"{Position}_pick_recsys")

  df_counter = spark.sql(f"""
  SELECT
  {Position},
  OPP_{Counter},
  score,
  win_rate,
  pick/total_{Position} as pick_rate,
  pick
  FROM {Position}_pick_recsys
  """).orderBy(F.desc(f"{Position}"),F.desc("score"))\
  .repartition(1)
  df_counter.write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/counter_{Position}_{Counter}.json")
  df_counter.write.format("mongodb").mode('overwrite')\
  .option("database","statistics").option("collection", f"counter_{Position}_{Counter}").save()

  # spark.sql(f"""
  # SELECT
  # b.name as {Position},
  # c.name OPP_{Counter},
  # a.score,
  # a.win_rate,
  # a.pick/a.total_{Position} as pick_rate,
  # a.pick
  # FROM {Position}_pick_recsys a
  # LEFT JOIN champ_meta b ON a.{Position}=b.championId
  # LEFT JOIN champ_meta c ON a.OPP_{Counter}=c.championId
  # """).orderBy(F.desc(f"{Position}"),F.desc("score"))\
  # .repartition(1).write.mode('overwrite').json(f"gs://summoner-match/processed/statistic/{Position}_{Counter}_counter.json")