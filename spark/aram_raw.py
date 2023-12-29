import argparse
import datetime

import pandas as pd

import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("--execution_date", type=datetime.date.fromisoformat, required=True, help="airflow execution date")


args = parser.parse_args()

execution_date = args.execution_date

spark = SparkSession \
    .builder \
    .appName('aram_raw') \
    .getOrCreate()

schema = StructType.fromJson(pd.read_json("gs://gnimty_bucket/schema/aram_schema.json"))
match_df = spark.read.schema(schema).parquet(f"gs://gnimty_bucket/raw/aram/{execution_date}/*.parquet")

drop_columns = ['participants','teams','participantId','bans']
drop_fields = ['perks']
match_df = match_df.select(
    F.col('metadata.tier'),
    F.col('metadata.gameVersion'),
    F.col('metadata.matchId'),
    F.col('metadata.gameDuration'),
    F.to_date((F.col("metadata.gameStartTimestamp")/1000+10800).cast('timestamp')).alias('date'),
    F.col('teams'),
    F.col('queueId'),
    F.explode('participants').alias('participant')
).withColumn('participant', F.col('participant').withField('statPerks', F.array(F.col('participant.perks.statPerks.defense'), F.col('participant.perks.statPerks.flex'), F.col('participant.perks.statPerks.offense'))))\
.withColumn('participant', F.col('participant').withField('primaryStyle',F.filter('participant.perks.styles',lambda x: x.description=='primaryStyle').style[0]))\
.withColumn('participant', F.col('participant').withField('subStyle',F.filter('participant.perks.styles',lambda x: x.description=='subStyle').style[0]))\
.withColumn('participant', F.col('participant').withField('primaryStyles',F.filter('participant.perks.styles',lambda x: x.description=='primaryStyle')[0].selections.perk))\
.withColumn('participant', F.col('participant').withField('subStyles',F.filter('participant.perks.styles',lambda x: x.description=='subStyle')[0].selections.perk))\
.withColumn('participant', F.col('participant').dropFields('perks'))\
.drop(*drop_columns)

struct_cols = []
for c in match_df.schema:
    if c.dataType in [LongType(),StringType(),DateType()]:
        struct_cols.append(F.col(c.name))
    else:
        for c_ in c.dataType.fields:
            struct_cols.append(F.col(f'{c.name}.{c_.name}').alias(f'{c.name[:1]}_{c_.name}'))


match_df.select(struct_cols) \
    .write.mode('overwrite') \
    .parquet(f"gs://gnimty_bucket/bigquery/aram/{execution_date}.parquet")
