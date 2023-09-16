import argparse
import datetime

import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import *

parser = argparse.ArgumentParser()
parser.add_argument("--execution_date", type=datetime.date.fromisoformat, required=True, help="airflow execution date")
parser.add_argument("--match", action='store_true', help="skip match df")
parser.add_argument("--champion", action='store_true', help="skip champion df")
# parser.add_argument("--pick", action='store_true', help="skip pick df")
# parser.add_argument("--ban", action='store_true', help="skip ban df")

args = parser.parse_args()

execution_date = args.execution_date

spark = SparkSession \
    .builder \
    .appName('sparkjob-test') \
    .getOrCreate()

df = spark.read.json(f"gs://summoner-match/raw/{execution_date}/*.json").where(F.col('info.gameVersion') != '').coalesce(12)
df.cache()

champ_df = df.select(
    F.col('metadata.tier'),
    F.col('info.gameVersion'),
    F.col('info.gameDuration'),
    F.col('metadata.matchId'),
    F.to_date((F.col("info.gameCreation")/1000-21600).cast('timestamp')).alias('date'),
    F.explode('info.participants').alias('participants')
    ).select(
    F.col('tier'),
    F.col('gameVersion'),
    F.col('gameDuration'),
    F.col('matchId'),
    F.col('date'),
    F.array(
      F.col('participants.perks.statPerks.defense'),
      F.col('participants.perks.statPerks.flex'),
      F.col('participants.perks.statPerks.offense')
      ).alias('statPerks'),
    F.array_sort(F.array(F.col('participants.summoner1Id'),F.col('participants.summoner2Id'))).alias('summonerSpell'),
    F.col('participants.perks.styles.style')[0].alias('primaryStyle'),
    F.col('participants.perks.styles.style')[1].alias('subStyle'),
    F.col('participants.perks.styles.selections')[0]['perk'].alias('primaryStyles'),
    F.col('participants.perks.styles.selections')[1]['perk'].alias('subStyles'),
    F.col('participants.championId'),
    F.col('participants.teamPosition'),
    F.col('participants.win'),
    F.col('participants.challenges.damageTakenOnTeamPercentage'),
    F.col('participants.challenges.teamDamagePercentage'),
    F.col('participants.champExperience'),
    F.col('participants.goldEarned'),
    F.col('participants.totalMinionsKilled'),
    F.col('participants.magicDamageDealtToChampions'),
    F.col('participants.physicalDamageDealtToChampions'),
    F.col('participants.trueDamageDealtToChampions'),
    F.col('participants.damageDealtToBuildings'),
    F.col('participants.damageDealtToObjectives'),
    F.col('participants.totalDamageTaken'),
    F.col('participants.totalHeal'),
    F.col('participants.totalHealsOnTeammates'),
    F.col('participants.timeCCingOthers'),
    F.col('participants.kills'),
    F.col('participants.deaths'),
    F.col('participants.assists'),
    F.col('participants.wardsKilled'),
    F.col('participants.wardsPlaced'),
    F.col('participants.detectorWardsPlaced'),
    F.col('participants.visionScore'),
    F.col('participants.firstBloodKill'),
    F.col('participants.firstTowerKill'),
    F.col('participants.gameEndedInSurrender'),
    F.col('participants.gameEndedInEarlySurrender'),
    )

if args.champion:
  print("champion_df is skipped")
else:
  champ_df.withColumn('execution_date', F.to_date(F.lit(execution_date))).repartition(1) \
    .write.mode('overwrite').parquet(f"gs://summoner-match/processed/champion/{execution_date}.parquet")

match_df = df.select(
    F.col('metadata.matchId'),
    F.col('metadata.tier'),
    F.col('info.gameDuration'),
    F.col('info.gameVersion'),
    F.to_date((F.col("info.gameCreation")/1000-21600).cast('timestamp')).alias('date'),
    F.col('info.teams.win')[0].alias('BLUE_WIN'),
    F.filter('info.participants',lambda x: (x.teamId==100) & (x.teamPosition=='TOP')).championId[0].alias('BLUE_TOP'),
    F.filter('info.participants',lambda x: (x.teamId==100) & (x.teamPosition=='JUNGLE')).championId[0].alias('BLUE_JUG'),
    F.filter('info.participants',lambda x: (x.teamId==100) & (x.teamPosition=='MIDDLE')).championId[0].alias('BLUE_MID'),
    F.filter('info.participants',lambda x: (x.teamId==100) & (x.teamPosition=='BOTTOM')).championId[0].alias('BLUE_ADC'),
    F.filter('info.participants',lambda x: (x.teamId==100) & (x.teamPosition=='UTILITY')).championId[0].alias('BLUE_SUP'),
    F.filter('info.participants',lambda x: (x.teamId==200) & (x.teamPosition=='TOP')).championId[0].alias('RED_TOP'),
    F.filter('info.participants',lambda x: (x.teamId==200) & (x.teamPosition=='JUNGLE')).championId[0].alias('RED_JUG'),
    F.filter('info.participants',lambda x: (x.teamId==200) & (x.teamPosition=='MIDDLE')).championId[0].alias('RED_MID'),
    F.filter('info.participants',lambda x: (x.teamId==200) & (x.teamPosition=='BOTTOM')).championId[0].alias('RED_ADC'),
    F.filter('info.participants',lambda x: (x.teamId==200) & (x.teamPosition=='UTILITY')).championId[0].alias('RED_SUP'),
    F.array_distinct(F.concat(F.col('info.teams.bans')[0].championId,F.col('info.teams.bans')[1].championId)).alias('BANS'),
    F.col('info.teams.objectives.baron.kills')[0].alias('BLUE_BARON'),
    F.col('info.teams.objectives.dragon.kills')[0].alias('BLUE_DRAGON'),
    F.col('info.teams.objectives.riftHerald.kills')[0].alias('BLUE_RIFT_HERALD'),
    F.col('info.teams.objectives.baron.kills')[1].alias('RED_BARON'),
    F.col('info.teams.objectives.dragon.kills')[1].alias('RED_DRAGON'),
    F.col('info.teams.objectives.riftHerald.kills')[1].alias('RED_RIFT_HERALD')
    )

if args.match:
  print("match_df is skipped")
else:
  match_df.withColumn('execution_date', F.to_date(F.lit(execution_date))).repartition(1) \
    .write.mode('overwrite').parquet(f"gs://summoner-match/processed/match/{execution_date}.parquet")

# match_df.write.format('bigquery') \
#   .option('temporaryGcsBucket', 'summoner-match-bigquery') \
#   .option('table', 'fourth-way-398009.summoner_match.match_data') \
#   .mode("append") \
#   .save()

## ++Bigquery
# match_df.createOrReplaceTempView('match_info')

# total_sdf = spark.sql("""
#                       SELECT date, tier, COUNT(*) as total
#                       FROM match_info
#                       GROUP BY date, tier
#                       """)
# total_sdf.cache()
# total_sdf.createOrReplaceTempView('total_match')

# ### gamecreation으로 Groupby 필요
# df_pick = spark.sql("""
#                     SELECT BLUE_ADC as championId, 'ADC' as position, CASE WHEN BLUE_WIN=True THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT RED_ADC as championId, 'ADC' as position, CASE WHEN BLUE_WIN=False THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT BLUE_TOP as championId, 'TOP' as position, CASE WHEN BLUE_WIN=True THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT RED_TOP as championId, 'TOP' as position, CASE WHEN BLUE_WIN=False THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT BLUE_MID as championId, 'MID' as position, CASE WHEN BLUE_WIN=True THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT RED_MID as championId, 'MID' as position, CASE WHEN BLUE_WIN=False THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT BLUE_JUG as championId, 'JUG' as position, CASE WHEN BLUE_WIN=True THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT RED_JUG as championId, 'JUG' as position, CASE WHEN BLUE_WIN=False THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT BLUE_SUP as championId, 'SUP' as position, CASE WHEN BLUE_WIN=True THEN 1 END as win, tier, date FROM match_info
#                     UNION ALL
#                     SELECT RED_SUP as championId, 'SUP' as position, CASE WHEN BLUE_WIN=False THEN 1 END as win, tier, date FROM match_info
#                     ;""")
# df_pick.createOrReplaceTempView('pick_info')


# spark.sql("""
#           SELECT
#           date,
#           tier,
#           championId,
#           COUNT(win) as win,
#           COUNT(*) as pick
#           FROM pick_info
#           GROUP BY championId,tier,date;
#           """).createOrReplaceTempView('champion_pick')

# spark.sql("""
#           SELECT tier, date, championId, COUNT(*) as ban FROM (
#           SELECT tier, date, EXPLODE(BANS) as championId FROM match_info
#           )
#           WHERE championId!=-1
#           GROUP BY tier,date,championId;
#           """).createOrReplaceTempView('champion_ban')

# if args.pick:
#   print("pick_df is skipped")
# else:
#   spark.sql("""
#             SELECT a.tier, a.date, a.championId, a.win, a.pick, b.total
#             FROM champion_pick a LEFT JOIN total_match b
#             ON a.tier=b.tier and a.date=b.date
#             """).withColumn('execution_date', F.to_date(F.lit(execution_date))).coalesce(6) \
#     .write.parquet(f"gs://summoner-match/processed/pick/{execution_date}.parquet")

# if args.ban:
#   print("ban_df is skipped")
# else:
#   spark.sql("""
#             SELECT a.tier, a.date, a.championId, a.ban, b.total
#             FROM champion_ban a LEFT JOIN total_match b
#             ON a.tier=b.tier and a.date=b.date
#             """).withColumn('execution_date', F.to_date(F.lit(execution_date))).coalesce(6) \
#     .write.parquet(f"gs://summoner-match/processed/ban/{execution_date}.parquet")


