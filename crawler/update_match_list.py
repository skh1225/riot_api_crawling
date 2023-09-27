import time
from datetime import datetime
import argparse

from utils.api_modules import ApiModule
from utils.rds_modules import RdsModule
from config import config

def main():
  parser = argparse.ArgumentParser(description='puuid 를 업데이트합니다.')
  parser.add_argument('-k', '--key', type=int, default=0, help='config.config.api_keys num')
  parser.add_argument('-e', '--effi', type=int, default=50, help='config.config.api_keys num')
  args = parser.parse_args()

  api_key = config.api_keys[args.key]
  effi = args.effi

  crawler = ApiModule(api_key)
  rds = RdsModule()
  cur = rds.get_cursor(config.rds_connection)
  cur.execute(rds.sql_create_match_table())
  cur.execute(rds.sql_select_users_effi(effi))
  for user_info in cur.fetchall():
    summonerid, puuid, tier, start = user_info
    if not puuid:
      puuid, sleep_time = crawler.get_puuid(summonerid)
      cur.execute(rds.sql_update_puuid(puuid, summonerid))
      if sleep_time > 0:
        print(f'{datetime.now()} wait for api rate init {sleep_time}s...')
        time.sleep(sleep_time)
    while True:
      match_list, sleep_time = crawler.get_match_list(puuid, start)
      if match_list:
        start += len(match_list)
        query = [f"('{matchid}','{tier}')" for matchid in match_list]
        cur.execute(rds.sql_match_insert_record(','.join(query)))
      if sleep_time > 0:
        print(f'{datetime.now()} wait for api rate init {sleep_time}s...')
        time.sleep(sleep_time)
      if len(match_list) < 100:
        break
    cur.execute(rds.sql_users_update_num(puuid, start))
    print(f'{datetime.now()} {puuid} {tier} complete!')
  cur.execute(rds.sql_users_update_num_to_match_num())
  rds.close_connection()

if __name__ == "__main__":
  main()