import time
from datetime import datetime
import argparse

from utils.api_modules import ApiModule
from utils.rds_modules import RdsModule
from config import config

def main():
  parser = argparse.ArgumentParser(description='유저 엔트리를 업데이트합니다.')
  parser.add_argument('-t', '--tier', default='EMERALD,DIAMOND,MASTER,GRANDMASTER,CHALLENGER', help='ex) PLATINUM,EMERALD')
  parser.add_argument('-k', '--key', type=int, default=0, help='config.config.api_keys num')
  parser.add_argument('-u','--update', action='store_true')
  args = parser.parse_args()

  api_key = config.api_keys[args.key]
  tier_list = args.tier.split(',')

  crawler = ApiModule(api_key)
  rds = RdsModule()
  cur = rds.get_cursor(config.rds_connection)
  cur.execute(rds.sql_create_users_table())

  for tier in tier_list:
    if tier in ['MASTER', 'GRANDMASTER', 'CHALLENGER']:
      result, sleep_time = crawler.get_user_entries(tier)
      query = []
      for entry in result:
        query.append(f"('{entry['summonerId']}',{entry['wins']+entry['losses']},'{tier}'")
      try:
        cur.execute(rds.sql_users_insert_record(','.join(query)))
      except Exception as e:
        print(f"{tier} 정보를 가져오는 과정에서 문제 발생")
        raise e

      if sleep_time > 0:
        print(f'{datetime.now()} wait for api rate init {sleep_time}s...')
        time.sleep(sleep_time)
    else:
      page, div = 1, 1
      while True:
        result, sleep_time = crawler.get_user_entries(tier,div,page)
        query = []
        for entry in result:
          query.append(f"('{entry['summonerId']}',{entry['wins']+entry['losses']},'{tier}')")

        if query:
          try:
            cur.execute(rds.sql_users_insert_record(','.join(query)))
          except Exception as e:
            print(f"{tier}/{div}/{page} 정보를 가져오는 과정에서 문제 발생")
            raise e

        if sleep_time > 0:
          print(f'{datetime.now()} wait for api rate init {sleep_time}s...')
          time.sleep(sleep_time)
        if len(result) < 205:
          page = 1
          if div == 4:
            break
          div += 1
        else:
          page += 1
  if args.update:
    cur.execute(rds.sql_users_update_num_to_match_num())
  rds.close_connection()

if __name__ == "__main__":
  main()