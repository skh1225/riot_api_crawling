import time
from datetime import datetime

from utils.api_modules import ApiModule
from utils.rds_modules import RdsModule
from config import config

def user_crawler(api_key, tier_list, start_time):
  crawler = ApiModule(api_key)
  rds = RdsModule()
  cur = rds.get_cursor(config.rds_connection)
  cur.execute(rds.sql_create_users_table())

  for tier in tier_list:
    if tier in ['MASTER', 'GRANDMASTER', 'CHALLENGER']:
      result, sleep_time = crawler.get_user_entries(tier)
      query = []
      for entry in result:
        query.append(f"('{entry['summonerId']}',{entry['wins']+entry['losses']},'{tier}',{start_time})")
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
          query.append(f"('{entry['summonerId']}',{entry['wins']+entry['losses']},'{tier}',{start_time})")

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
  rds.close_connection()

tier_list = ['EMERALD', 'DIAMOND', 'MASTER', 'GRANDMASTER', 'CHALLENGER']
start_time = '1689735600'
api_key = config.api_keys[0]

user_crawler(api_key, tier_list, start_time)