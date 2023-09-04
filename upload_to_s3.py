import asyncio
import aiohttp
import json
import boto3
import time
from datetime import datetime
from config import config
from utils import rds_modules

class AsyncApiCall:
  def __init__(self):
    self.data=[]
    self.match_ids=[]
    self.is_last=False
    self.s3_conn = boto3.resource('s3', **config.s3_connection)
    self.rds_module = rds_modules.RdsModule()
    self.rds_cur = self.rds_module.get_cursor(config.rds_connection)
    self.lock=asyncio.Lock()

  def _upload_to_s3(self):
    s3object = self.s3_conn.Object('summoner-match', f'{int(time.time())}.json')
    self.rds_module.conn.autocommit = False

    try:
      for data in self.data:
        self.rds_cur.execute(self.rds_module.sql_match_update_status(data['metadata']['matchId']))
      s3object.put(Body=(bytes(json.dumps(self.data).encode('UTF-8'))))
      self.rds_module.conn.commit()
    except:
      self.rds_module.conn.rollback()
    print(f'{datetime.now()}: {len(self.data)} uploaded!')
    self.rds_module.conn.autocommit = True
    self.data = []
    self.rds_cur.execute("SELECT count(*) from match where status=True;")
    print(f'total_upload_rds: {self.rds_cur.fetchall()} ')

  def _match_refill(self, batch_size):
    self.rds_cur.execute(self.rds_module.sql_match_refill(batch_size))
    self.match_ids = self.rds_cur.fetchall()
    for match_id, _ in self.match_ids:
      self.rds_cur.execute(f"UPDATE match SET status=NULL WHERE matchid='{match_id}'")


  async def _request_data(self, session, match_id, tier, headers):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}"
    async with session.get(url, headers=headers) as response:
      data = await response.json()
      status_code = response.status
      headers = response.headers
      if status_code != 200:
        if status_code == 429:
          print(f'{datetime.now()}: 100 requests done!')
          await asyncio.sleep(int(headers['Retry-After']))
        elif status_code == 403:
          raise Exception(f'{datetime.now()}: {status_code} error!')
        print(f'{datetime.now()}: {status_code} retry!')
        return
      data['metadata'] = {
        "matchId": match_id,
        "tier": tier
      }
      ## print(f'{datetime.now()}: {match_id} request complete!')
      return data

  async def executor(self,api_num,session):
    batch_size = 1000

    headers = {
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": config.api_keys[api_num]
        }

    while True:
      await self.lock.acquire()
      if len(self.match_ids)==0:
        if self.is_last:
          if self.data:
            ## data to s3, db 반영, data init
            self._upload_to_s3()
          self.lock.release()
          break
        ## match_update
        self._match_refill(batch_size)
        if len(self.match_ids) < batch_size:
          self.is_last=True
          if len(self.match_ids) == 0:
            self.lock.release()
            break
      match_id, tier = self.match_ids.pop()
      self.lock.release()
      ## request, request rate control
      data = await self._request_data(session, match_id, tier, headers)
      await self.lock.acquire()
      ## match_id append to data
      if data:
        self.data.append(data)
      else:
        self.match_ids.append((match_id, tier))
      if len(self.data) == batch_size:
        ## data to s3, db 반영, data init
        self._upload_to_s3()
      self.lock.release()

  async def start(self,n):
    async with aiohttp.ClientSession() as session:
      await asyncio.gather(*[self.executor(i, session) for i in range(n)])

test = AsyncApiCall()

try:
  asyncio.run(test.start(3))
except:
  print("shut down...")
  test.rds_cur.execute("UPDATE match SET status=False WHERE status is NULL;")
  test.rds_module.close_connection()