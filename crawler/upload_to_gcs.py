import os
import asyncio
import aiohttp
import requests
import json
from google.cloud import storage
import time
import argparse
from datetime import datetime
from config import config
from utils import rds_modules

class AsyncApiCall:
  def __init__(self):
    self.data=[]
    self.match_ids=[]
    self.is_last=False
    self.gcs_client=storage.Client()
    self.rds_module=rds_modules.RdsModule()
    self.rds_cur=self.rds_module.get_cursor(config.rds_connection)
    self.lock=asyncio.Lock()

    self.items = None
    self.item_set = set()
    self.item_mythic = set()
    self.item_boots = set()

  def _get_item_info(self):
    latest_version = requests.get('https://ddragon.leagueoflegends.com/api/versions.json').json()[0]
    url_item = f'http://ddragon.leagueoflegends.com/cdn/{latest_version}/data/ko_KR/item.json'
    self.items = requests.get(url_item).json()['data']

    self.item_set = set(['6693']) # 자객의 발톱

    for k,v in self.items.items():
      if len(k) >= 6:
        continue
      if 'Boots' in v['tags']:
        self.item_boots.add(k)
        continue
      if 'from' in v and 'into' not in v and k not in ['4403', '2033', '7000', '2019']: # 황금 뒤집개, 부패물약, 암살자의 발톱, 강철 인장
        self.item_set.add(k)
      if '<rarityMythic>' in v['description']:
        self.item_mythic.add(k); self.item_set.add(k)
  
  def _extract_timeline_info(self, data):
    item = [[] for _ in range(10)]
    result = [{'skillTree':[], 'itemStart':[], 'itemRarity':[], 'itemBoots':None, 'itemMyth':None} for _ in range(10)]

    for frame in data['info']['frames']:
        for event in frame['events']:
            if event['type'] == 'SKILL_LEVEL_UP':
                result[event['participantId']-1]['skillTree'].append(event['skillSlot'])
            if event['type'] in ['ITEM_SOLD', 'ITEM_PURCHASED']:
                item[event['participantId']-1].append((str(event['itemId']), event['type']))
            elif event['type'] == 'ITEM_UNDO':
                item[event['participantId']-1].pop()

    for i in range(10):
      start_gold = 500
      for it in item[i]:
        if start_gold >= 0:
          if it[1] == 'ITEM_PURCHASED':
            start_gold -= self.items[it[0]]['gold']['base']
            if start_gold >= 0:
              result[i]['itemStart'].append(it[0])
              continue
            result[i]['itemStart'].sort()

        if it[1] == 'ITEM_PURCHASED':
          if it[0] in self.item_set:
            result[i]['itemRarity'].append(it[0])
            if it[0] in self.item_mythic:
              result[i]['itemMyth'] = it[0]
          elif it[0] in self.item_boots:
            result[i]['itemBoots'] = it[0]
    return result

  def _extract_match_info(self, data):
    metadata = set(['tier', 'matchId'])
    info = set(['gameVersion', 'gameDuration', 'gameCreation', 'participants', 'teams'])
    info_participants = set(['perks', 'summoner1Id', 'summoner2Id', 'championId', 'teamPosition', 'teamId', 'win', 'damageTakenOnTeamPercentage', 'teamDamagePercentage', 'champExperience', 'goldEarned', 'totalMinionsKilled', 'magicDamageDealtToChampions', 'physicalDamageDealtToChampions', 'trueDamageDealtToChampions', 'damageDealtToBuildings', 'damageDealtToObjectives', 'totalDamageTaken', 'totalHeal', 'totalHealsOnTeammates', 'timeCCingOthers', 'kills', 'deaths', 'assists', 'wardsKilled', 'wardsPlaced', 'detectorWardsPlaced', 'visionScore', 'firstBloodKill', 'firstTowerKill', 'gameEndedInSurrender', 'gameEndedInEarlySurrender'])
    for k in tuple(data['metadata'].keys())[:]:
        if k not in metadata:
            data['metadata'].pop(k, None) 
    for k in tuple(data['info'].keys())[:]:
        if k not in info:
            data['info'].pop(k, None)
    for participant in data['info']['participants']:
        participant['damageTakenOnTeamPercentage'] = participant['challenges']['damageTakenOnTeamPercentage']
        participant['teamDamagePercentage'] = participant['challenges']['teamDamagePercentage']
        for k in tuple(participant.keys())[:]:
            if k not in info_participants:
                participant.pop(k, None)

  def _upload_to_gcs(self):
    bucket = self.gcs_client.bucket('summoner-match')
    now = int(time.time())
    blob = bucket.blob(f"raw/{time.strftime('%Y-%m-%d', time.localtime(now))}/{now}.json")
    self.rds_module.conn.autocommit = False

    try:
      for data in self.data:
        self.rds_cur.execute(self.rds_module.sql_match_update_status(data['metadata']['matchId']))
      blob.upload_from_string(data=json.dumps(self.data),content_type='application/json')
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
    self._extract_match_info(data)
    
    return data
  
  async def _request_timeline(self, session, match_id, tier, headers):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}"

    async with session.get(url+'/timeline', headers=headers) as response:
      timeline_data = await response.json()
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
    result = self._extract_timeline_info(timeline_data)
    
    return result

  async def executor(self,api_num,session,batch_size):
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
            self._upload_to_gcs()
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
      try:
        data = {}; timeline = {}
        data = await self._request_data(session, match_id, tier, headers)
        timeline = await self._request_timeline(session, match_id, tier, headers)
      except IndexError:
        continue
      except Exception as e:
        print(e, api_num)
        if e == 'Server disconnected':
          raise e
      
      await self.lock.acquire()
      ## match_id append to data

      if not data or not timeline:
        self.match_ids.append((match_id, tier))
      else:
        try:
          for i in range(10):
            data['info']['participants'][i].update(timeline[i])
          self.data.append(data)
        except IndexError:
          self.lock.release()
          continue

      if len(self.data) == batch_size:
        ## data to s3, db 반영, data init
        self._upload_to_gcs()
      self.lock.release()

  async def start(self,start,end,batch_size):
    async with aiohttp.ClientSession() as session:
      await asyncio.gather(*[self.executor(i, session, batch_size) for i in range(start, end)])


if __name__ == "__main__":
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./config/fourth-way-398009-b0ce29a3bf47.json"
  parser = argparse.ArgumentParser(description='매치 데이터를 업데이트 합니다.')
  parser.add_argument('-s','--start', type=int, default=0, help='사용할 api key 시작 index')
  parser.add_argument('-e','--end', type=int, default=12, help='사용할 api key 끝 index')
  parser.add_argument('-b','--batch', type=int, default=9000, help='사용할 api key의 수')

  args = parser.parse_args()

  test = AsyncApiCall()
  test._get_item_info()

  try:
    test.rds_cur.execute("UPDATE match SET status=False WHERE status is NULL;")
    asyncio.run(test.start(args.start,args.end,args.batch))
  except Exception as e:
    print(e)
    print("shut down...")
    test.rds_cur.execute("UPDATE match SET status=False WHERE status is NULL;")
    test.rds_module.close_connection()
