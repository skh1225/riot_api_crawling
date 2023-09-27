import time
from datetime import datetime
import requests

class ApiModule:

  update_user_url = {
      "MASTER": "https://kr.api.riotgames.com/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5",
      "GRANDMASTER": "https://kr.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5",
      "CHALLENGER": "https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5",
      "else": "https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5",
      "puuid": "https://kr.api.riotgames.com/lol/summoner/v4/summoners",
      "match_list": "https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid"
  }

  division = {
    1: 'I',
    2: 'II',
    3: 'III',
    4: 'IV'
  }

  def __init__(self, api_key, timestamp=0):
     self.headers = {
        "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://developer.riotgames.com",
        "X-Riot-Token": api_key
        }
     self.request_num = 0
     self.timestamp = timestamp

  def _check_limit(self, limit_count):
    second, minute = limit_count.split(',')
    s_cnt, m_cnt = second.split(':')[0], minute.split(':')[0]

    self.request_num = int(m_cnt)

    if m_cnt == '1':
      self.timestamp = int(time.time()) + 120
    elif m_cnt == '100':
      return self.timestamp - int(time.time())
    elif s_cnt == '20':
      return 1
    return 0

  def get_user_entries(self, tier, div=None, page=None):
    '''
    get user entries
    tier : ['EMERALD', 'DIAMOND', 'MASTER', 'GRANDMASTER', 'CHALLENGER']
    division : ['I', 'II', 'III', 'IV']
    '''
    while True:
      if page == None or page%5 == 0:
        print(f'{datetime.now()} tier/div/page : {tier}/{div}/{page}')
      if tier in ['MASTER', 'GRANDMASTER', 'CHALLENGER']:
        url = self.update_user_url[tier]
        res = requests.get(url, headers=self.headers)
        result = res.json()['entries']
      else:
        url = self.update_user_url['else'] + f'/{tier}/{self.division[div]}'
        res = requests.get(url, headers=self.headers, params={ "page": page })
        result = res.json()
      if res.status_code != 200:
        if res.status_code == 503:
          continue
        elif res.status_code == 429:
          time.sleep(int(res.headers['Retry-After']))
          continue
        else:
          raise Exception(f'status_code: {res.status_code}')
      sleep_time = self._check_limit(res.headers['X-App-Rate-Limit-Count'])
      break
    return result, sleep_time

  def get_puuid(self,summonerid):
    url = self.update_user_url['puuid'] + f'/{summonerid}'
    while True:
      res = requests.get(url, headers=self.headers)
      if res.status_code != 200:
        if res.status_code == 503:
          continue
        elif res.status_code == 429:
          time.sleep(int(res.headers['Retry-After']))
          continue
        else:
          raise Exception(f'status_code: {res.status_code}')
      sleep_time = self._check_limit(res.headers['X-App-Rate-Limit-Count'])
      result = res.json()['puuid']
      break
    return result, sleep_time

  def get_match_list(self,puuid,start):
    url = self.update_user_url['match_list'] + f'/{puuid}/ids'
    params = {
      "startTime": 1689735600,
      "queue": 420,
      "type": "ranked",
      "start": start,
      "count": 100
    }
    while True:
      res = requests.get(url, headers=self.headers, params=params)
      if res.status_code != 200:
        if res.status_code == 503:
          continue
        elif res.status_code == 429:
          time.sleep(int(res.headers['Retry-After']))
          continue
        else:
          raise Exception(f'status_code: {res.status_code}')
      sleep_time = self._check_limit(res.headers['X-App-Rate-Limit-Count'])
      result = res.json()
      break

    return result, sleep_time



