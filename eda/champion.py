import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/skh/Documents/workspace/project/lol_crawler/config/fourth-way-398009-b0ce29a3bf47.json"

import requests
from google.cloud import bigquery
url = "https://ddragon.leagueoflegends.com/cdn/13.16.1/data/ko_KR/champion.json"

response = requests.get(url)

data = response.json()

sql_create_table = """
  CREATE TABLE IF NOT EXISTS `fourth-way-398009.summoner_match.champion_info` (
  id INTEGER,
  name STRING
  );
"""

sql_insert_values = """
INSERT INTO `fourth-way-398009.summoner_match.champion_info` (id, name) VALUES
"""

for _, value in data['data'].items():
  sql_insert_values += f"({value['key']}, '{value['id']}'),"

client = bigquery.Client()

client.query(sql_create_table)
client.query(sql_insert_values[:-1])