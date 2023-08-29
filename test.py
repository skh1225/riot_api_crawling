rds_connection = {
    "dbname": "db1",
    "user": "skh",
    "password": "seokh1249!",
    "host": "riot-api.crsfbioddegg.ap-northeast-2.rds.amazonaws.com",
    "port": 5432
}

from utils.rds_modules import RdsModule


rds = RdsModule()

rds.get_cursor(**rds)