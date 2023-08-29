import psycopg2

class RdsModule:

    def __init__(self):
        self.conn = None
        self.cur = None

    def get_cursor(self,conn_info):
        self.conn = psycopg2.connect(**conn_info)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        return self.cur

    def close_connection(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def sql_create_users_table(self):
        return """
          CREATE TABLE IF NOT EXISTS users (
            summonerid VARCHAR(63) PRIMARY KEY,
            puuid VARCHAR(78) NULL,
            match_num SMALLINT NOT NULL,
            tier VARCHAR(16) NOT NULL,
            last_update INTEGER NOT NULL
          );
          """

    def sql_users_insert_record(self, values):
        return f"""
          INSERT INTO users
            (
              summonerid,
              match_num, tier,
              last_update
            )
          VALUES
            {values}
          ON CONFLICT DO NOTHING;
          """

    def sql_update_puuid(self, puuid, summonerid):
        return f"""
          UPDATE users
          SET puuid = '{puuid}'
          WHERE summonerid = '{summonerid}';
          """

