import psycopg2

class RdsModule:

    def __init__(self):
        self.conn = None
        self.cur = None

    def get_cursor(self,conn_info,autocommit=True):
        self.conn = psycopg2.connect(**conn_info)
        self.conn.autocommit = autocommit
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
            update_num SMALLINT NOT NULL DEFAULT 0,
            tier VARCHAR(16) NOT NULL
          );
          """

    def sql_create_match_table(self):
        return """
          CREATE TABLE IF NOT EXISTS match (
            matchid VARCHAR(16) PRIMARY KEY,
            tier VARCHAR(16) NOT NULL,
            status BOOLEAN NULL DEFAULT FALSE
          );
          """

    def sql_users_insert_record(self, values):
        return f"""
          INSERT INTO users
            (
              summonerid,
              match_num,
              tier
            )
          VALUES
            {values}
          ON CONFLICT (summonerid) DO UPDATE SET
          (match_num, tier) = (EXCLUDED.match_num, EXCLUDED.tier);
          """

    def sql_match_insert_record(self, values):
        return f"""
          INSERT INTO match
            (
              matchid,
              tier
            )
          VALUES
            {values}
          ON CONFLICT (matchid) DO NOTHING;
          """

    def sql_users_update_num_to_match_num(self):
        return f"""
          UPDATE users
          SET update_num = match_num
          WHERE update_num = 0;
          """

    def sql_users_update_num(self, puuid, start):
        return f"""
          UPDATE users
          SET update_num = {start}
          WHERE puuid = '{puuid}';
          """

    def sql_update_puuid(self, puuid, summonerid):
        return f"""
          UPDATE users
          SET puuid = '{puuid}'
          WHERE summonerid = '{summonerid}';
          """

    def sql_select_users(self):
        return 'SELECT * FROM users;'

    def sql_select_users_effi(self,effi):
        return f"""
        SELECT summonerid, puuid, tier, update_num
        FROM users
        WHERE (match_num-update_num)/NULLIF((match_num-update_num+99)/100+CASE WHEN puuid is NULL THEN 1 ELSE 0 END,0) > {effi}
        ORDER BY
        CASE
          WHEN tier='CHALLENGER' THEN 1
          WHEN tier='GRANDMASTER' THEN 2
          WHEN tier='MASTER' THEN 3
          WHEN tier='DIAMOND' THEN 4
          WHEN tier='EMERALD' THEN 5
          WHEN tier='PLATINUM' THEN 6
          ELSE 7
        END;
        """

