# tests/check_timescale.py
import psycopg2
from config import TS_HOST, TS_PORT, TS_DB, TS_USER, TS_PASSWORD

def main():
    conn = psycopg2.connect(host=TS_HOST, port=TS_PORT, dbname=TS_DB, user=TS_USER, password=TS_PASSWORD)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM measurements;")
    r = cur.fetchone()
    print("Row count in measurements:", r[0])
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
