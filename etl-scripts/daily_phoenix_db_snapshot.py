import MySQLdb
import time
import config

db = MySQLdb.connect(host=config.host, #hostname
          user=config.user, #  username
          passwd=config.pw, #  password
          db='phoenix_user_snapshots') # datatype conversions

cur = db.cursor()

snapshot_time = str(int(time.time()))

#Create Table Schema Snapshot
run_query = "CREATE TABLE IF NOT EXISTS phoenix_users_snapshot_" + snapshot_time + """ LIKE dosomething.users"""
cur.execute(run_query)

# Insert Daily Data from DS Users Table
run_query = "INSERT phoenix_users_snapshot_" + snapshot_time + """ SELECT * FROM dosomething.users"""
cur.execute(run_query)
db.commit()

# Close Cursor and Connection
cur.close()
db.close()
