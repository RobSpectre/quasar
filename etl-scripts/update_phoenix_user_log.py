import MySQLdb
import config
import time

# Setup DB Connection
db = MySQLdb.connect(host=config.host, #hostname
          user=config.user, #  username
          passwd=config.pw, #  password
          db='phoenix_user_snapshots' # source and staging area for snapshots
          )

cur = db.cursor()

# Record start time
start_time = time.time()

# Get List of All Phoenix Staging Tables
cur.execute("SHOW TABLES LIKE '%_staging_changes';")
db.commit()

# Delete all pre-existing staging tables for re-runs of script
old_staging_tables = cur.fetchall()
for x in old_staging_tables:
    staging_table_name = str(x).strip('(').strip(')').strip(',').strip("'")
    print("Dropping " + staging_table_name)
    drop_table = "DROP TABLE IF EXISTS "  + staging_table_name
    cur.execute(drop_table)
    db.commit()

# Get List of All Phoenix Snapshots
cur.execute("SHOW TABLES;")
db.commit()

# Iterate over tables and print name, testing
table_snapshot = cur.fetchall()
for x in table_snapshot:
    table_name = str(x).strip('(').strip(')').strip(',').strip("'")
    print ("Running daily diff from " + table_name)
    create_table = ('CREATE TABLE '+ table_name + '_staging_changes SELECT uid, name, mail, created, access, login, status, timezone, language'
                    ' FROM '
                    '('
                    'select uid, name, mail, created, access, login, status, timezone, language'
                    ' from quasar.phoenix_user_log_poc as p1'
                    ' UNION ALL'
                    ' select uid, name, mail, created, access, login, status, timezone, language'
                    ' from ' + table_name + ' as p2'
                    ') p'
                    ' GROUP BY uid, access'
                    ' HAVING COUNT(*) = 1'
                    ' ORDER BY uid;')
    cur.execute(create_table)
    db.commit()

# Get List of All Newly Generated Phoenix Staging Tables
cur.execute("SHOW TABLES LIKE '%_staging_changes';")
db.commit()

staging_tables = cur.fetchall()
for x in staging_tables:
    staging_table_name = str(x).strip('(').strip(')').strip(',').strip("'")
    print("Importing " + staging_table_name)
    import_table = "INSERT IGNORE into quasar.phoenix_user_log_poc select * from " + staging_table_name
    cur.execute(import_table)
    db.commit()
    print("Dropping " + staging_table_name)
    drop_table = "DROP TABLE IF EXISTS "  + staging_table_name
    cur.execute(drop_table)
    db.commit()

# Close Cursor and Connection
cur.close()
db.close()

# Show Total Time Run Takes
end_time = time.time()
duration = end_time - start_time
print ('duration: ', duration)
