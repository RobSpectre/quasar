import sys
import config
import MySQLdb
import MySQLdb.converters

### Create DB Connection with Appropriate Conversions
#   Conversions inherited from Josh's other ETL scripts.
conv_dict = MySQLdb.converters.conversions.copy()
conv_dict[246]=float
conv_dict[8]=int
#open connection
db = MySQLdb.connect(host=config.host, #hostname
      user=config.user, #  username
      passwd=config.pw, #  password
      conv=conv_dict) # datatype conversions
      #set cursor object, and set to dict dursor
cur = db.cursor(MySQLdb.cursors.DictCursor)

cur.execute("SELECT * FROM users_and_activities.mobile_campaign_id_lookup")
result = cur.fetchall()

for row in result:
    if row['nid'] is not None:
        if "alpha" in row['opt_in_path_id_name'] or "Alpha" in row['opt_in_path_id_name']:
            nid = row['nid']
            campaign_id = row['mobile_campaign_id']
            opt_in_id = row['opt_in_path_id']
            web_alpha = "1"
            if row['run_nid'] is None:
                campaign_run = "NULL"
                insert_campaign = "insert ignore into users_and_activities.mobile_campaign_ids VALUES({0}, {1}, {2}, {3}, \"{4}\")".format(nid, campaign_id, opt_in_id, web_alpha, campaign_run)
            else:
                campaign_run = row['run_nid']
                insert_campaign = "insert ignore into users_and_activities.mobile_campaign_ids VALUES({0}, {1}, {2}, {3}, {4})".format(nid, campaign_id, opt_in_id, web_alpha, campaign_run)
            print(insert_campaign)
            cur.execute(insert_campaign)
            db.commit()
        else:
            nid = row['nid']
            campaign_id = row['mobile_campaign_id']
            opt_in_id = row['opt_in_path_id']
            web_alpha = "0"
            if row['run_nid'] is None:
                campaign_run = "NULL"
                insert_campaign = "insert ignore into users_and_activities.mobile_campaign_ids VALUES({0}, {1}, {2}, {3}, \"{4}\")".format(nid, campaign_id, opt_in_id, web_alpha, campaign_run)
            else:
                campaign_run = row['run_nid']
                insert_campaign = "insert ignore into users_and_activities.mobile_campaign_ids VALUES({0}, {1}, {2}, {3}, {4})".format(nid, campaign_id, opt_in_id, web_alpha, campaign_run)
            print(insert_campaign)
            cur.execute(insert_campaign)
            db.commit()

cur.close()
db.close()
