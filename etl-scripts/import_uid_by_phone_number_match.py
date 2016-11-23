import sys
import config
import MySQLdb
import MySQLdb.converters
import re

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

# Populate Array with all mobile values from Phoenix DB
cur.execute("SELECT * FROM dosomething.field_data_field_mobile WHERE field_mobile_value is NOT NULL")
result = cur.fetchall()

# Regex Checker to only allow numbers.
check_for_num = re.compile('^[0-9]+$')

for row in result:
    raw_phone_number = row['field_mobile_value'].replace("-", "").replace("(", "").replace(")", "").replace(".", "").replace(" ", "")
    if (len(raw_phone_number.strip()) == 11 and raw_phone_number.startswith('1') and check_for_num.match(raw_phone_number) is not None):
        sanitized_phone_number = raw_phone_number[1:11]
        sanitized_phone_number_lookup = "SELECT * FROM users_and_activities.mobile_user_lookup as mu WHERE mu.us_phone_number = (\"{0}\")".format(sanitized_phone_number)
        cur.execute(sanitized_phone_number_lookup)
        mobile_query_result = cur.fetchall()
        if not mobile_query_result:
            print ("No matching record for this phone number.")
        else:
            uid_check = mobile_query_result[0]['uid']
            if uid_check is None:
                uid = row['entity_id']
                update_query = "update users_and_activities.mobile_user_lookup set uid={0} where us_phone_number=\"{1}\"".format(uid,sanitized_phone_number)
                print(update_query)
                cur.execute(update_query)
                db.commit()
            else:
                print ("Already updated uid %s" % uid_check)
    elif (len(raw_phone_number.strip()) == 10 and check_for_num.match(raw_phone_number) is not None):
        sanitized_phone_number = raw_phone_number
        sanitized_phone_number_lookup = "SELECT * FROM users_and_activities.mobile_user_lookup as mu WHERE mu.us_phone_number = (\"{0}\")".format(sanitized_phone_number)
        cur.execute(sanitized_phone_number_lookup)
        mobile_query_result = cur.fetchall()
        if not mobile_query_result:
            print ("No matching record for this phone number.")
        else:
            uid_check = mobile_query_result[0]['uid']
            if uid_check is None:
                uid = row['entity_id']
                update_query = "update users_and_activities.mobile_user_lookup set uid={0} where us_phone_number=\"{1}\"".format(uid,sanitized_phone_number)
                print(update_query)
                cur.execute(update_query)
                db.commit()
            else:
                print ("Already updated uid %s" % uid_check)
    else:
        print ("%s is not a valid phone number" % raw_phone_number)

cur.close()
db.close()
