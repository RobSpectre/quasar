import MySQLdb
import MySQLdb.converters
import config

# Setup DB Connnection
conv_dict = MySQLdb.converters.conversions.copy()
conv_dict[246]=float
conv_dict[3]=int

db = MySQLdb.connect(host=config.host, #hostname
          user=config.user, #  username
          passwd=config.pw, #  password
          conv=conv_dict) # datatype conversions

cur = db.cursor()

# Drop All Existing Tables for Demographics
cur.execute("DROP TABLE IF EXISTS users_and_activities.action_gender")
cur.execute("DROP TABLE IF EXISTS users_and_activities.action_income")
cur.execute("DROP TEMPORARY TABLE IF EXISTS users_and_activities.active_mobile_users, users_and_activities.drupal_mobile")
cur.execute("DROP TABLE IF EXISTS users_and_activities.baseline_mobile_age")
cur.execute("DROP TABLE IF EXISTS users_and_activities.baseline_mobile_gender")
cur.execute("DROP TABLE IF EXISTS users_and_activities.baseline_mobile_income")
cur.execute("DROP TABLE IF EXISTS users_and_activities.baseline_mobile_race")
cur.execute("DROP TABLE IF EXISTS users_and_activities.baseline_web_age")
cur.execute("DROP TABLE IF EXISTS users_and_activities.baseline_web_gender")
cur.execute("DROP TABLE IF EXISTS users_and_activities.baseline_web_income")
cur.execute("DROP TABLE IF EXISTS users_and_activities.baseline_web_race")
cur.execute("DROP TABLE IF EXISTS users_and_activities.cause_gender")
cur.execute("DROP TABLE IF EXISTS users_and_activities.cause_income")

db.commit()

runquery = """CREATE TABLE users_and_activities.action_gender
         SELECT td.name, wu.gender, COUNT(*) count
         FROM users_and_activities.web_users wu
         JOIN dosomething.dosomething_signup ds
         ON wu.uid = ds.uid
         JOIN dosomething.node n
         ON ds.nid = n.nid
         JOIN dosomething.field_data_field_action_type ac
         ON ds.nid = ac.entity_id
         JOIN dosomething.taxonomy_term_data td
         ON ac.field_action_type_tid = td.tid
         WHERE wu.gender IS NOT NULL
         GROUP BY td.name, wu.gender"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.action_income
         SELECT td.name, wu.income_level, COUNT(*) count
         FROM users_and_activities.web_users wu
         JOIN dosomething.dosomething_signup ds
         ON wu.uid = ds.uid
         JOIN dosomething.node n
         ON ds.nid = n.nid
         JOIN dosomething.field_data_field_action_type ac
         ON ds.nid = ac.entity_id
         JOIN dosomething.taxonomy_term_data td
         ON ac.field_action_type_tid = td.tid
         WHERE wu.income_level IS NOT NULL
         GROUP BY td.name, wu.income_level"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TEMPORARY TABLE users_and_activities.active_mobile_users
         SELECT phone_number
         FROM users_and_activities.mobile_users
         WHERE status = 'Active Subscriber';"""
cur.execute(runquery)
db.commit()
runquery = """ALTER TABLE users_and_activities.active_mobile_users
         ADD PRIMARY KEY (phone_number)"""
cur.execute(runquery)
db.commit()
runquery = """CREATE TEMPORARY TABLE users_and_activities.drupal_mobile (
         uid INT(11) NOT NULL DEFAULT 0,
         phone_number VARCHAR(255) UNIQUE DEFAULT NULL,
         PRIMARY KEY (uid))"""
cur.execute(runquery)
db.commit()
runquery = """INSERT IGNORE INTO users_and_activities.drupal_mobile
         SELECT dsm.entity_id, 10000000000 + dsm.field_mobile_value
         FROM dosomething.field_data_field_mobile dsm"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.baseline_mobile_age
         SELECT TIMESTAMPDIFF(YEAR, wu.birthdate, CURDATE()) age, COUNT(*) count
         FROM users_and_activities.drupal_mobile dm
         JOIN users_and_activities.web_users wu
         ON dm.uid = wu.uid
         JOIN users_and_activities.active_mobile_users mu
         ON dm.phone_number = mu.phone_number
         WHERE wu.birthdate IS NOT NULL
         AND wu.birthdate >= '1915-01-01'
         AND wu.birthdate < '2006-01-01'
         GROUP BY age"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.baseline_mobile_gender
         SELECT mu.gender, COUNT(*) count
         FROM users_and_activities.mobile_users mu
         WHERE mu.gender IS NOT NULL
         AND mu.status = 'Active Subscriber'
         GROUP BY mu.gender"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.baseline_mobile_income
         SELECT mu.income_level, COUNT(*) count
         FROM users_and_activities.mobile_users mu
         WHERE mu.income_level IS NOT NULL
         AND mu.status = 'Active Subscriber'
         GROUP BY mu.income_level"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.baseline_mobile_race
         SELECT mu.race, COUNT(*) count
         FROM users_and_activities.mobile_users mu
         WHERE mu.race IS NOT NULL
         AND mu.status = 'Active Subscriber'
         GROUP BY mu.race"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.baseline_web_age
         SELECT TIMESTAMPDIFF(YEAR, wu.birthdate, CURDATE()) age, COUNT(*) count
         FROM users_and_activities.web_users wu
         JOIN users_and_activities.mailchimp_sub mc
         ON wu.email = mc.email_address
         WHERE wu.birthdate IS NOT NULL
         AND wu.birthdate >= '1915-01-01'
         AND wu.birthdate < '2006-01-01'
         GROUP BY age"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.baseline_web_gender
         SELECT wu.gender, COUNT(*) count
         FROM users_and_activities.web_users wu
         JOIN users_and_activities.mailchimp_sub mc
         ON wu.email = mc.email_address
         WHERE wu.gender IS NOT NULL
         GROUP BY wu.gender"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.baseline_web_income
         SELECT wu.income_level, COUNT(*) count
         FROM users_and_activities.web_users wu
         JOIN users_and_activities.mailchimp_sub mc
         ON wu.email = mc.email_address
         WHERE wu.income_level IS NOT NULL
         GROUP BY wu.income_level"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.baseline_web_race
         SELECT wu.race, COUNT(*) count
         FROM users_and_activities.web_users wu
         JOIN users_and_activities.mailchimp_sub mc
         ON wu.email = mc.email_address
         WHERE wu.race IS NOT NULL
         GROUP BY wu.race"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.cause_gender
         SELECT td.name, wu.gender, COUNT(*) count
         FROM users_and_activities.web_users wu
         JOIN dosomething.dosomething_signup ds
         ON wu.uid = ds.uid
         JOIN dosomething.node n
         ON ds.nid = n.nid
         JOIN dosomething.field_data_field_primary_cause pc
         ON ds.nid = pc.entity_id
         JOIN dosomething.taxonomy_term_data td
         ON pc.field_primary_cause_tid = td.tid
         WHERE wu.gender IS NOT NULL
         GROUP BY td.name, wu.gender"""
cur.execute(runquery)
db.commit()

runquery = """CREATE TABLE users_and_activities.cause_income
         SELECT td.name, wu.income_level, COUNT(*) count
         FROM users_and_activities.web_users wu
         JOIN dosomething.dosomething_signup ds
         ON wu.uid = ds.uid
         JOIN dosomething.node n
         ON ds.nid = n.nid
         JOIN dosomething.field_data_field_primary_cause pc
         ON ds.nid = pc.entity_id
         JOIN dosomething.taxonomy_term_data td
         ON pc.field_primary_cause_tid = td.tid
         WHERE wu.income_level IS NOT NULL
         GROUP BY td.name, wu.income_level"""
cur.execute(runquery)
db.commit()

# Close Cursor and Connection
cur.close()
db.close()
