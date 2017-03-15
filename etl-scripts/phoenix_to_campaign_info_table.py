import MySQLdb
import config
import time
import sys


start_time = time.time()
"""Keep track of start time of script."""

db = MySQLdb.connect(host=config.host,  # hostname
                     user=config.user,  # username
                     passwd=config.pw)  # password

db.set_character_set('utf8')
cur = db.cursor()
cur.execute('SET NAMES utf8;')
cur.execute('SET CHARACTER SET utf8;')
cur.execute('SET character_set_connection=utf8;')
"""Set UTF-8 encoding on MySQL connection."""

# Query to update Quasar campaign_info table from Phoenix DB.
update_campaign_table_query = """
REPLACE INTO quasar.campaign_info (campaign_node_id, campaign_node_id_title,
                                   campaign_run_id, campaign_run_id_title,
                                   campaign_type, campaign_language,
                                   campaign_run_start_date,
                                   campaign_run_end_date,
                                   campaign_created_date,
                                   campaign_action_type, campaign_cause_type,
                                   campaign_cta, campaign_noun, campaign_verb)
SELECT
  c.field_campaigns_target_id as 'campaign_node_id',
  n2.title as 'campaign_node_id_title',
  c.entity_id as 'campaign_run_id',
  n1.title as 'campaign_run_id_title',
  fdfct.field_campaign_type_value as 'campaign_type',
  group_concat(distinct(c.language) separator ' ') as 'campaign_language',
  fdfrd.field_run_date_value as 'campaign_run_start_date',
  fdfrd.field_run_date_value2 as 'campaign_run_end_date',
  from_unixtime(n1.created) as 'campaign_created_date',
  ttd1.name as 'campaign_action_type',
  ttd2.name as 'campaign_cause_type',
  fdfcta.field_call_to_action_value as 'campaign_cta',
  fdfrn.field_reportback_noun_value as 'campaign_noun',
  fdfrv.field_reportback_verb_value as 'campaign_verb'
FROM
  dosomething.field_data_field_campaigns c
LEFT JOIN
  dosomething.node n1
    ON n1.nid = c.entity_id
LEFT JOIN
  dosomething.node n2
    ON n2.nid = c.field_campaigns_target_id
LEFT JOIN
  dosomething.field_data_field_campaign_type fdfct
    ON c.field_campaigns_target_id = fdfct.entity_id
LEFT JOIN
  dosomething.field_data_field_run_date fdfrd
    ON c.entity_id = fdfrd.entity_id and c.language = fdfrd.language
LEFT JOIN
  dosomething.field_data_field_call_to_action fdfcta
    ON c.field_campaigns_target_id = fdfcta.entity_id and c.language = fdfcta.language
LEFT JOIN
  dosomething.field_data_field_reportback_noun fdfrn
    ON c.field_campaigns_target_id = fdfrn.entity_id and c.language = fdfrn.language
LEFT JOIN
  dosomething.field_data_field_reportback_verb fdfrv
    ON c.field_campaigns_target_id = fdfrv.entity_id and c.language = fdfrv.language
LEFT JOIN
  dosomething.field_data_field_action_type fdfat
    ON fdfat.entity_id = c.field_campaigns_target_id
LEFT JOIN
  dosomething.taxonomy_term_data ttd1
    ON fdfat.field_action_type_tid = ttd1.tid
LEFT JOIN
  dosomething.field_data_field_cause fdfc
    ON fdfc.entity_id = c.field_campaigns_target_id
LEFT JOIN
  dosomething.taxonomy_term_data ttd2
    ON fdfc.field_cause_tid = ttd2.tid
WHERE c.bundle = 'campaign_run'
GROUP BY c.entity_id;"""

# Run and commit new data to campaign_info Quasar table.
cur.execute(update_campaign_table_query)
db.commit()

# Close Cursor and Connection
cur.close()
db.close()

end_time = time.time()  # Record when script stopped running.
duration = end_time - start_time  # Total duration in seconds.
print('duration: ', duration)
