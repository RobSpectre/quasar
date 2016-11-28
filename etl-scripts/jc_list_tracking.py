import MySQLdb
import MySQLdb.converters
import datetime
from collections import OrderedDict
import sys
import os
import config

conv_dict = MySQLdb.converters.conversions.copy()
conv_dict[246]=float
conv_dict[8]=int
db = MySQLdb.connect(host=config.host, #hostname
          user=config.user, #  username
          passwd=config.pw, #  password
          conv=conv_dict) # datatype conversions

cur = db.cursor(MySQLdb.cursors.DictCursor)


list_track_drop = "drop table if exists users_and_activities.list_tracking"

list_track_create = "create table users_and_activities.list_tracking (date date, mobile_created int, mail_created int, mobile_opt_out int, mail_opt_out int, net int, PRIMARY KEY(date))"

list_track_pop = """
insert into users_and_activities.list_tracking (date, mobile_created, mail_created, mobile_opt_out, mail_opt_out, net)
select c.date as date, c.created as mobile_created, COALESCE(m.created,0)+COALESCE(muc.created,0) as mail_created, o.opted_out as mobile_opt_out, muo.opted_out as mail_opt_out , (COALESCE(c.created,0)+COALESCE(m.created,0)+COALESCE(muc.created,0))-(o.opted_out+muo.opted_out) as net



from

  (select date_format(created_at, '%Y-%m-%d') as date, ifnull(count(*),0) as created from users_and_activities.mobile_users where date_format(created_at, '%Y-%m-%d') != '0000-00-00' and date_format(created_at, '%Y-%m-%d') >= date_sub(curdate(), interval 3 month) group by date_format(created_at, '%Y-%m-%d')) c


left join

(select date_format(confirm_time, '%Y-%m-%d') as date, ifnull(count(*), 0) as created from users_and_activities.mailchimp_sub where date_format(confirm_time, '%Y-%m-%d') != '0000-00-00' and date_format(confirm_time, '%Y-%m-%d') >= date_sub(curdate(), interval 3 month) group by date_format(confirm_time, '%Y-%m-%d')) m

on c.date=m.date

left join

(select date_format(confirm_time, '%Y-%m-%d') as date, ifnull(count(*), 0) as created from users_and_activities.mailchimp_unsub where date_format(confirm_time, '%Y-%m-%d') != '0000-00-00' and date_format(confirm_time, '%Y-%m-%d') >= date_sub(curdate(), interval 3 month) group by date_format(confirm_time, '%Y-%m-%d')) muc

on c.date=muc.date

left join

(select date_format(unsub_time, '%Y-%m-%d') as date, ifnull(count(*), 0) as opted_out from users_and_activities.mailchimp_unsub where date_format(unsub_time, '%Y-%m-%d') != '0000-00-00' and date_format(unsub_time, '%Y-%m-%d') >= date_sub(curdate(), interval 3 month) group by date_format(unsub_time, '%Y-%m-%d')) muo

on c.date=muo.date


left join

  (select date_format(opted_out_at, '%Y-%m-%d') as date, ifnull(count(*), 0) as opted_out from users_and_activities.mobile_users where date_format(opted_out_at, '%Y-%m-%d') != '0000-00-00' and date_format(opted_out_at, '%Y-%m-%d') >= date_sub(curdate(), interval 3 month) group by date_format(opted_out_at, '%Y-%m-%d')
  ) o on c.date=o.date
"""

cur.execute(list_track_drop)
cur.execute(list_track_create)
cur.execute(list_track_pop)

db.commit()

cur.close()
db.close()
