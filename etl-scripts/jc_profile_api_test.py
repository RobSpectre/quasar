import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
from datetime import datetime as d
from datetime import timedelta as t
from bs4 import BeautifulSoup as bs
from math import log
import jc_user_process
import time
import sys
import config

# Master list for all unprocessed users
master_unproc = []

MOBILE_KEYS = ["phone",
               "first",
               "last",
               "email",
               "status",
               "created",
               "opted_out_at",
               "type",
               "name",
               "street1",
               "street2",
               "city",
               "state",
               "country",
               "lat",
               "long",
               "gender",
               "zip",
               "race",
               "income_level"]

MOBILE_BASE = """
  INSERT IGNORE INTO users_and_activities.mobile_users
  (phone_number, first_name, last_name, email, status, created_at, opted_out_at,
   source_type, source_name, street1, street2, city, state, country, latitude,
   longitude, gender, zip, race, income_level)
  VALUES ({})
"""

WEB_KEYS = ["uid",
            "first",
            "last",
            "created",
            "email",
            "street1",
            "city",
            "state",
            "zip",
            "birthdate",
            "mobile",
            "gender",
            "race",
            "income_level"]

WEB_BASE = """
  INSERT IGNORE INTO users_and_activities.web_users
  (uid, first_name, last_name, created, email, street1, city, state, zip,
   birthdate, mobile, gender, race, income_level)
  VALUES ({})
"""

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]

#get a place to start for mobile users.
q = "select round(count(*)/25) as start from users_and_activities.mobile_users"
cur.execute(q)
out = cur.fetchall()
#what datetime to go back until
stop = d.utcnow() - t(hours=int(sys.argv[1]))
un = config.mc_user
pw = config.mc_pw

#all profiles
url_1 = "https://secure.mcommons.com/api/profiles"
url_1_payload = { 'page':'3652'}

def insert_to_db_mobile(obj, db, cur):
  values = []
  for k in MOBILE_KEYS:
    try:
      obj[k] = obj[k].encode('utf-8', 'ignore')
    except:
      pass
    if obj[k] is None or obj[k] == '':
      values.append('NULL')
    elif type(obj[k]) == int or type(obj[k]) == long:
      values.append(str(obj[k]))
    elif type(obj[k]) == d:
      values.append(str(obj[k]).join(['"', '"']))
    else:
      values.append(obj[k].join(['"', '"']))
  q_insert = MOBILE_BASE.format(', '.join(values))

  try:
    cur.execute(q_insert)
    db.commit()

  except Exception as e:
    print (e, obj, q_insert)
  return q_insert

def insert_to_db_web(obj, db, cur):

  values = []
  for k in WEB_KEYS:
    try:
      obj[k] = obj[k].encode('utf-8', 'ignore')
    except:
      pass
    if obj[k] is None or obj[k] == '':
      values.append('NULL')
    elif type(obj[k]) == int or type(obj[k]) == long:
      values.append(str(obj[k]))
    elif type(obj[k]) == d:
      values.append(str(obj[k]).join(['"', '"']))
    else:
      values.append(obj[k].join(['"', '"']))
  q_insert = WEB_BASE.format(', '.join(values))

  try:
    cur.execute(q_insert)
    db.commit()

  except Exception as e:
    print (e, obj, q_insert)
  return q_insert

def getPage(page, p):
  url_1_payload = {"page":str(page), "limit":str(p)}
  try:
    req = requests.Session()
    retries = Retry(total=6, backoff_factor=600)
    req.mount('https://', HTTPAdapter(max_retries=retries))
    req = requests.get(url_1, auth=(un, pw), data = url_1_payload)
  except:
    time.sleep(60)
    req = requests.Session()
    retries = Retry(total=6, backoff_factor=600)
    req.mount('https://', HTTPAdapter(max_retries=retries))
    req = requests.get(url_1, auth=(un, pw), data = url_1_payload)
  soup  = bs(req.text,'xml')
  num = soup.profiles['num']
  print (num)
  return num

def binarySearch(start):
  done = False
  p_ret = 25
  past_starter = int(start) + 1
  max = int(round(past_starter * (1+log(past_starter)*.1)))
  high = max
  low = 1
  param = max
  input = max
  count = 1

  while done == False:
    n = int(getPage(input,p_ret))

    if n == 0:
      count +=1
      high = param
      param = param - int(round((param  + 1 - low)/2))
      input = param


    if n == p_ret:
      count +=1
      low = param
      param = param + int(round((high - param)/2))

      if param >= max:
        if count <= 3:
          high = int(round(high * 2))
          param = high
        else:
          high = int(round(high * 1.05))
          param = high
      input = param + 1

    if n < p_ret and n > 0 or n == 0 and count > 12 and param <= 2 and high <= 2 and low <= 1 or count >= 100 or param <= 10 and count >= 50:
      done = True
  return param

def getProfile(page, p):
  url_1_payload = {"page":str(page), "limit":str(p)}

  try:
    req = requests.Session()
    retries = Retry(total=6, backoff_factor=600)
    req.mount('https://', HTTPAdapter(max_retries=retries))
    req = requests.get(url_1, auth=(un, pw), data = url_1_payload)
  except:
    time.sleep(60)
    req = requests.Session()
    retries = Retry(total=6, backoff_factor=600)
    req.mount('https://', HTTPAdapter(max_retries=retries))
    req = requests.get(url_1, auth=(un, pw), data = url_1_payload)

  soup  = bs(req.text,'xml')

  profiles = soup.find_all('profile')
  profile_data = {'id':['main_attrs',None],'phone_number':[None], 'first_name':[None],
                  'last_name':[None], 'email':[None], 'status':[None], 'created_at':[None],
                  'updated_at':[None], 'opted_out_at':[None], 'opted_out_at_source':[None],
                  'type':['attrs','source', None],'name':['attrs','source',None],
                  'street1':['child','address',None], 'street2':['child','address',None],
                  'city':['child','address',None], 'state':['child','address',None],
                  'country':['child','address',None], 'postal_code':['child','address',None],
                  'latitude':['child','location',None], 'longitude':['child','location',None]}
  profile_data_keys = profile_data.keys()
  #make more compact
  for i in profiles:
    for k in profile_data_keys:

      if len(profile_data[k]) == 1:
        try:
          profile_data[k][0] = i.find(k).text
        except:
          profile_data[k][0] = None

      if profile_data[k][0] == 'main_attrs':
        try:
          profile_data[k][1] = i.attrs.get(k)
        except:
          profile_data[k][1] = None

      if profile_data[k][0] == 'attrs':
        try:
          profile_data[k][2] = i.find(profile_data[k][1]).get(k)
        except:
          profile_data[k][2] == None

      if profile_data[k][0] == 'child':
        try:
          profile_data[k][2] = i.find(profile_data[k][1]).find(k).text
        except:
          profile_data[k][2] == None

    p_data = {'phone':profile_data['phone_number'][0], 'first':profile_data['first_name'][0].replace("'","").replace('"',''), 'last':profile_data['last_name'][0].replace("'","").replace('"',''), 'email':profile_data['email'][0], 'status':profile_data['status'][0], 'created':profile_data['created_at'][0], 'opted_out_at':profile_data['opted_out_at'][0], 'type':profile_data['type'][2], 'name':profile_data['name'][2], 'street1':profile_data['street1'][2], 'street2':profile_data['street2'][2], 'city':profile_data['city'][2], 'state':profile_data['state'][2], 'postal_code':profile_data['postal_code'][2], 'country':profile_data['country'][2], 'lat':profile_data['latitude'][2], 'long':profile_data['longitude'][2]}
    master_unproc.append(p_data)
  return profiles[0].created_at.text


q_last_start = "select count(*) as c from users_and_activities.mobile_users"
cur.execute(q_last_start)
last = int(cur.fetchall()[0]['c'])
page_param = binarySearch(last)
cur.close()
db.close()
tracker_date = d.utcnow()

while tracker_date > stop:

  dt = getProfile(page_param, 25)
  dt = d.strptime(dt,'%Y-%m-%d %H:%M:%S %Z')
  tracker_date = dt
  page_param = page_param - 1
  print (page_param)

print ('Mobile user processing starting')

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
gendered = [user_process.get_gender(i, db, cur) for i in master_unproc]
cur.close()
db.close()

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
zips = [user_process.get_zip(i, db, cur) for i in gendered]
cur.close()
db.close()

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
demo = [user_process.get_demo(i, db, cur) for i in zips]
cur.close()
db.close()

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
income = [user_process.get_income_level(i, db, cur) for i in demo]
cur.close()
db.close()

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
insert = [insert_to_db_mobile(i, db, cur) for i in income]
cur.close()
db.close()

print ('Mobile user processing finished')

master_unproc = []
web_users = """
select uid, replace(replace(fn.field_first_name_value, "'",""),'"','') as first, replace(replace(ln.field_last_name_value, "'",""),'"','') as last, u.mail as email, from_unixtime(created) as created, a.field_address_postal_code as postal_code, a.field_address_administrative_area as state, a.field_address_locality as city, a.field_address_thoroughfare as street1, b.field_birthdate_value as birthdate, m.field_mobile_value as mobile
from dosomething.users u
left join dosomething.field_data_field_first_name fn on u.uid=fn.entity_id
left join dosomething.field_data_field_last_name ln on u.uid=ln.entity_id
left join dosomething.field_data_field_birthdate b on u.uid=b.entity_id
left join dosomething.field_data_field_mobile m on u.uid=m.entity_id
left join dosomething.field_data_field_address a on u.uid=a.entity_id
where from_unixtime(created) >= date_sub(now(), interval %s hour)""" % (sys.argv[1])

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
cur.execute(web_users)

out = cur.fetchall()
cur.close()
db.close()
for i in out:
  master_unproc.append(i)


print ('Web user processing starting')

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
gendered = [user_process.get_gender(i, db, cur) for i in master_unproc]
cur.close()
db.close()

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
zips = [user_process.get_zip(i, db, cur) for i in gendered]
cur.close()
db.close()

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
demo = [user_process.get_demo(i, db, cur) for i in zips]
cur.close()
db.close()

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
income = [user_process.get_income_level(i, db, cur) for i in demo]
cur.close()
db.close()

conn_f = user_process.conn()
db = conn_f[0]
cur = conn_f[1]
insert = [insert_to_db_web(i, db, cur) for i in income]
cur.close()
db.close()

print ('Web user processing finished')
