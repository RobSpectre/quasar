from datetime import datetime as dt
from datetime import timedelta as t_delt
import multiprocessing as mp
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
from bs4 import BeautifulSoup as bs
from multiprocessing import Manager
import MySQLdb
import MySQLdb.converters
import time
import sys
import config
#can't easily oop because Python multiprocessing is difficult
#set time to track execution time
now = time.time()
#list for opt outs using Manager function. Required for multiprocessing
opt_outs = Manager().list([])

def getHours():
  """based on date or sys args creates list of tuples to feed to the api"""
  #list that will contain final tuples
  hours = list()
  #set start and end date as yesterday until midnight of current day unless cl input
  if len(sys.argv) == 1:
    day1 = dt.strptime(dt.strftime((dt.utcnow() - t_delt(days=1)),'%Y-%m-%d'),'%Y-%m-%d')
    day2 = dt.strptime(dt.strftime(dt.utcnow(),'%Y-%m-%d'),'%Y-%m-%d')
  else:
    day1 = dt.strptime(sys.argv[1], '%Y-%m-%d')
    day2 = dt.strptime(sys.argv[2], '%Y-%m-%d')
  print (day1, day2)
  #varibale equal to the start date (day1) that will be increased by hour
  #until it equals the end date (day2)
  start_date = day1
  while start_date != day2:
    #add 1 hour
    temp_date = start_date + t_delt(hours=1)
    #create a tuple with the start date and new end date
    tmp_tpl = (str(start_date), str(temp_date))
    hours.append(tmp_tpl)
    #set start_date to the new temp date
    start_date = temp_date
  return hours

def apiCreds():
  """mobilecommons auth and api url"""
  un = config.mc_user
  pw = config.mc_pw
  url = "https://secure.mcommons.com/api/profiles"
  return un, pw, url

def getProfile(idx, start, finish, cred):
  """gets updated profiles and checks for accounts that opted out"""
  print (idx, start, finish)
  acceptable_statuses = ['Undeliverable','Active Subscriber','No Subscriptions','Hard bounce','Texted a STOP word']
  #set start page
  page = 0
  #set count of retured profiles
  num = 100
  #if the number of returned profiles is 100, increase the page and keep calling the api
  while num == 100:
    page += 1
    print (idx, page)
    data = {'limit':'100', 'page':page, 'from':start, 'to':finish}
    req = requests.Session()
    retries = Retry(total=6, backoff_factor=600)
    req.mount('https://', HTTPAdapter(max_retries=retries))
    req = requests.get(cred[2], auth=(cred[0],cred[1]), params=data)
    #parse with beautiful soup
    soup = bs(req.text,'xml')
    #set num to count of returned profiles
    num = int(soup.profiles.get('num'))
    profiles = soup.find_all('profile')

    for i in profiles:
    #only get active to inactive to cut back on updates
      if i.status.text != 'Active Subscriber':
        #convert time, remove UTC
        try:
          converted_date = '"' + dt.strftime(dt.strptime(i.opted_out_at.text, '%Y-%m-%d %H:%M:%S %Z'), '%Y-%m-%d %H:%M:%S') + '"'
        except:
          converted_date = 'NULL'
        #check for broken statuses. Mobilecommons.
        if i.status.text in acceptable_statuses:
          status = i.status.text
        else:
          status = 'Undeliverable'

        opt_outs.append((i.phone_number.text, status, converted_date))
    print ()'current len opt_outs', len(opt_outs))

def dbConn():
  """mysql conn and cursor"""
  conv_dict = MySQLdb.converters.conversions.copy()
  conv_dict[246]=float
  conv_dict[8]=int
  db = MySQLdb.connect(host=config.host, #hostname
            user=config.user, #  username
            passwd=config.pw, #  password
            conv=conv_dict) # datatype conversions
  cur = db.cursor()
  return db, cur

def updateUsers(opt_out_list):
  """updates user in db"""
  #all numbers
  phone_str = ",".join([i[0] for i in opt_outs])
  #status
  status_str = " ".join(["WHEN phone_number = {0} THEN '{1}'".format(i[0], i[1]) for i in opt_outs])
  #opt out time
  opt_out_time_str = " ".join(["WHEN phone_number = {0} THEN {1}".format(i[0], i[2]) for i in opt_outs])
  q_set_mode = "SET @@session.sql_mode= ''"
  q_update = """
  update users_and_activities.mobile_users
  set status = CASE {0} END,
  opted_out_at = CASE {1} END
  where phone_number in ({2})""".format(status_str, opt_out_time_str, phone_str)

  cur.execute(q_set_mode)
  db.commit()
  cur.execute(q_update)
  db.commit()
#get hours list
hours = getHours()
#call credentials from config
creds = apiCreds()
#start multiprocessing pool
pool = mp.Pool(24)
#process hours
dates = [pool.apply(getProfile, args=(i, x[0],x[1],creds,)) for i,x in enumerate(hours)]
print ('total opt outs: ', len(opt_outs))
pool.close()
#open connection
connection = dbConn()
db = connection[0]
cur = connection[1]
#update users
updateUsers(opt_outs)
cur.close()
db.close()
#print time
print (time.time() - now)
