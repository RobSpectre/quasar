import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from warnings import filterwarnings
import json
from datetime import datetime as d
from datetime import timedelta as t
from bs4 import BeautifulSoup as bs
import MySQLdb
import MySQLdb.converters
from math import log
import sys
import os
import time
import config

#flags - hour backfill campaign force clean
# - hours - how many hours to go back
# - backfill - tells the script to look for a campaign to backfill.
#Will only backfill this campaign and campaigns in it's cluster
# - campaign - mobile campaign id to backfill
# - force - ignores if a campaign is marked as live or not and tries to get subscriptions anyway
# - clean - runs dailyClean function
#Null time is because returns no entries on last page
#set start time
start_time = time.time()

class MainSetup:
  """this sets global variables for the script and gets all campaign clusters that need to be processed"""

  def __init__(self):

    if d.now().hour == 2:
      self.clean = True
      self.backtime = 36

    if 'clean' in sys.argv:
      self.clean = True
    else:
      self.clean = False

    self.backtime = int(sys.argv[1])

    if 'force' in sys.argv:
      self.force = True
    else:
      self.force = False

    self.backfill_campaign = None
    if 'backfill' in sys.argv:
      for cl_arg in sys.argv[2:]:
        try:
          int(cl_arg)
          self.backfill_campaign = cl_arg
          break
        except:
          continue

    self.campaigns_dict = dict()
    self.all_clusters = list()
    self.connection = self.dbConnect()
    self.db = self.connection[0]
    self.cur = self.connection[1]
    #always refresh master
    self.refreshMaster()

  def dbConnect(self):
    """open db connection"""
    #set ignore mysqldb warnings for cleaner logs
    filterwarnings('ignore', category = MySQLdb.Warning)
    ###
    #open mysql connection
    ###
    #set datatype conversions
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
    return db, cur

  def refreshMaster(self):
    """refresh table of all drupal campaigns - users_and_activities.campaign_master"""
    q_refresh_master = """insert ignore into users_and_activities.campaign_master
                    select n.nid, n.title, u.alias, ifnull(s.field_campaign_status_value, 'active') as status
                    from dosomething.node n
                    left join
                    dosomething.field_data_field_campaign_status s
                    on n.nid=s.entity_id
                    left join dosomething.url_alias u
                    on n.nid=substring(source, 6)
                    where type = 'campaign'
                    on duplicate key update title=n.title, alias=u.alias, status=ifnull(s.field_campaign_status_value, 'active')"""

    self.cur.execute(q_refresh_master)
    self.db.commit()

  def dailyClean(self):
    q_set_mode = "SET @@session.sql_mode= ''"
    q_update_activated = "update users_and_activities.mobile_subscriptions set activated_at = opted_out_at_campaign where  activated_at = '0000-00-00 00:00:00';"
    q_all_live = "update user_processing.mobile_campaign_component set live = 'True'"
    self.cur.execute(q_set_mode)
    self.cur.execute(q_update_activated)
    self.cur.execute(q_all_live)
    self.db.commit()

  def fastCampaigns(self):
    """this gets just campaigns that are directly tied to an nid """
    q_campaigns = """select campaign_id from user_processing.sms_games
                  union
                  select campaign_id from users_and_activities.mobile_campaign_ids"""
    self.cur.execute(q_campaigns)
    campaigns = {str(c['campaign_id']):None for c in self.cur.fetchall()}
    return campaigns

  def getCampaigns(self):
    """gets campaigns and opt in paths from mobilecommons"""
    un = config.mc_user
    pw = config.mc_pw
    self.campaigns_list = []
    req = requests.Session()
    retries = Retry(total=6, backoff_factor=600)
    req.mount('https://', HTTPAdapter(max_retries=retries))
    url_1 = 'https://secure.mcommons.com/api/campaigns'
    url_1_payload = {"include_opt_in_paths":"1"}
    req = requests.get(url_1, auth=(un, pw), data = url_1_payload)
    soup1 = bs(req.text, 'xml')
    campaigns = soup1.find_all('campaign')
    #dict of manually entered campaigns from mobile_campaign_ids and sms_games
    fast_campaigns = self.fastCampaigns()
    for c in campaigns:
      campaign_id = str(c.attrs.get('id'))
      #only process campaign if present in fast_campaigns
      try:
        fast_campaigns[campaign_id]
      except:
        continue
      self.campaigns_dict[campaign_id] = list()
      opt = c.find_all('opt_in_path')
      for o in opt:
        temp_dict = {}
        temp_dict['campaign_status'] = c.attrs.get('active')
        temp_dict['campaign_id'] = campaign_id
        temp_dict['campaign_name'] = c.find('name').string
        temp_dict['opt_in_id_status'] = o.attrs.get('active')
        temp_dict['opt_in_id'] = o.attrs.get('id')
        temp_dict['opt_in_id_name'] = o.find('name').string
        self.campaigns_dict[campaign_id].append(temp_dict)
      #add blank opt_in_path. this is needed to catch subscriptions that don't hit an opt in path
      temp_dict = {}
      temp_dict['campaign_status'] = c.attrs.get('active')
      temp_dict['campaign_id'] = campaign_id
      temp_dict['campaign_name'] = c.find('name').string
      temp_dict['opt_in_id_status'] = 'true'
      temp_dict['opt_in_id'] = 0
      temp_dict['opt_in_id_name'] = 'catch_all'
      self.campaigns_dict[campaign_id].append(temp_dict)

  def getCluster(self):
    """gets clusters and nid if associated"""
    q_cluster = """select nid, group_concat(campaign_id) as c
        from users_and_activities.mobile_campaign_ids
        group by nid, campaign_run"""
    self.cur.execute(q_cluster)
    self.db.commit()
    out = self.cur.fetchall()
    for cluster in out:
      self.all_clusters.append({'cluster':cluster['c'].split(","), 'nid':int(cluster['nid']), 'all_paths':list()})
  #turn campaign_list to dict, get all paths to cluster and then delete that id from camppaiugns list. then
  #but remaining paths to cluster
  #TEST by counting campaigns and paths proccessed, and total inserts into special db
  #test alpahas usual way

  def processClusters(self):
    """gets all opt in path level data for all campaigns in cluster"""
    for cluster in self.all_clusters:
      for individual_id in cluster['cluster']:
        try:
          cluster['all_paths'].extend(self.campaigns_dict[individual_id])
          del self.campaigns_dict[individual_id]
        except Exception as e:
          print ('error', e)
    for remaining in self.campaigns_dict:
      temp_dict = {'cluster':[remaining], 'nid':None, 'all_paths':self.campaigns_dict[remaining]}
      self.all_clusters.append(temp_dict)

class ClusterSubscribers:
  """process each cluster"""

  def __init__(self, cluster_dict, force, backtime, db, cur):
    self.stop = d.utcnow() - t(hours=backtime)
    self.alpha_store = dict()
    self.all_nums = dict()
    self.cluster_list = cluster_dict['cluster']
    self.nid = cluster_dict['nid']
    self.all_paths = cluster_dict['all_paths']
    self.db = db
    self.cur = cur
    self.force = force
    self.backtime = backtime

  def isLive(self, campaign_id, opt_in_id):
    q_live = """select last_page, live from user_processing.mobile_campaign_component
        where campaign_id = %s and opt_in_id = %s""" % (campaign_id, opt_in_id)
    self.cur.execute(q_live)
    try:

      out_live = self.cur.fetchall()[0]
      current_page = out_live['last_page']
      live = bool(out_live['live'])
    except:
      print ('isLive fail')
      current_page = '1000'
      live = True
    return live, current_page

  def isWebAlpha(self, campaign_id, opt_in_id):
    q_webalpha = "select web_alpha from users_and_activities.mobile_campaign_ids where campaign_id = %s and opt_in_id = %s" % (campaign_id, opt_in_id)
    self.cur.execute(q_webalpha)
    try:
      out_webalpha = self.cur.fetchall()[0]['web_alpha']
      return int(out_webalpha)
    except:
      return 0

  def allNums(self):
    cluster_str = ",".join(self.cluster_list)
    q_allnums = "select distinct phone_number from users_and_activities.mobile_subscriptions where campaign_id in (%s)" % (cluster_str)
    self.cur.execute(q_allnums)
    out_allnums = self.cur.fetchall()
    self.all_nums = {i['phone_number']:None for i in out_allnums}

  def convertTime(self, times_list):
    proc_times_list = [d.strptime(i, '%Y-%m-%d %H:%M:%S %Z') for i in times_list if i is not None and i != '0000-00-00 00:00:00' and i != '']
    proc_times_list.sort()
    if len(proc_times_list) == 0:
      proc_times_list.append(d(1970, 1, 1, 0, 0))
    return proc_times_list

  def getSubs(self, page, p, campaign_id, opt_in_id, web_alpha):
    all_dates = []

    if opt_in_id == 0:
      opt_in_id = ''
    req = requests.Session()
    retries = Retry(total=6, backoff_factor=600)
    req.mount('https://', HTTPAdapter(max_retries=retries))
    url_2 = 'https://secure.mcommons.com/api/campaign_subscribers'
    url_2_payload = {"campaign_id":campaign_id, 'opt_in_path_id':opt_in_id, "page":page, "limit":p}
    req = requests.get(url_2, auth=(config.mc_user, config.mc_pw), data = url_2_payload)
    soup = bs(req.text, 'xml')
    subs = soup.find_all('sub')

    for s in subs:
      all_dates.append(s.activated_at.text)
      all_dates.append(s.opted_out_at.text)
      try:
        s.phone_number.text
        self.all_nums[s.phone_number.text]
        first_seen = 0
      except Exception as e:
        self.all_nums[s.phone_number.text] = None
        first_seen = 1
      if web_alpha == 1:
        self.alpha_store[s.phone_number.text] = None

      q_sub = """insert ignore into users_and_activities.mobile_subscriptions(phone_number, campaign_id, opt_in_id, activated_at, opted_out_at_campaign, web_alpha, first_seen_campaign)
            VALUES ('%s', '%s', '%s', '%s', '%s', %s, %s) """ % (s.phone_number.text, campaign_id, opt_in_id, s.activated_at.text, s.opted_out_at.text, web_alpha, first_seen)
      try:
        self.cur.execute(q_sub)
        self.db.commit()
      except Exception as e:
        print (e, q_sub)

    earliest_time = self.convertTime(all_dates)
    return earliest_time[-1]

  def getPage(self, page, p, campaign_id, opt_in_id):
    un = config.mc_user
    pw = config.mc_pw
    if opt_in_id == 0:
      opt_in_id = ''
    req = requests.Session()
    retries = Retry(total=6, backoff_factor=600)
    req.mount('https://', HTTPAdapter(max_retries=retries))
    url_2 = 'https://secure.mcommons.com/api/campaign_subscribers'
    url_2_payload = {"campaign_id":campaign_id, 'opt_in_path_id':opt_in_id, "page":page, "limit":p}
    req = requests.get(url_2, auth=(un, pw), data = url_2_payload)
    soup  = bs(req.text,'xml')
    num = soup.subscriptions['num']
    return num

  def binarySearch(self, start, campaign_id, opt_in_id):
    if opt_in_id == 0:
      opt_in_id = ''
    done = False
    p_ret = 100
    past_starter = int(start) + 1
    max = int(round(past_starter * (1+log(past_starter)*.1)))
    high = max
    low = 1
    param = max
    input = max
    count = 1

    while done == False:
      n = int(self.getPage(input,p_ret, campaign_id, opt_in_id))

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
        #might need to add one, because can get stuck
        input = param + 1

      if n < p_ret and n > 0 or n == 0 and count > 12 and param <= 2 and high <= 2 and low <= 1 or count >= 100 or param <= 10 and count >= 50:
        done = True

    return param

  def beenAlpha(self):
    alpha_store_list = [str(a) for a in self.alpha_store.keys()]
    if len(alpha_store_list) > 0:
      alpha_string = ",".join(alpha_store_list)
      cluster_str = ",".join(self.cluster_list)

      q_beenalpha = """update users_and_activities.mobile_subscriptions
                    set been_alpha = 1
                    where campaign_id in (%s) and phone_number in (%s)""" % (cluster_str, alpha_string)
      self.cur.execute(q_beenalpha)
      self.db.commit()

  def runPath(self):
    """runs it all for all campaigns in cluster by opt in path"""
    for path in self.all_paths:
      tracker_date = d.utcnow()
      print (path['campaign_id'], path['opt_in_id'])
      temp_isLive = self.isLive(path['campaign_id'], path['opt_in_id'])
      live = temp_isLive[0]
      current_page = temp_isLive[1]
      active = path['campaign_status']

      if live == True or self.force == True:
        web_alpha = self.isWebAlpha(path['campaign_id'], path['opt_in_id'])
        last_page = self.binarySearch(current_page, path['campaign_id'], path['opt_in_id'])
        page_param = last_page + 1
        p_ret = 100
        last_date = self.getSubs(page_param, p_ret, path['campaign_id'], path['opt_in_id'], web_alpha)
        #double check last date by going one page down
        if last_date == d(1970, 1, 1, 0, 0):
          print ('double checking...')
          dbl_chk_page = page_param - 1
          last_date = self.getSubs(dbl_chk_page, p_ret, path['campaign_id'], path['opt_in_id'], web_alpha)
          print (last_date)

        if last_date < (d.utcnow() - t(weeks=4)) and self.force != True:
          print ('not live anymore', path['campaign_id'], path['opt_in_id'])
          q_notlive = """insert into user_processing.mobile_campaign_component (campaign_id, opt_in_id, last_date, last_page, active, live)
           VALUES ({0},{1},'{2}',{3},'{4}','False') ON DUPLICATE KEY UPDATE last_date='{2}', last_page={3}, active='{4}', live='False'""".format(path['campaign_id'], path['opt_in_id'], d.strftime(last_date, '%Y-%m-%d %H:%M:%S'), last_page, path['campaign_status'])
          self.cur.execute(q_notlive)
          self.db.commit()
        else:
          ensure_count = 2
          while tracker_date > self.stop and int(page_param) > 0:
            ensure_count -= 1
            #print path['campaign_id'], path['opt_in_id'], page_param, tracker_date
            dt = self.getSubs(page_param, p_ret, path['campaign_id'], path['opt_in_id'], web_alpha)
            tracker_date = dt
            page_param = page_param - 1

#hour backfill campaign force daily_clean
main = MainSetup()
if main.clean == True:
  main.dailyClean()
  print ('cleaning...')
print ('backtime is ', main.backtime)
print ('force is ', main.force)
print ('backfill is ', main.backfill_campaign)
main.getCampaigns()
main.getCluster()
main.processClusters()

for c in main.all_clusters:
  if main.backfill_campaign != None:
    if main.backfill_campaign not in c['cluster']:
      continue
  print (c['cluster'])
  clust1 = ClusterSubscribers(c, main.force, main.backtime, main.db, main.cur)
  clust1.allNums()
  clust1.runPath()
  #print len(clust1.alpha_store.keys())
  clust1.beenAlpha()

end_time = time.time()
duration = end_time - start_time
print ('duration: ', duration)

main.cur.close()
main.db.close()
