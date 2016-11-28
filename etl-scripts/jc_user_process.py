import MySQLdb
import MySQLdb.converters
import sys
import config

def conn():
  conv_dict = MySQLdb.converters.conversions.copy()
  conv_dict[246]=float
  conv_dict[8]=int
  db = MySQLdb.connect(host=config.host, #hostname
          user=config.user, #  username
          passwd=config.pw, #  password
          db="user_processing",
          conv=conv_dict) # datatype conversions

  cur = db.cursor(MySQLdb.cursors.DictCursor)
  return db, cur

def get_demo(obj, db, cur2):
  #deal with this obj['postal_code']
  if obj['zip'] != None:
    #print obj['zip']
    zip = obj['zip']
  else:
    print 'no zip'
    obj['race']=None
    return obj

  q_ll = "select `long`, `lat` from zips where zcta5ce10 = '%s'" % (zip)
  #print q_ll
  cur2.execute(q_ll)
  out = cur2.fetchall()
  #print out
  try:
    q_z_find = "select ` White` as w,  ` Black or African American` as b ,  ` American Indian and Alaska Native` as i,  ` Asian:` as a,`Hispanic Hispanic or Latino (of any race):` as h from  census_tract where st_contains(shape, ST_POINTFROMTEXT('point(%s %s)',2))" % (out[0]['long'], out[0]['lat'])
    cur2.execute(q_z_find)
    out_2 = cur2.fetchall()
    #print out_2
    max_demo = max([out_2[0][i] for i in out_2[0]])
    top_demo = [i for i in out_2[0] if out_2[0][i] == max_demo][0]
    obj['race'] = top_demo
    return obj

  except Exception as e:
    print 'no data in get_demo, error: ', e
    obj['race']=None
    return obj

def get_gender(obj, db, cur2):
  if obj['first'] != None:
    first_name_formatted = obj['first'].split(' ')[0].upper()
  else:
    obj['gender']=None
    return obj
  try:
    q_g_find = "select gender from gender where first_name = '%s'" % (first_name_formatted)
    cur2.execute(q_g_find)

    out_2 = cur2.fetchall()
    if len(out_2) > 0:
      obj['gender'] = out_2[0]['gender']
      return obj
    else:
      obj['gender']=None
      return obj

  except Exception as e:
    print 'no data, error in get_gender: ', e, obj
    obj['gender']=None
    return obj

def get_zip(obj, db, cur2):
  #deal with this obj['postal_code']
  try:
    if obj['lat'] is not None and obj['long'] is not None and obj['postal_code'] == '':
      lat = obj['lat']
      llong = obj['long']

    #if obj['postal_code'] != None:
    #  obj['zip']=obj['postal_code']
    # return obj
    else:
      obj['zip']=None
      return obj

    try:
      q_ll_find = "select zcta5ce10 as zip from zips where st_contains(shape, ST_POINTFROMTEXT('point(%s %s)',1))" % (llong, lat)

      cur2.execute(q_ll_find)
      out_2 = cur2.fetchall()


      if len(out_2) > 0:
        obj['zip'] = str(out_2[0]['zip'])
        return obj
      else:
        obj['zip']=None
        return obj

    except Exception as e:
      print 'no data, error in get_zip: ', e
      obj['zip']=None
      return obj

  except Exception as e:
    print 'zip', e
    #split on - for longer than 5 digits
    if obj['postal_code'] == None:
      obj['zip'] = obj['postal_code']
      return obj

    if obj['postal_code'] == '':
      obj['zip'] = None
      return obj

    obj['postal_code'] = obj['postal_code'].split('-')[0]

    obj['zip'] = obj['postal_code']
    return obj

def get_income_level(obj, db, cur2):
    """Updates and returns the income_level key of obj"""
    #find current zip if any
    if obj['zip'] is not None:
        z = obj['zip']
    else:
        print "no zip"
        obj['income_level'] = None
        return obj

    #get median income for z
    try:
        q = "SELECT total FROM median_income_by_zip WHERE zip = {}".format(z)
        cur2.execute(q)
        res = cur2.fetchone()

        if res is None or res.get('total') is None:
            print "no median income data for zip", z
            obj['income_level'] = None
            return obj
        else:
            print "success for zip", z
            obj['income_level'] = _get_income_enum_index(int(res['total']))
            return obj
    except Exception as e:
        print "No data in get_income_level, error:", e
        obj['income_level'] = None
        return obj

def _get_income_enum_index(n):
    """Returns the MySQL index of the enum string corresponding to n:

        ENUM:                       INDEX:
        NULL                        NULL
        "Below poverty line"        1
        "Low income"                2
        "Low middle income"         3
        "Upper middle income"       4
        "High income"               5
    """
    if n < 24250:
        retval = 1
    elif n < 48500:
        retval = 2
    elif n < 90000:
        retval = 3
    elif n < 200000:
        retval = 4
    else:
        retval = 5
    return retval
