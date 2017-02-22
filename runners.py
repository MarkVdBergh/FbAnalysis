############################################################################################################################""
# Facebook Scraping
from datetime import timedelta, datetime

from pymongo import MongoClient

from facebook_scrape.helpers import logit
from facebook_scrape.settings import MONGO_HOST, MONGO_PORT, FB_APP_ID, FB_APP_SECRET
from facebook_scrape.workers import PageWorker

# start = datetime.now() - timedelta(days=2, hours=18)

# fix : -------------------------------------------------------------------------
fb_access_token = FB_APP_ID + "|" + FB_APP_SECRET
client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client['test']
pages = db.pages
# docs = {'type': 'news', 'flag': {'$ne': 1}}
docs = {'type': 'news'}
q = pages.find(docs)
# fix : -------------------------------------------------------------------------
# since = datetime(2014, 06, 19)
# until = datetime(2014, 06, 20)
since=until=None
for p in q:
    logit('Start', p['id'], p)
    pw = PageWorker(page_id=p['id'])
    pages.update_one({'id': p['id']}, {'$set': {'start': datetime.utcnow(), 'flag': 0}})
    pw.process_posts(since=since, until=until, old_db=True)
    pages.update_one({'id': p['id']}, {'$set': {'end': datetime.utcnow(), 'flag': 1}})
############################################################################################################################""



"""
ERROR:
2017-02-21 04:58:45.444620: Start: 37823307325: {u'_id': ObjectId('58a8e84b4520005a3c11ee5c'),
 u'flag': 0,
 u'id': u'37823307325',
 u'name': u'Nieuwsblad.be',
 u'start': datetime.datetime(2017, 2, 21, 3, 54, 58, 901000),
 u'sub_type': u'Popular',
 u'type': u'news',
 u'updated': datetime.datetime(2017, 2, 19, 0, 35, 23, 454000)}
Traceback (most recent call last):
  File "runners.py", line 25, in <module>
    pw.process_posts(since=None, until=None, old_db=True)
  File "/home/marc/DATA/Projects/FbAnalytics/facebook_scrape/workers.py", line 67, in process_posts
    _user_id_ = _user.get_id_()
  File "/home/marc/DATA/Projects/FbAnalytics/facebook_scrape/stat_objects.py", line 38, in get_id_
    result = self.collection.insert_one(document={'id': self.pk, 'flag': 'INIT', 'updated': datetime.utcnow()})
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 657, in insert_one
    bypass_doc_val=bypass_document_validation),
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 562, in _insert
    check_keys, manipulate, write_concern, op_id, bypass_doc_val)
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 544, in _insert_one
    _check_write_command_response([(0, result)])
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/helpers.py", line 314, in _check_write_command_response
    raise DuplicateKeyError(error.get("errmsg"), 11000, error)
pymongo.errors.DuplicateKeyError: E11000 duplicate key error collection: test.users index: id_1 dup key: { : "1402664639775919" }
(ml) marc@Z30:~/DATA/Projects/FbAnalytics$ python runners.py
------------------------------------------------------------------------------------------------------------------------
2017-02-21 04:59:28.389374: Start: 37823307325: {u'_id': ObjectId('58a8e84b4520005a3c11ee5c'),
 u'flag': 0,
 u'id': u'37823307325',
 u'name': u'Nieuwsblad.be',
 u'start': datetime.datetime(2017, 2, 21, 3, 58, 45, 449000),
 u'sub_type': u'Popular',
 u'type': u'news',
 u'updated': datetime.datetime(2017, 2, 19, 0, 35, 23, 454000)}
Traceback (most recent call last):
  File "runners.py", line 25, in <module>
    pw.process_posts(since=None, until=None, old_db=True)
  File "/home/marc/DATA/Projects/FbAnalytics/facebook_scrape/workers.py", line 67, in process_posts
    _user_id_ = _user.get_id_()
  File "/home/marc/DATA/Projects/FbAnalytics/facebook_scrape/stat_objects.py", line 38, in get_id_
    result = self.collection.insert_one(document={'id': self.pk, 'flag': 'INIT', 'updated': datetime.utcnow()})
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 657, in insert_one
    bypass_doc_val=bypass_document_validation),
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 562, in _insert
    check_keys, manipulate, write_concern, op_id, bypass_doc_val)
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 544, in _insert_one
    _check_write_command_response([(0, result)])
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/helpers.py", line 314, in _check_write_command_response
    raise DuplicateKeyError(error.get("errmsg"), 11000, error)
pymongo.errors.DuplicateKeyError: E11000 duplicate key error collection: test.users index: id_1 dup key: { : "1372008926177769" }
(ml) marc@Z30:~/DATA/Projects/FbAnalytics$ python runners.py

"""
