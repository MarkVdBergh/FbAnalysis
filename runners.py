############################################################################################################################""
# Facebook Scraping
from datetime import timedelta, datetime

from pymongo import MongoClient

from facebook_scrape.helpers import logit
from facebook_scrape.settings import MONGO_HOST, MONGO_PORT, FB_APP_ID, FB_APP_SECRET, DB
from facebook_scrape.workers import PageWorker

# start = datetime.now() - timedelta(days=2, hours=18)
########################################################################################################################
fb_access_token = FB_APP_ID + "|" + FB_APP_SECRET
client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB]
pages = db.pages


# docs = {'type': 'news', 'flag': {'$ne': 1}}
# docs = {'id':'37823307325'}
# docs = {'type': 'politics'}
# docs={'type': 'news'}
docs = {}
q = pages.find(docs)
########################################################################################################################
# since = datetime(2014, 06, 19)
# until = datetime(2014, 06, 20)
since = until = None
for p in q:
    logit('Start', p['id'], p)
    page_id = p['id']
    if p.get('flag', '') == 'STARTED' or p.get('flag', '') == 'ENDED': continue  # skip pages
    pw = PageWorker(page_id, from_fb=False)
    pages.update_one({'id': p['id']}, {'$set': {'start': datetime.utcnow(), 'flag': 'STARTED'}})
    pw.process_posts(since=since, until=until)
    pages.update_one({'id': p['id']}, {'$set': {'end': datetime.utcnow(), 'flag': 'ENDED'}})
########################################################################################################################
