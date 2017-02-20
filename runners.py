############################################################################################################################""
# Facebook Scraping
from datetime import timedelta, datetime

from pymongo import MongoClient

from facebook_scrape.settings import MONGO_HOST, MONGO_PORT, FB_APP_ID, FB_APP_SECRET
from facebook_scrape.workers import PageWorker

start = datetime.now() - timedelta(days=2, hours=18)

# fix : -------------------------------------------------------------------------
fb_access_token = FB_APP_ID + "|" + FB_APP_SECRET
client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client['test']
fb = db.pages
docs = {'type': 'politics'}
q = fb.find(docs)
# fix : -------------------------------------------------------------------------
for p in q:
    print p
    pw = PageWorker(page_id=p['id'])
    pw.process_posts(since=None, until=None, old_db=True)
############################################################################################################################""
