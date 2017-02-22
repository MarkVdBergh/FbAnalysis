from pprint import pprint
from time import mktime

import facebook
from datetime import datetime, timedelta

import requests
import sys
from pymongo import InsertOne
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError

from facebook_scrape.helpers import dt_to_ts, logit
from facebook_scrape.settings import FB_APP_SECRET, FB_APP_ID, MONGO_HOST, MONGO_PORT, DB

fb_access_token = FB_APP_ID + "|" + FB_APP_SECRET
client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, )


def fb_get_page(page_id, access_token=fb_access_token, time_out=120):
    """
        Application-tokens permits only 'id' and 'name' field.
        Users-tokens permit other fields, like 'about'.

        @Returns {u'name': u'VRT deredactie.be', u'id': u'270994524621'}
    """
    fields = 'id, name'
    graph = facebook.GraphAPI(access_token=access_token, timeout=time_out, version='2.8')
    page = graph.get_object(id=page_id, connection_name='posts', fields=fields)
    return page


def fb_get_posts(page_id, since=None, until=None):
    """

    """
    graph = facebook.GraphAPI(access_token=fb_access_token, timeout=60, version='2.8')
    # For fields see: https://developers.facebook.com/docs/graph-api/reference/v2.8/post/
    field_list = ['id', 'created_time', 'from', 'to',
                  'type', 'status_type',
                  'message', 'message_tags',
                  'link', 'name', 'description', 'picture',
                  'story',
                  'shares']
    fields = ','.join(field_list)
    chunk = graph.get_connections(page_id, connection_name='posts', fields=fields, date_format='U', since=since, until=until)
    # Add data to each post
    posts = []
    while True:  # get all chuncks of 25 posts for a page
        posts += [post for post in chunk['data']]
        # Attempt to make a request to the next page of data, if it exists.
        # When there are no more pages (['paging']['next']), break from the loop and end the script.
        try:
            chunk = requests.get(chunk['paging']['next']).json()
            logit(fb_get_posts.__name__, 'info', '{}: {} posts downloaded for {}'.format(datetime.now(), len(posts), page_id))
        except KeyError:
            break
    # posts.reverse()  # posts are retrieved in reverse order (oldest last)
    return posts


def fb_get_posts_from_old_db(page_id, since=None, until=None, sort=False, post_filter=None):
    db = client['politics']
    collection = db.facebook
    fltr = {'profile.id': page_id}
    proj = {'_id': 0, 'id': 1, 'created_time': 1, 'from': 1, 'to': 1,
            'type': 1, 'status_type': 1,
            'message': 1, 'message_tags': 1,
            'link': 1, 'name': 1, 'description': 1, 'picture': 1,
            'story': 1,
            'shares': 1}
    if since:
        since = dt_to_ts(since)
        fltr.update({'created_time': {'$gte': since}})
    if until:
        until = dt_to_ts(until)
        fltr.update({'created_time': {'$lte': until}})
    if post_filter: fltr.update(post_filter)
    posts = collection.find(filter=fltr, projection=proj, no_cursor_timeout=False).batch_size(100)
    if sort: posts = posts.sort([('created_time', 1)])
    return posts


def fb_get_reactions(post_id):
    graph = facebook.GraphAPI(access_token=fb_access_token, timeout=60, version='2.8')
    field_list = ['type', 'id', 'name', 'pic']
    fields = ','.join(field_list)
    chunk = graph.get_connections(post_id, connection_name='reactions', fields=fields, date_format='U')
    comments = []
    while True:
        comments += [comment for comment in chunk['data']]  # [{u'type': u'LOVE', u'id': u'1366741256698203', u'name': u'Daisy Van Lens', u'pic':'http://...'}, ...]
        # Attempt to make a request to the next page of data, if it exists.
        # When there are no more reactions (['paging']['next']), break from the loop and end the script.
        try:
            chunk = requests.get(chunk['paging']['next']).json()
            logit(fb_get_reactions.__name__, 'info', '{}: {} reactions downloaded for {}'.format(datetime.now(), len(comments), post_id))
        except KeyError:
            break
    return comments


def fb_get_reactions_from_old_db(post_id):
    db = client['politics']
    collection = db.facebook
    fltr = {'id': post_id}
    proj = {'id': 1, 'reactions.type': 1, 'reactions.id': 1, 'reactions.name': 1, 'reactions.pic': 1, '_id': 0}
    comments = collection.find(filter=fltr, projection=proj)
    if comments.count() > 0: comments = comments[0]['reactions']  # strip [{'reactions:{...}]
    else: comments = []
    return comments  # list


def fb_set_flag(post_id, flag):
    db = client['politics']
    collection = db.facebook
    fltr = {'id': post_id}
    updt = {'$set': {'flag': flag}}
    collection.update_one(filter=fltr, update=updt)


def update_page(page):
    db = client[DB]
    collection = db.pages
    # page.update({'$set': {'updated': datetime.utcnow()}}) don't update here
    collection.upsert_one(page)


def get_page_ids_(page_id):
    """
    Gets the page <_id>. If it doesn't exist, copy the page from <pages> to <users>
    :param page_id:
    :return: dict: {'p_id_': ObjectId, 'u_id_': ObjectId}: the <pages> and <users> _id from the page
    """
    db = client[DB]
    Users = db.users  # a page is also a user !
    Pages = db.pages
    page = Pages.find_one(filter={'id': page_id})  # must exist
    p_id_ = page['_id']
    user = Users.find_one(filter={'id': page_id}, projection={'_id': 1})  # {'_id': ObjectId(...)}
    if user:
        u_id_ = user['_id']
    else:
        result = Users.insert_one(page)
        u_id_ = result.inserted_id
        logit(get_page_ids_.__name__, 'info', 'New page created in <users>: {}'.format(page))
    _ids = {'p_page_id_': p_id_, 'u_page_id_': u_id_}
    return _ids


def upsert_user(user_upd):
    db = client[DB]
    collection = db.users
    fltr = {'id': user_upd['id']}
    u = collection.update_one(filter=fltr, update=user_upd, upsert=True)
    return u


def insert_page(page):
    db = client[DB]
    collection = db.pages
    try:
        result = collection.insert_one(page)
    except DuplicateKeyError as e:
        logit(insert_page.__name__, 'error', e)
        result = {'page': e}
    return result


######################################################################################################
# todo: check naming: bulk_insert... and bulk_upsert...
def bulk_insert_content(content_update_list):
    logit(bulk_insert_content.__name__, 'warning', '<description> and <message_tags> fields missing in old db')
    db = client[DB]
    collection = db.contents
    operations = (InsertOne(content) for content in content_update_list)  # fix: does generator works? otherwise [] iso ()
    try:
        result = collection.bulk_write(operations)
        logit(bulk_insert_content.__name__, 'info', 'Inserted {} documents'.format(len(content_update_list)))
    except BulkWriteError as e:
        logit(bulk_insert_content.__name__, 'error', e.details)
        result = e.details
    else:
        print content_update_list
        sys.exit(0)

    return result


def bulk_upsert_poststat(poststat_update_list):
    db = client[DB]
    collection = db.poststats
    operations = [UpdateOne(filter={'id': stat['$set']['id']}, update=stat, upsert=True) for stat in poststat_update_list]
    try:
        result = collection.bulk_write(operations)
        logit(bulk_upsert_poststat.__name__, 'info', 'Upserted {} documents'.format(len(poststat_update_list)))
    except BulkWriteError as e:
        logit(bulk_upsert_poststat.__name__, 'error', e.details)
        result = e.details
    return result


def bulk_upsert_users(user_update_list):
    db = client[DB]
    collection = db.users
    operations = [UpdateOne(filter={'id': user['$set']['id']}, update=user, upsert=True) for user in user_update_list]
    try:
        result = collection.bulk_write(operations)
        logit(bulk_upsert_users.__name__, 'info', 'Upserted {} documents'.format(len(user_update_list)))
    except BulkWriteError as e:
        logit(bulk_upsert_users.__name__, 'error', e.details)
        result = e.details
    return result


def management_log_errors(doc):
    db = client[DB]
    collection = db.errors
    collection.insert_one(doc)


######################################################################################################

if __name__ == '__main__':
    pass
    # since = datetime.now() - timedelta(days=200, hours=18)
