from pprint import pprint

import facebook
from datetime import datetime, timedelta

import requests
from pymongo import InsertOne
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from facebook_scrape.settings import FB_APP_SECRET, FB_APP_ID, MONGO_HOST, MONGO_PORT, DB

fb_access_token = FB_APP_ID + "|" + FB_APP_SECRET
client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB]


def fb_get_page(page_id, access_token=fb_access_token, time_out=120, fields='id, name'):
    """
        Application-tokens permits only 'id' and 'name' field.
        User-tokens permit other fields, like 'about'.

        @Returns {u'name': u'VRT deredactie.be', u'id': u'270994524621'}
    """
    graph = facebook.GraphAPI(access_token=access_token, timeout=time_out, version='2.8')
    page = graph.get_object(id=page_id, connection_name='posts', fields=fields)
    return page


def fb_get_posts(page_id, field_list=None, since=None, until=None):
    """

    """
    graph = facebook.GraphAPI(access_token=fb_access_token, timeout=60, version='2.8')
    # For fields see: https://developers.facebook.com/docs/graph-api/reference/v2.8/post/
    if not field_list: field_list = ['id', 'created_time', 'from', 'to',
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
            print '-' * 120 + '\n{}: {} posts downloaded for {}'.format(datetime.now(), len(posts), page_id)
        except KeyError:
            break
    posts.reverse()  # posts are retrieved in reverse order (oldest last)
    return posts


def update_page(page):
    collection = db.pages
    page.update({'$set': {'updated': datetime.utcnow()}})
    collection.upsert_one(page)


def insert_page(page):
    collection = db.pages
    page.update(updated=datetime.utcnow())
    result = collection.insert_one(page)
    _id = result.inserted_id
    return _id


def insert_bulk_content(post_update_list):
    collection = db.contents
    operations = []
    # operations += [UpdateOne(filter={'id': post['id']}, update=post, upsert=True) for post in post_update_list]
    operations += [InsertOne(post) for post in post_update_list]
    try:
        result = collection.bulk_write(operations)
    except BulkWriteError as e:
        print '{}\nERROR: {}'.format('-' * 120, e.details)
        result = e.details
    return result


if __name__ == '__main__':
    pass
