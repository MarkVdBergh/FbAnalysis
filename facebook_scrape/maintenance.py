from pprint import pprint

from datetime import datetime
from time import sleep

from bson import ObjectId
from profilehooks import timecall
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from termcolor import cprint

from facebook_scrape.facebook_page_lists import facebook_page_lists
from facebook_scrape import queries
from facebook_scrape.helpers import logit
from facebook_scrape.queries import fb_get_page
from facebook_scrape.settings import MONGO_HOST, MONGO_PORT, DB, TEST_DATABASE

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB]


# --------------------------------------------------------------------------------------------------------------------
def setup_databases():
    result = {}
    workflow = [
        setup_database_pages(),
        setup_database_contents(),
        setup_database_poststats(),
        setup_database_users(),
    ]
    for work in workflow:
        result.update(work)
    return result


def setup_database_pages():
    collection = db.pages
    collection.create_index('id', unique=True)
    collection.create_index('name')
    collection.create_index([('type', 1), ('sub_type', 1)])
    result = {'page': 'ok'}
    return result


def setup_database_contents():
    collection = db.contents
    collection.create_index('id', unique=True)
    collection.create_index('name')
    collection.create_index('created')
    collection.create_index('updated')
    collection.create_index([('id', 1), ('created', -1)])
    collection.create_index([('id', 1), ('updated', -1)])
    collection.create_index('nb_shares')
    collection.create_index([('message', 'text'), ('name', 'text'), ('description', 'text')], weights={'name': 10,
                                                                                                       'message': 5,
                                                                                                       'description': 5})
    result = {'contents': 'ok'}
    return result


def setup_database_poststats():
    ''''''
    collection = db.pagestats
    # indexes
    collection.create_index('id', unique=True)
    collection.create_index('created')
    collection.create_index('updated')
    collection.create_index([('id', 1), ('created', -1)])
    collection.create_index([('id', 1), ('updated', -1)])
    result = {'pagestats': 'ok'}
    return result


def setup_database_comments():
    collection = db.pagestats
    # indexes
    collection.create_index('id', unique=True)
    collection.create_index([('id', 1), ('created', -1)])
    collection.create_index([('id', 1), ('updated', -1)])
    result = {'comments': 'ok'}
    return result


def setup_database_users():
    collection = db.users
    # indexes
    collection.create_index('id', unique=True)
    collection.create_index('name')
    collection.create_index('updated')
    collection.create_index('tot_reactions')
    collection.create_index('tot_comments')
    result = {'users': 'ok'}
    return result


# --------------------------------------------------------------------------------------------------------------------
def drop_test_database():
    cprint('WARNING: DATABASE {} WILL BE DELETED'.format(TEST_DATABASE), color='red', attrs=['bold', 'blink'])
    # sleep(60)
    client.drop_database(TEST_DATABASE)

def load_pagelist():
    collection =db.pages
    for type in facebook_page_lists:
        for sub_type in facebook_page_lists[type]:
            for page_id in facebook_page_lists[type][sub_type]:
                page_doc={}
                page_doc['type']=type
                page_doc['sub_type']=sub_type
                page_doc['id']=page_id
                page_doc['name']=fb_get_page(page_id)['name'] #{name:..., id:...}
                collection.update_one({'id':page_id}, {'$set':page_doc}, upsert=True)


if __name__ == '__main__':
    print '_' * 120
    print 'Databases: ', client.database_names()
    print 'Collections: ', db.collection_names()
    print setup_databases()
    load_pagelist()

