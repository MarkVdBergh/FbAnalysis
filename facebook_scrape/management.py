from pprint import pprint

from datetime import datetime
from bson import ObjectId
from profilehooks import timecall
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

from facebook_scrape import facebook_page_lists
from facebook_scrape import queries
from facebook_scrape.helpers import logit
from facebook_scrape.settings import MONGO_HOST, MONGO_PORT, DB, TEST_DATABASE

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB]


# --------------------------------------------------------------------------------------------------------------------
def setup_databases():
    # fix: result
    result = {}
    workflow = [
        setup_database_pages(),
        setup_database_contents(),
        setup_database_users(),
    ]
    for work in workflow:
        result.update(work)
    return result


def setup_database_pages():
    collection = db.pages
    # indexes
    collection.create_index('id', unique=True)
    collection.create_index('name')
    collection.create_index([('type', 1), ('sub_type', 1)])
    result = {'page': 'ok'}
    return result


def setup_database_contents():
    collection = db.contents
    # indexes
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
def setup_database_pagestats():
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
    collection.create_index('n_reactions')
    collection.create_index('n_comments')
    result = {'users': 'ok'}
    return result


# --------------------------------------------------------------------------------------------------------------------
def drop_test_database():
    client.drop_database(TEST_DATABASE)


# --------------------------------------------------------------------------------------------------------------------


@timecall()
def insert_facebook_page_list(pages=facebook_page_lists):
    """
        Takes a dict with form : {type:{subtype:[page_id]}}, retrieves facebook details and stores each page in the <page> collection
        :param pages: dict:  Has the form : {type:{subtype:[page_id]}}
        :return: ObjectId: _id of the inerted page
        """
    inserted_ids = []
    for t in pages:
        for sub_t in pages[t]:
            for page_id in pages[t][sub_t]:
                page = queries.fb_get_page(page_id)  # page={'id':.., 'name':...}
                page['type'] = t
                page['sub_type'] = sub_t
                page['updated'] = datetime.utcnow()
                _id = queries.insert_page(page)
                inserted_ids.append(_id)
    return inserted_ids


# --------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    print '_' * 120
    print 'Databases: ', client.database_names()
    print 'Collections: ', db.collection_names()
    print setup_databases()
    # drop_test_database()
    #
