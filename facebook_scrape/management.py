from pprint import pprint

from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

from facebook_scrape.settings import MONGO_HOST, MONGO_PORT, DB, TEST_DATABASE

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB]


# --------------------------------------------------------------------------------------------------------------------
def setup_databases():
    result = {}
    workflow = [
        setup_database_pages(),
        setup_database_contents(),
        setup_database_users(),
    ]
    for work in workflow:
        result.update(work)
    return result


def setup_database_pages(inset_test_doc=True):
    collection = db.pages
    # indexes
    collection.create_index('id', unique=True)
    collection.create_index('name')
    collection.create_index([('type', 1), ('sub_type', 1)])
    result = {'page': 'ok'}
    if inset_test_doc:
        # schema
        page = {'_id': ObjectId('000000000000000000000000'),
                'id': 0,
                'name': 'name_0',
                'type': 'type_0',
                'sub_type': 'sub_type_0',
                'updated': datetime.utcnow()
                }
        # insert
        try:
            collection.insert_one(page)
        except DuplicateKeyError as e:
            print '{}\nERROR: {}'.format('-' * 120, e)
            result = {'page': e}
    return result


def setup_database_contents(inset_test_doc=True):
    collection = db.contents
    # indexes
    collection.create_index('id', unique=True)
    collection.create_index('name')
    collection.create_index('created')
    collection.create_index('updated')
    collection.create_index('nb_shares')
    collection.create_index([('id', 1), ('created', -1)])
    collection.create_index([('id', 1), ('updated', -1)])
    collection.create_index([('message', 'text'), ('name', 'text'), ('description', 'text')], weights={'name': 10,
                                                                                                       'message': 5,
                                                                                                       'description': 5})
    result = {'contents': 'ok'}
    if inset_test_doc:
        # schema
        content = {'_id': ObjectId('000000000000000000000000'),
                   'id': 0,
                   'created': datetime.utcnow(),
                   'u_from_ref': ObjectId('000000000000000000000000'),
                   'u_to_ref': [ObjectId('000000000000000000000001'), ObjectId('000000000000000000000002')],
                   'type': 'type_0',
                   'status_type': 'status_type_0',
                   'message': 'message_0',
                   'message_tags': ['tag_0', 'tag_1'],
                   'link': 'link_0',
                   'name': 'name_0',
                   'description': 'description_0',
                   'picture': 'picture_0',
                   'story': 'story_0',
                   'shares': 'shares_0',
                   'updated': datetime.utcnow()}
        # insert
        try:
            collection.insert_one(content)
        except DuplicateKeyError as e:
            print '{}\nERROR: {}'.format('-' * 120, e)
            result = {'page': e}
    return result


# todo: make <setup_database_users>
def setup_database_users(inset_test_doc=True):
    collection = db.users
    # indexes
    # collection.create_index('id', unique=True)
    # collection.create_index('name')
    # collection.create_index('created')
    # collection.create_index('updated')
    # collection.create_index('nb_shares')
    # collection.create_index([('id', 1), ('created', -1)])
    # collection.create_index([('id', 1), ('updated', -1)])
    result = {'users': 'ok'}
    if inset_test_doc:
        # schema
        user = {'_id': ObjectId('000000000000000000000000'),
                'id': 0,
                'updated': datetime.utcnow()
                }
        # insert
        try:
            collection.insert_one(user)
        except DuplicateKeyError as e:
            print '{}\nERROR: {}'.format('-' * 120, e)
            result = {'page': e}
    return result


# --------------------------------------------------------------------------------------------------------------------
def drop_test_database():
    client.drop_database(TEST_DATABASE)


# --------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    print '_' * 120
    print 'Databases: ', client.database_names()
    print 'Collections: ', db.collection_names()
    print setup_databases()
    # drop_test_database()
#