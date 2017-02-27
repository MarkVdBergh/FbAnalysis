from collections import defaultdict
from datetime import datetime
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError
from termcolor import cprint

from facebook_scrape.helpers import logit
from facebook_scrape.settings import DB, MONGO_HOST, MONGO_PORT


class StatBase(object):
    collection = None
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 1000
    bulk_updates_buffer_size = 1000

    set_fields = []
    inc_fields = []
    push_fields = []
    add_to_set_fields = []

    def __init__(self, doc_id):
        # Load or create new user
        self.id_ = None  # Just to make sure it's empty. 'id_' because '_id' is a protected class memeber
        try:
            result = self.collection.insert_one(document={'id': doc_id, 'flag': 'INIT', 'updated': datetime.utcnow()})
            # Raises DuplicateKeyError is exists
            self.id_ = result.inserted_id
        except DuplicateKeyError:
            result = self.collection.find_one(filter={'id': doc_id}, projection={'_id': 1, 'id': 1, 'flag': 1})
            self.id_ = result['_id']

    def add_to_bulk_update(self):
        self.__class__.bulk_updates_buffer.append(self)
        nb_documents = len(self.__class__.bulk_updates_buffer)
        if nb_documents >= self.__class__.bulk_updates_buffer_size:  # buffer full
            logit(self.__class__.__name__, 'info', 'Buffer flush ({}) {} documents'.format(nb_documents, self.__class__.__name__))
            self.bulk_write()  # flush buffer

    @classmethod
    def bulk_write(cls):
        result = None
        if cls.bulk_updates_buffer:  # there are docuents to update
            operations = []
            for class_doc in cls.bulk_updates_buffer:
                update_doc = defaultdict(dict)  # dict, but allows nested,
                for k, v in class_doc.__dict__.iteritems():
                    if v:  # not None
                        if k in cls.set_fields:
                            update_doc['$set'][k] = v
                        if k in cls.inc_fields:
                            update_doc['$inc'][k] = v
                        if k in cls.add_to_set_fields:
                            update_doc['$addToSet'][k] = v
                        if k in cls.push_fields:
                            update_doc['push'][k] = v
                update_doc['$set']['flag'] = 0
                operations.append(UpdateOne(filter={'id': class_doc.id}, update=update_doc, upsert=False))
            try:
                result = cls.collection.bulk_write(operations, ordered=False)
            except:
                print '-' * 120
                cprint(operations, 'red')
                cprint(cls.bulk_updates_buffer, 'red')
                cprint(cls, 'blue')
                print '-' * 120
                raise

            cls.bulk_updates_buffer = []
        return result

    def __str__(self):
        return self.__dict__.__str__()

    def __repr__(self):  # for printing class instances in lists
        return self.__str__()


class Pages(StatBase):
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[DB]
    collection = db.pages
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 10
    bulk_updates_buffer_size = 10
    set_fields = ['name', 'type', 'sub_type']
    inc_fields = []
    push_fields = []
    add_to_set_fields = []

    def __init__(self, page_id):
        # field declarations
        self.id = page_id
        self._id = None
        self.name = None
        self.type = None
        self.sub_type = None
        super(Pages, self).__init__(page_id)


class Contents(StatBase):
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[DB]
    collection = db.contents
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 1
    bulk_updates_buffer_size = 1
    set_fields = ['created', 'poststat_ref', 'page_ref', 'author_ref', 'post_type', 'status_type',
                  'message', 'name', 'story', 'link', 'picture_link', 'description', 'updated']
    inc_fields = ['nb_reactions', 'nb_comments', 'nb_shares']
    push_fields = []
    add_to_set_fields = []

    def __init__(self, content_id):
        # field declarations
        self.id = content_id
        self._id = None
        self.created = None

        self.page_ref = None
        self.poststat_ref = None
        self.author_ref = None

        self.post_type = None
        self.status_type = None

        self.message = None
        self.name = None
        self.story = None
        self.link = None
        self.picture_link = None
        self.description = None
        self.nb_reactions = None
        self.nb_comments = None
        self.nb_shares = None

        self.updated = datetime.utcnow()
        super(Contents, self).__init__(content_id)


class Poststats(StatBase):
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[DB]
    collection = db.poststats
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 1
    bulk_updates_buffer_size = 1

    set_fields = ['created', 'page_ref', 'content_ref', 'author_ref', 'post_type', 'status_type', 'to_refs',
                  'reactions', 'u_reacted', 'comments', 'u_commented', 'u_comments_liked', 'updated']
    inc_fields = ['nb_shares', 'nb_reactions', 'nb_comments', 'nb_comments_likes']
    push_fields = []
    add_to_set_fields = []

    def __init__(self, poststat_id):
        self.id = poststat_id
        self._id = None
        self.created = None

        self.page_ref = None
        self.content_ref = None
        self.author_ref = None

        self.post_type = None
        self.status_type = None

        self.to_refs = None

        self.nb_shares = None
        self.nb_reactions = None
        self.nb_comments = None
        self.nb_comments_likes = None

        self.reactions = None
        self.u_reacted = None
        self.comments = None
        self.u_commented = None
        self.u_comments_liked = None

        self.updated = datetime.utcnow()
        super(Poststats, self).__init__(poststat_id)


class Users(StatBase):
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[DB]
    collection = db.users
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 1000
    bulk_updates_buffer_size = 1000  # avg size of doc = 1646 => 16M/1,6 = 10K doc., But limit is 1000 per batch anyway
    set_fields = ['name', 'picture', 'is_silhouette', 'updated']
    inc_fields = ['tot_posts', 'tot_toed', 'tot_reactions', 'tot_comments', 'tot_comments_liked']
    add_to_set_fields = ['pages_active', 'posted', 'toed', 'reacted', 'commented', 'comment_liked']
    push_fields = []

    def __init__(self, user_id):
        # field declarations
        self.id = user_id
        self._id = None
        self.name = None
        self.picture = None
        self.is_silhouette = None
        self.pages_active = None
        self.posted = None
        self.tot_posts = None
        self.toed = None
        self.tot_toed = None
        self.reacted = None
        self.tot_reactions = None  # todo: make if {like: 100, angry:10, ...}
        self.commented = None
        self.tot_comments = None
        self.comment_liked = None
        self.tot_comments_liked = None

        self.updated = datetime.utcnow()
        super(Users, self).__init__(user_id)


class Comments(StatBase):
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[DB]
    collection = db.comments
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    set_fields = []
    inc_fields = []
    push_fields = []
    add_to_set_fields = []

    def __init__(self, comment_id):
        super(Comments, self).__init__(comment_id)
        self.id = comment_id
        self._id = None

        self.updated = datetime.utcnow()


if __name__ == '__main__':
    pass
