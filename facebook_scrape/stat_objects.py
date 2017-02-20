from collections import defaultdict
from datetime import datetime
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from facebook_scrape.helpers import logit
from facebook_scrape.settings import DB, MONGO_HOST, MONGO_PORT

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB]


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
        # common attributes
        self.pk = doc_id
        self._id = None
        self.flag = 'MEMORY'

    def get_id_(self):
        result = self.collection.find_one(filter={'id': self.pk}, projection={'_id': 1, 'id': 1, 'flag': 1})
        if result:
            self._id = result.get('_id', 'ERROR')
            self.flag = result.get('flag', 'MISSING')
        else:  # document doesn't exist, so make one
            result = self.collection.insert_one(document={'id': self.pk, 'flag': 'INIT'})
            self._id = result.inserted_id
            self.flag = 'INIT'
        return self._id

    def add_to_bulk_update(self):
        self.__class__.bulk_updates_buffer.append(self)
        nb_documents = len(self.__class__.bulk_updates_buffer)
        if nb_documents >= self.__class__.bulk_updates_buffer_size:
            logit(self.__class__.__name__, 'info', 'Buffer ({}) full'.format(nb_documents))  # fix: test it
            result = self.bulk_write()  # flush buffer

    @classmethod
    def bulk_write(cls):
        result = None
        if cls.bulk_updates_buffer:  # there are docuents to upgrade
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
                result = cls.collection.bulk_write(operations)
                cls.bulk_updates_buffer = []
                logit(cls.__name__, 'info', 'Updated {} documents'.format(len(operations)))
            except BulkWriteError as e:
                logit(cls.__name__, 'error', e.details)
                result = e.details
        return result

    def populate(self):
        doc = self.collection.find_one({'id': self.pk})
        for k, v in doc.items():
            setattr(self, k, v)
        return self

    def __str__(self):
        return self.__dict__.__str__()

    def __repr__(self):  # for printing class instances in lists
        return self.__str__()


class Pages(StatBase):
    collection = db.pages
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 10
    bulk_updates_buffer_size = 10
    set_fields = ['name', 'type', 'sub_type']
    inc_fields = []
    push_fields = []
    add_to_set_fields = []

    def __init__(self, pageid):
        # field declarations
        super(Pages, self).__init__(pageid)
        self.id = self.pk
        self._id = None
        self.name = None
        self.type = None
        self.sub_type = None


class Contents(StatBase):
    collection = db.contents
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 100
    bulk_updates_buffer_size = 100
    set_fields = ['created', 'poststat_ref', 'page_ref', 'author_ref', 'post_type', 'status_type',
                  'message', 'name', 'story', 'link', 'picture_link', 'description', 'updated']
    inc_fields = ['nb_reactions', 'nb_comments', 'nb_shares']
    push_fields = []
    add_to_set_fields = []

    def __init__(self, content_id):
        super(Contents, self).__init__(content_id)
        # field declarations
        self.id = self.pk
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


class Poststats(StatBase):
    collection = db.poststats
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 100
    bulk_updates_buffer_size = 100

    set_fields = ['created', 'page_ref', 'content_ref', 'author_ref', 'post_type', 'status_type', 'to_refs',
                  'reactions', 'u_reacted', 'comments', 'u_commented', 'u_comments_liked', 'updated']
    inc_fields = ['nb_shares', 'nb_reactions', 'nb_comments', 'nb_comments_likes']
    push_fields = []
    add_to_set_fields = []

    def __init__(self, poststat_id):
        super(Poststats, self).__init__(poststat_id)
        self.id = self.pk
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


class Users(StatBase):
    collection = db.users
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    bulk_inserts_buffer_size = 1000
    bulk_updates_buffer_size = 1000
    set_fields = ['name', 'picture', 'is_silhouette', 'updated']
    inc_fields = ['tot_posts', 'tot_toed', 'tot_reactions', 'tot_comments', 'tot_comments_liked']
    add_to_set_fields = ['pages_active', 'posted', 'toed', 'reacted', 'commented', 'comment_liked']
    push_fields = []

    def __init__(self, user_id):
        super(Users, self).__init__(user_id)
        # field declarations
        self.id = self.pk
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


class Comments(StatBase):
    collection = db.comments
    bulk_inserts_buffer = []
    bulk_updates_buffer = []
    set_fields = []
    inc_fields = []
    push_fields = []
    add_to_set_fields = []

    def __init__(self, comment_id):
        super(Comments, self).__init__(comment_id)
        self.id = self.pk
        self._id = None

        self.updated = datetime.utcnow()


if __name__ == '__main__':
    pass
    p = Pages('53668151866_70872315459')
    print p.collection
    p.populate()
