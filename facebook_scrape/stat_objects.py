from datetime import datetime

from pymongo import MongoClient

from facebook_scrape.settings import DB, MONGO_HOST, MONGO_PORT

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB]


class StatBase(object):
    collection = None

    def __init__(self, doc_id):
        # common attributes
        self.pk = doc_id
        self._id = None
        self.flag = 'MEMORY'

    def get_id_(self):
        result = self.collection.find_one(filter={'id': self.pk}, projection={'_id': 1})
        if result:
            self._id = result.get('_id', 'ERROR')
            self.flag = result.get('flag', 'MISSING')
        else:  # document doesn't exist, so make one
            result = self.collection.insert_one(document={'id': self.pk, 'flag': 'INIT'})
            self._id = result.inserted_id
            self.flag = 'INIT'
        return self._id

    def upsert(self):
        pass

    def update(self):
        pass

    def populatexxx(self):
        doc = self.collection.find_one({'id': self.pk})
        for k, v in doc.items():
            setattr(self, k, v)
        return self

    def __str__(self):
        return self.__dict__.__str__()


class Page(StatBase):
    def __init__(self, pageid):
        Page.collection = db.pages
        super(Page, self).__init__(pageid)
        # field declarations
        self.id = self.pk
        self._id = None
        self.name = None
        self.type = None
        self.sub_type = None


class Content(StatBase):
    def __init__(self, content_id):
        Content.collection = db.contents
        super(Content, self).__init__(content_id)
        # field declarations
        self.id = self.pk
        self._id = None
        self.created = None

        self.page_ref = None
        self.poststat_ref = None
        self.user_ref = None

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


class Poststat(StatBase):
    def __init__(self, poststat_id):
        Poststat.collection = db.poststats
        super(Poststat, self).__init__(poststat_id)
        # field declarations
        self.id = self.pk
        self._id = None
        self.created = None

        self.page_ref = None
        self.content_ref = None
        self.user_ref = None

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


if __name__ == '__main__':
    pass
    p = Page('53668151866_70872315459')
    print p.collection
    p.populate()
