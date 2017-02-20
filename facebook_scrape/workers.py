from datetime import datetime, timedelta
from pprint import pprint
from time import sleep

import pandas as pd
from profilehooks import profile, timecall
from pymongo import MongoClient

from facebook_scrape import queries
from facebook_scrape.settings import FB_APP_ID, MONGO_HOST, MONGO_PORT, FB_APP_SECRET
from facebook_scrape.stat_objects import Pages, Contents, Poststats, Users


class PageWorker(object):
    # todo check if bulk_update contains more than 1 document for the same user/content, ... and conflicts !
    # todo check if I don't overwrite fields when update
    # todo: implement 'own_page'
    def __init__(self, page_id):
        self.page_ref = Pages(page_id).get_id_()
        self.page_id = page_id

    @timecall()
    def process_posts(self, since=None, until=None, old_db=True):
        fb_posts = self._get_posts(since, until, old_db)
        for p in fb_posts:
            # initiation of refs
            page_ref = self.page_ref
            content = Contents(content_id=p['id'])
            content_ref = content.get_id_()
            poststat = Poststats(poststat_id=p['id'])
            poststat_ref = poststat.get_id_()
            created = datetime.fromtimestamp(p.get('created_time', 0))

            ##########################################################################################################
            reactions = self._get_reactions(p['id'], old_db=old_db)
            if reactions:
                df_reactions = pd.DataFrame(reactions)
                df_reactions['type'] = df_reactions['type'].str.lower()  # LIKE->like
                dfg_reactions = df_reactions.groupby(['type'])  # tuple of (str,df)

                # set the count per type
                poststat.reactions = dfg_reactions['id'].count().to_dict()
                _nb_reactions = sum(poststat.reactions.values())
                content.nb_reactions = _nb_reactions
                poststat.nb_reactions = _nb_reactions
                # Iterate reactions and extract userdata
                u_reacted = {}
                for i, usr_s in df_reactions.iterrows():  # usr_ser is pandas.Series
                    _user = Users(usr_s['id'])
                    _user_id_ = _user.get_id_()
                    _user.name = usr_s['name']
                    _user.picture = usr_s['pic'].strip('https://scontent.xx.fbcdn.net/v/t1.0-1/p100x100/')
                    _user.pages_active = page_ref
                    _user.reacted = {'date': created, 'reaction': usr_s['type'], 'page_ref': page_ref, 'poststat_ref': poststat_ref, 'content_ref': content_ref}
                    _user.tot_reactions = 1

                    _user.add_to_bulk_update()

                    # Add user._id to the correct list in 'poststat.u_reacted'
                    # see https://docs.quantifiedcode.com/python-anti-patterns/correctness/not_using_setdefault_to_initialize_a_dictionary.html
                    u_reacted.setdefault(usr_s['type'], []).append(_user_id_)
                Users.bulk_write()
                poststat.u_reacted = u_reacted

            # work on content
            content.created = created
            content.page_ref = page_ref
            content.poststat_ref = poststat_ref
            content.post_type = p.get('type', None)
            # content.author_ref = author_ref                                               # ok
            content.status_type = p.get('status_type', None)
            content.message = p.get('message', None)
            content.name = p.get('name', None)
            content.story = p.get('story', None)
            content.link = p.get('link', None)
            content.picture_link = p.get('picture', None)
            content.description = p.get('description', None)
            # content.nb_reactions = None                                                   # ok
            # content.nb_comments = None                                                    # todo
            content.nb_shares = p.get('shares', {}).get('count', 0)
            content.updated = datetime.utcnow()

            # work on poststat
            poststat.created = datetime.fromtimestamp(p.get('created_time', 0))
            poststat.page_ref = page_ref
            poststat.content_ref = content_ref
            # poststat.author_ref = author_ref                                              # ok
            poststat.post_type = p.get('type', None)
            poststat.status_type = p.get('status_type')
            # poststat.to_refs = to_refs                                                    # ok
            poststat.nb_shares = p.get('shares', {}).get('count', 0)

            # poststat.nb_reactions = None                                                  # ok
            # poststat.nb_comments = None                                                   # todo
            # poststat.nb_comments_likes = None                                             # todo
            # poststat.reactions = None                                                     # ok
            # poststat.u_reacted = None                                                     # ok
            # poststat.comments = None                                                      # todo
            # poststat.u_commented = None                                                   # todo
            # poststat.u_comments_liked = None                                              # todo

            # work on users
            # author
            author = Users(p.get('from', {}).get('id', None))  # => p['from.id']
            author_ref = author.get_id_()
            if author.flag == 'INIT':  # set missing fields
                author.name = p.get('from', {}).get('name', 'ERROR')
            author.pages_active = page_ref
            author.posted = {'date': created, 'page_ref': page_ref, 'poststat_ref': poststat_ref, 'content_ref': content_ref}
            author.tot_posts = 1

            author.add_to_bulk_update()

            # update content and poststat
            content.author_ref = author_ref
            poststat.author_ref = author_ref

            # tos
            to_refs = []
            for t in p.get('to', {}).get('data', []):  # {u'id': u'10154929041358011', u'name': u'Hilde Vautmans'}
                to = Users(t['id'])
                to_ref = to.get_id_()
                if to.flag == 'INIT':
                    to.name = t.get('name', 'ERROR')
                to.pages_active = page_ref
                to.toed = {'date': created, 'page_ref': page_ref, 'poststat_ref': poststat_ref, 'content_ref': content_ref}
                to.tot_toed = 1
                to.add_to_bulk_update()
                to_refs.append(to_ref)
            # update  poststat
            poststat.to_refs = to_refs
            # add content and poststat to bulk_update
            content.add_to_bulk_update()
            poststat.add_to_bulk_update()

        # bulk update everything
        Users.bulk_write()
        print '-' * 120
        Contents.bulk_write()
        print '-' * 120
        Poststats.bulk_write()
        print '-' * 120
        sleep(.000001)

    def _get_posts(self, since=None, until=None, old_db=True):
        if old_db:
            reactions = queries.fb_get_posts_from_old_db(page_id=self.page_id, since=since, until=until)  # cursor
        else:
            reactions = queries.fb_get_posts(page_id=self.page_id, since=since, until=until)  # list
        return reactions

    def _get_reactions(self, post_id, old_db=True):
        if old_db:
            reactions = queries.fb_get_reactions_from_old_db(post_id)  # list
        else:
            reactions = queries.fb_get_reactions(post_id)  # list
        return reactions

if __name__ == '__main__':
    # insert_facebook_page_list()
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
