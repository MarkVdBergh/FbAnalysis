import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from pprint import pprint
from time import sleep

import pandas as pd
import sys
from profilehooks import profile, timecall
from termcolor import cprint

from facebook_scrape import queries
from facebook_scrape.stat_objects import Pages, Contents, Poststats, Users


class PageWorker(object):
    def __init__(self, page_id, from_fb=False):
        self.page_id = page_id
        self.from_fb = from_fb
        self.page_ref = Pages(page_id).get_id_()
        self.post = None
        self.post_id = None
        self.content = None
        self.content_ref = None
        self.poststat = None
        self.poststat_ref = None
        self.created = None

    def process_posts(self, since=None, until=None):
        fb_posts = self._get_posts(since, until, from_fb=self.from_fb, ne_flag=1)
        for post in fb_posts:
            print post['id'], '   ',
            self.post = post
            self.post_id = post['id']
            self.content = Contents(content_id=self.post_id)
            self.content_ref = self.content.get_id_()
            self.poststat = Poststats(poststat_id=self.post_id)
            self.poststat_ref = self.poststat.get_id_()
            self.created = datetime.fromtimestamp(post.get('created_time', 0))
            try:
                self.poststat.u_reacted = self.process_reactions()
            except KeyError as e:
                print e
                print '-' * 120
                cprint(self.post, 'red', attrs=['blink'])
                cprint(self.post_id, 'red')
                print '-' * 120
                traceback.print_exc()
                sys.exit(0)
            except Exception:
                print '-' * 120
                print 'PROCESS_POSTS'
                traceback.print_exc()
                print post
                print '-' * 120
                sys.exit(0)
            try:
                self.process_content()
            except Exception as e:
                print '-' * 120
                print 'PROCESS_CONTENT'
                traceback.print_exc()
                print e
                print post
                print '-' * 120
                sys.exit(0)
            # all ok => set flag on fb post
            self.content.add_to_bulk_update()
            self.poststat.add_to_bulk_update()
            queries.fb_set_flag(self.post_id, 1)
        # update everything not saved yet for the page
        try:  # fix: better error trapping
            Users.bulk_write()
            Contents.bulk_write()
            Poststats.bulk_write()
        except Exception as e:
            print '-' * 120
            print 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            traceback.print_exc()
            print str(e)
            print '-' * 120
            Users.bulk_inserts_buffer = []
            Poststats.bulk_inserts_buffer = []
            Contents.bulk_inserts_buffer = []
            sys.exit(0)

    def process_reactions(self):
        reactions = self._get_reactions(self.post_id)
        if not reactions: return []  # No reactions
        df_reactions = pd.DataFrame(reactions)
        df_reactions['type'] = df_reactions['type'].str.lower()  # LIKE->like
        dfg_reactions = df_reactions.groupby(['type'])  # groupby object
        # set <poststat>, <content> fields
        self.poststat.reactions = dfg_reactions['id'].count().to_dict()  # {'like':10, ...}
        _nb_reactions = len(df_reactions.index)  # faster than df.shape or df[0].count()
        self.content.nb_reactions = _nb_reactions
        self.poststat.nb_reactions = _nb_reactions
        # save users
        u_reacted = defaultdict(list)
        for __, usr in df_reactions.iterrows():  # usr_ser is pandas.Series
            user = Users(usr['id'])
            user_id_ = user.get_id_()
            user.name = usr['name']
            user.picture = usr['pic'].strip('https://scontent.xx.fbcdn.net/v/t1.0-1/p100x100/')
            user.pages_active = self.page_ref
            user.reacted = {'date': self.created,
                            'reaction': usr['type'],
                            'page_ref': self.page_ref,
                            'poststat_ref': self.poststat_ref,
                            'content_ref': self.content_ref}
            user.tot_reactions = 1
            user.add_to_bulk_update()

            u_reacted[usr['type']].append(user_id_)
            # print usr['type'], len(u_reacted[usr['type']]), '      ',
        # Users.bulk_write()  # fix: is this necessary?, better just leve it in the buffer?
        return u_reacted

    def process_content(self):
        # work on content
        self.content.created = self.created
        self.content.page_ref = self.page_ref
        self.content.poststat_ref = self.poststat_ref
        self.content.post_type = self.post.get('type', None)
        # content.author_ref = author_ref                                               # ok
        self.content.status_type = self.post.get('status_type', None)
        self.content.message = self.post.get('message', None)
        self.content.name = self.post.get('name', None)
        self.content.story = self.post.get('story', None)
        self.content.link = self.post.get('link', None)
        self.content.picture_link = self.post.get('picture', None)
        self.content.description = self.post.get('description', None)
        # content.nb_reactions = None                                                   # ok
        # content.nb_comments = None                                                    # todo
        self.content.nb_shares = self.post.get('shares', {}).get('count', 0)
        self.content.updated = datetime.utcnow()

        # work on poststat
        self.poststat.created = datetime.fromtimestamp(self.post.get('created_time', 0))
        self.poststat.page_ref = self.page_ref
        self.poststat.content_ref = self.content_ref
        # poststat.author_ref = author_ref                                              # ok
        self.poststat.post_type = self.post.get('type', None)
        self.poststat.status_type = self.post.get('status_type')
        # poststat.to_refs = to_refs                                                    # ok
        self.poststat.nb_shares = self.post.get('shares', {}).get('count', 0)

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
        author = Users(self.post.get('from', {}).get('id', None))  # => p['from.id']
        author_ref = author.get_id_()
        if author.flag == 'INIT':  # set missing fields
            author.name = self.post.get('from', {}).get('name', 'ERROR')
        author.pages_active = self.page_ref
        author.posted = {'date': self.created, 'page_ref': self.page_ref, 'poststat_ref': self.poststat_ref, 'content_ref': self.content_ref}
        author.tot_posts = 1

        author.add_to_bulk_update()

        # update content and poststat
        self.content.author_ref = author_ref
        self.poststat.author_ref = author_ref

        # tos
        to_refs = []
        for t in self.post.get('to', {}).get('data', []):  # {u'id': u'10154929041358011', u'name': u'Hilde Vautmans'}
            to = Users(t['id'])
            to_ref = to.get_id_()
            if to.flag == 'INIT':
                to.name = t.get('name', 'ERROR')
            to.pages_active = self.page_ref
            to.toed = {'date': self.created, 'page_ref': self.page_ref, 'poststat_ref': self.poststat_ref, 'content_ref': self.content_ref}
            to.tot_toed = 1
            to.add_to_bulk_update()
            to_refs.append(to_ref)
        # update  poststat
        self.poststat.to_refs = to_refs
        # add content and poststat to bulk_update

    def _get_posts(self, since=None, until=None, from_fb=False, ne_flag=1):
        if not from_fb: reactions = queries.fb_get_posts_from_old_db(page_id=self.page_id, since=since, until=until,
                                                                     post_filter={'flag': {'$ne': ne_flag}})  # cursor
        else: reactions = queries.fb_get_posts(page_id=self.page_id, since=since, until=until)  # list
        return reactions

    def _get_reactions(self, post_id, use_fb=False):
        if not use_fb: reactions = queries.fb_get_reactions_from_old_db(post_id)  # list
        else: reactions = queries.fb_get_reactions(post_id)  # list
        return reactions


if __name__ == '__main__':
    pass
    # insert_facebook_page_list()
    # pw = PageWorker('84920854319')
    # pw.process_posts()
    '''
    post_id: 84920854319_10153707522284320, reactions
    This generates an error after +/-15 min. The key 127 is having a nan in the dataframe iso like
    TEMP_FIXED wit drop nan, but still problem how it came there.


    Traceback (most recent call last):
  File "runners.py", line 8, in <module>
    from facebook_scrape.settings import MONGO_HOST, MONGO_PORT, FB_APP_ID, FB_APP_SECRET
  File "/home/marc/DATA/Projects/FbAnalytics/facebook_scrape/workers.py", line 213, in <module>
    pw.process_posts()
  File "/home/marc/DATA/Projects/FbAnalytics/facebook_scrape/workers.py", line 49, in process_posts
    queries.management_log_errors(doc)
  File "/home/marc/DATA/Projects/FbAnalytics/facebook_scrape/queries.py", line 211, in management_log_errors
    collection.insert_one(doc)
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 657, in insert_one
    bypass_doc_val=bypass_document_validation),
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 562, in _insert
    check_keys, manipulate, write_concern, op_id, bypass_doc_val)
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/collection.py", line 543, in _insert_one
    check_keys=check_keys)
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/pool.py", line 424, in command
    self._raise_connection_failure(error)
  File "/home/marc/DATA/.virtualenv/ml/local/lib/python2.7/site-packages/pymongo/pool.py", line 552, in _raise_connection_failure

    raise error
    bson.errors.InvalidDocument: documents must have only string keys, key was 127



    '''
