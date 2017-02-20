from datetime import datetime, timedelta
from pprint import pprint
from time import sleep

from profilehooks import profile, timecall

from  facebook_page_lists import facebook_page_lists
from facebook_scrape import queries
from facebook_scrape.stat_objects import Pages, Contents, Poststats, Users


# # fix: should be in PageWorker
# class CommentWorker(object):
#     def __init__(self, page_id):
#         self.page_ref = Pages(page_id).get_id_()
#         self.page_id = page_id
#
#     def process_reactions(self, since=None, until=None, old_db=True):
#         pass
#
#     def _get_likes(self, since=None, until=None, old_db=True):
#         pass
#
# # fix: should be in PageWorker
# class ReactionWorker(object):
#     def __init__(self, page_id):
#         self.page_ref = Pages(page_id).get_id_()
#         self.page_id = page_id
#
#     def process_reactions(self, since=None, until=None, old_db=True):
#         fb_posts = self._get_reactions(since, until, old_db)
#         for p in fb_posts:
#             print p
#         1/0
#
#         # OLD CODE !!!
#         # u_reacted / nb_reactions
#         reacts = [self.fbpost.reactions[r].to_mongo().to_dict() for r in xrange(len(self.fbpost.reactions))]  # Fix: only needed to convert EmbeddedList to list of dics
#         if reacts:
#             # Fix: check if reacts[0].get('black..., None) is faster
#             if reacts[0].keys() != ['blacklisted']:  # Fix: Scraping
#                 df_reacts = pd.DataFrame(reacts)
#                 df_reacts['type'] = df_reacts['type'].str.lower()  # LIKE->like
#                 dfg_reacts = df_reacts.groupby(['type'])  # tuple of (str,df)
#                 # set the count per type
#                 self.poststat.reactions = dfg_reacts['id'].count().to_dict()
#                 self.poststat.nb_reactions = sum(self.poststat.reactions.values())
#                 # Iterate reactions and extract userdata
#                 reacted = {}
#                 for i, usr in df_reacts.iterrows():  # row is pandas.Series
#                     user, _useractivity = self.__make_user(user_id=usr['id'],
#                                                            user_name=usr['name'],
#                                                            user_picture=None,
#                                                            date=self.poststat.created,
#                                                            action_type='reaction',
#                                                            action_subtype=usr['type'])
#                     user_upsdoc = user.to_mongo().to_dict()
#                     user_upsdoc.update(push__reacted=_useractivity)
#                     user_upsdoc.update(inc__tot_reactions=1)
#                     user.upsert_doc(ups_doc=user_upsdoc)
#                     # Add user.oid to the correct list in 'poststat.u_reacted'
#                     # see https://docs.quantifiedcode.com/python-anti-patterns/correctness/not_using_setdefault_to_initialize_a_dictionary.html
#                     reacted.setdefault(usr['type'], []).append(user.oid)
#                 self.poststat.u_reacted = reacted
#         return None
#
#     def _get_reactions(self, since=None, until=None, old_db=True):
#         if old_db:
#             reactions = queries.fb_get_reactions_from_old_db(page_id=self.page_id, since=since, until=until)  # cursor
#         else:
#             reactions = queries.fb_get_reactions(page_id=self.page_id, since=since, until=until)  # list
#         return reactions


class PageWorker(object):
    def __init__(self, page_id):
        self.page_ref = Pages(page_id).get_id_()
        self.page_id = page_id

    @timecall()
    def process_posts(self, since=None, until=None, old_db=True):
        fb_posts = self._get_posts(since, until, old_db)
        for p in fb_posts:
            ##########################################################################################################
            # fix: implement reactions; rest works
            # Be careful where to put the 'reactions' code. Possible update problems when same user in past and reaction?

            # print queries.fb_get_reactions(p['id'])
            for r in queries.fb_get_reactions_from_old_db(p['id']):
                print r
                print 'x' * 100
            ###########################################################################################################

            # initiation of refs
            page_ref = self.page_ref
            content = Contents(content_id=p['id'])
            content_ref = content.get_id_()
            poststat = Poststats(poststat_id=p['id'])
            poststat_ref = poststat.get_id_()
            created = datetime.fromtimestamp(p.get('created_time', 0))

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
            # content.nb_reactions = None                                                   # todo
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

            # poststat.nb_reactions = None
            # poststat.nb_comments = None
            # poststat.nb_comments_likes = None
            # poststat.reactions = None
            # poststat.u_reacted = None
            # poststat.comments = None
            # poststat.u_commented = None
            # poststat.u_comments_liked = None

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

    def _get_posts(self, since=None, until=None, old_db=True):
        if old_db:
            posts = queries.fb_get_posts_from_old_db(page_id=self.page_id, since=since, until=until)  # cursor
        else:
            posts = queries.fb_get_posts(page_id=self.page_id, since=since, until=until)  # list
        return posts


if __name__ == '__main__':
    # insert_facebook_page_list()
    start = datetime.now() - timedelta(days=5, hours=18)
    pw = PageWorker(page_id='53668151866')
    pw.process_posts(since=start, until=None, old_db=True)






