from datetime import datetime, timedelta
from pprint import pprint
from time import sleep

from profilehooks import profile, timecall

from  facebook_page_lists import facebook_page_lists
from facebook_scrape import queries
from facebook_scrape.stat_objects import Page, Content, Poststat


@timecall()
def insert_pages_from_dict(pages=facebook_page_lists):
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


def upsert_users_from_post(post, page_id):
    u_from = post.get('from', {})  # {'id':.., 'name',..}
    u_from['updated'] = datetime.utcnow()
    u_from_ref = queries.upsert_user(u_from)
    print u_from_ref.upserted_id
    print u_from_ref.raw_result
    sleep(1)
    u_to_refs = None
    users = {'u_from_ref': u_from_ref, 'u_to_refs': u_to_refs}
    return users


# todo: make <get_posts_upsert_contents> worker
# @profile()
def get_posts_upsert_contents(page_id, since=None, until=None, old_db=True):
    # get facebook posts
    if old_db:
        posts = queries.fb_get_posts_from_old_db(page_id=page_id, since=since)
    else:
        posts = queries.fb_get_posts(page_id=page_id, since=since)
    # get <_id>'s for the page in <pages> and <users>, most documents have <page> as <from> in the fb_post
    page_ids_ = queries.get_page_ids_(page_id)
    # get <_id>'s for <contents> and <poststats>

    # For using bulk-upserts, 2 passes are needed. 1st upsert all and get references. 2nd updates all with references.
    # We iterate all the posts twice, but we can bulk-upsert iso one by one.
    # Pass 1: Upsert content, poststat,
    # todo: this doesn't work ! <BulkWrite> doesn't return <_id>s
    # fix: first upsert only <id> and <flag> and get <_id>
    contents = []
    users = []
    for post in posts:
        content = {
            'id': post['id'],
            'created': datetime.fromtimestamp(post.get('created_time', None)),
            'page_ref': page_ids_['p_page_id_'],
            # user_ref: pass 2
            # poststat_ref: pass 2
            'type': post.get('type', None),
            'status_type': post.get('status_type', None),
            'message': post.get('message', None),
            'link': post.get('link', None),
            'name': post.get('name', None),
            'description': post.get('description', None),
            'picture': post.get('picture', None),
            'story': post.get('story', None),
            # 'u_tos': pass 2
            'nb_reactions': 0,
            'nb_comments ': 0,
            'nb_shares ': post.get('shares', {}).get('count', 0)}
        contents.append(content)
        # user_ref
        user_ref = {'$set': {'id': post['from']['id'], 'name': post['from']['name']},
                    '$addToSet': {'pages_active': page_ids_['p_page_id_'],
                                  'posted': {'date': datetime.fromtimestamp(post['created_time']),
                                             'page_ref': page_ids_['p_page_id_'],
                                             'poststat_ref': 0,
                                             'content_ref': 0}},
                    '$inc': {'tot_posts': 1}}
        users.append(user_ref)
        for u_to in post.get('to', {}).get('data', {}):
            user_tos = {'$set': {'id': u_to['id'], 'name': u_to['name']},
                        '$addToSet': {'pages_active': page_ids_['p_page_id_'],
                                      'toed': {'date': datetime.fromtimestamp(post['created_time']),
                                               'page_ref': page_ids_['p_page_id_'],
                                               'poststat_ref': 0,
                                               'content_ref': 0}},
                        '$inc': {'tot_toed': 1}}
            users.append(user_tos)

    pprint(users)
    print len(users)
    r = queries.bulk_upsert_poststat(users)
    print r.bulk_api_result

    return None


class CommentWorker(object):
    pass


class ReactionWorker(object):
    pass


class PageWorker(object):
    def __init__(self, page_id):
        self.page_ref = Page(page_id).get_id_()
        self.page_id = page_id

    def process_posts(self, since=None, until=None, old_db=True):
        fb_posts = self._get_posts(since, until, old_db)
        for p in fb_posts:
            content = Content(content_id=p['id'])
            content_ref = content.get_id_()
            poststat = Poststat(poststat_id=p['id'])
            poststat_ref = poststat.get_id_()
            # work on content
            content.created = datetime.fromtimestamp(p.get('created_time', 0)),
            content.page_ref = self.page_ref,
            content.poststat_ref = poststat_ref
            content.post_type = p.get('type', None)
            content.user_ref = None
            content.status_type = p.get('status_type', None)
            content.message = p.get('message', None)
            content.name = p.get('name', None)
            content.story = p.get('story', None)
            content.link = p.get('link', None)
            content.picture_link = p.get('picture', None)
            content.description = p.get('description', None)
            # content.nb_reactions = None
            # content.nb_comments = None
            content.nb_shares = p.get('shares', {}).get('count', 0)
            content.updated = datetime.utcnow()

    def _get_posts(self, since=None, until=None, old_db=True):
        if old_db:
            posts = queries.fb_get_posts_from_old_db(page_id=self.page_id, since=since, until=until)  # cursor
        else:
            posts = queries.fb_get_posts(page_id=self.page_id, since=since, until=until)  # list
        return posts


if __name__ == '__main__':
    # insert_pages_from_dict()
    start = datetime.now() - timedelta(days=30, hours=18)
    # x = get_posts_upsert_contents('37823307325', since=since, old_db=True)
    pw = PageWorker(page_id='53668151866')
    pw.process_posts(since=start, until=None, old_db=True)







    # pprint(x.nInserted)
