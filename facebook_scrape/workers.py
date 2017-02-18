from datetime import datetime, timedelta
from pprint import pprint

from profilehooks import profile, timecall

from  facebook_page_lists import facebook_page_lists
from facebook_scrape import queries


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
                _id = queries.insert_page(page)
                inserted_ids.append(_id)
    return inserted_ids


# todo: make <get_posts_upsert_contents> worker
@timecall()
def get_posts_upsert_contents(page_id, since=None, until=None):
    # get facebook posts
    posts = queries.fb_get_posts(page_id=page_id, since=since)
    # pprint(posts[0])
    # transform post in content
    # bulkwrite content
    result = queries.insert_bulk_content(posts)
    return result


if __name__ == '__main__':
    # insert_pages_from_dict()
    since = datetime.now() - timedelta(days=0, hours=18)
    x = get_posts_upsert_contents('270994524621', since=since)
    pprint(x)
