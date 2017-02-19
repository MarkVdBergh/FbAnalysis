import pprint
from time import mktime

from datetime import datetime


def dt_to_ts(dt):
    return mktime(dt.timetuple())


def doc_to_update():
    pass


# def post_to_content(post):
#     """
#     Receives a facebook post document (from <> or <>) and transforms it into a <contents> document
#     :param post: dict: facebook post document
#     :return: content: dict: content document
#     """
#     content = post
#     content = {'id': post.get('id', None),
#                'created': datetime.utcnow(),
#                'u_from_ref': ObjectId('000000000000000000000000'),
#                'u_to_ref': [ObjectId('000000000000000000000001'), ObjectId('000000000000000000000002')],
#                'type': 'type_0',
#                'status_type': 'status_type_0',
#                'message': 'message_0',
#                'message_tags': ['tag_0', 'tag_1'],
#                'link': 'link_0',
#                'name': 'name_0',
#                'description': 'description_0',
#                'picture': 'picture_0',
#                'story': 'story_0',
#                'shares': 'shares_0',
#                'updated': datetime.utcnow()}
#     return content


def logit(name, kind, message):
    """
    :param name: str: <method>.__name__
    :param kind: str: error, warning, info
    :param message: str: will pprint(message)
    :return:
    """
    msg = '-' * 120 + '\n' + '{}: {}: {}: {}'.format(datetime.now(), name, kind, pprint.pformat(message))
    print(msg)
