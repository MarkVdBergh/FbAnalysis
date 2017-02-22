import pprint
from time import mktime

from datetime import datetime


def dt_to_ts(dt):
    return mktime(dt.timetuple())


def doc_to_update():
    pass



def logit(name, kind, message):
    """
    :param name: str: <method>.__name__
    :param kind: str: error, warning, info
    :param message: str: will pprint(message)
    :return:
    """
    msg = '-' * 120 + '\n' + '{}: {}: {}: {}'.format(datetime.now(), name, kind, pprint.pformat(message))
    print(msg)
