from collections import defaultdict
from pprint import pprint
from timeit import timeit

from profilehooks import profile, timecall

from facebook_scrape import queries
from facebook_scrape.settings import DB as db
from facebook_scrape.stat_objects import Users, Poststats, Contents
import pandas as pd

# post_id = '37823307325_10152438030852326' # 44K reactions
post_id = '37823307325_10152438030852326' # 44K reactions

reactions = queries.fb_get_reactions_from_old_db(post_id)  # [:1000]
# print list(reactions)
poststat=Poststats(post_id)
content =Contents(post_id)
print len(reactions)

def process_reactions2(post_id):
    pass
    # get _id for each user

# buffer: 10000 =>  1695.927 seconds
# buffer: 30000 =>  1647.347 seconds
# buffer: 5     =>  1665.318 seconds
# buffer: 5 reboot    =>  1482.481 seconds



# @timecall()
@profile()
def process_reactions(post_id):

    df_reactions = pd.DataFrame(reactions)
    df_reactions['type'] = df_reactions['type'].str.lower()  # LIKE->like
    dfg_reactions = df_reactions.groupby(['type'])  # groupby object
    # set <poststat>, <content> fields
    poststat.reactions = dfg_reactions['id'].count().to_dict()  # {'like':10, ...}
    _nb_reactions = len(df_reactions.index)  # faster than df.shape or df[0].count()
    content.nb_reactions = _nb_reactions
    poststat.nb_reactions = _nb_reactions
    # save users
    u_reacted = defaultdict(list)
    # fix: this is probably very slow
    # fix: test with '37823307325_10152438030852326' (44K users)
    for __, usr in df_reactions.iterrows():  # usr_ser is pandas.Series
        user = Users(usr['id'])
        user_id_ = user.get_id_()
        # user_id_ = 1
        user.name = usr['name']
        user.picture = usr['pic'].strip('https://scontent.xx.fbcdn.net/v/t1.0-1/p100x100/')
        user.pages_active = 100
        user.reacted = {'date': 111,
                        'reaction': usr['type'],
                        'page_ref': 111,
                        'poststat_ref': 111,
                        'content_ref': 111}
        user.tot_reactions = 1
        user.add_to_bulk_update()
        #
        u_reacted[usr['type']].append(user_id_)
        print len(u_reacted[usr['type']]),


    Users.bulk_write()  # fix: is this necessary?, better just leve it in the buffer?
    return u_reacted
process_reactions(post_id)
