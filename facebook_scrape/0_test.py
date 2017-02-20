from pprint import pprint

from facebook_scrape import queries
from facebook_scrape.settings import DB as db
from facebook_scrape.stat_objects import Users
import pandas as pd

post_id = '53668151866_10154252863826867'
reactions = queries.fb_get_reactions_from_old_db(post_id)[:50]
df_reactions = pd.DataFrame(reactions)
df_reactions['type'] = df_reactions['type'].str.lower()  # LIKE->like
dfg_reactions = df_reactions.groupby(['type'])  # tuple of (str,df)

# set the count per type
poststat.reactions = dfg_reactions['id'].count().to_dict()
poststat.nb_reactions = sum(poststat.reactions.values())
# Iterate reactions and extract userdata
reacted = {}
for i, usr in df_reacts.iterrows():  # row is pandas.Series
    user, _useractivity = self.__make_user(user_id=usr['id'],
                                           user_name=usr['name'],
                                           user_picture=None,
                                           date=self.poststat.created,
                                           action_type='reaction',
                                           action_subtype=usr['type'])
    user_upsdoc = user.to_mongo().to_dict()
    user_upsdoc.update(push__reacted=_useractivity)
    user_upsdoc.update(inc__tot_reactions=1)
    user.upsert_doc(ups_doc=user_upsdoc)
    # Add user.oid to the correct list in 'poststat.u_reacted'
    # see https://docs.quantifiedcode.com/python-anti-patterns/correctness/not_using_setdefault_to_initialize_a_dictionary.html
    reacted.setdefault(usr['type'], []).append(user.oid)
self.poststat.u_reacted = reacted
