from collections import defaultdict
from pprint import pprint
from timeit import timeit

import sys
from profilehooks import profile, timecall

from facebook_scrape import queries
from facebook_scrape.settings import DB as db
from facebook_scrape.stat_objects import Users, Poststats, Contents
import pandas as pd



for i in xrange(100000):
    print i,
    if i==1000:
        sys.exit(0)