#!usr/bin/python

import pandas as pd                        
from pytrends.request import TrendReq
import datetime
import sys, os
import warnings
warnings.filterwarnings("ignore")

pytrend = TrendReq()

kw_list = kw_list = ['Argentina win', 'France win']

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
with open(os.devnull, "w") as devnull:
    old_stdout = sys.stdout
    sys.stdout = devnull
    df = pytrend.get_historical_interest(
        kw_list, 
        year_start=yesterday.year, 
        month_start=yesterday.month, 
        day_start=yesterday.day, 
        hour_start=0, 
        year_end=today.year, 
        month_end=today.month, 
        day_end=today.day, 
        hour_end=0, 
        sleep=60
    )
    sys.stdout = old_stdout
df.to_csv(sys.stdout)
