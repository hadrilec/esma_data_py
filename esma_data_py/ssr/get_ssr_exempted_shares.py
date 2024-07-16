# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 20:37:00 2023

@author: hleclerc
"""

from pathlib import Path
import urllib.request as request
import shutil
import os
from contextlib import closing
import zipfile
import re
from collections import ChainMap
import math
import tempfile
import hashlib
import datetime
import functools
from functools import lru_cache
import warnings
import multiprocessing
import xml.etree.ElementTree as ET
import pandas as pd
import tqdm
import requests
from tqdm import trange
from esma_data_py.utils.utils import *

    
@lru_cache(maxsize=None)
def get_ssr_exempted_shares(today=True):

    list_countries = ['AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK',
                      'EE', 'ES', 'FI', 'FR', 'GR', 'HR', 'HU',
                      'IE', 'IT', 'LT', 'LU', 'LV', 'MT', 'NL',
                      'PL', 'PT', 'RO', 'SE', 'SI', 'SK'] + ['NO', 'GB']
    
    ssr_query = 'https://registers.esma.europa.eu/solr/esma_registers_mifid_shsexs/select?q=({!parent%20which=%27type_s:parent%27})&wt=json&indent=true&rows=150000'

    list_df = []

    for c in tqdm.trange(len(list_countries)):
        country = list_countries[c]
        q = ssr_query + f'&fq=(shs_countryCode:{country})'
        dfget = requests.get(q)
        dictjson = dfget.json()['response']['docs']
        df = pd.DataFrame(dictjson)
        if (len(df.index) == 100000):
            print('split query')
            print(q)
        list_df += [df]

    data = pd.concat(list_df).drop_duplicates()
    
    if today:
        today_date = datetime.today().strftime('%Y-%m-%d')
        data2 = data[(data['shs_modificationBDate'] > today_date) &                    
                    (data['shs_exemptionStartDate'] <= today_date)]
        
        data3 = data2[data2.duplicated(['shs_isin'], keep=False)]
        data3 = data3[(data3['shs_modificationDateStr'] <= today_date)]
        
        data4 = data2[~data2['shs_isin'].isin(data3['shs_isin'])]
        
        data = pd.concat([data3, data4]).reset_index(drop=True)

    return data