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
from esma_data_py.mifid.download_file import *


def get_mifid_file_list(db_list = ['fitrs', 'firds', 'dvcap'],
                        creation_date_from='2017-01-01',
                        creation_date_to=None,
                        limit='100000'):
    
    if type(db_list) == str:
        db_list = [db_list]
        
    limit = str(limit)
    
    if creation_date_to is None:
        creation_date_to = str(datetime.datetime.today().strftime('%Y-%m-%d'))
    
    list_data = []
    
    for db in db_list:
        
        if db == 'firds':
            date_col = 'publication_date'
        else:
            date_col = 'creation_date'
    
        q = (f"https://registers.esma.europa.eu/solr/esma_registers_{db}_files/select?q=*"
              f"&fq={date_col}:%5B{creation_date_from}T00:00:00Z+TO+{creation_date_to}T23:59:59Z%5D&wt=xml&indent=true&start=0&rows={limit}")
    
        dirpath = tempfile.mkdtemp()
        raw_data_file = dirpath + "\\" + _hash(q)
        
        req = requests.get(q)
        
        with open(raw_data_file, "wb") as f:
            f.write(req.content)
    
        root = ET.parse(raw_data_file).getroot()
        
        list_ddict = []
        
        for j in range(len(root[1])):
            list_ddict += [{root[1][j][i].attrib['name'] : root[1][j][i].text for i in range(len(root[1][j]))}]
            
        data = pd.DataFrame.from_records(list_ddict)
        list_data += [data]
    
    data_final = pd.concat(list_data)
    
    return data_final