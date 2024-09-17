# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 20:37:00 2023

@author: hleclerc
"""


import datetime
import functools
import pandas as pd
import tqdm
import requests

    
@functools.lru_cache(maxsize=None)
def get_ssr_exempted_shares(today=True):
    """
    Retrieves SSR (Short Selling Regulation) exempted shares data from the European Securities and Markets Authority register for all European countries including Norway and Great Britain. Optionally filters the results to include only records relevant to the current date.

    Args:
        today (bool): If True, the function filters the data to show only records where the modification date, start date of exemption, or both are relevant for today's date. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the SSR exempted shares data, filtered based on the 'today' parameter. This data includes fields such as country code, ISIN, modification dates, and exemption start dates.

    Examples:
        >>> # Retrieve all SSR exempted shares data without any date filtering
        >>> exempted_shares = get_ssr_exempted_shares(today=False)
        >>> # Retrieve SSR exempted shares data relevant for today's date
        >>> exempted_shares_today = get_ssr_exempted_shares()
    """

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
        today_date = datetime.date.today().strftime('%Y-%m-%d')
        data2 = data[(data['shs_modificationBDate'] > today_date) &                    
                    (data['shs_exemptionStartDate'] <= today_date)]
        
        data3 = data2[data2.duplicated(['shs_isin'], keep=False)]
        data3 = data3[(data3['shs_modificationDateStr'] <= today_date)]
        
        data4 = data2[~data2['shs_isin'].isin(data3['shs_isin'])]
        
        data = pd.concat([data3, data4]).reset_index(drop=True)

    return data
