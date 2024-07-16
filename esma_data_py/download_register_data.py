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
import os
from functools import lru_cache
import os
import tempfile
import hashlib
import warnings
import multiprocessing
from functools import lru_cache

import xml.etree.ElementTree as ET
import appdirs # got rid of via tempfile
import pandas as pd
import tqdm
import requests
from tqdm import trange


from set_global_args import set_global_args


def get_tag_clean(tag):
    pattern2remove = re.findall('{.*}', tag)[0]
    tag = tag.replace(pattern2remove, '')
    return tag

def extract_data(r, rtag=None, clean=True):    
    
    r2 = r
    if len(r) == 0:
        if clean is True:
            tag = get_tag_clean(r.tag)
            
            if rtag is not None:
                rtag = get_tag_clean(rtag)                
                tag = rtag + '_' + tag
        else:
            tag = r.tag
            
        ddict = {tag : r.text}
        return ddict
    
    elif get_tag_clean(r2.tag) == 'DerivSubClss':
        ddict = [{get_tag_clean(r2[0].tag): r2[0].text}]
        for i in range(1, len(r2)):
            ddict += [{r2[i][0].text: r2[i][1].text}]
        ddict = dict(ChainMap(*ddict))
        
        return ddict    
    else:

        list_ddict = [extract_data(r[i], clean=clean, rtag = r.tag) for i in range(len(r))]
        ddict = dict(ChainMap(*list_ddict))
        return ddict

def _hash(string):

    h = hashlib.new("md5")
    h.update(string.encode("utf-8"))
    return h.hexdigest()



def _hash(string):

    h = hashlib.new("md5")
    h.update(string.encode("utf-8"))
    return h.hexdigest()
    
@lru_cache(maxsize=None)
def _warning_cached_data(file):
    print(
        "Previously saved data used:\n{}\nSet update=True to get the most up-to-date data".format(
            file
        )
    )

@lru_cache(maxsize=None)

def _create_folder(folder="data"):    
    # main_folder = appdirs.user_cache_dir() + "/" + folder

    # if not os.path.exists(main_folder):
    #     os.mkdir(main_folder)

    main_folder = Path.home() / 'esma_data_py' / folder
    
    if not main_folder.exists():
        main_folder.mkdir(parents= True)

    return main_folder
    
def save_df(obj=pd.DataFrame, print_cached_data=True, folder='data'):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            
            data_folder = _create_folder(folder=folder)
            string_file_arg = [str(kwargs[a]) for a in kwargs.keys() if a != 'update'] + \
                                [func.__name__] + [str(a) for a in args]            
            file_name = data_folder + "/" + _hash(''.join(string_file_arg)) + ".csv"
                                    
            if any([(a == 'update') & (kwargs[a] == True) for a in kwargs.keys()]):
                update = True
            else:
                update = False
            
            if (not os.path.exists(file_name)) | (update is True):   
                # function execution
                df = func(*args, **kwargs)                    
                try:
                    df.to_pickle(file_name)
                except Exception as e:
                    warnings.warn(str(e))
                    print(f'Error, file not saved:\n{file_name}\n{df}')
                    print(type(df))
                    print('\n')
                
                df = obj(df) 
                
                print(f"Data saved: {file_name}")

            else:
                try:                        
                    
                    df = pd.read_pickle(file_name)
                        
                    if 'Unnamed: 0' in df.columns:
                        del df['Unnamed: 0']
                        
                except Exception as e:
                    warnings.warn(str(e))                    
            
                    os.remove(file_name)
                    
                    kwargs2 = kwargs
                    kwargs2['update'] = True
                    
                    print('!!! Unable to load data, function retriggered !!!')
                    
                    df = func(*args, **kwargs2)
                    df = obj(df)   
                else:
                    if print_cached_data:
                        _warning_cached_data(file_name)
                    df = obj(df)            
            
            return df
        return wrapper
    return decorator

@save_df()
def download_one_file(url, update=False, silent=False):
    #url = 'http://fitrs.esma.europa.eu/fitrs/FULECR_20230325_E_1of1.zip'
    
    list_string_split = url.split("/")
    filename = list_string_split[len(list_string_split)-1]
    list_string_split_filename = filename.split(".")
    filename_clean = list_string_split_filename[0]
    
    temp = tempfile.mkdtemp()
    dataDir = str(temp) + "/" + filename_clean
    if not os.path.exists(dataDir):
        os.mkdir(dataDir)
        
    file_dwn = dataDir + "/" + "file_" + filename_clean
    
    with closing(request.urlopen(url, timeout=None)) as r:
        with open(file_dwn, 'wb') as f:
            shutil.copyfileobj(r, f)
                
    with zipfile.ZipFile(file_dwn, 'r') as zip_ref:
        zip_ref.extractall(dataDir)
            
    file_xml = [dataDir + '/' + f for f in os.listdir(dataDir) if re.search('xml$', f)][0]
        
    root = ET.parse(file_xml).getroot()
        
    r100 = root[1][0][0]   

    list_ddict = []
    if not silent:
        for i in trange(1, len(r100)):
            list_ddict += [extract_data(r100[i])]
    else:
        for i in range(1, len(r100)):
            list_ddict += [extract_data(r100[i])]
                
    data = pd.DataFrame.from_records(list_ddict)
    
    dict_extra_metadata = extract_data(root[0][0])      
    
    for c in dict_extra_metadata.keys():
        if c not in data.columns:
            data[c] = dict_extra_metadata[c]
    
    data = data.assign(url = url)
    
    return data


def _download_one_file(u, update=False):
    url = url_list[u]
    df = download_one_file(url, silent=True)
    return df


# @save_df()
def download_file(url_list, update=False, multiprocess=True, cpu=10):
    
    multiprocess = False
    
    if isinstance(url_list, str):
        url_list = [url_list]
    
    list_df = []
    
    i = 1
    
    if not multiprocess:
    
        for url in url_list:
            
            print(f"{i}/{len(url_list)} files")
        
            data = download_one_file(url, update=update)
            
            list_df += [data]
            
            i += 1
            
        data_final = pd.concat(list_df)
    
    else:
    
        args = [url_list]
    
        length = len(url_list)
        irange = range(length)
        
        Nprocesses = min(cpu, multiprocessing.cpu_count())    
    
        with multiprocessing.Pool(
                initializer = set_global_args, initargs=(args,), processes=Nprocesses
            ) as pool:
                list_output = list(
                        tqdm.tqdm(
                            pool.imap(_download_one_file, irange),
                            total=length,
                            desc="Download",
                        )
                    )
                
        data_final = pd.concat(list_output)       
    
    return data_final
    
def get_mifid_file_list(db_list = ['fitrs', 'firds', 'dvcap'],
                        creation_date_from='2017-01-01',
                        creation_date_to=None,
                        limit='100000'):
    
    if type(db_list) == str:
        db_list = [db_list]
        
    limit = str(limit)
    
    if creation_date_to is None:
        creation_date_to = str(datetime.datetime.today().strftime('%Y-%m-%d'))
    
    
    #date_col_list = ['creation_date', 'publication_date', 'creation_date'] 
    
    #db = 'firds'
    #date_col = 'publication_date'
    #firds_query = (f"https://registers.esma.europa.eu/solr/esma_registers_{db}_files/select?q=*&rows={limit}"
    #    f"&fq={date_col}:%5B{creation_date_from}T00:00:00Z+TO+{creation_date_to}T23:59:59Z%5D")

    #db = 'dvcap'
    #date_col = 'creation_date'
    #dvcap_query = (f"https://registers.esma.europa.eu/solr/esma_registers_{db}_files/select?q=*"
    #    f"&fq={date_col}:%5B{creation_date_from}T00:00:00Z+TO+{creation_date_to}T23:59:59Z%5D&wt=xml&indent=true&start=0&rows={limit}")

    #db = 'fitrs'
    #fitrs_query = (f"https://registers.esma.europa.eu/solr/esma_registers_{db}_files/select?q=*"
    #    f"&fq={date_col}:%5B{creation_date_from}T00:00:00Z+TO+{creation_date_to}T23:59:59Z%5D&wt=xml&indent=true&start=0&rows={limit}")

    #db = 'coder58'
    #q = (f"https://registers.esma.europa.eu/solr/esma_registers_{db}_files/select?q=*&rows={limit}")
    
    #q = dvcap_query
    #q = firds_query
    #q = fitrs_query

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
   
    
def get_fca_firds_file_list(db_list = ['firds'],
                        creation_date_from='2017-01-01',
                        creation_date_to=None,
                        limit='10000'):
    
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
    
        q = (f"https://api.data.fca.org.uk/fca_data_firds_files?q="
              f"((file_type:FULINS)%20AND%20({date_col}:[{creation_date_from}%20TO%20{creation_date_to}]))&pretty=true&from=0&size={limit}")
                
        dirpath = tempfile.mkdtemp()
        raw_data_file = dirpath + "\\" + _hash(q)
        
        req = requests.get(q)
    
        root = req.json()
        
        data = [dd['_source'] for dd in root['hits']['hits']]
        
        data = pd.DataFrame.from_records(data)
        list_data += [data]
    
    data_final = pd.concat(list_data)
    
    return data_final
   
#fca = get_fca_firds_file_list(creation_date_from='2020-01-01') 
    
def get_last_full_files(isin=None, cfi=None, eqt=True):
    
    mifid_file_list = get_mifid_file_list('fitrs')
    
    mifid_file_list = mifid_file_list[mifid_file_list['file_type'] == 'Full']
    
    mifid_file_list[['filetype', 'date', 'cfi', 'nfile']] = mifid_file_list['file_name'].str.split('_', expand=True, n=3)
    
    list_col = ['instrument_type', 'file_type', 'filetype', 'cfi']
    mifid_file_list2 = mifid_file_list[list_col + ['file_name']].drop_duplicates(subset=list_col)
    
    mifid_file_list = mifid_file_list[mifid_file_list['date'] == max(mifid_file_list['date'])]
    
    if eqt:
        mifid_file_list = mifid_file_list[mifid_file_list['instrument_type'] == 'Equity Instruments']
    else:
        mifid_file_list = mifid_file_list[mifid_file_list['instrument_type'] == 'Non-Equity Instruments']
        
    cfi_list = ['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S']
    
    if type(cfi) == str:
        cfi = [cfi]
    
    if cfi is not None:
        if not any([i in cfi for i in cfi_list]):
            raise ValueError(f'cfi should be included in {cfi_list}')
        else:
            mifid_file_list = mifid_file_list[mifid_file_list['cfi'].isin(cfi)]
            
    list_files = list(mifid_file_list['file_name'].unique())
    str_file = '\n'.join(list_files)
    print(f"the following files are about to be downloaded:\n{str_file}")
    
    list_url = list(mifid_file_list['download_link'].unique())
    
    list_df = []
    
    for u in list_url :
        df = download_file(u)
        list_df += [df]
    
    data = pd.concat(list_df)       
    
    if isin is not None:
        if type(isin) == 'str':
            isin = [isin]
            
        data = data[data['Id'].isin(isin)]        
    
    return data
            


@lru_cache(maxsize=None)
def get_ssr_exempted_shares(today=True):

    list_countries = ['AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK',
                      'EE', 'ES', 'FI', 'FR', 'GR', 'HR', 'HU',
                      'IE', 'IT', 'LT', 'LU', 'LV', 'MT', 'NL',
                      'PL', 'PT', 'RO', 'SE', 'SI', 'SK'] + ['NO', 'GB']
    
    ssr_query = 'https://registers.esma.europa.eu/solr/esma_registers_mifid_shsexs/select?q=({!parent%20which=%27type_s:parent%27})&wt=json&indent=true&rows=150000'

    # q = ssr_query + '&fq=(!shs_countryCode:FR)'

    list_df = []

    for c in tqdm.trange(len(list_countries)):
        country = list_countries[c]
        q = ssr_query + f'&fq=(shs_countryCode:{country})'
        #print(q)
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

if __name__ == '__main__': 
    
    
    fitrs = (get_mifid_file_list('fitrs'))    
    
    fitrs_files = (fitrs
                    .query("instrument_type.str.contains('^Non-Equity')")
                    .query("file_type == 'Full'")
                    # .query("creation_date >= '2024-04-30'")
                    # .query("creation_date <= '2024-05-07'")
                    )
    
    df = download_file("http://fitrs.esma.europa.eu/fitrs/FULNCR_20240622_D_2of6.zip")
    
    list_file = fitrs_files.download_link.to_list()
    df = download_file(list_file)   
    
    year = (df
            .query("EqtyTrnsprncyData_Mthdlgy == 'YEAR'")
            .query("FrDtToDt_FrDt == '2023-01-01'")
            .assign(AppHdr_CreDt = lambda x: x['AppHdr_CreDt'].apply(lambda y: datetime.strptime(y[:10], "%Y-%m-%d")))
            .assign(max_date = lambda x: x.groupby(['EqtyTrnsprncyData_Id']).AppHdr_CreDt.transform('max'))
            .query("AppHdr_CreDt == max_date") 
            .assign(Sttstcs_AvrgDalyNbOfTxs = lambda x: pd.to_numeric(x['Sttstcs_AvrgDalyNbOfTxs']))
            # .assign(tot_number_trns = lambda x: x['Sttstcs_AvrgDalyNbOfTxs'] * 252)
            )
    
    
        
    fca_firds_list = (get_fca_firds_file_list()
                      .query("file_name.str.contains('^FULINS_F|^FULINS_O')")
                      .query("publication_date >= '2024-01-01'")                  
                      )
    
    list_file = fca_firds_list.download_link.to_list()
    meta_fca = download_file(list_file)   
    
    
    folder =  Path(r"M:\%5C\temp\hleclerc\register")  
    file = folder / "ir_list_isin.csv"
    isin_list = pd.read_csv(file)
    isin_list = isin_list['PRODUCT_IDENTIFICATION'].to_list()
    
    meta_fca_ccp = meta_fca.query("FinInstrmGnlAttrbts_Id.isin(@isin_list)")
    list_final_isin = list(meta_fca_ccp.FinInstrmGnlAttrbts_Id.unique())
    
    meta_fca_ccp.to_csv(folder / "cpp_isin_fca_firds_v2.csv")
    
    
    folder =  Path(r"M:\%5C\temp\hleclerc\register")  
    file = folder / "FCA ISIN list.csv"
    isin_list = pd.read_csv(file)
    isin_list = isin_list['PRODUCT_IDENTIFICATION'].to_list()
    
    meta_fca_ccp = meta_fca.query("FinInstrmGnlAttrbts_Id.isin(@isin_list)")
    list_final_isin = list(meta_fca_ccp.FinInstrmGnlAttrbts_Id.unique())
    
    meta_fca_ccp.to_csv(folder / "cpp_isin_fca_firds.csv")
    
    fitrs = (get_mifid_file_list('fitrs'))    
    
    fitrs_files = (fitrs
                    .query("instrument_type.str.contains('^Equity')")
                    # .query("file_type == 'Full'")
                    .query("creation_date >= '2024-04-01'"))
    
    list_files = fitrs_files['download_link'].to_list()
    
    df = download_file(list_files)
    
    grISIN = df.query("EqtyTrnsprncyData_Id in ['GRS536003007']")
    
    
    ES_ISIN = df.query("EqtyTrnsprncyData_Id in ['ES0119256032', 'ES0182170003']")
    
    qt = get_data_filter("EQT_QUANTITATIVE_DATA",
                         filters=[('ISIN', ['ES0119256032', 'ES0182170003']),
                                  ('REPORTING_DAY', '2023-01-01', '>='),
                                  ('REPORTING_DAY', '2023-12-31', '<=')])


    from datetime import datetime
    
    year = (df
            .query("EqtyTrnsprncyData_Mthdlgy == 'YEAR'")
            .query("FrDtToDt_FrDt == '2023-01-01'")
            .assign(AppHdr_CreDt = lambda x: x['AppHdr_CreDt'].apply(lambda y: datetime.strptime(y[:10], "%Y-%m-%d")))
            .assign(max_date = lambda x: x.groupby(['EqtyTrnsprncyData_Id']).AppHdr_CreDt.transform('max'))
            .query("AppHdr_CreDt == max_date") 
            .assign(Sttstcs_AvrgDalyNbOfTxs = lambda x: pd.to_numeric(x['Sttstcs_AvrgDalyNbOfTxs']))
            # .assign(tot_number_trns = lambda x: x['Sttstcs_AvrgDalyNbOfTxs'] * 252)
            )
    ES_ISIN = year.query("EqtyTrnsprncyData_Id in ['ES0119256032', 'ES0182170003']").transpose()
    
    NLinvestigation = year.query("EqtyTrnsprncyData_Id in ['AT0000A18XM4', 'IT0004147952']")
    
    XS2427355958 
    
    eqtCalc = get_data_filter('EQT_CALCULATION_RESULTS',
                              filters=[('ISIN', ['AT0000A18XM4', 'IT0004147952']),
                                       ('CALCULATION_PERIOD_FROM', '2023-01-01'),
                                       ('METHODOLOGY', 'YEAR')])
    
    sum(year['tot_number_trns'])/10**9
    
    liq = (year
           [['EqtyTrnsprncyData_FinInstrmClssfctn', 'EqtyTrnsprncyData_Lqdty', 'EqtyTrnsprncyData_Id']]
           .groupby(['EqtyTrnsprncyData_FinInstrmClssfctn', 'EqtyTrnsprncyData_Lqdty'], as_index=False)
           .count()
           )
    
    
    sint = (df
            .query("EqtyTrnsprncyData_Mthdlgy == 'SINT'")
            [:1000]
            )
    
    from pathlib import Path
    year.to_csv(Path.home() / 'Desktop' / 'equity_annual_calculations_2023.csv')
  
    
    
    
    
    year = (df
            .query("EqtyTrnsprncyData_Mthdlgy == 'SINT'")
            # .query("FrDtToDt_FrDt == '2023-01-01'")
            # .assign(AppHdr_CreDt = lambda x: x['AppHdr_CreDt'].apply(lambda y: datetime.strptime(y[:10], "%Y-%m-%d")))
            # .assign(max_date = lambda x: x.groupby(['EqtyTrnsprncyData_Id']).AppHdr_CreDt.transform('max'))
            # .query("AppHdr_CreDt == max_date")            
            )
    
    from pyfid import *
    
    LU1650492330 = get_data_filter('EQT_SI_RESULTS',
                                   filters=[('ISIN', 'LU1650492330'),
                                            ('METHODOLOGY', 'SINT')])
    
    df2 = df.query("EqtyTrnsprncyData_Id in ['AT0000A18XM4', 'IT0004147952']")

    df2 = df.query("EqtyTrnsprncyData_Id == 'NL0015001FS8'")
    
    NL0015001FS8 = df.query("EqtyTrnsprncyData_Id == 'NL0015001FS8'")
    
    yearly = df.query("EqtyTrnsprncyData_Mthdlgy == 'YEAR'")
    
    lqd = (yearly
           [["EqtyTrnsprncyData_Lqdty", "EqtyTrnsprncyData_FinInstrmClssfctn", "EqtyTrnsprncyData_Id"]]
           .groupby(["EqtyTrnsprncyData_Lqdty", "EqtyTrnsprncyData_FinInstrmClssfctn"], as_index=False)
           .count())
    
    firds = (get_mifid_file_list('firds'))
    
    firds_20240203 = (firds
                      .query("publication_date <= '2024-02-04'")                      
                      .query("publication_date >= '2024-02-03'"))
    
    url_list = firds_20240203['download_link'].to_list()
    
    
    
    from pathlib import Path
    import re
    folder = Path.home() / 'Desktop'
    folder = Path(r"M:\%5C\temp\hleclerc\firds_files")
    i = 0
    for url in url_list:
        print(i)
        print(url)
        df = download_file(url)
        filename = re.sub("\.zip", "", url.split("/")[-1:][0])
        df.to_csv(folder / (str(filename) + '.csv'))
        i += 1
    
    url_list = []
    url_list += ['http://firds.esma.europa.eu/firds/FULINS_S_20240203_04of04.zip']
    url_list += ['http://firds.esma.europa.eu/firds/DLTINS_20240206_02of02.zip']
    url_list += ['http://firds.esma.europa.eu/firds/FULINS_E_20231230_02of02.zip']
    url_list += ['http://firds.esma.europa.eu/firds/FULCAN_20230408_01of02.zip']
    
    fulins_s = download_file(url_list[0])
    dltins = download_file(url_list[1])
    fulins_e = download_file(url_list[2])
    fulcan = download_file(url_list[3])
    
    
    fulins_s.to_csv(folder / 'fulins_s.csv')
    fulins_e.to_csv(folder / 'fulins_e.csv')
    dltins.to_csv(folder / 'dltins.csv')
    fulcan.to_csv(folder / 'fulcan.csv')

    
    dvcap_file_list = (get_mifid_file_list('dvcap'))
    dvcap = download_file('http://dvcap.esma.europa.eu/DVCRES_20240109.zip')

    from tqdm import trange
    
    
    mifid_file_list = (get_mifid_file_list('firds'))
    
    mifid_file_list = (get_mifid_file_list('fitrs')
                       .query("instrument_type.str.contains('^Equity')")
                       .query("file_type == 'Delta'")
                       .query("creation_date >= '2023-11-01'")
                       .query("creation_date < '2024-12-01'"))
    
    list(mifid_file_list['file_type'].unique())
    
    list_df = []
    list_url = mifid_file_list['download_link'].to_list()
    
    for u in trange(len(list_url)):
        url = list_url[u]
        df = download_file(url)
        list_df += [df]
        
    data = pd.concat(list_df)
    data.columns
    
    data2 = data.query("EqtyTrnsprncyData_Id == 'FR001400LAA6'")
    
    from pyfid import *
    lci = get_data_filter(table='latest_consistent_instruments',
                          filters=[('ISIN', 'FR001400LAA6')])
    
    calc = get_data_filter(table='eqt_calculation_results',
                          filters=[('ISIN', 'FR001400LAA6')])
    
    #2023-11-15
    
    url = 'http://fitrs.esma.europa.eu/fitrs/DLTECR_20240115_1of1.zip'
    
    df = download_file(url)
    
    url = "http://fitrs.esma.europa.eu/fitrs/FULECR_20230325_E_1of1.zip"
    df = download_file(url)
    
    
    
    df = data[data['EqtyTrnsprncyData_Id'].isin(list_isin)]
    len([i for i in list_isin if i in list(df['EqtyTrnsprncyData_Id'])])

    
    FR001400D5I2  = get_last_full_files(isin='FR001400D5I2', cfi='E')
    
    mifid_file_list = get_mifid_file_list('fitrs')
    
    mifid_file_list = mifid_file_list[mifid_file_list['file_type'] == 'FullSubclass']
    mifid_file_list = mifid_file_list[mifid_file_list['instrument_type'].str.contains('^Non-Equity')]
    mifid_file_list = mifid_file_list[mifid_file_list['file_name'].str.contains('EQD')]
    mifid_file_list = mifid_file_list[mifid_file_list['creation_date'] >= '2023-01-01']
    
    url = 'http://fitrs.esma.europa.eu/fitrs/FULNCR_20230429_NYAR_EQD_7of8.zip'
    df = download_file(url)
    
    df['Desc'].unique()
    
    df2 = df[df['Desc'] == 'Stock index options']
    
    
    mifid_file_list = mifid_file_list[mifid_file_list['creation_date'] >= '2023-03-01']
    
    mifid_file_list = mifid_file_list[mifid_file_list['instrument_type'].str.contains('^Equity')]
    
    mifid_file_list = mifid_file_list[mifid_file_list['creation_date'] <= '2023-03-05']
   
    list_df = []
    for u in mifid_file_list['download_link']:
        df = download_file(u)
        list_df += [df]
        
    eqt = pd.concat(list_df)
    
    check = eqt[eqt['EqtyTrnsprncyData_Id'] == 'CA49836K1021']
    
    eqt.columns
    
    mifid_file_list[['filetype', 'date', 'cfi', 'nfile']] = mifid_file_list['file_name'].str.split('_', expand=True, n=3)


    fca = get_fca_firds_file_list(creation_date_from='2023-05-01')
    
    url = 'https://data.fca.org.uk/artefacts/FIRDS/FULINS_E_20230211_01of01.zip'
    df = download_file(url)
    
    eqt_file_list = mifid_file_list[mifid_file_list['instrument_type'].str.contains('^Equity')]
    nqt_file_list = mifid_file_list[mifid_file_list['instrument_type'].str.contains('^Non')]
    
    
    url = 'http://fitrs.esma.europa.eu/fitrs/FULNCR_20230429_E_1of2.zip'
    derv = download_file(url)
    
    
    url = 'http://fitrs.esma.europa.eu/fitrs/FULNCR_20230325_D_1of5.zip'
    derv = download_file(url)
        
    list_isin2= ["PLPGER000010","PLKRK0000010","PLTAURN00011","PLKETY000011",
    "PLPZU0000011","PLDINPL00011","PLXTRDM00011","PLLPP0000011","PLBMECH00012","PLBRE0000012","PLCFRPT00013","PLBZ00000044",
    "PLJSW0000015","PLALIOR00045","PLPKO0000016","PLBIG0000016","PLCCC0000016","PLLWBGD00016","PLPEKAO00016","NL0015000AU7",
    "PLTLKPL00017"]
    
    url2 = 'http://fitrs.esma.europa.eu/fitrs/FULECR_20230325_E_1of1.zip'
    url2= 'http://fitrs.esma.europa.eu/fitrs/DLTECR_20230328_1of1.zip'
    data = download_file(url2)
    
    df = data[data['Id'].isin(list_isin2)]
    df = df[df['Mthdlgy'] == 'YEAR']
    df = df[df['FrDt'] >= '2022-01-01']
    
    url3 = 'http://fitrs.esma.europa.eu/fitrs/FULNCR_20230325_E_1of2.zip'
    FULNCR_20230325_E_1of2 = download_file(url3)    
    df3 = FULNCR_20230325_E_1of2[FULNCR_20230325_E_1of2['ISIN'].isin(list_isin2)]
    
    url4 = 'http://fitrs.esma.europa.eu/fitrs/FULNCR_20230325_E_2of2.zip'
    FULNCR_20230325_E_2of2 = download_file(url4)
    df4 = FULNCR_20230325_E_2of2[FULNCR_20230325_E_2of2['ISIN'].isin(list_isin2)]
    
    dvcap_file = 'http://dvcap.esma.europa.eu/DVCRES_20220207.zip'
    dvcap = download_file(dvcap_file)
        
    firds_file = 'http://firds.esma.europa.eu/firds/FULINS_C_20221029_01of01.zip'
    firds = download_file(firds_file)
    
    from pathlib import Path
    firds.to_csv(Path.home() / 'Desktop' / 'FULINS_example.csv')
    
    url = 'http://firds.esma.europa.eu/firds/FULINS_E_20230325_01of02.zip'
    df = download_file(url)
    
    df.columns
    
    df0 = df[:10000]
    
    
    mifid_file_list = get_mifid_file_list('fitrs')
        
    mifid_file_list[['filetype', 'date', 'cfi', 'nfile']] = mifid_file_list['file_name'].str.split('_', expand=True, n=3)

    nqt_file_list = mifid_file_list[mifid_file_list['instrument_type'].str.contains('^Non')]
    nqt_file_list = nqt_file_list[nqt_file_list['file_type'] == 'FullSubclass']
    
    
    df = download_file('http://fitrs.esma.europa.eu/fitrs/FULNCR_20230429_NYAR_EQD_7of8.zip')
    df2 = df[df['CritVal'] == 'EQD02']
    
    list_file_type = list(nqt_file_list.file_type.unique())


    from pathlib import Path
    import pandas as pd
     
    path = Path('M:/%5C/temp/hleclerc/equity_yearly_calculations')
    
    df.to_csv(path / 'xwar_21isins_yearly_calc_correction.csv')


    # lis threshold check
    mifid_file_list = get_mifid_file_list('fitrs')     
    mifid_file_list = mifid_file_list[mifid_file_list['file_type'] == 'Full']
    eqt_file_list = mifid_file_list[mifid_file_list['instrument_type'].str.contains('^Equity')]
    
    from pyfid import *
    calc = get_data_filter(table= "EQT_CALCULATION_RESULTS",
                           update=True,
                           filters=[("ISIN", 'NL0015001FS8'),
                                    ("LATEST_CALCULATION_FLAG", "T"),
                                    ('METHODOLOGY', "FFWK")])

# args = [r100]

# length = len(range(1, len(r100)))
# Nprocesses = min(max(math.ceil(length / 100), 2),
#                  multiprocessing.cpu_count())

# with multiprocessing.Pool(
#         initializer=func_settings, initargs=(args,), processes=Nprocesses♦
#     ) as pool:
#         if desc is None:
#             list_output = list(
#                 tqdm.tqdm(
#                     pool.imap(func, irange),
#                     total=length
#                 )
#             )
#         else:
#             list_output = list(
#                 tqdm.tqdm(
#                     pool.imap(func, irange),
#                     total=length,
#                     desc=desc,
#                 )
#             )


    #r1000 = root[1][0][0][0]
    #r1001 = root[1][0][0][1]
    #r10010 = root[1][0][0][1][0]
    
    #r100102  = root[1][0][0][1][2]
    #r100103  = root[1][0][0][1][3]
    
    #r10010.tag
    #r10010.text
    
    #r = r100102
    #r = r100103
    
    #len(r1001)
    
    #extract_data(r100102)
    #extract_data(r100103)
    #extract_data(r1001)
    #extract_data(r1000)
    
    
    #def etree_to_dict(t):
    #    d = {t.tag : map(etree_to_dict, t.iterchildren())}
    #    d.update(('@' + k, v) for k, v in t.attrib.iteritems())
    #    d['text'] = t.text
    #    return d
    
   # ddict = etree_to_dict(root)
   
   
    
    
