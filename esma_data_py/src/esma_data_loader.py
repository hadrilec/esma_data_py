from datetime import datetime
from typing import List, Optional
import requests
import tempfile
import xml.etree.ElementTree as ET
from requests.models import Response
import pandas as pd
import os
from bs4 import BeautifulSoup
import zipfile
from tqdm import tqdm
import src.utils as u


class EsmaDataLoader:

    def __init__(self, 
                 creation_date_from: str = '2017-01-01',
                 creation_date_to: Optional[str] = None, 
                 limit: str = '10000'):

        self.creation_date_from = creation_date_from
        self.creation_date_to = creation_date_to
        self.limit = limit

        if not self.creation_date_to:
            self.creation_date_to = str(datetime.today().strftime("%Y-%m-%d"))

        self.query_url = u.QueryUrl()
        self.__utils = u.Utils()
        self.__logger = self.__utils.set_logger(name='EsmaDataLoader')


    def load_mifid_file_list(self, datasets: List[str] = ['dvcap', 'fitrs', 'firds']):

        try:
            datasets = [u.Dataset(dataset).value for dataset in datasets]
        except ValueError as ve:
            self.__logger.error(f'Error: {ve}') 

        files_dfs = []
        self.__logger.info(f'Loading {len(datasets)} datasets')
        
        for dataset in datasets:

            self.__logger.info(f'Loading {dataset} dataset')
            files_dfs.append(self.__get_files_single_df_mifid(dataset))

        self.__logger.info(f'Process done!')
        return pd.concat(files_dfs)
    

    def load_fca_firds_file_list(self):

        query_fca_firds = self.query_url.fca_firds.format(creation_date_from=self.creation_date_from, 
                                                      creation_date_to=self.creation_date_to, 
                                                      limit=self.limit)
        
        self.__logger.info(f'Requesting FCA FIRDS files')
        request = requests.get(query_fca_firds)

        if request.status_code == 200:
            self.__logger.info(f'Request successful, parsing response')
            root = request.json()
            list_of_dicts = [dd["_source"] for dd in root["hits"]["hits"]]
        else:
            self.__logger.error(f'Request failed, status code {request.status_code}')

        files = pd.DataFrame.from_records(list_of_dicts)
        self.__logger.info(f'Process done!')

        return files  


    def load_latest_files(self, 
                          file_type: str = 'Full', 
                          vcap: bool = False,
                          isin: Optional[List[str]] = [], 
                          cfi: str = 'E', 
                          eqt=True,
                          save_locally: bool = False,
                          update: bool = False):

        try:
            cfi = u.Cfi(cfi).value
        except Exception as e:
            self.__logger.error(f'Error: {e}')
            return

        if vcap:
            mifid_file_list = self.__get_latest_vcap_files()
        else:
            mifid_file_list = self.__get_latest_fitrs_files(file_type=file_type, eqt=eqt, cfi=cfi)
        
        if isin:
            self.__logger.info(f'Filtering records for the given ISINs')
            if len(mifid_file_list_isin := mifid_file_list[mifid_file_list["Id"].isin(isin)]) > 0:
                mifid_file_list = mifid_file_list_isin
            else:
                self.__logger.warning(f'No record found for the given ISINs, keeping default')

        list_urls = mifid_file_list["download_link"].unique()

        list_dwndl_dfs = []
        self.__logger.info(f'Downloading {len(list_urls)} files')
        
        if save_locally:
            self.__logger.info(f'Saving files locally')
        else:
            self.__logger.info(f'Files will not be saved locally, flag the parameter save_locally=True to save them') 
        
        for url in list_urls:
            self.__logger.info(f'Downloading and parsing {url}')
            list_dwndl_dfs.append(self.__utils.download_and_parse_file(url, save=save_locally, update=update))
            
        self.__logger.info('Process done!')
        return pd.concat(list_dwndl_dfs)


    
    def load_ssr_exempted_shares(self, today: bool = True):

        list_countries = [ "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", 
                           "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", 
                           "NL", "PL", "PT", "RO", "SE", "SI", "SK", "NO", "GB"]
        
        list_dfs = []
        
        self.__logger.info(f'Requesting SSR Exempted Shares')
        with tqdm(total=len(list_countries), position=0, leave=True) as pbar:
            for country in list_countries:
                
                pbar.set_description(f"Processing request for {country}")
                pbar.update(1)
                
                country_query = self.query_url.ssr.format(country=country)
                request = requests.get(country_query)
                if request.status_code == 200:
                    json_request = request.json()["response"]["docs"]
                    df = pd.DataFrame(json_request)
                    list_dfs.append(df)
                else:
                    self.warning(f'Request failed, status code {request.status_code} for country {country}')
        
        delivery_df = pd.concat(list_dfs) 

        if not today: 
            self.__logger.info(f'Process done!')
            return delivery_df

        self.__logger.info(f'Filtering for today\'s date')
        today_date = datetime.today().strftime("%Y-%m-%d")
        
        filtered_data = delivery_df.query('shs_modificationBDate > @today_date and shs_exemptionStartDate <= @today_date')

        duplicates = filtered_data[filtered_data.duplicated(subset='shs_isin', keep=False)]
        duplicates = duplicates[duplicates['shs_modificationDateStr'] <= today_date]
        
        non_duplicates = filtered_data[~filtered_data['shs_isin'].isin(duplicates['shs_isin'])]
        
        final_data = pd.concat([duplicates, non_duplicates]).reset_index(drop=True)
        self.__logger.info(f'Process done!')
        
        return final_data       


    def __get_files_single_df_mifid(self, dataset: str):

        if dataset == u.Dataset.FIRDS.value:

            query_mifid = self.query_url.mifid.format(db=dataset, 
                                                  date_column='publication_date', 
                                                  creation_date_from=self.creation_date_from, 
                                                  creation_date_to=self.creation_date_to, 
                                                  limit=self.limit)
        else:
            query_mifid = self.query_url.mifid.format(db=dataset,
                                                date_column='creation_date',
                                                creation_date_from=self.creation_date_from,
                                                creation_date_to=self.creation_date_to,
                                                limit=self.limit)
        
        self.__logger.info(f'Requesting {dataset} files')
        request = requests.get(query_mifid)
        
        if request.status_code == 200:
            self.__logger.info(f'Request successful, parsing response')
            files = self.__utils.parse_request_to_df(request)
        else:
            self.__logger.error(f'Request failed, status code {request.status_code}')
            return pd.DataFrame()

        return files  

        
    def __get_latest_vcap_files(self):
        
        pattern_extract_vcap =  pattern = r'(?P<filetype>[A-Za-z]+)_(?P<date>\d{8})'
        
        mifid_file_list = self.__get_files_single_df_mifid(u.Dataset.DVCAP.value)
        file_name_explosion = mifid_file_list.file_name.str.extract(pattern_extract_vcap)
        mifid_file_list = pd.concat([mifid_file_list, file_name_explosion], axis=1)
        max_date = max(mifid_file_list.date)

        return mifid_file_list.loc[lambda x: x.date == max_date]
    
    def __get_latest_fitrs_files(self, file_type: str, cfi: str, eqt: bool):
        
        pattern_extract_ft =  r'(?P<filetype>[A-Za-z]+)_(?P<date>\d{8})(?:_(?P<cfi>[A-Za-z]+))?_(?P<nfile>\d+of\d+)\.zip'
        
        mifid_file_list = self.__get_files_single_df_mifid(u.Dataset.FITRS.value)
        mifid_file_list = mifid_file_list.loc[lambda x: x.file_type == file_type]
        file_name_explosion = mifid_file_list.file_name.str.extract(pattern_extract_ft)
        mifid_file_list = pd.concat([mifid_file_list, file_name_explosion], axis=1)
        mifid_file_list = mifid_file_list.loc[lambda x: x.cfi == cfi]
        max_date = mifid_file_list.groupby('file_type').agg({'date': 'max'}).date.iloc[0]

        if eqt:
            mifid_file_list = mifid_file_list.loc[lambda x: x.instrument_type == "Equity Instruments"]
        else:
            mifid_file_list = mifid_file_list.loc[lambda x: x.instrument_type == "Non-Equity Instruments"]

        return mifid_file_list.loc[lambda x: x.date == max_date]
     

if __name__ == '__main__':

    dl = EsmaDataLoader()
    dl.load_latest_files(save_locally=True)
   