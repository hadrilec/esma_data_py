# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 20:37:00 2023

@author: hleclerc
"""

import pandas as pd
from esma_data_py.mifid.get_mifid_file_list import get_mifid_file_list
from esma_data_py.mifid.download_file import download_file

    
def get_last_full_files(isin=None, cfi=None, eqt=True):
    """
    Retrieves the latest full files from the 'fitrs' dataset, filtered by instrument type and optionally by CFI codes and ISINs.

    Args:
        isin (str or list of str, optional): ISIN(s) to filter the files. If provided, only files containing these ISINs are included.
        cfi (str or list of str, optional): CFI code(s) to further filter the files. Must be one of 'C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S'.
        eqt (bool): Determines if only equity instruments ('True') or non-equity instruments ('False') should be considered. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the concatenated data from all files that meet the specified criteria.

    Examples:
        >>> # Example to get the latest full files for equity instruments with specific CFI codes:
        >>> files_df = get_last_full_files(cfi=['C', 'D'], eqt=True)
        >>> # Example to get the latest full files for non-equity instruments:
        >>> files_df = get_last_full_files(eqt=False)
    """
    
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
