# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 20:37:00 2023

@author: hleclerc
"""

from pathlib import Path
import os
import hashlib
import functools
import warnings
import pandas as pd

def _hash(string):

    h = hashlib.new("md5")
    h.update(string.encode("utf-8"))
    return h.hexdigest()
    
@functools.lru_cache(maxsize=None)
def _warning_cached_data(file):
    print(
        "Previously saved data used:\n{}\nSet update=True to get the most up-to-date data".format(
            file
        )
    )

@functools.lru_cache(maxsize=None)
def _create_folder(folder="data"):    

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
            file_name = str(data_folder) + "/" + _hash(''.join(string_file_arg)) + ".csv"
                                    
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