# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 15:16:30 2024

@author: gdegl'innocenti
"""

import unittest
import os
import pandas as pd
import sys
import functools

from esma_data_py.ssr.get_ssr_exempted_shares import get_ssr_exempted_shares # Theoretically complete
from esma_data_py.mifid.get_mifid_file_list import get_mifid_file_list # Theoretically complete
from esma_data_py.mifid.download_file import download_file, download_one_file, _download_one_file # To check
from esma_data_py.mifid.get_fca_firds_file_list import get_fca_firds_file_list # Theoreticaly complete
from esma_data_py.mifid.get_last_full_files import get_last_full_files

class MyTests(unittest.TestCase):

    version = (sys.version_info[0] == 3) & (sys.version_info[1] == 10)

    if version == True:
    
        def test_get_last_full_files(self):
            test  = get_ssr_exempted_shares()
            self.assertTrue(isinstance(test, pd.DataFrame))
        
        # THIS SHOULD BE RUNNED BY get_last_full_files
        # def test_get_mifid_file_list_1(self):
        #         test = get_mifid_file_list('firds')
        #         self.assertTrue(isinstance(test, pd.DataFrame))

        def test_get_mifid_file_list_2(self):    
                test = get_mifid_file_list('dvcap')
                self.assertTrue(isinstance(test, pd.DataFrame))  
    
        def test_download_file(self):
            test = download_file("http://fitrs.esma.europa.eu/fitrs/FULNCR_20240622_D_2of6.zip", update = True)
            self.assertTrue(isinstance(test, pd.DataFrame))
        
        def test_download_one_file(self):
                test = download_one_file("http://fitrs.esma.europa.eu/fitrs/FULNCR_20240622_D_2of6.zip", update=True)
                self.assertTrue(isinstance(test, pd.DataFrame))
        
        def test_get_fca_firds_file_list_1(self):
                test = get_fca_firds_file_list('firds')
                self.assertTrue(isinstance(test, pd.DataFrame))
            
        def test_get_fca_firds_file_list_2(self):
                test = get_fca_firds_file_list('dvcap')
                self.assertTrue(isinstance(test, pd.DataFrame))
            
        def test_get_last_full_files_1(self):
                test = get_last_full_files(eqt = True, cfi = 'C')
                self.assertTrue(isinstance(test, pd.DataFrame))
            
        def test_get_last_full_files_2(self):
                test = get_last_full_files(eqt = False, cfi = None)
                self.assertTrue(isinstance(test, pd.DataFrame)) 
                
        def test_get_last_full_files_3(self):
                self.assertRaises(ValueError, get_last_full_files, cfi='X')


          
                        
if __name__ == '__main__':
    unittest.main()
