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

os.chdir(r"M:\%5C\temp\gdeglinnocenti\pkg\esma_data_py")

from esma_data_py.ssr.get_ssr_exempted_shares import get_ssr_exempted_shares
from esma_data_py.mifid.get_mifid_file_list import get_mifid_file_list
from esma_data_py.mifid.download_file import download_file


class MyTests(unittest.TestCase):

    version = (sys.version_info[0] == 3) & (sys.version_info[1] == 10)

    if version:
    
        def test_get_last_full_files(self):
            test  = get_ssr_exempted_shares()
            self.assertTrue(isinstance(test, pd.DataFrame)
        
        def test_get_mifid_file_list(self):
            test = get_mifid_file_list('firds')
            self.assertTrue(isinstance(test, pd.DataFrame))
    
        def test_download_file(self):
            test = download_file("http://fitrs.esma.europa.eu/fitrs/FULNCR_20240622_D_2of6.zip")
            self.assertTrue(isinstance(test, pd.DataFrame))
            
                        
if __name__ == '__main__':
    unittest.main()