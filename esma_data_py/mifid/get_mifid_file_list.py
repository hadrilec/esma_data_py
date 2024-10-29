# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 20:37:00 2023

@author: hleclerc
"""

import hashlib
import tempfile
import datetime
import xml.etree.ElementTree as ET
import pandas as pd
import requests
from esma_data_py.utils.utils import _hash


def get_mifid_file_list(
    db_list=["fitrs", "firds", "dvcap"],
    creation_date_from="2017-01-01",
    creation_date_to=None,
    limit="100000",
):
    """
    Fetches a list of MIFID files from specified ESMA databases filtered by creation or publication dates.

    Args:
      db_list (list or str): List of database names to fetch files from. Valid databases are 'fitrs', 'firds', and 'dvcap'. Defaults to ['fitrs', 'firds', 'dvcap']. If a single string is provided, it is converted into a list.

      creation_date_from (str): Start date for filtering files, in the format 'YYYY-MM-DD'. Defaults to '2017-01-01'.

      creation_date_to (str, optional): End date for filtering files. Defaults to today's date.

      limit (str): Maximum number of records to fetch from each database. Defaults to '100000'.

    Returns:
      pd.DataFrame: A DataFrame aggregating the records from all specified databases, containing file details.

    Examples:
      >>> # Fetch MIFID files from 'fitrs' and 'firds' databases from January 1, 2017 to the current date
      >>> files_df = get_mifid_file_list(db_list=['fitrs', 'firds'], creation_date_from='2017-01-01')
    """

    if type(db_list) == str:
        db_list = [db_list]

    limit = str(limit)

    if creation_date_to is None:
        creation_date_to = str(datetime.datetime.today().strftime("%Y-%m-%d"))

    list_data = []

    for db in db_list:
        if db == "firds":
            date_col = "publication_date"
        else:
            date_col = "creation_date"

        q = (
            f"https://registers.esma.europa.eu/solr/esma_registers_{db}_files/select?q=*"
            f"&fq={date_col}:%5B{creation_date_from}T00:00:00Z+TO+{creation_date_to}T23:59:59Z%5D&wt=xml&indent=true&start=0&rows={limit}"
        )

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
        list_data.append(data)

    data_final = pd.concat(list_data)

    return data_final
