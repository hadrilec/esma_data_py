# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 20:37:00 2023

@author: hleclerc
"""

import hashlib
import tempfile
import datetime
import pandas as pd
import requests
from esma_data_py.utils.utils import _hash


def get_fca_firds_file_list(
    db_list=["firds"],
    creation_date_from="2017-01-01",
    creation_date_to=None,
    limit="10000",
):
    """
    Retrieves a list of FCA FIRDS files from the specified API based on the given parameters.

    Args:
      db_list (list or str): A list of database identifiers where 'firds' refers to the FIRDS database. Defaults to ['firds']. If a single database identifier is provided as a string, it is converted to a list.

      creation_date_from (str): The start date for filtering files by their creation or publication date. The date should be in the 'YYYY-MM-DD' format. Defaults to '2017-01-01'.

      creation_date_to (str): The end date for filtering files. Defaults to the current date. If None, it uses today's date as the endpoint for the date filter.

      limit (str): The maximum number of records to fetch. Defaults to '10000'.

    Returns:
      pd.DataFrame: A DataFrame containing the records of files fetched based on the specified filters.

    Examples:
      >>> # Get a list of FIRDS files from January 1, 2017, to today, limited to the first 10000 records
      >>> firds_files = get_fca_firds_file_list(db_list = 'dvcap')
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
            f"https://api.data.fca.org.uk/fca_data_firds_files?q="
            f"((file_type:FULINS)%20AND%20({date_col}:[{creation_date_from}%20TO%20{creation_date_to}]))&pretty=true&from=0&size={limit}"
        )

        dirpath = tempfile.mkdtemp()
        raw_data_file = dirpath + "\\" + _hash(q)

        req = requests.get(q)

        root = req.json()

        data = [dd["_source"] for dd in root["hits"]["hits"]]

        data = pd.DataFrame.from_records(data)
        list_data += [data]

    data_final = pd.concat(list_data)

    return data_final
