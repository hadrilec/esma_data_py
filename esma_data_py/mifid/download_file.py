# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 20:37:00 2023

@author: hleclerc
"""

import urllib.request as request
import shutil
import os
from contextlib import closing
import zipfile
import re
from collections import ChainMap
import tempfile
import multiprocessing
import xml.etree.ElementTree as ET
import pandas as pd
import tqdm
import requests
from tqdm import trange
from esma_data_py.utils.utils import save_df


def set_global_args(args):
    global url_list
    url_list = args[0]


def get_tag_clean(tag):
    pattern2remove = re.findall("{.*}", tag)[0]
    tag = tag.replace(pattern2remove, "")
    return tag


def extract_data(r, rtag=None, clean=True):
    r2 = r
    if len(r) == 0:
        if clean is True:
            tag = get_tag_clean(r.tag)

            if rtag is not None:
                rtag = get_tag_clean(rtag)
                tag = rtag + "_" + tag
        else:
            tag = r.tag

        ddict = {tag: r.text}
        return ddict

    elif get_tag_clean(r2.tag) == "DerivSubClss":
        ddict = [{get_tag_clean(r2[0].tag): r2[0].text}]
        for i in range(1, len(r2)):
            ddict += [{r2[i][0].text: r2[i][1].text}]
        ddict = dict(ChainMap(*ddict))

        return ddict
    else:
        list_ddict = [
            extract_data(r[i], clean=clean, rtag=r.tag) for i in range(len(r))
        ]
        ddict = dict(ChainMap(*list_ddict))
        return ddict


@save_df()
def download_one_file(url, update=False, silent=False):
    list_string_split = url.split("/")
    filename = list_string_split[len(list_string_split) - 1]
    list_string_split_filename = filename.split(".")
    filename_clean = list_string_split_filename[0]

    temp = tempfile.mkdtemp()
    dataDir = str(temp) + "/" + filename_clean
    if not os.path.exists(dataDir):
        os.mkdir(dataDir)

    file_dwn = dataDir + "/" + "file_" + filename_clean

    with closing(request.urlopen(url, timeout=None)) as r:
        with open(file_dwn, "wb") as f:
            shutil.copyfileobj(r, f)

    with zipfile.ZipFile(file_dwn, "r") as zip_ref:
        zip_ref.extractall(dataDir)

    file_xml = [dataDir + "/" + f for f in os.listdir(dataDir) if re.search("xml$", f)][
        0
    ]

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

    data = data.assign(url=url)

    return data


def _download_one_file(u, update=False):
    url = url_list[u]
    df = download_one_file(url, silent=True)
    return df


def download_file(url_list, update=False, multiprocess=True, cpu=10):
    """
    Downloads data from a list of URLs and consolidates the results into a single DataFrame.

    Parameters:
    - url_list (list or str): A single URL or a list of URLs from which data is to be downloaded. If a single URL is provided,
                              it is converted into a list.
    - update (bool): If True, allows for the updating of files during the download process. Defaults to False.
    - multiprocess (bool): Controls whether to use multiprocessing to parallelize the download process.
    - cpu (int): Specifies the maximum number of CPU cores to use for multiprocessing. Defaults to 10.

    Returns:
    - pd.DataFrame: A DataFrame containing all the data fetched from the provided URLs.

    Example:
    --------
    # Example of downloading data from multiple URLs without multiprocessing:
    url_list = ['http://example.com/file1.csv', 'http://example.com/file2.csv']
    result_df = download_file(url_list, update=False)
    """

    # multiprocess = False

    if isinstance(url_list, str):
        url_list = [url_list]

    list_df = []

    i = 1

    #  if not multiprocess:
        for url in url_list:
            print(f"{i}/{len(url_list)} files")

            data = download_one_file(url, update=update)

            list_df += [data]

            i += 1

        data_final = pd.concat(list_df)

    # else:
    #     args = [url_list]

    #     length = len(url_list)
    #     irange = range(length)

    #     Nprocesses = min(cpu, multiprocessing.cpu_count())

    #     with multiprocessing.Pool(
    #         initializer=set_global_args, initargs=(args,), processes=Nprocesses
    #     ) as pool:
    #         list_output = list(
    #             tqdm.tqdm(
    #                 pool.imap(_download_one_file, irange),
    #                 total=length,
    #                 desc="Download",
    #             )
    #         )

    #     data_final = pd.concat(list_output)

    return data_final
