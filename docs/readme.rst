.. role:: raw-html-m2r(raw)
   :format: html

.. role:: raw-html(raw)
    :format: html

esma_data_py
======================================================================================================

.. image:: https://github.com/hadrilec/esma_data_py/actions/workflows/pkgTests.yml/badge.svg
   :target: https://github.com/hadrilec/esma_data_py/actions
   :alt: Build Status

.. image:: https://app.codecov.io/gh/hadrilec/esma_data_py/branch/master/graph/badge.svg?token=TO96FMWRHK
   :target: https://codecov.io/gh/hadrilec/esma_data_py?branch=master
   :alt: Codecov test coverage

.. image:: https://readthedocs.org/projects/esma-data-py/badge/?version=latest
   :target: https://pynsee.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue.svg
   :target: https://www.python.org/
   :alt: Python versions

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://pypi.org/project/black/
   :alt: Code formatting

.. image:: https://img.shields.io/badge/license-EUPL-blue
   :target: https://img.shields.io/badge/license-EUPL-blue
   :alt: License
   
:raw-html:`<br />`
   
The *esma_data_py* package provides a robust toolkit designed to streamline the process of searching for and downloading data from the ESMA (European Securities and Markets Authority) register. This Python package facilitates easy access to regulatory data, aiming to support financial analysts, researchers, and developers who require reliable and up-to-date information from ESMA.

This package is a contribution to reproducible research and public data transparency. 

Key features
-------
* **MIFID Data** : You can use, *get_mifid_file_list* to fetch a list of MIFID files from specified ESMA databases filtering by creation or publication date

* **FIRDS Data** : You can retreive data both with the *get_last_full_files*, applying filters by instrument type and optionally by CFI codes and ISINs or with the *get_fca_firds_file_list* by retrieveing a list of FCA files

* **SSR Data** : You can retreive SSR data with the *get_ssr_exempted_shares*, optionally filtering the results to include only records relevant to the current date.

Getting Started
-------

To get started with *esma_data_py*, you can install the package via git:

.. code-block:: python

   # Clone the repository
   git clone https://github.com/hadrilec/esma_data_py.git

   # Change directory to esma_data_py
   cd esma_data_py

   # Install the package with all optional dependencies
   pip install .[full]


Documentation
-------

Detailed documentation for all relevant functions and modules is available in the docs directory fo this repository, which includes guides and examples to help you integrate esma_data_py into your projects effictively.


Support
-------

Feel free to open an issue with any question about this package using https://github.com/hadrilec/esma_data_py/issues Github repository.

Contributing
------------

All contributions, whatever their forms, are welcome.
