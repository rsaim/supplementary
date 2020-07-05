import sys
from os.path import realpath

import tabula
import numpy as np
import pandas as pd
import os
from timeit import default_timer as timer

from loguru import logger as log
log.remove()
log.add(sys.stdout, level="INFO")


SANITIZED_NAME_MAP = {
    'Sr.No.  Name'   : 'name',
    'Roll No.'      : 'roll_no',
    'Unnamed: 1'    : 'papers_failed'
}


def sanitize_df(df):
    """
    Parse and sanitize a DatFrame to have correct values.

    NOTE: this utility is hard coded to sanitize results from tabula.read_pdf

    :param df:
        pandas.DataFrame from tabula.read_pdf
    :return:
        Sanitized pandas.DataFrame
    """
    sanitized_names = []
    for x in df.columns:
        new_name = SANITIZED_NAME_MAP.get(x, None)
        if new_name:
            log.debug(f"Sanitizing {x!r} to {new_name!r}")
            sanitized_names.append(new_name)
        else:
            sanitized_names.append(x)
    log.debug(f"Sanitized names to {sanitized_names}")
    df.columns = sanitized_names

    # Drop rows which have nan in name column.
    df = df[pd.notnull(df['name'])].copy()

    log.debug("Deleting column 'Unnamed: 0'.")
    del df['Unnamed: 0']

    """
    Drop first row. Assert that we are not dropping a random row. First row is expected to be read as:

    0                      NaN  Max. Marks / Credits  100/4  100/4  100/4  100/4  100/4  100/2  100/2  100/2  200/4  30    NaN  Papers Failed
    """
    if 'Max. Marks / Credits' in df.iloc[0].values:
        log.debug(f"Dropping first row {df.iloc[0].values.tolist()!r}")
        df = df.drop(0)
    df.reset_index(drop=True, inplace=True)

    # Remove number and space from the begining of names. Also replace double spaces with a single space.
    df['name'] = df['name'].apply(lambda x: x.strip("0123456789 ").replace("  ", " "))

    """
    A long name that spans multiple lines has all columns except name as NaN. Append this name to
    the name in the previous row. For instance 'KOMARAVOLU NITIN BHARDWAJ' is a single name below.
    
                     name     roll_no MC-301 MC-302 MC-303 MC-304 MC-305 MC-306 MC-307 MC-308 MC-309    TC    SPI     paper_failed
    0       KISHAN ASHIYA  2K12/MC/29     75     63     30     69     68     76     84     72    162  26.0  62.93           MC-303
    1    KOMARAVOLU NITIN  2K12/MC/30     86     69     82     84     89     75     88     87    185  30.0  83.67              NaN
    2            BHARDWAJ         NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN   NaN    NaN              NaN
    3       KRISHNA KUMAR  2K12/MC/31     47     53     48     49     45     60     88     70     95  30.0  53.13              NaN
    ...
    """
    drop_indices = []
    for row_num, row in df.iterrows():
        row_vals = row.values.tolist()
        row_vals.remove(row['name'])
        if not any([pd.notnull(val) for val in row_vals]) and row_num > 0:
            drop_indices.append(row_num)
            df.loc[row_num - 1, 'name'] = df.loc[row_num - 1, 'name'] + " " + row['name']
    if drop_indices:
        df = df.drop(drop_indices)
        df.reset_index(drop=True, inplace=True)
    return df


def parse_pdf(filename):
    """
    Parse a dtu result pdf

    :param filename:
        A path to the pdf file
    :return:
        A list of `pandas.DataFrame`
    """
    filename = realpath(filename)
    log.info(f"Parsing {filename}")
    # Use tabula to parse the pdf
    start_ts = timer()
    # This will be a list of `pandas.DataFrame`
    pages_df = tabula.read_pdf(filename, pages='all')
    log.info(f"Took {timer() - start_ts} to parse {filename}")

    log.info(f"Found {len(pages_df)} pages in {filename}")
    sanitized_dfs = []
    for num, page_df in enumerate(pages_df):
        log.debug(f"Sanitizing page no {num}...")
        sanitized_dfs.append(sanitize_df(page_df))
    log.info(f"Parsed {filename}")
    return sanitized_dfs






