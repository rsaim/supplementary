"""
Parse results of DTU into the following json structure.

{
    "roll_no" : {
        "branch": "<branchname>",
        "name"  : "<student full name>",
        "results" : {
            "<number of sem>" : {
                {
                    "marks" : {
                        "<name of subject>": "<marks>",
                        "<name of subject>": "<marks>",
                        # ...
                    }
                    "release_date": "<date>",
                    "examination_date": "<date>"
                }
            }
        }
    }
}
"""
from __future__ import absolute_import, division

from   os.path                  import realpath
import sys

import pandas as pd
import pdfplumber
import tabula
from   timeit                   import default_timer as timer

from   loguru                   import logger as log
log.remove()
log.add(sys.stdout, level="INFO")


SANITIZED_NAME_MAP = {
    'Sr.No.  Name'   : 'name',
    'Sr.No. Name'   : 'name',
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
    # if df['name'].lower():
    #     import ipdb; ipdb.set_trace()
    df = df[pd.notnull(df['name']) | pd.notnull(df['papers_failed'])].copy()

    """
    Papers failed in multiple lines must be added to the row above.
                         name     roll_no MC-301 MC-302 MC-303 MC-304 MC-305 MC-306 MC-307 MC-308 MC-309    TC    SPI    papers_failed
    0       25 KISHAN  ASHIYA  2K12/MC/29     75     63     30     69     68     76     84     72    162  26.0  62.93           MC-303
    ...
    22        45 RAHUL  MEENA  2K12/MC/49     12     23     25     40     28     62     71     64    100  14.0  25.13  MC-305MC-303MC-
    23                    NaN         NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN   NaN    NaN        302MC-301
    25       47 RAJAT  CHOPRA  2K12/MC/51     93     91     88     91     96     78     94     77    184  30.0  90.07              NaN
    """
    # TODO: Check the behavior when papers_failed span more than 2 lines.
    drop_indices = []
    for row_num, row in df.iterrows():
        row_vals = row.values.tolist()
        row_vals.remove(row['papers_failed'])
        if not any([pd.notnull(val) for val in row_vals]) and row_num > 0:
            drop_indices.append(row_num)
            df.loc[row_num - 1, 'papers_failed'] = df.loc[row_num - 1, 'papers_failed'] + " " + row['papers_failed']
    if drop_indices:
        df = df.drop(drop_indices)
        df.reset_index(drop=True, inplace=True)

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


def parse_pdfplumber_page(text):
    res = {}
    lines = text.split()
    return res

def parse_pdf_to_dfs(filename):
    """
    Parse a dtu result pdf

    :param filename:
        A path to the pdf file
    :return:
        A list of `pandas.DataFrame`

        results = {
            "roll_no" : {
                "program": "<btech, mtech, etc>",
                "branch": "<branchname>",
                "name": "<student full name>",
                "results" : {
                    "<number of sem>" : {
                        "pdf_info" : {
                            "filename": "<name>",
                            "page_no": "<val>"
                        }
                        "release_date": "<date>",
                        "examination_date": "<date>",
                        "total_credits": "<val>",
                        "spi": "<val>",
                        "papers_failed": ["sub1_code", "sub2_code", ...],
                        "marks" : {
                            "<subject1_code>": "<marks>",
                            "<subject2_code>": "<marks>",
                            # ...
                        }
                    }
                }
            },
            # ...
        }
    """
    filename = realpath(filename)
    log.info(f"Parsing {filename}")

    # Use tabula to parse the tables in the pdf
    start_ts = timer()
    # This will be a list of `pandas.DataFrame`
    pages_df = tabula.read_pdf(filename, pages='all')
    log.info(f"Took {timer() - start_ts} to parse {filename}")

    # Use pdfplumber to parse metadata like sem
    pdfplumber_pages = pdfplumber.open(filename)

    log.info(f"Found {len(pages_df)} pages in {filename}")
    sanitized_dfs = []
    for num, page_df in enumerate(pages_df):
        log.debug(f"Sanitizing page no {num}...")
        sanitized_dfs.append(sanitize_df(page_df))
        log.debug(f"Getting metadata using pdfplumber for page no {num}...")
        metadata = parse_pdfplumber_page(pdfplumber_pages[num])
    log.info(f"Parsed tables in {filename}")

    return sanitized_dfs






