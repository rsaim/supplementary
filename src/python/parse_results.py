"""
Parse results of DTU into the following json structure.

{
    "rollno" : {
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

from   concurrent.futures       import as_completed
from   concurrent.futures.process \
                                import ProcessPoolExecutor
import json
import os
from   os.path                  import basename, dirname, realpath
import re
import sys

import pandas as pd
import pdfplumber
import psutil
import tabula
from   timeit                   import default_timer as timer

from   loguru                   import logger as log
# fmt = "[{time}|{function:}|{line}|{level}] {message}"

sys.path.append(realpath(dirname(__file__)))
from   utils                    import HideUnderlyingStderrCtx, get_filepaths

log.remove()
log.add(sys.stdout, level="INFO")


SANITIZED_NAME_MAP = {
    'sr.no.name'   : 'name',
    'rollno.'      : 'rollno',
    'unnamed:1'    : 'papers_failed'
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
        new_name = SANITIZED_NAME_MAP.get(x.lower().replace(" ", ""), None)
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
                         name     rollno MC-301 MC-302 MC-303 MC-304 MC-305 MC-306 MC-307 MC-308 MC-309    TC    SPI    papers_failed
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
    df['name'] = df['name'].apply(lambda x: x.strip("0123456789 ").replace("  ", " ")
    if isinstance(x, str) else x)

    """
    A long name that spans multiple lines has all columns except name as NaN. Append this name to
    the name in the previous row. For instance 'KOMARAVOLU NITIN BHARDWAJ' is a single name below.
    
                     name     rollno MC-301 MC-302 MC-303 MC-304 MC-305 MC-306 MC-307 MC-308 MC-309    TC    SPI     paper_failed
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


def parse_metadata(filepath, page_num):
    """
    A typical page for BTech results looks as follows.
    =======================================================
    Delhi Technological University
    Visit - http://exam.dce.edu
    (Formerly Delhi College of Engineering)
    No.DTU/Results/BTECH/DEC/2014/
    Result Notification
    DEC/2014
    THE RESULT OF THE CANDIDATES WHO APPEARED IN THE FOLLOWING EXAMINATIONS HELD IN DEC-2014 IS DECLARED AS UNDER : -
    Program : Bachelor of Technology Sem : V
    Branch :  Environmental Engineering
    EN-301:WATER SUPPLY & ENVIRONMENTAL SANITATION EN-302:HEAVY METAL REMOVALS EN-303:GEOTECHNICAL ENGINEERING
    EN-304:ENVIRONMENTAL HYDRAULICS EN-305:INSTRUMENTATION EN-306:ENVIRONMENTAL HYDRAULICS LAB
    EN-307:GEOTECHNICAL ENGINEERING LAB EN-308:INSTRUMENTATION LAB EN-309:MINOR PROJECT-I /SURVEYING CAMP EVALUATION
    TC: Total Credits D: Detained A: Absent RL: Result Later RW: Result Withdrawn
    Sr.No. Name Roll No. EN-301 EN-302 EN-303 EN-304 EN-305 EN-306 EN-307 EN-308 EN-309 TC SPI
    Max. Marks / Credits 100/4 100/4 100/4 100/4 100/4 100/2 100/2 100/2 200/4 30 Papers Failed
    1 AAKRITI  MAKKER 2K12/EN/1 91 81 83 86 86 74 73 79 184 30 84.27
    2 AAYUSH  YADAV 2K12/EN/2 91 74 78 86 73 71 79 78 178 30 80.67
    3 ABHISHEK  GAUTAM 2K12/EN/3 81 70 48 67 63 65 65 75 174 30 69.13
    4 AMAN  PRIYADARSHI 2K12/EN/4 86 67 29 75 41 81 67 67 176 26 61.93 EN-303
    5 AMIT  PANDA 2K12/EN/5 93 73 66 76 75 64 79 80 164 30 76.87
    6 AMIT  YADAV 2K12/EN/6 90 74 49 74 63 66 69 76 186 30 73.13
    7 ANISH  KUKREJA 2K12/EN/7 86 75 48 77 55 79 76 68 184 30 72.60
    8 ANKIT  SIL 2K12/EN/8 79 70 30 42 62 68 62 72 172 26 58.67 EN-303
    9 ANMOL  VISHWAKARMA 2K12/EN/10 86 67 43 68 56 71 74 74 154 30 67.53
    10 ANSHUL  TYAGI 2K12/EN/11 86 60 66 73 54 79 76 68 176 30 71.80
    11 ARADHANA  GAHLAUT 2K12/EN/12 83 72 27 60 56 80 77 69 176 26 62.93 EN-303
    12 ARIHANT  KUMAR 2K12/EN/13 82 71 45 72 63 77 72 75 166 30 70.40
    13 ARUSHI  GUPTA 2K12/EN/14 97 87 72 87 82 78 82 84 176 30 84.67
    14 ATUL  KATARIA 2K12/EN/15 83 66 40 72 55 61 72 70 182 30 67.80
    15 BHARAT  SADHWANI 2K12/EN/16 81 56 40 87 51 87 71 72 162 30 68.13
    16 CHITRAKSHI   2K12/EN/17 75 71 62 78 66 67 70 72 165 30 71.87
    17 GARIMA  GARG 2K12/EN/18 94 76 77 88 69 77 78 79 170 30 80.80
    18 HIMANSHU  GUPTA 2K12/EN/19 82 56 57 64 47 66 63 69 178 30 65.87
    19 HITESH KUMAR JANGID 2K12/EN/20 66 57 56 78 55 78 68 60 148 30 65.20
    20 ISHAAN  JAIN 2K12/EN/21 91 79 74 72 65 66 72 80 180 30 77.33
    21 ISHANT  GOEL 2K12/EN/22 83 63 76 71 73 68 78 79 165 30 74.80
    22 JATIN KUMAR JAKHRA 2K12/EN/23 80 70 40 67 44 72 68 72 176 30 66.00
    23 JYOTI  BAGRANIA 2K12/EN/24 90 77 77 83 82 71 75 77 172 30 80.87
    24 KAPIL  KUMAR 2K12/EN/25 62 51 12 18 40 70 62 70 152 22 44.00 EN-304EN-303
    Any discrepancy in the result in r/o name/roll no/registration should be brought to the notice of A.R. (Acad.)/OIC B.Tech. (Eve.) within 15 days of declaration of result, in the prescribed proforma. ANY discrepancy in r/o MARKS, apply for RECHECKING.
    Date : 06/01/2015 OSD(Results)  __________________________ Deputy/Controller of Examinations:_________________________ 1/3
    =======================================================

    :param text:
    :return:
    """
    log.debug(f"Parsing metadata filepaht={filepath!r} page={page_num}...")
    pdf = pdfplumber.open(filepath)
    pages = list(pdf.pages)
    if page_num not in range(0, len(pages)):
        raise ValueError(f"{filepath!r} has {len(pages)}, passed page_num={page_num}")
    page = pages[page_num]
    text = page.extract_text()

    res = dict(
        program             = None,
        branch              = None,
        release_date        = None,
        examination_date    = None,
        notice              = None,
        semester            = None,
    )

    for line in text.split("\n"):
        if re.sub('[^0-9a-zA-Z]+', '', line).lower().startswith(("no", "resultnotification")):
            # Grab the notice number from lines like:
            #   No.DTU/Results/BTECH/DEC/2014/
            #   Result Notification No.DTU/Results/BTECH/DEC/2016/
            res["notice"] = line
        elif line.startswith("THE RESULT OF THE CANDIDATES WHO APPEARED IN THE FOLLOWING EXAMINATIONS HELD IN"):
            res["examination_date"] = re.match(r".*((JAN|FEB|MAR|APR|MAY|JUN|AUG|SEPT|OCT|NOV|DEC)[-|\.]2\d{3,}).*", line).groups()[0]
        elif line.startswith("Program :"):
            re_progr_sem_info = re.match(r"(Program) : (.*) (Sem) : (.*)", line)
            if re_progr_sem_info:
                res["program"] = re_progr_sem_info.groups()[1]
                res["semester"] = re_progr_sem_info.groups()[3]
        elif line.startswith("Branch :"):
            res["branch"] = line.split(":")[1].strip()

    re_release_date = re.match(r".*(Date : |Dated : )(\d{2,}/\d{2,}/\d{2,}).*", text.replace("\n", ""))
    if re_release_date:
        res["release_date"] = re_release_date.groups()[1]

    for k, v in res.items():
        if not v:
            raise ValueError(f"Couldn't determine {k!r} from {filepath!r} "
                             f"page number {page_num}:\n{text}")

    return res

def parse_dtu_result_pdf(filepath):
    """
    Parse a dtu result pdf.

    :param filepath:
        A path to the pdf file
    :return:
        A list of `dict` where each entry is in the following form.

        results = [
            {
                "name"                : "<student full name>",
                "rollno"              : "<rollno>"
                "program"             : "<btech, mtech, etc>",
                "branch"              : "<branchname>",
                "semester"            : "<semester number>",
                "pdf_filename"        : "<filename>",
                "pdf_pagenum"         : "<pagenum>",
                "release_date"        : "<date>",
                "examination_date"    : "<date>",
                "notice"              : "<notice>",
                "SPI"                 : "<spi>",
                "total_credits"       : "<total_credits>",
                "papers_failed"       : ["sub1_code", "sub2_code", ...],
                "marks"               : {
                    "<subject1_code>"    : "<marks>",
                    "<subject2_code>"    : "<marks>",
                    ...
                }
            },
            # ...
        ]
    """
    filepath = realpath(filepath)
    log.info(f"Parsing {filepath}")

    # Use tabula to parse the tables in the pdf
    start_ts = timer()
    with HideUnderlyingStderrCtx():
        # This will be a list of `pandas.DataFrame`
        pages_df = tabula.read_pdf(filepath, pages='all')
    log.info(f"Took {timer() - start_ts} to parse {filepath}")

    log.info(f"Found {len(pages_df)} pages in {filepath}")
    sanitized_dfs = []
    for num, df in enumerate(pages_df):
        log.info(f"Sanitizing page no {num}...")
        sanitized_dfs.append(sanitize_df(df))
    log.info(f"Parsed tables in {filepath}")

    parsed_data = []
    for num, df in enumerate(sanitized_dfs):
        log.debug(f"Extracting metadata from page no {num}...")
        metadata = parse_metadata(filepath, num)
        for row in json.loads(df.to_json(orient="records")):
            parsed_data.append(
            dict(
                name                = row.pop("name"),
                rollno              = row.pop("rollno"),
                program             = metadata["program"],
                branch              = metadata["branch"],
                semester            = metadata["semester"],
                pdf_filename        = basename(filepath),
                pdf_pagenum         = num,
                release_date        = metadata["release_date"],
                examination_date    = metadata["examination_date"],
                notice              = metadata["notice"],
                SPI                 = row.pop("SPI"),
                total_credits       = row.pop("TC"),
                papers_failed       = row.pop("papers_failed"),
                marks               = row,
            )
        )
    return parsed_data


def parse_all_pdf(dirpath,
                  parallel=False,
                  num_processes=psutil.cpu_count(logical=True),
                  progress_file=None):
    """Parse all pdf results available in `dirpath`"""
    res = []
    progress_history = {}
    if progress_file and os.path.exists(progress_file):
        progress_file = realpath(progress_file)
        log.info("Reading previous parsing progress from {!r}".format(progress_file))
        with open(progress_file, "r") as f:
            progress_history = json.load(f)

    curr_progress = {}

    if parallel:
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            future_to_pdf = {}
            for filepath in get_filepaths(realpath(dirpath)):
                if progress_history.get(basename(filepath), False) is True:
                    curr_progress[basename(filepath)] = True
                    continue
                else:
                    future_to_pdf[executor.submit(parse_dtu_result_pdf,
                                                  os.path.join(dirpath, filepath))] = filepath
            for future in as_completed(future_to_pdf):
                filepath = future_to_pdf[future]
                try:
                    res_filename = future.result()
                    log.info(f"Successfully parsed {filepath!r}")
                    curr_progress[basename(filepath)] = True
                    res.append(res_filename)
                except Exception as exc:
                    log.error(f"Failed to parse {filepath}: {exc}")
                    curr_progress[basename(filepath)] = repr(exc)
    else:
        # Enables interactive debugging on errors
        for filepath in get_filepaths(realpath(dirpath)):
            if progress_history.get(basename(filepath), False) is True:
                curr_progress[basename(filepath)] = True
            else:
                try:
                    res.extend(parse_dtu_result_pdf(filepath))
                    curr_progress[basename(filepath)] = True
                except Exception as exc:
                    log.error(f"Failed to parse {filepath}: {exc}")
                    curr_progress[basename(filepath)] = repr(exc)
                    import ipdb; ipdb.set_trace()
    with open(progress_file, "w+") as f:
        json.dump(curr_progress, f, indent=4, sort_keys=True)
    return res




