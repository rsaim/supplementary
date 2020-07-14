#!/usr/bin/env python

"""
This script populates a local MongoDB instance.
"""
from __future__ import absolute_import, division

import click
import concurrent
import json
from   loguru                   import logger as log
import os
from   os.path                  import realpath
import psutil
from   pymongo                  import MongoClient
import time

from   parse_results            import parse_pdf_to_dfs
from   utils                    import get_filepaths


MAX_NUM_PROCESSES = psutil.cpu_count(logical=True)

client = MongoClient(host="localhost", port=27017)
db = client.dtu


def parse_and_populate_db(pdf):
    if not pdf.endswith(".pdf"):
        return
    try:
        df_list = parse_pdf_to_dfs(pdf)
        log.info(f"{pdf}: Parsing OK")
    except Exception as err:
        log.error(f"{pdf}: Failed to parse: {err!r}")
        return

    for df in df_list:
        try:
            insert_df_to_mongodb(df)
            log.info(f"{pdf}: Inserted to DB")
        except Exception as err:
            log.error(f"{pdf}: Failed to insert a page to DB: {err!r}")


def insert_df_to_mongodb(df):
    """
    :param df:
        pandas.DataFrame
    :return:
    """
    for i, _row in df.iterrows():
        row = json.loads(_row.to_json()) # find a better way
        row.pop("TC", None)
        row.pop("SPI", None)
        row.pop("papers_failed", None)
        name = row.pop("name", None)
        if not name:
            log.error(f"name not present in row {_row.to_dict()!r}, SKIPPING...")
            continue
        collection_id = row.pop("roll_no")
        if not collection_id:
            log.error(f"roll_no not present in row {_row.to_dict()!r}, SKIPPING...")
            continue
        # row = {k:int(v) for k,v in row.items()}
        row["name"] = name
        row["_id"] = collection_id
        log.debug("Inserting {}".format(row))
        db.results.find_one_and_update(
            filter = {
                "_id": collection_id
            },
            update = {
                "$set": row
            },
            upsert=True
        )


DYNAMODB = None
def insert_df_to_dynamodb(df):
    if not DYNAMODB:
        import boto3
        global  DYNAMODB
        DYNAMODB = boto3.resource('dynamodb', region_name="ap-southeast-1")


def populate_db(dirname=None, filepath=None):
    if dirname and filepath:
        raise ValueError("Specify either filename or dirname")
    if filepath:
        if not filepath.endswith(".pdf"):
            return
        parse_and_populate_db(filepath)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_NUM_PROCESSES) as executor:
            future_to_pdf = {executor.submit(parse_and_populate_db, filepath): filepath
                             for filepath in get_filepaths(realpath(dirname))}
            for future in concurrent.futures.as_completed(future_to_pdf):
                filename = future_to_pdf[future]
                try:
                    future.result()
                except Exception as exc:
                    log.error(repr(exc))
                else:
                    log.info(f"Successfully parsed {filename!r}")


@click.command()
@click.option('--file',    type=click.STRING,
              help='Populate DB using this DTU result file.')
@click.option('--dir', type=click.Path(file_okay=False),
              help='Populate DB using all the DTU result files in this dir.')
@click.option('--logfile', type=click.STRING,
             help='Ouput logs to this file. Default is a rendom file in /tmp.')
def main(file, dir, logfile):
    logfile = f"~/tmp/populate_db.{time.time()}.{os.getpid()}.log" if not logfile else logfile
    logfile = realpath(logfile)
    log.add(sink=open(logfile, "w"), level="INFO")
    log.info(f"Writing logs to {logfile}")
    populate_db(dirname=dir, filepath=file)


if __name__ == '__main__':
    main()


