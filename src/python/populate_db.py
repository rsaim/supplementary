#!/usr/bin/env python

"""
This script populates a local MongoDB instance.
"""
import concurrent
import json
import time
import os
import click
import psutil
from pymongo import MongoClient
from loguru import logger as log
from os import listdir
from os.path import isfile, join, realpath

from utils import get_filepaths
from parse_results import parse_pdf


MAX_NUM_PROCESSES = psutil.cpu_count(logical=True)

client = MongoClient(host="localhost", port=27017)
db = client.dtu


def parse_and_populate_db(pdf):
    try:
        df_list = parse_pdf(pdf)
        log.info(f"{pdf}: Parsing OK")
    except Exception as err:
        log.error(f"{pdf}: Failed to parse: {err!r}")
        return

    for df in df_list:
        try:
            insert_df_to_db(df)
            log.info(f"{pdf}: Inserted to DB")
        except Exception as err:
            log.error(f"{pdf}: Failed to insert a page to DB: {err!r}")


def insert_df_to_db(df):
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


def populate_db(dirname=None, filepath=None):
    if dirname and filepath:
        raise ValueError("Specify either filename or dirname")
    if filepath:
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


