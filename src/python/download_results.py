#!/usr/bin/env python

"""
Download all the pdfs linked on a given webpage.


Sample Run:

$ ./download_results.py --url=https://file-examples.com/index.php/sample-documents-download/sample-pdf-download/ --outdir=test_pdfs --overwrite
2020-06-27 03:31:49.545 | INFO     | __main__:scrap_pdfs_from_url:81 - Creating directory /Users/saim/github/supplementary/test_pdfs
2020-06-27 03:31:50.314 | INFO     | __main__:scrap_pdfs_from_url:90 - 3 pdf links found from https://file-examples.com/index.php/sample-documents-download/sample-pdf-download/
2020-06-27 03:31:53.230 | ERROR    | __main__:scrap_pdfs_from_url:123 - Failed to download pdf from 1 urls. Load this mapping of urls->errors in a python session as:
with open('/Users/saim/github/supplementary/test_pdfs/failed_urls.pkl', 'rb') as f: failed_urls=pickle.load(f)
2020-06-27 03:31:53.230 | ERROR    | __main__:scrap_pdfs_from_url:124 - Failed urls:
{'https://file-examples.com/wp-content/uploads/2017/10/file-sample_150kB.pdfwrongit': '<HTTPError '
                                                                                      '404: '
                                                                                      "'Not "
                                                                                      "Found'>"}
2020-06-27 03:31:53.233 | INFO     | __main__:scrap_pdfs_from_url:132 - Successfully downloded 2 pdf files. Load this list of urls in an ipython session as
with open('/Users/saim/github/supplementary/test_pdfs/success_urls.pkl', 'rb') as f: success_urls=pickle.load(f)
2020-06-27 03:31:53.233 | INFO     | __main__:scrap_pdfs_from_url:133 - DONE!!

"""
from __future__ import absolute_import, division

import json
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from   os.path                  import realpath
import pickle
import shutil

from   bs4                      import BeautifulSoup as soup
import click
import concurrent
import os
from   os                       import path
import pprint
import requests
import sys
from   tqdm                     import tqdm
import urllib
from   urllib.parse             import urljoin

from   loguru                   import logger as log


MAX_DOWNLOAD_THREADS = 15
URL_TIMEOUT          = 60
DEFAULT_DOWNLOAD_URL = "http://exam.dtu.ac.in/result_all.htm"
DEFAULT_PROGRESS_FNAME   = "progress.json"
"""A mapping of <url>:<Bool> is dumped to this file in outdir. A False value means that the URL
wasn't downloaded"""


def _download_pdf(pdf_url, outdir):
    if " " in pdf_url:
        log.warning("{!r} has spaces in the URL; replacing the spaces with %20 manually".format(
            pdf_url))
        pdf_url = pdf_url.replace(" ", "%20")
    with urllib.request.urlopen(pdf_url, timeout=URL_TIMEOUT) as conn:
        contents = conn.read()
    if not contents:
        raise ValueError("Content empty for {}".format(pdf_url))
    else:
        file_path = path.join(outdir, os.path.basename(pdf_url))
        with open(file_path, 'wb+') as pdf_file:
            pdf_file.write(contents)


def _filter_urls_using_progress_file(urls, progress_file):
    progress_history = {}
    if not os.path.exists(progress_file):
        log.info("progress file {!r} doesn't exist".format(progress_file))
        return urls, progress_history

    log.info("Reading previous progress from {}".format(progress_file))
    with open(progress_file, "r") as f:
        progress_history = json.load(f)

    # Remove successful urls as per progress file
    success_urls_progress_file = [k for k,v in progress_history.items() if v]
    log.info("{} succeessful URLs found from progress file".format(len(success_urls_progress_file)))
    [urls.remove(k) for k in success_urls_progress_file if k in urls]

    # Add unssuccessful urls from the progress file
    failed_urls_progress_file = [k for k,v in progress_history.items() if not v]
    log.info("Adding {} failed URLs from progress file".format(len(failed_urls_progress_file)))
    urls.extend(failed_urls_progress_file)

    return list(set(urls)), progress_history


def scrap_pdfs_from_url(download_url, outdir, overwrite, progress_file):
    """
    Save all pdfs from `url` to `outdir`.

    :param download_url:
        URL to scrape.
    :param outdir:
        Where to save the pdfs.
    :param progress_file:
        Save status of URL download to this file. Pass this file to retry the failed URLs.
    :return:
        None
    """
    download_url = DEFAULT_DOWNLOAD_URL if not download_url else download_url

    progress_file = (realpath(progress_file)
                     if progress_file
                     else os.path.join(outdir, DEFAULT_PROGRESS_FNAME))

    outdir = realpath(outdir)
    if os.path.exists(outdir):
        if not overwrite:
            overwrite_dir = input("{} already exists. "
                                  "Overwrite files in the directory? [y/n]:".format(outdir))
            if overwrite_dir.lower() == "n":
                log.info("Exiting...")
                sys.exit(0)
    else:
        log.info("Creating directory {}".format(outdir))
        os.mkdir(outdir)

    content = requests.get(download_url)
    # If the content was successful, no Exception will be raised
    content.raise_for_status()
    links = list(filter(lambda link:link['href'].endswith('.pdf'),
                        soup(content.text, "html.parser").findAll('a')))
    urls = list(map(lambda link: urljoin(download_url, link["href"]), links))
    urls, progress_history = _filter_urls_using_progress_file(urls, progress_file)

    # Remove duplicates
    if len(urls):
        log.info("{} pdf links found from {}. Downloading {} URLs as per progress file.".format(
            len(links), download_url, len(urls)))
    else:
        log.warning("No link to pdf found in {}".format(download_url))
        sys.exit(1)

    curr_progress  = {}
    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_THREADS) as executor:

        # Start the load operations and mark each future with its URL
        future_to_url = {}
        for download_url in urls:
            future_to_url[executor.submit(_download_pdf, download_url, outdir)] = download_url

        progress_bar = tqdm(range(len(urls)), leave=False)

        for future in as_completed(future_to_url):
            download_url = future_to_url[future]
            try:
                future.result()
            except Exception as err:
                log.error(err)
                success = False
            else:
                success = True
            curr_progress[download_url] = success

            progress_bar.display()
            progress_bar.update()
            sys.stdout.flush()
        # progress_bar.close()

    failed_urls = [k for k,v in curr_progress.items() if v is False]
    if failed_urls:
        log.error("Failed to download pdf from {} urls. Failed urls:\n{}".format(
            len(failed_urls), pprint.pformat(failed_urls)))

    success_urls = [v for k, v in curr_progress.items() if v is True]
    log.info("Successfully downloaded {} pdf files.".format(len(success_urls)))

    with open(progress_file + ".tmp", "w+") as f:
        progress_history.update(curr_progress)
        json.dump(progress_history, f, indent=4, sort_keys=True)
    # Write atomically
    os.rename(progress_file + ".tmp", progress_file)
    log.info("Progress saved in {!r}".format(progress_file))
    log.info("DONE!!")


@click.command()
@click.option('--url',    type=click.STRING,
              help='Download all pdfs available on this URL.')
@click.option('--outdir', type=click.Path(file_okay=False),
              help='Save the pdfs to this directory.')
@click.option('--progress_file', type=click.Path(file_okay=True),
              help='Progress file of downloads.')
@click.option('--overwrite', is_flag=True,
              help='Overwrite if outdir exists.')
def main(url, outdir, progress_file, overwrite):
    scrap_pdfs_from_url(download_url=url, outdir=outdir, overwrite=overwrite, progress_file=progress_file)


if __name__ == '__main__':
    main()
