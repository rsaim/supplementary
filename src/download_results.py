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
import pickle
import shutil
from os.path import realpath

from tqdm import tqdm
from urllib.parse import urljoin
from os import path
from bs4 import BeautifulSoup as soup
import os
import click
import requests
import concurrent
import urllib
import sys
import pprint

from loguru import logger as log


MAX_DOWNLOAD_THREADS = 15
URL_TIMEOUT          = 5 * 60
SUCCESS_URL_FNAME    = "success_urls.pkl" # A list of successfully parsed urls is dumped to this file in outdir
FAILED_URL_FNAME     = "failed_urls.pkl" # A mapping of <url>:<error> is dumped to this file in outdir


def _download_pdf(pdf_url, outdir):
    with urllib.request.urlopen(pdf_url, timeout=URL_TIMEOUT) as conn:
        contents = conn.read()
    if not contents:
        raise ValueError("Content empty for {}".format(pdf_url))
    else:
        file_path = path.join(outdir, os.path.basename(pdf_url))
        with open(file_path, 'wb+') as pdf_file:
            pdf_file.write(contents)


def scrap_pdfs_from_url(url, outdir, overwrite):
    """
    Save all pdfs from `url` to `outdir`.

    :param url:
        URL to scrape.
    :param outdir:
        Where to save the pdfs.
    :return:
        None
    """
    outdir = realpath(outdir)
    if os.path.exists(outdir):
        if overwrite:
            shutil.rmtree(outdir)
        else:
            overwrite_dir = input("{} already exists. Overwrite? [y/n]: ".format(outdir))
            if overwrite_dir.lower() == "y":
                shutil.rmtree(outdir)
            else:
                log.info("Exiting...")
                sys.exit(0)
    log.info("Creating directory {}".format(outdir))
    os.mkdir(outdir)

    content = requests.get(url)
    # If the content was successful, no Exception will be raised
    content.raise_for_status()
    links = list(filter(lambda link:link['href'].endswith('.pdf'),
                        soup(content.text, "html.parser").findAll('a')))
    links[0]['href'] = links[0]['href'] + "wrongit"
    log.info("{} pdf links found from {}".format(len(links), url))
    if len(links) == 0:
        log.warning("No link to pdf found in {}".format(url))
        sys.exit(1)

    failed_urls  = {}
    success_urls = []
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_THREADS) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(_download_pdf, urljoin(url, link['href']), outdir): link
                         for link in links}
        progress_bar = tqdm(range(len(links)), leave=False)
        for future in concurrent.futures.as_completed(future_to_url):
            res = future_to_url[future]
            try:
                future.result()
            except Exception as exc:
                failed_urls[urljoin(url, res['href'])] = str(exc)
            else:
                success_urls.append(res)
            progress_bar.display()
            progress_bar.update()
            sys.stdout.flush()
        progress_bar.close()

    if failed_urls:
        # Save failed urls along with the errors - this can be useful for retrying later
        failed_urls_file = os.path.join(outdir, FAILED_URL_FNAME)
        with open(failed_urls_file, "wb") as f:
            pickle.dump(failed_urls, f)
        log.error("Failed to download pdf from {} urls. Load this mapping of urls->errors in a python session as: "
                     "\nwith open('{}', 'rb') as f: failed_urls=pickle.load(f)".format(len(failed_urls),
                                                                               failed_urls_file))
        log.error("Failed urls:\n{}".format(pprint.pformat(failed_urls)))

    # Save successful urls as well - this can be useful for skipping the downloaded files
    success_urls_file = os.path.join(outdir, SUCCESS_URL_FNAME)
    with open(success_urls_file, "wb") as f:
        pickle.dump(success_urls, f)
    log.info("Successfully downloded {} pdf files. Load this list of urls in an ipython session as \n"
                 "with open('{}', 'rb') as f: success_urls=pickle.load(f)".format(len(success_urls),
                                                                                  success_urls_file))
    log.info("DONE!!")


@click.command()
@click.option('--url',    type=click.STRING,
              help='Download all pdfs available on this URL.')
@click.option('--outdir', type=click.Path(file_okay=False),
              help='Save the pdfs to this directory.')
@click.option('--overwrite', is_flag=True,
              help='Overwrite if outdir exists.')
def main(url, outdir, overwrite):
    scrap_pdfs_from_url(url=url, outdir=outdir, overwrite=overwrite)


if __name__ == '__main__':
    main()
