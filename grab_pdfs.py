#!/usr/bin/env python

"""
Download all the pdfs linked on a given webpage

Usage -

    python grab_pdfs.py url <path/to/directory>
        url is required
        path is optional. Path needs to be absolute
        will save in the current directory if no path is given
        will save in the current directory if given path does not exist

Requires - requests >= 1.0.4
           beautifulsoup >= 4.0.0

Download and install using

    pip install requests
    pip install beautifulsoup4
"""

__author__ = 'elssar <elssar@altrawcode.com>'
__license__ = 'MIT'
__version__ = '1.0.0'

from requests import get
from urllib.parse import urljoin
from os import path, getcwd
from bs4 import BeautifulSoup as soup
from sys import argv
import os
import time

def get_page(base_url):
    req = get(base_url)
    if req.status_code == 200:
        return req.text
    raise Exception('Error {0}'.format(req.status_code))


def get_all_links(html):
    bs = soup(html, "html.parser")
    links = bs.findAll('a')
    return links


def get_pdf(base_url, base_dir):
    html= get_page(base_url)
    links = get_all_links(html)
    # import ipdb; ipdb.set_trace()
    print(f"{len(links)} links found for pdf")
    if len(links) == 0:
        raise Exception('No links found on the webpage')
    n_pdfs = 0
    failed_urls = []
    for link in links:
        if link['href'][-4:] == '.pdf':
            n_pdfs += 1
            pdf_url = urljoin(base_url, link['href'])
            content = get(pdf_url)
            if content.status_code == 200 and content.headers['content-type'] == 'application/pdf':
                out_path = path.join(base_dir, link.text.replace("/", "\\") + '.pdf')
                if os.path.exists(out_path):
                    out_path = out_path + str(time.time())
                with open(out_path, 'wb+') as pdf:
                    pdf.write(content.content)
                print(f"{n_pdfs}... {pdf_url} downloaded to {out_path}")
            else:
                failed_urls.append(pdf_url)
    if n_pdfs == 0:
        raise Exception('No pdfs found on the page')
    if failed_urls:
        print(f"Pdf files from following URLs couldn't be downloaded:\n{failed_urls}")
    print("{0} pdfs downloaded and saved in {1}".format(n_pdfs, base_dir))


if __name__ == '__main__':
    if len(argv) not in (2, 3):
        print('Error! Invalid arguments')
        print(__doc__)
        exit(-1)
    arg = ''
    url = argv[1]
    if len(argv) == 3:
        arg = argv[2]
    base_dir = [getcwd(), arg][path.isdir(arg)]
    get_pdf(url ,base_dir)

"""
SAMPLE RUN:

COMMAND:
python grab_pdfs.py http://exam.dtu.ac.in/result_all.htm all_results

OUTPUT:
...
Pdf files from following URLs couldn't be downloaded:
['http://www.reg.exam.dtu.ac.in/alist/O18_1_UFM_MT_877.pdf', 'http://exam.dtu.ac.in/result_2019/O18_3_UFM_MT_877.pdf', 'http://www.reg.exam.dtu.ac.in/alist/O18_1_BBA_UFM_877.pdf', 'http://exam.dtu.ac.in/result_2019/O18_3_BBA_UFM_877.pdf', 'http://exam.dtu.ac.in/result_2019/O18_3_BAE_UFM_877.pdf', 'http://exam.dtu.ac.in/result1/CON_RW_BT_738.pdf']
1323 pdfs downloaded and saved in all_results
"""