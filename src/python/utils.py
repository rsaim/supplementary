from __future__ import absolute_import, division

import re


import os

# https://stackoverflow.com/a/19308592/6463555
from os.path import realpath, basename
import sys
import diskcache
from pathlib import Path

import pdfplumber
import tabula


def get_filepaths(directory):
    """
    This function will generate the file names in a directory
    tree by walking the tree either top-down or bottom-up. For each
    directory in the tree rooted at directory top (including top itself),
    it yields a 3-tuple (dirpath, dirnames, filenames).
    """
    file_paths = []  # List which will store all of the full filepaths.

    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            # Join the two strings in order to form the full filepath.
            filepath = realpath(os.path.join(root, filename))
            file_paths.append(filepath)  # Add it to the list.

    return file_paths  # Self-explanatory.

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.

    Ref: https://stackoverflow.com/a/295466/6463555
    """
    value = ascii(value)
    value = re.sub('[^\w\s-]', '', value).strip()
    value = re.sub('[-\s]+', '-', value)
    return value

def sanitize_filenames(dir):
    # Code used to sanitize file names in CWD.
    num_files_before = len(os.listdir(dir))
    for filename in os.listdir("."):
        sanitized_filename = slugify(filename)
        print(f"{filename!r} -> {sanitized_filename!r}")
        if os.path.exists(sanitized_filename):
            prefix = 1
            while os.path.exists(sanitized_filename):
                sanitized_filename = str(prefix) + "_" + sanitized_filename
                prefix += 1
        os.rename(filename, sanitized_filename)
    num_files_after = len(os.listdir("."))

    diff = num_files_after - num_files_before
    if diff != 0:
        raise ValueError(f"{diff} files lost!")


class RedirectStdStreams(object):
    """This works only on Python calls. Stdout/err from underlying
    C calls would be missed."""
    def __init__(self, stdout=None, stderr=None):
        self._stdout = stdout or sys.stdout
        self._stderr = stderr or sys.stderr

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush(); self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self._stdout.flush(); self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr


# Based on https://stackoverflow.com/a/14797594/6463555
class HideUnderlyingStderrCtx(object):
    '''
    A context manager that block stdout for its scope, usage:

    Hides stderr from underlying libraries (not Python).

    with HideUnderlyingStderrCtx():
        os.system('ls -l')
    '''

    def __init__(self, *args, **kw):
        sys.stdout.flush()
        self._origstdout = sys.stdout
        self._oldstdout_fno = os.dup(sys.stdout.fileno())
        self._devnull = os.open(os.devnull, os.O_WRONLY)

    def __enter__(self):
        self._newstdout = os.dup(2)
        os.dup2(self._devnull, 2)
        os.close(self._devnull)
        sys.stdout = os.fdopen(self._newstdout, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._origstdout
        sys.stdout.flush()
        os.dup2(self._oldstdout_fno, 2)


def get_topdir():
    """
    Searches for a file .top in the parent dirs iteratively.

    Returns a `pathlib.PosixPath` object.
    """
    path = Path(os.path.dirname(__file__))
    while True:
        if (path / ".top").exists():
            return path
        if path.parent == path:
            # Seems like we reached the home /
            raise ValueError("Couldn't determine root directory.")
        path = path.parent


tabula_cache = diskcache.Cache(get_topdir() / "data/caches/tabula")

def tabula_read_pdf(filepath, pages):
    """Wrapper over `tabule.read_pdf which memoizes results using
    `os.path.basename(filepath)` and param `pages`."""
    try:
        return tabula_cache[(basename(filepath), pages)]
    except KeyError:
        pass
    with HideUnderlyingStderrCtx():
        pages_df = tabula.read_pdf(filepath, pages=pages)
    tabula_cache[(basename(filepath), pages)] = pages_df
    return pages_df


pdfplumber_cache = diskcache.Cache(get_topdir() / "data/caches/pdfplumber")

def pdfplumber_extract_text(filepath, page_num):
    try:
        return pdfplumber_cache[(basename(filepath), page_num)]
    except KeyError:
        pass
    pdf = pdfplumber.open(filepath)
    pages = list(pdf.pages)
    if page_num not in range(0, len(pages)):
        raise ValueError(f"{filepath!r} has {len(pages)}, passed page_num={page_num}")
    page = pages[page_num]
    text = page.extract_text()
    pdfplumber_cache[(basename(filepath), page_num)] = text
    return text
