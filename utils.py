import os
import re

import sys

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