import os
import re


# Code used to sanitize file names in CWD.
num_files_before = len(os.listdir("."))
for filename in os.listdir("."):
    sanitized_filename = filename.replace("\n", "").strip()
    sanitized_filename = re.sub(' +', ' ', sanitized_filename).replace(" ", "_")
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