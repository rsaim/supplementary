import tabula
import numpy as np
import pandas as pd

pdf_pages = tabula.read_pdf("/Users/saim/supplementary/some_file.pdf", pages='all')


# Sanitizing a particular page.
page = pdf_pages[0]

name_replacement_map = {
    'Sr.No. Name'   : 'name',
    'Roll No.'      : 'roll_no',
    'Unnamed: 1'    : 'paper_failed'
}

sanitized_pages = []
for num, page in enumerate(pdf_pages):
    print(f"Processing page {num}...")
    sanitized_names = []
    for x in page.columns:
        new_name = name_replacement_map.get(x, None)
        if new_name:
            print(f"Replacing {x!r} with {new_name!r}")
            sanitized_names.append(new_name)
        else:
            sanitized_names.append(x)
    print(sanitized_names)
    page.columns = sanitized_names

    # Drop rows which have nan in name column.
    page = page[pd.notnull(page['name'])].copy()

    print("Deleting column 'Unnamed: 0'.")
    del page['Unnamed: 0']

    # Drop first row. Assert that we are not dropping a random row. First row is expected to be read as:
    # 0                      NaN  Max. Marks / Credits  100/4  100/4  100/4  100/4  100/4  100/2  100/2  100/2  200/4  30    NaN  Papers Failed
    if 'Max. Marks / Credits' in page.iloc[0].values:
        print(f"Dropping first row {page.iloc[0].values.tolist()!r}")
        page = page.drop(0)

    # Remove number and space from the begining of names. Also replace double spaces with a single space.
    page['name'] = page['name'].apply(lambda x: x.strip("0123456789 ").replace("  ", " "))
    print(f"Successfully process page {num}...\n")
    print(page)
    sanitized_pages.append(page)





