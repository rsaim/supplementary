import tabula
import numpy as np
import pandas as pd
import os


for filename in os.listdir("."):
    filename = os.path.abspath(filename)
    pdf_pages = tabula.read_pdf(filename, pages='all')
    for page_df in pdf_pages:
        for column in page_df:
            for val in page_df[column]:
                if isinstance(val, str) and "SAIM" in val.upper():
                    print(filename)
import sys
sys.exit(0)

# This will be a list of pd.DataFrame.
pdf_pages = tabula.read_pdf("/Users/saim/supplementary/some_file.pdf", pages='all')


# Sanitizing a particular page.
page = pdf_pages[0]

name_replacement_map = {
    'Sr.No. Name'   : 'name',
    'Roll No.'      : 'roll_no',
    'Unnamed: 1'    : 'papers_failed'
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
    page.reset_index(drop=True, inplace=True)

    # Remove number and space from the begining of names. Also replace double spaces with a single space.
    page['name'] = page['name'].apply(lambda x: x.strip("0123456789 ").replace("  ", " "))

    # A long name that spans multiple lines has all columns except name as NaN. Append this name to
    # the name in the previous row. For instance 'KOMARAVOLU NITIN BHARDWAJ' is a single name below.
    #                  name     roll_no MC-301 MC-302 MC-303 MC-304 MC-305 MC-306 MC-307 MC-308 MC-309    TC    SPI     paper_failed
    # 0       KISHAN ASHIYA  2K12/MC/29     75     63     30     69     68     76     84     72    162  26.0  62.93           MC-303
    # 1    KOMARAVOLU NITIN  2K12/MC/30     86     69     82     84     89     75     88     87    185  30.0  83.67              NaN
    # 2            BHARDWAJ         NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN   NaN    NaN              NaN
    # 3       KRISHNA KUMAR  2K12/MC/31     47     53     48     49     45     60     88     70     95  30.0  53.13              NaN
    drop_indices = []
    for row_num, row in page.iterrows():
        row_vals = row.values.tolist()
        row_vals.remove(row['name'])
        if not any([pd.notnull(val) for val in row_vals]) and row_num > 0:
            drop_indices.append(row_num)
            page.loc[row_num - 1, 'name'] = page.loc[row_num - 1, 'name'] + " " + row['name']
    if drop_indices:
        page = page.drop(drop_indices)
        page.reset_index(drop=True, inplace=True)

    print(f"Successfully process page {num}...\n")
    print(page)
    sanitized_pages.append(page)





