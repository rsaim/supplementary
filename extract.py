file = "some_file.pdf"

import slate3k as slate

with open(file, 'rb') as f:
    doc = slate.PDF(f)

print(doc[0])