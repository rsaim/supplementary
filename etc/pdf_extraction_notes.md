### podofo
Seems like the DTU pdfs are created using http://podofo.sourceforge.net/
- It is a C++ library. 
- I tried to install its Python bindings based on SWIG - https://github.com/alvanieto/pypodofo. Though I was successfuly, the interpreter crashed during some operation.
- We will need to write C++ code if we want to explore podofo more.
- For now, I will try to work with alternatives.

Tutorial: https://realpython.com/pdf-python/#history-of-pypdf-pypdf2-and-pypdf4

#### pdfreader doesn't seem very useful

PyPDF2 doesn't work well for extracting texts.
```python
In [26]: file_name="/Users/saim/dtu_results/O14_BT_MCEN_335.pdf"

In [27]: from PyPDF2 import PdfFileReader

In [28]: file_name="/Users/saim/dtu_results/O14_BT_MCEN_335.pdf"

In [29]: f= open(file_name, 'rb')

In [30]: pdf = PdfFileReader(f)

In [31]: p1=pdf.getPage(1)

In [32]: p1.extractText()
Out[32]: ''
```

####pdfminer - https://github.com/euske/pdfminer
> PDFMiner is a text extraction tool for PDF documents.
> Warning: As of 2020, PDFMiner is not actively maintained. The code still works, but this project is largely dormant. For the active project, check out its fork pdfminer.six.

##### pdfminer.six https://github.com/pdfminer/pdfminer.six


### PDFPLUMBER - https://github.com/jsvine/pdfplumber#python-library
```python
import pdfplumber
file_name="/Users/saim/dtu_results/O14_BT_MCEN_335.pdf"
pdf=pdfplumber.open(file_name)
page=pdf.pages[0]
print(page.extract_text())
```