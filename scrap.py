import urllib.request


result_file = "http://exam.dtu.ac.in/result1/O14_BT_MCEN_335.pdf"


def download_file(download_url, path=None):
    path = 'some_file.pdf' if path is None else path
    print(f"Downloading file from url {download_url} to {path}")
    with urllib.request.urlopen(download_url) as web_file:
        with open(path, 'wb') as local_file:
            local_file.write(web_file.read())


if __name__ == '__main__':
    download_file(result_file)