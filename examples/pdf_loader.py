import dropbox
import os

AUTH_KEY = os.environ.get('SUPPLEMENTARY_DROPBOX_TOKEN')
dbx = dropbox.Dropbox(AUTH_KEY)

folder_path = '/pdf/'
# expects list of paths to be called with to upload them
# Dont upload files larger than 150MB using this.
def upload_files(paths):
    for path in paths:
        print(path)
        with open(path, 'rb') as file:
            dbx.files_upload(file.read(), folder_path + path)

# Expects path to file to download
# It downloads them into a file with same name on root directory (can change as per requirement)
def download_files(path):
    print(path)
    temp = dbx.files_download_to_file(path, folder_path+path)

def download_cache_as_zip():
    dbx.files_download_zip_to_file('data.zip', '/data')

if __name__ == "__main__":
    download_cache_as_zip()