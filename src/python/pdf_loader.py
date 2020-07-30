import dropbox

dbx = dropbox.Dropbox(<AUTH_KEY>)

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

# if __name__ == "__main__":
    upload_files(['demo.txt']) 
    download_files('demo.txt')