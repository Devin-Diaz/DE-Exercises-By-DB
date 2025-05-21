from urllib.parse import urlparse 
import os

def extract_filename(uri):
    return os.path.basename(urlparse(uri).path)


    