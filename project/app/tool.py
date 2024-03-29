import os
import json
import datetime
from bs4 import BeautifulSoup as bs

### files operations
def save_file(dest_filename, data):
    if data:
        dirname = os.path.dirname(dest_filename)
        if len(dirname)>0 and not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(dest_filename, 'w', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False))
        print(f'save {dest_filename} successfully')

def open_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        file = json.load(f)
    return file

### other tools
def convert_unix_time(datetime_str):
    datetime_obj = datetime.datetime.strptime(datetime_str, '%Y/%m/%d')
    unix_timestamp = datetime_obj.timestamp()
    return int(unix_timestamp)

def remove_html(post):
    '''
        Remove html tag
    '''
    soup = bs(post, 'html.parser')
    for a_tag in soup.find_all('a'):
        a_tag.extract()
    filtered_post = soup.get_text()
    return filtered_post