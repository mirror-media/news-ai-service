import copy
from app.tool import convert_unix_time, remove_html

def gen_external_documents(externals, keyword_table):
    '''
        Organizing external posts and keyword into documents which are used as storage unit in meiliseach
    '''
    if len(externals)!=len(keyword_table):
        print('length of externals and keyword table is not matched!')
        return None
    documents = copy.deepcopy(externals)
    for idx, post in enumerate(documents):
        keywords = list(keyword_table[idx].keys())
        tmp = {
            'id': post['id'],
            'title': post['title'],
            'publishedDate': convert_unix_time(post['publishedDateString']),
            'source': post['source'],
            'thumbnail': post['thumb'],
            'partner': post['partner'],
            'brief': post['brief'],
            'keywords': keywords
        }
        # If brief is empty, retrieve some in content
        if tmp['brief']=='':
            content = remove_html(post['content'])
            tmp['brief'] = content[:100]
        documents[idx] = tmp
    return documents