import os
import time
from celery import Celery
from app.config import load_vectorizer
from app.document import gen_external_documents
from app.gql import generate_tags_string, generate_external_string, gql_update_sync, gql_update_tags, gql_update_external
import meilisearch

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")
keyword_extractor = load_vectorizer()
cached_tag_mapping = {}

@celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    return True

@celery.task(name="keyword_task")
def keyword_task(data: dict):
    externals            = data['externals']
    gql_endpoint         = os.environ['GQL_ENDPOINT']
    MASTER_KEY           = os.environ['MEILISEARCH_KEY']
    MEILISEARCH_ENDPOINT = os.environ['MEILISEARCH_ENDPOINT']
    
    ### run keyword extraction algorithm
    print('start keyword extraction...')
    posts = [post['title']+post['content'] for post in externals]
    keyword_table = keyword_extractor.get_keywords(posts)
    print('keyword extraction finished...')

    ### write the data into meilisearch engine for furthur usage
    client = meilisearch.Client(MEILISEARCH_ENDPOINT, MASTER_KEY)
    print('start writing documents into meilisearch...')
    docs = gen_external_documents(externals, keyword_table)
    client.index('externals').add_documents(docs)
    print('Write documents into meilisearch finished...')

    ### update tags on cms
    tags_string = generate_tags_string(keyword_table)
    try:
        gql_update_sync(
            gql_endpoint = gql_endpoint, 
            gql_string   = gql_update_tags, 
            gql_variable = tags_string
        )
    except:
        print("Update with some repetition tags")
    
    ### update keyword field of externals
    for idx, external in enumerate(externals):
        id   = external['id']
        tags = external['tags']
        if tags==None or tags==[]:
            continue  # we don't overwrite the external which already has tags
        keywords = list(keyword_table[idx].keys())
        external_string = generate_external_string(id, keywords)
        try:
            gql_update_sync(
                gql_endpoint = gql_endpoint,
                gql_string = gql_update_external,
                gql_variable = external_string
            )
            print(f"Successfully update tags for external_id={id}")
        except:
            print(f'Update tags for external_id={id} failed')
    return True
