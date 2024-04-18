import os
import time
from celery import Celery
from app.config import load_vectorizer
from app.document import gen_external_documents
from app.gql import update_external_tags, create_tags
import meilisearch

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")
keyword_extractor = load_vectorizer()

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
    posts = [post['title']+post['brief']+post['content'] for post in externals]
    keyword_table = keyword_extractor.get_keywords(posts)
    print('keyword extraction finished...')

    ### write the data into meilisearch engine for furthur usage
    client = meilisearch.Client(MEILISEARCH_ENDPOINT, MASTER_KEY)
    print('start writing documents into meilisearch...')
    docs = gen_external_documents(externals, keyword_table)
    client.index('externals').add_documents(docs)
    print('Write documents into meilisearch finished...')

    ### create new tags on cms
    create_tags(
        gql_endpoint  = gql_endpoint,
        keyword_table = keyword_table
    )
    
    ### update keyword field of externals
    update_external_tags(
        gql_endpoint  = gql_endpoint, 
        externals     = externals, 
        keyword_table = keyword_table
    )
    return True
