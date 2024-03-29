import os
import time
from celery import Celery
from app.config import load_vectorizer
from app.document import gen_external_documents
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
    MASTER_KEY           = os.environ['MEILISEARCH_KEY']
    MEILISEARCH_ENDPOINT = os.environ['MEILISEARCH_ENDPOINT']
    
    ### run keyword extraction algorithm
    print('start keyword extraction...')
    posts = [post['title']+post['content'] for post in externals]
    keyword_table = keyword_extractor.get_keywords(posts)
    print('keyword extraction finished...')

    ### write the data into meilisearch engine
    print('start writing documents into meilisearch...')
    docs = gen_external_documents(externals, keyword_table)
    client = meilisearch.Client(MEILISEARCH_ENDPOINT, MASTER_KEY)
    client.index('externals').add_documents(docs)
    print('Write documents into meilisearch finished...')
