import os
import meilisearch
from app.gql import gql_tags, gql_fetch

async def synchronize_tags(max_num: int=100):
  gql_endpoint         = os.environ['GQL_ENDPOINT']
  MASTER_KEY           = os.environ['MEILISEARCH_KEY']
  MEILISEARCH_ENDPOINT = os.environ['MEILISEARCH_ENDPOINT']
  meili_client = meilisearch.Client(MEILISEARCH_ENDPOINT, MASTER_KEY)
  
  ### 先確定meilisearch裡面最大的tags編號
  hits = meili_client.index('tags').search('', {
      'hitsPerPage': 1,
      'sort': ['id:desc']
  })
  hits = hits['hits']
  max_tag_id = int(hits[0]['id']) if len(hits)>0 else 0
  
  ### 使用該編號為起始條件查詢CMS的Tags
  gql_tags_string = gql_tags.format(start_id=str(max_tag_id), take=max_num)
  tags = await gql_fetch(gql_endpoint, gql_tags_string)
  tags = tags['tags']

  ### 寫回meilisearch
  if len(tags)>0:
    meili_client.index('tags').add_documents(tags)
  print(f'Update meilisearch Tags index for {len(tags)} tags. original max tag_id is {max_tag_id}')
  return True