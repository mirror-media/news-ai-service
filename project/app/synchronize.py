import os
from app.gql import gql_tags, gql_fetch_sync

def synchronize_tags(meili_client, max_num: int=1000):
  ### 先確定meilisearch裡面最大的tags編號
  hits = meili_client.index('tags').search('', {
      'hitsPerPage': 1,
      'sort': ['id:desc']
  })
  hits = hits['hits']
  max_tag_id = int(hits[0]['id']) if len(hits)>0 else 0
  
  ### 使用該編號為起始條件查詢CMS的Tags
  gql_endpoint         = os.environ['GQL_ENDPOINT']
  gql_tags_string = gql_tags.format(start_id=str(max_tag_id), take=max_num)
  tags = gql_fetch_sync(gql_endpoint, gql_tags_string)
  tags = tags['tags']

  ### 寫回meilisearch
  if len(tags)>0:
    meili_client.index('tags').add_documents(tags)
  print(f'Update meilisearch Tags index for {len(tags)} tags. original max tag_id is {max_tag_id}')
  return True