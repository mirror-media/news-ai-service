from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
from gql import gql, Client

### 抓取外部文章的gql
gql_externals = '''
query Externals {{
  externals(where: {{state: {{equals: "published"}} }}, take: {take}, orderBy: {{publishedDate: desc}}) {{
    id
    state
    publishedDateString
    title
    partner {{
      id
      name
    }}
    tags {{
      id
    }}
    thumb
    source
    brief
    content
  }}
}}
'''

### 抓取Tags
gql_tags = '''
query Tags {{
  tags(where: {{id: {{gt: {start_id} }} }}, take: {take}, orderBy: {{id: asc}}) {{
    id
    name
  }}
}}
'''

### 更新tags
gql_update_tags = """
mutation CreateTags($data: [TagCreateInput!]!) {
  createTags(data: $data) {
    name
  }
}
"""

### 更新external
gql_update_external = """
mutation UpdateExternal($where: ExternalWhereUniqueInput!, $data: ExternalUpdateInput!) {
  updateExternal(where: $where, data: $data) {
    tags {
      name
    }
  }
}
"""

def gql_fetch_async(gql_endpoint, gql_string):
    gql_transport = AIOHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport,
                        fetch_schema_from_transport=True)
    json_data = gql_client.execute_async(gql(gql_string))
    return json_data

def gql_fetch_sync(gql_endpoint, gql_string):
    gql_transport = RequestsHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport,
                        fetch_schema_from_transport=True)
    json_data = gql_client.execute(gql(gql_string))
    return json_data
  
def gql_update_sync(gql_endpoint, gql_string, gql_variable):
    gql_transport = RequestsHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport,
                        fetch_schema_from_transport=True)
    json_data = gql_client.execute(gql(gql_string), variable_values=gql_variable)
    return json_data
  
def generate_tags_string(keyword_table):
    '''
      generate the updating tags string from keyword_table
    '''
    total_keywords = set()
    for keywords in keyword_table:
        total_keywords = total_keywords | set(keywords.keys())
    
    tags_string = {
        "data": []
    }
    for keyword in total_keywords:
        tags_string["data"].append({
          "name": keyword
        })
    return tags_string

    
def generate_external_string(id: str, keywords: list):
    '''
      generate the updating externals string
    '''
    external_string = {  
      "where": {
        "id": id
      },
      "data": {
        "tags": {
          "set": []
        }
      },
    }
    for keyword in keywords:
      external_string["data"]["tags"]["set"].append(
        {
          "name": keyword
        }
      )
    return external_string

def create_tags(gql_endpoint, keyword_table):
    tags_string = generate_tags_string(keyword_table)
    try:
        gql_update_sync(
            gql_endpoint = gql_endpoint, 
            gql_string   = gql_update_tags, 
            gql_variable = tags_string
        )
    except:
        print("create some repetitive tags")
    return True

def update_external_tags(gql_endpoint, externals, keyword_table):
    ### share the same transport client
    gql_transport = RequestsHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport, fetch_schema_from_transport=True)
    for idx, external in enumerate(externals):
      id   = external['id']
      tags = external['tags']
      if tags!=[]:
          print(f"prevent modify external_id={id} because it already has tags")
          continue
      keywords = list(keyword_table[idx].keys())
      external_string = generate_external_string(id, keywords)
      try:
          gql_client.execute(gql(gql_update_external), variable_values=external_string)
          print(f"Successfully update tags for external_id={id}")
      except:
          print(f'Update tags for external_id={id} failed')
    return True
    