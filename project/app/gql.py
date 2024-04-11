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