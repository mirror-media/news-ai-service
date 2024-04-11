from gql.transport.aiohttp import AIOHTTPTransport
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

def gql_fetch_async(gql_endpoint, gql_string):
    gql_transport = AIOHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport,
                        fetch_schema_from_transport=True)
    json_data = gql_client.execute_async(gql(gql_string))
    return json_data

def gql_fetch_sync(gql_endpoint, gql_string):
    gql_transport = AIOHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport,
                        fetch_schema_from_transport=True)
    json_data = gql_client.execute(gql(gql_string))
    return json_data