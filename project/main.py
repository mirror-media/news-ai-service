import os
from celery.result import AsyncResult
from fastapi import Body, FastAPI, Form, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

from worker import create_task, keyword_task
from app.gql import gql_fetch_async, gql_externals
from app.config import DEFAULT_FETCH_EXTERNALS

from schema.schemata import External

### App related variables
app = FastAPI()
origins = ["*"]
methods = ["*"]
headers = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials = True,
    allow_methods = methods,
    allow_headers = headers
)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

### Define APIs
@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("home.html", context={"request": request})

@app.post("/tasks", status_code=201)
def run_task(payload = Body(...)):
    task_type = payload["type"]
    task = create_task.delay(int(task_type))
    return JSONResponse({"task_id": task.id})

@app.get("/tasks/{task_id}")
def get_status(task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JSONResponse(result)

@app.post('/keyword')
async def keyword(external: External):
    '''
        calculate keyword for externals and write the result into meilisearch 
    '''
    take = external.take ### how many externals should we process, take should be less than 50
    gql_endpoint = os.environ['GQL_ENDPOINT']
    gql_externals_string = gql_externals.format(take=DEFAULT_FETCH_EXTERNALS)
    externals = await gql_fetch_async(gql_endpoint, gql_externals_string)
    externals = externals['externals']
    
    ### filter the externals that have no keywords and with keywords
    externals_no_keyword = []
    for external in externals:
        if external['tags'] == []:
            externals_no_keyword.append(external)
    externals = externals_no_keyword

    ### keyword extraction is cpu-intensive work, put it in task
    task = keyword_task.delay({
        'externals': externals
    })
    result = {
        'message': "Keyword extraction sent in the background",
        'task_id': task.id
    }
    return JSONResponse(result)
