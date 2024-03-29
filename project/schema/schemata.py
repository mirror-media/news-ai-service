from pydantic import BaseModel

class Article(BaseModel):
    content: str
class External(BaseModel):
    take: int