from pydantic import BaseModel

class Load_reques(BaseModel):
    
    request_id: str


class Match_img(BaseModel):
    
    request_id: str
    input: dict