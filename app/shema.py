from pydantic import BaseModel, validator
from typing import Union

class Load_reques(BaseModel):
    
    request_id: str


class Match_img(BaseModel):
    
    request_id: str
    input: dict
    s3_path_img_origin: str
    s3_path_img_alternative: str