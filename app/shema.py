from pydantic import BaseModel

class Load_reques(BaseModel):
    
    request_id: str


class Match_img(BaseModel):
    
    request_id: str
    input: dict
    s3_path_img_origin: str
    s3_path_img_alternative: str