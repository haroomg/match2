from .match import match_img
from fastapi import FastAPI
from .request import load
from .shema import *


app = FastAPI()

#3
# @app.post("/request/load/")
# def load_request(request: Load_reques) -> dict:
    
#     request_id: str = request.request_id
    
#     return load(request_id)


@app.post("/match/img")
def match_images(request: Match_img) -> dict:
    
    request = {
        "request_id": request.request_id,
        "input": request.input,
        "s3_path_img_origin": request.s3_path_img_origin,
        "s3_path_img_alternative": request.s3_path_img_alternative
    }
    
    return match_img(**request)