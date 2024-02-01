from .match import match_img, list_matches
from fastapi import FastAPI
from .request import load
from .shema import *


app = FastAPI()

@app.post("/request/load/")
def load_request(request: Load_reques) -> dict:
    
    request_id: str = request.request_id
    
    return load(request_id)


@app.post("/match/img")
def match_images(request: Match_img):
    
    request_id: str = request.request_id
    input: dict = request.input
    s3_path_img_origin: str = request.s3_path_img_origin
    s3_path_img_alternative: str = request.s3_path_img_alternative
    
    return match_img(request_id, input, s3_path_img_origin, s3_path_img_alternative)

# @app.get("/match/list/<request_id>/<input_id>")
# def list_matches(request_id: str = None, input_id: int = None) -> list:
#     pass