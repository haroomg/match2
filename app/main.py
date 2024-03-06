from .tools.constans import paramsl, paramsp
from .match import match_img
from .tools.db import DatabaseConnection
from fastapi import FastAPI
from .request import load
from .shema import *


app = FastAPI()

#3
@app.post("/request/load/")
def load_request(request: Load_reques) -> dict:
    
    request_id: str = request.request_id

    connp = DatabaseConnection(**paramsp)
    connl = DatabaseConnection(**paramsl)
    
    return load(request_id, connp, connl)


@app.post("/match/img")
def match_images(request: Match_img):
    
    request_id: str = request.request_id
    input_: dict = request.input
    s3_path_img_origin: str = request.s3_path_img_origin
    s3_path_img_alternative: str = request.s3_path_img_alternative

    connl = DatabaseConnection(**paramsl)
    conn_origin = DatabaseConnection(**paramsl)
    conn_alternative = DatabaseConnection(**paramsl)
    
    return match_img(request_id, input_, s3_path_img_origin, s3_path_img_alternative, connl, conn_origin, conn_alternative)

# @app.get("/match/list/<request_id>/<input_id>")
# def list_matches(request_id: str = None, input_id: int = None) -> list:
#     pass