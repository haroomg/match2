from .tools.classes import Request
from fastapi import HTTPException
from .match import match_img
from fastapi import FastAPI
from .shema import *


app = FastAPI()

#3
@app.post("/request/load/")
def load_request(request: Load_reques) -> dict:
    
    request_id = Load_reques.request_id
    request_ = Request(request_id)

    if request_.exists_request == True:

        if request_.exists_schema == False:

            request.create_schema()

        request.load_data()

        msm = {
            "msm": "La data acaba de ser subida.",
            "request_id": request_.request_id,
            "schema_name": request_.schema_name
        }

        return 

    else:
        msm = {
            "msm": "El request no existe o esta mal escrito",
            "request_id": request_id
        }
        raise HTTPException(status_code=409, detail=msm)


@app.post("/match/img")
def match_images(request: Match_img) -> dict:
    
    request = {
        "request_id": request.request_id,
        "input": request.input
    }
    
    return match_img(**request)