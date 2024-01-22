from fastapi import FastAPI
from dotenv import load_dotenv
from .request import load
from .shema import Load_reques

app = FastAPI()


@app.post("/request/load/")
def load_request(request: Load_reques) -> dict:
    
    request_id: str = request.request_id
    
    return load(request_id)