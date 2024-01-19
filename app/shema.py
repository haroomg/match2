from pydantic import BaseModel, validator
from typing import Union

class Load_reques(BaseModel):
    
    request_id: str