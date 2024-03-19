from .app.tools.constans import TRASH_PATH
from dotenv import load_dotenv
from uvicorn import run
import os

load_dotenv(".env")
app: str = "app.main:app"

if __name__ == "__main__":

    if not os.path.exists(TRASH_PATH):
        os.makedirs(TRASH_PATH)
    
    run(
            app, 
            host=  os.environ.get("FASTDUP_HOST"),
            port= int(os.environ.get("FASTDUP_PORT")),
            reload= bool(int(os.getenv("DEBUG"))),
            timeout_keep_alive=None
        )