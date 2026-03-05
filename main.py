from typing import Union
from fastapi import FastAPI
import src.hybrid_search as hs

app = FastAPI()

@app.get("/")
def read_root():
    return {"msg": "Hello World", "status":200}

@app.get("/search")
def search(q: Union[str, None] = None):
    if not q:
        return {"q":None, "result":[], "status": 200}
    
    res = hs.hybrid_search(user_qeury=q)
    return res