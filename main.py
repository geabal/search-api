from typing import Union
from fastapi import FastAPI
import src.hybrid_search as hs
import src.hybrid_search2 as hs2
import src.trend as tr

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

@app.get("/search2")
def search(q: Union[str, None] = None):
    if not q:
        return {"q":None, "result":[], "status": 200}
    
    res = hs2.hybrid_search(user_qeury=q)
    return res

@app.get("/trend")
def get_trend(today:str=''):
    res = tr.get_today_trend(today=today)
    return res