from typing import Union, Optional
from fastapi import FastAPI

import src.trend as tr
import src.reverse_index_search as isearch

app = FastAPI()

@app.get("/")
def read_root():
    return {"msg": "Hello World", "status":200}

# @app.get("/search")
# def search2(q: Union[str, None] = None):
#     if not q:
#         return {"q":None, "result":[], "status": 200}
    
#     res = hs.hybrid_search(user_qeury=q)
#     return res

@app.get("/trend")
def get_trend(today:str=''):
    res = tr.get_today_trend(today=today)
    return res

@app.get("/search")
def index_search(q: Union[str, None] = None, cursor: Union[str, None] = None):
    if not q:
        return {"q":None, "result":[], "status": 200, "cursor":None}
    
    res = isearch.search(user_qeury=q, search_after=cursor)
    return res