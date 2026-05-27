from typing import Optional, Dict, List
from pymongo import MongoClient
import math
import certifi

# from data import secret
# host = secret.host

import boto3

# 파라메터 가져오기
# SSM 클라이언트 생성
ssm = boto3.client('ssm', region_name='ap-northeast-2')

def get_parameter(parameter_name, isDescrypt=False):
    try:
        response = ssm.get_parameter(
            Name=parameter_name,
            WithDecryption=isDescrypt
        )
        return response['Parameter']['Value']
    except Exception as e:
        print(f"파라미터 조회 실패: {e}")
        return None

# 몽고db 연결
host = get_parameter('/search-api/prod/mongoDBKey', isDescrypt=True)

ca = certifi.where()
client = MongoClient(host, 27017, tlsCAFile=ca)
db = client['Document_DB']
collection = db['SUMMARY_INFO_D']


def search(user_qeury:str=''):
    '''
    검색을 수행하는 wrapper 함수
    '''
    if not user_qeury:
        return {"q":None, "status": 200, 'result':[]}
    
    text_results = text_search(user_query=user_qeury)

    results = prettify(text_results)
    response = {'q':user_qeury, 'result':results, 'status':200}


    return response


def text_search(user_query:str):
    
    pipeline =[
    {
        "$search": {
        "index": "fulltext_search_index",
        "compound": {
            "should": [
            {
                "text": {
                "query": user_query,
                "path": "doc_keyword"
                }
            }
            ],
            "should": [
            {
                "text": {
                "query": user_query,
                "path": "title_words",
                "score": {
                    "boost": {
                    "value": 2
                    }
                }
                }
            }
            ],
            "should": [
            {
                "text": {
                "query": user_query,
                "path": "chunk_keyword"
                }
            }
            ],
            "should": [
            {
                "text": {
                "query": user_query,
                "path": "chunk_words",
                "score": {
                    "boost": {
                    "value": 2
                    }
                }
                }
            }
            ],
        }
        }
    },
    {
    "$project": {
      "_id": 1,
      "url": 1,
      "title": 1,
      "chunk": 1,
      'published_date':1,
      "score": { "$meta": "searchScore" }
    }
    }
    ]

    cursor = collection.aggregate(pipeline=pipeline)
    res = [doc for doc in cursor]
        
    return res

def prettify(search_res):
    '''
    검색 결과를 fastapi에서 이해할 수 있는 형식으로 다듬는 함수
    '''
    for i, row in enumerate(search_res):
        search_res[i]['_id'] = str(search_res[i]['_id'])
        try:
            if math.isnan(search_res[i]['published_date']):
                search_res[i]['published_date'] = 'None'
        except:
            search_res[i]['published_date'] = str(search_res[i]['published_date'])
            continue

    return search_res

