from typing import Optional, Dict, List
from sentence_transformers import SentenceTransformer
import torch
from pymongo import MongoClient
import math
import certifi

# from data import secret
# host = secret.host
# model_path = secret.model_path

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
model_path =  get_parameter('/search-api/prod/model-path')

ca = certifi.where()
client = MongoClient(host, 27017, tlsCAFile=ca)
db = client['Document_DB']
collection = db['SUMMARY_INFO_C']

# LLM 모델 로드
model = SentenceTransformer(model_path)

if torch.cuda.is_available():
    model.to('cuda')
    print('gpu에 모델 할당 완료!')
else:
    print('gpu를 감지하지 못했습니다.')


def hybrid_search(user_qeury:str=''):
    '''
    하이브리드 검색을 수행하는 wrapper 함수
    '''
    if not user_qeury:
        return {"q":None, "status": 200, 'result':[]}
    
    vec_results = vector_search(user_query=user_qeury)
    text_results = text_search(user_query=user_qeury)

    results = concat(vec_results, text_results)

    response = {'q':user_qeury, 'result':results, 'status':200}

    return response

def RRFscore(vector_result, fulltext_result, weight=(1,1), sensitivity=60):
    rrf = {}
    # text 검색 결과가 있는 경우만 결과로 반환.
    # vector 검색 결과만 있는 경우 결과 반환 x
    for i, doc in enumerate(fulltext_result):
        rank = i + 1
        text_weight = weight[1]*doc['score']
        rrf[doc['_id']] = doc
        rrf[doc['_id']]['RRFscore'] = (1/(sensitivity+rank)) * text_weight

    for i, doc in enumerate(vector_result):
        rank = i + 1
        vec_weight = weight[0]*doc['score']
        if doc['_id'] in rrf:
            rrf[doc['_id']]['RRFscore'] += (1/(sensitivity + rank)) * vec_weight

    # rrf 내림차순 정렬
    rrf_list = [v for k, v in rrf.items()]
    sorted_rrf = sorted(rrf_list, key= lambda x: x['RRFscore'], reverse=True)

    return sorted_rrf


def concat(vec_result, text_result, MIN_SCORE:float=0.02, delta=0.02):
    cols = ['published_date', 'title','chunk','url', 'RRFscore', '_id']

    weight = (1, 1)
    sorted_rrf = RRFscore(vector_result=vec_result, fulltext_result=text_result, weight=weight)
    if not sorted_rrf:
        return []
    MAX_SCORE = sorted_rrf[0]['RRFscore']

    cutted = []
    urls = set()
    for doc in sorted_rrf:
        url = doc['url']
        if doc['RRFscore'] < MIN_SCORE or doc['RRFscore'] < MAX_SCORE - delta:
            break
        if url in urls: # 동일한 url에 대해 중복된 chunk를 가져오지 않도록 설정
            continue
        urls.add(url)
        cutted.append(doc)
    
    result = []
    for row in cutted:
        doc = {}
        for col in cols:
            doc[col] = row[col]
        result.append(doc)
    result = prettify(result)
    return result

def vector_search(user_query: str, filter: Optional[Dict] = {}) -> None:
    """
    title 콜렉션에서 벡터 검색으로 연관 문서를 찾는다.

    Args:
    user_query (str): 유저가 입력하는 검색 쿼리
    filter (Optional[Dict], optional): Optional vector search pre-filter
    """

    query_embedding = model.encode(user_query).tolist()

    pipeline = [
        # 첫 번째 벡터 필드(vector_field_1) 검색
        {
            "$vectorSearch": {
                "index": "vector_index", 
                "path": "chunk_vec",
                "queryVector": query_embedding,
                "numCandidates": 50,
                "limit": 40
            }
        },
        # 결과 구분을 위한 가중치 또는 태그 추가 (선택 사항)
        { "$set": { "search_source": "field_1" } },
        
        # 두 번째 벡터 필드(vector_field_2) 검색 결과를 합침
        {
            "$unionWith": {
                "coll": "SUMMARY_INFO_C",
                "pipeline": [
                    {
                        "$vectorSearch": {
                            "index": "vector_index",
                            "path": "title_vec",
                            "queryVector": query_embedding,
                            "numCandidates": 50,
                            "limit": 40
                        }
                    },
                    { "$set": { "search_source": "field_2" } }
                ]
            }
        },
        
        # 중복 제거 및 점수 합산 (Reranking)
        {
            "$group": {
                "_id": "$_id",
                "final_score": { "$sum": { "$meta": "vectorSearchScore" } },
                "doc": { "$first": "$$ROOT" }
            }
        },
        { "$sort": { "final_score": -1 } },
        { "$limit": 40 }
        
    ]


    # Execute the aggregation `pipeline` and store the results in `results`
    cursor = collection.aggregate(pipeline=pipeline)
    result = []
    for row in cursor:
        doc = row['doc']
        doc['score'] = row['final_score']
        result.append(doc)
  
    return result


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
                "path": "title_norm",
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
                "path": "chunk_norm",
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
        if math.isnan(search_res[i]['published_date']):
            search_res[i]['published_date'] = 'None'

    return search_res

