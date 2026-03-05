from typing import Optional, Dict, List
from sentence_transformers import SentenceTransformer
import torch
from pymongo import MongoClient
import math

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
client = MongoClient(host, 27017)
db = client['Document_DB']
chunk_collection = db['SUMMARY_INFO_B']
title_collection = db['TITLE']

# LLM 모델 로드
model_path =  get_parameter('/search-api/prod/model-path')

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
    title_res, query_embedding = hybrid_search_title(user_query=user_qeury)

    urls = [row['url'] for row in title_res]
    chunk_res = hybrid_search_chunk(user_query=user_qeury, urls=urls, query_embedding=query_embedding)

    concat_res = concat(title_res, chunk_res)
    concat_res = prettify(user_query=user_qeury, search_res=concat_res)
    response = {'q':user_qeury, 'result':concat_res, 'status':200}

    return response


def hybrid_search_title(user_query, filter: Optional[Dict] = {}):
    #벡터 검색, 키워드 검색 결과를 가져온다.
    vec_res, query_embedding = vector_search_title(user_query=user_query)
    text_res = text_search_title(user_query=user_query)

    #각 결과를 조합해 RRFscore를 계산한다.
    # weight: (vector search 결과에 줄 가중치, fulltext search에 줄 가중치)
    weight = (1,1)  
    rrf_res = RRFscore(vec_res, text_res, weight=weight)
        
    return rrf_res, query_embedding

# Define a function to retrieve relevant documents for a user query using vector search
def vector_search_title(user_query: str, filter: Optional[Dict] = {}) -> None:
    """
    title 콜렉션에서 벡터 검색으로 연관 문서를 찾는다.

    Args:
    user_query (str): 유저가 입력하는 검색 쿼리
    filter (Optional[Dict], optional): Optional vector search pre-filter
    """

    query_embedding = model.encode(user_query).tolist()

    pipeline = [{
            "$vectorSearch": {
            "index": "hybrid_search_title",
            "limit": 40,
            "numCandidates": 50,
            "path": "title_vec", # 벡터 인덱스로 지정된 컬럼 이름
            "queryVector": query_embedding,
    }
    },
    {
        '$project':{
            '_id':1,
            'title':1,
            'url':1,
            'published_date':1,
            'score':{'$meta':'vectorSearchScore'}
        }
    }
    ]

    # Execute the aggregation `pipeline` and store the results in `results`
    results = title_collection.aggregate(pipeline=pipeline)
    
    return results, query_embedding

def text_search_title(user_query:str):
    
    pipeline =[
    {
        "$search": {
        "index": "hybrid_search_title",
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
                    "value": 1
                    }
                }
                }
            }
            ]
        }
        }
    },
    {
    "$project": {
      "_id": 1,
      "url": 1,
      "title": 1,
      'published_date':1,
      "score": { "$meta": "searchScore" }
    }
    }
    ]

    cursor = title_collection.aggregate(pipeline=pipeline)
    res = [doc for doc in cursor]
        
    return res

def hybrid_search_chunk(user_query, filter: Optional[Dict] = {}, urls:List[str]=[], query_embedding:List[float]=[]):
    #벡터 검색, 키워드 검색 결과를 가져온다.
    vec_res = vector_search_chunk(user_query=user_query, urls=urls, query_embedding=query_embedding)
    text_res = text_search_chunk(user_query=user_query, urls=urls)

    #각 결과를 조합해 RRFscore를 계산한다.
    # weight: (vector search 결과에 줄 가중치, fulltext search에 줄 가중치)
    weight = (1,1)  # fulltext search가 본문+키워드 서치를 함께 하고 있으므로 vector보다 더 큰 가중치를 줌
    rrf_res = RRFscore(vec_res, text_res, weight=weight)
        
    return rrf_res

def prettify(user_query, search_res):
    '''
    검색 결과를 fastapi에서 이해할 수 있는 형식으로 다듬는 함수
    '''
    for i, row in enumerate(search_res):
        search_res[i]['_id'] = str(search_res[i]['_id'])
        if math.isnan(search_res[i]['published_date']):
            search_res[i]['published_date'] = 'None'

    return search_res

def list2dict(list_data, key='url'):
    dict_data = {}
    for data in list_data:
        dict_data[data[key]] = data
    return dict_data

def concat(title_res, chunk_res, delta=0.02, MIN_SCORE = 0.02):
    '''
    title 검색과 chunk 검색의 결과를 합치고 정리하는 메소드
    Args:
    :param delta: RRFscore 최대값-delta보다 큰 값을 갖는 결과만 반환
    :param MIN_SCORE: RRFscore가 MIN_SCORE를 넘기는 결과만 반환
    '''
    api_res = []
    dict_title = list2dict(title_res)

    MAX_SCORE = chunk_res[0]['RRFscore']
    for doc in chunk_res:
        if doc['RRFscore'] >= MAX_SCORE - delta and doc['RRFscore'] >=MIN_SCORE:
            api_res.append(doc)
            url = doc['url']
            api_res[-1]['title'] = dict_title[url]['title']
            api_res[-1]['published_date'] = dict_title[url]['published_date']
        else:
            break

    # 같은 문서에서 여러 결과가 나오지 않도록 url이 중복되는 청크 제거
    urls = set()
    api_res_dropduple = []
    for doc in api_res:
        url = doc['url']
        if url not in urls:
            urls.add(url)
            api_res_dropduple.append(doc)

    return api_res_dropduple


def vector_search_chunk(user_query: str, filter: Optional[Dict] = {},urls=None, query_embedding:List[float]=[]) -> None:
    """
    주어진 url 필터 조건 안에서 가장 적절한 chunk를 찾는 vector search 수행

    Args:
    user_query (str): 유저가 입력하는 검색 쿼리
    filter (Optional[Dict], optional): Optional vector search pre-filter
    """


    pipeline = [{
            "$vectorSearch": {
            "index": "vector_index_chunk", #벡터 인덱스의 이름
            "limit": 40,
            "numCandidates": 50,
            "path": "chunk_vec", # 벡터 인덱스로 지정된 컬럼 이름
            "queryVector": query_embedding,
            "filter":{"url": {"$in":urls}}
    }
    },
    {
        '$project':{
            '_id':1,
            'chunk':1,
            'url':1,
            'score':{'$meta':'vectorSearchScore'}
        }
    }
    ]

    # Execute the aggregation `pipeline` and store the results in `results`
    
    cursor = chunk_collection.aggregate(pipeline=pipeline)
    res = [doc for doc in cursor]

    return res

def text_search_chunk(user_query:str,filter: Optional[Dict] = {},urls=None):
    
    pipeline =[
    {
        "$search": {
        "index": "hybrid_search_chunk",
        "compound": {
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
                    "value": 1
                    }
                }
                }
            }
            ],
            "filter":{"text":{'query': urls, "path":"url"}}
        }
        
        }
    },
    {
    "$project": {
      "_id": 1,
      "url": 1,
      "chunk": 1,
      "score": { "$meta": "searchScore" }
    }
    }
    ]

    cursor = chunk_collection.aggregate(pipeline=pipeline)
    res = [doc for doc in cursor]
        
    return res
    
    
# 수식 참고: https://www.mongodb.com/docs/manual/reference/operator/aggregation/rankFusion/
# weight: 벡터 검색과 텍스트 검색에 인위적으로 부여할 가중치. 기본적으로 쿼리에 따라 유동적으로 변할 수 있도록 각 검색 결과에 score를 가중치로 곱한다.
# sensitivity: 분모에 넣는 수치로 민감도를 결정함. 숫자가 작을수록 rank에 의해 score가 크게 바뀜.
def RRFscore(vector_result, fulltext_result, weight=(1,1), sensitivity=60):
    rrf = {}

    for i, doc in enumerate(vector_result):
        rank = i + 1
        vec_weight = weight[0]*doc['score']
        rrf[doc['_id']] = doc
        rrf[doc['_id']]['RRFscore'] = (1/(sensitivity + rank)) * vec_weight
    
    for i, doc in enumerate(fulltext_result):
        rank = i + 1
        text_weight = weight[1]*doc['score']
        if doc['_id'] in rrf:
            rrf[doc['_id']]['RRFscore'] += (1/(sensitivity+rank)) * text_weight
        else:
            rrf[doc['_id']] = doc
            rrf[doc['_id']]['RRFscore'] = (1/(sensitivity+rank)) * text_weight

    # rrf 내림차순 정렬
    rrf_list = [v for k, v in rrf.items()]
    sorted_rrf = sorted(rrf_list, key= lambda x: x['RRFscore'], reverse=True)

    return sorted_rrf
