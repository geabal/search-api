from typing import Optional, Dict, List, Tuple
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

MAX_RESULT = 100


def search(user_qeury:str='',search_after: Optional[str] = None):
    '''
    검색을 수행하는 wrapper 함수
    '''
    if not user_qeury:
        return {"q":None, "status": 200, 'result':[]}

    text_results, next_cursor = text_search(user_query=user_qeury, search_after=search_after)
 
    results = prettify(text_results)
    # 기존 응답 구조(q, result, status)는 그대로 유지하고 cursor 키만 추가.
    # 기존 응답을 파싱하는 다른 서비스는 모르는 키를 무시하므로 하위 호환이 유지됨.
    response = {'q': user_qeury, 'result': results, 'status': 200, 'cursor': next_cursor}

    return response




def text_search(
    user_query: str,
    search_after: Optional[str] = None,
    page_size: int = 100,
) -> Tuple[List[Dict], Optional[str]]:
    '''
    Atlas Search 쿼리를 수행하는 함수.
 
    search_after: 이전 페이지 응답에서 받은 재개 지점 토큰(searchSequenceToken).
                   None이면 연관도(score)가 가장 높은 결과부터 시작.
    page_size:    한 번에 mongo에서 가져올 원본 문서 수 (dedup 이전 기준).
 
    반환값: (검색 결과 리스트, 다음 페이지를 위한 커서 또는 None)
    다음 페이지가 없을 경우(원본 문서 수 < page_size) cursor는 None으로 반환됨.
    '''
 
    search_stage: Dict = {
        "index": "fulltext_search_index",
        "compound": {
            # NOTE: 아래 "should" 키가 4번 정의되어 있어 파이썬 dict 특성상
            # 마지막 항목(chunk_words)만 실제로 반영되고 있음. 페이지네이션과는
            # 별개 이슈라 이번 수정에서는 건드리지 않았으니 별도로 확인 필요.
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
        },
        # score 내림차순(기본값) + _id 오름차순을 tie-breaker로 사용.
        # _id가 인덱스에서 정렬 가능한 필드로 매핑되어 있어야 동작하므로
        # fulltext_search_index 정의를 반드시 확인할 것.
        "sort": {
            "score": {"$meta": "searchScore"},
            "_id": 1
        }
    }
 
    if search_after:
        search_stage["searchAfter"] = search_after
 
    pipeline = [
        {"$search": search_stage},
        {"$limit": page_size},
        {
            "$project": {
                "_id": 1,
                "url": 1,
                "title": 1,
                "chunk": 1,
                'published_date': 1,
                "score": {"$meta": "searchScore"},
                "paginationToken": {"$meta": "searchSequenceToken"}
            }
        }
    ]
 
    cursor = collection.aggregate(pipeline=pipeline)
    urls = set()
    res = []
    raw_count = 0
    last_token = None
 
    # 중복 제거. 기본적으로 검색 함수는 score 순으로 주어지므로 가장 먼저 들어오는 id의 청크만 남기면 됨.
    i = 0
    for doc in cursor:
        raw_count += 1
        # dedup으로 스킵되는 문서라도 원본 스캔 위치는 진행되므로,
        # 다음 페이지 커서는 항상 "실제로 순회한 마지막 문서"의 토큰으로 갱신해야
        # 다음 요청에서 같은 구간을 다시 스캔하며 재중복이 발생하지 않음.
        last_token = doc.pop('paginationToken', None)
 
        if doc['url'] not in urls:
            res.append(doc)
            urls.add(doc['url'])
            i += 1
        if MAX_RESULT == i:
            break
 
    # page_size만큼 원본 문서를 다 받았다면 다음 페이지가 있을 수 있음.
    # 그보다 적게 받았다면 검색 결과 끝에 도달한 것이므로 커서를 노출하지 않음.
    next_cursor = last_token if raw_count == page_size else None
 
    return res, next_cursor


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

