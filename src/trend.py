from datetime import datetime, timedelta
from pymongo import MongoClient
from dateutil.parser import parse
import certifi
#from data import secret

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
#host = secret.host

ca = certifi.where()
client = MongoClient(host, 27017, tlsCAFile=ca)
db = client['TREND']
trend_collection = db['TREND_INFO']

def get_today_trend(today:str=''):
    try:
        if today == '':
            today = datetime.today()
        else:            
            today = parse(today)

        res = _get_trend_info(today)

    except Exception as e:
        error = str(e)
        return {'state':200, 'error':error, 'result':[]}

    return {'state':200, 'result': res}

def _get_trend_info(today:datetime):
    year, month, day = _seperate_date(today)
    today_start = datetime(year=year, month=month, day=day)

    tomorrow = today+timedelta(days=1)
    year, month, day = _seperate_date(tomorrow)
    tomorrow_start = datetime(year=year, month=month, day=day)

    # 입력으로 들어온 날짜에 만들어진 데이터만 가져옴
    query =  {"created_date": {
        "$gte": today_start,
        "$lt": tomorrow_start
    }}
    cursor = trend_collection.find(query)
    res = []
    for doc in cursor:
        res.append(doc)
        res[-1]['_id'] = str(doc['_id'])
    return res

def _seperate_date(date:datetime):
    year = date.year
    month = date.month
    day = date.day
    return year, month, day
