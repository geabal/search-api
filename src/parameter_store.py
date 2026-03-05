import boto3

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

# 사용 예시
# parameter_value = get_parameter('/search-api/prod/model-path')
# print(parameter_value)
