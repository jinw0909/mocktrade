import redis

# 레디스 연결
rd = redis.Redis(host='172.31.11.200', port=6379, db=0)

# 'price:BTCUSDT' 키에서 값 가져오기
value = rd.get('price:BTCUSDT')

# 값이 존재할 경우 출력
if value:
    print(value.decode())  # 바이트 문자열을 디코딩하여 출력
else:
    print("값이 존재하지 않습니다.")

