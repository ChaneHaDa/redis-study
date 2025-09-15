# redis-cli Distributed Lock 명령 모음

## 단일 인스턴스 락 기본 (SET NX PX)
- 획득(5초 TTL): `SET lock:demo <token> NX PX 5000`
- TTL 확인: `PTTL lock:demo`

## 안전 해제 (소유자 검증 + DEL)
```
EVAL "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end" 1 lock:demo <token>
```

## 안전 연장 (소유자 검증 + PEXPIRE)
```
EVAL "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end" 1 lock:demo <token> 5000
```

## 기타 확인
- 현재 값(토큰): `GET lock:demo`
- 남은 TTL(ms): `PTTL lock:demo`

주의: 단순 `DEL lock:demo`는 위험. 토큰 검증 없는 해제는 경쟁 상황에서 타 소유자 락을 지울 수 있습니다.

