# Redis Distributed Lock 실습 가이드

## 개념 요약
- 목적: 여러 프로세스/서버가 공유 자원에 동시 접근하지 않도록 상호배제를 보장.
- 기본 패턴(단일 Redis): `SET lock:{resource} {token} NX PX {ttl_ms}`로 획득, 소유자 토큰이 일치할 때만 Lua로 안전 해제.
- 핵심 속성: 고유 토큰(소유자 식별), 만료(죽은 락 해제), 안전 해제(다른 소유자 락 오해제 방지).
- 한계: 단일 Redis 인스턴스 기반에서는 at-most-one 보장이 강력하지만, 네트워크 분할/장애 모델에 따라 추가 고려 필요.

## 언제 쓰면 좋을까
- 단일 자원에 대한 동시성 제어: 배치 잡 중복 실행 방지, 워크플로우 단일 리더 선출, 재고 차감 단일 처리 등.
- 작업 시간이 짧거나 예측 가능하고, 잠금 유실에 따른 영향이 크지 않은 경우.

## 동작 원리 자세히
- 획득(Acquire)
  - `SET key value NX PX ttl`은 키가 없을 때만 설정하며 TTL을 함께 부여.
  - `value`는 고유 토큰(예: UUID). 락 소유자 식별에 사용.
- 해제(Release)
  - 단순 `DEL`은 위험. 다른 소유자가 갱신 직후 덮어쓴 경우를 지울 수 있음.
  - Lua로 “키의 값이 내 토큰과 일치하면 DEL”을 원자적으로 수행.
- 연장(Renew)
  - 긴 작업 시 만료 전에 TTL 연장 필요. 마찬가지로 Lua로 “토큰 확인 → PEXPIRE” 원자 수행.
- 대기(Blocking)
  - 즉시 실패 대신, 백오프 + 지터로 재시도하며 총 대기 시간 내 획득 시 성공.

## redis-cli 빠른 실습
1) 락 획득(5초 TTL)
- `SET lock:demo <token> NX PX 5000`

2) TTL 확인/대기
- `PTTL lock:demo`

3) 안전 해제(Lua)
- `EVAL "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end" 1 lock:demo <token>`

4) 안전 연장(Lua)
- `EVAL "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end" 1 lock:demo <token> 5000`

더 많은 명령: `python/locks/redis-cli/commands.md`

## Python 예제 실행
- 위치: `python/locks`
- 의존성: `pip install -r python/locks/requirements.txt`
- Redis 연결: 기본 `127.0.0.1:6379` 또는 `REDIS_URL`

예제 1) 단일 획득/해제
- `python acquire_once.py --resource demo --work-ms 2000 --ttl-ms 5000`

예제 2) 대기형 획득(Blocking)
- `python acquire_blocking.py --resource demo --timeout-ms 10000 --retry-ms 200 --ttl-ms 5000`

예제 3) 워치독(Watchdog)으로 TTL 자동 연장
- 긴 작업 시: `python acquire_with_watchdog.py --resource demo --work-ms 15000 --ttl-ms 5000 --renew-ms 2000`

예제 4) DistLock 클래스로 사용
- 컨텍스트 매니저: `python distlock_demo.py --resource demo --work-ms 3000`

예제 5) 캐시 스탬피드 방지(Blocking)
- 캐시 미스 시 한 워커만 DB 조회 후 캐시 재빌드, 나머지는 대기 → `python cache_rebuild_blocking.py --key item:1 --cache-ttl 30 --lock-ttl-ms 5000 --timeout-ms 1000 --retry-ms 100 --db-ms 500`
- 다중 터미널에서 동시에 실행해 단일 진입과 대기/폴링 동작을 관찰

예제 6) Stale-While-Revalidate(SWR)
- 소프트 TTL 지나면 오래된 값을 즉시 서빙하고, 락으로 1개 워커만 백그라운드 갱신 시도 → `python cache_rebuild_swr.py --key item:1 --soft-ttl 10 --swr-window 10 --db-ms 500`
- 동일 키 여러 번 호출하면서 fresh→stale→refresh 흐름 확인

## Hands-on 랩 시나리오
1) 기본 상호배제 확인
- 터미널1: `python acquire_blocking.py --resource demo --work-ms 5000`
- 터미널2: 즉시 실행. 두 번째는 첫 번째가 해제할 때까지 대기 후 진입해야 함.

2) TTL 연장(Watchdog)
- `python acquire_with_watchdog.py --resource demo --ttl-ms 3000 --renew-ms 1000 --work-ms 12000`
- 만약 연장이 없다면 TTL 만료로 다른 프로세스가 락을 선점할 수 있음.

3) 안전 해제 검증
- 터미널1: `python acquire_once.py --resource demo --work-ms 5000`
- 중간에 `GET lock:demo`로 토큰 확인 → 다른 토큰으로는 Lua 해제 스크립트가 0을 반환해야 함.

4) 캐시 스탬피드 방어 실습
- 동시에 `cache_rebuild_blocking.py --key hot:1` 여러 개 실행 → 단 1개만 DB로 가서 SETEX 하고, 나머지는 채워진 캐시를 사용
- SWR 모드로 `cache_rebuild_swr.py --key hot:2 --soft-ttl 5 --swr-window 10` 반복 실행 → 소프트 만료 후에도 빠른 응답 유지 + 백그라운드 갱신 확인

## 운영 팁/체크리스트
- 재시도 정책: `retry + jitter`로 공명/쏠림 회피.
- TTL 선택: 평균 작업시간 + 여유. P99 기반으로 산정 권장.
- 워치독 스레드: `ttl/2` 주기 권장, 연장 실패 시 즉시 중단/로그.
- 장애 대비: 락 만료 후 중복 처리 가능성 → 비즈니스 로직 멱등성 확보.
- 네임스페이스: `lock:{resource}` 컨벤션 유지. 테넌트 분리 필요 시 접두사 사용.

## 베스트 프랙티스
- 토큰: 반드시 고유(프로세스/쓰레드/요청 단위 UUID).
- TTL: 작업 시간 + 여유분. 너무 짧으면 만료 위험, 너무 길면 장애 시 회복 지연.
- 재시도: 고정+지터 백오프. 과도한 스핀 회피.
- 안전 해제/연장: 항상 토큰 검증 후 Lua로 원자 실행.
- 장애 설계: 프로세스 정지/GC/일시 중지로 인한 만료 가능성 고려. 멱등한 작업 설계.

## 참고: 멀티-노드 락(Redlock)
- 여러 Redis 노드에 다수결로 락을 잡는 알고리즘이 존재하나, 환경/장애 모델에 따라 장단이 큼.
- 본 실습은 단일 Redis 기반 기본 락 패턴에 집중. 고가용성은 Sentinel/Cluster + 애플리케이션 재시도로 보완 권장.
