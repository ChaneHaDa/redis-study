# Redis Streams 실습 가이드

## 개념 요약
- Streams: 시간순 로그 구조에 메시지를 기록(XADD)하고, 소비자가 오프셋 기반으로 읽는 구조(XREAD/XREADGROUP).
- 보존/재처리: 메시지가 저장되므로(보존 기간/길이 관리 가능) 지연 소비/재처리/다시 읽기가 가능.
- 컨슈머 그룹: 같은 그룹 내 여러 컨슈머가 워커풀처럼 분산 처리(XREADGROUP, XACK, XPENDING, XCLAIM/XAUTOCLAIM).
- ID: 각 메시지는 `ms-seq` 형태의 ID를 가짐(예: `1694760000000-0`).

## 언제 쓰면 좋을까
- 내구성/재처리가 필요: 작업 큐, 이벤트 소싱, 비동기 파이프라인.
- 다수 컨슈머의 분산 처리: 컨슈머 그룹으로 부하 분산 및 처리 추적.
- 유실 방지/중복 처리 관리가 필요: ACK/재전송/클레임으로 안전성 강화.

## Pub/Sub와의 차이
- Pub/Sub: 브로드캐스트, 비영속, 재처리/ACK 없음 → 초저지연 신호.
- Streams: 영속 큐, 컨슈머 그룹/ACK/재전송 → 안정적 처리.

## 빠른 체험 (redis-cli)
1) 단순 스트림 읽기/쓰기
- 추가: `XADD mystream * type=order id=1001`
- 읽기: `XRANGE mystream - +` 또는 `XREAD BLOCK 0 STREAMS mystream $` (새 항목부터 블로킹)

2) 트리밍(보존 길이 관리)
- 대략 트리밍: `XTRIM mystream MAXLEN ~ 1000` (약 1000개 유지)

3) 컨슈머 그룹
- 그룹 생성(없으면): `XGROUP CREATE mystream mygroup $ MKSTREAM`
- 그룹 읽기: `XREADGROUP GROUP mygroup c1 BLOCK 0 COUNT 10 STREAMS mystream >`
- 처리 완료(ACK): `XACK mystream mygroup <id>`
- 미처리 상태: `XPENDING mystream mygroup`
- 재할당: `XCLAIM` 또는 `XAUTOCLAIM` (Redis 6.2+)

자세한 명령 모음: `python/streams/redis-cli/commands.md`

## Python 예제 실행
- 위치: `python/streams`
- 요구 사항: Python 3.9+, `pip install -r python/streams/requirements.txt`
- Redis 연결: 기본 `127.0.0.1:6379` 또는 `REDIS_URL` 환경변수 사용

예제 1) 프로듀서(쓰기)
- 폴더 이동: `cd python/streams`
- 실행: `python producer.py --stream mystream --count 5 --interval 0.5 --field type=order --field user=42`

예제 2) 단순 리더(XREAD)
- 새 항목부터 블로킹: `python simple_reader.py mystream --from $`
- 처음부터 읽기: `python simple_reader.py mystream --from 0-0 --count 10`

예제 3) 컨슈머 그룹
- 그룹 생성(한 번): `python group_create.py --stream mystream --group mygroup --from $`
- 컨슈머 워커: `python group_consumer.py --stream mystream --group mygroup --consumer c1`
- 다른 워커 추가: `python group_consumer.py --stream mystream --group mygroup --consumer c2`

팁: 워커가 처리 중 죽으면 ACK가 없으므로 `XPENDING`에 쌓임. 일정 시간 후 `XCLAIM/XAUTOCLAIM`으로 다른 워커가 가져가 재처리.

예제 4) 멱등 컨슈머(Idempotent)
- 동일 항목 재전달/중복에 대비해 스트림 ID 기반 중복 방지
- 실행: `python idempotent_consumer.py --stream mystream --group mygroup --consumer c1 --sleep 0.1`

예제 5) 자동 재할당(Autoclaim) + 데드레터
- 충돌/크래시 등으로 ACK 누락된 항목을 `XAUTOCLAIM`으로 가져와 처리
- 실행(리클레이머): `python group_auto_reclaimer.py --stream mystream --group mygroup --consumer repair1 --min-idle 60000 --max-retries 5 --sleep 0.1`
- 동작:
  - `proc:{stream}` 세트로 멱등 처리, 처리 완료 시 ACK
  - 시도 횟수(`attempts:{stream}:{group}`)가 `--max-retries` 초과 시 데드레터 스트림(`dead:{stream}`)으로 이동 후 ACK

예제 6) 파이프라인 적용 워커
- 배치로 `SADD + XACK`를 묶어 처리량 향상
- 실행: `python group_consumer_pipeline.py --stream mystream --group mygroup --consumer c1 --batch 100`

예제 7) 파이프라인 적용 리클레이머
- `XAUTOCLAIM` 결과에 대해 데드레터/ACK/멱등 마킹을 배치 처리
- 실행: `python group_auto_reclaimer_pipeline.py --stream mystream --group mygroup --consumer repair1 --min-idle 60000 --batch 100`

예제 8) Lua로 처리 후 ACK 원자화(엔트리 ID 기준)
- 외부 처리 완료 후, Lua로 `SADD proc:{stream} entry_id`와 `XACK`를 원자적으로 실행
- 실행: `python group_consumer_lua.py --stream mystream --group mygroup --consumer c1`

예제 9) Lua + 비즈니스 키 멱등성(order_id 등)
- 외부 처리 완료 후, Lua로 `SADD proc-key:{stream}:{field} {value}`와 `XACK`를 원자적으로 실행
- 실행: `python group_consumer_lua_by_key.py --stream mystream --group mygroup --consumer c1 --field order_id`

예제 10) 비즈니스 키 기반 멱등 워커(간단)
- 처리 전 `SADD proc-key:{stream}:{field} {value}`로 선점 후 실패 시 `SREM`으로 롤백
- 실행: `python idempotent_consumer_by_key.py --stream mystream --group mygroup --consumer c1 --field order_id`

## 운영 팁
- 보존 정책: `XTRIM MAXLEN ~ N`으로 대략 유지, 또는 `MINID`로 ID 기준 유지.
- 중복 처리: 네트워크/재시도 환경에서 동일 메시지 처리 대비해 멱등성 고려.
- 관찰: `XPENDING`, `XINFO STREAM/GROUP/CONSUMERS`로 상태 점검.
