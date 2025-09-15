# Redis Streams 실습 가이드

## 개념 요약
- Streams: 시간순 로그에 이벤트를 `XADD`로 기록하고, 소비자가 오프셋(ID) 기반으로 읽는 구조(`XREAD`, `XREADGROUP`).
- 보존/재처리: 메시지가 저장됨. 길이/ID 기준 트리밍으로 보존량 제어, 나중에 다시 읽기·재처리 가능.
- 컨슈머 그룹: 한 그룹 내 여러 컨슈머가 워커풀처럼 분산 처리(`XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`/`XAUTOCLAIM`).
- ID: 각 메시지는 `ms-seq` ID(예: `1694760000000-0`). 같은 ms에 여러 엔트리가 들어오면 seq가 증가.

## 데이터 모델과 읽기 모델 자세히
- 엔트리: 하나의 Stream 항목은 필드-값 맵입니다. 예) `{type: order, order_id: 1001}`
- `XADD`
  - 기본적으로 스트림이 없으면 생성합니다. `MAXLEN ~ N`(대략) 또는 `MINID ~ id`로 트리밍 가능.
  - ID `*`는 서버가 자동 생성, 명시 ID도 가능(역행 ID는 에러).
- `XRANGE`/`XREVRANGE`: 범위 조회. 교육/디버깅에 유용.
- `XREAD`
  - 다중 스트림을 한번에 읽음. `--from $`는 “지금 이후 새 항목만” 의미(기존은 제외).
  - 애플리케이션이 마지막 ID를 직접 관리해야 함(호출 간 상태 저장 필요).

## 컨슈머 그룹 동작 원리
- 그룹 상태
  - 그룹은 스트림마다 생성(`XGROUP CREATE`). `last-delivered-id`와 각 컨슈머의 PEL(Pending Entries List)을 관리.
  - `>`: 그룹에 아직 배달되지 않은 새 항목을 의미. 특정 ID를 넣으면 그 ID 이후를 재읽기.
- 배달/처리 사이클
  1) `XREADGROUP GROUP g c STREAMS mystream >`로 새 항목 배달 → 컨슈머의 PEL에 등록
  2) 비즈니스 로직 처리
  3) 처리 완료 후 `XACK` → PEL에서 제거
- 재시도
  - 처리 중 장애로 ACK 누락 시 PEL에 남음. `XPENDING`으로 확인, `XCLAIM`/`XAUTOCLAIM`으로 다른 컨슈머가 가져와 재처리.
  - `XAUTOCLAIM`(Redis 6.2+): idle 시간 기준으로 자동 클레임하며, 커서 기반 스캔을 제공.
- 전달 보장
  - 기본은 “at-least-once”. 중복 가능성에 대비해야 함 → 멱등 처리 필요.

## Pub/Sub와의 차이 요약
- Pub/Sub: 브로드캐스트, 비영속, 재처리/ACK 없음 → 초저지연 알림/신호.
- Streams: 영속 큐, 컨슈머 그룹/ACK/재전송/클레임 → 안정적 처리 파이프라인.

## redis-cli 빠른 체험
1) 단순 쓰기/읽기
- 추가: `XADD mystream * type=order id=1001`
- 읽기: `XRANGE mystream - +` 또는 `XREAD BLOCK 0 STREAMS mystream $`

2) 트리밍(보존 관리)
- 대략 길이 유지: `XTRIM mystream MAXLEN ~ 1000`
- ID 기준 유지: `XTRIM mystream MINID ~ 1694760000000-0`

3) 컨슈머 그룹
- 그룹 생성: `XGROUP CREATE mystream mygroup $ MKSTREAM`
- 그룹 읽기: `XREADGROUP GROUP mygroup c1 BLOCK 0 COUNT 10 STREAMS mystream >`
- 처리 완료: `XACK mystream mygroup <id>`
- 미처리 확인: `XPENDING mystream mygroup`
- 재할당: `XCLAIM` 또는 `XAUTOCLAIM`(6.2+)

더 많은 명령: `python/streams/redis-cli/commands.md`

## Python 예제 실행
- 위치: `python/streams`
- 요구 사항: Python 3.9+, `pip install -r python/streams/requirements.txt`
- Redis 연결: 기본 `127.0.0.1:6379` 또는 `REDIS_URL`

예제 1) 프로듀서(쓰기)
- 폴더 이동: `cd python/streams`
- 실행: `python producer.py --stream mystream --count 5 --interval 0.5 --field type=order --field user=42`

예제 2) 단순 리더(XREAD)
- 새 항목부터 블로킹: `python simple_reader.py mystream --from $`
- 처음부터 읽기: `python simple_reader.py mystream --from 0-0 --count 10`

예제 3) 컨슈머 그룹
- 그룹 생성(한 번): `python group_create.py --stream mystream --group mygroup --from $`
- 컨슈머 워커: `python group_consumer.py --stream mystream --group mygroup --consumer c1`
- 추가 워커: `python group_consumer.py --stream mystream --group mygroup --consumer c2`
- 팁: 워커가 죽어 ACK가 누락되면 `XPENDING`에 남고, 이후 `XCLAIM/XAUTOCLAIM`으로 재처리.

예제 4) 멱등 컨슈머(Idempotent)
- 스트림 엔트리 ID로 중복 방지: `python idempotent_consumer.py --stream mystream --group mygroup --consumer c1 --sleep 0.1`

예제 5) 자동 재할당(Autoclaim) + 데드레터
- 리클레이머: `python group_auto_reclaimer.py --stream mystream --group mygroup --consumer repair1 --min-idle 60000 --max-retries 5 --sleep 0.1`
- 동작: `proc:{stream}` 멱등, `attempts:{stream}:{group}` 시도 카운트, 초과 시 `dead:{stream}`으로 이동 후 ACK

예제 6) 파이프라인 워커
- 배치로 `SADD + XACK` 묶기: `python group_consumer_pipeline.py --stream mystream --group mygroup --consumer c1 --batch 100`

예제 7) 파이프라인 리클레이머
- 배치 데드레터/ACK/마킹: `python group_auto_reclaimer_pipeline.py --stream mystream --group mygroup --consumer repair1 --min-idle 60000 --batch 100`

예제 8) Lua로 처리 후 ACK 원자화(엔트리 ID)
- 실행: `python group_consumer_lua.py --stream mystream --group mygroup --consumer c1`

예제 9) Lua + 비즈니스 키 멱등(order_id 등)
- 실행: `python group_consumer_lua_by_key.py --stream mystream --group mygroup --consumer c1 --field order_id`

예제 10) 비즈니스 키 기반 멱등 워커(간단)
- 실행: `python idempotent_consumer_by_key.py --stream mystream --group mygroup --consumer c1 --field order_id`

## Hands-on 랩 시나리오
1) 기본 파이프라인 구축
- 스트림/그룹 준비: `python group_create.py --stream mystream --group mygroup --from $ --mkstream`
- 생산: `python producer.py --stream mystream --count 10 --interval 0.1 --field type=job`
- 소비: `python group_consumer.py --stream mystream --group mygroup --consumer c1`

2) 장애-복구 실습(ACK 누락 → 재처리)
- 의도적 미ACK 워커: `python group_consumer.py --stream mystream --group mygroup --consumer c2 --noack --sleep 0.5`
- 생산: `python producer.py --stream mystream --count 5 --interval 0.2 --field type=job`
- 펜딩 확인: `docker exec -it redis-study redis-cli XPENDING mystream mygroup`
- 일정 시간 후 리클레이머: `python group_auto_reclaimer.py --stream mystream --group mygroup --consumer repair1 --min-idle 1000`

3) 멱등 처리 전략 비교
- 엔트리 ID 멱등: `idempotent_consumer.py`
- 비즈니스 키 멱등: `idempotent_consumer_by_key.py --field order_id`
- Lua 원자화: `group_consumer_lua.py`, `group_consumer_lua_by_key.py`

4) 성능 실습
- 파이프라인 배치 크기 변화: `--batch 10/100/500` 비교
- `COUNT`/`BLOCK` 파라미터 조절로 처리량/지연 트레이드오프 관찰

## 멱등성 설계 패턴 요약
- 엔트리 ID 기준: `SADD proc:{stream}:{group} entry_id`
  - 장점: 간단/정확. 단점: set이 커질 수 있음 → TTL/트리밍 필요.
- 비즈니스 키 기준: `SADD proc-key:{stream}:{field} value`
  - 장점: 중복 입력에도 1회 처리 보장. 단점: 키 설계/충돌 주의, 실패 시 롤백 필요.
- 원자화: 처리 완료 후 `SADD + XACK`를 Lua로 묶어 원자 실행(부분 실패 방지).
- 보관 정책: `proc*` 세트에 TTL/주기적 SREM(예: 최근 N일 ID만 유지)로 메모리 제어.

## 운영/튜닝 체크리스트
- 트리밍: `XTRIM MAXLEN ~ N`(근사)으로 비용을 낮추고, 필요 시 `MINID`로 시간 기반 유지.
- 그룹 초기화: 백로그부터 처리하려면 `XGROUP CREATE ... 0-0`. 새 항목부터면 `$`.
- 모니터링: `XINFO STREAM/GROUPS/CONSUMERS`, `XPENDING`(idle, count, consumer, first/last ID).
- 재할당 전략: `min-idle` 적절 설정, `XAUTOCLAIM` 배치 크기 튜닝.
- 파이프라인: 네트워크 RTT 절감. 강한 일관성 필요 시 Lua/트랜잭션 사용.
- 장애/정리: 소비자 명은 유니크. 장기간 미사용 컨슈머/그룹 정리(`XGROUP DELCONSUMER/DESTROY`).
- 스토리지: Streams는 영속됨(AOF/RDB). 트래픽이 많으면 AOF 리라이트/디스크 IOPS 고려.
- 클러스터: 스트림 키 단일키 연산. 같은 스트림 키로만 작업하면 클러스터에서도 안전.

## 버전 노트
- 본 저장소 `docker-compose.yml`은 Redis 7을 사용. `XAUTOCLAIM`/`MINID` 등 최신 기능 사용 가능.
