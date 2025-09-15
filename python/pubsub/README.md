# Redis Pub/Sub 실습 가이드

## 개념 요약
- Pub/Sub: 발행자(`PUBLISH`)가 채널에 메시지를 보내면, 해당 채널을 구독(`SUBSCRIBE`) 중인 클라이언트가 실시간 수신.
- 브로드캐스트: 메시지는 저장되지 않음. 발행 시점에 구독 중인 클라이언트만 수신.
- 특성: 매우 빠르고 가벼움. 대신 내구성/재전송/ACK 없음(유실 가능).
- 순서: 한 구독자와 특정 채널 내에서는 입력 순서 유지. 여러 채널 간 전역 순서는 보장되지 않음.

## 내부 동작 한눈에
- `SUBSCRIBE channel` 시 서버가 연결을 Pub/Sub 모드로 전환하고 채널에 바인딩합니다.
- 발행(`PUBLISH ch payload`) 시점에 해당 채널을 구독 중인 연결로 즉시 팬아웃합니다.
- 클라이언트 버퍼가 꽉 차거나 연결이 불안정하면 메시지 드롭/연결 종료가 발생할 수 있습니다.

## 언제 쓰면 좋을까
- 실시간 알림/신호: 채팅 타이핑 알림, 가격 틱, 간단한 상태 브로드캐스트.
- 서버 간 브로드캐스트: 캐시 무효화, 기능 플래그 변경, 구성 동기화.
- 반대로, 보존/재처리가 필요하면 Streams/전용 MQ를 사용하세요.

## 채널/패턴 설계 가이드
- 네이밍: `app:env:topic[:detail]` 형태 권장(예: `shop:prod:cache:invalidate`).
- 멀티 테넌시: `tenant:{id}:topic`으로 분리해 과도한 팬아웃을 막습니다.
- 패턴 구독: `PSUBSCRIBE chat:*`로 넓게 받되, 애플리케이션에서 필터링을 고려.
- 역방향 흐름 방지: 발행과 구독 채널을 구분해 자기 자신이 보낸 메시지를 재수신하지 않도록 설계.

## 실습(redis-cli)
두 터미널을 엽니다. 하나는 구독, 다른 하나는 발행에 사용합니다.

1) 구독 터미널
- 컨테이너 접속: `docker exec -it redis-study redis-cli`
- 채널 구독: `SUBSCRIBE chat:room1`
- 패턴 구독: `PSUBSCRIBE chat:*`

2) 발행 터미널
- 컨테이너 접속: `docker exec -it redis-study redis-cli`
- 메시지 발행: `PUBLISH chat:room1 "hello pubsub"`

주의: 메시지 보존이 없으므로, 반드시 먼저 구독을 시작한 뒤 발행하세요.

더 많은 명령: `python/pubsub/redis-cli/commands.md`

## Python 예제 실행
- 위치: `python/pubsub`
- 요구 사항: Python 3.9+, `pip install -r python/pubsub/requirements.txt`
- Redis 연결: 기본 `127.0.0.1:6379` 또는 `REDIS_URL`

예제 1) 기본 구독/발행
- 폴더 이동: `cd python/pubsub`
- 구독: `python subscriber.py chat:room1 chat:room2`
- 발행: `python publisher.py --channel chat:room1 --message "hello" --count 3 --interval 1.0`

예제 2) 패턴 구독
- 패턴 구독: `python pattern_subscriber.py 'chat:*'`
- 발행 예: `python publisher.py --channel chat:room1 --message "pattern test"`

예제 3) Keyspace Notifications (만료 이벤트 등)
- 일시 설정: `docker exec -it redis-study redis-cli CONFIG SET notify-keyspace-events Ex`
  - `E`: keyevent, `x`: expired
- 구독: `python keyspace_subscriber.py --db 0`
- 테스트: `docker exec -it redis-study redis-cli SET tmp foo EX 1`
  - 1초 뒤 `expired` 이벤트 수신

## 실전 팁/주의사항
- 내구성 없음: 소비자가 없으면 메시지가 사라집니다. 중요 데이터는 Pub/Sub에만 의존하지 마세요.
- 백프레셔 없음: 발행 속도가 소비자 처리 속도를 초과하면 버퍼 초과/드롭이 발생할 수 있습니다.
- 소비자 복수: 여러 소비자가 동일 채널을 구독하면 모두 수신합니다(브로드캐스트). 워커풀 분산 처리에는 부적합.
- 모니터링: 채널 수/구독자 수는 `PUBSUB NUMSUB`, `PUBSUB CHANNELS`로 점검 가능.
- 네트워크/클라이언트 버퍼: 메시지 크기/빈도 제한을 설정하고, 큰 페이로드는 키 참조로 대체(예: `PUBLISH ch {key}` + 별도 GET).

## Pub/Sub vs Redis Streams 요약
- Pub/Sub: 초저지연 브로드캐스트, 비영속, 재처리/ACK 없음, 소비자 그룹 없음.
- Streams: 영속 큐, 컨슈머 그룹/ACK/재전송, 순번/오프셋 관리, 약간 더 높은 지연.
- 선택 기준: “신호/알림/팬아웃”은 Pub/Sub, “작업 큐/보존/재처리”는 Streams.

## Hands-on 랩 시나리오
1) 채팅 알림 미니랩
- `subscriber.py`를 두 개 실행하여 `chat:room1`을 구독
- `publisher.py --channel chat:room1 --count 5`로 발행 → 두 구독자 모두 동일 메시지 수신 확인

2) 패턴 구독 라우팅
- `pattern_subscriber.py 'chat:*'`로 넓게 구독
- `publisher.py`로 `chat:room1`, `chat:room2`에 발행 → 패턴 구독자가 모두 수신, 특정 채널 구독자는 해당 채널만 수신

3) Keyspace Notifications로 만료 이벤트 처리
- `CONFIG SET notify-keyspace-events Ex`
- `keyspace_subscriber.py --db 0` 실행
- `SET tmp foo EX 1` → 1초 뒤 `__keyevent@0__:expired` 수신 로직 확인

4) 안정성 패턴 체험
- 큰 페이로드는 키 참조를 발행하고 소비자가 직접 조회하도록 구현해 드롭 위험 감소(연습 문제)
- 중요 알림은 Pub/Sub + Streams 이중화: Pub/Sub로 즉시 반응, Streams로 보존/재처리(설계 토의)

## 운영 체크리스트
- 채널 네이밍: `app:env:topic` 컨벤션으로 관리/필터링 용이성 확보.
- 리소스: 과도한 채널/구독자 수는 메모리/CPU에 영향. 정리 주기 설정.
- 보안: 인증/권한(ACL)으로 민감 채널 접근 제한.
- 관찰: `MONITOR`는 부하가 크므로 개발/장애 분석 시에만. 운영은 `PUBSUB` 계열과 외부 로깅 활용.
