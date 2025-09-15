# Redis Pub/Sub 실습 가이드

## 개념 요약
- Pub/Sub: 발행자(`PUBLISH`)가 채널에 메시지를 보내면, 해당 채널을 구독(`SUBSCRIBE`) 중인 클라이언트가 실시간 수신.
- 브로드캐스트: 메시지는 저장되지 않음. 발행 시점에 구독 중인 클라이언트만 수신.
- 특성: 매우 빠르고 가벼움. 대신 내구성/재전송/ACK 없음(유실 가능).
- 순서: 한 구독자와 특정 채널 내에서는 입력 순서대로 전달되는 특성이 있으나, 여러 채널 간 전역 순서 보장은 없음.

## 언제 쓰면 좋을까
- 실시간 알림: 채팅방 이벤트, 거래 체결 신호, 푸시 알림 팬아웃.
- 분산 서버 간 이벤트 브로드캐스트: 캐시 무효화, 설정 변경 알림 등.
- 단, 메시지 보존/재처리가 중요하면 Streams/전용 MQ를 고려.

## 빠른 체험 (redis-cli)
두 터미널을 엽니다. 하나는 구독, 다른 하나는 발행에 사용합니다.

1) 구독 터미널
- 컨테이너에 바로 접속: `docker exec -it redis-study redis-cli`
- 채널 구독: `SUBSCRIBE chat:room1`
- 패턴 구독: `PSUBSCRIBE chat:*`

2) 발행 터미널
- 컨테이너에 바로 접속: `docker exec -it redis-study redis-cli`
- 메시지 발행: `PUBLISH chat:room1 "hello pubsub"`

메시지 보존이 없으므로, 반드시 먼저 구독을 시작한 뒤 발행하세요.

## Python 예제 실행
- 위치: `python/pubsub` 폴더
- 요구 사항: Python 3.9+, `pip install -r python/pubsub/requirements.txt`
- Redis 연결: 기본 `127.0.0.1:6379` 또는 `REDIS_URL` 환경변수 사용

예제 1) 일반 구독/발행
- 폴더 이동: `cd python/pubsub`
- 구독 시작: `python3 subscriber.py chat:room1 chat:room2`
- 발행 예: `python3 publisher.py --channel chat:room1 --message "hello" --count 3 --interval 1.0`

예제 2) 패턴 구독
- 패턴 구독: `python3 pattern_subscriber.py 'chat:*'`
- 발행 예: `python3 publisher.py --channel chat:room1 --message "pattern test"`

예제 3) Keyspace Notifications (만료 이벤트 등)
- 일시 설정: `docker exec -it redis-study redis-cli CONFIG SET notify-keyspace-events Ex`
  - `E`: Keyevent events, `x`: expired
- 구독: `python keyspace_subscriber.py --db 0`
- 테스트: `docker exec -it redis-study redis-cli SET tmp foo EX 1`
  - 1초 뒤 `expired` 이벤트가 수신됩니다.

## 주의사항과 팁
- 내구성 없음: 소비자가 없으면 메시지는 사라짐. 중요 데이터는 Pub/Sub에만 의존하지 말 것.
- 백프레셔 없음: 발행 속도를 소비자가 따라가지 못하면 드롭/버퍼 이슈 가능(클라이언트별 버퍼 제한 고려).
- 다중 채널: 구독 측에서 여러 채널/패턴을 동시에 구독하면 메시지 분배 로직을 명확히 설계.
- 운영 팁: 채널 네이밍 컨벤션(예: `app:env:topic`)을 정해 관리. 모니터링/로그와 함께 사용.

## Pub/Sub vs Redis Streams 요약
- Pub/Sub: 초저지연 브로드캐스트, 비영속, 재처리 불가, 소비자 그룹 없음.
- Streams: 영속 큐, 컨슈머 그룹/ACK/재전송 지원, 순번 관리, 지연은 다소 증가.
- “실시간 신호”는 Pub/Sub, “보존/재처리/워커풀”은 Streams에 적합.
