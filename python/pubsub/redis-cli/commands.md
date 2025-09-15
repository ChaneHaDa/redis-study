# redis-cli Pub/Sub 빠른 명령 모음

## 기본 구독/발행
- 구독 시작: `SUBSCRIBE chat:room1`
- 발행: `PUBLISH chat:room1 "hello pubsub"`

## 패턴 구독
- 패턴 구독: `PSUBSCRIBE chat:*`
- 발행 예: `PUBLISH chat:room2 "pattern test"`

## 멀티 채널 구독
- 한 번에 여러 채널: `SUBSCRIBE chat:room1 chat:room2 updates`

## Keyspace Notifications (예: 만료 이벤트)
- 일시 설정: `CONFIG SET notify-keyspace-events Ex`
  - `E`: keyevent, `x`: expired
- 이벤트 구독: `SUBSCRIBE __keyevent@0__:expired`
- 테스트: `SET tmp foo EX 1`

참고: `CONFIG RESETSTAT`로 통계 초기화 가능, `CONFIG GET notify-keyspace-events`로 현재 설정 확인 가능.

