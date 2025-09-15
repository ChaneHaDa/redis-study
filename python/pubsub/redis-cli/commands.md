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

## 운영 관련 명령
- 구독 중 채널 목록: `PUBSUB CHANNELS`
- 채널별 구독자 수: `PUBSUB NUMSUB chat:room1 chat:room2`
- 패턴 구독자 수: `PUBSUB NUMPAT`
- 설정 확인: `CONFIG GET notify-keyspace-events`
- 통계 초기화: `CONFIG RESETSTAT`

주의: `MONITOR`는 서버 부하가 크므로 개발/장애 분석 시에만 사용.
