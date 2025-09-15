# redis-cli Streams 빠른 명령 모음

## 기본 쓰기/읽기
- 쓰기: `XADD mystream * type=order id=1001`
- 전체 조회: `XRANGE mystream - +`
- 최신에서 거꾸로: `XREVRANGE mystream + - COUNT 10`
- 블로킹 읽기: `XREAD BLOCK 0 STREAMS mystream $`

## 트리밍(보존 관리)
- 대략 길이 유지: `XTRIM mystream MAXLEN ~ 1000`
- 정확히 길이 유지(느림): `XTRIM mystream MAXLEN = 1000`
- 최소 ID 기준: `XTRIM mystream MINID ~ 1694760000000-0`

## 컨슈머 그룹
- 생성: `XGROUP CREATE mystream mygroup $ MKSTREAM`
- 읽기: `XREADGROUP GROUP mygroup c1 COUNT 10 BLOCK 0 STREAMS mystream >`
- 처리 완료: `XACK mystream mygroup <id>`
- 미처리 조회: `XPENDING mystream mygroup`
- 재할당: `XCLAIM mystream mygroup c2 60000 <id1> <id2>`
- 자동 재할당(6.2+): `XAUTOCLAIM mystream mygroup c2 60000 0-0 COUNT 10`

## 정보 조회
- 스트림: `XINFO STREAM mystream`
- 그룹: `XINFO GROUPS mystream`
- 컨슈머: `XINFO CONSUMERS mystream mygroup`

