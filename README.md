# Redis 학습 프로젝트

## 시작하기

### Redis 서버 실행
```bash
docker-compose up -d
```

### Redis CLI 접속
```bash
# 대화형 CLI 접속
docker exec -it redis-study redis-cli

# 일회성 명령어 실행
docker exec redis-study redis-cli PING
docker exec redis-study redis-cli SET key "value"
docker exec redis-study redis-cli GET key
```

### Redis Insight GUI 접속
브라우저에서 `http://localhost:5540` 접속

### Redis 서버 중지
```bash
docker-compose down
```

## 포트 정보
- Redis Server: `localhost:6379`
- Redis Insight: `http://localhost:5540`

## 학습 주제 바로가기
- Pub/Sub 실습: `python/pubsub/README.md`
- Streams 실습: `python/streams/README.md`
- Distributed Lock 실습: `python/locks/README.md`
