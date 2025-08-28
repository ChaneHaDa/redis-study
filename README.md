# Redis 학습 프로젝트

## 시작하기

### Redis 서버 실행
```bash
docker-compose up -d
```

### Redis CLI 접속
```bash
docker exec -it redis-study redis-cli
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