# Redis Study Spring Boot

Redis 학습을 위한 Spring Boot 애플리케이션입니다.

## 프로젝트 구조
```
spring/
├── src/main/java/com/redis/study/
│   ├── RedisStudySpringApplication.java    # 메인 애플리케이션
│   ├── controller/
│   │   └── RedisController.java            # Redis 연동 API
│   └── config/
│       └── SwaggerConfig.java              # Swagger 설정
├── src/main/resources/
│   └── application.yml                      # 애플리케이션 설정
└── build.gradle                            # Gradle 설정
```

## 실행 방법

### 1. Redis 실행
먼저 Docker Compose로 Redis를 실행합니다:
```bash
docker-compose up -d
```

### 2. Spring Boot 애플리케이션 실행
```bash
cd spring
./gradlew bootRun
```

### 3. API 확인
- 애플리케이션: http://localhost:8080
- Swagger UI: http://localhost:8080/swagger-ui.html

## 주요 API

### 기본 키-값 연산
- `POST /api/redis/set` - 키-값 저장
- `GET /api/redis/get/{key}` - 값 조회
- `DELETE /api/redis/delete/{key}` - 키 삭제
- `GET /api/redis/exists/{key}` - 키 존재 여부 확인
- `GET /api/redis/keys` - 모든 키 조회

### Hash 연산
- `POST /api/redis/hash/set` - Hash 필드 설정
- `GET /api/redis/hash/get/{key}` - Hash 조회

## Redis Insight
Redis 데이터를 시각적으로 확인하려면:
http://localhost:5540

## 예제 사용법

### 값 저장
```bash
curl -X POST "http://localhost:8080/api/redis/set?key=test&value=hello&expireSeconds=300"
```

### 값 조회
```bash
curl -X GET "http://localhost:8080/api/redis/get/test"
```
