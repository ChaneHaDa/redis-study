# Redis Redlock 실습 가이드

## 개념 요약
- **목적**: 단일 Redis 노드의 장애 상황에서도 분산 락의 안정성을 보장하기 위해, 여러 개의 독립적인 Redis 마스터에 락을 거는 알고리즘입니다.
- **동작 원리**: N개의 Redis 마스터 중 과반수(N/2 + 1) 이상에서 락을 성공적으로 획득했을 때만 락이 유효한 것으로 간주합니다. 획득에 실패하면 모든 노드에 락 해제를 시도합니다.
- **장점**: 단일 Redis 인스턴스 기반 락보다 고가용성을 제공합니다.
- **단점/논란**: 시스템 클럭 동기화, GC pause, 네트워크 지연 등 복잡한 시나리오에서 안정성이 깨질 수 있다는 비판이 있습니다. 사용 전 알고리즘의 한계를 충분히 이해해야 합니다.

## 1. Redlock 환경 실행

이 실습은 3개의 독립적인 Redis 마스터 노드를 사용합니다. `docker-compose.redlock.yml` 파일을 이용해 3개의 Redis 컨테이너를 실행합니다.

```bash
# -f 옵션으로 redlock용 docker-compose 파일을 지정하여 실행
docker-compose -f docker-compose.redlock.yml up -d
```

실행 후, 각 Redis 인스턴스는 다음 포트로 접근 가능합니다.
- `localhost:6380`
- `localhost:6381`
- `localhost:6382`

## 2. Python 예제 실행

- **위치**: `python/redlock`
- **의존성**: `pip install -r python/redlock/requirements.txt`

### Redlock 데모 실행

`redlock_demo.py` 스크립트를 실행하여 Redlock 획득/해제 과정을 테스트합니다. `--urls` 인자로 3개 마스터의 주소를 전달해야 합니다.

```bash
python redlock_demo.py \
  --resource my-shared-resource \
  --urls "redis://localhost:6380,redis://localhost:6381,redis://localhost:6382" \
  --work-ms 3000
```

- 다른 터미널에서 동일한 리소스 이름으로 위 명령을 실행하면, 첫 번째 락이 해제될 때까지 획득에 실패하는 것을 확인할 수 있습니다.

### 환경 변수 사용

`--urls` 인자 대신 `REDIS_URLS` 환경 변수를 사용할 수도 있습니다.

```bash
export REDIS_URLS="redis://localhost:6380,redis://localhost:6381,redis://localhost:6382"
python redlock_demo.py --resource my-shared-resource
```

## 3. 환경 종료

실습이 끝나면 다음 명령어로 컨테이너를 중지하고 삭제합니다.

```bash
docker-compose -f docker-compose.redlock.yml down
```

