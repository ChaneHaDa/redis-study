# redis_basics.py
import redis
from redis.exceptions import WatchError

# 1) 연결
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# ---------- Hash 기본 (작은 객체/Map) ----------
def set_user(user_id: int, **fields):
    key = f"user:{user_id}"
    # HSET user:1 name "Haechan" role "backend" 처럼
    if fields:
        r.hset(key, mapping=fields)

def get_user(user_id: int):
    return r.hgetall(f"user:{user_id}")

# ---------- 딱 100장: WATCH/MULTI/EXEC (Lua 없이 안전) ----------
def try_issue_tx(key: str, limit: int, max_retries: int = 10):
    """
    - key: "coupon:today:count" 처럼 누적 카운터 키
    - limit: 100 등
    - 반환: (ok, seq_or_curr)
        ok=True  → 발급 성공, seq_or_curr=발급된 순번(1..limit)
        ok=False → 매진, seq_or_curr=현재 누적 수(>=limit)
    """
    for _ in range(max_retries):
        pipe = r.pipeline()
        try:
            pipe.watch(key)                         # 다른 변경 감시
            curr = int(r.get(key) or "0")
            if curr >= limit:
                pipe.unwatch()
                return False, curr                  # 매진

            pipe.multi()                            # 트랜잭션 시작
            pipe.incr(key)                          # INCR
            res = pipe.execute()                    # 커밋 (충돌 시 WatchError)
            next_val = int(res[0])                  # INCR 결과
            return True, next_val
        except WatchError:
            # 경합 발생 → 재시도
            continue
        finally:
            pipe.reset()

    # 재시도 초과
    return False, int(r.get(key) or "0")

# ---------- 딱 100장: DECR 방식 (간단; 원복 필수) ----------
def try_issue_decr(key_remain: str):
    """
    - key_remain: "coupon:today:remain" 처럼 남은 수량 키 (초기값 100)
    - 반환: (ok, remaining_after)
        ok=True  → 성공, remaining_after=발급 후 남은 수량(>=0)
        ok=False → 실패, remaining_after=음수(즉시 INCR로 복구)
    """
    val = r.decr(key_remain)          # 남은 수량 1 감소
    if val >= 0:
        return True, val              # 성공
    # 음수면 초과 → 즉시 복구
    r.incr(key_remain)
    return False, val

# ---------- 랭킹 보드 (ZSET) ----------
def add_score(board: str, member: str, delta: float):
    r.zincrby(f"rank:{board}", delta, member)

def top_n(board: str, n: int = 3):
    # 높은 점수 우선
    return r.zrevrange(f"rank:{board}", 0, n - 1, withscores=True)

# ---------- 간단 실행 데모 ----------
if __name__ == "__main__":
    print("== Hash 기초 ==")
    set_user(42, name="강해찬", role="backend", level="junior")
    print(get_user(42))  # {'name': '강해찬', 'role': 'backend', 'level': 'junior'}

    print("\n== 쿠폰 발급(트랜잭션 방식) ==")
    counter_key = "coupon:today:count"
    r.delete(counter_key)
    limit = 5
    for _ in range(7):  # 7번 시도해도 5번만 성공
        ok, v = try_issue_tx(counter_key, limit)
        print("issue_tx:", ok, v)
    print("final count:", r.get(counter_key))  # 5

    print("\n== 쿠폰 발급(DECR 방식) ==")
    remain_key = "coupon:today:remain"
    r.set(remain_key, 3)
    for _ in range(5):  # 5번 시도해도 3번만 성공
        ok, v = try_issue_decr(remain_key)
        print("issue_decr:", ok, v)
    print("final remain:", r.get(remain_key))  # 0 또는 복구 덕분에 정확

    print("\n== 랭킹(ZSET) ==")
    board = "game"
    r.delete(f"rank:{board}")
    add_score(board, "alice", 30)
    add_score(board, "bob", 50)
    add_score(board, "carol", 40)
    print(top_n(board, 3))  # [('bob', 50.0), ('carol', 40.0), ('alice', 30.0)]
