# Redis의 핵심 기능들을 Python으로 구현한 간단한 인메모리 데이터베이스
# 지원하는 기능들:
# - String: SET/GET/INCR/DECR/EXPIRE/TTL
# - Hash: HSET/HGET/HGETALL/HINCRBY
# - List: LPUSH/RPOP/LRANGE/LTRIM
# - Set: SADD/SMEMBERS/SISMEMBER
# - Sorted Set: ZADD/ZREVRANGE/ZINCRBY
# 단일 스레드 환경에서 동작하며, Redis의 동작 방식을 학습하기 위한 교육용 코드입니다.

from __future__ import annotations
from dataclasses import dataclass
from time import time
from typing import Any, Dict, List, Optional, Tuple

@dataclass
class Entry:
    """데이터 저장을 위한 엔트리 클래스"""
    type: str          # 데이터 타입 (string, hash, list, set, zset)
    value: Any         # 실제 데이터 값
    expire_at: Optional[float] = None  # 만료 시간 (Unix timestamp, None이면 만료 없음)


class RedisLite:
    """Redis와 유사한 기능을 제공하는 간단한 인메모리 데이터베이스"""
    
    def __init__(self):
        """데이터 저장소 초기화"""
        self.store: Dict[str, Entry] = {}  # 키-값 저장소

    # -------------- 내부 헬퍼 메서드들 --------------
    
    def _is_expired(self, key: str) -> bool:
        """키가 만료되었는지 확인하고, 만료된 경우 자동 삭제 (지연 삭제)"""
        e = self.store.get(key)
        if not e:
            return False
        if e.expire_at is None:
            return False
        if time() >= e.expire_at:
            # 만료된 키를 지연 삭제 (조회할 때만 삭제)
            del self.store[key]
            return True
        return False

    def _require_type(self, key: str, expected: str) -> Entry:
        """키의 타입을 검증하고, 필요시 빈 컨테이너를 자동 생성"""
        if self._is_expired(key):
            raise KeyError("no such key")
        e = self.store.get(key)
        if not e:
            # 키가 존재하지 않으면 요청된 타입의 빈 컨테이너를 자동 생성
            if expected == "string":
                e = Entry("string", "")
            elif expected == "hash":
                e = Entry("hash", {})
            elif expected == "list":
                e = Entry("list", [])
            elif expected == "set":
                e = Entry("set", set())
            elif expected == "zset":
                e = Entry("zset", {})  # member -> score 매핑
            else:
                raise TypeError("unknown type")
            self.store[key] = e
        if e.type != expected:
            # Redis와 동일한 에러 메시지
            raise TypeError(f"WRONGTYPE Operation against a key holding the wrong kind of value (expected {expected}, got {e.type})")
        return e

    def _set_expire(self, key: str, ex_seconds: Optional[int]):
        """키에 만료 시간 설정"""
        if ex_seconds is not None:
            self.store[key].expire_at = time() + ex_seconds

    # -------------- String 타입 명령어들 --------------
    
    def set(self, key: str, value: str, ex: Optional[int] = None):
        """문자열 값 설정 (SET 명령어)"""
        self.store[key] = Entry("string", str(value))
        self._set_expire(key, ex)

    def get(self, key: str) -> Optional[str]:
        """문자열 값 조회 (GET 명령어)"""
        if self._is_expired(key):
            return None
        e = self.store.get(key)
        return e.value if (e and e.type == "string") else None

    def incr(self, key: str, by: int = 1) -> int:
        """문자열 값을 정수로 해석하여 증가 (INCR 명령어)"""
        e = self._require_type(key, "string")
        try:
            num = int(e.value or "0")
        except ValueError:
            raise ValueError("value is not an integer")
        num += by
        e.value = str(num)
        return num

    def decr(self, key: str, by: int = 1) -> int:
        """문자열 값을 정수로 해석하여 감소 (DECR 명령어)"""
        return self.incr(key, -by)

    def expire(self, key: str, seconds: int) -> bool:
        """키에 만료 시간 설정 (EXPIRE 명령어)"""
        if key in self.store and not self._is_expired(key):
            self.store[key].expire_at = time() + seconds
            return True
        return False

    def ttl(self, key: str) -> int:
        """키의 남은 만료 시간 반환 (TTL 명령어)
        -2: 키가 존재하지 않음, -1: 만료 시간이 설정되지 않음, >=0: 남은 초"""
        if key not in self.store or self._is_expired(key):
            return -2  # Redis: key does not exist
        e = self.store[key]
        if e.expire_at is None:
            return -1  # exists but no expire set
        remaining = int(e.expire_at - time())
        return max(remaining, -2)

    def delete(self, key: str) -> int:
        """키 삭제 (DEL 명령어)"""
        if key in self.store:
            del self.store[key]
            return 1
        return 0

    # -------------- Hash 타입 명령어들 --------------
    
    def hset(self, key: str, mapping: Dict[str, str] | None = None, **kwargs):
        """해시 필드 설정 (HSET 명령어)"""
        e = self._require_type(key, "hash")
        mapping = (mapping or {}) | kwargs
        added = 0
        for f, v in mapping.items():
            if f not in e.value:
                added += 1
            e.value[f] = str(v)
        return added

    def hget(self, key: str, field: str) -> Optional[str]:
        """해시 특정 필드 값 조회 (HGET 명령어)"""
        e = self._require_type(key, "hash")
        return e.value.get(field)

    def hgetall(self, key: str) -> Dict[str, str]:
        """해시의 모든 필드와 값 조회 (HGETALL 명령어)"""
        e = self._require_type(key, "hash")
        return dict(e.value)

    def hincrby(self, key: str, field: str, by: int = 1) -> int:
        """해시 필드 값을 정수로 해석하여 증가 (HINCRBY 명령어)"""
        e = self._require_type(key, "hash")
        cur = int(e.value.get(field, "0"))
        cur += by
        e.value[field] = str(cur)
        return cur

    # -------------- List 타입 명령어들 --------------
    
    def lpush(self, key: str, *values: str) -> int:
        """리스트 앞에 요소 추가 (LPUSH 명령어)"""
        e = self._require_type(key, "list")
        for v in values:
            e.value.insert(0, str(v))
        return len(e.value)

    def rpop(self, key: str) -> Optional[str]:
        """리스트 뒤에서 요소 제거 및 반환 (RPOP 명령어)"""
        e = self._require_type(key, "list")
        if not e.value:
            return None
        return e.value.pop()

    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """리스트 범위 조회 (LRANGE 명령어)"""
        e = self._require_type(key, "list")
        # Redis lrange는 포함적이며 음수 인덱스 지원
        n = len(e.value)
        if start < 0: start = n + start
        if stop < 0: stop = n + stop
        stop = min(stop, n - 1)
        if start < 0: start = 0
        if start > stop: return []
        return e.value[start:stop+1]

    def ltrim(self, key: str, start: int, stop: int):
        """리스트 길이 조정 (LTRIM 명령어)"""
        e = self._require_type(key, "list")
        n = len(e.value)
        if start < 0: start = n + start
        if stop < 0: stop = n + stop
        stop = min(stop, n - 1)
        if start < 0: start = 0
        if start > stop:
            e.value = []
        else:
            e.value = e.value[start:stop+1]

    # -------------- Set 타입 명령어들 --------------
    
    def sadd(self, key: str, *members: str) -> int:
        """집합에 멤버 추가 (SADD 명령어)"""
        e = self._require_type(key, "set")
        before = len(e.value)
        for m in members:
            e.value.add(str(m))
        return len(e.value) - before

    def smembers(self, key: str) -> set[str]:
        """집합의 모든 멤버 조회 (SMEMBERS 명령어)"""
        e = self._require_type(key, "set")
        return set(e.value)

    def sismember(self, key: str, member: str) -> bool:
        """멤버가 집합에 존재하는지 확인 (SISMEMBER 명령어)"""
        e = self._require_type(key, "set")
        return str(member) in e.value

    # -------------- Sorted Set 타입 명령어들 --------------
    
    def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        """정렬된 집합에 멤버와 점수 추가 (ZADD 명령어)"""
        e = self._require_type(key, "zset")
        added = 0
        for member, score in mapping.items():
            if member not in e.value:
                added += 1
            e.value[str(member)] = float(score)
        return added

    def zincrby(self, key: str, delta: float, member: str) -> float:
        """정렬된 집합 멤버의 점수 증가 (ZINCRBY 명령어)"""
        e = self._require_type(key, "zset")
        cur = float(e.value.get(str(member), 0.0))
        cur += float(delta)
        e.value[str(member)] = cur
        return cur

    def zrevrange(self, key: str, start: int, stop: int, withscores: bool = False) -> List[Any]:
        """정렬된 집합을 점수 기준 내림차순으로 조회 (ZREVRANGE 명령어)"""
        e = self._require_type(key, "zset")
        # 점수 기준 내림차순 정렬 (동점인 경우 멤버명으로 정렬)
        items = sorted(e.value.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
        n = len(items)
        if start < 0: start = n + start
        if stop < 0: stop = n + stop
        stop = min(stop, n - 1)
        if start < 0: start = 0
        if start > stop: return []
        slice_ = items[start:stop+1]
        if withscores:
            return [(m, s) for m, s in slice_]
        return [m for m, _ in slice_]

# ------------------- 데모 코드 -------------------
if __name__ == "__main__":
    rd = RedisLite()

    print("== String + TTL ==")
    rd.set("greet", "hello", ex=2)  # 2초 후 만료
    print("GET greet ->", rd.get("greet"))
    print("TTL greet ->", rd.ttl("greet"))

    print("\n== INCR/DECR ==")
    print("INCR count ->", rd.incr("count"))
    print("INCR count ->", rd.incr("count", 10))
    print("DECR count ->", rd.decr("count"))

    print("\n== Hash ==")
    rd.hset("user:42", name="해찬", role="backend")
    rd.hincrby("user:42", "posts", 1)
    print("HGETALL user:42 ->", rd.hgetall("user:42"))

    print("\n== List (recent 5) ==")
    for i in range(1, 8):
        rd.lpush("recent:42", f"post-{i}")
        rd.ltrim("recent:42", 0, 4)  # 최근 5개만 유지
    print("LRANGE recent:42 0 -1 ->", rd.lrange("recent:42", 0, -1))

    print("\n== Set ==")
    rd.sadd("viewed:99", "u1", "u2", "u2")  # 중복된 u2는 무시됨
    print("SMEMBERS viewed:99 ->", rd.smembers("viewed:99"))
    print("SISMEMBER viewed:99 u3 ->", rd.sismember("viewed:99", "u3"))

    print("\n== Sorted Set (ranking) ==")
    rd.zadd("rank:game", {"alice": 30, "bob": 50})
    rd.zincrby("rank:game", 15, "carol")
    print("ZREVRANGE rank:game 0 2 WITHSCORES ->", rd.zrevrange("rank:game", 0, 2, withscores=True))

    print("\n== Naive coupon feel (not safe for race) ==")
    # 단순한 쿠폰 카운터 예제 (실제로는 WATCH/MULTI나 Lua로 레이스 컨디션 방지 필요)
    for _ in range(5):
        rd.incr("coupon:count")
    print("coupon:count ->", rd.get("coupon:count"))
