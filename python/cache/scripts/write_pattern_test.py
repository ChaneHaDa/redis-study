#!/usr/bin/env python3
"""
쓰기 패턴 비교 테스트 스크립트 - Lab 4용

3가지 쓰기 패턴 (Invalidation, Write-Through, Write-Behind)의 
일관성과 stale read 빈도를 비교 측정

사용 예시:
python3 scripts/write_pattern_test.py --url http://localhost:8000/api/v1/products --product-id 1 --duration 30s --read-ratio 0.8
"""

import argparse
import asyncio
import json
import logging
import random
import sys
import threading
import time
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import statistics

import aiohttp

# 상위 모듈 import를 위한 path 설정
sys.path.append(str(Path(__file__).parent))


@dataclass
class TestResult:
    """테스트 결과를 저장하는 데이터 클래스"""
    operation: str  # 'read' or 'write'
    status_code: int
    response_time: float
    product_id: int
    price_value: Optional[float] = None  # 읽기 시 받은 가격
    came_from_cache: Optional[bool] = None  # 캐시에서 왔는지 여부 (응답시간으로 추정)
    timestamp: float = 0.0
    request_id: int = 0
    error: Optional[str] = None


class WritePatternTester:
    """쓰기 패턴 비교 테스트 클래스"""
    
    def __init__(self, base_url: str, product_id: int, read_ratio: float):
        self.base_url = base_url.rstrip('/')
        self.product_id = product_id
        self.read_ratio = read_ratio  # 읽기 작업 비율 (0.0~1.0)
        self.results: List[TestResult] = []
        self.request_counter = 0
        self.counter_lock = threading.Lock()  # request_counter 동시성 보호
        self.results_dir = None
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """로거 설정"""
        # 결과 폴더 생성
        self.results_dir = Path('results') / f"write_pattern_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger('write_pattern_test')
        logger.setLevel(logging.INFO)
        
        # 파일 핸들러 설정
        log_file = self.results_dir / 'write_pattern_test.log'
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # 기존 핸들러 제거 후 추가
        logger.handlers.clear()
        logger.addHandler(handler)
        
        return logger
    
    async def set_write_pattern(self, pattern: str) -> bool:
        """서버의 쓰기 패턴 설정"""
        try:
            config_url = f"{self.base_url}/config/write-pattern"
            timeout = aiohttp.ClientTimeout(total=5)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(config_url, params={"pattern": pattern}) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.logger.info(f"Write pattern set to: {result['pattern']}")
                        return True
                    else:
                        self.logger.error(f"Failed to set write pattern: {response.status}")
                        return False
        except Exception as e:
            self.logger.error(f"Error setting write pattern: {e}")
            return False
    
    async def get_current_price(self) -> Optional[float]:
        """현재 제품 가격 조회"""
        try:
            product_url = f"{self.base_url}/{self.product_id}"
            timeout = aiohttp.ClientTimeout(total=5)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(product_url) as response:
                    if response.status == 200:
                        product_data = await response.json()
                        return float(product_data['price'])
                    return None
        except Exception as e:
            self.logger.error(f"Error getting current price: {e}")
            return None
    
    async def make_read_request(self, session: aiohttp.ClientSession) -> TestResult:
        """읽기 요청 실행"""
        with self.counter_lock:
            self.request_counter += 1
            request_id = self.request_counter
        
        timestamp = time.time()
        start = time.time()
        
        product_url = f"{self.base_url}/{self.product_id}"
        
        try:
            async with session.get(product_url) as response:
                response_data = await response.json()
                end = time.time()
                response_time = end - start
                
                price_value = float(response_data['price']) if response.status == 200 else None
                
                # 캐시 히트 추정: 빠른 응답시간(15ms 미만)은 캐시에서 온 것으로 판단
                came_from_cache = response_time < 0.015  # 15ms
                
                cache_source = "CACHE" if came_from_cache else "DB"
                self.logger.info(f"READ - Request {request_id}: {price_value} from {cache_source} ({response_time*1000:.2f}ms)")
                
                return TestResult(
                    operation="read",
                    status_code=response.status,
                    response_time=response_time,
                    product_id=self.product_id,
                    price_value=price_value,
                    came_from_cache=came_from_cache,
                    timestamp=timestamp,
                    request_id=request_id
                )
        except Exception as e:
            end = time.time()
            response_time = end - start
            
            return TestResult(
                operation="read",
                status_code=0,
                response_time=response_time,
                product_id=self.product_id,
                error=str(e),
                timestamp=timestamp,
                request_id=request_id
            )
    
    async def make_write_request(self, session: aiohttp.ClientSession) -> TestResult:
        """쓰기 요청 실행"""
        with self.counter_lock:
            self.request_counter += 1
            request_id = self.request_counter
        
        timestamp = time.time()
        start = time.time()
        
        # 새로운 가격 생성 (50~150 범위)
        new_price = round(random.uniform(50.0, 150.0), 2)
        
        product_url = f"{self.base_url}/{self.product_id}"
        payload = {"price": new_price}
        
        try:
            async with session.put(product_url, json=payload) as response:
                if response.status == 200:
                    await response.json()
                end = time.time()
                response_time = end - start
                
                # 쓰기 성공 시 로깅
                if response.status == 200:
                    self.logger.info(f"WRITE - Request {request_id}: Updated price to {new_price} ({response_time*1000:.2f}ms)")
                
                return TestResult(
                    operation="write",
                    status_code=response.status,
                    response_time=response_time,
                    product_id=self.product_id,
                    price_value=new_price,
                    timestamp=timestamp,
                    request_id=request_id
                )
        except Exception as e:
            end = time.time()
            response_time = end - start
            
            return TestResult(
                operation="write",
                status_code=0,
                response_time=response_time,
                product_id=self.product_id,
                error=str(e),
                timestamp=timestamp,
                request_id=request_id
            )
    
    
    async def worker(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, 
                    stop_event: asyncio.Event):
        """워커 코루틴 - 읽기/쓰기 작업을 비율에 따라 실행"""
        while not stop_event.is_set():
            async with semaphore:
                if stop_event.is_set():
                    break
                
                # 읽기/쓰기 작업 결정
                if random.random() < self.read_ratio:
                    result = await self.make_read_request(session)
                else:
                    result = await self.make_write_request(session)
                
                self.results.append(result)
    
    async def run_test(self, duration: int, concurrency: int, write_pattern: str):
        """쓰기 패턴별 테스트 실행"""
        self.logger.info(f"Starting test with pattern: {write_pattern}")
        
        # 1. 쓰기 패턴 설정
        if not await self.set_write_pattern(write_pattern):
            raise Exception(f"Failed to set write pattern: {write_pattern}")
        
        # 2. 초기 가격 설정 및 확인
        initial_price = await self.get_current_price()
        if initial_price is not None:
            self.current_expected_price = initial_price
            self.logger.info(f"Initial price: {initial_price}")
        
        # 3. 테스트 실행
        semaphore = asyncio.Semaphore(concurrency)
        stop_event = asyncio.Event()
        
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=concurrency * 2)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # 워커 태스크들 생성
            workers = [
                asyncio.create_task(self.worker(session, semaphore, stop_event))
                for _ in range(concurrency)
            ]
            
            start_time = time.time()
            
            # 지정된 시간 대기
            await asyncio.sleep(duration)
            
            # 모든 워커 중지
            stop_event.set()
            
            # 모든 워커 완료 대기
            await asyncio.gather(*workers, return_exceptions=True)
            
            end_time = time.time()
        
        self.logger.info(f"Test completed in {end_time - start_time:.2f} seconds")
        return end_time - start_time
    
    def analyze_results(self, write_pattern: str) -> dict:
        """결과 분석 - 캐시 히트율과 성능 중심"""
        if not self.results:
            return {}
        
        # 읽기/쓰기 작업 분리
        reads = [r for r in self.results if r.operation == "read" and r.error is None]
        writes = [r for r in self.results if r.operation == "write" and r.error is None]
        
        # 캐시 히트 분석
        successful_reads = [r for r in reads if r.status_code == 200]
        cache_hits = [r for r in successful_reads if r.came_from_cache]
        db_reads = [r for r in successful_reads if not r.came_from_cache]
        
        cache_hit_count = len(cache_hits)
        total_read_count = len(successful_reads)
        cache_hit_rate = (cache_hit_count / total_read_count * 100) if total_read_count > 0 else 0
        
        # 응답 시간 분석
        read_times = [r.response_time for r in successful_reads]
        write_times = [r.response_time for r in writes if r.status_code == 200]
        cache_read_times = [r.response_time for r in cache_hits]
        db_read_times = [r.response_time for r in db_reads]
        
        analysis = {
            "write_pattern": write_pattern,
            "total_requests": len(self.results),
            "reads": {
                "count": len(reads),
                "successful": total_read_count,
                "cache_hits": cache_hit_count,
                "db_reads": len(db_reads),
                "cache_hit_rate_percent": round(cache_hit_rate, 2),
                "avg_response_time_ms": round(statistics.mean(read_times) * 1000, 2) if read_times else 0,
                "p95_response_time_ms": round(sorted(read_times)[int(len(read_times) * 0.95)] * 1000, 2) if read_times else 0,
                "cache_avg_ms": round(statistics.mean(cache_read_times) * 1000, 2) if cache_read_times else 0,
                "db_avg_ms": round(statistics.mean(db_read_times) * 1000, 2) if db_read_times else 0
            },
            "writes": {
                "count": len(writes),
                "successful": len([w for w in writes if w.status_code == 200]),
                "avg_response_time_ms": round(statistics.mean(write_times) * 1000, 2) if write_times else 0,
                "p95_response_time_ms": round(sorted(write_times)[int(len(write_times) * 0.95)] * 1000, 2) if write_times else 0
            }
        }
        
        return analysis
    
    def save_results(self, analysis: dict):
        """결과 저장"""
        # 분석 결과 저장
        analysis_file = self.results_dir / f"{analysis['write_pattern']}_analysis.json"
        with open(analysis_file, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
        
        # 상세 결과 저장
        detailed_results = {
            "analysis": analysis,
            "raw_results": [
                {
                    "request_id": r.request_id,
                    "operation": r.operation,
                    "timestamp": r.timestamp,
                    "status_code": r.status_code,
                    "response_time_ms": round(r.response_time * 1000, 2),
                    "price_value": r.price_value,
                    "came_from_cache": r.came_from_cache,
                    "error": r.error
                }
                for r in self.results
            ]
        }
        
        detailed_file = self.results_dir / f"{analysis['write_pattern']}_detailed.json"
        with open(detailed_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_results, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Results saved to {self.results_dir}")
    
    def print_analysis(self, analysis: dict):
        """분석 결과 출력"""
        pattern = analysis['write_pattern']
        reads = analysis['reads']
        writes = analysis['writes']
        
        print(f"\n{'='*60}")
        print(f"🔄 쓰기 패턴: {pattern.upper()}")
        print(f"{'='*60}")
        print(f"총 요청: {analysis['total_requests']}")
        
        print(f"\n📖 읽기 작업:")
        print(f"  총 읽기: {reads['count']}회 (성공: {reads['successful']}회)")
        print(f"  캐시 히트: {reads['cache_hits']}회 ({reads['cache_hit_rate_percent']}%)")
        print(f"  DB 읽기: {reads['db_reads']}회")
        print(f"  평균 응답시간: {reads['avg_response_time_ms']}ms")
        print(f"  P95 응답시간: {reads['p95_response_time_ms']}ms")
        print(f"  캐시 평균: {reads['cache_avg_ms']}ms, DB 평균: {reads['db_avg_ms']}ms")
        
        print(f"\n✏️ 쓰기 작업:")
        print(f"  총 쓰기: {writes['count']}회 (성공: {writes['successful']}회)")
        print(f"  평균 응답시간: {writes['avg_response_time_ms']}ms")
        print(f"  P95 응답시간: {writes['p95_response_time_ms']}ms")


def parse_duration(duration_str: str) -> int:
    """지속시간 문자열을 초단위로 변환"""
    if duration_str.endswith('s'):
        return int(duration_str[:-1])
    elif duration_str.endswith('m'):
        return int(duration_str[:-1]) * 60
    elif duration_str.endswith('h'):
        return int(duration_str[:-1]) * 3600
    else:
        return int(duration_str)


async def main():
    parser = argparse.ArgumentParser(description='쓰기 패턴 비교 테스트 도구')
    parser.add_argument('--url', required=True, help='베이스 URL (제품 ID 없이)')
    parser.add_argument('--product-id', type=int, default=1, help='테스트할 제품 ID (기본값: 1)')
    parser.add_argument('--duration', '-d', default='30s', help='테스트 지속시간 (기본값: 30s)')
    parser.add_argument('--concurrency', '-c', type=int, default=20, help='동시 연결 수 (기본값: 20)')
    parser.add_argument('--read-ratio', type=float, default=0.8, help='읽기 작업 비율 (0.0~1.0, 기본값: 0.8)')
    parser.add_argument('--patterns', nargs='+', 
                       choices=['invalidation', 'write_through', 'write_behind'],
                       default=['invalidation', 'write_through', 'write_behind'],
                       help='테스트할 쓰기 패턴들')
    
    args = parser.parse_args()
    duration_seconds = parse_duration(args.duration)
    
    print(f"🚀 쓰기 패턴 비교 테스트 시작")
    print(f"URL: {args.url}")
    print(f"제품 ID: {args.product_id}")
    print(f"지속시간: {duration_seconds}초")
    print(f"동시성: {args.concurrency}")
    print(f"읽기 비율: {args.read_ratio}")
    print(f"테스트 패턴: {args.patterns}")
    
    all_analyses = []
    
    for pattern in args.patterns:
        print(f"\n🔧 Testing {pattern} pattern...")
        
        tester = WritePatternTester(args.url, args.product_id, args.read_ratio)
        
        try:
            await tester.run_test(duration_seconds, args.concurrency, pattern)
            analysis = tester.analyze_results(pattern)
            tester.save_results(analysis)
            tester.print_analysis(analysis)
            all_analyses.append(analysis)
            
            # 패턴 간 간격
            if pattern != args.patterns[-1]:
                print(f"\n⏳ Waiting 5 seconds before next pattern...")
                await asyncio.sleep(5)
                
        except KeyboardInterrupt:
            print(f"\n테스트가 중단되었습니다.")
            break
        except Exception as e:
            print(f"Error testing {pattern}: {e}")
    
    # 전체 비교 결과
    if len(all_analyses) > 1:
        print_comparison(all_analyses)


def print_comparison(analyses: List[dict]):
    """패턴별 비교 결과 출력"""
    print(f"\n{'='*80}")
    print(f"📊 쓰기 패턴 비교 결과")
    print(f"{'='*80}")
    
    print(f"{'Pattern':<15} {'Cache Hit':<12} {'Read P95':<12} {'Write P95':<12}")
    print(f"{'-'*60}")
    
    for analysis in analyses:
        pattern = analysis['write_pattern']
        cache_hit_rate = analysis['reads']['cache_hit_rate_percent']
        read_p95 = analysis['reads']['p95_response_time_ms']
        write_p95 = analysis['writes']['p95_response_time_ms']
        
        print(f"{pattern:<15} {cache_hit_rate:<11}% {read_p95:<11}ms {write_p95:<11}ms")
    
    print(f"\n🎯 분석:")
    
    # 캐시 히트율이 가장 높은 패턴
    best_cache = max(analyses, key=lambda a: a['reads']['cache_hit_rate_percent'])
    print(f"• 캐시 효율성 최고: {best_cache['write_pattern']} ({best_cache['reads']['cache_hit_rate_percent']}% hit)")
    
    # 읽기 성능이 가장 좋은 패턴
    best_read_perf = min(analyses, key=lambda a: a['reads']['p95_response_time_ms'])
    print(f"• 읽기 성능 최고: {best_read_perf['write_pattern']} ({best_read_perf['reads']['p95_response_time_ms']}ms P95)")
    
    # 쓰기 성능이 가장 좋은 패턴
    best_write_perf = min(analyses, key=lambda a: a['writes']['p95_response_time_ms'])
    print(f"• 쓰기 성능 최고: {best_write_perf['write_pattern']} ({best_write_perf['writes']['p95_response_time_ms']}ms P95)")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n테스트가 중단되었습니다.")
    except Exception as e:
        print(f"테스트 중 오류 발생: {e}")