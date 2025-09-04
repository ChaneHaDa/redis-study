#!/usr/bin/env python3
"""
캐시 전용 부하 테스트 스크립트 - Lab 2용

특징:
- 동일 ID 반복 요청으로 높은 캐시 히트율 달성
- 캐시 메트릭 모니터링
- Before/After 비교용

사용 예시:
python3 scripts/cache_load_test.py --url http://localhost:8000/api/v1/products --concurrency 50 --duration 30s --product-ids 1 42 100
"""
import argparse
import asyncio
import json
import logging
import random
import sys
import time
from collections import defaultdict, Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import statistics

import aiohttp

# 상위 모듈 import를 위한 path 설정
sys.path.append(str(Path(__file__).parent))
from db_monitor import DatabaseMonitor


@dataclass
class TestResult:
    """테스트 결과를 저장하는 데이터 클래스"""
    status_code: int
    response_time: float
    product_id: int
    error: Optional[str] = None
    timestamp: float = 0.0
    request_id: int = 0


class CacheLoadTester:
    """캐시 전용 비동기 부하 테스트 클래스"""
    
    def __init__(self, base_url: str, concurrency: int, product_ids: List[int]):
        self.base_url = base_url.rstrip('/')
        self.concurrency = concurrency
        self.product_ids = product_ids
        self.results: List[TestResult] = []
        self.start_time = 0
        self.end_time = 0
        self.request_counter = 0
        self.results_dir = None
        self.logger = self._setup_logger()
        self.db_monitor = DatabaseMonitor()
        self.cache_metrics_start = None
        self.cache_metrics_end = None
    
    def _setup_logger(self) -> logging.Logger:
        """로거 설정"""
        # 결과 폴더 생성
        self.results_dir = Path('results') / f"cache_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger('cache_load_test')
        logger.setLevel(logging.INFO)
        
        # 파일 핸들러 설정
        log_file = self.results_dir / 'cache_load_test.log'
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # 기존 핸들러 제거 후 추가
        logger.handlers.clear()
        logger.addHandler(handler)
        
        return logger
    
    def _get_random_product_url(self) -> tuple[str, int]:
        """랜덤 제품 ID로 URL 생성"""
        product_id = random.choice(self.product_ids)
        url = f"{self.base_url}/{product_id}"
        return url, product_id
    
    async def get_cache_metrics(self) -> Dict[str, Any]:
        """서버에서 캐시 메트릭 가져오기"""
        try:
            # base_url에서 호스트 추출
            import re
            match = re.match(r'(https?://[^/]+)', self.base_url)
            if match:
                base_host = match.group(1)
                metrics_url = f"{base_host}/metrics"
                
                timeout = aiohttp.ClientTimeout(total=5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(metrics_url) as response:
                        if response.status == 200:
                            return await response.json()
        except Exception as e:
            self.logger.warning(f"캐시 메트릭 가져오기 실패: {e}")
        
        return {}
    
    async def make_request(self, session: aiohttp.ClientSession) -> TestResult:
        """단일 HTTP 요청 실행"""
        self.request_counter += 1
        request_id = self.request_counter
        timestamp = time.time()
        start = time.time()
        
        # 랜덤 제품 ID로 요청
        test_url, product_id = self._get_random_product_url()
        
        try:
            async with session.get(test_url) as response:
                await response.text()
                end = time.time()
                response_time = end - start
                response_time_ms = response_time * 1000
                
                # 요청-응답 시간 로깅 (ms 단위)
                self.logger.info(f"Request {request_id}: {response.status} - {response_time_ms:.2f}ms - Product {product_id}")
                
                return TestResult(
                    status_code=response.status,
                    response_time=response_time,
                    product_id=product_id,
                    timestamp=timestamp,
                    request_id=request_id
                )
        except Exception as e:
            end = time.time()
            response_time = end - start
            response_time_ms = response_time * 1000
            
            # 에러 로깅
            self.logger.error(f"Request {request_id}: ERROR - {response_time_ms:.2f}ms - Product {product_id} - {str(e)}")
            
            return TestResult(
                status_code=0,
                response_time=response_time,
                product_id=product_id,
                error=str(e),
                timestamp=timestamp,
                request_id=request_id
            )
    
    async def worker(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, 
                    stop_event: asyncio.Event):
        """워커 코루틴 - 세마포어로 동시성 제어"""
        while not stop_event.is_set():
            async with semaphore:
                if stop_event.is_set():
                    break
                result = await self.make_request(session)
                self.results.append(result)
    
    async def run_duration_test(self, duration: int):
        """지정된 시간 동안 캐시 부하 테스트 실행"""
        semaphore = asyncio.Semaphore(self.concurrency)
        stop_event = asyncio.Event()
        
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=self.concurrency * 2)
        
        # 시작 전 캐시 메트릭 수집
        self.cache_metrics_start = await self.get_cache_metrics()
        
        # DB 모니터링 시작
        self.db_monitor.start_monitoring(interval=0.5)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # 워커 태스크들 생성
            workers = [
                asyncio.create_task(self.worker(session, semaphore, stop_event))
                for _ in range(self.concurrency)
            ]
            
            self.start_time = time.time()
            
            # 지정된 시간 대기
            await asyncio.sleep(duration)
            
            # 모든 워커 중지
            stop_event.set()
            
            # 모든 워커 완료 대기
            await asyncio.gather(*workers, return_exceptions=True)
            
            self.end_time = time.time()
        
        # DB 모니터링 중지
        self.db_monitor.stop_monitoring()
        
        # 종료 후 캐시 메트릭 수집
        self.cache_metrics_end = await self.get_cache_metrics()
    
    async def run_request_count_test(self, total_requests: int):
        """지정된 요청 수만큼 캐시 부하 테스트 실행"""
        semaphore = asyncio.Semaphore(self.concurrency)
        
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=self.concurrency * 2)
        
        # 시작 전 캐시 메트릭 수집
        self.cache_metrics_start = await self.get_cache_metrics()
        
        # DB 모니터링 시작
        self.db_monitor.start_monitoring(interval=0.5)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            self.start_time = time.time()
            
            # 태스크 생성
            tasks = []
            for _ in range(total_requests):
                task = asyncio.create_task(self.make_request_with_semaphore(session, semaphore))
                tasks.append(task)
            
            # 모든 요청 완료 대기
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            self.end_time = time.time()
            
            # 결과 저장
            for result in results:
                if isinstance(result, TestResult):
                    self.results.append(result)
        
        # DB 모니터링 중지
        self.db_monitor.stop_monitoring()
        
        # 종료 후 캐시 메트릭 수집
        self.cache_metrics_end = await self.get_cache_metrics()
    
    async def make_request_with_semaphore(self, session: aiohttp.ClientSession, 
                                        semaphore: asyncio.Semaphore) -> TestResult:
        """세마포어와 함께 요청 실행"""
        async with semaphore:
            return await self.make_request(session)
    
    def print_results(self):
        """캐시 테스트 결과 출력"""
        if not self.results:
            print("테스트 결과가 없습니다.")
            return
        
        total_time = self.end_time - self.start_time
        total_requests = len(self.results)
        
        # 상태 코드별 통계
        status_counts = Counter(result.status_code for result in self.results)
        
        # 제품 ID별 요청 분포
        product_id_counts = Counter(result.product_id for result in self.results)
        
        # 성공한 요청만 필터링 (2xx, 3xx 상태 코드 + 에러 없음)
        successful_requests = [r for r in self.results if r.error is None and 200 <= r.status_code < 400]
        response_times = [r.response_time for r in successful_requests]
        
        # 에러율 계산 (4xx, 5xx 또는 예외 발생)
        error_count = len([r for r in self.results if r.error is not None or r.status_code >= 400])
        error_rate = (error_count / total_requests) * 100 if total_requests > 0 else 0
        
        # QPS 계산
        qps = total_requests / total_time if total_time > 0 else 0
        
        # 백분위수 계산
        p50 = p95 = p99 = 0
        if response_times:
            sorted_times = sorted(response_times)
            p50 = sorted_times[int(len(sorted_times) * 0.5)] * 1000  # ms 변환
            p95 = sorted_times[int(len(sorted_times) * 0.95)] * 1000
            p99 = sorted_times[int(len(sorted_times) * 0.99)] * 1000
        
        print("=" * 60)
        print("🚀 캐시 부하 테스트 결과")
        print("=" * 60)
        print(f"Base URL: {self.base_url}")
        print(f"제품 IDs: {self.product_ids}")
        print(f"동시성 (Concurrency): {self.concurrency}")
        print(f"총 실행 시간: {total_time:.2f}초")
        print(f"총 요청 수: {total_requests}")
        print(f"QPS: {qps:.2f}")
        
        print(f"\n📈 상태 코드별 통계:")
        for status_code, count in sorted(status_counts.items()):
            percentage = (count / total_requests) * 100
            status_emoji = "✅" if 200 <= status_code < 400 else "❌"
            print(f"  {status_emoji} {status_code}: {count}회 ({percentage:.1f}%)")
        
        print(f"\n📊 제품 ID별 요청 분포:")
        for product_id, count in sorted(product_id_counts.items()):
            percentage = (count / total_requests) * 100
            print(f"  Product {product_id}: {count}회 ({percentage:.1f}%)")
        
        print(f"\n⚡ 성능 메트릭:")
        print(f"  에러율: {error_rate:.1f}% ({error_count}/{total_requests})")
        if response_times:
            print(f"  P50: {p50:.2f}ms, P95: {p95:.2f}ms, P99: {p99:.2f}ms")
        else:
            print(f"  성공 요청이 없어 응답시간 통계를 계산할 수 없습니다.")
        
        # 캐시 메트릭 출력
        if self.cache_metrics_start and self.cache_metrics_end:
            print(f"\n💾 캐시 메트릭:")
            start = self.cache_metrics_start
            end = self.cache_metrics_end
            
            hits_diff = end.get('hits', 0) - start.get('hits', 0)
            misses_diff = end.get('misses', 0) - start.get('misses', 0)
            sets_diff = end.get('sets', 0) - start.get('sets', 0)
            
            total_cache_requests = hits_diff + misses_diff
            hit_rate = (hits_diff / total_cache_requests * 100) if total_cache_requests > 0 else 0
            
            print(f"  캐시 히트: {hits_diff}회")
            print(f"  캐시 미스: {misses_diff}회") 
            print(f"  캐시 저장: {sets_diff}회")
            print(f"  캐시 히트율: {hit_rate:.1f}%")
        
        # 결과 저장
        self.save_results()
        
        # DB 모니터링 요약 출력
        self.db_monitor.print_summary()
        
        print("=" * 60)
    
    def save_results(self):
        """결과를 파일로 저장"""
        if not self.results:
            return
        
        total_time = self.end_time - self.start_time
        total_requests = len(self.results)
        
        # 성공한 요청만 필터링
        successful_requests = [r for r in self.results if r.error is None and 200 <= r.status_code < 400]
        response_times = [r.response_time for r in successful_requests]
        
        # 에러율 계산
        error_count = len([r for r in self.results if r.error is not None or r.status_code >= 400])
        error_rate = (error_count / total_requests) * 100 if total_requests > 0 else 0
        
        # QPS 계산
        qps = total_requests / total_time if total_time > 0 else 0
        
        # 백분위수 계산
        p50 = p95 = p99 = 0
        if response_times:
            sorted_times = sorted(response_times)
            p50 = sorted_times[int(len(sorted_times) * 0.5)] * 1000
            p95 = sorted_times[int(len(sorted_times) * 0.95)] * 1000
            p99 = sorted_times[int(len(sorted_times) * 0.99)] * 1000
        
        # 캐시 메트릭 계산
        cache_hits = cache_misses = cache_sets = cache_hit_rate = 0
        if self.cache_metrics_start and self.cache_metrics_end:
            start = self.cache_metrics_start
            end = self.cache_metrics_end
            cache_hits = end.get('hits', 0) - start.get('hits', 0)
            cache_misses = end.get('misses', 0) - start.get('misses', 0)
            cache_sets = end.get('sets', 0) - start.get('sets', 0)
            total_cache_requests = cache_hits + cache_misses
            cache_hit_rate = (cache_hits / total_cache_requests * 100) if total_cache_requests > 0 else 0
        
        baseline_data = {
            "test_type": "cache_load_test",
            "timestamp": datetime.now().isoformat(),
            "base_url": self.base_url,
            "product_ids": self.product_ids,
            "concurrency": self.concurrency,
            "duration_seconds": total_time,
            "total_requests": total_requests,
            "successful_requests": len(successful_requests),
            "qps": round(qps, 2),
            "error_rate_percent": round(error_rate, 2),
            "response_times_ms": {
                "p50": round(p50, 2),
                "p95": round(p95, 2),
                "p99": round(p99, 2),
                "avg": round(statistics.mean(response_times) * 1000, 2) if response_times else 0,
                "min": round(min(response_times) * 1000, 2) if response_times else 0,
                "max": round(max(response_times) * 1000, 2) if response_times else 0
            },
            "cache_metrics": {
                "hits": cache_hits,
                "misses": cache_misses,
                "sets": cache_sets,
                "hit_rate_percent": round(cache_hit_rate, 2)
            },
            "product_distribution": dict(Counter(r.product_id for r in self.results)),
            "status_codes": dict(Counter(r.status_code for r in self.results))
        }
        
        # baseline.json 저장
        baseline_file = self.results_dir / 'cache_baseline.json'
        with open(baseline_file, 'w', encoding='utf-8') as f:
            json.dump(baseline_data, f, indent=2, ensure_ascii=False)
        
        # 상세 결과 저장
        detailed_results = {
            "test_summary": baseline_data,
            "raw_results": [
                {
                    "request_id": result.request_id,
                    "timestamp": result.timestamp,
                    "product_id": result.product_id,
                    "status_code": result.status_code,
                    "response_time_ms": round(result.response_time * 1000, 2),
                    "error": result.error,
                    "is_success": result.error is None and 200 <= result.status_code < 400
                }
                for result in self.results
            ]
        }
        
        detailed_file = self.results_dir / 'detailed_results.json'
        with open(detailed_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_results, f, indent=2, ensure_ascii=False)
        
        # DB 모니터링 결과 저장
        db_stats_file = self.results_dir / 'db_monitoring.json'
        self.db_monitor.save_stats(db_stats_file)
        
        print(f"\n📁 결과가 {self.results_dir}에 저장되었습니다.")
        print(f"   - cache_baseline.json: 캐시 성능 기준선")
        print(f"   - detailed_results.json: 상세 결과")
        print(f"   - cache_load_test.log: 요청/응답 로그")
        print(f"   - db_monitoring.json: DB 부하 모니터링")


def parse_duration(duration_str: str) -> int:
    """지속시간 문자열을 초단위로 변환 (예: 30s, 2m, 1h)"""
    if duration_str.endswith('s'):
        return int(duration_str[:-1])
    elif duration_str.endswith('m'):
        return int(duration_str[:-1]) * 60
    elif duration_str.endswith('h'):
        return int(duration_str[:-1]) * 3600
    else:
        return int(duration_str)


def main():
    parser = argparse.ArgumentParser(description='캐시 전용 HTTP 부하 테스트 도구')
    parser.add_argument('--url', required=True, help='베이스 URL (제품 ID 없이)')
    parser.add_argument('--concurrency', '-c', type=int, default=50, 
                       help='동시 연결 수 (기본값: 50)')
    parser.add_argument('--product-ids', nargs='+', type=int, default=[1, 42, 100],
                       help='테스트할 제품 ID 목록 (기본값: 1 42 100)')
    
    # 지속시간 또는 요청 수 중 하나 선택
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--duration', '-d', help='테스트 지속시간 (예: 30s, 2m, 1h)')
    group.add_argument('--requests', '-r', type=int, help='총 요청 수')
    
    args = parser.parse_args()
    
    print(f"🚀 캐시 부하 테스트 시작...")
    print(f"Base URL: {args.url}")
    print(f"제품 IDs: {args.product_ids}")
    print(f"동시성: {args.concurrency}")
    
    tester = CacheLoadTester(args.url, args.concurrency, args.product_ids)
    
    try:
        if args.duration:
            duration_seconds = parse_duration(args.duration)
            print(f"지속시간: {duration_seconds}초")
            asyncio.run(tester.run_duration_test(duration_seconds))
        else:
            print(f"총 요청 수: {args.requests}")
            asyncio.run(tester.run_request_count_test(args.requests))
    
    except KeyboardInterrupt:
        print("\n테스트가 중단되었습니다.")
    except Exception as e:
        print(f"테스트 중 오류 발생: {e}")
    finally:
        tester.print_results()


if __name__ == "__main__":
    main()