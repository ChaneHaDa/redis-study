#!/usr/bin/env python3
"""
부하 테스트 스크립트

사용 예시:
python3 scripts/load_test.py --url http://localhost:8000/api/v1/products/42 --concurrency 50 --duration 30s
python3 scripts/load_test.py --url http://localhost:8000/api/v1/products --concurrency 100 --requests 1000
"""
import argparse
import asyncio
import json
import logging
import sys
import time
from collections import defaultdict, Counter
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
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
    error: Optional[str] = None
    timestamp: float = 0.0
    request_id: int = 0


class LoadTester:
    """비동기 부하 테스트 클래스"""
    
    def __init__(self, url: str, concurrency: int):
        self.url = url
        self.concurrency = concurrency
        self.results: List[TestResult] = []
        self.start_time = 0
        self.end_time = 0
        self.request_counter = 0
        self.results_dir = None  # _setup_logger에서 설정됨
        self.logger = self._setup_logger()
        self.db_monitor = DatabaseMonitor()
    
    def _setup_logger(self) -> logging.Logger:
        """로거 설정"""
        # 결과 폴더 생성
        self.results_dir = Path('results') / datetime.now().strftime('%Y%m%d_%H%M%S')
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        logger = logging.getLogger('load_test')
        logger.setLevel(logging.INFO)
        
        # 파일 핸들러 설정
        log_file = self.results_dir / 'load_test.log'
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # 기존 핸들러 제거 후 추가
        logger.handlers.clear()
        logger.addHandler(handler)
        
        return logger
    
    async def make_request(self, session: aiohttp.ClientSession) -> TestResult:
        """단일 HTTP 요청 실행"""
        self.request_counter += 1
        request_id = self.request_counter
        timestamp = time.time()
        start = time.time()
        
        try:
            async with session.get(self.url) as response:
                await response.text()  # 응답 본문을 읽어서 완전히 처리
                end = time.time()
                response_time = end - start
                response_time_ms = response_time * 1000
                
                # 요청-응답 시간 로깅 (ms 단위)
                self.logger.info(f"Request {request_id}: {response.status} - {response_time_ms:.2f}ms")
                
                return TestResult(
                    status_code=response.status,
                    response_time=response_time,
                    timestamp=timestamp,
                    request_id=request_id
                )
        except Exception as e:
            end = time.time()
            response_time = end - start
            response_time_ms = response_time * 1000
            
            # 에러 로깅
            self.logger.error(f"Request {request_id}: ERROR - {response_time_ms:.2f}ms - {str(e)}")
            
            return TestResult(
                status_code=0,
                response_time=response_time,
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
        """지정된 시간 동안 부하 테스트 실행"""
        self._test_duration = duration
        semaphore = asyncio.Semaphore(self.concurrency)
        stop_event = asyncio.Event()
        
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=self.concurrency * 2)
        
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
    
    async def run_request_count_test(self, total_requests: int):
        """지정된 요청 수만큼 부하 테스트 실행"""
        self._test_requests = total_requests
        semaphore = asyncio.Semaphore(self.concurrency)
        
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=self.concurrency * 2)
        
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
    
    async def make_request_with_semaphore(self, session: aiohttp.ClientSession, 
                                        semaphore: asyncio.Semaphore) -> TestResult:
        """세마포어와 함께 요청 실행"""
        async with semaphore:
            return await self.make_request(session)
    
    def print_results(self):
        """테스트 결과 출력"""
        if not self.results:
            print("테스트 결과가 없습니다.")
            return
        
        total_time = self.end_time - self.start_time
        total_requests = len(self.results)
        
        # 상태 코드별 통계
        status_counts = Counter(result.status_code for result in self.results)
        
        # 응답 시간 통계 (성공 요청만)
        response_times = [result.response_time for result in self.results if result.error is None and 200 <= result.status_code < 400]
        
        # 에러 통계 (4xx, 5xx 또는 예외)
        errors = [result for result in self.results if result.error is not None or result.status_code >= 400]
        
        print("=" * 60)
        print("부하 테스트 결과")
        print("=" * 60)
        print(f"URL: {self.url}")
        print(f"동시성 (Concurrency): {self.concurrency}")
        print(f"총 실행 시간: {total_time:.2f}초")
        print(f"총 요청 수: {total_requests}")
        print(f"초당 요청 수 (RPS): {total_requests / total_time:.2f}")
        
        print("\n상태 코드별 통계:")
        for status_code, count in sorted(status_counts.items()):
            percentage = (count / total_requests) * 100
            print(f"  {status_code}: {count}회 ({percentage:.1f}%)")
        
        if response_times:
            print("\n응답 시간 통계:")
            print(f"  평균: {statistics.mean(response_times):.3f}초")
            print(f"  중간값: {statistics.median(response_times):.3f}초")
            print(f"  최소값: {min(response_times):.3f}초")
            print(f"  최대값: {max(response_times):.3f}초")
            if len(response_times) > 1:
                print(f"  표준편차: {statistics.stdev(response_times):.3f}초")
            
            # 백분위수
            sorted_times = sorted(response_times)
            p50 = sorted_times[int(len(sorted_times) * 0.5)]
            p90 = sorted_times[int(len(sorted_times) * 0.9)]
            p95 = sorted_times[int(len(sorted_times) * 0.95)]
            p99 = sorted_times[int(len(sorted_times) * 0.99)]
            
            print(f"  50th percentile: {p50:.3f}초")
            print(f"  90th percentile: {p90:.3f}초")
            print(f"  95th percentile: {p95:.3f}초")
            print(f"  99th percentile: {p99:.3f}초")
        
        if errors:
            print(f"\n에러 발생: {len(errors)}건")
            # HTTP 에러 상태 코드별 통계
            http_errors = Counter(result.status_code for result in errors if result.status_code >= 400)
            if http_errors:
                print("  HTTP 에러 상태 코드:")
                for status_code, count in http_errors.most_common():
                    print(f"    {status_code}: {count}회")
            
            # 예외 에러 통계
            exception_errors = Counter(result.error for result in errors if result.error is not None)
            if exception_errors:
                print("  예외 에러:")
                for error_type, count in exception_errors.most_common(3):
                    print(f"    {error_type}: {count}회")
        
        # baseline.json 저장
        self.save_baseline_results()
        
        print("=" * 60)


    def save_baseline_results(self):
        """baseline.json에 테스트 결과 저장"""
        if not self.results:
            return
        
        total_time = self.end_time - self.start_time
        total_requests = len(self.results)
        
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
        
        baseline_data = {
            "timestamp": datetime.now().isoformat(),
            "url": self.url,
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
            "status_codes": dict(Counter(r.status_code for r in self.results)),
            "test_config": {
                "test_type": "duration" if hasattr(self, '_test_duration') else "request_count",
                "target_duration": getattr(self, '_test_duration', None),
                "target_requests": getattr(self, '_test_requests', None)
            }
        }
        
        # baseline.json 저장 (결과 폴더에)
        baseline_file = self.results_dir / 'baseline.json'
        with open(baseline_file, 'w', encoding='utf-8') as f:
            json.dump(baseline_data, f, indent=2, ensure_ascii=False)
        
        # 상세 결과 파일 저장
        detailed_results = {
            "test_summary": baseline_data,
            "raw_results": [
                {
                    "request_id": result.request_id,
                    "timestamp": result.timestamp,
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
        
        # 요약 텍스트 저장 (DB 정보 포함)
        summary_file = self.results_dir / 'summary.txt'
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(f"📊 부하 테스트 결과 요약\n")
            f.write(f"=" * 40 + "\n")
            f.write(f"URL: {self.url}\n")
            f.write(f"동시성: {self.concurrency}\n")
            f.write(f"QPS: {qps:.2f}\n")
            f.write(f"P50: {p50:.2f}ms, P95: {p95:.2f}ms, P99: {p99:.2f}ms\n")
            f.write(f"에러율: {error_rate:.2f}%\n")
            f.write(f"총 요청: {total_requests}건\n")
            f.write(f"성공 요청: {len(successful_requests)}건\n")
            f.write(f"\n--- DB 부하 통계 ---\n")
            
            # DB 요약 정보 추가
            if self.db_monitor.stats:
                db_query_times = [s.get('db_query_time_ms') for s in self.db_monitor.stats if s.get('db_query_time_ms')]
                cpu_values = [s.get('cpu_percent', 0) for s in self.db_monitor.stats]
                memory_values = [s.get('memory_percent', 0) for s in self.db_monitor.stats]
                
                if db_query_times:
                    f.write(f"DB 쿼리 시간: 평균 {sum(db_query_times)/len(db_query_times):.2f}ms, 최대 {max(db_query_times):.2f}ms\n")
                if cpu_values:
                    f.write(f"CPU 사용률: 평균 {sum(cpu_values)/len(cpu_values):.1f}%, 최대 {max(cpu_values):.1f}%\n")
                if memory_values:
                    f.write(f"메모리 사용률: 평균 {sum(memory_values)/len(memory_values):.1f}%, 최대 {max(memory_values):.1f}%\n")
        
        print(f"\n📊 결과가 {self.results_dir}에 저장되었습니다.")
        print(f"   - baseline.json: 성능 기준선 데이터")
        print(f"   - detailed_results.json: 상세 결과 데이터")
        print(f"   - load_test.log: 요청/응답 로그")
        print(f"   - db_monitoring.json: DB 부하 모니터링 데이터")
        print(f"   - summary.txt: 결과 요약")
        print(f"   - QPS: {qps:.2f}")
        print(f"   - P50: {p50:.2f}ms, P95: {p95:.2f}ms, P99: {p99:.2f}ms")
        print(f"   - 에러율: {error_rate:.2f}%")
        
        # DB 모니터링 요약 출력
        self.db_monitor.print_summary()


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
    parser = argparse.ArgumentParser(description='HTTP 부하 테스트 도구')
    parser.add_argument('--url', required=True, help='테스트할 URL')
    parser.add_argument('--concurrency', '-c', type=int, default=10, 
                       help='동시 연결 수 (기본값: 10)')
    
    # 지속시간 또는 요청 수 중 하나 선택
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--duration', '-d', help='테스트 지속시간 (예: 30s, 2m, 1h)')
    group.add_argument('--requests', '-r', type=int, help='총 요청 수')
    
    args = parser.parse_args()
    
    print(f"부하 테스트 시작...")
    print(f"URL: {args.url}")
    print(f"동시성: {args.concurrency}")
    
    tester = LoadTester(args.url, args.concurrency)
    
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