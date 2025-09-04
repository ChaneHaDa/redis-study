#!/usr/bin/env python3
"""
DB 부하 모니터링 유틸리티
"""
import json
import psutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from threading import Thread
import sys

# DB 파일 경로를 직접 설정
DB_PATH = Path(__file__).parent.parent / "products.db"


class DatabaseMonitor:
    """SQLite 데이터베이스 부하 모니터링"""
    
    def __init__(self, db_path: Path = None):
        self.db_path = db_path or DB_PATH
        self.monitoring = False
        self.stats = []
    
    def get_db_stats(self) -> dict:
        """DB 통계 수집"""
        stats = {
            "timestamp": time.time(),
            "datetime": datetime.now().isoformat(),
        }
        
        # DB 파일 정보
        if self.db_path.exists():
            file_stat = self.db_path.stat()
            stats.update({
                "db_file_size_mb": round(file_stat.st_size / (1024 * 1024), 2),
                "db_file_modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
            })
        else:
            stats.update({
                "db_file_size_mb": 0,
                "db_file_modified": None,
            })
        
        # 시스템 리소스
        stats.update({
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": psutil.virtual_memory().percent,
            "memory_used_mb": round(psutil.virtual_memory().used / (1024 * 1024), 2),
        })
        
        # 디스크 I/O
        disk_io = psutil.disk_io_counters()
        if disk_io:
            stats.update({
                "disk_read_mb": round(disk_io.read_bytes / (1024 * 1024), 2),
                "disk_write_mb": round(disk_io.write_bytes / (1024 * 1024), 2),
                "disk_read_count": disk_io.read_count,
                "disk_write_count": disk_io.write_count,
            })
        
        # DB 연결 테스트 (응답 시간 측정)
        try:
            start_time = time.time()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM products")
                result = cursor.fetchone()
                end_time = time.time()
                
                stats.update({
                    "db_query_time_ms": round((end_time - start_time) * 1000, 2),
                    "db_record_count": result[0] if result else 0,
                    "db_connection_ok": True,
                })
        except Exception as e:
            stats.update({
                "db_query_time_ms": None,
                "db_record_count": None,
                "db_connection_ok": False,
                "db_error": str(e),
            })
        
        return stats
    
    def start_monitoring(self, interval: float = 1.0):
        """모니터링 시작"""
        self.monitoring = True
        self.stats = []
        
        def monitor_loop():
            while self.monitoring:
                try:
                    stats = self.get_db_stats()
                    self.stats.append(stats)
                    time.sleep(interval)
                except Exception as e:
                    print(f"모니터링 에러: {e}")
                    time.sleep(interval)
        
        self.monitor_thread = Thread(target=monitor_loop, daemon=True)
        self.monitor_thread.start()
        print(f"🔍 DB 모니터링 시작 (간격: {interval}초)")
    
    def stop_monitoring(self):
        """모니터링 중지"""
        self.monitoring = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=2)
        print("⏹️ DB 모니터링 중지")
    
    def save_stats(self, output_file: Path):
        """통계를 파일로 저장"""
        if not self.stats:
            print("저장할 통계 데이터가 없습니다.")
            return
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2, ensure_ascii=False)
        
        print(f"📊 DB 모니터링 결과가 {output_file}에 저장되었습니다.")
    
    def print_summary(self):
        """통계 요약 출력"""
        if not self.stats:
            print("통계 데이터가 없습니다.")
            return
        
        print("\n" + "="*50)
        print("DB 부하 모니터링 요약")
        print("="*50)
        
        # CPU/메모리 통계
        cpu_values = [s['cpu_percent'] for s in self.stats if 'cpu_percent' in s]
        memory_values = [s['memory_percent'] for s in self.stats if 'memory_percent' in s]
        
        if cpu_values:
            print(f"CPU 사용률: 평균 {sum(cpu_values)/len(cpu_values):.1f}%, 최대 {max(cpu_values):.1f}%")
        if memory_values:
            print(f"메모리 사용률: 평균 {sum(memory_values)/len(memory_values):.1f}%, 최대 {max(memory_values):.1f}%")
        
        # DB 쿼리 시간 통계
        query_times = [s['db_query_time_ms'] for s in self.stats if s.get('db_query_time_ms')]
        if query_times:
            print(f"DB 쿼리 시간: 평균 {sum(query_times)/len(query_times):.2f}ms, 최대 {max(query_times):.2f}ms")
        
        # DB 연결 실패율
        total_checks = len(self.stats)
        failed_connections = len([s for s in self.stats if not s.get('db_connection_ok', True)])
        if total_checks > 0:
            failure_rate = (failed_connections / total_checks) * 100
            print(f"DB 연결 실패율: {failure_rate:.1f}% ({failed_connections}/{total_checks})")
        
        # 디스크 I/O 변화
        if len(self.stats) > 1:
            first = self.stats[0]
            last = self.stats[-1]
            if 'disk_read_mb' in first and 'disk_read_mb' in last:
                read_diff = last['disk_read_mb'] - first['disk_read_mb']
                write_diff = last['disk_write_mb'] - first['disk_write_mb']
                print(f"디스크 I/O: 읽기 +{read_diff:.1f}MB, 쓰기 +{write_diff:.1f}MB")


def main():
    """독립 실행시 실시간 모니터링"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SQLite DB 부하 모니터링')
    parser.add_argument('--duration', '-d', type=int, default=30, help='모니터링 시간(초)')
    parser.add_argument('--interval', '-i', type=float, default=1.0, help='체크 간격(초)')
    parser.add_argument('--output', '-o', help='결과 저장 파일명')
    
    args = parser.parse_args()
    
    monitor = DatabaseMonitor()
    
    try:
        monitor.start_monitoring(args.interval)
        time.sleep(args.duration)
        monitor.stop_monitoring()
        
        monitor.print_summary()
        
        if args.output:
            output_file = Path(args.output)
            monitor.save_stats(output_file)
    
    except KeyboardInterrupt:
        print("\n모니터링이 중단되었습니다.")
        monitor.stop_monitoring()
        monitor.print_summary()


if __name__ == "__main__":
    main()