#!/usr/bin/env python3
"""
MySQL에 1만건의 샘플 데이터를 삽입하는 스크립트
"""
import asyncio
import random
import sys
from pathlib import Path

import aiomysql

# 상위 모듈들을 import하기 위한 path 설정
sys.path.append(str(Path(__file__).parent / "backend"))

from backend.app.core.config import settings


def generate_sample_products(count: int = 10000):
    """샘플 제품 데이터 생성"""
    product_names = [
        "스마트폰", "노트북", "태블릿", "헤드폰", "키보드", "마우스", "모니터", "스피커",
        "카메라", "프린터", "웹캠", "마이크", "충전기", "케이블", "배터리", "메모리카드",
        "하드디스크", "SSD", "그래픽카드", "CPU", "메인보드", "RAM", "파워서플라이", "쿨러",
        "게임패드", "조이스틱", "VR헤드셋", "드론", "액션캠", "블루투스이어폰",
        "스마트워치", "피트니스밴드", "전자책리더", "포터블배터리", "무선충전기",
        "USB허브", "어댑터", "컨버터", "스위치", "라우터", "모뎀", "공유기",
        "네트워크카드", "사운드카드", "TV튜너", "캡처카드", "외장하드", "NAS",
        "서버", "워크스테이션"
    ]
    
    brands = ["삼성", "LG", "애플", "소니", "HP", "델", "레노버", "ASUS", "MSI", "기가바이트"]
    
    for i in range(count):
        name = f"{random.choice(brands)} {random.choice(product_names)} {random.randint(100, 999)}"
        price = round(random.uniform(10000, 2000000), -2)  # 100원 단위
        yield (name, price)


async def insert_sample_data():
    """샘플 데이터를 MySQL 데이터베이스에 삽입"""
    print(f"MySQL 연결 정보: {settings.MYSQL_HOST}:{settings.MYSQL_PORT}/{settings.MYSQL_DATABASE}")
    
    try:
        conn = await aiomysql.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            db=settings.MYSQL_DATABASE,
            charset='utf8mb4',
            autocommit=False
        )
        
        async with conn.cursor() as cursor:
            # 기존 데이터 개수 확인
            await cursor.execute("SELECT COUNT(*) FROM products")
            result = await cursor.fetchone()
            existing_count = result[0]
            print(f"기존 데이터 개수: {existing_count}")
            
            # 배치 단위로 데이터 삽입 (성능 향상)
            batch_size = 1000
            total_inserted = 0
            
            print("샘플 데이터 생성 및 삽입 중...")
            
            products = list(generate_sample_products(10000))
            
            for i in range(0, len(products), batch_size):
                batch = products[i:i+batch_size]
                await cursor.executemany(
                    "INSERT INTO products (name, price) VALUES (%s, %s)",
                    batch
                )
                total_inserted += len(batch)
                print(f"진행률: {total_inserted}/10000 ({total_inserted/100:.1f}%)")
            
            await conn.commit()
            
            # 최종 데이터 개수 확인
            await cursor.execute("SELECT COUNT(*) FROM products")
            result = await cursor.fetchone()
            final_count = result[0]
            
            print(f"\n삽입 완료!")
            print(f"총 삽입된 데이터: {total_inserted}건")
            print(f"최종 데이터 개수: {final_count}건")
            
    except Exception as e:
        print(f"오류 발생: {e}")
        if 'conn' in locals():
            await conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    asyncio.run(insert_sample_data())