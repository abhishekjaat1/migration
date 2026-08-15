#!/usr/bin/env python3
"""
PSYCHOPATHMC - FULL INDIAN NUMBER DATABASE MIGRATION
API: https://markplace.site/api.php?key=psycho_2fee8e2e07286f1d&type=number&num={}
VPS Speed: 3 Gbps | Concurrent: 3000
"""

import os
import asyncio
import aiohttp
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values

# ========== CONFIG ==========
API_URL = "https://markplace.site/api.php?key=psycho_2fee8e2e07286f1d&type=number&num={}"
DATABASE_URL = "postgresql://neondb_owner:npg_wzV5qXtDANb7@ep-wild-breeze-azdi9w7w-pooler.c-3.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# Indian mobile number ranges (10 digits)
RANGES = [
    (9000000000, 9999999999),  # 9 series - 100 crore
    (8000000000, 8999999999),  # 8 series - 100 crore
    (7000000000, 7999999999),  # 7 series - 100 crore
    (6000000000, 6999999999),  # 6 series - 100 crore
]

CONCURRENT = 3000  # 🔥 3000 parallel requests
SAVE_INTERVAL = 100000  # Log progress every 100k numbers

# ========== DATABASE SETUP ==========
def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS number_records (
            id SERIAL PRIMARY KEY,
            number VARCHAR(10) UNIQUE,
            name VARCHAR(255),
            fname VARCHAR(255),
            aadhar VARCHAR(12),
            address TEXT,
            circle VARCHAR(100),
            alt VARCHAR(15),
            email VARCHAR(255),
            found BOOLEAN DEFAULT FALSE,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_number ON number_records(number);
        CREATE INDEX IF NOT EXISTS idx_found ON number_records(found);
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Database ready")

# ========== RESUME ==========
def get_last_scraped():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT MAX(number) FROM number_records WHERE found = true")
        row = cur.fetchone()
        cur.close()
        conn.close()
        return int(row[0]) if row[0] else None
    except:
        return None

# ========== FETCH ==========
async def fetch(session, num, semaphore):
    async with semaphore:
        try:
            url = API_URL.format(num)
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # API response format: {"status":"success","result":[...]} OR {"found":2,"data":[...]}
                    if data.get('status') == 'success' and data.get('result'):
                        return data['result']
                    elif data.get('found', 0) > 0 and data.get('data'):
                        return data['data']
        except Exception as e:
            pass
        return None

# ========== BULK SAVE ==========
def save_bulk(records, conn):
    if not records:
        return 0
    cur = conn.cursor()
    data = []
    for rec in records:
        if isinstance(rec, dict):
            num = rec.get('num') or rec.get('number')
            if num:
                data.append((
                    num,
                    rec.get('name'),
                    rec.get('fname'),
                    rec.get('aadhar'),
                    rec.get('address'),
                    rec.get('circle'),
                    rec.get('alt'),
                    rec.get('email'),
                    True
                ))
    if not data:
        return 0
    try:
        execute_values(cur, """
            INSERT INTO number_records (number, name, fname, aadhar, address, circle, alt, email, found)
            VALUES %s
            ON CONFLICT (number) DO UPDATE SET
                name = EXCLUDED.name,
                fname = EXCLUDED.fname,
                aadhar = EXCLUDED.aadhar,
                address = EXCLUDED.address,
                circle = EXCLUDED.circle,
                alt = EXCLUDED.alt,
                email = EXCLUDED.email,
                found = EXCLUDED.found,
                scraped_at = CURRENT_TIMESTAMP;
        """, data)
        conn.commit()
        cur.close()
        return len(data)
    except Exception as e:
        print(f"⚠️ DB Error: {e}")
        cur.close()
        return 0

# ========== MAIN ==========
async def main():
    init_db()
    conn = psycopg2.connect(DATABASE_URL)
    
    last = get_last_scraped()
    start_from = last + 1 if last else 6000000000
    
    total_saved = 0
    total_scanned = 0
    start_time = datetime.now()
    
    print(f"🔄 Resuming from: {start_from}")
    print(f"⚡ Concurrent: {CONCURRENT}")
    print(f"📊 Target: All Indian numbers (6,7,8,9 series)")
    
    semaphore = asyncio.Semaphore(CONCURRENT)
    connector = aiohttp.TCPConnector(limit=CONCURRENT)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        for start, end in RANGES:
            if end < start_from:
                continue
            actual_start = max(start, start_from)
            print(f"\n🔄 Processing: {actual_start} to {end}")
            
            for i in range(actual_start, end + 1, CONCURRENT):
                batch = list(range(i, min(i + CONCURRENT, end + 1)))
                tasks = [fetch(session, str(n), semaphore) for n in batch]
                results = await asyncio.gather(*tasks)
                
                all_records = []
                for res in results:
                    if res:
                        if isinstance(res, list):
                            all_records.extend(res)
                        else:
                            all_records.append(res)
                
                if all_records:
                    saved = save_bulk(all_records, conn)
                    total_saved += saved
                
                total_scanned += len(batch)
                
                if total_scanned % SAVE_INTERVAL == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = total_scanned / elapsed if elapsed > 0 else 0
                    remaining = (4000000000 - total_scanned) / rate if rate > 0 else 0
                    print(f"📊 Scanned: {total_scanned:,} | Saved: {total_saved:,} | Rate: {rate:.0f}/sec | Remaining: {remaining/3600:.1f}h")
    
    conn.close()
    
    # Final count
    conn2 = psycopg2.connect(DATABASE_URL)
    cur = conn2.cursor()
    cur.execute("SELECT COUNT(*) FROM number_records WHERE found = true")
    count = cur.fetchone()[0]
    cur.close()
    conn2.close()
    
    print(f"\n✅ COMPLETE!")
    print(f"📊 Total scanned: {total_scanned:,}")
    print(f"📊 Total saved: {total_saved:,}")
    print(f"📊 Database total: {count:,}")

if __name__ == "__main__":
    asyncio.run(main())
