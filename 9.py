#!/usr/bin/env python3
# =====================================================
# PSYCHOPATHMC - NEW API WORKING
# API: https://info-ap-is.vercel.app/number?q=
# =====================================================

import sys
import asyncio
import aiohttp
import asyncpg
import random
import time
from datetime import datetime
import ssl

# ========== SSL FIX ==========
try:
    import _ssl
    if not hasattr(ssl, 'SSLContext'):
        ssl.SSLContext = _ssl.SSLContext
    print("✅ SSL fixed")
except:
    pass

# 🔥 NEW API
API_URL = "https://info-ap-is.vercel.app/number?q={}"

DATABASE_URL = "postgresql://psycho:XmH59qu_XE5IioROF3LTyA@void-kudu-32088.j77.aws-ap-south-1.cockroachlabs.cloud:26257/psychodb?sslmode=require"

START = 9000000000
END = 9999999999
CONCURRENT = 300
BATCH_SIZE = 2000

async def get_db_connection():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Database connected")
        return conn
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return None

async def init_db(conn):
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS number_records (
                id SERIAL PRIMARY KEY,
                number VARCHAR(10) UNIQUE,
                name VARCHAR(255), fname VARCHAR(255),
                aadhar VARCHAR(12), address TEXT,
                circle VARCHAR(100), alt VARCHAR(15), email VARCHAR(255),
                found BOOLEAN DEFAULT FALSE,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_number ON number_records(number);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_found ON number_records(found);")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS migration_progress (
                id SERIAL PRIMARY KEY,
                last_number BIGINT,
                total_saved BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await conn.execute("INSERT INTO migration_progress (id, last_number, total_saved) VALUES (1, $1, 0) ON CONFLICT (id) DO NOTHING;", START - 1)
        print("✅ Database ready")
        return True
    except Exception as e:
        print(f"❌ Init error: {e}")
        return False

async def get_progress(conn):
    try:
        row = await conn.fetchrow("SELECT last_number, total_saved FROM migration_progress WHERE id = 1")
        return row['last_number'], row['total_saved'] if row else (START - 1, 0)
    except:
        return START - 1, 0

async def fetch(session, num, semaphore):
    async with semaphore:
        try:
            url = API_URL.format(num)
            async with session.get(url, timeout=10, ssl=False) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('status') == True and data.get('result'):
                        # 🔥 Convert field names to lowercase for consistency
                        records = []
                        for rec in data['result']:
                            records.append({
                                'num': rec.get('num'),
                                'name': rec.get('NAME') or rec.get('name'),
                                'fname': rec.get('fname'),
                                'aadhar': rec.get('aadhar'),
                                'address': rec.get('ADDRESS') or rec.get('address'),
                                'circle': rec.get('circle'),
                                'alt': rec.get('alt'),
                                'email': rec.get('email')
                            })
                        return records
        except Exception as e:
            print(f"⚠️ Error fetching {num}: {e}")
        return None

async def save_bulk(conn, records):
    if not records:
        return 0
    data = []
    for rec in records:
        if isinstance(rec, dict):
            num = rec.get('num')
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
        await conn.executemany("""
            INSERT INTO number_records (number, name, fname, aadhar, address, circle, alt, email, found)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
        return len(data)
    except Exception as e:
        print(f"⚠️ DB save error: {e}")
        return 0

async def main():
    conn = await get_db_connection()
    if not conn:
        print("❌ Cannot proceed without database connection.")
        return
    
    if not await init_db(conn):
        await conn.close()
        return
    
    last, total_saved = await get_progress(conn)
    if last >= END:
        print("✅ All numbers already scanned!")
        await conn.close()
        return

    print(f"🔄 Resuming from: {last+1}")
    print(f"⚡ Concurrent: {CONCURRENT}")
    print(f"📊 Target: 9 Series ({START} to {END})")
    print(f"🔗 API: {API_URL}")

    total_scanned = last - START + 1 if last >= START else 0
    start_time = datetime.now()
    semaphore = asyncio.Semaphore(CONCURRENT)
    connector = aiohttp.TCPConnector(limit=CONCURRENT)

    async with aiohttp.ClientSession(connector=connector) as session:
        i = last + 1
        while i <= END:
            batch = list(range(i, min(i + BATCH_SIZE, END + 1)))
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
                saved = await save_bulk(conn, all_records)
                total_saved += saved
                print(f"✅ Batch {batch[0]}-{batch[-1]} → Found {len(all_records)} records, Saved {saved}")
            else:
                print(f"❌ Batch {batch[0]}-{batch[-1]} → No data")

            total_scanned += len(batch)
            last_processed = batch[-1]
            await conn.execute("UPDATE migration_progress SET last_number = $1, total_saved = $2 WHERE id = 1", last_processed, total_saved)

            if total_scanned % 50000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = total_scanned / elapsed if elapsed > 0 else 0
                remaining = (END - last_processed) / rate if rate > 0 else 0
                print(f"📊 Progress: {total_scanned:,} | Saved: {total_saved:,} | Speed: {rate:.0f}/sec | ETA: {remaining/3600:.1f}h")

            i = last_processed + 1

    count = await conn.fetchval("SELECT COUNT(*) FROM number_records WHERE found = true")
    print(f"✅ Final DB count: {count:,}")
    print(f"\n✅ COMPLETE! Total scanned: {total_scanned:,}, Saved: {total_saved:,}")
    await conn.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted. Progress saved in DB.")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)
