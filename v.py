#!/usr/bin/env python3
# =====================================================
# PSYCHOPATHMC - RATE LIMIT PROOF MIGRATION (VPS)
# Auto backoff | Retry | Resume | Adaptive concurrency
# =====================================================

import sys
import asyncio
import aiohttp
import asyncpg
import random
from datetime import datetime

API_URL = "https://markplace.site/api.php?key=psycho_2fee8e2e07286f1d&type=number&num={}"
DATABASE_URL = "postgresql://neondb_owner:npg_wzV5qXtDANb7@ep-wild-breeze-azdi9w7w-pooler.c-3.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

START = 6000000000
END = 9999999999
INITIAL_CONCURRENT = 3000
MAX_RETRIES = 3
BASE_DELAY = 0.5

# ========== DATABASE SETUP ==========
async def init_db(conn):
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

async def get_progress(conn):
    row = await conn.fetchrow("SELECT last_number, total_saved FROM migration_progress WHERE id = 1")
    return row['last_number'], row['total_saved'] if row else (START - 1, 0)

async def save_progress(conn, last, saved):
    await conn.execute("UPDATE migration_progress SET last_number = $1, total_saved = $2, updated_at = CURRENT_TIMESTAMP WHERE id = 1", last, saved)

# ========== FETCH WITH RETRY ==========
async def fetch_with_retry(session, num, semaphore, retry_count=0):
    async with semaphore:
        try:
            url = API_URL.format(num)
            async with session.get(url, timeout=10) as resp:
                if resp.status == 429:
                    # Rate limit – wait and retry
                    wait = (BASE_DELAY * (2 ** retry_count)) + random.uniform(0, 0.5)
                    print(f"⏳ Rate limit for {num}, waiting {wait:.1f}s")
                    await asyncio.sleep(wait)
                    if retry_count < MAX_RETRIES:
                        return await fetch_with_retry(session, num, semaphore, retry_count + 1)
                    return None
                elif resp.status == 200:
                    data = await resp.json()
                    if data.get('status') == 'success' and data.get('result'):
                        return data['result']
                    elif data.get('found', 0) > 0 and data.get('data'):
                        return data['data']
                else:
                    # Other errors – retry
                    if retry_count < MAX_RETRIES:
                        await asyncio.sleep(BASE_DELAY * (2 ** retry_count))
                        return await fetch_with_retry(session, num, semaphore, retry_count + 1)
        except asyncio.TimeoutError:
            if retry_count < MAX_RETRIES:
                await asyncio.sleep(BASE_DELAY * (2 ** retry_count))
                return await fetch_with_retry(session, num, semaphore, retry_count + 1)
        except:
            pass
        return None

# ========== SAVE BULK ==========
async def save_bulk(conn, records):
    if not records:
        return 0
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
        print(f"⚠️ DB Error: {e}")
        return 0

# ========== MAIN ==========
async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    await init_db(conn)
    last, total_saved = await get_progress(conn)
    if last >= END:
        print("✅ All numbers already scanned!")
        await conn.close()
        return

    print(f"🔄 Resuming from: {last+1}")
    print(f"⚡ Initial concurrent: {INITIAL_CONCURRENT}")
    print(f"📊 Target: All Indian numbers ({START} to {END})")

    total_scanned = last - START + 1 if last >= START else 0
    start_time = datetime.now()

    # Adaptive concurrency
    concurrent = INITIAL_CONCURRENT
    semaphore = asyncio.Semaphore(concurrent)
    connector = aiohttp.TCPConnector(limit=concurrent)

    async with aiohttp.ClientSession(connector=connector) as session:
        i = last + 1
        while i <= END:
            batch = list(range(i, min(i + concurrent, END + 1)))
            tasks = [fetch_with_retry(session, str(n), semaphore, 0) for n in batch]
            results = await asyncio.gather(*tasks)

            # Count errors to detect rate limit
            error_count = sum(1 for r in results if r is None)
            success_count = len(results) - error_count

            if error_count > len(results) * 0.3:  # >30% errors
                # Reduce concurrency
                new_concurrent = max(500, concurrent // 2)
                if new_concurrent != concurrent:
                    print(f"⚠️ High error rate ({error_count}/{len(batch)}). Reducing concurrency to {new_concurrent}")
                    concurrent = new_concurrent
                    semaphore = asyncio.Semaphore(concurrent)
                    connector = aiohttp.TCPConnector(limit=concurrent)
                    # Recreate session? We'll continue with same session but semaphore changed
                    # Actually we need to update semaphore for next batch
                await asyncio.sleep(2)  # Cool down

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
            await save_progress(conn, last_processed, total_saved)

            # Adaptively increase concurrency if no errors
            if error_count == 0 and concurrent < INITIAL_CONCURRENT:
                new_concurrent = min(INITIAL_CONCURRENT, int(concurrent * 1.2))
                if new_concurrent != concurrent:
                    print(f"🚀 Increasing concurrency to {new_concurrent}")
                    concurrent = new_concurrent
                    semaphore = asyncio.Semaphore(concurrent)
                    connector = aiohttp.TCPConnector(limit=concurrent)

            if total_scanned % 100000 == 0:
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
