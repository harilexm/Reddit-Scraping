import pandas as pd
import time
import re
import asyncio
import aiohttp
import random
import nltk
from collections import deque
from nltk.corpus import stopwords
from urllib.parse import urlparse

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

english_stops = set(stopwords.words('english'))
urdu_markers = {
    # Pronouns
    'main', 'mein', 'mjhe', 'mujhe', 'mera', 'meri', 'mere', 'hum', 'humein', 'hm', 'hamara', 
    'tum', 'tumhein', 'tm', 'tumhara', 'aap', 'apka', 'apki', 'apke', 'wo', 'woh', 'us', 'un', 
    'uska', 'uski', 'uske', 'unka', 'apna', 'apni', 'apne',
    
    # Postpositions
    'ka', 'ki', 'ke', 'kay', 'ko', 'se', 'sey', 'ne', 'mein', 'mn', 'par', 'pe', 'pay', 'tak', 
    'sath', 'saath', 'liye', 'lye', 'taraf',
    
    # Verbs/Tense
    'hai', 'hain', 'han', 'hyn', 'hn', 'ho', 'hu', 'hoon', 'tha', 'thi', 'the', 'thay', 
    'raha', 'rahi', 'rahe', 'rahay', 'kar', 'karo', 'karna', 'kia', 'kiya', 'karta',
    
    # Conjunctions/Logic
    'aur', 'or', 'lekin', 'magar', 'bhi', 'toh', 'to', 'tu', 'agar', 'agr', 'phir', 'phr', 
    'kyunke', 'kyunkay', 'jab', 'tab', 'isliye',
    
    # Interrogatives/Negations
    'kya', 'kia', 'kyun', 'kyu', 'kab', 'kahan', 'kesay', 'kaise', 'kon', 'kaun', 
    'nahi', 'nhi', 'na', 'ni', 'mat', 'han', 'haan', 'ji', 'jee',
    
    # Fillers/Slang
    'acha', 'achha', 'wese', 'wesay', 'matlab', 'bas', 'bs', 'yaar', 'yar', 'bhai', 
    'shyd', 'shayed', 'bilkul', 'zaroor', 'ab', 'abhi'
}

INPUT_FILE = r"C:\Users\DeLL\Desktop\Reddit Scraping\ScrapeLinks\links1.csv"
OUTPUT_FILE = "commentsScrape.csv"
PROGRESS_FILE = "scrape_progress.txt"


'''HYBRID BATCH APPROACH:
    Phase 1: Batch check 100 post IDs at once using /api/info (counts as 1 request)
    Phase 2: Fetch comments only for valid posts (individual requests)
    Result: Skip deleted/404 posts, save requests!'''

BATCH_CHECK_SIZE = 100  # Check 100 post IDs per request (Reddit limit)
REQUESTS_PER_MINUTE = 10  # Rate for comment fetching
CONCURRENT_REQUESTS = 2
MAX_RETRIES = 3

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) RomanUrduHunter/2.0 by ScrapeUmer'
}

def is_roman_urdu(text):
    words = text.lower().split()
    if not words: return False
    eng_score = sum(1 for w in words if w in english_stops)
    urdu_score = sum(1 for w in words if w in urdu_markers)
    return urdu_score > 0 and urdu_score >= eng_score

def contains_urdu_script(text):
    return bool(re.search(r'[\u0600-\u06FF]', text))

def get_comments_from_json(data, comments_list):
    if isinstance(data, dict):
        if data.get('kind') == 't1': 
            body = data.get('data', {}).get('body', '')
            if body and not contains_urdu_script(body) and is_roman_urdu(body):
                clean_text = body.replace("\n", " ").replace("\r", " ").strip()
                if len(clean_text) > 10:
                    comments_list.append(clean_text)
        for key, value in data.items():
            get_comments_from_json(value, comments_list)
    elif isinstance(data, list):
        for item in data:
            get_comments_from_json(item, comments_list)

def extract_post_id(url):
    """Extract post ID from Reddit URL"""
    # Example: https://www.reddit.com/r/pakistan/comments/abc123/title/
    match = re.search(r'/comments/([a-z0-9]+)', url)
    if match:
        return match.group(1)
    return None

async def batch_check_posts(session, urls, rate_limiter):
    """
    Phase 1: Batch check if posts exist using /api/info
    Returns list of valid post IDs
    """
    post_ids = []
    url_to_id = {}
    
    for url in urls:
        post_id = extract_post_id(url)
        if post_id:
            post_ids.append(f"t3_{post_id}")
            url_to_id[post_id] = url
    
    if not post_ids:
        return []
    
    valid_urls = []
    
    # Process in batches of 100 (Reddit's limit)
    for i in range(0, len(post_ids), BATCH_CHECK_SIZE):
        batch = post_ids[i:i + BATCH_CHECK_SIZE]
        batch_str = ",".join(batch)
        
        await rate_limiter.acquire()
        
        try:
            api_url = f"https://api.reddit.com/api/info.json?id={batch_str}"
            async with session.get(api_url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    # Check which posts exist
                    for child in data.get('data', {}).get('children', []):
                        post_id = child.get('data', {}).get('id')
                        if post_id and post_id in url_to_id:
                            valid_urls.append(url_to_id[post_id])
                    
                    print(f"✓ Batch check: {len(batch)} IDs → {len(data.get('data', {}).get('children', []))} valid")
                    
                elif response.status == 429:
                    print(f"⚠️ 429 during batch check, waiting...")
                    await asyncio.sleep(60)
                    # Retry this batch
                    continue
                    
        except Exception as e:
            print(f"⚠️ Batch check error: {str(e)[:50]}")
            # If batch check fails, include all URLs (fallback to old behavior)
            for post_id in [pid.replace('t3_', '') for pid in batch]:
                if post_id in url_to_id:
                    valid_urls.append(url_to_id[post_id])
        
        await asyncio.sleep(0.5)  # Small delay between batch checks
    
    return valid_urls

async def fetch_comments(session, url, index, total, semaphore, rate_limiter):
    """
    Phase 2: Fetch comments for a single valid post
    """
    json_url = url.rstrip('/') + ".json?sort=controversial"
    
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                await rate_limiter.acquire()
                await asyncio.sleep(random.uniform(0.1, 0.3))
                
                async with session.get(json_url, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        json_data = await response.json()
                        thread_comments = []
                        get_comments_from_json(json_data, thread_comments)
                        print(f"[{index+1}/{total}] ✓ {len(thread_comments)} comments")
                        return thread_comments
                    
                    elif response.status == 429:
                        print(f"[{index+1}/{total}] ⚠️ 429, waiting 60s...")
                        await asyncio.sleep(60)
                        continue
                    
                    elif response.status in [404, 403]:
                        print(f"[{index+1}/{total}] ⊘ {response.status}")
                        return []
                    
                    else:
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(3)
                            continue
                        return []
            
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(3)
                    continue
                print(f"[{index+1}/{total}] ⚠️ Error: {str(e)[:30]}")
                return []
        
        return []

# Token Bucket Rate Limiter
class TokenBucketRateLimiter:
    def __init__(self, requests_per_minute):
        self.requests_per_minute = requests_per_minute
        self.request_times = deque()
        self.lock = asyncio.Lock()
        
    async def acquire(self):
        async with self.lock:
            now = time.time()
            
            # Remove requests older than 60 seconds
            while self.request_times and now - self.request_times[0] > 60:
                self.request_times.popleft()
            
            # Wait if at limit
            if len(self.request_times) >= self.requests_per_minute:
                wait_time = 60 - (now - self.request_times[0]) + 1
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    now = time.time()
                    while self.request_times and now - self.request_times[0] > 60:
                        self.request_times.popleft()
            
            self.request_times.append(now)

async def process_batch(session, batch, start_index, total, semaphore, rate_limiter):
    """Process a batch with 2-phase approach"""
    
    # Phase 1: Batch check which posts exist (1 request per 100 posts)
    print(f"\n📋 Phase 1: Batch checking {len(batch)} posts...")
    valid_urls = await batch_check_posts(session, batch, rate_limiter)
    
    skipped = len(batch) - len(valid_urls)
    if skipped > 0:
        print(f"⚡ Skipped {skipped} deleted/invalid posts (saved {skipped} requests!)")
    
    # Phase 2: Fetch comments for valid posts
    if valid_urls:
        print(f"📥 Phase 2: Fetching comments from {len(valid_urls)} valid posts...")
        tasks = [
            fetch_comments(session, url, start_index + batch.index(url), total, semaphore, rate_limiter)
            for url in valid_urls
        ]
        return await asyncio.gather(*tasks)
    
    return []

async def scrape_all_urls(urls, start_from_batch=0):
    """Main scraping function with hybrid batch approach"""
    all_comments = []
    
    # Load existing
    if start_from_batch > 0:
        try:
            existing_df = pd.read_csv(OUTPUT_FILE)
            all_comments = existing_df['text'].tolist()
            print(f"📂 Loaded {len(all_comments)} existing comments")
        except:
            pass
    
    rate_limiter = TokenBucketRateLimiter(REQUESTS_PER_MINUTE)
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        batch_size = 20
        total_batches = (len(urls) + batch_size - 1) // batch_size
        
        for batch_idx in range(start_from_batch, total_batches):
            i = batch_idx * batch_size
            batch = urls[i:i + batch_size]
            
            print(f"\n{'='*70}")
            print(f"📦 Batch {batch_idx + 1}/{total_batches} (URLs {i+1} to {i+len(batch)})")
            print(f"{'='*70}")
            
            batch_start = time.time()
            results = await process_batch(session, batch, i, len(urls), semaphore, rate_limiter)
            batch_time = time.time() - batch_start
            
            batch_comments = 0
            for comments in results:
                batch_comments += len(comments)
                all_comments.extend(comments)
            
            print(f"\n✅ Batch complete! {batch_comments} comments in {batch_time:.1f}s")
            print(f"📊 Total: {len(all_comments)} comments")
            
            # Save checkpoint AND progress
            if all_comments:
                df = pd.DataFrame(all_comments, columns=["text"]).drop_duplicates()
                df.to_csv(OUTPUT_FILE, index=False)
                save_progress(batch_idx + 1, total_batches)
                print(f"💾 Saved: {len(df)} unique | Progress: {batch_idx + 1}/{total_batches}")
            else:
                # Even if no comments, save progress to avoid re-processing
                save_progress(batch_idx + 1, total_batches)
            
            await asyncio.sleep(1)
    
    return all_comments

def save_progress(batch_num, total):
    with open(PROGRESS_FILE, 'w') as f:
        f.write(f"{batch_num}/{total}")

def load_progress():
    try:
        with open(PROGRESS_FILE, 'r') as f:
            batch_num, _ = f.read().strip().split('/')
            return int(batch_num)
    except:
        return 0

def main():
    try:
        targets_df = pd.read_csv(INPUT_FILE)
        links = targets_df['url'].tolist()
        
        start_batch = load_progress()
        
        print(f"\n{'='*70}")
        print(f"🚀 HYBRID BATCH REDDIT SCRAPER")
        print(f"{'='*70}")
        print(f"Total URLs: {len(links)}")
        print(f"Strategy: 2-Phase Hybrid")
        print(f"  Phase 1: Batch check 100 IDs = 1 request")
        print(f"  Phase 2: Fetch comments from valid posts only")
        print(f"Rate: {REQUESTS_PER_MINUTE} req/min")
        print(f"Batch size: 20 URLs")
        if start_batch > 0:
            print(f"🔄 RESUMING from batch {start_batch + 1}")
        print(f"{'='*70}\n")
        
        start_time = time.time()
        all_comments = asyncio.run(scrape_all_urls(links, start_from_batch=start_batch))
        
        df = pd.DataFrame(all_comments, columns=["text"]).drop_duplicates()
        df.to_csv(OUTPUT_FILE, index=False)
        
        # Clean up progress file on completion
        try:
            import os
            os.remove(PROGRESS_FILE)
        except:
            pass
        
        elapsed = time.time() - start_time
        
        print(f"\n{'='*70}")
        print(f"🎉 SUCCESS!")
        print(f"{'='*70}")
        print(f"Unique comments: {len(df)}")
        print(f"Time: {elapsed/60:.1f} minutes")
        print(f"Avg per URL: {elapsed/len(links):.1f}s")
        print(f"{'='*70}\n")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Interrupted. Run again to resume.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()