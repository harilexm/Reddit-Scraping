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
from datetime import datetime, timedelta

# fastText-based language detection (pip install fast-langdetect)
try:
    from fast_langdetect import detect as ft_detect
    FASTTEXT_AVAILABLE = True
    print("✅ fastText language detection loaded")
except ImportError:
    ft_detect = None
    FASTTEXT_AVAILABLE = False
    print("⚠️  fast-langdetect not installed. Using keyword-only detection.")
    print("   Install with: pip install fast-langdetect")

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

english_stops = set(stopwords.words('english'))

# ============================================================
# ROMAN URDU MARKERS (Extended Set)
# ============================================================
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
    'aur', 'lekin', 'magar', 'bhi', 'toh', 'to', 'tu', 'agar', 'agr', 'phir', 'phr', 
    'kyunke', 'kyunkay', 'jab', 'tab', 'isliye',
    
    # Interrogatives/Negations
    'kya', 'kyun', 'kyu', 'kab', 'kahan', 'kesay', 'kaise', 'kon', 'kaun', 
    'nahi', 'nhi', 'na', 'ni', 'mat', 'haan', 'ji', 'jee',
    
    # Fillers/Slang
    'acha', 'achha', 'wese', 'wesay', 'matlab', 'bas', 'bs', 'yaar', 'yar', 'bhai', 
    'shyd', 'shayed', 'bilkul', 'zaroor', 'ab', 'abhi',
    
    # === EXTENDED SET (50+ new words) ===
    # Common everyday words
    'kuch', 'bohot', 'bohat', 'bahut', 'sab', 'koi', 'dusra', 'pehle', 'baad',
    'wala', 'wali', 'wale', 'ghar', 'dost', 'log', 'cheez', 'kaam', 'din',
    'raat', 'waqt', 'soch', 'samajh', 'pata', 'chalo', 'dekho', 'suno',
    'bata', 'rakho', 'jao', 'aao', 'khush', 'dukh', 'mushkil', 'asaan',
    'zyada', 'kam', 'theek', 'sahi', 'galat', 'kharab', 'behtareen',
    
    # Money/numbers
    'paisay', 'paisa', 'rupee', 'lakh', 'crore', 'hazaar',
    
    # Internet/social slang
    'janab', 'sahab', 'arre', 'oye', 'haye', 'wah', 'shabash',
    
    # Religious/cultural expressions
    'mashallah', 'inshallah', 'alhamdulillah', 'subhanallah',
    
    # Additional verbs/adjectives
    'dekha', 'suna', 'mila', 'chala', 'gaya', 'gayi', 'gaye', 'aya', 'ayi', 'aaye',
    'laga', 'lagi', 'lage', 'hoga', 'hogi', 'honge', 'hua', 'hui', 'hue',
    'sakta', 'sakti', 'sakte', 'chahiye', 'zaruri', 'lazmi',
}

# Common Roman Urdu bigrams for improved detection
URDU_BIGRAMS = {
    'kya hai', 'nahi hai', 'ho gaya', 'kar raha', 'kaise ho',
    'bhai sahab', 'yaar bhai', 'kya kar', 'bohat acha', 'bohot acha',
    'nahi ho', 'ho sakta', 'kar sakta', 'kya hua', 'theek hai',
    'ho raha', 'ho rahi', 'kar rahe', 'nahi karna', 'kuch nahi',
    'pata nahi', 'sab log', 'koi nahi', 'bilkul nahi', 'zaroor hai',
}

# ============================================================
# FILE CONFIGURATION
# ============================================================
INPUT_FILE = r"C:\Users\DeLL\Desktop\Reddit Scraping\ScrapeLinks\links1.csv"
OUTPUT_FILE = "commentsScrape1.csv"
PROGRESS_FILE = "scrape_progress1.txt"

# ============================================================
# SCRAPING CONFIGURATION
# ============================================================
'''HYBRID BATCH APPROACH + MORE CHILDREN:
    Phase 1: Batch check 100 post IDs at once using /api/info (counts as 1 request)
    Phase 2: Fetch comments for valid posts (individual requests) WITH limit=500
    Phase 3: Fetch hidden "more children" comments via /api/morechildren
    Result: Skip deleted/404 posts AND get ALL comments!'''

BATCH_CHECK_SIZE = 100
REQUESTS_PER_MINUTE = 10
CONCURRENT_REQUESTS = 2
MAX_RETRIES = 5  # Increased from 3

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) RomanUrduHunter/3.0 by ScrapeUmer'
}

# ============================================================
# TIMING UTILITIES
# ============================================================
def format_duration(seconds):
    """Format seconds into human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{int(m)}m {int(s)}s"
    else:
        h, remainder = divmod(seconds, 3600)
        m, s = divmod(remainder, 60)
        return f"{int(h)}h {int(m)}m {int(s)}s"

def estimate_eta(elapsed, done, total):
    """Estimate time remaining."""
    if done == 0:
        return "calculating..."
    rate = elapsed / done
    remaining = (total - done) * rate
    eta_time = datetime.now() + timedelta(seconds=remaining)
    return f"{format_duration(remaining)} (finish ~{eta_time.strftime('%H:%M:%S')})"

# ============================================================
# COMMENT CLEANING PIPELINE
# ============================================================
def clean_comment(text):
    """Clean Reddit comment text: remove URLs, markdown, quotes, excess whitespace."""
    # Remove URLs
    text = re.sub(r'https?://\S+', '', text)
    # Remove Reddit-style quotes
    text = re.sub(r'&gt;.*', '', text)
    # Remove markdown links [text](url)
    text = re.sub(r'\[.*?\]\(.*?\)', '', text)
    # Remove markdown formatting characters
    text = re.sub(r'[*_~`#]', '', text)
    # Remove newlines/carriage returns
    text = text.replace("\n", " ").replace("\r", " ")
    # Collapse excess whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text if len(text) > 10 else None

# ============================================================
# ROMAN URDU DETECTION (Enhanced with fastText + Bigrams)
# ============================================================
def is_roman_urdu(text):
    """
    Enhanced Roman Urdu detection using:
    1. fastText as negative filter (reject clearly non-English/non-Urdu languages)
    2. Bigram matching for common Roman Urdu phrases
    3. Extended keyword scoring with ratio-based threshold
    
    Note: fastText sees Roman Urdu as "English" (Latin script), so we can't use it
    for positive detection. Instead we use it to filter out French, Spanish, etc.
    """
    words = text.lower().split()
    if len(words) < 3:
        return False
    
    # Method 1: fastText negative filter
    # Reject text that is clearly another Latin-script language (not en/ur)
    if FASTTEXT_AVAILABLE:
        try:
            results = ft_detect(text, model='lite')
            if results:
                lang = results[0].get('lang', '')
                score = results[0].get('score', 0)
                # If high confidence in a non-English, non-Urdu language → reject
                non_urdu_langs = {'fr', 'es', 'de', 'it', 'pt', 'nl', 'pl', 'ro', 'sv',
                                  'da', 'no', 'fi', 'cs', 'hr', 'id', 'ms', 'tr', 'vi'}
                if lang in non_urdu_langs and score > 0.5:
                    return False
        except:
            pass
    
    # Method 2: Bigram matching
    text_lower = text.lower()
    bigram_hits = sum(1 for bg in URDU_BIGRAMS if bg in text_lower)
    
    # Method 3: Extended keyword scoring
    urdu_score = sum(1 for w in words if w in urdu_markers)
    eng_score = sum(1 for w in words if w in english_stops)
    
    # Add bigram weight (each bigram counts as 2 keyword hits)
    urdu_score += bigram_hits * 2
    
    # Ratio-based detection
    total_meaningful = urdu_score + eng_score
    if total_meaningful == 0:
        return False
    
    urdu_ratio = urdu_score / total_meaningful
    return urdu_ratio >= 0.4 and urdu_score >= 2

def contains_urdu_script(text):
    return bool(re.search(r'[\u0600-\u06FF]', text))

# ============================================================
# COMMENT EXTRACTION (with "more children" tracking)
# ============================================================
def get_comments_from_json(data, comments_list, more_ids=None):
    """
    Recursively extract comments from Reddit JSON response.
    Also collects 'more' comment IDs that need separate fetching.
    """
    if more_ids is None:
        more_ids = []
    
    if isinstance(data, dict):
        if data.get('kind') == 't1':
            body = data.get('data', {}).get('body', '')
            if body:
                # Clean first, then check language
                cleaned = clean_comment(body)
                if cleaned and not contains_urdu_script(cleaned) and is_roman_urdu(cleaned):
                    comments_list.append(cleaned)
        elif data.get('kind') == 'more':
            # Collect hidden comment IDs for Phase 3!
            children = data.get('data', {}).get('children', [])
            if children:
                more_ids.extend(children)
        
        for key, value in data.items():
            get_comments_from_json(value, comments_list, more_ids)
    
    elif isinstance(data, list):
        for item in data:
            get_comments_from_json(item, comments_list, more_ids)
    
    return more_ids

def extract_post_id(url):
    """Extract post ID from Reddit URL"""
    match = re.search(r'/comments/([a-z0-9]+)', url)
    if match:
        return match.group(1)
    return None

# ============================================================
# RATE LIMITER (Token Bucket)
# ============================================================
class TokenBucketRateLimiter:
    def __init__(self, requests_per_minute):
        self.requests_per_minute = requests_per_minute
        self.request_times = deque()
        self.lock = asyncio.Lock()
        
    async def acquire(self):
        async with self.lock:
            now = time.time()
            while self.request_times and now - self.request_times[0] > 60:
                self.request_times.popleft()
            if len(self.request_times) >= self.requests_per_minute:
                wait_time = 60 - (now - self.request_times[0]) + 1
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    now = time.time()
                    while self.request_times and now - self.request_times[0] > 60:
                        self.request_times.popleft()
            self.request_times.append(now)

# ============================================================
# PHASE 1: BATCH CHECK POSTS
# ============================================================
async def batch_check_posts(session, urls, rate_limiter):
    """Batch check if posts exist using /api/info (1 request per 100 posts)"""
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
    
    for i in range(0, len(post_ids), BATCH_CHECK_SIZE):
        batch = post_ids[i:i + BATCH_CHECK_SIZE]
        batch_str = ",".join(batch)
        
        await rate_limiter.acquire()
        
        try:
            api_url = f"https://api.reddit.com/api/info.json?id={batch_str}"
            async with session.get(api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    for child in data.get('data', {}).get('children', []):
                        post_id = child.get('data', {}).get('id')
                        if post_id and post_id in url_to_id:
                            valid_urls.append(url_to_id[post_id])
                    
                    print(f"   ✓ Batch check: {len(batch)} IDs → {len(data.get('data', {}).get('children', []))} valid")
                    
                elif response.status == 429:
                    reset_after = int(response.headers.get('X-Ratelimit-Reset', 60))
                    print(f"   ⚠️ 429 during batch check, waiting {reset_after}s...")
                    await asyncio.sleep(reset_after + 1)
                    
        except Exception as e:
            print(f"   ⚠️ Batch check error: {str(e)[:50]}")
            for post_id in [pid.replace('t3_', '') for pid in batch]:
                if post_id in url_to_id:
                    valid_urls.append(url_to_id[post_id])
        
        await asyncio.sleep(0.5)
    
    return valid_urls

# ============================================================
# PHASE 3: FETCH "MORE CHILDREN" COMMENTS
# ============================================================
async def fetch_more_children(session, link_id, children_ids, rate_limiter):
    """
    Fetch hidden comments using Reddit's /api/morechildren endpoint.
    Processes in chunks of 100 (API limit).
    """
    all_comments = []
    
    if not children_ids:
        return all_comments
    
    # Filter out empty strings
    children_ids = [c for c in children_ids if c]
    
    for i in range(0, len(children_ids), 100):
        chunk = children_ids[i:i + 100]
        
        url = "https://api.reddit.com/api/morechildren.json"
        params = {
            'link_id': f't3_{link_id}',
            'children': ','.join(chunk),
            'api_type': 'json',
            'sort': 'controversial'
        }
        
        for attempt in range(3):
            try:
                await rate_limiter.acquire()
                async with session.get(url, params=params, headers=headers, 
                                       timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        things = data.get('json', {}).get('data', {}).get('things', [])
                        for thing in things:
                            if thing.get('kind') == 't1':
                                body = thing.get('data', {}).get('body', '')
                                if body:
                                    cleaned = clean_comment(body)
                                    if cleaned and not contains_urdu_script(cleaned) and is_roman_urdu(cleaned):
                                        all_comments.append(cleaned)
                        break  # Success
                    elif resp.status == 429:
                        reset_after = int(resp.headers.get('X-Ratelimit-Reset', 60))
                        await asyncio.sleep(reset_after + 1)
                    else:
                        await asyncio.sleep(2 ** attempt)
                        
            except Exception as e:
                await asyncio.sleep(2 ** attempt)
    
    return all_comments

# ============================================================
# PHASE 2: FETCH COMMENTS WITH EXPONENTIAL BACKOFF
# ============================================================
async def fetch_comments(session, url, index, total, semaphore, rate_limiter):
    """Fetch comments for a single valid post, including 'more children'."""
    # limit=500 to get maximum comments in one request
    json_url = url.rstrip('/') + ".json?sort=controversial&limit=500"
    post_id = extract_post_id(url)
    
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                await rate_limiter.acquire()
                await asyncio.sleep(random.uniform(0.1, 0.3))
                
                async with session.get(json_url, headers=headers, 
                                       timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        json_data = await response.json()
                        thread_comments = []
                        
                        # Phase 2: Extract visible comments + collect "more" IDs
                        more_ids = get_comments_from_json(json_data, thread_comments)
                        
                        # Phase 3: Fetch hidden "more children" comments
                        more_comments = []
                        if more_ids and post_id:
                            more_comments = await fetch_more_children(
                                session, post_id, more_ids, rate_limiter
                            )
                            thread_comments.extend(more_comments)
                        
                        more_info = f" (+{len(more_comments)} hidden)" if more_comments else ""
                        print(f"   [{index+1}/{total}] ✓ {len(thread_comments)} comments{more_info}")
                        return thread_comments
                    
                    elif response.status == 429:
                        # Read rate limit headers for smarter backoff
                        reset_after = int(response.headers.get('X-Ratelimit-Reset', 60))
                        print(f"   [{index+1}/{total}] ⚠️ 429, waiting {reset_after}s...")
                        await asyncio.sleep(reset_after + 1)
                        continue
                    
                    elif response.status in [404, 403]:
                        print(f"   [{index+1}/{total}] ⊘ {response.status}")
                        return []
                    
                    else:
                        # Exponential backoff for other errors
                        wait = 2 ** attempt
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(wait)
                            continue
                        return []
            
            except Exception as e:
                wait = 2 ** attempt
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(wait)
                    continue
                print(f"   [{index+1}/{total}] ⚠️ Error: {str(e)[:30]}")
                return []
        
        return []

# ============================================================
# BATCH PROCESSING (2-Phase + More Children)
# ============================================================
async def process_batch(session, batch, start_index, total, semaphore, rate_limiter):
    """Process a batch with 3-phase approach"""
    
    # Phase 1: Batch check which posts exist
    print(f"\n   📋 Phase 1: Batch checking {len(batch)} posts...")
    valid_urls = await batch_check_posts(session, batch, rate_limiter)
    
    skipped = len(batch) - len(valid_urls)
    if skipped > 0:
        print(f"   ⚡ Skipped {skipped} deleted/invalid posts (saved {skipped} requests!)")
    
    # Phase 2+3: Fetch comments for valid posts (including more children)
    if valid_urls:
        print(f"   📥 Phase 2+3: Fetching comments from {len(valid_urls)} valid posts...")
        tasks = [
            fetch_comments(session, url, start_index + i, total, semaphore, rate_limiter)
            for i, url in enumerate(valid_urls)
        ]
        return await asyncio.gather(*tasks)
    
    return []

# ============================================================
# MAIN SCRAPING LOOP
# ============================================================
async def scrape_all_urls(urls, start_from_batch=0):
    """Main scraping function with hybrid batch approach + timing"""
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
        
        scrape_start = time.time()
        
        for batch_idx in range(start_from_batch, total_batches):
            i = batch_idx * batch_size
            batch = urls[i:i + batch_size]
            
            elapsed_total = time.time() - scrape_start
            batches_done = batch_idx - start_from_batch
            eta = estimate_eta(elapsed_total, batches_done, total_batches - start_from_batch)
            
            print(f"\n{'='*70}")
            print(f"📦 Batch {batch_idx + 1}/{total_batches} (URLs {i+1} to {i+len(batch)})")
            print(f"⏱️  Elapsed: {format_duration(elapsed_total)} | ETA: {eta}")
            print(f"{'='*70}")
            
            batch_start = time.time()
            results = await process_batch(session, batch, i, len(urls), semaphore, rate_limiter)
            batch_time = time.time() - batch_start
            
            batch_comments = 0
            for comments in results:
                batch_comments += len(comments)
                all_comments.extend(comments)
            
            # Calculate speed metrics
            urls_per_min = (len(batch) / batch_time * 60) if batch_time > 0 else 0
            
            print(f"\n   ✅ Batch complete! {batch_comments} comments in {format_duration(batch_time)}")
            print(f"   📊 Total: {len(all_comments)} comments | Speed: {urls_per_min:.1f} URLs/min")
            
            # Save checkpoint AND progress
            if all_comments:
                df = pd.DataFrame(all_comments, columns=["text"]).drop_duplicates()
                df.to_csv(OUTPUT_FILE, index=False)
                save_progress(batch_idx + 1, total_batches)
                print(f"   💾 Saved: {len(df)} unique | Progress: {batch_idx + 1}/{total_batches}")
            else:
                save_progress(batch_idx + 1, total_batches)
            
            await asyncio.sleep(1)
    
    return all_comments

# ============================================================
# PROGRESS SAVE/LOAD
# ============================================================
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

# ============================================================
# MAIN ENTRY POINT
# ============================================================
def main():
    try:
        targets_df = pd.read_csv(INPUT_FILE)
        links = targets_df['url'].tolist()
        
        start_batch = load_progress()
        
        print(f"\n{'='*70}")
        print(f"🚀 HYBRID BATCH REDDIT COMMENT SCRAPER v3.0")
        print(f"{'='*70}")
        print(f"  Total URLs: {len(links)}")
        print(f"  Strategy: 3-Phase Hybrid")
        print(f"    Phase 1: Batch check 100 IDs = 1 request")
        print(f"    Phase 2: Fetch comments (limit=500) from valid posts")
        print(f"    Phase 3: Fetch hidden 'more children' comments")
        print(f"  Roman Urdu: {'fastText + Keywords + Bigrams' if FASTTEXT_AVAILABLE else 'Keywords + Bigrams (fastText not installed)'}")
        print(f"  Rate: {REQUESTS_PER_MINUTE} req/min")
        print(f"  Batch size: 20 URLs")
        print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if start_batch > 0:
            print(f"  🔄 RESUMING from batch {start_batch + 1}")
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
        print(f"🎉 SCRAPE COMPLETE!")
        print(f"{'='*70}")
        print(f"  Unique comments: {len(df)}")
        print(f"  Total time:      {format_duration(elapsed)}")
        print(f"  Avg per URL:     {elapsed/max(len(links),1):.1f}s")
        print(f"  Finished:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Output file:     {OUTPUT_FILE}")
        print(f"{'='*70}\n")
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Interrupted at {datetime.now().strftime('%H:%M:%S')}. Run again to resume.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()