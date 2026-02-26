import pandas as pd
import time
import random
import requests
import json
import os
from datetime import datetime, timedelta

TARGET_SUBS = [
    # Major Cities & Regions
    "pakistan", "karachi", "islamabad", "lahore", "peshawar", "quetta", "multan", "faisalabad", "rawalpindi", "kashmir", "gilgitbaltistan",
    
    # Universities & Education
    "LUMS", "NUST", "UET", "Comsats", "IBA", "FASTNU", "PakistanMentoringClub",
    
    # Lifestyle & Interests
    "Fitness_Pakistan", "PakistanFashionAdvice", "PakistaniFood", "PakistaniTech", "PakistanAutoHub", "PakGamers", "PakCricket", "CokeStudio", "PakistaniTV",
    
    # Social & Discussion
    "PakistaniiConfessions", "PakLounge", "ActualPakistan", "PakistanDiscussions", "TeenPakistani", "PakistaniTwenties", "Overseas_Pakistani", "DesiVideoMemes", "PakMemeistan", "chutyapa", "TheRealPakistan", "PAK",
    
    # Niche & Specific Interests
    "PAKCELEBGOSSIP", "FIREPakistan", "PakistaniPolitics", "Ancient_Pakistan", "PakistanHistory", "exmuslim", "Pashtun", "Balochistan", "Sindh", "Punjab"
]

SORT_ORDERS = ["controversial", "top", "new", "hot"]
GOAL_LINKS = 50000 # Change to as many links you want
OUTPUT_FILE = "links2.csv"
PROGRESS_FILE = "link_scrape_progress.json"
MAX_PAGES_PER_SORT = 40  # 40 pages × 100 posts = 4000 posts max per sort
REQUEST_DELAY = (2.0, 4.0)  # Random delay range in seconds (unauthenticated = 10 req/min)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) RedditLinkScraper/2.0 by ScrapeUmer'
}


# PROGRESS TRACKING (Resume Support)
def save_progress(completed_subs, collected_links):
    """Save current progress to resume after crash/interrupt."""
    data = {
        'completed_subs': completed_subs,
        'total_links': len(collected_links),
        'timestamp': datetime.now().isoformat()
    }
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(data, f)

def load_progress():
    """Load progress from previous run."""
    try:
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    except:
        return None

def cleanup_progress():
    """Remove progress file on completion."""
    try:
        os.remove(PROGRESS_FILE)
    except:
        pass

# TIMING UTILITIES
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
    return format_duration(remaining)

# SCRAPING LOGIC (JSON API)
def scrape_subreddit(sub, sort, collected_links):
    """
    Scrape links from a subreddit using old.reddit.com JSON API.
    Uses the `after` parameter for pagination instead of Selenium page clicking.
    Returns number of new links found.
    """
    base_url = f"https://old.reddit.com/r/{sub}/{sort}.json"
    params = {
        'limit': 100,
        't': 'all',  # Time range: all time
        'raw_json': 1
    }
    
    new_count = 0
    page = 0
    
    while page < MAX_PAGES_PER_SORT:
        try:
            response = requests.get(base_url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                children = data.get('data', {}).get('children', [])
                
                if not children:
                    break
                
                page_new = 0
                for child in children:
                    post_data = child.get('data', {})
                    permalink = post_data.get('permalink', '')
                    if permalink:
                        full_url = f"https://www.reddit.com{permalink}"
                        if full_url not in collected_links:
                            collected_links.add(full_url)
                            new_count += 1
                            page_new += 1
                
                # Get the `after` token for next page
                after = data.get('data', {}).get('after')
                if not after or page_new == 0:
                    break  # No more pages or no new links
                
                params['after'] = after
                page += 1
                
                # Rate limiting delay
                time.sleep(random.uniform(*REQUEST_DELAY))
                
            elif response.status_code == 429:
                print(f"      ⚠️  Rate limited (429). Waiting 60s...")
                time.sleep(60)
                continue  # Retry same page
                
            elif response.status_code == 403:
                # Subreddit might be private/banned
                break
                
            elif response.status_code == 404:
                break
                
            else:
                print(f"      ⚠️  HTTP {response.status_code}. Retrying in 10s...")
                time.sleep(10)
                continue
                
        except requests.exceptions.Timeout:
            print(f"      ⚠️  Timeout. Retrying in 5s...")
            time.sleep(5)
            continue
        except Exception as e:
            print(f"      ⚠️  Error: {str(e)[:60]}. Skipping sort.")
            break
    
    return new_count

# MAIN
def main():
    print(f"\n{'='*70}")
    print(f"🚀 REDDIT LINK SCRAPER v2.0 (JSON API)")
    print(f"{'='*70}")
    print(f"  Method:     JSON API (no browser needed)")
    print(f"  Subreddits: {len(TARGET_SUBS)}")
    print(f"  Sort orders: {', '.join(SORT_ORDERS)}")
    print(f"  Goal:       {GOAL_LINKS:,} links")
    print(f"  Started:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    collected_links = set()
    completed_subs = []
    
    # Resume from previous run if available
    progress = load_progress()
    if progress:
        completed_subs = progress.get('completed_subs', [])
        # Reload existing links from CSV
        try:
            existing_df = pd.read_csv(OUTPUT_FILE)
            collected_links = set(existing_df['url'].tolist())
            print(f"🔄 RESUMING: {len(collected_links)} links from previous run")
            print(f"   Skipping {len(completed_subs)} already-completed subreddits\n")
        except:
            completed_subs = []
    
    # Filter out already-completed subs
    remaining_subs = [s for s in TARGET_SUBS if s not in completed_subs]
    total_subs = len(remaining_subs)
    
    global_start = time.time()
    
    try:
        for idx, sub in enumerate(remaining_subs):
            if len(collected_links) >= GOAL_LINKS:
                print(f"\n Goal of {GOAL_LINKS:,} links reached!")
                break
            
            sub_start = time.time()
            elapsed_total = time.time() - global_start
            eta = estimate_eta(elapsed_total, idx, total_subs) if idx > 0 else "calculating..."
            
            print(f"[{idx+1}/{total_subs}] r/{sub}")
            print(f"  ⏱ Elapsed: {format_duration(elapsed_total)} | ETA: {eta} | Links: {len(collected_links):,}")
            
            sub_new = 0
            for sort in SORT_ORDERS:
                sort_new = scrape_subreddit(sub, sort, collected_links)
                sub_new += sort_new
                if sort_new > 0:
                    print(f" /{sort}: +{sort_new} links")
                
                if len(collected_links) >= GOAL_LINKS:
                    break
            
            sub_time = time.time() - sub_start
            print(f" r/{sub} done: +{sub_new} new | {format_duration(sub_time)} | Total: {len(collected_links):,}\n")
            
            # Save checkpoint after each subreddit
            completed_subs.append(sub)
            if collected_links:
                pd.DataFrame(list(collected_links), columns=["url"]).to_csv(OUTPUT_FILE, index=False)
                save_progress(completed_subs, collected_links)
    
    except KeyboardInterrupt:
        print(f"\n Interrupted! Progress saved. Run again to resume.")
    
    finally:
        # Final save
        total_time = time.time() - global_start
        if collected_links:
            df = pd.DataFrame(list(collected_links), columns=["url"])
            df.to_csv(OUTPUT_FILE, index=False)
            save_progress(completed_subs, collected_links)
        
        print(f"\n{'='*70}")
        print(f"FINAL RESULTS")
        print(f"{'='*70}")
        print(f"  Total links:  {len(collected_links):,}")
        print(f"  Total time:   {format_duration(total_time)}")
        print(f"  Subs scraped: {len(completed_subs)}/{len(TARGET_SUBS)}")
        if len(completed_subs) > 0:
            print(f"  Avg per sub:  {format_duration(total_time / len(completed_subs))}")
        print(f"  Finished:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Output file:  {OUTPUT_FILE}")
        print(f"{'='*70}\n")
        
        # Clean up progress file only if fully complete
        if len(completed_subs) >= len(TARGET_SUBS) or len(collected_links) >= GOAL_LINKS:
            cleanup_progress()
            print(" Scrape complete! Progress file cleaned up.")

if __name__ == "__main__":
    main()