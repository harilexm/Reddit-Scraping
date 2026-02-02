import pandas as pd
import time
import re
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
INPUT_FILE = "massive_thread_list.csv" 
OUTPUT_FILE = "raw_comments_only.csv"

# Speed Settings
COMMENTS_PER_THREAD = 20  
MAX_THREADS_TO_VISIT = 5000 

def is_mostly_english(text):
    """
    Returns True if text is statistically English.
    """
    eng_markers = {'the', 'and', 'is', 'that', 'with', 'for', 'are', 'this', 'you', 'not', 'have', 'what', 'because'}
    urdu_markers = {'hai', 'main', 'tum', 'aap', 'ka', 'ki', 'ko', 'aur', 'hum', 'yeh', 'woh', 'bhi', 'kar', 'tha', 'thi', 'ne', 'k', 'nhi', 'ni'}
    
    words = text.lower().split()
    if not words: return True
    
    eng_count = sum(1 for w in words if w in eng_markers)
    urdu_count = sum(1 for w in words if w in urdu_markers)
    
    # Logic: If English words > Urdu words, it's English. Skip it.
    if eng_count > urdu_count: return True
    # Logic: If NO Urdu words at all, it's suspicious. Skip it.
    if urdu_count == 0: return True
    
    return False

def contains_urdu_script(text):
    """Returns True if text contains Arabic/Urdu letters"""
    if re.search(r'[\u0600-\u06FF]', text): return True
    return False

# Setup Driver
options = webdriver.ChromeOptions()
options.add_argument("--disable-gpu")
# options.add_argument("--headless") # Uncomment to run in background
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# We use a simple list to store text only
dataset = []

try:
    print("--- STARTING PURE TEXT EXTRACTION ---")
    
    try:
        targets_df = pd.read_csv(INPUT_FILE)
        links = targets_df['url'].tolist()
        random.shuffle(links) 
    except:
        print(f"Error: {INPUT_FILE} not found. Please run Step 2 first.")
        links = []

    print(f"Loaded {len(links)} threads.")
    
    threads_processed = 0

    for index, link in enumerate(links):
        if threads_processed >= MAX_THREADS_TO_VISIT: break
        
        print(f"[{index+1}/{len(links)}] Scraping: {link}")
        
        url_with_sort = link + "?sort=controversial" if "?" not in link else link + "&sort=controversial"
        
        try:
            driver.get(url_with_sort)
            time.sleep(1) 
            
            comment_area = driver.find_element(By.CSS_SELECTOR, "div.commentarea")
            entries = comment_area.find_elements(By.CSS_SELECTOR, "div.entry")
            
            captured_this_thread = 0
            
            for entry in entries:
                if captured_this_thread >= COMMENTS_PER_THREAD: break
                
                try:
                    usertext = entry.find_element(By.CSS_SELECTOR, "div.usertext-body").text.strip()
                    
                    # --- FILTERS ---
                    # 1. Clean Junk
                    if not usertext or usertext in ["[deleted]", "[removed]"] or len(usertext) < 15: continue
                    if "DARK MODE" in usertext: continue
                    
                    # 2. Remove Urdu Script
                    if contains_urdu_script(usertext): continue
                    
                    # 3. Remove English
                    if is_mostly_english(usertext): continue

                    # --- SUCCESS ---
                    # Clean newlines to keep CSV neat
                    clean_text = usertext.replace("\n", " ").replace("\r", " ")
                    
                    print(f"   [SAVED]: {clean_text[:50]}...")
                    
                    dataset.append(clean_text)
                    captured_this_thread += 1
                    
                except:
                    continue
            
            threads_processed += 1
            
        except:
            print("   Skipping (Link Error)")
            continue
            
        # SAVE PROGRESS every 20 threads
        if threads_processed % 20 == 0:
            df = pd.DataFrame(dataset, columns=["text"])
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"   >>> BACKUP SAVED: {len(df)} rows <<<")

finally:
    driver.quit()
    
    if dataset:
        # Final Save
        df = pd.DataFrame(dataset, columns=["text"])
        df.drop_duplicates(inplace=True)
        df.to_csv(OUTPUT_FILE, index=False)
        print("------------------------------------------------")
        print(f"EXTRACTION COMPLETE.")
        print(f"Total Unique Comments: {len(df)}")
        print(f"Saved to: {OUTPUT_FILE}")
    else:
        print("No data collected.")