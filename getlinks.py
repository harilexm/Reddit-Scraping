import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
# 1. The Targets (Where the fights happen)
TARGET_SUBS = [
    "chutyapa", 
    "pakistan", 
    "karachi", 
    "lahore", 
    "islamabad", 
    "Overseas_Pakistani",
    "PakistaniiConfessions" # Good for drama/arguments
]

# 2. How many links do you want TOTAL?
# Note: 10,000 is A LOT. It might take hours. 
# Let's set it to 2,000 for a solid start. Change to 10000 if you have time.
GOAL_LINKS = 2000 

OUTPUT_FILE = "massive_thread_list.csv"

# Setup Driver
options = webdriver.ChromeOptions()
options.add_argument("--disable-gpu")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

collected_links = set() # Using a 'set' automatically removes duplicates
total_collected = 0

try:
    print(f"--- STARTING MASSIVE HARVEST (Goal: {GOAL_LINKS}) ---")
    
    for sub in TARGET_SUBS:
        if total_collected >= GOAL_LINKS: break
        
        # We start with Controversial - All Time (The gold mine of hate speech)
        base_url = f"https://old.reddit.com/r/{sub}/controversial/?sort=controversial&t=all"
        driver.get(base_url)
        time.sleep(2)
        
        print(f"\n>>> Mining Subreddit: r/{sub}")
        
        while total_collected < GOAL_LINKS:
            # 1. Grab all links on current page
            posts = driver.find_elements(By.CSS_SELECTOR, "a.title")
            new_links_found = 0
            
            for post in posts:
                link = post.get_attribute("href")
                if link and "/r/" in link and "comments" in link:
                    if link not in collected_links:
                        collected_links.add(link)
                        total_collected += 1
                        new_links_found += 1
            
            print(f"   Collected {new_links_found} new links. Total: {total_collected}")
            
            # 2. Try to click "Next >"
            try:
                # In old.reddit, the next button is usually in a span class="next-button"
                next_button = driver.find_element(By.CSS_SELECTOR, "span.next-button a")
                next_url = next_button.get_attribute("href")
                
                # Move to next page
                driver.get(next_url)
                time.sleep(2) # Be polite to Reddit server
            except:
                print(f"   End of the line for r/{sub} (No 'Next' button).")
                break # Break the while loop, move to next subreddit

finally:
    driver.quit()
    
    # Save to CSV
    if collected_links:
        df = pd.DataFrame(list(collected_links), columns=["url"])
        df.to_csv(OUTPUT_FILE, index=False)
        print("------------------------------------------------")
        print(f"HARVEST COMPLETE.")
        print(f"Total Unique Threads Found: {len(collected_links)}")
        print(f"Saved to: {OUTPUT_FILE}")
    else:
        print("Failed to collect links.")