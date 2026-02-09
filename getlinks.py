import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

TARGET_SUBS = [
    "chutyapa", 
    "pakistan", 
    "karachi", 
    "lahore", 
    "islamabad", 
    "Overseas_Pakistani",
    "PakistaniiConfessions"
]

GOAL_LINKS = 2000 
OUTPUT_FILE = "links.csv"

# Setup Driver
options = webdriver.ChromeOptions()
options.add_argument("--disable-gpu")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

collected_links = set()
total_collected = 0

try:
    print(f"STARTING... (Goal: {GOAL_LINKS})")
    
    for sub in TARGET_SUBS:
        if total_collected >= GOAL_LINKS: break
        
        # Controversial sorting for finding comments where peoples are triggered
        base_url = f"https://old.reddit.com/r/{sub}/controversial/?sort=controversial&t=all"
        driver.get(base_url)
        time.sleep(2)
        
        print(f"\n Scrapping  Subreddit: r/{sub}")
        
        while total_collected < GOAL_LINKS:
            # Get all links on current page
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
            
            # clicking "Next button"
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, "span.next-button a")
                next_url = next_button.get_attribute("href")
                
                driver.get(next_url)
                time.sleep(2) # Avoid spamming
            except:
                print(f"End for r/{sub} (No 'Next' button).")
                break

finally:
    driver.quit()
    if collected_links:
        df = pd.DataFrame(list(collected_links), columns=["url"])
        df.to_csv(OUTPUT_FILE, index=False)
        print("------------------------------------------------")
        print(f"Total LINKS: {len(collected_links)}")
        print(f"Saved to: {OUTPUT_FILE}")
    else:
        print("Failed to collect links.")