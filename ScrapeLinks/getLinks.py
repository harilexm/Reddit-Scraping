import pandas as pd
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# TARGETS
TARGET_SUBS = [
    # Major Cities & Regions
    "pakistan", "karachi", "islamabad", "lahore", "peshawar", "quetta", "multan", "faisalabad", "rawalpindi", "kashmir","gilgitbaltistan",
    
    # Universities & Education
    "LUMS","NUST","UET","Comsats","IBA","FASTNU","PakistanMentoringClub",
    
    # Lifestyle & Interests
    "Fitness_Pakistan","PakistanFashionAdvice","PakistaniFood","PakistaniTech","PakistanAutoHub","PakGamers","PakCricket","CokeStudio","PakistaniTV",
    
    # Social & Discussion
    "PakistaniiConfessions","PakLounge","ActualPakistan","PakistanDiscussions","TeenPakistani","PakistaniTwenties","Overseas_Pakistani","DesiVideoMemes","PakMemeistan","chutyapa","TheRealPakistan","PAK",
    
    # Niche & Specific Interests
    "PAKCELEBGOSSIP","FIREPakistan","PakistaniPolitics","Ancient_Pakistan","PakistanHistory","exmuslim","Pashtun","Balochistan","Sindh","Punjab"
]

GOAL_LINKS = 50000 # 50k links
OUTPUT_FILE = "links1.csv"

# Setup
options = webdriver.ChromeOptions()
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Remove 'webdriver' indicator
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
  "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
})

collected_links = set()

def perform_back_forward_maneuver():
    """Back then Forward maneuver(bypass security)."""
    print("Back-Forward maneuver")
    try:
        driver.back()
        time.sleep(3)
        driver.forward()
        time.sleep(5)
    except Exception as e:
        print(f"[Error] maneuver failed: {e}")

def wait_and_clear_security():
    """Waits for content, if it fails, tries the Back-Forward trick."""
    try:
        # Check if content exists
        content_found = len(driver.find_elements(By.CSS_SELECTOR, "div.thing")) > 0
        
        if not content_found:
            # Content not found, might be blocked. Wait a bit.
            time.sleep(5)
            # Re-check
            if len(driver.find_elements(By.CSS_SELECTOR, "div.thing")) == 0:
                perform_back_forward_maneuver()
        
        # Final wait for elements to be clickable
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.thing, span.next-button"))
        )
        return True
    except:
        return False

def bypass_18_plus():
    try:
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for btn in buttons:
            if "yes" in btn.text.lower() or "over 18" in btn.text.lower():
                btn.click()
                print("   [Bypass] Clicked 18+ Confirmation.")
                time.sleep(2)
    except: pass

try:
    print(f"--- INITIATING TRICK HARVEST (Goal: {GOAL_LINKS}) ---")
    
    for sub in TARGET_SUBS:
        if len(collected_links) >= GOAL_LINKS: break
        
        url = f"https://old.reddit.com/r/{sub}/controversial/?sort=controversial&t=all"
        print(f"\n[Target] r/{sub} ...")
        driver.get(url)
        
        # Apply the logic
        if not wait_and_clear_security():
            print(f"   [!] Failed to clear security for r/{sub} even with trick.")
            # One last try: Force Refresh
            driver.refresh()
            time.sleep(5)

        bypass_18_plus()
        
        sub_count = 0
        while sub_count < 500:
            posts = driver.find_elements(By.CSS_SELECTOR, "div.thing")
            if not posts: break
            
            new_on_page = 0
            for post in posts:
                try:
                    link = post.find_element(By.CSS_SELECTOR, "a.title").get_attribute("href")
                    if link and "/comments/" in link and link not in collected_links:
                        collected_links.add(link)
                        sub_count += 1
                        new_on_page += 1
                except: continue
            
            print(f"Captured {new_on_page} | Total: {len(collected_links)}")
            
            if len(collected_links) >= GOAL_LINKS: break

            # Next Page
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "span.next-button a")
                next_url = next_btn.get_attribute("href")
                driver.get(next_url)
                time.sleep(random.uniform(3, 5))
                bypass_18_plus() # Check for age gate on next pages too
            except:
                break

finally:
    driver.quit()
    if collected_links:
        pd.DataFrame(list(collected_links), columns=["url"]).to_csv(OUTPUT_FILE, index=False)
        print(f"DONE. Total: {len(collected_links)} links in {OUTPUT_FILE}")