from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

print("--- SYSTEM CHECK INITIATED ---")

# Setup Chrome Options
options = webdriver.ChromeOptions()
# We run it with a visible head first to ensure it works. 
# Later we will make it headless (invisible).

try:
    # This automatically downloads the correct driver for your Chrome version
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    # 1. Go to old.reddit
    print("Navigating to target...")
    driver.get("https://old.reddit.com")
    
    # 2. Check Title
    page_title = driver.title
    print(f"Target Acquired. Page Title: {page_title}")
    
    # 3. Wait 3 seconds so you can see it working
    time.sleep(3)
    
    print("--- SYSTEM CHECK PASSED ---")
    
except Exception as e:
    print(f"--- SYSTEM FAILURE ---")
    print(f"Error: {e}")

finally:
    # Close the browser
    if 'driver' in locals():
        driver.quit()