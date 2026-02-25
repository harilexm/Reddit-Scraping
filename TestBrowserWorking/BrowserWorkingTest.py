from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

print("SYSTEM WORKING")

# Setup Chrome Options
options = webdriver.ChromeOptions()

try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    print("Checking old reddit")
    driver.get("https://old.reddit.com")
    page_title = driver.title
    print(f"Page Title: {page_title}")
    time.sleep(3)
    print("Success")

except Exception as e:
    print(f"FAILURE")
    print(f"Error: {e}")

finally:
    if 'driver' in locals():
        driver.quit()