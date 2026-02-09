import pandas as pd
import time
import re
import requests
import random
import nltk
from nltk.corpus import stopwords

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

INPUT_FILE = "lists.csv"
OUTPUT_FILE = "commentsScrape.csv"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) RomanUrduHunter/1.1 by ScrapeUmer'
}

def is_roman_urdu(text):
    words = text.lower().split()
    if not words: return False
    eng_score = sum(1 for w in words if w in english_stops)
    urdu_score = sum(1 for w in words if w in urdu_markers)
    return urdu_score > 0 and urdu_score >= eng_score # Balanced or more Urdu

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

# MAIN
try:
    targets_df = pd.read_csv(INPUT_FILE)
    links = targets_df['url'].tolist()
    
    # load existing data to resume
    try:
        final_dataset = pd.read_csv(OUTPUT_FILE)['text'].tolist()
        print(f"Resuming from {len(final_dataset)} existing records...")
    except:
        final_dataset = []

    print(f" Scrapping... ({len(links)} links) ---")
    
    for index, url in enumerate(links):
        # Skip if we already have it
        json_url = url.rstrip('/') + ".json?sort=controversial"
        
        success = False
        while not success:
            try:
                response = requests.get(json_url, headers=headers)
                
                if response.status_code == 200:
                    json_data = response.json()
                    thread_comments = []
                    get_comments_from_json(json_data, thread_comments)
                    final_dataset.extend(thread_comments)
                    print(f"[{index+1}/{len(links)}] Captured {len(thread_comments)} | Total: {len(final_dataset)}")
                    success = True
                    time.sleep(random.uniform(2, 4)) # 2-4 sec sleep

                elif response.status_code == 429:
                    print("(429)... Sleep for 60 seconds")
                    time.sleep(60) # reset block
                
                else:
                    print(f"[{index+1}] Failed status {response.status_code}. Skipping thread.")
                    success = True # Skip

            except Exception as e:
                print(f"Request error: {e}")
                time.sleep(5)
                success = True

        # Save Checkpoint every 20 threads
        if (index + 1) % 20 == 0:
            pd.DataFrame(final_dataset, columns=["text"]).drop_duplicates().to_csv(OUTPUT_FILE, index=False)

    # Final Save
    df = pd.DataFrame(final_dataset, columns=["text"]).drop_duplicates()
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSUCCESS! Total Unique Rows: {len(df)}")

except Exception as e:
    print(f"System Error: {e}")