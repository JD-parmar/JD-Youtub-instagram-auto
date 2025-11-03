# यह स्क्रिप्ट GitHub Actions वर्कफ़्लो द्वारा चलाई जाती है।
# इसमें डेटा लोडिंग, कंटेंट जेनरेशन (Gemini), स्टेट मैनेजमेंट, 
# और YouTube अपलोड (प्लेसहोल्डर) के फ़ंक्शन शामिल हैं।

import sys
import os
import json
import pandas as pd
import zipfile
import re
# from google import genai
# from google.genai import types

# --- Configuration ---
STATE_FILE_PATH = os.path.join(os.path.dirname(_file_), 'state.txt')
MAX_ITEMS_TO_PROCESS = 1 
ZIP_FILE_NAME = "production_package.zip"
TEMP_UPLOAD_DIR = "./temp_upload_dir"

# Secrets (Required for actual upload)
YOUTUBE_CLIENT_ID = os.getenv('YOUTUBE_CLIENT_ID')
YOUTUBE_CLIENT_SECRET = os.getenv('YOUTUBE_CLIENT_SECRET')
YOUTUBE_REFRESH_TOKEN = os.getenv('YOUTUBE_REFRESH_TOKEN')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# --- Utility Functions ---
def read_state(default_index=1):
    """राज्य फ़ाइल से अगले शुरुआती इंडेक्स को पढ़ता है।"""
    try:
        with open(STATE_FILE_PATH, 'r') as f:
            return int(f.read().strip())
    except Exception as e:
        print(f"WARN: Failed to read state file or file not found: {e}. Starting from index {default_index}.")
        return default_index

def write_state(next_index):
    """अगले रन के लिए नए शुरुआती इंडेक्स को राज्य फ़ाइल में लिखता है।"""
    try:
        if next_index > 1:
            with open(STATE_FILE_PATH, 'w') as f:
                f.write(str(next_index))
            print(f"INFO: Successfully updated state file to index {next_index}.")
    except Exception as e:
        print(f"ERROR: Failed to write state file: {e}")

# --- Content Generation (MOCK/PLACEHOLDER) ---

def generate_joke_with_gemini(api_key: str, topic: str) -> str:
    """Gemini API का उपयोग करके टॉपिक से संबंधित जोक जनरेट करने का मॉक फ़ंक्शन।"""
    if not api_key:
        return f"MOCK JOKE: ऑटोमेशन बॉट ने YouTube पर वीडियो अपलोड क्यों किया? क्योंकि वह किसी का इंतज़ार नहीं कर सकता था! (विषय: {topic})"
    
    # यहाँ वास्तविक Gemini API कॉल को लागू करें
    return f"MOCK JOKE: उस विषय को क्या कहते हैं जो केवल एक बार चलता है? 'start_index = 1' वाला जोक! (विषय: {topic})"

def generate_content_with_gemini(api_key: str, prompt: str) -> str:
    """Gemini API का उपयोग करके कंटेंट जनरेट करने का मॉक फ़ंक्शन। (इसे वास्तविक API कॉल से बदलें)"""
    # यहाँ वास्तविक Gemini API कॉल को लागू करें
    topic_name = prompt.split('topic:')[1].strip() if 'topic:' in prompt else 'Default Topic'
    
    # जोक जनरेट करें और उसे कंटेंट में जोड़ें
    generated_joke = generate_joke_with_gemini(api_key, topic_name)
    
    mock_response = (
        f"--- वीडियो स्क्रिप्ट फॉर टॉपिक: {topic_name} ---\n\n"
        f"यह {topic_name} विषय पर आपके वीडियो का मुख्य स्क्रिप्ट कंटेंट है।\n"
        f"कैप्शन: आज का सबसे बड़ा रहस्य जानें! | Video Title: {topic_name} (Index {read_state()})\n"
        f"Tags: automation, gemini, youtube, shorts\n\n"
        f"--- आज का जोक ---\n"
        f"जोक: {generated_joke}\n\n"
        f"--- फाइल का अंत ---"
    )
    return mock_response

def mock_video_rendering(output_path: str):
    """वीडियो फ़ाइल बनाने के लिए प्लेसहोल्डर फ़ंक्शन।"""
    try:
        # 500 KB डमी फ़ाइल
        with open(output_path, 'wb') as f:
            f.write(b'\x00' * (500 * 1024)) 
        return True
    except Exception:
        return False

# --- Main Pipeline Logic ---

def run_automation_pipeline(csv_url: str, gemini_api_key: str):
    """मुख्य स्वचालन तर्क को चलाता है।"""
    start_index = read_state()
    videos_generated = 0
    next_start_index = start_index
    current_zip_path = ""

    try:
        df = pd.read_csv(csv_url)
        df['index'] = df.index + 1
        rows_to_process = df[df['index'] >= start_index].head(MAX_ITEMS_TO_PROCESS)
        
        if rows_to_process.empty:
            write_state(start_index)
            return { "videos_generated": 0, "zip_path": "", "next_start_index": start_index }

    except Exception as e:
        print(f"CRITICAL ERROR: Failed to load data from CSV URL: {e}")
        return { "videos_generated": 0, "zip_path": "", "next_start_index": start_index }

    # ZIP फ़ाइल बनाने के लिए 'w' मोड का उपयोग करें
    try:
        with zipfile.ZipFile(ZIP_FILE_NAME, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for _, row in rows_to_process.iterrows():
                current_row_index = row['index']
                topic = row.get('Topic', f'Auto Topic {current_row_index}')
                
                script_file_name = f"script_{current_row_index}.txt"
                video_file_name = f"video_{current_row_index}.mp4"
                
                # 1. कंटेंट जनरेट करें (जिसमें अब जोक भी शामिल है)
                final_script_content = generate_content_with_gemini(gemini_api_key, f"topic: {topic}")
                
                # 2. वीडियो फ़ाइल बनाएं
                if not mock_video_rendering(video_file_name):
                     print(f"WARN: Video rendering failed for row {current_row_index}.")
                     continue # इस रो को छोड़ दें
                     
                # 3. ZIP फ़ाइल में कंटेंट और वीडियो दोनों जोड़ें
                zip_file.writestr(script_file_name, final_script_content)
                zip_file.write(video_file_name, arcname=video_file_name) 
                
                # 4. अस्थायी वीडियो फ़ाइल हटाएँ
                os.remove(video_file_name)

                videos_generated += 1
                next_start_index = current_row_index + 1
                current_zip_path = ZIP_FILE_NAME 
                print(f"INFO: Successfully processed and packaged row {current_row_index}.")
                # हम केवल एक ही वीडियो प्रोसेस कर रहे हैं (MAX_ITEMS_TO_PROCESS = 1)
                break
                
    except Exception as e:
        print(f"CRITICAL ERROR: Failed during ZIP creation or file handling: {e}")
        # अगर कोई अस्थायी फ़ाइल बची है तो उसे हटा दें
        if os.path.exists(video_file_name):
             os.remove(video_file_name)
        videos_generated = 0 

    # --- 4. अगले रन के लिए स्टेट को अपडेट करें ---
    write_state(next_start_index)
    
    # JSON आउटपुट stdout पर प्रिंट करें
    output = {
        "videos_generated": videos_generated,
        "zip_path": current_zip_path, 
        "next_start_index": next_start_index
    }
    
    return output

# --- YouTube Upload Logic ---

def upload_to_youtube(video_path: str, script_path: str):
    """YouTube Data API v3 का उपयोग करके वीडियो अपलोड करने के लिए प्लेसहोल्डर फ़ंक्शन।"""
    if not os.path.exists(video_path) or not os.path.exists(script_path):
        print(f"CRITICAL: Video or script file not found for upload. Video: {video_path}, Script: {script_path}")
        return False
        
    # स्क्रिप्ट फ़ाइल से मेटाडेटा निकालें
    try:
        with open(script_path, 'r') as f:
            content = f.read()
        
        title_match = re.search(r'Video Title:\s*(.*)', content)
        caption_match = re.search(r'कैप्शन:\s*(.*)', content)
        tags_match = re.search(r'Tags:\s*(.*)', content)
        
        title = title_match.group(1).strip() if title_match else "Default Upload Title"
        description = caption_match.group(1).strip() if caption_match else "Default Upload Description"
        tags = [t.strip() for t in tags_match.group(1).split(',')] if tags_match else []
        
    except Exception as e:
        print(f"WARN: Failed to parse metadata from script: {e}. Using defaults.")
        title = "Default Upload Title"
        description = "Default Upload Description"
        tags = []

    print(f"INFO: Attempting to upload video '{title}' from {video_path}")
    
    # --- यहाँ वास्तविक YouTube API अपलोड लॉजिक लागू करें ---
    
    # Mock Success for demonstration
    print(f"MOCK SUCCESS: Video '{title}' successfully uploaded to YouTube.")
    return True

def run_youtube_upload_step(zip_file_path: str):
    """YouTube पर अपलोड करने के लिए ZIP फ़ाइल से फ़ाइलें निकालता है और अपलोड करता है।"""
    print("INFO: Running YouTube upload step.")
    
    # ZIP फ़ाइल को एक अस्थायी डायरेक्टरी में निकालें
    if os.path.exists(TEMP_UPLOAD_DIR):
        os.system(f"rm -rf {TEMP_UPLOAD_DIR}")
    os.makedirs(TEMP_UPLOAD_DIR)
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(TEMP_UPLOAD_DIR)
            print(f"INFO: Successfully extracted artifact to {TEMP_UPLOAD_DIR}")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to extract ZIP file: {e}. Skipping upload.")
        return

    # एक्सट्रैक्टेड फ़ाइलों को खोजें और अपलोड करें
    uploaded_count = 0
    video_files = [f for f in os.listdir(TEMP_UPLOAD_DIR) if f.endswith('.mp4')]
    
    for video_name in video_files:
        video_path = os.path.join(TEMP_UPLOAD_DIR, video_name)
        script_name = video_name.replace('video_', 'script_').replace('.mp4', '.txt')
        script_path = os.path.join(TEMP_UPLOAD_DIR, script_name)
        
        if os.path.exists(script_path):
            if upload_to_youtube(video_path, script_path):
                uploaded_count += 1
            else:
                print(f"WARN: Failed to upload {video_name}.")
        else:
            print(f"WARN: Corresponding script {script_name} not found for {video_name}.")

    print(f"INFO: Total videos processed for upload: {uploaded_count}")
    
    # अस्थायी फ़ाइलें हटाएँ
    os.system(f"rm -rf {TEMP_UPLOAD_DIR}")


def main():
    """वर्कफ़्लो के दो अलग-अलग मोड को संभालता है।"""
    # YouTube अपलोड स्टेप के लिए तर्क को संभालें
    if len(sys.argv) > 2 and sys.argv[1] == 'upload-youtube':
        zip_path = sys.argv[2]
        run_youtube_upload_step(zip_path)
        return

    # मुख्य स्वचालन पाइपलाइन तर्क को संभालें
    if len(sys.argv) < 2:
        print("ERROR: Google Sheet CSV URL not provided.")
        sys.exit(1)
        
    csv_url = sys.argv[1]
    
    output = run_automation_pipeline(csv_url, GEMINI_API_KEY)
    
    # JSON आउटपुट stdout पर प्रिंट करें
    print(json.dumps(output))


if _name_ == "_main_":
    main()
