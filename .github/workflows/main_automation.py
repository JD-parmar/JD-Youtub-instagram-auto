# main_automation.py
# --------------------------------------------------------------
# यह स्क्रिप्ट GitHub Actions वर्कफ़्लो द्वारा चलाई जाती है।
# यह डेटा लोडिंग, Gemini कंटेंट जेनरेशन, ZIP पैकेज बनाना,
# स्टेट मैनेजमेंट और (मॉक) YouTube अपलोड को हैंडल करती है।
# --------------------------------------------------------------

import sys
import os
import json
import pandas as pd
import zipfile
import re

# --- Configuration ---
STATE_FILE_PATH = os.path.join(os.path.dirname(__file__), 'state.txt')
MAX_ITEMS_TO_PROCESS = 1
ZIP_FILE_NAME = "production_package.zip"
TEMP_UPLOAD_DIR = "./temp_upload_dir"

# Secrets (fetched from environment)
YOUTUBE_CLIENT_ID = os.getenv('YOUTUBE_CLIENT_ID')
YOUTUBE_CLIENT_SECRET = os.getenv('YOUTUBE_CLIENT_SECRET')
YOUTUBE_REFRESH_TOKEN = os.getenv('YOUTUBE_REFRESH_TOKEN')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# --------------------------------------------------------------
# Utility Functions
# --------------------------------------------------------------
def read_state(default_index=1):
    """राज्य फ़ाइल से अगले शुरुआती इंडेक्स को पढ़ता है।"""
    if not os.path.exists(STATE_FILE_PATH):
        with open(STATE_FILE_PATH, 'w', encoding='utf-8') as f:
            f.write(str(default_index))
        return default_index

    try:
        with open(STATE_FILE_PATH, 'r', encoding='utf-8') as f:
            return int(f.read().strip())
    except Exception as e:
        print(f"WARN: Failed to read state file: {e}. Starting from index {default_index}.")
        return default_index


def write_state(next_index):
    """अगले रन के लिए नए शुरुआती इंडेक्स को राज्य फ़ाइल में लिखता है।"""
    try:
        with open(STATE_FILE_PATH, 'w', encoding='utf-8') as f:
            f.write(str(next_index))
        print(f"INFO: Updated state file → index {next_index}")
    except Exception as e:
        print(f"ERROR: Failed to write state file: {e}")


# --------------------------------------------------------------
# Mock Gemini Content Generation
# --------------------------------------------------------------
def generate_joke_with_gemini(api_key: str, topic: str) -> str:
    """Gemini API का उपयोग करके टॉपिक से संबंधित जोक जनरेट करने का मॉक फ़ंक्शन।"""
    if not api_key:
        return f"MOCK JOKE: बॉट ने वीडियो क्यों बनाया? क्योंकि उसे व्यूज़ चाहिए थे! (विषय: {topic})"
    # असली Gemini API कॉल यहां जोड़ें
    return f"MOCK JOKE: '{topic}' पर एक बार एक बॉट था जो कभी नहीं रुका!"


def generate_content_with_gemini(api_key: str, topic: str) -> str:
    """Gemini API का उपयोग करके कंटेंट जनरेट करने का मॉक फ़ंक्शन।"""
    joke = generate_joke_with_gemini(api_key, topic)
    current_index = read_state()

    return (
        f"--- वीडियो स्क्रिप्ट ---\n"
        f"विषय: {topic}\n\n"
        f"यह '{topic}' विषय पर एक छोटा वीडियो स्क्रिप्ट है।\n"
        f"कैप्शन: आज का रहस्य जानिए!\n"
        f"Video Title: {topic} (Index {current_index})\n"
        f"Tags: automation, gemini, youtube, shorts\n\n"
        f"--- आज का जोक ---\n{joke}\n"
    )


def mock_video_rendering(output_path: str) -> bool:
    """वीडियो फ़ाइल बनाने का मॉक (डमी) फ़ंक्शन।"""
    try:
        with open(output_path, 'wb') as f:
            f.write(b'\x00' * (500 * 1024))  # 500 KB dummy file
        return True
    except Exception as e:
        print(f"ERROR: Failed to create video: {e}")
        return False


# --------------------------------------------------------------
# Main Automation Pipeline
# --------------------------------------------------------------
def run_automation_pipeline(csv_url: str, gemini_api_key: str):
    """मुख्य ऑटोमेशन पाइपलाइन लॉजिक।"""
    start_index = read_state()
    next_start_index = start_index
    videos_generated = 0
    current_zip_path = f"./{ZIP_FILE_NAME}"

    try:
        df = pd.read_csv(csv_url)
        df['index'] = df.index + 1
        rows_to_process = df[df['index'] >= start_index].head(MAX_ITEMS_TO_PROCESS)
        if rows_to_process.empty:
            print("INFO: कोई नया डेटा प्रोसेस करने के लिए नहीं है।")
            write_state(start_index)
            return {"videos_generated": 0, "zip_path": "", "next_start_index": start_index}
    except Exception as e:
        print(f"CRITICAL: CSV लोड करने में असफल - {e}")
        return {"videos_generated": 0, "zip_path": "", "next_start_index": start_index}

    # ZIP बनाना शुरू
    try:
        if os.path.exists(ZIP_FILE_NAME):
            os.remove(ZIP_FILE_NAME)

        with zipfile.ZipFile(ZIP_FILE_NAME, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for _, row in rows_to_process.iterrows():
                idx = row['index']
                topic = row.get('Topic', f'Auto Topic {idx}')

                script_name = f"script_{idx}.txt"
                video_name = f"video_{idx}.mp4"

                # कंटेंट जनरेट करें
                content = generate_content_with_gemini(gemini_api_key, topic)
                with open(script_name, 'w', encoding='utf-8') as f:
                    f.write(content)

                # वीडियो बनाएँ
                if not mock_video_rendering(video_name):
                    print(f"WARN: Video rendering failed for {idx}")
                    continue

                # ZIP में ऐड करें
                zipf.write(script_name)
                zipf.write(video_name)

                # क्लीनअप
                os.remove(script_name)
                os.remove(video_name)

                videos_generated += 1
                next_start_index = idx + 1
                print(f"INFO: Successfully processed row {idx}")
                break  # केवल एक वीडियो बनाना (MAX_ITEMS_TO_PROCESS = 1)
    except Exception as e:
        print(f"CRITICAL: ZIP निर्माण विफल - {e}")
        videos_generated = 0

    write_state(next_start_index)

    return {
        "videos_generated": videos_generated,
        "zip_path": current_zip_path if videos_generated > 0 else "",
        "next_start_index": next_start_index,
    }


# --------------------------------------------------------------
# YouTube Upload (Mock)
# --------------------------------------------------------------
def upload_to_youtube(video_path: str, script_path: str) -> bool:
    """YouTube पर वीडियो अपलोड करने का मॉक फ़ंक्शन।"""
    if not os.path.exists(video_path) or not os.path.exists(script_path):
        print(f"CRITICAL: Video या Script नहीं मिला: {video_path}, {script_path}")
        return False

    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
        title = re.search(r'Video Title:\s*(.*)', content)
        caption = re.search(r'कैप्शन:\s*(.*)', content)
        title = title.group(1).strip() if title else "Default Title"
        caption = caption.group(1).strip() if caption else "Default Caption"
    except Exception as e:
        print(f"WARN: Script पार्स करने में त्रुटि - {e}")
        title, caption = "Default Title", "Default Caption"

    print(f"MOCK UPLOAD: '{title}' → {video_path}")
    return True


def run_youtube_upload_step(zip_file_path: str):
    """ZIP फ़ाइल से वीडियो और स्क्रिप्ट निकालकर अपलोड करें।"""
    if not os.path.exists(zip_file_path):
        print(f"ERROR: ZIP नहीं मिला: {zip_file_path}")
        return

    if os.path.exists(TEMP_UPLOAD_DIR):
        os.system(f"rm -rf {TEMP_UPLOAD_DIR}")
    os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(TEMP_UPLOAD_DIR)
    except Exception as e:
        print(f"ERROR: ZIP एक्सट्रैक्ट करने में विफल: {e}")
        return

    uploaded = 0
    for file in os.listdir(TEMP_UPLOAD_DIR):
        if file.endswith('.mp4'):
            video_path = os.path.join(TEMP_UPLOAD_DIR, file)
            script_path = video_path.replace('video_', 'script_').replace('.mp4', '.txt')
            if upload_to_youtube(video_path, script_path):
                uploaded += 1

    print(f"INFO: Uploaded {uploaded} videos to YouTube (mock).")
    os.system(f"rm -rf {TEMP_UPLOAD_DIR}")


# --------------------------------------------------------------
# Main Entrypoint
# --------------------------------------------------------------
def main():
    if len(sys.argv) > 2 and sys.argv[1] == 'upload-youtube':
        zip_path = sys.argv[2]
        run_youtube_upload_step(zip_path)
        return

    if len(sys.argv) < 2:
        print("ERROR: CSV URL प्रदान नहीं किया गया।")
        sys.exit(1)

    csv_url = sys.argv[1]
    output = run_automation_pipeline(csv_url, GEMINI_API_KEY)
    print(json.dumps(output))


if __name__ == "__main__":
    main()
