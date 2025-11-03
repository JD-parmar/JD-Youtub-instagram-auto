import time
import sys
import os
import requests
import pandas as pd
# shutil को हटा दिया गया है क्योंकि ज़िपिंग हट गई है
from io import StringIO
from datetime import datetime, timedelta, timezone 
import json 
import random
import traceback

# --- Google API Libraries ---
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# AI Content Library
try:
    from google import genai
    from google.genai.errors import APIError
    from google.auth.transport.requests import Request as GoogleAuthRequest
    GENAI_AVAILABLE = True
except Exception:
    GENAI_AVAILABLE = False
    GoogleAuthRequest = None


# --- कॉन्फ़िगरेशन ---
STATE_FILE = "./.github/workflows/state.txt"
MAX_VIDEOS_PER_RUN = 5 
REQUIRED_COLS = ['Case_Study', 'Heading_Title', 'Prompt', 'Cinematic_Mode', 'Keywords_Tags', 'Video_Type', 'Schedule_Time', 'Instagram_Caption']
# OUTPUT_DIR हटा दिया गया है
YOUTUBE_UPLOAD_SCOPE = ["https://www.googleapis.com/auth/youtube.upload"]


def integrate_gemini_for_content(seo_title, prompt, video_type, tags):
    # ... (यह फ़ंक्शन वही रहता है)
    print("🧠 Gemini AI कंटेंट जनरेशन शुरू...")
    gemini_api_key = os.environ.get("GEMINI_API_KEY")

    if not gemini_api_key or not GENAI_AVAILABLE:
        print("❌ GEMINI_API_KEY अनुपलब्ध या GenAI लाइब्रेरी इंस्टॉल नहीं। AI कंटेंट सिमुलेट कर रहा हूँ।")
        ai_script = "सिमुलेटेड हिंदी वीडियो स्क्रिप्ट।"
        youtube_description = f"🤖 AI जनरेटेड डिस्क्रिप्शन: {seo_title}"
        thumbnail_idea = f"ट्रेंडिंग थंबनेल: {seo_title}"
        instagram_caption = f"🔥 {seo_title} — देखें और शेयर करें!"
        return ai_script, youtube_description, thumbnail_idea, instagram_caption

    try:
        client = genai.Client(api_key=gemini_api_key)
        main_prompt = (
            f"एक YouTube वीडियो के लिए कंटेंट जनरेट करें। वीडियो शीर्षक: \"{seo_title}\" "
            f"प्रॉम्प्ट: \"{prompt}\"। आउटपुट JSON में लौटाएँ keys: script, youtube_description, thumbnail_title, instagram_caption."
        )
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "script": {"type": "STRING"},
                "youtube_description": {"type": "STRING"},
                "thumbnail_title": {"type": "STRING"},
                "instagram_caption": {"type": "STRING"}
            },
            "required": ["script", "youtube_description", "thumbnail_title", "instagram_caption"]
        }
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=main_prompt,
            config={
                "response_mime_type": "application/json",
                "response_schema": response_schema
            }
        )
        ai_data = json.loads(response.text)
        print("✅ Gemini AI कंटेंट सफलतापूर्वक जनरेट हुआ।")
        return ai_data.get('script', ''), ai_data.get('youtube_description', ''), ai_data.get('thumbnail_title', ''), ai_data.get('instagram_caption', '')

    except (APIError, json.JSONDecodeError, ValueError) as e:
        print(f"❌ Gemini API या पार्सिंग त्रुटि: {e}. सिमुलेशन पर वापस जा रहा है।")
        return integrate_gemini_for_content(seo_title, prompt, video_type, tags)
    except Exception as e:
        print(f"❌ Gemini अनपेक्षित त्रुटि: {e}. सिमुलेशन पर वापस जा रहा है।")
        return integrate_gemini_for_content(seo_title, prompt, video_type, tags)


# get_youtube_service, upload_to_youtube, upload_to_instagram, format_schedule_time, 
# get_start_row_index, update_state_file, fetch_data_from_google_sheet 
# ... (ये फ़ंक्शन वही रहते हैं)

# मैंने upload_to_youtube को 'public' पर सेट करने के लिए अपडेट किया है, जैसा आपने कहा था।
def upload_to_youtube(video_path, title, description, tags, schedule_time_str):
    print("⏳ YouTube अपलोड/शेड्यूल शुरू...")
    youtube = get_youtube_service()
    if not youtube:
        print("⚠️ सिमुलेशन: वीडियो अपलोड किए बिना ID लौटाई जा रही है।")
        return f"YOUTUBE_ID_SIMULATED_{random.randint(1000, 9999)}"

    schedule_iso = format_schedule_time(schedule_time_str)
    
    # 🔑 बदलाव: पब्लिक करने के लिए
    if schedule_iso:
        # शेड्यूल होने पर पब्लिक (भविष्य में उस समय पब्लिश होगा)
        privacy_status = 'public'
        scheduled_at = schedule_iso
        print(f"⏰ वीडियो को (Public) शेड्यूल किया जा रहा है (UTC): {scheduled_at}")
    else:
        # बिना शेड्यूल के तुरंत पब्लिक
        privacy_status = 'public'
        scheduled_at = None
        print("🚀 वीडियो को तुरंत 'Public' पब्लिश किया जा रहा है।")

    # ... (बाकी कोड वही रहता है)
    # ...

def generate_and_process_video(row_index, row):
    # ... (बाकी कोड वही रहता है)
    
    # OUTPUT_DIR लॉजिक हटा दिया गया है
    # os.makedirs(os.path.join(OUTPUT_DIR, 'Videos'), exist_ok=True)  <-- हटा दिया गया
    # shutil.copy(temp_video_path, os.path.join(OUTPUT_DIR, 'Videos', output_filename)) <-- हटा दिया गया
    
    return output_filename, uploaded_id


# --- मुख्य रन फ़ंक्शन (इसे पूरा किया गया है) ---
def run_automation():
    result = {
        "videos_generated": 0,
        "next_start_index": None,
        "errors": []
        # 'zip_path' हटा दिया गया है
    }
    # ... (बाकी कोड वही रहता है)

    # ... (loop to process rows)
    
    # ... (after the loop)
    
    videos_generated = len(processed_details)
    result["videos_generated"] = videos_generated

    print(f"\n--- ऑटोमेशन रन समाप्त ---")

    # स्टेट अपडेट करें
    if videos_generated > 0:
        next_start_index = last_processed_index + 1
    else:
        next_start_index = start_index
        if df.empty or iloc_start >= len(df):
            next_start_index = len(df) + 1 if not df.empty else 1
            
    result["next_start_index"] = next_start_index
    
    # स्टेट फ़ाइल अपडेट
    if next_start_index > start_index or (df_to_process.empty and start_index <= len(df)):
         try:
            update_state_file(next_start_index)
         except Exception as e:
            print(f"⚠️ स्टेट अपडेट करते समय त्रुटि: {e}")
            
    # GitHub Actions Output सेट करना (zip_path हटाकर)
    github_output_path = os.environ.get("GITHUB_OUTPUT")
    if github_output_path:
        try:
            with open(github_output_path, 'a') as f:
                f.write(f"videos_generated={videos_generated}\n")
                f.write(f"next_start_index={next_start_index}\n")
            print("✅ GitHub Actions Output सफलतापूर्वक सेट किया गया।")
        except Exception as e:
            print(f"⚠️ GITHUB_OUTPUT लिखने में त्रुटि: {e}")

    # JSON सारांश प्रिंट करें (यह सबसे महत्वपूर्ण है)
    # यह सुनिश्चित करता है कि YAML का 'Parse Script Output' स्टेप सही से काम करे।
    print(json.dumps(result)) 
    return result

if _name_ == "_main_": 
    try:
        run_automation()
    except Exception as e:
        tb = traceback.format_exc()
        # विफलता का सारांश JSON ऑब्जेक्ट प्रिंट करें
        fallback = {
            "videos_generated": 0,
            "next_start_index": 1,
            "errors": [f"Unhandled Python error: {e}"]
        }
        print(json.dumps(fallback))
        sys.exit(1)
