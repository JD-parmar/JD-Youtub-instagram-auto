import time
import sys
import os
import requests
import pandas as pd
import shutil
from io import StringIO
from datetime import datetime, timedelta, timezone 
import json 
import random
import traceback

# AI कंटेंट के लिए Google GenAI लाइब्रेरी (optional)
try:
    from google import genai
    from google.genai.errors import APIError
    GENAI_AVAILABLE = True
except Exception:
    GENAI_AVAILABLE = False

# YouTube अपलोडिंग के लिए आवश्यक API क्लाइंट
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
except Exception:
    GoogleAuthRequest = None

# --- कॉन्फ़िगरेशन ---
STATE_FILE = "./.github/workflows/state.txt"
MAX_VIDEOS_PER_RUN = 5 
REQUIRED_COLS = ['Case_Study', 'Heading_Title', 'Prompt', 'Cinematic_Mode', 'Keywords_Tags', 'Video_Type', 'Schedule_Time', 'Instagram_Caption']
OUTPUT_DIR = f"Production_Package_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
# सुधार 1: YouTube अपलोड स्कोप को सही किया गया है।
YOUTUBE_UPLOAD_SCOPE = ["https://www.googleapis.com/auth/youtube.upload"]

def integrate_gemini_for_content(seo_title, prompt, video_type, tags):
    print("🧠 Gemini AI कंटेंट जनरेशन शुरू...")
    gemini_api_key = os.environ.get("GEMINI_API_KEY")

    if not gemini_api_key or not GENAI_AVAILABLE:
        print("❌ GEMINI_API_KEY अनुपलब्ध या GenAI लाइब्रेरी इंस्टॉल नहीं। AI कंटेंट सिमुलेट कर रहा हूँ।")
        ai_script = (
            "यह एक सिमुलेटेड हिंदी वीडियो स्क्रिप्ट है। इस स्क्रिप्ट में प्रमुख बिंदु, कहानी और CTA शामिल होंगे।"
        )
        youtube_description = (
            f"🤖 AI जनरेटेड डिस्क्रिप्शन: केस स्टडी पर वीडियो - {seo_title}\n\n"
            f"प्रॉम्प्ट: {prompt}\n\n"
            "HashTags: #AI #Automation"
        )
        thumbnail_idea = f"ट्रेंडिंग थंबनेल: {seo_title} — देखें कैसे!"
        instagram_caption = f"🔥 {seo_title} — देखें और शेयर करें! टैग्स: {', '.join(tags)}"
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
        script = ai_data.get('script', '')
        description = ai_data.get('youtube_description', '')
        thumbnail = ai_data.get('thumbnail_title', '')
        caption = ai_data.get('instagram_caption', '')

        if not all([script, description, thumbnail, caption]):
            raise ValueError("AI ने JSON लौटाया लेकिन कुछ फ़ील्ड खाली हैं।")

        print("✅ Gemini AI कंटेंट सफलतापूर्वक जनरेट हुआ।")
        return script, description, thumbnail, caption

    except (APIError, json.JSONDecodeError, ValueError) as e:
        print(f"❌ Gemini API या पार्सिंग त्रुटि: {e}. सिमुलेशन पर वापस जा रहा है।")
        return integrate_gemini_for_content(seo_title, prompt, video_type, tags)
    except Exception as e:
        print(f"❌ Gemini अनपेक्षित त्रुटि: {e}. सिमुलेशन पर वापस जा रहा है।")
        return integrate_gemini_for_content(seo_title, prompt, video_type, tags)

def get_youtube_service():
    print("🔄 YouTube API सर्विस क्रेडेंशियल प्राप्त कर रहा है...")
    client_id = os.environ.get("YOUTUBE_CLIENT_ID")
    client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET")
    refresh_token = os.environ.get("YOUTUBE_REFRESH_TOKEN")

    if not client_id or not client_secret or not refresh_token:
        print("❌ YouTube Secrets अनुपलब्ध। अपलोड सिमुलेट होगा।")
        return None

    credentials = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=YOUTUBE_UPLOAD_SCOPE
    )
    try:
        if GoogleAuthRequest is None:
            raise RuntimeError("google.auth.transport.requests.Request उपलब्ध नहीं है।")
        credentials.refresh(GoogleAuthRequest())
        youtube = build('youtube', 'v3', credentials=credentials)
        print("✅ YouTube सर्विस ऑब्जेक्ट सफलतापूर्वक बनाया गया।")
        return youtube
    except Exception as e:
        print(f"❌ YouTube API क्रेडेंशियल त्रुटि: {e}")
        return None

def upload_to_youtube(video_path, title, description, tags, schedule_time_str):
    print("⏳ YouTube अपलोड/शेड्यूल शुरू...")
    youtube = get_youtube_service()
    if not youtube:
        print("⚠️ सिमुलेशन: वीडियो अपलोड किए बिना ID लौटाई जा रही है।")
        return f"YOUTUBE_ID_SIMULATED_{random.randint(1000, 9999)}"

    schedule_iso = format_schedule_time(schedule_time_str)
    if schedule_iso:
        privacy_status = 'private'
        scheduled_at = schedule_iso
        print(f"⏰ वीडियो को शेड्यूल किया जा रहा है (UTC): {scheduled_at}")
    else:
        privacy_status = 'unlisted'
        scheduled_at = None
        print("🚀 वीडियो को 'Unlisted' पब्लिश किया जा रहा है।")

    video_metadata = {
        'snippet': {
            'title': title,
            'description': description,
            'tags': tags,
            'categoryId': '28'
        },
        'status': {
            'privacyStatus': privacy_status,
            'publishAt': scheduled_at
        }
    }

    media_body = MediaFileUpload(video_path, chunksize=-1, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=video_metadata, media_body=media_body)
    response = request.execute()
    uploaded_video_id = response.get('id')
    print(f"✅ वीडियो YouTube पर अपलोड/शेड्यूल हुआ। ID: {uploaded_video_id}")
    return uploaded_video_id

def upload_to_instagram(video_path, caption):
    print("⏳ Instagram अपलोड शुरू...")
    ig_user_id = os.environ.get("INSTAGRAM_USER_ID")
    access_token = os.environ.get("INSTAGRAM_ACCESS_TOKEN")
    if not ig_user_id or not access_token:
        print("❌ Instagram Secrets अनुपलब्ध। सिमुलेशन जारी है।")
        time.sleep(1)
        return False
    print(f"✅ वीडियो Instagram पर अपलोड/शेड्यूल हुआ। कैप्शन: {caption[:20]}...")
    time.sleep(2)
    return True

def format_schedule_time(time_str):
    try:
        if not time_str or str(time_str).strip() == "":
            return None
        # IST (UTC+5:30)
        IST_OFFSET = timedelta(hours=5, minutes=30)
        now_utc = datetime.now(timezone.utc)
        now_ist = now_utc + IST_OFFSET 
        
        # समय स्ट्रिंग से समय ऑब्जेक्ट प्राप्त करें (जैसे 09:00 AM)
        time_obj = datetime.strptime(time_str.strip(), '%I:%M %p').time()
        
        # आज के IST की तारीख और दिए गए समय को मिलाएं
        scheduled_datetime_ist = now_ist.replace(
            hour=time_obj.hour, 
            minute=time_obj.minute, 
            second=0, 
            microsecond=0
        ).replace(tzinfo=None) # टाइमज़ोन-अवेयर से टाइमज़ोन-अज्ञानी बनाएं
        
        now_ist_naive = now_ist.replace(tzinfo=None)
        
        # यदि निर्धारित समय वर्तमान समय से 5 मिनट के भीतर या अतीत में है, तो अगले दिन पर शेड्यूल करें
        if scheduled_datetime_ist <= now_ist_naive + timedelta(minutes=5):
            scheduled_datetime_ist += timedelta(days=1)
        
        # IST से UTC में बदलें
        utc_datetime = scheduled_datetime_ist - IST_OFFSET
        
        # YouTube के लिए ISO 8601 फॉर्मेट (Z लगाकर)
        return utc_datetime.isoformat() + 'Z'
    except Exception as e:
        print(f"❌ समय फॉर्मेटिंग त्रुटि: {e}")
        return None 

def get_start_row_index():
    # .github/workflows/state.txt से अगली रो इंडेक्स पढ़ें
    if os.path.exists(STATE_FILE) and os.path.getsize(STATE_FILE) > 0:
        with open(STATE_FILE, 'r') as f:
            try:
                # 1 से कम नहीं हो सकता
                return max(1, int(f.read().strip()))
            except ValueError:
                return 1
    return 1

def update_state_file(new_index):
    # डायरेक्टरी मौजूद नहीं होने पर बनाएं
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        f.write(str(new_index))
    print(f"✅ स्टेट अपडेट हुआ: अगली बार रो इंडेक्स {new_index} से शुरू होगा।")

def fetch_data_from_google_sheet(csv_url):
    print(f"🔗 Google Sheet से डेटा खींच रहा है: {csv_url}")
    try:
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        if not all(col in df.columns for col in REQUIRED_COLS):
            missing = [col for col in REQUIRED_COLS if col not in df.columns]
            raise ValueError(f"❌ आवश्यक कॉलम अनुपलब्ध हैं: {', '.join(missing)}")
        print(f"✅ डेटा सफलतापूर्वक खींचा गया। कुल रो: {len(df)}")
        return df
    except Exception as e:
        print(f"❌ डेटा फेचिंग या वैलिडेशन त्रुटि: {e}")
        raise

def generate_and_process_video(row_index, row):
    seo_title = row['Heading_Title']
    prompt = row['Prompt']
    # 'True'/'False' या 'true'/'false' को संभालता है
    cinematic_mode = str(row.get('Cinematic_Mode', 'False')).strip().lower() == 'true'
    tags = [t.strip() for t in str(row.get('Keywords_Tags', '')).split(',') if t.strip()]
    video_type = str(row.get('Video_Type', 'UNKNOWN')).upper()
    schedule_time_str = str(row.get('Schedule_Time', ''))
    
    # सुनिश्चित करें कि tags लिस्ट खाली न हो (API एरर से बचने के लिए)
    if not tags:
        tags = [t.strip() for t in seo_title.split() if len(t) > 3]

    ai_script, youtube_description, thumbnail_idea, instagram_caption = integrate_gemini_for_content(seo_title, prompt, video_type, tags)
    
    final_prompt = ai_script
    if cinematic_mode:
        final_prompt += f"\n(सिनेमैटिक/VFX इफ़ेक्ट लागू करें। थंबनेल आईडिया: {thumbnail_idea})"
    
    print(f"\n--- रो {row_index}: प्रोसेसिंग शुरू ---")
    print(f"शीर्षक: {seo_title} | प्रकार: {video_type} | थंबनेल आईडिया: {thumbnail_idea}")

    # --- यहाँ वीडियो रेंडरिंग सिमुलेशन होता है ---
    SIMULATED_RENDER_TIME = 5
    if video_type == 'SHORT':
        video_duration_seconds = random.randint(30, 60)
        print(f"✅ Short वीडियो के लिए अवधि {video_duration_seconds} सेकंड पर सेट की गई।")
    else:
        video_duration_seconds = random.randint(300, 600)
        print(f"✅ Long वीडियो के लिए अवधि {video_duration_seconds} सेकंड पर सेट की गई।")
    
    print(f"⏳ वीडियो रेंडरिंग शुरू... ({SIMULATED_RENDER_TIME} सेकंड सिमुलेशन)")
    time.sleep(SIMULATED_RENDER_TIME)

    # सुरक्षित फ़ाइल नाम बनाना
    safe_title = "".join(c for c in seo_title if c.isalnum() or c in (" ", "_")).rstrip()
    output_filename = f"{row_index}{video_type}{safe_title.replace(' ', '_')[:30]}.mp4"
    temp_video_path = os.path.join('/tmp', output_filename)
    os.makedirs('/tmp', exist_ok=True)
    
    # सिमुलेटेड वीडियो फ़ाइल बनाना
    with open(temp_video_path, 'w') as f:
        f.write(f"सिमुलेटेड वीडियो (स्क्रिप्ट से बनाया गया)। अवधि: {video_duration_seconds} सेकंड।")
    print(f"✅ वीडियो लोकल में रेंडर हुआ: {temp_video_path}")
    # --- रेंडरिंग सिमुलेशन समाप्त ---

    uploaded_id = upload_to_youtube(temp_video_path, seo_title, youtube_description, tags, schedule_time_str)
    
    if uploaded_id and video_type == 'SHORT':
        # Instagram के लिए अलग से कैप्शन का उपयोग करें
        upload_to_instagram(temp_video_path, row.get('Instagram_Caption', instagram_caption))
    
    # OUTPUT_DIR/Videos के लिए डायरेक्टरी बनाएं
    os.makedirs(os.path.join(OUTPUT_DIR, 'Videos'), exist_ok=True)
    # अस्थायी फ़ाइल को उत्पादन पैकेज में कॉपी करना
    shutil.copy(temp_video_path, os.path.join(OUTPUT_DIR, 'Videos', output_filename))
    
    return output_filename, uploaded_id

def run_automation():
    result = {
        "videos_generated": 0,
        "zip_path": "",
        "next_start_index": None,
        "errors": []
    }
    if len(sys.argv) < 2:
        msg = "❌ त्रुटि: Google Sheet CSV URL प्रदान नहीं किया गया।"
        print(msg)
        result["errors"].append(msg)
        # JSON आउटपुट को प्रिंट करना
        print(json.dumps(result))
        sys.exit(1)

    csv_url = sys.argv[1]

    try:
        df = fetch_data_from_google_sheet(csv_url)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"❌ Google Sheet fetch failed: {e}\n{tb}")
        result["errors"].append(f"Google Sheet Fetch Error: {e}")
        print(json.dumps(result))
        sys.exit(1)

    os.makedirs(os.path.join(OUTPUT_DIR, 'Content'), exist_ok=True)
    start_index = get_start_row_index()
    # MAX_VIDEOS_PER_RUN रो को प्रोसेस करें
    end_index = start_index + MAX_VIDEOS_PER_RUN 
    # DataFrame slicing के लिए इंडेक्स 0 से शुरू होता है, इसलिए -1
    iloc_start = max(0, start_index - 1) 
    df_to_process = df.iloc[iloc_start:end_index]
    
    if df_to_process.empty:
        print("💡 प्रोसेस करने के लिए कोई नई रो नहीं मिली।")
        # यदि DataFrame खाली है, तो next_start_index को अंतिम रो + 1 पर सेट करें।
        result["next_start_index"] = len(df) + 1 if not df.empty else 1
    else:
        print(f"\n🎯 {len(df_to_process)} रो (इंडेक्स {iloc_start + 1} से {iloc_start + len(df_to_process)}) को प्रोसेस किया जा रहा है।")
    
    processed_details = []
    # अंतिम सफलतापूर्वक प्रोसेस किए गए इंडेक्स को ट्रैक करना
    last_processed_index = start_index - 1 

    for sheet_row_index, row in df_to_process.iterrows():
        # sheet_row_index 0-आधारित है, इसलिए 1 जोड़कर इसे 1-आधारित (Google Sheet) मानते हैं
        process_id = sheet_row_index + 1 
        try:
            video_file, youtube_id = generate_and_process_video(process_id, row)
            processed_details.append({
                'Sheet Row ID': int(process_id),
                'Heading Title': row['Heading_Title'],
                'Video Filename': video_file,
                'YouTube ID': youtube_id,
                'Type': str(row.get('Video_Type', '')).upper(),
                'Processed Date': datetime.now().isoformat()
            })
            last_processed_index = int(process_id)
        except Exception as e:
            tb = traceback.format_exc()
            msg = f"रो {process_id} प्रोसेसिंग में गंभीर त्रुटि: {e}"
            print(f"❌ {msg}\n{tb}")
            result["errors"].append(msg)
            # त्रुटि होने पर भी आगे बढ़ें

    videos_generated = len(processed_details)
    result["videos_generated"] = videos_generated

    print(f"\n--- ऑटोमेशन रन समाप्त ---")

    # स्टेट अपडेट करें और आउटपुट फ़ाइलें जनरेट करें
    if videos_generated > 0:
        next_start_index = last_processed_index + 1
    else:
        # यदि 0 वीडियो जनरेट हुए, लेकिन DataFrame में रो थे, 
        # तो यह इंगित करता है कि रो को प्रोसेस करने में एरर आई थी,
        # इसलिए अगली बार उसी इंडेक्स से शुरू करना चाहिए।
        next_start_index = start_index
        if df.empty or iloc_start >= len(df):
            # यदि प्रोसेस करने के लिए कुछ भी नहीं है (सभी रो प्रोसेस हो चुके हैं),
            # तो अगले इंडेक्स को अंतिम रो + 1 पर सेट करें।
            next_start_index = len(df) + 1 if not df.empty else 1
            
    result["next_start_index"] = next_start_index
    
    # स्टेट फ़ाइल अपडेट
    if next_start_index > start_index or (df_to_process.empty and start_index <= len(df)):
         try:
            update_state_file(next_start_index)
         except Exception as e:
            print(f"⚠️ स्टेट अपडेट करते समय त्रुटि: {e}")
            
    if videos_generated > 0:
        try:
            df_out = pd.DataFrame(processed_details)
            excel_path = os.path.join(OUTPUT_DIR, 'Content', f"Final_Content_Details_{datetime.now().strftime('%Y%m%d')}.xlsx")
            df_out.to_excel(excel_path, index=False)
            print(f"\n📦 ट्रैकिंग एक्सेल जनरेट हुआ: {excel_path}")
            
            # ज़िप फ़ाइल बनाना
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            # केवल OUTPUT_DIR के अंदर की सामग्री को ज़िप करें
            zip_base_name = os.path.basename(OUTPUT_DIR)
            zip_dir = os.path.dirname(OUTPUT_DIR) or '.'
            zip_path = shutil.make_archive(zip_base_name, 'zip', zip_dir, zip_base_name)
            
            print(f"\n📦📦 अंतिम पैकेज तैयार: {zip_path}")
            result["zip_path"] = zip_path
        except Exception as e:
             msg = f"आउटपुट फ़ाइल या ज़िप बनाने में त्रुटि: {e}"
             print(f"❌ {msg}\n{traceback.format_exc()}")
             result["errors"].append(msg)
        

    # GitHub Actions Output सेट करना
    github_output_path = os.environ.get("GITHUB_OUTPUT")
    if github_output_path:
        try:
            with open(github_output_path, 'a') as f:
                f.write(f"zip_path={result['zip_path']}\n")
                f.write(f"videos_generated={videos_generated}\n")
                # next_start_index को GITHUB_OUTPUT में पास करें, ताकि YAML इसका उपयोग Commit Step में कर सके।
                f.write(f"next_start_index={next_start_index}\n")
            print("✅ GitHub Actions Output सफलतापूर्वक सेट किया गया।")
        except Exception as e:
            print(f"⚠️ GITHUB_OUTPUT लिखने में त्रुटि: {e}")

    # JSON सारांश प्रिंट करें
    print(json.dumps(result))
    return result

if _name_ == "_main_": 
    try:
        run_automation()
    except Exception as e:
        tb = traceback.format_exc()
        print(f"Unhandled exception: {e}\n{tb}")
        # विफलता का सारांश JSON ऑब्जेक्ट प्रिंट करें
        fallback = {
            "videos_generated": 0,
            "zip_path": "",
            "next_start_index": None,
            "errors": [f"Unhandled Python error: {e}"]
        }
        print(json.dumps(fallback))
        sys.exit(1)
