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

# AI कंटेंट के लिए Google GenAI लाइब्रेरी
# यह import optional है — अगर पैकेज उपलब्ध न हो तो फॉलबैक सिमुलेशन चलेगा।
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
# Token refresh के लिए सही Request क्लास
try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
except Exception:
    GoogleAuthRequest = None

# --- कॉन्फ़िगरेशन ---
STATE_FILE = "./state.txt"
MAX_VIDEOS_PER_RUN = 5 
# Google Sheet के सभी आवश्यक कॉलम
REQUIRED_COLS = ['Case_Study', 'Heading_Title', 'Prompt', 'Cinematic_Mode', 'Keywords_Tags', 'Video_Type', 'Schedule_Time', 'Instagram_Caption']
OUTPUT_DIR = f"Production_Package_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# YouTube API स्कोप
YOUTUBE_UPLOAD_SCOPE = ["https://www.googleapis.com/auth/youtube.upload"]


# --- 1. AI कंटेंट जनरेशन लॉजिक (Gemini) ---

def integrate_gemini_for_content(seo_title, prompt, video_type, tags):
    """
    Gemini API का उपयोग करके कंटेंट, कैप्शन, थंबनेल टाइटल जनरेट करता है।
    API Key न होने पर यह चरण निःशुल्क सिमुलेट होता है।
    """
    print("🧠 Gemini AI कंटेंट जनरेशन शुरू...")
    
    gemini_api_key = os.environ.get("GEMINI_API_KEY")

    if not gemini_api_key or not GENAI_AVAILABLE:
        print("❌ GEMINI_API_KEY अनुपलब्ध या GenAI लाइब्रेरी इंस्टॉल नहीं। AI कंटेंट निःशुल्क सिमुलेट किया जा रहा है।")

        # --- सिमुलेशन आउटपुट (सुरक्षित, पूर्ण स्ट्रिंग्स) ---
        ai_script = (
            "यह एक सिमुलेटेड हिंदी वीडियो स्क्रिप्ट है। इस स्क्रिप्ट में प्रमुख बिंदु, इंट्रो, "
            "मेन कॉन्टेन्ट और CTA शामिल होंगे। (यहाँ वास्तविक AI-जनरेटेड टेक्स्ट होना चाहिए।)"
        )
        youtube_description = (
            f"🤖 AI जनरेटेड डिस्क्रिप्शन: केस स्टडी पर वीडियो - {seo_title}\n\n"
            f"प्रॉम्प्ट: {prompt}\n\n"
            "HashTags: #AI #Automation"
        )
        thumbnail_idea = f"ट्रेंडिंग थंबनेल: {seo_title} — देखें कैसे!"
        instagram_caption = f"🔥 {seo_title} — देखें और शेयर करें! टैग्स: {', '.join(tags)}"
        
        return ai_script, youtube_description, thumbnail_idea, instagram_caption
    
    # --- वास्तविक Gemini API कॉल (Structured JSON Output) ---
    try:
        client = genai.Client(api_key=gemini_api_key)
        
        main_prompt = (
            f"एक YouTube वीडियो के लिए कंटेंट जनरेट करें। वीडियो का शीर्षक है: \"{seo_title}\" "
            f"और प्रॉम्प्ट है: \"{prompt}\"। आउटपुट JSON फॉर्मेट में लौटाएँ जिसमें keys हों: "
            "script, youtube_description, thumbnail_title, instagram_caption."
        )
        
        # JSON Schema को परिभाषित करें
        response_schema = {
            "type": "OBJECT",
            "properties": {
                "script": {
                    "type": "STRING",
                    "description": "वीडियो के लिए विस्तृत हिंदी स्क्रिप्ट।"
                },
                "youtube_description": {
                    "type": "STRING",
                    "description": "YouTube डिस्क्रिप्शन, जिसमें हैशटैग और SEO टाइटल शामिल हों।"
                },
                "thumbnail_title": {
                    "type": "STRING",
                    "description": "एक ट्रेंडिंग और क्लिक-योग्य थंबनेल टाइटल आईडिया।"
                },
                "instagram_caption": {
                    "type": "STRING",
                    "description": "इंस्टाग्राम रील के लिए छोटा, आकर्षक कैप्शन।"
                }
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
        
        # JSON स्ट्रिंग को डिक्शनरी में पार्स करें
        ai_data = json.loads(response.text)
        
        script = ai_data.get('script', '')
        description = ai_data.get('youtube_description', '')
        thumbnail = ai_data.get('thumbnail_title', '')
        caption = ai_data.get('instagram_caption', '')

        if not all([script, description, thumbnail, caption]):
            raise ValueError("AI ने JSON लौटाया लेकिन कुछ फ़ील्ड खाली हैं।")

        print("✅ Gemini AI कंटेंट सफलतापूर्वक JSON फॉर्मेट में जनरेट हुआ (API का उपयोग करके)।")
        return script, description, thumbnail, caption
        
    except (APIError, json.JSONDecodeError, ValueError) as e:
        # API या पार्सिंग त्रुटियों को संभालें और सिमुलेशन पर वापस जाएं
        print(f"❌ Gemini API या पार्सिंग त्रुटि: {e}. सिमुलेशन पर वापस जा रहा है।")
        return integrate_gemini_for_content(seo_title, prompt, video_type, tags) 
    except Exception as e:
        print(f"❌ Gemini अनपेक्षित त्रुटि: {e}. सिमुलेशन पर वापस जा रहा है।")
        return integrate_gemini_for_content(seo_title, prompt, video_type, tags)


# --- 2. YouTube अपलोड लॉजिक ---

def get_youtube_service():
    """ YouTube API सर्विस को OAuth 2.0 क्रेडेंशियल के साथ इनिशियलाइज़ करता है। """
    
    client_id = os.environ.get("YOUTUBE_CLIENT_ID")
    client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET")
    refresh_token = os.environ.get("YOUTUBE_REFRESH_TOKEN")

    if not client_id or not client_secret or not refresh_token:
        print("❌ YouTube Secrets अनुपलब्ध। अपलोड निःशुल्क सिमुलेट किया जाएगा।")
        return None

    credentials = Credentials(
        None, # access_token
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=YOUTUBE_UPLOAD_SCOPE
    )
    
    try:
        # Access Token को Refresh करें (यदि GoogleAuthRequest उपलब्ध है)
        if GoogleAuthRequest is None:
            raise RuntimeError("google.auth.transport.requests.Request उपलब्ध नहीं है।")
        credentials.refresh(GoogleAuthRequest())
        youtube = build('youtube', 'v3', credentials=credentials)
        return youtube
    except Exception as e:
        print(f"❌ YouTube API क्रेडेंशियल त्रुटि: {e}")
        return None

def upload_to_youtube(video_path, title, description, tags, schedule_time_str):
    """
    YouTube Data API का उपयोग करके वीडियो अपलोड/शेड्यूल करता है।
    """
    print("⏳ YouTube अपलोड/शेड्यूल शुरू...")
    
    youtube = get_youtube_service()
    if not youtube:
        # यदि सेवा उपलब्ध नहीं है (सिमुलेशन रन), तो सिमुलेशन ID लौटाएँ
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
            'categoryId': '28' # Technology Category ID
        },
        'status': {
            'privacyStatus': privacy_status,
            'publishAt': scheduled_at
        }
    }
    
    media_body = MediaFileUpload(video_path, chunksize=-1, resumable=True)

    request = youtube.videos().insert(
        part="snippet,status",
        body=video_metadata,
        media_body=media_body
    )
    
    response = request.execute()
    
    uploaded_video_id = response.get('id')
    print(f"✅ वीडियो YouTube पर अपलोड/शेड्यूल हुआ। ID: {uploaded_video_id}")
    return uploaded_video_id

# --- 3. सहायक और पैकेजिंग लॉजिक ---

def upload_to_instagram(video_path, caption):
    """
    Instagram API का उपयोग करके अपलोड करता है (सिमुलेशन)।
    """
    print("⏳ Instagram अपलोड शुरू...")
    ig_user_id = os.environ.get("INSTAGRAM_USER_ID")
    access_token = os.environ.get("INSTAGRAM_ACCESS_TOKEN")
    
    if not ig_user_id or not access_token:
        print("❌ Instagram Secrets अनुपलब्ध। सिमुलेशन जारी है।")
        time.sleep(1)
        return False
        
    # ⚠️ यहाँ वास्तविक Instagram API कोड आएगा...
    print(f"✅ वीडियो Instagram पर अपलोड/शेड्यूल हुआ। कैप्शन: {caption[:20]}...")
    time.sleep(2)
    return True

def format_schedule_time(time_str):
    """
    समय को IST से UTC में बदलता है और सुनिश्चित करता है कि यह भविष्य में 5 मिनट से अधिक हो।
    अपेक्षित इनपुट फॉर्मैट: 'HH:MM AM/PM' (उदा. '07:30 PM')
    """
    try:
        if not time_str or str(time_str).strip() == "":
            return None

        # IST is UTC + 5:30
        IST_OFFSET = timedelta(hours=5, minutes=30)
        
        # 1. Get current time in UTC (Runner's default) and calculate current IST
        now_utc = datetime.now(timezone.utc)
        now_ist = now_utc + IST_OFFSET 

        # 2. Parse the time part from the string
        time_obj = datetime.strptime(time_str.strip(), '%I:%M %p').time() 
        
        # 3. Combine today's IST date with the target time
        scheduled_datetime_ist = now_ist.replace(
            hour=time_obj.hour, 
            minute=time_obj.minute, 
            second=0, 
            microsecond=0
        ).replace(tzinfo=None) # temporarily remove timezone for easy comparison

        # Ensure the current 'now_ist' also has no tzinfo for safe comparison
        now_ist_naive = now_ist.replace(tzinfo=None)

        # 4. If the scheduled time is in the past or too soon, schedule for tomorrow
        if scheduled_datetime_ist <= now_ist_naive + timedelta(minutes=5):
            scheduled_datetime_ist += timedelta(days=1)

        # 5. Convert the final naive IST time to UTC (by subtracting offset)
        utc_datetime = scheduled_datetime_ist - IST_OFFSET
        
        # YouTube API के लिए 'Z' फॉर्मेट में return करें
        return utc_datetime.isoformat() + 'Z' 

    except Exception as e:
        print(f"❌ समय फॉर्मेटिंग त्रुटि: {e}")
        return None 

def get_start_row_index():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            try:
                # राज्य फ़ाइल को सुरक्षित रूप से संख्या के रूप में पढ़ें
                return max(1, int(f.read().strip())) 
            except ValueError:
                return 1
    return 1 

def update_state_file(new_index):
    with open(STATE_FILE, 'w') as f:
        f.write(str(new_index))
    print(f"✅ स्टेट अपडेट हुआ: अगली बार रो इंडेक्स {new_index} से शुरू होगा।")

def fetch_data_from_google_sheet(csv_url):
    print(f"🔗 Google Sheet से डेटा खींच रहा है: {csv_url}")
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        
        df = pd.read_csv(StringIO(response.text))
        
        if not all(col in df.columns for col in REQUIRED_COLS):
            missing = [col for col in REQUIRED_COLS if col not in df.columns]
            raise ValueError(f"❌ आवश्यक कॉलम अनुपलब्ध हैं: {', '.join(missing)}")
            
        print(f"✅ डेटा सफलतापूर्वक खींचा गया। कुल रो: {len(df)}")
        return df
        
    except Exception as e:
        print(f"❌ डेटा फेचिंग या वैलिडेशन त्रुटि: {e}")
        sys.exit(1)


# --- 4. वीडियो जनरेशन और अपलोड ---

def generate_and_process_video(row_index, row):
    seo_title = row['Heading_Title']
    prompt = row['Prompt']
    cinematic_mode = str(row.get('Cinematic_Mode', 'False')).strip().lower() == 'true'
    tags = [t.strip() for t in str(row.get('Keywords_Tags', '')).split(',') if t.strip()]
    video_type = str(row.get('Video_Type', 'UNKNOWN')).upper()
    schedule_time_str = str(row.get('Schedule_Time', ''))
    
    # 🚨 AI कंटेंट जनरेट करें 
    ai_script, youtube_description, thumbnail_idea, instagram_caption = integrate_gemini_for_content(
        seo_title, 
        prompt, 
        video_type, 
        tags
    )
    
    final_prompt = ai_script 
    if cinematic_mode:
        final_prompt += f"\n(सिनेमैटिक/VFX इफ़ेक्ट लागू करें। थंबनेल आईडिया: {thumbnail_idea})"

    print(f"\n--- रो {row_index}: प्रोसेसिंग शुरू ---")
    print(f"शीर्षक: {seo_title} | प्रकार: {video_type} | थंबनेल आईडिया: {thumbnail_idea}")
    
    # --- 4.1. वीडियो रेंडरिंग सिमुलेशन ---
    SIMULATED_RENDER_TIME = 5 
    
    video_duration_seconds = 0
    if video_type == 'SHORT':
        # 60 सेकंड सीमा
        video_duration_seconds = random.randint(30, 60)
        print(f"✅ Short वीडियो के लिए अवधि {video_duration_seconds} सेकंड पर सेट की गई।")
    else:
        video_duration_seconds = random.randint(300, 600)
        print(f"✅ Long वीडियो के लिए अवधि {video_duration_seconds} सेकंड पर सेट की गई।")


    print(f"⏳ वीडियो रेंडरिंग शुरू... ({SIMULATED_RENDER_TIME} सेकंड सिमुलेशन)")
    time.sleep(SIMULATED_RENDER_TIME) 
    
    safe_title = "".join(c for c in seo_title if c.isalnum() or c in (" ", "_")).rstrip()
    output_filename = f"{row_index}_{video_type}_{safe_title.replace(' ', '_')[:30]}.mp4"
    temp_video_path = os.path.join('/tmp', output_filename) 
    
    os.makedirs('/tmp', exist_ok=True)
    # सिमुलेटेड वीडियो फाइल (वास्तविक में यह रेंडरर से आता है)
    with open(temp_video_path, 'w') as f:
        f.write(f"सिमुलेटेड वीडियो (स्क्रिप्ट से बनाया गया)। अवधि: {video_duration_seconds} सेकंड।")
    
    print(f"✅ वीडियो लोकल में रेंडर हुआ: {temp_video_path}")
    
    # --- 4.2. YouTube पर अपलोड/शेड्यूल ---
    uploaded_id = upload_to_youtube(
        temp_video_path, 
        seo_title, 
        youtube_description, 
        tags,
        schedule_time_str
    )
    
    # --- 4.3. Instagram अपलोड ---
    if uploaded_id and video_type == 'SHORT': 
        upload_to_instagram(temp_video_path, instagram_caption) 

    # फ़ाइल को पैकेज के लिए कॉपी करें
    os.makedirs(os.path.join(OUTPUT_DIR, 'Videos'), exist_ok=True)
    shutil.copy(temp_video_path, os.path.join(OUTPUT_DIR, 'Videos', output_filename))
    
    return output_filename, uploaded_id

# --- 5. मुख्य ऑटोमेशन फ़ंक्शन ---

def run_automation():
    if len(sys.argv) < 2:
        print("❌ त्रुटि: Google Sheet CSV URL प्रदान नहीं किया गया।")
        sys.exit(1)
        
    csv_url = sys.argv[1]
    df = fetch_data_from_google_sheet(csv_url)
    
    os.makedirs(os.path.join(OUTPUT_DIR, 'Content'), exist_ok=True)
    
    start_index = get_start_row_index()
    end_index = start_index + MAX_VIDEOS_PER_RUN
    
    # pandas iloc is 0-based; if state file stores 1-based index adjust accordingly.
    # If start_index looks like 1 and you want to process from the first row (iloc 0), subtract 1.
    iloc_start = max(0, start_index - 1)
    df_to_process = df.iloc[iloc_start:end_index]
    
    print(f"\n🎯 {len(df_to_process)} रो (इंडेक्स {start_index} से {end_index-1}) को प्रोसेस किया जा रहा है।")
    
    processed_details = []
    last_processed_index = start_index - 1
    
    for row_index, row in df_to_process.iterrows():
        try:
            video_file, youtube_id = generate_and_process_video(row_index, row)
            
            processed_details.append({
                'Sheet Row ID': int(row_index),
                'Heading Title': row['Heading_Title'],
                'Video Filename': video_file,
                'YouTube ID': youtube_id,
                'Type': str(row.get('Video_Type', '')).upper(),
                'Processed Date': datetime.now().isoformat()
            })
            
            last_processed_index = int(row_index)
            
        except Exception as e:
            print(f"❌ रो {row_index} प्रोसेसिंग में गंभीर त्रुटि: {e}")
            
    videos_generated = len(processed_details)
    
    print(f"\n--- ऑटोमेशन रन समाप्त ---")
    
    if videos_generated > 0:
        next_start_index = last_processed_index + 1
        update_state_file(next_start_index)
        
        df_out = pd.DataFrame(processed_details)
        excel_path = os.path.join(OUTPUT_DIR, 'Content', f"Final_Content_Details_{datetime.now().strftime('%Y%m%d')}.xlsx")
        df_out.to_excel(excel_path, index=False)
        print(f"\n📦 ट्रैकिंग एक्सेल जनरेट हुआ: {excel_path}")
        
        zip_path = shutil.make_archive(OUTPUT_DIR, 'zip', OUTPUT_DIR)
        print(f"\n📦📦 अंतिम पैकेज तैयार: {zip_path}")
        
        # --- GITHUB ACTIONS OUTPUT LOGIC (नया सिंटैक्स) ---
        github_output_path = os.environ.get("GITHUB_OUTPUT")
        if github_output_path:
            with open(github_output_path, 'a') as f:
                f.write(f"zip_path={OUTPUT_DIR}.zip\n")
                f.write(f"videos_generated={videos_generated}\n")
                f.write(f"next_start_index={next_start_index}\n")
            print("✅ GitHub Actions Output सफलतापूर्वक सेट किया गया।")
    
    elif videos_generated == 0:
        # अगर कोई वीडियो जनरेट नहीं हुआ, तो भी आउटपुट सेट करें
        github_output_path = os.environ.get("GITHUB_OUTPUT")
        if github_output_path:
            with open(github_output_path, 'a') as f:
                f.write(f"videos_generated=0\n")


if __name__ == "__main__": 
    run_automation()
