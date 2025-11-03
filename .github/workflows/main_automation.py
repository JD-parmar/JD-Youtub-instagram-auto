# main_automation.py
# --------------------------------------------------------------
# Video Automation Script
# Handles: CSV data, Gemini content generation, ZIP package creation,
# state management, and YouTube video upload (mock or real)
# --------------------------------------------------------------

import sys
import os
import json
import pandas as pd
import zipfile
import re

# --- Config ---
STATE_FILE_PATH = os.path.join(os.path.dirname(__file__), "state.txt")
MAX_ITEMS_TO_PROCESS = 1
ZIP_FILE_NAME = "production_package.zip"
TEMP_UPLOAD_DIR = "./temp_upload_dir"

# Secrets from environment
YOUTUBE_CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# --------------------------------------------------------------
# Utilities
# --------------------------------------------------------------
def read_state(default_index=1):
    if not os.path.exists(STATE_FILE_PATH):
        with open(STATE_FILE_PATH, "w", encoding="utf-8") as f:
            f.write(str(default_index))
        return default_index
    try:
        with open(STATE_FILE_PATH, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except:
        return default_index

def write_state(next_index):
    try:
        with open(STATE_FILE_PATH, "w", encoding="utf-8") as f:
            f.write(str(next_index))
        print(f"INFO: Updated state to index {next_index}")
    except Exception as e:
        print(f"ERROR: Cannot write state - {e}")

# --------------------------------------------------------------
# Mock Gemini Content
# --------------------------------------------------------------
def generate_content_with_gemini(api_key: str, topic: str) -> str:
    current_index = read_state()
    joke = f"MOCK JOKE on {topic}"
    return (
        f"--- VIDEO SCRIPT ---\n"
        f"Topic: {topic}\n"
        f"Video Title: {topic} (Index {current_index})\n"
        f"Caption: Today’s mystery!\n"
        f"Tags: automation, gemini, youtube, shorts\n"
        f"--- JOKE ---\n{joke}\n"
    )

def mock_video_rendering(output_path: str) -> bool:
    try:
        with open(output_path, "wb") as f:
            f.write(b"\x00" * 500 * 1024)  # 500 KB dummy
        return True
    except:
        return False

# --------------------------------------------------------------
# Main Automation Pipeline
# --------------------------------------------------------------
def run_automation_pipeline(csv_url: str, gemini_api_key: str):
    start_index = read_state()
    next_index = start_index
    videos_generated = 0
    zip_path = f"./{ZIP_FILE_NAME}"

    try:
        df = pd.read_csv(csv_url)
        df["index"] = df.index + 1
        rows_to_process = df[df["index"] >= start_index].head(MAX_ITEMS_TO_PROCESS)
        if rows_to_process.empty:
            return {"videos_generated": 0, "zip_path": "", "next_start_index": start_index}
    except Exception as e:
        print(f"CRITICAL: Failed CSV load - {e}")
        return {"videos_generated": 0, "zip_path": "", "next_start_index": start_index}

    # Create ZIP
    try:
        if os.path.exists(ZIP_FILE_NAME):
            os.remove(ZIP_FILE_NAME)

        with zipfile.ZipFile(ZIP_FILE_NAME, "w", zipfile.ZIP_DEFLATED) as zipf:
            for _, row in rows_to_process.iterrows():
                idx = row["index"]
                topic = row.get("Topic", f"Auto Topic {idx}")
                script_name = f"script_{idx}.txt"
                video_name = f"video_{idx}.mp4"

                content = generate_content_with_gemini(gemini_api_key, topic)
                with open(script_name, "w", encoding="utf-8") as f:
                    f.write(content)

                if not mock_video_rendering(video_name):
                    continue

                zipf.write(script_name)
                zipf.write(video_name)

                os.remove(script_name)
                os.remove(video_name)

                videos_generated += 1
                next_index = idx + 1
                break  # Only 1 video

    except Exception as e:
        print(f"CRITICAL: ZIP creation failed - {e}")
        videos_generated = 0

    write_state(next_index)
    return {"videos_generated": videos_generated, "zip_path": zip_path if videos_generated > 0 else "", "next_start_index": next_index}

# --------------------------------------------------------------
# YouTube Upload (Mock)
# --------------------------------------------------------------
def upload_to_youtube(video_path: str, script_path: str) -> bool:
    if not os.path.exists(video_path) or not os.path.exists(script_path):
        return False
    print(f"MOCK UPLOAD: {video_path}")
    return True

def run_youtube_upload_step(zip_file_path: str):
    if not os.path.exists(zip_file_path):
        print(f"ERROR: ZIP not found: {zip_file_path}")
        return

    if os.path.exists(TEMP_UPLOAD_DIR):
        os.system(f"rm -rf {TEMP_UPLOAD_DIR}")
    os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(TEMP_UPLOAD_DIR)

    uploaded = 0
    for file in os.listdir(TEMP_UPLOAD_DIR):
        if file.endswith(".mp4"):
            video_path = os.path.join(TEMP_UPLOAD_DIR, file)
            script_path = video_path.replace("video_", "script_").replace(".mp4", ".txt")
            if upload_to_youtube(video_path, script_path):
                uploaded += 1

    print(f"INFO: Uploaded {uploaded} videos")
    os.system(f"rm -rf {TEMP_UPLOAD_DIR}")

# --------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------
def main():
    if len(sys.argv) > 2 and sys.argv[1] == "upload-youtube":
        run_youtube_upload_step(sys.argv[2])
        return

    if len(sys.argv) < 2:
        print("ERROR: CSV URL required")
        sys.exit(1)

    csv_url = sys.argv[1]
    output = run_automation_pipeline(csv_url, GEMINI_API_KEY)
    print(json.dumps(output))

if __name__ == "__main__":
    main()
