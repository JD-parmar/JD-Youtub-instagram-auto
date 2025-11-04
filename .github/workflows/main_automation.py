# === Video Content Automation (Free & GitHub-Compatible) ===
# Generates simple text videos from a Google Sheet (CSV)
# 100% free, uses only MoviePy (no paid APIs)

import os
import sys
import json
import pandas as pd
import zipfile
from moviepy.editor import TextClip, CompositeVideoClip, ColorClip

# --- Config ---
STATE_FILE_PATH = os.path.join(os.path.dirname(__file__), 'state.txt')
MAX_ITEMS_TO_PROCESS = 2   # how many videos per run
ZIP_FILE_NAME = "production_package.zip"
TEMP_UPLOAD_DIR = "./temp_upload_dir"

# --- Utilities ---
def read_state(default_index=1):
    try:
        with open(STATE_FILE_PATH, 'r') as f:
            return int(f.read().strip())
    except Exception:
        return default_index

def write_state(next_index):
    try:
        with open(STATE_FILE_PATH, 'w') as f:
            f.write(str(next_index))
        print(f"INFO: Updated state to index {next_index}")
    except Exception as e:
        print(f"ERROR: Failed to write state file: {e}")

# --- Video Creation ---
def create_video(output_path: str, text: str):
    """Creates a simple black background video with text."""
    try:
        bg = ColorClip(size=(720, 1280), color=(0, 0, 0), duration=10)
        txt = TextClip(
            text,
            fontsize=40,
            color='white',
            size=(700, None),
            method='caption',
            align='center'
        ).set_duration(10).set_position('center')
        final = CompositeVideoClip([bg, txt])
        final.write_videofile(output_path, fps=24, codec='libx264', audio=False, verbose=False, logger=None)
        print(f"INFO: Created video: {output_path}")
        return True
    except Exception as e:
        print(f"ERROR: Could not create video: {e}")
        return False

# --- Main Automation ---
def run_pipeline(csv_url: str):
    start_index = read_state()
    videos_generated = 0
    next_start_index = start_index
    zip_path = ""

    try:
        df = pd.read_csv(csv_url)
        df['index'] = df.index + 1
        rows = df[df['index'] >= start_index].head(MAX_ITEMS_TO_PROCESS)
        if rows.empty:
            write_state(start_index)
            return {"videos_generated": 0, "zip_path": "", "next_start_index": start_index}
    except Exception as e:
        print(f"CRITICAL: Failed to read CSV: {e}")
        return {"videos_generated": 0, "zip_path": "", "next_start_index": start_index}

    try:
        with zipfile.ZipFile(ZIP_FILE_NAME, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for _, row in rows.iterrows():
                idx = row['index']
                topic = row.get('Topic', f'Auto Topic {idx}')
                text = f"🎬 Video {idx}\nTopic: {topic}\n\nThis video was auto-generated for free using GitHub Actions!"

                video_name = f"video_{idx}.mp4"
                script_name = f"script_{idx}.txt"

                if not create_video(video_name, text):
                    continue

                with open(script_name, 'w', encoding='utf-8') as f:
                    f.write(text)

                zipf.write(video_name, arcname=video_name)
                zipf.write(script_name, arcname=script_name)

                os.remove(video_name)
                os.remove(script_name)

                videos_generated += 1
                next_start_index = idx + 1
                zip_path = ZIP_FILE_NAME
                print(f"INFO: Packaged video {idx}")

        write_state(next_start_index)
    except Exception as e:
        print(f"CRITICAL: Failed creating zip: {e}")

    return {
        "videos_generated": videos_generated,
        "zip_path": zip_path,
        "next_start_index": next_start_index
    }

# --- Entry Point ---
def main():
    if len(sys.argv) < 2:
        print("ERROR: Missing CSV URL argument")
        sys.exit(1)

    csv_url = sys.argv[1]
    result = run_pipeline(csv_url)
    print(json.dumps(result))

if __name__ == "__main__":
    main()
