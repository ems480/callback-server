import os
import dropbox

DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_TOKEN")
DBX_PATH = "/estack.db"
LOCAL_DB = "estack.db"

def get_dbx():
    if not DROPBOX_ACCESS_TOKEN:
        raise ValueError("❌ Missing DROPBOX_TOKEN environment variable.")
    return dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

def upload_db():
    try:
        dbx = get_dbx()
        with open(LOCAL_DB, "rb") as f:
            dbx.files_upload(f.read(), DBX_PATH, mode=dropbox.files.WriteMode("overwrite"))
        print("✅ estack.db uploaded to Dropbox.")
    except FileNotFoundError:
        print("⚠️ Local estack.db not found for upload.")
    except Exception as e:
        print("❌ Dropbox upload failed:", e)

def download_db():
    try:
        dbx = get_dbx()
        metadata, res = dbx.files_download(DBX_PATH)
        with open(LOCAL_DB, "wb") as f:
            f.write(res.content)
        print("✅ estack.db downloaded from Dropbox.")
    except dropbox.exceptions.ApiError:
        print("⚠️ No existing estack.db found in Dropbox (starting fresh).")
    except Exception as e:
        print("❌ Dropbox download failed:", e)
