import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import time

# Load configuration from .env file
load_dotenv()

# --- CONFIGURATION ---
MEMOS_URL = os.getenv("MEMOS_URL", "http://192.168.X.X:5230")
MEMOS_TOKEN = os.getenv("MEMOS_TOKEN")
# Set the cutoff date (delete everything BEFORE this date)
CUTOFF_DATE = os.getenv("CUTOFF_DATE", "2025-12-26T00:00:00Z")
DRY_RUN = False  # Set to False to actually delete
# ---------------------


def get_all_memos():
    """Fetch all memos from Memos with pagination."""
    headers = {"Authorization": f"Bearer {MEMOS_TOKEN}"}
    all_memos = []
    page_token = None
    page_count = 0
    
    while True:
        page_count += 1
        url = f"{MEMOS_URL}/api/v1/memos"
        params = {"pageSize": 100}
        
        if page_token:
            params["pageToken"] = page_token
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                memos = data.get("memos", [])
                all_memos.extend(memos)
                
                print(f"  Page {page_count}: {len(memos)} memos")
                
                # Check for next page
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
            else:
                print(f"❌ Failed to fetch memos: {response.text[:100]}")
                break
        except Exception as e:
            print(f"❌ Error fetching memos: {e}")
            break
    
    print(f"\n✅ Fetched {len(all_memos)} total memos\n")
    return all_memos


def delete_memo(memo_name, memo_id):
    """Delete a single memo."""
    headers = {"Authorization": f"Bearer {MEMOS_TOKEN}"}
    
    # Try with name first (newer API)
    if memo_name:
        url = f"{MEMOS_URL}/api/v1/{memo_name}"
    else:
        url = f"{MEMOS_URL}/api/v1/memos/{memo_id}"
    
    try:
        response = requests.delete(url, headers=headers)
        return response.status_code == 200
    except Exception as e:
        print(f"  ⚠️  Error: {e}")
        return False


def main():
    """Main execution function."""
    if not all([MEMOS_URL, MEMOS_TOKEN]):
        print("❌ Missing configuration! Set MEMOS_URL and MEMOS_TOKEN.")
        return

    print("=" * 60)
    print("MEMOS CLEANUP SCRIPT")
    print("=" * 60)
    print(f"📅 Cutoff date: {CUTOFF_DATE}")
    print(f"🗑️  Will delete all memos BEFORE this date")
    print(f"🔒 Dry run: {'ON (no deletion)' if DRY_RUN else 'OFF (WILL DELETE)'}")
    print("=" * 60 + "\n")
    
    # Parse cutoff date
    try:
        cutoff_dt = datetime.fromisoformat(
            CUTOFF_DATE.replace("Z", "+00:00")
        )
    except Exception as e:
        print(f"❌ Invalid cutoff date format: {e}")
        return
    
    # Fetch all memos
    print("📥 Fetching all memos...\n")
    memos = get_all_memos()
    
    if not memos:
        print("No memos found. Exiting.")
        return
    
    # Filter memos before cutoff date
    memos_to_delete = []
    
    for memo in memos:
        create_time = memo.get("createTime") or memo.get("createdTs")
        
        if not create_time:
            continue
        
        # Handle both ISO string and Unix timestamp
        try:
            if isinstance(create_time, str):
                memo_dt = datetime.fromisoformat(
                    create_time.replace("Z", "+00:00")
                )
            else:
                memo_dt = datetime.fromtimestamp(create_time)
            
            if memo_dt < cutoff_dt:
                memos_to_delete.append({
                    "name": memo.get("name"),
                    "id": memo.get("id") or memo.get("uid"),
                    "content": memo.get("content", "")[:50],
                    "date": memo_dt.strftime("%Y-%m-%d %H:%M:%S")
                })
        except Exception as e:
            print(f"⚠️  Skipping memo due to date parsing error: {e}")
            continue
    
    # Show summary
    print(f"📊 Found {len(memos_to_delete)} memos to delete:\n")
    
    for i, memo in enumerate(memos_to_delete[:10], 1):
        print(f"  {i}. [{memo['date']}] {memo['content']}...")
    
    if len(memos_to_delete) > 10:
        print(f"  ... and {len(memos_to_delete) - 10} more")
    
    print("\n" + "=" * 60)
    
    if not memos_to_delete:
        print("✅ No memos to delete!")
        return
    
    # Delete memos
    deleted_count = 0
    failed_count = 0
    
    for i, memo in enumerate(memos_to_delete, 1):
        print(f"[{i}/{len(memos_to_delete)}] ", end="")
        
        if DRY_RUN:
            print(f"Would delete: [{memo['date']}] {memo['content']}...")
            deleted_count += 1
        else:
            if delete_memo(memo['name'], memo['id']):
                print(f"✅ Deleted: [{memo['date']}]")
                deleted_count += 1
            else:
                print(f"❌ Failed: [{memo['date']}]")
                failed_count += 1
            
            time.sleep(0.1)  # Rate limiting
    
    # Final summary
    print("\n" + "=" * 60)
    print("CLEANUP COMPLETE")
    print("=" * 60)
    
    if DRY_RUN:
        print(f"🔍 Dry run: {deleted_count} memos would be deleted")
        print(
            "\n💡 Set DRY_RUN = False to actually delete these memos"
        )
    else:
        print(f"✅ Successfully deleted: {deleted_count}")
        if failed_count > 0:
            print(f"❌ Failed to delete: {failed_count}")


if __name__ == "__main__":
    main()
