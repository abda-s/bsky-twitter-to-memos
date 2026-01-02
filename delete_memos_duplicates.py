import os
import hashlib
import requests
from dotenv import load_dotenv
from collections import defaultdict
from urllib.parse import quote
from datetime import datetime

load_dotenv()

# --- CONFIGURATION ---
MEMOS_URL = os.getenv("MEMOS_URL")
MEMOS_TOKEN = os.getenv("MEMOS_TOKEN")
DRY_RUN = False  # Set to False to actually delete duplicates
# ---------------------


def extract_date(timestamp_str):
    """Extract just the date (YYYY-MM-DD) from ISO timestamp."""
    try:
        if not timestamp_str:
            return "unknown"
        dt = datetime.fromisoformat(
            timestamp_str.replace('Z', '+00:00')
        )
        return dt.strftime('%Y-%m-%d')
    except Exception as e:
        return "unknown"


def fetch_all_memos():
    """Fetch all memos with pagination and error handling."""
    print("🔍 Fetching all memos...\n")
    all_memos = []
    page_token = None
    page_count = 0

    headers = {"Authorization": f"Bearer {MEMOS_TOKEN}"}

    try:
        while True:
            page_count += 1
            url = f"{MEMOS_URL}/api/v1/memos?pageSize=100"
            if page_token:
                encoded_token = quote(page_token, safe='')
                url += f"&pageToken={encoded_token}"

            try:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 400:
                    print(
                        f"  ⚠️  Page {page_count}: Bad request error "
                        f"(possibly end of results)"
                    )
                    break
                else:
                    raise

            data = response.json()
            memos = data.get("memos", [])
            
            if not memos:
                print(f"  📄 Page {page_count}: No more memos")
                break
                
            all_memos.extend(memos)
            print(f"  📄 Page {page_count}: {len(memos)} memos")

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        print(f"\n✅ Total memos fetched: {len(all_memos)}\n")
        return all_memos

    except Exception as e:
        print(f"❌ Error fetching memos: {e}")
        if all_memos:
            print(
                f"⚠️  Continuing with {len(all_memos)} memos "
                f"fetched so far...\n"
            )
            return all_memos
        return []


def find_duplicates(memos):
    """Group memos by content hash AND date to find duplicates."""
    print("🔎 Analyzing memos for duplicates (by content + date)...\n")

    # Group memos by content hash + date
    content_date_groups = defaultdict(list)

    for memo in memos:
        content = memo.get("content", "")
        if content:
            # Get date from createTime
            create_time = memo.get("createTime", "")
            date = extract_date(create_time)
            
            # Create composite key: content_hash + date
            content_hash = hashlib.md5(content.encode()).hexdigest()
            composite_key = f"{content_hash}_{date}"
            
            content_date_groups[composite_key].append(memo)

    # Filter to only groups with duplicates
    duplicates = {
        key: memo_list
        for key, memo_list in content_date_groups.items()
        if len(memo_list) > 1
    }

    if not duplicates:
        print("✨ No duplicates found!\n")
        return {}

    print(
        f"⚠️  Found {len(duplicates)} sets of duplicate "
        f"content on the same date\n"
    )

    # Display duplicate sets (show first 15 in detail)
    total_to_delete = 0
    display_limit = 15
    
    for idx, (composite_key, memo_list) in enumerate(
        list(duplicates.items())[:display_limit], 1
    ):
        # Extract date from composite key
        date = composite_key.split('_')[-1]
        preview = memo_list[0]["content"][:60].replace("\n", " ")
        
        print(f"Set {idx}: {len(memo_list)} copies on {date}")
        print(f"  Content: {preview}...")
        print(f"  Will keep: {memo_list[0]['name']} (oldest)")
        print(f"  Will delete: {len(memo_list) - 1} duplicate(s)")
        
        # Show all timestamps in this group
        for memo in memo_list:
            create_time = memo.get("createTime", "N/A")
            print(f"    • {memo['name']}: {create_time}")
        print()
        
        total_to_delete += len(memo_list) - 1

    # Count remaining
    for memo_list in list(duplicates.values())[display_limit:]:
        total_to_delete += len(memo_list) - 1

    if len(duplicates) > display_limit:
        print(
            f"... and {len(duplicates) - display_limit} more "
            f"duplicate sets\n"
        )

    print(f"📊 Total duplicates to delete: {total_to_delete}\n")
    return duplicates


def delete_duplicates(duplicates):
    """Delete duplicate memos, keeping the oldest one in each set."""
    if not duplicates:
        return

    mode = "DRY RUN - No actual deletions" if DRY_RUN else "LIVE MODE"
    print(f"{'='*60}")
    print(f"⚠️  {mode}")
    print(f"{'='*60}\n")

    if not DRY_RUN:
        print(
            f"This will permanently delete {sum(len(m) - 1 for m in duplicates.values())} "
            f"duplicate memos."
        )
        confirm = input("❗ Continue? (yes/no): ")
        if confirm.lower() != "yes":
            print("❌ Deletion cancelled.\n")
            return

    headers = {"Authorization": f"Bearer {MEMOS_TOKEN}"}
    deleted_count = 0
    failed_count = 0

    for idx, (composite_key, memo_list) in enumerate(
        duplicates.items(), 1
    ):
        # Sort by create time to keep the oldest
        memo_list.sort(key=lambda x: x.get("createTime", ""))

        date = composite_key.split('_')[-1]
        
        # Keep first (oldest), delete the rest
        for memo in memo_list[1:]:
            memo_name = memo.get("name")
            preview = memo["content"][:50].replace("\n", " ")

            if DRY_RUN:
                print(f"[DRY RUN] Would delete: {memo_name}")
                print(f"  Date: {date}")
                print(f"  Content: {preview}...")
                deleted_count += 1
            else:
                try:
                    response = requests.delete(
                        f"{MEMOS_URL}/api/v1/{memo_name}",
                        headers=headers,
                        timeout=30,
                    )
                    response.raise_for_status()
                    print(f"✅ Deleted: {memo_name}")
                    print(f"  Date: {date}")
                    print(f"  Content: {preview}...")
                    deleted_count += 1
                except Exception as e:
                    print(f"❌ Failed to delete {memo_name}: {e}")
                    failed_count += 1

        # Progress indicator
        if idx % 10 == 0:
            print(
                f"\n  Progress: {idx}/{len(duplicates)} "
                f"sets processed\n"
            )

    print(f"\n{'='*60}")
    print(f"📊 SUMMARY")
    print(f"{'='*60}")
    if DRY_RUN:
        print(f"Would delete: {deleted_count} duplicate memos")
    else:
        print(f"✅ Deleted: {deleted_count} memos")
        if failed_count > 0:
            print(f"❌ Failed: {failed_count} memos")
    print(f"{'='*60}\n")


def main():
    if not MEMOS_TOKEN:
        print("❌ MEMOS_TOKEN not set in .env")
        return

    print(f"\n{'='*60}")
    print(f"🗑️  MEMOS DUPLICATE CLEANER")
    print(f"{'='*60}")
    print(f"Instance: {MEMOS_URL}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Strategy: Same content + same date")
    print(f"{'='*60}\n")

    # Fetch all memos
    memos = fetch_all_memos()
    if not memos:
        print("❌ No memos fetched. Cannot continue.\n")
        return

    # Find duplicates
    duplicates = find_duplicates(memos)

    # Delete duplicates (or show what would be deleted)
    if duplicates:
        delete_duplicates(duplicates)

        if DRY_RUN:
            print("💡 To actually delete duplicates, set DRY_RUN = False")
            print("   in the script configuration.\n")


if __name__ == "__main__":
    main()