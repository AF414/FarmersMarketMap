#!/usr/bin/env python3
"""
Monitor the progress of the full crawl
"""

import json
import os
import time
from datetime import datetime

def check_crawl_progress():
    """Check progress of the full crawl"""

    print(f"=== CRAWL PROGRESS CHECK ({datetime.now().strftime('%H:%M:%S')}) ===")

    # Check basic scraping progress
    if os.path.exists('all_markets_basic.json'):
        with open('all_markets_basic.json', 'r') as f:
            basic_results = json.load(f)

        total = len(basic_results)
        successful = len([r for r in basic_results if r['error'] is None])

        print(f"✓ Basic scraping complete: {total} markets ({successful} successful)")

        # Site type breakdown
        site_types = {}
        for result in basic_results:
            site_type = result['site_type']
            site_types[site_type] = site_types.get(site_type, 0) + 1

        print("Site types found:")
        for site_type, count in sorted(site_types.items(), key=lambda x: x[1], reverse=True):
            print(f"  {site_type}: {count}")
    else:
        print("⏳ Basic scraping still in progress...")

    # Check vendor crawling progress
    batch_files = []
    for i in range(1, 20):  # Check for batch files
        batch_file = f'vendor_pages_batch_{i}.json'
        if os.path.exists(batch_file):
            batch_files.append(batch_file)

    if batch_files:
        print(f"✓ Vendor crawling batches completed: {len(batch_files)}")

        # Count total vendors from batch files
        total_vendor_pages = 0
        total_vendors = 0

        for batch_file in batch_files:
            with open(batch_file, 'r') as f:
                batch_data = json.load(f)
                total_vendor_pages += len(batch_data)
                total_vendors += sum(len(page['vendors']) for page in batch_data)

        print(f"  Total vendor pages: {total_vendor_pages}")
        print(f"  Total vendors: {total_vendors}")
    else:
        print("⏳ Vendor crawling not yet started...")

    # Check final results
    if os.path.exists('all_vendor_pages.json'):
        print("✓ Full crawl complete! Final results available.")
    elif os.path.exists('full_crawl_analysis.json'):
        print("✓ Full crawl and analysis complete!")

    print()

if __name__ == "__main__":
    while True:
        check_crawl_progress()

        # Check if crawl is complete
        if os.path.exists('full_crawl_analysis.json'):
            print("Full crawl complete!")
            break

        print("Waiting 30 seconds before next check...")
        time.sleep(30)