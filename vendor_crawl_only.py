#!/usr/bin/env python3
"""
Run only the vendor crawling phase using existing basic results
"""

import json
import time
from vendor_crawler import VendorPageCrawler

def vendor_crawl_from_basic_results():
    """Run vendor crawling using the basic scraping results"""

    print("=== VENDOR CRAWLING FROM BASIC RESULTS ===")

    # Load basic results
    with open('all_markets_basic.json', 'r', encoding='utf-8') as f:
        all_results = json.load(f)

    # Filter to successful sites with vendor potential
    promising_sites = []
    for result in all_results:
        if (result['error'] is None and
            result['site_type'] in ['dedicated_market', 'municipal', 'other'] and
            any(keyword in result['text_content'].lower()
                for keyword in ['vendor', 'farmer', 'producer', 'artisan'])):
            promising_sites.append(result)

    print(f"Found {len(promising_sites)} promising sites for vendor crawling")

    # Crawl in batches
    crawler = VendorPageCrawler(delay=1.5, max_depth=2)
    all_vendor_pages = []
    batch_size = 15

    for i in range(0, len(promising_sites), batch_size):
        batch = promising_sites[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(promising_sites) + batch_size - 1) // batch_size

        print(f"\n--- Batch {batch_num}/{total_batches} ({len(batch)} sites) ---")

        for site in batch:
            crawler.visited_urls.clear()  # Reset for each site

            try:
                vendor_pages = crawler.crawl_for_vendors(site['market_name'], site['url'])
                all_vendor_pages.extend(vendor_pages)

                if vendor_pages:
                    total_vendors = sum(len(page.vendors) for page in vendor_pages)
                    print(f"  ✓ {site['market_name']}: {len(vendor_pages)} pages, {total_vendors} vendors")
                else:
                    print(f"  - {site['market_name']}: No vendor pages found")

            except Exception as e:
                print(f"  ✗ {site['market_name']}: Error - {e}")

            # Respectful delay between sites
            time.sleep(crawler.delay)

        # Save progress after each batch
        crawler.save_vendor_pages(all_vendor_pages, f"vendor_pages_batch_{batch_num}.json")

        # Longer delay between batches
        if i + batch_size < len(promising_sites):
            print(f"Completed batch {batch_num}. Pausing before next batch...")
            time.sleep(5)

    # Save final results
    crawler.save_vendor_pages(all_vendor_pages, "all_vendor_pages_final.json")

    print(f"\n=== VENDOR CRAWLING COMPLETE ===")
    print(f"Total vendor pages found: {len(all_vendor_pages)}")
    total_vendors = sum(len(page.vendors) for page in all_vendor_pages)
    print(f"Total vendors extracted: {total_vendors}")

    return all_vendor_pages

if __name__ == "__main__":
    vendor_pages = vendor_crawl_from_basic_results()

    # Quick analysis
    if vendor_pages:
        markets_with_vendors = len(set(page.market_name for page in vendor_pages))
        avg_confidence = sum(page.confidence_score for page in vendor_pages) / len(vendor_pages)

        print(f"Markets with vendor data: {markets_with_vendors}")
        print(f"Average confidence score: {avg_confidence:.2f}")

        # Top markets
        vendor_counts = {}
        for page in vendor_pages:
            market = page.market_name
            vendor_counts[market] = vendor_counts.get(market, 0) + len(page.vendors)

        print(f"\nTop 10 markets by vendor count:")
        for market, count in sorted(vendor_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {market}: {count} vendors")