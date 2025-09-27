#!/usr/bin/env python3
"""
Full-scale crawler for all farmers markets in the CSV
"""

import csv
import json
import time
from vendor_crawler import VendorPageCrawler
from scraper import FarmersMarketScraper

def crawl_all_markets(csv_file: str, batch_size: int = 20):
    """Crawl all markets in batches to avoid overwhelming servers"""

    # First, scrape all markets to get basic site info
    print("=== PHASE 1: Initial site scraping ===")
    basic_scraper = FarmersMarketScraper(delay=1.0)

    # Get total count first
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        total_markets = sum(1 for _ in reader)

    print(f"Found {total_markets} markets to process")

    # Scrape all sites for basic info
    all_results = basic_scraper.scrape_from_csv(csv_file)
    basic_scraper.save_results(all_results, "all_markets_basic.json")
    basic_scraper.analyze_results(all_results)

    # Filter to successful sites with vendor potential
    print("\n=== PHASE 2: Vendor page crawling ===")

    promising_sites = []
    for result in all_results:
        if (result.error is None and
            result.site_type in ['dedicated_market', 'municipal', 'other'] and
            any(keyword in result.text_content.lower()
                for keyword in ['vendor', 'farmer', 'producer', 'artisan'])):
            promising_sites.append({
                'market_name': result.market_name,
                'url': result.url,
                'site_type': result.site_type,
                'text_content': result.text_content
            })

    print(f"Found {len(promising_sites)} promising sites for vendor crawling")

    # Crawl in batches
    crawler = VendorPageCrawler(delay=1.5, max_depth=2)
    all_vendor_pages = []

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
            time.sleep(10)

    # Save final results
    crawler.save_vendor_pages(all_vendor_pages, "all_vendor_pages.json")

    return all_results, all_vendor_pages

def analyze_full_results(basic_results, vendor_pages):
    """Analyze results from full crawl"""

    print("\n=== FULL CRAWL ANALYSIS ===")

    # Basic scraping stats
    total_sites = len(basic_results)
    successful_basic = len([r for r in basic_results if r['error'] is None])

    print(f"Basic scraping results:")
    print(f"  Total markets: {total_sites}")
    print(f"  Successful scrapes: {successful_basic} ({successful_basic/total_sites*100:.1f}%)")

    # Site type breakdown
    site_types = {}
    for result in basic_results:
        site_type = result['site_type']
        if site_type not in site_types:
            site_types[site_type] = {'total': 0, 'successful': 0}
        site_types[site_type]['total'] += 1
        if result['error'] is None:
            site_types[site_type]['successful'] += 1

    print(f"\nBy site type:")
    for site_type, stats in sorted(site_types.items()):
        success_rate = stats['successful'] / stats['total'] * 100
        print(f"  {site_type}: {stats['successful']}/{stats['total']} ({success_rate:.1f}%)")

    # Vendor crawling stats
    markets_with_vendors = len(set(page.market_name for page in vendor_pages))
    total_vendor_pages = len(vendor_pages)
    total_vendors = sum(len(page.vendors) for page in vendor_pages)

    print(f"\nVendor crawling results:")
    print(f"  Markets with vendor data: {markets_with_vendors}")
    print(f"  Total vendor pages found: {total_vendor_pages}")
    print(f"  Total vendors extracted: {total_vendors}")

    if vendor_pages:
        avg_vendors_per_market = total_vendors / markets_with_vendors
        avg_confidence = sum(page.confidence_score for page in vendor_pages) / len(vendor_pages)
        print(f"  Average vendors per market: {avg_vendors_per_market:.1f}")
        print(f"  Average confidence score: {avg_confidence:.2f}")

    # Top performing markets
    vendor_counts = {}
    for page in vendor_pages:
        market = page.market_name
        vendor_counts[market] = vendor_counts.get(market, 0) + len(page.vendors)

    print(f"\nTop markets by vendor count:")
    for market, count in sorted(vendor_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {market}: {count} vendors")

    # Sites that need alternative approaches
    failed_sites = [r for r in basic_results if r['error'] is not None]
    facebook_sites = [r for r in basic_results if r['site_type'] == 'facebook']

    print(f"\nSites needing alternative approaches:")
    print(f"  Failed scrapes: {len(failed_sites)}")
    print(f"  Facebook pages: {len(facebook_sites)}")

    if facebook_sites:
        print("  Facebook markets (need special handling):")
        for site in facebook_sites[:5]:  # Show first 5
            print(f"    - {site['market_name']}")

    # Save analysis results
    analysis = {
        'total_markets': total_sites,
        'successful_basic_scrapes': successful_basic,
        'site_type_breakdown': site_types,
        'markets_with_vendors': markets_with_vendors,
        'total_vendor_pages': total_vendor_pages,
        'total_vendors': total_vendors,
        'top_markets': dict(sorted(vendor_counts.items(), key=lambda x: x[1], reverse=True)[:20]),
        'failed_sites': [{'name': s['market_name'], 'url': s['url'], 'error': s['error']} for s in failed_sites],
        'facebook_sites': [{'name': s['market_name'], 'url': s['url']} for s in facebook_sites]
    }

    with open('full_crawl_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)

    return analysis

if __name__ == "__main__":
    print("Starting full crawl of all farmers markets...")
    print("This will take a while - processing in respectful batches")

    basic_results, vendor_pages = crawl_all_markets("Farmers Markets NJ.csv", batch_size=15)

    print(f"\n=== CRAWL COMPLETE ===")
    print(f"Basic results saved to: all_markets_basic.json")
    print(f"Vendor pages saved to: all_vendor_pages.json")

    analysis = analyze_full_results(basic_results, vendor_pages)
    print(f"Analysis saved to: full_crawl_analysis.json")