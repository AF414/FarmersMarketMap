#!/usr/bin/env python3
"""
Test vendor page finder on a few markets from websites_only.csv
"""

import csv
import json
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time

def simple_vendor_page_finder(market_name, base_url):
    """
    Simple vendor page detection without LLM
    """
    vendor_indicators = [
        'vendors', 'farmers', 'growers', 'producers', 'artisans',
        'vendor directory', 'our vendors', 'meet the vendors',
        'participants', 'market vendors'
    ]

    product_indicators = [
        'farm', 'bakery', 'dairy', 'orchard', 'gardens', 'kitchen',
        'organic', 'fresh', 'local', 'handmade', 'artisan'
    ]

    results = {
        'market_name': market_name,
        'base_url': base_url,
        'vendor_pages_found': [],
        'potential_vendors': [],
        'processing_time': 0
    }

    start_time = time.time()

    try:
        print(f"  Fetching: {base_url}")
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for vendor-related links
        vendor_links = []
        for link in soup.find_all('a', href=True):
            link_text = link.get_text().lower().strip()
            link_url = urljoin(base_url, link['href'])

            for indicator in vendor_indicators:
                if indicator in link_text:
                    vendor_links.append({
                        'text': link_text,
                        'url': link_url,
                        'indicator': indicator
                    })
                    print(f"    Found vendor link: {link_text} -> {link_url}")
                    break

        results['vendor_pages_found'] = vendor_links

        # Look for potential vendor names in main page content
        page_text = soup.get_text().lower()
        potential_vendors = []

        # Simple heuristic: look for patterns like "X Farm", "Y Bakery", etc.
        import re
        vendor_patterns = [
            r'(\w+\s+farm(?:s)?)',
            r'(\w+\s+bakery)',
            r'(\w+\s+dairy)',
            r'(\w+\s+orchard)',
            r'(\w+\s+gardens?)',
            r'(\w+\s+kitchen)',
            r'(\w+\s+ranch)',
            r'(\w+\s+creamery)'
        ]

        for pattern in vendor_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                if len(match) > 3 and match not in potential_vendors:
                    potential_vendors.append(match.title())

        # Remove duplicates and common false positives
        potential_vendors = [v for v in set(potential_vendors)
                           if v not in ['The Farm', 'Local Farm', 'Farm Fresh', 'Farm Market']]

        results['potential_vendors'] = potential_vendors[:10]  # Limit to first 10

        if potential_vendors:
            print(f"    Potential vendors: {', '.join(potential_vendors[:5])}...")

    except Exception as e:
        print(f"    Error: {str(e)}")
        results['error'] = str(e)

    results['processing_time'] = time.time() - start_time
    return results

def test_markets(csv_file="websites_only.csv", max_markets=10):
    """Test vendor finding on first few markets"""

    print(f"=== Testing vendor page finder on {max_markets} markets ===")

    markets = []
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= max_markets:
                break
            if row and not row[0].startswith('#'):
                url = row[0].strip()
                if url:
                    if not url.startswith(('http://', 'https://')):
                        url = f"https://{url}"

                    market_name = url.replace('https://', '').replace('http://', '').split('/')[0]
                    markets.append({'name': market_name, 'url': url})

    results = []

    for i, market in enumerate(markets, 1):
        print(f"\n{i}. Processing: {market['name']}")

        result = simple_vendor_page_finder(market['name'], market['url'])
        results.append(result)

        # Brief pause between requests
        time.sleep(1)

    # Summary
    print(f"\n=== SUMMARY ===")
    total_vendor_pages = sum(len(r.get('vendor_pages_found', [])) for r in results)
    total_potential_vendors = sum(len(r.get('potential_vendors', [])) for r in results)
    successful_markets = len([r for r in results if not r.get('error')])

    print(f"Markets processed: {len(results)}")
    print(f"Successful: {successful_markets}")
    print(f"Vendor pages found: {total_vendor_pages}")
    print(f"Potential vendors identified: {total_potential_vendors}")

    # Save results
    output_file = "test_vendor_finder_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"Results saved to: {output_file}")

    return results

if __name__ == "__main__":
    max_markets = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    test_markets(max_markets=max_markets)