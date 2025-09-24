#!/usr/bin/env python3
"""
Analyze and clean extracted vendor data
"""

import json
import re
from collections import defaultdict, Counter

def clean_vendor_name(name: str) -> str:
    """Clean up vendor names that may have been incorrectly parsed"""

    # Remove obvious parsing errors
    name = re.sub(r'^(Produce|Herbs|Jersey Clams|Sheep Dairy Products|Pet Items|Oil Baked Goods & Sweets)', '', name)

    # Fix common concatenation issues
    name = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)  # Add space before capital letters

    # Remove common non-vendor words at the start
    prefixes_to_remove = ['Site design by The', 'THICGet one of our limited', 'The ', 'A ']
    for prefix in prefixes_to_remove:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()

    return name.strip()

def is_likely_vendor(vendor: dict) -> bool:
    """Determine if this is likely a real vendor vs parsing noise"""

    name = vendor['name']

    # Skip obviously bad names
    bad_indicators = [
        'site design', 'limited edition', 'organic cotton', 'menu close',
        'skip to content', 'open menu', 'copyright', '©', 'all rights',
        'entering its', 'delightful place', 'friendly volunteers'
    ]

    if any(indicator in name.lower() for indicator in bad_indicators):
        return False

    # Skip very short or very long names
    if len(name) < 3 or len(name) > 50:
        return False

    # Good indicators
    good_types = ['farm', 'bakery', 'kitchen', 'market', 'garden', 'orchard']
    if vendor.get('type') in good_types:
        return True

    # Good patterns in name
    good_patterns = [
        r'\b(farm|bakery|kitchen|market|garden|orchard|dairy|ranch)\b',
        r'\b(organic|fresh|local|family)\b',
        r"[A-Z][a-z]+'s",  # Possessive names like "Smith's"
    ]

    if any(re.search(pattern, name, re.IGNORECASE) for pattern in good_patterns):
        return True

    return False

def analyze_vendor_data(vendor_file: str):
    """Analyze extracted vendor data quality"""

    with open(vendor_file, 'r') as f:
        pages = json.load(f)

    print("=== VENDOR DATA ANALYSIS ===\n")

    # Overall stats
    total_pages = len(pages)
    total_raw_vendors = sum(len(page['vendors']) for page in pages)

    # Clean and filter vendors
    all_clean_vendors = []
    market_vendor_counts = defaultdict(int)

    for page in pages:
        market_name = page['market_name']
        clean_vendors_for_page = []

        for vendor in page['vendors']:
            clean_name = clean_vendor_name(vendor['name'])
            vendor['clean_name'] = clean_name

            if is_likely_vendor(vendor):
                clean_vendors_for_page.append(vendor)
                all_clean_vendors.append({
                    'market': market_name,
                    'name': clean_name,
                    'type': vendor.get('type', 'unknown'),
                    'description': vendor.get('description', ''),
                    'url': page['page_url']
                })

        market_vendor_counts[market_name] += len(clean_vendors_for_page)

    print(f"Total pages crawled: {total_pages}")
    print(f"Raw vendors extracted: {total_raw_vendors}")
    print(f"Clean vendors after filtering: {len(all_clean_vendors)}")
    print(f"Cleaning success rate: {len(all_clean_vendors)/total_raw_vendors*100:.1f}%")

    print(f"\n=== VENDORS BY MARKET ===")
    for market, count in sorted(market_vendor_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{market}: {count} vendors")

    print(f"\n=== VENDOR TYPES ===")
    type_counts = Counter(v['type'] for v in all_clean_vendors)
    for vendor_type, count in type_counts.most_common():
        print(f"{vendor_type}: {count}")

    print(f"\n=== SAMPLE CLEAN VENDORS ===")
    # Show sample vendors by market
    by_market = defaultdict(list)
    for vendor in all_clean_vendors:
        by_market[vendor['market']].append(vendor)

    for market, vendors in list(by_market.items())[:5]:  # Top 5 markets
        print(f"\n{market}:")
        for vendor in vendors[:8]:  # Show up to 8 vendors per market
            desc = f" - {vendor['description'][:50]}..." if vendor['description'] else ""
            print(f"  • {vendor['name']} ({vendor['type']}){desc}")

    # Save clean data
    with open('clean_vendors.json', 'w') as f:
        json.dump(all_clean_vendors, f, indent=2, ensure_ascii=False)

    print(f"\nClean vendor data saved to: clean_vendors.json")

    return all_clean_vendors

def find_vendor_websites(vendors):
    """Look for vendor websites in descriptions or names"""
    vendors_with_websites = []

    for vendor in vendors:
        websites = []

        # Look for websites in description
        if vendor.get('description'):
            url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
            urls = re.findall(url_pattern, vendor['description'])
            websites.extend(urls)

            # Look for domain patterns
            domain_pattern = r'\b[a-zA-Z0-9-]+\.(com|org|net|farm)\b'
            domains = re.findall(domain_pattern, vendor['description'])
            websites.extend([f"https://{domain}" for domain in domains])

        if websites:
            vendor['potential_websites'] = list(set(websites))  # Remove duplicates
            vendors_with_websites.append(vendor)

    return vendors_with_websites

if __name__ == "__main__":
    clean_vendors = analyze_vendor_data("vendor_pages.json")

    # Find vendors with potential websites
    vendors_with_sites = find_vendor_websites(clean_vendors)

    if vendors_with_sites:
        print(f"\n=== VENDORS WITH POTENTIAL WEBSITES ===")
        print(f"Found {len(vendors_with_sites)} vendors with potential websites:")

        for vendor in vendors_with_sites[:10]:  # Show first 10
            print(f"  • {vendor['name']} ({vendor['market']})")
            for site in vendor['potential_websites']:
                print(f"    - {site}")