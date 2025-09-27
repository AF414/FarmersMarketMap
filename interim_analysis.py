#!/usr/bin/env python3
"""
Interim analysis based on current crawling results to identify improvement opportunities
"""

import json
import csv
from collections import defaultdict, Counter

def analyze_market_coverage():
    """Analyze what types of markets we have and identify gaps"""

    print("=== INTERIM ANALYSIS: MARKET COVERAGE & IMPROVEMENT OPPORTUNITIES ===\n")

    # Read the CSV to see all markets
    all_markets = []
    with open('Farmers Markets NJ.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_markets.append(row)

    print(f"Total markets in dataset: {len(all_markets)}")

    # Analyze URL patterns
    url_patterns = defaultdict(int)
    facebook_markets = []
    missing_url_markets = []

    for market in all_markets:
        url = market['URL'].strip()

        if not url or url in ['', 'or facebook.com']:
            missing_url_markets.append(market['Name'])
            url_patterns['missing'] += 1
        elif 'facebook.com' in url:
            facebook_markets.append(market['Name'])
            url_patterns['facebook'] += 1
        elif any(domain in url for domain in ['.gov', 'twp.', 'borough', 'city']):
            url_patterns['municipal'] += 1
        elif 'chamber' in url:
            url_patterns['chamber'] += 1
        elif any(keyword in url for keyword in ['farmersmarket', 'market']):
            url_patterns['dedicated_market'] += 1
        else:
            url_patterns['other'] += 1

    print(f"\nURL Pattern Analysis:")
    for pattern, count in sorted(url_patterns.items(), key=lambda x: x[1], reverse=True):
        percentage = count / len(all_markets) * 100
        print(f"  {pattern}: {count} ({percentage:.1f}%)")

    # Facebook markets (major opportunity)
    print(f"\nFacebook Markets ({len(facebook_markets)} total) - Need Special Handling:")
    for market in facebook_markets[:10]:  # Show first 10
        print(f"  • {market}")
    if len(facebook_markets) > 10:
        print(f"  ... and {len(facebook_markets) - 10} more")

    # Missing URL markets
    print(f"\nMarkets with Missing URLs ({len(missing_url_markets)} total):")
    for market in missing_url_markets[:5]:
        print(f"  • {market}")
    if len(missing_url_markets) > 5:
        print(f"  ... and {len(missing_url_markets) - 5} more")

def analyze_vendor_extraction_quality():
    """Analyze vendor extraction quality from existing results"""

    print(f"\n=== VENDOR EXTRACTION QUALITY ANALYSIS ===\n")

    # Load existing vendor data
    if not os.path.exists('clean_vendors.json'):
        print("No vendor data available yet for quality analysis")
        return

    with open('clean_vendors.json', 'r') as f:
        vendors = json.load(f)

    print(f"Current vendor extraction results:")
    print(f"  Total clean vendors: {len(vendors)}")

    # Analyze vendor types
    type_counts = Counter(v['type'] for v in vendors)
    print(f"\nVendor types found:")
    for vendor_type, count in type_counts.most_common():
        print(f"  {vendor_type}: {count}")

    # Analyze markets with good vendor data
    market_counts = Counter(v['market'] for v in vendors)
    print(f"\nMarkets with most vendor data:")
    for market, count in market_counts.most_common(10):
        print(f"  {market}: {count} vendors")

    # Quality indicators
    vendors_with_descriptions = len([v for v in vendors if v.get('description')])
    unique_vendor_names = len(set(v['name'] for v in vendors))

    print(f"\nQuality metrics:")
    print(f"  Vendors with descriptions: {vendors_with_descriptions}/{len(vendors)} ({vendors_with_descriptions/len(vendors)*100:.1f}%)")
    print(f"  Unique vendor names: {unique_vendor_names}/{len(vendors)} ({unique_vendor_names/len(vendors)*100:.1f}%)")

    # Common vendor patterns (for improvement)
    farm_vendors = [v for v in vendors if v['type'] == 'farm']
    bakery_vendors = [v for v in vendors if v['type'] == 'bakery']

    print(f"\nSpecialty breakdowns:")
    print(f"  Farms: {len(farm_vendors)}")
    print(f"  Bakeries: {len(bakery_vendors)}")

def identify_improvement_opportunities():
    """Identify specific areas for improvement"""

    print(f"\n=== IMPROVEMENT OPPORTUNITIES ===\n")

    opportunities = [
        {
            'priority': 'HIGH',
            'area': 'Facebook Page Scraping',
            'description': 'Need selenium/API approach for ~25% of markets on Facebook',
            'impact': 'Could add 30+ markets with vendor data',
            'approach': 'Use selenium webdriver or Facebook Graph API'
        },
        {
            'priority': 'HIGH',
            'area': 'Vendor Name Cleaning',
            'description': 'Current parsing has ~28% noise rate in vendor names',
            'impact': 'Improve data quality and reduce false positives',
            'approach': 'AI-powered content extraction + better regex patterns'
        },
        {
            'priority': 'MEDIUM',
            'area': 'Contact Information Extraction',
            'description': 'Not extracting vendor websites, phones, emails consistently',
            'impact': 'Enable direct vendor contact and validation',
            'approach': 'Enhanced regex patterns + structured data detection'
        },
        {
            'priority': 'MEDIUM',
            'area': 'Municipal Site Deep Crawling',
            'description': 'Municipal sites often have vendor info buried in PDFs/docs',
            'impact': 'Access to 15-20 additional markets',
            'approach': 'PDF parsing + document crawling capabilities'
        },
        {
            'priority': 'LOW',
            'area': 'Vendor Classification',
            'description': 'Basic type classification (farm/bakery/etc)',
            'impact': 'Better categorization for mapping and analysis',
            'approach': 'ML classification based on product descriptions'
        }
    ]

    for opp in opportunities:
        print(f"🔴 {opp['priority']} PRIORITY: {opp['area']}")
        print(f"   Issue: {opp['description']}")
        print(f"   Impact: {opp['impact']}")
        print(f"   Approach: {opp['approach']}")
        print()

def project_full_results():
    """Project what full results might look like"""

    print(f"=== PROJECTED FULL CRAWL RESULTS ===\n")

    # Based on 50-site sample, project to 130 markets
    sample_markets = 10  # Markets we crawled for vendors
    sample_vendors = 167  # Clean vendors found

    # Estimate coverage
    estimated_crawlable = 80  # ~60% of markets have crawlable websites
    estimated_vendors_per_market = sample_vendors / sample_markets  # ~16.7

    projected_vendors = estimated_crawlable * estimated_vendors_per_market

    print(f"Projections based on current sample:")
    print(f"  Estimated crawlable markets: ~{estimated_crawlable}")
    print(f"  Average vendors per market: ~{estimated_vendors_per_market:.1f}")
    print(f"  Projected total vendors: ~{int(projected_vendors)}")
    print(f"  Unique farms estimated: ~{int(projected_vendors * 0.3)}")  # 30% are farms
    print(f"  Unique businesses estimated: ~{int(projected_vendors * 0.7)}")

    print(f"\nWith Facebook scraping improvement:")
    print(f"  Additional markets: ~30")
    print(f"  Additional vendors: ~{int(30 * estimated_vendors_per_market)}")
    print(f"  Total potential vendors: ~{int(projected_vendors + 30 * estimated_vendors_per_market)}")

if __name__ == "__main__":
    import os

    analyze_market_coverage()
    analyze_vendor_extraction_quality()
    identify_improvement_opportunities()
    project_full_results()

    print(f"\n=== NEXT STEPS WHILE WAITING FOR FULL CRAWL ===")
    print(f"1. Implement Facebook scraping solution")
    print(f"2. Improve vendor name extraction with AI assistance")
    print(f"3. Add contact information extraction")
    print(f"4. Wait for full crawl to complete for comprehensive analysis")