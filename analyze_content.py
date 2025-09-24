#!/usr/bin/env python3
"""
Analyze scraped content to identify vendor information patterns
"""

import json
import re
from collections import defaultdict, Counter

def analyze_vendor_patterns(results_file):
    """Analyze scraped content for vendor information patterns"""

    with open(results_file, 'r', encoding='utf-8') as f:
        results = json.load(f)

    print("=== CONTENT ANALYSIS FOR VENDOR INFORMATION ===\n")

    # Track vendor-related keywords
    vendor_keywords = [
        'vendor', 'vendors', 'farm', 'farms', 'farmer', 'farmers',
        'bakery', 'bakeries', 'produce', 'artisan', 'local',
        'booth', 'booths', 'stall', 'stalls'
    ]

    site_analysis = defaultdict(list)

    for result in results:
        if result['error'] or not result['text_content']:
            continue

        site_type = result['site_type']
        content = result['text_content'].lower()

        # Count vendor keywords
        keyword_matches = {}
        for keyword in vendor_keywords:
            count = len(re.findall(r'\b' + keyword + r'\b', content))
            if count > 0:
                keyword_matches[keyword] = count

        # Look for specific vendor patterns
        vendor_patterns = {
            'business_names': len(re.findall(r'\b[A-Z][a-z]+ (?:Farm|Bakery|Gardens?|Orchards?)\b', result['text_content'])),
            'vendor_lists': len(re.findall(r'(?:vendors?|farmers?).*?include|featuring.*?(?:vendors?|farmers?)', content)),
            'contact_info': len(re.findall(r'(?:www\.|http|@|\.com|\.org|\.net)', content)),
            'addresses': len(re.findall(r'\d+\s+[A-Za-z\s]+(?:st|street|ave|avenue|rd|road|dr|drive|blvd|boulevard)', content, re.IGNORECASE))
        }

        analysis = {
            'market_name': result['market_name'],
            'url': result['url'],
            'content_length': len(result['text_content']),
            'keyword_matches': keyword_matches,
            'patterns': vendor_patterns,
            'has_vendor_info': sum(vendor_patterns.values()) > 0 or len(keyword_matches) > 0
        }

        site_analysis[site_type].append(analysis)

    # Print analysis by site type
    for site_type, analyses in site_analysis.items():
        print(f"=== {site_type.upper()} SITES ===")
        print(f"Total: {len(analyses)}")

        vendor_info_count = sum(1 for a in analyses if a['has_vendor_info'])
        print(f"Sites with vendor info: {vendor_info_count}/{len(analyses)} ({vendor_info_count/len(analyses)*100:.1f}%)")

        # Top keyword counts
        all_keywords = Counter()
        for analysis in analyses:
            for keyword, count in analysis['keyword_matches'].items():
                all_keywords[keyword] += count

        if all_keywords:
            print("Top vendor keywords:")
            for keyword, count in all_keywords.most_common(5):
                print(f"  {keyword}: {count}")

        # Pattern analysis
        pattern_totals = defaultdict(int)
        for analysis in analyses:
            for pattern, count in analysis['patterns'].items():
                pattern_totals[pattern] += count

        if pattern_totals:
            print("Pattern matches:")
            for pattern, count in pattern_totals.items():
                print(f"  {pattern}: {count}")

        # Show promising examples
        promising = [a for a in analyses if a['has_vendor_info']]
        if promising:
            print("Most promising sites:")
            for analysis in sorted(promising, key=lambda x: sum(x['patterns'].values()), reverse=True)[:3]:
                print(f"  {analysis['market_name']}: {analysis['url']}")

        print()

def find_vendor_examples(results_file):
    """Find specific examples of vendor information in scraped content"""

    with open(results_file, 'r', encoding='utf-8') as f:
        results = json.load(f)

    print("=== VENDOR INFORMATION EXAMPLES ===\n")

    for result in results:
        if result['error'] or not result['text_content']:
            continue

        content = result['text_content']

        # Look for vendor list patterns
        vendor_list_patterns = [
            r'(?:vendors?|farmers?|participants?).*?include[^.]*?[A-Z][a-z]+[^.]*?(?:\.|$)',
            r'featuring[^.]*?(?:farm|bakery|market|vendor)[^.]*?(?:\.|$)',
            r'(?:our|local)\s+(?:vendors?|farmers?)[^.]*?(?:\.|$)'
        ]

        found_examples = []
        for pattern in vendor_list_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
            found_examples.extend(matches[:2])  # Limit examples

        if found_examples:
            print(f"Market: {result['market_name']}")
            print(f"URL: {result['url']}")
            print("Vendor info found:")
            for example in found_examples[:2]:  # Show top 2 examples
                clean_example = ' '.join(example.split())[:200]  # Clean and limit length
                print(f"  - {clean_example}...")
            print()

if __name__ == "__main__":
    analyze_vendor_patterns("scraping_results.json")
    find_vendor_examples("scraping_results.json")