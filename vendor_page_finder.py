#!/usr/bin/env python3
"""
Vendor Page Finder - Phase 1
Smart crawler to find the most likely vendor listing page for each farmers market
"""

import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from typing import List, Optional, Set
import csv
import json

@dataclass
class VendorPageCandidate:
    url: str
    title: str
    score: float
    reasons: List[str]
    content_preview: str

class VendorPageFinder:
    def __init__(self, delay: float = 1.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; VendorFinder/1.0)'
        })

        # High-value keywords for vendor pages
        self.vendor_keywords = {
            'high': ['vendors', 'vendor directory', 'our vendors', 'vendor list', 'meet our vendors'],
            'medium': ['farmers', 'growers', 'producers', 'market vendors', 'participants'],
            'low': ['about', 'who we are', 'businesses', 'artisans']
        }

    def find_best_vendor_page(self, market_name: str, base_url: str, max_depth: int = 2) -> Optional[VendorPageCandidate]:
        """Find the best vendor listing page for a farmers market"""
        print(f"\n🔍 Finding vendor page for: {market_name}")
        print(f"   Base URL: {base_url}")

        visited = set()
        candidates = []

        # Always check the main page first (even if score is low)
        main_candidate = self._evaluate_page(base_url, "Main Page", force_include=True)
        if main_candidate:
            candidates.append(main_candidate)

        # Find and evaluate linked pages
        try:
            soup = self._fetch_page(base_url)
            if soup:
                links = self._find_vendor_links(soup, base_url)
                print(f"   Found {len(links)} potential vendor links")

                for link in links[:5]:  # Limit to top 5 links
                    if link not in visited:
                        visited.add(link)
                        time.sleep(self.delay)

                        candidate = self._evaluate_page(link, f"Linked page: {link}")
                        if candidate:
                            candidates.append(candidate)

        except Exception as e:
            print(f"   ⚠️  Error crawling links: {str(e)}")

        # Return best candidate
        if candidates:
            best = max(candidates, key=lambda c: c.score)
            print(f"   ✅ Best page: {best.url} (score: {best.score:.2f})")
            print(f"   Reasons: {', '.join(best.reasons)}")
            return best
        else:
            print(f"   ❌ No vendor pages found")
            return None

    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a web page"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"      Error fetching {url}: {str(e)}")
            return None

    def _find_vendor_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find links that likely lead to vendor information"""
        vendor_links = []

        for link in soup.find_all('a', href=True):
            href = link.get('href')
            link_text = link.get_text().strip().lower()

            # Skip non-HTTP links
            full_url = urljoin(base_url, href)
            if not full_url.startswith(('http://', 'https://')):
                continue

            # Score link based on text
            score = 0
            for keyword in self.vendor_keywords['high']:
                if keyword in link_text:
                    score += 3
            for keyword in self.vendor_keywords['medium']:
                if keyword in link_text:
                    score += 2
            for keyword in self.vendor_keywords['low']:
                if keyword in link_text:
                    score += 1

            if score > 0:
                vendor_links.append((full_url, score, link_text))

        # Sort by score and return URLs
        vendor_links.sort(key=lambda x: x[1], reverse=True)
        return [link[0] for link in vendor_links]

    def _evaluate_page(self, url: str, page_type: str, force_include: bool = False) -> Optional[VendorPageCandidate]:
        """Evaluate how likely a page is to contain vendor information"""
        soup = self._fetch_page(url)
        if not soup:
            return None

        # Get page text - extract both visible text and text from JSON/JavaScript
        page_text = soup.get_text()
        page_title = soup.title.string if soup.title else "No title"

        # Also extract text from JSON/script content that might contain vendor data
        for script in soup.find_all('script'):
            if script.string:
                page_text += " " + script.string

        page_text = page_text.lower()

        score = 0
        reasons = []

        # Score based on content indicators
        vendor_mentions = page_text.count('vendor') + page_text.count('farmer') + page_text.count('grower')
        if vendor_mentions >= 5:
            score += 3
            reasons.append(f"{vendor_mentions} vendor/farmer mentions")
        elif vendor_mentions >= 2:
            score += 1
            reasons.append(f"{vendor_mentions} vendor mentions")

        # Look for structured lists
        lists = soup.find_all(['ul', 'ol', 'table'])
        for lst in lists:
            list_text = lst.get_text().lower()
            if any(keyword in list_text for keyword in ['farm', 'vendor', 'business']):
                score += 2
                reasons.append("Contains structured vendor list")
                break

        # Look for contact information patterns
        if 'email' in page_text or '@' in page_text:
            score += 1
            reasons.append("Contains contact information")

        # Look for business names (patterns like "X Farm", "Y Bakery", etc.)
        import re
        business_patterns = [
            r'\w+\s+farm', r'\w+\s+bakery', r'\w+\s+dairy',
            r'\w+\s+orchard', r'\w+\s+kitchen', r'\w+\s+gardens?',
            r'\w+\s+cafe', r'\w+\s+company', r'\w+\s+treats',
            r'\w+\s+kettle\s+corn', r'\w+\s+dips', r'\w+\s+bites'
        ]
        business_count = 0
        for pattern in business_patterns:
            business_count += len(re.findall(pattern, page_text, re.IGNORECASE))

        # Also look for vendor-like business names without specific keywords
        vendor_name_patterns = [
            r'[A-Z][a-z]+\'s\s+[A-Z][a-z]+',  # "Abraham's Landscape"
            r'[A-Z][a-z]+\s+&\s+[A-Z][a-z]+', # "Bark & Bites"
            r'\d+\s+[A-Z][a-z]+\s+[A-Z][a-z]+', # "24 Karrot Spread"
            r'[A-Z][a-z]+\s+[A-Z][a-z]+\s+LLC', # "Company LLC"
        ]

        for pattern in vendor_name_patterns:
            business_count += len(re.findall(pattern, page_text))

        if business_count >= 10:
            score += 3
            reasons.append(f"{business_count} business names found")
        elif business_count >= 5:
            score += 2
            reasons.append(f"{business_count} business names found")
        elif business_count >= 2:
            score += 1
            reasons.append(f"{business_count} business names")

        # Penalty for very short pages
        if len(page_text) < 500:
            score -= 1
            reasons.append("Short page content")

        # Create preview
        preview = page_text[:300] + "..." if len(page_text) > 300 else page_text

        # For main pages, always return a candidate even with low score
        # This catches cases where vendors are listed on main page without explicit links
        if score > 0 or force_include:
            if score == 0 and force_include:
                score = 0.5  # Minimum score for main pages
                reasons.append("Main page (fallback)")

            return VendorPageCandidate(
                url=url,
                title=page_title,
                score=score,
                reasons=reasons,
                content_preview=preview
            )
        return None

    def process_all_markets(self, csv_file: str = "websites_only.csv") -> List[dict]:
        """Process all markets and find their best vendor pages"""
        print("🚀 Starting vendor page discovery for all markets")

        # Load markets from CSV
        markets = []
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if row and not row[0].startswith('#'):
                    url = row[0].strip()
                    if url:
                        if not url.startswith(('http://', 'https://')):
                            url = f"https://{url}"
                        market_name = url.replace('https://', '').replace('http://', '').split('/')[0]
                        markets.append({'name': market_name, 'url': url, 'index': i})

        print(f"📋 Processing {len(markets)} markets")

        results = []
        successful = 0

        for i, market in enumerate(markets, 1):
            print(f"\n[{i}/{len(markets)}] Processing: {market['name']}")

            best_page = self.find_best_vendor_page(market['name'], market['url'])

            result = {
                'market_name': market['name'],
                'market_url': market['url'],
                'market_index': market['index'],
                'vendor_page_found': best_page is not None,
                'vendor_page_url': best_page.url if best_page else None,
                'vendor_page_title': best_page.title if best_page else None,
                'vendor_page_score': best_page.score if best_page else 0,
                'vendor_page_reasons': best_page.reasons if best_page else [],
                'content_preview': best_page.content_preview if best_page else None
            }

            results.append(result)

            if best_page:
                successful += 1

            # Save intermediate results every 10 markets
            if i % 10 == 0:
                self._save_results(results, f"vendor_pages_batch_{i-9}_to_{i}.json")

        # Final summary
        print(f"\n✅ Vendor page discovery complete!")
        print(f"   Markets processed: {len(markets)}")
        print(f"   Vendor pages found: {successful}")
        print(f"   Success rate: {successful/len(markets)*100:.1f}%")

        # Save final results
        output_file = "vendor_pages_discovery.json"
        self._save_results(results, output_file)
        print(f"   Results saved to: {output_file}")

        return results

    def _save_results(self, results: List[dict], filename: str):
        """Save results to JSON file"""
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Find vendor pages for farmers markets")
    parser.add_argument('--csv-file', default='websites_only.csv', help='CSV file with market URLs')
    parser.add_argument('--delay', type=float, default=1.5, help='Delay between requests (seconds)')
    parser.add_argument('--test-single', help='Test on a single market URL')
    parser.add_argument('--test-name', help='Name for single market test')

    args = parser.parse_args()

    finder = VendorPageFinder(delay=args.delay)

    if args.test_single:
        # Test on single market
        market_name = args.test_name or "Test Market"
        result = finder.find_best_vendor_page(market_name, args.test_single)
        if result:
            print(f"\n✅ Found vendor page: {result.url}")
            print(f"   Score: {result.score}")
            print(f"   Reasons: {', '.join(result.reasons)}")
        else:
            print("\n❌ No vendor page found")
    else:
        # Process all markets
        finder.process_all_markets(args.csv_file)

if __name__ == "__main__":
    main()