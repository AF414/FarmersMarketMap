#!/usr/bin/env python3
"""
Comprehensive vendor extraction from all farmers markets
Enhanced version that follows vendor page links and extracts detailed information
"""

import csv
import json
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import re
from typing import List, Dict, Set

class ComprehensiveVendorExtractor:
    def __init__(self, delay=1.5):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        # Enhanced vendor detection patterns
        self.vendor_indicators = [
            'vendors', 'farmers', 'growers', 'producers', 'artisans',
            'vendor directory', 'our vendors', 'meet the vendors',
            'participants', 'market vendors', 'vendor list'
        ]

        # Patterns to identify farm/vendor names
        self.vendor_name_patterns = [
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Farm|Farms|Farmstead))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Dairy|Creamery))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Bakery|Kitchen))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Orchard|Orchards))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Gardens?|Greenhouse))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Ranch|Acres|Estate))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Produce|Vegetables))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Honey|Maple))',
            r'([A-Z][a-z]+\s+(?:Family\s+)?(?:Farm|Farms))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:LLC|Inc|Co\.))',
            r'((?:[A-Z][a-z]+\s+)*[A-Z][a-z]+(?:\s+&\s+[A-Z][a-z]+)*\s+Farm)'
        ]

        # Location patterns
        self.location_patterns = [
            r'(\d+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Road|Rd|Street|St|Avenue|Ave|Lane|Ln|Drive|Dr|Boulevard|Blvd))',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2}(?:\s+\d{5})?)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*New\s+Jersey)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*NJ)'
        ]

        # Product patterns
        self.product_keywords = [
            'organic', 'fresh', 'local', 'seasonal', 'grass-fed', 'free-range',
            'vegetables', 'fruits', 'herbs', 'flowers', 'honey', 'bread',
            'cheese', 'milk', 'eggs', 'meat', 'pork', 'beef', 'chicken',
            'produce', 'tomatoes', 'lettuce', 'peppers', 'onions', 'carrots',
            'apples', 'berries', 'peaches', 'corn', 'squash', 'beans'
        ]

    def fetch_page(self, url: str, timeout: int = 10) -> BeautifulSoup:
        """Fetch and parse a web page"""
        try:
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"    Error fetching {url}: {str(e)}")
            return None

    def find_vendor_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Find links that likely lead to vendor information"""
        vendor_links = []
        seen_urls = set()

        for link in soup.find_all('a', href=True):
            link_text = link.get_text().lower().strip()
            link_url = urljoin(base_url, link['href'])

            # Skip duplicates and invalid URLs
            if link_url in seen_urls or not link_url.startswith(('http://', 'https://')):
                continue

            # Check if link text contains vendor indicators
            for indicator in self.vendor_indicators:
                if indicator in link_text:
                    vendor_links.append({
                        'text': link_text,
                        'url': link_url,
                        'indicator': indicator,
                        'priority': self._get_link_priority(link_text)
                    })
                    seen_urls.add(link_url)
                    break

        return sorted(vendor_links, key=lambda x: x['priority'], reverse=True)

    def _get_link_priority(self, link_text: str) -> int:
        """Assign priority to vendor links based on text quality"""
        high_priority = ['vendor directory', 'our vendors', 'vendor list', 'meet the vendors']
        medium_priority = ['vendors', 'farmers', 'growers', 'current vendors']

        for term in high_priority:
            if term in link_text:
                return 3

        for term in medium_priority:
            if term in link_text:
                return 2

        return 1

    def extract_vendors_from_page(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract vendor information from a page"""
        vendors = []

        if not soup:
            return vendors

        # Get all text content
        page_text = soup.get_text()

        # Extract vendor names using patterns
        vendor_names = set()
        for pattern in self.vendor_name_patterns:
            matches = re.findall(pattern, page_text)
            for match in matches:
                if len(match.strip()) > 3:  # Filter out very short matches
                    vendor_names.add(match.strip())

        # For each vendor name, try to extract additional information
        for vendor_name in vendor_names:
            vendor_info = {
                'name': vendor_name,
                'page_url': url,
                'location': None,
                'products': [],
                'contact_info': {},
                'description': None
            }

            # Try to find location information near the vendor name
            vendor_info['location'] = self._find_location_near_vendor(page_text, vendor_name)

            # Try to find product information
            vendor_info['products'] = self._find_products_near_vendor(page_text, vendor_name)

            # Try to find contact information
            vendor_info['contact_info'] = self._find_contact_info_near_vendor(soup, vendor_name)

            vendors.append(vendor_info)

        # Also look for structured vendor listings (tables, lists, etc.)
        structured_vendors = self._extract_structured_vendors(soup, url)
        vendors.extend(structured_vendors)

        return vendors

    def _find_location_near_vendor(self, text: str, vendor_name: str) -> str:
        """Find location information near a vendor name in text"""
        # Look for location patterns within 200 characters of vendor name
        vendor_pos = text.lower().find(vendor_name.lower())
        if vendor_pos == -1:
            return None

        # Search in vicinity of vendor name
        start = max(0, vendor_pos - 100)
        end = min(len(text), vendor_pos + len(vendor_name) + 100)
        vicinity = text[start:end]

        for pattern in self.location_patterns:
            matches = re.findall(pattern, vicinity)
            if matches:
                return matches[0]

        return None

    def _find_products_near_vendor(self, text: str, vendor_name: str) -> List[str]:
        """Find product information near a vendor name"""
        products = []
        vendor_pos = text.lower().find(vendor_name.lower())
        if vendor_pos == -1:
            return products

        # Search in vicinity of vendor name
        start = max(0, vendor_pos - 50)
        end = min(len(text), vendor_pos + len(vendor_name) + 200)
        vicinity = text[start:end].lower()

        for keyword in self.product_keywords:
            if keyword in vicinity:
                products.append(keyword)

        return products[:5]  # Limit to 5 products

    def _find_contact_info_near_vendor(self, soup: BeautifulSoup, vendor_name: str) -> Dict:
        """Find contact information near a vendor name"""
        contact_info = {}

        # Look for email addresses
        email_pattern = r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        emails = re.findall(email_pattern, soup.get_text())
        if emails:
            contact_info['email'] = emails[0]

        # Look for phone numbers
        phone_pattern = r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})'
        phones = re.findall(phone_pattern, soup.get_text())
        if phones:
            contact_info['phone'] = phones[0]

        # Look for websites
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith(('http://', 'https://')) and 'farm' in href.lower():
                contact_info['website'] = href
                break

        return contact_info

    def _extract_structured_vendors(self, soup: BeautifulSoup, url: str) -> List[Dict]:
        """Extract vendors from structured data (tables, lists)"""
        vendors = []

        # Look for tables with vendor information
        for table in soup.find_all('table'):
            table_text = table.get_text()
            if any(keyword in table_text.lower() for keyword in ['vendor', 'farm', 'business']):
                # Extract vendor names from table rows
                for row in table.find_all('tr'):
                    row_text = row.get_text().strip()
                    if len(row_text) > 10:  # Skip header rows
                        for pattern in self.vendor_name_patterns:
                            matches = re.findall(pattern, row_text)
                            for match in matches:
                                vendors.append({
                                    'name': match,
                                    'page_url': url,
                                    'location': None,
                                    'products': [],
                                    'contact_info': {},
                                    'description': row_text[:200]
                                })

        # Look for lists with vendor information
        for ul in soup.find_all(['ul', 'ol']):
            for li in ul.find_all('li'):
                li_text = li.get_text().strip()
                if len(li_text) > 5:
                    for pattern in self.vendor_name_patterns:
                        matches = re.findall(pattern, li_text)
                        for match in matches:
                            vendors.append({
                                'name': match,
                                'page_url': url,
                                'location': None,
                                'products': [],
                                'contact_info': {},
                                'description': li_text[:200]
                            })

        return vendors

    def process_market(self, market_name: str, market_url: str) -> Dict:
        """Process a single farmers market"""
        print(f"\nProcessing: {market_name}")
        print(f"  URL: {market_url}")

        result = {
            'market_name': market_name,
            'market_url': market_url,
            'vendor_pages_found': [],
            'vendors': [],
            'processing_time': 0,
            'error': None
        }

        start_time = time.time()

        try:
            # Fetch main market page
            soup = self.fetch_page(market_url)
            if not soup:
                result['error'] = 'Failed to fetch main page'
                return result

            # Find vendor links
            vendor_links = self.find_vendor_links(soup, market_url)
            result['vendor_pages_found'] = vendor_links[:5]  # Limit to top 5

            print(f"  Found {len(vendor_links)} vendor page links")

            # Extract vendors from main page
            main_page_vendors = self.extract_vendors_from_page(soup, market_url)
            result['vendors'].extend(main_page_vendors)

            if main_page_vendors:
                print(f"  Found {len(main_page_vendors)} vendors on main page")

            # Follow top vendor links
            for i, link in enumerate(vendor_links[:3]):  # Follow top 3 links
                print(f"  Following link {i+1}: {link['text']}")

                time.sleep(self.delay)  # Be respectful

                vendor_soup = self.fetch_page(link['url'])
                if vendor_soup:
                    page_vendors = self.extract_vendors_from_page(vendor_soup, link['url'])
                    result['vendors'].extend(page_vendors)

                    if page_vendors:
                        print(f"    Found {len(page_vendors)} vendors")

            # Remove duplicates
            seen_names = set()
            unique_vendors = []
            for vendor in result['vendors']:
                if vendor['name'] not in seen_names:
                    unique_vendors.append(vendor)
                    seen_names.add(vendor['name'])

            result['vendors'] = unique_vendors
            print(f"  Total unique vendors: {len(unique_vendors)}")

        except Exception as e:
            print(f"  Error: {str(e)}")
            result['error'] = str(e)

        result['processing_time'] = time.time() - start_time
        return result

    def process_all_markets(self, csv_file: str = "websites_only.csv", max_markets: int = None, start_from: int = 0):
        """Process all markets from CSV file"""
        print(f"=== COMPREHENSIVE VENDOR EXTRACTION ===")

        # Load markets
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

        # Apply filtering
        if start_from > 0:
            markets = markets[start_from:]
        if max_markets:
            markets = markets[:max_markets]

        print(f"Processing {len(markets)} markets (starting from index {start_from})")

        all_results = []
        total_vendors = 0
        successful_markets = 0

        start_time = time.time()

        for i, market in enumerate(markets, 1):
            result = self.process_market(market['name'], market['url'])
            all_results.append(result)

            if not result.get('error'):
                successful_markets += 1
                total_vendors += len(result['vendors'])

            # Save intermediate results every 10 markets
            if i % 10 == 0:
                intermediate_file = f"vendors_batch_{start_from + i - 9}_to_{start_from + i}.json"
                with open(intermediate_file, 'w') as f:
                    json.dump(all_results[-10:], f, indent=2)
                print(f"\n  Saved batch to {intermediate_file}")

        # Final summary
        total_time = time.time() - start_time

        print(f"\n=== EXTRACTION COMPLETE ===")
        print(f"Total time: {total_time/60:.1f} minutes")
        print(f"Markets processed: {len(markets)}")
        print(f"Successful: {successful_markets}")
        print(f"Success rate: {successful_markets/len(markets)*100:.1f}%")
        print(f"Total vendors found: {total_vendors}")

        if successful_markets > 0:
            print(f"Average vendors per successful market: {total_vendors/successful_markets:.1f}")

        # Save final results
        output_file = f"comprehensive_vendors_{start_from}_{start_from + len(markets)}.json"
        final_results = {
            'summary': {
                'total_markets': len(markets),
                'successful_markets': successful_markets,
                'total_vendors': total_vendors,
                'processing_time_minutes': total_time/60,
                'success_rate': successful_markets/len(markets),
                'start_index': start_from
            },
            'results': all_results
        }

        with open(output_file, 'w') as f:
            json.dump(final_results, f, indent=2)

        print(f"Final results saved to: {output_file}")

        return final_results

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Comprehensive vendor extraction from farmers markets")
    parser.add_argument('--csv-file', default='websites_only.csv', help='Input CSV file with market URLs')
    parser.add_argument('--max-markets', type=int, help='Maximum number of markets to process')
    parser.add_argument('--start-from', type=int, default=0, help='Start processing from this market index')
    parser.add_argument('--delay', type=float, default=1.5, help='Delay between requests (seconds)')

    args = parser.parse_args()

    extractor = ComprehensiveVendorExtractor(delay=args.delay)
    extractor.process_all_markets(
        csv_file=args.csv_file,
        max_markets=args.max_markets,
        start_from=args.start_from
    )

if __name__ == "__main__":
    main()