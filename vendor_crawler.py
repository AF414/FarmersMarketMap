#!/usr/bin/env python3
"""
Enhanced crawler for finding and scraping vendor sub-pages
"""

import requests
import time
import re
import json
from urllib.parse import urlparse, urljoin, urlunparse
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Optional, Dict, Set
from scraper import FarmersMarketScraper

@dataclass
class VendorPage:
    market_name: str
    page_url: str
    page_title: str
    vendors: List[Dict]
    content_snippet: str
    confidence_score: float

class VendorPageCrawler(FarmersMarketScraper):
    def __init__(self, delay=1.5, max_depth=2):
        super().__init__(delay)
        self.max_depth = max_depth
        self.visited_urls = set()

        # Keywords that indicate vendor-related pages
        self.vendor_link_keywords = [
            'vendor', 'vendors', 'farmer', 'farmers', 'participant', 'participants',
            'market-vendors', 'our-vendors', 'vendor-list', 'vendor-directory',
            'who-we-are', 'about-vendors', 'meet', 'producers', 'growers',
            'artisans', 'businesses', 'stalls', 'booths'
        ]

        # Keywords that indicate high-value vendor content
        self.vendor_content_indicators = [
            'specializes in', 'produces', 'offers', 'family farm', 'organic',
            'fresh', 'locally grown', 'artisan', 'handmade', 'bakery',
            'contact', 'website', 'phone', 'email', '@', 'www.'
        ]

    def is_vendor_related_link(self, link_text: str, link_url: str) -> float:
        """Score how likely a link is to lead to vendor information (0-1)"""
        score = 0.0

        # Check link text
        link_text_lower = link_text.lower().strip()
        for keyword in self.vendor_link_keywords:
            if keyword in link_text_lower:
                if keyword in ['vendor', 'vendors', 'farmer', 'farmers']:
                    score += 0.4
                else:
                    score += 0.2

        # Check URL path
        url_path = urlparse(link_url).path.lower()
        for keyword in self.vendor_link_keywords:
            if keyword in url_path:
                score += 0.3

        # Bonus for exact matches
        if link_text_lower in ['vendors', 'our vendors', 'farmers', 'vendor directory']:
            score += 0.3

        return min(score, 1.0)

    def find_vendor_links(self, soup: BeautifulSoup, base_url: str) -> List[tuple]:
        """Find links that likely lead to vendor information"""
        vendor_links = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text().strip()

            # Skip empty links or non-relevant ones
            if not link_text or len(link_text) > 100:
                continue

            # Convert relative URLs to absolute
            if href.startswith('/'):
                full_url = urljoin(base_url, href)
            elif href.startswith('http'):
                full_url = href
            else:
                continue

            # Skip external domains, PDFs, images
            if (urlparse(full_url).netloc != urlparse(base_url).netloc or
                any(full_url.lower().endswith(ext) for ext in ['.pdf', '.jpg', '.png', '.gif'])):
                continue

            score = self.is_vendor_related_link(link_text, full_url)
            if score > 0.2:  # Only consider links with reasonable probability
                vendor_links.append((full_url, link_text, score))

        # Sort by score (highest first)
        vendor_links.sort(key=lambda x: x[2], reverse=True)
        return vendor_links[:5]  # Top 5 most promising links

    def extract_vendors_from_content(self, content: str, url: str) -> List[Dict]:
        """Extract vendor information from page content"""
        vendors = []

        # Pattern 1: Business names with type indicators
        business_patterns = [
            r'\b([A-Z][a-zA-Z\s&\'\.]{2,30})\s+(Farm|Bakery|Gardens?|Orchards?|Market|Kitchen|Creamery|Dairy|Ranch)\b',
            r'\b([A-Z][a-zA-Z\s&\'\.]{2,30})\s+(LLC|Inc\.?|Co\.?)\b',
        ]

        for pattern in business_patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                business_name = f"{match[0].strip()} {match[1].strip()}"
                if len(business_name) > 5 and business_name not in [v['name'] for v in vendors]:
                    vendors.append({
                        'name': business_name,
                        'type': match[1].lower(),
                        'source': 'pattern_match',
                        'url': url
                    })

        # Pattern 2: Look for structured vendor lists
        # Often vendors are listed with descriptions like "Farm Name - Description"
        list_patterns = [
            r'([A-Z][a-zA-Z\s&\'\.]{3,25})\s*[-–—]\s*([^\.]{10,100})',
            r'([A-Z][a-zA-Z\s&\'\.]{3,25}):\s*([^\.]{10,100})',
        ]

        for pattern in list_patterns:
            matches = re.findall(pattern, content, re.MULTILINE)
            for match in matches:
                name = match[0].strip()
                description = match[1].strip()

                # Filter out obviously non-vendor content
                if (len(name) > 3 and not any(skip in name.lower() for skip in
                    ['market', 'location', 'contact', 'about', 'info', 'news']) and
                    name not in [v['name'] for v in vendors]):

                    # Determine type from description
                    vendor_type = 'unknown'
                    if any(word in description.lower() for word in ['farm', 'grow', 'produce', 'vegetable']):
                        vendor_type = 'farm'
                    elif any(word in description.lower() for word in ['bake', 'bread', 'pastry']):
                        vendor_type = 'bakery'
                    elif any(word in description.lower() for word in ['craft', 'handmade', 'artisan']):
                        vendor_type = 'artisan'

                    vendors.append({
                        'name': name,
                        'type': vendor_type,
                        'description': description[:200],
                        'source': 'list_match',
                        'url': url
                    })

        return vendors[:20]  # Limit to prevent noise

    def calculate_vendor_page_confidence(self, content: str, vendors: List[Dict]) -> float:
        """Calculate confidence that this page contains vendor information"""
        score = 0.0

        # Base score from number of vendors found
        score += min(len(vendors) * 0.1, 0.5)

        # Score from vendor-related keywords in content
        content_lower = content.lower()
        keyword_count = 0
        for indicator in self.vendor_content_indicators:
            keyword_count += content_lower.count(indicator)
        score += min(keyword_count * 0.02, 0.3)

        # Bonus for structured content (likely a real vendor directory)
        if len(vendors) >= 5:
            score += 0.2

        return min(score, 1.0)

    def crawl_for_vendors(self, market_name: str, base_url: str) -> List[VendorPage]:
        """Crawl a market website to find vendor information"""
        vendor_pages = []
        to_visit = [(base_url, 0)]  # (url, depth)

        print(f"Crawling {market_name}: {base_url}")

        while to_visit and len(vendor_pages) < 10:  # Limit results
            current_url, depth = to_visit.pop(0)

            if (current_url in self.visited_urls or
                depth >= self.max_depth):
                continue

            self.visited_urls.add(current_url)

            try:
                response = self.session.get(current_url, timeout=10)
                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')

                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()

                content = soup.get_text()
                # Clean up whitespace
                lines = (line.strip() for line in content.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                content = ' '.join(chunk for chunk in chunks if chunk)

                # Extract vendors from current page
                vendors = self.extract_vendors_from_content(content, current_url)

                if vendors:
                    confidence = self.calculate_vendor_page_confidence(content, vendors)

                    if confidence > 0.3:  # Only keep high-confidence pages
                        title = soup.find('title')
                        title_text = title.get_text().strip() if title else "No title"

                        vendor_page = VendorPage(
                            market_name=market_name,
                            page_url=current_url,
                            page_title=title_text,
                            vendors=vendors,
                            content_snippet=content[:500],
                            confidence_score=confidence
                        )
                        vendor_pages.append(vendor_page)
                        print(f"  Found vendor page: {title_text} ({len(vendors)} vendors, {confidence:.2f} confidence)")

                # Find more vendor links to explore (only from base page)
                if depth == 0:
                    vendor_links = self.find_vendor_links(soup, base_url)
                    for link_url, link_text, link_score in vendor_links:
                        if link_url not in self.visited_urls:
                            to_visit.append((link_url, depth + 1))
                            print(f"    Queuing: {link_text} ({link_score:.2f})")

                time.sleep(self.delay)

            except Exception as e:
                print(f"  Error crawling {current_url}: {e}")
                continue

        return vendor_pages

    def crawl_from_successful_sites(self, results_file: str, limit: Optional[int] = None) -> List[VendorPage]:
        """Crawl vendor pages from previously successful scraping results"""

        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)

        # Filter to successful sites with high vendor potential
        promising_sites = []
        for result in results:
            if (result['error'] is None and
                result['site_type'] in ['dedicated_market', 'municipal', 'other'] and
                'vendor' in result['text_content'].lower()):
                promising_sites.append(result)

        if limit:
            promising_sites = promising_sites[:limit]

        print(f"Crawling {len(promising_sites)} promising sites for vendor pages...")

        all_vendor_pages = []
        for site in promising_sites:
            self.visited_urls.clear()  # Reset for each site
            vendor_pages = self.crawl_for_vendors(site['market_name'], site['url'])
            all_vendor_pages.extend(vendor_pages)
            time.sleep(self.delay * 2)  # Extra delay between sites

        return all_vendor_pages

    def save_vendor_pages(self, vendor_pages: List[VendorPage], output_file: str):
        """Save vendor pages to JSON file"""
        data = []
        for page in vendor_pages:
            data.append({
                'market_name': page.market_name,
                'page_url': page.page_url,
                'page_title': page.page_title,
                'vendors': page.vendors,
                'content_snippet': page.content_snippet,
                'confidence_score': page.confidence_score
            })

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    crawler = VendorPageCrawler(delay=1.5, max_depth=2)

    # Test crawl on our most promising sites
    vendor_pages = crawler.crawl_from_successful_sites("scraping_results_50.json", limit=10)

    # Save results
    crawler.save_vendor_pages(vendor_pages, "vendor_pages.json")

    # Print summary
    print(f"\n=== VENDOR CRAWLING SUMMARY ===")
    print(f"Total vendor pages found: {len(vendor_pages)}")

    total_vendors = sum(len(page.vendors) for page in vendor_pages)
    print(f"Total vendors extracted: {total_vendors}")

    if vendor_pages:
        avg_confidence = sum(page.confidence_score for page in vendor_pages) / len(vendor_pages)
        print(f"Average confidence: {avg_confidence:.2f}")

        print(f"\nTop vendor pages:")
        for page in sorted(vendor_pages, key=lambda x: x.confidence_score, reverse=True)[:5]:
            print(f"  {page.market_name}: {len(page.vendors)} vendors ({page.confidence_score:.2f})")
            print(f"    {page.page_url}")

    print(f"\nResults saved to: vendor_pages.json")