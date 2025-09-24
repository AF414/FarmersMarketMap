#!/usr/bin/env python3
"""
Simple web scraper for farmers market websites
"""

import requests
import csv
import time
import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import json

@dataclass
class ScrapedSite:
    market_name: str
    url: str
    status_code: Optional[int]
    content_length: Optional[int]
    title: Optional[str]
    text_content: str
    links: List[str]
    error: Optional[str]
    site_type: str

class FarmersMarketScraper:
    def __init__(self, delay=1.0):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def clean_url(self, url: str) -> Optional[str]:
        """Clean and validate URL"""
        if not url or url.strip() == '':
            return None

        url = url.strip()

        # Handle incomplete URLs
        if url.startswith('facebook.com') or url.startswith('www.'):
            url = 'https://' + url
        elif not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        # Skip obviously invalid URLs
        if url in ['https://or facebook.com', 'https://facebook.com']:
            return None

        return url

    def categorize_site(self, url: str) -> str:
        """Categorize website type based on URL"""
        if 'facebook.com' in url:
            return 'facebook'
        elif any(gov in url for gov in ['.gov', 'twp.', 'borough', 'city']):
            return 'municipal'
        elif 'chamber' in url:
            return 'chamber'
        elif 'farmersmarket' in url or 'market' in url:
            return 'dedicated_market'
        else:
            return 'other'

    def scrape_site(self, market_name: str, url: str) -> ScrapedSite:
        """Scrape a single website"""
        clean_url = self.clean_url(url)

        if not clean_url:
            return ScrapedSite(
                market_name=market_name,
                url=url,
                status_code=None,
                content_length=None,
                title=None,
                text_content="",
                links=[],
                error="Invalid URL",
                site_type="invalid"
            )

        site_type = self.categorize_site(clean_url)

        try:
            response = self.session.get(clean_url, timeout=10)

            if response.status_code != 200:
                return ScrapedSite(
                    market_name=market_name,
                    url=clean_url,
                    status_code=response.status_code,
                    content_length=None,
                    title=None,
                    text_content="",
                    links=[],
                    error=f"HTTP {response.status_code}",
                    site_type=site_type
                )

            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract title
            title = soup.find('title')
            title_text = title.get_text().strip() if title else None

            # Extract text content
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            text_content = soup.get_text()
            # Clean up whitespace
            lines = (line.strip() for line in text_content.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text_content = ' '.join(chunk for chunk in chunks if chunk)

            # Extract links
            links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('http'):
                    links.append(href)
                elif href.startswith('/'):
                    links.append(urljoin(clean_url, href))

            return ScrapedSite(
                market_name=market_name,
                url=clean_url,
                status_code=response.status_code,
                content_length=len(response.content),
                title=title_text,
                text_content=text_content[:5000],  # Limit text for initial analysis
                links=links[:20],  # Limit links
                error=None,
                site_type=site_type
            )

        except requests.exceptions.RequestException as e:
            return ScrapedSite(
                market_name=market_name,
                url=clean_url,
                status_code=None,
                content_length=None,
                title=None,
                text_content="",
                links=[],
                error=str(e),
                site_type=site_type
            )

    def scrape_from_csv(self, csv_file: str, limit: Optional[int] = None) -> List[ScrapedSite]:
        """Scrape websites from CSV file"""
        results = []

        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break

                market_name = row['Name']
                url = row['URL']

                print(f"Scraping {i+1}: {market_name} - {url}")

                result = self.scrape_site(market_name, url)
                results.append(result)

                # Be respectful with delays
                time.sleep(self.delay)

        return results

    def save_results(self, results: List[ScrapedSite], output_file: str):
        """Save results to JSON file"""
        data = []
        for result in results:
            data.append({
                'market_name': result.market_name,
                'url': result.url,
                'status_code': result.status_code,
                'content_length': result.content_length,
                'title': result.title,
                'text_content': result.text_content,
                'links': result.links,
                'error': result.error,
                'site_type': result.site_type
            })

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def analyze_results(self, results: List[ScrapedSite]):
        """Print analysis of scraping results"""
        total = len(results)
        successful = len([r for r in results if r.error is None])

        print(f"\n=== SCRAPING ANALYSIS ===")
        print(f"Total sites: {total}")
        print(f"Successful: {successful} ({successful/total*100:.1f}%)")
        print(f"Failed: {total-successful} ({(total-successful)/total*100:.1f}%)")

        # By site type
        site_types = {}
        for result in results:
            site_types[result.site_type] = site_types.get(result.site_type, 0) + 1

        print(f"\n=== BY SITE TYPE ===")
        for site_type, count in sorted(site_types.items()):
            successful_type = len([r for r in results if r.site_type == site_type and r.error is None])
            print(f"{site_type}: {count} total, {successful_type} successful ({successful_type/count*100:.1f}%)")

        # Common errors
        errors = {}
        for result in results:
            if result.error:
                error_type = result.error.split(':')[0] if ':' in result.error else result.error
                errors[error_type] = errors.get(error_type, 0) + 1

        if errors:
            print(f"\n=== COMMON ERRORS ===")
            for error, count in sorted(errors.items(), key=lambda x: x[1], reverse=True):
                print(f"{error}: {count}")

if __name__ == "__main__":
    scraper = FarmersMarketScraper(delay=1.5)

    # Start with a small sample to test
    print("Starting scraping of farmers market websites...")
    results = scraper.scrape_from_csv("Farmers Markets NJ.csv", limit=20)

    # Save results
    scraper.save_results(results, "scraping_results.json")

    # Analyze results
    scraper.analyze_results(results)

    print(f"\nResults saved to: scraping_results.json")