#!/usr/bin/env python3
"""
Stage 1: Enhanced crawler for finding vendor list pages
Focuses on detection rather than extraction
"""

import requests
import time
import json
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List, Optional, Dict, Set
from scraper import FarmersMarketScraper

@dataclass
class VendorPageCandidate:
    market_name: str
    page_url: str
    page_title: str
    content_sample: str
    detection_score: float
    detection_reasons: List[str]

class VendorPageFinder(FarmersMarketScraper):
    def __init__(self, delay=1.5, max_depth=2):
        super().__init__(delay)
        self.max_depth = max_depth
        self.visited_urls = set()

        # Enhanced keywords for vendor page detection
        self.vendor_link_keywords = {
            'high_priority': ['vendors', 'our vendors', 'vendor directory', 'farmers', 'our farmers', 'growers'],
            'medium_priority': ['participants', 'market vendors', 'who we are', 'meet the vendors', 'producers'],
            'low_priority': ['vendor list', 'vendor info', 'about vendors', 'artisans', 'businesses']
        }

        # Content indicators that suggest a vendor listing page
        self.content_indicators = {
            'vendor_names': ['farm', 'bakery', 'gardens', 'orchard', 'kitchen', 'creamery', 'ranch', 'dairy'],
            'business_indicators': ['llc', 'inc', 'co.', 'company', 'family owned', 'organic', 'certified'],
            'product_indicators': ['specializes in', 'produces', 'offers', 'sells', 'grows', 'bakes', 'makes'],
            'contact_indicators': ['phone:', 'email:', 'website:', 'contact:', '@', 'www.']
        }

    def score_link_for_vendors(self, link_text: str, link_url: str) -> tuple[float, List[str]]:
        """Score how likely a link leads to vendor information"""
        score = 0.0
        reasons = []

        link_text_lower = link_text.lower().strip()
        url_path = urlparse(link_url).path.lower()

        # High priority keywords
        for keyword in self.vendor_link_keywords['high_priority']:
            if keyword in link_text_lower:
                score += 0.4
                reasons.append(f"High priority keyword '{keyword}' in link text")
            if keyword in url_path:
                score += 0.3
                reasons.append(f"High priority keyword '{keyword}' in URL")

        # Medium priority keywords
        for keyword in self.vendor_link_keywords['medium_priority']:
            if keyword in link_text_lower:
                score += 0.2
                reasons.append(f"Medium priority keyword '{keyword}' in link text")
            if keyword in url_path:
                score += 0.15
                reasons.append(f"Medium priority keyword '{keyword}' in URL")

        # Low priority keywords
        for keyword in self.vendor_link_keywords['low_priority']:
            if keyword in link_text_lower:
                score += 0.1
                reasons.append(f"Low priority keyword '{keyword}' in link text")

        return min(score, 1.0), reasons

    def analyze_content_for_vendors(self, content: str) -> tuple[float, List[str]]:
        """Analyze page content to determine if it contains vendor listings"""
        score = 0.0
        reasons = []
        content_lower = content.lower()

        # Count different types of indicators
        vendor_name_count = 0
        business_count = 0
        product_count = 0
        contact_count = 0

        for indicator in self.content_indicators['vendor_names']:
            count = content_lower.count(indicator)
            vendor_name_count += count

        for indicator in self.content_indicators['business_indicators']:
            count = content_lower.count(indicator)
            business_count += count

        for indicator in self.content_indicators['product_indicators']:
            count = content_lower.count(indicator)
            product_count += count

        for indicator in self.content_indicators['contact_indicators']:
            count = content_lower.count(indicator)
            contact_count += count

        # Score based on indicator density
        if vendor_name_count >= 5:
            score += 0.3
            reasons.append(f"Many business type words found ({vendor_name_count})")
        elif vendor_name_count >= 2:
            score += 0.15
            reasons.append(f"Some business type words found ({vendor_name_count})")

        if business_count >= 3:
            score += 0.2
            reasons.append(f"Multiple business indicators ({business_count})")

        if product_count >= 3:
            score += 0.2
            reasons.append(f"Multiple product indicators ({product_count})")

        if contact_count >= 5:
            score += 0.25
            reasons.append(f"Many contact indicators ({contact_count})")
        elif contact_count >= 2:
            score += 0.1
            reasons.append(f"Some contact indicators ({contact_count})")

        # Bonus for structured content patterns
        # Look for repeated patterns that suggest a directory
        import re

        # Pattern: Name followed by dash/colon and description
        list_pattern = r'[A-Z][a-zA-Z\s&\'\.]{3,30}\s*[-–—:]\s*[^\.]{15,}'
        list_matches = len(re.findall(list_pattern, content))
        if list_matches >= 3:
            score += 0.3
            reasons.append(f"Structured list pattern detected ({list_matches} entries)")

        # Pattern: Phone numbers (suggests contact directory)
        phone_pattern = r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'
        phone_matches = len(re.findall(phone_pattern, content))
        if phone_matches >= 3:
            score += 0.2
            reasons.append(f"Multiple phone numbers found ({phone_matches})")

        # Pattern: Email addresses
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        email_matches = len(re.findall(email_pattern, content))
        if email_matches >= 2:
            score += 0.15
            reasons.append(f"Multiple email addresses found ({email_matches})")

        return min(score, 1.0), reasons

    def find_vendor_page_candidates(self, market_name: str, base_url: str) -> List[VendorPageCandidate]:
        """Find pages that likely contain vendor listings"""
        candidates = []
        to_visit = [(base_url, 0)]  # (url, depth)

        print(f"Searching for vendor pages: {market_name}")

        while to_visit and len(candidates) < 5:  # Limit candidates per site
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

                # Analyze content for vendor indicators
                content_score, content_reasons = self.analyze_content_for_vendors(content)

                # Only consider pages with reasonable vendor content
                if content_score >= 0.3:
                    title = soup.find('title')
                    title_text = title.get_text().strip() if title else "No title"

                    candidate = VendorPageCandidate(
                        market_name=market_name,
                        page_url=current_url,
                        page_title=title_text,
                        content_sample=content[:1000],  # First 1000 chars for LLM analysis
                        detection_score=content_score,
                        detection_reasons=content_reasons
                    )
                    candidates.append(candidate)
                    print(f"  Found candidate: {title_text} (score: {content_score:.2f})")

                # Find vendor-related links to explore (only from main page)
                if depth == 0:
                    vendor_links = []
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        link_text = link.get_text().strip()

                        # Skip empty or very long link text
                        if not link_text or len(link_text) > 100:
                            continue

                        # Convert to absolute URL
                        if href.startswith('/'):
                            full_url = urljoin(base_url, href)
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            continue

                        # Skip external domains and files
                        if (urlparse(full_url).netloc != urlparse(base_url).netloc or
                            any(full_url.lower().endswith(ext) for ext in ['.pdf', '.jpg', '.png', '.gif'])):
                            continue

                        link_score, link_reasons = self.score_link_for_vendors(link_text, full_url)
                        if link_score > 0.2:
                            vendor_links.append((full_url, link_text, link_score, link_reasons))

                    # Sort by score and add top links to visit queue
                    vendor_links.sort(key=lambda x: x[2], reverse=True)
                    for link_url, link_text, link_score, link_reasons in vendor_links[:3]:
                        if link_url not in self.visited_urls:
                            to_visit.append((link_url, depth + 1))
                            print(f"    Queuing: {link_text} (score: {link_score:.2f})")

                time.sleep(self.delay)

            except Exception as e:
                print(f"  Error accessing {current_url}: {e}")
                continue

        return candidates

    def find_candidates_from_results(self, results_file: str, limit: Optional[int] = None) -> List[VendorPageCandidate]:
        """Find vendor page candidates from successful scraping results"""

        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)

        # Filter to successful sites
        successful_sites = []
        for result in results:
            if (result['error'] is None and
                result['site_type'] in ['dedicated_market', 'municipal', 'other']):
                successful_sites.append(result)

        if limit:
            successful_sites = successful_sites[:limit]

        print(f"Searching {len(successful_sites)} sites for vendor page candidates...")

        all_candidates = []
        for site in successful_sites:
            self.visited_urls.clear()  # Reset for each site
            candidates = self.find_vendor_page_candidates(site['market_name'], site['url'])
            all_candidates.extend(candidates)
            time.sleep(self.delay * 2)  # Extra delay between sites

        return all_candidates

    def save_candidates(self, candidates: List[VendorPageCandidate], output_file: str):
        """Save candidate pages to JSON file"""
        data = []
        for candidate in candidates:
            data.append({
                'market_name': candidate.market_name,
                'page_url': candidate.page_url,
                'page_title': candidate.page_title,
                'content_sample': candidate.content_sample,
                'detection_score': candidate.detection_score,
                'detection_reasons': candidate.detection_reasons
            })

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    finder = VendorPageFinder(delay=1.5, max_depth=2)

    # Find candidate pages from existing scraping results
    candidates = finder.find_candidates_from_results("scraping_results_50.json", limit=15)

    # Save candidates
    finder.save_candidates(candidates, "vendor_page_candidates.json")

    # Print summary
    print(f"\n=== VENDOR PAGE DETECTION SUMMARY ===")
    print(f"Total candidate pages found: {len(candidates)}")

    if candidates:
        avg_score = sum(c.detection_score for c in candidates) / len(candidates)
        print(f"Average detection score: {avg_score:.2f}")

        print(f"\nTop candidates:")
        for candidate in sorted(candidates, key=lambda x: x.detection_score, reverse=True)[:5]:
            print(f"  {candidate.market_name}: {candidate.detection_score:.2f}")
            print(f"    {candidate.page_title}")
            print(f"    {candidate.page_url}")
            print(f"    Reasons: {', '.join(candidate.detection_reasons[:3])}")
            print()

    print(f"Results saved to: vendor_page_candidates.json")