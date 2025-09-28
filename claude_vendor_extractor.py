#!/usr/bin/env python3
"""
Claude Vendor Extractor - Phase 2
Uses Claude API to extract structured vendor data from farmers market pages
"""

import json
import time
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict
import anthropic
import os

@dataclass
class Vendor:
    name: str
    business_type: Optional[str] = None
    products: Optional[List[str]] = None
    location: Optional[str] = None
    contact_info: Optional[Dict[str, str]] = None
    description: Optional[str] = None
    confidence: float = 0.8

@dataclass
class ExtractionResult:
    market_name: str
    market_url: str
    vendor_page_url: str
    vendors: List[Vendor]
    extraction_success: bool
    error_message: Optional[str] = None
    processing_time: float = 0.0

class ClaudeVendorExtractor:
    def __init__(self, api_key: Optional[str] = None, model: str = "claude-3-haiku-20240307"):
        self.api_key = api_key or os.environ.get('ANTHROPIC_API_KEY')
        if not self.api_key:
            raise ValueError("Anthropic API key required. Set ANTHROPIC_API_KEY env var or pass api_key parameter")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = model

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; VendorExtractor/1.0)'
        })

    def extract_vendors_from_page(self, market_name: str, page_url: str) -> ExtractionResult:
        """Extract vendor information from a single page using Claude"""
        print(f"\n🧠 Extracting vendors from: {market_name}")
        print(f"   Page: {page_url}")

        start_time = time.time()

        try:
            # Fetch page content
            page_content = self._fetch_page_content(page_url)
            if not page_content:
                return ExtractionResult(
                    market_name=market_name,
                    market_url="",
                    vendor_page_url=page_url,
                    vendors=[],
                    extraction_success=False,
                    error_message="Failed to fetch page content",
                    processing_time=time.time() - start_time
                )

            # Use Claude to extract vendors
            vendors = self._extract_with_claude(page_content, market_name, page_url)

            processing_time = time.time() - start_time
            print(f"   ✅ Extracted {len(vendors)} vendors in {processing_time:.1f}s")

            return ExtractionResult(
                market_name=market_name,
                market_url="",
                vendor_page_url=page_url,
                vendors=vendors,
                extraction_success=True,
                processing_time=processing_time
            )

        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Extraction failed: {error_msg}")

            return ExtractionResult(
                market_name=market_name,
                market_url="",
                vendor_page_url=page_url,
                vendors=[],
                extraction_success=False,
                error_message=error_msg,
                processing_time=time.time() - start_time
            )

    def _fetch_page_content(self, url: str) -> Optional[str]:
        """Fetch and clean page content"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract text from script tags first (for sites like Squarespace)
            script_content = ""
            for script in soup.find_all("script"):
                if script.string:
                    script_content += " " + script.string

            # Remove script and style elements
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()

            # Get visible text content
            text = soup.get_text()

            # Combine visible text with script content
            combined_text = text + " " + script_content

            # Clean up whitespace
            lines = (line.strip() for line in combined_text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            combined_text = ' '.join(chunk for chunk in chunks if chunk)

            # Limit content length for API efficiency
            if len(combined_text) > 8000:
                combined_text = combined_text[:8000] + "..."

            return combined_text

        except Exception as e:
            print(f"      Error fetching content: {str(e)}")
            return None

    def _extract_with_claude(self, content: str, market_name: str, page_url: str) -> List[Vendor]:
        """Use Claude API to extract vendor information"""

        prompt = f"""Extract information about farmers, vendors, and producers from this farmers market website content.

Market: {market_name}
URL: {page_url}

Website Content:
{content}

Extract a list of vendors/farms/producers with the following information for each:
- name (required)
- business_type (farm, bakery, dairy, etc.)
- products (list of products they sell)
- location (address, city, or general location if mentioned)
- contact_info (phone, email, website)
- description (brief description of what they do)

Focus on actual businesses/farms that sell at this market. Ignore:
- Market organizers or staff
- Generic references to "local farms"
- Event listings or schedules

Return ONLY a valid JSON array with no additional text. Each object should have these fields:
- name (string, required)
- business_type (string, optional)
- products (array of strings, optional)
- location (string, optional)
- contact_info (object with phone/email/website, optional)
- description (string, optional)
- confidence (number 0-1, your confidence in this extraction)

If no vendors are found, return: []"""

        try:
            message = self.client.messages.create(
                model=self.model,
                max_tokens=4000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )

            response_text = message.content[0].text

            # Parse JSON response - Claude should return pure JSON now
            try:
                vendors_data = json.loads(response_text)
                print(f"      ✅ Successfully parsed {len(vendors_data)} vendors")
            except json.JSONDecodeError as e:
                print(f"      ❌ JSON parsing failed: {str(e)}")
                print(f"      Response: {response_text[:200]}...")
                vendors_data = []

            # Convert to Vendor objects
            vendors = []
            for vendor_data in vendors_data:
                if isinstance(vendor_data, dict) and 'name' in vendor_data:
                    vendor = Vendor(
                        name=vendor_data.get('name', ''),
                        business_type=vendor_data.get('business_type'),
                        products=vendor_data.get('products'),
                        location=vendor_data.get('location'),
                        contact_info=vendor_data.get('contact_info'),
                        description=vendor_data.get('description'),
                        confidence=vendor_data.get('confidence', 0.8)
                    )
                    vendors.append(vendor)

            return vendors

        except Exception as e:
            print(f"      Claude API error: {str(e)}")
            return []


    def process_vendor_pages(self, vendor_pages_file: str) -> List[ExtractionResult]:
        """Process all vendor pages from discovery results"""
        print("🧠 Starting Claude-powered vendor extraction")

        # Load vendor page discovery results
        with open(vendor_pages_file, 'r') as f:
            vendor_pages = json.load(f)

        # Filter to only pages where vendor pages were found
        valid_pages = [page for page in vendor_pages if page.get('vendor_page_found', False)]

        print(f"📋 Processing {len(valid_pages)} vendor pages")

        results = []
        successful = 0

        for i, page_data in enumerate(valid_pages, 1):
            print(f"\n[{i}/{len(valid_pages)}] Processing: {page_data['market_name']}")

            result = self.extract_vendors_from_page(
                market_name=page_data['market_name'],
                page_url=page_data['vendor_page_url']
            )

            results.append(result)

            if result.extraction_success and result.vendors:
                successful += 1
                print(f"   Vendors found: {', '.join([v.name for v in result.vendors[:3]])}{'...' if len(result.vendors) > 3 else ''}")

            # Save intermediate results every 10 extractions
            if i % 10 == 0:
                self._save_results(results, f"claude_extraction_batch_{i-9}_to_{i}.json")

            # Rate limiting for API
            time.sleep(0.5)

        # Final summary
        print(f"\n✅ Claude extraction complete!")
        print(f"   Pages processed: {len(valid_pages)}")
        print(f"   Successful extractions: {successful}")
        print(f"   Total vendors found: {sum(len(r.vendors) for r in results)}")

        # Save final results
        output_file = "claude_vendor_extraction.json"
        self._save_results(results, output_file)
        print(f"   Results saved to: {output_file}")

        return results

    def _save_results(self, results: List[ExtractionResult], filename: str):
        """Save extraction results to JSON file"""
        # Convert dataclass objects to dictionaries
        serializable_results = []
        for result in results:
            result_dict = asdict(result)
            serializable_results.append(result_dict)

        with open(filename, 'w') as f:
            json.dump(serializable_results, f, indent=2)

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract vendors using Claude API")
    parser.add_argument('--vendor-pages', default='vendor_pages_discovery.json',
                       help='JSON file with vendor page discovery results')
    parser.add_argument('--api-key', help='Anthropic API key')
    parser.add_argument('--model', default='claude-3-haiku-20240307', help='Claude model to use')
    parser.add_argument('--test-url', help='Test extraction on a single URL')
    parser.add_argument('--test-name', help='Name for single URL test')

    args = parser.parse_args()

    try:
        extractor = ClaudeVendorExtractor(api_key=args.api_key, model=args.model)

        if args.test_url:
            # Test on single URL
            market_name = args.test_name or "Test Market"
            result = extractor.extract_vendors_from_page(market_name, args.test_url)

            if result.extraction_success:
                print(f"\n✅ Found {len(result.vendors)} vendors:")
                for vendor in result.vendors:
                    print(f"   - {vendor.name} ({vendor.business_type or 'Unknown type'})")
                    if vendor.products:
                        print(f"     Products: {', '.join(vendor.products[:3])}{'...' if len(vendor.products) > 3 else ''}")
            else:
                print(f"\n❌ Extraction failed: {result.error_message}")
        else:
            # Process all vendor pages
            extractor.process_vendor_pages(args.vendor_pages)

    except ValueError as e:
        print(f"❌ Error: {e}")
        print("Please set ANTHROPIC_API_KEY environment variable or use --api-key")

if __name__ == "__main__":
    main()