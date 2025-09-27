#!/usr/bin/env python3
"""
Stage 2: LLM-based vendor extraction from candidate pages
Uses Claude AI API to intelligently parse vendor information
"""

import json
import time
from dataclasses import dataclass
from typing import List, Optional, Dict
import anthropic
import os

@dataclass
class ExtractedVendor:
    name: str
    business_type: Optional[str]
    products: Optional[str]
    description: Optional[str]
    contact_info: Optional[Dict[str, str]]  # phone, email, website
    location: Optional[str]
    confidence: float

@dataclass
class VendorExtractionResult:
    market_name: str
    page_url: str
    vendors: List[ExtractedVendor]
    extraction_success: bool
    error_message: Optional[str]
    processing_time: float

class LLMVendorExtractor:
    def __init__(self, api_key: Optional[str] = None, model: str = "claude-3-haiku-20240307"):
        self.client = anthropic.Anthropic(api_key=api_key or os.getenv('ANTHROPIC_API_KEY'))
        self.model = model

        # System prompt for vendor extraction
        self.system_prompt = """You are a specialized data extraction assistant for farmers markets. Your job is to extract vendor/participant information from farmers market website content.

Given website content, identify and extract information about individual vendors, farmers, producers, or participants. For each vendor, extract:

1. Name (business/farm name)
2. Business type (farm, bakery, artisan, etc.)
3. Products (what they sell/produce)
4. Description (brief description of their business)
5. Contact info (phone, email, website if mentioned)
6. Location (if mentioned)

Return the results as a JSON array of vendor objects. Each vendor object should have these fields:
- "name": string (required)
- "business_type": string or null
- "products": string or null
- "description": string or null
- "contact_info": object with phone/email/website fields or null
- "location": string or null
- "confidence": number between 0-1 indicating your confidence in the extraction

Only include entries that you're reasonably confident are actual vendors/businesses, not general market information. If no clear vendor information is found, return an empty array.

Example output:
[
  {
    "name": "Smith Family Farm",
    "business_type": "farm",
    "products": "organic vegetables, seasonal fruits",
    "description": "Third generation family farm specializing in organic produce",
    "contact_info": {"phone": "555-1234", "email": "smith@farm.com"},
    "location": "Camden County",
    "confidence": 0.9
  }
]"""

    def extract_vendors_from_content(self, content: str, market_name: str, page_url: str) -> VendorExtractionResult:
        """Extract vendor information using LLM"""
        start_time = time.time()

        try:
            # Prepare the user message
            user_message = f"""Market: {market_name}
Website: {page_url}

Content to analyze:
{content}

Please extract vendor/participant information from this farmers market website content."""

            # Make API call
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                temperature=0.1,  # Low temperature for consistent extraction
                system=self.system_prompt,
                messages=[
                    {"role": "user", "content": user_message}
                ]
            )

            # Parse the response
            response_text = response.content[0].text.strip()

            # Extract JSON from response (handle cases where LLM adds extra text)
            json_start = response_text.find('[')
            json_end = response_text.rfind(']') + 1

            if json_start == -1 or json_end == 0:
                # No JSON found, try to find it in code blocks
                if '```json' in response_text:
                    json_start = response_text.find('```json') + 7
                    json_end = response_text.find('```', json_start)
                    json_content = response_text[json_start:json_end].strip()
                elif '```' in response_text:
                    json_start = response_text.find('```') + 3
                    json_end = response_text.find('```', json_start)
                    json_content = response_text[json_start:json_end].strip()
                else:
                    raise ValueError("No JSON array found in response")
            else:
                json_content = response_text[json_start:json_end]

            # Parse JSON
            vendor_data = json.loads(json_content)

            # Convert to ExtractedVendor objects
            vendors = []
            for vendor_dict in vendor_data:
                vendor = ExtractedVendor(
                    name=vendor_dict.get('name', ''),
                    business_type=vendor_dict.get('business_type'),
                    products=vendor_dict.get('products'),
                    description=vendor_dict.get('description'),
                    contact_info=vendor_dict.get('contact_info'),
                    location=vendor_dict.get('location'),
                    confidence=float(vendor_dict.get('confidence', 0.5))
                )
                vendors.append(vendor)

            processing_time = time.time() - start_time

            return VendorExtractionResult(
                market_name=market_name,
                page_url=page_url,
                vendors=vendors,
                extraction_success=True,
                error_message=None,
                processing_time=processing_time
            )

        except Exception as e:
            processing_time = time.time() - start_time

            return VendorExtractionResult(
                market_name=market_name,
                page_url=page_url,
                vendors=[],
                extraction_success=False,
                error_message=str(e),
                processing_time=processing_time
            )

    def extract_from_candidates(self, candidates_file: str, output_file: str, limit: Optional[int] = None) -> List[VendorExtractionResult]:
        """Extract vendors from all candidate pages"""

        with open(candidates_file, 'r', encoding='utf-8') as f:
            candidates = json.load(f)

        if limit:
            candidates = candidates[:limit]

        print(f"Processing {len(candidates)} candidate pages with LLM...")

        results = []

        for i, candidate in enumerate(candidates, 1):
            print(f"\nProcessing {i}/{len(candidates)}: {candidate['market_name']}")
            print(f"  URL: {candidate['page_url']}")
            print(f"  Detection score: {candidate['detection_score']:.2f}")

            result = self.extract_vendors_from_content(
                content=candidate['content_sample'],
                market_name=candidate['market_name'],
                page_url=candidate['page_url']
            )

            if result.extraction_success:
                print(f"  ✓ Extracted {len(result.vendors)} vendors in {result.processing_time:.1f}s")
                for vendor in result.vendors[:3]:  # Show first 3
                    print(f"    - {vendor.name} ({vendor.business_type or 'unknown'}) - {vendor.confidence:.2f}")
            else:
                print(f"  ✗ Extraction failed: {result.error_message}")

            results.append(result)

            # Save intermediate results
            self.save_results(results, output_file)

            # Rate limiting - be respectful to the API
            time.sleep(1)

        return results

    def save_results(self, results: List[VendorExtractionResult], output_file: str):
        """Save extraction results to JSON file"""
        data = []

        for result in results:
            vendors_data = []
            for vendor in result.vendors:
                vendors_data.append({
                    'name': vendor.name,
                    'business_type': vendor.business_type,
                    'products': vendor.products,
                    'description': vendor.description,
                    'contact_info': vendor.contact_info,
                    'location': vendor.location,
                    'confidence': vendor.confidence
                })

            data.append({
                'market_name': result.market_name,
                'page_url': result.page_url,
                'vendors': vendors_data,
                'extraction_success': result.extraction_success,
                'error_message': result.error_message,
                'processing_time': result.processing_time
            })

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def analyze_results(self, results: List[VendorExtractionResult]):
        """Analyze and print extraction statistics"""
        total_pages = len(results)
        successful_extractions = len([r for r in results if r.extraction_success])
        total_vendors = sum(len(r.vendors) for r in results)

        print(f"\n=== LLM EXTRACTION ANALYSIS ===")
        print(f"Total pages processed: {total_pages}")
        print(f"Successful extractions: {successful_extractions} ({successful_extractions/total_pages*100:.1f}%)")
        print(f"Total vendors extracted: {total_vendors}")

        if successful_extractions > 0:
            avg_vendors_per_page = total_vendors / successful_extractions
            print(f"Average vendors per successful page: {avg_vendors_per_page:.1f}")

            # Business type distribution
            business_types = {}
            confidence_scores = []

            for result in results:
                if result.extraction_success:
                    for vendor in result.vendors:
                        btype = vendor.business_type or 'unknown'
                        business_types[btype] = business_types.get(btype, 0) + 1
                        confidence_scores.append(vendor.confidence)

            if business_types:
                print(f"\nBusiness type distribution:")
                for btype, count in sorted(business_types.items(), key=lambda x: x[1], reverse=True):
                    print(f"  {btype}: {count}")

            if confidence_scores:
                avg_confidence = sum(confidence_scores) / len(confidence_scores)
                print(f"\nAverage confidence score: {avg_confidence:.2f}")

        # Show top results
        successful_results = [r for r in results if r.extraction_success and r.vendors]
        if successful_results:
            print(f"\nTop extraction results:")
            top_results = sorted(successful_results, key=lambda x: len(x.vendors), reverse=True)[:5]

            for result in top_results:
                print(f"  {result.market_name}: {len(result.vendors)} vendors")
                print(f"    URL: {result.page_url}")

if __name__ == "__main__":
    # Check for API key
    if not os.getenv('ANTHROPIC_API_KEY'):
        print("Error: ANTHROPIC_API_KEY environment variable not set")
        print("Please set your Anthropic API key: export ANTHROPIC_API_KEY=your_key_here")
        exit(1)

    extractor = LLMVendorExtractor(model="claude-3-haiku-20240307")

    # Process candidate pages
    results = extractor.extract_from_candidates(
        candidates_file="vendor_page_candidates.json",
        output_file="llm_extracted_vendors.json",
        limit=10  # Start with a small batch for testing
    )

    # Analyze results
    extractor.analyze_results(results)

    print(f"\nResults saved to: llm_extracted_vendors.json")