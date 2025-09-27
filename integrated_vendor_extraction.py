#!/usr/bin/env python3
"""
Integrated two-stage vendor extraction pipeline
Stage 1: Find vendor page candidates
Stage 2: Extract vendors using LLM
"""

import argparse
import json
import time
from typing import Optional, List
from vendor_page_finder import VendorPageFinder
from llm_vendor_extractor import LLMVendorExtractor

class IntegratedVendorExtraction:
    def __init__(self, anthropic_api_key: Optional[str] = None, crawler_delay: float = 1.5, llm_model: str = "claude-3-haiku-20240307"):
        self.finder = VendorPageFinder(delay=crawler_delay, max_depth=2)
        self.extractor = LLMVendorExtractor(api_key=anthropic_api_key, model=llm_model)

    def extract_vendors_from_markets(
        self,
        scraping_results_file: str,
        output_file: str,
        max_sites: Optional[int] = None,
        max_candidates_per_site: int = 3
    ) -> dict:
        """
        Complete pipeline: find vendor pages and extract vendor data
        """
        print("=== INTEGRATED VENDOR EXTRACTION PIPELINE ===")
        print(f"Input file: {scraping_results_file}")
        print(f"Output file: {output_file}")
        print(f"Max sites to process: {max_sites or 'all'}")

        # Stage 1: Find vendor page candidates
        print(f"\n--- Stage 1: Finding vendor page candidates ---")
        start_time = time.time()

        candidates = self.finder.find_candidates_from_results(
            scraping_results_file,
            limit=max_sites
        )

        stage1_time = time.time() - start_time
        print(f"Stage 1 completed in {stage1_time:.1f}s")
        print(f"Found {len(candidates)} candidate pages")

        if not candidates:
            print("No vendor page candidates found. Exiting.")
            return {
                'stage1_candidates': 0,
                'stage2_extractions': 0,
                'total_vendors': 0,
                'processing_time': stage1_time
            }

        # Save intermediate candidates file
        candidates_file = "temp_candidates.json"
        self.finder.save_candidates(candidates, candidates_file)

        # Stage 2: Extract vendors using LLM
        print(f"\n--- Stage 2: Extracting vendors with LLM ---")
        stage2_start = time.time()

        extraction_results = self.extractor.extract_from_candidates(
            candidates_file,
            output_file
        )

        stage2_time = time.time() - stage2_start
        total_time = time.time() - start_time

        # Clean up temp file
        import os
        if os.path.exists(candidates_file):
            os.remove(candidates_file)

        # Summary statistics
        successful_extractions = len([r for r in extraction_results if r.extraction_success])
        total_vendors = sum(len(r.vendors) for r in extraction_results)

        print(f"\n=== PIPELINE SUMMARY ===")
        print(f"Stage 1 time: {stage1_time:.1f}s")
        print(f"Stage 2 time: {stage2_time:.1f}s")
        print(f"Total time: {total_time:.1f}s")
        print(f"Candidate pages found: {len(candidates)}")
        print(f"Successful extractions: {successful_extractions}/{len(extraction_results)}")
        print(f"Total vendors extracted: {total_vendors}")

        if successful_extractions > 0:
            avg_vendors = total_vendors / successful_extractions
            print(f"Average vendors per successful extraction: {avg_vendors:.1f}")

        return {
            'stage1_candidates': len(candidates),
            'stage2_extractions': successful_extractions,
            'total_extractions': len(extraction_results),
            'total_vendors': total_vendors,
            'stage1_time': stage1_time,
            'stage2_time': stage2_time,
            'total_time': total_time
        }

    def test_single_market(self, market_name: str, url: str) -> dict:
        """
        Test the pipeline on a single market for debugging
        """
        print(f"=== TESTING SINGLE MARKET: {market_name} ===")
        print(f"URL: {url}")

        # Stage 1: Find candidates for this single market
        print(f"\n--- Finding vendor pages ---")
        self.finder.visited_urls.clear()
        candidates = self.finder.find_vendor_page_candidates(market_name, url)

        print(f"Found {len(candidates)} candidates")
        for candidate in candidates:
            print(f"  - {candidate.page_title} (score: {candidate.detection_score:.2f})")
            print(f"    {candidate.page_url}")

        if not candidates:
            return {'candidates': 0, 'vendors': 0}

        # Stage 2: Extract vendors from best candidate
        best_candidate = max(candidates, key=lambda c: c.detection_score)
        print(f"\n--- Extracting vendors from best candidate ---")
        print(f"Processing: {best_candidate.page_title}")

        result = self.extractor.extract_vendors_from_content(
            content=best_candidate.content_sample,
            market_name=market_name,
            page_url=best_candidate.page_url
        )

        if result.extraction_success:
            print(f"✓ Extracted {len(result.vendors)} vendors:")
            for vendor in result.vendors:
                print(f"  - {vendor.name} ({vendor.business_type or 'unknown'}) - confidence: {vendor.confidence:.2f}")
                if vendor.products:
                    print(f"    Products: {vendor.products}")
        else:
            print(f"✗ Extraction failed: {result.error_message}")

        return {
            'candidates': len(candidates),
            'vendors': len(result.vendors) if result.extraction_success else 0,
            'success': result.extraction_success
        }

def main():
    parser = argparse.ArgumentParser(description="Integrated vendor extraction from farmers market websites")
    parser.add_argument('command', choices=['extract', 'test'], help="Command to run")
    parser.add_argument('--input', required=True, help="Input file (scraping results JSON for extract, URL for test)")
    parser.add_argument('--output', help="Output file for extraction results")
    parser.add_argument('--max-sites', type=int, help="Maximum number of sites to process")
    parser.add_argument('--market-name', help="Market name for test mode")
    parser.add_argument('--api-key', help="Anthropic API key (or use ANTHROPIC_API_KEY env var)")
    parser.add_argument('--model', default="claude-3-haiku-20240307", help="LLM model to use")

    args = parser.parse_args()

    # Initialize the pipeline
    pipeline = IntegratedVendorExtraction(
        anthropic_api_key=args.api_key,
        llm_model=args.model
    )

    if args.command == 'extract':
        if not args.output:
            print("Error: --output required for extract command")
            return

        results = pipeline.extract_vendors_from_markets(
            scraping_results_file=args.input,
            output_file=args.output,
            max_sites=args.max_sites
        )

        print(f"\nResults saved to: {args.output}")

    elif args.command == 'test':
        if not args.market_name:
            print("Error: --market-name required for test command")
            return

        results = pipeline.test_single_market(
            market_name=args.market_name,
            url=args.input
        )

        print(f"\nTest completed: {results['candidates']} candidates, {results['vendors']} vendors")

if __name__ == "__main__":
    main()