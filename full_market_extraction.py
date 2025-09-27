#!/usr/bin/env python3
"""
Full market vendor extraction from websites_only.csv
Processes all 114 farmers market websites to extract vendor information
"""

import csv
import json
import time
import os
from typing import List, Dict
from integrated_vendor_extraction import IntegratedVendorExtraction

def load_market_urls(csv_file: str) -> List[Dict[str, str]]:
    """Load market URLs from CSV file"""
    markets = []
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader, 1):
            if row and not row[0].startswith('#'):  # Skip comments
                url = row[0].strip()
                if url:
                    # Extract market name from URL or use line number
                    market_name = url.replace('https://', '').replace('http://', '').replace('www.', '')
                    market_name = market_name.split('/')[0].replace('.com', '').replace('.org', '').replace('.net', '').replace('.us', '')

                    markets.append({
                        'id': i,
                        'name': market_name.title(),
                        'url': url if url.startswith(('http://', 'https://')) else f"https://{url}"
                    })
    return markets

def extract_vendors_from_all_markets(
    csv_file: str = "websites_only.csv",
    output_file: str = "all_vendors_extracted.json",
    api_key: str = None,
    batch_size: int = 10,
    delay_between_batches: float = 30.0
):
    """
    Extract vendors from all markets in batches
    """
    print("=== FULL MARKET VENDOR EXTRACTION ===")

    # Load markets
    markets = load_market_urls(csv_file)
    print(f"Loaded {len(markets)} markets from {csv_file}")

    # Initialize extraction pipeline
    pipeline = IntegratedVendorExtraction(
        anthropic_api_key=api_key,
        crawler_delay=2.0,  # Be respectful to servers
        llm_model="claude-3-haiku-20240307"
    )

    all_results = []
    total_vendors = 0
    successful_extractions = 0

    start_time = time.time()

    # Process markets in batches
    for batch_start in range(0, len(markets), batch_size):
        batch_end = min(batch_start + batch_size, len(markets))
        batch_markets = markets[batch_start:batch_end]

        print(f"\n--- Processing batch {batch_start//batch_size + 1}: markets {batch_start+1}-{batch_end} ---")

        batch_results = []

        for market in batch_markets:
            print(f"\nProcessing: {market['name']} ({market['url']})")

            try:
                # Test single market extraction
                result = pipeline.test_single_market(market['name'], market['url'])

                # If successful, get detailed extraction
                if result.get('success', False) and result.get('vendors', 0) > 0:
                    print(f"✓ Found {result['vendors']} vendors")
                    successful_extractions += 1
                    total_vendors += result['vendors']

                    # Store result
                    batch_results.append({
                        'market_id': market['id'],
                        'market_name': market['name'],
                        'market_url': market['url'],
                        'vendor_count': result['vendors'],
                        'extraction_success': True,
                        'timestamp': time.time()
                    })
                else:
                    print(f"✗ No vendors found")
                    batch_results.append({
                        'market_id': market['id'],
                        'market_name': market['name'],
                        'market_url': market['url'],
                        'vendor_count': 0,
                        'extraction_success': False,
                        'timestamp': time.time()
                    })

            except Exception as e:
                print(f"✗ Error processing {market['name']}: {str(e)}")
                batch_results.append({
                    'market_id': market['id'],
                    'market_name': market['name'],
                    'market_url': market['url'],
                    'vendor_count': 0,
                    'extraction_success': False,
                    'error': str(e),
                    'timestamp': time.time()
                })

            # Small delay between markets
            time.sleep(1.0)

        all_results.extend(batch_results)

        # Save intermediate results
        intermediate_file = f"batch_{batch_start//batch_size + 1}_results.json"
        with open(intermediate_file, 'w') as f:
            json.dump(batch_results, f, indent=2)

        print(f"Batch {batch_start//batch_size + 1} completed. Results saved to {intermediate_file}")

        # Delay between batches (except for last batch)
        if batch_end < len(markets):
            print(f"Waiting {delay_between_batches}s before next batch...")
            time.sleep(delay_between_batches)

    # Final summary
    total_time = time.time() - start_time

    print(f"\n=== EXTRACTION COMPLETE ===")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Markets processed: {len(markets)}")
    print(f"Successful extractions: {successful_extractions}")
    print(f"Success rate: {successful_extractions/len(markets)*100:.1f}%")
    print(f"Total vendors found: {total_vendors}")

    if successful_extractions > 0:
        print(f"Average vendors per successful market: {total_vendors/successful_extractions:.1f}")

    # Save final results
    final_results = {
        'summary': {
            'total_markets': len(markets),
            'successful_extractions': successful_extractions,
            'total_vendors': total_vendors,
            'processing_time_minutes': total_time/60,
            'success_rate': successful_extractions/len(markets)
        },
        'results': all_results
    }

    with open(output_file, 'w') as f:
        json.dump(final_results, f, indent=2)

    print(f"Final results saved to: {output_file}")

    return final_results

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract vendors from all farmers markets")
    parser.add_argument('--csv-file', default='websites_only.csv', help='Input CSV file with market URLs')
    parser.add_argument('--output', default='all_vendors_extracted.json', help='Output JSON file')
    parser.add_argument('--api-key', help='Anthropic API key (or use ANTHROPIC_API_KEY env var)')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of markets to process per batch')
    parser.add_argument('--batch-delay', type=float, default=30.0, help='Delay between batches (seconds)')

    args = parser.parse_args()

    # Check for API key
    api_key = args.api_key or os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print("Error: Anthropic API key required. Set ANTHROPIC_API_KEY env var or use --api-key")
        return

    extract_vendors_from_all_markets(
        csv_file=args.csv_file,
        output_file=args.output,
        api_key=api_key,
        batch_size=args.batch_size,
        delay_between_batches=args.batch_delay
    )

if __name__ == "__main__":
    main()