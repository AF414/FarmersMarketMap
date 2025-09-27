#!/usr/bin/env python3
"""
Network Analysis Data Builder
Creates structured data for analyzing farmer-market relationships and travel distances
"""

import json
import csv
import re
from typing import Dict, List, Set
from dataclasses import dataclass
import requests
import time

@dataclass
class Farm:
    name: str
    location: str
    products: List[str]
    contact_info: Dict
    markets_attended: List[str]
    confidence_score: float

@dataclass
class Market:
    name: str
    location: str
    address: str
    schedule: str
    url: str
    vendors_count: int

@dataclass
class FarmMarketRelationship:
    farm_name: str
    farm_location: str
    market_name: str
    market_location: str
    distance_miles: float
    products_sold: List[str]

class NetworkAnalysisBuilder:
    def __init__(self):
        self.farms = {}  # farm_name -> Farm object
        self.markets = {}  # market_name -> Market object
        self.relationships = []  # List of FarmMarketRelationship objects

    def load_market_data(self, csv_file: str = "Farmers Markets NJ.csv"):
        """Load farmers market data from the main CSV file"""
        print("Loading market data from CSV...")

        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                market_name = row['Name'].strip()
                if market_name and market_name != 'Name':

                    # Clean up market name (remove location suffixes)
                    clean_name = market_name.split('–')[0].strip()
                    if clean_name.endswith(' COUNTY'):
                        clean_name = clean_name.replace(' COUNTY', '').strip()

                    self.markets[clean_name] = Market(
                        name=clean_name,
                        location=row.get('Address/Location', '').strip(),
                        address=row.get('Address/Location', '').strip(),
                        schedule=row.get('Day/Hours', '').strip(),
                        url=row.get('URL', '').strip(),
                        vendors_count=0  # Will be updated when processing vendor data
                    )

        print(f"Loaded {len(self.markets)} markets")

    def load_vendor_data(self, vendor_files: List[str]):
        """Load vendor data from comprehensive extraction results"""
        print("Loading vendor data...")

        total_vendors = 0

        for file_path in vendor_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                # Handle both summary format and direct results format
                if 'results' in data:
                    results = data['results']
                else:
                    results = data

                for market_result in results:
                    market_name = self._normalize_market_name(market_result.get('market_name', ''))
                    vendors = market_result.get('vendors', [])

                    # Update market vendor count
                    if market_name in self.markets:
                        self.markets[market_name].vendors_count = len(vendors)

                    for vendor in vendors:
                        farm_name = self._clean_farm_name(vendor.get('name', ''))
                        if not farm_name or len(farm_name) < 3:
                            continue

                        # Create or update farm entry
                        if farm_name not in self.farms:
                            self.farms[farm_name] = Farm(
                                name=farm_name,
                                location=vendor.get('location', '') or '',
                                products=vendor.get('products', []) or [],
                                contact_info=vendor.get('contact_info', {}) or {},
                                markets_attended=[],
                                confidence_score=vendor.get('confidence', 0.8)
                            )

                        # Add market to farm's attendance list
                        if market_name and market_name not in self.farms[farm_name].markets_attended:
                            self.farms[farm_name].markets_attended.append(market_name)

                        total_vendors += 1

            except Exception as e:
                print(f"Error loading {file_path}: {str(e)}")

        print(f"Loaded {len(self.farms)} unique farms from {total_vendors} vendor records")

    def _normalize_market_name(self, market_name: str) -> str:
        """Normalize market names to match between vendor data and market CSV"""
        if not market_name:
            return ""

        # Remove common suffixes and clean up
        name = market_name.replace('.com', '').replace('.org', '').replace('.net', '')
        name = name.replace('farmersmarket', '').replace('farmers-market', '')
        name = name.replace('market', '').strip()

        # Convert domain-style names to location names
        domain_to_location = {
            'asburyfresh': 'ASBURY PARK',
            'barnegat': 'BARNEGAT',
            'bedminster': 'BEDMINSTER',
            'berlinfarmers': 'BERLIN',
            'bernardsvillefarmers': 'BERNARDSVILLE',
            'blairstownfarmers': 'BLAIRSTOWN',
            'bloomfieldtwpnj': 'BLOOMFIELD',
            'boontonmainstreet': 'BOONTON',
            'brickfarmers': 'BRICK',
            'collingswoodmarket': 'COLLINGSWOOD',
            'columbusfarmers': 'COLUMBUS',
            'ramseyfarmers': 'RAMSEY',
            'vcfm': 'VENTNOR CITY'
        }

        for domain, location in domain_to_location.items():
            if domain in name.lower():
                return location

        return name.upper()

    def _clean_farm_name(self, name: str) -> str:
        """Clean and validate farm names"""
        if not name:
            return ""

        # Remove newlines and extra whitespace
        name = re.sub(r'\s+', ' ', name.replace('\n', ' ')).strip()

        # Filter out obviously bad names
        bad_patterns = [
            r'^[0-9]+\s*$',  # Just numbers
            r'^[A-Z]\s*$',   # Single letters
            r'^\s*$',        # Empty or whitespace
            r'^(and|the|of|at|in|on|to|for|with|by)\s+',  # Starting with common words
            r'farm\s*fresh',  # Generic terms
            r'local\s*farm',
            r'farmers?\s*market'
        ]

        for pattern in bad_patterns:
            if re.match(pattern, name, re.IGNORECASE):
                return ""

        # Must contain at least one word that looks like a farm name
        if not re.search(r'[A-Z][a-z]+', name):
            return ""

        return name

    def build_relationships(self):
        """Build farm-market relationships with distance calculations"""
        print("Building farm-market relationships...")

        for farm_name, farm in self.farms.items():
            for market_name in farm.markets_attended:
                if market_name in self.markets:
                    market = self.markets[market_name]

                    # Calculate distance (placeholder for now)
                    distance = self._calculate_distance(farm.location, market.location)

                    relationship = FarmMarketRelationship(
                        farm_name=farm_name,
                        farm_location=farm.location,
                        market_name=market_name,
                        market_location=market.location,
                        distance_miles=distance,
                        products_sold=farm.products
                    )

                    self.relationships.append(relationship)

        print(f"Built {len(self.relationships)} farm-market relationships")

    def _calculate_distance(self, farm_location: str, market_location: str) -> float:
        """Calculate distance between farm and market (placeholder implementation)"""
        # For now, return a placeholder distance
        # In a full implementation, you would use a geocoding service
        if not farm_location or not market_location:
            return 0.0

        # Simple heuristic based on location string similarity
        # Replace this with actual geocoding + distance calculation
        return 25.0  # Placeholder: average distance in NJ

    def generate_network_analysis_data(self, output_file: str = "network_analysis_data.json"):
        """Generate the final network analysis dataset"""
        print("Generating network analysis dataset...")

        # Create farm summaries with travel calculations
        farm_summaries = []

        for farm_name, farm in self.farms.items():
            if not farm.markets_attended:
                continue

            farm_relationships = [r for r in self.relationships if r.farm_name == farm_name]
            total_distance = sum(r.distance_miles for r in farm_relationships)

            farm_summary = {
                'farm_name': farm_name,
                'farm_location': farm.location,
                'products': farm.products,
                'contact_info': farm.contact_info,
                'markets_attended': len(farm.markets_attended),
                'market_list': farm.markets_attended,
                'total_miles_per_season': total_distance * 20,  # Assuming 20 market days per season
                'average_distance_per_market': total_distance / len(farm.markets_attended) if farm.markets_attended else 0,
                'confidence_score': farm.confidence_score
            }

            farm_summaries.append(farm_summary)

        # Create market summaries
        market_summaries = []
        for market_name, market in self.markets.items():
            market_vendors = [f for f in self.farms.values() if market_name in f.markets_attended]

            market_summary = {
                'market_name': market_name,
                'location': market.location,
                'address': market.address,
                'schedule': market.schedule,
                'url': market.url,
                'vendor_count': len(market_vendors),
                'vendor_list': [v.name for v in market_vendors]
            }

            market_summaries.append(market_summary)

        # Create network statistics
        network_stats = {
            'total_farms': len(self.farms),
            'total_markets': len(self.markets),
            'total_relationships': len(self.relationships),
            'average_markets_per_farm': sum(len(f.markets_attended) for f in self.farms.values()) / len(self.farms) if self.farms else 0,
            'average_vendors_per_market': sum(m.vendors_count for m in self.markets.values()) / len(self.markets) if self.markets else 0,
            'estimated_total_miles_per_season': sum(f.get('total_miles_per_season', 0) for f in farm_summaries)
        }

        # Final dataset
        network_data = {
            'metadata': {
                'generated_timestamp': time.time(),
                'description': 'New Jersey Farmers Market Network Analysis Data',
                'data_sources': ['Farmers Markets NJ.csv', 'Comprehensive vendor extraction'],
                'network_statistics': network_stats
            },
            'farms': farm_summaries,
            'markets': market_summaries,
            'relationships': [
                {
                    'farm_name': r.farm_name,
                    'market_name': r.market_name,
                    'distance_miles': r.distance_miles,
                    'products': r.products_sold
                } for r in self.relationships
            ]
        }

        # Save to file
        with open(output_file, 'w') as f:
            json.dump(network_data, f, indent=2)

        print(f"Network analysis data saved to: {output_file}")
        print(f"Dataset includes:")
        print(f"  - {network_stats['total_farms']} farms")
        print(f"  - {network_stats['total_markets']} markets")
        print(f"  - {network_stats['total_relationships']} farm-market relationships")
        print(f"  - Estimated {network_stats['estimated_total_miles_per_season']:,.0f} total miles driven per season")

        return network_data

def main():
    import argparse
    import glob

    parser = argparse.ArgumentParser(description="Build network analysis data from extracted vendor information")
    parser.add_argument('--market-csv', default='Farmers Markets NJ.csv', help='Market data CSV file')
    parser.add_argument('--vendor-files', nargs='+', help='Vendor extraction JSON files')
    parser.add_argument('--output', default='network_analysis_data.json', help='Output file')

    args = parser.parse_args()

    # Auto-discover vendor files if not specified
    if not args.vendor_files:
        vendor_files = glob.glob("comprehensive_vendors_*.json") + glob.glob("vendors_batch_*.json")
        if not vendor_files:
            print("No vendor files found. Run the comprehensive vendor extractor first.")
            return
        args.vendor_files = vendor_files

    print(f"Using vendor files: {args.vendor_files}")

    # Build network analysis
    builder = NetworkAnalysisBuilder()
    builder.load_market_data(args.market_csv)
    builder.load_vendor_data(args.vendor_files)
    builder.build_relationships()

    # Generate final dataset
    network_data = builder.generate_network_analysis_data(args.output)

    return network_data

if __name__ == "__main__":
    main()