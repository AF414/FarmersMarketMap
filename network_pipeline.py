#!/usr/bin/env python3
"""
Complete Farmers Market Network Analysis Pipeline
Combines vendor discovery, Claude extraction, and location analysis
"""

import json
import csv
import math
import time
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
from vendor_page_finder import VendorPageFinder
from claude_vendor_extractor import ClaudeVendorExtractor

@dataclass
class Farm:
    name: str
    business_type: Optional[str]
    products: List[str]
    location: Optional[str]
    contact_info: Dict[str, str]
    markets_attended: List[str]
    confidence: float

@dataclass
class Market:
    name: str
    original_name: str
    url: str
    location: Optional[str]
    coordinates: Optional[Tuple[float, float]]
    vendor_count: int

@dataclass
class NetworkRelationship:
    farm_name: str
    market_name: str
    distance_miles: Optional[float]
    products_sold: List[str]

class NetworkPipeline:
    def __init__(self, anthropic_api_key: Optional[str] = None):
        self.anthropic_api_key = anthropic_api_key

        # NJ location coordinates for distance calculations
        self.nj_coordinates = {
            'ATLANTIC COUNTY': (39.3643, -74.4229),
            'BRIGANTINE': (39.4101, -74.3648),
            'GALLOWAY': (39.4665, -74.4704),
            'MARGATE': (39.3245, -74.5049),
            'SOMERS POINT': (39.3179, -74.5898),
            'VENTNOR CITY': (39.3398, -74.4743),
            'BERGEN COUNTY': (40.9268, -74.0752),
            'RAMSEY': (41.0576, -74.1410),
            'RIDGEWOOD': (40.9787, -74.1165),
            'RIVER EDGE': (40.9287, -74.0393),
            'RUTHERFORD': (40.8265, -74.1071),
            'BURLINGTON COUNTY': (39.9187, -74.6094),
            'COLUMBUS': (40.0448, -74.6932),
            'MOUNT LAUREL': (39.9548, -74.8919),
            'CAMDEN COUNTY': (39.8154, -75.0059),
            'COLLINGSWOOD': (39.9181, -75.0718),
            'HADDONFIELD': (39.8912, -75.0376),
            'HADDON HEIGHTS': (39.8773, -75.0607),
            'ESSEX COUNTY': (40.7834, -74.2291),
            'BLOOMFIELD': (40.8068, -74.1854),
            'MAPLEWOOD': (40.7312, -74.2732),
            'MONTCLAIR': (40.8176, -74.2093),
            'NUTLEY': (40.8223, -74.1601),
            'WEST ORANGE': (40.7979, -74.2390),
            'GLOUCESTER COUNTY': (39.7065, -75.2174),
            'WOODBURY': (39.8384, -75.1568),
            'HUDSON COUNTY': (40.7439, -74.0324),
            'HOBOKEN': (40.7439, -74.0324),
            'JERSEY CITY': (40.7282, -74.0776),
            'HUNTERDON COUNTY': (40.5687, -74.9090),
            'HOPEWELL': (40.3884, -74.7621),
            'FLEMINGTON': (40.5123, -74.8593),
            'MERCER COUNTY': (40.2206, -74.7565),
            'PENNINGTON': (40.3276, -74.7893),
            'PRINCETON': (40.3573, -74.6672),
            'TRENTON': (40.2206, -74.7565),
            'MIDDLESEX COUNTY': (40.4862, -74.4518),
            'METUCHEN': (40.5426, -74.3635),
            'NEW BRUNSWICK': (40.4862, -74.4518),
            'MONMOUTH COUNTY': (40.3140, -74.2020),
            'BRICK': (40.0576, -74.1157),
            'FREEHOLD': (40.2618, -74.2743),
            'LONG BRANCH': (40.3043, -73.9924),
            'MARLBORO': (40.3151, -74.2465),
            'MORRIS COUNTY': (40.7968, -74.4815),
            'BOONTON': (40.9026, -74.4071),
            'CHATHAM': (40.7401, -74.3854),
            'DENVILLE': (40.8912, -74.4882),
            'MORRISTOWN': (40.7968, -74.4815),
            'OCEAN COUNTY': (39.9526, -74.2772),
            'BARNEGAT': (39.7526, -74.2226),
            'LACEY': (39.8540, -74.2124),
            'SEASIDE PARK': (39.9298, -74.0776),
            'TOMS RIVER': (39.9537, -74.1979),
            'PASSAIC COUNTY': (40.9596, -74.1838),
            'CLIFTON': (40.8584, -74.1638),
            'HAWTHORNE': (40.9493, -74.1543),
            'LITTLE FALLS': (40.8751, -74.2176),
            'WAYNE': (40.9254, -74.2765),
            'WEST MILFORD': (41.1312, -74.3665),
            'SALEM COUNTY': (39.5701, -75.4677),
            'SALEM': (39.5701, -75.4677),
            'SOMERSET COUNTY': (40.5965, -74.6093),
            'BEDMINSTER': (40.6787, -74.6379),
            'BERNARDSVILLE': (40.7173, -74.5665),
            'BRIDGEWATER': (40.5965, -74.6093),
            'SUSSEX COUNTY': (41.1785, -74.6446),
            'BLAIRSTOWN': (40.9857, -74.9579),
            'SPARTA': (41.0323, -74.6293),
            'UNION COUNTY': (40.6976, -74.2632),
            'ELIZABETH': (40.6640, -74.2107),
            'RAHWAY': (40.6084, -74.2776),
            'SCOTCH PLAINS': (40.6301, -74.3893),
            'SPRINGFIELD': (40.6990, -74.3204),
            'SUMMIT': (40.7163, -74.3643),
            'UNION': (40.6976, -74.2632),
            'WARREN COUNTY': (40.8384, -75.0115),
            'WASHINGTON': (40.7579, -74.9818)
        }

    def run_complete_pipeline(self, csv_file: str = "websites_only.csv") -> Dict:
        """Run the complete pipeline from vendor discovery to network analysis"""
        print("🚀 Starting complete farmers market network analysis pipeline")
        print("=" * 60)

        pipeline_start = time.time()

        # Phase 1: Vendor Page Discovery
        print("\n📍 PHASE 1: Vendor Page Discovery")
        print("-" * 40)

        finder = VendorPageFinder(delay=1.5)
        vendor_pages = finder.process_all_markets(csv_file)

        # Phase 2: Claude Extraction (only if API key provided)
        if self.anthropic_api_key:
            print("\n🧠 PHASE 2: Claude-Powered Vendor Extraction")
            print("-" * 40)

            extractor = ClaudeVendorExtractor(api_key=self.anthropic_api_key)
            extraction_results = extractor.process_vendor_pages("vendor_pages_discovery.json")
        else:
            print("\n⚠️  PHASE 2: Skipping Claude extraction (no API key provided)")
            extraction_results = []

        # Phase 3: Network Analysis
        print("\n🕸️  PHASE 3: Network Analysis & Location Enhancement")
        print("-" * 40)

        network_data = self._build_network_analysis(vendor_pages, extraction_results, csv_file)

        # Phase 4: Final Results
        total_time = time.time() - pipeline_start
        print("\n✅ PIPELINE COMPLETE")
        print("=" * 60)
        print(f"Total processing time: {total_time/60:.1f} minutes")
        print(f"Markets processed: {len(vendor_pages)}")
        print(f"Vendor pages found: {sum(1 for p in vendor_pages if p['vendor_page_found'])}")

        if extraction_results:
            total_vendors = sum(len(r['vendors']) for r in extraction_results)
            print(f"Vendors extracted: {total_vendors}")

        print(f"Network relationships: {len(network_data.get('relationships', []))}")
        print(f"Results saved to: farmers_market_network.json")

        return network_data

    def _build_network_analysis(self, vendor_pages: List[Dict], extraction_results: List[Dict], csv_file: str) -> Dict:
        """Build complete network analysis with location data"""

        # Load market data from original CSV
        markets = self._load_market_data(csv_file)

        # Process farms from extraction results
        farms = self._process_farms(extraction_results)

        # Build relationships
        relationships = self._build_relationships(farms, markets)

        # Calculate distances
        self._calculate_distances(relationships, farms, markets)

        # Create final network data
        network_data = {
            'metadata': {
                'generated_timestamp': time.time(),
                'description': 'New Jersey Farmers Market Network Analysis',
                'data_sources': ['websites_only.csv', 'Farmers Markets NJ.csv', 'Claude extraction'],
                'statistics': {
                    'total_markets': len(markets),
                    'total_farms': len(farms),
                    'total_relationships': len(relationships),
                    'markets_with_vendor_pages': sum(1 for p in vendor_pages if p['vendor_page_found']),
                    'successful_extractions': len([r for r in extraction_results if r.get('extraction_success', False)])
                }
            },
            'markets': [asdict(market) for market in markets.values()],
            'farms': [asdict(farm) for farm in farms.values()],
            'relationships': [asdict(rel) for rel in relationships]
        }

        # Save results
        with open('farmers_market_network.json', 'w') as f:
            json.dump(network_data, f, indent=2)

        print(f"Network analysis saved with {len(farms)} farms and {len(relationships)} relationships")

        return network_data

    def _load_market_data(self, csv_file: str) -> Dict[str, Market]:
        """Load market data from original CSV"""
        markets = {}

        # Load from Farmers Markets NJ.csv if it exists
        try:
            with open('Farmers Markets NJ.csv', 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('Name') and row['Name'].strip():
                        name = self._normalize_market_name(row['Name'])
                        markets[name] = Market(
                            name=name,
                            original_name=row['Name'],
                            url=row.get('URL', ''),
                            location=row.get('Address/Location', ''),
                            coordinates=self._get_coordinates(name),
                            vendor_count=0
                        )
        except FileNotFoundError:
            print("   Warning: Farmers Markets NJ.csv not found, using website data only")

        # Also load from websites_only.csv to ensure we have all markets
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if row and not row[0].startswith('#'):
                    url = row[0].strip()
                    if url:
                        market_name = url.replace('https://', '').replace('http://', '').split('/')[0]
                        normalized_name = self._normalize_market_name(market_name)

                        if normalized_name not in markets:
                            markets[normalized_name] = Market(
                                name=normalized_name,
                                original_name=market_name,
                                url=f"https://{url}" if not url.startswith('http') else url,
                                location=None,
                                coordinates=self._get_coordinates(normalized_name),
                                vendor_count=0
                            )

        return markets

    def _normalize_market_name(self, name: str) -> str:
        """Normalize market names for consistent matching"""
        # Remove common suffixes and clean up
        name = name.replace('.com', '').replace('.org', '').replace('.net', '').replace('.us', '')
        name = name.replace('farmers', '').replace('market', '').strip()

        # Convert specific patterns
        domain_mapping = {
            'asburyfresh': 'ASBURY PARK',
            'barnegat': 'BARNEGAT',
            'bedminster': 'BEDMINSTER',
            'berlinfarmers': 'BERLIN',
            'bernardsvillefarmers': 'BERNARDSVILLE',
            'brickfarmers': 'BRICK',
            'columbusfarmers': 'COLUMBUS',
            'ramseyfarmers': 'RAMSEY',
            'vcfm': 'VENTNOR CITY'
        }

        name_clean = name.lower()
        for domain, location in domain_mapping.items():
            if domain in name_clean:
                return location

        return name.upper()

    def _get_coordinates(self, market_name: str) -> Optional[Tuple[float, float]]:
        """Get coordinates for a market"""
        return self.nj_coordinates.get(market_name)

    def _process_farms(self, extraction_results: List[Dict]) -> Dict[str, Farm]:
        """Process extraction results into Farm objects"""
        farms = {}

        for result in extraction_results:
            if not result.get('extraction_success', False):
                continue

            market_name = self._normalize_market_name(result.get('market_name', ''))

            for vendor_data in result.get('vendors', []):
                farm_name = vendor_data.get('name', '').strip()
                if not farm_name or len(farm_name) < 3:
                    continue

                if farm_name not in farms:
                    farms[farm_name] = Farm(
                        name=farm_name,
                        business_type=vendor_data.get('business_type'),
                        products=vendor_data.get('products', []) or [],
                        location=vendor_data.get('location'),
                        contact_info=vendor_data.get('contact_info', {}) or {},
                        markets_attended=[],
                        confidence=vendor_data.get('confidence', 0.8)
                    )

                # Add market to farm's attendance list
                if market_name and market_name not in farms[farm_name].markets_attended:
                    farms[farm_name].markets_attended.append(market_name)

        return farms

    def _build_relationships(self, farms: Dict[str, Farm], markets: Dict[str, Market]) -> List[NetworkRelationship]:
        """Build farm-market relationships"""
        relationships = []

        for farm_name, farm in farms.items():
            for market_name in farm.markets_attended:
                if market_name in markets:
                    relationships.append(NetworkRelationship(
                        farm_name=farm_name,
                        market_name=market_name,
                        distance_miles=None,  # Will be calculated
                        products_sold=farm.products
                    ))

                    # Update market vendor count
                    markets[market_name].vendor_count += 1

        return relationships

    def _calculate_distances(self, relationships: List[NetworkRelationship],
                           farms: Dict[str, Farm], markets: Dict[str, Market]):
        """Calculate distances for all relationships"""
        for relationship in relationships:
            farm = farms.get(relationship.farm_name)
            market = markets.get(relationship.market_name)

            if farm and market:
                # Try to calculate exact distance if we have coordinates
                farm_coords = self._geocode_farm_location(farm.location)
                market_coords = market.coordinates

                if farm_coords and market_coords:
                    relationship.distance_miles = self._haversine_distance(farm_coords, market_coords)
                else:
                    # Use estimated distance based on NJ geography
                    relationship.distance_miles = self._estimate_nj_distance(farm.location, market.location)

    def _geocode_farm_location(self, location: str) -> Optional[Tuple[float, float]]:
        """Simple geocoding for farm locations"""
        if not location:
            return None

        # Check if location contains known NJ places
        location_upper = location.upper()
        for place, coords in self.nj_coordinates.items():
            if place in location_upper:
                return coords

        return None

    def _estimate_nj_distance(self, farm_location: str, market_location: str) -> float:
        """Estimate distance based on NJ geography"""
        # Default estimates based on typical NJ distances
        if not farm_location and not market_location:
            return 25.0  # Average NJ distance

        # If locations are similar, assume closer
        if farm_location and market_location:
            if any(word in market_location.lower() for word in farm_location.lower().split() if len(word) > 3):
                return 10.0

        return 30.0  # Default estimate

    def _haversine_distance(self, coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
        """Calculate distance between coordinates using Haversine formula"""
        lat1, lon1 = coord1
        lat2, lon2 = coord2

        # Convert to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))

        # Earth's radius in miles
        return round(3959 * c, 1)

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Complete farmers market network analysis pipeline")
    parser.add_argument('--csv-file', default='websites_only.csv', help='CSV file with market URLs')
    parser.add_argument('--api-key', help='Anthropic API key for Claude extraction')
    parser.add_argument('--discovery-only', action='store_true', help='Only run vendor page discovery')
    parser.add_argument('--extraction-only', action='store_true', help='Only run Claude extraction (requires existing discovery results)')
    parser.add_argument('--network-analysis-only', action='store_true', help='Only run network analysis (requires existing extraction results)')
    parser.add_argument('--extraction-file', default='claude_vendor_extraction.json', help='Extraction results file for network analysis')

    args = parser.parse_args()

    pipeline = NetworkPipeline(anthropic_api_key=args.api_key)

    if args.discovery_only:
        print("🔍 Running vendor page discovery only")
        finder = VendorPageFinder()
        finder.process_all_markets(args.csv_file)
    elif args.extraction_only:
        if not args.api_key:
            print("❌ API key required for extraction. Use --api-key or set ANTHROPIC_API_KEY")
            return
        print("🧠 Running Claude extraction only")
        extractor = ClaudeVendorExtractor(api_key=args.api_key)
        extractor.process_vendor_pages("vendor_pages_discovery.json")
    elif args.network_analysis_only:
        print("📊 Running network analysis only")
        import os
        if not os.path.exists(args.extraction_file):
            print(f"❌ Extraction file not found: {args.extraction_file}")
            print("   Run extraction first or specify correct file with --extraction-file")
            return

        # Load extraction results and run network analysis
        print(f"   Loading extraction results from: {args.extraction_file}")
        import json
        with open(args.extraction_file, 'r') as f:
            extraction_results = json.load(f)

        # For network analysis only, we don't need vendor pages discovery data
        # Create empty vendor pages list since we already have extraction results
        vendor_pages = []

        analysis = pipeline._build_network_analysis(vendor_pages, extraction_results, args.csv_file)

        # Save analysis results
        output_file = "network_analysis_results.json"
        with open(output_file, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        print(f"   Network analysis saved to: {output_file}")

        # Print summary
        total_farms = len(analysis.get('farms', {}))
        total_markets = len(analysis.get('markets', {}))
        total_relationships = len(analysis.get('relationships', []))
        print(f"   Summary: {total_farms} farms, {total_markets} markets, {total_relationships} relationships")
    else:
        # Run complete pipeline
        pipeline.run_complete_pipeline(args.csv_file)

if __name__ == "__main__":
    main()