#!/usr/bin/env python3
"""
Distance Calculator for Farm-Market Network Analysis
Geocodes locations and calculates travel distances for farmers market network
"""

import json
import math
import time
import re
from typing import Dict, Tuple, Optional
import requests

class DistanceCalculator:
    def __init__(self, use_api: bool = False, api_key: str = None):
        self.use_api = use_api
        self.api_key = api_key
        self.location_cache = {}  # Cache geocoded locations

        # Approximate coordinates for known NJ locations (fallback)
        self.nj_locations = {
            'ASBURY PARK': (40.2204, -74.0118),
            'ATLANTIC CITY': (39.3643, -74.4229),
            'BARNEGAT': (39.7526, -74.2226),
            'BEDMINSTER': (40.6787, -74.6379),
            'BERLIN': (39.7912, -74.9290),
            'BERNARDSVILLE': (40.7173, -74.5665),
            'BLAIRSTOWN': (40.9857, -74.9579),
            'BLOOMFIELD': (40.8068, -74.1854),
            'BOONTON': (40.9026, -74.4071),
            'BRICK': (40.0576, -74.1157),
            'BRIDGEWATER': (40.5965, -74.6093),
            'CAMDEN': (39.9259, -75.1196),
            'CHATHAM': (40.7401, -74.3854),
            'COLLINGSWOOD': (39.9181, -75.0718),
            'COLUMBUS': (40.0448, -74.6932),
            'DENVILLE': (40.8912, -74.4882),
            'ELIZABETH': (40.6640, -74.2107),
            'ENGLISHTOWN': (40.2979, -74.3507),
            'FREEHOLD': (40.2618, -74.2743),
            'GALLOWAY': (39.4665, -74.4704),
            'HADDONFIELD': (39.8912, -75.0376),
            'HOBOKEN': (40.7439, -74.0324),
            'HOPEWELL': (40.3884, -74.7621),
            'JERSEY CITY': (40.7282, -74.0776),
            'LACEY': (39.8540, -74.2124),
            'LONG BRANCH': (40.3043, -73.9924),
            'MAPLEWOOD': (40.7312, -74.2732),
            'MARLBORO': (40.3151, -74.2465),
            'METUCHEN': (40.5426, -74.3635),
            'MONTCLAIR': (40.8176, -74.2093),
            'MORRISTOWN': (40.7968, -74.4815),
            'NEWARK': (40.7357, -74.1724),
            'NEW BRUNSWICK': (40.4862, -74.4518),
            'NUTLEY': (40.8223, -74.1601),
            'OCEAN CITY': (39.2776, -74.5746),
            'PENNINGTON': (40.3276, -74.7893),
            'PRINCETON': (40.3573, -74.6672),
            'RAHWAY': (40.6084, -74.2776),
            'RAMSEY': (41.0576, -74.1410),
            'RIDGEWOOD': (40.9787, -74.1165),
            'RIVER EDGE': (40.9287, -74.0393),
            'RIVERVIEW': (40.2454, -74.5643),
            'RUTHERFORD': (40.8265, -74.1071),
            'SCOTCH PLAINS': (40.6301, -74.3893),
            'SPARTA': (41.0323, -74.6293),
            'SPRINGFIELD': (40.6990, -74.3204),
            'SUMMIT': (40.7163, -74.3643),
            'TRENTON': (40.2206, -74.7565),
            'UNION': (40.6976, -74.2632),
            'VENTNOR CITY': (39.3398, -74.4743),
            'WASHINGTON': (40.7579, -74.9818),
            'WEST MILFORD': (41.1312, -74.3665),
            'WOODBURY': (39.8384, -75.1568)
        }

    def geocode_location(self, location: str, state: str = "NJ") -> Optional[Tuple[float, float]]:
        """Geocode a location string to latitude/longitude coordinates"""
        if not location or len(location.strip()) < 3:
            return None

        # Check cache first
        cache_key = f"{location}, {state}".upper()
        if cache_key in self.location_cache:
            return self.location_cache[cache_key]

        # Clean up location string
        clean_location = self._clean_location_string(location)

        # Try exact match with known NJ locations
        for nj_city, coords in self.nj_locations.items():
            if nj_city in clean_location.upper():
                self.location_cache[cache_key] = coords
                return coords

        # If using API, try geocoding service
        if self.use_api and self.api_key:
            coords = self._geocode_with_api(f"{clean_location}, {state}")
            if coords:
                self.location_cache[cache_key] = coords
                return coords

        # Fallback: extract city name and try partial matching
        city_name = self._extract_city_name(clean_location)
        if city_name:
            for nj_city, coords in self.nj_locations.items():
                if city_name.upper() in nj_city or nj_city in city_name.upper():
                    self.location_cache[cache_key] = coords
                    return coords

        return None

    def _clean_location_string(self, location: str) -> str:
        """Clean up location string for better matching"""
        if not location:
            return ""

        # Remove common prefixes/suffixes
        location = re.sub(r'^\d+\s+', '', location)  # Remove leading numbers
        location = re.sub(r'\b(road|rd|street|st|avenue|ave|lane|ln|drive|dr|boulevard|blvd)\b', '', location, flags=re.IGNORECASE)
        location = re.sub(r'\b(new jersey|nj)\b', '', location, flags=re.IGNORECASE)
        location = re.sub(r'\s+', ' ', location).strip()

        return location

    def _extract_city_name(self, location: str) -> str:
        """Extract city name from location string"""
        # Look for patterns like "City, State" or just "City"
        if ',' in location:
            parts = location.split(',')
            return parts[-2].strip() if len(parts) > 1 else parts[0].strip()

        # Take the last word that looks like a city name
        words = location.split()
        for word in reversed(words):
            if len(word) > 2 and word.isalpha():
                return word

        return location

    def _geocode_with_api(self, address: str) -> Optional[Tuple[float, float]]:
        """Geocode using an external API (placeholder)"""
        # This would use a service like Google Maps, MapBox, or OpenCage
        # For now, return None to use fallback methods
        return None

    def calculate_distance(self, coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
        """Calculate distance between two coordinates using Haversine formula"""
        if not coord1 or not coord2:
            return 0.0

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
        earth_radius_miles = 3959
        distance = earth_radius_miles * c

        return round(distance, 1)

    def estimate_nj_average_distance(self, farm_location: str, market_location: str) -> float:
        """Estimate distance based on New Jersey geography patterns"""
        # If we have any location information, use geography-based estimates
        if not farm_location and not market_location:
            return 30.0  # Average distance for NJ

        # Use heuristics based on location strings
        if farm_location and market_location:
            # If both have similar text, assume they're close
            if any(word in market_location.lower() for word in farm_location.lower().split() if len(word) > 3):
                return 15.0  # Same general area

        # Default estimates based on NJ geography
        north_keywords = ['morris', 'bergen', 'passaic', 'sussex', 'warren', 'essex']
        central_keywords = ['middlesex', 'union', 'somerset', 'hunterdon', 'mercer']
        south_keywords = ['camden', 'gloucester', 'burlington', 'ocean', 'monmouth', 'atlantic', 'cape', 'salem', 'cumberland']

        farm_region = self._classify_region(farm_location, north_keywords, central_keywords, south_keywords)
        market_region = self._classify_region(market_location, north_keywords, central_keywords, south_keywords)

        # Same region: shorter distance
        if farm_region == market_region and farm_region != 'unknown':
            return 20.0

        # Adjacent regions: medium distance
        if abs(farm_region_num := {'north': 1, 'central': 2, 'south': 3}.get(farm_region, 2)) - \
           abs(market_region_num := {'north': 1, 'central': 2, 'south': 3}.get(market_region, 2)) == 1:
            return 35.0

        # Different regions: longer distance
        if farm_region != 'unknown' and market_region != 'unknown' and farm_region != market_region:
            return 50.0

        return 30.0  # Default

    def _classify_region(self, location: str, north_kw: list, central_kw: list, south_kw: list) -> str:
        """Classify location into North, Central, or South Jersey"""
        if not location:
            return 'unknown'

        location_lower = location.lower()

        if any(kw in location_lower for kw in north_kw):
            return 'north'
        elif any(kw in location_lower for kw in central_kw):
            return 'central'
        elif any(kw in location_lower for kw in south_kw):
            return 'south'
        else:
            return 'unknown'

    def update_network_with_distances(self, network_file: str = "network_analysis_data.json",
                                    output_file: str = "network_analysis_with_distances.json"):
        """Update network analysis with calculated distances"""
        print("Loading network data...")

        with open(network_file, 'r') as f:
            network_data = json.load(f)

        print("Calculating distances for farm-market relationships...")

        total_calculated = 0
        total_estimated = 0

        # Update relationships with distances
        for relationship in network_data['relationships']:
            farm_name = relationship['farm_name']
            market_name = relationship['market_name']

            # Find farm and market details
            farm_location = ""
            market_location = ""

            for farm in network_data['farms']:
                if farm['farm_name'] == farm_name:
                    farm_location = farm.get('farm_location', '')
                    break

            for market in network_data['markets']:
                if market['market_name'] == market_name:
                    market_location = market.get('location', '')
                    break

            # Try to geocode and calculate exact distance
            farm_coords = self.geocode_location(farm_location)
            market_coords = self.geocode_location(market_location)

            if farm_coords and market_coords:
                distance = self.calculate_distance(farm_coords, market_coords)
                relationship['distance_calculation_method'] = 'geocoded'
                total_calculated += 1
            else:
                distance = self.estimate_nj_average_distance(farm_location, market_location)
                relationship['distance_calculation_method'] = 'estimated'
                total_estimated += 1

            relationship['distance_miles'] = distance

        # Update farm summaries with new distance calculations
        for farm in network_data['farms']:
            farm_relationships = [r for r in network_data['relationships'] if r['farm_name'] == farm['farm_name']]

            if farm_relationships:
                total_distance = sum(r['distance_miles'] for r in farm_relationships)
                farm['total_miles_per_season'] = total_distance * 20  # 20 market days per season
                farm['average_distance_per_market'] = total_distance / len(farm_relationships)
            else:
                farm['total_miles_per_season'] = 0
                farm['average_distance_per_market'] = 0

        # Update network statistics
        total_seasonal_miles = sum(f.get('total_miles_per_season', 0) for f in network_data['farms'])
        network_data['metadata']['network_statistics']['estimated_total_miles_per_season'] = total_seasonal_miles
        network_data['metadata']['distance_calculation_summary'] = {
            'total_relationships': len(network_data['relationships']),
            'geocoded_distances': total_calculated,
            'estimated_distances': total_estimated,
            'total_seasonal_miles': total_seasonal_miles
        }

        # Save updated network data
        with open(output_file, 'w') as f:
            json.dump(network_data, f, indent=2)

        print(f"Distance calculations complete!")
        print(f"  - Geocoded distances: {total_calculated}")
        print(f"  - Estimated distances: {total_estimated}")
        print(f"  - Total estimated miles per season: {total_seasonal_miles:,.0f}")
        print(f"Updated network data saved to: {output_file}")

        return network_data

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Calculate distances for farm-market network")
    parser.add_argument('--network-file', default='network_analysis_data.json', help='Network analysis input file')
    parser.add_argument('--output', default='network_analysis_with_distances.json', help='Output file with distances')
    parser.add_argument('--use-api', action='store_true', help='Use external geocoding API')
    parser.add_argument('--api-key', help='API key for geocoding service')

    args = parser.parse_args()

    calculator = DistanceCalculator(use_api=args.use_api, api_key=args.api_key)
    calculator.update_network_with_distances(args.network_file, args.output)

if __name__ == "__main__":
    main()