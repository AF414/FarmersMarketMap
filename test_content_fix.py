#!/usr/bin/env python3
"""
Test the improved content extraction on Bernardsville
"""

import requests
from bs4 import BeautifulSoup
import sys
import os

# Add current directory to path to import our extractor
sys.path.append(os.path.dirname(__file__))

def test_old_vs_new_extraction():
    """Compare old vs new content extraction methods"""
    url = "https://www.bernardsvillefarmersmarket.com/summer-vendors"

    print(f"Testing content extraction on: {url}")
    print("=" * 60)

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # OLD METHOD - what was happening before
        soup_old = BeautifulSoup(response.text, 'html.parser')
        for script in soup_old(["script", "style", "nav", "footer", "header"]):
            script.decompose()

        old_text = soup_old.get_text()
        lines = (line.strip() for line in old_text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        old_text = ' '.join(chunk for chunk in chunks if chunk)

        print("OLD METHOD RESULTS:")
        print(f"Content length: {len(old_text)}")
        vendor_keywords = ["AquaSprout", "Farm", "Bakery", "Kitchen", "Dairy", "Orchard"]
        for keyword in vendor_keywords:
            count = old_text.count(keyword)
            print(f"  '{keyword}' appears {count} times")
        print()

        # NEW METHOD - our fix
        soup_new = BeautifulSoup(response.text, 'html.parser')

        # Extract script content first
        script_content = ""
        for script in soup_new.find_all("script"):
            if script.string:
                script_content += " " + script.string

        # Remove elements
        for element in soup_new(["script", "style", "nav", "footer", "header"]):
            element.decompose()

        # Get visible text and combine
        visible_text = soup_new.get_text()
        combined_text = visible_text + " " + script_content

        # Clean whitespace
        lines = (line.strip() for line in combined_text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        new_text = ' '.join(chunk for chunk in chunks if chunk)

        if len(new_text) > 8000:
            new_text = new_text[:8000] + "..."

        print("NEW METHOD RESULTS:")
        print(f"Content length: {len(new_text)}")
        for keyword in vendor_keywords:
            count = new_text.count(keyword)
            print(f"  '{keyword}' appears {count} times")
        print()

        # Look for specific vendor names we know should be there
        print("SPECIFIC VENDOR SEARCH:")
        known_vendors = ["AquaSprout", "Black River", "Brick Farm", "Sweet Sassy"]
        for vendor in known_vendors:
            old_found = vendor in old_text
            new_found = vendor in new_text
            print(f"  '{vendor}': Old={old_found}, New={new_found}")
        print()

        print(f"Improvement factor: {len(new_text) / len(old_text):.1f}x more content")

        # Save samples for inspection
        with open("bernardsville_old_content.txt", "w") as f:
            f.write(old_text)
        with open("bernardsville_new_content.txt", "w") as f:
            f.write(new_text)
        print("Content samples saved to bernardsville_old_content.txt and bernardsville_new_content.txt")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_old_vs_new_extraction()