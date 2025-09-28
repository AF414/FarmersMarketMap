#!/usr/bin/env python3
"""
Test script to debug Bernardsville content extraction
"""

import requests
from bs4 import BeautifulSoup

def test_content_extraction_old_way():
    """Test the current (problematic) content extraction"""
    url = "https://www.bernardsvillefarmersmarket.com/summer-vendors"

    response = requests.get(url, timeout=15)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Current method - removes all scripts
    for script in soup(["script", "style", "nav", "footer", "header"]):
        script.decompose()

    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = ' '.join(chunk for chunk in chunks if chunk)

    print("=== OLD METHOD (removes all scripts) ===")
    print(f"Content length: {len(text)}")
    print(f"Contains 'AquaSprout': {'AquaSprout' in text}")
    print(f"Contains 'Farms': {'Farm' in text}")
    print("First 500 chars:")
    print(text[:500])
    print("\n")

def test_content_extraction_new_way():
    """Test improved content extraction that preserves vendor data"""
    url = "https://www.bernardsvillefarmersmarket.com/summer-vendors"

    response = requests.get(url, timeout=15)
    soup = BeautifulSoup(response.text, 'html.parser')

    # NEW method - extract text from scripts first, then remove them
    script_content = ""
    for script in soup.find_all("script"):
        if script.string:
            script_content += " " + script.string

    # Remove scripts and other elements
    for element in soup(["script", "style", "nav", "footer", "header"]):
        element.decompose()

    # Get visible text
    text = soup.get_text()

    # Combine visible text with script content
    combined_text = text + " " + script_content

    # Clean up whitespace
    lines = (line.strip() for line in combined_text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    combined_text = ' '.join(chunk for chunk in chunks if chunk)

    print("=== NEW METHOD (extracts from scripts first) ===")
    print(f"Content length: {len(combined_text)}")
    print(f"Contains 'AquaSprout': {'AquaSprout' in combined_text}")
    print(f"Contains 'Farm': {'Farm' in combined_text}")
    print("First 500 chars:")
    print(combined_text[:500])
    print("\n")

    if len(combined_text) > 8000:
        combined_text = combined_text[:8000] + "..."

    return combined_text

if __name__ == "__main__":
    test_content_extraction_old_way()
    improved_content = test_content_extraction_new_way()

    # Look for vendor names specifically
    print("=== VENDOR NAME SEARCH ===")
    vendor_names = ["AquaSprout", "Brick Farm Market", "Black River Roasters"]
    for name in vendor_names:
        print(f"'{name}' found: {name in improved_content}")